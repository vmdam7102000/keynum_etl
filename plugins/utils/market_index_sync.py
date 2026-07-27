from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

from plugins.utils.api_utils import request_json

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PGConnection


INDEX_COLUMNS: Tuple[str, ...] = (
    "vn_index",
    "upcom_index",
    "vn30_index",
    "hnx30_index",
)
_MONEY_QUANTUM = Decimal("0.01")


def utc_now_naive() -> datetime:
    """Return the current UTC timestamp for a Postgres `timestamp` column."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _unwrap_records(payload: Any, code: str) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        payload = payload.get("data", payload)

    if payload is None:
        raise ValueError(f"No payload returned for market index {code}")
    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list):
        raise ValueError(
            f"Unexpected payload type for market index {code}: {type(payload).__name__}"
        )
    if not all(isinstance(record, dict) for record in payload):
        raise ValueError(f"Market index payload for {code} contains a non-object record")
    return payload


def _parse_source_date(value: Any) -> Optional[date]:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _parse_index_value(value: Any) -> Optional[Decimal]:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value)).quantize(_MONEY_QUANTUM, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed > 0 else None


def normalize_market_index_records(
    payload: Any,
    *,
    code: str,
    from_date: date,
    to_date: date,
) -> Dict[date, Decimal]:
    """Validate one WiFeed index response and return date -> close_root."""
    raw_records = _unwrap_records(payload, code)
    if not raw_records:
        return {}

    normalized: Dict[date, Decimal] = {}
    matching_records = 0
    for record in raw_records:
        if record.get("mack") != code:
            logging.warning(
                "Discarding market index record for %s while fetching %s",
                record.get("mack"),
                code,
            )
            continue
        matching_records += 1

        record_date = _parse_source_date(record.get("ngay"))
        if record_date is None:
            logging.warning("Discarding %s record with invalid ngay=%r", code, record.get("ngay"))
            continue
        if not from_date <= record_date <= to_date:
            logging.warning(
                "Discarding %s record outside requested range: %s", code, record_date
            )
            continue

        close_root = _parse_index_value(record.get("close_root"))
        if close_root is None:
            logging.warning(
                "Discarding %s record on %s with invalid close_root=%r",
                code,
                record_date,
                record.get("close_root"),
            )
            continue
        if record_date in normalized:
            raise ValueError(f"Duplicate market index record for {code} on {record_date}")
        normalized[record_date] = close_root

    if matching_records == 0:
        raise ValueError(f"Market index payload contains no records for requested code {code}")
    return normalized


def fetch_market_index_series(
    *,
    code: str,
    from_date: date,
    to_date: date,
    api_cfg: Mapping[str, Any],
    api_key: str,
) -> Dict[date, Decimal]:
    """Fetch a single market-index series from WiFeed."""
    params = {
        "code": code,
        "from-date": from_date.isoformat(),
        "to-date": to_date.isoformat(),
        "apikey": api_key,
    }
    payload = request_json(
        api_cfg["url"],
        params=params,
        timeout=api_cfg.get("timeout", 30),
    )
    if payload is None:
        raise RuntimeError(f"WiFeed request failed for market index {code}")
    return normalize_market_index_records(
        payload,
        code=code,
        from_date=from_date,
        to_date=to_date,
    )


def fetch_market_index_records(
    *,
    from_date: date,
    to_date: date,
    api_cfg: Mapping[str, Any],
    api_key: str,
) -> List[Dict[str, Any]]:
    """Fetch and pivot all configured index series into one record per date."""
    rows_by_date: Dict[date, Dict[str, Any]] = {}
    code_mapping = api_cfg["codes"]
    for position, (code, target_column) in enumerate(code_mapping.items()):
        if target_column not in INDEX_COLUMNS:
            raise ValueError(f"Unsupported market index target column: {target_column}")

        series = fetch_market_index_series(
            code=code,
            from_date=from_date,
            to_date=to_date,
            api_cfg=api_cfg,
            api_key=api_key,
        )
        for record_date, value in series.items():
            row = rows_by_date.setdefault(record_date, {"ngay": record_date})
            row[target_column] = value

        if position < len(code_mapping) - 1:
            time.sleep(api_cfg.get("throttle_seconds", 0))

    return [rows_by_date[record_date] for record_date in sorted(rows_by_date)]


def build_sync_ranges(
    *,
    logical_date: datetime,
    run_conf: Optional[Mapping[str, Any]],
    api_cfg: Mapping[str, Any],
) -> List[Tuple[date, date]]:
    """Build either one rolling window or bounded annual backfill windows."""
    effective_conf = run_conf or {}
    to_date = logical_date.date()

    if effective_conf.get("mode") != "backfill":
        lookback_days = int(api_cfg["lookback_days"])
        if lookback_days < 1:
            raise ValueError("lookback_days must be at least 1")
        return [(to_date - timedelta(days=lookback_days - 1), to_date)]

    from_value = effective_conf.get("from_date", api_cfg["history_start_date"])
    to_value = effective_conf.get("to_date", to_date.isoformat())
    try:
        from_date = date.fromisoformat(str(from_value))
        backfill_to_date = date.fromisoformat(str(to_value))
    except ValueError as exc:
        raise ValueError("backfill from_date and to_date must use YYYY-MM-DD") from exc
    if from_date > backfill_to_date:
        raise ValueError("backfill from_date must not be after to_date")

    batch_days = int(api_cfg["backfill_batch_days"])
    if batch_days < 1:
        raise ValueError("backfill_batch_days must be at least 1")

    ranges: List[Tuple[date, date]] = []
    batch_start = from_date
    while batch_start <= backfill_to_date:
        batch_end = min(batch_start + timedelta(days=batch_days - 1), backfill_to_date)
        ranges.append((batch_start, batch_end))
        batch_start = batch_end + timedelta(days=1)
    return ranges


def upsert_market_index_records(
    *,
    conn: PGConnection,
    table: str,
    records: Sequence[Mapping[str, Any]],
    ingested_at: datetime,
) -> None:
    """Atomically upsert partial index rows without replacing present values by NULL."""
    if not records:
        return

    columns = ("ngay", *INDEX_COLUMNS, "created_time", "updated_time")
    placeholders = ", ".join(["%s"] * len(columns))
    update_clause = ",\n            ".join(
        [f"{column} = COALESCE(EXCLUDED.{column}, target.{column})" for column in INDEX_COLUMNS]
        + ["updated_time = EXCLUDED.updated_time"]
    )
    sql = f"""
        INSERT INTO {table} AS target ({", ".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT (ngay) DO UPDATE SET
            {update_clause}
    """

    cursor = conn.cursor()
    try:
        for record in records:
            values = [record.get("ngay")]
            values.extend(record.get(column) for column in INDEX_COLUMNS)
            values.extend([ingested_at, ingested_at])
            cursor.execute(sql, values)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
