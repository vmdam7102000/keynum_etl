from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


UTC = timezone.utc
TIMEFRAME_MS = 3 * 60 * 1000


@dataclass(frozen=True)
class CmcBackfillJob:
    symbol: str
    exchange: str
    data_start_at: datetime
    requested_from: datetime
    requested_to: datetime
    existing_first_at: Optional[datetime] = None
    existing_last_at: Optional[datetime] = None


def _parse_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError("Backfill dates must use YYYY-MM-DD") from exc


def _parse_bool(value: Any, *, field: str, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value.strip().lower() in {"true", "false"}:
        return value.strip().lower() == "true"
    raise ValueError(f"{field} must be a JSON boolean")


def _parse_symbols(value: Any) -> Optional[List[str]]:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        values = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        values = value
    else:
        raise ValueError("symbols must be a JSON list or comma-separated string")
    symbols = sorted({str(item).strip().upper() for item in values if str(item).strip()})
    return symbols or None


def normalize_backfill_conf(value: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    conf = dict(value or {})
    supported_fields = {
        "mode",
        "symbols",
        "from_date",
        "to_date",
        "confirm_large_backfill",
        "triggered_by_mapping",
    }
    unsupported = sorted(set(conf) - supported_fields)
    if unsupported:
        raise ValueError(
            "Unsupported Phase 1 backfill fields: " + ", ".join(unsupported)
        )
    mode = str(conf.get("mode", "missing_only"))
    if mode not in {"missing_only", "force"}:
        raise ValueError("mode must be missing_only or force")
    normalized = {
        "mode": mode,
        "symbols": _parse_symbols(conf.get("symbols")),
        "from_date": _parse_date(conf.get("from_date")),
        "to_date": _parse_date(conf.get("to_date")),
        "confirm_large_backfill": _parse_bool(
            conf.get("confirm_large_backfill"),
            field="confirm_large_backfill",
            default=False,
        ),
        "triggered_by_mapping": _parse_bool(
            conf.get("triggered_by_mapping"),
            field="triggered_by_mapping",
            default=False,
        ),
    }
    if (
        normalized["from_date"] is not None
        and normalized["to_date"] is not None
        and normalized["from_date"] > normalized["to_date"]
    ):
        raise ValueError("from_date must be on or before to_date")
    return normalized


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def build_jobs(
    rows: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    timeframe_ms: int = TIMEFRAME_MS,
) -> List[CmcBackfillJob]:
    """Build tail-only jobs; existing history is never downloaded again."""
    if timeframe_ms <= 0:
        raise ValueError("timeframe_ms must be positive")
    requested_to_default = _utc(now)
    if to_date:
        requested_to_default = min(
            requested_to_default,
            datetime.combine(to_date + timedelta(days=1), time.min, tzinfo=UTC),
        )

    jobs: List[CmcBackfillJob] = []
    for row in rows:
        data_start_at = _utc(row["data_start_at"])
        existing_first_at = (
            _utc(row["existing_first_at"]) if row.get("existing_first_at") else None
        )
        existing_last_at = (
            _utc(row["existing_last_at"]) if row.get("existing_last_at") else None
        )
        requested_from = (
            existing_last_at + timedelta(milliseconds=timeframe_ms)
            if existing_last_at is not None
            else data_start_at
        )
        if from_date:
            requested_from = max(
                requested_from,
                datetime.combine(from_date, time.min, tzinfo=UTC),
            )
        requested_to = requested_to_default
        if requested_to <= requested_from:
            continue
        jobs.append(
            CmcBackfillJob(
                symbol=str(row["symbol"]).strip().upper(),
                exchange=str(row["selected_exchange"]).strip(),
                data_start_at=data_start_at,
                requested_from=requested_from,
                requested_to=requested_to,
                existing_first_at=existing_first_at,
                existing_last_at=existing_last_at,
            )
        )
    return sorted(jobs, key=lambda job: (job.symbol, job.exchange))


def serialize_job(job: CmcBackfillJob) -> Dict[str, Any]:
    payload = asdict(job)
    for key in (
        "data_start_at",
        "requested_from",
        "requested_to",
        "existing_first_at",
        "existing_last_at",
    ):
        if payload[key] is not None:
            payload[key] = payload[key].isoformat()
    return payload


def deserialize_job(payload: Mapping[str, Any]) -> CmcBackfillJob:
    def parse_optional(key: str) -> Optional[datetime]:
        return datetime.fromisoformat(str(payload[key])) if payload.get(key) else None

    return CmcBackfillJob(
        symbol=str(payload["symbol"]).strip().upper(),
        exchange=str(payload["exchange"]),
        data_start_at=datetime.fromisoformat(str(payload["data_start_at"])),
        requested_from=datetime.fromisoformat(str(payload["requested_from"])),
        requested_to=datetime.fromisoformat(str(payload["requested_to"])),
        existing_first_at=parse_optional("existing_first_at"),
        existing_last_at=parse_optional("existing_last_at"),
    )


def estimated_candle_count(
    jobs: Sequence[CmcBackfillJob], *, timeframe_ms: int = TIMEFRAME_MS
) -> int:
    if timeframe_ms <= 0:
        raise ValueError("timeframe_ms must be positive")
    return sum(
        max(
            0,
            int(
                (job.requested_to - job.requested_from).total_seconds()
                * 1000
                / timeframe_ms
            ),
        )
        for job in jobs
    )


def load_backfill_rows(
    conn,
    *,
    symbol_target_table: str,
    ohlcv_table: str,
    conf: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    conditions = [
        "target.is_canonical IS TRUE",
        "target.mapping_status = 'resolved'",
        "target.is_stablecoin IS FALSE",
        "target.is_wrapped IS FALSE",
        "target.selected_exchange IS NOT NULL",
    ]
    params: List[Any] = []
    if conf.get("symbols"):
        conditions.append("target.symbol = ANY(%s)")
        params.append(list(conf["symbols"]))
    if conf.get("mode") == "missing_only":
        # Complete targets are intentionally inspected too: a daily repair run
        # derives any missing tail from the real OHLCV maximum. `unavailable`
        # is terminal unless an operator explicitly uses force mode.
        conditions.append("target.backfill_status <> 'unavailable'")

    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                target.symbol,
                target.selected_exchange,
                target.data_start_at,
                history.existing_first_at,
                history.existing_last_at,
                target.backfill_status
            FROM {symbol_target_table} AS target
            LEFT JOIN LATERAL (
                SELECT
                    to_timestamp(MIN(price.timestamp) / 1000.0) AS existing_first_at,
                    to_timestamp(MAX(price.timestamp) / 1000.0) AS existing_last_at
                FROM {ohlcv_table} AS price
                WHERE price.symbol = target.symbol
                  AND price.exchange = target.selected_exchange
                  AND price.timestamp >= EXTRACT(
                      EPOCH FROM target.data_start_at
                  ) * 1000
            ) AS history ON TRUE
            WHERE {' AND '.join(conditions)}
            ORDER BY target.symbol, target.selected_exchange
            """,
            params,
        )
        return [
            {
                "symbol": row[0],
                "selected_exchange": row[1],
                "data_start_at": row[2],
                "existing_first_at": row[3],
                "existing_last_at": row[4],
                "backfill_status": row[5],
            }
            for row in cursor.fetchall()
        ]


def normalize_ohlcv_page(
    rows: Iterable[Sequence[Any]],
    *,
    job: CmcBackfillJob,
    timeframe_ms: int,
    closed_before_ms: int,
) -> List[Dict[str, Any]]:
    unique: Dict[int, Dict[str, Any]] = {}
    start_ms = int(job.requested_from.timestamp() * 1000)
    end_ms = int(job.requested_to.timestamp() * 1000)
    for row in rows:
        if len(row) < 6:
            raise ValueError("CCXT OHLCV row must contain timestamp and OHLCV values")
        timestamp_ms = int(row[0])
        if timestamp_ms < start_ms or timestamp_ms >= end_ms:
            continue
        if timestamp_ms + timeframe_ms > closed_before_ms:
            continue
        unique[timestamp_ms] = {
            "symbol": job.symbol,
            "exchange": job.exchange,
            "timestamp": timestamp_ms,
            "open": row[1],
            "high": row[2],
            "low": row[3],
            "close": row[4],
            "volume": row[5],
            "datetime": datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC),
        }
    return [unique[key] for key in sorted(unique)]


def next_page_since(
    current_since_ms: int,
    rows: Sequence[Mapping[str, Any]],
    *,
    timeframe_ms: int = TIMEFRAME_MS,
) -> int:
    if not rows:
        return current_since_ms
    next_since = int(rows[-1]["timestamp"]) + timeframe_ms
    if next_since <= current_since_ms:
        raise RuntimeError("CCXT pagination did not advance")
    return next_since


def upsert_ohlcv_page(
    conn,
    *,
    table: str,
    rows: Sequence[Mapping[str, Any]],
    commit: bool = True,
) -> int:
    if not rows:
        return 0
    from psycopg2.extras import execute_values

    values = [
        (
            row["symbol"],
            row["exchange"],
            row["timestamp"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
            row["datetime"],
        )
        for row in rows
    ]
    with conn.cursor() as cursor:
        execute_values(
            cursor,
            f"""
            INSERT INTO {table} (
                symbol, exchange, timestamp, open, high, low, close, volume, datetime
            ) VALUES %s
            ON CONFLICT (symbol, exchange, timestamp) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                datetime = EXCLUDED.datetime
            """,
            values,
            page_size=1000,
        )
    if commit:
        conn.commit()
    return len(values)


def upsert_checkpoint(
    conn,
    *,
    table: str,
    job: CmcBackfillJob,
    last_ts_ms: int,
    commit: bool = True,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO {table} AS checkpoint (
                symbol, exchange, timeframe, last_ts_ms, updated_at
            ) VALUES (%s, %s, '3m', %s, now())
            ON CONFLICT (symbol, exchange, timeframe) DO UPDATE SET
                last_ts_ms = GREATEST(
                    COALESCE(checkpoint.last_ts_ms, EXCLUDED.last_ts_ms),
                    EXCLUDED.last_ts_ms
                ),
                updated_at = now()
            """,
            (job.symbol, job.exchange, last_ts_ms),
        )
    if commit:
        conn.commit()


def update_target_backfill_status(
    conn,
    *,
    table: str,
    job: CmcBackfillJob,
    status: str,
    actual_from: Optional[datetime] = None,
    actual_to: Optional[datetime] = None,
    error: Optional[Any] = None,
    increment_attempt: bool = False,
    commit: bool = True,
) -> None:
    if status not in {"pending", "running", "complete", "failed", "unavailable"}:
        raise ValueError(f"Unsupported backfill status: {status}")
    error_text = str(error).strip()[:4000] if error is not None else None
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {table}
            SET backfill_status = %s,
                attempt_count = attempt_count + CASE WHEN %s THEN 1 ELSE 0 END,
                actual_first_candle_at = CASE
                    WHEN %s IS NULL THEN actual_first_candle_at
                    WHEN actual_first_candle_at IS NULL THEN %s
                    ELSE LEAST(actual_first_candle_at, %s)
                END,
                actual_last_candle_at = CASE
                    WHEN %s IS NULL THEN actual_last_candle_at
                    WHEN actual_last_candle_at IS NULL THEN %s
                    ELSE GREATEST(actual_last_candle_at, %s)
                END,
                last_verified_at = now(),
                last_error = %s,
                updated_at = now()
            WHERE symbol = %s
              AND selected_exchange = %s
            """,
            (
                status,
                increment_attempt,
                actual_from,
                actual_from,
                actual_from,
                actual_to,
                actual_to,
                actual_to,
                error_text,
                job.symbol,
                job.exchange,
            ),
        )
    if commit:
        conn.commit()
