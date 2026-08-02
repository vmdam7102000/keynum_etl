from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

from plugins.utils.api_utils import request_json
from plugins.utils.db_utils import insert_dynamic_records
from plugins.utils.eod_price_sync import _parse_eodhd_records
from plugins.utils.sp500_fundamentals import sync_fundamentals_one

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PGConnection


@dataclass(frozen=True)
class BackfillJob:
    membership_ids: Tuple[int, ...]
    source_ticker: str
    provider_ticker: str
    company_id: int
    member_from: date
    member_to: date
    is_open: bool
    price_statuses: Tuple[str, ...]
    fundamentals_statuses: Tuple[str, ...]


def _parse_optional_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError("Backfill dates must use YYYY-MM-DD") from exc


def normalize_backfill_conf(run_conf: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    provided = dict(run_conf or {})
    mode = provided.get("mode", "missing_only")
    if mode not in {"missing_only", "force"}:
        raise ValueError("Backfill mode must be missing_only or force")
    tickers = provided.get("tickers")
    if tickers is not None and not isinstance(tickers, list):
        raise ValueError("Backfill tickers must be a JSON list")
    membership_ids = provided.get("membership_ids")
    if membership_ids is not None and not isinstance(membership_ids, list):
        raise ValueError("Backfill membership_ids must be a JSON list")
    batch_size = int(provided.get("batch_size", 50))
    if batch_size < 1:
        raise ValueError("Backfill batch_size must be positive")
    return {
        "mode": mode,
        "tickers": [str(item).upper() for item in tickers] if tickers else None,
        "membership_ids": [int(item) for item in membership_ids] if membership_ids else None,
        "from_date": _parse_optional_date(provided.get("from_date")),
        "to_date": _parse_optional_date(provided.get("to_date")),
        "include_prices": bool(provided.get("include_prices", True)),
        "include_fundamentals": bool(provided.get("include_fundamentals", True)),
        "batch_size": batch_size,
    }


def load_backfill_rows(
    conn: PGConnection,
    *,
    membership_table: str,
    mapping_table: str,
    conf: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    conditions = ["mapping.provider = 'EODHD'", "mapping.mapping_status = 'resolved'"]
    parameters: List[Any] = []
    if conf["membership_ids"]:
        conditions.append("membership.id = ANY(%s)")
        parameters.append(conf["membership_ids"])
    if conf["tickers"]:
        conditions.append("membership.source_ticker = ANY(%s)")
        parameters.append(conf["tickers"])
    if conf["mode"] == "missing_only":
        status_conditions = []
        if conf["include_prices"]:
            status_conditions.append(
                "mapping.price_backfill_status IN ('pending', 'running', 'failed')"
            )
        if conf["include_fundamentals"]:
            status_conditions.append(
                "mapping.fundamentals_backfill_status IN ('pending', 'running', 'failed')"
            )
        if status_conditions:
            conditions.append(f"({' OR '.join(status_conditions)})")

    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            SELECT
                membership.id,
                membership.source_ticker,
                membership.valid_from,
                membership.valid_to,
                mapping.provider_ticker,
                mapping.company_id,
                mapping.price_backfill_status,
                mapping.fundamentals_backfill_status
            FROM {membership_table} AS membership
            JOIN {mapping_table} AS mapping
              ON mapping.membership_id = membership.id
            WHERE {" AND ".join(conditions)}
            ORDER BY membership.source_ticker, membership.valid_from
            """,
            parameters,
        )
        return [
            {
                "membership_id": row[0],
                "source_ticker": row[1],
                "valid_from": row[2],
                "valid_to": row[3],
                "provider_ticker": row[4],
                "company_id": row[5],
                "price_status": row[6],
                "fundamentals_status": row[7],
            }
            for row in cursor.fetchall()
        ]
    finally:
        cursor.close()


def build_backfill_jobs(
    rows: Sequence[Mapping[str, Any]],
    *,
    logical_date: date,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
) -> List[BackfillJob]:
    grouped: Dict[Tuple[int, str, str], List[Mapping[str, Any]]] = {}
    for row in rows:
        key = (
            int(row["company_id"]),
            str(row["source_ticker"]),
            str(row["provider_ticker"]),
        )
        grouped.setdefault(key, []).append(row)

    jobs: List[BackfillJob] = []
    for (company_id, source_ticker, provider_ticker), group in grouped.items():
        starts = [row["valid_from"] for row in group]
        exclusive_ends = [row["valid_to"] for row in group if row["valid_to"] is not None]
        member_from = min(starts)
        member_to = (
            max(end - timedelta(days=1) for end in exclusive_ends)
            if len(exclusive_ends) == len(group)
            else logical_date
        )
        if from_date:
            member_from = max(member_from, from_date)
        if to_date:
            member_to = min(member_to, to_date)
        if member_from > member_to:
            continue
        jobs.append(
            BackfillJob(
                membership_ids=tuple(int(row["membership_id"]) for row in group),
                source_ticker=source_ticker,
                provider_ticker=provider_ticker,
                company_id=company_id,
                member_from=member_from,
                member_to=member_to,
                is_open=any(row["valid_to"] is None for row in group),
                price_statuses=tuple(str(row["price_status"]) for row in group),
                fundamentals_statuses=tuple(
                    str(row["fundamentals_status"]) for row in group
                ),
            )
        )
    return sorted(jobs, key=lambda job: (job.source_ticker, job.member_from))


def chunk_jobs(jobs: Sequence[BackfillJob], batch_size: int) -> List[List[BackfillJob]]:
    return [list(jobs[index : index + batch_size]) for index in range(0, len(jobs), batch_size)]


def serialize_job(job: BackfillJob) -> Dict[str, Any]:
    return {
        "membership_ids": list(job.membership_ids),
        "source_ticker": job.source_ticker,
        "provider_ticker": job.provider_ticker,
        "company_id": job.company_id,
        "member_from": job.member_from.isoformat(),
        "member_to": job.member_to.isoformat(),
        "is_open": job.is_open,
        "price_statuses": list(job.price_statuses),
        "fundamentals_statuses": list(job.fundamentals_statuses),
    }


def deserialize_job(payload: Mapping[str, Any]) -> BackfillJob:
    return BackfillJob(
        membership_ids=tuple(int(item) for item in payload["membership_ids"]),
        source_ticker=str(payload["source_ticker"]),
        provider_ticker=str(payload["provider_ticker"]),
        company_id=int(payload["company_id"]),
        member_from=date.fromisoformat(str(payload["member_from"])),
        member_to=date.fromisoformat(str(payload["member_to"])),
        # Backward compatible with payloads prepared before this field existed.
        is_open=bool(payload.get("is_open", False)),
        price_statuses=tuple(str(item) for item in payload["price_statuses"]),
        fundamentals_statuses=tuple(
            str(item) for item in payload["fundamentals_statuses"]
        ),
    )


def _record_date(record: Mapping[str, Any]) -> Optional[date]:
    value = record.get("date")
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _fetch_price_window(
    provider_ticker: str,
    *,
    from_date: date,
    to_date: date,
    api_cfg: Mapping[str, Any],
    api_key: str,
    retry_empty: bool = False,
) -> List[Dict[str, Any]]:
    params = {
        "fmt": api_cfg.get("fmt", "json"),
        api_cfg.get("from_param", "from"): from_date.isoformat(),
        api_cfg.get("to_param", "to"): to_date.isoformat(),
    }
    if api_key:
        params["api_token"] = api_key

    empty_attempts = (
        max(int(api_cfg.get("empty_response_attempts", 3)), 1)
        if retry_empty
        else 1
    )
    empty_backoff = float(api_cfg.get("empty_response_backoff_seconds", 2))
    for attempt in range(1, empty_attempts + 1):
        payload = request_json(
            api_cfg["url"].format(ticker=provider_ticker),
            params=params,
            timeout=api_cfg.get("timeout", 30),
            retries=api_cfg.get("retries", 3),
            backoff=api_cfg.get("backoff", 1.5),
        )
        if payload is None:
            raise RuntimeError(f"EODHD price request failed for {provider_ticker}")
        if isinstance(payload, dict):
            error = payload.get("error")
            if error:
                message = error.get("message") if isinstance(error, dict) else error
                raise RuntimeError(
                    f"EODHD price error for {provider_ticker}: {message}"
                )
            if payload.get("message") and payload.get("code"):
                raise RuntimeError(
                    f"EODHD price error for {provider_ticker}: "
                    f"{payload.get('message')}"
                )

        records = _parse_eodhd_records(payload, provider_ticker)
        if records:
            logging.info(
                "Fetched %s EOD records for %s from %s to %s",
                len(records),
                provider_ticker,
                from_date,
                to_date,
            )
            return records

        logging.warning(
            "EODHD returned no EOD records for %s from %s to %s "
            "(empty attempt %s/%s)",
            provider_ticker,
            from_date,
            to_date,
            attempt,
            empty_attempts,
        )
        if attempt < empty_attempts:
            time.sleep(empty_backoff * attempt)
    return []


def fetch_prices_with_warmup(
    job: BackfillJob,
    *,
    api_cfg: Mapping[str, Any],
    api_key: str,
    warmup_bars: int = 252,
    initial_calendar_days: int = 400,
    extension_calendar_days: int = 200,
    max_extensions: int = 10,
) -> Tuple[List[Dict[str, Any]], int]:
    request_start = job.member_from - timedelta(days=initial_calendar_days)
    records_by_date: Dict[date, Dict[str, Any]] = {}

    def add_records(records: Sequence[Mapping[str, Any]]) -> None:
        for record in records:
            record_date = _record_date(record)
            if record_date is not None and record_date <= job.member_to:
                records_by_date[record_date] = dict(record)

    add_records(
        _fetch_price_window(
            job.provider_ticker,
            from_date=request_start,
            to_date=job.member_to,
            api_cfg=api_cfg,
            api_key=api_key,
            retry_empty=True,
        )
    )
    extensions = 0
    while (
        len([item for item in records_by_date if item < job.member_from]) < warmup_bars
        and extensions < max_extensions
    ):
        extension_end = request_start - timedelta(days=1)
        extension_start = request_start - timedelta(days=extension_calendar_days)
        extension_records = _fetch_price_window(
            job.provider_ticker,
            from_date=extension_start,
            to_date=extension_end,
            api_cfg=api_cfg,
            api_key=api_key,
        )
        if not extension_records:
            break
        add_records(extension_records)
        request_start = extension_start
        extensions += 1

    pre_dates = sorted(item for item in records_by_date if item < job.member_from)
    kept_dates = pre_dates[-warmup_bars:] + sorted(
        item for item in records_by_date if job.member_from <= item <= job.member_to
    )
    return [records_by_date[item] for item in kept_dates], min(len(pre_dates), warmup_bars)


def _update_mapping_status(
    conn: PGConnection,
    *,
    mapping_table: str,
    membership_ids: Sequence[int],
    status_column: str,
    status: str,
    error: Optional[str] = None,
) -> None:
    allowed_columns = {"price_backfill_status", "fundamentals_backfill_status"}
    if status_column not in allowed_columns:
        raise ValueError(f"Unsupported mapping status column: {status_column}")
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            UPDATE {mapping_table}
            SET {status_column} = %s,
                last_error = %s,
                updated_at = now()
            WHERE membership_id = ANY(%s) AND provider = 'EODHD'
            """,
            (status, error, list(membership_ids)),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def _store_price_records(
    conn: PGConnection,
    *,
    records: Sequence[Mapping[str, Any]],
    job: BackfillJob,
    db_cfg: Mapping[str, Any],
) -> None:
    enriched = []
    for record in records:
        row = dict(record)
        row["company_id"] = job.company_id
        row["ticker"] = job.source_ticker
        enriched.append(row)
    insert_dynamic_records(
        postgres_conn_id=db_cfg["postgres_conn_id"],
        table=db_cfg["price_table"],
        records=enriched,
        columns_map=db_cfg["price_columns"],
        conflict_keys=db_cfg["price_conflict_keys"],
        on_conflict_do_update=True,
        update_columns=("open", "high", "low", "close", "adjusted_close", "volume"),
        conn=conn,
    )


def run_backfill_batch(
    conn: PGConnection,
    *,
    jobs: Sequence[BackfillJob],
    logical_date: datetime,
    include_prices: bool,
    include_fundamentals: bool,
    force: bool,
    price_api_cfg: Mapping[str, Any],
    fundamentals_api_cfg: Mapping[str, Any],
    db_cfg: Mapping[str, Any],
    api_key: str,
    backfill_cfg: Mapping[str, Any],
) -> Dict[str, int]:
    counts = {
        "price_complete": 0,
        "price_partial": 0,
        "price_failed": 0,
        "fundamentals_complete": 0,
        "fundamentals_failed": 0,
    }
    fundamentals_cache: Dict[int, str] = {}
    for job in jobs:
        if include_prices and (force or any(status != "complete" for status in job.price_statuses)):
            _update_mapping_status(
                conn,
                mapping_table=db_cfg["mapping_table"],
                membership_ids=job.membership_ids,
                status_column="price_backfill_status",
                status="running",
            )
            try:
                records, warmup_count = fetch_prices_with_warmup(
                    job,
                    api_cfg=price_api_cfg,
                    api_key=api_key,
                    warmup_bars=int(backfill_cfg.get("warmup_bars", 252)),
                    initial_calendar_days=int(
                        backfill_cfg.get("initial_calendar_days", 400)
                    ),
                    extension_calendar_days=int(
                        backfill_cfg.get("extension_calendar_days", 200)
                    ),
                    max_extensions=int(backfill_cfg.get("max_extensions", 10)),
                )
                if not records:
                    if job.is_open:
                        status = "failed"
                        error = (
                            f"EODHD returned no price records for active ticker "
                            f"{job.provider_ticker} from "
                            f"{job.member_from - timedelta(days=int(backfill_cfg.get('initial_calendar_days', 400)))} "
                            f"to {job.member_to}"
                        )
                        counts["price_failed"] += 1
                    else:
                        status = "unavailable"
                        error = None
                else:
                    _store_price_records(conn, records=records, job=job, db_cfg=db_cfg)
                    status = (
                        "complete"
                        if warmup_count >= int(backfill_cfg.get("warmup_bars", 252))
                        else "partial"
                    )
                    error = None
                    counts[f"price_{status}"] += 1
                _update_mapping_status(
                    conn,
                    mapping_table=db_cfg["mapping_table"],
                    membership_ids=job.membership_ids,
                    status_column="price_backfill_status",
                    status=status,
                    error=error,
                )
            except Exception as exc:
                logging.exception("Price backfill failed for %s", job.source_ticker)
                counts["price_failed"] += 1
                _update_mapping_status(
                    conn,
                    mapping_table=db_cfg["mapping_table"],
                    membership_ids=job.membership_ids,
                    status_column="price_backfill_status",
                    status="failed",
                    error=str(exc)[:2000],
                )

        if include_fundamentals and (
            force or any(status != "complete" for status in job.fundamentals_statuses)
        ):
            if job.company_id not in fundamentals_cache:
                _update_mapping_status(
                    conn,
                    mapping_table=db_cfg["mapping_table"],
                    membership_ids=job.membership_ids,
                    status_column="fundamentals_backfill_status",
                    status="running",
                )
                try:
                    fundamentals_cache[job.company_id] = sync_fundamentals_one(
                        provider_ticker=job.provider_ticker,
                        company_id=job.company_id,
                        logical_date=logical_date,
                        api_cfg=fundamentals_api_cfg,
                        db_cfg=db_cfg,
                        api_key=api_key,
                        conn=conn,
                    )
                except Exception as exc:
                    logging.exception(
                        "Fundamentals backfill failed for %s", job.source_ticker
                    )
                    fundamentals_cache[job.company_id] = "failed"
                    counts["fundamentals_failed"] += 1
                    error = str(exc)[:2000]
                else:
                    error = None
            else:
                error = None
            status = fundamentals_cache[job.company_id]
            if status == "complete":
                counts["fundamentals_complete"] += 1
            _update_mapping_status(
                conn,
                mapping_table=db_cfg["mapping_table"],
                membership_ids=job.membership_ids,
                status_column="fundamentals_backfill_status",
                status=status,
                error=error,
            )
        time.sleep(backfill_cfg.get("job_throttle_seconds", 0))
    if counts["price_failed"] or counts["fundamentals_failed"]:
        raise RuntimeError(f"S&P 500 backfill batch completed with failures: {counts}")
    return counts
