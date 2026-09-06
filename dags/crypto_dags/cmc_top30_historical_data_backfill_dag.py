from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional


DAGS_ROOT = str(Path(__file__).resolve().parent.parent)
if DAGS_ROOT not in sys.path:
    sys.path.insert(0, DAGS_ROOT)

import ccxt.async_support as ccxt
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.cmc_top30_backfill import (
    CmcBackfillJob,
    build_jobs,
    deserialize_job,
    estimated_candle_count,
    load_backfill_rows,
    normalize_backfill_conf,
    normalize_ohlcv_page,
    serialize_job,
    update_target_backfill_status,
    upsert_checkpoint,
    upsert_ohlcv_page,
)
from plugins.utils.config_loader import load_yaml_config
from plugins.utils.ohlcv_aggregate import aggregate_ohlcv_window


ROOT_CONFIG = load_yaml_config("crypto_configs/cmc_top30_universe.yml")
CONFIG = ROOT_CONFIG["backfill_cmc_top30_historical_data_dag"]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
AGGREGATE_CFG = CONFIG.get("aggregate", [])
TIMEFRAME = str(API_CFG.get("timeframe", "3m"))
TIMEFRAME_MS = 3 * 60 * 1000
BATCH_LIMIT = int(API_CFG.get("limit", 1000))
SLEEP_FLOOR = float(API_CFG.get("rate_limit_floor", 0.2))
POOL_NAME = str(API_CFG.get("pool_name", "ccxt_ohlcv_pool"))
TASK_CONCURRENCY = int(API_CFG.get("task_concurrency", 3))
QUOTE = "USDT"

if TIMEFRAME != "3m":
    raise ValueError("CMC Top 30 Phase 1 backfill only supports the 3m timeframe")


class OhlcvUnavailableError(RuntimeError):
    """The selected exchange has no usable 3m candles for this symbol."""


def _last_closed_boundary_ms(now: datetime) -> int:
    now_ms = int(now.astimezone(timezone.utc).timestamp() * 1000)
    return (now_ms // TIMEFRAME_MS) * TIMEFRAME_MS


def _min_datetime(*values: Optional[datetime]) -> Optional[datetime]:
    present = [value for value in values if value is not None]
    return min(present) if present else None


def _max_datetime(*values: Optional[datetime]) -> Optional[datetime]:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _load_existing_bounds(
    conn,
    *,
    job: CmcBackfillJob,
) -> tuple[Optional[datetime], Optional[datetime]]:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                to_timestamp(MIN(timestamp) / 1000.0),
                to_timestamp(MAX(timestamp) / 1000.0)
            FROM {DB_CFG['ohlcv_table']}
            WHERE symbol = %s
              AND exchange = %s
              AND timestamp >= %s
            """,
            (
                job.symbol,
                job.exchange,
                int(job.data_start_at.timestamp() * 1000),
            ),
        )
        row = cursor.fetchone()
    if not row:
        return None, None
    return row[0], row[1]


async def _fetch_and_store_ohlcv(conn, job: CmcBackfillJob) -> Dict[str, Any]:
    runtime_first_at, runtime_last_at = _load_existing_bounds(conn, job=job)
    requested_start_ms = int(job.requested_from.timestamp() * 1000)
    if runtime_last_at is not None:
        requested_start_ms = max(
            requested_start_ms,
            int(runtime_last_at.timestamp() * 1000) + TIMEFRAME_MS,
        )
    requested_end_ms = int(job.requested_to.timestamp() * 1000)
    closed_boundary_ms = _last_closed_boundary_ms(datetime.now(timezone.utc))
    end_ms = min(requested_end_ms, closed_boundary_ms)
    if end_ms <= requested_start_ms:
        return {
            "status": "complete",
            "row_count": 0,
            "actual_from": _min_datetime(job.existing_first_at, runtime_first_at),
            "actual_to": _max_datetime(job.existing_last_at, runtime_last_at),
            "new_from": None,
            "new_to": None,
        }

    exchange_class = getattr(ccxt, job.exchange, None)
    if exchange_class is None:
        raise RuntimeError(f"Exchange {job.exchange} is not available in CCXT")

    ccxt_pair = f"{job.symbol}/{QUOTE}"
    exchange = exchange_class({"enableRateLimit": True})
    total_rows = 0
    new_first_at: Optional[datetime] = None
    new_last_at: Optional[datetime] = None
    since_ms = requested_start_ms
    try:
        markets = await exchange.load_markets()
        market = markets.get(ccxt_pair)
        if market is None or market.get("spot") is not True:
            raise OhlcvUnavailableError(
                f"{job.exchange} does not expose exact spot market {ccxt_pair}"
            )

        while since_ms < end_ms:
            page = await exchange.fetch_ohlcv(
                ccxt_pair,
                timeframe=TIMEFRAME,
                since=since_ms,
                limit=BATCH_LIMIT,
            )
            if not page:
                break

            raw_timestamps = [int(row[0]) for row in page if row]
            if not raw_timestamps:
                raise RuntimeError("CCXT returned an OHLCV page without timestamps")
            new_since_ms = max(raw_timestamps) + TIMEFRAME_MS
            if new_since_ms <= since_ms:
                raise RuntimeError("CCXT pagination did not advance")

            normalized = normalize_ohlcv_page(
                page,
                job=job,
                timeframe_ms=TIMEFRAME_MS,
                closed_before_ms=closed_boundary_ms,
            )
            if normalized:
                page_first_at = normalized[0]["datetime"]
                page_last_at = normalized[-1]["datetime"]
                new_first_at = _min_datetime(new_first_at, page_first_at)
                new_last_at = _max_datetime(new_last_at, page_last_at)
                total_rows += upsert_ohlcv_page(
                    conn,
                    table=DB_CFG["ohlcv_table"],
                    rows=normalized,
                    commit=False,
                )
                upsert_checkpoint(
                    conn,
                    table=DB_CFG["checkpoint_table"],
                    job=job,
                    last_ts_ms=int(normalized[-1]["timestamp"]),
                    commit=False,
                )
                update_target_backfill_status(
                    conn,
                    table=DB_CFG["symbol_target_table"],
                    job=job,
                    status="running",
                    actual_from=page_first_at,
                    actual_to=page_last_at,
                    commit=False,
                )
                # One transaction per API page keeps the checkpoint aligned with data.
                conn.commit()

            if new_since_ms >= end_ms or len(page) < BATCH_LIMIT:
                break
            since_ms = new_since_ms
            await asyncio.sleep(max(exchange.rateLimit / 1000, SLEEP_FLOOR))
    finally:
        await exchange.close()

    actual_from = _min_datetime(job.existing_first_at, runtime_first_at, new_first_at)
    actual_to = _max_datetime(job.existing_last_at, runtime_last_at, new_last_at)
    if total_rows == 0 and actual_from is None:
        raise OhlcvUnavailableError(
            f"{job.exchange} returned no closed candles for {ccxt_pair}"
        )

    # A prior task attempt may already have committed one or more 3m pages before
    # a later API or aggregate error. Include that committed prefix in the
    # affected range so an Airflow retry rebuilds every aggregate idempotently.
    previously_committed_from = None
    previously_committed_to = None
    if runtime_last_at is not None and runtime_last_at >= job.requested_from:
        previously_committed_from = job.requested_from
        previously_committed_to = min(runtime_last_at, job.requested_to)
    return {
        "status": "complete",
        "row_count": total_rows,
        "actual_from": actual_from,
        "actual_to": actual_to,
        "new_from": _min_datetime(previously_committed_from, new_first_at),
        "new_to": _max_datetime(previously_committed_to, new_last_at),
    }


def _rebuild_aggregates(
    conn,
    *,
    job: CmcBackfillJob,
    new_from: Optional[datetime],
    new_to: Optional[datetime],
) -> Dict[str, int]:
    if new_from is None or new_to is None:
        return {}
    first_ts_ms = int(new_from.timestamp() * 1000)
    last_ts_ms = int(new_to.timestamp() * 1000)
    counts: Dict[str, int] = {}
    for target in AGGREGATE_CFG:
        bucket_ms = int(target["bucket_minutes"]) * 60 * 1000
        aggregate_start_ms = (first_ts_ms // bucket_ms) * bucket_ms
        aggregate_end_ms = ((last_ts_ms // bucket_ms) + 1) * bucket_ms
        counts[str(target["timeframe"])] = aggregate_ohlcv_window(
            conn,
            source_table=DB_CFG["ohlcv_table"],
            target_table=str(target["target_table"]),
            bucket_ms=bucket_ms,
            start_ts_ms=aggregate_start_ms,
            end_ts_ms=aggregate_end_ms,
            symbols=[job.symbol],
            exchanges=[job.exchange],
        )
    return counts


with DAG(
    dag_id="backfill_cmc_top30_historical_data_dag",
    description="Backfill CMC Top 30 3m OHLCV by canonical symbol and exchange",
    default_args={
        "owner": "crypto-data",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(
            hours=int(CONFIG.get("execution_timeout_hours", 24))
        ),
    },
    schedule_interval=CONFIG.get("schedule", CONFIG.get("repair_schedule")),
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_tasks=TASK_CONCURRENCY,
    max_active_runs=1,
    tags=["crypto", "cmc", "top30", "point-in-time", "backfill", "ccxt"],
) as dag:

    @task
    def prepare_jobs() -> List[Dict[str, Any]]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = normalize_backfill_conf(
            dict(dag_run.conf or {}) if dag_run and dag_run.conf else {}
        )
        now = datetime.now(timezone.utc)
        closed_boundary = datetime.fromtimestamp(
            _last_closed_boundary_ms(now) / 1000,
            tz=timezone.utc,
        )
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            rows = load_backfill_rows(
                conn,
                symbol_target_table=DB_CFG["symbol_target_table"],
                ohlcv_table=DB_CFG["ohlcv_table"],
                conf=conf,
            )
            jobs = build_jobs(
                rows,
                now=closed_boundary,
                from_date=conf["from_date"],
                to_date=conf["to_date"],
                timeframe_ms=TIMEFRAME_MS,
            )
            job_keys = {(job.symbol, job.exchange) for job in jobs}
            for row in rows:
                key = (str(row["symbol"]).upper(), str(row["selected_exchange"]))
                existing_last_at = row.get("existing_last_at")
                if (
                    key in job_keys
                    or existing_last_at is None
                    or row.get("backfill_status") == "failed"
                ):
                    continue
                current_job = CmcBackfillJob(
                    symbol=key[0],
                    exchange=key[1],
                    data_start_at=row["data_start_at"],
                    requested_from=closed_boundary,
                    requested_to=closed_boundary,
                    existing_first_at=row.get("existing_first_at"),
                    existing_last_at=existing_last_at,
                )
                update_target_backfill_status(
                    conn,
                    table=DB_CFG["symbol_target_table"],
                    job=current_job,
                    status="complete",
                    actual_from=current_job.existing_first_at,
                    actual_to=current_job.existing_last_at,
                )
        finally:
            conn.close()

        estimate = estimated_candle_count(jobs, timeframe_ms=TIMEFRAME_MS)
        auto_limit = int(API_CFG.get("auto_confirm_candle_limit", 2_000_000))
        if estimate > auto_limit and not conf["confirm_large_backfill"]:
            raise ValueError(
                "Large CMC Top 30 backfill requires confirm_large_backfill=true: "
                f"estimated_candles={estimate} limit={auto_limit}"
            )
        logging.info("Prepared %s jobs; estimated 3m candles=%s", len(jobs), estimate)
        return [serialize_job(job) for job in jobs]

    @task(pool=POOL_NAME)
    def run_job(payload: Mapping[str, Any]) -> Dict[str, Any]:
        job = deserialize_job(payload)
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            update_target_backfill_status(
                conn,
                table=DB_CFG["symbol_target_table"],
                job=job,
                status="running",
                increment_attempt=True,
            )
            try:
                result = asyncio.run(_fetch_and_store_ohlcv(conn, job))
                result["aggregates"] = _rebuild_aggregates(
                    conn,
                    job=job,
                    new_from=result["new_from"],
                    new_to=result["new_to"],
                )
                update_target_backfill_status(
                    conn,
                    table=DB_CFG["symbol_target_table"],
                    job=job,
                    status="complete",
                    actual_from=result["actual_from"],
                    actual_to=result["actual_to"],
                )
                return {"symbol": job.symbol, "exchange": job.exchange, **result}
            except OhlcvUnavailableError as exc:
                conn.rollback()
                update_target_backfill_status(
                    conn,
                    table=DB_CFG["symbol_target_table"],
                    job=job,
                    status="unavailable",
                    actual_from=job.existing_first_at,
                    actual_to=job.existing_last_at,
                    error=exc,
                )
                return {
                    "symbol": job.symbol,
                    "exchange": job.exchange,
                    "status": "unavailable",
                    "row_count": 0,
                }
            except Exception as exc:
                conn.rollback()
                update_target_backfill_status(
                    conn,
                    table=DB_CFG["symbol_target_table"],
                    job=job,
                    status="failed",
                    error=exc,
                )
                raise
        finally:
            conn.close()

    @task(trigger_rule="all_done")
    def report_coverage() -> Dict[str, int]:
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT coverage_status, COUNT(*)
                    FROM raw_crypto_data.cmc_top30_data_coverage
                    GROUP BY coverage_status
                    ORDER BY coverage_status
                    """
                )
                report = {str(status): int(count) for status, count in cursor.fetchall()}
                logging.info("CMC Top 30 data coverage: %s", report)
                return report
        finally:
            conn.close()

    mapped_results = run_job.expand(payload=prepare_jobs())
    mapped_results >> report_coverage()
