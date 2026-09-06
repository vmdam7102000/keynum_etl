# dags/crypto/sync_crypto_ohlcv_3m_dag_dag.py
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import ccxt.async_support as ccxt
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from plugins.utils.config_loader import load_yaml_config


CONFIG = load_yaml_config("crypto_configs/ccxt_ohlcv_3m.yml")["ccxt_ohlcv_3m"]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]

TIMEFRAME: str = API_CFG.get("timeframe", "3m")
BATCH_LIMIT: int = int(API_CFG.get("limit", 1000))
TIMEFRAME_MS = 3 * 60 * 1000
SLEEP_FLOOR: float = float(API_CFG.get("rate_limit_floor", 0.2))
QUOTE = str(DB_CFG.get("symbol_quote", "USDT")).upper()
POOL_NAME: str = API_CFG.get("pool_name", "ccxt_ohlcv_pool")
PAIR_TASK_CONCURRENCY: int = int(API_CFG.get("task_concurrency", 3))
SYMBOL_TARGET_TABLE = str(
    DB_CFG.get(
        "cmc_top30_symbol_target_table",
        "raw_crypto_data.cmc_top30_symbol_targets",
    )
)
EXCLUDE_SYMBOLS = {
    str(symbol).strip().upper()
    for symbol in API_CFG.get("exclude_symbols", [])
    if str(symbol).strip()
}

if TIMEFRAME != "3m":
    raise ValueError("The incremental OHLCV DAG only supports the 3m timeframe")
if QUOTE != "USDT":
    raise ValueError("CMC Top 30 Phase 1 requires SYMBOL/USDT markets")

logger = logging.getLogger("ccxt_ohlcv_3m_dag")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _since_from_checkpoint(last_ts_ms: Optional[int]) -> Optional[int]:
    if last_ts_ms is not None:
        return int(last_ts_ms) + TIMEFRAME_MS
    try:
        since_days = int(API_CFG.get("since_days") or 0)
    except (TypeError, ValueError):
        since_days = 0
    if since_days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
        return int(cutoff.timestamp() * 1000)
    return None


def load_pairs(conn) -> List[Tuple[str, str]]:
    legacy_sql = f"""
    SELECT symbol, available_exchange
    FROM {DB_CFG['metadata_table']}
    WHERE available_exchange IS NOT NULL AND available_exchange <> ''
    """
    target_sql = f"""
    SELECT symbol, selected_exchange
    FROM {SYMBOL_TARGET_TABLE}
    WHERE is_canonical IS TRUE
      AND mapping_status = 'resolved'
      AND backfill_status = 'complete'
      AND is_stablecoin IS FALSE
      AND is_wrapped IS FALSE
      AND selected_exchange IS NOT NULL
    """
    with conn.cursor() as cursor:
        cursor.execute(legacy_sql)
        legacy_rows = cursor.fetchall()
        cursor.execute(target_sql)
        target_rows = cursor.fetchall()

    pairs = set()
    excluded_hits = 0
    for symbol, exchange_csv in legacy_rows:
        normalized_symbol = str(symbol).strip().upper()
        if normalized_symbol in EXCLUDE_SYMBOLS:
            excluded_hits += 1
            continue
        for exchange in str(exchange_csv).split(","):
            normalized_exchange = exchange.strip().lower()
            if normalized_exchange:
                pairs.add((normalized_symbol, normalized_exchange))

    for symbol, exchange in target_rows:
        normalized_symbol = str(symbol).strip().upper()
        if normalized_symbol in EXCLUDE_SYMBOLS:
            excluded_hits += 1
            continue
        normalized_exchange = str(exchange).strip().lower()
        if normalized_exchange:
            pairs.add((normalized_symbol, normalized_exchange))

    if excluded_hits:
        logger.info(
            "Skipped %s pairs due to exclude_symbols=%s",
            excluded_hits,
            sorted(EXCLUDE_SYMBOLS),
        )
    return sorted(pairs)


def load_checkpoint(conn, symbol: str, exchange_id: str) -> Optional[int]:
    """Prefer the real OHLCV tail so existing history is never backdated."""
    sql = f"""
    SELECT COALESCE(
        (
            SELECT MAX(price.timestamp)
            FROM {DB_CFG['target_table']} AS price
            WHERE price.symbol = %s AND price.exchange = %s
        ),
        (
            SELECT checkpoint.last_ts_ms
            FROM {DB_CFG['checkpoint_table']} AS checkpoint
            WHERE checkpoint.symbol = %s
              AND checkpoint.exchange = %s
              AND checkpoint.timeframe = %s
        )
    )
    """
    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (symbol, exchange_id, symbol, exchange_id, TIMEFRAME),
        )
        row = cursor.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def upsert_checkpoint(
    conn,
    symbol: str,
    exchange_id: str,
    last_ts_ms: int,
) -> None:
    sql = f"""
    INSERT INTO {DB_CFG['checkpoint_table']} AS checkpoint (
        symbol, exchange, timeframe, last_ts_ms, updated_at
    ) VALUES (%s, %s, %s, %s, now())
    ON CONFLICT (symbol, exchange, timeframe) DO UPDATE SET
        last_ts_ms = GREATEST(
            COALESCE(checkpoint.last_ts_ms, EXCLUDED.last_ts_ms),
            EXCLUDED.last_ts_ms
        ),
        updated_at = now()
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (symbol, exchange_id, TIMEFRAME, last_ts_ms))
    conn.commit()


def upsert_ohlcv(conn, records: List[Dict[str, Any]]) -> int:
    if not records:
        return 0

    insert_sql = f"""
    INSERT INTO {DB_CFG['target_table']} (
        symbol, exchange, timestamp, open, high, low, close, volume, datetime
    ) VALUES %s
    ON CONFLICT (symbol, exchange, timestamp) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        datetime = EXCLUDED.datetime
    """
    values = [
        (
            record["symbol"],
            record["exchange"],
            record["timestamp"],
            record["open"],
            record["high"],
            record["low"],
            record["close"],
            record["volume"],
            record["datetime"],
        )
        for record in records
    ]
    with conn.cursor() as cursor:
        execute_values(cursor, insert_sql, values, page_size=1000)
    conn.commit()
    return len(records)


async def _fetch_ohlcv(
    exchange: ccxt.Exchange,
    ccxt_pair: str,
    since_ms: Optional[int],
) -> List[List[Any]]:
    all_rows: List[List[Any]] = []
    while True:
        rows = await exchange.fetch_ohlcv(
            ccxt_pair,
            timeframe=TIMEFRAME,
            since=since_ms,
            limit=BATCH_LIMIT,
        )
        if not rows:
            break
        all_rows.extend(rows)
        raw_timestamps = [int(row[0]) for row in rows if row]
        if not raw_timestamps:
            raise RuntimeError("CCXT returned an OHLCV page without timestamps")
        next_since_ms = max(raw_timestamps) + TIMEFRAME_MS
        if since_ms is not None and next_since_ms <= since_ms:
            raise RuntimeError("CCXT pagination did not advance")
        since_ms = next_since_ms
        if len(rows) < BATCH_LIMIT:
            break
        await asyncio.sleep(max(exchange.rateLimit / 1000, SLEEP_FLOOR))
    return all_rows


async def fetch_for_pair(
    exchange_id: str,
    symbol: str,
    since_ms: Optional[int],
) -> List[Dict[str, Any]]:
    exchange_class = getattr(ccxt, exchange_id, None)
    if not exchange_class:
        raise ValueError(f"Exchange {exchange_id} not found in ccxt")
    ccxt_pair = f"{symbol}/{QUOTE}"
    exchange = exchange_class({"enableRateLimit": True})
    try:
        markets = await exchange.load_markets()
        market = markets.get(ccxt_pair)
        if market is None or market.get("spot") is not True:
            raise ValueError(f"Exact spot market {ccxt_pair} not found on {exchange_id}")
        rows = await _fetch_ohlcv(exchange, ccxt_pair, since_ms)
    finally:
        await exchange.close()

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    closed_boundary_ms = (now_ms // TIMEFRAME_MS) * TIMEFRAME_MS
    records_by_timestamp: Dict[int, Dict[str, Any]] = {}
    for timestamp_ms, open_, high, low, close, volume in rows:
        timestamp_ms = int(timestamp_ms)
        if timestamp_ms + TIMEFRAME_MS > closed_boundary_ms:
            continue
        records_by_timestamp[timestamp_ms] = {
            "symbol": symbol,
            "exchange": exchange_id,
            "timestamp": timestamp_ms,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "datetime": datetime.fromtimestamp(
                timestamp_ms / 1000,
                tz=timezone.utc,
            ),
        }
    return [records_by_timestamp[key] for key in sorted(records_by_timestamp)]


with DAG(
    dag_id="sync_crypto_ohlcv_3m_dag",
    description="Sync legacy Top 100 and CMC Top 30 SYMBOL/USDT OHLCV incrementally",
    default_args={
        "owner": "crypto-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="*/3 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    concurrency=PAIR_TASK_CONCURRENCY,
    max_active_runs=1,
    tags=["crypto", "ccxt", "ohlcv", "3m"],
) as dag:

    @task
    def get_pairs() -> List[Dict[str, str]]:
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            pairs = load_pairs(conn)
            if not pairs:
                raise ValueError("No symbol/exchange pairs found for 3m sync")
            logger.info("Loaded %s unique symbol/exchange pairs to sync", len(pairs))
            return [
                {"symbol": symbol, "exchange": exchange}
                for symbol, exchange in pairs
            ]
        finally:
            conn.close()

    @task(pool=POOL_NAME)
    def sync_pair(pair: Dict[str, str]) -> None:
        symbol = pair["symbol"]
        exchange_id = pair["exchange"]

        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            last_ts_ms = load_checkpoint(conn, symbol, exchange_id)
            since_ms = _since_from_checkpoint(last_ts_ms)
            logger.info("Fetching %s %s/%s since %s", exchange_id, symbol, QUOTE, since_ms)

            records = asyncio.run(fetch_for_pair(exchange_id, symbol, since_ms))
            if not records:
                logger.info("No new closed data for %s %s", exchange_id, symbol)
                return

            inserted = upsert_ohlcv(conn, records)
            upsert_checkpoint(
                conn,
                symbol,
                exchange_id,
                records[-1]["timestamp"],
            )
            logger.info("Upserted %s rows for %s %s", inserted, exchange_id, symbol)
        finally:
            conn.close()

    sync_pair.expand(pair=get_pairs())
