from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

import ccxt
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json, execute_values

from plugins.utils.config_loader import load_yaml_config
from plugins.utils.venue_market_rules import (
    build_snapshot_record,
    build_venue_targets,
    normalize_quote,
    parse_selection,
    resolve_market_with_fallback,
    snapshot_upsert_sql,
)

CONFIG = load_yaml_config("crypto_configs/venue_market_rules.yml")[
    "sync_crypto_venue_market_rules_dag"
]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
SCHEDULE = CONFIG["schedule_interval"]
QUOTE = normalize_quote(API_CFG.get("quote"))
FALLBACK_QUOTE = normalize_quote(API_CFG.get("fallback_quote", "USDC"))
POOL_NAME = API_CFG.get("pool_name", "ccxt_ohlcv_pool")
TASK_CONCURRENCY = int(API_CFG.get("task_concurrency", 5))
TIMEOUT_MS = int(API_CFG.get("timeout_ms", 60000))
MARKET_OVERRIDES: Mapping[str, Mapping[str, str]] = API_CFG.get(
    "market_symbol_overrides", {}
)

logger = logging.getLogger("sync_crypto_venue_market_rules_dag")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def load_metadata_rows(conn) -> List[Tuple[Any, Any]]:
    sql = f"""
    SELECT symbol, available_exchange
    FROM {DB_CFG['metadata_table']}
    WHERE available_exchange IS NOT NULL
      AND BTRIM(available_exchange) <> ''
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def _market_override(venue: str, asset_symbol: str) -> Optional[str]:
    venue_overrides = MARKET_OVERRIDES.get(venue, {})
    if not isinstance(venue_overrides, Mapping):
        raise ValueError(
            "market_symbol_overrides must map each venue to an asset-symbol mapping"
        )
    value = venue_overrides.get(asset_symbol)
    return str(value).strip() if value else None


def fetch_venue_markets(venue: str) -> Tuple[Mapping[str, Mapping[str, Any]], Any]:
    exchange_class = getattr(ccxt, venue, None)
    if exchange_class is None:
        raise ValueError(f"Exchange {venue} is not available in installed CCXT")

    exchange = exchange_class({"enableRateLimit": True, "timeout": TIMEOUT_MS})
    try:
        markets = exchange.load_markets()
        return markets, exchange.precisionMode
    finally:
        close = getattr(exchange, "close", None)
        if callable(close):
            close()


def upsert_snapshots(conn, records: List[Dict[str, Any]]) -> int:
    if not records:
        return 0

    values = [
        (
            record["venue"],
            record["market_symbol"],
            record["asset_symbol"],
            record["base_asset"],
            record["quote_asset"],
            record["active"],
            record["amount_step"],
            record["price_tick"],
            record["min_amount"],
            record["max_amount"],
            record["min_notional"],
            record["max_notional"],
            record["precision_mode"],
            Json(record["raw_info"]),
            record["captured_at"],
        )
        for record in records
    ]
    with conn.cursor() as cursor:
        execute_values(cursor, snapshot_upsert_sql(DB_CFG["target_table"]), values)
    conn.commit()
    return len(records)


with DAG(
    dag_id="sync_crypto_venue_market_rules_dag",
    description="Snapshot CCXT market constraints for configured crypto venue pairs",
    default_args={
        "owner": "crypto-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    concurrency=TASK_CONCURRENCY,
    max_active_runs=1,
    tags=["crypto", "ccxt", "metadata", "venue", "market-rules"],
) as dag:

    @task(task_id="get_venue_targets")
    def get_venue_targets(dag_run=None) -> List[Dict[str, Any]]:
        conf = dict(dag_run.conf or {}) if dag_run and dag_run.conf else {}
        selected_venues = parse_selection(conf.get("venues"))
        selected_symbols = parse_selection(conf.get("symbols"), upper=True)

        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            targets = build_venue_targets(
                load_metadata_rows(conn),
                selected_venues=selected_venues,
                selected_symbols=selected_symbols,
            )
        finally:
            conn.close()

        if not targets:
            raise ValueError("No configured venue/pair targets match the requested filters")
        logger.info(
            "Prepared %s venue targets with %s total pairs",
            len(targets),
            sum(len(target["asset_symbols"]) for target in targets),
        )
        return targets

    @task(task_id="snapshot_venue", pool=POOL_NAME)
    def snapshot_venue(target: Dict[str, Any]) -> Dict[str, Any]:
        venue = str(target["venue"])
        asset_symbols = [str(symbol).upper() for symbol in target["asset_symbols"]]
        markets, precision_mode = fetch_venue_markets(venue)
        captured_at = datetime.now(timezone.utc).date()

        records: List[Dict[str, Any]] = []
        unresolved_markets = []
        for asset_symbol in asset_symbols:
            market, resolved_quote, missing_reason = resolve_market_with_fallback(
                markets,
                asset_symbol=asset_symbol,
                primary_quote=QUOTE,
                fallback_quote=FALLBACK_QUOTE,
                override=_market_override(venue, asset_symbol),
            )
            if market is None:
                unresolved_markets.append(
                    f"{asset_symbol}/{QUOTE} or {asset_symbol}/{FALLBACK_QUOTE}: "
                    f"{missing_reason}"
                )
                continue
            records.append(
                build_snapshot_record(
                    venue=venue,
                    asset_symbol=asset_symbol,
                    quote=resolved_quote,
                    market=market,
                    precision_mode=precision_mode,
                    captured_at=captured_at,
                )
            )

        if unresolved_markets:
            raise RuntimeError(
                f"{venue} has unresolved {QUOTE}/{FALLBACK_QUOTE} spot markets: "
                + ", ".join(unresolved_markets)
            )

        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            upsert_snapshots(conn, records)
        finally:
            conn.close()

        inactive_count = sum(record["active"] is False for record in records)
        result = {
            "venue": venue,
            "captured": len(records),
            "inactive": inactive_count,
            "captured_at": captured_at.isoformat(),
        }
        logger.info("Venue rule snapshot completed: %s", result)
        return result

    snapshot_venue.expand(target=get_venue_targets())
