from __future__ import annotations

from typing import Any, Dict, Sequence


DAILY_MARKET_METRIC_COLUMNS: Sequence[str] = (
    "coin_id",
    "metric_date",
    "snapshot_at",
    "price_usd",
    "market_cap_usd",
    "fdv_usd",
    "volume_24h_usd",
    "price_change_pct_24h",
    "circulating_supply",
    "total_supply",
    "max_supply",
    "exchange_count",
    "best_liquidity_exchange",
    "cost_to_move_up_usd",
    "cost_to_move_down_usd",
    "bid_ask_spread_pct",
    "tvl_usd",
    "fees_24h_usd",
    "revenue_24h_usd",
    "dex_volume_24h_usd",
    "dex_liquidity_usd",
    "active_addresses_1d",
    "tx_count_1d",
    "tx_volume_usd_1d",
    "fees_usd_1d",
    "mc_to_tvl",
    "mc_to_fees_annualized",
    "sources",
    "quality",
    "source_last_synced_at",
)

DAILY_MARKET_METRIC_CONFLICT_KEYS = ("coin_id", "metric_date")


def build_daily_market_metric_upsert_sql(table_name: str) -> str:
    """Build the CoinGecko daily-metrics upsert used after the DB migration."""
    columns_sql = ",\n            ".join(DAILY_MARKET_METRIC_COLUMNS)
    placeholders_sql = ", ".join(["%s"] * len(DAILY_MARKET_METRIC_COLUMNS))
    update_columns = [
        column
        for column in DAILY_MARKET_METRIC_COLUMNS
        if column not in DAILY_MARKET_METRIC_CONFLICT_KEYS
    ]
    update_sql = ",\n            ".join(
        f"{column} = EXCLUDED.{column}" for column in update_columns
    )
    conflict_sql = ", ".join(DAILY_MARKET_METRIC_CONFLICT_KEYS)

    return f"""
        INSERT INTO {table_name} (
            {columns_sql}
        ) VALUES (
            {placeholders_sql}
        )
        ON CONFLICT ({conflict_sql}) DO UPDATE SET
            {update_sql}
    """


def upsert_daily_market_metric(conn, table_name: str, record: Dict[str, Any]) -> int:
    """Upsert one daily metric without deleting the existing audit timestamp."""
    sql = build_daily_market_metric_upsert_sql(table_name)
    values = tuple(record[column] for column in DAILY_MARKET_METRIC_COLUMNS)
    with conn.cursor() as cursor:
        cursor.execute(sql, values)
        return cursor.rowcount
