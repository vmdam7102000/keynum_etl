from __future__ import annotations

from typing import Any, List, Optional, Sequence


def aggregate_ohlcv_window(
    conn,
    *,
    source_table: str,
    target_table: str,
    bucket_ms: int,
    start_ts_ms: int,
    end_ts_ms: int,
    conflict_keys: Sequence[str] = ("symbol", "exchange", "timestamp"),
    symbols: Optional[Sequence[str]] = None,
    exchanges: Optional[Sequence[str]] = None,
) -> int:
    """Aggregate one bounded OHLCV window using the legacy table contract."""
    if end_ts_ms <= start_ts_ms:
        raise ValueError("end_ts_ms must be greater than start_ts_ms")
    if bucket_ms <= 0:
        raise ValueError("bucket_ms must be positive")

    filters = []
    params: List[Any] = [start_ts_ms, end_ts_ms]
    if symbols:
        filters.append(f"{source_table}.symbol = ANY(%s)")
        params.append([str(symbol).upper() for symbol in symbols])
    if exchanges:
        filters.append(f"{source_table}.exchange = ANY(%s)")
        params.append([str(exchange) for exchange in exchanges])
    filter_sql = "".join(f"\n          AND {condition}" for condition in filters)

    sql = f"""
    WITH base AS (
        SELECT
            symbol,
            exchange,
            ({source_table}.timestamp / {bucket_ms}) * {bucket_ms} AS bucket_ts,
            {source_table}.timestamp AS ts,
            open,
            high,
            low,
            close,
            volume,
            ROW_NUMBER() OVER (
                PARTITION BY symbol, exchange,
                    ({source_table}.timestamp / {bucket_ms})
                ORDER BY {source_table}.timestamp ASC
            ) AS rn_asc,
            ROW_NUMBER() OVER (
                PARTITION BY symbol, exchange,
                    ({source_table}.timestamp / {bucket_ms})
                ORDER BY {source_table}.timestamp DESC
            ) AS rn_desc
        FROM {source_table}
        WHERE {source_table}.timestamp >= %s
          AND {source_table}.timestamp < %s
          {filter_sql}
    ),
    aggregated AS (
        SELECT
            symbol,
            exchange,
            bucket_ts AS timestamp,
            MAX(open) FILTER (WHERE rn_asc = 1) AS open,
            MAX(high) AS high,
            MIN(low) AS low,
            MAX(close) FILTER (WHERE rn_desc = 1) AS close,
            SUM(volume) AS volume,
            to_timestamp(bucket_ts / 1000)::timestamptz AS datetime
        FROM base
        GROUP BY symbol, exchange, bucket_ts
    )
    INSERT INTO {target_table} (
        symbol, exchange, timestamp, open, high, low, close, volume, datetime
    )
    SELECT
        symbol, exchange, timestamp, open, high, low, close, volume, datetime
    FROM aggregated
    ON CONFLICT ({", ".join(conflict_keys)})
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        datetime = EXCLUDED.datetime;
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, tuple(params))
        affected = cursor.rowcount
    conn.commit()
    return affected
