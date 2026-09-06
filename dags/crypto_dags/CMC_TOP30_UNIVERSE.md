# CMC Top 30 point-in-time — Phase 1

This pipeline stores CoinMarketCap ranks 1 through 30 at each UTC month-end and
uses the existing symbol-based OHLCV tables. `cmc_id` remains raw CMC metadata;
it is not added to OHLCV, checkpoints, aggregates, or market mappings.

## Deploy

1. Apply the single idempotent migration
   `dags/crypto_dags/sql/20260829_add_cmc_top30_market_data_pipeline.sql`.
2. Configure Airflow Variable `cmc_api_key` and create pool
   `ccxt_ohlcv_pool` with three slots.
3. Trigger `sync_cmc_top30_point_in_time_universe_dag`.
4. Review `raw_crypto_data.cmc_top30_data_coverage`.
5. Trigger `backfill_cmc_top30_historical_data_dag` with
   `{"mode":"missing_only","confirm_large_backfill":true}` when the estimate
   exceeds the configured safety threshold.

The universe DAG stores each successful snapshot atomically, then triggers the
symbol mapping DAG. The mapping DAG triggers a missing-only OHLCV backfill.

## Point-in-time rules

CMC publishes a completed month-end snapshot at `00:30 UTC` on the first day of
the next month. The timestamp API therefore avoids look-ahead:

```sql
SELECT *
FROM raw_crypto_data.cmc_top30_universe_as_of(
    TIMESTAMPTZ '2026-08-01 00:29:00+00'
);

SELECT *
FROM raw_crypto_data.cmc_top30_universe_as_of(
    TIMESTAMPTZ '2026-08-01 00:30:00+00'
);
```

The first query still uses the preceding complete snapshot. The DATE overload
keeps the existing daily convention, where a month-end becomes effective on the
next calendar day. Missing months carry the last successful snapshot forward.

## Symbol and venue policy

For each raw `cmc_id`, only the latest observed symbol becomes a canonical
market-data target. Consequently old aliases such as `RNDR` and `GRAM` are not
Top 30 price targets; `RENDER` and `TON` start at the first snapshot publication
time where their new symbol appears. Non-renamed symbols use
`2013-01-01 00:00 UTC` as their requested history start.

Markets are exact spot `SYMBOL/USDT` pairs. Venue priority is:

`binance -> okx -> bybit -> kucoin -> bitget -> gateio`

The resolver tries the next venue only when the prior venue does not list the
pair. Once a venue has been selected, it remains pinned; an empty historical
response is recorded as `unavailable` and never causes cross-venue stitching.
Use `market.exchange_overrides` for a reviewed venue change. To return a symbol
to automatic priority selection, explicitly clear its `selected_exchange`, set
both statuses back to `pending`, and clear its actual range before rerunning the
mapping DAG. Existing OHLCV rows are not deleted by that reset.

```sql
UPDATE raw_crypto_data.cmc_top30_symbol_targets
SET selected_exchange = NULL,
    mapping_status = 'pending',
    backfill_status = 'pending',
    actual_first_candle_at = NULL,
    actual_last_candle_at = NULL,
    last_error = NULL,
    updated_at = now()
WHERE symbol = 'RENDER';
```

Stablecoins and wrapped tokens remain visible in the raw Top 30 snapshot but
are marked `excluded_by_policy` in symbol targets and omitted from the
investable universe.

## OHLCV behavior

OHLCV and checkpoints retain their legacy keys:

- candle: `(symbol, exchange, timestamp)`
- checkpoint: `(symbol, exchange, timeframe)`

If the selected pair already has rows, backfill resumes from its maximum
timestamp plus three minutes. If it has no rows, backfill starts from
`data_start_at`. The first candle returned may be later than the request; this
is recorded as actual coverage and is not filled with artificial zero candles.

Pagination commits each page, advances strictly by timestamp, drops incomplete
candles, and stops on a non-progressing response. Successful inserts rebuild
only the affected symbol/range for 15m, 1h, 2h, 4h, 8h, 1d, and 7d.

Manual backfill filters use symbols rather than CMC ids:

```json
{
  "mode": "missing_only",
  "symbols": ["RENDER", "TON"],
  "confirm_large_backfill": true
}
```

## Queries and audit

```sql
SELECT *
FROM raw_crypto_data.cmc_top30_investable_universe_as_of(now());

SELECT *
FROM raw_crypto_data.cmc_top30_data_coverage
ORDER BY symbol;

SELECT *
FROM raw_crypto_data.cmc_top30_ohlcv_3m_pit
WHERE timestamp >= EXTRACT(
    EPOCH FROM TIMESTAMPTZ '2026-01-01 00:00:00+00'
) * 1000;
```

Coverage is always evaluated by `(symbol, selected_exchange)`, not symbol alone.
Existing rows for old aliases and other exchanges are retained but excluded
from Top 30 PIT views. Phase 1 does not ingest asset fundamentals, scan internal
candle gaps, or change the existing global-metrics pipeline.
