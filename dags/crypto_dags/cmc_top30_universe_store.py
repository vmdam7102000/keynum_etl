from __future__ import annotations

from datetime import date
from typing import Any, Iterable, List, Mapping, Sequence

from crypto_dags.cmc_top30_universe import TOP_N, effective_from, source_available_at


DEFAULT_RUN_TABLE = "raw_crypto_data.cmc_top30_universe_runs"
DEFAULT_SNAPSHOT_TABLE = "raw_crypto_data.cmc_top30_universe_snapshot"


def completed_snapshot_dates(
    conn,
    requested_dates: Sequence[date],
    *,
    run_table: str = DEFAULT_RUN_TABLE,
) -> set[date]:
    if not requested_dates:
        return set()
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT snapshot_date
            FROM {run_table}
            WHERE snapshot_date = ANY(%s)
              AND row_count = %s
              AND payload_sha256 IS NOT NULL
            """,
            (list(requested_dates), TOP_N),
        )
        return {row[0] for row in cursor.fetchall()}


def missing_snapshot_dates(
    conn,
    requested_dates: Sequence[date],
    *,
    refresh_existing: bool,
    run_table: str = DEFAULT_RUN_TABLE,
) -> List[date]:
    if refresh_existing or not requested_dates:
        return list(requested_dates)
    completed = completed_snapshot_dates(
        conn,
        requested_dates,
        run_table=run_table,
    )
    return [value for value in requested_dates if value not in completed]


def mark_snapshot_pending(
    conn,
    snapshot_date: date,
    *,
    run_table: str = DEFAULT_RUN_TABLE,
) -> None:
    """Record an attempt without discarding prior complete snapshot metadata."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {run_table} (
                    snapshot_date,
                    effective_from,
                    source_available_at,
                    status,
                    attempt_count,
                    last_attempt_at,
                    updated_at
                )
                VALUES (%s, %s, %s, 'pending', 1, now(), now())
                ON CONFLICT (snapshot_date) DO UPDATE SET
                    status = 'pending',
                    attempt_count = {run_table}.attempt_count + 1,
                    last_attempt_at = now(),
                    last_error = NULL,
                    updated_at = now()
                """,
                (
                    snapshot_date,
                    effective_from(snapshot_date),
                    source_available_at(snapshot_date),
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def mark_snapshot_failed(
    conn,
    snapshot_date: date,
    error: Any,
    *,
    run_table: str = DEFAULT_RUN_TABLE,
) -> None:
    """Persist a failure while preserving an older complete payload, if any."""
    message = str(error).strip() or error.__class__.__name__
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {run_table} (
                    snapshot_date,
                    effective_from,
                    source_available_at,
                    status,
                    attempt_count,
                    last_attempt_at,
                    last_error,
                    updated_at
                )
                VALUES (%s, %s, %s, 'failed', 1, now(), %s, now())
                ON CONFLICT (snapshot_date) DO UPDATE SET
                    status = 'failed',
                    last_attempt_at = now(),
                    last_error = EXCLUDED.last_error,
                    updated_at = now()
                """,
                (
                    snapshot_date,
                    effective_from(snapshot_date),
                    source_available_at(snapshot_date),
                    message[:4000],
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def replace_snapshot(
    conn,
    normalized: Mapping[str, Any],
    *,
    run_table: str = DEFAULT_RUN_TABLE,
    snapshot_table: str = DEFAULT_SNAPSHOT_TABLE,
) -> None:
    """Atomically replace one complete snapshot and mark its audit row successful."""
    from psycopg2.extras import Json, execute_values

    rows = list(normalized["rows"])
    if len(rows) != TOP_N:
        raise ValueError(f"Refusing to load an incomplete {len(rows)}-row snapshot")

    snapshot_date = normalized["snapshot_date"]
    snapshot_sql = f"""
        INSERT INTO {snapshot_table} (
            snapshot_date,
            cmc_id,
            cmc_rank,
            symbol,
            name,
            slug,
            price_usd,
            market_cap_usd,
            volume_24h_usd,
            circulating_supply,
            total_supply,
            max_supply,
            num_market_pairs,
            asset_last_updated_at,
            quote_last_updated_at,
            platform,
            tags,
            raw_payload
        )
        VALUES %s
    """
    values: Iterable[tuple[Any, ...]] = (
        (
            row["snapshot_date"],
            row["cmc_id"],
            row["cmc_rank"],
            row["symbol"],
            row["name"],
            row["slug"],
            row["price_usd"],
            row["market_cap_usd"],
            row["volume_24h_usd"],
            row["circulating_supply"],
            row["total_supply"],
            row["max_supply"],
            row["num_market_pairs"],
            row["asset_last_updated_at"],
            row["quote_last_updated_at"],
            Json(row["platform"]) if row["platform"] is not None else None,
            Json(row["tags"]),
            Json(row["raw_payload"]),
        )
        for row in rows
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {snapshot_table} WHERE snapshot_date = %s",
                (snapshot_date,),
            )
            execute_values(cursor, snapshot_sql, values, page_size=TOP_N)
            cursor.execute(
                f"""
                UPDATE {run_table}
                SET status = 'success',
                    source_status_at = %s,
                    collected_at = %s,
                    completed_at = now(),
                    row_count = %s,
                    api_credit_count = %s,
                    payload_sha256 = %s,
                    last_error = NULL,
                    updated_at = now()
                WHERE snapshot_date = %s
                """,
                (
                    normalized["source_status_at"],
                    normalized["collected_at"],
                    TOP_N,
                    normalized["api_credit_count"],
                    normalized["payload_sha256"],
                    snapshot_date,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"Missing pending audit row for snapshot {snapshot_date.isoformat()}"
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
