"""Airflow-native Sentix backfill, incremental load, and legacy import."""
from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, timedelta

import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_values

from . import romeo, sentix_api
from .config import SentixCredentials, Settings
from .db import qualified_identifier


log = logging.getLogger(__name__)
ConnectionFactory = Callable[[], object]


def _start_run(conn, settings: Settings, mode: str, start_date: date) -> int:
    query = sql.SQL(
        "INSERT INTO {} (mode, start_date_req) VALUES (%s, %s) RETURNING run_id"
    ).format(qualified_identifier(settings.load_run_table))
    with conn.cursor() as cursor:
        cursor.execute(query, (mode, start_date))
        return int(cursor.fetchone()[0])


def _finish_run(conn, settings: Settings, run_id: int, **fields) -> None:
    assignments = [
        sql.SQL("{} = %s").format(sql.Identifier(column))
        for column in fields
    ]
    query = sql.SQL(
        "UPDATE {} SET finished_at = now(), {} WHERE run_id = %s"
    ).format(
        qualified_identifier(settings.load_run_table),
        sql.SQL(", ").join(assignments),
    )
    with conn.cursor() as cursor:
        cursor.execute(query, (*fields.values(), run_id))


def upsert_observations(
    conn,
    settings: Settings,
    frame: pd.DataFrame,
) -> int:
    """Upsert vendor corrections without deleting historical observations."""
    if frame.empty:
        return 0
    query = sql.SQL(
        """
        INSERT INTO {} (code, obs_date, value)
        VALUES %s
        ON CONFLICT (code, obs_date) DO UPDATE SET
            value = EXCLUDED.value,
            ingested_at = now()
        """
    ).format(qualified_identifier(settings.observation_table))
    records = list(
        frame[["code", "obs_date", "value"]].itertuples(index=False, name=None)
    )
    with conn.cursor() as cursor:
        execute_values(
            cursor,
            query.as_string(conn),
            records,
            page_size=settings.batch_size,
        )
    return len(records)


def refresh_series_catalogue(
    conn,
    settings: Settings,
    active_codes: set[str],
) -> None:
    query = sql.SQL(
        """
        INSERT INTO {series} (
            code, first_obs_date, last_obs_date, obs_count,
            is_active, last_seen_date, updated_at
        )
        SELECT
            observation.code,
            min(observation.obs_date),
            max(observation.obs_date),
            count(*),
            observation.code = ANY(%s),
            CASE WHEN observation.code = ANY(%s) THEN CURRENT_DATE END,
            now()
        FROM {observation} AS observation
        GROUP BY observation.code
        ON CONFLICT (code) DO UPDATE SET
            first_obs_date = EXCLUDED.first_obs_date,
            last_obs_date = EXCLUDED.last_obs_date,
            obs_count = EXCLUDED.obs_count,
            is_active = EXCLUDED.is_active,
            last_seen_date = COALESCE(
                EXCLUDED.last_seen_date,
                {series}.last_seen_date
            ),
            updated_at = now()
        """
    ).format(
        series=qualified_identifier(settings.series_table),
        observation=qualified_identifier(settings.observation_table),
    )
    codes = sorted(active_codes)
    with conn.cursor() as cursor:
        cursor.execute(query, (codes, codes))


def _latest_observation_codes(frame: pd.DataFrame) -> tuple[date, set[str]]:
    """Return the latest response date and the codes present on that date."""
    latest_obs_date = frame["obs_date"].max()
    active_codes = set(
        frame.loc[frame["obs_date"] == latest_obs_date, "code"].unique()
    )
    return latest_obs_date, active_codes


def _total_rows(conn, settings: Settings) -> int:
    query = sql.SQL("SELECT count(*) FROM {}").format(
        qualified_identifier(settings.observation_table)
    )
    with conn.cursor() as cursor:
        cursor.execute(query)
        return int(cursor.fetchone()[0])


def _observation_code_count(
    conn,
    settings: Settings,
    obs_date: date,
) -> int:
    query = sql.SQL(
        "SELECT count(DISTINCT code) FROM {} WHERE obs_date = %s"
    ).format(qualified_identifier(settings.observation_table))
    with conn.cursor() as cursor:
        cursor.execute(query, (obs_date,))
        return int(cursor.fetchone()[0])


def _record_failure(
    audit_connection_factory: ConnectionFactory,
    settings: Settings,
    run_id: int,
    exc: Exception,
) -> None:
    audit_conn = audit_connection_factory()
    try:
        _finish_run(
            audit_conn,
            settings,
            run_id,
            status="error",
            error=str(exc)[:2000],
        )
        audit_conn.commit()
    finally:
        audit_conn.close()


def _load(
    conn,
    audit_connection_factory: ConnectionFactory,
    mode: str,
    start_date: date,
    credentials: SentixCredentials,
    settings: Settings,
) -> dict:
    run_id = _start_run(conn, settings, mode, start_date)
    conn.commit()
    try:
        frame = sentix_api.fetch(credentials, start_date, settings)
        written = upsert_observations(conn, settings, frame)
        latest_obs_date, active_codes = _latest_observation_codes(frame)
        refresh_series_catalogue(conn, settings, active_codes)
        total_rows_after = _total_rows(conn, settings)
        summary = {
            "run_id": run_id,
            "mode": mode,
            "start_date_req": start_date,
            "rows_received": len(frame),
            "rows_upserted": written,
            "codes_seen": len(active_codes),
            "max_obs_date": latest_obs_date,
            "total_rows_after": total_rows_after,
        }
        _finish_run(
            conn,
            settings,
            run_id,
            status="ok",
            rows_received=summary["rows_received"],
            rows_upserted=summary["rows_upserted"],
            codes_seen=summary["codes_seen"],
            max_obs_date=summary["max_obs_date"],
            total_rows_after=summary["total_rows_after"],
            error=None,
        )
        conn.commit()
        log.info(
            "Sentix %s load completed: rows=%d codes=%d latest=%s total=%d",
            mode,
            summary["rows_received"],
            summary["codes_seen"],
            summary["max_obs_date"],
            summary["total_rows_after"],
        )
        return summary
    except Exception as exc:
        conn.rollback()
        _record_failure(audit_connection_factory, settings, run_id, exc)
        log.exception("Sentix %s load failed", mode)
        raise


def backfill(
    conn,
    audit_connection_factory: ConnectionFactory,
    credentials: SentixCredentials,
    settings: Settings,
) -> dict:
    return _load(
        conn,
        audit_connection_factory,
        "backfill",
        settings.parsed_backfill_start_date(),
        credentials,
        settings,
    )


def incremental(
    conn,
    audit_connection_factory: ConnectionFactory,
    credentials: SentixCredentials,
    settings: Settings,
) -> dict:
    query = sql.SQL("SELECT max(obs_date) FROM {}").format(
        qualified_identifier(settings.observation_table)
    )
    with conn.cursor() as cursor:
        cursor.execute(query)
        latest = cursor.fetchone()[0]
    if latest is None:
        log.warning("No Sentix observations exist; promoting incremental to backfill")
        return backfill(conn, audit_connection_factory, credentials, settings)
    start_date = latest - timedelta(days=settings.incremental_overlap_days)
    return _load(
        conn,
        audit_connection_factory,
        "incremental",
        start_date,
        credentials,
        settings,
    )


def read_series(conn, code: str, settings: Settings) -> pd.Series:
    query = sql.SQL(
        "SELECT obs_date, value FROM {} WHERE code = %s ORDER BY obs_date"
    ).format(qualified_identifier(settings.observation_table))
    with conn.cursor() as cursor:
        cursor.execute(query, (code,))
        rows = cursor.fetchall()
    if not rows:
        raise LookupError(f"No observations for Sentix code {code!r}")
    frame = pd.DataFrame(rows, columns=["obs_date", "value"])
    frame["obs_date"] = pd.to_datetime(frame["obs_date"])
    return frame.set_index("obs_date")["value"].astype(float).rename(code)


def validate_loaded_data(
    conn,
    settings: Settings,
    as_of_date: date,
) -> dict:
    """Fail before publication when the weekly vendor feed is incomplete."""
    audit_query = sql.SQL(
        """
        SELECT codes_seen, max_obs_date, total_rows_after
        FROM {}
        WHERE status = 'ok' AND mode IN ('backfill', 'incremental')
        ORDER BY run_id DESC
        LIMIT 2
        """
    ).format(qualified_identifier(settings.load_run_table))
    with conn.cursor() as cursor:
        cursor.execute(audit_query)
        load_rows = cursor.fetchall()
    if not load_rows:
        raise RuntimeError("No successful Sentix vendor load is available")
    codes_seen, latest_obs_date, total_rows = load_rows[0]
    if int(codes_seen) < settings.minimum_codes:
        raise RuntimeError(
            f"Latest Sentix observation date contained {codes_seen} codes; "
            f"minimum is {settings.minimum_codes}"
        )
    if len(load_rows) > 1 and load_rows[1][1]:
        previous_obs_date = load_rows[1][1]
        previous_codes = _observation_code_count(
            conn,
            settings,
            previous_obs_date,
        )
        if previous_codes:
            retention = int(codes_seen) / previous_codes
            if retention < settings.minimum_code_retention_ratio:
                raise RuntimeError(
                    f"Sentix code count retained only {retention:.1%}; "
                    f"minimum is {settings.minimum_code_retention_ratio:.1%}"
                )
    expected_friday = as_of_date - timedelta(days=(as_of_date.weekday() - 4) % 7)
    if latest_obs_date < expected_friday:
        raise RuntimeError(
            f"Latest Sentix observation is {latest_obs_date}; "
            f"expected at least {expected_friday}"
        )
    age_days = (as_of_date - latest_obs_date).days
    if age_days > settings.stale_after_days:
        raise RuntimeError(
            f"Sentix feed is stale by {age_days} days; "
            f"limit is {settings.stale_after_days}"
        )
    zdf = read_series(conn, romeo.ZDF_CODE, settings)
    sentiment = read_series(conn, romeo.SENT_CODE, settings)
    mismatched = zdf.index.symmetric_difference(sentiment.index)
    if len(mismatched):
        raise RuntimeError(
            f"Romeo inputs cover different dates ({len(mismatched)} mismatches)"
        )
    window = settings.unchanged_sentiment_weeks
    recent = sentiment.tail(window)
    if len(recent) == window and recent.nunique(dropna=False) == 1:
        raise RuntimeError(
            f"{romeo.SENT_CODE} is unchanged for {window} consecutive weeks"
        )
    return {
        "codes_seen": int(codes_seen),
        "latest_obs_date": latest_obs_date,
        "expected_friday": expected_friday,
        "total_rows": int(total_rows),
        "romeo_input_rows": len(zdf),
    }


def import_retired_observations(
    conn,
    audit_connection_factory: ConnectionFactory,
    legacy_frame: pd.DataFrame,
    settings: Settings,
) -> dict:
    """Import only legacy codes absent from the current vendor catalogue."""
    active_query = sql.SQL("SELECT code FROM {} WHERE is_active IS TRUE").format(
        qualified_identifier(settings.series_table)
    )
    with conn.cursor() as cursor:
        cursor.execute(active_query)
        active_codes = {row[0] for row in cursor.fetchall()}
    if len(active_codes) < settings.minimum_codes:
        raise RuntimeError(
            "Refusing retired-code import before a successful current vendor backfill"
        )
    legacy_codes = set(legacy_frame["code"].unique())
    retired_codes = legacy_codes - active_codes
    retired = legacy_frame[legacy_frame["code"].isin(retired_codes)].copy()
    if retired.empty:
        return {
            "run_id": None,
            "rows_received": 0,
            "rows_upserted": 0,
            "codes_seen": 0,
            "retired_codes": [],
        }
    start_date = retired["obs_date"].min()
    run_id = _start_run(conn, settings, "legacy_import", start_date)
    conn.commit()
    try:
        written = upsert_observations(conn, settings, retired)
        catalogue_query = sql.SQL(
            """
            INSERT INTO {series} (
                code, first_obs_date, last_obs_date, obs_count,
                is_active, last_seen_date, updated_at
            )
            SELECT code, min(obs_date), max(obs_date), count(*), FALSE, NULL, now()
            FROM {observation}
            WHERE code = ANY(%s)
            GROUP BY code
            ON CONFLICT (code) DO UPDATE SET
                first_obs_date = EXCLUDED.first_obs_date,
                last_obs_date = EXCLUDED.last_obs_date,
                obs_count = EXCLUDED.obs_count,
                is_active = FALSE,
                updated_at = now()
            """
        ).format(
            series=qualified_identifier(settings.series_table),
            observation=qualified_identifier(settings.observation_table),
        )
        with conn.cursor() as cursor:
            cursor.execute(catalogue_query, (sorted(retired_codes),))
        total_rows_after = _total_rows(conn, settings)
        _finish_run(
            conn,
            settings,
            run_id,
            status="ok",
            rows_received=len(retired),
            rows_upserted=written,
            codes_seen=len(retired_codes),
            max_obs_date=retired["obs_date"].max(),
            total_rows_after=total_rows_after,
            error=None,
        )
        conn.commit()
        return {
            "run_id": run_id,
            "rows_received": len(retired),
            "rows_upserted": written,
            "codes_seen": len(retired_codes),
            "retired_codes": sorted(retired_codes),
            "total_rows_after": total_rows_after,
        }
    except Exception as exc:
        conn.rollback()
        _record_failure(audit_connection_factory, settings, run_id, exc)
        raise
