"""Compute and persist the current version of the Romeo signal."""
from __future__ import annotations

import json
import logging
from collections.abc import Callable

import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_values

from . import romeo
from .config import Settings
from .db import qualified_identifier
from .loader import read_series


log = logging.getLogger(__name__)
SIGNAL_ID = "romeo.sp500"
DIAGNOSTIC_COLUMNS = [
    "zdf",
    "sent",
    "zdf_low",
    "zdf_high",
    "sent_low",
    "sent_high",
]
ConnectionFactory = Callable[[], object]


def _require_definition(
    conn,
    settings: Settings,
    signal_id: str,
    spec_version: str,
) -> None:
    query = sql.SQL(
        """
        SELECT is_current
        FROM {}
        WHERE signal_id = %s AND spec_version = %s
        """
    ).format(qualified_identifier(settings.signal_definition_table))
    with conn.cursor() as cursor:
        cursor.execute(query, (signal_id, spec_version))
        row = cursor.fetchone()
    if row is None:
        raise RuntimeError(
            f"Signal definition {signal_id!r}/{spec_version!r} is not deployed"
        )
    if row[0] is not True:
        raise RuntimeError(
            f"Signal definition {signal_id!r}/{spec_version!r} is not current"
        )


def _start_run(
    conn,
    settings: Settings,
    signal_id: str,
    spec_version: str,
) -> int:
    query = sql.SQL(
        """
        INSERT INTO {} (signal_id, spec_version)
        VALUES (%s, %s)
        RETURNING run_id
        """
    ).format(qualified_identifier(settings.signal_run_table))
    with conn.cursor() as cursor:
        cursor.execute(query, (signal_id, spec_version))
        return int(cursor.fetchone()[0])


def _finish_run(conn, settings: Settings, run_id: int, **fields) -> None:
    assignments = [
        sql.SQL("{} = %s").format(sql.Identifier(column))
        for column in fields
    ]
    query = sql.SQL(
        "UPDATE {} SET finished_at = now(), {} WHERE run_id = %s"
    ).format(
        qualified_identifier(settings.signal_run_table),
        sql.SQL(", ").join(assignments),
    )
    with conn.cursor() as cursor:
        cursor.execute(query, (*fields.values(), run_id))


def _to_records(
    frame: pd.DataFrame,
    signal_id: str,
    spec_version: str,
) -> list[tuple]:
    records = []
    for timestamp, row in frame.iterrows():
        diagnostics = {
            column: (
                bool(row[column])
                if frame[column].dtype == bool
                else float(row[column])
            )
            for column in DIAGNOSTIC_COLUMNS
        }
        records.append(
            (
                signal_id,
                spec_version,
                timestamp.date(),
                float(row["position"]),
                json.dumps(diagnostics),
            )
        )
    return records


def write_signal(
    conn,
    settings: Settings,
    records: list[tuple],
) -> int:
    query = sql.SQL(
        """
        INSERT INTO {} (
            signal_id, spec_version, obs_date, value, diagnostics
        )
        VALUES %s
        ON CONFLICT (signal_id, spec_version, obs_date) DO UPDATE SET
            value = EXCLUDED.value,
            diagnostics = EXCLUDED.diagnostics,
            computed_at = now()
        """
    ).format(qualified_identifier(settings.signal_observation_table))
    with conn.cursor() as cursor:
        execute_values(cursor, query.as_string(conn), records, page_size=1_000)
    return len(records)


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


def publish(
    conn,
    audit_connection_factory: ConnectionFactory,
    settings: Settings,
    params: romeo.RomeoParams = romeo.VARIANT_A,
    signal_id: str = SIGNAL_ID,
) -> dict:
    _require_definition(conn, settings, signal_id, params.spec_version)
    run_id = _start_run(conn, settings, signal_id, params.spec_version)
    conn.commit()
    try:
        zdf = read_series(conn, romeo.ZDF_CODE, settings)
        sentiment = read_series(conn, romeo.SENT_CODE, settings)
        frame = romeo.compute(zdf, sentiment, params)
        records = _to_records(frame, signal_id, params.spec_version)
        written = write_signal(conn, settings, records)
        latest = romeo.latest(frame)
        summary = {
            "run_id": run_id,
            "signal_id": signal_id,
            "spec_version": params.spec_version,
            "rows_written": written,
            "max_obs_date": frame.index[-1].date(),
            "latest_value": float(frame["position"].iloc[-1]),
            "latest": latest,
        }
        _finish_run(
            conn,
            settings,
            run_id,
            status="ok",
            rows_written=written,
            max_obs_date=summary["max_obs_date"],
            latest_value=summary["latest_value"],
            error=None,
        )
        conn.commit()
        log.info(
            "Published %s/%s: rows=%d latest=%s value=%s",
            signal_id,
            params.spec_version,
            written,
            summary["max_obs_date"],
            summary["latest_value"],
        )
        return summary
    except Exception as exc:
        conn.rollback()
        _record_failure(audit_connection_factory, settings, run_id, exc)
        log.exception("Romeo publication failed")
        raise


def validate_published_signal(
    conn,
    settings: Settings,
    signal_id: str = SIGNAL_ID,
    spec_version: str = romeo.VARIANT_A.spec_version,
) -> dict:
    latest_query = sql.SQL(
        """
        SELECT signal_id, obs_date, value, spec_version
        FROM {}
        WHERE signal_id = %s
        """
    ).format(qualified_identifier(settings.signal_latest_view))
    with conn.cursor() as cursor:
        cursor.execute(latest_query, (signal_id,))
        latest = cursor.fetchone()
    if latest is None:
        raise RuntimeError(f"No latest signal row exists for {signal_id!r}")
    _, signal_date, latest_value, stored_version = latest
    if stored_version != spec_version:
        raise RuntimeError(
            f"Latest {signal_id} version is {stored_version}; expected {spec_version}"
        )
    input_query = sql.SQL(
        """
        SELECT code, max(obs_date)
        FROM {}
        WHERE code = ANY(%s)
        GROUP BY code
        """
    ).format(qualified_identifier(settings.observation_table))
    with conn.cursor() as cursor:
        cursor.execute(input_query, ([romeo.ZDF_CODE, romeo.SENT_CODE],))
        input_dates = dict(cursor.fetchall())
    if set(input_dates) != {romeo.ZDF_CODE, romeo.SENT_CODE}:
        raise RuntimeError("One or more Romeo input series are missing")
    if len(set(input_dates.values())) != 1:
        raise RuntimeError(f"Romeo input latest dates differ: {input_dates}")
    input_date = next(iter(input_dates.values()))
    if signal_date != input_date:
        raise RuntimeError(
            f"Latest signal date {signal_date} does not match input date {input_date}"
        )
    if float(latest_value) not in {-1.0, 0.0, 0.5, 1.0}:
        raise RuntimeError(f"Unexpected Romeo value: {latest_value}")
    return {
        "signal_id": signal_id,
        "spec_version": stored_version,
        "obs_date": signal_date,
        "value": float(latest_value),
    }

