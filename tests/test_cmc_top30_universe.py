from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import types
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = REPOSITORY_ROOT / "dags"
for import_root in (REPOSITORY_ROOT, DAGS_ROOT):
    if str(import_root) not in sys.path:
        sys.path.insert(0, str(import_root))

from crypto_dags.cmc_top30_universe import (  # noqa: E402
    TOP_N,
    effective_from,
    historical_listing_params,
    latest_available_daily_snapshot_date,
    latest_available_month_end,
    normalize_historical_listing,
    requested_snapshot_dates,
    source_available_at,
)
from crypto_dags.cmc_top30_universe_store import (  # noqa: E402
    DEFAULT_RUN_TABLE,
    DEFAULT_SNAPSHOT_TABLE,
    mark_snapshot_failed,
    missing_snapshot_dates,
    replace_snapshot,
)


SNAPSHOT_DATE = date(2026, 7, 31)
COLLECTED_AT = datetime(2026, 8, 1, 1, 15, tzinfo=timezone.utc)


def _listing(rank: int, *, cmc_id: int | None = None) -> dict:
    return {
        "id": cmc_id if cmc_id is not None else 10_000 + rank,
        "cmc_rank": rank,
        "symbol": f"c{rank}",
        "name": f"Coin {rank}",
        "slug": f"coin-{rank}",
        "circulating_supply": f"{rank}.5",
        "total_supply": rank * 2,
        "max_supply": rank * 3,
        "num_market_pairs": rank + 100,
        "last_updated": "2026-07-31T23:59:00Z",
        "platform": {"id": 1, "symbol": "ETH"} if rank == 2 else None,
        "tags": ["mineable"] if rank == 1 else [],
        "quote": {
            "USD": {
                "price": f"{rank}.25",
                "market_cap": rank * 1_000_000,
                "volume_24h": rank * 10_000,
                "last_updated": "2026-07-31T23:59:30Z",
            }
        },
    }


def _payload(count: int = TOP_N) -> dict:
    return {
        "status": {
            "timestamp": "2026-08-01T00:30:00Z",
            "error_code": 0,
            "error_message": None,
            "credit_count": 1,
        },
        "data": [_listing(rank) for rank in range(1, count + 1)],
    }


def _normalized() -> dict:
    return normalize_historical_listing(
        _payload(),
        snapshot_date=SNAPSHOT_DATE,
        collected_at=COLLECTED_AT,
    )


class _Cursor:
    def __init__(self, connection: "_Connection"):
        self.connection = connection
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, values=None):
        self.connection.statements.append((sql, values))
        if self.connection.fail_when_sql_contains in sql:
            raise RuntimeError("simulated database failure")
        if sql.lstrip().startswith("UPDATE"):
            self.rowcount = self.connection.update_rowcount

    def fetchall(self):
        return list(self.connection.fetchall_rows)


class _Connection:
    def __init__(
        self,
        *,
        fetchall_rows=(),
        update_rowcount: int = 1,
        fail_when_sql_contains: str = "__never_matches__",
    ):
        self.fetchall_rows = list(fetchall_rows)
        self.update_rowcount = update_rowcount
        self.fail_when_sql_contains = fail_when_sql_contains
        self.statements = []
        self.commit_count = 0
        self.rollback_count = 0

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1


def _fake_psycopg2_modules():
    calls = []

    class _Json:
        def __init__(self, value):
            self.value = value

    def _execute_values(cursor, sql, values, *, page_size):
        calls.append((cursor, sql, list(values), page_size))

    extras = types.ModuleType("psycopg2.extras")
    extras.Json = _Json
    extras.execute_values = _execute_values
    psycopg2 = types.ModuleType("psycopg2")
    psycopg2.__path__ = []
    psycopg2.extras = extras
    return {"psycopg2": psycopg2, "psycopg2.extras": extras}, calls


class CmcTop30DateAndApiTest(unittest.TestCase):
    def test_top_n_and_historical_request_contract(self):
        params = historical_listing_params(SNAPSHOT_DATE)

        self.assertEqual(TOP_N, 30)
        self.assertEqual(params["date"], "2026-07-31")
        self.assertEqual(params["start"], 1)
        self.assertEqual(params["limit"], 30)
        self.assertEqual(params["sort"], "cmc_rank")
        self.assertEqual(params["sort_dir"], "asc")
        self.assertEqual(params["cryptocurrency_type"], "all")

    def test_historical_request_rejects_non_month_end(self):
        with self.assertRaisesRegex(ValueError, "calendar month-end"):
            historical_listing_params(date(2026, 7, 30))

    def test_publication_boundary_and_effective_dates_are_point_in_time(self):
        just_before = datetime(2026, 8, 1, 0, 29, 59, tzinfo=timezone.utc)
        at_publication = datetime(2026, 8, 1, 0, 30, tzinfo=timezone.utc)

        self.assertEqual(
            latest_available_daily_snapshot_date(just_before), date(2026, 7, 30)
        )
        self.assertEqual(latest_available_month_end(just_before), date(2026, 6, 30))
        self.assertEqual(
            latest_available_daily_snapshot_date(at_publication), SNAPSHOT_DATE
        )
        self.assertEqual(latest_available_month_end(at_publication), SNAPSHOT_DATE)
        self.assertEqual(source_available_at(SNAPSHOT_DATE), at_publication)
        self.assertEqual(effective_from(SNAPSHOT_DATE), date(2026, 8, 1))

    def test_scheduled_range_covers_accessible_month_ends_in_history_window(self):
        result = requested_snapshot_dates(
            {},
            now=datetime(2026, 8, 1, 0, 30, tzinfo=timezone.utc),
            history_years=1,
        )

        self.assertEqual(result[0], date(2025, 7, 31))
        self.assertEqual(result[-1], SNAPSHOT_DATE)
        self.assertEqual(len(result), 13)

    def test_manual_range_is_inclusive_and_requires_month_ends(self):
        now = datetime(2026, 8, 1, 0, 30, tzinfo=timezone.utc)

        self.assertEqual(
            requested_snapshot_dates(
                {"start_date": "2026-05-31", "end_date": "2026-07-31"},
                now=now,
            ),
            [date(2026, 5, 31), date(2026, 6, 30), SNAPSHOT_DATE],
        )
        with self.assertRaisesRegex(ValueError, "calendar month-end"):
            requested_snapshot_dates(
                {"start_date": "2026-06-29", "end_date": "2026-07-31"},
                now=now,
            )


class CmcTop30NormalizationTest(unittest.TestCase):
    def test_normalize_accepts_exactly_30_and_sorts_complete_rank_set(self):
        payload = _payload()
        payload["data"].reverse()

        result = normalize_historical_listing(
            payload,
            snapshot_date=SNAPSHOT_DATE,
            collected_at=COLLECTED_AT,
        )

        self.assertEqual(len(result["rows"]), 30)
        self.assertEqual(
            [row["cmc_rank"] for row in result["rows"]], list(range(1, 31))
        )
        self.assertEqual(result["rows"][0]["symbol"], "C1")
        self.assertEqual(result["rows"][0]["price_usd"], Decimal("1.25"))
        self.assertEqual(
            result["source_available_at"],
            datetime(2026, 8, 1, 0, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(result["effective_from"], date(2026, 8, 1))
        self.assertEqual(len(result["payload_sha256"]), 64)

    def test_normalize_rejects_incomplete_or_oversized_payload(self):
        for row_count in (29, 31):
            with self.subTest(row_count=row_count):
                with self.assertRaisesRegex(ValueError, "expected exactly 30"):
                    normalize_historical_listing(
                        _payload(row_count),
                        snapshot_date=SNAPSHOT_DATE,
                        collected_at=COLLECTED_AT,
                    )

    def test_normalize_rejects_missing_and_out_of_range_rank(self):
        payload = _payload()
        payload["data"][-1]["cmc_rank"] = 31

        with self.assertRaisesRegex(ValueError, r"ranks must be 1\.\.30"):
            normalize_historical_listing(
                payload,
                snapshot_date=SNAPSHOT_DATE,
                collected_at=COLLECTED_AT,
            )

    def test_normalize_rejects_missing_identity_fields(self):
        for field, value in (
            ("id", None),
            ("cmc_rank", None),
            ("symbol", ""),
            ("name", ""),
            ("slug", ""),
        ):
            with self.subTest(field=field):
                payload = _payload()
                payload["data"][0][field] = value
                with self.assertRaisesRegex(ValueError, "missing identity or rank"):
                    normalize_historical_listing(
                        payload,
                        snapshot_date=SNAPSHOT_DATE,
                        collected_at=COLLECTED_AT,
                    )

    def test_normalize_rejects_duplicate_identity_or_rank(self):
        for field, replacement, error in (
            ("id", 10_001, "Duplicate CMC id"),
            ("cmc_rank", 1, "Duplicate CMC rank"),
        ):
            with self.subTest(field=field):
                payload = _payload()
                payload["data"][1][field] = replacement
                with self.assertRaisesRegex(ValueError, error):
                    normalize_historical_listing(
                        payload,
                        snapshot_date=SNAPSHOT_DATE,
                        collected_at=COLLECTED_AT,
                    )

    def test_normalize_rejects_cmc_error_payload(self):
        payload = _payload()
        payload["status"].update(
            {"error_code": 1006, "error_message": "key invalid"}
        )

        with self.assertRaisesRegex(ValueError, "CMC API error"):
            normalize_historical_listing(
                payload,
                snapshot_date=SNAPSHOT_DATE,
                collected_at=COLLECTED_AT,
            )


class CmcTop30StoreTest(unittest.TestCase):
    def test_missing_dates_skip_only_complete_30_row_hashed_snapshots(self):
        requested = [date(2026, 6, 30), SNAPSHOT_DATE]
        conn = _Connection(fetchall_rows=[(requested[0],)])

        result = missing_snapshot_dates(
            conn,
            requested,
            refresh_existing=False,
        )

        self.assertEqual(result, [SNAPSHOT_DATE])
        sql, values = conn.statements[0]
        self.assertIn(DEFAULT_RUN_TABLE, sql)
        self.assertIn("row_count = %s", sql)
        self.assertIn("payload_sha256 IS NOT NULL", sql)
        self.assertEqual(values, (requested, 30))

    def test_refresh_existing_fetches_every_date_without_reading_audit_table(self):
        requested = [date(2026, 6, 30), SNAPSHOT_DATE]
        conn = _Connection(fetchall_rows=[(requested[0],), (requested[1],)])

        self.assertEqual(
            missing_snapshot_dates(conn, requested, refresh_existing=True),
            requested,
        )
        self.assertEqual(conn.statements, [])

    def test_replace_snapshot_inserts_30_rows_and_marks_audit_success(self):
        modules, execute_values_calls = _fake_psycopg2_modules()
        conn = _Connection()

        with patch.dict(sys.modules, modules):
            replace_snapshot(conn, _normalized())

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(conn.rollback_count, 0)
        delete_sql, delete_values = conn.statements[0]
        self.assertIn(f"DELETE FROM {DEFAULT_SNAPSHOT_TABLE}", delete_sql)
        self.assertEqual(delete_values, (SNAPSHOT_DATE,))
        self.assertEqual(len(execute_values_calls), 1)
        _, insert_sql, inserted_values, page_size = execute_values_calls[0]
        self.assertIn(DEFAULT_SNAPSHOT_TABLE, insert_sql)
        self.assertEqual(len(inserted_values), 30)
        self.assertEqual(page_size, 30)
        update_sql, update_values = conn.statements[-1]
        self.assertIn(f"UPDATE {DEFAULT_RUN_TABLE}", update_sql)
        self.assertIn("status = 'success'", update_sql)
        self.assertEqual(update_values[2], 30)
        self.assertEqual(update_values[-1], SNAPSHOT_DATE)

    def test_replace_snapshot_rejects_non_30_row_input_before_writing(self):
        modules, _ = _fake_psycopg2_modules()
        conn = _Connection()
        normalized = _normalized()
        normalized["rows"] = normalized["rows"][:-1]

        with patch.dict(sys.modules, modules):
            with self.assertRaisesRegex(ValueError, "incomplete 29-row snapshot"):
                replace_snapshot(conn, normalized)

        self.assertEqual(conn.statements, [])
        self.assertEqual(conn.commit_count, 0)
        self.assertEqual(conn.rollback_count, 0)

    def test_replace_snapshot_rolls_back_when_pending_audit_row_is_missing(self):
        modules, _ = _fake_psycopg2_modules()
        conn = _Connection(update_rowcount=0)

        with patch.dict(sys.modules, modules):
            with self.assertRaisesRegex(RuntimeError, "Missing pending audit row"):
                replace_snapshot(conn, _normalized())

        self.assertEqual(conn.commit_count, 0)
        self.assertEqual(conn.rollback_count, 1)
        self.assertTrue(any("DELETE FROM" in sql for sql, _ in conn.statements))

    def test_failed_refresh_preserves_snapshot_and_completeness_metadata(self):
        conn = _Connection()

        mark_snapshot_failed(conn, SNAPSHOT_DATE, RuntimeError("temporary API error"))

        self.assertEqual(conn.commit_count, 1)
        self.assertEqual(conn.rollback_count, 0)
        self.assertEqual(len(conn.statements), 1)
        sql, values = conn.statements[0]
        self.assertIn(f"INSERT INTO {DEFAULT_RUN_TABLE}", sql)
        self.assertIn("ON CONFLICT (snapshot_date) DO UPDATE", sql)
        self.assertIn("status = 'failed'", sql)
        self.assertNotIn("DELETE FROM", sql)
        self.assertNotIn("row_count", sql)
        self.assertNotIn("payload_sha256", sql)
        self.assertEqual(values[-1], "temporary API error")


AIRFLOW_RUNTIME_AVAILABLE = importlib.util.find_spec("airflow") is not None


class CmcTop30DagTest(unittest.TestCase):
    def test_dag_source_uses_top30_public_contract(self):
        dag_path = DAGS_ROOT / "crypto_dags" / "cmc_top30_universe_dag.py"
        source = dag_path.read_text(encoding="utf-8")

        self.assertIn(
            'dag_id="sync_cmc_top30_point_in_time_universe_dag"', source
        )
        self.assertIn(
            'load_yaml_config("crypto_configs/cmc_top30_universe.yml")', source
        )
        self.assertIn(
            'schedule_interval=CONFIG.get("schedule", "15 1 1 * *")', source
        )
        self.assertIn("catchup=False", source)
        self.assertIn("max_active_runs=1", source)
        self.assertIn('"retries": 0', source)

    @unittest.skipUnless(AIRFLOW_RUNTIME_AVAILABLE, "Airflow is not installed")
    def test_dagbag_import_and_runtime_contract(self):
        with tempfile.TemporaryDirectory(prefix="cmc-top30-airflow-") as temp_dir:
            airflow_home = Path(temp_dir) / "airflow"
            environment = {
                "AIRFLOW_HOME": str(airflow_home),
                "AIRFLOW__LOGGING__BASE_LOG_FOLDER": str(airflow_home / "logs"),
                "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            }
            with patch.dict(os.environ, environment):
                from airflow.models import DagBag

                dag_path = (
                    DAGS_ROOT / "crypto_dags" / "cmc_top30_universe_dag.py"
                )
                dag_bag = DagBag(dag_folder=str(dag_path), include_examples=False)

        self.assertEqual(dag_bag.import_errors, {})
        dag = dag_bag.dags.get("sync_cmc_top30_point_in_time_universe_dag")
        self.assertIsNotNone(dag)
        self.assertEqual(dag.schedule_interval, "15 1 1 * *")
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(dag.default_args["retries"], 0)
        self.assertEqual(
            set(dag.task_ids),
            {"select_dates", "fetch_and_load", "trigger_asset_mapping_sync"},
        )


if __name__ == "__main__":
    unittest.main()
