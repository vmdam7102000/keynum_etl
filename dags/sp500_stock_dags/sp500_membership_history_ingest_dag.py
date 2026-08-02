from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from plugins.utils.api_utils import request_text
from plugins.utils.config_loader import load_yaml_config
from plugins.utils.sp500_membership_sync import (
    download_membership_csv,
    fetch_latest_commit_sha,
    get_latest_stored_commit,
    monitor_wikipedia_current,
    parse_membership_csv,
    resolve_pending_mappings,
    sync_membership_history,
)


CONFIG = load_yaml_config("sp500_stock_configs/sp500_membership.yml")[
    "sp500_membership"
]
SOURCE_CFG = CONFIG["source"]
WIKIPEDIA_CFG = CONFIG["wikipedia"]
EODHD_CFG = CONFIG["eodhd"]
DB_CFG = CONFIG["db"]
GITHUB_TOKEN = Variable.get(SOURCE_CFG["github_token_var"], default_var="")
EODHD_API_KEY = Variable.get(EODHD_CFG["api_key_var"], default_var="")
EODHD_POOL = Variable.get("eodhd_airflow_pool", default_var="default_pool")


with DAG(
    dag_id="sync_sp500_membership_history_dag",
    description="Sync point-in-time S&P 500 membership and resolve EODHD tickers",
    default_args={
        "owner": "sp500-stock-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval=CONFIG.get("schedule", "30 1 * * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["stock", "sp500", "membership", "point-in-time"],
) as dag:

    @task
    def sync_history() -> Dict[str, Any]:
        commit_sha = fetch_latest_commit_sha(
            SOURCE_CFG,
            github_token=GITHUB_TOKEN,
        )
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            stored_commit = get_latest_stored_commit(
                conn,
                membership_table=DB_CFG["membership_table"],
                index_code="SP500",
                source_repo=SOURCE_CFG["repository"],
            )
            if stored_commit == commit_sha:
                logging.info("S&P 500 membership source unchanged at %s", commit_sha)
                return {
                    "changed": False,
                    "commit_sha": commit_sha,
                    "affected_membership_ids": [],
                    "affected_tickers": [],
                }

            csv_text = download_membership_csv(
                SOURCE_CFG,
                commit_sha=commit_sha,
                github_token=GITHUB_TOKEN,
            )
            records = parse_membership_csv(csv_text)
            result = sync_membership_history(
                conn,
                records=records,
                commit_sha=commit_sha,
                source_repo=SOURCE_CFG["repository"],
                membership_table=DB_CFG["membership_table"],
                mapping_table=DB_CFG["mapping_table"],
                minimum_rows=int(SOURCE_CFG.get("minimum_rows", 1000)),
                maximum_drop_fraction=float(
                    SOURCE_CFG.get("maximum_drop_fraction", 0.05)
                ),
            )
            quality_cutoff = SOURCE_CFG.get("quality_warning_before", "2001-01-01")
            if any(item.valid_from.isoformat() < quality_cutoff for item in records):
                logging.warning(
                    "Imported pre-%s membership rows; source documents reduced early coverage",
                    quality_cutoff,
                )
            logging.info("Membership sync result: %s", result)
            return result
        finally:
            conn.close()

    @task
    def monitor_current_universe() -> Dict[str, Any]:
        try:
            html = request_text(
                WIKIPEDIA_CFG["url"],
                headers={
                    "User-Agent": WIKIPEDIA_CFG.get(
                        "user_agent",
                        "Keynum-SP500-Airflow/1.0",
                    ),
                    "Accept": "text/html,application/xhtml+xml",
                    "Accept-Language": "en-US,en;q=0.9",
                },
                timeout=WIKIPEDIA_CFG.get("timeout", 30),
            )
            if html is None:
                logging.warning(
                    "Skipping current-universe monitoring because Wikimedia "
                    "did not return a page"
                )
                return {"status": "unavailable"}

            hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
            conn = hook.get_conn()
            try:
                result = monitor_wikipedia_current(
                    conn,
                    html=html,
                    companies_table=DB_CFG["company_table"],
                    membership_table=DB_CFG["membership_table"],
                )
            finally:
                conn.close()
            if result["added_vs_history"] or result["removed_vs_history"]:
                logging.warning("Wikimedia/GitHub membership diff: %s", result)
            else:
                logging.info("Wikimedia current universe matches open history")
            return {"status": "complete", **result}
        except Exception as exc:
            logging.exception(
                "Skipping non-blocking Wikimedia current-universe monitoring: %s",
                exc,
            )
            return {"status": "failed", "error": str(exc)[:1000]}

    @task(pool=EODHD_POOL)
    def resolve_tickers(result: Dict[str, Any]) -> Dict[str, Any]:
        if not EODHD_API_KEY:
            raise ValueError(f"Airflow Variable {EODHD_CFG['api_key_var']} is required")
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            resolution = resolve_pending_mappings(
                conn,
                api_cfg=EODHD_CFG,
                db_cfg=DB_CFG,
                api_key=EODHD_API_KEY,
                affected_membership_ids=result.get("affected_membership_ids"),
                manual_overrides=CONFIG.get("mapping", {}).get(
                    "manual_overrides", {}
                ),
            )
            logging.info("Ticker resolution result: %s", resolution)
            return resolution
        finally:
            conn.close()

    @task
    def collect_backfill_targets(
        _history: Dict[str, Any],
        resolution: Dict[str, Any],
    ) -> Dict[str, Any]:
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT membership_id
                FROM {DB_CFG["mapping_table"]}
                WHERE provider = 'EODHD'
                  AND mapping_status = 'resolved'
                  AND (
                      price_backfill_status IN ('pending', 'running', 'failed')
                      OR fundamentals_backfill_status IN ('pending', 'running', 'failed')
                  )
                ORDER BY membership_id
                """
            )
            pending_ids = {int(row[0]) for row in cursor.fetchall()}
        finally:
            cursor.close()
            conn.close()
        pending_ids.update(resolution.get("resolved_membership_ids", []))
        return {"membership_ids": sorted(pending_ids)}

    @task.short_circuit
    def has_backfill_targets(result: Dict[str, Any]) -> bool:
        return bool(result.get("membership_ids"))

    history_result = sync_history()
    monitor_result = monitor_current_universe()
    resolution_result = resolve_tickers(history_result)
    target_result = collect_backfill_targets(history_result, resolution_result)
    has_targets = has_backfill_targets(target_result)
    trigger_backfill = TriggerDagRunOperator(
        task_id="trigger_delta_backfill",
        trigger_dag_id=CONFIG["backfill"]["dag_id"],
        conf={
            "mode": "missing_only",
            "membership_ids": (
                "{{ ti.xcom_pull(task_ids='collect_backfill_targets')"
                "['membership_ids'] }}"
            ),
            "include_prices": True,
            "include_fundamentals": True,
            "batch_size": CONFIG["backfill"].get("default_batch_size", 50),
        },
        wait_for_completion=False,
    )

    history_result >> monitor_result >> resolution_result
    resolution_result >> target_result >> has_targets >> trigger_backfill
