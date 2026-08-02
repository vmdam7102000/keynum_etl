from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.config_loader import load_yaml_config
from plugins.utils.sp500_historical_backfill import (
    build_backfill_jobs,
    chunk_jobs,
    deserialize_job,
    load_backfill_rows,
    normalize_backfill_conf,
    run_backfill_batch,
    serialize_job,
)


MEMBERSHIP_CONFIG = load_yaml_config(
    "sp500_stock_configs/sp500_membership.yml"
)["sp500_membership"]
PRICE_CONFIG = load_yaml_config("sp500_stock_configs/sp500_eod_prices.yml")[
    "sp500_eod_prices"
]
FUNDAMENTALS_CONFIG = load_yaml_config(
    "sp500_stock_configs/sp500_fundamentals.yml"
)["sp500_fundamentals"]
MEMBERSHIP_DB = MEMBERSHIP_CONFIG["db"]
PRICE_DB = PRICE_CONFIG["db"]
FUNDAMENTALS_DB = FUNDAMENTALS_CONFIG["db"]
API_KEY = Variable.get(PRICE_CONFIG["api"]["api_key_var"], default_var="")
EODHD_POOL = Variable.get("eodhd_airflow_pool", default_var="default_pool")

BACKFILL_DB: Dict[str, Any] = {
    **MEMBERSHIP_DB,
    **FUNDAMENTALS_DB,
    "price_table": PRICE_DB["price_table"],
    "price_columns": PRICE_DB["columns"],
    "price_conflict_keys": PRICE_DB["conflict_keys"],
}


with DAG(
    dag_id="backfill_sp500_historical_data_dag",
    description="Backfill S&P 500 historical EOD prices and fundamentals",
    default_args={
        "owner": "sp500-stock-data",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "pool": EODHD_POOL,
    },
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stock", "sp500", "backfill", "point-in-time", "eodhd"],
) as dag:

    @task
    def prepare_batches() -> List[List[Dict[str, Any]]]:
        if not API_KEY:
            raise ValueError(
                f"Airflow Variable {PRICE_CONFIG['api']['api_key_var']} is required"
            )
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = normalize_backfill_conf(dict(dag_run.conf or {}) if dag_run else {})
        hook = PostgresHook(postgres_conn_id=MEMBERSHIP_DB["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            rows = load_backfill_rows(
                conn,
                membership_table=MEMBERSHIP_DB["membership_table"],
                mapping_table=MEMBERSHIP_DB["mapping_table"],
                conf=conf,
            )
        finally:
            conn.close()
        jobs = build_backfill_jobs(
            rows,
            logical_date=context["logical_date"].date(),
            from_date=conf["from_date"],
            to_date=conf["to_date"],
        )
        batches = chunk_jobs(jobs, conf["batch_size"])
        logging.info(
            "Prepared %s S&P 500 backfill jobs in %s batches",
            len(jobs),
            len(batches),
        )
        return [[serialize_job(job) for job in batch] for batch in batches]

    @task(pool=EODHD_POOL)
    def run_batch(job_payloads: List[Dict[str, Any]]) -> Dict[str, int]:
        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = normalize_backfill_conf(dict(dag_run.conf or {}) if dag_run else {})
        jobs = [deserialize_job(payload) for payload in job_payloads]
        hook = PostgresHook(postgres_conn_id=MEMBERSHIP_DB["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            result = run_backfill_batch(
                conn,
                jobs=jobs,
                logical_date=context["logical_date"],
                include_prices=conf["include_prices"],
                include_fundamentals=conf["include_fundamentals"],
                force=conf["mode"] == "force",
                price_api_cfg=PRICE_CONFIG["api"],
                fundamentals_api_cfg=FUNDAMENTALS_CONFIG["api"],
                db_cfg=BACKFILL_DB,
                api_key=API_KEY,
                backfill_cfg=MEMBERSHIP_CONFIG["backfill"],
            )
            logging.info("Backfill batch result: %s", result)
            return result
        finally:
            conn.close()

    @task(trigger_rule="all_done")
    def report_coverage() -> Dict[str, int]:
        hook = PostgresHook(postgres_conn_id=MEMBERSHIP_DB["postgres_conn_id"])
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT
                    mapping_status,
                    price_backfill_status,
                    fundamentals_backfill_status,
                    COUNT(*)
                FROM {MEMBERSHIP_DB["mapping_table"]}
                WHERE provider = 'EODHD'
                GROUP BY
                    mapping_status,
                    price_backfill_status,
                    fundamentals_backfill_status
                ORDER BY 1, 2, 3
                """
            )
            report = {
                f"{mapping}:{price}:{fundamentals}": int(count)
                for mapping, price, fundamentals, count in cursor.fetchall()
            }
            logging.info("S&P 500 mapping/backfill coverage: %s", report)
            return report
        finally:
            cursor.close()
            conn.close()

    mapped_batches = run_batch.expand(job_payloads=prepare_batches())
    mapped_batches >> report_coverage()
