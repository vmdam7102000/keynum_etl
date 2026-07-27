from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.config_loader import load_yaml_config
from plugins.utils.market_index_sync import (
    build_sync_ranges,
    fetch_market_index_records,
    upsert_market_index_records,
    utc_now_naive,
)

CONFIG = load_yaml_config("vn_stock_configs/market_index.yml")["market_index"]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
API_KEY = Variable.get(API_CFG["api_key_var"], default_var="")


with DAG(
    dag_id="sync_vn_market_index_dag",
    description="Sync Vietnam market indexes from Wifeed EOD API to Postgres",
    default_args={
        "owner": "vn-stock-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="15 3 * * *",  # daily at 03:15 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stock", "wifeed", "market-index"],
) as dag:

    @task
    def sync_market_indexes() -> None:
        if not API_KEY:
            raise ValueError(f"Airflow Variable {API_CFG['api_key_var']} is required")

        context = get_current_context()
        logical_date = context["logical_date"]
        dag_run = context.get("dag_run")
        run_conf: Dict[str, Any] = dict(dag_run.conf or {}) if dag_run else {}
        date_ranges = build_sync_ranges(
            logical_date=logical_date,
            run_conf=run_conf,
            api_cfg=API_CFG,
        )

        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            for from_date, to_date in date_ranges:
                records = fetch_market_index_records(
                    from_date=from_date,
                    to_date=to_date,
                    api_cfg=API_CFG,
                    api_key=API_KEY,
                )
                upsert_market_index_records(
                    conn=conn,
                    table=DB_CFG["table"],
                    records=records,
                    ingested_at=utc_now_naive(),
                )
                logging.info(
                    "Upserted %s market-index rows for %s through %s",
                    len(records),
                    from_date,
                    to_date,
                )
        finally:
            conn.close()

    sync_market_indexes()
