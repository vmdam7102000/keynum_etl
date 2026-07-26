from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.config_loader import load_yaml_config
from plugins.utils.db_utils import get_all_stock_codes
from plugins.utils.eod_price_sync import sync_vn_eod_one

CONFIG = load_yaml_config("vn_stock_configs/eod_prices.yml")["eod_prices"]
API_CFG = CONFIG["api"]
REFRESH_CFG = CONFIG["adjusted_refresh"]
DB_CFG = CONFIG["db"]
CHUNK_SIZE = 400
API_KEY = Variable.get(API_CFG["api_key_var"], default_var="")
ADJUSTED_UPDATE_COLUMNS = (
    "open_adjust",
    "high_adjust",
    "low_adjust",
    "close_adjust",
    "volume_adjust",
)


with DAG(
    dag_id="refresh_vn_stock_adjusted_prices_weekly_dag",
    description="Weekly full refresh of Vietnam-stock adjusted EOD prices from Wifeed",
    default_args={
        "owner": "vn-stock-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval=REFRESH_CFG["schedule_interval"],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stock", "wifeed", "eod", "adjusted-refresh"],
) as dag:

    @task
    def get_codes() -> List[str]:
        codes = get_all_stock_codes(
            postgres_conn_id=DB_CFG["postgres_conn_id"],
            stock_list_table=DB_CFG["stock_list_table"],
            code_column=DB_CFG.get("stock_list_code_column", "code"),
        )
        logging.info("Fetched %s stock codes from %s", len(codes), DB_CFG["stock_list_table"])
        return codes

    @task
    def chunk_codes(codes: List[str]) -> List[List[str]]:
        return [codes[index : index + CHUNK_SIZE] for index in range(0, len(codes), CHUNK_SIZE)]

    @task
    def refresh_code_batch(codes: List[str]) -> None:
        logical_date = get_current_context()["logical_date"]
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            for code in codes:
                sync_vn_eod_one(
                    code=code,
                    logical_date=logical_date,
                    lookback_days=REFRESH_CFG["lookback_days"],
                    api_cfg=API_CFG,
                    db_cfg=DB_CFG,
                    api_key=API_KEY,
                    update_columns=ADJUSTED_UPDATE_COLUMNS,
                    conn=conn,
                )
        finally:
            conn.close()

    codes = get_codes()
    code_batches = chunk_codes(codes)
    refresh_code_batch.expand(codes=code_batches)
