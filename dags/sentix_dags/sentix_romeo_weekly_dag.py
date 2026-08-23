from __future__ import annotations

import html
import logging
from datetime import date, datetime, timedelta
from typing import Any

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email

from plugins.keynum_sentix import loader, publish
from plugins.keynum_sentix.config import (
    Settings,
    credentials_and_url_from_connection,
)
from plugins.utils.config_loader import load_yaml_config


CONFIG = load_yaml_config("sentix_configs/sentix_romeo.yml")["sentix_romeo"]
DB_CONFIG = CONFIG["db"]
API_CONFIG = CONFIG["api"]
TIMEZONE = CONFIG.get("timezone", "Europe/Berlin")
SETTINGS = Settings.from_config(CONFIG)


def _json_safe(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def sentix_failure_email(context: dict) -> None:
    """Send a credential-safe Airflow failure notification."""
    variable_name = CONFIG["alert_email_variable"]
    raw_recipients = Variable.get(variable_name, default_var="[]", deserialize_json=True)
    if isinstance(raw_recipients, str):
        recipients = [item.strip() for item in raw_recipients.split(",") if item.strip()]
    else:
        recipients = [str(item).strip() for item in raw_recipients if str(item).strip()]
    if not recipients:
        logging.error(
            "Cannot send Sentix failure email: Airflow Variable %s is empty",
            variable_name,
        )
        return
    task_instance = context.get("task_instance")
    dag_id = getattr(task_instance, "dag_id", "sync_sentix_romeo_weekly_dag")
    task_id = getattr(task_instance, "task_id", "unknown")
    run_id = context.get("run_id") or getattr(task_instance, "run_id", "unknown")
    log_url = getattr(task_instance, "log_url", "")
    subject = f"[Airflow] Sentix/Romeo failure: {dag_id}.{task_id}"
    body = (
        "<p>The Sentix/Romeo pipeline failed after retries.</p>"
        f"<p><b>DAG:</b> {html.escape(str(dag_id))}<br>"
        f"<b>Task:</b> {html.escape(str(task_id))}<br>"
        f"<b>Run:</b> {html.escape(str(run_id))}</p>"
    )
    if log_url:
        escaped_url = html.escape(str(log_url), quote=True)
        body += f'<p><a href="{escaped_url}">Open Airflow task log</a></p>'
    send_email(to=recipients, subject=subject, html_content=body)


with DAG(
    dag_id="sync_sentix_romeo_weekly_dag",
    description="Load weekly Sentix surveys and publish unlagged Romeo Variant A",
    default_args={
        "owner": "data-platform",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(minutes=30),
        "email_on_failure": False,
        "on_failure_callback": sentix_failure_email,
    },
    schedule=CONFIG.get("schedule", "0 20 * * 0"),
    start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["sentix", "sentiment", "romeo", "nordlb", "weekly"],
) as dag:

    @task
    def load_sentix() -> dict:
        context = get_current_context()
        dag_run = context.get("dag_run")
        mode = str((dag_run.conf or {}).get("mode", "incremental")) if dag_run else "incremental"
        if mode not in {"incremental", "backfill"}:
            raise ValueError("dag_run.conf.mode must be 'incremental' or 'backfill'")

        api_connection = BaseHook.get_connection(API_CONFIG["conn_id"])
        credentials, api_url = credentials_and_url_from_connection(
            api_connection,
            API_CONFIG["endpoint"],
        )
        settings = SETTINGS.with_api_url(api_url)
        hook = PostgresHook(postgres_conn_id=DB_CONFIG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            if mode == "backfill":
                result = loader.backfill(conn, hook.get_conn, credentials, settings)
            else:
                result = loader.incremental(conn, hook.get_conn, credentials, settings)
            logging.info("Sentix load result: %s", result)
            return _json_safe(result)
        finally:
            conn.close()

    @task
    def validate_sentix(_load_result: dict) -> dict:
        hook = PostgresHook(postgres_conn_id=DB_CONFIG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            as_of_date = pendulum.now(TIMEZONE).date()
            result = loader.validate_loaded_data(conn, SETTINGS, as_of_date)
            logging.info("Sentix validation result: %s", result)
            return _json_safe(result)
        finally:
            conn.close()

    @task
    def publish_romeo(_validation_result: dict) -> dict:
        hook = PostgresHook(postgres_conn_id=DB_CONFIG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            result = publish.publish(conn, hook.get_conn, SETTINGS)
            logging.info("Romeo publication result: %s", result)
            return _json_safe(result)
        finally:
            conn.close()

    @task
    def validate_signal(_publish_result: dict) -> dict:
        hook = PostgresHook(postgres_conn_id=DB_CONFIG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            result = publish.validate_published_signal(conn, SETTINGS)
            logging.info("Romeo signal validation result: %s", result)
            return _json_safe(result)
        finally:
            conn.close()

    loaded = load_sentix()
    checked = validate_sentix(loaded)
    published = publish_romeo(checked)
    validate_signal(published)
