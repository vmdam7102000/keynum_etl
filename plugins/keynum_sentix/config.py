"""Typed configuration for the Airflow-native Sentix/Romeo pipeline."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from typing import Any, Mapping
from urllib.parse import urljoin


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class SentixCredentials:
    userid: str
    passcode: str
    token: str


@dataclass(frozen=True)
class Settings:
    observation_table: str
    series_table: str
    load_run_table: str
    signal_definition_table: str
    signal_observation_table: str
    signal_run_table: str
    sentix_health_view: str
    signal_latest_view: str
    signal_health_view: str
    api_url: str = "https://api.sentix.de/datadownload/remote_data.php"
    api_timeout_seconds: int = 180
    incremental_overlap_days: int = 21
    backfill_start_date: str = "2000-01-01"
    batch_size: int = 5_000
    minimum_codes: int = 700
    minimum_code_retention_ratio: float = 0.95
    stale_after_days: int = 10
    unchanged_sentiment_weeks: int = 3

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> "Settings":
        db = config["db"]
        load = config.get("load", {})
        health = config.get("health", {})
        api = config.get("api", {})
        return cls(
            observation_table=db["observation_table"],
            series_table=db["series_table"],
            load_run_table=db["load_run_table"],
            signal_definition_table=db["signal_definition_table"],
            signal_observation_table=db["signal_observation_table"],
            signal_run_table=db["signal_run_table"],
            sentix_health_view=db["sentix_health_view"],
            signal_latest_view=db["signal_latest_view"],
            signal_health_view=db["signal_health_view"],
            api_timeout_seconds=int(api.get("timeout_seconds", 180)),
            incremental_overlap_days=int(load.get("incremental_overlap_days", 21)),
            backfill_start_date=str(load.get("backfill_start_date", "2000-01-01")),
            batch_size=int(load.get("batch_size", 5_000)),
            minimum_codes=int(health.get("minimum_codes", 700)),
            minimum_code_retention_ratio=float(
                health.get("minimum_code_retention_ratio", 0.95)
            ),
            stale_after_days=int(health.get("stale_after_days", 10)),
            unchanged_sentiment_weeks=int(
                health.get("unchanged_sentiment_weeks", 3)
            ),
        )

    def with_api_url(self, api_url: str) -> "Settings":
        return replace(self, api_url=api_url)

    def parsed_backfill_start_date(self) -> date:
        return date.fromisoformat(self.backfill_start_date)


def credentials_and_url_from_connection(
    connection: Any,
    endpoint: str,
) -> tuple[SentixCredentials, str]:
    """Build vendor credentials without importing Airflow into testable modules."""
    token = (connection.extra_dejson or {}).get("token")
    if not connection.login or not connection.password or not token:
        raise ConfigError(
            "Airflow connection requires login, password, and extra.token"
        )
    host = (connection.host or "").strip()
    if not host:
        raise ConfigError("Airflow connection host is required")
    if not host.startswith(("http://", "https://")):
        scheme = connection.schema or "https"
        host = f"{scheme}://{host}"
    return (
        SentixCredentials(connection.login, connection.password, str(token)),
        urljoin(host.rstrip("/") + "/", endpoint.lstrip("/")),
    )

