# plugins/utils/api_utils.py
"""
Utility helpers for making HTTP requests to external APIs.
Centralizes retry/backoff logic so DAGs & operators reuse a single entry point.
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Sequence, Union

import requests

JsonType = Union[Dict[str, Any], List[Any]]
_SENSITIVE_PARAM_NAMES = {"apikey", "api_key", "api_token", "access_token", "token"}
_SENSITIVE_QUERY_VALUE = re.compile(
    r"(?i)(apikey|api_key|api_token|access_token|token)=([^&\s]+)"
)


def _redact_sensitive_params(params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return request parameters that are safe to include in application logs."""
    if params is None:
        return None
    return {
        key: "***" if key.lower() in _SENSITIVE_PARAM_NAMES else value
        for key, value in params.items()
    }


def _redact_sensitive_text(value: Any) -> str:
    """Redact credentials embedded in a URL or an exception message."""
    return _SENSITIVE_QUERY_VALUE.sub(r"\1=***", str(value))


def request_json(
    url: str,
    *,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    json: Any = None,
    timeout: int | float = 30,
    retries: int = 3,
    backoff: float = 1.5,
    retry_statuses: Optional[Sequence[int]] = None,
    fatal_statuses: Optional[Sequence[int]] = None,
    session: Optional[requests.Session] = None,
) -> Optional[JsonType]:
    """
    Make an HTTP request and parse JSON with simple retries/backoff.

    Returns the decoded JSON on success, otherwise None after exhausting retries.
    """
    http = session or requests.Session()
    last_exc: Exception | None = None
    safe_params = _redact_sensitive_params(params)
    safe_url = _redact_sensitive_text(url)

    for attempt in range(1, retries + 1):
        try:
            resp = http.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                json=json,
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            last_exc = exc
            resp = exc.response
            status_code = resp.status_code if resp is not None else None

            if fatal_statuses and status_code in fatal_statuses:
                logging.error(
                    "Fatal API response (%s): %s %s params=%s",
                    status_code,
                    method,
                    safe_url,
                    safe_params,
                )
                raise

            retry_after = None
            retry_on = set(retry_statuses or ())
            if status_code == 429 or status_code in retry_on:
                retry_after = _parse_retry_after(
                    resp.headers.get("Retry-After") if resp is not None else None
                )

            logging.warning(
                "API request failed (%s/%s): %s %s params=%s error=%s",
                attempt,
                retries,
                method,
                safe_url,
                safe_params,
                _redact_sensitive_text(exc),
            )
            if attempt < retries:
                sleep_time = backoff ** (attempt - 1)
                if retry_after:
                    sleep_time = max(sleep_time, retry_after)
                time.sleep(sleep_time)
        except Exception as exc:
            last_exc = exc
            logging.warning(
                "API request failed (%s/%s): %s %s params=%s error=%s",
                attempt,
                retries,
                method,
                safe_url,
                safe_params,
                _redact_sensitive_text(exc),
            )
            if attempt < retries:
                sleep_time = backoff ** (attempt - 1)
                time.sleep(sleep_time)

    if last_exc:
        logging.error(
            "All retries failed for %s %s: %s",
            method,
            safe_url,
            _redact_sensitive_text(last_exc),
        )

    return None
