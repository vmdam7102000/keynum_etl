"""Client for the Sentix bulk download endpoint.

Vendor contract (reverse-engineered from the legacy loader and verified live on
2026-08-04 -- Sentix publish no formal API documentation that we hold):

    POST https://api.sentix.de/datadownload/remote_data.php
    Content-Type: application/x-www-form-urlencoded

    userid          account name
    passcode        account password
    token           API token
    startdate       YYYY-MM-DD, inclusive
    datacode        'ALL' for every subscribed indicator
    outputformat    'CSV'
    fielddelimiter  ','
    dateformat      'YYYY-MM-DD'
    sortorder       'ASC'

    200 OK, body is CSV:

        Code,Date,Indicator Value
        SNTBATH6,2026-07-24,0.0465
        SNTBATH6,2026-07-31,0.2571

There is no `enddate`, no pagination, and no metadata/catalogue endpoint -- you
get every observation from `startdate` to the present in one response. A full
history request returns ~692k rows (~20 MB). That is small enough to hold in
memory, which is why this module returns a DataFrame rather than streaming.

The endpoint returns HTTP 200 for authentication failures as well, with an error
message in the body instead of CSV, so `parse_response` validates the header
rather than trusting the status code.
"""
from __future__ import annotations

import logging
from datetime import date
from io import StringIO

import pandas as pd
import requests

from .config import SentixCredentials, Settings

log = logging.getLogger(__name__)

EXPECTED_HEADER = ["Code", "Date", "Indicator Value"]
COLUMNS = ["code", "obs_date", "value"]


class SentixApiError(RuntimeError):
    pass


def fetch_csv(
    credentials: SentixCredentials,
    start_date: date | str,
    settings: Settings | None = None,
    session: requests.Session | None = None,
) -> str:
    """Return the raw CSV body for all observations from `start_date` onward."""
    if settings is None:
        raise ValueError("settings is required in the Airflow integration")
    start = start_date.isoformat() if isinstance(start_date, date) else str(start_date)

    payload = {
        "userid": credentials.userid,
        "passcode": credentials.passcode,
        "token": credentials.token,
        "startdate": start,
        "fielddelimiter": ",",
        "dateformat": "YYYY-MM-DD",
        "sortorder": "ASC",
        "outputformat": "CSV",
        "datacode": "ALL",
    }

    log.info("Requesting Sentix data from %s", start)
    http = session or requests
    try:
        response = http.post(
            settings.api_url, data=payload, timeout=settings.api_timeout_seconds
        )
    except requests.RequestException as exc:
        raise SentixApiError(f"Sentix request failed: {exc}") from exc

    if response.status_code != 200:
        raise SentixApiError(
            f"Sentix returned HTTP {response.status_code}: {response.text[:500]}"
        )

    log.info("Received %d bytes", len(response.text))
    return response.text


def parse_response(csv_text: str) -> pd.DataFrame:
    """Parse a vendor CSV body into a validated DataFrame.

    Returns columns ['code', 'obs_date', 'value'] with obs_date as date objects.
    Raises SentixApiError if the body is not the expected CSV -- which is how an
    auth failure presents, since the endpoint still answers 200.
    """
    stripped = csv_text.strip()
    if not stripped:
        raise SentixApiError("Sentix returned an empty body")

    first_line = stripped.splitlines()[0]
    header = [c.strip() for c in first_line.split(",")]
    if header != EXPECTED_HEADER:
        raise SentixApiError(
            "Unexpected response -- this is how an authentication failure or a "
            f"vendor format change presents. First line was: {first_line[:300]!r}"
        )

    frame = pd.read_csv(StringIO(stripped))
    frame.columns = COLUMNS

    frame["code"] = frame["code"].astype(str).str.strip()
    frame["obs_date"] = pd.to_datetime(frame["obs_date"], format="%Y-%m-%d").dt.date
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")

    bad = frame["value"].isna().sum()
    if bad:
        # The legacy table declared value NOT NULL, so unparseable values were
        # never stored. Drop and report rather than fail the whole load.
        log.warning("Dropping %d rows with non-numeric values", bad)
        frame = frame.dropna(subset=["value"])

    if frame.empty:
        raise SentixApiError("Sentix returned a header but no usable data rows")

    duplicates = frame.duplicated(subset=["code", "obs_date"]).sum()
    if duplicates:
        log.warning("Vendor returned %d duplicate (code, date) pairs; keeping last", duplicates)
        frame = frame.drop_duplicates(subset=["code", "obs_date"], keep="last")

    return frame.reset_index(drop=True)


def fetch(
    credentials: SentixCredentials,
    start_date: date | str,
    settings: Settings | None = None,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """fetch_csv + parse_response."""
    return parse_response(fetch_csv(credentials, start_date, settings, session))
