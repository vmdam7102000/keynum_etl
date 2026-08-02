from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

from plugins.utils.api_utils import request_json

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PGConnection


Converter = Callable[[Any], Any]


def parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).date()
        except ValueError:
            return None
    return None


def coerce_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def coerce_int(value: Any) -> Optional[int]:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        return int(Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError):
        return None


SECTION_SPECS: Dict[str, Sequence[Tuple[str, str, Converter]]] = {
    "Highlights": (
        ("MarketCapitalization", "market_capitalization", coerce_int),
        ("MarketCapitalizationMln", "market_capitalization_mln", coerce_decimal),
        ("EBITDA", "ebitda", coerce_int),
        ("PERatio", "pe_ratio", coerce_decimal),
        ("PEGRatio", "peg_ratio", coerce_decimal),
        ("WallStreetTargetPrice", "wall_street_target_price", coerce_decimal),
        ("BookValue", "book_value", coerce_decimal),
        ("DividendShare", "dividend_share", coerce_decimal),
        ("DividendYield", "dividend_yield", coerce_decimal),
        ("EarningsShare", "earnings_share", coerce_decimal),
        ("EPSEstimateCurrentYear", "eps_estimate_current_year", coerce_decimal),
        ("EPSEstimateNextYear", "eps_estimate_next_year", coerce_decimal),
        ("EPSEstimateNextQuarter", "eps_estimate_next_quarter", coerce_decimal),
        ("EPSEstimateCurrentQuarter", "eps_estimate_current_quarter", coerce_decimal),
        ("MostRecentQuarter", "most_recent_quarter", parse_date),
        ("ProfitMargin", "profit_margin", coerce_decimal),
        ("OperatingMarginTTM", "operating_margin_ttm", coerce_decimal),
        ("ReturnOnAssetsTTM", "return_on_assets_ttm", coerce_decimal),
        ("ReturnOnEquityTTM", "return_on_equity_ttm", coerce_decimal),
        ("RevenueTTM", "revenue_ttm", coerce_int),
        ("RevenuePerShareTTM", "revenue_per_share_ttm", coerce_decimal),
        ("QuarterlyRevenueGrowthYOY", "quarterly_revenue_growth_yoy", coerce_decimal),
        ("GrossProfitTTM", "gross_profit_ttm", coerce_int),
        ("DilutedEpsTTM", "diluted_eps_ttm", coerce_decimal),
        ("QuarterlyEarningsGrowthYOY", "quarterly_earnings_growth_yoy", coerce_decimal),
    ),
    "Valuation": (
        ("TrailingPE", "trailing_pe", coerce_decimal),
        ("ForwardPE", "forward_pe", coerce_decimal),
        ("PriceSalesTTM", "price_sales_ttm", coerce_decimal),
        ("PriceBookMRQ", "price_book_mrq", coerce_decimal),
        ("EnterpriseValue", "enterprise_value", coerce_int),
        ("EnterpriseValueRevenue", "enterprise_value_revenue", coerce_decimal),
        ("EnterpriseValueEbitda", "enterprise_value_ebitda", coerce_decimal),
    ),
    "SharesStats": (
        ("SharesOutstanding", "shares_outstanding", coerce_int),
        ("SharesFloat", "shares_float", coerce_int),
        ("PercentInsiders", "percent_insiders", coerce_decimal),
        ("PercentInstitutions", "percent_institutions", coerce_decimal),
        ("SharesShort", "shares_short", coerce_int),
        ("SharesShortPriorMonth", "shares_short_prior_month", coerce_int),
        ("ShortRatio", "short_ratio", coerce_decimal),
        ("ShortPercentOutstanding", "short_percent_outstanding", coerce_decimal),
        ("ShortPercentFloat", "short_percent_float", coerce_decimal),
    ),
    "Technicals": (
        ("Beta", "beta", coerce_decimal),
        ("52WeekHigh", "week_52_high", coerce_decimal),
        ("52WeekLow", "week_52_low", coerce_decimal),
        ("50DayMA", "ma_50_day", coerce_decimal),
        ("200DayMA", "ma_200_day", coerce_decimal),
        ("SharesShort", "shares_short", coerce_int),
        ("SharesShortPriorMonth", "shares_short_prior_month", coerce_int),
        ("ShortRatio", "short_ratio", coerce_decimal),
        ("ShortPercent", "short_percent", coerce_decimal),
    ),
}

SECTION_TABLE_KEYS = {
    "Highlights": "highlights_table",
    "Valuation": "valuation_table",
    "SharesStats": "shares_stats_table",
    "Technicals": "technicals_table",
}


def build_section_values(
    section_name: str,
    payload: Any,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    return {
        target_column: converter(payload.get(source_key))
        for source_key, target_column, converter in SECTION_SPECS[section_name]
    }


def fetch_fundamentals(
    ticker: str,
    *,
    api_cfg: Mapping[str, Any],
    api_key: str,
) -> Optional[Dict[str, Any]]:
    params = {"fmt": api_cfg.get("fmt", "json")}
    if api_key:
        params["api_token"] = api_key
    payload = request_json(
        api_cfg["url"].format(ticker=ticker),
        params=params,
        timeout=api_cfg.get("timeout", 30),
        retries=api_cfg.get("retries", 3),
        backoff=api_cfg.get("backoff", 1.5),
        retry_statuses=api_cfg.get("retry_statuses"),
        fatal_statuses=api_cfg.get("fatal_statuses"),
    )
    if payload is None:
        raise RuntimeError(f"EODHD fundamentals request failed for {ticker}")
    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected fundamentals payload for {ticker}")
    if payload.get("message") and payload.get("code"):
        logging.warning("EODHD fundamentals error for %s: %s", ticker, payload["message"])
        return None
    return payload


def _upsert_section(
    cursor: Any,
    *,
    table: str,
    snapshot_id: int,
    snapshot_date: date,
    values: Mapping[str, Any],
) -> None:
    populated = {key: value for key, value in values.items() if value is not None}
    if not populated:
        return
    columns = ["snapshot_id", "date_of_data", *populated]
    parameters = [snapshot_id, snapshot_date, *populated.values()]
    placeholders = ", ".join(["%s"] * len(columns))
    update_columns = [column for column in columns if column != "snapshot_id"]
    updates = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
    cursor.execute(
        f"""
        INSERT INTO {table} ({", ".join(columns)})
        VALUES ({placeholders})
        ON CONFLICT (snapshot_id) DO UPDATE SET {updates}
        """,
        parameters,
    )


def store_fundamentals_payload(
    conn: PGConnection,
    *,
    company_id: int,
    logical_date: datetime,
    payload: Mapping[str, Any],
    db_cfg: Mapping[str, Any],
) -> int:
    general = payload.get("General")
    updated_at = general.get("UpdatedAt") if isinstance(general, dict) else None
    snapshot_date = parse_date(updated_at) or logical_date.date()
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            INSERT INTO {db_cfg["snapshot_table"]} (company_id, date_of_data, raw_json)
            VALUES (%s, %s, %s)
            ON CONFLICT (company_id, date_of_data)
            DO UPDATE SET raw_json = EXCLUDED.raw_json
            RETURNING id
            """,
            (company_id, snapshot_date, json.dumps(payload, ensure_ascii=True)),
        )
        snapshot_id = int(cursor.fetchone()[0])
        for section_name, table_key in SECTION_TABLE_KEYS.items():
            values = build_section_values(section_name, payload.get(section_name))
            _upsert_section(
                cursor,
                table=db_cfg[table_key],
                snapshot_id=snapshot_id,
                snapshot_date=snapshot_date,
                values=values,
            )
        conn.commit()
        return snapshot_id
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()


def sync_fundamentals_one(
    *,
    provider_ticker: str,
    company_id: int,
    logical_date: datetime,
    api_cfg: Mapping[str, Any],
    db_cfg: Mapping[str, Any],
    api_key: str,
    conn: PGConnection,
) -> str:
    payload = fetch_fundamentals(
        provider_ticker,
        api_cfg=api_cfg,
        api_key=api_key,
    )
    if not payload:
        return "unavailable"
    store_fundamentals_payload(
        conn,
        company_id=company_id,
        logical_date=logical_date,
        payload=payload,
        db_cfg=db_cfg,
    )
    time.sleep(api_cfg.get("throttle_seconds", 1))
    return "complete"
