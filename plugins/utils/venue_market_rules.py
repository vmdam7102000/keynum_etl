from __future__ import annotations

import json
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


_PRECISION_MODE_NAMES = {
    2: "decimal_places",
    3: "significant_digits",
    4: "tick_size",
}


def normalize_quote(value: Any = None) -> str:
    """Return a normalized quote asset, defaulting to USDT."""
    quote = str(value or "USDT").strip().upper()
    return quote or "USDT"


def parse_selection(value: Any, *, upper: bool = False) -> List[str]:
    """Normalize a comma-delimited string or a list from dag_run.conf."""
    if value is None:
        return []
    values = value.split(",") if isinstance(value, str) else value
    if not isinstance(values, (list, tuple, set)):
        raise ValueError("Selection must be a comma-delimited string or a list")

    normalized = []
    for item in values:
        text = str(item or "").strip()
        if text:
            normalized.append(text.upper() if upper else text.lower())
    return sorted(set(normalized))


def build_venue_targets(
    rows: Iterable[Tuple[Any, Any]],
    *,
    selected_venues: Sequence[str] = (),
    selected_symbols: Sequence[str] = (),
) -> List[Dict[str, Any]]:
    """Turn metadata rows into one sorted target payload per venue."""
    venue_filter = {str(venue).strip().lower() for venue in selected_venues}
    symbol_filter = {str(symbol).strip().upper() for symbol in selected_symbols}
    targets: Dict[str, set[str]] = {}

    for symbol, available_exchange in rows:
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol or (symbol_filter and normalized_symbol not in symbol_filter):
            continue
        for raw_venue in str(available_exchange or "").split(","):
            venue = raw_venue.strip().lower()
            if not venue or (venue_filter and venue not in venue_filter):
                continue
            targets.setdefault(venue, set()).add(normalized_symbol)

    return [
        {"venue": venue, "asset_symbols": sorted(symbols)}
        for venue, symbols in sorted(targets.items())
    ]


def _as_decimal(value: Any) -> Optional[Decimal]:
    if value is None or isinstance(value, bool):
        return None
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return decimal if decimal.is_finite() else None


def precision_mode_name(value: Any) -> str:
    if isinstance(value, str):
        normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
        aliases = {
            "decimal_places": "decimal_places",
            "significant_digits": "significant_digits",
            "tick_size": "tick_size",
        }
        if normalized in aliases:
            return aliases[normalized]
    return _PRECISION_MODE_NAMES.get(value, "unknown")


def precision_to_step(value: Any, precision_mode: Any) -> Optional[Decimal]:
    """Return a physical increment only when CCXT precision defines one."""
    mode = precision_mode_name(precision_mode)
    precision = _as_decimal(value)
    if precision is None:
        return None
    if mode == "tick_size":
        return precision
    if mode == "decimal_places":
        if precision != precision.to_integral_value() or precision < 0:
            return None
        return Decimal(1).scaleb(-int(precision))
    return None


def resolve_market(
    markets: Mapping[str, Mapping[str, Any]],
    *,
    asset_symbol: str,
    quote: str,
    override: Optional[str] = None,
) -> Tuple[Optional[Mapping[str, Any]], Optional[str]]:
    """Resolve one configured asset to one CCXT spot market without guessing."""
    expected_symbol = f"{asset_symbol}/{quote}"
    if override:
        candidate = markets.get(override)
        if candidate is None:
            candidate = next(
                (market for market in markets.values() if market.get("id") == override),
                None,
            )
        if candidate is None:
            return None, "override_not_found"
        if candidate.get("spot") is not True:
            return None, "override_not_spot"
        if candidate.get("base") != asset_symbol or candidate.get("quote") != quote:
            return None, "override_not_expected_pair"
        return candidate, None

    direct = markets.get(expected_symbol)
    if direct and direct.get("spot") is True:
        return direct, None

    candidates = [
        market
        for market in markets.values()
        if market.get("base") == asset_symbol
        and market.get("quote") == quote
        and market.get("spot") is True
    ]
    active_candidates = [market for market in candidates if market.get("active") is True]
    if len(active_candidates) == 1:
        return active_candidates[0], None
    if len(candidates) == 1:
        return candidates[0], None
    if len(candidates) > 1:
        return None, "ambiguous_market"
    return None, "market_not_found"


def resolve_market_with_fallback(
    markets: Mapping[str, Mapping[str, Any]],
    *,
    asset_symbol: str,
    primary_quote: str,
    fallback_quote: str,
    override: Optional[str] = None,
) -> Tuple[Optional[Mapping[str, Any]], Optional[str], Optional[str]]:
    """Resolve the preferred quote, then fall back only when it is absent."""
    market, reason = resolve_market(
        markets,
        asset_symbol=asset_symbol,
        quote=primary_quote,
        override=override,
    )
    if market is not None:
        return market, primary_quote, None
    if reason != "market_not_found" or fallback_quote == primary_quote:
        return None, None, reason

    market, fallback_reason = resolve_market(
        markets,
        asset_symbol=asset_symbol,
        quote=fallback_quote,
    )
    if market is not None:
        return market, fallback_quote, None
    return None, None, f"{primary_quote.lower()}_not_found; {fallback_quote.lower()}_{fallback_reason}"


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str, allow_nan=False))


def build_snapshot_record(
    *,
    venue: str,
    asset_symbol: str,
    quote: str,
    market: Mapping[str, Any],
    precision_mode: Any,
    captured_at: date,
) -> Dict[str, Any]:
    """Normalize one CCXT market into a DB-ready snapshot record."""
    expected_symbol = f"{asset_symbol}/{quote}"
    mode_name = precision_mode_name(precision_mode)
    precision = market.get("precision") or {}
    limits = market.get("limits") or {}
    amount_limits = limits.get("amount") or {}
    cost_limits = limits.get("cost") or {}
    return {
        "venue": venue,
        "market_symbol": str(market.get("symbol") or expected_symbol),
        "asset_symbol": asset_symbol,
        "base_asset": market.get("base") or asset_symbol,
        "quote_asset": market.get("quote") or quote,
        "active": market.get("active"),
        "amount_step": precision_to_step(precision.get("amount"), precision_mode),
        "price_tick": precision_to_step(precision.get("price"), precision_mode),
        "min_amount": _as_decimal(amount_limits.get("min")),
        "max_amount": _as_decimal(amount_limits.get("max")),
        "min_notional": _as_decimal(cost_limits.get("min")),
        "max_notional": _as_decimal(cost_limits.get("max")),
        "precision_mode": mode_name,
        "raw_info": _json_safe(
            {"ccxt_market": market, "venue_info": market.get("info")}
        ),
        "captured_at": captured_at,
    }


def snapshot_upsert_sql(table_name: str) -> str:
    return f"""
    INSERT INTO {table_name} (
        venue, market_symbol, asset_symbol, base_asset, quote_asset,
        active, amount_step, price_tick, min_amount, max_amount, min_notional,
        max_notional, precision_mode, raw_info, captured_at
    ) VALUES %s
    ON CONFLICT (captured_at, venue, market_symbol) DO UPDATE SET
        asset_symbol = EXCLUDED.asset_symbol,
        base_asset = EXCLUDED.base_asset,
        quote_asset = EXCLUDED.quote_asset,
        active = EXCLUDED.active,
        amount_step = EXCLUDED.amount_step,
        price_tick = EXCLUDED.price_tick,
        min_amount = EXCLUDED.min_amount,
        max_amount = EXCLUDED.max_amount,
        min_notional = EXCLUDED.min_notional,
        max_notional = EXCLUDED.max_notional,
        precision_mode = EXCLUDED.precision_mode,
        raw_info = EXCLUDED.raw_info
    """
