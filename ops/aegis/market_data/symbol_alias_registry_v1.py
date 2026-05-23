from __future__ import annotations

from typing import Any


VIX_CANONICAL_SYMBOL = "VIX"
VIX_ALIASES = ("VIX", "^VIX", "$VIX", "vix", "VIXCLS")
VIX_LOCAL_CACHE_ALIASES = ("VIX", "^VIX", "vix", "VIXCLS", "$VIX")
VIX_STOOQ_ALIASES = ("^VIX", "vix")

CANONICAL_MARKET_SYMBOL_ALIASES: dict[str, tuple[str, ...]] = {
    VIX_CANONICAL_SYMBOL: VIX_ALIASES,
}

CANONICAL_MARKET_PROVIDER_ALIASES: dict[str, dict[str, tuple[str, ...]]] = {
    VIX_CANONICAL_SYMBOL: {
        "LOCAL_CACHE": VIX_LOCAL_CACHE_ALIASES,
        "CANONICAL_TRUTH": VIX_LOCAL_CACHE_ALIASES,
        "CANONICAL_MARKET_DATA_SNAPSHOT_V1": VIX_LOCAL_CACHE_ALIASES,
        "MANUAL_CSV_DROP": VIX_LOCAL_CACHE_ALIASES,
        "STOOQ": VIX_STOOQ_ALIASES,
        "YFINANCE": ("^VIX",),
        "YAHOO_CHART": ("^VIX",),
        "FRED": ("VIXCLS",),
        "CBOE": ("VIX",),
    }
}


def normalize_market_symbol_v1(symbol: Any) -> str:
    text = str(symbol or "").strip()
    if not text:
        return ""
    for canonical, aliases in CANONICAL_MARKET_SYMBOL_ALIASES.items():
        if any(text == alias or text.upper() == alias.upper() for alias in aliases):
            return canonical
    return text.upper()


def canonicalize_symbol_list_v1(symbols: list[Any] | tuple[Any, ...] | set[Any]) -> list[str]:
    return sorted({normalized for symbol in symbols if (normalized := normalize_market_symbol_v1(symbol))})


def canonical_aliases_v1(canonical_symbol: Any) -> tuple[str, ...]:
    canonical = normalize_market_symbol_v1(canonical_symbol)
    return CANONICAL_MARKET_SYMBOL_ALIASES.get(canonical, (canonical,))


def provider_symbol_candidates_v1(symbol_map: dict[str, Any], canonical_symbol: Any, provider: str) -> tuple[str, ...]:
    canonical = normalize_market_symbol_v1(canonical_symbol)
    provider_name = str(provider or "").strip().upper()
    provider_name = "LOCAL_CACHE" if provider_name in {"CANONICAL_TRUTH", "CANONICAL_MARKET_DATA_SNAPSHOT_V1"} else provider_name
    mapped = _mapped_provider_symbols(symbol_map=symbol_map, canonical_symbol=canonical, provider=provider_name)
    registry = CANONICAL_MARKET_PROVIDER_ALIASES.get(canonical, {}).get(provider_name, ())
    if provider_name == "LOCAL_CACHE":
        registry = CANONICAL_MARKET_PROVIDER_ALIASES.get(canonical, {}).get("LOCAL_CACHE", ())
    return _dedupe_candidates([*mapped, *registry])


def primary_provider_symbol_v1(symbol_map: dict[str, Any], canonical_symbol: Any, provider: str) -> str:
    candidates = provider_symbol_candidates_v1(symbol_map, canonical_symbol, provider)
    return candidates[0] if candidates else ""


def symbol_matches_canonical_v1(symbol: Any, canonical_symbol: Any) -> bool:
    canonical = normalize_market_symbol_v1(canonical_symbol)
    return normalize_market_symbol_v1(symbol) == canonical


def market_data_item_id_v1(symbol: Any) -> str:
    canonical = normalize_market_symbol_v1(symbol)
    if canonical == VIX_CANONICAL_SYMBOL:
        return "market.volatility.VIX"
    return f"market.price.{canonical}"


def market_data_kind_v1(symbol: Any) -> str:
    return "INDEX" if normalize_market_symbol_v1(symbol) == VIX_CANONICAL_SYMBOL else "OHLCV"


def symbol_resolution_row_v1(symbol_map: dict[str, Any], canonical_symbol: Any, providers: list[str] | tuple[str, ...]) -> dict[str, Any]:
    canonical = normalize_market_symbol_v1(canonical_symbol)
    return {
        "canonical_symbol": canonical,
        "aliases": list(canonical_aliases_v1(canonical)),
        "provider_mappings": {
            str(provider or "").strip().upper(): list(provider_symbol_candidates_v1(symbol_map, canonical, str(provider or "")))
            for provider in providers
            if str(provider or "").strip()
        },
        "fail_closed_policy": "Missing canonical VIX blocks only VIX-dependent sleeves; no synthetic or substitute VIX is allowed.",
    }


def _mapped_provider_symbols(*, symbol_map: dict[str, Any], canonical_symbol: str, provider: str) -> tuple[str, ...]:
    symbols = symbol_map.get("symbols") if isinstance(symbol_map.get("symbols"), dict) else {}
    row = symbols.get(canonical_symbol) if isinstance(symbols.get(canonical_symbol), dict) else {}
    out: list[str] = []
    provider_aliases = row.get("provider_aliases") if isinstance(row.get("provider_aliases"), dict) else {}
    raw_aliases = provider_aliases.get(provider)
    if isinstance(raw_aliases, list):
        out.extend(str(item).strip() for item in raw_aliases if str(item).strip())
    providers = row.get("providers") if isinstance(row.get("providers"), dict) else {}
    primary = str(providers.get(provider) or "").strip()
    if primary:
        out.append(primary)
    return tuple(out)


def _dedupe_candidates(candidates: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for candidate in candidates:
        text = str(candidate or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return tuple(out)
