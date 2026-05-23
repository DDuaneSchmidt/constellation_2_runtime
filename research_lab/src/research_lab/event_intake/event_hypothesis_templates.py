from __future__ import annotations

from copy import deepcopy
from typing import Any

COMMON_RISK_NOTES = [
    "Research intake only; no broker execution, order management, sleeve mutation, capital allocation, or automatic promotion.",
    "Backtests or model outputs, if later produced, are hypothetical research evidence and are not achieved portfolio results.",
]

DEFAULT_REGIME_DIMENSIONS = ["risk_on", "mixed", "risk_off", "volatility_regime", "trend_regime"]

def _event_def(event_type: str, **params: Any) -> dict[str, Any]:
    return {"type": event_type, "params": dict(params)}

def _family(
    event_family_id: str,
    name: str,
    description: str,
    asset_focus: list[str],
    symbols: list[str],
    definitions: list[dict[str, Any]],
    intents: list[str],
    *,
    windows: list[int] | None = None,
    regimes: list[str] | None = None,
    data_requirement_status: str = "unknown",
    risk_notes: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "event_family_id": event_family_id,
        "name": name,
        "description": description,
        "asset_focus": asset_focus,
        "default_symbols": symbols,
        "default_event_definitions": definitions,
        "default_forward_windows": windows or [1, 2, 5, 10],
        "default_regime_dimensions": regimes or DEFAULT_REGIME_DIMENSIONS,
        "default_research_intents": intents,
        "default_data_requirement_status": data_requirement_status,
        "risk_notes": [*(risk_notes or []), *COMMON_RISK_NOTES],
    }

DEFAULT_EVENT_FAMILY_SPECS: dict[str, dict[str, Any]] = {
    "oil_shock": _family(
        "oil_shock",
        "Oil shock",
        "Large oil or energy ETF shocks associated with geopolitical or macro headline risk.",
        ["energy", "commodities", "broad_market"],
        ["USO", "XLE", "XOP", "DBC", "SPY"],
        [_event_def("daily_return_above_threshold", threshold=0.03, return_column="adj_close"), _event_def("daily_range_percentile_above", percentile=95)],
        ["oil_shock_continuation_or_reversal"],
    ),
    "volatility_spike": _family(
        "volatility_spike",
        "Volatility spike",
        "Large volatility or range spikes in broad-market and volatility-linked ETFs.",
        ["equity_index", "volatility"],
        ["SPY", "QQQ", "IWM", "VIXY", "VXX", "SVXY"],
        [_event_def("daily_range_percentile_above", percentile=95), _event_def("daily_return_below_threshold", threshold=-0.02, return_column="adj_close")],
        ["volatility_spike_mean_reversion"],
    ),
    "volatility_compression": _family(
        "volatility_compression",
        "Volatility compression",
        "Compressed daily range or realized volatility before range expansion or breakout.",
        ["equity_index", "rates", "gold"],
        ["SPY", "QQQ", "IWM", "TLT", "GLD"],
        [_event_def("daily_range_percentile_below", percentile=10)],
        ["volatility_compression_breakout"],
    ),
    "breadth_collapse": _family(
        "breadth_collapse",
        "Breadth collapse",
        "Extreme market breadth deterioration across broad-market and sector ETFs.",
        ["equity_index", "sector_breadth"],
        ["SPY", "QQQ", "IWM", "RSP", "XLF", "XLK", "XLE", "XLV", "XLY"],
        [_event_def("daily_return_below_threshold", threshold=-0.02, return_column="adj_close")],
        ["breadth_collapse_recovery_or_continuation"],
        data_requirement_status="missing_or_future",
        risk_notes=["Breadth-specific data may require future BreadthSnapshot support."],
    ),
    "breadth_recovery": _family(
        "breadth_recovery",
        "Breadth recovery",
        "Sharp breadth recovery after broad deterioration.",
        ["equity_index", "sector_breadth"],
        ["SPY", "QQQ", "IWM", "RSP", "XLF", "XLK", "XLE", "XLV", "XLY"],
        [_event_def("daily_return_above_threshold", threshold=0.02, return_column="adj_close")],
        ["breadth_recovery_follow_through"],
        data_requirement_status="missing_or_future",
        risk_notes=["Breadth-specific data may require future BreadthSnapshot support."],
    ),
    "gap_event": _family(
        "gap_event",
        "Gap event",
        "Large overnight gaps and opening dislocations.",
        ["equity_index", "rates", "gold", "oil"],
        ["SPY", "QQQ", "IWM", "GLD", "TLT", "USO"],
        [_event_def("daily_range_percentile_above", percentile=95)],
        ["gap_fill_or_continuation"],
    ),
    "drawdown_recovery": _family(
        "drawdown_recovery",
        "Drawdown recovery",
        "Large drawdowns followed by recovery thrusts.",
        ["equity_index", "rates", "gold"],
        ["SPY", "QQQ", "IWM", "TLT", "GLD"],
        [_event_def("daily_return_above_threshold", threshold=0.02, return_column="adj_close")],
        ["drawdown_recovery_continuation_or_failure"],
        windows=[5, 10, 20],
    ),
    "regime_transition": _family(
        "regime_transition",
        "Regime transition",
        "Transitions between risk_on, mixed, and risk_off regimes.",
        ["cross_asset", "regime"],
        ["SPY", "QQQ", "IWM", "TLT", "HYG", "LQD", "GLD"],
        [_event_def("daily_range_percentile_above", percentile=90)],
        ["regime_transition_filter"],
        windows=[5, 10, 20],
    ),
    "correlation_break": _family(
        "correlation_break",
        "Correlation break",
        "Cross-asset correlation behavior diverges from recent regime expectations.",
        ["cross_asset"],
        ["SPY", "QQQ", "IWM", "TLT", "GLD", "HYG"],
        [_event_def("daily_range_percentile_above", percentile=90)],
        ["correlation_break_regime_filter"],
    ),
    "credit_stress": _family(
        "credit_stress",
        "Credit stress",
        "Stress behavior in credit ETFs and treasury/equity confirmation.",
        ["credit", "rates", "equity_index"],
        ["HYG", "LQD", "TLT", "SPY"],
        [_event_def("daily_return_below_threshold", threshold=-0.015, return_column="adj_close")],
        ["credit_stress_exhaustion_or_continuation"],
    ),
    "rate_shock": _family(
        "rate_shock",
        "Rate shock",
        "Large moves in rate-sensitive ETFs and equity duration proxies.",
        ["rates", "equity_index"],
        ["TLT", "IEF", "SHY", "QQQ", "SPY"],
        [_event_def("daily_range_percentile_above", percentile=95), _event_def("daily_return_below_threshold", threshold=-0.02, return_column="adj_close")],
        ["rate_shock_reversal_or_continuation"],
    ),
    "commodity_dislocation": _family(
        "commodity_dislocation",
        "Commodity dislocation",
        "Large commodity-linked ETF dislocations across energy, metals, and broad commodity baskets.",
        ["commodities"],
        ["DBC", "USO", "GLD", "XLE", "SPY"],
        [_event_def("daily_range_percentile_above", percentile=95)],
        ["commodity_dislocation_follow_through"],
    ),
    "macro_headline_shock": _family(
        "macro_headline_shock",
        "Macro headline shock",
        "Macro or geopolitical headline-linked market dislocations captured as public-event research observations.",
        ["cross_asset", "headline"],
        ["SPY", "QQQ", "IWM", "TLT", "GLD", "USO"],
        [_event_def("daily_range_percentile_above", percentile=95)],
        ["macro_headline_reversal_or_continuation"],
        risk_notes=["Headline assertions must be captured as observations, not facts, unless independently verified."],
    ),
    "lottery_event": _family(
        "lottery_event",
        "Lottery event",
        "Rare, extreme macro/geopolitical dislocations with high variance and low frequency.",
        ["cross_asset", "tail_risk"],
        ["USO", "XLE", "VIXY", "VXX", "GLD", "TLT", "SPY"],
        [_event_def("daily_range_percentile_above", percentile=99)],
        ["lottery_dislocation_governed_research"],
        risk_notes=["stricter_review_required", "Requires stricter evidence and governance thresholds than ordinary event families."],
    ),
}

BASE_REQUIRED_DATA = ["daily OHLCV", "event definitions", "forward returns", "regime labels where used"]

INTENT_TEMPLATES: dict[str, dict[str, Any]] = {
    "oil_shock": {
        "intent_name": "oil_shock_continuation_or_reversal",
        "intent_description": "Test whether oil and energy ETF shock days show short-horizon continuation or reversal behavior.",
        "candidate_edge_type": "momentum_continuation",
        "expected_holding_period": "1-10 sessions",
        "expected_frequency": "episodic",
        "expected_fragility": "high",
        "default_test_type": "event_study",
        "required_data": ["daily OHLCV for USO/XLE/XOP/DBC/SPY", "optional headline/event timestamp data later"],
        "governance_notes": ["research_only", "public-event market behavior only"],
        "title": "Oil shock continuation or reversal proposal",
        "hypothesis": "Large oil or energy ETF shocks following geopolitical or macro headlines exhibit measurable short-horizon continuation or reversal behavior.",
        "primary_test": "daily_return_above_threshold or daily_range_percentile_above on USO/XLE/XOP",
        "governance_classification": "experimental",
    },
    "volatility_spike": {
        "intent_name": "volatility_spike_mean_reversion",
        "intent_description": "Test broad-market mean reversion after large volatility or range spikes, conditional on regime.",
        "candidate_edge_type": "mean_reversion",
        "expected_holding_period": "1-10 sessions",
        "expected_frequency": "occasional",
        "expected_fragility": "high",
        "default_test_type": "event_study",
        "required_data": ["daily OHLCV for SPY/QQQ/IWM/VIXY/VXX/SVXY", "regime labels"],
        "governance_notes": ["research_only", "hypothetical evidence only"],
        "title": "Volatility spike mean-reversion proposal",
        "hypothesis": "Large volatility/range spikes in broad-market ETFs create short-horizon mean-reversion opportunities, conditional on risk regime.",
        "primary_test": "daily_range_percentile_above on SPY/QQQ/IWM",
        "governance_classification": "experimental",
    },
    "volatility_compression": {
        "intent_name": "volatility_compression_breakout",
        "intent_description": "Test whether compressed range regimes precede expansion or breakout behavior.",
        "candidate_edge_type": "volatility_expansion",
        "expected_holding_period": "2-10 sessions",
        "expected_frequency": "recurring",
        "expected_fragility": "medium",
        "default_test_type": "event_study",
        "required_data": ["daily OHLCV for SPY/QQQ/IWM/TLT/GLD"],
        "governance_notes": ["research_only"],
        "title": "Volatility compression expansion proposal",
        "hypothesis": "Periods of compressed daily range or realized volatility precede subsequent range expansion or directional breakout.",
        "primary_test": "daily_range_percentile_below",
        "governance_classification": "experimental",
    },
    "breadth_collapse": {
        "intent_name": "breadth_collapse_recovery_or_continuation",
        "intent_description": "Test recovery or continuation after extreme breadth deterioration.",
        "candidate_edge_type": "risk_off_exhaustion",
        "expected_holding_period": "1-10 sessions",
        "expected_frequency": "episodic",
        "expected_fragility": "high",
        "default_test_type": "event_study",
        "required_data": ["daily OHLCV for breadth proxies", "future BreadthSnapshot support"],
        "governance_notes": ["research_only", "data_required"],
        "title": "Breadth collapse recovery or continuation proposal",
        "hypothesis": "Extreme breadth deterioration creates short-horizon recovery or continuation behavior depending on trend and volatility regime.",
        "primary_test": "breadth deterioration threshold or proxy ETF return/range event",
        "governance_classification": "data_required",
    },
    "gap_event": {
        "intent_name": "gap_fill_or_continuation",
        "intent_description": "Test gap-fill and continuation behavior after large overnight gaps.",
        "candidate_edge_type": "gap_fill",
        "expected_holding_period": "intraday to 5 sessions",
        "expected_frequency": "recurring",
        "expected_fragility": "high",
        "default_test_type": "event_study",
        "required_data": ["daily OHLCV", "future open/intraday opening range data for refined tests"],
        "governance_notes": ["research_only"],
        "title": "Gap event fill or continuation proposal",
        "hypothesis": "Large overnight gaps show measurable gap-fill or continuation behavior depending on regime and opening range.",
        "primary_test": "gap threshold or daily range proxy until intraday support exists",
        "governance_classification": "experimental",
    },
    "drawdown_recovery": {
        "intent_name": "drawdown_recovery_continuation_or_failure",
        "intent_description": "Test continuation or failure after recovery thrusts from drawdowns.",
        "candidate_edge_type": "momentum_continuation",
        "expected_holding_period": "5-20 sessions",
        "expected_frequency": "episodic",
        "expected_fragility": "medium",
        "default_test_type": "event_study",
        "required_data": ["daily OHLCV", "drawdown path calculations"],
        "governance_notes": ["research_only"],
        "title": "Drawdown recovery continuation or failure proposal",
        "hypothesis": "Large drawdowns followed by recovery thrusts show measurable continuation or failure behavior over 5-20 sessions.",
        "primary_test": "drawdown plus recovery thrust definition",
        "governance_classification": "core_research",
    },
    "regime_transition": {
        "intent_name": "regime_transition_filter",
        "intent_description": "Test whether regime transitions alter sleeve-style mean-reversion and momentum expectations.",
        "candidate_edge_type": "regime_filter",
        "expected_holding_period": "5-20 sessions",
        "expected_frequency": "recurring",
        "expected_fragility": "medium",
        "default_test_type": "regime_event_study",
        "required_data": ["daily OHLCV", "regime labels"],
        "governance_notes": ["research_only", "filter_only_no_sleeve_mutation"],
        "title": "Regime transition filter proposal",
        "hypothesis": "Transitions between risk_on, mixed, and risk_off regimes change the expected performance of mean-reversion and momentum sleeves.",
        "primary_test": "regime label transition event study",
        "governance_classification": "regime_filter",
    },
    "lottery_event": {
        "intent_name": "lottery_dislocation_governed_research",
        "intent_description": "Catalog rare dislocations for governed, high-threshold research review.",
        "candidate_edge_type": "lottery_dislocation",
        "expected_holding_period": "1-20 sessions",
        "expected_frequency": "rare",
        "expected_fragility": "extreme",
        "default_test_type": "event_study",
        "required_data": ["daily OHLCV", "public event definitions", "governance review evidence"],
        "governance_notes": ["research_only", "stricter_review_required"],
        "title": "Lottery dislocation governed research proposal",
        "hypothesis": "Rare, extreme macro/geopolitical dislocations can create high-variance, low-frequency opportunities, but require stricter evidence and governance thresholds.",
        "primary_test": "extreme daily range percentile event study",
        "governance_classification": "lottery",
    },
}

_ALIAS_TEMPLATE = {
    "breadth_recovery": "breadth_collapse",
    "correlation_break": "regime_transition",
    "credit_stress": "volatility_spike",
    "rate_shock": "volatility_spike",
    "commodity_dislocation": "oil_shock",
    "macro_headline_shock": "oil_shock",
}

def default_event_family_specs() -> list[dict[str, Any]]:
    return [deepcopy(DEFAULT_EVENT_FAMILY_SPECS[key]) for key in sorted(DEFAULT_EVENT_FAMILY_SPECS)]

def event_family_spec(event_family_id: str) -> dict[str, Any]:
    key = str(event_family_id).strip().lower()
    if key not in DEFAULT_EVENT_FAMILY_SPECS:
        raise KeyError(f"unknown event family: {event_family_id}")
    return deepcopy(DEFAULT_EVENT_FAMILY_SPECS[key])

def intent_template(event_family_id: str) -> dict[str, Any]:
    key = str(event_family_id).strip().lower()
    template_key = key if key in INTENT_TEMPLATES else _ALIAS_TEMPLATE.get(key)
    if not template_key or template_key not in INTENT_TEMPLATES:
        raise KeyError(f"no event family template: {event_family_id}")
    template = deepcopy(INTENT_TEMPLATES[template_key])
    if key != template_key:
        template["intent_name"] = f"{key}_{template['intent_name']}"
        template["title"] = template["title"].replace(template_key.replace("_", " ").title(), key.replace("_", " ").title())
    if key == "lottery_event":
        template["expected_fragility"] = "extreme"
        if "stricter_review_required" not in template["governance_notes"]:
            template["governance_notes"].append("stricter_review_required")
    return template
