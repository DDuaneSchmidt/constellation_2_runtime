from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1

MAPPING_CONFIDENCE_LEGACY = "LEGACY_INFERRED"
MAPPING_CONFIDENCE_DECLARED = "SOURCE_DECLARED"

THESIS_RULES: dict[str, dict[str, str]] = {
    "THESIS_TREND_PERSISTENCE_V1": {
        "name": "Trend Persistence Across Liquid Markets",
        "description": "Trend persistence exists across liquid equity and cross-asset markets.",
        "rationale": "Momentum and trend-following effects can persist when signals are measured over intermediate horizons and costs are controlled.",
    },
    "THESIS_EQUITY_MEAN_REVERSION_V1": {
        "name": "Equity Mean Reversion",
        "description": "Short-horizon overextension in liquid equities can mean revert after liquidity and regime filters.",
        "rationale": "Crowded short-term moves may revert when liquidity normalizes and trend confirmation is absent.",
    },
    "THESIS_RELATIVE_VALUE_SPREAD_V1": {
        "name": "Relative Value Spread Convergence",
        "description": "Market-neutral spreads can converge when relationships are temporarily dislocated.",
        "rationale": "Related instruments can deviate from fair relationship bands and later normalize.",
    },
    "THESIS_VOLATILITY_RISK_PREMIUM_V1": {
        "name": "Defined-Risk Volatility Risk Premium",
        "description": "Defined-risk structures can harvest volatility risk premium when pricing and regime conditions are favorable.",
        "rationale": "Option-implied risk premia may compensate defined-risk exposure in selected regimes.",
    },
    "THESIS_DEFENSIVE_CONVEXITY_V1": {
        "name": "Defensive Convexity and Tail Protection",
        "description": "Defensive/tail structures can improve portfolio resilience in stress regimes.",
        "rationale": "Explicit downside convexity may be valuable when market fragility signals rise.",
    },
    "THESIS_EVENT_DISLOCATION_V1": {
        "name": "Event Dislocation Mean Repricing",
        "description": "Event-driven market dislocations can create temporary mispricings with measurable follow-through or reversal.",
        "rationale": "Event shocks can move prices faster than fundamentals or cross-market relationships adjust.",
    },
    "THESIS_SIMULATION_CONTROL_V1": {
        "name": "Simulation Control and Workflow Validation",
        "description": "Simulator sleeves validate candidate and paper workflow plumbing rather than investable edges.",
        "rationale": "Control sleeves help test architecture without claiming an investment edge.",
    },
}

SLEEVE_RULES: dict[str, dict[str, str]] = {
    "C2_TREND_EQ_PRIMARY_V1": {
        "thesis_id": "THESIS_TREND_PERSISTENCE_V1",
        "hypothesis_id": "HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1",
        "hypothesis_name": "Large-Cap Equity Momentum 20-60D",
        "formal_claim": "Large-cap equity momentum over 20-60 trading days has positive forward expectancy after transaction costs.",
        "market_universe": "Liquid large-cap US equities",
        "signal_definition": "Intermediate-horizon trend/momentum exposure intents from the primary equity trend sleeve.",
        "expected_behavior": "Positive forward expectancy and persistent candidate yield when trend regimes are present.",
        "invalidation_criteria": "Sustained negative expectancy, failed validation, or persistent candidate dormancy outside explainable regimes.",
    },
    "C2_CROSS_ASSET_TREND_V1": {
        "thesis_id": "THESIS_TREND_PERSISTENCE_V1",
        "hypothesis_id": "HYP_CROSS_ASSET_TREND_PERSISTENCE_V1",
        "hypothesis_name": "Cross-Asset Trend Persistence",
        "formal_claim": "Cross-asset trend signals across liquid markets have positive forward expectancy after costs and regime filters.",
        "market_universe": "Liquid cross-asset instruments and proxies",
        "signal_definition": "Cross-asset trend exposure intents.",
        "expected_behavior": "Diversifying positive expectancy when trend regimes broaden beyond equities.",
        "invalidation_criteria": "Low candidate yield, weak validation, or negative expectancy across sampled regimes.",
    },
    "C2_MEAN_REVERSION_EQ_V1": {
        "thesis_id": "THESIS_EQUITY_MEAN_REVERSION_V1",
        "hypothesis_id": "HYP_EQUITY_SHORT_HORIZON_MEAN_REVERSION_V1",
        "hypothesis_name": "Equity Short-Horizon Mean Reversion",
        "formal_claim": "Short-horizon equity dislocations mean revert when liquidity and regime filters are favorable.",
        "market_universe": "Liquid US equities",
        "signal_definition": "Equity mean-reversion exposure intents.",
        "expected_behavior": "Positive expectancy from reversals after overextension.",
        "invalidation_criteria": "Persistent continuation against signals or insufficient validated samples.",
    },
    "C2_MARKET_NEUTRAL_SPREAD_V1": {
        "thesis_id": "THESIS_RELATIVE_VALUE_SPREAD_V1",
        "hypothesis_id": "HYP_MARKET_NEUTRAL_SPREAD_CONVERGENCE_V1",
        "hypothesis_name": "Market-Neutral Spread Convergence",
        "formal_claim": "Selected market-neutral spreads converge enough to overcome transaction costs and implementation drag.",
        "market_universe": "Related liquid instruments eligible for spread construction",
        "signal_definition": "Spread dislocation and convergence rules.",
        "expected_behavior": "Mean-reverting spread outcomes with controlled directional exposure.",
        "invalidation_criteria": "Breakdown in spread relationships, negative expectancy, or untradeable implementation cost.",
    },
    "C2_VOL_INCOME_DEFINED_RISK_V1": {
        "thesis_id": "THESIS_VOLATILITY_RISK_PREMIUM_V1",
        "hypothesis_id": "HYP_DEFINED_RISK_VOL_PREMIUM_V1",
        "hypothesis_name": "Defined-Risk Volatility Premium",
        "formal_claim": "Defined-risk volatility premium structures can generate positive expectancy in favorable regimes.",
        "market_universe": "Liquid optionable underlyings and index proxies",
        "signal_definition": "Volatility income setup filters with defined-risk constraints.",
        "expected_behavior": "Positive premium capture with bounded downside under regime controls.",
        "invalidation_criteria": "Poor risk-adjusted outcomes, insufficient mark confidence, or adverse regime sensitivity.",
    },
    "C2_DEFENSIVE_TAIL_V1": {
        "thesis_id": "THESIS_DEFENSIVE_CONVEXITY_V1",
        "hypothesis_id": "HYP_DEFENSIVE_TAIL_CONVEXITY_V1",
        "hypothesis_name": "Defensive Tail Convexity",
        "formal_claim": "Defensive/tail sleeve structures improve portfolio resilience when stress indicators rise.",
        "market_universe": "Liquid protective instruments and defensive exposures",
        "signal_definition": "Stress and convexity trigger rules.",
        "expected_behavior": "Asymmetric protection or reduced drawdown during stress regimes.",
        "invalidation_criteria": "Persistent bleed without protective response or poor regime trigger precision.",
    },
    "C2_EVENT_DISLOCATION_V1": {
        "thesis_id": "THESIS_EVENT_DISLOCATION_V1",
        "hypothesis_id": "HYP_EVENT_DISLOCATION_REPRICING_V1",
        "hypothesis_name": "Event Dislocation Repricing",
        "formal_claim": "Selected event dislocations create measurable repricing opportunities after event shock normalization.",
        "market_universe": "Liquid event-affected instruments",
        "signal_definition": "Event dislocation detection and follow-through/reversal filters.",
        "expected_behavior": "Positive expectancy after event selection and risk filters.",
        "invalidation_criteria": "False positives dominate, sample outcomes fail, or event taxonomy lacks predictive value.",
    },

    "C2_OIL_SHOCK_REVERSAL_V1": {
        "thesis_id": "THESIS_EVENT_DISLOCATION_V1",
        "hypothesis_id": "ehp_cdbd8fe683acb622",
        "hypothesis_name": "Oil shock reversals across energy ETFs",
        "formal_claim": "Oil shock reversals across energy ETFs can produce deterministic paper candidates when proposal-defined oil shock conditions occur.",
        "market_universe": "Approved Oil Shock ETF universe: DBC, SPY, USO, XLE",
        "signal_definition": "Proposal-defined oil shock event conditions from the approved Oil Shock hypothesis artifacts.",
        "expected_behavior": "Candidate flow only when deterministic oil shock conditions qualify and candidate contracts certify the raw signal.",
        "invalidation_criteria": "Missing source data, persistent no-market setup, failed candidate contracts, or weak validation outcomes after sufficient samples.",
    },
    "C2_INTENT_SIMULATOR_V1": {
        "thesis_id": "THESIS_SIMULATION_CONTROL_V1",
        "hypothesis_id": "HYP_INTENT_SIMULATOR_CONTROL_V1",
        "hypothesis_name": "Intent Simulator Control",
        "formal_claim": "Simulator-generated intents are useful only as workflow control evidence and must not be treated as investment edge validation.",
        "market_universe": "Simulation/control records",
        "signal_definition": "Synthetic or workflow validation intents.",
        "expected_behavior": "Workflow coverage without investment allocation recommendation.",
        "invalidation_criteria": "Simulator evidence contaminates live research allocation or candidate proof.",
    },
}


def text_v1(value: Any) -> str:
    return str(value or "").strip()


def stable_hash_v1(payload: Any) -> str:
    import json
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def file_hash_v1(path: Path | str | None) -> str:
    if not path:
        return ""
    try:
        return hashlib.sha256(Path(path).expanduser().read_bytes()).hexdigest()
    except OSError:
        return ""


def declared_or_legacy_mapping_v1(row: Mapping[str, Any] | None = None, *, sleeve_id: str = "") -> dict[str, Any]:
    source = row if isinstance(row, Mapping) else {}
    declared_hypothesis = text_v1(source.get("hypothesis_id"))
    declared_thesis = text_v1(source.get("thesis_id"))
    if declared_hypothesis and declared_thesis:
        return {"hypothesis_id": declared_hypothesis, "thesis_id": declared_thesis, "mapping_confidence": MAPPING_CONFIDENCE_DECLARED, "mapping_source": "source_declared"}
    sleeve = text_v1(sleeve_id or source.get("sleeve_id"))
    rule = SLEEVE_RULES.get(sleeve)
    if rule:
        return {"hypothesis_id": rule["hypothesis_id"], "thesis_id": rule["thesis_id"], "mapping_confidence": MAPPING_CONFIDENCE_LEGACY, "mapping_source": f"legacy_sleeve_rule:{sleeve}"}
    return {"hypothesis_id": "", "thesis_id": "", "mapping_confidence": "UNMAPPED", "mapping_source": "unmapped"}


def source_ref_v1(path: Path | str | None) -> dict[str, str]:
    return {"path": str(path or ""), "sha256": file_hash_v1(path)}


def report_path_v1(root: Path | str, family: str, day_utc: str, filename: str) -> Path:
    return Path(root).expanduser().resolve() / "reports" / family / str(day_utc) / filename


def read_report_v1(root: Path | str, family: str, day_utc: str, filename: str) -> dict[str, Any]:
    return read_json_v1(report_path_v1(root, family, day_utc, filename))


def iter_report_days_v1(root: Path | str, family: str, filename: str, day_utc: str):
    base = Path(root).expanduser().resolve() / "reports" / family
    if not base.exists():
        return
    for path in sorted(base.glob(f"*/{filename}")):
        if path.parent.name <= str(day_utc):
            yield path, read_json_v1(path)
