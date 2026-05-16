from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from typing import Any


RISK_TIERS = {
    "SMOKE_TEST": Decimal("0"),
    "VERY_SMALL": Decimal("0.10"),
    "NORMAL": Decimal("0.25"),
    "STRONG": Decimal("0.50"),
    "MAX": Decimal("1.00"),
}
RUNTIME_TRUTH_CLASSIFICATIONS = {"REAL_RUNTIME", "DEMO_ONLY", "DRY_RUN_ONLY"}
DEFAULT_PORTFOLIO_VALUE = Decimal("100000")
PAPER_RISK_CAP_PCT = Decimal("0.25")


def build_trade_sizing_guidance_v1(
    *,
    candidate: dict[str, Any],
    promoted_sleeve_source_valid: bool,
    portfolio_value: str | Decimal | None = None,
    sizing_tier: str = "",
    early_paper_mode: bool = True,
) -> dict[str, Any]:
    tier = _tier(sizing_tier or candidate.get("sizing_tier") or candidate.get("risk_tier") or "SMOKE_TEST")
    runtime_truth = str(candidate.get("runtime_truth_classification") or "REAL_RUNTIME").strip().upper()
    entry = _decimal(candidate.get("entry_reference_price"))
    stop = _decimal(candidate.get("stop_price"))
    portfolio = _decimal(portfolio_value or candidate.get("portfolio_value_used") or candidate.get("portfolio_value")) or DEFAULT_PORTFOLIO_VALUE
    overlap_adjustment = _adjustment(candidate.get("overlap_adjustment"), default=Decimal("1"))
    market_context_adjustment, market_context_reasons = _market_context_adjustment(candidate.get("market_context") or candidate.get("market_context_status"))
    regime_adjustment = _adjustment(candidate.get("regime_adjustment"), default=market_context_adjustment)
    concentration_adjustment = _adjustment(candidate.get("concentration_adjustment"), default=Decimal("1"))
    blockers: list[str] = []
    reason_codes: list[str] = [*market_context_reasons]

    if runtime_truth not in RUNTIME_TRUTH_CLASSIFICATIONS:
        blockers.append("INVALID_RUNTIME_TRUTH_CLASSIFICATION")
    if runtime_truth == "DEMO_ONLY" or bool(candidate.get("demo_mode", False)):
        blockers.append("DEMO_ONLY_BLOCKS_SIZING")
    if runtime_truth == "DRY_RUN_ONLY" or bool(candidate.get("dry_run_only", False)):
        blockers.append("DRY_RUN_ONLY_BLOCKS_SIZING")
    if _stale(candidate):
        blockers.append("STALE_PACKET_BLOCKS_SIZING")
    if not promoted_sleeve_source_valid:
        blockers.append("UNPROMOTED_SLEEVE_BLOCKS_SIZING")
    if not str(candidate.get("symbol") or "").strip():
        blockers.append("MISSING_SYMBOL")
    if not str(candidate.get("side") or candidate.get("direction") or "").strip():
        blockers.append("MISSING_SIDE")
    if entry is None:
        blockers.append("MISSING_ENTRY_REFERENCE_PRICE")
    if stop is None:
        blockers.append("MISSING_STOP_PRICE")
    if not str(candidate.get("stop_logic") or "").strip():
        blockers.append("MISSING_STOP_LOGIC")
    if not str(candidate.get("risk_per_trade") or "").strip():
        blockers.append("MISSING_RISK_RULES")
    if tier not in RISK_TIERS:
        blockers.append("INVALID_SIZING_TIER")
    if concentration_adjustment <= 0 or str(candidate.get("concentration_status") or "").upper() in {"BLOCKED", "MAX_EXCEEDED"}:
        blockers.append("CONCENTRATION_CAP_BLOCKED")

    risk_per_share = abs(entry - stop) if entry is not None and stop is not None else Decimal("0")
    if entry is not None and stop is not None and risk_per_share <= 0:
        blockers.append("INVALID_STOP_DISTANCE")

    base_pct = RISK_TIERS.get(tier, Decimal("0"))
    risk_pct_used = min(base_pct, PAPER_RISK_CAP_PCT) if early_paper_mode and tier != "SMOKE_TEST" else base_pct
    if early_paper_mode and base_pct > PAPER_RISK_CAP_PCT:
        reason_codes.append("PAPER_RISK_CAPPED_AT_0_25_PERCENT")
    adjusted_pct = risk_pct_used * overlap_adjustment * regime_adjustment * concentration_adjustment
    allowed_risk = (portfolio * adjusted_pct / Decimal("100")).quantize(Decimal("0.01"))

    suggested_quantity = 0
    if not blockers and risk_per_share > 0:
        if tier == "SMOKE_TEST":
            suggested_quantity = 1
            allowed_risk = risk_per_share.quantize(Decimal("0.01"))
            risk_pct_used = (allowed_risk / portfolio * Decimal("100")).quantize(Decimal("0.0001")) if portfolio > 0 else Decimal("0")
            reason_codes.append("SMOKE_TEST_ONE_SHARE")
        else:
            suggested_quantity = int((allowed_risk / risk_per_share).to_integral_value(rounding=ROUND_FLOOR))
            if suggested_quantity <= 0:
                blockers.append("ALLOWED_RISK_TOO_SMALL_FOR_ONE_SHARE")

    estimated_position_value = (entry * Decimal(suggested_quantity)).quantize(Decimal("0.01")) if entry is not None else Decimal("0")
    max_loss = (risk_per_share * Decimal(suggested_quantity)).quantize(Decimal("0.01")) if risk_per_share > 0 else Decimal("0")
    if not blockers:
        reason_codes.append("SIZING_GUIDANCE_READY")
    return {
        "portfolio_value_used": _fmt(portfolio),
        "sizing_tier": tier,
        "risk_pct_used": _fmt(risk_pct_used),
        "allowed_dollar_risk": _fmt(allowed_risk),
        "entry_reference_price": _fmt(entry) if entry is not None else "",
        "stop_price": _fmt(stop) if stop is not None else "",
        "risk_per_share": _fmt(risk_per_share),
        "suggested_quantity": suggested_quantity,
        "estimated_position_value": _fmt(estimated_position_value),
        "max_loss_if_stopped": _fmt(max_loss),
        "overlap_adjustment": _fmt(overlap_adjustment),
        "regime_adjustment": _fmt(regime_adjustment),
        "market_context_adjustment": _fmt(market_context_adjustment),
        "concentration_adjustment": _fmt(concentration_adjustment),
        "sizing_blockers": sorted(set(blockers)),
        "sizing_reason_codes": sorted(set(reason_codes)),
        "ai_selected_size": False,
        "operator_can_override": True,
    }


def _tier(value: Any) -> str:
    return str(value or "").strip().upper()


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _adjustment(value: Any, *, default: Decimal) -> Decimal:
    parsed = _decimal(value)
    if parsed is None:
        return default
    if parsed < 0:
        return Decimal("0")
    if parsed > 1:
        return Decimal("1")
    return parsed


def _market_context_adjustment(value: Any) -> tuple[Decimal, list[str]]:
    if not isinstance(value, dict):
        return Decimal("1"), []
    regime = str(value.get("regime_label") or value.get("regime") or "").strip().upper()
    volatility = str(value.get("volatility_classification") or value.get("volatility_status") or "").strip().upper()
    breadth = str(value.get("breadth_classification") or value.get("breadth_status") or "").strip().upper()
    macro_risk = str(value.get("macro_event_risk_level") or "").strip().upper()
    stale = str(value.get("stale_data_status") or "").strip().upper()
    adjustment = Decimal("1")
    reasons: list[str] = []
    if stale in {"STALE", "MISSING_INPUT"}:
        adjustment = min(adjustment, Decimal("0.5"))
        reasons.append(f"MARKET_CONTEXT_{stale}_RISK_REDUCTION")
    if regime == "PANIC" or volatility == "PANIC_VOL":
        adjustment = min(adjustment, Decimal("0.25"))
        reasons.append("MARKET_CONTEXT_PANIC_RISK_REDUCTION")
    elif regime == "HIGH_VOLATILITY" or volatility == "HIGH_VOL":
        adjustment = min(adjustment, Decimal("0.5"))
        reasons.append("MARKET_CONTEXT_HIGH_VOL_RISK_REDUCTION")
    if breadth == "BREADTH_COLLAPSE":
        adjustment = min(adjustment, Decimal("0.5"))
        reasons.append("MARKET_CONTEXT_BREADTH_COLLAPSE_RISK_REDUCTION")
    if macro_risk in {"HIGH", "EXTREME"}:
        adjustment = min(adjustment, Decimal("0.75"))
        reasons.append("MARKET_CONTEXT_MACRO_EVENT_RISK_REDUCTION")
    return adjustment, sorted(set(reasons))


def _stale(candidate: dict[str, Any]) -> bool:
    return bool(candidate.get("stale_packet", False)) or str(candidate.get("stale_status") or candidate.get("data_status") or "").upper() == "STALE"


def _fmt(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    return "0" if text == "-0" else text
