from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Sequence


STRUCTURE_STOCK_ETF = "STOCK_ETF"
STRUCTURE_LONG_OPTION = "LONG_OPTION"
STRUCTURE_VERTICAL_SPREAD = "VERTICAL_SPREAD"
STRUCTURE_CREDIT_SPREAD = "CREDIT_SPREAD"
STRUCTURE_IRON_CONDOR = "IRON_CONDOR"
STRUCTURE_NO_TRADE = "NO_TRADE"

TRADABLE_STRUCTURES = (
    STRUCTURE_STOCK_ETF,
    STRUCTURE_LONG_OPTION,
    STRUCTURE_VERTICAL_SPREAD,
    STRUCTURE_CREDIT_SPREAD,
    STRUCTURE_IRON_CONDOR,
)
ALLOWED_STRUCTURES = (*TRADABLE_STRUCTURES, STRUCTURE_NO_TRADE)

STRUCTURE_STATUS_SELECTED = "structure_selected"
STRUCTURE_STATUS_REJECTED = "structure_rejected"

RISK_STATUS_NOT_EVALUATED = "not_evaluated"
EXECUTION_STATUS_NOT_EVALUATED = "not_evaluated"

RC_SELECTED_STOCK_ETF = "STRUCTURE_SELECTED_STOCK_ETF"
RC_SELECTED_LONG_OPTION = "STRUCTURE_SELECTED_LONG_OPTION"
RC_SELECTED_VERTICAL_SPREAD = "STRUCTURE_SELECTED_VERTICAL_SPREAD"
RC_SELECTED_CREDIT_SPREAD = "STRUCTURE_SELECTED_CREDIT_SPREAD"
RC_SELECTED_IRON_CONDOR = "STRUCTURE_SELECTED_IRON_CONDOR"
RC_REJECTED_NO_TRADE = "STRUCTURE_REJECTED_NO_TRADE"
RC_REJECTED_INPUT_MISSING = "STRUCTURE_REJECTED_INPUT_MISSING"
RC_REJECTED_WEAK_SIGNAL = "STRUCTURE_REJECTED_WEAK_SIGNAL"
RC_REJECTED_REGIME_UNKNOWN = "STRUCTURE_REJECTED_REGIME_UNKNOWN"
RC_REJECTED_HIGH_VOL = "STRUCTURE_REJECTED_HIGH_VOL"
RC_FALLBACK_USED = "STRUCTURE_FALLBACK_USED"
RC_MATCHES_TRENDING = "STRUCTURE_MATCHES_TRENDING"
RC_MATCHES_CHOPPY = "STRUCTURE_MATCHES_CHOPPY"
RC_DOES_NOT_CHANGE_DIRECTION = "STRUCTURE_DOES_NOT_CHANGE_DIRECTION"
RC_DEFINED_RISK_REQUIRED = "STRUCTURE_DEFINED_RISK_REQUIRED"

STRUCTURE_REASON_CODES = frozenset(
    {
        RC_SELECTED_STOCK_ETF,
        RC_SELECTED_LONG_OPTION,
        RC_SELECTED_VERTICAL_SPREAD,
        RC_SELECTED_CREDIT_SPREAD,
        RC_SELECTED_IRON_CONDOR,
        RC_REJECTED_NO_TRADE,
        RC_REJECTED_INPUT_MISSING,
        RC_REJECTED_WEAK_SIGNAL,
        RC_REJECTED_REGIME_UNKNOWN,
        RC_REJECTED_HIGH_VOL,
        RC_FALLBACK_USED,
        RC_MATCHES_TRENDING,
        RC_MATCHES_CHOPPY,
        RC_DOES_NOT_CHANGE_DIRECTION,
        RC_DEFINED_RISK_REQUIRED,
    }
)

_OPTIONAL_FIELDS = ("signal_strength", "expected_move", "holding_period")
_REQUIRED_FIELDS = ("candidate_id", "sleeve_id", "symbol", "direction", "current_regime")

_STRUCTURE_SELECTED_REASON = {
    STRUCTURE_STOCK_ETF: RC_SELECTED_STOCK_ETF,
    STRUCTURE_LONG_OPTION: RC_SELECTED_LONG_OPTION,
    STRUCTURE_VERTICAL_SPREAD: RC_SELECTED_VERTICAL_SPREAD,
    STRUCTURE_CREDIT_SPREAD: RC_SELECTED_CREDIT_SPREAD,
    STRUCTURE_IRON_CONDOR: RC_SELECTED_IRON_CONDOR,
}


def _norm_text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text if text else default


def _norm_upper(value: Any, default: str = "") -> str:
    return _norm_text(value, default).upper()


def _candidate_optional_value(candidate: Mapping[str, Any], field: str) -> str:
    if field == "holding_period":
        value = candidate.get("holding_period")
        if value is None:
            value = candidate.get("expected_holding_period")
        if value is None:
            value = candidate.get("expected_holding_days")
        return _norm_text(value, "")
    return _norm_text(candidate.get(field), "")


def _normalise_direction(direction: Any) -> str:
    text = _norm_upper(direction, "UNKNOWN")
    if text in {"BULLISH", "LONG", "BUY", "LONG_EQUITY"}:
        return "bullish"
    if text in {"BEARISH", "SHORT", "SELL", "SHORT_EQUITY"}:
        return "bearish"
    if text in {"NEUTRAL", "MARKET_NEUTRAL", "FLAT"}:
        return "neutral"
    return "UNKNOWN"


def _is_high_confidence(confidence: Any) -> bool:
    return _norm_upper(confidence, "UNKNOWN") in {"HIGH", "STRONG"}


def _is_weak_signal(signal_strength: str) -> bool:
    return _norm_upper(signal_strength, "") == "WEAK"


def _has_required_inputs(candidate: Mapping[str, Any], normalized_direction: str) -> bool:
    for field in _REQUIRED_FIELDS:
        value = _norm_text(candidate.get(field), "")
        if not value:
            return False
        if field != "current_regime" and value.upper() == "UNKNOWN":
            return False
    return normalized_direction != "UNKNOWN"


def _fallback_used(candidate: Mapping[str, Any]) -> bool:
    return any(not _candidate_optional_value(candidate, field) for field in _OPTIONAL_FIELDS)


def _selected_risk_flags(selected_structure: str) -> tuple[bool, bool, bool]:
    if selected_structure == STRUCTURE_NO_TRADE:
        return False, False, False
    return True, True, True


def _rejected_structures(selected_structure: str) -> list[str]:
    if selected_structure == STRUCTURE_NO_TRADE:
        return list(TRADABLE_STRUCTURES)
    return [structure for structure in TRADABLE_STRUCTURES if structure != selected_structure]


def _append_reason(reason_codes: list[str], reason_code: str) -> None:
    if reason_code not in reason_codes:
        reason_codes.append(reason_code)


def _decision_id_seed(
    *,
    candidate_id: str,
    sleeve_id: str,
    symbol: str,
    normalized_direction: str,
    current_regime: str,
    selected_structure: str,
    fallback_used: bool,
) -> str:
    return "|".join(
        (
            candidate_id,
            sleeve_id,
            symbol,
            normalized_direction,
            current_regime,
            selected_structure,
            "true" if fallback_used else "false",
        )
    )


def structure_decision_id_v1(
    *,
    candidate_id: str,
    sleeve_id: str,
    symbol: str,
    normalized_direction: str,
    current_regime: str,
    selected_structure: str,
    fallback_used: bool,
) -> str:
    return hashlib.sha256(
        _decision_id_seed(
            candidate_id=candidate_id,
            sleeve_id=sleeve_id,
            symbol=symbol,
            normalized_direction=normalized_direction,
            current_regime=current_regime,
            selected_structure=selected_structure,
            fallback_used=fallback_used,
        ).encode("utf-8")
    ).hexdigest()


def build_structure_input_snapshot_v1(candidate: Mapping[str, Any]) -> dict[str, Any]:
    normalized_direction = _normalise_direction(candidate.get("direction"))
    snapshot: dict[str, Any] = {
        "original_direction": _norm_text(candidate.get("direction"), "UNKNOWN"),
        "normalized_direction": normalized_direction,
        "current_regime": _norm_upper(candidate.get("current_regime"), "UNKNOWN"),
        "confidence": _norm_upper(candidate.get("confidence"), "UNKNOWN"),
        "fallback_used": _fallback_used(candidate),
    }
    for field in _OPTIONAL_FIELDS:
        value = _candidate_optional_value(candidate, field)
        if value:
            snapshot[field] = value
    return snapshot


def _selection_for_candidate(
    *,
    candidate: Mapping[str, Any],
    normalized_direction: str,
    current_regime: str,
    signal_strength: str,
) -> tuple[str, list[str], str]:
    if not _has_required_inputs(candidate, normalized_direction):
        return STRUCTURE_NO_TRADE, [RC_REJECTED_INPUT_MISSING], "Required structure-selection input is missing or UNKNOWN."
    if _is_weak_signal(signal_strength):
        return STRUCTURE_NO_TRADE, [RC_REJECTED_WEAK_SIGNAL], "Explicit weak signal rejects structure selection."
    if current_regime == "UNKNOWN" or current_regime not in {"TRENDING", "CHOPPY", "HIGH_VOL_UNSTABLE"}:
        return STRUCTURE_NO_TRADE, [RC_REJECTED_REGIME_UNKNOWN], "Unknown regime rejects structure selection."
    if current_regime == "HIGH_VOL_UNSTABLE":
        return STRUCTURE_NO_TRADE, [RC_REJECTED_HIGH_VOL], "HIGH_VOL_UNSTABLE blocks structures in V1."
    if current_regime == "TRENDING":
        if normalized_direction == "bullish":
            if _is_high_confidence(candidate.get("confidence")):
                return STRUCTURE_STOCK_ETF, [RC_MATCHES_TRENDING], "Trending bullish high-confidence candidate maps to STOCK_ETF."
            return STRUCTURE_VERTICAL_SPREAD, [RC_MATCHES_TRENDING], "Trending bullish candidate maps to defined-risk VERTICAL_SPREAD."
        if normalized_direction == "bearish":
            if _is_high_confidence(candidate.get("confidence")):
                return STRUCTURE_LONG_OPTION, [RC_MATCHES_TRENDING], "Trending bearish high-confidence candidate maps to LONG_OPTION."
            return STRUCTURE_VERTICAL_SPREAD, [RC_MATCHES_TRENDING], "Trending bearish candidate maps to defined-risk VERTICAL_SPREAD."
        return STRUCTURE_NO_TRADE, [RC_REJECTED_NO_TRADE, RC_MATCHES_TRENDING], "Trending neutral candidate has no V1 trade structure."
    if current_regime == "CHOPPY":
        if normalized_direction == "neutral":
            return STRUCTURE_IRON_CONDOR, [RC_MATCHES_CHOPPY], "Choppy neutral candidate maps to defined-risk IRON_CONDOR."
        if normalized_direction in {"bullish", "bearish"}:
            return STRUCTURE_CREDIT_SPREAD, [RC_MATCHES_CHOPPY], "Choppy directional candidate maps to defined-risk CREDIT_SPREAD."
        return STRUCTURE_NO_TRADE, [RC_REJECTED_NO_TRADE, RC_MATCHES_CHOPPY], "Choppy candidate direction is unsupported."
    return STRUCTURE_NO_TRADE, [RC_REJECTED_NO_TRADE], "No V1 structure mapping matched."


def select_structure_for_candidate_v1(candidate: Mapping[str, Any]) -> dict[str, Any]:
    raw_candidate = dict(candidate)
    candidate_id = _norm_text(candidate.get("candidate_id"), "UNKNOWN")
    sleeve_id = _norm_text(candidate.get("sleeve_id"), "UNKNOWN")
    symbol = _norm_upper(candidate.get("symbol"), "UNKNOWN")
    timestamp = _norm_text(candidate.get("timestamp"), "")
    original_direction = _norm_text(candidate.get("direction"), "UNKNOWN")
    normalized_direction = _normalise_direction(original_direction)
    current_regime = _norm_upper(candidate.get("current_regime"), "UNKNOWN")
    confidence = _norm_upper(candidate.get("confidence"), "UNKNOWN")
    signal_strength = _candidate_optional_value(candidate, "signal_strength")
    expected_move = _candidate_optional_value(candidate, "expected_move")
    holding_period = _candidate_optional_value(candidate, "holding_period")
    fallback_used = _fallback_used(candidate)

    selected_structure, reason_codes, structure_reason = _selection_for_candidate(
        candidate=candidate,
        normalized_direction=normalized_direction,
        current_regime=current_regime,
        signal_strength=signal_strength,
    )
    if fallback_used:
        _append_reason(reason_codes, RC_FALLBACK_USED)
    if selected_structure in _STRUCTURE_SELECTED_REASON:
        _append_reason(reason_codes, _STRUCTURE_SELECTED_REASON[selected_structure])
        _append_reason(reason_codes, RC_DEFINED_RISK_REQUIRED)
    else:
        _append_reason(reason_codes, RC_REJECTED_NO_TRADE)
    _append_reason(reason_codes, RC_DOES_NOT_CHANGE_DIRECTION)

    risk_defined, max_loss_known, max_gain_known_or_bounded = _selected_risk_flags(selected_structure)
    decision: dict[str, Any] = {
        "structure_decision_id": structure_decision_id_v1(
            candidate_id=candidate_id,
            sleeve_id=sleeve_id,
            symbol=symbol,
            normalized_direction=normalized_direction,
            current_regime=current_regime,
            selected_structure=selected_structure,
            fallback_used=fallback_used,
        ),
        "candidate_id": candidate_id,
        "timestamp": timestamp,
        "sleeve_id": sleeve_id,
        "symbol": symbol,
        "original_direction": original_direction,
        "normalized_direction": normalized_direction,
        "current_regime": current_regime,
        "confidence": confidence,
        "fallback_used": fallback_used,
        "selected_structure": selected_structure,
        "rejected_structures": _rejected_structures(selected_structure),
        "structure_status": STRUCTURE_STATUS_SELECTED
        if selected_structure != STRUCTURE_NO_TRADE
        else STRUCTURE_STATUS_REJECTED,
        "reason_codes": reason_codes,
        "structure_reason": structure_reason,
        "risk_defined": risk_defined,
        "max_loss_known": max_loss_known,
        "max_gain_known_or_bounded": max_gain_known_or_bounded,
        "risk_status": RISK_STATUS_NOT_EVALUATED,
        "execution_status": EXECUTION_STATUS_NOT_EVALUATED,
        "raw_candidate_payload": raw_candidate,
    }
    if signal_strength:
        decision["signal_strength"] = signal_strength
    if expected_move:
        decision["expected_move"] = expected_move
    if holding_period:
        decision["holding_period"] = holding_period
    validate_structure_decision_invariants_v1(decision)
    return decision


def select_structures_for_candidates_v1(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [select_structure_for_candidate_v1(candidate) for candidate in candidates]


def build_structure_selection_summary_v1(structure_decisions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    selected_counts: dict[str, int] = {}
    rejection_reason_counts: dict[str, int] = {}
    selected_count = 0
    rejected_count = 0
    fallback_count = 0
    for decision in structure_decisions:
        structure = _norm_text(decision.get("selected_structure"), "UNKNOWN")
        selected_counts[structure] = selected_counts.get(structure, 0) + 1
        if bool(decision.get("fallback_used") is True):
            fallback_count += 1
        if decision.get("structure_status") == STRUCTURE_STATUS_SELECTED:
            selected_count += 1
        else:
            rejected_count += 1
            for code in decision.get("reason_codes") or []:
                code_text = _norm_text(code, "")
                if code_text:
                    rejection_reason_counts[code_text] = rejection_reason_counts.get(code_text, 0) + 1
    return {
        "candidates_evaluated": len(structure_decisions),
        "structures_selected_count": selected_count,
        "structures_rejected_count": rejected_count,
        "selected_structure_counts": dict(sorted(selected_counts.items())),
        "rejection_reason_counts": dict(sorted(rejection_reason_counts.items())),
        "fallback_used_count": fallback_count,
        "advisory_only": True,
        "controls_broker_execution": False,
        "controls_phasec_materialization": False,
    }


def validate_structure_decision_invariants_v1(decision: Mapping[str, Any]) -> None:
    selected = _norm_text(decision.get("selected_structure"), "")
    if selected not in ALLOWED_STRUCTURES:
        raise ValueError(f"STRUCTURE_SELECTED_UNSUPPORTED:{selected}")
    if decision.get("risk_status") != RISK_STATUS_NOT_EVALUATED:
        raise ValueError("STRUCTURE_RISK_STATUS_MUST_REMAIN_NOT_EVALUATED")
    if decision.get("execution_status") != EXECUTION_STATUS_NOT_EVALUATED:
        raise ValueError("STRUCTURE_EXECUTION_STATUS_MUST_REMAIN_NOT_EVALUATED")
    if _normalise_direction(decision.get("original_direction")) != decision.get("normalized_direction"):
        raise ValueError("STRUCTURE_DIRECTION_NORMALIZATION_INCONSISTENT")
    if selected != STRUCTURE_NO_TRADE:
        if not bool(decision.get("risk_defined")):
            raise ValueError("STRUCTURE_RISK_MUST_BE_DEFINED")
        if not bool(decision.get("max_loss_known")):
            raise ValueError("STRUCTURE_MAX_LOSS_MUST_BE_KNOWN")
        if not bool(decision.get("max_gain_known_or_bounded")):
            raise ValueError("STRUCTURE_MAX_GAIN_MUST_BE_KNOWN_OR_BOUNDED")


def canonical_structure_decision_json_v1(decision: Mapping[str, Any]) -> str:
    return json.dumps(dict(decision), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
