from __future__ import annotations

from typing import Any

from .mechanism_search_models import MECHANISM_FAMILIES

MAX_MECHANISM_HYPOTHESES = 1000
FORBIDDEN_MECHANISM_SEARCH_FIELDS = {
    "broker_execution_authorized",
    "capital_authorized",
    "position_sizing_authorized",
    "automatic_paper_placement_authorized",
    "live_trading_authorized",
}
FORBIDDEN_MECHANISM_SEARCH_TEXT = [
    "broker execution approved",
    "capital allocation approved",
    "position sizing approved",
    "automatic paper placement",
    "live trading approved",
    "trade recommendation",
]


class MechanismSearchGovernanceError(ValueError):
    pass


def validate_mechanism_search_request(*, limit: int) -> bool:
    if limit < 1:
        raise MechanismSearchGovernanceError("mechanism search limit must be positive")
    if limit > MAX_MECHANISM_HYPOTHESES:
        raise MechanismSearchGovernanceError(f"mechanism search limit exceeds {MAX_MECHANISM_HYPOTHESES}")
    return True


def validate_mechanism_hypothesis(payload: dict[str, Any]) -> bool:
    if payload.get("artifact_type") != "ResearchHypothesis":
        raise MechanismSearchGovernanceError("mechanism search may only emit ResearchHypothesis artifacts")
    if payload.get("evidence_level") != "GENERATED_ONLY":
        raise MechanismSearchGovernanceError("new mechanism hypotheses must start as GENERATED_ONLY")
    if payload.get("mechanism") not in MECHANISM_FAMILIES:
        raise MechanismSearchGovernanceError(f"unsupported mechanism family: {payload.get('mechanism')}")
    for field in [
        "conditions",
        "regime",
        "timeframe",
        "entry_observation_rule",
        "exit_observation_rule",
        "invalidation_rule",
        "complexity_score",
        "source_search_config",
    ]:
        if field not in payload:
            raise MechanismSearchGovernanceError(f"missing mechanism hypothesis field: {field}")
    metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {}
    for field in FORBIDDEN_MECHANISM_SEARCH_FIELDS:
        if payload.get(field) is True or metadata.get(field) is True:
            raise MechanismSearchGovernanceError(f"mechanism search forbids authority field: {field}")
    scan_payload = {key: value for key, value in payload.items() if key not in {"source_search_config", "metadata"}}
    lowered = str({"payload": scan_payload, "metadata": metadata}).lower()
    if any(term in lowered for term in FORBIDDEN_MECHANISM_SEARCH_TEXT):
        raise MechanismSearchGovernanceError("mechanism search cannot carry execution, capital, sizing, placement, or recommendation authority")
    return True


def validate_mechanism_search_run(payload: dict[str, Any]) -> dict[str, Any]:
    failures: list[str] = []
    hypotheses = payload.get("hypotheses", [])
    if not isinstance(hypotheses, list):
        failures.append("hypotheses must be a list")
    elif len(hypotheses) > MAX_MECHANISM_HYPOTHESES:
        failures.append(f"hypothesis count exceeds {MAX_MECHANISM_HYPOTHESES}")
    for row in hypotheses if isinstance(hypotheses, list) else []:
        try:
            validate_mechanism_hypothesis(row)
        except Exception as exc:
            failures.append(str(exc))
    return {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "research_only": True,
        "max_hypotheses": MAX_MECHANISM_HYPOTHESES,
    }
