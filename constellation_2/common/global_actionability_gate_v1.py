from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence


ACTIONABILITY_GATE_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/TRADE_STATE/global_actionability_gate.v1.schema.json"
)
GATE_CONTRACT_RELPATH = "governance/05_CONTRACTS/C2/global_actionability_gate_v1.contract.md"
GATE_CONTRACT_ID = "C2_GLOBAL_ACTIONABILITY_GATE_CONTRACT_V1"
GATE_CONTRACT_VERSION = 1

GATE_ACTIONABLE = "ACTIONABLE"
GATE_DEGRADED = "DEGRADED_REVIEW_REQUIRED"
GATE_BLOCKED = "BLOCKED"

REASON_ACTIONABLE_GATE_CLEAR = "ACTIONABLE_GATE_CLEAR"
REASON_UPSTREAM_TRADE_TRUTH_BLOCKED = "UPSTREAM_TRADE_TRUTH_BLOCKED"
REASON_STALE_TRADE_TRUTH = "STALE_TRADE_TRUTH"
REASON_OWNERSHIP_AMBIGUOUS = "OWNERSHIP_AMBIGUOUS"
REASON_FOREIGN_MANUAL_CLASSIFICATION = "FOREIGN_MANUAL_CLASSIFICATION"
REASON_RECONCILIATION_DEGRADED = "RECONCILIATION_DEGRADED"
REASON_INSUFFICIENT_EVIDENCE_FOR_AUTONOMY = "INSUFFICIENT_EVIDENCE_FOR_AUTONOMY"
REASON_TRADE_IDENTITY_BLOCKED = "TRADE_IDENTITY_BLOCKED"
REASON_DESCRIPTION_BLOCKED_FOR_DOWNSTREAM_ACTION = "DESCRIPTION_BLOCKED_FOR_DOWNSTREAM_ACTION"

GATE_FIELDS_READ: tuple[str, ...] = (
    "trade_identity.trade_identity_id",
    "trade_identity.ownership_classification",
    "trade_identity.ambiguity_state",
    "trade_identity.blocker_state",
    "reconciliation_health.current_state",
    "reconciliation_health.freshness_status",
    "reconciliation_health.ambiguity_status",
    "reconciliation_health.insufficient_evidence_status",
    "reconciliation_health.downstream_action_posture",
    "reconciled_trade_description.reconciliation_descriptive_status",
    "reconciled_trade_description.downstream_posture",
)


def _stable_unique(values: Sequence[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for value in values:
        item = str(value or "").strip()
        if item and item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def evaluate_global_actionability_gate_v1(
    *,
    trade_identity: Mapping[str, Any],
    reconciled_description: Mapping[str, Any],
    reconciliation_health: Mapping[str, Any],
    upstream_core2_refs: Mapping[str, Any],
    trade_identity_ref: Mapping[str, Any],
    materialization_set_id: str,
    day_utc: str,
    evaluated_at_utc: str,
) -> Dict[str, Any]:
    ownership = str(trade_identity.get("ownership_classification") or "").strip()
    ambiguity_state = str(trade_identity.get("ambiguity_state") or "").strip()
    blocker_state = str(trade_identity.get("blocker_state") or "").strip()

    health_state = str(reconciliation_health.get("current_state") or "").strip()
    freshness_status = str(reconciliation_health.get("freshness_status") or "").strip()
    ambiguity_status = str(reconciliation_health.get("ambiguity_status") or "").strip()
    insufficient_status = str(reconciliation_health.get("insufficient_evidence_status") or "").strip()
    downstream_action_posture = str(reconciliation_health.get("downstream_action_posture") or "").strip()

    description_downstream = str(reconciled_description.get("downstream_posture") or "").strip()
    description_reconciliation = str(reconciled_description.get("reconciliation_descriptive_status") or "").strip()

    blocked_reasons: List[str] = []
    degraded_reasons: List[str] = []

    if health_state == "BLOCKED":
        blocked_reasons.append(REASON_UPSTREAM_TRADE_TRUTH_BLOCKED)
    if description_downstream == "BLOCKED" or description_reconciliation in {"BLOCKED", "AMBIGUOUS"}:
        blocked_reasons.append(REASON_DESCRIPTION_BLOCKED_FOR_DOWNSTREAM_ACTION)
    if blocker_state == "BLOCKED":
        blocked_reasons.append(REASON_TRADE_IDENTITY_BLOCKED)
    if ownership == "AMBIGUOUS_OWNERSHIP" or ambiguity_state != "NONE" or ambiguity_status == "AMBIGUOUS":
        blocked_reasons.append(REASON_OWNERSHIP_AMBIGUOUS)
    if ownership == "INSUFFICIENT_EVIDENCE" or insufficient_status == "INSUFFICIENT":
        blocked_reasons.append(REASON_INSUFFICIENT_EVIDENCE_FOR_AUTONOMY)
    if freshness_status == "TOO_STALE":
        blocked_reasons.append(REASON_STALE_TRADE_TRUTH)

    if health_state == "DEGRADED" or downstream_action_posture == "DEGRADED" or description_downstream == "DEGRADED":
        degraded_reasons.append(REASON_RECONCILIATION_DEGRADED)
    if freshness_status == "STALE":
        degraded_reasons.append(REASON_STALE_TRADE_TRUTH)
    if ownership == "FOREIGN_MANUAL":
        degraded_reasons.append(REASON_FOREIGN_MANUAL_CLASSIFICATION)

    blocked_reasons = _stable_unique(blocked_reasons)
    degraded_reasons = _stable_unique(degraded_reasons)

    if blocked_reasons:
        verdict = GATE_BLOCKED
        first_reason = blocked_reasons[0]
        reason_codes = blocked_reasons
    elif degraded_reasons:
        verdict = GATE_DEGRADED
        first_reason = degraded_reasons[0]
        reason_codes = degraded_reasons
    else:
        verdict = GATE_ACTIONABLE
        first_reason = REASON_ACTIONABLE_GATE_CLEAR
        reason_codes = [REASON_ACTIONABLE_GATE_CLEAR]

    return {
        "schema_id": "global_actionability_gate",
        "schema_version": "v1",
        "authority_owner": "global_actionability_gate_v1",
        "materialization_set_id": str(materialization_set_id),
        "day_utc": str(day_utc),
        "evaluated_at_utc": str(evaluated_at_utc),
        "trade_identity_ref": dict(trade_identity_ref),
        "gate_verdict": verdict,
        "first_reason_code": first_reason,
        "reason_codes": reason_codes,
        "freshness_basis": {
            "reconciliation_health_state": health_state,
            "freshness_status": freshness_status,
            "downstream_action_posture": downstream_action_posture,
        },
        "ownership_basis": {
            "ownership_classification": ownership,
            "ambiguity_state": ambiguity_state,
            "blocker_state": blocker_state,
        },
        "reconciliation_basis": {
            "current_state": health_state,
            "freshness_status": freshness_status,
            "ambiguity_status": ambiguity_status,
            "insufficient_evidence_status": insufficient_status,
            "description_downstream_posture": description_downstream,
            "description_reconciliation_status": description_reconciliation,
        },
        "upstream_core2_refs": dict(upstream_core2_refs),
        "rule_version": {
            "gate_contract_id": GATE_CONTRACT_ID,
            "gate_contract_version": GATE_CONTRACT_VERSION,
        },
        "derived_only": True,
    }
