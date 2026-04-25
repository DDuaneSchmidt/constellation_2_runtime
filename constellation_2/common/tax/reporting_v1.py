from __future__ import annotations

from typing import Any, Iterable

from constellation_2.common.tax.common_v1 import canonical_hash_v1, validate_tax_payload_v1
from constellation_2.common.tax.reason_codes_v1 import require_reason_codes_v1


def build_realized_tax_report_v1(*, snapshot: dict[str, Any], accepted_facts: Iterable[dict[str, Any]]) -> dict[str, Any]:
    realizations = [
        {
            "accepted_fact_id": fact["accepted_fact_id"],
            "lot_id": fact["payload"].get("lot_id"),
            "security_id": fact["payload"].get("security_id"),
            "realized_gain_loss": fact["payload"].get("realized_gain_loss"),
            "realized_at": fact["payload"].get("realized_at"),
        }
        for fact in accepted_facts
        if str(fact.get("fact_family") or "") == "realization_recorded"
    ]
    payload = {
        "schema_id": "realized_tax_report",
        "schema_version": "v1",
        "report_id": canonical_hash_v1({"snapshot_id": snapshot["snapshot_id"], "realizations": realizations}),
        "scope_id": snapshot["scope_id"],
        "snapshot_id": snapshot["snapshot_id"],
        "realizations": realizations,
        "reason_codes": ["TAX_REPORT_GENERATED"],
    }
    return validate_tax_payload_v1(payload)


def build_tax_advisory_explanation_v1(*, decision: dict[str, Any]) -> dict[str, Any]:
    details = [
        f"decision_status={decision['decision_status']}",
        f"decision_mode={decision['decision_mode']}",
    ]
    payload = {
        "schema_id": "tax_advisory_explanation",
        "schema_version": "v1",
        "explanation_id": canonical_hash_v1({"decision_id": decision["decision_id"], "reason_codes": decision.get("reason_codes") or []}),
        "scope_id": decision["scope_id"],
        "decision_id": decision["decision_id"],
        "summary": f"{decision['schema_id']} produced {decision['decision_status']}",
        "details": details,
        "reason_codes": list(require_reason_codes_v1(decision.get("reason_codes") or ())),
    }
    return validate_tax_payload_v1(payload)


def build_decision_replay_report_v1(
    *,
    decision: dict[str, Any],
    replayed_decision: dict[str, Any] | None = None,
    original_dependency_fingerprint: dict[str, Any] | None = None,
    decision_time_accepted_facts: Iterable[dict[str, Any]] = (),
    current_accepted_facts: Iterable[dict[str, Any]] = (),
    current_snapshot: dict[str, Any] | None = None,
    current_resolved_policy_set: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if replayed_decision is not None:
        same = decision == replayed_decision
        payload = {
            "schema_id": "decision_replay_report",
            "schema_version": "v1",
            "report_id": canonical_hash_v1({"decision_id": decision["decision_id"], "replayed_equal": same}),
            "decision_id": decision["decision_id"],
            "snapshot_id": decision["snapshot_id"],
            "resolved_policy_set_id": decision["resolved_policy_set_id"],
            "dependency_fingerprint_id": decision["dependency_fingerprint_id"],
            "replayed_equal": same,
            "original_truth_view": {},
            "current_truth_view": {},
            "original_decision": dict(decision),
            "current_recomputed_decision": dict(replayed_decision),
            "original_decision_valid_under_current_truth": same,
            "divergence_reason_codes": [] if same else ["TAX_REPLAY_DIVERGENCE"],
            "reason_codes": ["TAX_REPORT_GENERATED"] if same else ["TAX_REPLAY_DIVERGENCE"],
        }
        return validate_tax_payload_v1(payload)
    if original_dependency_fingerprint is None or current_snapshot is None or current_resolved_policy_set is None:
        raise ValueError("TAX_REPLAY_INPUTS_MISSING")
    from constellation_2.common.tax.replay_v1 import replay_tax_decision_v1

    return replay_tax_decision_v1(
        decision=decision,
        original_dependency_fingerprint=original_dependency_fingerprint,
        decision_time_accepted_facts=decision_time_accepted_facts,
        current_accepted_facts=current_accepted_facts,
        current_snapshot=current_snapshot,
        current_resolved_policy_set=current_resolved_policy_set,
    )


def build_broker_tax_reconciliation_report_v1(
    *,
    snapshot: dict[str, Any] | None = None,
    accepted_facts: Iterable[dict[str, Any]] = (),
    broker_lot_view: Iterable[dict[str, Any]] = (),
    broker_realized_view: Iterable[dict[str, Any]] = (),
    scope_id: str | None = None,
) -> dict[str, Any]:
    if snapshot is None:
        payload = {
            "schema_id": "broker_tax_reconciliation_report",
            "schema_version": "v1",
            "report_id": canonical_hash_v1({"scope_id": scope_id or "", "status": "scaffold"}),
            "scope_id": str(scope_id or ""),
            "snapshot_id": canonical_hash_v1({"scope_id": scope_id or "", "snapshot": "scaffold"}),
            "status": "scaffold",
            "lot_report_id": canonical_hash_v1({"scope_id": scope_id or "", "lot_report": "scaffold"}),
            "realized_items": [],
            "matched_items": [],
            "unmatched_items": [],
            "mismatch_summary": {},
            "reason_codes": ["TAX_REPORT_GENERATED"],
        }
        return validate_tax_payload_v1(payload)
    from constellation_2.common.tax.reconciliation_v1 import build_broker_tax_reconciliation_report_v1 as build_operational_broker_tax_reconciliation_report_v1

    return build_operational_broker_tax_reconciliation_report_v1(
        snapshot=snapshot,
        accepted_facts=accepted_facts,
        broker_lot_view=broker_lot_view,
        broker_realized_view=broker_realized_view,
    )
