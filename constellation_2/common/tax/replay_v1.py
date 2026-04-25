from __future__ import annotations

from typing import Any, Iterable

from constellation_2.common.tax.common_v1 import (
    algorithm_version_v1,
    canonical_hash_v1,
    canonical_payload_hashes_v1,
    now_utc_iso_v1,
    validate_tax_payload_v1,
)
from constellation_2.common.tax.harvest_v1 import preview_harvest_candidate_decision_v1
from constellation_2.common.tax.reason_codes_v1 import require_reason_codes_v1
from constellation_2.common.tax.routing_v1 import preview_account_routing_tax_decision_v1
from constellation_2.common.tax.truth_v1 import effective_accepted_facts_v1
from constellation_2.common.tax.decision_v1 import preview_buy_tax_decision_v1, preview_sell_tax_decision_v1


def _active_facts_for_cutoff_v1(
    *,
    accepted_facts: Iterable[dict[str, Any]],
    recorded_at_cutoff: str | None,
) -> tuple[dict[str, Any], ...]:
    facts = tuple(dict(item) for item in accepted_facts)
    if not facts:
        return ()
    cutoff = recorded_at_cutoff or max(str(item["recorded_at"]) for item in facts)
    return effective_accepted_facts_v1(accepted_facts=facts, recorded_at_cutoff=cutoff)


def _accepted_fact_set_hash_v1(accepted_facts: Iterable[dict[str, Any]]) -> str:
    return canonical_hash_v1(canonical_payload_hashes_v1(accepted_facts))


def build_tax_decision_time_truth_view_v1(
    *,
    decision: dict[str, Any],
    accepted_facts: Iterable[dict[str, Any]],
    recorded_at_cutoff: str | None = None,
) -> dict[str, Any]:
    active_facts = _active_facts_for_cutoff_v1(accepted_facts=accepted_facts, recorded_at_cutoff=recorded_at_cutoff)
    payload = {
        "schema_id": "tax_decision_time_truth_view",
        "schema_version": "v1",
        "view_id": canonical_hash_v1({"decision_id": decision["decision_id"], "accepted_fact_ids": [item["accepted_fact_id"] for item in active_facts]}),
        "decision_id": decision["decision_id"],
        "scope_id": decision["scope_id"],
        "snapshot_id": decision["snapshot_id"],
        "accepted_fact_ids": [item["accepted_fact_id"] for item in active_facts],
        "accepted_fact_set_hash": _accepted_fact_set_hash_v1(active_facts),
        "produced_utc": now_utc_iso_v1(),
        "reason_codes": ["TAX_STATE_CURRENT"],
    }
    return validate_tax_payload_v1(payload)


def build_tax_current_corrected_truth_view_v1(
    *,
    decision: dict[str, Any],
    snapshot: dict[str, Any],
    accepted_facts: Iterable[dict[str, Any]],
    recorded_at_cutoff: str | None = None,
) -> dict[str, Any]:
    active_facts = _active_facts_for_cutoff_v1(accepted_facts=accepted_facts, recorded_at_cutoff=recorded_at_cutoff)
    payload = {
        "schema_id": "tax_current_corrected_truth_view",
        "schema_version": "v1",
        "view_id": canonical_hash_v1({"decision_id": decision["decision_id"], "snapshot_id": snapshot["snapshot_id"], "accepted_fact_ids": [item["accepted_fact_id"] for item in active_facts]}),
        "decision_id": decision["decision_id"],
        "scope_id": decision["scope_id"],
        "snapshot_id": snapshot["snapshot_id"],
        "accepted_fact_ids": [item["accepted_fact_id"] for item in active_facts],
        "accepted_fact_set_hash": _accepted_fact_set_hash_v1(active_facts),
        "produced_utc": now_utc_iso_v1(),
        "reason_codes": ["TAX_STATE_CURRENT"],
    }
    return validate_tax_payload_v1(payload)


def build_tax_correction_impact_index_v1(
    *,
    correction: dict[str, Any],
    corrected_fact: dict[str, Any],
    snapshot_memberships: Iterable[dict[str, Any]] = (),
    decision_truth_views: Iterable[dict[str, Any]] = (),
) -> dict[str, Any]:
    superseded_fact_id = str(correction["superseded_fact_id"])
    affected_snapshot_ids = sorted(
        {
            str(item["snapshot_id"])
            for item in snapshot_memberships
            if superseded_fact_id in {str(fact_id) for fact_id in item.get("accepted_fact_ids") or ()}
        }
    )
    affected_decision_ids = sorted(
        {
            str(item["decision_id"])
            for item in decision_truth_views
            if superseded_fact_id in {str(fact_id) for fact_id in item.get("accepted_fact_ids") or ()}
        }
    )
    if affected_decision_ids:
        impact_class = "decision_affecting"
    elif affected_snapshot_ids:
        impact_class = "state_only"
    elif str(corrected_fact.get("fact_family") or "") == "realization_recorded":
        impact_class = "report_affecting"
    else:
        impact_class = "none"
    replay_required = bool(affected_decision_ids)
    payload = {
        "schema_id": "tax_correction_impact_index",
        "schema_version": "v1",
        "impact_index_id": canonical_hash_v1(
            {
                "correction_id": correction["correction_id"],
                "affected_snapshot_ids": affected_snapshot_ids,
                "affected_decision_ids": affected_decision_ids,
                "impact_class": impact_class,
            }
        ),
        "correction_id": correction["correction_id"],
        "scope_id": correction["scope_id"],
        "superseded_fact_id": correction["superseded_fact_id"],
        "correction_fact_id": corrected_fact["accepted_fact_id"],
        "affected_snapshot_ids": affected_snapshot_ids,
        "affected_decision_ids": affected_decision_ids,
        "replay_required": replay_required,
        "impact_class": impact_class,
        "produced_utc": now_utc_iso_v1(),
        "reason_codes": list(
            require_reason_codes_v1(
                ["TAX_REPLAY_REQUIRED"] if replay_required else ["TAX_CORRECTION_SUPERSEDES_PRIOR_FACT"]
            )
        ),
    }
    return validate_tax_payload_v1(payload)


def _recompute_decision_v1(
    *,
    decision: dict[str, Any],
    snapshot: dict[str, Any],
    resolved_policy_set: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    schema_id = str(decision.get("schema_id") or "")
    request = dict(decision.get("request") or {})
    if schema_id == "sell_tax_decision":
        return preview_sell_tax_decision_v1(snapshot=snapshot, resolved_policy_set=resolved_policy_set, request=request)
    if schema_id == "buy_tax_decision":
        return preview_buy_tax_decision_v1(snapshot=snapshot, resolved_policy_set=resolved_policy_set, request=request)
    if schema_id == "account_routing_tax_decision":
        return preview_account_routing_tax_decision_v1(snapshot=snapshot, resolved_policy_set=resolved_policy_set, request=request)
    if schema_id == "harvest_candidate_decision":
        return preview_harvest_candidate_decision_v1(snapshot=snapshot, resolved_policy_set=resolved_policy_set, request=request)
    raise ValueError(f"TAX_DECISION_REPLAY_UNSUPPORTED:{schema_id}")


def replay_tax_decision_v1(
    *,
    decision: dict[str, Any],
    original_dependency_fingerprint: dict[str, Any],
    decision_time_accepted_facts: Iterable[dict[str, Any]],
    current_accepted_facts: Iterable[dict[str, Any]],
    current_snapshot: dict[str, Any],
    current_resolved_policy_set: dict[str, Any],
) -> dict[str, Any]:
    algorithm_version_v1("decision_replay")
    original_truth_view = build_tax_decision_time_truth_view_v1(
        decision=decision,
        accepted_facts=decision_time_accepted_facts,
    )
    current_truth_view = build_tax_current_corrected_truth_view_v1(
        decision=decision,
        snapshot=current_snapshot,
        accepted_facts=current_accepted_facts,
    )
    current_decision, current_fingerprint = _recompute_decision_v1(
        decision=decision,
        snapshot=current_snapshot,
        resolved_policy_set=current_resolved_policy_set,
    )
    divergence_reason_codes: list[str] = []
    if original_truth_view["accepted_fact_set_hash"] != current_truth_view["accepted_fact_set_hash"]:
        divergence_reason_codes.append("TAX_DEPENDENCY_FINGERPRINT_MISMATCH")
    if original_dependency_fingerprint["fingerprint_id"] != current_fingerprint["fingerprint_id"]:
        divergence_reason_codes.append("TAX_DECISION_STALE")
    replayed_equal = decision == current_decision
    if not replayed_equal:
        divergence_reason_codes.append("TAX_REPLAY_DIVERGENCE")
    divergence_reason_codes = list(require_reason_codes_v1(divergence_reason_codes))
    original_decision_valid = replayed_equal and (
        original_dependency_fingerprint["fingerprint_id"] == current_fingerprint["fingerprint_id"]
    )
    payload = {
        "schema_id": "decision_replay_report",
        "schema_version": "v1",
        "report_id": canonical_hash_v1(
            {
                "decision_id": decision["decision_id"],
                "current_snapshot_id": current_snapshot["snapshot_id"],
                "current_fingerprint_id": current_fingerprint["fingerprint_id"],
                "replayed_equal": replayed_equal,
            }
        ),
        "decision_id": decision["decision_id"],
        "snapshot_id": decision["snapshot_id"],
        "resolved_policy_set_id": decision["resolved_policy_set_id"],
        "dependency_fingerprint_id": original_dependency_fingerprint["fingerprint_id"],
        "replayed_equal": replayed_equal,
        "original_truth_view": original_truth_view,
        "current_truth_view": current_truth_view,
        "original_decision": dict(decision),
        "current_recomputed_decision": current_decision,
        "original_decision_valid_under_current_truth": original_decision_valid,
        "divergence_reason_codes": divergence_reason_codes,
        "reason_codes": list(
            require_reason_codes_v1(
                ["TAX_REPORT_GENERATED"] if original_decision_valid else ["TAX_REPLAY_REQUIRED"] + divergence_reason_codes
            )
        ),
    }
    return validate_tax_payload_v1(payload)
