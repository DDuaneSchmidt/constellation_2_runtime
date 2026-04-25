from __future__ import annotations

from typing import Any

from constellation_2.common.tax.common_v1 import algorithm_version_v1, canonical_hash_v1, now_utc_iso_v1, validate_tax_payload_v1
from constellation_2.common.tax.decision_v1 import build_tax_decision_dependency_fingerprint_v1
from constellation_2.common.tax.reason_codes_v1 import require_reason_codes_v1


TRUTH_FIELDS_V1 = (
    "accepted_fact_set_hash",
    "lot_state_hashes",
    "wash_state_hash",
    "account_tax_regime_hash",
)

STALE_FIELDS_V1 = (
    "resolved_policy_set_id",
    "ranking_policy_hash",
    "rounding_policy_hash",
    "algorithm_version",
)


def _fingerprint_deltas_v1(current: dict[str, Any], expected: dict[str, Any]) -> tuple[str, ...]:
    mismatches: list[str] = []
    for field_name in (
        "accepted_fact_set_hash",
        "lot_state_hashes",
        "wash_state_hash",
        "account_tax_regime_hash",
        "resolved_policy_set_id",
        "ranking_policy_hash",
        "rounding_policy_hash",
        "algorithm_version",
    ):
        if current.get(field_name) != expected.get(field_name):
            mismatches.append(field_name)
    return tuple(sorted(mismatches))


def validate_tax_decision_for_execution_v1(
    *,
    decision: dict[str, Any],
    dependency_fingerprint: dict[str, Any],
    snapshot: dict[str, Any],
    resolved_policy_set: dict[str, Any],
    account_tax_regime: str,
    execution_subject_ref: str,
) -> dict[str, Any]:
    validator_version = algorithm_version_v1("execution_gate")
    expected = build_tax_decision_dependency_fingerprint_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved_policy_set,
        account_tax_regime=account_tax_regime,
        algorithm_version=str(dependency_fingerprint["algorithm_version"]),
    )
    mismatch_fields = _fingerprint_deltas_v1(dependency_fingerprint, expected)
    review_reasons: list[str] = []
    blocking_reasons: list[str] = []
    if "TAX_POLICY_BLOCK" in (decision.get("reason_codes") or ()) or str(decision.get("decision_status") or "") == "hard_blocked":
        gate_status = "EXECUTION_BLOCKED_POLICY"
        blocking_reasons.append("TAX_POLICY_BLOCK")
        reason_codes = ["TAX_POLICY_BLOCK"]
    elif any(field_name in TRUTH_FIELDS_V1 for field_name in mismatch_fields):
        gate_status = "EXECUTION_BLOCKED_TRUTH_MISMATCH"
        blocking_reasons.extend(["TAX_EXECUTION_TRUTH_MISMATCH", "TAX_DEPENDENCY_FINGERPRINT_MISMATCH"])
        reason_codes = ["TAX_EXECUTION_TRUTH_MISMATCH", "TAX_DEPENDENCY_FINGERPRINT_MISMATCH"]
    elif any(field_name in STALE_FIELDS_V1 for field_name in mismatch_fields):
        gate_status = "EXECUTION_BLOCKED_STALE"
        blocking_reasons.extend(["TAX_DECISION_STALE", "TAX_DEPENDENCY_FINGERPRINT_MISMATCH"])
        reason_codes = ["TAX_DECISION_STALE", "TAX_DEPENDENCY_FINGERPRINT_MISMATCH"]
    elif str(decision.get("decision_mode") or "") in {"SAFE_EXECUTION_ALLOWED", "PREVIEW_ONLY", "REQUIRES_OPERATOR_REVIEW"}:
        gate_status = "EXECUTION_REVIEW_REQUIRED"
        review_reasons.extend(list(require_reason_codes_v1(decision.get("reason_codes") or ("TAX_EXECUTION_REVIEW_REQUIRED", "TAX_SAFE_DEGRADED_MODE"))))
        if "TAX_EXECUTION_REVIEW_REQUIRED" not in review_reasons:
            review_reasons.append("TAX_EXECUTION_REVIEW_REQUIRED")
        reason_codes = list(require_reason_codes_v1(review_reasons))
    else:
        gate_status = "EXECUTION_ELIGIBLE"
        reason_codes = ["TAX_EXECUTION_ELIGIBLE"]
    payload = {
        "schema_id": "tax_execution_gate_result",
        "schema_version": "v1",
        "gate_result_id": canonical_hash_v1({"decision_id": decision["decision_id"], "execution_subject_ref": execution_subject_ref, "gate_status": gate_status, "mismatch_fields": mismatch_fields}),
        "decision_id": decision["decision_id"],
        "decision_family": decision["schema_id"],
        "scope_id": decision["scope_id"],
        "snapshot_id": decision["snapshot_id"],
        "resolved_policy_set_id": decision["resolved_policy_set_id"],
        "dependency_fingerprint_id": dependency_fingerprint["fingerprint_id"],
        "execution_subject_ref": execution_subject_ref,
        "validator_version": validator_version,
        "gate_status": gate_status,
        "matched": not mismatch_fields,
        "mismatch_fields": list(mismatch_fields),
        "review_reasons": sorted(set(review_reasons)),
        "blocking_reasons": sorted(set(blocking_reasons)),
        "produced_utc": now_utc_iso_v1(),
        "reason_codes": list(require_reason_codes_v1(reason_codes)),
    }
    return validate_tax_payload_v1(payload)
