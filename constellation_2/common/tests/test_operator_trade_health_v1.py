from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.operator_trade_health_v1 import (
    materialize_operator_trade_health_queues_v1,
    materialize_operator_trade_health_v1,
)


DAY = "2026-04-11"
TRADE_ID = "a" * 64
CORE2_SET = "2" * 64
CORE3_SET = "3" * 64
CORE3_SET_ALT = "4" * 64


def _sha256_file(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _ref(path: Path) -> dict:
    return {"artifact_path": str(path.resolve()), "artifact_sha256": _sha256_file(path.resolve())}


def _write_stub(path: Path, name: str) -> dict:
    _write_json(path, {"name": name})
    return _ref(path)


def _prepare_fixture(
    tmp_path: Path,
    *,
    trade_identity_id: str = TRADE_ID,
    core2_set: str = CORE2_SET,
    core3_set: str = CORE3_SET,
    include_core3: bool = True,
    include_core1_audit: bool = True,
    boundary_status: str = "AUTHORIZED",
    submission_authorized: bool = True,
    final_action_posture: str = "ACTION_REQUIRED",
    action_safety_posture: str = "ACTIONABLE",
    required_actions: list[str] | None = None,
    freshness_status: str = "FRESH",
    health_state: str = "TRUSTED",
    drift_status: str = "CLEAR",
    ownership_classification: str = "CONSTELLATION_OWNED",
) -> dict:
    required_actions = ["ADD_INITIAL_PROTECTION"] if required_actions is None else list(required_actions)
    execution_root = (tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()

    raw_journal = execution_root / "broker_fact_spine_v1" / "raw_journal" / DAY / "broker_raw_evidence_envelope.v1.jsonl"
    raw_journal.parent.mkdir(parents=True, exist_ok=True)
    raw_journal.write_text("{}\n", encoding="utf-8")
    fact_refs = {}
    for schema_id in ["observation_session_fact", "observed_order_fact", "observed_order_status_fact", "observed_fill_fact", "observed_position_fact"]:
        path = execution_root / "broker_fact_spine_v1" / "fact_ledger" / DAY / f"{schema_id}.v1.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n", encoding="utf-8")
        fact_refs[schema_id] = _ref(path)

    health_path = execution_root / "reports" / "broker_observation_health_v1" / DAY / "broker_observation_health.v1.json"
    health_payload = {
        "schema_id": "broker_observation_health",
        "schema_version": "v1",
        "generated_utc": f"{DAY}T09:30:00Z",
        "day_utc": DAY,
        "evaluation_utc": f"{DAY}T09:30:00Z",
        "authority_owner": "broker_observation_health_v1",
        "trust_rule_owner": "broker_observation_health_v1",
        "execution_root_path": str(execution_root),
        "raw_journal_ref": _ref(raw_journal),
        "fact_ledger_refs": fact_refs,
        "current_state": health_state,
        "downstream_trust_verdict": health_state,
        "downstream_consumption_posture": "FAIL_CLOSED" if health_state == "BLOCKED" else ("DEGRADED_ONLY" if health_state == "DEGRADED" else "NORMAL_CONSUMPTION"),
        "may_consume_normally": health_state == "TRUSTED",
        "may_consume_with_degraded_posture": health_state in {"TRUSTED", "DEGRADED"},
        "must_fail_closed": health_state == "BLOCKED",
        "freshness_status": "FRESH" if freshness_status != "UNAVAILABLE" else "UNAVAILABLE",
        "freshness_age_seconds": 15,
        "session_status": "HEALTHY",
        "sequence_status": "OK",
        "replay_status": "NOT_OBSERVED",
        "reconnect_status": "NONE",
        "gap_status": "NONE",
        "attribution_status": "ATTRIBUTED",
        "event_identity_rule_version": "broker_fact_identity_v1",
        "blocker_codes": [] if health_state != "BLOCKED" else ["BROKER_OBSERVATION_STREAM_STALE"],
        "degraded_codes": [] if health_state != "DEGRADED" else ["BROKER_OBSERVATION_STREAM_STALE"],
        "counts": {
            "raw_record_count": 1,
            "observation_session_fact_count": 1,
            "observed_order_fact_count": 1,
            "observed_order_status_fact_count": 1,
            "observed_fill_fact_count": 1,
            "observed_position_fact_count": 1,
            "attributed_raw_record_count": 1,
            "partial_raw_record_count": 0,
            "ambiguous_raw_record_count": 0,
            "foreign_raw_record_count": 0,
            "unresolved_raw_record_count": 0,
            "duplicate_fact_count": 0,
            "replay_overlap_fact_count": 0,
            "conflicting_duplicate_fact_count": 0,
            "replay_session_fact_count": 0,
            "replay_uncertain_fact_count": 0,
        },
        "summary": "Core 1 health fixture.",
        "recommended_operator_action": "NONE",
    }
    _write_json(health_path, health_payload)
    health_ref = _ref(health_path)

    if include_core1_audit:
        audit_path = execution_root / "reports" / "broker_fact_spine_audit_v1" / DAY / "broker_fact_spine_audit.v1.json"
        _write_json(
            audit_path,
            {
                "schema_id": "broker_fact_spine_audit",
                "schema_version": "v1",
                "generated_utc": f"{DAY}T09:30:05Z",
                "day_utc": DAY,
                "authority_owner": "broker_fact_spine_v1",
                "execution_root_path": str(execution_root),
                "source_input_ref": health_ref,
                "raw_journal_ref": _ref(raw_journal),
                "fact_ledger_refs": fact_refs,
                "health_ref": health_ref,
                "trust_dependency_ref": health_ref,
                "current_state": health_state,
                "downstream_consumption_posture": "FAIL_CLOSED" if health_state == "BLOCKED" else ("DEGRADED_ONLY" if health_state == "DEGRADED" else "NORMAL_CONSUMPTION"),
                "latest_observed_utc": f"{DAY}T09:29:59Z",
                "summary": "Core 1 audit fixture.",
                "counts": {"rows": 1},
                "derived_only": True,
            },
        )

    core2_trade_dir = execution_root / "reconciled_trade_state_v1" / "materializations" / DAY / core2_set / "trades" / trade_identity_id
    trade_identity_path = core2_trade_dir / "trade_identity.v1.json"
    trade_identity_payload = {
        "schema_id": "trade_identity",
        "schema_version": "v1",
        "authority_owner": "trade_identity_resolution_v1",
        "materialization_set_id": core2_set,
        "day_utc": DAY,
        "evaluation_utc": f"{DAY}T09:31:00Z",
        "trade_identity_id": trade_identity_id,
        "environment": "PAPER",
        "sleeve_id": "PRIMARY",
        "account_id": "DU1234567",
        "instrument_identity": {"symbol": "SPY", "sec_type": "STK", "exchange": "SMART", "currency": "USD", "raw_summary": "SPY"},
        "ownership_classification": ownership_classification,
        "ownership_reason_codes": ["OWNED"],
        "lineage_attachment_refs": {"order_ids": ["1001"], "perm_ids": ["2001"], "execution_ids": ["3001"], "fact_record_ids": ["b" * 64]},
        "open_close_continuity": {"continuity_key": "SPY|1", "segment_index": 1, "continuity_status": "OPEN", "opened_by_fact_record_id": "b" * 64, "closed_by_fact_record_id": ""},
        "ambiguity_state": "NONE",
        "blocker_state": "CLEAR",
        "blocker_codes": [],
        "derived_only": False,
    }
    _write_json(trade_identity_path, trade_identity_payload)
    trade_identity_ref = _ref(trade_identity_path)

    state_path = core2_trade_dir / "incorporated_broker_trade_state.v1.json"
    state_payload = {
        "schema_id": "incorporated_broker_trade_state",
        "schema_version": "v1",
        "authority_owner": "incorporated_broker_trade_state_v1",
        "truth_owner_status": "CANONICAL_CURRENT_TRUTH_OWNER",
        "materialization_set_id": core2_set,
        "day_utc": DAY,
        "evaluation_utc": f"{DAY}T09:31:00Z",
        "trade_identity_ref": {**trade_identity_ref, "trade_identity_id": trade_identity_id},
        "environment": "PAPER",
        "sleeve_id": "PRIMARY",
        "account_id": "DU1234567",
        "instrument_identity": {"symbol": "SPY", "sec_type": "STK", "exchange": "SMART", "currency": "USD", "raw_summary": "SPY"},
        "ownership_classification": ownership_classification,
        "current_quantity": "1",
        "side": "LONG",
        "average_cost": "100.00",
        "incorporated_fills": [],
        "current_working_orders": [],
        "terminal_order_lineage": [],
        "orphan_order_facts": [],
        "last_reconciled_fact_boundary": {"max_journal_sequence_number": 1, "latest_observed_utc": f"{DAY}T09:30:59Z", "raw_journal_path": str(raw_journal), "raw_journal_sha256": _sha256_file(raw_journal)},
        "drift_basis": {"position_quantity": "1", "net_fill_quantity": "1", "position_vs_fill_consistent": drift_status != "DRIFTED", "mismatch_quantity": "0", "reason_codes": [] if drift_status != "DRIFTED" else ["TRADE_RECON_POSITION_ORDER_FILL_INCONSISTENCY"]},
        "freshness_basis": {"core1_downstream_trust_verdict": health_state, "core1_freshness_status": freshness_status, "evaluation_utc": f"{DAY}T09:31:00Z", "latest_observed_utc": f"{DAY}T09:30:59Z", "age_seconds": 15 if freshness_status == "FRESH" else (180 if freshness_status == "STALE" else 900)},
        "first_blocker": "" if health_state != "BLOCKED" else "TRADE_RECON_CURRENT_TRUTH_TOO_STALE_FOR_DOWNSTREAM_ACTION_USE",
        "ambiguity_state": "NONE",
        "blocker_codes": [] if health_state != "BLOCKED" else ["TRADE_RECON_CURRENT_TRUTH_TOO_STALE_FOR_DOWNSTREAM_ACTION_USE"],
        "degraded_codes": [] if health_state != "DEGRADED" else ["TRADE_RECON_CURRENT_TRUTH_TOO_STALE_FOR_DOWNSTREAM_ACTION_USE"],
        "last_successful_reconciliation_utc": f"{DAY}T09:31:00Z",
        "upstream_core1_evidence_refs": {"raw_journal_ref": _ref(raw_journal), "fact_ledger_refs": {k: v for k, v in fact_refs.items() if k != "observation_session_fact"}, "health_ref": health_ref},
        "derived_only": False,
    }
    _write_json(state_path, state_payload)
    state_ref = _ref(state_path)

    description_path = core2_trade_dir / "reconciled_trade_description.v1.json"
    _write_json(
        description_path,
        {
            "schema_id": "reconciled_trade_description",
            "schema_version": "v1",
            "authority_owner": "reconciled_trade_description_v1",
            "materialization_set_id": core2_set,
            "day_utc": DAY,
            "evaluation_utc": f"{DAY}T09:31:00Z",
            "trade_identity_id": trade_identity_id,
            "incorporated_state_ref": state_ref,
            "lifecycle_status": "OPEN_LONG",
            "protection_status": "PROTECTION_NOT_OBSERVED",
            "reconciliation_descriptive_status": "BLOCKED" if health_state == "BLOCKED" else ("DEGRADED" if health_state == "DEGRADED" else "TRUSTED"),
            "downstream_posture": "BLOCKED" if health_state == "BLOCKED" else ("DEGRADED" if health_state == "DEGRADED" else "SAFE"),
            "derived_from_incorporated_state": True,
            "derived_only": True,
        },
    )
    description_ref = _ref(description_path)

    core2_health_path = core2_trade_dir / "reconciliation_health.v1.json"
    _write_json(
        core2_health_path,
        {
            "schema_id": "reconciliation_health",
            "schema_version": "v1",
            "authority_owner": "reconciliation_health_v1",
            "materialization_set_id": core2_set,
            "day_utc": DAY,
            "evaluation_utc": f"{DAY}T09:31:00Z",
            "trade_identity_id": trade_identity_id,
            "incorporated_state_ref": state_ref,
            "current_state": health_state,
            "blocker_reasons": [] if health_state != "BLOCKED" else ["TRADE_RECON_CURRENT_TRUTH_TOO_STALE_FOR_DOWNSTREAM_ACTION_USE"],
            "degraded_reasons": [] if health_state != "DEGRADED" else ["TRADE_RECON_CURRENT_TRUTH_TOO_STALE_FOR_DOWNSTREAM_ACTION_USE"],
            "freshness_status": freshness_status,
            "ambiguity_status": "CLEAR",
            "drift_status": drift_status,
            "insufficient_evidence_status": "SUFFICIENT",
            "downstream_action_posture": "BLOCKED" if health_state == "BLOCKED" else ("DEGRADED" if health_state == "DEGRADED" else "SAFE"),
            "derived_only": True,
        },
    )
    core2_health_ref = _ref(core2_health_path)

    core2_provenance_path = core2_trade_dir / "reconciliation_provenance.v1.json"
    _write_json(
        core2_provenance_path,
        {
            "schema_id": "reconciliation_provenance",
            "schema_version": "v1",
            "authority_owner": "reconciliation_provenance_v1",
            "materialization_set_id": core2_set,
            "day_utc": DAY,
            "evaluation_utc": f"{DAY}T09:31:00Z",
            "trade_identity_id": trade_identity_id,
            "incorporated_state_ref": state_ref,
            "rule_versions": {"core2_rule_pack": "core2_final_strengthened_architecture_v1", "identity_rules": "trade_identity_resolution_v1", "description_rules": "reconciled_trade_description_v1", "health_rules": "reconciliation_health_v1"},
            "upstream_core1_evidence_refs": {"raw_journal_ref": _ref(raw_journal), "fact_ledger_refs": {k: v for k, v in fact_refs.items() if k != "observation_session_fact"}, "health_ref": health_ref},
            "incorporated_fact_refs": [],
            "ignored_fact_refs": [],
            "blocked_fact_refs": [],
            "prior_state_change_summary": {"change_status": "INITIAL_MATERIALIZATION", "prior_state_ref": {"artifact_path": "/tmp/prior.json", "artifact_sha256": "f" * 64}, "changed_fields": [], "summary": "initial"},
            "derived_only": True,
        },
    )
    core2_provenance_ref = _ref(core2_provenance_path)

    core3_trade_dir = execution_root / "lifecycle_action_authority_v1" / "materializations" / DAY / core3_set / "trades" / trade_identity_id
    gate_ref = _write_stub(core3_trade_dir / "global_actionability_gate.v1.json", "gate")
    candidate_ref = _write_stub(core3_trade_dir / "candidate_action_set.v1.json", "candidate")
    policy_ref = _write_stub(core3_trade_dir / "policy_basis.json", "policy")
    prior_auth_ref = _write_stub(core3_trade_dir / "prior_authority.json", "prior")

    if include_core3:
        core3_authority_path = core3_trade_dir / "lifecycle_action_authority.v1.json"
        _write_json(
            core3_authority_path,
            {
                "schema_id": "lifecycle_action_authority",
                "schema_version": "v1",
                "authority_owner": "lifecycle_action_authority_v1",
                "canonical_owner_status": "CANONICAL_ACTION_AUTHORITY_OWNER",
                "materialization_set_id": core3_set,
                "day_utc": DAY,
                "evaluated_at_utc": f"{DAY}T09:32:00Z",
                "trade_identity_ref": {**trade_identity_ref, "trade_identity_id": trade_identity_id},
                "environment": "PAPER",
                "sleeve_id": "PRIMARY",
                "account_id": "DU1234567",
                "ownership_classification": ownership_classification,
                "final_action_posture": final_action_posture,
                "allowed_actions": ["HOLD"],
                "required_actions": required_actions,
                "forbidden_actions": [],
                "blocked_actions": ["BLOCK_ALL_ACTIONS"] if final_action_posture == "BLOCKED" else [],
                "action_safety_posture": action_safety_posture,
                "first_blocker": "NONE",
                "ambiguity_state": "NONE",
                "policy_basis_refs": [policy_ref],
                "upstream_core2_refs": {
                    "trade_identity_ref": trade_identity_ref,
                    "incorporated_state_ref": state_ref,
                    "reconciled_description_ref": description_ref,
                    "reconciliation_health_ref": core2_health_ref,
                    "reconciliation_provenance_ref": core2_provenance_ref,
                },
                "gate_ref": gate_ref,
                "candidate_set_ref": candidate_ref,
                "rule_version": {"policy_contract_id": "C2_POST_ENTRY_ACTION_POLICY_CONTRACT_V1", "policy_contract_version": 1, "conflict_contract_id": "C2_ACTION_CONFLICT_RESOLUTION_CONTRACT_V1", "conflict_contract_version": 1},
                "derived_only": False,
            },
        )
        _write_json(
            core3_trade_dir / "action_decision_provenance.v1.json",
            {
                "schema_id": "action_decision_provenance",
                "schema_version": "v1",
                "authority_owner": "action_decision_provenance_v1",
                "materialization_set_id": core3_set,
                "day_utc": DAY,
                "evaluated_at_utc": f"{DAY}T09:32:00Z",
                "trade_identity_id": trade_identity_id,
                "core2_input_refs_used": [trade_identity_ref, state_ref, description_ref, core2_health_ref, core2_provenance_ref],
                "core2_fields_read": ["ownership_classification", "freshness_status", "drift_status"],
                "candidate_actions_generated": [{"action_code": "HOLD", "reason": "safe"}],
                "rejected_candidates": [],
                "blocker_rules_fired": [],
                "conflict_rule_applied": {"rule_id": "NONE", "summary": "none"},
                "final_posture_rationale": "fixture",
                "policy_versions_used": {"gate_contract_id": "C2_GLOBAL_ACTIONABILITY_GATE_CONTRACT_V1", "gate_contract_version": 1, "policy_contract_id": "C2_POST_ENTRY_ACTION_POLICY_CONTRACT_V1", "policy_contract_version": 1, "conflict_contract_id": "C2_ACTION_CONFLICT_RESOLUTION_CONTRACT_V1", "conflict_contract_version": 1},
                "prior_state_comparison": {"comparison_status": "INITIAL_MATERIALIZATION", "prior_authority_ref": prior_auth_ref, "changed_fields": []},
                "derived_only": True,
            },
        )

    boundary_path = execution_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json"
    _write_json(
        boundary_path,
        {
            "schema_id": "submit_boundary_status",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": DAY,
            "session_id": f"paper_session:{DAY}:PAPER",
            "submission_authorized": submission_authorized,
            "boundary_status": boundary_status,
            "required_boundary_checks": [],
            "failed_checks": [],
            "blocking_codes": [] if boundary_status == "AUTHORIZED" and submission_authorized else ["SUBMIT_BOUNDARY_BLOCKED"],
            "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
            "produced_at_utc": f"{DAY}T09:32:30Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "paper_account": "DU1234567",
        },
    )

    return {
        "execution_root": execution_root,
        "boundary_path": boundary_path,
        "trade_identity_id": trade_identity_id,
        "core2_set": core2_set,
        "core3_set": core3_set,
    }


def test_snapshot_binding_is_deterministic_for_same_refs(tmp_path: Path) -> None:
    fixture = _prepare_fixture(tmp_path)
    first = materialize_operator_trade_health_v1(repo_root=REPO_ROOT, execution_root_path=fixture["execution_root"], day_utc=DAY, trade_identity_id=fixture["trade_identity_id"], core2_materialization_set_id=fixture["core2_set"], core3_materialization_set_id=fixture["core3_set"], evaluation_utc=f"{DAY}T09:33:00Z")
    second = materialize_operator_trade_health_v1(repo_root=REPO_ROOT, execution_root_path=fixture["execution_root"], day_utc=DAY, trade_identity_id=fixture["trade_identity_id"], core2_materialization_set_id=fixture["core2_set"], core3_materialization_set_id=fixture["core3_set"], evaluation_utc=f"{DAY}T09:33:00Z")
    assert first.materialization_set_id == second.materialization_set_id
    snapshot = json.loads(first.snapshot_binding_path.read_text(encoding="utf-8"))
    assert snapshot["binding_status"] == "BOUND"
    assert snapshot["coherence_status"] == "COHERENT"


def test_boundary_blocked_remains_blocked(tmp_path: Path) -> None:
    fixture = _prepare_fixture(tmp_path, boundary_status="BLOCKED", submission_authorized=False, final_action_posture="ACTION_REQUIRED")
    result = materialize_operator_trade_health_v1(repo_root=REPO_ROOT, execution_root_path=fixture["execution_root"], day_utc=DAY, trade_identity_id=fixture["trade_identity_id"], core2_materialization_set_id=fixture["core2_set"], core3_materialization_set_id=fixture["core3_set"], evaluation_utc=f"{DAY}T09:34:00Z")
    assert result.summary["overall_operator_state"] == "BLOCKED"
    assert result.summary["blocked_reason_class"] == "BLOCKED_BY_BOUNDARY"
    assert result.summary["highest_severity"] == "CRITICAL"
    assert result.summary["required_operator_action"] == "REMEDIATE_BLOCKED_BOUNDARY"


def test_review_required_and_stale_warning_remain_explicit(tmp_path: Path) -> None:
    fixture = _prepare_fixture(tmp_path, final_action_posture="REVIEW_REQUIRED", action_safety_posture="DEGRADED_REVIEW_REQUIRED", required_actions=[], freshness_status="STALE", health_state="DEGRADED")
    result = materialize_operator_trade_health_v1(repo_root=REPO_ROOT, execution_root_path=fixture["execution_root"], day_utc=DAY, trade_identity_id=fixture["trade_identity_id"], core2_materialization_set_id=fixture["core2_set"], core3_materialization_set_id=fixture["core3_set"], evaluation_utc=f"{DAY}T09:35:00Z")
    assert result.summary["overall_operator_state"] == "REVIEW_REQUIRED"
    assert result.summary["review_reason_class"] == "ACTION_AUTHORITY_REVIEW_REQUIRED"
    assert result.summary["stale_degraded_class"] == "STALE_TRUTH_WARNING"


def test_missing_core3_artifact_stays_fail_transparent(tmp_path: Path) -> None:
    fixture = _prepare_fixture(tmp_path, include_core3=False)
    result = materialize_operator_trade_health_v1(repo_root=REPO_ROOT, execution_root_path=fixture["execution_root"], day_utc=DAY, trade_identity_id=fixture["trade_identity_id"], core2_materialization_set_id=fixture["core2_set"], core3_materialization_set_id=fixture["core3_set"], evaluation_utc=f"{DAY}T09:36:00Z")
    assert result.summary["overall_operator_state"] == "DEGRADED_INCOMPLETE"
    assert result.summary["review_reason_class"] == "SNAPSHOT_COHERENCE_REVIEW_REQUIRED"
    provenance = json.loads(result.provenance_path.read_text(encoding="utf-8"))
    assert provenance["snapshot_coherence_result"]["binding_status"] in {"BLOCKED", "DEGRADED"}
    assert provenance["omitted_or_degraded_elements"]


def test_timeline_is_derived_from_prior_operator_health(tmp_path: Path) -> None:
    fixture = _prepare_fixture(tmp_path, final_action_posture="ACTION_REQUIRED")
    first = materialize_operator_trade_health_v1(repo_root=REPO_ROOT, execution_root_path=fixture["execution_root"], day_utc=DAY, trade_identity_id=fixture["trade_identity_id"], core2_materialization_set_id=fixture["core2_set"], core3_materialization_set_id=fixture["core3_set"], evaluation_utc=f"{DAY}T09:37:00Z")
    fixture_alt = _prepare_fixture(tmp_path, trade_identity_id=fixture["trade_identity_id"], core3_set=CORE3_SET_ALT, final_action_posture="REVIEW_REQUIRED", action_safety_posture="DEGRADED_REVIEW_REQUIRED", required_actions=[])
    second = materialize_operator_trade_health_v1(repo_root=REPO_ROOT, execution_root_path=fixture_alt["execution_root"], day_utc=DAY, trade_identity_id=fixture_alt["trade_identity_id"], core2_materialization_set_id=fixture_alt["core2_set"], core3_materialization_set_id=fixture_alt["core3_set"], evaluation_utc=f"{DAY}T09:38:00Z")
    timeline = json.loads(second.timeline_view_path.read_text(encoding="utf-8"))
    assert first.materialization_set_id != second.materialization_set_id
    assert timeline["timeline_classification"] == "MATERIAL_CHANGE"
    assert timeline["material_change_entries"]
    assert timeline["narrative_lines"]


def test_queue_derivation_uses_operator_health_and_provenance_only(tmp_path: Path) -> None:
    fixture = _prepare_fixture(tmp_path, trade_identity_id=TRADE_ID, final_action_posture="ACTION_REQUIRED")
    _ = materialize_operator_trade_health_v1(repo_root=REPO_ROOT, execution_root_path=fixture["execution_root"], day_utc=DAY, trade_identity_id=fixture["trade_identity_id"], core2_materialization_set_id=fixture["core2_set"], core3_materialization_set_id=fixture["core3_set"], evaluation_utc=f"{DAY}T09:39:00Z")
    report_a = materialize_operator_trade_health_queues_v1(repo_root=REPO_ROOT, execution_root_path=fixture["execution_root"], day_utc=DAY)
    boundary_doc = json.loads(Path(fixture["boundary_path"]).read_text(encoding="utf-8"))
    boundary_doc["boundary_status"] = "BLOCKED"
    boundary_doc["submission_authorized"] = False
    boundary_doc["blocking_codes"] = ["SUBMIT_BOUNDARY_BLOCKED"]
    _write_json(Path(fixture["boundary_path"]), boundary_doc)
    report_b = materialize_operator_trade_health_queues_v1(repo_root=REPO_ROOT, execution_root_path=fixture["execution_root"], day_utc=DAY)
    assert report_a.queue_set_id == report_b.queue_set_id
    assert report_a.summary["session_summary"]["action_required_count"] == report_b.summary["session_summary"]["action_required_count"]
    assert report_a.summary["session_summary"]["blocked_count"] == report_b.summary["session_summary"]["blocked_count"]
