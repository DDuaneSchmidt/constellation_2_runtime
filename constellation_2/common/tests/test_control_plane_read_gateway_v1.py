from __future__ import annotations

import json
from pathlib import Path

import pytest

from constellation_2.common.control_plane_read_boundary_v1 import (
    READ_DOMINANCE_ACTIVE_PATHS,
    direct_control_plane_read_hits_v1,
)
from constellation_2.common.control_plane_read_gateway_v1 import (
    ControlPlaneReadRefV1,
    read_control_plane_collection_v1,
    read_control_plane_semantic_v1,
    read_control_plane_surface_v1,
)


REPO_ROOT = Path(__file__).resolve().parents[3]

HEX64 = "a" * 64
GIT_SHA40 = "a" * 40


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _runtime_control_record_payload(*, day_utc: str, produced_utc: str, effective_at_utc: str, record_id: str) -> dict[str, object]:
    return {
        "schema_id": "runtime_control_record",
        "schema_version": "v1",
        "record_id": record_id,
        "runtime_control_record_id": record_id,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "contract_version": "v1",
        "capability_scope": "paper_trade_submit_entry_v1",
        "environment": "PAPER",
        "ib_account": "DU123456",
        "sleeve_id": "PRIMARY",
        "control_state": "ALLOW",
        "effective_at_utc": effective_at_utc,
        "kill_switch_state": "ALLOW",
        "allow_entries": True,
        "readiness_state": "OK",
        "readiness_ok": True,
        "readiness_as_of_utc": produced_utc,
        "readiness_expires_utc": "",
        "evidence_fingerprint": HEX64,
        "source_artifact_refs": [
            {"path": "/tmp/a.json", "sha256": HEX64},
            {"path": "/tmp/b.json", "sha256": HEX64},
        ],
        "reason_codes": [],
        "canonical_json_hash": HEX64,
    }


def _runtime_control_decision_payload(*, day_utc: str, produced_utc: str, effective_at_utc: str, decision_id: str) -> dict[str, object]:
    return {
        "schema_id": "runtime_control_decision",
        "schema_version": "v1",
        "record_id": decision_id,
        "runtime_control_decision_id": decision_id,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "contract_version": "v1",
        "capability_scope": "paper_trade_submit_entry_v1",
        "environment": "PAPER",
        "ib_account": "DU123456",
        "sleeve_id": "PRIMARY",
        "outcome": "allow",
        "candidate_control_state": "ALLOW",
        "effective_at_utc": effective_at_utc,
        "source_artifact_refs": ["/tmp/a.json"],
        "reason_codes": [],
        "evidence_fingerprint": HEX64,
        "canonical_json_hash": HEX64,
    }


def _runtime_control_run_envelope_payload(
    *, day_utc: str, produced_utc: str, run_id: str, record_id: str = HEX64
) -> dict[str, object]:
    return {
        "schema_id": "runtime_control_run_envelope",
        "schema_version": "v1",
        "record_id": record_id,
        "run_id": run_id,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "contract_version": "v1",
        "capability_scope": "paper_trade_submit_entry_v1",
        "environment": "PAPER",
        "ib_account": "DU123456",
        "sleeve_id": "PRIMARY",
        "run_outcome": "allow",
        "stage_status": {"decision": "allow"},
        "artifact_refs": {"submission_id": "sub-1"},
        "reason_codes": [],
        "canonical_json_hash": HEX64,
    }


def _gate_stack_verdict_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "gate_stack_verdict",
        "schema_version": "v1",
        "produced_utc": "2026-04-20T12:00:00Z",
        "day_utc": day_utc,
        "producer": {"repo": "constellation", "module": "ops/tools/run_gate_stack_verdict_v1.py", "git_sha": GIT_SHA40},
        "status": "PASS",
        "reason_codes": [],
        "gates": [
            {
                "gate_id": "gate-1",
                "gate_class": "CLASS4_ADVISORY",
                "required": True,
                "blocking": False,
                "status": "PASS",
                "artifact_path": "/tmp/gate.json",
                "artifact_sha256": HEX64,
                "reason_codes": [],
            }
        ],
        "input_manifest": [{"type": "gate", "path": "/tmp/gate.json", "sha256": HEX64}],
    }


def _platform_readiness_payload(day_utc: str) -> dict[str, object]:
    metric = {"raw_value": 90, "unit": "score", "display_value": "90"}
    return {
        "schema_id": "C2_PLATFORM_READINESS_V1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": "2026-04-20T12:00:00Z",
        "platform_readiness_state": "READY",
        "platform_readiness_score": 90,
        "platform_readiness_grade": "A",
        "score_threshold_ready": 80,
        "platform_promotion_candidate": True,
        "root_blockers": [],
        "derived_blockers": [],
        "aggregate_blocker_summary": {},
        "readiness_summary": "Ready",
        "promotion_decision_basis": "Threshold met",
        "top_blockers_ordered": [],
        "minimum_conditions_summary": [],
        "current_vs_required": {},
        "promotion_checklist": {
            "must_be_true": [],
            "currently_false": [],
            "gating_conditions": [],
            "informational_conditions": [],
        },
        "smallest_clearance_set": [],
        "blocker_dependency_order": [],
        "score_contribution": [],
        "metric_views": {
            "platform_readiness_score": metric,
            "score_threshold_ready": {"raw_value": 80, "unit": "score", "display_value": "80"},
            "bug_velocity_7d_avg": {"raw_value": 0, "unit": "bugs/day", "display_value": "0"},
            "recurrence_rate": {"raw_value": 0, "unit": "rate", "display_value": "0"},
            "diagnostic_stability_rate": {"raw_value": 100, "unit": "percent", "display_value": "100"},
        },
        "policy_values": {},
        "evidence_paths": [],
        "bug_stability_summary": "Stable",
        "calibration_support": {},
    }


def _execution_reconciliation_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "C2_EXECUTION_RECONCILIATION_V1",
        "schema_version": 1,
        "produced_utc": "2026-04-20T12:00:00Z",
        "day_utc": day_utc,
        "producer": {"repo": "constellation", "git_sha": GIT_SHA40, "module": "ops/tools/run_execution_reconciliation_day_v1.py"},
        "status": "PASS",
        "semantic_status": "FULLY_OBSERVED_AND_CONFIRMED",
        "reason_codes": [],
        "input_manifest": [{"type": "submission", "path": "/tmp/submission.json", "sha256": HEX64}],
        "checks": [{"check_id": "check-1", "pass": True, "details": "ok"}],
        "audit_guidance": {
            "preferred_consumption_mode": "IMMUTABLE_VERSIONED_SNAPSHOT",
            "versioned_snapshot_root": "/tmp/versioned",
            "alias_path": "/tmp/alias",
            "alias_is_best_effort": True,
        },
        "runtime_ledger_projection": {
            "ledger_path": "/tmp/runtime_ledger.jsonl",
            "ledger_sha256": HEX64,
            "derived_from_runtime_ledger": True,
            "event_types": ["submission"],
            "event_count": 1,
            "appended_event_ids": [HEX64],
            "reused_event_ids": [],
            "projection_notice": "ok",
        },
        "canonical_json_hash": HEX64,
    }


def _economic_state_build_payload(day_utc: str, context_hash: str) -> dict[str, object]:
    return {
        "schema_id": "economic_state_build",
        "schema_version": "v1",
        "day_utc": day_utc,
        "sleeve_id": "PRIMARY",
        "mode": "PAPER",
        "account_id": "DU123456",
        "operation_type": "fresh_paper_entry_v1",
        "context_hash": context_hash,
        "dependency_results": [{"dependency_id": "cash_ledger_snapshot_v1", "status": "PRESENT"}],
        "constitutional_dependency_declaration": {"declared_dependency_artifacts": ["cash_ledger_snapshot_v1"]},
        "constitutional_lineage": {"artifact_type": "economic_state_build_v1", "authority_id": "economic_state_build_v1"},
        "closure_status": "COMPLETE",
        "economic_evaluation": {
            "benchmark_state": "PASS",
            "performance_state": "PASS",
            "attribution_state": "PASS",
            "risk_state": "PASS",
            "r_metrics_state": "PASS",
            "tax_economic_state": "PASS",
            "capital_efficiency_state": "PASS",
            "reallocation_signal_state": "PASS",
            "input_manifest": [_artifact_manifest_row("economic_input")],
            "reason_codes": [],
            "upstream_truth": {"present": True},
        },
        "generated_utc": f"{day_utc}T00:00:00Z",
    }


def _economic_state_package_payload(day_utc: str, context_hash: str) -> dict[str, object]:
    return {
        "schema_id": "economic_state_package",
        "schema_version": "v1",
        "day_utc": day_utc,
        "sleeve_id": "PRIMARY",
        "mode": "PAPER",
        "account_id": "DU123456",
        "operation_type": "fresh_paper_entry_v1",
        "context_hash": context_hash,
        "package_hash": HEX64,
        "sealed": True,
        "sealed_utc": f"{day_utc}T00:05:00Z",
        "build_ref": {"path": f"/tmp/{context_hash}/economic_state_build.v1.json", "sha256": HEX64},
        "economic_evaluation_ref": {"logical_name": "economic_state_build_v1.economic_evaluation", "path": f"/tmp/{context_hash}/economic_state_build.v1.json"},
        "manifest_ref": "/tmp/manifest.json",
        "global_context_package_ref": {"path": "/tmp/global_context_package.v1.json", "sha256": HEX64},
        "dependency_refs": [{"dependency_id": "cash_ledger_snapshot_v1", "path": "/tmp/cash.json", "sha256": HEX64}],
        "constitutional_dependency_declaration": {"declared_dependency_artifacts": ["cash_ledger_snapshot_v1"]},
        "constitutional_lineage": {"artifact_type": "economic_state_package_v1", "authority_id": "economic_state_package_v1"},
    }


def _producer_payload(module: str = "test.module") -> dict[str, object]:
    return {"repo": "constellation", "module": module, "git_sha": GIT_SHA40}


def _metric_view(raw_value: int | None, unit: str, display_value: str) -> dict[str, object]:
    return {"raw_value": raw_value, "unit": unit, "display_value": display_value}


def _artifact_manifest_row(artifact_type: str, path: str = "/tmp/input.json") -> dict[str, object]:
    return {"type": artifact_type, "path": path, "sha256": HEX64}


def _day_authority_decision_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "day_authority_decision",
        "schema_version": "v1",
        "decision_id": f"day-authority-{day_utc}",
        "trading_day": day_utc,
        "decision_state": "OPEN",
        "heartbeat": {"status": "READY", "source_ref": "results.ib_api_handshake"},
        "first_failure": None,
        "stage": "PRE_ORCHESTRATION_PREFLIGHT",
        "orchestrator_started": False,
        "blocking_class": "NONE",
        "blocking_evidence": [],
        "missing_or_invalid_prerequisite_refs": [],
        "authority_attestation_refs_used": [],
        "constitutional_dependency_declaration": {
            "artifact_type": "day_authority_decision_v1",
            "artifact_class": "DERIVED_DECISION",
            "authority_id": "day_authority_decision_v1",
            "declared_dependency_artifacts": [],
            "dependency_refs": [],
        },
        "constitutional_lineage": {
            "artifact_type": "day_authority_decision_v1",
            "artifact_version": "v1",
            "artifact_class": "DERIVED_DECISION",
            "authority_id": "day_authority_decision_v1",
            "producer_id": "test.module",
            "generated_at_utc": f"{day_utc}T00:00:00Z",
            "effective_at_utc": f"{day_utc}T00:00:00Z",
            "finality_state": "PROVISIONAL",
            "input_artifact_refs": [],
            "policy_snapshot_refs": [],
            "code_version": GIT_SHA40,
            "run_id": f"day_authority_decision:{day_utc}",
        },
        "compatibility_status": {
            "mapping_status": "OK",
            "schema_status": "OK",
            "reader_compatibility_status": "OK",
            "details": [],
        },
        "diagnostic_warnings": [],
        "emitted_at": f"{day_utc}T00:00:00Z",
        "run_metadata": {
            "truth_root": "/tmp/truth",
            "producer_module": "test.module",
            "producer_git_sha": GIT_SHA40,
        },
    }


def _pre_open_row(artifact_id: str, day_utc: str, *, role_class: str = "REQUIRED_BINDING_INPUT") -> dict[str, object]:
    return {
        "artifact_id": artifact_id,
        "artifact_name": artifact_id,
        "required": True,
        "role_class": role_class,
        "classification": "PRE_OPEN_PREREQUISITE",
        "canonical_path": f"/tmp/{artifact_id}.json",
        "authority_path": f"/tmp/{artifact_id}.json",
        "path_family": "CANONICAL_RUNTIME_TRUTH_SUBPATH",
        "observed_status": "OK",
        "result_status": "PASS",
        "blocker_codes": [],
        "blocking_reason_code": "",
        "schema_status": "VALID",
        "schema_ref": "governance/test.schema.json",
        "freshness_rule": "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        "freshness_status": "CURRENT",
        "target_day_expected": day_utc,
        "target_day_observed": day_utc,
        "date_binding_status": "MATCH",
        "date_binding_value": day_utc,
        "provenance_required": True,
        "provenance_summary": {
            "required": True,
            "present": True,
            "fields_present": ["producer.module"],
            "source": "producer",
        },
        "closure_status": "CLOSED",
        "producer": {"module": "test", "git_sha": "abc123"},
        "source_refs": [],
        "observed_dependency_artifacts": [],
    }


def _pre_open_bundle_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "pre_open_bundle",
        "schema_version": "v1",
        "target_day": day_utc,
        "active_day_observed": day_utc,
        "active_day_alignment_status": "MATCH",
        "owner_tool": "ops/tools/run_pre_open_materializer_v1.py",
        "materialization_state": "COMPLETE",
        "completion_state": "COMPLETE",
        "blocking_reason_codes": [],
        "prerequisite_checks": [
            _pre_open_row("ib_api_handshake_latest_pointer_v1", day_utc),
            _pre_open_row("global_kill_switch_state_v1", day_utc, role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
        "producer_results": [],
        "market_calendar_status": {
            "available": True,
            "artifact_path": f"/tmp/{day_utc}/market_calendar.v1.json",
            "artifact_sha256": HEX64,
            "severity": "INFO",
            "required_target_day": day_utc,
            "reason_codes": [],
        },
        "built_at_utc": f"{day_utc}T00:00:00Z",
        "producer": _producer_payload("constellation_2/common/pre_open_materializer_v1.py"),
    }


def _paper_startup_intent_input_convergence_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "paper_startup_intent_input_convergence_v1",
        "schema_version": 1,
        "generated_utc": f"{day_utc}T00:00:00Z",
        "target_day": day_utc,
        "environment": "PAPER",
        "ib_account": "DU123456",
        "sleeve_id": "PRIMARY",
        "sleeve_truth_root": "/tmp/truth_sleeves/PRIMARY/PAPER",
        "authority_root": "/tmp/truth",
        "convergence_status": "SUCCESS",
        "required_inputs": ["paper_policy_verdict_v1"],
        "artifact_results": [],
        "blocker_chain": [],
        "source_refs": [],
    }


def _paper_startup_authorization_convergence_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "paper_startup_authorization_convergence_v1",
        "schema_version": 1,
        "binding_classification": "SUBSET_PROOF_ONLY",
        "generated_utc": f"{day_utc}T00:00:00Z",
        "target_day": day_utc,
        "environment": "PAPER",
        "ib_account": "DU123456",
        "sleeve_id": "PRIMARY",
        "sleeve_truth_root": "/tmp/truth_sleeves/PRIMARY/PAPER",
        "authority_root": "/tmp/truth",
        "convergence_status": "SUCCESS",
        "authorization_verdict_ready": True,
        "authorization_required_artifacts": ["authorization_gate_verdict_v1"],
        "artifact_results": [],
        "blocker_chain": [],
        "source_refs": [],
    }


def _startup_materialization_input_convergence_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "startup_materialization_input_convergence_v1",
        "schema_version": 1,
        "generated_utc": f"{day_utc}T00:00:00Z",
        "target_day": day_utc,
        "authority_root": "/tmp/truth",
        "convergence_status": "SUCCESS",
        "required_inputs": ["startup_materialization_inputs_prep_v1"],
        "artifact_results": [],
        "blocker_chain": [],
        "source_refs": [],
    }


def _bod_execution_environment_proof_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "bod_execution_environment_proof",
        "schema_version": "v1",
        "authority_scope": "NON_AUTHORITY_FACT",
        "day_utc": day_utc,
        "status": "PASS",
        "blocking_codes": [],
        "producer": _producer_payload("constellation_2/common/bod_execution_environment_proof_v1.py"),
        "produced_at_utc": f"{day_utc}T00:00:00Z",
        "python_executable": "/usr/bin/python3",
        "repo_root": "/home/node/constellation",
        "cwd": "/home/node/constellation",
        "pythonpath": "",
        "virtual_env": "",
        "environment_snapshot": {"HOME": "/tmp", "PATH": "/usr/bin", "PYTHONPATH": "", "VIRTUAL_ENV": ""},
        "sys_path_head": ["/home/node/constellation"],
        "constellation_2_import": {"ok": True, "error": ""},
        "bridge_import_probe": {
            "cmd": ["/usr/bin/python3", "bridge.py"],
            "cwd": "/home/node/constellation",
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "proof_payload": {},
        },
        "human_readable_summary": f"BOD execution substrate proof PASS for {day_utc}",
    }


def _phasec_risk_inputs_prep_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "phasec_risk_inputs_prep",
        "schema_version": "v1",
        "authority_scope": "NON_AUTHORITY_FACT",
        "day_utc": day_utc,
        "session_id": f"paper_session:{day_utc}:PAPER",
        "status": "PASS",
        "compatible_nav_path": f"/tmp/{day_utc}/nav.v2.json",
        "drawdown_pct": "0.000000",
        "required_inputs_checked": [],
        "blocking_codes": [],
        "producer": _producer_payload("ops/tools/run_phasec_risk_inputs_prep_v1.py"),
        "produced_at_utc": f"{day_utc}T00:00:00Z",
        "bridge_result": {
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "artifact_path": f"/tmp/{day_utc}/nav_bridge.json",
            "artifact_status": "PRESENT",
        },
        "human_readable_summary": "test",
    }


def _capability_state_payload(day_utc: str) -> dict[str, object]:
    artifact_ref = {"artifact_family": "startup_materialization_v1", "artifact_path": "/tmp/startup.json", "artifact_sha256": HEX64}
    return {
        "schema_id": "capability_state",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": "PAPER",
        "ib_account": "DU123456",
        "sleeve_id": "PRIMARY",
        "overall_status": "PASS",
        "capabilities": [
            {
                "capability_id": "startup_materialization_ready",
                "status": "PASS",
                "kind": "HARD_OPERATIONAL_FACT",
                "reason_codes": [],
                "source_artifacts": [artifact_ref],
                "details": {},
            }
        ],
        "source_artifacts": [artifact_ref],
        "registry_ref": {
            "artifact_path": "/tmp/capability_policy_registry_v1.json",
            "artifact_sha256": HEX64,
            "schema_id": "capability_policy_registry_v1",
            "schema_version": "v1",
        },
        "release_id": "release-001",
        "git_sha": GIT_SHA40,
        "generated_at_utc": f"{day_utc}T00:00:00Z",
    }


def _intents_day_completeness_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "intents_day_completeness",
        "schema_version": "v1",
        "authority_scope": "NON_AUTHORITY_PREREQUISITE_FACT",
        "day_utc": day_utc,
        "session_id": f"paper_session:{day_utc}:PAPER",
        "completeness_status": "COMPLETE",
        "required_inputs_checked": [
            {
                "logical_name": "intent_snapshot:spy.intent.json",
                "absolute_path": f"/tmp/{day_utc}/spy.intent.json",
                "sha256": HEX64,
                "day_utc": day_utc,
                "status": "PRESENT",
                "reason_codes": [],
            }
        ],
        "missing_inputs": [],
        "freshness_verdict": "CURRENT",
        "linkage_verdict": "LINKED",
        "blocking_codes": [],
        "producer": _producer_payload("ops/tools/run_intents_day_completeness_v1.py"),
        "produced_at_utc": f"{day_utc}T00:00:00Z",
    }


def _deployment_state_machine_payload(day_utc: str, release_root: str) -> dict[str, object]:
    check = {"status": "PASS", "details": {}}
    return {
        "schema_id": "deployment_state_machine",
        "schema_version": "v1",
        "authority_scope": "TOP_LEVEL_DEPLOYMENT_STATE_MACHINE_OWNER",
        "day_utc": day_utc,
        "deployment_attempt_id": f"deployment_state_machine_attempt:{day_utc}:test",
        "deployment_state_machine_id": f"deployment_state_machine:{day_utc}:test",
        "evaluated_at_utc": f"{day_utc}T00:00:00Z",
        "authoritative_source": {
            "authoritative_repo_root": "/home/node/constellation",
            "authoritative_git_sha": GIT_SHA40,
            "authoritative_branch": "feature/test",
            "authoritative_cleanliness_status": "CLEAN",
            "dirty_entry_count": 0,
            "dirty_entry_sample": [],
            "source_snapshot_id": GIT_SHA40,
        },
        "release_build": {
            "release_id": "release-001",
            "release_root": release_root,
            "release_manifest_sha": HEX64,
            "bundled_file_hash_summary": {"file_count": 0, "aggregate_sha256": HEX64},
            "build_status": "RELEASE_PRESENT",
            "required_startup_stack_files_present": True,
            "missing_required_startup_stack_files": [],
        },
        "active_release": {
            "active_symlink_path": "/home/node/constellation_active",
            "active_symlink_target": release_root,
            "activation_status": "ACTIVE_POINTER_PRESENT",
            "active_runtime_contract_path": "/tmp/active_runtime_contract.v1.json",
            "active_runtime_contract_status": "ACTIVE",
        },
        "live_execution": {
            "service_unit_path": "/tmp/c2.service",
            "launcher_path": "/tmp/launcher.sh",
            "resolved_execution_root": release_root,
            "active_root_match": True,
            "authoritative_service_source_path": "/tmp/c2.service",
            "authoritative_service_source_root": release_root,
        },
        "drift_checks": {
            "authoritative_vs_release": check,
            "release_vs_active": check,
            "active_vs_live_execution": check,
            "runtime_copy_still_executable": check,
            "required_startup_stack_files_present": check,
        },
        "post_activation_verification": {
            "passed": True,
            "release_id": "release-001",
            "release_root": release_root,
            "active_symlink_path": "/home/node/constellation_active",
            "active_symlink_target": release_root,
            "active_pointer_matches_release": True,
            "required_startup_stack_files_present": True,
            "missing_required_startup_stack_files": [],
            "service_unit_path": "/tmp/c2.service",
            "launcher_path": "/tmp/launcher.sh",
            "resolved_execution_root": release_root,
            "active_root_match": True,
            "active_runtime_contract_path": "/tmp/active_runtime_contract.v1.json",
            "active_runtime_contract_match": True,
            "blocking_codes": [],
        },
        "final_deployment_decision": "DEPLOY_ACTIVE",
        "blocking_codes": [],
        "first_true_blocker_code": "",
        "first_true_blocker_path": "",
        "human_readable_summary": "Deployment active.",
        "producer": _producer_payload("ops/tools/run_deployment_state_machine_v1.py"),
    }


def _global_kill_switch_state_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "global_kill_switch_state",
        "schema_version": "v1",
        "day_utc": day_utc,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "producer": _producer_payload("ops/tools/run_global_kill_switch_v1.py"),
        "state": "INACTIVE",
        "allow_entries": True,
        "allow_exits": True,
        "forced_mode": "NORMAL",
        "reason_codes": [],
        "input_manifest": [_artifact_manifest_row("authorization_gate_verdict_v1")],
        "state_sha256": HEX64,
    }


def _authorization_gate_verdict_payload(day_utc: str, *, status: str = "PASS") -> dict[str, object]:
    return {
        "schema_id": "authorization_gate_verdict_v1",
        "schema_version": 1,
        "gate_classification_registry_id": "gate_registry",
        "gate_classification_registry_version": 1,
        "lifecycle_state_authority_ref": "/tmp/lifecycle_state.json",
        "day_utc": day_utc,
        "mode": "PAPER",
        "produced_utc": f"{day_utc}T00:00:00Z",
        "included_gates": [
            {
                "gate_id": "gate-1",
                "status": status,
                "artifact_path": "/tmp/gate-1.json",
                "artifact_sha256": HEX64,
                "reason_codes": [],
            }
        ],
        "excluded_gates": [],
        "blocking_gates": [],
        "status": status,
        "blocking_class": "NONE" if status == "PASS" else "BLOCKED",
        "reason_codes": [],
        "evidence_refs": ["/tmp/gate-1.json"],
        "decision_ledger_ref": "/tmp/decision_ledger.jsonl",
    }


def _day_start_blocked_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "day_start_blocked",
        "schema_version": "v1",
        "day_utc": day_utc,
        "blocked_stage": "PREOPEN",
        "failing_service": "c2-preopen-preflight.service",
        "failing_script": "ops/run/c2_preopen_preflight_v1.sh",
        "producer": {"repo": "constellation", "module": "ops/tools/run_day_start_blocked_v1.py"},
        "evidence_paths": ["/tmp/blocked-evidence"],
        "provenance": {"authoritative_write": True},
    }


def _trading_day_state_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "trading_day_state",
        "schema_version": "v1",
        "day_utc": day_utc,
        "state": "READY_NOW",
        "heartbeat_status": "PASS",
        "first_failing_prerequisite": "",
        "first_failing_path": "",
        "orchestrator_started": True,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "producer": {"repo": "constellation", "module": "ops/run/c2_preopen_preflight_v1.sh"},
        "evidence_paths": ["/tmp/evidence-a"],
        "provenance": {"authoritative_write": True},
    }


def _sleeve_live_readiness_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "C2_SLEEVE_LIVE_READINESS_V1",
        "schema_version": 1,
        "sleeve_id": "PRIMARY",
        "mode": "PAPER",
        "day_utc": day_utc,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "authority_classification": "NON_CANONICAL_ADVISORY_ONLY",
        "control_decision_warning": "Not execution authority.",
        "readiness_state": "READY",
        "readiness_summary": "Ready",
        "promotion_decision_basis": "All checks passed",
        "readiness_score": 95,
        "readiness_grade": "A",
        "readiness_grade_scale": "1_to_7",
        "readiness_grade_1_to_7": 7,
        "score_threshold_grade_1_to_7": 6,
        "grading_thresholds_1_to_7": [
            {"grade": 7, "min_score": 95},
            {"grade": 6, "min_score": 85},
            {"grade": 5, "min_score": 75},
            {"grade": 4, "min_score": 65},
            {"grade": 3, "min_score": 50},
            {"grade": 2, "min_score": 30},
            {"grade": 1, "min_score": 0},
        ],
        "score_threshold": 80,
        "promotion_candidate": True,
        "promotion_blockers": [],
        "root_blockers": [],
        "derived_blockers": [],
        "aggregate_blocker_summary": {},
        "promotion_blockers_detail": [],
        "minimum_conditions_summary": [],
        "current_vs_required": {},
        "smallest_clearance_set": [],
        "blocker_dependency_order": [],
        "estimated_promotion_gate_sequence": [],
        "top_blockers_ordered": [],
        "pass_conditions_remaining": [],
        "recommended_next_actions": [],
        "calibration_support": {},
        "promotion_checklist": {
            "must_be_true": [],
            "currently_false": [],
            "gating_conditions": [],
            "informational_conditions": [],
        },
        "policy_ref": {"path": "/tmp/policy.json", "sha256": HEX64},
        "checks": [{"check_id": "check-1", "required": True, "status": "PASS", "weight": 100, "score_awarded": 100, "details": {}}],
        "reason_codes": [],
        "evidence_paths": ["/tmp/evidence-a"],
        "evidence": [{"path": "/tmp/evidence-a", "sha256": HEX64, "note": None}],
    }


def _platform_bug_metrics_payload(day_utc: str) -> dict[str, object]:
    return {
        "schema_id": "C2_BUG_METRICS_V1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "window_days_evaluated": {"window_7d": 7, "window_14d": 14},
        "new_bug_events_today": 0,
        "bug_velocity_7d_avg": 0,
        "bug_velocity_14d_avg": 0,
        "recurring_bug_events": [],
        "recurrence_rate": 0,
        "mttr_hours": None,
        "median_ttr_hours": None,
        "bug_velocity_trend": "STABLE",
        "bug_half_life_estimate_days": None,
        "diagnostic_stability_rate": 10000,
        "metric_views": {
            "new_bug_events_today": _metric_view(0, "count", "0"),
            "bug_velocity_7d_avg": _metric_view(0, "events/day_x100", "0"),
            "bug_velocity_14d_avg": _metric_view(0, "events/day_x100", "0"),
            "recurrence_rate": _metric_view(0, "bps", "0"),
            "diagnostic_stability_rate": _metric_view(10000, "bps", "10000"),
            "mttr_hours": _metric_view(None, "hours", "UNKNOWN"),
            "median_ttr_hours": _metric_view(None, "hours", "UNKNOWN"),
            "bug_half_life_estimate_days": _metric_view(None, "days", "UNKNOWN"),
        },
        "calculation_summary": {
            "new_bug_events_today_basis": "artifact_count",
            "bug_velocity_basis": "window_average",
            "recurrence_rate_basis": "structured_recurrence",
            "diagnostic_stability_basis": "healthy_events",
            "window_days_used": {"velocity_7d_days": 7, "velocity_14d_days": 14, "recurrence_days": 14},
        },
        "evidence_paths": ["/tmp/metrics-evidence.json"],
        "event_counts_by_day": [{"day_utc": day_utc, "event_count": 0}],
        "unknown_fields": ["mttr_hours", "median_ttr_hours", "bug_half_life_estimate_days"],
    }


def test_active_read_dominance_paths_have_no_direct_control_plane_reads() -> None:
    assert direct_control_plane_read_hits_v1(REPO_ROOT) == []


def test_gateway_rejects_unknown_surface_fail_closed() -> None:
    with pytest.raises(ValueError, match="CONTROL_PLANE_READ_UNKNOWN_SURFACE"):
        read_control_plane_surface_v1(domain="session", surface="definitely_unknown_surface", truth_root=REPO_ROOT)


def test_gateway_rejects_unknown_collection_fail_closed() -> None:
    with pytest.raises(ValueError, match="CONTROL_PLANE_READ_UNKNOWN_COLLECTION"):
        read_control_plane_collection_v1(
            domain="operator",
            surface="definitely_unknown_collection",
            truth_root=REPO_ROOT,
            day_utc="2026-04-20",
        )


def test_gateway_rejects_unknown_semantic_fail_closed() -> None:
    with pytest.raises(ValueError, match="CONTROL_PLANE_READ_UNKNOWN_SEMANTIC"):
        read_control_plane_semantic_v1(
            domain="session",
            surface="definitely_unknown_semantic",
            truth_root=REPO_ROOT,
            day_utc="2026-04-20",
            environment="PAPER",
            ib_account="DUO847203",
        )


def test_gateway_semantic_requires_context_fail_closed() -> None:
    with pytest.raises(ValueError, match="CONTROL_PLANE_SEMANTIC_IB_ACCOUNT_REQUIRED"):
        read_control_plane_semantic_v1(
            domain="session",
            surface="next_day_readiness_probe_inputs",
            truth_root=REPO_ROOT,
            day_utc="2026-04-20",
            environment="PAPER",
        )


def test_next_day_semantic_composition_is_deterministic(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_ref(surface: str, payload: dict[str, object]) -> ControlPlaneReadRefV1:
        return ControlPlaneReadRefV1(
            domain="test",
            surface=surface,
            read_kind="validated_json",
            path=Path(f"/tmp/{surface}.json"),
            payload=payload,
            sha256="a" * 64,
            schema_relpath=None,
            metadata={},
        )

    def fake_read_surface(**kwargs):
        surface = kwargs["surface"]
        day = kwargs.get("day_utc")
        if surface == "paper_policy_verdict":
            return fake_ref(surface, {"overall_status": "PASS", "day_utc": day})
        if surface == "production_policy_verdict":
            return fake_ref(surface, {"overall_status": "PASS", "day_utc": day})
        if surface == "trade_submit_readiness":
            return fake_ref(surface, {"state": "OK", "ok": True, "day_utc": day})
        if surface == "recurrence_kill_gate":
            return fake_ref(surface, {"proof_status": "RECURRENCE_SAFE", "day_utc": day})
        raise AssertionError(surface)

    monkeypatch.setattr(
        "constellation_2.common.control_plane_read_gateway_v1.read_control_plane_surface_v1",
        fake_read_surface,
    )

    semantic = read_control_plane_semantic_v1(
        domain="session",
        surface="next_day_readiness_probe_inputs",
        truth_root=REPO_ROOT,
        day_utc="2026-04-20",
        environment="PAPER",
        ib_account="DUO847203",
    )
    assert semantic.payload["current_baseline_status"] == "PASS"
    assert semantic.payload["probe_status"] == "READY"
    assert semantic.payload["predicted_blocking_items"] == []


def test_gateway_new_surfaces_happy_path(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(truth_root / "market_calendar_v1" / "dataset_manifest.json", {"files": [{"year": 2026, "file": "2026.jsonl"}]})
    (truth_root / "market_calendar_v1" / "2026.jsonl").write_text(
        json.dumps(
            {
                "dataset_version": "v1",
                "exchange": "XNYS",
                "day_utc": "2026-04-20",
                "is_trading_session": True,
                "source_name": "calendar-source",
                "source_hash": HEX64,
                "ingested_utc": "2026-04-19T00:00:00Z",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    _write_json(
        truth_root / "runtime_control_kernel_v1" / "records" / "2026-04-20" / "PAPER" / "DU123456" / f"{HEX64}.runtime_control_record.v1.json",
        _runtime_control_record_payload(
            day_utc="2026-04-20",
            produced_utc="2026-04-20T12:00:00Z",
            effective_at_utc="2026-04-20T11:00:00Z",
            record_id=HEX64,
        ),
    )
    _write_json(
        truth_root / "runtime_control_kernel_v1" / "decisions" / "2026-04-20" / f"{HEX64}.runtime_control_decision.v1.json",
        _runtime_control_decision_payload(
            day_utc="2026-04-20",
            produced_utc="2026-04-20T12:00:00Z",
            effective_at_utc="2026-04-20T11:00:00Z",
            decision_id=HEX64,
        ),
    )
    _write_json(
        truth_root / "runtime_control_kernel_v1" / "run_envelopes" / "2026-04-20" / "run-1.runtime_control_run_envelope.v1.json",
        _runtime_control_run_envelope_payload(
            day_utc="2026-04-20",
            produced_utc="2026-04-20T12:00:00Z",
            run_id="run-1",
            record_id=HEX64,
        ),
    )
    _write_json(
        truth_root / "reports" / "gate_stack_verdict_v1" / "2026-04-20" / "gate_stack_verdict.v1.json",
        _gate_stack_verdict_payload("2026-04-20"),
    )
    _write_json(
        truth_root / "readiness_v1" / "constellation_platform_readiness_v1" / "2026-04-20" / "constellation_platform_readiness.v1.json",
        _platform_readiness_payload("2026-04-20"),
    )
    _write_json(
        truth_root / "reports" / "execution_reconciliation_v1" / "2026-04-20" / "execution_reconciliation.v1.json",
        _execution_reconciliation_payload("2026-04-20"),
    )

    market_ref = read_control_plane_surface_v1(
        domain="session", surface="market_calendar_day", truth_root=truth_root, day_utc="2026-04-20"
    )
    record_ref = read_control_plane_surface_v1(domain="execution", surface="runtime_control_record", truth_root=truth_root)
    decision_ref = read_control_plane_surface_v1(domain="execution", surface="runtime_control_decision", truth_root=truth_root)
    envelope_ref = read_control_plane_surface_v1(
        domain="execution", surface="runtime_control_run_envelope", truth_root=truth_root
    )
    gate_ref = read_control_plane_surface_v1(
        domain="execution", surface="gate_stack_verdict", truth_root=truth_root, day_utc="2026-04-20"
    )
    readiness_ref = read_control_plane_surface_v1(
        domain="operator", surface="platform_readiness", truth_root=truth_root, day_utc="2026-04-20"
    )
    reconciliation_ref = read_control_plane_surface_v1(
        domain="execution", surface="execution_reconciliation", truth_root=truth_root, day_utc="2026-04-20"
    )

    assert market_ref.payload["day_utc"] == "2026-04-20"
    assert record_ref.payload["schema_id"] == "runtime_control_record"
    assert decision_ref.payload["schema_id"] == "runtime_control_decision"
    assert envelope_ref.payload["schema_id"] == "runtime_control_run_envelope"
    assert gate_ref.payload["schema_id"] == "gate_stack_verdict"
    assert readiness_ref.payload["schema_id"] == "C2_PLATFORM_READINESS_V1"
    assert readiness_ref.metadata["resolution_mode"] == "REQUESTED_DAY"
    assert reconciliation_ref.payload["schema_id"] == "C2_EXECUTION_RECONCILIATION_V1"


@pytest.mark.parametrize(
    ("domain", "surface", "kwargs", "expected"),
    [
        ("session", "market_calendar_day", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_MARKET_CALENDAR_MANIFEST_MISSING"),
        ("execution", "runtime_control_record", {}, "CONTROL_PLANE_READ_LATEST_ROOT_MISSING"),
        ("execution", "runtime_control_decision", {}, "CONTROL_PLANE_READ_LATEST_ROOT_MISSING"),
        ("execution", "runtime_control_run_envelope", {}, "CONTROL_PLANE_READ_LATEST_ROOT_MISSING"),
        ("execution", "gate_stack_verdict", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("operator", "platform_readiness", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_PLATFORM_READINESS_MISSING"),
        ("execution", "execution_reconciliation", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
    ],
)
def test_gateway_new_surfaces_missing_artifact_fail_closed(
    tmp_path: Path, domain: str, surface: str, kwargs: dict[str, str], expected: str
) -> None:
    with pytest.raises(ValueError, match=expected):
        read_control_plane_surface_v1(domain=domain, surface=surface, truth_root=tmp_path / "truth", **kwargs)


@pytest.mark.parametrize(
    ("domain", "surface", "kwargs", "expected"),
    [
        ("session", "market_calendar_day", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("execution", "gate_stack_verdict", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("operator", "platform_readiness", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("execution", "execution_reconciliation", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("execution", "runtime_control_record", {"truth_root": None}, "CONTROL_PLANE_READ_GLOBAL_TRUTH_ROOT_REQUIRED"),
        ("execution", "runtime_control_decision", {"truth_root": None}, "CONTROL_PLANE_READ_GLOBAL_TRUTH_ROOT_REQUIRED"),
        ("execution", "runtime_control_run_envelope", {"truth_root": None}, "CONTROL_PLANE_READ_GLOBAL_TRUTH_ROOT_REQUIRED"),
    ],
)
def test_gateway_new_surfaces_missing_context_fail_closed(
    tmp_path: Path, domain: str, surface: str, kwargs: dict[str, object], expected: str
) -> None:
    params = dict(kwargs)
    truth_root = params.pop("truth_root", tmp_path / "truth")
    with pytest.raises(ValueError, match=expected):
        read_control_plane_surface_v1(domain=domain, surface=surface, truth_root=truth_root, **params)


@pytest.mark.parametrize(
    ("surface", "family_relpath", "filename_one", "filename_two", "payload_factory"),
    [
        (
            "runtime_control_record",
            ("runtime_control_kernel_v1", "records", "2026-04-20", "PAPER", "DU123456"),
            f"{HEX64}.runtime_control_record.v1.json",
            f"{'b' * 64}.runtime_control_record.v1.json",
            lambda: _runtime_control_record_payload(
                day_utc="2026-04-20",
                produced_utc="2026-04-20T12:00:00Z",
                effective_at_utc="2026-04-20T11:00:00Z",
                record_id=HEX64,
            ),
        ),
        (
            "runtime_control_decision",
            ("runtime_control_kernel_v1", "decisions", "2026-04-20"),
            f"{HEX64}.runtime_control_decision.v1.json",
            f"{'b' * 64}.runtime_control_decision.v1.json",
            lambda: _runtime_control_decision_payload(
                day_utc="2026-04-20",
                produced_utc="2026-04-20T12:00:00Z",
                effective_at_utc="2026-04-20T11:00:00Z",
                decision_id=HEX64,
            ),
        ),
        (
            "runtime_control_run_envelope",
            ("runtime_control_kernel_v1", "run_envelopes", "2026-04-20"),
            "run-1.runtime_control_run_envelope.v1.json",
            "run-2.runtime_control_run_envelope.v1.json",
            lambda: _runtime_control_run_envelope_payload(
                day_utc="2026-04-20",
                produced_utc="2026-04-20T12:00:00Z",
                run_id="run-1",
                record_id=HEX64,
            ),
        ),
    ],
)
def test_gateway_runtime_control_latest_selection_is_ambiguous_fail_closed(
    tmp_path: Path,
    surface: str,
    family_relpath: tuple[str, ...],
    filename_one: str,
    filename_two: str,
    payload_factory,
) -> None:
    truth_root = tmp_path / "truth"
    family_root = truth_root.joinpath(*family_relpath)
    payload = payload_factory()
    _write_json(family_root / filename_one, payload)
    payload_two = dict(payload)
    if surface == "runtime_control_record":
        payload_two["record_id"] = "b" * 64
        payload_two["runtime_control_record_id"] = "b" * 64
    elif surface == "runtime_control_decision":
        payload_two["record_id"] = "b" * 64
        payload_two["runtime_control_decision_id"] = "b" * 64
    else:
        payload_two["record_id"] = "b" * 64
        payload_two["run_id"] = "run-2"
    _write_json(family_root / filename_two, payload_two)
    with pytest.raises(ValueError, match="CONTROL_PLANE_READ_AMBIGUOUS_LATEST"):
        read_control_plane_surface_v1(domain="execution", surface=surface, truth_root=truth_root)


def test_gateway_platform_readiness_pointer_fallback_happy_path(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    target_path = (
        truth_root / "readiness_v1" / "constellation_platform_readiness_v1" / "2026-04-19" / "constellation_platform_readiness.v1.json"
    )
    _write_json(target_path, _platform_readiness_payload("2026-04-19"))
    _write_json(
        truth_root / "readiness_v1" / "constellation_platform_readiness_v1" / "latest_pointer.v1.json",
        {"target_path": str(target_path)},
    )
    ref = read_control_plane_surface_v1(
        domain="operator", surface="platform_readiness", truth_root=truth_root, day_utc="2026-04-20"
    )
    assert ref.metadata["resolution_mode"] == "LATEST_POINTER_FALLBACK"
    assert ref.metadata["resolved_via_latest_pointer"] is True
    assert ref.payload["day_utc"] == "2026-04-19"


def test_gateway_secondary_surfaces_happy_path(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    truth_sleeves_root = tmp_path / "truth_sleeves"
    day = "2026-04-20"
    economic_context_hash = "econ-ctx-1"
    scoped_truth_root = truth_sleeves_root / "PRIMARY" / "PAPER"

    _write_json(truth_root / "reports" / "day_authority_decision_v1" / day / "day_authority_decision.v1.json", _day_authority_decision_payload(day))
    _write_json(truth_root / "reports" / "pre_open_bundle_v1" / day / "pre_open_bundle.v1.json", _pre_open_bundle_payload(day))
    _write_json(
        truth_root / "reports" / "paper_startup_intent_input_convergence_v1" / day / "paper_startup_intent_input_convergence.v1.json",
        _paper_startup_intent_input_convergence_payload(day),
    )
    _write_json(
        truth_root / "reports" / "paper_startup_authorization_convergence_v1" / day / "paper_startup_authorization_convergence.v1.json",
        _paper_startup_authorization_convergence_payload(day),
    )
    _write_json(
        truth_root / "reports" / "startup_materialization_input_convergence_v1" / day / "startup_materialization_input_convergence.v1.json",
        _startup_materialization_input_convergence_payload(day),
    )
    _write_json(
        truth_root / "reports" / "bod_execution_environment_proof_v1" / day / "bod_execution_environment_proof.v1.json",
        _bod_execution_environment_proof_payload(day),
    )
    _write_json(
        truth_root / "reports" / "phasec_risk_inputs_prep_v1" / day / "phasec_risk_inputs_prep.v1.json",
        _phasec_risk_inputs_prep_payload(day),
    )
    _write_json(truth_root / "reports" / "capability_state_v1" / day / "capability_state.v1.json", _capability_state_payload(day))
    _write_json(
        truth_root / "reports" / "intents_day_completeness_v1" / day / "intents_day_completeness.v1.json",
        _intents_day_completeness_payload(day),
    )
    _write_json(
        truth_root / "reports" / "deployment_state_machine_v1" / day / "deployment_state_machine.v1.json",
        _deployment_state_machine_payload(day, str(tmp_path / "release")),
    )
    _write_json(
        truth_root / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json",
        _global_kill_switch_state_payload(day),
    )
    _write_json(
        truth_root / "reports" / "day_start_blocked_v1" / day / "day_start_blocked.v1.json",
        _day_start_blocked_payload(day),
    )
    _write_json(
        truth_root / "reports" / "trading_day_state_v1" / day / "trading_day_state.v1.json",
        _trading_day_state_payload(day),
    )
    _write_json(
        scoped_truth_root / "reports" / "authorization_gate_verdict_v1" / day / "authorization_gate_verdict.v1.json",
        _authorization_gate_verdict_payload(day),
    )
    _write_json(
        scoped_truth_root / "readiness_v1" / "sleeve_live_readiness_v1" / day / "sleeve_live_readiness.v1.json",
        _sleeve_live_readiness_payload(day),
    )
    _write_json(
        truth_root / "readiness_v1" / "constellation_bug_metrics_v1" / day / "constellation_bug_metrics.v1.json",
        _platform_bug_metrics_payload(day),
    )
    _write_json(
        truth_root / "reports" / "economic_state_build_v1" / day / economic_context_hash / "economic_state_build.v1.json",
        _economic_state_build_payload(day, economic_context_hash),
    )
    _write_json(
        scoped_truth_root / "economic_state_package_v1" / day / economic_context_hash / "economic_state_package.v1.json",
        _economic_state_package_payload(day, economic_context_hash),
    )

    refs = {
        "day_authority_decision": read_control_plane_surface_v1(domain="lifecycle", surface="day_authority_decision", truth_root=truth_root, day_utc=day),
        "pre_open_bundle": read_control_plane_surface_v1(domain="lifecycle", surface="pre_open_bundle", truth_root=truth_root, day_utc=day),
        "paper_startup_intent_input_convergence": read_control_plane_surface_v1(domain="lifecycle", surface="paper_startup_intent_input_convergence", truth_root=truth_root, day_utc=day),
        "paper_startup_authorization_convergence": read_control_plane_surface_v1(domain="lifecycle", surface="paper_startup_authorization_convergence", truth_root=truth_root, day_utc=day),
        "startup_materialization_input_convergence": read_control_plane_surface_v1(domain="lifecycle", surface="startup_materialization_input_convergence", truth_root=truth_root, day_utc=day),
        "bod_execution_environment_proof": read_control_plane_surface_v1(domain="lifecycle", surface="bod_execution_environment_proof", truth_root=truth_root, day_utc=day),
        "phasec_risk_inputs_prep": read_control_plane_surface_v1(domain="lifecycle", surface="phasec_risk_inputs_prep", truth_root=truth_root, day_utc=day),
        "capability_state": read_control_plane_surface_v1(domain="lifecycle", surface="capability_state", truth_root=truth_root, day_utc=day),
        "intents_day_completeness": read_control_plane_surface_v1(domain="lifecycle", surface="intents_day_completeness", truth_root=truth_root, day_utc=day),
        "global_kill_switch_state": read_control_plane_surface_v1(domain="execution", surface="global_kill_switch_state", truth_root=truth_root, day_utc=day),
        "authorization_gate_verdict": read_control_plane_surface_v1(
            domain="execution",
            surface="authorization_gate_verdict",
            truth_root=truth_root,
            truth_sleeves_root=truth_sleeves_root,
            day_utc=day,
            sleeve_id="PRIMARY",
            environment="PAPER",
        ),
        "kill_switch": read_control_plane_surface_v1(domain="operator", surface="kill_switch", truth_root=truth_root, day_utc=day),
        "day_start_blocked": read_control_plane_surface_v1(domain="lifecycle", surface="day_start_blocked", truth_root=truth_root, day_utc=day),
        "trading_day_state": read_control_plane_surface_v1(domain="lifecycle", surface="trading_day_state", truth_root=truth_root, day_utc=day),
        "sleeve_live_readiness": read_control_plane_surface_v1(
            domain="lifecycle",
            surface="sleeve_live_readiness",
            truth_sleeves_root=truth_sleeves_root,
            day_utc=day,
            sleeve_id="PRIMARY",
            environment="PAPER",
        ),
        "deployment_state_machine": read_control_plane_surface_v1(domain="platform", surface="deployment_state_machine", truth_root=truth_root, day_utc=day),
        "platform_bug_metrics": read_control_plane_surface_v1(domain="platform", surface="platform_bug_metrics", truth_root=truth_root, day_utc=day),
        "economic_state_build": read_control_plane_surface_v1(
            domain="execution",
            surface="economic_state_build",
            truth_root=truth_root,
            day_utc=day,
            context_hash=economic_context_hash,
        ),
        "economic_state_package": read_control_plane_surface_v1(
            domain="execution",
            surface="economic_state_package",
            truth_sleeves_root=truth_sleeves_root,
            day_utc=day,
            context_hash=economic_context_hash,
            sleeve_id="PRIMARY",
            environment="PAPER",
        ),
    }

    assert refs["day_authority_decision"].payload["schema_id"] == "day_authority_decision"
    assert refs["pre_open_bundle"].payload["schema_id"] == "pre_open_bundle"
    assert refs["paper_startup_intent_input_convergence"].payload["schema_id"] == "paper_startup_intent_input_convergence_v1"
    assert refs["paper_startup_authorization_convergence"].payload["schema_id"] == "paper_startup_authorization_convergence_v1"
    assert refs["startup_materialization_input_convergence"].payload["schema_id"] == "startup_materialization_input_convergence_v1"
    assert refs["bod_execution_environment_proof"].payload["schema_id"] == "bod_execution_environment_proof"
    assert refs["phasec_risk_inputs_prep"].payload["schema_id"] == "phasec_risk_inputs_prep"
    assert refs["capability_state"].payload["schema_id"] == "capability_state"
    assert refs["intents_day_completeness"].payload["schema_id"] == "intents_day_completeness"
    assert refs["global_kill_switch_state"].payload["schema_id"] == "global_kill_switch_state"
    assert refs["authorization_gate_verdict"].payload["schema_id"] == "authorization_gate_verdict_v1"
    assert refs["kill_switch"].payload["state"] == "INACTIVE"
    assert refs["day_start_blocked"].payload["schema_id"] == "day_start_blocked"
    assert refs["trading_day_state"].payload["schema_id"] == "trading_day_state"
    assert refs["sleeve_live_readiness"].payload["schema_id"] == "C2_SLEEVE_LIVE_READINESS_V1"
    assert refs["sleeve_live_readiness"].payload["readiness_grade_1_to_7"] == 7
    assert refs["sleeve_live_readiness"].payload["score_threshold_grade_1_to_7"] == 6
    assert refs["deployment_state_machine"].payload["schema_id"] == "deployment_state_machine"
    assert refs["platform_bug_metrics"].payload["schema_id"] == "C2_BUG_METRICS_V1"
    assert refs["economic_state_build"].payload["schema_id"] == "economic_state_build"
    assert refs["economic_state_package"].payload["schema_id"] == "economic_state_package"


@pytest.mark.parametrize(
    ("domain", "surface", "kwargs", "expected"),
    [
        ("lifecycle", "day_authority_decision", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("lifecycle", "pre_open_bundle", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("lifecycle", "paper_startup_intent_input_convergence", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("lifecycle", "paper_startup_authorization_convergence", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("lifecycle", "startup_materialization_input_convergence", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("lifecycle", "bod_execution_environment_proof", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("lifecycle", "phasec_risk_inputs_prep", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("lifecycle", "capability_state", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("lifecycle", "intents_day_completeness", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("execution", "global_kill_switch_state", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_GLOBAL_KILL_SWITCH_STATE_FAIL_CLOSED"),
        ("execution", "authorization_gate_verdict", {"day_utc": "2026-04-20", "truth_sleeves_root": "/tmp/missing"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("execution", "economic_state_build", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_DAY_CONTEXT_ROOT_MISSING"),
        ("execution", "economic_state_package", {"day_utc": "2026-04-20", "truth_sleeves_root": "/tmp/missing", "sleeve_id": "PRIMARY", "environment": "PAPER"}, "CONTROL_PLANE_READ_DAY_CONTEXT_ROOT_MISSING"),
        ("operator", "kill_switch", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_GLOBAL_KILL_SWITCH_STATE_FAIL_CLOSED"),
        ("lifecycle", "day_start_blocked", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("lifecycle", "trading_day_state", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("lifecycle", "sleeve_live_readiness", {"day_utc": "2026-04-20", "truth_sleeves_root": "/tmp/missing"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("platform", "deployment_state_machine", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_JSON_FAILED"),
        ("platform", "platform_bug_metrics", {"day_utc": "2026-04-20"}, "CONTROL_PLANE_READ_PLATFORM_BUG_METRICS_MISSING"),
    ],
)
def test_gateway_secondary_surfaces_missing_artifact_fail_closed(
    tmp_path: Path, domain: str, surface: str, kwargs: dict[str, object], expected: str
) -> None:
    params = dict(kwargs)
    truth_root = params.pop("truth_root", tmp_path / "truth")
    with pytest.raises(ValueError, match=expected):
        read_control_plane_surface_v1(domain=domain, surface=surface, truth_root=truth_root, **params)


@pytest.mark.parametrize(
    ("domain", "surface", "kwargs", "expected"),
    [
        ("lifecycle", "day_authority_decision", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("lifecycle", "pre_open_bundle", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("lifecycle", "paper_startup_intent_input_convergence", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("lifecycle", "paper_startup_authorization_convergence", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("lifecycle", "startup_materialization_input_convergence", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("lifecycle", "bod_execution_environment_proof", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("lifecycle", "phasec_risk_inputs_prep", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("lifecycle", "capability_state", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("lifecycle", "intents_day_completeness", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("execution", "global_kill_switch_state", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("execution", "authorization_gate_verdict", {"truth_root": None}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("execution", "economic_state_build", {"truth_root": None}, "CONTROL_PLANE_READ_GLOBAL_TRUTH_ROOT_REQUIRED"),
        ("execution", "economic_state_package", {"truth_root": None, "truth_sleeves_root": None}, "CONTROL_PLANE_READ_SURFACE_ROOT_REQUIRED"),
        ("operator", "kill_switch", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("lifecycle", "day_start_blocked", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("lifecycle", "trading_day_state", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("lifecycle", "sleeve_live_readiness", {"truth_root": None}, "CONTROL_PLANE_READ_SURFACE_ROOT_REQUIRED"),
        ("platform", "deployment_state_machine", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
        ("platform", "platform_bug_metrics", {}, "CONTROL_PLANE_READ_DAY_REQUIRED"),
    ],
)
def test_gateway_secondary_surfaces_missing_context_fail_closed(
    tmp_path: Path, domain: str, surface: str, kwargs: dict[str, object], expected: str
) -> None:
    params = dict(kwargs)
    truth_root = params.pop("truth_root", tmp_path / "truth")
    with pytest.raises(ValueError, match=expected):
        read_control_plane_surface_v1(domain=domain, surface=surface, truth_root=truth_root, **params)


def test_gateway_authorization_gate_verdict_head_fallback_happy_path(tmp_path: Path) -> None:
    truth_sleeves_root = tmp_path / "truth_sleeves"
    scoped_truth_root = truth_sleeves_root / "PRIMARY" / "PAPER"
    requested_day = "2026-04-20"
    target_path = scoped_truth_root / "reports" / "authorization_gate_verdict_v1" / requested_day / "authorization_gate_verdict.v1.json"
    _write_json(target_path, _authorization_gate_verdict_payload(requested_day))
    _write_json(
        scoped_truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json",
        {"day_utc": requested_day, "points_to": str(target_path)},
    )
    ref = read_control_plane_surface_v1(
        domain="execution",
        surface="authorization_gate_verdict",
        truth_root=tmp_path / "truth",
        truth_sleeves_root=truth_sleeves_root,
        day_utc=requested_day,
        sleeve_id="PRIMARY",
        environment="PAPER",
    )
    assert ref.metadata["resolved_via_head"] is True
    assert ref.payload["day_utc"] == requested_day


def test_gateway_authorization_gate_verdict_invalid_head_fail_closed(tmp_path: Path) -> None:
    truth_sleeves_root = tmp_path / "truth_sleeves"
    scoped_truth_root = truth_sleeves_root / "PRIMARY" / "PAPER"
    requested_day = "2026-04-20"
    _write_json(
        scoped_truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json",
        {"day_utc": requested_day, "points_to": str(scoped_truth_root / "reports" / "not_authorization_v1" / requested_day / "bad.json")},
    )
    with pytest.raises(ValueError, match="CONTROL_PLANE_READ_AUTHORIZATION_GATE_HEAD_INVALID"):
        read_control_plane_surface_v1(
            domain="execution",
            surface="authorization_gate_verdict",
            truth_root=tmp_path / "truth",
            truth_sleeves_root=truth_sleeves_root,
            day_utc=requested_day,
            sleeve_id="PRIMARY",
            environment="PAPER",
        )


def test_gateway_global_kill_switch_sleeve_mismatch_fail_closed(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    truth_sleeves_root = tmp_path / "truth_sleeves"
    day = "2026-04-20"
    _write_json(
        truth_root / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json",
        _global_kill_switch_state_payload(day),
    )
    mismatch_payload = dict(_global_kill_switch_state_payload(day))
    mismatch_payload["allow_entries"] = False
    _write_json(
        truth_sleeves_root / "PRIMARY" / "PAPER" / "risk_v1" / "kill_switch_v1" / day / "global_kill_switch_state.v1.json",
        mismatch_payload,
    )
    with pytest.raises(ValueError, match="CONTROL_PLANE_READ_GLOBAL_KILL_SWITCH_STATE_FAIL_CLOSED"):
        read_control_plane_surface_v1(
            domain="execution",
            surface="global_kill_switch_state",
            truth_root=truth_root,
            truth_sleeves_root=truth_sleeves_root,
            day_utc=day,
        )


def test_gateway_platform_bug_metrics_pointer_fallback_happy_path(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    target_path = truth_root / "readiness_v1" / "constellation_bug_metrics_v1" / "2026-04-19" / "constellation_bug_metrics.v1.json"
    _write_json(target_path, _platform_bug_metrics_payload("2026-04-19"))
    _write_json(
        truth_root / "readiness_v1" / "constellation_bug_metrics_v1" / "latest_pointer.v1.json",
        {"target_path": str(target_path)},
    )
    ref = read_control_plane_surface_v1(
        domain="platform", surface="platform_bug_metrics", truth_root=truth_root, day_utc="2026-04-20"
    )
    assert ref.metadata["resolution_mode"] == "LATEST_POINTER_FALLBACK"
    assert ref.payload["day_utc"] == "2026-04-19"


def test_gateway_platform_bug_metrics_pointer_sha_mismatch_fail_closed(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    target_path = truth_root / "readiness_v1" / "constellation_bug_metrics_v1" / "2026-04-19" / "constellation_bug_metrics.v1.json"
    _write_json(target_path, _platform_bug_metrics_payload("2026-04-19"))
    _write_json(
        truth_root / "readiness_v1" / "constellation_bug_metrics_v1" / "latest_pointer.v1.json",
        {"target_path": str(target_path), "target_sha256": "b" * 64},
    )
    with pytest.raises(ValueError, match="CONTROL_PLANE_READ_PLATFORM_BUG_METRICS_POINTER_SHA_MISMATCH"):
        read_control_plane_surface_v1(
            domain="platform", surface="platform_bug_metrics", truth_root=truth_root, day_utc="2026-04-20"
        )


def test_gateway_economic_state_build_ambiguous_day_fail_closed(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day = "2026-04-20"
    _write_json(
        truth_root / "reports" / "economic_state_build_v1" / day / "ctx-a" / "economic_state_build.v1.json",
        _economic_state_build_payload(day, "ctx-a"),
    )
    _write_json(
        truth_root / "reports" / "economic_state_build_v1" / day / "ctx-b" / "economic_state_build.v1.json",
        _economic_state_build_payload(day, "ctx-b"),
    )
    with pytest.raises(ValueError, match="CONTROL_PLANE_READ_DAY_CONTEXT_AMBIGUOUS"):
        read_control_plane_surface_v1(
            domain="execution",
            surface="economic_state_build",
            truth_root=truth_root,
            day_utc=day,
        )


def test_gateway_economic_state_package_ambiguous_day_fail_closed(tmp_path: Path) -> None:
    truth_sleeves_root = tmp_path / "truth_sleeves"
    day = "2026-04-20"
    scoped_truth_root = truth_sleeves_root / "PRIMARY" / "PAPER"
    _write_json(
        scoped_truth_root / "economic_state_package_v1" / day / "ctx-a" / "economic_state_package.v1.json",
        _economic_state_package_payload(day, "ctx-a"),
    )
    _write_json(
        scoped_truth_root / "economic_state_package_v1" / day / "ctx-b" / "economic_state_package.v1.json",
        _economic_state_package_payload(day, "ctx-b"),
    )
    with pytest.raises(ValueError, match="CONTROL_PLANE_READ_DAY_CONTEXT_AMBIGUOUS"):
        read_control_plane_surface_v1(
            domain="execution",
            surface="economic_state_package",
            truth_sleeves_root=truth_sleeves_root,
            day_utc=day,
            sleeve_id="PRIMARY",
            environment="PAPER",
        )


def test_gateway_economic_state_package_missing_root_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="CONTROL_PLANE_READ_SURFACE_ROOT_REQUIRED"):
        read_control_plane_surface_v1(
            domain="execution",
            surface="economic_state_package",
            truth_root=None,
            truth_sleeves_root=None,
            day_utc="2026-04-20",
        )


def test_gateway_economic_state_build_missing_root_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="CONTROL_PLANE_READ_GLOBAL_TRUTH_ROOT_REQUIRED"):
        read_control_plane_surface_v1(
            domain="execution",
            surface="economic_state_build",
            truth_root=None,
            day_utc="2026-04-20",
        )


def test_live_readers_are_explicitly_gateway_routed() -> None:
    expected = {
        "ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh": "read_control_plane_surface_v1.py",
        "constellation_2/phaseL/ui_api/operations_read_model.py": "read_control_plane_surface_dict",
        "constellation_2/phaseL/ui_api/alerts_read_model.py": "read_control_plane_surface_dict",
        "constellation_2/phaseL/ui_api/integrity_read_model.py": "read_control_plane_surface_dict",
        "constellation_2/phaseL/ui_api/policy_evolution_state_read_model.py": "read_control_plane_collection",
        "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py": "read_control_plane_surface_v1",
        "constellation_2/common/runtime_contract_v1.py": "read_control_plane_surface_v1",
        "constellation_2/common/session_authority_v1.py": "read_control_plane_surface_v1",
        "constellation_2/common/paper_session_fact_plane_v1.py": "read_control_plane_surface_v1",
        "constellation_2/common/session_authority_monitor_v1.py": "read_control_plane_surface_v1",
        "constellation_2/common/market_calendar_coverage_authority_v1.py": "read_control_plane_surface_v1",
        "constellation_2/common/control_plane_validation_kernel_v1.py": "read_control_plane_surface_v1",
        "constellation_2/common/control_plane_trust_surface_validator_v1.py": "read_control_plane_surface_v1",
        "constellation_2/common/fresh_day_admission_v1.py": "read_control_plane_semantic_v1",
        "constellation_2/common/next_day_readiness_probe_v1.py": "read_control_plane_semantic_v1",
        "constellation_2/common/pre_open_materializer_v1.py": "read_control_plane_surface_v1",
        "constellation_2/common/subsystem_authority_v1.py": "read_control_plane_surface_v1",
        "constellation_2/common/configuration_activation_family_validator_v1.py": "read_control_plane_semantic_v1",
        "ops/tools/run_execution_journal_v1.py": "read_control_plane_surface_v1",
        "ops/tools/run_trading_day_state_machine_v1.py": "read_control_plane_surface_v1",
    }
    for relpath, marker in expected.items():
        text = (REPO_ROOT / relpath).read_text(encoding="utf-8")
        assert marker in text, relpath
    for relpath in READ_DOMINANCE_ACTIVE_PATHS:
        assert (REPO_ROOT / relpath).exists(), relpath
