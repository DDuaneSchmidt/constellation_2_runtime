from __future__ import annotations

import hashlib
import json
import sys
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_trading_day_state_machine_v1 as state_machine_module
from constellation_2.common.paper_session_fact_plane_v1 import canonical_paper_session_id_v1
from constellation_2.common.paper_session_ledger_v1 import build_paper_session_ledger_v1, write_paper_session_ledger_v1


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _producer() -> dict:
    return {"repo": "constellation", "module": "test", "git_sha": "a" * 40}


def _fact_dep(day_utc: str, logical_name: str) -> dict:
    return {
        "logical_name": logical_name,
        "absolute_path": f"/tmp/{logical_name}.json",
        "sha256": "b" * 64,
        "day_utc": day_utc,
        "status": "PASS",
        "reason_codes": [],
    }


def _minimal_fact_row(day_utc: str, logical_name: str) -> dict:
    return {
        "logical_name": logical_name,
        "required_for_authority": logical_name != "sleeve_rollup_v1",
        "absolute_path": f"/tmp/{logical_name}.json",
        "schema_id": logical_name.replace("_v1", ""),
        "schema_version": "v1",
        "producer": _producer(),
        "artifact_timestamp_utc": f"{day_utc}T00:00:00Z",
        "artifact_day_utc": day_utc,
        "artifact_session_id": canonical_paper_session_id_v1(day_utc),
        "content_hash": "f" * 64,
        "presence_verdict": "PRESENT",
        "schema_verdict": "VALID",
        "linkage_verdict": "LINKED",
        "freshness_verdict": "CURRENT",
        "duplicate_resolution_verdict": "SINGLE_CANONICAL_PATH",
        "blocking_codes": [],
        "lookup_evidence": [f"resolved_path=/tmp/{logical_name}.json"],
        "fact_snapshot": {},
    }


def _write_complete_prerequisite(truth_root: Path, day_utc: str) -> None:
    _write_json(
        truth_root / "reports" / "intents_day_completeness_v1" / day_utc / "intents_day_completeness.v1.json",
        {
            "schema_id": "intents_day_completeness",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_PREREQUISITE_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "completeness_status": "COMPLETE",
            "required_inputs_checked": [_fact_dep(day_utc, "intents_day_directory")],
            "missing_inputs": [],
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "blocking_codes": [],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:00:00Z",
        },
    )


def _write_no_intents_marker(truth_root: Path, day_utc: str) -> None:
    marker_path = truth_root / "intents_v1" / "snapshots" / day_utc / "no_intents_day.v1.json"
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    marker_payload = {
        "schema_id": "C2_NO_INTENTS_DAY_V1",
        "schema_version": 1,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": _producer(),
        "status": "OK",
        "reason_codes": ["NO_INTENTS_DECLARED"],
        "input_manifest": [
            {
                "type": "registry_snapshot",
                "path": "/tmp/registry.json",
                "sha256": "c" * 64,
                "day_utc": day_utc,
                "producer": "test",
            }
        ],
        "intents_dir": str(marker_path.parent),
        "intents_json_count": 0,
        "intents_listing_sha256": hashlib.sha256(b"").hexdigest(),
    }
    _write_json(marker_path, marker_payload)


def _write_intent_generation_report(
    truth_root: Path,
    day_utc: str,
    *,
    final_status: str,
    blocker_code: str = "",
    blocker_path: str = "",
) -> None:
    _write_json(
        truth_root / "reports" / "trading_day_intent_generation_v1" / day_utc / "trading_day_intent_generation.v1.json",
        {
            "schema_id": "trading_day_intent_generation",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_PREREQUISITE_FACT",
            "day_utc": day_utc,
            "generation_run_id": f"trading_day_intent_generation:{day_utc}:test",
            "produced_at_utc": f"{day_utc}T00:00:00Z",
            "producer": _producer(),
            "truth_root": str(truth_root),
            "active_engine_registry_path": "/tmp/registry.json",
            "active_engine_registry_sha256": "d" * 64,
            "producer_topology": [],
            "skipped_active_engines": [],
            "producer_results": [],
            "final_status": final_status,
            "first_blocker_code": blocker_code,
            "first_blocker_artifact_path": blocker_path,
            "canonical_outputs": {
                "intents_dir": str(truth_root / "intents_v1" / "snapshots" / day_utc),
                "intent_output_paths": [],
                "no_intents_marker_path": str(
                    truth_root / "intents_v1" / "snapshots" / day_utc / "no_intents_day.v1.json"
                )
                if final_status == "VALID_ZERO"
                else "",
                "output_count": 0,
            },
            "blocking_codes": [blocker_code] if blocker_code else [],
            "human_readable_summary": "test",
        },
    )


def _write_startup(truth_root: Path, day_utc: str, *, status: str = "SUCCESS") -> None:
    _write_json(
        truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json",
        {
            "schema_id": "startup_materialization",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "status": status,
            "required_inputs_checked": [_fact_dep(day_utc, "intent_file:spy.json")],
            "materialized_outputs": [_fact_dep(day_utc, "phasec_materialized_output:spy:binding_record.v2.json")],
            "blocking_codes": [] if status == "SUCCESS" else ["STARTUP_DENIED"],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:00:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "path_resolution_evidence": {
                "phasec_root": "/tmp/phasec",
                "latest_active_attempt_path": "/tmp/latest_active_attempt.v1.json",
            },
            "producer_run_id": f"startup_materialization_v1:{day_utc}",
            "phasec_materializer_result": {"returncode": 0, "stdout": "", "stderr": ""},
        },
    )


def _write_posture(truth_root: Path, day_utc: str, *, enabled: bool = True) -> None:
    _write_json(
        truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json",
        {
            "schema_id": "paper_trading_posture",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "system_ready": enabled,
            "posture_status": "ENABLED" if enabled else "DISABLED",
            "posture_class": "PAPER_READY_ACTIVE" if enabled else "PAPER_READY_NO_OP",
            "blocking_family": "NONE" if enabled else "EXPECTED_NO_OP",
            "expected_no_op_today": not enabled,
            "policy_reasons": ["MARKET_CALENDAR_TRADING_SESSION"] if enabled else ["MARKET_CALENDAR_NON_TRADING_SESSION"],
            "blocking_codes": [] if enabled else ["MARKET_CALENDAR_NON_TRADING_SESSION"],
            "blocking_reason_codes": [] if enabled else ["MARKET_CALENDAR_NON_TRADING_SESSION"],
            "source_dependencies": [
                {
                    "logical_name": "market_calendar_manifest",
                    "absolute_path": "/tmp/market_calendar_manifest.json",
                    "sha256": "c" * 64,
                    "day_utc": day_utc,
                    "status": "OK",
                    "reason_codes": [],
                    "producer": "market_calendar_v1",
                }
            ],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:01:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
        },
    )


def _write_boundary(truth_root: Path, day_utc: str, *, authorized: bool = True) -> None:
    check = _fact_dep(day_utc, "trade_submit_readiness_c2_v1")
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json",
        {
            "schema_id": "submit_boundary_status",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "submission_authorized": authorized,
            "boundary_status": "AUTHORIZED" if authorized else "DENIED",
            "required_boundary_checks": [check],
            "failed_checks": [] if authorized else [check],
            "blocking_codes": [] if authorized else ["SUBMIT_BOUNDARY_TRADE_SUBMIT_READINESS_DENIED"],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:02:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "paper_account": "DUO847203",
        },
    )


def _write_ledger(truth_root: Path, day_utc: str, *, authority_status: str) -> None:
    fact_rows = [
        _minimal_fact_row(day_utc, "paper_trading_posture_v1"),
        _minimal_fact_row(day_utc, "startup_materialization_v1"),
        _minimal_fact_row(day_utc, "submit_boundary_status_v1"),
        _minimal_fact_row(day_utc, "sleeve_rollup_v1"),
    ]
    ledger = build_paper_session_ledger_v1(
        day_utc=day_utc,
        session_id=canonical_paper_session_id_v1(day_utc),
        evaluated_at_utc=f"{day_utc}T00:00:00Z",
        provenance=_producer(),
        fact_refs=fact_rows,
        evidence_freeze={
            "evidence_digest": "b" * 64,
            "overall_evidence_status": "READY" if authority_status == "GRANTED" else "DENY",
            "blocking_codes": [] if authority_status == "GRANTED" else ["BLOCKED"],
            "inputs": [row for row in fact_rows if row["required_for_authority"]],
        },
        authority_status=authority_status,
        system_ready=authority_status == "GRANTED",
        submission_authorized=authority_status == "GRANTED",
        control_blocking_codes=[] if authority_status == "GRANTED" else ["BLOCKED"],
        submit_lifecycle={
            "submit_attempt_status": "ATTEMPTED" if authority_status == "GRANTED" else "SKIPPED",
            "submit_attempted": authority_status == "GRANTED",
            "submit_result_status": "PASS" if authority_status == "GRANTED" else "NOT_AUTHORIZED",
            "reason_codes": [] if authority_status == "GRANTED" else ["BLOCKED"],
            "submit_evidence_refs": ["/tmp/sleeve_rollup.v1.json"] if authority_status == "GRANTED" else [],
            "finalization_status": "OPEN",
        },
        post_submit_lifecycle={
            "lineage_status": "NOT_OBSERVED_AT_EVALUATION",
            "latest_authoritative_lineage_ref": "",
            "latest_authoritative_lineage_sha256": "",
            "execution_evidence_refs": [],
            "reconciliation_refs": [],
            "gap_codes": ["PAPER_SESSION_LEDGER_POST_SUBMIT_NOT_OBSERVED"],
        },
        operator_summary={
            "authority_scope": "DERIVED_ONLY_VIEW",
            "ledger_ref": "/tmp/paper_session_ledger.v1.json",
            "summary_state": "AUTHORIZED_TO_PROCEED" if authority_status == "GRANTED" else "AUTHORITY_DENIED",
            "authority_status": authority_status,
            "submission_authorized": authority_status == "GRANTED",
            "non_authority_notice": "Derived only.",
            "blocking_codes": [] if authority_status == "GRANTED" else ["BLOCKED"],
        },
    )
    write_paper_session_ledger_v1(truth_root=truth_root, ledger=ledger)


def _write_startup_proof(truth_root: Path, day_utc: str, *, ready: bool) -> None:
    _write_json(
        truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json",
        {
            "schema_id": "startup_proof_validation",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_STARTUP_VALIDATION",
            "validation_scope": "BOD_CONTROL_PLANE_ONLY",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "status": "STARTUP_READY" if ready else "STARTUP_BLOCKED",
            "ledger_ref": str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"),
            "ledger_id": f"paper_session_ledger:{day_utc}:test",
            "ledger_authority_status": "GRANTED" if ready else "DENIED",
            "checks": [
                {"logical_name": "repo_authority", "status": "PASS", "evidence_ref": "/tmp/repo_role.v1.json", "blocking_codes": [], "detail": {}},
                {"logical_name": "startup_materialization", "status": "PASS" if ready else "FAIL", "evidence_ref": "/tmp/startup_materialization.v1.json", "blocking_codes": [] if ready else ["BLOCKED"], "detail": {}},
                {"logical_name": "paper_session_ledger", "status": "PASS" if ready else "FAIL", "evidence_ref": "/tmp/paper_session_ledger.v1.json", "blocking_codes": [] if ready else ["BLOCKED"], "detail": {}}
            ],
            "blocking_codes": [] if ready else ["BLOCKED"],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:05:00Z",
            "non_authority_notice": "Derived validation only.",
        },
    )


def _write_paper_day_control_plane(truth_root: Path, day_utc: str, *, final_decision: str, authority_status: str) -> None:
    blocker_code = "" if authority_status == "GRANTED" else "LEDGER_BLOCKED"
    _write_json(
        truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json",
        {
            "schema_id": "paper_day_control_plane",
            "schema_version": "v1",
            "authority_scope": "SUPPORTING_DAY_CONTROL_ARTIFACT",
            "day_utc": day_utc,
            "startup_attempt_id": f"paper_day_start_attempt:{day_utc}:test",
            "control_plane_id": f"paper_day_control_plane:{day_utc}:test",
            "evaluated_at_utc": f"{day_utc}T00:00:00Z",
            "producer": _producer(),
            "prerequisite_gate": {
                "prerequisite_status": "PASS",
                "prerequisite_blocking_codes": [],
                "prerequisite_artifact_refs": [str(truth_root / "reports" / "intents_day_completeness_v1" / day_utc / "intents_day_completeness.v1.json")],
                "first_missing_prerequisite": "",
            },
            "canonical_regeneration_results": [
                {"logical_name": "startup_materialization_v1", "path": str(truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json"), "status": "SUCCESS", "return_code": 0, "digest": "1" * 64, "produced_at": f"{day_utc}T00:01:00Z"},
                {"logical_name": "paper_trading_posture_v1", "path": str(truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json"), "status": "ENABLED", "return_code": 0, "digest": "2" * 64, "produced_at": f"{day_utc}T00:02:00Z"},
                {"logical_name": "submit_boundary_status_v1", "path": str(truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"), "status": "AUTHORIZED" if authority_status == "GRANTED" else "DENIED", "return_code": 0 if authority_status == "GRANTED" else 2, "digest": "3" * 64, "produced_at": f"{day_utc}T00:03:00Z"},
                {"logical_name": "paper_session_ledger_v1", "path": str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"), "status": authority_status, "return_code": 0 if authority_status == "GRANTED" else 2, "digest": "4" * 64, "produced_at": f"{day_utc}T00:04:00Z"},
                {"logical_name": "startup_proof_validation_v1", "path": str(truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json"), "status": "STARTUP_READY" if authority_status == "GRANTED" else "STARTUP_BLOCKED", "return_code": 0 if authority_status == "GRANTED" else 2, "digest": "5" * 64, "produced_at": f"{day_utc}T00:05:00Z"}
            ],
            "authority_result": {
                "ledger_path": str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"),
                "ledger_id": f"paper_session_ledger:{day_utc}:test",
                "ledger_authority_status": authority_status,
                "ledger_evidence_status": "READY" if authority_status == "GRANTED" else "DENY",
                "first_true_blocker_code": blocker_code,
                "first_true_blocker_artifact_path": "" if authority_status == "GRANTED" else str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"),
            },
            "startup_proof_result": {
                "startup_proof_validation_path": str(truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json"),
                "startup_proof_validation_status": "STARTUP_READY" if authority_status == "GRANTED" else "STARTUP_BLOCKED",
                "agreement_with_ledger": True,
            },
            "final_start_decision": final_decision,
            "blocking_codes": [] if authority_status == "GRANTED" else ["LEDGER_BLOCKED"],
            "human_readable_summary": "Supporting paper-day control plane for test.",
            "ignored_legacy_surfaces": [{"logical_name": "trading_day_state_v1", "path": "/tmp/trading_day_state.v1.json", "exists": False}],
            "suppression_reason": "NON_AUTHORITATIVE_FOR_STARTUP",
        },
    )


def _write_trading_day_control_plane(truth_root: Path, day_utc: str, *, final_decision: str, authority_status: str) -> None:
    _write_json(
        truth_root / "reports" / "trading_day_control_plane_v1" / day_utc / "trading_day_control_plane.v1.json",
        {
            "schema_id": "trading_day_control_plane",
            "schema_version": "v1",
            "authority_scope": "SUPPORTING_TRADING_DAY_CONTROL_ARTIFACT",
            "day_utc": day_utc,
            "day_attempt_id": f"trading_day_start_attempt:{day_utc}:test",
            "control_plane_id": f"trading_day_control_plane:{day_utc}:test",
            "evaluated_at_utc": f"{day_utc}T00:00:00Z",
            "producer": _producer(),
            "supersession": {
                "semantics": "LATEST_AUTHORITATIVE_SAME_DAY_PATH",
                "supersedes_prior_control_plane": False,
                "prior_control_plane_id": "",
                "prior_day_attempt_id": "",
            },
            "upstream_completeness": {
                "intents_day_completeness_ref": str(truth_root / "reports" / "intents_day_completeness_v1" / day_utc / "intents_day_completeness.v1.json"),
                "completeness_status": "COMPLETE",
                "prerequisite_blocking_codes": [],
                "first_missing_prerequisite": "",
            },
            "supporting_daily_control": {
                "paper_day_control_plane_path": str(truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json"),
                "paper_day_control_plane_id": f"paper_day_control_plane:{day_utc}:test",
                "paper_day_startup_attempt_id": f"paper_day_start_attempt:{day_utc}:test",
                "paper_day_final_start_decision": final_decision,
            },
            "canonical_regeneration_results": [
                {"logical_name": "startup_materialization_v1", "path": str(truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json"), "status": "SUCCESS", "return_code": 0, "digest": "1" * 64, "produced_at": f"{day_utc}T00:01:00Z"},
                {"logical_name": "paper_trading_posture_v1", "path": str(truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json"), "status": "ENABLED", "return_code": 0, "digest": "2" * 64, "produced_at": f"{day_utc}T00:02:00Z"},
                {"logical_name": "submit_boundary_status_v1", "path": str(truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"), "status": "AUTHORIZED" if authority_status == "GRANTED" else "DENIED", "return_code": 0 if authority_status == "GRANTED" else 2, "digest": "3" * 64, "produced_at": f"{day_utc}T00:03:00Z"},
                {"logical_name": "paper_session_ledger_v1", "path": str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"), "status": authority_status, "return_code": 0 if authority_status == "GRANTED" else 2, "digest": "4" * 64, "produced_at": f"{day_utc}T00:04:00Z"},
                {"logical_name": "startup_proof_validation_v1", "path": str(truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json"), "status": "STARTUP_READY" if authority_status == "GRANTED" else "STARTUP_BLOCKED", "return_code": 0 if authority_status == "GRANTED" else 2, "digest": "5" * 64, "produced_at": f"{day_utc}T00:05:00Z"}
            ],
            "supporting_session_authority": {
                "paper_session_ledger_path": str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"),
                "ledger_id": f"paper_session_ledger:{day_utc}:test",
                "ledger_authority_status": authority_status,
                "ledger_evidence_status": "READY" if authority_status == "GRANTED" else "DENY",
                "system_ready": authority_status == "GRANTED",
                "submission_authorized": authority_status == "GRANTED",
            },
            "first_true_blocker": {
                "first_true_blocker_code": "" if authority_status == "GRANTED" else "LEDGER_BLOCKED",
                "first_true_blocker_artifact_path": "" if authority_status == "GRANTED" else str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"),
                "blocker_classification": "UNKNOWN" if authority_status == "GRANTED" else "SUPPORTING_AUTHORITY_DENY",
            },
            "startup_proof_result": {
                "startup_proof_validation_path": str(truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json"),
                "startup_proof_validation_status": "STARTUP_READY" if authority_status == "GRANTED" else "STARTUP_BLOCKED",
                "agreement_with_supporting_authority": True,
            },
            "final_start_decision": final_decision,
            "blocking_codes": [] if authority_status == "GRANTED" else ["LEDGER_BLOCKED"],
            "human_readable_summary": "Supporting trading-day control plane for test.",
            "ignored_legacy_surfaces": [
                {"logical_name": "paper_day_control_plane_v1", "path": "/tmp/paper_day_control_plane.v1.json", "exists": True},
                {"logical_name": "trading_day_state_v1", "path": "/tmp/trading_day_state.v1.json", "exists": False}
            ],
            "suppression_reason": "NON_AUTHORITATIVE_FOR_STARTUP",
            "derived_daily_summary": {
                "summary_state": final_decision,
                "control_plane_id": f"trading_day_control_plane:{day_utc}:test",
                "day_attempt_id": f"trading_day_start_attempt:{day_utc}:test",
                "first_true_blocker_code": "" if authority_status == "GRANTED" else "LEDGER_BLOCKED",
                "ledger_id": f"paper_session_ledger:{day_utc}:test",
                "non_authority_notice": "Supporting artifact only.",
            },
        },
    )


def _write_execution_control_plane(truth_root: Path, day_utc: str, *, final_decision: str, authority_status: str) -> None:
    blocker_code = "" if authority_status == "GRANTED" else "LEDGER_BLOCKED"
    _write_json(
        truth_root / "reports" / "trading_day_execution_control_plane_v1" / day_utc / "trading_day_execution_control_plane.v1.json",
        {
            "schema_id": "trading_day_execution_control_plane",
            "schema_version": "v1",
            "authority_scope": "SUPPORTING_TRADING_DAY_EXECUTION_CONTROL_ARTIFACT",
            "day_utc": day_utc,
            "day_attempt_id": f"trading_day_execution_start_attempt:{day_utc}:test",
            "execution_control_plane_id": f"trading_day_execution_control_plane:{day_utc}:test",
            "evaluated_at_utc": f"{day_utc}T00:00:00Z",
            "supersedes_prior_control_plane": False,
            "prior_control_plane_id": "",
            "prior_day_attempt_id": "",
            "producer": _producer(),
            "upstream_completeness": {
                "intents_day_completeness_ref": str(truth_root / "reports" / "intents_day_completeness_v1" / day_utc / "intents_day_completeness.v1.json"),
                "completeness_status": "COMPLETE",
                "prerequisite_blocking_codes": [],
                "first_missing_prerequisite": "",
            },
            "supporting_daily_controls": {
                "paper_day_control_plane_path": str(truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json"),
                "paper_day_control_plane_id": f"paper_day_control_plane:{day_utc}:test",
                "paper_day_startup_attempt_id": f"paper_day_start_attempt:{day_utc}:test",
                "paper_day_final_start_decision": final_decision,
                "trading_day_control_plane_path": str(truth_root / "reports" / "trading_day_control_plane_v1" / day_utc / "trading_day_control_plane.v1.json"),
                "trading_day_control_plane_id": f"trading_day_control_plane:{day_utc}:test",
                "trading_day_day_attempt_id": f"trading_day_start_attempt:{day_utc}:test",
                "trading_day_final_start_decision": final_decision,
            },
            "canonical_regeneration_results": [
                {"logical_name": "startup_materialization_v1", "path": str(truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json"), "status": "SUCCESS", "return_code": 0, "digest": "1" * 64, "produced_at": f"{day_utc}T00:01:00Z"},
                {"logical_name": "paper_trading_posture_v1", "path": str(truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json"), "status": "ENABLED", "return_code": 0, "digest": "2" * 64, "produced_at": f"{day_utc}T00:02:00Z"},
                {"logical_name": "submit_boundary_status_v1", "path": str(truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"), "status": "AUTHORIZED" if authority_status == "GRANTED" else "DENIED", "return_code": 0 if authority_status == "GRANTED" else 2, "digest": "3" * 64, "produced_at": f"{day_utc}T00:03:00Z"},
                {"logical_name": "paper_session_ledger_v1", "path": str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"), "status": authority_status, "return_code": 0 if authority_status == "GRANTED" else 2, "digest": "4" * 64, "produced_at": f"{day_utc}T00:04:00Z"},
                {"logical_name": "startup_proof_validation_v1", "path": str(truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json"), "status": "STARTUP_READY" if authority_status == "GRANTED" else "STARTUP_BLOCKED", "return_code": 0 if authority_status == "GRANTED" else 2, "digest": "5" * 64, "produced_at": f"{day_utc}T00:05:00Z"}
            ],
            "supporting_session_authority": {
                "paper_session_ledger_path": str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"),
                "ledger_id": f"paper_session_ledger:{day_utc}:test",
                "ledger_authority_status": authority_status,
                "ledger_evidence_status": "READY" if authority_status == "GRANTED" else "DENY",
                "system_ready": authority_status == "GRANTED",
                "submission_authorized": authority_status == "GRANTED",
            },
            "first_true_blocker": {
                "first_true_blocker_code": blocker_code,
                "first_true_blocker_artifact_path": "" if authority_status == "GRANTED" else str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"),
                "blocker_classification": "UNKNOWN" if authority_status == "GRANTED" else "SUPPORTING_AUTHORITY_DENY",
            },
            "startup_proof_result": {
                "startup_proof_validation_path": str(truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json"),
                "startup_proof_validation_status": "STARTUP_READY" if authority_status == "GRANTED" else "STARTUP_BLOCKED",
                "agreement_with_supporting_authority": True,
            },
            "final_start_decision": final_decision,
            "blocking_codes": [] if authority_status == "GRANTED" else ["LEDGER_BLOCKED"],
            "human_readable_summary": "Supporting execution control plane for test.",
            "ignored_legacy_surfaces": [{"logical_name": "trading_day_state_v1", "path": "/tmp/trading_day_state.v1.json", "exists": False}],
            "ignored_superseded_surfaces": [
                {"logical_name": "paper_day_control_plane_v1", "path": str(truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json"), "exists": True},
                {"logical_name": "trading_day_control_plane_v1", "path": str(truth_root / "reports" / "trading_day_control_plane_v1" / day_utc / "trading_day_control_plane.v1.json"), "exists": True}
            ],
            "suppression_reason": "NON_AUTHORITATIVE_FOR_STARTUP",
            "derived_daily_summary": {
                "summary_state": final_decision,
                "execution_control_plane_id": f"trading_day_execution_control_plane:{day_utc}:test",
                "day_attempt_id": f"trading_day_execution_start_attempt:{day_utc}:test",
                "first_true_blocker_code": blocker_code,
                "ledger_id": f"paper_session_ledger:{day_utc}:test",
                "non_authority_notice": "Supporting artifact only.",
            },
        },
    )


def _surface_ref(path: Path) -> SimpleNamespace:
    return SimpleNamespace(path=path, payload=json.loads(path.read_text(encoding="utf-8")), sha256="d" * 64)


def _supporting_surface_patchers(truth_root: Path, day_utc: str) -> list:
    return [
        patch.object(
            state_machine_module,
            "read_startup_materialization_ref_v1",
            side_effect=lambda *args, **kwargs: _surface_ref(
                truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json"
            ),
        ),
        patch.object(
            state_machine_module,
            "read_paper_trading_posture_ref_v1",
            side_effect=lambda *args, **kwargs: _surface_ref(
                truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json"
            ),
        ),
        patch.object(
            state_machine_module,
            "read_submit_boundary_status_ref_v1",
            side_effect=lambda *args, **kwargs: _surface_ref(
                truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"
            ),
        ),
        patch.object(
            state_machine_module,
            "read_paper_session_ledger_ref_v1",
            side_effect=lambda *args, **kwargs: _surface_ref(
                truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"
            ),
        ),
        patch.object(
            state_machine_module,
            "read_startup_proof_validation_ref_v1",
            side_effect=lambda *args, **kwargs: _surface_ref(
                truth_root
                / "reports"
                / "startup_proof_validation_v1"
                / day_utc
                / "startup_proof_validation.v1.json"
            ),
        ),
    ]


def _write_deployment_state_machine(truth_root: Path, day_utc: str, *, release_root: Path) -> None:
    release_root.mkdir(parents=True, exist_ok=True)
    _write_json(
        release_root / "release_manifest.v1.json",
        {
            "schema_id": "release_manifest.v1",
            "schema_version": "v1",
            "release_id": "release-001",
            "git_sha": "a" * 40,
            "source_root": "/home/node/constellation",
            "release_root": str(release_root),
            "included_files": [],
            "included_file_hashes": {},
            "generated_at_utc": "2026-04-08T00:00:00Z",
        },
    )
    _write_json(
        truth_root / "reports" / "deployment_state_machine_v1" / day_utc / "deployment_state_machine.v1.json",
        {
            "schema_id": "deployment_state_machine",
            "schema_version": "v1",
            "authority_scope": "TOP_LEVEL_DEPLOYMENT_STATE_MACHINE_OWNER",
            "day_utc": day_utc,
            "deployment_attempt_id": f"deployment_state_machine_attempt:{day_utc}:test",
            "deployment_state_machine_id": f"deployment_state_machine:{day_utc}:test",
            "evaluated_at_utc": f"{day_utc}T00:00:00Z",
            "authoritative_source": {
                "authoritative_repo_root": "/home/node/constellation",
                "authoritative_git_sha": "a" * 40,
                "authoritative_branch": "feature/test",
                "authoritative_cleanliness_status": "CLEAN",
                "dirty_entry_count": 0,
                "dirty_entry_sample": [],
                "source_snapshot_id": "a" * 40,
            },
            "release_build": {
                "release_id": "release-001",
                "release_root": str(release_root),
                "release_manifest_sha": "b" * 64,
                "bundled_file_hash_summary": {"file_count": 0, "aggregate_sha256": "c" * 64},
                "build_status": "RELEASE_PRESENT",
                "required_startup_stack_files_present": True,
                "missing_required_startup_stack_files": [],
            },
            "active_release": {
                "active_symlink_path": "/home/node/constellation_active",
                "active_symlink_target": str(release_root),
                "activation_status": "ACTIVE_POINTER_PRESENT",
                "active_runtime_contract_path": "/tmp/active_runtime_contract.v1.json",
                "active_runtime_contract_status": "ACTIVE",
            },
            "live_execution": {
                "service_unit_path": "/tmp/live.service",
                "launcher_path": "/home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
                "resolved_execution_root": "/home/node/constellation_active",
                "active_root_match": True,
                "authoritative_service_source_path": "/tmp/source.service",
                "authoritative_service_source_root": "/home/node/constellation_active",
            },
            "drift_checks": {
                "authoritative_vs_release": {"status": "PASS", "details": {}},
                "release_vs_active": {"status": "PASS", "details": {}},
                "active_vs_live_execution": {"status": "PASS", "details": {}},
                "runtime_copy_still_executable": {"status": "PASS", "details": {}},
                "required_startup_stack_files_present": {"status": "PASS", "details": {"missing_required_startup_stack_files": []}}
            },
            "post_activation_verification": {
                "passed": True,
                "service_unit_path": "/tmp/live.service",
                "launcher_path": "/home/node/constellation_active/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh",
                "resolved_execution_root": "/home/node/constellation_active",
                "active_root_match": True,
                "blocking_codes": [],
            },
            "final_deployment_decision": "DEPLOY_ACTIVE",
            "blocking_codes": [],
            "first_true_blocker_code": "",
            "first_true_blocker_path": "",
            "human_readable_summary": "test",
            "producer": _producer(),
        },
    )


def test_trading_day_state_machine_missing_intents_blocks_immediately(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"

    original_run = state_machine_module._run
    with patch.object(state_machine_module, "resolve_decision_truth_root_v1", return_value=truth_root), patch.object(state_machine_module, "_run", side_effect=lambda cmd, truth_root: (
        (_write_intent_generation_report(truth_root, day_utc, final_status="INTENTS_PRESENT") or {"return_code": 0, "stdout": "{}", "stderr": ""})
        if Path(cmd[1]).name == "run_trading_day_intent_generation_v1.py"
        else original_run(cmd, truth_root=truth_root)
    )):
        rc = state_machine_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 2
    payload = json.loads(
        (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").read_text(encoding="utf-8")
    )
    assert payload["final_start_decision"] == "BLOCKED_VALID"
    assert payload["first_true_blocker"]["first_true_blocker_code"] == "INTENTS_DAY_COMPLETENESS_MISSING_DAY_DIR"
    assert payload["first_true_blocker"]["blocker_classification"] == "UPSTREAM_PREREQUISITE"
    assert payload["upstream_intent_status"]["valid_zero_intent_status"] == "NOT_DECLARED"
    assert "PAPER_TRADING_POSTURE:OPERATIONAL_READINESS" not in payload["blocking_codes"]
    assert [row["to_state"] for row in payload["state_transitions"]][-1] == "DAY_NOT_OPENED"


def test_trading_day_state_machine_zero_intent_marker_blocks_validly(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    _write_no_intents_marker(truth_root, day_utc)

    original_run = state_machine_module._run
    with patch.object(state_machine_module, "resolve_decision_truth_root_v1", return_value=truth_root), patch.object(state_machine_module, "_run", side_effect=lambda cmd, truth_root: (
        (_write_intent_generation_report(truth_root, day_utc, final_status="VALID_ZERO") or {"return_code": 0, "stdout": "{}", "stderr": ""})
        if Path(cmd[1]).name == "run_trading_day_intent_generation_v1.py"
        else original_run(cmd, truth_root=truth_root)
    )):
        rc = state_machine_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 2
    payload = json.loads(
        (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").read_text(encoding="utf-8")
    )
    assert payload["upstream_intent_status"]["completeness_status"] == "NO_INTENTS_DECLARED"
    assert payload["upstream_intent_status"]["valid_zero_intent_status"] == "DECLARED"
    assert payload["final_start_decision"] == "BLOCKED_VALID"
    assert payload["first_true_blocker"]["blocker_classification"] == "UPSTREAM_PREREQUISITE"


def test_trading_day_state_machine_runs_supporting_chain_records_order_and_can_ready_now(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    order: list[str] = []

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        order.append(tool_name)
        if tool_name == "run_trading_day_intent_generation_v1.py":
            _write_intent_generation_report(truth_root, day_utc, final_status="INTENTS_PRESENT")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_trading_day_execution_control_plane_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
            _write_posture(truth_root, day_utc, enabled=True)
            _write_boundary(truth_root, day_utc, authorized=True)
            _write_ledger(truth_root, day_utc, authority_status="GRANTED")
            _write_startup_proof(truth_root, day_utc, ready=True)
            _write_paper_day_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            _write_trading_day_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            _write_execution_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with ExitStack() as stack:
        stack.enter_context(patch.object(state_machine_module, "resolve_decision_truth_root_v1", return_value=truth_root))
        stack.enter_context(patch.object(state_machine_module, "_run", side_effect=fake_run))
        for patcher in _supporting_surface_patchers(truth_root, day_utc):
            stack.enter_context(patcher)
        rc = state_machine_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    assert order == [
        "run_trading_day_intent_generation_v1.py",
        "run_intents_day_completeness_v1.py",
        "run_trading_day_execution_control_plane_v1.py",
    ]
    payload = json.loads(
        (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").read_text(encoding="utf-8")
    )
    assert payload["final_start_decision"] == "READY_NOW"
    assert payload["supporting_session_authority"]["ledger_authority_status"] == "GRANTED"
    assert payload["supporting_session_authority"]["system_ready"] is True
    assert payload["supporting_session_authority"]["submission_authorized"] is True
    assert payload["supporting_daily_control_refs"]["trading_day_execution_final_start_decision"] == "READY_NOW"
    assert [row["to_state"] for row in payload["state_transitions"]] == [
        "DAY_CREATED",
        "PREREQUISITE_EVALUATED",
        "INTENTS_READY",
        "SUPPORTING_REGEN_COMPLETE",
        "SESSION_AUTHORITY_GRANTED",
        "DAY_OPEN_ALLOWED",
        "DAY_NOT_OPENED",
    ]


def test_trading_day_state_machine_ready_now_clears_stale_blockers(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_trading_day_intent_generation_v1.py":
            _write_intent_generation_report(truth_root, day_utc, final_status="INTENTS_PRESENT")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_trading_day_execution_control_plane_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
            _write_posture(truth_root, day_utc, enabled=True)
            _write_boundary(truth_root, day_utc, authorized=True)
            _write_ledger(truth_root, day_utc, authority_status="GRANTED")
            _write_startup_proof(truth_root, day_utc, ready=True)
            _write_paper_day_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            _write_trading_day_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            _write_execution_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            execution_path = (
                truth_root
                / "reports"
                / "trading_day_execution_control_plane_v1"
                / day_utc
                / "trading_day_execution_control_plane.v1.json"
            )
            execution_payload = json.loads(execution_path.read_text(encoding="utf-8"))
            execution_payload["blocking_codes"] = ["HIDDEN_DEPENDENCY_DETECTED"]
            execution_payload["first_true_blocker"] = {
                "first_true_blocker_code": "HIDDEN_DEPENDENCY_DETECTED",
                "first_true_blocker_artifact_path": "/tmp/stale.json",
                "blocker_classification": "UNKNOWN",
            }
            _write_json(execution_path, execution_payload)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with ExitStack() as stack:
        stack.enter_context(patch.object(state_machine_module, "resolve_decision_truth_root_v1", return_value=truth_root))
        stack.enter_context(patch.object(state_machine_module, "_run", side_effect=fake_run))
        for patcher in _supporting_surface_patchers(truth_root, day_utc):
            stack.enter_context(patcher)
        rc = state_machine_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    payload = json.loads(
        (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").read_text(encoding="utf-8")
    )
    assert payload["final_start_decision"] == "READY_NOW"
    assert payload["blocking_codes"] == []
    assert payload["first_true_blocker"]["first_true_blocker_code"] == ""


def test_trading_day_state_machine_direct_emits_journal_event_idempotently(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"
    _write_deployment_state_machine(truth_root, day_utc, release_root=tmp_path / "release")

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_trading_day_intent_generation_v1.py":
            _write_intent_generation_report(truth_root, day_utc, final_status="INTENTS_PRESENT")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_trading_day_execution_control_plane_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
            _write_posture(truth_root, day_utc, enabled=True)
            _write_boundary(truth_root, day_utc, authorized=True)
            _write_ledger(truth_root, day_utc, authority_status="GRANTED")
            _write_startup_proof(truth_root, day_utc, ready=True)
            _write_paper_day_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            _write_trading_day_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            _write_execution_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with ExitStack() as stack:
        stack.enter_context(patch.object(state_machine_module, "resolve_decision_truth_root_v1", return_value=truth_root))
        stack.enter_context(patch.object(state_machine_module, "_run", side_effect=fake_run))
        for patcher in _supporting_surface_patchers(truth_root, day_utc):
            stack.enter_context(patcher)
        first_rc = state_machine_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        first_payload = json.loads(
            (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").read_text(encoding="utf-8")
        )
        second_rc = state_machine_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        second_payload = json.loads(
            (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").read_text(encoding="utf-8")
        )

    journal_payload = json.loads(
        (truth_root / "reports" / "execution_journal_v1" / day_utc / "execution_journal.v1.json").read_text(encoding="utf-8")
    )
    event_types = [row["event_type"] for row in journal_payload["events"]]
    assert first_rc == 0
    assert second_rc == 0
    assert second_payload["day_attempt_id"] == first_payload["day_attempt_id"]
    assert event_types.count("STATE_MACHINE_DECISION_RECORDED") == 1


def test_trading_day_state_machine_records_supporting_authority_deny(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_trading_day_intent_generation_v1.py":
            _write_intent_generation_report(truth_root, day_utc, final_status="INTENTS_PRESENT")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_trading_day_execution_control_plane_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
            _write_posture(truth_root, day_utc, enabled=True)
            _write_boundary(truth_root, day_utc, authorized=False)
            _write_ledger(truth_root, day_utc, authority_status="DENIED")
            _write_startup_proof(truth_root, day_utc, ready=False)
            _write_paper_day_control_plane(truth_root, day_utc, final_decision="BLOCKED_VALID", authority_status="DENIED")
            _write_trading_day_control_plane(truth_root, day_utc, final_decision="BLOCKED_VALID", authority_status="DENIED")
            _write_execution_control_plane(truth_root, day_utc, final_decision="BLOCKED_VALID", authority_status="DENIED")
            return {"return_code": 2, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with ExitStack() as stack:
        stack.enter_context(patch.object(state_machine_module, "resolve_decision_truth_root_v1", return_value=truth_root))
        stack.enter_context(patch.object(state_machine_module, "_run", side_effect=fake_run))
        for patcher in _supporting_surface_patchers(truth_root, day_utc):
            stack.enter_context(patcher)
        rc = state_machine_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 2
    payload = json.loads(
        (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").read_text(encoding="utf-8")
    )
    assert payload["final_start_decision"] == "BLOCKED_VALID"
    assert payload["supporting_session_authority"]["ledger_authority_status"] == "DENIED"
    assert payload["first_true_blocker"]["blocker_classification"] == "SUPPORTING_AUTHORITY_DENY"


def test_trading_day_state_machine_records_open_trigger_emitted(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"

    _write_json(
        truth_root / "reports" / "day_open_trigger_v1" / day_utc / "day_open_trigger.v1.json",
        {
            "trigger_status": "EMITTED",
            "dedupe_key": f"{day_utc}:INITIAL_BOD_TRIGGER:{day_utc}T09:31:00-04:00",
            "consumed": False,
        },
    )

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_trading_day_intent_generation_v1.py":
            _write_intent_generation_report(truth_root, day_utc, final_status="INTENTS_PRESENT")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_trading_day_execution_control_plane_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
            _write_posture(truth_root, day_utc, enabled=True)
            _write_boundary(truth_root, day_utc, authorized=True)
            _write_ledger(truth_root, day_utc, authority_status="GRANTED")
            _write_startup_proof(truth_root, day_utc, ready=True)
            _write_paper_day_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            _write_trading_day_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            _write_execution_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with ExitStack() as stack:
        stack.enter_context(patch.object(state_machine_module, "resolve_decision_truth_root_v1", return_value=truth_root))
        stack.enter_context(patch.object(state_machine_module, "_run", side_effect=fake_run))
        stack.enter_context(patch.object(state_machine_module, "build_day_open_window_v1", return_value=SimpleNamespace(window_status="OPEN_WINDOW")))
        for patcher in _supporting_surface_patchers(truth_root, day_utc):
            stack.enter_context(patcher)
        rc = state_machine_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    payload = json.loads(
        (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").read_text(encoding="utf-8")
    )
    assert payload["open_lifecycle_state"] == "OPEN_TRIGGER_EMITTED"
    assert payload["state_transitions"][-1]["to_state"] == "DAY_OPEN_TRIGGER_EMITTED"


def test_trading_day_state_machine_marks_paper_open_available_after_window(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_trading_day_intent_generation_v1.py":
            _write_intent_generation_report(truth_root, day_utc, final_status="INTENTS_PRESENT")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_trading_day_execution_control_plane_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
            _write_posture(truth_root, day_utc, enabled=True)
            _write_boundary(truth_root, day_utc, authorized=True)
            _write_ledger(truth_root, day_utc, authority_status="GRANTED")
            _write_startup_proof(truth_root, day_utc, ready=True)
            _write_paper_day_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            _write_trading_day_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            _write_execution_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with ExitStack() as stack:
        stack.enter_context(patch.object(state_machine_module, "resolve_decision_truth_root_v1", return_value=truth_root))
        stack.enter_context(patch.object(state_machine_module, "_run", side_effect=fake_run))
        stack.enter_context(patch.object(state_machine_module, "build_day_open_window_v1", return_value=SimpleNamespace(window_status="POST_OPEN_WINDOW")))
        for patcher in _supporting_surface_patchers(truth_root, day_utc):
            stack.enter_context(patcher)
        rc = state_machine_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 0
    payload = json.loads(
        (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").read_text(encoding="utf-8")
    )
    assert payload["open_lifecycle_state"] == "PAPER_OPEN_AVAILABLE"
    assert payload["first_true_blocker"]["first_true_blocker_code"] == ""
    assert "available now" in payload["human_readable_summary"].lower()
    assert payload["state_transitions"][-1]["transition_reason_code"] == "STATE_MACHINE_AUTHORITY_ONLY_NO_OPEN_COMMAND_EXECUTED"


def test_trading_day_state_machine_marks_missing_supporting_output_as_defect(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_trading_day_intent_generation_v1.py":
            _write_intent_generation_report(truth_root, day_utc, final_status="INTENTS_PRESENT")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_trading_day_execution_control_plane_v1.py":
            return {"return_code": 3, "stdout": "{}", "stderr": "missing"}
        raise AssertionError(tool_name)

    with patch.object(state_machine_module, "resolve_decision_truth_root_v1", return_value=truth_root), patch.object(state_machine_module, "_run", side_effect=fake_run):
        rc = state_machine_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])

    assert rc == 3
    payload = json.loads(
        (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").read_text(encoding="utf-8")
    )
    assert payload["final_start_decision"] == "BLOCKED_BY_DEFECT"
    assert payload["first_true_blocker"]["first_true_blocker_code"] == "TRADING_DAY_STATE_MACHINE_EXECUTION_CONTROL_OUTPUT_MISSING_OR_INVALID"


def test_trading_day_state_machine_supersession_is_explicit(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-08"

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_trading_day_intent_generation_v1.py":
            _write_intent_generation_report(truth_root, day_utc, final_status="INTENTS_PRESENT")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_complete_prerequisite(truth_root, day_utc)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_trading_day_execution_control_plane_v1.py":
            _write_startup(truth_root, day_utc, status="SUCCESS")
            _write_posture(truth_root, day_utc, enabled=True)
            _write_boundary(truth_root, day_utc, authorized=True)
            _write_ledger(truth_root, day_utc, authority_status="GRANTED")
            _write_startup_proof(truth_root, day_utc, ready=True)
            _write_paper_day_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            _write_trading_day_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            _write_execution_control_plane(truth_root, day_utc, final_decision="READY_NOW", authority_status="GRANTED")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with ExitStack() as stack:
        stack.enter_context(patch.object(state_machine_module, "resolve_decision_truth_root_v1", return_value=truth_root))
        stack.enter_context(patch.object(state_machine_module, "_run", side_effect=fake_run))
        for patcher in _supporting_surface_patchers(truth_root, day_utc):
            stack.enter_context(patcher)
        first_rc = state_machine_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        first_payload = json.loads(
            (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").read_text(encoding="utf-8")
        )
        second_rc = state_machine_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        second_payload = json.loads(
            (truth_root / "reports" / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json").read_text(encoding="utf-8")
        )

    assert first_rc == 0
    assert second_rc == 0
    assert second_payload["supersedes_prior_state_machine"] is True
    assert second_payload["prior_state_machine_id"] == first_payload["state_machine_id"]
    assert second_payload["prior_day_attempt_id"] == first_payload["day_attempt_id"]
