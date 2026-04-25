from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_paper_day_control_plane_v1 as control_plane_module
from constellation_2.common.next_day_readiness_consistency_gate_v1 import (
    CONSISTENCY_GATE_FAILURE,
    CONSISTENCY_GATE_SURFACE_SCOPE_ADMISSION,
    CONSISTENCY_GATE_STATUS_FAIL,
    CONSISTENCY_GATE_STATUS_PASS,
    evaluate_next_day_readiness_consistency_gate_v1,
)
from constellation_2.common.paper_session_authority_v1 import write_paper_session_authority_v1
from constellation_2.common.paper_session_ledger_v1 import build_paper_session_ledger_v1, write_paper_session_ledger_v1
from constellation_2.common.session_authority_monitor_v1 import (
    build_session_authority_status_payload_v1,
    write_session_authority_status_v1,
)
from constellation_2.common.session_authority_v1 import (
    derive_active_session_payload_v1,
    derive_target_day_admission_payload_v1,
    derive_target_day_build_payload_v1,
    write_active_session_v1,
    write_target_day_admission_v1,
    write_target_day_build_v1,
)


DAY = "2026-04-10"


def _artifact_row(
    truth_root: Path,
    artifact_id: str,
    *,
    result_status: str = "PASS",
    role_class: str = "REQUIRED_DERIVED_GATE",
    classification: str = "TEST",
    blocking_reason_code: str = "",
    blocker_codes: list[str] | None = None,
    required: bool = True,
    path_family: str = "CANONICAL_RUNTIME_TRUTH_SUBPATH",
    target_day_expected: str = DAY,
    target_day_observed: str = DAY,
    date_binding_status: str = "MATCH",
    freshness_status: str = "CURRENT",
    provenance_present: bool = True,
    closure_status: str | None = None,
    observed_dependency_artifacts: list[str] | None = None,
) -> dict:
    if closure_status is None:
        closure_status = (
            "CLOSED"
            if result_status == "PASS"
            and path_family.startswith("CANONICAL_RUNTIME_TRUTH")
            and date_binding_status == "MATCH"
            and freshness_status == "CURRENT"
            and provenance_present
            else "OPEN"
        )
    return {
        "artifact_id": artifact_id,
        "artifact_name": artifact_id,
        "required": required,
        "role_class": role_class,
        "classification": classification,
        "canonical_path": str((truth_root / f"{artifact_id}.json").resolve()),
        "authority_path": str((truth_root / f"{artifact_id}.json").resolve()),
        "path_family": path_family,
        "observed_status": result_status,
        "result_status": result_status,
        "blocker_codes": blocker_codes or ([blocking_reason_code] if blocking_reason_code else []),
        "blocking_reason_code": blocking_reason_code,
        "schema_status": "VALID",
        "schema_ref": "governance/test.schema.json",
        "freshness_rule": "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        "freshness_status": freshness_status,
        "target_day_expected": target_day_expected,
        "target_day_observed": target_day_observed,
        "date_binding_status": date_binding_status,
        "date_binding_value": target_day_observed,
        "provenance_required": True,
        "provenance_summary": {
            "required": True,
            "present": provenance_present,
            "fields_present": ["producer.module", "generated_utc"] if provenance_present else [],
            "source": "test" if provenance_present else "",
        },
        "closure_status": closure_status,
        "producer": {"module": "test", "git_sha": "abc123"},
        "source_refs": [],
        "observed_dependency_artifacts": list(observed_dependency_artifacts or []),
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _producer() -> dict:
    return {"repo": "constellation", "module": "test", "git_sha": "a" * 40}


def _operator_summary_dossier(
    *,
    day_utc: str,
    current_state: str,
    submission_authorization_status: str,
    submission_authorized: bool,
    first_blocker_code: str = "",
    first_blocker_summary: str = "",
) -> dict:
    return {
        "schema_id": "operator_summary_dossier",
        "schema_version": "v1",
        "generated_utc": "2026-04-11T00:00:00Z",
        "day_utc": day_utc,
        "subsystem_id": "operator_summary_authority",
        "current_state": current_state,
        "owner_ref": {"artifact_path": "", "artifact_sha256": ""},
        "first_blocker": {
            "reason_code": first_blocker_code,
            "summary": first_blocker_summary,
            "source": "submit_boundary_status_v1",
        },
        "upstream_evidence_refs": [],
        "ambiguity_state": {
            "status": "CLEAR",
            "summary": "",
            "reason_codes": [],
            "conflicting_authorities": [],
        },
        "recommended_operator_action": "No operator action required." if not first_blocker_code else "Inspect blocker.",
        "authority_owner": "session_authority_status_v1",
        "active_day": day_utc,
        "admission_status": "ADMIT" if submission_authorized else "BLOCKED",
        "submission_authorization_status": submission_authorization_status,
        "submission_authorized": submission_authorized,
        "advisory_only_signals": [],
        "is_canonical": False,
        "authority_level": "derived",
        "derived_from": [
            "active_session_v1",
            "target_day_admission_v1",
            "target_day_build_v1",
            "submit_boundary_status_v1",
        ],
        "legacy_surface_classification": [],
        "subsystem_dossier_refs": [],
    }


def _write_kill_switch(
    truth_root: Path,
    *,
    day_utc: str,
    canonical_active: bool,
    sleeve_active: bool | None = None,
) -> None:
    def payload(active: bool) -> dict:
        return {
            "schema_id": "global_kill_switch_state",
            "schema_version": "v1",
            "day_utc": day_utc,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "producer": _producer(),
            "state": "ACTIVE" if active else "INACTIVE",
            "allow_entries": False if active else True,
            "allow_exits": True,
            "forced_mode": "FLATTEN_ONLY" if active else "NORMAL",
            "reason_codes": ["C2_KILL_SWITCH_ACTIVE"] if active else [],
            "input_manifest": [],
            "state_sha256": "0" * 64,
        }

    _write_json(
        truth_root / "risk_v1" / "kill_switch_v1" / day_utc / "global_kill_switch_state.v1.json",
        payload(canonical_active),
    )
    if sleeve_active is not None:
        _write_json(
            truth_root.parent
            / "truth_sleeves"
            / "PRIMARY"
            / "PAPER"
            / "risk_v1"
            / "kill_switch_v1"
            / day_utc
            / "global_kill_switch_state.v1.json",
            payload(sleeve_active),
        )


def _write_boundary(truth_root: Path, day_utc: str, *, authorized: bool, produced_at_utc: str = "") -> None:
    first_blocker_code = "" if authorized else "SUBMIT_BOUNDARY_BLOCKED"
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json",
        {
            "schema_id": "submit_boundary_status",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": f"paper_session:{day_utc}:PAPER",
            "submission_authorized": authorized,
            "boundary_status": "AUTHORIZED" if authorized else "DENIED",
            "required_boundary_checks": [],
            "failed_checks": [],
            "blocking_codes": [] if authorized else ["SUBMIT_BOUNDARY_BLOCKED"],
            "closure_state": "COMPLETE" if authorized else "BLOCKED",
            "first_blocker_code": first_blocker_code,
            "missing_dependency_artifacts": [],
            "constitutional_dependency_declaration": {},
            "constitutional_lineage": {},
            "producer": _producer(),
            "produced_at_utc": produced_at_utc or f"{day_utc}T00:02:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "paper_account": "DUO847203",
        },
    )


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
        "artifact_session_id": f"paper_session:{day_utc}:PAPER",
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


def _write_ledger(
    truth_root: Path,
    day_utc: str,
    *,
    authority_status: str,
    evaluated_at_utc: str = "",
) -> None:
    rows = [
        _minimal_fact_row(day_utc, "paper_trading_posture_v1"),
        _minimal_fact_row(day_utc, "startup_materialization_v1"),
        _minimal_fact_row(day_utc, "submit_boundary_status_v1"),
        _minimal_fact_row(day_utc, "sleeve_rollup_v1"),
    ]
    ledger = build_paper_session_ledger_v1(
        day_utc=day_utc,
        session_id=f"paper_session:{day_utc}:PAPER",
        evaluated_at_utc=evaluated_at_utc or f"{day_utc}T00:04:00Z",
        provenance=_producer(),
        fact_refs=rows,
        evidence_freeze={
            "evidence_digest": "b" * 64,
            "overall_evidence_status": "READY" if authority_status == "GRANTED" else "DENY",
            "blocking_codes": [] if authority_status == "GRANTED" else ["BLOCKED"],
            "inputs": [row for row in rows if row["required_for_authority"]],
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
            "submit_evidence_refs": [],
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


def _write_control_plane(
    truth_root: Path,
    day_utc: str,
    *,
    final_start_decision: str,
    ledger_authority_status: str,
    evaluated_at_utc: str = "",
) -> None:
    blocker_code = "" if final_start_decision == "READY_NOW" else "LEDGER_BLOCKED"
    _write_json(
        truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json",
        {
            "schema_id": "paper_day_control_plane",
            "schema_version": "v1",
            "authority_scope": "SUPPORTING_DAY_CONTROL_ARTIFACT",
            "day_utc": day_utc,
            "startup_attempt_id": f"paper_day_start_attempt:{day_utc}:test",
            "control_plane_id": f"paper_day_control_plane:{day_utc}:test",
            "evaluated_at_utc": evaluated_at_utc or f"{day_utc}T00:05:00Z",
            "producer": _producer(),
            "prerequisite_gate": {
                "prerequisite_status": "PASS",
                "prerequisite_blocking_codes": [],
                "prerequisite_artifact_refs": [],
                "first_missing_prerequisite": "",
            },
            "canonical_regeneration_results": [],
            "authority_result": {
                "ledger_path": str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"),
                "ledger_id": f"paper_session_ledger:{day_utc}:test",
                "ledger_authority_status": ledger_authority_status,
                "ledger_evidence_status": "READY" if ledger_authority_status == "GRANTED" else "DENY",
                "first_true_blocker_code": blocker_code,
                "first_true_blocker_artifact_path": ""
                if not blocker_code
                else str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"),
            },
            "startup_proof_result": {
                "startup_proof_validation_path": str(truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json"),
                "startup_proof_validation_status": "STARTUP_READY" if ledger_authority_status == "GRANTED" else "STARTUP_BLOCKED",
                "agreement_with_ledger": True,
            },
            "final_start_decision": final_start_decision,
            "blocking_codes": [] if not blocker_code else [blocker_code],
            "human_readable_summary": "Supporting paper-day control plane for consistency-gate tests.",
            "ignored_legacy_surfaces": [
                {"logical_name": "trading_day_state_v1", "path": "/tmp/trading_day_state.v1.json", "exists": False}
            ],
            "suppression_reason": "NON_AUTHORITATIVE_FOR_STARTUP",
        },
    )


def _write_startup_materialization(truth_root: Path, day_utc: str) -> None:
    _write_json(
        truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json",
        {
            "schema_id": "startup_materialization",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": f"paper_session:{day_utc}:PAPER",
            "status": "SUCCESS",
            "required_inputs_checked": [],
            "materialized_outputs": [],
            "blocking_codes": [],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:00:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "path_resolution_evidence": {"phasec_root": "/tmp/phasec", "latest_active_attempt_path": "/tmp/latest_active_attempt.v1.json"},
            "producer_run_id": f"startup_materialization_v1:{day_utc}",
            "phasec_materializer_result": {"returncode": 0, "stdout": "", "stderr": ""},
        },
    )


def _write_startup_proof(truth_root: Path, day_utc: str, *, ready: bool) -> None:
    _write_json(
        truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json",
        {
            "schema_id": "startup_proof_validation",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_STARTUP_VALIDATION",
            "validation_scope": "BOD_CONTROL_PLANE_ONLY",
            "day_utc": day_utc,
            "session_id": f"paper_session:{day_utc}:PAPER",
            "status": "STARTUP_READY" if ready else "STARTUP_BLOCKED",
            "ledger_ref": str(truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"),
            "ledger_id": f"paper_session_ledger:{day_utc}:test",
            "ledger_authority_status": "GRANTED" if ready else "DENIED",
            "checks": [
                {"logical_name": "repo_authority", "status": "PASS", "evidence_ref": "/tmp/repo_role.v1.json", "blocking_codes": [], "detail": {}},
                {"logical_name": "startup_materialization", "status": "PASS" if ready else "FAIL", "evidence_ref": "/tmp/startup_materialization.v1.json", "blocking_codes": [] if ready else ["BLOCKED"], "detail": {}},
                {"logical_name": "paper_session_ledger", "status": "PASS" if ready else "FAIL", "evidence_ref": "/tmp/paper_session_ledger.v1.json", "blocking_codes": [] if ready else ["BLOCKED"], "detail": {}},
            ],
            "blocking_codes": [] if ready else ["BLOCKED"],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:05:00Z",
            "non_authority_notice": "paper_session_ledger_v1 remains the only authoritative paper-session control owner for the touched startup path.",
        },
    )


def _write_status(
    truth_root: Path,
    *,
    monkeypatch: pytest.MonkeyPatch,
    day_utc: str,
    current_state: str,
    submission_authorization_status: str,
    submission_authorized: bool,
    generated_at: datetime,
    first_blocker_code: str = "",
    first_blocker_summary: str = "",
) -> None:
    denied_reason_code = str(first_blocker_code or "SUBMIT_BOUNDARY_BLOCKED").strip()
    safety_checks = [
        {
            "check_id": "stale_authority_head",
            "status": "PASS",
            "reason_code": "",
            "summary": "Authority head is current.",
            "artifact_path": "",
        },
        {
            "check_id": "broker_handshake",
            "status": "PASS",
            "reason_code": "",
            "summary": "Broker handshake is valid.",
            "artifact_path": "",
        },
        {
            "check_id": "account_environment_match",
            "status": "PASS",
            "reason_code": "",
            "summary": "Account/environment match is valid.",
            "artifact_path": "",
        },
        {
            "check_id": "canonical_kill_switch",
            "status": "PASS",
            "reason_code": "",
            "summary": "Kill switch is present and inactive.",
            "artifact_path": "",
        },
        {
            "check_id": "submission_authorization",
            "status": "PASS" if submission_authorized else "FAIL",
            "reason_code": "" if submission_authorized else denied_reason_code,
            "summary": (
                "Submission authorization is granted."
                if submission_authorized
                else "Submission authorization is denied for this fixture."
            ),
            "artifact_path": str(
                (
                    truth_root
                    / "reports"
                    / "submit_boundary_status_v1"
                    / day_utc
                    / "submit_boundary_status.v1.json"
                ).resolve()
            ),
        },
    ]
    blocking_reason_details = []
    if not submission_authorized:
        blocking_reason_details.append(
            {
                "reason_code": denied_reason_code,
                "blocker_class": "SAFETY_CRITICAL",
                "check_id": "submission_authorization",
                "summary": "Submission authorization is denied for this fixture.",
                "artifact_path": str(
                    (
                        truth_root
                        / "reports"
                        / "submit_boundary_status_v1"
                        / day_utc
                        / "submit_boundary_status.v1.json"
                    ).resolve()
                ),
            }
        )
    write_paper_session_authority_v1(
        truth_root=truth_root,
        payload={
            "schema_id": "paper_session_authority",
            "schema_version": "v1",
            "authority_scope": "CANONICAL_PAPER_SESSION_AUTHORITY",
            "day_utc": day_utc,
            "produced_utc": generated_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "mode": "PAPER",
            "authority_status": "GRANTED" if submission_authorized else "DENIED",
            "paper_open_allowed": submission_authorized,
            "blocking_reason_codes": [] if submission_authorized else [denied_reason_code],
            "blocking_reason_details": blocking_reason_details,
            "safety_checks": safety_checks,
            "advisory_checks": [],
            "degraded_mode": False,
            "submission_authorized": submission_authorized,
            "upstream_refs": {
                "paper_session_bootstrap_v1": str(
                    (truth_root / "reports" / "paper_session_bootstrap_v1" / day_utc / "paper_session_bootstrap.v1.json").resolve()
                ),
                "paper_capital_seed": str((truth_root / "reports" / "paper_capital_seed_v1" / day_utc / "paper_capital_seed.v1.json").resolve()),
                "operator_statement": str((truth_root / "reports" / "operator_statement_v1" / day_utc / "operator_statement.v1.json").resolve()),
                "pre_open_bundle_v1": str((truth_root / "reports" / "pre_open_bundle_v1" / day_utc / "pre_open_bundle.v1.json").resolve()),
                "canonical_kill_switch_v1": str(
                    (
                        truth_root
                        / "risk_v1"
                        / "kill_switch_v1"
                        / day_utc
                        / "global_kill_switch_state.v1.json"
                    ).resolve()
                ),
            },
            "producer": _producer(),
        },
    )

    monkeypatch.setattr(
        "constellation_2.common.session_authority_monitor_v1.build_operator_summary_dossier_v1",
        lambda **kwargs: _operator_summary_dossier(
            day_utc=str(kwargs.get("day_utc") or day_utc),
            current_state=current_state,
            submission_authorization_status=submission_authorization_status,
            submission_authorized=submission_authorized,
            first_blocker_code=first_blocker_code,
            first_blocker_summary=first_blocker_summary,
        ),
    )
    payload = build_session_authority_status_payload_v1(
        truth_root=truth_root,
        environment="PAPER",
        now=generated_at,
    )
    write_session_authority_status_v1(truth_root=truth_root, payload=payload)


def _write_build_admission_active(truth_root: Path, *, day_utc: str) -> tuple[object, object, object]:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=truth_root,
        target_day=day_utc,
        artifact_results=[
            _artifact_row(truth_root, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT", target_day_expected=day_utc, target_day_observed=day_utc),
            _artifact_row(truth_root, "paper_policy_verdict_v1", target_day_expected=day_utc, target_day_observed=day_utc),
            _artifact_row(truth_root, "trade_submit_readiness_c2_v1", target_day_expected=day_utc, target_day_observed=day_utc),
            _artifact_row(truth_root, "submit_boundary_status_v1", role_class="REQUIRED_EXECUTION_BOUNDARY", target_day_expected=day_utc, target_day_observed=day_utc),
            _artifact_row(truth_root, "paper_session_ledger_v1", role_class="REQUIRED_EXECUTION_BOUNDARY", target_day_expected=day_utc, target_day_observed=day_utc),
            _artifact_row(truth_root, "paper_day_control_plane_v1", role_class="REQUIRED_EXECUTION_BOUNDARY", target_day_expected=day_utc, target_day_observed=day_utc),
        ],
        source_refs=[],
    )
    build_ref = write_target_day_build_v1(truth_root=truth_root, payload=build_payload)
    admission_payload = derive_target_day_admission_payload_v1(
        truth_root=truth_root,
        target_day=day_utc,
        build_ref=build_ref,
        enforce_consistency_gate=False,
    )
    admission_ref = write_target_day_admission_v1(truth_root=truth_root, payload=admission_payload)
    active_payload = derive_active_session_payload_v1(
        truth_root=truth_root,
        target_day=day_utc,
        admission_ref=admission_ref,
    )
    active_ref = write_active_session_v1(truth_root=truth_root, payload=active_payload)
    return build_ref, admission_ref, active_ref


def _write_aligned_surface_set(truth_root: Path, *, monkeypatch: pytest.MonkeyPatch, day_utc: str = DAY) -> tuple[object, object, object]:
    build_ref, admission_ref, active_ref = _write_build_admission_active(truth_root, day_utc=day_utc)
    _write_kill_switch(truth_root, day_utc=day_utc, canonical_active=False)
    _write_boundary(truth_root, day_utc, authorized=True)
    _write_ledger(truth_root, day_utc, authority_status="GRANTED")
    _write_startup_materialization(truth_root, day_utc)
    _write_startup_proof(truth_root, day_utc, ready=True)
    _write_control_plane(truth_root, day_utc, final_start_decision="READY_NOW", ledger_authority_status="GRANTED")
    _write_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=day_utc,
        current_state="READY",
        submission_authorization_status="AUTHORIZED",
        submission_authorized=True,
        generated_at=datetime(2026, 4, 11, 12, 30, 0, tzinfo=UTC),
    )
    return build_ref, admission_ref, active_ref


def test_consistency_gate_passes_when_all_surfaces_align(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    _write_aligned_surface_set(truth_root, monkeypatch=monkeypatch)

    result = evaluate_next_day_readiness_consistency_gate_v1(truth_root=truth_root, day_utc=DAY)

    assert result.status == CONSISTENCY_GATE_STATUS_PASS
    assert result.blocking_reason_codes == ()


def test_consistency_gate_allows_ready_now_with_submit_boundary_denied_in_paper_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    _write_build_admission_active(truth_root, day_utc=DAY)
    _write_kill_switch(truth_root, day_utc=DAY, canonical_active=False)
    _write_boundary(truth_root, DAY, authorized=False)
    _write_ledger(truth_root, DAY, authority_status="GRANTED")
    _write_startup_materialization(truth_root, DAY)
    _write_startup_proof(truth_root, DAY, ready=True)
    _write_control_plane(truth_root, DAY, final_start_decision="READY_NOW", ledger_authority_status="GRANTED")
    _write_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=DAY,
        current_state="READY",
        submission_authorization_status="DENIED",
        submission_authorized=False,
        generated_at=datetime(2026, 4, 11, 12, 30, 0, tzinfo=UTC),
    )

    result = evaluate_next_day_readiness_consistency_gate_v1(truth_root=truth_root, day_utc=DAY)

    assert result.status == CONSISTENCY_GATE_STATUS_PASS
    assert CONSISTENCY_GATE_FAILURE not in result.blocking_reason_codes


def test_consistency_gate_still_fails_on_ready_now_when_ledger_authority_is_denied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    _write_build_admission_active(truth_root, day_utc=DAY)
    _write_kill_switch(truth_root, day_utc=DAY, canonical_active=False)
    _write_boundary(truth_root, DAY, authorized=False)
    _write_ledger(truth_root, DAY, authority_status="DENIED")
    _write_startup_materialization(truth_root, DAY)
    _write_startup_proof(truth_root, DAY, ready=False)
    _write_control_plane(truth_root, DAY, final_start_decision="READY_NOW", ledger_authority_status="DENIED")
    _write_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=DAY,
        current_state="READY",
        submission_authorization_status="DENIED",
        submission_authorized=False,
        generated_at=datetime(2026, 4, 11, 12, 31, 0, tzinfo=UTC),
    )

    result = evaluate_next_day_readiness_consistency_gate_v1(truth_root=truth_root, day_utc=DAY)

    assert result.status == CONSISTENCY_GATE_STATUS_FAIL
    assert CONSISTENCY_GATE_FAILURE in result.blocking_reason_codes
    assert any("ledger_authority_status is not GRANTED" in issue.summary for issue in result.issues)


def test_refreshing_active_session_clears_stale_final_state_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    old_build_ref, _, _ = _write_build_admission_active(truth_root, day_utc=DAY)
    _write_kill_switch(truth_root, day_utc=DAY, canonical_active=False)

    newer_build_payload = derive_target_day_build_payload_v1(
        truth_root=truth_root,
        target_day=DAY,
        artifact_results=[
            _artifact_row(truth_root, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT", target_day_expected=DAY, target_day_observed=DAY),
            _artifact_row(truth_root, "paper_policy_verdict_v1", target_day_expected=DAY, target_day_observed=DAY),
            _artifact_row(truth_root, "trade_submit_readiness_c2_v1", target_day_expected=DAY, target_day_observed=DAY),
            _artifact_row(truth_root, "submit_boundary_status_v1", role_class="REQUIRED_EXECUTION_BOUNDARY", required=False, target_day_expected=DAY, target_day_observed=DAY),
            _artifact_row(truth_root, "paper_session_ledger_v1", role_class="REQUIRED_EXECUTION_BOUNDARY", required=False, target_day_expected=DAY, target_day_observed=DAY),
            _artifact_row(truth_root, "paper_day_control_plane_v1", role_class="REQUIRED_EXECUTION_BOUNDARY", required=False, target_day_expected=DAY, target_day_observed=DAY),
        ],
        source_refs=[],
    )
    newer_build_payload["generated_utc"] = "2099-01-01T00:00:00Z"
    newer_build_ref = write_target_day_build_v1(truth_root=truth_root, payload=newer_build_payload)
    newer_admission_ref = write_target_day_admission_v1(
        truth_root=truth_root,
        payload=derive_target_day_admission_payload_v1(
            truth_root=truth_root,
            target_day=DAY,
            build_ref=newer_build_ref,
            enforce_consistency_gate=False,
        ),
    )
    _write_boundary(truth_root, DAY, authorized=True)
    _write_ledger(truth_root, DAY, authority_status="GRANTED")
    _write_startup_materialization(truth_root, DAY)
    _write_startup_proof(truth_root, DAY, ready=True)
    _write_control_plane(truth_root, DAY, final_start_decision="READY_NOW", ledger_authority_status="GRANTED")
    _write_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=DAY,
        current_state="READY",
        submission_authorization_status="AUTHORIZED",
        submission_authorized=True,
        generated_at=datetime(2026, 4, 11, 12, 30, 0, tzinfo=UTC),
    )

    stale_status_payload = build_session_authority_status_payload_v1(truth_root=truth_root, environment="PAPER")
    write_session_authority_status_v1(truth_root=truth_root, payload=stale_status_payload)
    stale_gate = evaluate_next_day_readiness_consistency_gate_v1(truth_root=truth_root, day_utc=DAY)

    assert stale_gate.status == CONSISTENCY_GATE_STATUS_FAIL
    assert any("active_session_v1/current.json build sha256 does not match" in issue.summary for issue in stale_gate.issues)

    refreshed_active_ref = write_active_session_v1(
        truth_root=truth_root,
        payload=derive_active_session_payload_v1(
            truth_root=truth_root,
            target_day=DAY,
            admission_ref=newer_admission_ref,
        ),
    )

    assert refreshed_active_ref.payload["target_day_build_ref"]["artifact_sha256"] == newer_build_ref.sha256
    assert refreshed_active_ref.payload["target_day_admission_ref"] == str(newer_admission_ref.path)

    fresh_status_payload = build_session_authority_status_payload_v1(truth_root=truth_root, environment="PAPER")
    write_session_authority_status_v1(truth_root=truth_root, payload=fresh_status_payload)
    fresh_gate = evaluate_next_day_readiness_consistency_gate_v1(truth_root=truth_root, day_utc=DAY)

    assert fresh_status_payload["first_real_blocker_code"] == ""
    assert fresh_status_payload["submission_authorization_status"] == "AUTHORIZED"
    assert fresh_gate.status == CONSISTENCY_GATE_STATUS_PASS


def test_consistency_gate_fails_when_session_authority_status_is_stale(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    _write_build_admission_active(truth_root, day_utc=DAY)
    _write_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=DAY,
        current_state="BLOCKED",
        submission_authorization_status="BLOCKED",
        submission_authorized=False,
        generated_at=datetime(2026, 4, 11, 3, 53, 22, tzinfo=UTC),
        first_blocker_code="C2_KILL_SWITCH_ACTIVE",
        first_blocker_summary="Submit boundary has not granted submission authorization for the requested day.",
    )
    _write_kill_switch(truth_root, day_utc=DAY, canonical_active=False)
    _write_boundary(truth_root, DAY, authorized=True, produced_at_utc="2026-04-11T22:11:51Z")
    _write_ledger(truth_root, DAY, authority_status="GRANTED", evaluated_at_utc="2026-04-11T22:11:54Z")
    _write_startup_materialization(truth_root, DAY)
    _write_startup_proof(truth_root, DAY, ready=True)
    _write_control_plane(
        truth_root,
        DAY,
        final_start_decision="READY_NOW",
        ledger_authority_status="GRANTED",
        evaluated_at_utc="2026-04-11T22:11:50Z",
    )

    result = evaluate_next_day_readiness_consistency_gate_v1(truth_root=truth_root, day_utc=DAY)

    assert result.status == CONSISTENCY_GATE_STATUS_FAIL
    assert CONSISTENCY_GATE_FAILURE in result.blocking_reason_codes
    assert any(issue.reason_code == "SESSION_AUTHORITY_STATUS_STALE" for issue in result.issues)


def test_consistency_gate_fails_on_kill_switch_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    _write_aligned_surface_set(truth_root, monkeypatch=monkeypatch)
    _write_kill_switch(truth_root, day_utc=DAY, canonical_active=False, sleeve_active=True)

    result = evaluate_next_day_readiness_consistency_gate_v1(truth_root=truth_root, day_utc=DAY)

    assert result.status == CONSISTENCY_GATE_STATUS_FAIL
    assert "KILL_SWITCH_AUTHORITY_MISMATCH" in result.blocking_reason_codes


def test_consistency_gate_passes_when_day_is_blocked_for_non_kill_switch_reasons_and_kill_switch_allows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    build_payload = derive_target_day_build_payload_v1(
        truth_root=truth_root,
        target_day=DAY,
        artifact_results=[
            _artifact_row(truth_root, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT", target_day_expected=DAY, target_day_observed=DAY),
            _artifact_row(
                truth_root,
                "day_authority_decision_v1",
                role_class="REQUIRED_BINDING_INPUT",
                target_day_expected=DAY,
                target_day_observed=DAY,
                result_status="FAIL",
                blocking_reason_code="REQUIRED_GATE_FAIL",
                blocker_codes=["PRIMARY_CAPITAL_AUTHORITY_ALLOCATION_ARTIFACT_MISSING"],
                closure_status="OPEN",
            ),
        ],
        source_refs=[{"script": "ops/tools/run_day_authority_decision_v1.py", "return_code": 2, "required_for_closure": True}],
    )
    build_ref = write_target_day_build_v1(truth_root=truth_root, payload=build_payload)
    admission_payload = derive_target_day_admission_payload_v1(
        truth_root=truth_root,
        target_day=DAY,
        build_ref=build_ref,
        enforce_consistency_gate=False,
    )
    admission_ref = write_target_day_admission_v1(truth_root=truth_root, payload=admission_payload)
    active_payload = derive_active_session_payload_v1(
        truth_root=truth_root,
        target_day=DAY,
        admission_ref=admission_ref,
    )
    write_active_session_v1(truth_root=truth_root, payload=active_payload)
    _write_kill_switch(truth_root, day_utc=DAY, canonical_active=False)
    _write_boundary(truth_root, DAY, authorized=False)
    _write_ledger(truth_root, DAY, authority_status="DENIED")
    _write_startup_materialization(truth_root, DAY)
    _write_startup_proof(truth_root, DAY, ready=False)
    _write_control_plane(truth_root, DAY, final_start_decision="BLOCKED_VALID", ledger_authority_status="DENIED")
    _write_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=DAY,
        current_state="BLOCKED",
        submission_authorization_status="BLOCKED",
        submission_authorized=False,
        generated_at=datetime(2026, 4, 11, 12, 30, 0, tzinfo=UTC),
        first_blocker_code="PARTIAL_BUILD",
        first_blocker_summary="Session Authority has not admitted the target day.",
    )

    result = evaluate_next_day_readiness_consistency_gate_v1(truth_root=truth_root, day_utc=DAY)

    assert result.status == CONSISTENCY_GATE_STATUS_PASS
    assert result.blocking_reason_codes == ()


def test_consistency_gate_fails_on_submit_boundary_vs_ledger_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    _write_aligned_surface_set(truth_root, monkeypatch=monkeypatch)
    _write_boundary(truth_root, DAY, authorized=True)
    _write_ledger(truth_root, DAY, authority_status="DENIED")

    result = evaluate_next_day_readiness_consistency_gate_v1(truth_root=truth_root, day_utc=DAY)

    assert result.status == CONSISTENCY_GATE_STATUS_FAIL
    assert CONSISTENCY_GATE_FAILURE in result.blocking_reason_codes


def test_consistency_gate_fails_on_ledger_vs_control_plane_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    _write_aligned_surface_set(truth_root, monkeypatch=monkeypatch)
    _write_control_plane(truth_root, DAY, final_start_decision="BLOCKED_VALID", ledger_authority_status="GRANTED")

    result = evaluate_next_day_readiness_consistency_gate_v1(truth_root=truth_root, day_utc=DAY)

    assert result.status == CONSISTENCY_GATE_STATUS_FAIL
    assert CONSISTENCY_GATE_FAILURE in result.blocking_reason_codes


def test_consistency_gate_fails_when_required_surface_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    _write_aligned_surface_set(truth_root, monkeypatch=monkeypatch)
    (truth_root / "reports" / "paper_day_control_plane_v1" / DAY / "paper_day_control_plane.v1.json").unlink()

    result = evaluate_next_day_readiness_consistency_gate_v1(truth_root=truth_root, day_utc=DAY)

    assert result.status == CONSISTENCY_GATE_STATUS_FAIL
    assert CONSISTENCY_GATE_FAILURE in result.blocking_reason_codes


def test_consistency_gate_reproduces_original_contradiction_class(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    _write_build_admission_active(truth_root, day_utc=DAY)
    _write_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=DAY,
        current_state="BLOCKED",
        submission_authorization_status="BLOCKED",
        submission_authorized=False,
        generated_at=datetime(2026, 4, 11, 3, 53, 22, tzinfo=UTC),
        first_blocker_code="C2_KILL_SWITCH_ACTIVE",
        first_blocker_summary="Submit boundary has not granted submission authorization for the requested day.",
    )
    _write_kill_switch(truth_root, day_utc=DAY, canonical_active=False)
    _write_boundary(truth_root, DAY, authorized=True, produced_at_utc="2026-04-11T22:11:51Z")
    _write_ledger(truth_root, DAY, authority_status="GRANTED", evaluated_at_utc="2026-04-11T22:11:54Z")
    _write_startup_materialization(truth_root, DAY)
    _write_startup_proof(truth_root, DAY, ready=True)
    _write_control_plane(
        truth_root,
        DAY,
        final_start_decision="READY_NOW",
        ledger_authority_status="GRANTED",
        evaluated_at_utc="2026-04-11T22:11:50Z",
    )

    result = evaluate_next_day_readiness_consistency_gate_v1(truth_root=truth_root, day_utc=DAY)

    assert result.status == CONSISTENCY_GATE_STATUS_FAIL
    assert any(issue.reason_code == "SESSION_AUTHORITY_STATUS_STALE" for issue in result.issues)


def test_admission_consistency_gate_does_not_block_admit_on_downstream_stale_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    build_ref, _, _ = _write_aligned_surface_set(truth_root, monkeypatch=monkeypatch)
    _write_ledger(truth_root, DAY, authority_status="DENIED")

    payload = derive_target_day_admission_payload_v1(
        truth_root=truth_root,
        target_day=DAY,
        build_ref=build_ref,
        enforce_consistency_gate=True,
    )

    assert payload["admission_status"] == "ADMIT"
    assert CONSISTENCY_GATE_FAILURE not in payload["blocking_reason_codes"]

    full_gate = evaluate_next_day_readiness_consistency_gate_v1(truth_root=truth_root, day_utc=DAY)

    assert full_gate.status == CONSISTENCY_GATE_STATUS_FAIL
    assert CONSISTENCY_GATE_FAILURE in full_gate.blocking_reason_codes


def test_admission_scope_ignores_stale_downstream_blockers_after_build_completes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    build_ref, _, _ = _write_aligned_surface_set(truth_root, monkeypatch=monkeypatch)
    _write_boundary(truth_root, DAY, authorized=False)
    _write_ledger(truth_root, DAY, authority_status="DENIED")
    _write_startup_proof(truth_root, DAY, ready=False)
    _write_control_plane(truth_root, DAY, final_start_decision="BLOCKED_VALID", ledger_authority_status="DENIED")
    _write_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=DAY,
        current_state="BLOCKED",
        submission_authorization_status="BLOCKED",
        submission_authorized=False,
        generated_at=datetime(2026, 4, 11, 12, 30, 0, tzinfo=UTC),
        first_blocker_code="CONSISTENCY_GATE_FAILURE",
        first_blocker_summary="Stale downstream block before recomputation.",
    )

    gate_result = evaluate_next_day_readiness_consistency_gate_v1(
        truth_root=truth_root,
        day_utc=DAY,
        surface_scope=CONSISTENCY_GATE_SURFACE_SCOPE_ADMISSION,
        build_payload=build_ref.payload,
        admission_payload={
            "schema_id": "target_day_admission",
            "schema_version": "v1",
            "generated_utc": "2026-04-11T12:31:00Z",
            "target_day": DAY,
            "admission_status": "ADMIT",
            "blocker_chain": [],
            "blocking_reason_codes": [],
            "build_ref": {"artifact_path": str(build_ref.path), "artifact_sha256": build_ref.sha256},
            "closure_status": "CLOSED",
            "hidden_dependency_check_result": {"status": "PASS"},
            "rules_version": "canonical_session_authority_v1",
            "binding": True,
        },
        active_session_payload={
            "schema_id": "active_session",
            "schema_version": "v1",
            "generated_utc": "2026-04-11T12:31:01Z",
            "target_day": DAY,
            "active_day": DAY,
            "prior_day": "2026-04-09",
            "next_target_day": "2026-04-11",
            "active_day_admission_ref": str((truth_root / "target_day_admission_v1" / f"{DAY}.json").resolve()),
            "active_day_build_ref": {"artifact_path": str(build_ref.path), "artifact_sha256": build_ref.sha256},
            "target_day_admission_ref": str((truth_root / "target_day_admission_v1" / f"{DAY}.json").resolve()),
            "target_day_build_ref": {"artifact_path": str(build_ref.path), "artifact_sha256": build_ref.sha256},
            "target_day_admission_status": "ADMIT",
            "rollover_status": "ROLLOVER_COMPLETED",
            "rollover_reason": "",
            "rollover_reason_code": "",
            "rollover_reason_summary": "",
            "blocked_target_day": "",
            "blocked_admission_ref": "",
            "blocked_reason_codes": [],
            "authority_owner": "session_authority_v1",
        },
    )

    assert gate_result.status == CONSISTENCY_GATE_STATUS_PASS

    payload = derive_target_day_admission_payload_v1(
        truth_root=truth_root,
        target_day=DAY,
        build_ref=build_ref,
        enforce_consistency_gate=True,
    )

    assert payload["admission_status"] == "ADMIT"
    assert CONSISTENCY_GATE_FAILURE not in payload["blocking_reason_codes"]


def test_paper_day_control_plane_consistency_gate_blocks_ready_now_on_original_contradiction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    truth_root = tmp_path / "truth"
    _write_build_admission_active(truth_root, day_utc=DAY)
    _write_status(
        truth_root,
        monkeypatch=monkeypatch,
        day_utc=DAY,
        current_state="BLOCKED",
        submission_authorization_status="BLOCKED",
        submission_authorized=False,
        generated_at=datetime(2026, 4, 11, 3, 53, 22, tzinfo=UTC),
        first_blocker_code="C2_KILL_SWITCH_ACTIVE",
        first_blocker_summary="Submit boundary has not granted submission authorization for the requested day.",
    )
    _write_kill_switch(truth_root, day_utc=DAY, canonical_active=False)

    def fake_run(cmd: list[str], *, truth_root: Path) -> dict:
        tool_name = Path(cmd[1]).name
        if tool_name == "run_intents_day_completeness_v1.py":
            _write_json(
                truth_root / "reports" / "intents_day_completeness_v1" / DAY / "intents_day_completeness.v1.json",
                {
                    "schema_id": "intents_day_completeness",
                    "schema_version": "v1",
                    "authority_scope": "NON_AUTHORITY_PREREQUISITE_FACT",
                    "day_utc": DAY,
                    "session_id": f"paper_session:{DAY}:PAPER",
                    "completeness_status": "COMPLETE",
                    "required_inputs_checked": [
                        {
                            "logical_name": "intents_day_directory",
                            "absolute_path": "/tmp/intents_day_directory",
                            "sha256": "",
                            "day_utc": DAY,
                            "status": "PASS",
                            "reason_codes": [],
                        }
                    ],
                    "missing_inputs": [],
                    "freshness_verdict": "CURRENT",
                    "linkage_verdict": "LINKED",
                    "blocking_codes": [],
                    "producer": _producer(),
                    "produced_at_utc": f"{DAY}T00:00:00Z",
                },
            )
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_startup_materialization_v1.py":
            _write_startup_materialization(truth_root, DAY)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_trading_posture_v1.py":
            _write_json(
                truth_root / "reports" / "paper_trading_posture_v1" / DAY / "paper_trading_posture.v1.json",
                {
                    "schema_id": "paper_trading_posture",
                    "schema_version": "v1",
                    "authority_scope": "NON_AUTHORITY_FACT",
                    "day_utc": DAY,
                    "session_id": f"paper_session:{DAY}:PAPER",
                    "system_ready": True,
                    "posture_status": "ENABLED",
                    "posture_class": "PAPER_READY_ACTIVE",
                    "blocking_family": "NONE",
                    "expected_no_op_today": False,
                    "policy_reasons": ["MARKET_CALENDAR_TRADING_SESSION"],
                    "blocking_codes": [],
                    "blocking_reason_codes": [],
                    "source_dependencies": [],
                    "producer": _producer(),
                    "produced_at_utc": f"{DAY}T00:01:00Z",
                    "freshness_verdict": "CURRENT",
                    "linkage_verdict": "LINKED",
                },
            )
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_submit_boundary_status_v1.py":
            _write_boundary(truth_root, DAY, authorized=True, produced_at_utc="2026-04-11T22:11:51Z")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_paper_session_ledger_v1.py":
            _write_ledger(truth_root, DAY, authority_status="GRANTED", evaluated_at_utc="2026-04-11T22:11:54Z")
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        if tool_name == "run_startup_proof_validation_v1.py":
            _write_startup_proof(truth_root, DAY, ready=True)
            return {"return_code": 0, "stdout": "{}", "stderr": ""}
        raise AssertionError(tool_name)

    with patch.object(control_plane_module, "_run", side_effect=fake_run):
        rc = control_plane_module.main(["--day_utc", DAY, "--truth_root", str(truth_root)])

    payload = json.loads(
        (truth_root / "reports" / "paper_day_control_plane_v1" / DAY / "paper_day_control_plane.v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert rc == 3
    assert payload["final_start_decision"] == "BLOCKED_BY_DEFECT"
    assert CONSISTENCY_GATE_FAILURE in payload["blocking_codes"]
