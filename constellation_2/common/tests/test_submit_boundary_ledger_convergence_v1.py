from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path(__file__).resolve().parents[3]
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_paper_session_ledger_v1 as ledger_module
import ops.tools.run_submit_boundary_status_v1 as boundary_module
from constellation_2.common.aegis_day_closure_authority_v1 import closure_authority_output_path
from constellation_2.common.execution_evidence_current_head_v1 import current_head_output_path
from constellation_2.common.submission_index_v1 import submission_index_output_path


DAY = "2026-04-13"
ACCOUNT = "DUO847203"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_startup_materialization(truth_root: Path) -> None:
    _write_json(
        truth_root / "reports" / "startup_materialization_v1" / DAY / "startup_materialization.v1.json",
        {
            "schema_id": "startup_materialization",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": DAY,
            "session_id": f"paper_session:{DAY}:PAPER",
            "status": "SUCCESS",
            "required_inputs_checked": [],
            "materialized_outputs": [],
            "blocking_codes": [],
            "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
            "produced_at_utc": f"{DAY}T00:00:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "path_resolution_evidence": {"phasec_root": "/tmp/phasec", "latest_active_attempt_path": "/tmp/latest.json"},
            "producer_run_id": "startup:test",
            "phasec_materializer_result": {"returncode": 0, "stdout": "", "stderr": ""},
        },
    )


def _write_paper_trading_posture(truth_root: Path) -> None:
    _write_json(
        truth_root / "reports" / "paper_trading_posture_v1" / DAY / "paper_trading_posture.v1.json",
        {
            "schema_id": "paper_trading_posture",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": DAY,
            "session_id": f"paper_session:{DAY}:PAPER",
            "posture_status": "ENABLED",
            "system_ready": True,
            "blocking_codes": [],
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
            "produced_at_utc": f"{DAY}T00:00:00Z",
        },
    )


def _write_trade_submit_status(
    truth_root: Path,
    *,
    ok: bool = True,
    state: str = "OK",
    reasons: list[str] | None = None,
    submit_allowed: bool | None = None,
) -> None:
    payload = {
        "schema_id": "trade_submit_readiness_c2",
        "schema_version": "v1",
        "day_utc": DAY,
        "as_of_utc": f"{DAY}T00:00:00Z",
        "expires_utc": f"{DAY}T00:02:00Z",
        "ok": bool(ok),
        "state": str(state),
        "environment": "PAPER",
        "ib_account": ACCOUNT,
        "reasons": list(reasons or []),
        "input_manifest": [],
        "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
        "provenance": {
            "truth_root": str(truth_root.resolve()),
            "registry_sha256": "a" * 64,
            "sleeve_registry_sha256": "b" * 64,
        },
        "session_authority_attestation": {
            "decision_artifact_path": "/tmp/day_authority.json",
            "decision_artifact_sha256": "c" * 64,
            "policy_version": "validation_result_only",
            "evaluator_version": "validation_result_only",
            "venue": "C2",
            "session_date": DAY,
            "decision_status": "OK",
            "session_class": None,
            "stage_id": "PRE_ORCHESTRATION_PREFLIGHT",
            "policy_action": "SKIP",
            "stage_execution_status": "OK",
            "reason_codes": [],
        },
        "run_state_authority_attestation": {
            "authority_family": "day_authority_decision_v1",
            "authority_artifact_path": "/tmp/day_authority.json",
            "authority_artifact_sha256": "d" * 64,
            "policy_version": "validation_result_only",
            "evaluator_version": "validation_result_only",
            "decision_status": "OK",
            "classification_field": "decision_state",
            "classification_value": "OPEN",
            "cycle_snapshot_family": "gate_stack_verdict_v1",
            "cycle_snapshot_artifact_path": "/tmp/gate_stack.json",
            "cycle_snapshot_artifact_sha256": "e" * 64,
            "cycle_id": f"{DAY}:OK",
            "cycle_coherence_status": "COHERENT",
            "stage_id": "TRADE_SUBMIT_READINESS",
            "stage_execution_status": "OK",
            "reason_codes": [],
            "upstream_authority_refs": [],
        },
    }
    if isinstance(submit_allowed, bool):
        payload["submit_allowed"] = submit_allowed
    _write_json(
        truth_root / "trade_submit_readiness_c2_v1" / "_history" / "PAPER" / ACCOUNT / DAY / "status.json",
        payload,
    )


def _write_kill_switch(truth_root: Path) -> None:
    _write_json(
        truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        {
            "schema_id": "global_kill_switch_state",
            "schema_version": "v1",
            "day_utc": DAY,
            "produced_utc": f"{DAY}T00:00:00Z",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
            "state": "INACTIVE",
            "allow_entries": True,
            "allow_exits": True,
            "forced_mode": "NORMAL",
            "reason_codes": [],
            "input_manifest": [],
            "state_sha256": "0" * 64,
        },
    )


def _write_paper_session_ledger_surface(truth_root: Path, payload: dict) -> None:
    _write_json(
        truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json",
        payload,
    )


def _paper_session_authority_payload(*, authority_status: str = "GRANTED") -> dict:
    allowed = authority_status == "GRANTED"
    blocker_codes = [] if allowed else ["PAPER_SESSION_AUTHORITY_DENIED"]
    return {
        "schema_id": "paper_session_authority",
        "schema_version": "v1",
        "authority_scope": "CANONICAL_PAPER_SESSION_AUTHORITY",
        "day_utc": DAY,
        "produced_utc": f"{DAY}T00:00:00Z",
        "mode": "PAPER",
        "authority_status": authority_status,
        "paper_open_allowed": allowed,
        "blocking_reason_codes": blocker_codes,
        "blocking_reason_details": (
            []
            if allowed
            else [
                {
                    "reason_code": blocker_codes[0],
                    "blocker_class": "SAFETY_CRITICAL",
                    "check_id": "PRE_OPEN_BUNDLE_COMPLETE",
                    "summary": "BLOCKED",
                    "artifact_path": "",
                }
            ]
        ),
        "safety_checks": [
            {"check_id": "PAPER_CAPITAL_SEED_READY", "status": "PASS", "reason_code": "", "summary": "OK", "artifact_path": ""},
            {"check_id": "OPERATOR_STATEMENT_READY", "status": "PASS", "reason_code": "", "summary": "OK", "artifact_path": ""},
            {
                "check_id": "PRE_OPEN_BUNDLE_COMPLETE",
                "status": "PASS" if allowed else "FAIL",
                "reason_code": "" if allowed else blocker_codes[0],
                "summary": "COMPLETE" if allowed else "BLOCKED",
                "artifact_path": "",
            },
            {"check_id": "CANONICAL_KILL_SWITCH_PRESENT", "status": "PASS", "reason_code": "", "summary": "PRESENT", "artifact_path": ""},
            {"check_id": "CANONICAL_KILL_SWITCH_INACTIVE", "status": "PASS", "reason_code": "", "summary": "INACTIVE", "artifact_path": ""},
        ],
        "advisory_checks": [],
        "degraded_mode": False,
        "submission_authorized": False,
        "upstream_refs": {
            "paper_session_bootstrap_v1": "/tmp/paper_session_bootstrap.v1.json",
            "paper_capital_seed": "",
            "operator_statement": "",
            "pre_open_bundle_v1": "",
            "canonical_kill_switch_v1": "",
        },
        "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
    }


def _write_closure_lineage_pass(*, truth_root: Path) -> None:
    _write_json(
        closure_authority_output_path(truth_root=truth_root, day_utc=DAY),
        {
            "schema_version": "aegis_day_closure_authority.v1",
            "day": DAY,
            "status": "PASS",
            "canonical_blocker": "",
            "blocking_evidence": [],
            "generated_at_utc": f"{DAY}T00:00:00Z",
        },
    )
    _write_json(
        submission_index_output_path(execution_root=truth_root.resolve(), day_utc=DAY),
        {
            "schema_version": "submission_index.v1",
            "day": DAY,
            "status": "PASS",
            "attempts": [],
            "blocking_evidence": [],
            "generated_at_utc": f"{DAY}T00:00:00Z",
        },
    )
    _write_json(
        current_head_output_path(execution_root=truth_root.resolve(), day_utc=DAY),
        {
            "schema_version": "execution_evidence_current_head.v1",
            "day": DAY,
            "status": "PASS",
            "selected_attempt_id": "attempt-1",
            "selected_artifact_path": "/tmp/execution_stream_snapshot.v1.json",
            "rejected_candidates": [],
            "generated_at_utc": f"{DAY}T00:00:00Z",
        },
    )


def _write_trading_day_calendar_row(*, truth_root: Path) -> None:
    calendar_path = truth_root / "market_calendar_v1" / "NYSE" / f"{DAY[:4]}.jsonl"
    calendar_path.parent.mkdir(parents=True, exist_ok=True)
    calendar_path.write_text(
        json.dumps({"day_utc": DAY, "is_trading_session": True}, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _run_submit_boundary(
    truth_root: Path,
    *,
    build_payload: dict,
    admission_payload: dict,
    startup_status: str = "SUCCESS",
    startup_blocking_codes: list[str] | None = None,
    readiness_payload: dict | None = None,
    presubmit_payload: dict | None = None,
    decision_payload: dict | None = None,
) -> dict:
    with tempfile.TemporaryDirectory() as td:
        repo_root = Path(td)
        _write_json(
            repo_root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json",
            {
                "schema_id": "c2_sleeve_registry",
                "schema_version": "v1",
                "sleeves": [{"sleeve_id": "PRIMARY", "enabled": True, "mode": "PAPER", "ib_account": ACCOUNT}],
            },
        )
        startup_path = truth_root / "reports" / "startup_materialization_v1" / DAY / "startup_materialization.v1.json"
        posture_path = truth_root / "reports" / "paper_trading_posture_v1" / DAY / "paper_trading_posture.v1.json"
        readiness_path = truth_root / "trade_submit_readiness_c2_v1" / "_history" / "PAPER" / ACCOUNT / DAY / "status.json"
        presubmit_path = truth_root / "reports" / "trade_readiness_presubmit_v1" / DAY / "trade_readiness_presubmit.v1.json"
        decision_path = truth_root / "reports" / "trade_readiness_decision_v1" / DAY / "trade_readiness_decision.v1.json"
        build_path = (truth_root / "target_day_build_v1" / f"{DAY}.json").resolve()
        admission_path = (truth_root / "target_day_admission_v1" / f"{DAY}.json").resolve()
        _write_json(build_path, dict(build_payload))
        _write_json(admission_path, dict(admission_payload))
        if isinstance(presubmit_payload, dict):
            _write_json(presubmit_path, dict(presubmit_payload))
        if isinstance(decision_payload, dict):
            _write_json(decision_path, dict(decision_payload))
        _write_closure_lineage_pass(truth_root=truth_root)
        _write_trading_day_calendar_row(truth_root=truth_root)
        startup_codes = list(startup_blocking_codes or [])
        effective_readiness_payload = dict(readiness_payload or {"ok": True, "state": "OK", "reasons": []})
        with patch.object(boundary_module, "REPO_ROOT", repo_root), patch.object(
            boundary_module, "resolve_decision_truth_root_bridge_v1", return_value=truth_root.resolve()
        ), patch.object(
            boundary_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value=ACCOUNT
        ), patch.object(
            boundary_module, "_refresh_trade_submit_readiness_artifact_v1", return_value=0
        ), patch.object(
            boundary_module,
            "effective_enforcement_mode_v1",
            return_value="OFF",
        ), patch.object(
            boundary_module,
            "run_runtime_control_kernel_v1",
            return_value={
                "runtime_control_decision": type("Decision", (), {"reason_codes": ()})(),
                "runtime_control_record": type("Record", (), {"control_state": "ALLOW"})(),
                "runtime_control_record_path": str(
                    (truth_root / "runtime_control_kernel_v1" / "records" / DAY / "PAPER" / ACCOUNT / "record.v1.json").resolve()
                ),
                "runtime_control_decision_path": str(
                    (truth_root / "runtime_control_kernel_v1" / "decisions" / DAY / "PAPER" / ACCOUNT / "decision.v1.json").resolve()
                ),
            },
        ), patch.object(
            boundary_module,
            "resolve_sleeve_execution_root_v1",
            return_value=type("ExecutionRoot", (), {"execution_root_path": truth_root.resolve()})(),
        ), patch.object(
            boundary_module,
            "resolve_kill_switch_authority_v1",
            return_value=type(
                "KillSwitch",
                (),
                {
                    "status": boundary_module.KILL_SWITCH_STATUS_PASS,
                    "state": "INACTIVE",
                    "allow_entries": True,
                    "reason_codes": [],
                    "canonical_path": truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
                },
            )(),
        ), patch.object(
            boundary_module,
            "read_startup_materialization_ref_v1",
            return_value=type(
                "Ref",
                (),
                {
                    "path": startup_path,
                    "payload": {"status": startup_status, "blocking_codes": startup_codes},
                    "sha256": "1" * 64,
                },
            )(),
        ), patch.object(
            boundary_module,
            "read_paper_trading_posture_ref_v1",
            return_value=type(
                "Ref",
                (),
                {
                    "path": posture_path,
                    "payload": {"system_ready": True, "posture_status": "ENABLED", "blocking_codes": []},
                    "sha256": "2" * 64,
                },
            )(),
        ), patch.object(
            boundary_module,
            "read_trade_submit_readiness_for_day_v1",
            return_value=type(
                "Ref",
                (),
                {
                    "path": readiness_path,
                    "payload": effective_readiness_payload,
                    "sha256": "3" * 64,
                },
            )(),
        ), patch.object(
            boundary_module,
            "validate_governed_artifact_payload_v1",
            return_value=None,
        ), patch.object(
            boundary_module,
            "read_target_day_build_ref_v1",
            return_value=type("BuildRef", (), {"path": build_path, "payload": build_payload, "sha256": "4" * 64})(),
        ), patch.object(
            boundary_module,
            "read_target_day_admission_ref_v1",
            return_value=type("AdmissionRef", (), {"path": admission_path, "payload": admission_payload, "sha256": "5" * 64})(),
        ):
            rc = boundary_module.main(["--day_utc", DAY, "--truth_root", str(truth_root)])
            assert rc in {0, 2}
    return json.loads(
        (
            truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json"
        ).read_text(encoding="utf-8")
    )


def _run_ledger(truth_root: Path, *, boundary_payload: dict) -> tuple[int, dict]:
    pre_open_path = truth_root / "reports" / "pre_open_bundle_v1" / DAY / "pre_open_bundle.v1.json"
    startup_path = truth_root / "reports" / "startup_materialization_v1" / DAY / "startup_materialization.v1.json"
    posture_path = truth_root / "reports" / "paper_trading_posture_v1" / DAY / "paper_trading_posture.v1.json"
    boundary_path = truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json"
    authority_path = truth_root / "reports" / "paper_session_authority_v1" / DAY / "paper_session_authority.v1.json"
    with patch.object(ledger_module, "resolve_decision_truth_root_v1", return_value=truth_root.resolve()), patch.object(
        ledger_module,
        "read_pre_open_bundle_ref_v1",
        return_value=type(
            "Ref",
            (),
            {
                "path": pre_open_path,
                "payload": {
                    "schema_id": "pre_open_bundle",
                    "schema_version": "v1",
                    "target_day": DAY,
                    "day_utc": DAY,
                    "session_id": f"paper_session:{DAY}:PAPER",
                    "built_at_utc": f"{DAY}T00:00:00Z",
                    "materialization_state": "COMPLETE",
                    "completion_state": "COMPLETE",
                    "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
                    "blocking_reason_codes": [],
                    "blocking_codes": [],
                },
                "sha256": "0" * 64,
            },
        )(),
    ), patch.object(
        ledger_module,
        "read_startup_materialization_ref_v1",
        return_value=type("Ref", (), {"path": startup_path, "payload": {"status": "SUCCESS", "day_utc": DAY, "session_id": f"paper_session:{DAY}:PAPER", "produced_at_utc": f"{DAY}T00:00:00Z", "schema_id": "startup_materialization", "schema_version": "v1", "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"}, "blocking_codes": []}, "sha256": "1" * 64})(),
    ), patch.object(
        ledger_module,
        "read_paper_trading_posture_ref_v1",
        return_value=type("Ref", (), {"path": posture_path, "payload": {"posture_status": "ENABLED", "system_ready": True, "day_utc": DAY, "session_id": f"paper_session:{DAY}:PAPER", "produced_at_utc": f"{DAY}T00:00:00Z", "schema_id": "paper_trading_posture", "schema_version": "v1", "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"}, "blocking_codes": []}, "sha256": "2" * 64})(),
    ), patch.object(
        ledger_module,
        "read_submit_boundary_status_ref_v1",
        return_value=type("Ref", (), {"path": boundary_path, "payload": boundary_payload, "sha256": "a" * 64})(),
    ), patch.object(
        ledger_module,
        "read_paper_session_authority_ref_v1",
        return_value=type("Ref", (), {"path": authority_path, "payload": _paper_session_authority_payload(), "sha256": "9" * 64})(),
    ):
        rc = ledger_module.main(["--day_utc", DAY, "--truth_root", str(truth_root)])
    payload = json.loads(
        (
            truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json"
        ).read_text(encoding="utf-8")
    )
    return rc, payload


def test_submit_boundary_surfaces_headroom_rejection_reason() -> None:
    with tempfile.TemporaryDirectory() as td:
        execution_truth_root = Path(td) / "truth_sleeves" / "PRIMARY" / "PAPER"
        intent_hash = "4d246a5a31d6c40d74d5af509c549a4daa950b540f293cdc9de8390051e6b184"
        _write_json(
            execution_truth_root / "intents_v1" / "snapshots" / DAY / f"{intent_hash}.exposure_intent.v1.json",
            {
                "schema_id": "exposure_intent",
                "schema_version": "v1",
                "intent_id": "c2_trend_eq_spy_2026-04-23_v1",
                "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1"},
            },
        )
        _write_json(
            execution_truth_root / "engine_activity_v1" / "authorization_v1" / DAY / f"{intent_hash}.authorization.v1.json",
            {
                "schema_id": "C2_AUTHORIZATION_V1",
                "schema_version": 1,
                "status": "REJECTED",
                "reason_codes": ["BUNDLE_B_HEADROOM_REJECTED"],
                "authorization": {
                    "decision": "REJECTED",
                    "authorized_quantity": 0,
                    "constraints": [
                        "HEADROOM_REQUIRED_RISK_CENTS=100000",
                        "HEADROOM_AVAILABLE_CENTS=0",
                    ],
                    "decision_hash": "0" * 64,
                },
            },
        )
        summary = boundary_module._summarize_execution_intent_authorization_v1(  # noqa: SLF001
            execution_truth_root=execution_truth_root,
            day_utc=DAY,
        )
    assert summary["status"] == "FAIL"
    assert "BUNDLE_B_HEADROOM_REJECTED" in summary["reason_codes"]
    assert "SUBMIT_BOUNDARY_HEADROOM_REJECTED" in summary["reason_codes"]
    assert "SUBMIT_BOUNDARY_INTENT_AUTHORIZATION_DENIED" in summary["reason_codes"]
    assert "SUBMIT_BOUNDARY_INTENT_AUTHORIZATION_ZERO_QUANTITY" in summary["reason_codes"]


def test_upstream_blocked_day_no_longer_blocks_submit_boundary_when_submit_safety_is_pass() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root)
        boundary = _run_submit_boundary(
            truth_root,
            build_payload={
                "build_status": "BLOCKED",
                "completeness_result": "INCOMPLETE",
                "closure_status": "OPEN",
                "hidden_dependency_check_result": {"status": "FAIL"},
                "blocker_chain": [{"blocker_code": "PARTIAL_BUILD"}],
            },
            admission_payload={
                "admission_status": "BLOCKED",
                "binding": True,
                "blocking_reason_codes": ["PARTIAL_BUILD"],
            },
        )
        assert boundary["boundary_status"] == "AUTHORIZED"
        assert boundary["submission_authorized"] is True

        rc, ledger_payload = _run_ledger(truth_root, boundary_payload=boundary)
        assert rc == 0
        assert ledger_payload["control_state"]["authority_status"] == "GRANTED"
        assert ledger_payload["control_state"]["submission_authorized"] is True
        assert ledger_payload["control_state"]["blocking_codes"] == []
        assert "PAPER_SESSION_LEDGER_ADVISORY_SUBMIT_BOUNDARY_DENIED" not in ledger_payload["operator_summary"]["non_authority_notice"]


def test_complete_day_authorizes_boundary_and_grants_ledger() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root)
        boundary = _run_submit_boundary(
            truth_root,
            build_payload={
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
            admission_payload={
                "admission_status": "ADMIT",
                "binding": True,
                "blocking_reason_codes": [],
            },
        )
        assert boundary["boundary_status"] == "AUTHORIZED"

        rc, ledger_payload = _run_ledger(truth_root, boundary_payload=boundary)
        assert rc == 0
        assert ledger_payload["control_state"]["authority_status"] == "GRANTED"
        assert ledger_payload["control_state"]["submission_authorized"] is bool(boundary.get("submission_authorized") is True)
        assert "PAPER_SESSION_LEDGER_EVIDENCE_DENIED" not in ledger_payload["control_state"]["blocking_codes"]


def test_aligned_valid_day_authorizes_boundary_and_grants_ledger() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root)
        boundary = _run_submit_boundary(
            truth_root,
            build_payload={
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
            admission_payload={
                "admission_status": "ADMIT",
                "binding": True,
                "blocking_reason_codes": [],
            },
        )
        assert boundary["boundary_status"] == "AUTHORIZED"

        rc, ledger_payload = _run_ledger(truth_root, boundary_payload=boundary)
        assert rc == 0
        assert ledger_payload["control_state"]["authority_status"] == "GRANTED"
        assert ledger_payload["control_state"]["submission_authorized"] is bool(boundary.get("submission_authorized") is True)


def test_startup_materialization_failure_still_blocks_submit_boundary() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root)
        boundary = _run_submit_boundary(
            truth_root,
            build_payload={
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
            admission_payload={
                "admission_status": "ADMIT",
                "binding": True,
                "blocking_reason_codes": [],
            },
            startup_status="FAIL",
            startup_blocking_codes=["STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED"],
        )
        assert boundary["boundary_status"] == "BLOCKED"
        assert boundary["submission_authorized"] is False
        assert "STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED" in boundary["blocking_codes"]


def test_submit_boundary_respects_trade_readiness_presubmit_yes_and_submit_allowed_true() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(
            truth_root,
            ok=True,
            state="OK",
            reasons=["INFO:PRODUCTION_POLICY_NOT_PASS"],
        )
        _write_kill_switch(truth_root)
        boundary = _run_submit_boundary(
            truth_root,
            build_payload={
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
            admission_payload={
                "admission_status": "ADMIT",
                "binding": True,
                "blocking_reason_codes": [],
            },
            readiness_payload={
                "ok": True,
                "state": "OK",
                "reasons": ["INFO:PRODUCTION_POLICY_NOT_PASS"],
            },
            presubmit_payload={
                "schema_version": "trade_readiness_presubmit.v1",
                "day_utc": DAY,
                "decision": "YES",
                "submit_allowed": True,
                "all_blockers": [],
            },
        )
        assert boundary["boundary_status"] == "AUTHORIZED"
        assert boundary["submit_allowed"] is True
        assert boundary["readiness_submit_allowed"] is True
        assert boundary["readiness_decision"] == "YES"
        assert "SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS" not in boundary["blocking_codes"]


def test_submit_boundary_blocks_when_trade_readiness_policy_decision_is_no() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root, ok=True, state="OK", reasons=[])
        _write_kill_switch(truth_root)
        boundary = _run_submit_boundary(
            truth_root,
            build_payload={
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
            admission_payload={
                "admission_status": "ADMIT",
                "binding": True,
                "blocking_reason_codes": [],
            },
            readiness_payload={"ok": True, "state": "OK", "reasons": []},
            presubmit_payload={
                "schema_version": "trade_readiness_presubmit.v1",
                "day_utc": DAY,
                "decision": "NO",
                "submit_allowed": False,
                "all_blockers": ["RISK_POLICY_BLOCKED"],
            },
        )
        assert boundary["boundary_status"] == "BLOCKED"
        assert boundary["canonical_blocker"] == "SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS"
        assert boundary["failed_conditions"]
        assert boundary["blocking_evidence"]
        assert any(
            item.get("condition") in {"READINESS_POLICY_DECISION_NO", "READINESS_POLICY_SUBMIT_ALLOWED_FALSE"}
            for item in boundary["failed_conditions"]
        )


def test_submit_boundary_generic_readiness_blocker_has_source_path_and_failed_condition() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root, ok=False, state="FAIL", reasons=["FAIL:HEADROOM_INSUFFICIENT"])
        _write_kill_switch(truth_root)
        boundary = _run_submit_boundary(
            truth_root,
            build_payload={
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
            admission_payload={
                "admission_status": "ADMIT",
                "binding": True,
                "blocking_reason_codes": [],
            },
            readiness_payload={
                "ok": False,
                "state": "FAIL",
                "reasons": ["FAIL:HEADROOM_INSUFFICIENT"],
            },
        )
        assert boundary["boundary_status"] == "BLOCKED"
        assert boundary["source_surface_path"]
        assert boundary["failed_conditions"]
        assert boundary["blocking_evidence"]
        assert any(
            evidence.get("logical_name") == "trade_submit_readiness_c2_v1"
            for evidence in boundary["blocking_evidence"]
        )


def test_submit_boundary_does_not_treat_not_attempted_presubmit_gates_as_failure() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root, ok=True, state="OK", reasons=[])
        _write_kill_switch(truth_root)
        boundary = _run_submit_boundary(
            truth_root,
            build_payload={
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
            admission_payload={
                "admission_status": "ADMIT",
                "binding": True,
                "blocking_reason_codes": [],
            },
            readiness_payload={"ok": True, "state": "OK", "reasons": []},
            presubmit_payload={
                "schema_version": "trade_readiness_presubmit.v1",
                "day_utc": DAY,
                "decision": "YES",
                "submit_allowed": True,
                "ordered_gate_results": [
                    {"gate": "Broker Submission Result", "status": "NOT_ATTEMPTED"},
                    {"gate": "Lifecycle & Outcome Tracking", "status": "NOT_ATTEMPTED"},
                ],
                "all_blockers": [],
            },
        )
        assert boundary["boundary_status"] == "AUTHORIZED"
        assert boundary["submit_allowed"] is True
        assert "SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS" not in boundary["blocking_codes"]


def test_submit_boundary_ignores_stale_presubmit_policy_surface_and_uses_current_decision_surface() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root, ok=True, state="OK", reasons=[])
        _write_kill_switch(truth_root)
        boundary = _run_submit_boundary(
            truth_root,
            build_payload={
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
            admission_payload={
                "admission_status": "ADMIT",
                "binding": True,
                "blocking_reason_codes": [],
            },
            readiness_payload={"ok": True, "state": "OK", "reasons": []},
            presubmit_payload={
                "schema_version": "trade_readiness_presubmit.v1",
                "day_utc": "2026-04-12",
                "decision": "NO",
                "submit_allowed": False,
                "all_blockers": ["RISK_POLICY_BLOCKED"],
            },
            decision_payload={
                "schema_version": "trade_readiness_decision.v1",
                "day_utc": DAY,
                "decision": "YES",
                "submit_allowed": True,
                "status": "READY",
                "all_blockers": [],
            },
        )
        assert boundary["boundary_status"] == "AUTHORIZED"
        assert boundary["readiness_decision"] == "YES"
        assert boundary["source_paths"]["trade_readiness_decision_v1"].endswith(
            f"/trade_readiness_decision_v1/{DAY}/trade_readiness_decision.v1.json"
        )


def test_submit_boundary_accepts_nested_bound_lineage_without_legacy_top_level_field() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root, ok=True, state="OK", reasons=[])
        _write_kill_switch(truth_root)
        _write_paper_session_ledger_surface(
            truth_root,
            {
                "submit_lifecycle": {
                    "submit_result_status": "FAIL",
                    "submit_attempt_status": "ATTEMPTED",
                    "finalization_status": "OPEN",
                },
                "post_submit_lifecycle": {"lineage_status": "BOUND"},
            },
        )
        boundary = _run_submit_boundary(
            truth_root,
            build_payload={
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
            admission_payload={
                "admission_status": "ADMIT",
                "binding": True,
                "blocking_reason_codes": [],
            },
        )
        assert boundary["boundary_status"] == "AUTHORIZED"
        assert "POST_SUBMIT_LINEAGE_GAP" not in (boundary.get("blocking_codes") or [])


def test_submit_boundary_supports_legacy_top_level_bound_lineage_status() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root, ok=True, state="OK", reasons=[])
        _write_kill_switch(truth_root)
        _write_paper_session_ledger_surface(
            truth_root,
            {
                "submit_lifecycle": {
                    "submit_result_status": "FAIL",
                    "submit_attempt_status": "ATTEMPTED",
                    "finalization_status": "OPEN",
                },
                "lineage_status": "BOUND",
            },
        )
        boundary = _run_submit_boundary(
            truth_root,
            build_payload={
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
            admission_payload={
                "admission_status": "ADMIT",
                "binding": True,
                "blocking_reason_codes": [],
            },
        )
        assert boundary["boundary_status"] == "AUTHORIZED"
        assert "POST_SUBMIT_LINEAGE_GAP" not in (boundary.get("blocking_codes") or [])


def test_submit_boundary_fails_closed_when_lineage_status_missing_from_both_paths() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root, ok=True, state="OK", reasons=[])
        _write_kill_switch(truth_root)
        _write_paper_session_ledger_surface(
            truth_root,
            {
                "submit_lifecycle": {
                    "submit_result_status": "OPEN",
                    "submit_attempt_status": "ATTEMPTED",
                    "finalization_status": "OPEN",
                },
            },
        )
        boundary = _run_submit_boundary(
            truth_root,
            build_payload={
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
            admission_payload={
                "admission_status": "ADMIT",
                "binding": True,
                "blocking_reason_codes": [],
            },
        )
        assert boundary["boundary_status"] == "BLOCKED"
        assert "POST_SUBMIT_LINEAGE_GAP" in (boundary.get("blocking_codes") or [])
