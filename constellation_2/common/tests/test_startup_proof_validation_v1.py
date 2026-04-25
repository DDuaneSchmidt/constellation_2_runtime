from __future__ import annotations

import json
import sys
import tempfile
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_startup_proof_validation_v1 as startup_proof_module
import ops.tools.run_submit_boundary_status_v1 as submit_boundary_module
from constellation_2.common.paper_session_fact_plane_v1 import canonical_paper_session_id_v1
from constellation_2.common.paper_session_ledger_v1 import build_paper_session_ledger_v1, write_paper_session_ledger_v1


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _producer() -> dict[str, str]:
    return {"repo": "constellation", "module": "test_startup_proof_validation_v1", "git_sha": "a" * 40}


def _fact_dep(day_utc: str, logical_name: str) -> dict[str, object]:
    return {
        "logical_name": logical_name,
        "absolute_path": f"/tmp/{logical_name}.json",
        "sha256": "b" * 64,
        "day_utc": day_utc,
        "status": "PASS",
        "reason_codes": [],
    }


def _minimal_fact_row(day_utc: str, logical_name: str) -> dict[str, object]:
    return {
        "logical_name": logical_name,
        "required_for_authority": True,
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


def _write_granted_ledger(truth_root: Path, day_utc: str) -> None:
    fact_rows = [
        _minimal_fact_row(day_utc, "paper_trading_posture_v1"),
        _minimal_fact_row(day_utc, "pre_open_bundle_v1"),
        _minimal_fact_row(day_utc, "startup_materialization_v1"),
        _minimal_fact_row(day_utc, "sleeve_rollup_v1"),
    ]
    ledger = build_paper_session_ledger_v1(
        day_utc=day_utc,
        session_id=canonical_paper_session_id_v1(day_utc),
        evaluated_at_utc=f"{day_utc}T00:00:00Z",
        provenance=_producer(),
        fact_refs=fact_rows,
        evidence_freeze={
            "evidence_digest": "c" * 64,
            "overall_evidence_status": "READY",
            "blocking_codes": [],
            "inputs": list(fact_rows),
        },
        authority_status="GRANTED",
        system_ready=True,
        submission_authorized=False,
        control_blocking_codes=[],
        submit_lifecycle={
            "submit_attempt_status": "NOT_OBSERVED_AT_EVALUATION",
            "submit_attempted": False,
            "submit_result_status": "NOT_OBSERVED",
            "reason_codes": [],
            "submit_evidence_refs": [],
            "finalization_status": "OPEN",
        },
        post_submit_lifecycle={
            "lineage_status": "NOT_OBSERVED_AT_EVALUATION",
            "latest_authoritative_lineage_ref": "",
            "latest_authoritative_lineage_sha256": "",
            "execution_evidence_refs": [],
            "reconciliation_refs": [],
            "gap_codes": [],
        },
        operator_summary={
            "authority_scope": "DERIVED_ONLY_VIEW",
            "ledger_ref": "/tmp/paper_session_ledger.v1.json",
            "summary_state": "AUTHORIZED_PRE_SUBMIT",
            "authority_status": "GRANTED",
            "submission_authorized": False,
            "non_authority_notice": "derived only",
            "blocking_codes": [],
        },
    )
    write_paper_session_ledger_v1(truth_root=truth_root, ledger=ledger)


def _write_startup_materialization_fail_options_pending(truth_root: Path, day_utc: str) -> None:
    phasec_root = truth_root / "phaseC_preflight_v1" / day_utc
    attempt_dir = phasec_root / "attempt_A0001"
    veto_path = attempt_dir / "spy.veto_record.v1.json"
    _write_json(
        veto_path,
        {
            "schema_id": "veto_record",
            "schema_version": "v1",
            "boundary": "SUBMIT",
            "observed_at_utc": f"{day_utc}T00:00:00Z",
            "reason_code": "C2_SUBMIT_FAIL_CLOSED_REQUIRED",
            "reason_detail": f"OPTIONS_SNAPSHOT_ROOT_MISSING: {truth_root / 'options_chain_snapshot_v1' / day_utc}",
            "inputs": {
                "intent_hash": "spy",
                "plan_hash": None,
                "chain_snapshot_hash": None,
                "freshness_cert_hash": None,
            },
            "pointers": [],
            "input_manifest": [],
            "upstream_hash": "spy",
            "canonical_json_hash": None,
        },
    )
    _write_json(
        phasec_root / "latest_active_attempt.v1.json",
        {
            "schema_id": "phasec_latest_active_attempt.v1",
            "schema_version": "v1",
            "day_utc": day_utc,
            "attempt_id": "A0001",
            "attempt_dir": str(attempt_dir.resolve()),
        },
    )
    _write_json(
        truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json",
        {
            "schema_id": "startup_materialization",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "status": "FAIL",
            "required_inputs_checked": [_fact_dep(day_utc, "intent_file:spy.exposure_intent.v1.json")],
            "materialized_outputs": [],
            "blocking_codes": ["STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED"],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:00:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "producer_run_id": f"startup_materialization_v1:{day_utc}",
            "path_resolution_evidence": {
                "phasec_root": str(phasec_root.resolve()),
                "latest_active_attempt_path": str((phasec_root / "latest_active_attempt.v1.json").resolve()),
            },
            "phasec_materializer_result": {
                "returncode": 0,
                "stdout": f"BLOCKED: intent_hash=spy path={veto_path.resolve()}",
                "stderr": "",
            },
        },
    )


def _write_startup_materialization_success(truth_root: Path, day_utc: str) -> None:
    _write_json(
        truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json",
        {
            "schema_id": "startup_materialization",
            "schema_version": "v1",
            "authority_scope": "NON_AUTHORITY_FACT",
            "day_utc": day_utc,
            "session_id": canonical_paper_session_id_v1(day_utc),
            "status": "SUCCESS",
            "required_inputs_checked": [_fact_dep(day_utc, "intent_file:spy.exposure_intent.v1.json")],
            "materialized_outputs": [_fact_dep(day_utc, "phasec_materialized_output:spy:binding_record.v2.json")],
            "blocking_codes": [],
            "producer": _producer(),
            "produced_at_utc": f"{day_utc}T00:00:00Z",
            "freshness_verdict": "CURRENT",
            "linkage_verdict": "LINKED",
            "producer_run_id": f"startup_materialization_v1:{day_utc}",
        },
    )


def _read_startup_proof_payload(truth_root: Path, day_utc: str) -> dict:
    path = truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_paper_startup_allows_missing_options_snapshot_with_explicit_tolerance() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-22"
        _write_granted_ledger(truth_root, day_utc)
        _write_startup_materialization_fail_options_pending(truth_root, day_utc)

        with patch.object(
            startup_proof_module,
            "resolve_decision_truth_root_bridge_v1",
            lambda _arg, repo_root, caller: Path(truth_root).resolve(),
        ):
            rc = startup_proof_module.main(
                ["--day_utc", day_utc, "--truth_root", str(truth_root), "--environment", "PAPER"]
            )
        assert rc == 0
        payload = _read_startup_proof_payload(truth_root, day_utc)
        assert payload["status"] == "STARTUP_READY"
        startup_row = next(row for row in payload["checks"] if row["logical_name"] == "startup_materialization_v1")
        assert startup_row["status"] == "PASS"
        assert "STARTUP_PROOF_STARTUP_MATERIALIZATION_NOT_SUCCESS" not in payload["blocking_codes"]
        assert startup_row["detail"]["startup_tolerance_applied"] is True
        assert "PAPER_START_ALLOWED_OPTIONS_SNAPSHOT_PENDING" in startup_row["detail"]["startup_tolerance_reason_codes"]


def test_live_environment_keeps_fail_closed_behavior_for_same_startup_materialization_failure() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-22"
        _write_granted_ledger(truth_root, day_utc)
        _write_startup_materialization_fail_options_pending(truth_root, day_utc)

        with patch.object(
            startup_proof_module,
            "resolve_decision_truth_root_bridge_v1",
            lambda _arg, repo_root, caller: Path(truth_root).resolve(),
        ):
            rc = startup_proof_module.main(
                ["--day_utc", day_utc, "--truth_root", str(truth_root), "--environment", "LIVE"]
            )
        assert rc == 2
        payload = _read_startup_proof_payload(truth_root, day_utc)
        assert payload["status"] == "STARTUP_BLOCKED"
        assert "STARTUP_PROOF_STARTUP_MATERIALIZATION_NOT_SUCCESS" in payload["blocking_codes"]
        startup_row = next(row for row in payload["checks"] if row["logical_name"] == "startup_materialization_v1")
        assert startup_row["detail"].get("startup_tolerance_applied") is not True


def test_startup_proof_normal_success_path_is_unchanged_when_startup_materialization_is_success() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-22"
        _write_granted_ledger(truth_root, day_utc)
        _write_startup_materialization_success(truth_root, day_utc)

        with patch.object(
            startup_proof_module,
            "resolve_decision_truth_root_bridge_v1",
            lambda _arg, repo_root, caller: Path(truth_root).resolve(),
        ):
            rc = startup_proof_module.main(
                ["--day_utc", day_utc, "--truth_root", str(truth_root), "--environment", "PAPER"]
            )
        assert rc == 0
        payload = _read_startup_proof_payload(truth_root, day_utc)
        assert payload["status"] == "STARTUP_READY"
        startup_row = next(row for row in payload["checks"] if row["logical_name"] == "startup_materialization_v1")
        assert startup_row["status"] == "PASS"
        assert startup_row["detail"].get("startup_tolerance_applied") is not True


def test_submit_boundary_stays_blocked_when_startup_materialization_is_fail_even_if_startup_tolerance_applies() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        day_utc = "2026-04-22"
        build_ref = SimpleNamespace(
            path=(truth_root / "target_day_build_v1" / f"{day_utc}.json"),
            payload={
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
        )
        admission_ref = SimpleNamespace(
            path=(truth_root / "target_day_admission_v1" / f"{day_utc}.json"),
            payload={"admission_status": "ADMIT", "binding": True, "blocking_reason_codes": []},
        )
        readiness_ref = SimpleNamespace(
            path=(truth_root / "trade_submit_readiness" / "status.json"),
            payload={"ok": True, "reasons": []},
        )
        startup_ref = SimpleNamespace(
            path=(truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json"),
            payload={
                "status": "FAIL",
                "blocking_codes": ["STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED"],
            },
        )
        posture_ref = SimpleNamespace(
            path=(truth_root / "reports" / "paper_trading_posture_v1" / day_utc / "paper_trading_posture.v1.json"),
            payload={"system_ready": True, "posture_status": "ENABLED", "blocking_codes": []},
        )

        with ExitStack() as stack:
            stack.enter_context(patch.object(submit_boundary_module, "require_authoritative_repo_runtime_v1", lambda _repo: None))
            stack.enter_context(
                patch.object(
                    submit_boundary_module,
                    "resolve_decision_truth_root_bridge_v1",
                    lambda _arg, repo_root, caller: Path(truth_root).resolve(),
                )
            )
            stack.enter_context(
                patch.object(submit_boundary_module, "resolve_single_paper_ib_account_from_sleeve_registry", lambda _repo: "DUO847203")
            )
            stack.enter_context(
                patch.object(
                    submit_boundary_module,
                    "resolve_sleeve_execution_root_v1",
                    lambda **kwargs: SimpleNamespace(execution_root_path=Path(truth_root).resolve()),
                )
            )
            stack.enter_context(patch.object(submit_boundary_module, "_refresh_trade_submit_readiness_artifact_v1", lambda **kwargs: 0))
            stack.enter_context(patch.object(submit_boundary_module, "_sha256_file", lambda _path: "d" * 64))
            stack.enter_context(patch.object(submit_boundary_module, "read_target_day_build_ref_v1", lambda **kwargs: build_ref))
            stack.enter_context(patch.object(submit_boundary_module, "read_target_day_admission_ref_v1", lambda **kwargs: admission_ref))
            stack.enter_context(patch.object(submit_boundary_module, "read_trade_submit_readiness_for_day_v1", lambda **kwargs: readiness_ref))
            stack.enter_context(patch.object(submit_boundary_module, "read_startup_materialization_ref_v1", lambda **kwargs: startup_ref))
            stack.enter_context(patch.object(submit_boundary_module, "read_paper_trading_posture_ref_v1", lambda **kwargs: posture_ref))
            stack.enter_context(
                patch.object(
                    submit_boundary_module,
                    "resolve_kill_switch_authority_v1",
                    lambda **kwargs: SimpleNamespace(
                        reason_codes=[],
                        status=submit_boundary_module.KILL_SWITCH_STATUS_PASS,
                        state="INACTIVE",
                        allow_entries=True,
                        canonical_path=(truth_root / "risk_v1" / "kill_switch_v1" / day_utc / "global_kill_switch_state.v1.json"),
                    ),
                )
            )
            stack.enter_context(patch.object(submit_boundary_module, "validate_governed_artifact_payload_v1", lambda **kwargs: None))
            stack.enter_context(
                patch.object(
                    submit_boundary_module,
                    "run_runtime_control_kernel_v1",
                    lambda **kwargs: {
                        "runtime_control_decision": SimpleNamespace(reason_codes=[]),
                        "runtime_control_record": SimpleNamespace(control_state="ALLOW"),
                        "runtime_control_record_path": str(truth_root / "runtime_control_kernel_v1" / "record.v1.json"),
                        "runtime_control_decision_path": str(truth_root / "runtime_control_kernel_v1" / "decision.v1.json"),
                    },
                )
            )
            stack.enter_context(patch.object(submit_boundary_module, "effective_enforcement_mode_v1", lambda: "SOFT"))
            stack.enter_context(
                patch.object(
                    submit_boundary_module,
                    "assert_constitutional_writer_allowed_v1",
                    lambda *args, **kwargs: {"artifact_class": "NON_AUTHORITY_FACT", "required_upstream_dependencies": []},
                )
            )
            stack.enter_context(
                patch.object(submit_boundary_module, "build_artifact_dependency_declaration_v1", lambda **kwargs: {})
            )
            stack.enter_context(patch.object(submit_boundary_module, "build_governed_artifact_lineage_v1", lambda **kwargs: {}))

            rc = submit_boundary_module.main(["--day_utc", day_utc, "--truth_root", str(truth_root)])
        assert rc == 2
        submit_payload = json.loads(
            (
                truth_root
                / "reports"
                / "submit_boundary_status_v1"
                / day_utc
                / "submit_boundary_status.v1.json"
            ).read_text(encoding="utf-8")
        )
        assert submit_payload["boundary_status"] == "BLOCKED"
        assert submit_payload["submission_authorized"] is False
        assert "STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED" in submit_payload["blocking_codes"]
