from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_submit_boundary_status_v1 as boundary_module
from constellation_2.common.kill_switch_authority_v1 import (  # noqa: E402
    RC_KILL_SWITCH_AUTHORITY_MISMATCH,
    RC_KILL_SWITCH_CANONICAL_MISSING,
    STATUS_FAIL_CLOSED,
)


DAY = "2026-04-08"
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


def _write_trade_submit_status(truth_root: Path, *, ok: bool = True, state: str = "OK") -> None:
    day_authority_path = truth_root / "reports" / "day_authority_decision_v1" / DAY / "day_authority_decision.v1.json"
    economic_build_path = truth_root / "reports" / "economic_state_build_v1" / DAY / "ctx" / "economic_state_build.v1.json"
    economic_package_path = truth_root / "economic_state_package_v1" / DAY / "ctx" / "economic_state_package.v1.json"
    _write_json(day_authority_path, {"artifact": "day_authority_decision_v1", "day_utc": DAY})
    _write_json(economic_build_path, {"artifact": "economic_state_build_v1", "day_utc": DAY})
    _write_json(economic_package_path, {"artifact": "economic_state_package_v1", "day_utc": DAY})
    _write_json(
        truth_root / "trade_submit_readiness_c2_v1" / "_history" / "PAPER" / ACCOUNT / DAY / "status.json",
        {
            "schema_id": "trade_submit_readiness_c2",
            "schema_version": "v1",
            "day_utc": DAY,
            "as_of_utc": f"{DAY}T00:00:00Z",
            "expires_utc": f"{DAY}T00:02:00Z",
            "ok": ok,
            "state": state,
            "environment": "PAPER",
            "ib_account": ACCOUNT,
            "reasons": [] if ok else ["READINESS_NOT_OK"],
            "input_manifest": [],
            "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
            "constitutional_dependency_declaration": {
                "schema_id": "artifact_dependency_declaration",
                "schema_version": "v1",
                "artifact_type": "trade_submit_readiness_c2_v1",
                "artifact_class": "admission_result",
                "authority_id": "trade_submit_readiness_c2_v1",
                "declared_dependency_artifacts": ["day_authority_decision_v1"],
                "dependency_refs": [
                    {
                        "artifact_id": "day_authority_decision_v1",
                        "path": str(day_authority_path.resolve()),
                        "sha256": boundary_module.canonical_hash_for_c2_artifact_v1({"artifact": "day_authority_decision_v1", "day_utc": DAY}),
                        "artifact_class": "admission_result",
                        "finality_state": "provisional",
                    },
                ],
            },
            "constitutional_lineage": {
                "schema_id": "governed_artifact_lineage",
                "schema_version": "v1",
                "artifact_type": "trade_submit_readiness_c2_v1",
                "artifact_version": "v1",
                "artifact_class": "admission_result",
                "authority_id": "trade_submit_readiness_c2_v1",
                "producer_id": "ops/tools/run_trade_submit_readiness_c2_v1.py",
                "generated_at_utc": f"{DAY}T00:00:00Z",
                "effective_at_utc": f"{DAY}T00:00:00Z",
                "finality_state": "provisional",
                "input_artifact_refs": [
                    {
                        "artifact_id": "day_authority_decision_v1",
                        "path": str(day_authority_path.resolve()),
                        "sha256": boundary_module.canonical_hash_for_c2_artifact_v1({"artifact": "day_authority_decision_v1", "day_utc": DAY}),
                        "artifact_class": "admission_result",
                        "finality_state": "provisional",
                    },
                ],
                "policy_snapshot_refs": [],
                "code_version": "abc1234",
                "run_id": f"{DAY}:PAPER:{ACCOUNT}",
                "corrected_from_ref": None,
                "supersedes_ref": None,
            },
            "provenance": {
                "truth_root": str(truth_root.resolve()),
                "registry_sha256": "a" * 64,
                "sleeve_registry_sha256": "b" * 64,
            },
            "economic_state": {
                "status": "OK",
                "source_day_utc": DAY,
                "package_path": str(economic_package_path.resolve()),
                "package_sha256": boundary_module.canonical_hash_for_c2_artifact_v1({"artifact": "economic_state_package_v1", "day_utc": DAY}),
                "build_path": str(economic_build_path.resolve()),
                "build_sha256": boundary_module.canonical_hash_for_c2_artifact_v1({"artifact": "economic_state_build_v1", "day_utc": DAY}),
                "drawdown_pct": "0.000000",
                "drawdown_guard_status": "PASS",
                "policy_baseline_comparison_vs_portfolio_return": "0.000000",
                "external_benchmark_underperformer_count": 0,
                "reason_codes": [],
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
                "stage_execution_status": state,
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
                "stage_execution_status": state,
                "reason_codes": [],
                "upstream_authority_refs": [],
            },
        },
    )


def _write_kill_switch(truth_root: Path, *, state: str = "INACTIVE", allow_entries: bool = True) -> None:
    _write_json(
        truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        {
            "schema_id": "global_kill_switch_state",
            "schema_version": "v1",
            "day_utc": DAY,
            "produced_utc": f"{DAY}T00:00:00Z",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
            "state": state,
            "allow_entries": allow_entries,
            "allow_exits": True,
            "forced_mode": "NORMAL" if state == "INACTIVE" else "FLATTEN_ONLY",
            "reason_codes": [] if (state == "INACTIVE" and allow_entries) else ["C2_KILL_SWITCH_ACTIVE"],
            "input_manifest": [{"type": "gate_stack_verdict_v1", "path": "/tmp/gate_stack.json", "sha256": "f" * 64}],
            "state_sha256": "0" * 64,
        },
    )


def _write_sleeve_kill_switch(truth_root: Path, *, state: str = "INACTIVE", allow_entries: bool = True) -> None:
    _write_json(
        truth_root.parent / "truth_sleeves" / "PRIMARY" / "PAPER" / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        {
            "schema_id": "global_kill_switch_state",
            "schema_version": "v1",
            "day_utc": DAY,
            "produced_utc": f"{DAY}T00:00:00Z",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
            "state": state,
            "allow_entries": allow_entries,
            "allow_exits": True,
            "forced_mode": "NORMAL" if state == "INACTIVE" else "FLATTEN_ONLY",
            "reason_codes": [] if (state == "INACTIVE" and allow_entries) else ["C2_KILL_SWITCH_ACTIVE"],
            "input_manifest": [{"type": "gate_stack_verdict_v1", "path": "/tmp/gate_stack.json", "sha256": "f" * 64}],
            "state_sha256": "0" * 64,
        },
    )


def _write_execution_intent_and_authorization(
    truth_root: Path,
    *,
    intent_hash: str,
    status: str,
    decision: str,
    authorized_quantity: int,
    reason_codes: list[str] | None = None,
) -> None:
    execution_root = (truth_root.parent / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    _write_json(
        execution_root / "intents_v1" / "snapshots" / DAY / f"{intent_hash}.exposure_intent.v1.json",
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "day_utc": DAY,
            "intent_id": f"intent-{intent_hash[:8]}",
            "intent_hash": intent_hash,
        },
    )
    _write_json(
        execution_root / "engine_activity_v1" / "authorization_v1" / DAY / f"{intent_hash}.authorization.v1.json",
        {
            "schema_id": "C2_AUTHORIZATION_V1",
            "schema_version": 1,
            "day_utc": DAY,
            "status": status,
            "reason_codes": list(reason_codes or []),
            "authorization": {
                "decision": decision,
                "authorized_quantity": int(authorized_quantity),
                "constraints": [],
            },
        },
    )


def _run_boundary(
    truth_root: Path,
    *,
    session_day_blocker: str = "",
    day_authority_state: str = "OPEN_READY",
    day_authority_can_submit: bool = True,
    day_authority_reason_codes: list[str] | None = None,
) -> tuple[int, dict]:
    with tempfile.TemporaryDirectory() as td:
        repo_root = Path(td)
        execution_root = (truth_root.parent / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
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
        build_path = (truth_root / "target_day_build_v1" / f"{DAY}.json").resolve()
        day_authority_path = (
            truth_root
            / "reports"
            / "paper_trading_day_authority_v1"
            / DAY
            / "paper_trading_day_authority.v1.json"
        ).resolve()
        authority_reason_codes = list(day_authority_reason_codes or [])
        authority_canonical_blocker = authority_reason_codes[0] if authority_reason_codes else ""
        _write_json(
            build_path,
            {
                "build_status": "COMPLETE",
                "completeness_result": "COMPLETE",
                "closure_status": "CLOSED",
                "hidden_dependency_check_result": {"status": "PASS"},
                "blocker_chain": [],
            },
        )
        with patch.object(boundary_module, "REPO_ROOT", repo_root), patch.object(
            boundary_module, "resolve_decision_truth_root_bridge_v1", return_value=truth_root.resolve()
        ), patch.object(
            boundary_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value=ACCOUNT
        ), patch.object(
            boundary_module,
            "_resolve_session_day_blocker_v1",
            return_value=(
                str(session_day_blocker or "").strip().upper(),
                (truth_root / "reports" / "paper_session_authority_v1" / DAY / "paper_session_authority.v1.json").resolve(),
            ),
        ), patch.object(
            boundary_module,
            "read_target_day_build_ref_v1",
            return_value=type(
                "BuildRef",
                (),
                {
                    "path": build_path,
                    "payload": {
                        "build_status": "COMPLETE",
                        "completeness_result": "COMPLETE",
                        "closure_status": "CLOSED",
                        "hidden_dependency_check_result": {"status": "PASS"},
                        "blocker_chain": [],
                    },
                },
            )(),
        ), patch.object(
            boundary_module,
            "read_target_day_admission_ref_v1",
            return_value=type(
                "AdmissionRef",
                (),
                {
                    "path": (truth_root / "target_day_admission_v1" / f"{DAY}.json").resolve(),
                    "payload": {
                        "admission_status": "ADMIT",
                        "binding": True,
                        "blocking_reason_codes": [],
                    },
                },
            )(),
        ), patch.object(
            boundary_module, "_refresh_trade_submit_readiness_artifact_v1", return_value=0
        ), patch.object(
            boundary_module, "_refresh_paper_trading_day_authority_artifact_v1", return_value=0
        ), patch.object(
            boundary_module,
            "resolve_sleeve_execution_root_v1",
            return_value=type("ExecutionRoot", (), {"execution_root_path": execution_root})(),
        ), patch.object(
            boundary_module,
            "read_validated_surface_v1",
            return_value=type(
                "Ref",
                (),
                {
                    "path": day_authority_path,
                    "payload": {
                        "state": str(day_authority_state).strip().upper(),
                        "can_submit_paper_orders": bool(day_authority_can_submit),
                        "reason_codes": authority_reason_codes,
                        "canonical_blocker": authority_canonical_blocker,
                    },
                },
            )(),
        ), patch.object(
            boundary_module,
            "read_startup_materialization_ref_v1",
            return_value=type("Ref", (), {"path": startup_path, "payload": {"status": "SUCCESS", "blocking_codes": []}})(),
        ), patch.object(
            boundary_module,
            "read_paper_trading_posture_ref_v1",
            return_value=type("Ref", (), {"path": posture_path, "payload": {"system_ready": True, "posture_status": "ENABLED", "blocking_codes": []}})(),
        ), patch.object(
            boundary_module,
            "read_trade_submit_readiness_for_day_v1",
            return_value=type("Ref", (), {"path": readiness_path, "payload": json.loads(readiness_path.read_text(encoding="utf-8"))})(),
        ), patch.object(
            boundary_module,
            "run_runtime_control_kernel_v1",
            return_value={
                "runtime_control_decision": type("Decision", (), {"reason_codes": []})(),
                "runtime_control_record": type("Record", (), {"control_state": "ALLOW"})(),
                "runtime_control_record_path": (truth_root / "runtime_control_kernel_v1" / "records" / DAY / "PAPER" / ACCOUNT / "record.json").resolve(),
                "runtime_control_decision_path": (truth_root / "runtime_control_kernel_v1" / "decisions" / DAY / "decision.json").resolve(),
            },
        ):
            rc = boundary_module.main(["--day_utc", DAY, "--truth_root", str(truth_root)])
        payload = json.loads((truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json").read_text(encoding="utf-8"))
        return rc, payload


def _run_boundary_with_upstream(
    truth_root: Path,
    *,
    build_payload: dict,
    admission_payload: dict,
    session_day_blocker: str = "",
    day_authority_state: str = "OPEN_READY",
    day_authority_can_submit: bool = True,
    day_authority_reason_codes: list[str] | None = None,
) -> tuple[int, dict]:
    with tempfile.TemporaryDirectory() as td:
        repo_root = Path(td)
        execution_root = (truth_root.parent / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
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
        build_path = (truth_root / "target_day_build_v1" / f"{DAY}.json").resolve()
        admission_path = (truth_root / "target_day_admission_v1" / f"{DAY}.json").resolve()
        day_authority_path = (
            truth_root
            / "reports"
            / "paper_trading_day_authority_v1"
            / DAY
            / "paper_trading_day_authority.v1.json"
        ).resolve()
        authority_reason_codes = list(day_authority_reason_codes or [])
        authority_canonical_blocker = authority_reason_codes[0] if authority_reason_codes else ""
        _write_json(build_path, build_payload)
        with patch.object(boundary_module, "REPO_ROOT", repo_root), patch.object(
            boundary_module, "resolve_decision_truth_root_bridge_v1", return_value=truth_root.resolve()
        ), patch.object(
            boundary_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value=ACCOUNT
        ), patch.object(
            boundary_module,
            "_resolve_session_day_blocker_v1",
            return_value=(
                str(session_day_blocker or "").strip().upper(),
                (truth_root / "reports" / "paper_session_authority_v1" / DAY / "paper_session_authority.v1.json").resolve(),
            ),
        ), patch.object(
            boundary_module, "_refresh_trade_submit_readiness_artifact_v1", return_value=0
        ), patch.object(
            boundary_module, "_refresh_paper_trading_day_authority_artifact_v1", return_value=0
        ), patch.object(
            boundary_module,
            "resolve_sleeve_execution_root_v1",
            return_value=type("ExecutionRoot", (), {"execution_root_path": execution_root})(),
        ), patch.object(
            boundary_module,
            "read_validated_surface_v1",
            return_value=type(
                "Ref",
                (),
                {
                    "path": day_authority_path,
                    "payload": {
                        "state": str(day_authority_state).strip().upper(),
                        "can_submit_paper_orders": bool(day_authority_can_submit),
                        "reason_codes": authority_reason_codes,
                        "canonical_blocker": authority_canonical_blocker,
                    },
                },
            )(),
        ), patch.object(
            boundary_module,
            "read_startup_materialization_ref_v1",
            return_value=type("Ref", (), {"path": startup_path, "payload": {"status": "SUCCESS", "blocking_codes": []}})(),
        ), patch.object(
            boundary_module,
            "read_paper_trading_posture_ref_v1",
            return_value=type("Ref", (), {"path": posture_path, "payload": {"system_ready": True, "posture_status": "ENABLED", "blocking_codes": []}})(),
        ), patch.object(
            boundary_module,
            "read_trade_submit_readiness_for_day_v1",
            return_value=type("Ref", (), {"path": readiness_path, "payload": json.loads(readiness_path.read_text(encoding="utf-8"))})(),
        ), patch.object(
            boundary_module,
            "read_target_day_build_ref_v1",
            return_value=type("BuildRef", (), {"path": build_path, "payload": build_payload})(),
        ), patch.object(
            boundary_module,
            "read_target_day_admission_ref_v1",
            return_value=type("AdmissionRef", (), {"path": admission_path, "payload": admission_payload})(),
        ), patch.object(
            boundary_module,
            "run_runtime_control_kernel_v1",
            return_value={
                "runtime_control_decision": type("Decision", (), {"reason_codes": []})(),
                "runtime_control_record": type("Record", (), {"control_state": "ALLOW"})(),
                "runtime_control_record_path": (truth_root / "runtime_control_kernel_v1" / "records" / DAY / "PAPER" / ACCOUNT / "record.json").resolve(),
                "runtime_control_decision_path": (truth_root / "runtime_control_kernel_v1" / "decisions" / DAY / "decision.json").resolve(),
            },
        ):
            rc = boundary_module.main(["--day_utc", DAY, "--truth_root", str(truth_root)])
        payload = json.loads((truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json").read_text(encoding="utf-8"))
        return rc, payload


def test_submit_boundary_blocks_when_kill_switch_missing() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        rc, payload = _run_boundary(truth_root)
        assert rc == 2
        assert payload["boundary_status"] == "BLOCKED"
        assert payload["submission_authorized"] is False
        failed = {row["logical_name"]: row for row in payload["failed_checks"]}
        assert "global_kill_switch_state_v1" in failed
        assert RC_KILL_SWITCH_CANONICAL_MISSING in failed["global_kill_switch_state_v1"]["reason_codes"]


def test_submit_boundary_authorizes_when_kill_switch_present_and_inactive() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root, state="INACTIVE", allow_entries=True)
        rc, payload = _run_boundary(truth_root)
        assert rc == 0
        assert payload["boundary_status"] == "AUTHORIZED"
        assert payload["submission_authorized"] is True
        assert payload["constitutional_lineage"]["artifact_type"] == "submit_boundary_status_v1"
        assert payload["constitutional_dependency_declaration"]["declared_dependency_artifacts"] == [
            "target_day_build_v1",
            "trade_submit_readiness_c2_v1",
        ]


def test_submit_boundary_blocks_when_kill_switch_is_active() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root, state="ACTIVE", allow_entries=False)
        rc, payload = _run_boundary(truth_root)
        assert rc == 2
        assert payload["boundary_status"] == "BLOCKED"
        assert payload["submission_authorized"] is False
        failed = {row["logical_name"]: row for row in payload["failed_checks"]}
        assert failed["global_kill_switch_state_v1"]["status"] == "FAIL"


def test_submit_boundary_blocks_when_sleeve_mismatches_canonical() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root, state="INACTIVE", allow_entries=True)
        _write_sleeve_kill_switch(truth_root, state="ACTIVE", allow_entries=False)
        rc, payload = _run_boundary(truth_root)
        assert rc == 2
        assert payload["boundary_status"] == "BLOCKED"
        assert payload["submission_authorized"] is False
        failed = {row["logical_name"]: row for row in payload["failed_checks"]}
        assert RC_KILL_SWITCH_AUTHORITY_MISMATCH in failed["global_kill_switch_state_v1"]["reason_codes"]


def test_submit_boundary_authorizes_when_sleeve_missing_and_canonical_inactive() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root, state="INACTIVE", allow_entries=True)
        rc, payload = _run_boundary(truth_root)
        assert rc == 0
        assert payload["boundary_status"] == "AUTHORIZED"
        assert payload["submission_authorized"] is True


def test_submit_boundary_blocks_when_intents_exist_but_none_authorized() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root, state="INACTIVE", allow_entries=True)
        _write_execution_intent_and_authorization(
            truth_root,
            intent_hash="a" * 64,
            status="REJECTED",
            decision="REJECTED",
            authorized_quantity=0,
            reason_codes=["AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS"],
        )
        rc, payload = _run_boundary(truth_root)
        assert rc == 2
        assert payload["boundary_status"] == "BLOCKED"
        assert payload["submission_authorized"] is False
        failed = {row["logical_name"]: row for row in payload["failed_checks"]}
        auth_row = failed["engine_activity_authorization_v1"]
        assert auth_row["status"] == "FAIL"
        assert "SUBMIT_BOUNDARY_NO_AUTHORIZED_INTENTS" in auth_row["reason_codes"]
        assert "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS" in auth_row["reason_codes"]


def test_submit_boundary_authorizes_when_any_intent_has_positive_authorized_quantity() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root, state="INACTIVE", allow_entries=True)
        _write_execution_intent_and_authorization(
            truth_root,
            intent_hash="b" * 64,
            status="AUTHORIZED",
            decision="AUTHORIZED",
            authorized_quantity=1,
        )
        rc, payload = _run_boundary(truth_root)
        assert rc == 0
        assert payload["boundary_status"] == "AUTHORIZED"
        assert payload["submission_authorized"] is True


def test_submit_boundary_uses_kill_switch_authority_resolver() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root, state="INACTIVE", allow_entries=True)
        with patch.object(
            boundary_module,
            "resolve_kill_switch_authority_v1",
            return_value=type(
                "KillResult",
                (),
                {
                    "status": STATUS_FAIL_CLOSED,
                    "canonical_path": (truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json").resolve(),
                    "canonical_sha256": "0" * 64,
                    "payload": None,
                    "state": "UNKNOWN",
                    "allow_entries": False,
                    "allow_exits": None,
                    "reason_codes": (RC_KILL_SWITCH_AUTHORITY_MISMATCH,),
                    "reason_code": RC_KILL_SWITCH_AUTHORITY_MISMATCH,
                    "reason_detail": "patched",
                    "sleeve_present": True,
                    "sleeve_paths": tuple(),
                    "mismatch_paths": tuple(),
                },
            )(),
        ):
            rc, payload = _run_boundary(truth_root)
        assert rc == 2
        failed = {row["logical_name"]: row for row in payload["failed_checks"]}
        assert RC_KILL_SWITCH_AUTHORITY_MISMATCH in failed["global_kill_switch_state_v1"]["reason_codes"]


def test_submit_boundary_status_end_to_end_propagates_fail_closed_resolver_reason() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root, state="INACTIVE", allow_entries=True)
        with patch.object(
            boundary_module,
            "resolve_kill_switch_authority_v1",
            return_value=type(
                "KillResult",
                (),
                {
                    "status": STATUS_FAIL_CLOSED,
                    "canonical_path": (truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json").resolve(),
                    "canonical_sha256": "0" * 64,
                    "payload": None,
                    "state": "UNKNOWN",
                    "allow_entries": False,
                    "allow_exits": None,
                    "reason_codes": (RC_KILL_SWITCH_AUTHORITY_MISMATCH,),
                    "reason_code": RC_KILL_SWITCH_AUTHORITY_MISMATCH,
                    "reason_detail": "forced_fail_closed",
                    "sleeve_present": True,
                    "sleeve_paths": tuple(),
                    "mismatch_paths": tuple(),
                },
            )(),
        ):
            rc, payload = _run_boundary(truth_root)
        assert rc == 2
        assert payload["submission_authorized"] is False
        failed = {row["logical_name"]: row for row in payload["failed_checks"]}
        assert RC_KILL_SWITCH_AUTHORITY_MISMATCH in failed["global_kill_switch_state_v1"]["reason_codes"]


def test_submit_boundary_keeps_build_and_admission_advisory_when_submit_local_checks_pass() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root, state="INACTIVE", allow_entries=True)
        rc, payload = _run_boundary_with_upstream(
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
        assert rc == 0
        assert payload["boundary_status"] == "AUTHORIZED"
        assert payload["submission_authorized"] is True
        required = {row["logical_name"]: row for row in payload["required_boundary_checks"]}
        assert required["target_day_build_v1"]["status"] == "ADVISORY_FAIL"
        assert required["target_day_admission_v1"]["status"] == "ADVISORY_FAIL"
        assert payload["blocking_codes"] == []


def test_submit_boundary_emits_uniform_closure_fields() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root, state="INACTIVE", allow_entries=True)
        _write_sleeve_kill_switch(truth_root, state="INACTIVE", allow_entries=True)

        with patch.object(
            boundary_module,
            "run_runtime_control_kernel_v1",
            return_value={
                "runtime_control_decision": type("Decision", (), {"reason_codes": []})(),
                "runtime_control_record": type("Record", (), {"control_state": "ALLOW"})(),
                "runtime_control_record_path": (truth_root / "runtime_control_kernel_v1" / "records" / DAY / "PAPER" / ACCOUNT / "record.json").resolve(),
                "runtime_control_decision_path": (truth_root / "runtime_control_kernel_v1" / "decisions" / DAY / "decision.json").resolve(),
            },
        ):
            rc, payload = _run_boundary(truth_root)

        assert rc == 0
        assert payload["boundary_status"] == "AUTHORIZED"
        assert payload["closure_state"] == "COMPLETE"
        assert payload["first_blocker_code"] == ""
        assert payload["missing_dependency_artifacts"] == []


def test_submit_boundary_non_trading_day_emits_blocked_surface() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root, state="INACTIVE", allow_entries=True)
        rc, payload = _run_boundary(truth_root, session_day_blocker="NON_TRADING_DAY")
        assert rc == 2
        assert payload["boundary_status"] == "BLOCKED"
        assert payload["submit_allowed"] is False
        assert payload["canonical_blocker"] == "NON_TRADING_DAY"
        assert "NON_TRADING_DAY" in payload["reason_codes"]
        assert payload["status"] == "NOT_READY"
        assert payload["source_surface_path"].endswith("/paper_session_authority.v1.json")


def test_submit_boundary_missing_session_authority_fails_closed() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root, state="INACTIVE", allow_entries=True)
        rc, payload = _run_boundary(truth_root, session_day_blocker="SESSION_AUTHORITY_MISSING")
        assert rc == 2
        assert payload["boundary_status"] == "BLOCKED"
        assert payload["submit_allowed"] is False
        assert payload["canonical_blocker"] == "SESSION_AUTHORITY_MISSING"
        assert "SESSION_AUTHORITY_MISSING" in payload["reason_codes"]


def test_submit_boundary_refuses_orders_when_day_authority_not_open_ready() -> None:
    with tempfile.TemporaryDirectory() as td:
        truth_root = Path(td) / "truth"
        _write_startup_materialization(truth_root)
        _write_paper_trading_posture(truth_root)
        _write_trade_submit_status(truth_root)
        _write_kill_switch(truth_root, state="INACTIVE", allow_entries=True)
        rc, payload = _run_boundary(
            truth_root,
            day_authority_state="PREFLIGHT_BLOCKED",
            day_authority_can_submit=False,
            day_authority_reason_codes=["REPLAY_CERTIFICATION_GATE_V1_MISSING"],
        )
        assert rc == 2
        assert payload["boundary_status"] == "BLOCKED"
        assert payload["submission_authorized"] is False
        failed = {row["logical_name"]: row for row in payload["failed_checks"]}
        assert "paper_trading_day_authority_v1" in failed
        assert "REPLAY_CERTIFICATION_GATE_V1_MISSING" in failed["paper_trading_day_authority_v1"]["reason_codes"]
