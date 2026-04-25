from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import constellation_2.common.trade_readiness_reducer_v1 as reducer_module
import ops.tools.run_trade_readiness_reducer_v1 as reducer_tool_module


DAY = "2026-04-23"
ACCOUNT = "DUO847203"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed_common_truth(truth_root: Path, *, authority_granted: bool = True, startup_status: str = "SUCCESS", startup_codes: list[str] | None = None) -> None:
    _write_json(
        truth_root / "reports" / "paper_session_authority_v1" / DAY / "paper_session_authority.v1.json",
        {
            "schema_id": "paper_session_authority",
            "schema_version": "v1",
            "day_utc": DAY,
            "mode": "PAPER",
            "authority_status": "GRANTED" if authority_granted else "DENIED",
            "paper_open_allowed": bool(authority_granted),
            "safety_checks": [
                {"check_id": "CANONICAL_KILL_SWITCH_INACTIVE", "status": "PASS" if authority_granted else "FAIL"},
            ],
        },
    )
    _write_json(
        truth_root / "reports" / "startup_materialization_v1" / DAY / "startup_materialization.v1.json",
        {
            "schema_id": "startup_materialization",
            "schema_version": "v1",
            "day_utc": DAY,
            "status": startup_status,
            "blocking_codes": list(startup_codes or []),
        },
    )


def _seed_readiness(
    execution_truth_root: Path,
    *,
    reasons: list[str] | None = None,
    ok: bool = True,
    state: str = "OK",
    as_of_utc: str | None = None,
    expires_utc: str | None = None,
) -> None:
    as_of_value = str(as_of_utc or f"{DAY}T00:00:00Z")
    expires_value = str(expires_utc or f"{DAY}T23:59:59Z")
    _write_json(
        execution_truth_root / "trade_submit_readiness_c2_v1" / "_history" / "PAPER" / ACCOUNT / DAY / "status.json",
        {
            "schema_id": "trade_submit_readiness_c2",
            "schema_version": "v1",
            "day_utc": DAY,
            "as_of_utc": as_of_value,
            "expires_utc": expires_value,
            "ok": ok,
            "state": state,
            "environment": "PAPER",
            "ib_account": ACCOUNT,
            "reasons": list(reasons or []),
            "input_manifest": [],
            "producer": {"repo": "constellation", "module": "test", "git_sha": "abc1234"},
            "provenance": {"truth_root": str(execution_truth_root), "registry_sha256": "a" * 64, "sleeve_registry_sha256": "b" * 64},
            "constitutional_dependency_declaration": {},
            "constitutional_lineage": {},
            "economic_state": {
                "status": "OK",
                "source_day_utc": DAY,
                "package_path": "",
                "package_sha256": "",
                "build_path": "",
                "build_sha256": "",
                "drawdown_pct": "0.0",
                "drawdown_guard_status": "PASS",
                "policy_baseline_comparison_vs_portfolio_return": "0.0",
                "external_benchmark_underperformer_count": 0,
                "reason_codes": [],
            },
            "session_authority_attestation": {
                "decision_artifact_path": "",
                "decision_artifact_sha256": "c" * 64,
                "policy_version": "test",
                "evaluator_version": "test",
                "venue": "C2",
                "session_date": DAY,
                "decision_status": "OK",
                "session_class": None,
                "stage_id": "TEST",
                "policy_action": "ALLOW",
                "stage_execution_status": "OK",
                "reason_codes": [],
            },
            "run_state_authority_attestation": {
                "authority_family": "test",
                "authority_artifact_path": "",
                "authority_artifact_sha256": "d" * 64,
                "policy_version": "test",
                "evaluator_version": "test",
                "decision_status": "OK",
                "classification_field": "decision_state",
                "classification_value": "OPEN",
                "cycle_snapshot_family": "test",
                "cycle_snapshot_artifact_path": "",
                "cycle_snapshot_artifact_sha256": "e" * 64,
                "cycle_id": "cycle",
                "cycle_coherence_status": "COHERENT",
                "stage_id": "TEST",
                "stage_execution_status": "OK",
                "reason_codes": [],
                "upstream_authority_refs": [],
            },
        },
    )


def _seed_intent_authorization(execution_truth_root: Path, *, authorized_qty: int = 1, status: str = "AUTHORIZED", decision: str = "AUTHORIZED", reason_codes: list[str] | None = None) -> None:
    intent_hash = "f" * 64
    _write_json(execution_truth_root / "intents_v1" / "snapshots" / DAY / f"{intent_hash}.exposure_intent.v1.json", {"intent_hash": intent_hash})
    _write_json(
        execution_truth_root / "engine_activity_v1" / "authorization_v1" / DAY / f"{intent_hash}.authorization.v1.json",
        {
            "status": status,
            "authorization": {
                "decision": decision,
                "authorized_quantity": authorized_qty,
                "reason_codes": list(reason_codes or []),
            },
        },
    )


def _seed_execution_build(truth_root: Path, *, submission_id: str = "submission-1") -> None:
    _write_json(
        truth_root / "reports" / "execution_build_v1" / DAY / submission_id / "execution_build.v1.json",
        {
            "schema_id": "execution_build",
            "schema_version": "v1",
            "submission_id": submission_id,
            "closure_status": "COMPLETE",
        },
    )


def _seed_submission_pointer(execution_truth_root: Path, *, submission_id: str) -> None:
    _write_json(
        execution_truth_root / "execution_evidence_v1" / "submissions" / DAY / "latest_pointer.v1.json",
        {
            "schema_id": "submission_pointer",
            "schema_version": "v1",
            "day_utc": DAY,
            "submission_id": submission_id,
        },
    )


def _seed_broker_submission_record(
    execution_truth_root: Path,
    *,
    submission_id: str,
    status: str,
    order_id: int | None,
    perm_id: int | None,
    error_code: str = "",
    submitted_at_utc: str | None = None,
) -> None:
    error_obj = None
    if error_code:
        error_obj = {"code": error_code, "message": "seeded test error"}
    _write_json(
        execution_truth_root / "execution_evidence_v1" / "submissions" / DAY / submission_id / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "submission_id": submission_id,
            "submitted_at_utc": str(submitted_at_utc or f"{DAY}T00:00:00Z"),
            "binding_hash": "a" * 64,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "status": status,
            "broker_ids": {"order_id": order_id, "perm_id": perm_id},
            "error": error_obj,
        },
    )


def _seed_broker_outcome(
    execution_truth_root: Path,
    *,
    submission_id: str,
    outcome_state: str,
    status: str,
    order_id: int | None,
    perm_id: int | None,
    reason_codes: list[str] | None = None,
    evaluated_at_utc: str | None = None,
    error: dict | None = None,
) -> None:
    _write_json(
        execution_truth_root / "execution_evidence_v1" / "submissions" / DAY / submission_id / "broker_order_outcome_v1.json",
        {
            "schema_id": "broker_order_outcome",
            "schema_version": "v1",
            "day_utc": DAY,
            "environment": "PAPER",
            "submission_id": submission_id,
            "evaluated_at_utc": str(evaluated_at_utc or f"{DAY}T00:10:00Z"),
            "outcome_state": outcome_state,
            "status": status,
            "ib_account": ACCOUNT,
            "broker_ids": {"order_id": order_id, "perm_id": perm_id},
            "reason_codes": list(reason_codes or []),
            "error": error,
            "evidence_artifacts": [],
        },
    )


def _patch_execution_root(monkeypatch, execution_truth_root: Path) -> None:
    monkeypatch.setattr(
        reducer_module,
        "resolve_sleeve_execution_root_v1",
        lambda **kwargs: SimpleNamespace(execution_root_path=execution_truth_root.resolve()),
    )


def test_trade_readiness_blocked_by_wrong_environment(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(execution_truth_root)
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="LIVE",
        ib_account=ACCOUNT,
    )

    assert payload["decision"] == "NO"
    assert payload["canonical_blocker"] == "WRONG_ENVIRONMENT"


def test_trade_readiness_missing_options_snapshot_maps_to_data_not_ready(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root, startup_status="FAIL", startup_codes=["OPTIONS_SNAPSHOT_ROOT_MISSING"])
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(execution_truth_root)
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
    )

    assert payload["decision"] == "NO"
    assert payload["canonical_blocker"] == "DATA_NOT_READY"


def test_trade_readiness_missing_nav_maps_to_nav_invalid(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root, reasons=["FAIL:NAV_INVALID"])
    _seed_intent_authorization(execution_truth_root)
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
    )

    assert payload["decision"] == "NO"
    assert payload["canonical_blocker"] == "NAV_INVALID"


def test_trade_readiness_signal_filtered_is_canonical_blocker(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root, reasons=["FAIL:SIGNAL_FILTERED"])
    _seed_intent_authorization(execution_truth_root)
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
    )

    assert payload["decision"] == "NO"
    assert payload["canonical_blocker"] == "SIGNAL_FILTERED"


def test_trade_readiness_headroom_insufficient_is_canonical_blocker(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(
        execution_truth_root,
        authorized_qty=0,
        status="DENIED",
        decision="DENIED",
        reason_codes=["BUNDLE_B_HEADROOM_REJECTED"],
    )
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
    )

    assert payload["decision"] == "NO"
    assert payload["canonical_blocker"] == "HEADROOM_INSUFFICIENT"


def test_trade_readiness_zero_authorized_quantity_maps_to_size_not_proven(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(
        execution_truth_root,
        authorized_qty=0,
        status="AUTHORIZED",
        decision="AUTHORIZED",
        reason_codes=[],
    )
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
    )

    assert payload["decision"] == "NO"
    assert payload["canonical_blocker"] == "SIZE_NOT_PROVEN"


def test_trade_readiness_prioritizes_blockers_deterministically(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(
        truth_root,
        authority_granted=False,
        startup_status="FAIL",
        startup_codes=["OPTIONS_SNAPSHOT_ROOT_MISSING"],
    )
    _seed_readiness(execution_truth_root, reasons=["FAIL:NAV_INVALID", "FAIL:SIGNAL_FILTERED"])
    _seed_intent_authorization(execution_truth_root)
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload_a = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
    )
    payload_b = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
    )

    assert payload_a["canonical_blocker"] == "KILL_SWITCH_ACTIVE"
    assert payload_b["canonical_blocker"] == payload_a["canonical_blocker"]


def test_trade_readiness_fresh_submit_readiness_clears_submit_not_authorized(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(
        execution_truth_root,
        as_of_utc=f"{DAY}T12:00:00Z",
        expires_utc=f"{DAY}T12:15:00Z",
    )
    _seed_intent_authorization(execution_truth_root)
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        execution_build_ready_hint=True,
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )

    submit_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Submit Permission")
    assert payload["decision"] == "YES"
    assert payload["submit_allowed"] is True
    assert payload["canonical_blocker"] is None
    assert submit_gate["status"] == "PASS"
    assert "SUBMIT_NOT_AUTHORIZED" not in submit_gate["blockers"]


def test_trade_readiness_stale_submit_readiness_still_fails_closed(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(
        execution_truth_root,
        as_of_utc=f"{DAY}T12:00:00Z",
        expires_utc=f"{DAY}T12:01:00Z",
    )
    _seed_intent_authorization(execution_truth_root)
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )

    submit_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Submit Permission")
    assert payload["decision"] == "NO"
    assert payload["submit_allowed"] is False
    assert payload["canonical_blocker"] == "SUBMIT_NOT_AUTHORIZED"
    assert submit_gate["status"] == "FAIL"
    assert "SUBMIT_NOT_AUTHORIZED" in submit_gate["blockers"]
    assert "stale" in submit_gate["reason"].lower()


def test_trade_readiness_dry_run_pending_submit_without_broker_ids_does_not_block_broker_gate(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(execution_truth_root)
    _seed_execution_build(truth_root, submission_id="submission-dry-run")
    _seed_broker_submission_record(
        execution_truth_root,
        submission_id="submission-dry-run",
        status="PENDINGSUBMIT",
        order_id=None,
        perm_id=None,
        error_code="DRY_RUN_NO_BROKER_ID",
    )
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        execution_build_ready_hint=True,
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )

    broker_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Broker Submission Result")
    lifecycle_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Lifecycle & Outcome Tracking")
    assert payload["decision"] == "NO"
    assert payload["canonical_blocker"] == "BROKER_OUTCOME_NOT_RECONCILED"
    assert broker_gate["status"] == "PASS"
    assert "BROKER_SUBMIT_FAILED" not in broker_gate["blockers"]
    assert lifecycle_gate["status"] == "FAIL"
    assert "BROKER_OUTCOME_NOT_RECONCILED" in lifecycle_gate["blockers"]


def test_trade_readiness_submitted_cross_day_timestamp_does_not_force_day_mismatch(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(execution_truth_root)
    _seed_execution_build(truth_root, submission_id="submission-cross-day")
    _seed_broker_submission_record(
        execution_truth_root,
        submission_id="submission-cross-day",
        status="PRESUBMITTED",
        order_id=80,
        perm_id=25882942,
        submitted_at_utc="2026-04-24T00:10:00Z",
    )
    _seed_broker_outcome(
        execution_truth_root,
        submission_id="submission-cross-day",
        outcome_state="BROKER_ACCEPTED",
        status="PRESUBMITTED",
        order_id=80,
        perm_id=25882942,
        reason_codes=["BROKER_ACCEPTED_FROM_STATUS"],
    )
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        execution_build_ready_hint=True,
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )

    broker_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Broker Submission Result")
    assert payload["decision"] == "YES"
    assert payload["canonical_blocker"] is None
    assert broker_gate["status"] == "PASS"
    assert "BROKER_SUBMIT_FAILED" not in broker_gate["blockers"]
    assert "differs_from_submission_partition" in broker_gate["reason"]


def test_trade_readiness_submitted_without_broker_ids_still_fails_closed(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(execution_truth_root)
    _seed_execution_build(truth_root, submission_id="submission-invalid")
    _seed_broker_submission_record(
        execution_truth_root,
        submission_id="submission-invalid",
        status="SUBMITTED",
        order_id=None,
        perm_id=None,
    )
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        execution_build_ready_hint=True,
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )

    broker_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Broker Submission Result")
    assert payload["decision"] == "NO"
    assert payload["canonical_blocker"] == "BROKER_SUBMIT_FAILED"
    assert "BROKER_SUBMIT_FAILED" in payload["all_blockers"]
    assert "LIFECYCLE_NOT_TRACKED" in payload["all_blockers"]
    assert broker_gate["status"] == "FAIL"
    assert "BROKER_SUBMIT_FAILED" in broker_gate["blockers"]


def test_trade_readiness_order_ids_without_outcome_fails_reconciled(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(execution_truth_root)
    _seed_execution_build(truth_root, submission_id="submission-outcome-missing")
    _seed_broker_submission_record(
        execution_truth_root,
        submission_id="submission-outcome-missing",
        status="PRESUBMITTED",
        order_id=90,
        perm_id=764621016,
    )
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        execution_build_ready_hint=True,
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )

    broker_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Broker Submission Result")
    lifecycle_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Lifecycle & Outcome Tracking")
    assert broker_gate["status"] == "PASS"
    assert payload["decision"] == "NO"
    assert payload["canonical_blocker"] == "BROKER_OUTCOME_NOT_RECONCILED"
    assert lifecycle_gate["status"] == "FAIL"
    assert "BROKER_OUTCOME_NOT_RECONCILED" in lifecycle_gate["blockers"]


def test_trade_readiness_rejected_outcome_is_canonical_blocker(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(execution_truth_root)
    _seed_execution_build(truth_root, submission_id="submission-rejected")
    _seed_broker_submission_record(
        execution_truth_root,
        submission_id="submission-rejected",
        status="PRESUBMITTED",
        order_id=90,
        perm_id=764621016,
    )
    _seed_broker_outcome(
        execution_truth_root,
        submission_id="submission-rejected",
        outcome_state="BROKER_REJECTED",
        status="REJECTED",
        order_id=90,
        perm_id=764621016,
        reason_codes=["IB_ERROR_201_RISKLESS_COMBINATION", "BROKER_REJECTED"],
        error={"code": "IB_ERROR_201", "message": "Riskless combination orders are not allowed."},
    )
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        execution_build_ready_hint=True,
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )

    lifecycle_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Lifecycle & Outcome Tracking")
    assert payload["decision"] == "NO"
    assert payload["canonical_blocker"] == "BROKER_REJECTED"
    assert lifecycle_gate["status"] == "FAIL"
    assert "BROKER_REJECTED" in lifecycle_gate["blockers"]


def test_trade_readiness_historical_poisoned_record_is_not_canonical_blocker(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(execution_truth_root)
    _seed_execution_build(truth_root, submission_id="submission-old-poisoned")
    _seed_execution_build(truth_root, submission_id="submission-current-valid")
    _seed_broker_submission_record(
        execution_truth_root,
        submission_id="submission-old-poisoned",
        status="SUBMITTED",
        order_id=None,
        perm_id=None,
        submitted_at_utc=f"{DAY}T10:00:00Z",
    )
    _seed_broker_submission_record(
        execution_truth_root,
        submission_id="submission-current-valid",
        status="PRESUBMITTED",
        order_id=80,
        perm_id=25882942,
        submitted_at_utc=f"{DAY}T10:05:00Z",
    )
    _seed_broker_outcome(
        execution_truth_root,
        submission_id="submission-current-valid",
        outcome_state="BROKER_ACCEPTED",
        status="PRESUBMITTED",
        order_id=80,
        perm_id=25882942,
        reason_codes=["BROKER_ACCEPTED_FROM_STATUS"],
    )
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        execution_build_ready_hint=True,
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )

    broker_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Broker Submission Result")
    assert payload["decision"] == "YES"
    assert payload["canonical_blocker"] is None
    assert broker_gate["status"] == "PASS"
    assert "BROKER_SUBMIT_FAILED" not in broker_gate["blockers"]
    assert "submission-current-valid" in broker_gate["reason"]
    assert "ignored_historical_invalid_submission_ids=submission-old-poisoned" in broker_gate["reason"]
    assert any("submission-old-poisoned/broker_submission_record.v2.json" in path for path in payload["evidence_artifacts"])


def test_trade_readiness_missing_selected_current_record_fails_closed(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(execution_truth_root)
    _seed_execution_build(truth_root, submission_id="submission-missing-broker-record")
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )

    broker_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Broker Submission Result")
    assert payload["decision"] == "NO"
    assert payload["canonical_blocker"] == "BROKER_SUBMIT_FAILED"
    assert broker_gate["status"] == "FAIL"
    assert "No broker submission evidence found for expected current submission scope." in broker_gate["reason"]


def test_trade_readiness_presubmit_view_defers_broker_and_lifecycle_gates(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(execution_truth_root)
    _seed_execution_build(truth_root, submission_id="submission-missing-broker-record")
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        readiness_view="PRE_SUBMIT",
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )

    broker_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Broker Submission Result")
    lifecycle_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Lifecycle & Outcome Tracking")
    assert payload["decision"] == "YES"
    assert payload["canonical_blocker"] is None
    assert broker_gate["status"] == "NOT_ATTEMPTED"
    assert broker_gate["blockers"] == []
    assert lifecycle_gate["status"] == "NOT_ATTEMPTED"
    assert lifecycle_gate["blockers"] == []


def test_trade_readiness_presubmit_view_ignores_historical_broker_failure_as_blocker(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(execution_truth_root)
    _seed_execution_build(truth_root, submission_id="submission-invalid")
    _seed_broker_submission_record(
        execution_truth_root,
        submission_id="submission-invalid",
        status="SUBMITTED",
        order_id=None,
        perm_id=None,
    )
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        readiness_view="PRE_SUBMIT",
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )

    broker_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Broker Submission Result")
    assert payload["decision"] == "YES"
    assert payload["canonical_blocker"] is None
    assert broker_gate["status"] == "NOT_ATTEMPTED"
    assert "defers broker submission evidence" in broker_gate["reason"]
    assert "BROKER_SUBMIT_FAILED" not in payload["all_blockers"]


def test_trade_readiness_broker_selection_is_deterministic_with_multiple_records(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(execution_truth_root)
    _seed_execution_build(truth_root, submission_id="submission-A")
    _seed_execution_build(truth_root, submission_id="submission-B")
    _seed_broker_submission_record(
        execution_truth_root,
        submission_id="submission-A",
        status="PRESUBMITTED",
        order_id=70,
        perm_id=1001,
        submitted_at_utc=f"{DAY}T09:00:00Z",
    )
    _seed_broker_submission_record(
        execution_truth_root,
        submission_id="submission-B",
        status="PRESUBMITTED",
        order_id=80,
        perm_id=1002,
        submitted_at_utc=f"{DAY}T10:00:00Z",
    )
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload_a = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        execution_build_ready_hint=True,
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )
    payload_b = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        execution_build_ready_hint=True,
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )

    gate_a = next(row for row in payload_a["ordered_gate_results"] if row["gate"] == "Broker Submission Result")
    gate_b = next(row for row in payload_b["ordered_gate_results"] if row["gate"] == "Broker Submission Result")
    assert payload_a["canonical_blocker"] == payload_b["canonical_blocker"]
    assert gate_a["reason"] == gate_b["reason"]
    assert "submission-B" in gate_a["reason"]


def test_trade_readiness_pointer_selection_keeps_current_invalid_fail_closed(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    execution_truth_root = tmp_path / "execution_truth"
    _seed_common_truth(truth_root)
    _seed_readiness(execution_truth_root)
    _seed_intent_authorization(execution_truth_root)
    _seed_execution_build(truth_root, submission_id="submission-invalid-current")
    _seed_execution_build(truth_root, submission_id="submission-valid-older")
    _seed_broker_submission_record(
        execution_truth_root,
        submission_id="submission-valid-older",
        status="PRESUBMITTED",
        order_id=70,
        perm_id=1001,
        submitted_at_utc=f"{DAY}T09:00:00Z",
    )
    _seed_broker_submission_record(
        execution_truth_root,
        submission_id="submission-invalid-current",
        status="SUBMITTED",
        order_id=None,
        perm_id=None,
        submitted_at_utc=f"{DAY}T10:00:00Z",
    )
    _seed_submission_pointer(execution_truth_root, submission_id="submission-invalid-current")
    _patch_execution_root(monkeypatch, execution_truth_root)

    payload = reducer_module.build_trade_readiness_decision_payload_v1(
        repo_root=SOURCE_ROOT,
        truth_root=truth_root,
        day_utc=DAY,
        environment="PAPER",
        ib_account=ACCOUNT,
        execution_build_ready_hint=True,
        evaluation_time_utc=f"{DAY}T12:05:00Z",
    )

    broker_gate = next(row for row in payload["ordered_gate_results"] if row["gate"] == "Broker Submission Result")
    assert payload["decision"] == "NO"
    assert payload["canonical_blocker"] == "BROKER_SUBMIT_FAILED"
    assert broker_gate["status"] == "FAIL"
    assert "submission-invalid-current" in broker_gate["reason"]
    assert "lacks broker_ids" in broker_gate["reason"]


def test_trade_readiness_cli_fails_closed_when_jit_refresh_fails(tmp_path: Path, monkeypatch) -> None:
    truth_root = tmp_path / "truth"
    _seed_common_truth(truth_root)
    monkeypatch.setattr(
        reducer_tool_module,
        "resolve_decision_truth_root_bridge_v1",
        lambda *args, **kwargs: truth_root.resolve(),
    )
    monkeypatch.setattr(reducer_tool_module, "_refresh_trade_submit_readiness_artifact_v1", lambda **kwargs: 5)

    rc = reducer_tool_module.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(truth_root),
            "--environment",
            "PAPER",
            "--ib_account",
            ACCOUNT,
        ]
    )

    assert rc == 2
