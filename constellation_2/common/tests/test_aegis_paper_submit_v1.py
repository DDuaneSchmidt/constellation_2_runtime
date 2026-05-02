from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_aegis_paper_submit_v1 as submit_tool
import ops.tools.run_trade_submit_readiness_c2_v1 as trade_readiness_tool


DAY = "2026-04-27"
INTENT_HASH = "6" * 64
SUBMISSION_ID = "7" * 64


@pytest.fixture(autouse=True)
def _default_aegis_submit_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        submit_tool,
        "evaluate_submit_enforcement_v1",
        lambda **_kwargs: {"ok": True, "status": "PASS", "canonical_blocker": "", "blockers": []},
    )


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _seed_candidate(execution_root: Path) -> Path:
    candidate = execution_root / "phaseC_preflight_v1" / DAY / "attempt_A0001" / INTENT_HASH
    _write_json(candidate / "execution_identity_record.v1.json", {"schema_id": "execution_identity_record", "submission_id": SUBMISSION_ID})
    _write_json(candidate / "submit_preflight_decision.v1.json", {"schema_id": "submit_preflight_decision", "decision": "ALLOW"})
    _write_json(candidate / "order_plan.v1.json", {"schema_id": "order_plan", "schema_version": "v1", "intent_hash": INTENT_HASH})
    _write_json(candidate / "mapping_ledger_record.v1.json", {"schema_id": "mapping_ledger_record", "schema_version": "v1"})
    _write_json(candidate / "binding_record.v1.json", {"schema_id": "binding_record", "schema_version": "v1", "submission_id": SUBMISSION_ID})
    return candidate


def _seed_ready_roots(tmp_path: Path) -> tuple[submit_tool.SubmitContext, Path]:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _write_json(
        truth_root / "reports" / "paper_trading_day_authority_v1" / DAY / "paper_trading_day_authority.v1.json",
        {"schema_id": "paper_trading_day_authority", "state": "OPEN_READY", "can_submit_paper_orders": True},
    )
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {"schema_id": "submit_boundary_status", "status": "READY", "submission_authorized": True, "submit_allowed": True},
    )
    _write_json(
        execution_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        {"schema_id": "global_kill_switch_state", "state": "INACTIVE", "reason_codes": []},
    )
    ledger = truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json"
    _write_json(ledger, {"schema_id": "paper_session_ledger", "day_utc": DAY})
    candidate = _seed_candidate(execution_root)
    ctx = submit_tool.SubmitContext(
        day_utc=DAY,
        produced_utc=f"{DAY}T14:30:00Z",
        truth_root=truth_root,
        execution_root=execution_root,
        ib_account="DUO847203",
        paper_session_ledger_path=ledger,
        dry_run_policy="YES",
        env={"C2_GOVERNED_SUBMIT_DRY_RUN": "YES"},
    )
    return ctx, candidate


def test_open_ready_phasec_release_invokes_submit_creator(tmp_path: Path) -> None:
    ctx, _candidate = _seed_ready_roots(tmp_path)
    calls: list[list[str]] = []

    def runner(cmd: list[str], env: dict[str, str]) -> dict:
        calls.append(cmd)
        return {"cmd": cmd, "return_code": 0, "stdout": "", "stderr": ""}

    with patch.object(submit_tool.orchestrator_v2, "_run_governed_submit_stage", return_value=(0, ["GOV_SUBMIT_SUBMISSION_COUNT=1"])) as run_submit:
        report = submit_tool.run_aegis_paper_submit_v1(ctx, runner=runner)

    assert report["status"] == "PASS"
    assert any("run_authorization_artifacts_day_v1.py" in cmd for call in calls for cmd in call)
    assert any("run_trade_submit_readiness_c2_v1.py" in cmd for call in calls for cmd in call)
    assert any("run_execution_build_authority_v1.py" in cmd for call in calls for cmd in call)
    assert run_submit.call_count == 1


def test_submit_creator_refuses_when_day_authority_not_open_ready(tmp_path: Path) -> None:
    ctx, _candidate = _seed_ready_roots(tmp_path)
    _write_json(
        ctx.truth_root / "reports" / "paper_trading_day_authority_v1" / DAY / "paper_trading_day_authority.v1.json",
        {"schema_id": "paper_trading_day_authority", "state": "PREFLIGHT_BLOCKED", "can_submit_paper_orders": False},
    )

    with patch.object(submit_tool.orchestrator_v2, "_run_governed_submit_stage") as run_submit:
        report = submit_tool.run_aegis_paper_submit_v1(ctx, runner=lambda *_: {"return_code": 0})

    assert report["status"] == "BLOCKED"
    assert "PAPER_DAY_AUTHORITY_NOT_OPEN_READY" in report["reason_codes"]
    run_submit.assert_not_called()


def test_submit_creator_refuses_when_kill_switch_active(tmp_path: Path) -> None:
    ctx, _candidate = _seed_ready_roots(tmp_path)
    _write_json(
        ctx.execution_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        {"schema_id": "global_kill_switch_state", "state": "ACTIVE", "active": True},
    )

    report = submit_tool.run_aegis_paper_submit_v1(ctx, runner=lambda *_: {"return_code": 0})

    assert report["status"] == "BLOCKED"
    assert "GLOBAL_KILL_SWITCH_ACTIVE" in report["reason_codes"]


def test_submit_creator_refuses_when_submit_boundary_not_ready_authorized(tmp_path: Path) -> None:
    ctx, _candidate = _seed_ready_roots(tmp_path)
    _write_json(
        ctx.truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {"schema_id": "submit_boundary_status", "status": "NOT_READY", "submission_authorized": False},
    )

    report = submit_tool.run_aegis_paper_submit_v1(ctx, runner=lambda *_: {"return_code": 0})

    assert report["status"] == "BLOCKED"
    assert "SUBMIT_BOUNDARY_NOT_READY_AUTHORIZED" in report["reason_codes"]


def test_submit_creator_refuses_when_shared_aegis_submit_gate_blocks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ctx, _candidate = _seed_ready_roots(tmp_path)
    monkeypatch.setattr(
        submit_tool,
        "evaluate_submit_enforcement_v1",
        lambda **_kwargs: {
            "ok": False,
            "status": "BLOCKED",
            "canonical_blocker": "ACTION_VALIDITY_FORBIDS_SUBMIT",
            "blockers": [{"code": "ACTION_VALIDITY_FORBIDS_SUBMIT"}],
        },
    )

    with patch.object(submit_tool.orchestrator_v2, "_run_governed_submit_stage") as run_submit:
        report = submit_tool.run_aegis_paper_submit_v1(ctx, runner=lambda *_: {"return_code": 0})

    assert report["status"] == "BLOCKED"
    assert "ACTION_VALIDITY_FORBIDS_SUBMIT" in report["reason_codes"]
    run_submit.assert_not_called()


def test_released_candidate_produces_authorization_build_and_package_evidence(tmp_path: Path) -> None:
    ctx, candidate = _seed_ready_roots(tmp_path)

    def runner(cmd: list[str], env: dict[str, str]) -> dict:
        joined = " ".join(cmd)
        if "run_authorization_artifacts_day_v1.py" in joined:
            _write_json(
                ctx.execution_root / "engine_activity_v1" / "authorization_v1" / DAY / f"{INTENT_HASH}.authorization.v1.json",
                {"schema_id": "authorization", "status": "AUTHORIZED", "authorization": {"decision": "AUTHORIZED", "authorized_quantity": 1}},
            )
        if "run_execution_build_authority_v1.py" in joined:
            _write_json(
                ctx.truth_root / "reports" / "execution_build_v1" / DAY / SUBMISSION_ID / "execution_build.v1.json",
                {"schema_id": "execution_build", "status": "PASS"},
            )
            _write_json(
                ctx.execution_root / "execution_package_v1" / DAY / SUBMISSION_ID / "execution_package.v1.json",
                {"schema_id": "execution_package", "status": "PASS"},
            )
        return {"cmd": cmd, "return_code": 0, "stdout": "", "stderr": ""}

    with patch.object(submit_tool.orchestrator_v2, "_run_governed_submit_stage", return_value=(0, ["GOV_SUBMIT_SUBMISSION_COUNT=1"])):
        report = submit_tool.run_aegis_paper_submit_v1(ctx, runner=runner)

    evidence = report["evidence"]
    assert evidence["engine_activity_authorization_v1"][0]["exists"] is True
    assert evidence["execution_build_v1"][0]["exists"] is True
    assert evidence["execution_package_v1"][0]["exists"] is True
    assert str(candidate) in report["guards"]["released_candidate_paths"]


def test_dry_run_policy_does_not_create_false_broker_submission_claims(tmp_path: Path) -> None:
    ctx, _candidate = _seed_ready_roots(tmp_path)

    with patch.object(submit_tool.orchestrator_v2, "_run_governed_submit_stage", return_value=(0, ["GOV_SUBMIT_SUBMISSION_COUNT=1"])):
        report = submit_tool.run_aegis_paper_submit_v1(
            ctx,
            runner=lambda cmd, env: {"cmd": cmd, "return_code": 0, "stdout": "", "stderr": ""},
        )

    assert report["dry_run_policy"] == "YES"
    assert report["broker_transmit_enabled"] is False
    assert report["evidence"]["execution_evidence_submissions"]["broker_submission_record_count"] == 0


def test_existing_dry_run_submit_reports_current_guard_block(tmp_path: Path) -> None:
    ctx, _candidate = _seed_ready_roots(tmp_path)
    submission_dir = ctx.execution_root / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
    _write_json(
        submission_dir / "broker_submission_record.v2.json",
        {
            "submission_id": SUBMISSION_ID,
            "status": "PENDINGSUBMIT",
            "broker_ids": {"order_id": None, "perm_id": None},
            "error": {"code": "DRY_RUN_NO_BROKER_ID"},
        },
    )
    _write_json(
        submission_dir / "broker_submit_attempt_v1.json",
        {"submission_id": SUBMISSION_ID, "dry_run": True, "reason_codes": ["DRY_RUN_SUBMIT_ATTEMPT"]},
    )
    _write_json(
        ctx.truth_root / "reports" / "paper_trading_day_authority_v1" / DAY / "paper_trading_day_authority.v1.json",
        {"schema_id": "paper_trading_day_authority", "state": "SUBMITTING", "can_submit_paper_orders": False},
    )
    _write_json(
        ctx.truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {"schema_id": "submit_boundary_status", "status": "DRY_RUN_COMPLETE", "submission_authorized": False},
    )

    with patch.object(submit_tool.orchestrator_v2, "_run_governed_submit_stage") as run_submit:
        report = submit_tool.run_aegis_paper_submit_v1(
            ctx,
            runner=lambda cmd, env: {"cmd": cmd, "return_code": 0, "stdout": "", "stderr": ""},
        )

    assert report["status"] == "BLOCKED"
    assert report["submit_mode_status"] == "DRY_RUN_COMPLETE"
    assert report["historical_dry_run_completed"] is True
    assert report["current_submit_guard_status"] == "BLOCKED"
    assert "PAPER_DAY_AUTHORITY_NOT_OPEN_READY" in report["current_guard_failure_reasons"]
    assert report["broker_transmit_enabled"] is False
    assert report["missing_broker_ids_blocker"] is False
    assert report["missing_broker_ids_diagnostic"] is True
    run_submit.assert_not_called()


def test_existing_dry_run_submit_does_not_mask_control_plane_not_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ctx, _candidate = _seed_ready_roots(tmp_path)
    submission_dir = ctx.execution_root / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
    _write_json(
        submission_dir / "broker_submission_record.v2.json",
        {
            "submission_id": SUBMISSION_ID,
            "status": "PENDINGSUBMIT",
            "broker_ids": {"order_id": None, "perm_id": None},
            "error": {"code": "DRY_RUN_NO_BROKER_ID"},
        },
    )
    _write_json(submission_dir / "broker_submit_attempt_v1.json", {"submission_id": SUBMISSION_ID, "dry_run": True})
    monkeypatch.setattr(
        submit_tool,
        "evaluate_submit_enforcement_v1",
        lambda **_kwargs: {
            "ok": False,
            "status": "BLOCKED",
            "canonical_blocker": "CONTROL_PLANE_NOT_READY",
            "blockers": [{"code": "CONTROL_PLANE_NOT_READY"}],
        },
    )

    with patch.object(submit_tool.orchestrator_v2, "_run_governed_submit_stage") as run_submit:
        report = submit_tool.run_aegis_paper_submit_v1(ctx, runner=lambda *_: {"return_code": 0})

    assert report["status"] == "BLOCKED"
    assert report["submit_mode_status"] == "DRY_RUN_COMPLETE"
    assert report["historical_dry_run_completed"] is True
    assert report["current_submit_guard_status"] == "BLOCKED"
    assert "CONTROL_PLANE_NOT_READY" in report["current_guard_failure_reasons"]
    assert "CONTROL_PLANE_NOT_READY" in report["reason_codes"]
    run_submit.assert_not_called()


def test_submission_evidence_is_only_claimed_when_artifact_exists(tmp_path: Path) -> None:
    ctx, candidate = _seed_ready_roots(tmp_path)
    empty = submit_tool.collect_submit_evidence_v1(ctx, [candidate])
    assert empty["execution_evidence_submissions"]["broker_submission_record_count"] == 0

    _write_json(
        ctx.execution_root / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID / "broker_submission_record.v2.json",
        {"schema_id": "broker_submission_record", "schema_version": "v2", "status": "SUBMITTED", "broker_ids": {"order_id": None}},
    )
    observed = submit_tool.collect_submit_evidence_v1(ctx, [candidate])
    assert observed["execution_evidence_submissions"]["broker_submission_record_count"] == 1


def test_trade_submit_readiness_uses_previous_trading_day_for_monday(monkeypatch) -> None:
    def fake_calendar(*, truth_root: Path, day_utc: str) -> dict:
        return {
            "status": "OK",
            "record": {"day_utc": day_utc, "is_trading_session": day_utc == "2026-04-24"},
        }

    monkeypatch.setattr(trade_readiness_tool, "resolve_market_calendar_record_v1", fake_calendar)

    assert trade_readiness_tool._prior_trading_day_utc("2026-04-27") == "2026-04-24"  # noqa: SLF001
