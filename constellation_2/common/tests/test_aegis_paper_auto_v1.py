from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_aegis_paper_auto_v1 as auto


DAY = "2026-04-27"
INTENT_HASH = "6" * 64
SUBMISSION_ID = "7" * 64
OPEN_NOW_UTC = datetime(2026, 4, 27, 14, 0, 0, tzinfo=UTC)
AFTER_CLOSE_UTC = datetime(2026, 4, 28, 14, 0, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _default_aegis_submit_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        auto,
        "evaluate_submit_enforcement_v1",
        lambda **_kwargs: {"ok": True, "status": "PASS", "canonical_blocker": "", "blockers": []},
    )


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _seed_calendar(truth_root: Path, *, open_day: bool = True) -> None:
    _write_json(
        truth_root / "market_calendar_v1" / "dataset_manifest.json",
        {"files": [{"exchange": "NYSE", "file": "NYSE/2026.jsonl", "year": 2026}]},
    )
    (truth_root / "market_calendar_v1" / "NYSE").mkdir(parents=True, exist_ok=True)
    (truth_root / "market_calendar_v1" / "NYSE" / "2026.jsonl").write_text(
        json.dumps({"day_utc": DAY, "exchange": "NYSE", "is_trading_session": open_day}) + "\n",
        encoding="utf-8",
    )


def _seed_open_session(truth_root: Path) -> None:
    _seed_calendar(truth_root, open_day=True)
    _write_json(
        truth_root / "reports" / "paper_session_authority_v1" / DAY / "paper_session_authority.v1.json",
        {"authority_status": "GRANTED", "paper_open_allowed": True},
    )
    _write_json(
        truth_root / "active_session_v1" / "current.json",
        {"active_day": DAY, "rollover_status": "ACTIVE_SESSION_CONFIRMED"},
    )


def _seed_ready_authorities(truth_root: Path, execution_root: Path) -> None:
    _write_json(
        truth_root / "reports" / "paper_trading_day_authority_v1" / DAY / "paper_trading_day_authority.v1.json",
        {"state": "OPEN_READY", "can_submit_paper_orders": True},
    )
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {"status": "READY", "submission_authorized": True, "submit_allowed": True},
    )
    _write_json(
        truth_root / "reports" / "execution_mode_authority_v1" / DAY / "execution_mode_authority.v1.json",
        {"status": "PASS", "mode_state": "DRY_RUN_LOCKED", "broker_transmit_enabled": False},
    )
    _write_json(
        truth_root / "reports" / "runtime_service_authority_v1" / DAY / "runtime_service_authority.v1.json",
        {"status": "PASS", "auto_runner_running": True},
    )
    _write_json(
        execution_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json",
        {"state": "INACTIVE", "active": False},
    )


def _seed_candidate(execution_root: Path) -> Path:
    candidate = execution_root / "phaseC_preflight_v1" / DAY / "attempt_A0001" / INTENT_HASH
    _write_json(candidate / "execution_identity_record.v1.json", {"submission_id": SUBMISSION_ID})
    _write_json(candidate / "submit_preflight_decision.v1.json", {"decision": "ALLOW"})
    return candidate


def _seed_dry_run_complete(execution_root: Path) -> None:
    submission = execution_root / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
    _write_json(submission / "broker_submit_attempt_v1.json", {"submission_id": SUBMISSION_ID, "dry_run": True})
    _write_json(
        submission / "broker_submission_record.v2.json",
        {"submission_id": SUBMISSION_ID, "error": {"code": "DRY_RUN_NO_BROKER_ID"}, "broker_ids": {"order_id": None, "perm_id": None}},
    )


def _patch_lightweight_outputs(monkeypatch) -> None:
    monkeypatch.setattr(auto, "_refresh_operator_outputs", lambda **_: {})


def test_market_closed_no_submit_clean_market_closed(monkeypatch, tmp_path: Path) -> None:
    _patch_lightweight_outputs(monkeypatch)
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "execution"
    runtime_root = tmp_path / "runtime"
    _seed_calendar(truth_root, open_day=False)
    monkeypatch.setattr(auto, "_utc_now_dt", lambda: OPEN_NOW_UTC)
    calls: list[str] = []
    monkeypatch.setattr(auto, "_run_command", lambda name, cmd, env: calls.append(name) or {"name": name, "return_code": 0})

    result = auto._cycle(  # noqa: SLF001
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        runtime_root=runtime_root,
        mode="DRY_RUN",
        dry_run_only=True,
        poll_seconds=300,
        cycle_number=1,
        env={"C2_GOVERNED_SUBMIT_DRY_RUN": "YES"},
    )

    assert result["state"] == "MARKET_CLOSED"
    assert result["submit_attempted"] is False
    assert "aegis_paper_submit" not in calls


def test_market_open_ready_released_candidate_invokes_submit_path(monkeypatch, tmp_path: Path) -> None:
    _patch_lightweight_outputs(monkeypatch)
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "execution"
    runtime_root = tmp_path / "runtime"
    _seed_open_session(truth_root)
    _seed_ready_authorities(truth_root, execution_root)
    _seed_candidate(execution_root)
    monkeypatch.setattr(auto, "_utc_now_dt", lambda: OPEN_NOW_UTC)
    submit_envs: list[dict[str, str]] = []

    def fake_run(name: str, cmd: list[str], env: dict[str, str]) -> dict:
        if name == "aegis_paper_submit":
            submit_envs.append(dict(env))
            _seed_dry_run_complete(execution_root)
        return {"name": name, "return_code": 0}

    monkeypatch.setattr(auto, "_run_command", fake_run)
    result = auto._cycle(  # noqa: SLF001
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        runtime_root=runtime_root,
        mode="DRY_RUN",
        dry_run_only=True,
        poll_seconds=300,
        cycle_number=1,
        env={"C2_GOVERNED_SUBMIT_DRY_RUN": "YES"},
    )

    assert result["submit_attempted"] is True
    assert submit_envs[0]["C2_GOVERNED_SUBMIT_DRY_RUN"] == "YES"


def test_market_open_auto_runs_preflight_to_resolve_partial_target_day_before_submit(monkeypatch, tmp_path: Path) -> None:
    _patch_lightweight_outputs(monkeypatch)
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "execution"
    runtime_root = tmp_path / "runtime"
    _seed_open_session(truth_root)
    _seed_candidate(execution_root)
    _write_json(
        truth_root / "target_day_admission_v1" / f"{DAY}.json",
        {"target_day": DAY, "admission_status": "BLOCKED", "blocking_reason_codes": ["PARTIAL_BUILD"]},
    )
    _write_json(
        truth_root / "reports" / "paper_trading_day_authority_v1" / DAY / "paper_trading_day_authority.v1.json",
        {
            "state": "PREFLIGHT_BLOCKED",
            "can_submit_paper_orders": False,
            "canonical_blocker": "TARGET_DAY_ADMISSION_NOT_READY",
        },
    )
    monkeypatch.setattr(auto, "_utc_now_dt", lambda: OPEN_NOW_UTC)
    calls: list[str] = []

    def fake_run(name: str, cmd: list[str], env: dict[str, str]) -> dict:
        calls.append(name)
        if name == "aegis_paper_preflight":
            admission_before = json.loads(
                (truth_root / "target_day_admission_v1" / f"{DAY}.json").read_text()
            )
            assert admission_before["blocking_reason_codes"] == ["PARTIAL_BUILD"]
            _write_json(
                truth_root / "target_day_build_v1" / f"{DAY}.json",
                {
                    "target_day": DAY,
                    "build_status": "COMPLETE",
                    "completeness_result": "COMPLETE",
                    "closure_status": "CLOSED",
                },
            )
            _write_json(
                truth_root / "target_day_admission_v1" / f"{DAY}.json",
                {
                    "target_day": DAY,
                    "admission_status": "ADMIT",
                    "binding": True,
                    "closure_status": "CLOSED",
                    "blocking_reason_codes": [],
                },
            )
            _write_json(
                truth_root / "reports" / "session_promotion_decision_v1" / DAY / "session_promotion_decision.v1.json",
                {"target_day": DAY, "promotion_state": "PROMOTED", "blocked_reason_codes": []},
            )
            _seed_ready_authorities(truth_root, execution_root)
        if name == "aegis_paper_submit":
            _seed_dry_run_complete(execution_root)
        return {"name": name, "return_code": 0}

    monkeypatch.setattr(auto, "_run_command", fake_run)
    result = auto._cycle(  # noqa: SLF001
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        runtime_root=runtime_root,
        mode="DRY_RUN",
        dry_run_only=True,
        poll_seconds=300,
        cycle_number=1,
        env={"C2_GOVERNED_SUBMIT_DRY_RUN": "YES"},
    )

    assert calls.index("aegis_paper_preflight") < calls.index("aegis_paper_submit")
    assert result["submit_attempted"] is True
    admission_after = json.loads((truth_root / "target_day_admission_v1" / f"{DAY}.json").read_text())
    assert admission_after["admission_status"] == "ADMIT"


def test_market_open_existing_dry_run_complete_skips_duplicate(monkeypatch, tmp_path: Path) -> None:
    _patch_lightweight_outputs(monkeypatch)
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "execution"
    runtime_root = tmp_path / "runtime"
    _seed_open_session(truth_root)
    _seed_ready_authorities(truth_root, execution_root)
    _seed_candidate(execution_root)
    _seed_dry_run_complete(execution_root)
    monkeypatch.setattr(auto, "_utc_now_dt", lambda: OPEN_NOW_UTC)
    calls: list[str] = []
    monkeypatch.setattr(auto, "_run_command", lambda name, cmd, env: calls.append(name) or {"name": name, "return_code": 0})

    result = auto._cycle(  # noqa: SLF001
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        runtime_root=runtime_root,
        mode="DRY_RUN",
        dry_run_only=True,
        poll_seconds=300,
        cycle_number=1,
        env={"C2_GOVERNED_SUBMIT_DRY_RUN": "YES"},
    )

    assert result["submit_attempted"] is False
    assert result["skip_reason"] == "AUTO_SKIPPED_ALREADY_SUBMITTED"
    assert "aegis_paper_submit" not in calls
    assert result["duplicate_guard"]["duplicate"] is True


def test_market_open_duplicate_is_canonical_even_when_day_authority_stale(monkeypatch, tmp_path: Path) -> None:
    _patch_lightweight_outputs(monkeypatch)
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "execution"
    runtime_root = tmp_path / "runtime"
    _seed_open_session(truth_root)
    _seed_ready_authorities(truth_root, execution_root)
    _seed_candidate(execution_root)
    _seed_dry_run_complete(execution_root)
    _write_json(
        truth_root / "reports" / "paper_trading_day_authority_v1" / DAY / "paper_trading_day_authority.v1.json",
        {
            "state": "PREFLIGHT_REQUIRED",
            "can_submit_paper_orders": False,
            "canonical_blocker": "SESSION_AUTHORITY_STALE",
        },
    )
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {"status": "DRY_RUN_COMPLETE", "submission_authorized": False, "submit_allowed": False},
    )
    monkeypatch.setattr(auto, "_utc_now_dt", lambda: OPEN_NOW_UTC)
    calls: list[str] = []
    monkeypatch.setattr(auto, "_run_command", lambda name, cmd, env: calls.append(name) or {"name": name, "return_code": 0})

    result = auto._cycle(  # noqa: SLF001
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        runtime_root=runtime_root,
        mode="DRY_RUN",
        dry_run_only=True,
        poll_seconds=300,
        cycle_number=1,
        env={"C2_GOVERNED_SUBMIT_DRY_RUN": "YES"},
    )

    assert result["submit_attempted"] is False
    assert result["skip_reason"] == "AUTO_SKIPPED_ALREADY_SUBMITTED"
    assert result["latest_auto_cycle_outcome"] == "AUTO_SKIPPED_ALREADY_SUBMITTED"
    assert "aegis_paper_submit" not in calls


def test_kill_switch_active_no_submit(monkeypatch, tmp_path: Path) -> None:
    _patch_lightweight_outputs(monkeypatch)
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "execution"
    runtime_root = tmp_path / "runtime"
    _seed_open_session(truth_root)
    _seed_ready_authorities(truth_root, execution_root)
    _seed_candidate(execution_root)
    monkeypatch.setattr(auto, "_utc_now_dt", lambda: OPEN_NOW_UTC)
    _write_json(execution_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json", {"state": "ACTIVE", "active": True})
    calls: list[str] = []
    monkeypatch.setattr(auto, "_run_command", lambda name, cmd, env: calls.append(name) or {"name": name, "return_code": 0})

    result = auto._cycle(  # noqa: SLF001
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        runtime_root=runtime_root,
        mode="DRY_RUN",
        dry_run_only=True,
        poll_seconds=300,
        cycle_number=1,
        env={"C2_GOVERNED_SUBMIT_DRY_RUN": "YES"},
    )

    assert result["submit_attempted"] is False
    assert result["skip_reason"] == "GLOBAL_KILL_SWITCH_ACTIVE"
    assert "aegis_paper_submit" not in calls


def test_submit_boundary_blocked_no_submit(monkeypatch, tmp_path: Path) -> None:
    _patch_lightweight_outputs(monkeypatch)
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "execution"
    runtime_root = tmp_path / "runtime"
    _seed_open_session(truth_root)
    _seed_ready_authorities(truth_root, execution_root)
    _seed_candidate(execution_root)
    monkeypatch.setattr(auto, "_utc_now_dt", lambda: OPEN_NOW_UTC)
    _write_json(
        truth_root / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json",
        {"status": "BLOCKED", "submission_authorized": False},
    )
    calls: list[str] = []
    monkeypatch.setattr(auto, "_run_command", lambda name, cmd, env: calls.append(name) or {"name": name, "return_code": 0})

    result = auto._cycle(  # noqa: SLF001
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        runtime_root=runtime_root,
        mode="DRY_RUN",
        dry_run_only=True,
        poll_seconds=300,
        cycle_number=1,
        env={"C2_GOVERNED_SUBMIT_DRY_RUN": "YES"},
    )

    assert result["submit_attempted"] is False
    assert result["skip_reason"] == "SUBMIT_BOUNDARY_NOT_READY_AUTHORIZED"
    assert "aegis_paper_submit" not in calls


def test_auto_records_evidence_ledger_cycle(monkeypatch, tmp_path: Path) -> None:
    _patch_lightweight_outputs(monkeypatch)
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "execution"
    runtime_root = tmp_path / "runtime"
    _seed_calendar(truth_root, open_day=False)
    monkeypatch.setattr(auto, "_utc_now_dt", lambda: OPEN_NOW_UTC)
    monkeypatch.setattr(auto, "_run_command", lambda name, cmd, env: {"name": name, "return_code": 0})

    auto._cycle(  # noqa: SLF001
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        runtime_root=runtime_root,
        mode="DRY_RUN",
        dry_run_only=True,
        poll_seconds=300,
        cycle_number=1,
        env={"C2_GOVERNED_SUBMIT_DRY_RUN": "YES"},
    )
    ledger = json.loads((truth_root / "reports" / "aegis_day_evidence_ledger_v1" / DAY / "aegis_day_evidence_ledger.v1.json").read_text())
    assert ledger["cycles"][0]["cycle_id"] == f"AUTO-{DAY}-0001"
    assert ledger["cycles"][0]["submit_attempted"] is False


def test_market_closed_preserves_existing_dry_run_success(monkeypatch, tmp_path: Path) -> None:
    _patch_lightweight_outputs(monkeypatch)
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "execution"
    runtime_root = tmp_path / "runtime"
    _seed_calendar(truth_root, open_day=False)
    _seed_dry_run_complete(execution_root)
    monkeypatch.setattr(auto, "_utc_now_dt", lambda: OPEN_NOW_UTC)
    monkeypatch.setattr(auto, "_run_command", lambda name, cmd, env: {"name": name, "return_code": 0})

    result = auto._cycle(  # noqa: SLF001
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        runtime_root=runtime_root,
        mode="DRY_RUN",
        dry_run_only=True,
        poll_seconds=300,
        cycle_number=1,
        env={"C2_GOVERNED_SUBMIT_DRY_RUN": "YES"},
    )

    ledger = json.loads((truth_root / "reports" / "aegis_day_evidence_ledger_v1" / DAY / "aegis_day_evidence_ledger.v1.json").read_text())
    assert result["no_silent_day_outcome"] == "SUCCESS_DRY_RUN"
    assert ledger["final_daily_outcome"] == "SUCCESS_DRY_RUN"


def test_auto_does_not_enable_broker_transmit_by_default(monkeypatch, tmp_path: Path) -> None:
    _patch_lightweight_outputs(monkeypatch)
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "execution"
    runtime_root = tmp_path / "runtime"
    _seed_open_session(truth_root)
    _seed_ready_authorities(truth_root, execution_root)
    _seed_candidate(execution_root)
    monkeypatch.setattr(auto, "_utc_now_dt", lambda: OPEN_NOW_UTC)
    submit_envs: list[dict[str, str]] = []

    def fake_run(name: str, cmd: list[str], env: dict[str, str]) -> dict:
        if name == "aegis_paper_submit":
            submit_envs.append(dict(env))
            _seed_dry_run_complete(execution_root)
        return {"name": name, "return_code": 0}

    monkeypatch.setattr(auto, "_run_command", fake_run)
    auto._cycle(  # noqa: SLF001
        day_utc=DAY,
        truth_root=truth_root,
        execution_root=execution_root,
        runtime_root=runtime_root,
        mode="DRY_RUN",
        dry_run_only=True,
        poll_seconds=300,
        cycle_number=1,
        env={"C2_GOVERNED_SUBMIT_DRY_RUN": "YES"},
    )
    assert submit_envs
    assert submit_envs[0]["C2_GOVERNED_SUBMIT_DRY_RUN"] == "YES"


def test_current_et_time_during_regular_session_is_market_open(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_calendar(truth_root, open_day=True)

    observed = auto._market_session_state(truth_root=truth_root, day_utc=DAY, now_utc=OPEN_NOW_UTC)  # noqa: SLF001

    assert observed["market_session_state"] == "MARKET_OPEN"
    assert observed["reason"] == "REGULAR_SESSION_OPEN"
    assert observed["current_time_et"].startswith("2026-04-27T10:00:00")
    assert observed["session_open_et"].startswith("2026-04-27T09:30:00")
    assert observed["session_close_et"].startswith("2026-04-27T16:00:00")


def test_prior_day_after_close_is_market_closed(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_calendar(truth_root, open_day=True)

    observed = auto._market_session_state(truth_root=truth_root, day_utc=DAY, now_utc=AFTER_CLOSE_UTC)  # noqa: SLF001

    assert observed["market_session_state"] == "MARKET_CLOSED"
    assert observed["reason"] == "AFTER_REGULAR_SESSION_CLOSE"


def test_explicit_prior_day_does_not_pretend_market_is_open(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _seed_calendar(truth_root, open_day=True)

    observed = auto._market_session_state(truth_root=truth_root, day_utc=DAY, now_utc=AFTER_CLOSE_UTC)  # noqa: SLF001

    assert observed["evaluated_day_utc"] == DAY
    assert observed["market_session_state"] == "MARKET_CLOSED"


def test_omitted_day_utc_resolves_to_current_et_day(monkeypatch, tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    runtime_root = tmp_path / "runtime"
    execution_root = tmp_path / "execution"
    captured: dict[str, str] = {}

    monkeypatch.setenv("AEGIS_RUN_STYLE", "AUTO")
    monkeypatch.setenv("AEGIS_AUTO_MAX_CYCLES", "1")
    monkeypatch.setattr(auto, "require_authoritative_repo_runtime_v1", lambda repo: None)
    monkeypatch.setattr(auto, "resolve_decision_truth_root_bridge_v1", lambda *a, **k: truth_root)
    monkeypatch.setattr(auto, "resolve_single_paper_ib_account_from_sleeve_registry", lambda repo: "DUO847203")
    monkeypatch.setattr(
        auto,
        "resolve_sleeve_execution_root_v1",
        lambda **k: type("Root", (), {"execution_root_path": execution_root})(),
    )
    monkeypatch.setattr(auto, "register_auto_runner_v1", lambda **k: runtime_root / "process_state" / "service_status.json")
    monkeypatch.setattr(auto, "_utc_now_dt", lambda: datetime(2026, 4, 28, 14, 7, 0, tzinfo=UTC))

    def fake_cycle(**kwargs):
        captured["day_utc"] = kwargs["day_utc"]
        return {
            "state": "MARKET_CLOSED",
            "market_session_state": "MARKET_CLOSED",
            "evaluated_day_utc": kwargs["day_utc"],
            "submit_attempted": False,
            "submit_eligibility_evaluated": False,
            "actions": [],
            "ledger_path": "",
            "no_silent_day_outcome": "NO_INTENT_EXPECTED",
        }

    monkeypatch.setattr(auto, "_cycle", fake_cycle)

    rc = auto.main(["--runtime_root", str(runtime_root)])

    assert rc == 0
    assert captured["day_utc"] == "2026-04-28"
