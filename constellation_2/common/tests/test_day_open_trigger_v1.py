from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path("/home/node/constellation")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common import day_open_trigger_v1 as trigger_module


DAY = "2026-04-14"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _authority_surfaces(
    truth_root: Path,
    *,
    active_day: str = DAY,
    rollover_status: str = "ACTIVE_SESSION_CONFIRMED",
    admission_status: str = "ADMIT",
    authority_status: str = "GRANTED",
    paper_open_allowed: bool | None = None,
    ledger_authority_status: str | None = None,
) -> None:
    allowed = authority_status == "GRANTED" if paper_open_allowed is None else bool(paper_open_allowed)
    blocker_codes = [] if allowed else ["PAPER_CAPITAL_SEED_MISSING"]
    safety_status = "PASS" if allowed else "FAIL"
    reason_code = "" if allowed else blocker_codes[0]
    ledger_status = authority_status if ledger_authority_status is None else ledger_authority_status
    _write_json(
        truth_root / "active_session_v1" / "current.json",
        {"active_day": active_day, "rollover_status": rollover_status},
    )
    _write_json(
        truth_root / "target_day_admission_v1" / f"{DAY}.json",
        {"admission_status": admission_status},
    )
    _write_json(
        truth_root / "session_authority_status_v1" / "current.json",
        {"status_severity": "WARNING"},
    )
    _write_json(
        truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json",
        {"control_state": {"authority_status": ledger_status}},
    )
    _write_json(
        truth_root / "reports" / "paper_session_authority_v1" / DAY / "paper_session_authority.v1.json",
        {
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
                        "check_id": "PAPER_CAPITAL_SEED_READY",
                        "summary": "MISSING",
                        "artifact_path": str(truth_root / "paper_capital_seed_v1" / f"{DAY}.json"),
                    }
                ]
            ),
            "safety_checks": [
                {
                    "check_id": "PAPER_CAPITAL_SEED_READY",
                    "status": safety_status,
                    "reason_code": reason_code,
                    "summary": "OK" if allowed else "MISSING",
                    "artifact_path": str(truth_root / "paper_capital_seed_v1" / f"{DAY}.json"),
                },
                {
                    "check_id": "OPERATOR_STATEMENT_READY",
                    "status": "PASS",
                    "reason_code": "",
                    "summary": "OK",
                    "artifact_path": str(truth_root / "operator_statements" / DAY / "operator_statement.json"),
                },
                {
                    "check_id": "PRE_OPEN_BUNDLE_COMPLETE",
                    "status": "PASS",
                    "reason_code": "",
                    "summary": "COMPLETE",
                    "artifact_path": str(truth_root / "reports" / "pre_open_bundle_v1" / DAY / "pre_open_bundle.v1.json"),
                },
                {
                    "check_id": "CANONICAL_KILL_SWITCH_PRESENT",
                    "status": "PASS",
                    "reason_code": "",
                    "summary": "PRESENT",
                    "artifact_path": str(truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json"),
                },
                {
                    "check_id": "CANONICAL_KILL_SWITCH_INACTIVE",
                    "status": "PASS",
                    "reason_code": "",
                    "summary": "INACTIVE",
                    "artifact_path": str(truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json"),
                },
            ],
            "advisory_checks": [],
            "degraded_mode": False,
            "submission_authorized": False,
            "upstream_refs": {
                "paper_session_bootstrap_v1": str(
                    truth_root / "reports" / "paper_session_bootstrap_v1" / DAY / "paper_session_bootstrap.v1.json"
                ),
                "paper_capital_seed": str(truth_root / "paper_capital_seed_v1" / f"{DAY}.json"),
                "operator_statement": str(truth_root / "operator_statements" / DAY / "operator_statement.json"),
                "pre_open_bundle_v1": str(truth_root / "reports" / "pre_open_bundle_v1" / DAY / "pre_open_bundle.v1.json"),
                "canonical_kill_switch_v1": str(
                    truth_root / "risk_v1" / "kill_switch_v1" / DAY / "global_kill_switch_state.v1.json"
                ),
            },
            "producer": {"repo": "constellation", "module": "test", "git_sha": "a" * 40},
        },
    )


def _attempt_payload(sequence: int = 1) -> dict:
    return {
        "schema_id": "day_open_attempt",
        "schema_version": "v1",
        "day_utc": DAY,
        "attempt_sequence": sequence,
        "trigger_kind": "INITIAL_BOD_TRIGGER",
        "started_at_utc": f"{DAY}T13:31:00Z",
        "finished_at_utc": f"{DAY}T13:31:05Z",
        "trigger_ref": "/tmp/day_open_trigger.v1.json",
        "consumed_trigger_key": f"{DAY}:INITIAL_BOD_TRIGGER:{sequence}",
        "actor_identity": {"actor_name": "test", "actor_path": "/tmp/test"},
        "open_command_executed": True,
        "submit_stage_entered": True,
        "submit_stage_owner": "run_c2_paper_day_orchestrator_v2.py",
        "result_code": "ORCHESTRATOR_RC_0",
        "reason_codes": ["ORCHESTRATOR_RC=0"],
        "consumed_authority_refs": {
            "paper_session_ledger_v1": "/tmp/paper_session_ledger.v1.json",
            "day_open_trigger_v1": "/tmp/day_open_trigger.v1.json",
        },
        "direct_evidence_refs": {
            "sleeve_rollup_v1": "/tmp/sleeve_rollup.v1.json",
            "orchestrator_run_verdict_v2": ["/tmp/verdict.json"],
            "run_pointer_v1": ["/tmp/pointer.jsonl"],
        },
        "final_classification": "OPEN_SUCCEEDED",
        "attempt_history": [],
        "environment": "PAPER",
        "open_policy": {
            "policy_mode": "PAPER_READY_WHEN_GRANTED_UNBOUNDED_SAME_DAY",
            "environment": "PAPER",
            "max_successful_opens_per_day": 0,
            "max_late_open_attempts_per_day": 0,
            "same_day_open_cap_enforced": False,
            "time_window_enforced": False,
            "successful_open_already_recorded": True,
            "prior_success_ref": "/tmp/day_open_attempt.v1.json",
            "initial_open_consumed": True,
            "late_open_available": False,
            "late_open_consumed": False,
            "late_open_consumed_ref": "",
            "open_terminal": False,
            "terminal_reason_code": "",
            "policy_status": "PAPER_REPEAT_OPEN_ALLOWED_WHEN_GRANTED",
            "attempt_count": 1,
            "latest_attempt_kind": "INITIAL_BOD_TRIGGER",
            "latest_attempt_result": "OPEN_SUCCEEDED",
            "prior_trigger_kind": "INITIAL_BOD_TRIGGER",
            "prior_trigger_status": "EMITTED",
        },
        "producer": {"repo": "constellation", "module": "test", "git_sha": "a" * 40},
    }


def _window(status: str) -> SimpleNamespace:
    return SimpleNamespace(
        day_utc=DAY,
        timezone="America/New_York",
        bod_time_et=f"{DAY}T09:31:00-04:00",
        cutoff_time_et=f"{DAY}T09:45:00-04:00",
        bod_time_utc=f"{DAY}T13:31:00Z",
        cutoff_time_utc=f"{DAY}T13:45:00Z",
        window_status=status,
        source_timer_path="/tmp/c2-paper-day-orchestrator.timer",
        cutoff_timer_path="/tmp/c2-global-monitoring-refresh.timer",
    )


def test_day_open_trigger_suppresses_without_authority(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _authority_surfaces(truth_root, authority_status="DENIED")

    with patch.object(trigger_module, "build_day_open_window_v1", return_value=_window("OPEN_WINDOW")):
        payload = trigger_module.build_day_open_trigger_payload(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            environment="PAPER",
        )

    assert payload["trigger_status"] == "SUPPRESSED_AUTHORITY_NOT_GRANTED"
    assert payload["trigger_reason_code"] == "OPEN_TRIGGER_AUTHORITY_NOT_GRANTED"


def test_day_open_trigger_emits_for_paper_even_late_in_day(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _authority_surfaces(truth_root)

    with patch.object(trigger_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")):
        payload = trigger_module.build_day_open_trigger_payload(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            environment="PAPER",
        )

    assert payload["trigger_status"] == "EMITTED"
    assert payload["trigger_reason_code"] == "OPEN_TRIGGER_EMITTED_PAPER_READY"
    assert payload["trigger_sequence"] == 1
    assert payload["open_policy"]["time_window_enforced"] is False


def test_day_open_trigger_paper_ignores_admission_and_active_session_when_authority_granted(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _authority_surfaces(
        truth_root,
        active_day="2026-04-13",
        rollover_status="ROLLOVER_WITHHELD",
        admission_status="BLOCKED",
        authority_status="GRANTED",
    )

    with patch.object(trigger_module, "build_day_open_window_v1", return_value=_window("OPEN_WINDOW")):
        payload = trigger_module.build_day_open_trigger_payload(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            environment="PAPER",
        )

    assert payload["trigger_status"] == "EMITTED"
    assert payload["trigger_reason_code"] == "OPEN_TRIGGER_EMITTED_PAPER_READY"


def test_day_open_trigger_paper_uses_canonical_paper_authority_not_ledger_status(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _authority_surfaces(
        truth_root,
        authority_status="GRANTED",
        paper_open_allowed=True,
        ledger_authority_status="DENIED",
    )

    with patch.object(trigger_module, "build_day_open_window_v1", return_value=_window("OPEN_WINDOW")):
        payload = trigger_module.build_day_open_trigger_payload(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            environment="PAPER",
        )

    assert payload["trigger_status"] == "EMITTED"
    assert payload["trigger_reason_code"] == "OPEN_TRIGGER_EMITTED_PAPER_READY"
    assert payload["source_authority_refs"]["paper_session_authority_v1"].endswith(
        f"/reports/paper_session_authority_v1/{DAY}/paper_session_authority.v1.json"
    )


def test_day_open_trigger_allows_repeat_paper_open_after_prior_success(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _authority_surfaces(truth_root)
    _write_json(
        truth_root / "reports" / "day_open_attempt_v1" / DAY / "day_open_attempt.v1.json",
        _attempt_payload(),
    )
    _write_json(
        truth_root / "reports" / "day_open_trigger_v1" / DAY / "day_open_trigger.v1.json",
        {
            "schema_id": "day_open_trigger",
            "schema_version": "v1",
            "day_utc": DAY,
            "trigger_status": "EMITTED",
            "emitted_at_utc": f"{DAY}T13:31:00Z",
            "trigger_reason_code": "OPEN_TRIGGER_EMITTED_PAPER_READY",
            "trigger_kind": "INITIAL_BOD_TRIGGER",
            "environment": "PAPER",
            "open_policy": _attempt_payload()["open_policy"],
            "source_authority_refs": {
                "active_session_v1": str(truth_root / "active_session_v1" / "current.json"),
                "target_day_admission_v1": str(truth_root / "target_day_admission_v1" / f"{DAY}.json"),
                "session_authority_status_v1": str(truth_root / "session_authority_status_v1" / "current.json"),
                "paper_session_ledger_v1": str(truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json"),
                "paper_session_authority_v1": str(
                    truth_root / "reports" / "paper_session_authority_v1" / DAY / "paper_session_authority.v1.json"
                ),
            },
            "open_window_status": "OPEN_WINDOW",
            "open_window": {
                "timezone": "America/New_York",
                "bod_time_et": f"{DAY}T09:31:00-04:00",
                "cutoff_time_et": f"{DAY}T09:45:00-04:00",
                "bod_time_utc": f"{DAY}T13:31:00Z",
                "cutoff_time_utc": f"{DAY}T13:45:00Z",
                "source_timer_path": "/tmp/c2-paper-day-orchestrator.timer",
                "cutoff_timer_path": "/tmp/c2-global-monitoring-refresh.timer",
            },
            "dedupe_key": f"{DAY}:INITIAL_BOD_TRIGGER:1",
            "trigger_sequence": 1,
            "trigger_history": [],
            "consumed": True,
            "consumed_at_utc": f"{DAY}T13:31:01Z",
            "consumed_by": {"actor_name": "test", "actor_path": "/tmp/test"},
            "expires_at_utc": f"{DAY}T13:45:00Z",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "a" * 40},
        },
    )

    with patch.object(trigger_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")):
        payload = trigger_module.build_day_open_trigger_payload(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            environment="PAPER",
        )

    assert payload["trigger_status"] == "EMITTED"
    assert payload["trigger_reason_code"] == "OPEN_TRIGGER_EMITTED_PAPER_READY_REPEAT"
    assert payload["trigger_sequence"] == 2
    assert payload["trigger_history"][0]["trigger_sequence"] == 1
    assert payload["open_policy"]["successful_open_already_recorded"] is True


def test_day_open_trigger_live_remains_window_bounded(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _authority_surfaces(truth_root)

    with patch.object(trigger_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")):
        payload = trigger_module.build_day_open_trigger_payload(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            environment="LIVE",
        )

    assert payload["trigger_status"] == "SUPPRESSED_OUTSIDE_OPEN_WINDOW"
    assert payload["trigger_reason_code"] == "OPEN_TRIGGER_OPEN_WINDOW_EXPIRED"


def test_day_open_trigger_live_still_requires_admission_and_active_session(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _authority_surfaces(
        truth_root,
        active_day="2026-04-13",
        rollover_status="ROLLOVER_WITHHELD",
        admission_status="BLOCKED",
        authority_status="GRANTED",
    )

    with patch.object(trigger_module, "build_day_open_window_v1", return_value=_window("OPEN_WINDOW")):
        payload = trigger_module.build_day_open_trigger_payload(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            environment="LIVE",
        )

    assert payload["trigger_status"] == "SUPPRESSED_ADMISSION_NOT_ADMIT"
    assert payload["trigger_reason_code"] == "OPEN_TRIGGER_ADMISSION_NOT_ADMIT"
