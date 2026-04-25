from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path("/home/node/constellation")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common import day_open_attempt_v1 as attempt_module
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
) -> Path:
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
    ledger_path = truth_root / "reports" / "paper_session_ledger_v1" / DAY / "paper_session_ledger.v1.json"
    _write_json(
        ledger_path,
        {
            "control_state": {"authority_status": authority_status},
            "authority_status": authority_status,
        },
    )
    return ledger_path


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


def test_day_open_attempt_allows_repeated_same_day_paper_opens_and_preserves_history(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    ledger_path = _authority_surfaces(truth_root)
    rollup_path = truth_root / "reports" / "sleeve_rollup_v1" / DAY / "sleeve_rollup.v1.json"
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs):
        calls.append(cmd)
        _write_json(
            rollup_path,
            {
                "schema_id": "sleeve_rollup",
                "schema_version": "v1",
                "status": "PASS",
                "day_utc": DAY,
            },
        )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    with (
        patch.object(trigger_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")),
        patch.object(attempt_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")),
        patch.object(trigger_module, "producer_block_v1", return_value={"repo": "constellation", "module": "test", "git_sha": "a" * 40}),
        patch.object(attempt_module, "producer_block_v1", return_value={"repo": "constellation", "module": "test", "git_sha": "a" * 40}),
        patch.object(attempt_module, "_find_per_sleeve_refs", return_value=(["/tmp/verdict.json"], ["/tmp/pointer.jsonl"])),
        patch.object(attempt_module, "_post_attempt_refresh", return_value=None),
        patch.object(attempt_module.subprocess, "run", side_effect=fake_run),
    ):
        first_ref = attempt_module.run_day_open_attempt_v1(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            paper_session_ledger_path=ledger_path,
            actor_name="test-actor",
            actor_path="/tmp/test-actor",
            environment="PAPER",
        )
        first_payload = json.loads(first_ref.path.read_text(encoding="utf-8"))
        second_ref = attempt_module.run_day_open_attempt_v1(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            paper_session_ledger_path=ledger_path,
            actor_name="test-actor",
            actor_path="/tmp/test-actor",
            environment="PAPER",
        )
        second_payload = json.loads(second_ref.path.read_text(encoding="utf-8"))

    trigger_payload = json.loads(
        (truth_root / "reports" / "day_open_trigger_v1" / DAY / "day_open_trigger.v1.json").read_text(encoding="utf-8")
    )
    assert first_payload["final_classification"] == "OPEN_SUCCEEDED"
    assert second_payload["final_classification"] == "OPEN_SUCCEEDED"
    assert second_payload["attempt_sequence"] == 2
    assert len(second_payload["attempt_history"]) == 1
    assert second_payload["attempt_history"][0]["final_classification"] == "OPEN_SUCCEEDED"
    assert second_payload["open_policy"]["same_day_open_cap_enforced"] is False
    assert trigger_payload["trigger_sequence"] == 2
    assert len(trigger_payload["trigger_history"]) == 1
    assert len(calls) == 2


def test_day_open_attempt_paper_remains_blocked_when_readiness_denied(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    ledger_path = _authority_surfaces(truth_root, authority_status="DENIED")

    with (
        patch.object(trigger_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")),
        patch.object(attempt_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")),
        patch.object(trigger_module, "producer_block_v1", return_value={"repo": "constellation", "module": "test", "git_sha": "a" * 40}),
        patch.object(attempt_module, "producer_block_v1", return_value={"repo": "constellation", "module": "test", "git_sha": "a" * 40}),
        patch.object(attempt_module, "_post_attempt_refresh", return_value=None),
        patch.object(attempt_module.subprocess, "run") as run_mock,
    ):
        ref = attempt_module.run_day_open_attempt_v1(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            paper_session_ledger_path=ledger_path,
            actor_name="test-actor",
            actor_path="/tmp/test-actor",
            environment="PAPER",
        )

    payload = json.loads(ref.path.read_text(encoding="utf-8"))
    assert payload["final_classification"] == "NOT_EXECUTED"
    assert payload["reason_codes"] == ["OPEN_TRIGGER_AUTHORITY_NOT_GRANTED"]
    assert run_mock.call_count == 0


def test_day_open_attempt_records_runtime_lifecycle_ref_when_provided(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    ledger_path = _authority_surfaces(truth_root)

    with (
        patch.object(trigger_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")),
        patch.object(attempt_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")),
        patch.object(trigger_module, "producer_block_v1", return_value={"repo": "constellation", "module": "test", "git_sha": "a" * 40}),
        patch.object(attempt_module, "producer_block_v1", return_value={"repo": "constellation", "module": "test", "git_sha": "a" * 40}),
        patch.object(attempt_module, "_find_per_sleeve_refs", return_value=(["/tmp/verdict.json"], ["/tmp/pointer.jsonl"])),
        patch.object(attempt_module, "_post_attempt_refresh", return_value=None),
        patch.object(attempt_module.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout="", stderr="")),
    ):
        ref = attempt_module.run_day_open_attempt_v1(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            paper_session_ledger_path=ledger_path,
            actor_name="test-actor",
            actor_path="/tmp/test-actor",
            environment="PAPER",
            runtime_run_id="20260418T141700Z__c2_paper_day_orchestrator_service__pid321",
            runtime_identity_contract_path="/tmp/runtime_contract_v1/active_runtime_contract.v1.json",
            runtime_identity_contract_sha256="b" * 64,
            startup_identity_receipt_path="/tmp/runtime_startup_identity.v1.json",
            lifecycle_start_receipt_path="/tmp/runtime_lifecycle_receipt.v1.json",
        )

    payload = json.loads(ref.path.read_text(encoding="utf-8"))
    assert payload["runtime_lifecycle_ref"]["run_id"] == "20260418T141700Z__c2_paper_day_orchestrator_service__pid321"
    assert payload["runtime_lifecycle_ref"]["runtime_identity_contract_sha256"] == "b" * 64
    assert payload["runtime_lifecycle_ref"]["lifecycle_start_receipt_path"] == "/tmp/runtime_lifecycle_receipt.v1.json"


def test_day_open_attempt_live_still_marks_open_missed_after_window(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    ledger_path = _authority_surfaces(truth_root)

    with (
        patch.object(trigger_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")),
        patch.object(attempt_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")),
        patch.object(trigger_module, "producer_block_v1", return_value={"repo": "constellation", "module": "test", "git_sha": "a" * 40}),
        patch.object(attempt_module, "producer_block_v1", return_value={"repo": "constellation", "module": "test", "git_sha": "a" * 40}),
        patch.object(attempt_module, "_post_attempt_refresh", return_value=None),
        patch.object(attempt_module.subprocess, "run") as run_mock,
    ):
        ref = attempt_module.run_day_open_attempt_v1(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            paper_session_ledger_path=ledger_path,
            actor_name="test-actor",
            actor_path="/tmp/test-actor",
            environment="LIVE",
        )

    payload = json.loads(ref.path.read_text(encoding="utf-8"))
    assert payload["final_classification"] == "OPEN_MISSED"
    assert payload["result_code"] == "OPEN_WINDOW_EXPIRED"
    assert run_mock.call_count == 0


def test_day_open_attempt_normalizes_legacy_missing_trigger_kind_in_prior_attempt(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    ledger_path = _authority_surfaces(truth_root)
    rollup_path = truth_root / "reports" / "sleeve_rollup_v1" / DAY / "sleeve_rollup.v1.json"
    _write_json(
        truth_root / "reports" / "day_open_attempt_v1" / DAY / "day_open_attempt.v1.json",
        {
            "schema_id": "day_open_attempt",
            "schema_version": "v1",
            "day_utc": DAY,
            "attempt_sequence": 1,
            "started_at_utc": f"{DAY}T15:53:58Z",
            "finished_at_utc": f"{DAY}T15:53:58Z",
            "trigger_ref": "/tmp/day_open_trigger.v1.json",
            "consumed_trigger_key": f"{DAY}:NO_TRIGGER:{DAY}T09:31:00-04:00",
            "actor_identity": {"actor_name": "legacy", "actor_path": "/tmp/legacy"},
            "open_command_executed": False,
            "submit_stage_entered": False,
            "submit_stage_owner": "ops/tools/run_c2_paper_day_orchestrator_v2.py",
            "result_code": "OPEN_WINDOW_EXPIRED",
            "reason_codes": ["OPEN_TRIGGER_OPEN_WINDOW_EXPIRED"],
            "consumed_authority_refs": {
                "paper_session_ledger_v1": str(ledger_path),
                "day_open_trigger_v1": "/tmp/day_open_trigger.v1.json",
            },
            "direct_evidence_refs": {
                "sleeve_rollup_v1": str(rollup_path),
                "orchestrator_run_verdict_v2": [],
                "run_pointer_v1": [],
            },
            "final_classification": "OPEN_MISSED",
            "environment": "PAPER",
            "producer": {"repo": "constellation", "module": "test", "git_sha": "a" * 40},
        },
    )

    def fake_run(cmd: list[str], **kwargs):
        _write_json(
            rollup_path,
            {"schema_id": "sleeve_rollup", "schema_version": "v1", "status": "PASS", "day_utc": DAY},
        )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    with (
        patch.object(trigger_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")),
        patch.object(attempt_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")),
        patch.object(trigger_module, "producer_block_v1", return_value={"repo": "constellation", "module": "test", "git_sha": "a" * 40}),
        patch.object(attempt_module, "producer_block_v1", return_value={"repo": "constellation", "module": "test", "git_sha": "a" * 40}),
        patch.object(attempt_module, "_find_per_sleeve_refs", return_value=(["/tmp/verdict.json"], ["/tmp/pointer.jsonl"])),
        patch.object(attempt_module, "_post_attempt_refresh", return_value=None),
        patch.object(attempt_module.subprocess, "run", side_effect=fake_run),
    ):
        ref = attempt_module.run_day_open_attempt_v1(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            paper_session_ledger_path=ledger_path,
            actor_name="test-actor",
            actor_path="/tmp/test-actor",
            environment="PAPER",
        )

    payload = json.loads(ref.path.read_text(encoding="utf-8"))
    assert payload["final_classification"] == "OPEN_SUCCEEDED"
    assert payload["attempt_sequence"] == 2
    assert payload["attempt_history"][0]["trigger_kind"] == "NO_TRIGGER"
    assert payload["submit_stage_owner"] == "ops/tools/run_c2_paper_day_orchestrator_v2.py"
