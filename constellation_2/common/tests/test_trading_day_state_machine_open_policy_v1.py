from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path("/home/node/constellation")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools import run_trading_day_state_machine_v1 as state_machine_module


DAY = "2026-04-14"


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


def test_open_lifecycle_reports_paper_open_available_even_after_window(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    attempt_path = truth_root / "reports" / "day_open_attempt_v1" / DAY / "day_open_attempt.v1.json"
    attempt_path.parent.mkdir(parents=True, exist_ok=True)
    with patch.object(state_machine_module, "build_day_open_window_v1", return_value=_window("POST_OPEN_WINDOW")):
        payload = state_machine_module._open_lifecycle_state(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            final_start_decision="READY_NOW",
            trigger_payload={"trigger_status": "SUPPRESSED_OUTSIDE_OPEN_WINDOW", "trigger_kind": "NO_TRIGGER", "trigger_sequence": 1},
            attempt_payload=None,
        )

    assert payload["state"] == "PAPER_OPEN_AVAILABLE"
    assert payload["policy"]["time_window_enforced"] is False


def test_open_lifecycle_keeps_paper_failure_nonterminal(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    attempt_path = truth_root / "reports" / "day_open_attempt_v1" / DAY / "day_open_attempt.v1.json"
    attempt_path.parent.mkdir(parents=True, exist_ok=True)
    with patch.object(state_machine_module, "build_day_open_window_v1", return_value=_window("OPEN_WINDOW")):
        payload = state_machine_module._open_lifecycle_state(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc=DAY,
            final_start_decision="READY_NOW",
            trigger_payload={"trigger_status": "SUPPRESSED_AUTHORITY_NOT_GRANTED", "trigger_kind": "NO_TRIGGER", "trigger_sequence": 1},
            attempt_payload={
                "attempt_sequence": 1,
                "trigger_kind": "INITIAL_BOD_TRIGGER",
                "final_classification": "OPEN_FAILED",
                "result_code": "ORCHESTRATOR_RC_2",
                "attempt_history": [],
            },
        )

    assert payload["state"] == "OPEN_FAILED"
    assert payload["policy"]["open_terminal"] is False
