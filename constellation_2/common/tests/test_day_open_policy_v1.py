from __future__ import annotations

from pathlib import Path

from constellation_2.common.day_open_policy_v1 import build_day_open_policy_snapshot


DAY = "2026-04-14"


def _attempt(trigger_kind: str, classification: str, sequence: int = 1) -> dict:
    return {
        "attempt_sequence": sequence,
        "trigger_kind": trigger_kind,
        "final_classification": classification,
        "result_code": classification,
        "attempt_history": [],
    }


def test_paper_policy_allows_repeat_open_when_currently_granted(tmp_path: Path) -> None:
    attempt_path = tmp_path / "truth" / "reports" / "day_open_attempt_v1" / DAY / "day_open_attempt.v1.json"
    payload = build_day_open_policy_snapshot(
        environment="PAPER",
        window_status="POST_OPEN_WINDOW",
        trigger_payload={"trigger_status": "EMITTED", "trigger_kind": "INITIAL_BOD_TRIGGER", "trigger_sequence": 1},
        attempt_payload=_attempt("INITIAL_BOD_TRIGGER", "OPEN_SUCCEEDED"),
        attempt_path=attempt_path,
    )

    assert payload["policy_mode"] == "PAPER_READY_WHEN_GRANTED_UNBOUNDED_SAME_DAY"
    assert payload["same_day_open_cap_enforced"] is False
    assert payload["time_window_enforced"] is False
    assert payload["max_successful_opens_per_day"] == 0
    assert payload["open_terminal"] is False
    assert payload["successful_open_already_recorded"] is True
    assert payload["policy_status"] == "PAPER_REPEAT_OPEN_ALLOWED_WHEN_GRANTED"


def test_live_policy_remains_strict_after_prior_success(tmp_path: Path) -> None:
    attempt_path = tmp_path / "truth" / "reports" / "day_open_attempt_v1" / DAY / "day_open_attempt.v1.json"
    payload = build_day_open_policy_snapshot(
        environment="LIVE",
        window_status="OPEN_WINDOW",
        trigger_payload={"trigger_status": "EMITTED", "trigger_kind": "INITIAL_BOD_TRIGGER", "trigger_sequence": 1},
        attempt_payload=_attempt("INITIAL_BOD_TRIGGER", "OPEN_SUCCEEDED"),
        attempt_path=attempt_path,
    )

    assert payload["policy_mode"] == "STRICT_SINGLE_OPEN_WINDOW"
    assert payload["same_day_open_cap_enforced"] is True
    assert payload["time_window_enforced"] is True
    assert payload["open_terminal"] is True
    assert payload["terminal_reason_code"] == "OPEN_ALREADY_SUCCEEDED_FOR_DAY"
