from __future__ import annotations

from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ops.aegis.command_center_queue_audit_v1 import build_command_center_queue_audit_v1

TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
DAY = "2026-05-29"


def test_command_center_queue_audit_classifies_current_rows() -> None:
    payload = build_command_center_queue_audit_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    summary = payload["summary"]
    assert summary["total_rows"] == 40
    assert summary["awaiting_review_rows_audited"] == 37
    assert summary["needs_attention_rows_audited"] == 3
    assert summary["count_by_classification"]["OPERATOR_ACTION_REQUIRED"] == 1
    assert summary["count_by_classification"]["ALREADY_CAPTURED"] == 26
    assert summary["count_by_classification"]["DIAGNOSTICS_ONLY"] == 10
    assert summary["count_by_classification"]["MONITOR_ONLY"] == 3
    assert summary["incorrectly_shown_as_awaiting_review_count"] == 0
    assert summary["incorrectly_shown_as_needs_attention_count"] == 0
    assert payload["current_command_center_counts"]["awaiting_review_metric"] == 1


def test_command_center_queue_audit_is_read_only_and_safe() -> None:
    payload = build_command_center_queue_audit_v1(truth_root=TRUTH_ROOT, day_utc=DAY)
    assert payload["policy"]["ui_changed"] is True
    assert payload["policy"]["trade_advice_allowed"] is False
    assert payload["policy"]["broker_execution_allowed"] is False
    assert payload["policy"]["live_trading_allowed"] is False
    assert payload["policy"]["autonomous_live_trading_allowed"] is False
    for row in payload["rows"]:
        assert row["classification"] in {
            "OPERATOR_ACTION_REQUIRED",
            "SYSTEM_WAITING",
            "ALREADY_CAPTURED",
            "MONITOR_ONLY",
            "DUPLICATE_SUPPRESSED",
            "RESEARCH_ONLY",
            "DIAGNOSTICS_ONLY",
        }


def test_current_day_wrong_session_rows_are_fail_closed() -> None:
    payload = build_command_center_queue_audit_v1(truth_root=TRUTH_ROOT, day_utc="2026-05-30")
    summary = payload["summary"]

    assert summary["count_by_classification"]["OPERATOR_ACTION_REQUIRED"] == 0
    assert payload["current_command_center_counts"]["awaiting_review_metric"] == 0
    assert summary["incorrectly_shown_as_awaiting_review_count"] == 0
    assert any(row.get("day_boundary_status") == "STALE_PAPER_SESSION" for row in payload["rows"])
    assert all(row.get("actionable") is not True for row in payload["rows"] if row.get("paper_session_day") == "2026-05-29")
