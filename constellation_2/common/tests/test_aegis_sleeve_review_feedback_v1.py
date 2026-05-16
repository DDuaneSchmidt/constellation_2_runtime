from __future__ import annotations

import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_research_lab_v1 import validate_research_lab_artifact_v1  # noqa: E402
from constellation_2.common.aegis_sleeve_performance_report_v1 import build_sleeve_performance_report_v1  # noqa: E402
from constellation_2.common.aegis_sleeve_review_feedback_v1 import (  # noqa: E402
    build_eod_sleeve_review_v1,
    build_eow_sleeve_review_v1,
    validate_eod_sleeve_review_v1,
    validate_eow_sleeve_review_v1,
    write_research_task_queue_from_review_v1,
)
from constellation_2.common.tests.test_sleeve_performance_report_v1 import (  # noqa: E402
    _outcome_ledger,
    _packet,
    _promoted_library,
    _receipt,
    _write,
)
from ops.tools.build_eod_sleeve_review_v1 import main as eod_cli_main  # noqa: E402
from ops.tools.build_eow_sleeve_review_v1 import main as eow_cli_main  # noqa: E402
from ops.tools.build_sleeve_performance_report_v1 import main as performance_cli_main  # noqa: E402


NOW = "2026-05-15T21:00:00Z"
DAY = "2026-05-15"


def _performance_report() -> dict[str, object]:
    return build_sleeve_performance_report_v1(
        day_utc=DAY,
        generated_at_utc=NOW,
        manual_trade_packets=[_packet()],
        manual_execution_receipts=[
            _receipt("trade-1"),
            _receipt(
                "trade-4",
                source_packet_type="EVENT_TACTICAL_PACKET",
                alert_id="alert-1",
                event_id="event-1",
                fill_before_valid_until=False,
                max_entry_slippage_respected=False,
                stop_order_entered=False,
            ),
        ],
        outcome_ledgers=[_outcome_ledger()],
        promoted_sleeve_libraries=[_promoted_library()],
        event_tactical_packets=[{"recommended_trade_id": "trade-4", "event_id": "event-1", "event_type": "PANIC_EXHAUSTION", "execution_sensitivity": "HIGH"}],
        trade_capture_alert_ledgers=[{"alert_attempts": [{"alert_id": "alert-1", "source_packet_id": "trade-4", "alert_gate_status": "ACTIONABLE_TRADE"}]}],
    )


def test_eod_sleeve_review_builds_from_performance_report_and_flags_feedback() -> None:
    review = build_eod_sleeve_review_v1(
        day_utc=DAY,
        generated_at_utc=NOW,
        sleeve_performance_report=_performance_report(),
        event_awareness_ledgers=[{"events": [{"event_id": "event-1"}]}],
        event_rules_registries=[{"rules": []}],
        input_artifact_refs=[{"artifact_type": "sleeve_performance_report", "path": "/tmp/report"}],
    )

    validate_eod_sleeve_review_v1(review)
    assert review["review_type"] == "EOD"
    assert review["ai_used"] is False
    assert review["ai_model_source"] == "DETERMINISTIC_PLACEHOLDER_NO_LLM_RUNTIME"
    assert review["review_summary"]["missing_receipt_count"] == 1
    assert review["review_summary"]["missing_outcome_count"] == 0
    assert review["slippage_issues"]
    assert review["stop_behavior_issues"]
    assert any(task["task_type"] == "slippage_review" for task in review["research_tasks_created_or_recommended"])
    assert any(task["task_type"] == "stop_behavior_review" for task in review["research_tasks_created_or_recommended"])
    assert review["manual_execution_only"] is True
    assert review["broker_submit_required"] is False
    assert review["runtime_mutation_allowed"] is False
    assert review["trade_creation_allowed"] is False
    assert review["automatic_promotion_allowed"] is False


def test_eow_sleeve_review_aggregates_daily_reports_and_blocks_auto_promotion() -> None:
    eod_review = build_eod_sleeve_review_v1(
        day_utc=DAY,
        generated_at_utc=NOW,
        sleeve_performance_report=_performance_report(),
    )
    review = build_eow_sleeve_review_v1(
        week_ending=DAY,
        generated_at_utc=NOW,
        daily_sleeve_performance_reports=[_performance_report()],
        daily_eod_reviews=[eod_review],
        event_awareness_ledgers=[{"events": [{"event_id": "event-1"}]}],
        research_task_queues=[],
    )

    validate_eow_sleeve_review_v1(review)
    assert review["review_type"] == "EOW"
    assert review["weekly_scorecard"]
    assert review["event_false_positives"]
    assert review["repeated_failure_modes"]
    assert review["research_tasks_created_or_recommended"]
    assert review["ai_used"] is False
    assert review["broker_submit_required"] is False
    assert review["trade_creation_allowed"] is False
    assert review["automatic_promotion_allowed"] is False


def test_review_can_write_offline_research_tasks_only(tmp_path: Path) -> None:
    review = build_eod_sleeve_review_v1(
        day_utc=DAY,
        generated_at_utc=NOW,
        sleeve_performance_report=_performance_report(),
    )
    path = write_research_task_queue_from_review_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        generated_at_utc=NOW,
        tasks=review["research_tasks_created_or_recommended"],
    )

    queue = json.loads(path.read_text(encoding="utf-8"))
    validate_research_lab_artifact_v1(queue)
    assert queue["tasks"]
    assert all(task["status"] == "open" for task in queue["tasks"])
    assert not (tmp_path / "reports" / "aegis_lite_eod_report_v1").exists()
    assert not (tmp_path / "broker").exists()


def test_eod_and_eow_cli_write_operator_readable_reviews_without_broker(tmp_path: Path, capsys) -> None:
    packet_path = _write(tmp_path / "reports" / "manual_trade_packet_v1" / DAY / "run-1" / "manual_trade_packet.v1.json", _packet())
    receipt_path = _write(
        tmp_path / "research_lab" / "manual_execution_receipt_v1" / DAY / "receipt-trade-1" / "manual_execution_receipt.v1.json",
        _receipt("trade-1"),
    )
    outcome_path = _write(tmp_path / "research_lab" / "outcome_ledger_v1" / DAY / "index" / "outcome_ledger.v1.json", _outcome_ledger())
    library_path = _write(tmp_path / "reports" / "promoted_sleeve_library_v1" / DAY / "run-1" / "promoted_sleeve_library.v1.json", _promoted_library())

    assert performance_cli_main(
        [
            "--truth_root",
            str(tmp_path),
            "--day",
            DAY,
            "--generated_at_utc",
            NOW,
            "--manual_trade_packet",
            str(packet_path),
            "--manual_execution_receipt",
            str(receipt_path),
            "--outcome_ledger",
            str(outcome_path),
            "--promoted_sleeve_library",
            str(library_path),
        ]
    ) == 0
    assert eod_cli_main(["--truth_root", str(tmp_path), "--day", DAY, "--generated_at_utc", NOW, "--write_research_tasks"]) == 0
    eod_output = capsys.readouterr().out
    assert "AEGIS EOD SLEEVE REVIEW" in eod_output
    assert "AI used: false" in eod_output
    assert "Safety: manual-only" in eod_output
    eod_path = tmp_path / "reports" / "eod_sleeve_review_v1" / DAY / "eod_sleeve_review.v1.json"
    queue_path = tmp_path / "research_lab" / "research_task_queue_v1" / DAY / "index" / "research_task_queue.v1.json"
    assert eod_path.exists()
    assert queue_path.exists()

    assert eow_cli_main(["--truth_root", str(tmp_path), "--week_ending", DAY, "--generated_at_utc", NOW, "--write_research_tasks"]) == 0
    eow_output = capsys.readouterr().out
    assert "AEGIS EOW SLEEVE REVIEW" in eow_output
    assert "no broker submit" in eow_output
    eow_path = tmp_path / "reports" / "eow_sleeve_review_v1" / DAY / "eow_sleeve_review.v1.json"
    assert eow_path.exists()
    assert not (tmp_path / "broker").exists()
