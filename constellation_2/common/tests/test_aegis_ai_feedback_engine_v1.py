from __future__ import annotations

import json
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_ai_feedback_engine_v1 import (  # noqa: E402
    build_ai_feedback_review_v1,
    build_evidence_gate_v1,
    validate_ai_feedback_review_v1,
    validate_evidence_gate_v1,
    write_research_task_queue_from_ai_feedback_v1,
)
from constellation_2.common.aegis_research_lab_v1 import validate_research_lab_artifact_v1  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # noqa: E402
from constellation_2.common.tests.test_sleeve_performance_report_v1 import (  # noqa: E402
    _outcome_ledger,
    _packet,
    _promoted_library,
    _receipt,
    _write,
)
from ops.tools.build_ai_eod_feedback_review_v1 import main as eod_cli_main  # noqa: E402
from ops.tools.build_ai_eow_feedback_review_v1 import main as eow_cli_main  # noqa: E402
from ops.tools.build_sleeve_performance_report_v1 import main as performance_cli_main  # noqa: E402


NOW = "2026-05-15T21:00:00Z"
DAY = "2026-05-15"


def _row(trade_id: str, **overrides: object) -> dict[str, object]:
    base = {
        "trade_id": trade_id,
        "lifecycle_status": "EXECUTED_CLOSED",
        "sleeve_id": "sleeve-a",
        "source_hypothesis_id": "rh-a",
        "symbol": "SPY",
        "outcome_status": "win",
        "return_pct": "1.0",
        "entry_slippage_respected": True,
        "max_entry_slippage_respected": True,
        "stop_entered": True,
        "stop_matched_recommendation": True,
        "regime_state": "PANIC",
        "event_type": "",
        "event_id": "",
        "alert_id": "",
        "failure_reason": "",
        "edge_overlap_attribution": "LOW_OVERLAP",
    }
    base.update(overrides)
    return base


def _report(rows: list[dict[str, object]], day: str = DAY) -> dict[str, object]:
    return {
        "schema_id": "sleeve_performance_report",
        "schema_version": "v1",
        "artifact_id": "sleeve_performance_report_v1",
        "day_utc": day,
        "generated_at_utc": NOW,
        "portfolio_summary": {"total_recommended_trades": len(rows), "total_executed_trades": len(rows), "missing_receipt_count": 0, "missing_outcome_count": 0},
        "sleeve_summary": [{"sleeve_id": "sleeve-a", "total_return": "1.0"}],
        "execution_quality": {},
        "outcome_quality": {},
        "research_feedback": {"generated_or_recommended_tasks": [], "task_count": 0, "writes_research_task_queue": False, "automatic_promotion_allowed": False, "lite_runtime_mutation_allowed": False},
        "trade_lifecycle_rows": rows,
        "join_diagnostics": [],
        "input_counts": {},
        "source_artifact_lineage": [{"artifact_type": "fixture", "path": "/tmp/fixture"}],
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_required": False,
        "runtime_mutation_allowed": False,
        "canonical_eod_state_mutated": False,
        "canonical_json_hash": "0" * 64,
    }


def _evidence(rows: list[dict[str, object]], *, day: str = DAY) -> dict[str, object]:
    gate = build_evidence_gate_v1(
        review_type="EOD",
        period_start=DAY,
        period_end=DAY,
        generated_at_utc=NOW,
        sleeve_performance_reports=[_report(rows, day=day)],
        input_artifact_refs=[{"artifact_type": "sleeve_performance_report.v1", "path": "/tmp/report"}],
    )
    validate_evidence_gate_v1(gate)
    return gate


def test_evidence_gate_blocks_insufficient_sample_and_research_tasks() -> None:
    gate = _evidence([_row("trade-1")])
    review = build_ai_feedback_review_v1(
        review_type="EOD",
        period_start=DAY,
        period_end=DAY,
        generated_at_utc=NOW,
        sleeve_performance_reports=[_report([_row("trade-1")])],
        evidence_gate=gate,
        input_artifact_refs=[{"artifact_type": "sleeve_performance_report.v1", "path": "/tmp/report"}],
    )

    validate_ai_feedback_review_v1(review)
    assert gate["sample_band"] == "OBSERVATION_ONLY"
    assert gate["evidence_gate_status"] == "OBSERVATION_ONLY"
    assert gate["automatic_research_task_creation_allowed"] is False
    assert review["research_tasks_created"] == []
    assert review["findings"][0]["confidence"] == "LOW"


def test_missing_receipts_and_outcomes_downgrade_and_are_audited() -> None:
    rows = [
        _row("trade-1", lifecycle_status="MISSING_RECEIPT", outcome_status="", return_pct=""),
        _row("trade-2", lifecycle_status="MISSING_OUTCOME", outcome_status="", return_pct=""),
        _row("trade-3", outcome_status="loss", return_pct="-2.0", entry_slippage_respected=False, stop_entered=False),
    ]
    gate = _evidence(rows)
    review = build_ai_feedback_review_v1(
        review_type="EOD",
        period_start=DAY,
        period_end=DAY,
        generated_at_utc=NOW,
        sleeve_performance_reports=[_report(rows)],
        evidence_gate=gate,
        input_artifact_refs=[{"artifact_type": "sleeve_performance_report.v1", "path": "/tmp/report"}],
    )

    assert gate["missing_receipts"] == 1
    assert gate["missing_outcomes"] == 1
    assert "MISSING_RECEIPTS" in gate["blocked_conclusion_reason_codes"]
    assert "MISSING_OUTCOMES" in gate["blocked_conclusion_reason_codes"]
    assert any(finding["finding_type"] == "DATA_QUALITY_ISSUE" for finding in review["findings"])
    assert all(finding["evidence_refs"] for finding in review["findings"])


def test_stale_data_blocks_strong_conclusions() -> None:
    rows = [_row(f"trade-{idx}") for idx in range(1, 12)]
    gate = _evidence(rows, day="2026-05-14")

    assert gate["sample_band"] == "REVIEWABLE_PATTERN"
    assert gate["stale_data"] is True
    assert gate["evidence_gate_status"] == "BLOCKED"
    assert gate["strong_conclusions_allowed"] is False
    assert gate["automatic_research_task_creation_allowed"] is False


def test_eod_review_is_deterministic_and_never_mutates_production() -> None:
    rows = [_row(f"trade-{idx}", outcome_status="loss", return_pct="-1.0", failure_reason="regime failure") for idx in range(1, 5)]
    gate = _evidence(rows)
    review_a = build_ai_feedback_review_v1(
        review_type="EOD",
        period_start=DAY,
        period_end=DAY,
        generated_at_utc=NOW,
        sleeve_performance_reports=[_report(rows)],
        evidence_gate=gate,
        input_artifact_refs=[{"artifact_type": "sleeve_performance_report.v1", "path": "/tmp/report"}],
    )
    review_b = build_ai_feedback_review_v1(
        review_type="EOD",
        period_start=DAY,
        period_end=DAY,
        generated_at_utc=NOW,
        sleeve_performance_reports=[_report(rows)],
        evidence_gate=gate,
        input_artifact_refs=[{"artifact_type": "sleeve_performance_report.v1", "path": "/tmp/report"}],
    )

    assert canonical_json_bytes_v1(review_a) == canonical_json_bytes_v1(review_b)
    assert review_a["ai_used"] is False
    assert review_a["deterministic_fallback_used"] is True
    assert review_a["production_mutation"] is False
    assert review_a["broker_action_allowed"] is False
    assert review_a["auto_promotion_allowed"] is False
    assert review_a["auto_demotion_allowed"] is False
    assert all(finding["production_action_allowed"] is False for finding in review_a["findings"])


def test_eow_review_aggregates_daily_reports_and_limits_findings() -> None:
    rows = [
        _row(f"trade-{idx}", outcome_status="loss", return_pct="-1.0", failure_reason="edge overlap failure", event_id="event-1", event_type="PANIC_EXHAUSTION", valid_until_respected=False)
        for idx in range(1, 13)
    ]
    gate = build_evidence_gate_v1(
        review_type="EOW",
        period_start="2026-05-11",
        period_end=DAY,
        generated_at_utc=NOW,
        sleeve_performance_reports=[_report(rows[:6], day="2026-05-14"), _report(rows[6:], day=DAY)],
        input_artifact_refs=[{"artifact_type": "sleeve_performance_report.v1", "path": "/tmp/report"}],
    )
    review = build_ai_feedback_review_v1(
        review_type="EOW",
        period_start="2026-05-11",
        period_end=DAY,
        generated_at_utc=NOW,
        sleeve_performance_reports=[_report(rows[:6], day="2026-05-14"), _report(rows[6:], day=DAY)],
        evidence_gate=gate,
        input_artifact_refs=[{"artifact_type": "sleeve_performance_report.v1", "path": "/tmp/report"}],
    )

    validate_evidence_gate_v1(gate)
    validate_ai_feedback_review_v1(review)
    assert gate["sample_band"] == "REVIEWABLE_PATTERN"
    assert len(review["findings"]) <= 5
    assert any(finding["finding_type"] == "EVENT_FALSE_POSITIVE" for finding in review["findings"])


def test_research_task_gate_allows_weak_signal_low_priority_tasks(tmp_path: Path) -> None:
    rows = [_row(f"trade-{idx}", outcome_status="loss", return_pct="-1.0", failure_reason="stop failure") for idx in range(1, 4)]
    gate = _evidence(rows)
    review = build_ai_feedback_review_v1(
        review_type="EOD",
        period_start=DAY,
        period_end=DAY,
        generated_at_utc=NOW,
        sleeve_performance_reports=[_report(rows)],
        evidence_gate=gate,
        input_artifact_refs=[{"artifact_type": "sleeve_performance_report.v1", "path": "/tmp/report"}],
    )
    path = write_research_task_queue_from_ai_feedback_v1(
        truth_root=tmp_path,
        period_end=DAY,
        generated_at_utc=NOW,
        tasks=review["research_tasks_created"],
    )

    assert gate["sample_band"] == "WEAK_SIGNAL"
    assert review["research_tasks_created"]
    assert all(task["priority"] == "low" for task in review["research_tasks_created"])
    queue = json.loads(path.read_text(encoding="utf-8"))
    validate_research_lab_artifact_v1(queue)
    assert not (tmp_path / "broker").exists()
    assert not (tmp_path / "reports" / "aegis_lite_eod_report_v1").exists()


def test_cli_builds_eod_and_eow_reviews_without_broker_or_trade_creation(tmp_path: Path, capsys) -> None:
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
    assert eod_cli_main(["--truth_root", str(tmp_path), "--day", DAY, "--generated_at_utc", NOW]) == 0
    eod_output = capsys.readouterr().out
    assert "AEGIS AI EOD FEEDBACK REVIEW" in eod_output
    assert "Safety: human review required" in eod_output
    assert (tmp_path / "reports" / "evidence_gate_v1" / "EOD" / DAY / "evidence_gate.v1.json").exists()
    assert (tmp_path / "reports" / "ai_feedback_review_v1" / "EOD" / DAY / "ai_feedback_review.v1.json").exists()

    assert eow_cli_main(["--truth_root", str(tmp_path), "--week_ending", DAY, "--generated_at_utc", NOW]) == 0
    eow_output = capsys.readouterr().out
    assert "AEGIS AI EOW FEEDBACK REVIEW" in eow_output
    assert (tmp_path / "reports" / "evidence_gate_v1" / "EOW" / DAY / "evidence_gate.v1.json").exists()
    assert (tmp_path / "reports" / "ai_feedback_review_v1" / "EOW" / DAY / "ai_feedback_review.v1.json").exists()
    assert not (tmp_path / "broker").exists()
