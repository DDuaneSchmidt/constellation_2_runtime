from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ops.aegis.semantic_invariants_v1 import build_semantic_invariants_v1, run_semantic_invariants_self_check_v1, write_semantic_invariants_v1
from ops.aegis.surface_readiness_v1 import build_surface_readiness_v1

DAY = "2026-05-30"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_clean(root: Path) -> None:
    reports = root / "reports"
    _write(reports / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json", {"day_utc": DAY, "overview": {"full_portfolio_pnl_status": "NOT_CANONICAL", "total_pnl": "NOT_CANONICAL", "open_positions": 36, "missing_mark_count": 36, "data_quality": "DEGRADED", "mark_coverage": {"mark_coverage_by_position_pct": 0}}})
    _write(reports / "aegis_daily_paper_performance_v1" / DAY / "daily_paper_performance.v1.json", {"day_utc": DAY, "full_portfolio_pnl_status": "NOT_CANONICAL", "total_paper_pnl": "NOT_CANONICAL", "data_quality_status": "DEGRADED"})
    _write(reports / "aegis_sleeve_analytics_v1" / DAY / "sleeve_analytics.v1.json", {"day_utc": DAY, "status": "PARTIAL", "summary": {"total_sleeves": 3, "total_open_positions": 36, "total_pnl": 0, "mark_coverage_pct": 0, "sleeve_attribution_coverage_pct": 30, "data_quality_status": "PARTIAL"}, "data_quality": {"missing_mark_count": 36, "data_quality_status": "PARTIAL"}, "sleeves": []})
    _write(reports / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json", {"day_utc": DAY, "open_positions": []})


def _failures(payload: dict, surface_id: str) -> list[str]:
    return [row["invariant_id"] for row in payload["invariants"] if row["surface_id"] == surface_id and row["status"] == "FAIL"]


def test_semantic_invariant_detects_zero_sleeves_with_complete_attribution(tmp_path: Path) -> None:
    _seed_clean(tmp_path)
    _write(tmp_path / "reports" / "aegis_sleeve_analytics_v1" / DAY / "sleeve_analytics.v1.json", {"day_utc": DAY, "status": "CANONICAL", "summary": {"total_sleeves": 0, "total_open_positions": 0, "total_pnl": 0, "mark_coverage_pct": 0, "sleeve_attribution_coverage_pct": 100, "data_quality_status": "PASS"}, "data_quality": {"missing_mark_count": 0, "data_quality_status": "PASS"}})

    payload = build_semantic_invariants_v1(truth_root=tmp_path, day_utc=DAY)

    assert "sleeve_total_sleeves_zero_attribution_complete" in _failures(payload, "sleeve_analytics")
    assert payload["summary"]["blocking_failure_count"] >= 1


def test_semantic_invariant_detects_not_canonical_complete_analytics(tmp_path: Path) -> None:
    _seed_clean(tmp_path)
    _write(tmp_path / "reports" / "aegis_sleeve_analytics_v1" / DAY / "sleeve_analytics.v1.json", {"day_utc": DAY, "status": "NOT_CANONICAL", "summary": {"total_sleeves": 2, "total_open_positions": 36, "total_pnl": 12.34, "mark_coverage_pct": 100, "sleeve_attribution_coverage_pct": 100, "data_quality_status": "PASS"}, "data_quality": {"missing_mark_count": 0, "data_quality_status": "PASS"}})

    payload = build_semantic_invariants_v1(truth_root=tmp_path, day_utc=DAY)

    assert "sleeve_not_canonical_with_complete_analytics" in _failures(payload, "sleeve_analytics")
    assert "sleeve_status_not_canonical_total_pnl_canonical" in _failures(payload, "sleeve_analytics")


def test_performance_missing_marks_complete_coverage_fails(tmp_path: Path) -> None:
    _seed_clean(tmp_path)
    _write(tmp_path / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json", {"day_utc": DAY, "overview": {"full_portfolio_pnl_status": "CANONICAL", "total_pnl": 42.0, "open_positions": 36, "missing_mark_count": 1, "data_quality": "PASS", "mark_coverage": {"mark_coverage_by_position_pct": 100}}})

    payload = build_semantic_invariants_v1(truth_root=tmp_path, day_utc=DAY)

    failures = _failures(payload, "performance")
    assert "performance_missing_marks_with_complete_coverage" in failures


def test_surface_readiness_reflects_semantic_invariant_failure(tmp_path: Path) -> None:
    _seed_clean(tmp_path)
    _write(tmp_path / "reports" / "aegis_sleeve_analytics_v1" / DAY / "sleeve_analytics.v1.json", {"day_utc": DAY, "status": "CANONICAL", "summary": {"total_sleeves": 0, "total_open_positions": 0, "total_pnl": 0, "mark_coverage_pct": 100, "sleeve_attribution_coverage_pct": 100, "data_quality_status": "PASS"}, "data_quality": {"missing_mark_count": 0, "data_quality_status": "PASS"}})
    write_semantic_invariants_v1(truth_root=tmp_path, day_utc=DAY)
    # Seed the other required surface artifacts minimally so the sleeve failure is isolated.
    reports = tmp_path / "reports"
    _write(reports / "aegis_canonical_operator_state_v1" / DAY / "canonical_operator_state.v1.json", {"day_utc": DAY})
    _write(reports / "aegis_command_center_queue_audit_v1" / DAY / "command_center_queue_audit.v1.json", {"day_utc": DAY, "summary": {"operator_action_required_count": 0, "incorrectly_shown_as_awaiting_review_count": 0}, "current_command_center_counts": {}})
    _write(reports / "aegis_engineering_priority_queue_v1" / DAY / "engineering_priority_queue.v1.json", {"day_utc": DAY})
    _write(reports / "aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json", {"day_utc": DAY, "highest_readiness_layer": "READY"})
    _write(reports / "aegis_verified_runtime_graph_v1" / DAY / "verified_runtime_graph.v1.json", {"day_utc": DAY})
    _write(reports / "aegis_position_review_context_v1" / DAY / "position_review_context.v1.json", {"day_utc": DAY})
    _write(reports / "aegis_position_review_score_v1" / DAY / "position_review_score.v1.json", {"day_utc": DAY})
    _write(reports / "aegis_position_review_brief_v1" / DAY / "position_review_brief.v1.json", {"day_utc": DAY})
    _write(reports / "aegis_ai_operations_context_v1" / DAY / "ai_operations_context.v1.json", {"day_utc": DAY, "source_day": DAY})

    payload = build_surface_readiness_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["surface_by_id"]["sleeve_analytics"]

    assert row["surface_status"] == "BLOCKED"
    assert row["actions_allowed"] is False
    assert any("SEMANTIC_INVARIANT_FAILED" in reason for reason in row["blocking_reasons"])


def test_semantic_self_check_passes_for_consistent_degraded_payload(tmp_path: Path) -> None:
    _seed_clean(tmp_path)
    write_semantic_invariants_v1(truth_root=tmp_path, day_utc=DAY)

    result = run_semantic_invariants_self_check_v1(truth_root=tmp_path, day_utc=DAY)

    assert result["ok"] is True
