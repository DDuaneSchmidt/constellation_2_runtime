from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ops.aegis.golden_scenarios_v1 import build_golden_scenarios_v1, run_golden_scenarios_self_check_v1, write_golden_scenarios_v1


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(root: Path, day: str, *, canonical: bool) -> None:
    reports = root / "reports"
    _write(reports / "aegis_verified_runtime_graph_v1" / day / "verified_runtime_graph.v1.json", {"day_utc": day, "graph_status": "READY", "audit_blockers": [], "runtime_blockers": []})
    _write(reports / "aegis_chatgpt_control_packet_v1" / day / "aegis_chatgpt_control_packet.v1.json", {"day_utc": day, "next_operator_actions": []})
    _write(reports / "aegis_audit_handoff_v1" / day / "aegis_audit_handoff.txt", {"day_utc": day})
    _write(reports / "aegis_hydrate_packet_v1" / day / "aegis_chatgpt_hydrate_packet.v1.json", {"day_utc": day})
    _write(reports / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json", {"day_utc": day, "summary": {"actions_required_count": 0, "warnings_count": 0, "missing_inputs_count": 0}})
    _write(reports / "aegis_command_center_queue_audit_v1" / day / "command_center_queue_audit.v1.json", {"day_utc": day, "summary": {"operator_action_required_count": 0, "incorrectly_shown_as_awaiting_review_count": 0}, "current_command_center_counts": {"awaiting_review_metric": 0}, "rows": []})
    _write(reports / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json", {"day_utc": day, "open_positions": []})
    if canonical:
        _write(reports / "aegis_paper_pnl_report_v1" / day / "paper_pnl_report.v1.json", {"day_utc": day, "overview": {"full_portfolio_pnl_status": "CANONICAL", "total_pnl": 10, "open_positions": 1, "missing_mark_count": 0, "data_quality": "PASS", "mark_coverage": {"mark_coverage_by_position_pct": 100}}})
        _write(reports / "aegis_daily_paper_performance_v1" / day / "daily_paper_performance.v1.json", {"day_utc": day, "full_portfolio_pnl_status": "CANONICAL", "total_paper_pnl": 10, "data_quality_status": "PASS"})
        _write(reports / "aegis_sleeve_analytics_v1" / day / "sleeve_analytics.v1.json", {"day_utc": day, "status": "CANONICAL", "summary": {"total_sleeves": 2, "total_open_positions": 1, "total_pnl": 10, "mark_coverage_pct": 100, "sleeve_attribution_coverage_pct": 100, "data_quality_status": "PASS"}, "data_quality": {"missing_mark_count": 0, "data_quality_status": "PASS"}, "sleeves": [{"sleeve_id": "A"}, {"sleeve_id": "B"}]})
    else:
        _write(reports / "aegis_paper_pnl_report_v1" / day / "paper_pnl_report.v1.json", {"day_utc": day, "overview": {"full_portfolio_pnl_status": "NOT_CANONICAL", "total_pnl": "NOT_CANONICAL", "open_positions": 1, "missing_mark_count": 1, "data_quality": "DEGRADED", "mark_coverage": {"mark_coverage_by_position_pct": 0}}})
        _write(reports / "aegis_daily_paper_performance_v1" / day / "daily_paper_performance.v1.json", {"day_utc": day, "full_portfolio_pnl_status": "NOT_CANONICAL", "total_paper_pnl": "NOT_CANONICAL", "data_quality_status": "DEGRADED"})
        _write(reports / "aegis_sleeve_analytics_v1" / day / "sleeve_analytics.v1.json", {"day_utc": day, "status": "PARTIAL", "summary": {"total_sleeves": 1, "total_open_positions": 1, "total_pnl": 0, "mark_coverage_pct": 0, "sleeve_attribution_coverage_pct": 0, "data_quality_status": "PARTIAL"}, "data_quality": {"missing_mark_count": 1, "data_quality_status": "PARTIAL"}, "sleeves": [{"sleeve_id": "A"}]})


def test_golden_scenarios_pass_with_locked_seed_data(tmp_path: Path) -> None:
    _seed(tmp_path, "2026-05-29", canonical=True)
    _seed(tmp_path, "2026-05-30", canonical=False)

    payload = build_golden_scenarios_v1(truth_root=tmp_path)
    path = write_golden_scenarios_v1(truth_root=tmp_path, payload=payload)
    result = run_golden_scenarios_self_check_v1(truth_root=tmp_path)

    assert path.exists()
    assert payload["status"] == "PASS"
    assert result["ok"] is True
    assert payload["summary"]["scenario_count"] == 2
