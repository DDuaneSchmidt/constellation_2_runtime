from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ops.aegis.surface_readiness_v1 import build_surface_readiness_v1, run_surface_readiness_self_check_v1, write_surface_readiness_v1

DAY = "2026-05-30"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_minimal(root: Path, day: str = DAY) -> None:
    reports = root / "reports"
    _write(reports / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json", {"day_utc": day, "candidate_ui_projection": {}})
    _write(reports / "aegis_command_center_queue_audit_v1" / day / "command_center_queue_audit.v1.json", {"day_utc": day, "summary": {"operator_action_required_count": 0, "incorrectly_shown_as_awaiting_review_count": 0}, "current_command_center_counts": {"awaiting_review_metric": 0}, "rows": []})
    _write(reports / "aegis_engineering_priority_queue_v1" / day / "engineering_priority_queue.v1.json", {"day_utc": day, "issues": []})
    _write(reports / "aegis_runtime_truth_kernel_v1" / day / "runtime_truth_kernel.v1.json", {"day_utc": day, "highest_readiness_layer": "READY", "runtime_truth_classification": "CANONICAL"})
    _write(reports / "aegis_verified_runtime_graph_v1" / day / "verified_runtime_graph.v1.json", {"day_utc": day, "graph_status": "READY"})
    _write(reports / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json", {"day_utc": day, "open_positions": []})
    _write(reports / "aegis_position_review_context_v1" / day / "position_review_context.v1.json", {"day_utc": day, "contexts": []})
    _write(reports / "aegis_position_review_score_v1" / day / "position_review_score.v1.json", {"day_utc": day, "scores": []})
    _write(reports / "aegis_position_review_brief_v1" / day / "position_review_brief.v1.json", {"day_utc": day, "briefs": []})
    _write(reports / "aegis_sleeve_analytics_v1" / day / "sleeve_analytics.v1.json", {"day_utc": day, "status": "CANONICAL", "summary": {"data_quality_status": "PASS"}, "data_quality": {"data_quality_status": "PASS"}})
    _write(reports / "aegis_ai_operations_context_v1" / day / "ai_operations_context.v1.json", {"day_utc": day, "source_day": day, "included_evidence": []})
    _write(reports / "aegis_ai_operations_response_v1" / day / "ai_operations_response.v1.json", {"day_utc": day, "latest_response": {"context_day": day, "source_day": day}})


def _row(payload: dict, surface_id: str) -> dict:
    return payload["surface_by_id"][surface_id]


def test_surface_readiness_emits_all_required_surfaces(tmp_path: Path) -> None:
    _seed_minimal(tmp_path)
    payload = build_surface_readiness_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["summary"]["surface_count"] == 9
    for surface_id in ["command_center", "engineering", "positions", "history", "performance", "position_review", "sleeve_analytics", "research", "ask_aegis"]:
        assert surface_id in payload["surface_by_id"]
    assert payload["trade_advice_allowed"] is False


def test_wrong_day_artifact_disables_actions(tmp_path: Path) -> None:
    _seed_minimal(tmp_path)
    old = "2026-05-29"
    _write(tmp_path / "reports" / "aegis_command_center_queue_audit_v1" / DAY / "command_center_queue_audit.v1.json", {"day_utc": old, "summary": {"operator_action_required_count": 1}, "rows": []})

    payload = build_surface_readiness_v1(truth_root=tmp_path, day_utc=DAY)
    row = _row(payload, "command_center")

    assert row["surface_status"] in {"BLOCKED", "HISTORICAL", "INCONSISTENT"}
    assert row["actions_allowed"] is False
    assert any("WRONG_DAY" in reason or "SOURCE_DAY_MISMATCH" in reason for reason in row["blocking_reasons"])


def test_missing_position_review_artifacts_are_canonical_unavailable(tmp_path: Path) -> None:
    _seed_minimal(tmp_path)
    (tmp_path / "reports" / "aegis_position_review_brief_v1" / DAY / "position_review_brief.v1.json").unlink()

    payload = build_surface_readiness_v1(truth_root=tmp_path, day_utc=DAY)
    row = _row(payload, "position_review")

    assert row["surface_status"] == "BLOCKED"
    assert row["actions_allowed"] is False
    assert any("MISSING_ARTIFACT:position_review_brief" in reason for reason in row["blocking_reasons"])


def test_ask_aegis_context_day_mismatch_blocks_current_answer(tmp_path: Path) -> None:
    _seed_minimal(tmp_path)
    _write(tmp_path / "reports" / "aegis_ai_operations_context_v1" / DAY / "ai_operations_context.v1.json", {"day_utc": "2026-05-29", "source_day": "2026-05-29"})

    payload = build_surface_readiness_v1(truth_root=tmp_path, day_utc=DAY)
    row = _row(payload, "ask_aegis")

    assert row["surface_status"] in {"BLOCKED", "HISTORICAL", "INCONSISTENT"}
    assert row["actions_allowed"] is False
    assert any("CONTEXT_DAY_MISMATCH" in reason or "SOURCE_DAY_MISMATCH" in reason for reason in row["blocking_reasons"])


def test_not_canonical_pass_sleeve_analytics_contradiction_blocks(tmp_path: Path) -> None:
    _seed_minimal(tmp_path)
    _write(tmp_path / "reports" / "aegis_sleeve_analytics_v1" / DAY / "sleeve_analytics.v1.json", {"day_utc": DAY, "status": "NOT_CANONICAL", "summary": {"data_quality_status": "PASS"}, "data_quality": {"data_quality_status": "PASS"}})

    payload = build_surface_readiness_v1(truth_root=tmp_path, day_utc=DAY)
    row = _row(payload, "sleeve_analytics")

    assert row["surface_status"] == "BLOCKED"
    assert row["actions_allowed"] is False
    assert any("NOT_CANONICAL_WITH_PASS" in reason for reason in row["blocking_reasons"])


def test_write_and_self_check_pass(tmp_path: Path) -> None:
    _seed_minimal(tmp_path)
    write_surface_readiness_v1(truth_root=tmp_path, day_utc=DAY)

    result = run_surface_readiness_self_check_v1(truth_root=tmp_path, day_utc=DAY)

    assert result["ok"] is True
    assert result["surface_count"] == 9
