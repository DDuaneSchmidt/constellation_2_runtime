from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ops.aegis.operator_surface_contract_v1 import build_operator_surface_contract_v1, run_operator_surface_contract_self_check_v1, write_operator_surface_contract_v1

DAY = "2026-05-30"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(root: Path) -> None:
    reports = root / "reports"
    surfaces = ["command_center", "engineering", "positions", "history", "performance", "position_review", "sleeve_analytics", "research", "ask_aegis"]
    surface_by_id = {}
    for surface in surfaces:
        surface_by_id[surface] = {
            "surface_id": surface,
            "requested_day": DAY,
            "source_day": DAY,
            "artifact_days": {},
            "context_day": DAY if surface == "ask_aegis" else "",
            "render_allowed": True,
            "actions_allowed": surface == "command_center",
            "surface_status": "READY",
            "blocking_reasons": [],
            "warning_reasons": [],
            "source_artifacts": [{"artifact_id": f"{surface}_artifact", "path": f"/tmp/{surface}.json", "artifact_day": DAY, "status": "AVAILABLE", "content_hash": "abc"}],
            "source_artifact_hashes": {f"{surface}_artifact": "abc"},
            "generated_at": "2026-05-30T00:00:00Z",
            "details": {"operator_action_required_count": 1} if surface == "command_center" else {},
        }
    surface_by_id["performance"]["surface_status"] = "DEGRADED"
    surface_by_id["performance"]["actions_allowed"] = False
    surface_by_id["performance"]["warning_reasons"] = ["PERFORMANCE_INPUT_DEGRADED:paper_pnl_report:NOT_CANONICAL"]
    _write(reports / "aegis_surface_readiness_v1" / DAY / "surface_readiness.v1.json", {"day_utc": DAY, "surfaces": list(surface_by_id.values()), "surface_by_id": surface_by_id, "summary": {}})
    _write(reports / "aegis_semantic_invariants_v1" / DAY / "semantic_invariants.v1.json", {"day_utc": DAY, "status": "PASS", "summary": {"blocking_failure_count": 0}, "surfaces": {"performance": {"status": "PASS", "failed_invariants": []}, "sleeve_analytics": {"status": "PASS", "failed_invariants": []}}, "invariants": []})


def test_operator_surface_contract_has_required_rows_and_fields(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_operator_surface_contract_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["summary"]["surface_count"] == 9
    for surface_id in ["command_center", "engineering", "positions", "history", "performance", "position_review", "sleeve_analytics", "research", "ask_aegis"]:
        row = payload["contract_by_id"][surface_id]
        assert row["status"] in {"READY", "DEGRADED", "BLOCKED", "UNAVAILABLE", "HISTORICAL", "INCONSISTENT"}
        assert row["primary_message"]
        assert row["reason"]
        assert row["impact"]
        assert row["next_step"]
        assert row["ask_aegis_prompt"]
        assert isinstance(row["artifact_refs"], list)


def test_semantic_failure_blocks_metrics_and_actions(tmp_path: Path) -> None:
    _seed(tmp_path)
    semantic = json.loads((tmp_path / "reports" / "aegis_semantic_invariants_v1" / DAY / "semantic_invariants.v1.json").read_text())
    semantic["surfaces"]["sleeve_analytics"] = {"status": "FAIL", "failed_invariants": [{"surface_id": "sleeve_analytics", "invariant_id": "sleeve_total_sleeves_zero_attribution_complete", "status": "FAIL", "blocking": True, "reason": "Total sleeves is zero but attribution coverage is 100%.", "field_values": {}}]}
    _write(tmp_path / "reports" / "aegis_semantic_invariants_v1" / DAY / "semantic_invariants.v1.json", semantic)

    payload = build_operator_surface_contract_v1(truth_root=tmp_path, day_utc=DAY)
    row = payload["contract_by_id"]["sleeve_analytics"]

    assert row["status"] == "BLOCKED"
    assert row["actions_allowed"] is False
    assert row["metrics_allowed"] is False
    assert "SEMANTIC_INVARIANT_FAILED" in row["reason"]


def test_self_check_passes_contract_and_ui_helpers(tmp_path: Path) -> None:
    _seed(tmp_path)
    write_operator_surface_contract_v1(truth_root=tmp_path, day_utc=DAY)

    result = run_operator_surface_contract_self_check_v1(truth_root=tmp_path, day_utc=DAY, repo_root=REPO)

    assert result["ok"] is True
    assert result["surface_count"] == 9
