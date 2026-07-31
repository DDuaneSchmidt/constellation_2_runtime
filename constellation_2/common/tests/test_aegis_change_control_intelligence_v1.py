from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.aegis.change_control_intelligence_v1 import (  # noqa: E402
    build_all_v1,
    build_advisor_score_v1,
    build_ai_review_v1,
    build_evidence_snapshot_v1,
    validate_intelligence_v1,
)
from ops.aegis.change_control_v1 import REGISTER_PATH  # noqa: E402


def _truth_root(tmp_path: Path) -> Path:
    root = tmp_path / "truth"
    root.mkdir()
    return root


def test_evidence_snapshot_includes_blocked_parent_required_children_and_current_register(tmp_path: Path) -> None:
    snapshot = build_evidence_snapshot_v1(truth_root=_truth_root(tmp_path), day="2026-05-30")

    assert snapshot["schema_id"] == "aegis_change_control_evidence_snapshot_v1"
    assert snapshot["register_source_path"].endswith("aegis_change_control_register_v1.json")
    assert snapshot["register_validation"]["ok"] is True
    assert any(row.get("id") == "ACC-20260530-008" for row in snapshot["blocked_parent_records"])
    child_ids = {row.get("id") for row in snapshot["required_child_records"]}
    assert {"ACC-20260530-008A", "ACC-20260530-008B", "ACC-20260530-008C", "ACC-20260530-008D"}.issubset(child_ids)
    rollup = next(row for row in snapshot["incomplete_child_rollups"] if row["parent_id"] == "ACC-20260530-008")
    assert rollup["completion_rollup"] == "0/4 complete"


def test_deterministic_advisor_ranks_008a_as_top_and_score_is_reproducible(tmp_path: Path) -> None:
    snapshot = build_evidence_snapshot_v1(truth_root=_truth_root(tmp_path), day="2026-05-30")
    advisor_one = build_advisor_score_v1(snapshot=snapshot)
    advisor_two = build_advisor_score_v1(snapshot=snapshot)

    assert advisor_one["top_recommended_record_id"] == "ACC-20260530-008A"
    assert advisor_two["top_recommended_record_id"] == "ACC-20260530-008A"
    assert advisor_one["scores"] == advisor_two["scores"]
    top = advisor_one["scores"][0]
    assert top["recommendation_category"] == "FIX_BLOCKING_CHILD"
    assert "ACC-20260530-008" in top["blocks_records"]
    assert top["score_components"]["required_child_blocker_weight"] > 0


def test_ai_review_is_advisory_only_and_cannot_mutate_change_control(tmp_path: Path) -> None:
    snapshot = build_evidence_snapshot_v1(truth_root=_truth_root(tmp_path), day="2026-05-30")
    advisor = build_advisor_score_v1(snapshot=snapshot)
    review = build_ai_review_v1(snapshot=snapshot, advisor=advisor)

    assert review["mutation_performed"] is False
    assert {"APPROVE", "REJECT", "DEFER", "PRIORITIZE", "VALIDATE", "CLOSE", "IMPLEMENT", "MUTATE_RECORD"}.issubset(set(review["forbidden_actions"]))
    assert review["recommended_next_actions"][0]["advisory_only"] is True
    assert "Research Validation Engine cannot be validated" in review["plain_english_summary"]
    assert validate_intelligence_v1(snapshot, advisor, review)["ok"] is True


def test_stale_snapshot_is_flagged_when_register_hash_changes(tmp_path: Path) -> None:
    register_copy = tmp_path / "register.json"
    shutil.copyfile(REGISTER_PATH, register_copy)
    snapshot = build_evidence_snapshot_v1(truth_root=_truth_root(tmp_path), day="2026-05-30", register_path=register_copy)

    payload = json.loads(register_copy.read_text(encoding="utf-8"))
    payload["updated_at"] = "2099-01-01T00:00:00Z"
    register_copy.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    advisor = build_advisor_score_v1(snapshot=snapshot, current_register_path=register_copy)
    review = build_ai_review_v1(snapshot=snapshot, advisor=advisor)
    validation = validate_intelligence_v1(snapshot, advisor, review, current_register_path=register_copy)

    assert advisor["stale_snapshot_flag"] is True
    assert validation["ok"] is True



def test_change_control_intelligence_build_all_writes_current_artifacts(tmp_path: Path) -> None:
    root = _truth_root(tmp_path)
    result = build_all_v1(truth_root=root, day="2026-05-31", write=True)

    assert result["ok"] is True
    for key in ("snapshot", "advisor_score", "ai_review"):
        path = Path(result["paths"][key])
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert path.exists()
        assert payload.get("target_day") == "2026-05-31" or payload.get("day_utc") == "2026-05-31"
        assert payload.get("generated_at")
    validation = validate_intelligence_v1(result["snapshot"], result["advisor_score"], result["ai_review"])
    assert validation["ok"] is True
