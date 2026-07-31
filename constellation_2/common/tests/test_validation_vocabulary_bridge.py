from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.validation_vocabulary_bridge import (
    STATUS_GENERATED_ONLY,
    VocabularyBridgeMatch,
    build_validation_vocabulary_bridge,
    write_validation_vocabulary_bridge,
)


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def test_bridge_contracts_and_initial_mappings(tmp_path: Path) -> None:
    _write_latest(
        tmp_path,
        "direct_candidate_data_validation",
        {
            "candidate_validations": [
                {"candidate_id": "c_trend", "regime": "TREND"},
                {"candidate_id": "c_chop", "regime": "CHOP"},
                {"candidate_id": "c_breakout", "regime": "BREAKOUT"},
                {"candidate_id": "c_trending", "regime": "TRENDING"},
            ]
        },
    )

    result = build_validation_vocabulary_bridge(tmp_path, created_at="2026-06-05T00:00:00Z")
    matches = {row.candidate_id: row for row in result.matches}

    assert isinstance(matches["c_chop"], VocabularyBridgeMatch)
    assert matches["c_trend"].mapping_type == "EXACT_MATCH"
    assert matches["c_trend"].validator_regime == "TRENDING"
    assert matches["c_chop"].mapping_type == "PARTIAL_MATCH"
    assert matches["c_chop"].validator_regime == "RANGE_BOUND"
    assert matches["c_breakout"].mapping_type == "UNMAPPABLE"
    assert matches["c_trending"].mapping_type == "EXACT_MATCH"
    assert result.status == STATUS_GENERATED_ONLY


def test_bridge_is_generated_only_and_preserves_authority_boundaries(tmp_path: Path) -> None:
    _write_latest(tmp_path, "direct_candidate_data_validation", {"candidate_validations": [{"candidate_id": "c1", "regime": "CHOP"}]})

    result = build_validation_vocabulary_bridge(tmp_path, created_at="2026-06-05T00:00:00Z")
    payload = result.to_dict()

    assert payload["status"] == "GENERATED_ONLY"
    assert payload["authority_boundary"]["evaluation_overlay_only"] is True
    assert payload["authority_boundary"]["replay_behavior_changed"] is False
    assert payload["authority_boundary"]["qualification_changed"] is False
    assert payload["authority_boundary"]["candidate_changed"] is False
    assert payload["authority_boundary"]["governance_changed"] is False
    assert payload["authority_boundary"]["broker_execution_authorized"] is False
    assert payload["authority_boundary"]["capital_authorized"] is False
    assert payload["authority_boundary"]["position_sizing_authorized"] is False
    assert payload["authority_boundary"]["automatic_paper_trade_placement_authorized"] is False


def test_bridge_writes_json_and_markdown_outputs(tmp_path: Path) -> None:
    _write_latest(tmp_path, "direct_candidate_data_validation", {"candidate_validations": [{"candidate_id": "c1", "regime": "CHOP"}]})
    result = build_validation_vocabulary_bridge(tmp_path, created_at="2026-06-05T00:00:00Z")

    paths = write_validation_vocabulary_bridge(result, root=tmp_path)

    assert paths["json"].name == "bridge_result.json"
    assert paths["summary"].name == "bridge_result.md"
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["status"] == "GENERATED_ONLY"
    assert "Authority Boundary" in paths["summary"].read_text(encoding="utf-8")
