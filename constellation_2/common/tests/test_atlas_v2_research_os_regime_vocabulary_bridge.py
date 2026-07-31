from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.candidate_backtests import _regime_allowed
from constellation_2.common.atlas_v2_research_os.regime_vocabulary_bridge import (
    NO_EXECUTABLE_REGIME_EQUIVALENT,
    build_regime_vocabulary_bridge_report,
    explain_regime_mapping,
    map_replay_regime_to_research_regime,
    map_research_regime_to_replay_regime,
    mapped_replay_regimes,
    validate_regime_mapping_is_auditable,
    write_regime_vocabulary_bridge_report,
)


def test_chop_maps_to_executable_range_bound_and_back() -> None:
    assert map_research_regime_to_replay_regime("CHOP") == "RANGE_BOUND"
    assert map_replay_regime_to_research_regime("RANGE_BOUND") == "CHOP"

    mapping = explain_regime_mapping("CHOP")
    assert mapping["accepted"] is True
    assert validate_regime_mapping_is_auditable(mapping) is True


def test_unknown_and_trending_are_explicit_self_mappings() -> None:
    assert map_research_regime_to_replay_regime("TRENDING") == "TRENDING"
    assert map_research_regime_to_replay_regime("UNKNOWN") == "UNKNOWN"
    assert map_replay_regime_to_research_regime("TRENDING") == "TRENDING"
    assert map_replay_regime_to_research_regime("UNKNOWN") == "UNKNOWN"


def test_unexecutable_regime_is_rejected_not_force_matched() -> None:
    assert map_research_regime_to_replay_regime("BULL") == NO_EXECUTABLE_REGIME_EQUIVALENT
    mapping = explain_regime_mapping("BULL")
    assert mapping["accepted"] is False
    assert mapping["status"] == NO_EXECUTABLE_REGIME_EQUIVALENT
    assert validate_regime_mapping_is_auditable(mapping) is True


def test_candidate_backtest_regime_allowed_uses_mapped_replay_regimes() -> None:
    allowed, explanations = mapped_replay_regimes(["CHOP"])
    assert allowed == ["RANGE_BOUND"]
    assert explanations[0]["accepted"] is True

    assert _regime_allowed("RANGE_BOUND", {"allowed_replay_regimes": allowed, "primary_replay_regime": "RANGE_BOUND"}) is True
    assert _regime_allowed("TRENDING", {"allowed_replay_regimes": allowed, "primary_replay_regime": "RANGE_BOUND"}) is False


def test_regime_vocabulary_bridge_report_writes_latest(tmp_path: Path) -> None:
    report = build_regime_vocabulary_bridge_report(tmp_path, created_at="2026-06-06T00:00:00Z")
    paths = write_regime_vocabulary_bridge_report(report, root=tmp_path)

    assert paths["latest_json"].exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["report_type"] == "REGIME_VOCABULARY_BRIDGE"
    assert payload["authority_boundary"]["broker_execution_authorized"] is False
    assert any(row["input_regime"] == "CHOP" and row["mapped_regime"] == "RANGE_BOUND" for row in payload["mappings_applied"])
    assert any(row["input_regime"] == "BULL" and row["status"] == NO_EXECUTABLE_REGIME_EQUIVALENT for row in payload["mappings_rejected"])
