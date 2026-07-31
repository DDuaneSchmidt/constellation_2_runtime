from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.regime_expansion import (
    EXPANDED_REGIMES,
    build_regime_expansion_report,
    normalize_regime,
    write_regime_expansion_report,
)

NOW = "2026-06-05T00:00:00Z"


def test_regime_expansion_report_preserves_required_metrics(tmp_path: Path) -> None:
    report = build_regime_expansion_report(root=tmp_path, created_at=NOW, dry_run_limit=12)
    metrics = report["metrics"]

    assert set(EXPANDED_REGIMES).issubset(set(report["canonical_regimes"]))
    assert normalize_regime("HIGH_VOLATILITY") == "HIGH_VOL"
    assert metrics["observations_by_regime"]["HIGH_VOL"] > 0
    assert metrics["claims_by_regime"]["VOL_EXPANSION"] > 0
    assert "hypotheses_by_regime" in metrics
    assert "positive_replay_rate_by_regime" in metrics
    assert "eligible_candidates_by_regime" in metrics
    assert "family_count_by_regime" in metrics
    assert report["authority_boundary"]["research_only"] is True
    assert report["authority_boundary"]["broker_execution_authorized"] is False
    assert report["authority_boundary"]["automatic_paper_trade_placement_authorized"] is False


def test_regime_expansion_writes_required_report_paths(tmp_path: Path) -> None:
    paths = write_regime_expansion_report(root=tmp_path, day="2026-06-05", created_at=NOW, dry_run_limit=8)

    assert paths["json"].name == "regime_expansion_report.json"
    assert paths["summary"].name == "regime_expansion_summary.md"
    assert paths["latest_json"].name == "latest.json"
    assert paths["latest_summary"].name == "latest_summary.md"
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["schema_id"] == "atlas_v2_research_os_regime_expansion_report_v1"
    assert "Regime Expansion" in paths["latest_summary"].read_text(encoding="utf-8")
