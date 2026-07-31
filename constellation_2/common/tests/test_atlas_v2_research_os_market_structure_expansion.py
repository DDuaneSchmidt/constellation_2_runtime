from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.market_structure_expansion import (
    build_market_structure_expansion_report,
    write_market_structure_expansion_report,
)

NOW = "2026-06-05T00:00:00Z"


def test_market_structure_expansion_reports_required_metrics(tmp_path: Path) -> None:
    report = build_market_structure_expansion_report(root=tmp_path, created_at=NOW, dry_run_limit=20)
    metrics = report["required_metrics"]

    assert report["schema_id"] == "atlas_v2_research_os_market_structure_expansion_report_v1"
    assert len(report["market_structures_supported"]) == 10
    assert metrics["observations_by_structure"]["INSIDE_DAY"] > 0
    assert metrics["claims_by_structure"]["OUTSIDE_DAY"] > 0
    assert metrics["hypotheses_by_structure"]["COMPRESSION"] >= 0
    assert "positive_replay_rate_by_structure" in metrics
    assert "eligible_candidates_by_structure" in metrics
    assert isinstance(metrics["top_structures_among_winners"], list)
    assert isinstance(metrics["top_structures_among_failures"], list)
    assert report["authority_boundary"]["research_only"] is True
    assert report["authority_boundary"]["capital_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False


def test_market_structure_expansion_writes_requested_paths(tmp_path: Path) -> None:
    paths = write_market_structure_expansion_report(root=tmp_path, day="2026-06-05", created_at=NOW, dry_run_limit=8)

    assert paths["json"].name == "market_structure_expansion_report.json"
    assert paths["summary"].name == "market_structure_expansion_summary.md"
    for path in paths.values():
        assert path.exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["report_type"] == "MARKET_STRUCTURE_EXPANSION"
    assert "Market Structure Expansion" in paths["latest_summary"].read_text(encoding="utf-8")
