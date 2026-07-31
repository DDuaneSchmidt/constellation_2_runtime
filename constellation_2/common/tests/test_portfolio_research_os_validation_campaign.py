from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.portfolio_research_os.validation_campaign import run_validation_campaign_001


def test_portfolio_atlas_is_separate_from_trade_atlas(tmp_path: Path) -> None:
    report = run_validation_campaign_001(tmp_path)

    assert report["namespace"] == "constellation_2.common.portfolio_research_os"
    assert report["separate_from_trade_atlas"] is True
    assert report["trade_atlas_namespace"] == "constellation_2.common.atlas_v2_research_os"
    assert "atlas_v2_research_os" not in report["report_root"]


def test_v1_exclusions_and_no_authority_are_enforced(tmp_path: Path) -> None:
    report = run_validation_campaign_001(tmp_path)

    for excluded in [
        "options overlays",
        "international equities",
        "adaptive factor weights",
        "AI prediction systems",
        "broker execution",
        "real recommendations",
    ]:
        assert excluded in report["v1_excluded"]

    authority = report["authority_boundary"]
    assert authority["research_only"] is True
    assert authority["trading"] is False
    assert authority["broker_execution"] is False
    assert authority["capital_allocation"] is False
    assert authority["recommendations"] is False

    scope_rows = _read_csv(tmp_path / "validation_campaign_001" / "scope_lock.csv")
    excluded_rows = {row["scope_item"]: row for row in scope_rows if row["scope_type"] == "EXCLUDED"}
    assert excluded_rows["broker execution"]["classification"] == "DESIGN_LOCKED"
    assert excluded_rows["real recommendations"]["classification"] == "DESIGN_LOCKED"


def test_required_reports_and_benchmark_stack_are_written(tmp_path: Path) -> None:
    run_validation_campaign_001(tmp_path)

    report_dir = tmp_path / "validation_campaign_001"
    for filename in [
        "latest.json",
        "latest_summary.md",
        "benchmark_stack.csv",
        "scope_lock.csv",
        "research_questions.csv",
        "implementation_readiness.csv",
    ]:
        assert (report_dir / filename).exists()

    latest = json.loads((report_dir / "latest.json").read_text())
    assert latest["design_lock_classification"] == "DESIGN_LOCKED"
    assert latest["validation_status"] == "VALIDATION_NOT_STARTED"

    benchmark_rows = _read_csv(report_dir / "benchmark_stack.csv")
    benchmark_ids = {row["benchmark_id"] for row in benchmark_rows}
    assert {"VTI", "SIXTY_FORTY", "SIMPLE_FACTOR_PORTFOLIO", "OAK_HARVEST_PROXY"} <= benchmark_ids


def test_validation_rules_include_bias_controls_and_walk_forward(tmp_path: Path) -> None:
    report = run_validation_campaign_001(tmp_path)
    rules_text = "\n".join(report["validation_rules"]).lower()

    assert "survivorship bias" in rules_text
    assert "lookahead bias" in rules_text
    assert "future index membership leakage" in rules_text
    assert "no optimized factor weights" in rules_text
    assert "no cherry-picked benchmark" in rules_text
    assert "no single-metric victory" in rules_text
    assert "walk-forward" in rules_text
    assert "gap/embargo" in rules_text


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))

