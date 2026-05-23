from __future__ import annotations

from pathlib import Path

from research_lab.portfolio.sleeve_comparison import build_sleeve_comparison_report, write_sleeve_comparison_report
from research_lab.tests.test_evidence_inventory import _portfolio_fixture


def test_sleeve_comparison_marks_current_sleeve_watch(tmp_path: Path) -> None:
    store = tmp_path / "store"
    sleeve_id, _ = _portfolio_fixture(store)

    report = build_sleeve_comparison_report(store_root=store, created_at="2024-01-04T00:00:00Z")

    row = report["comparison_rows"][0]
    assert row["sleeve_id"] == sleeve_id
    assert row["overall_health"] == "watch"
    assert row["paper_trial_status"] == "active"
    assert row["priority_bucket"] in {"high", "medium"}
    assert "hypothetical research evidence" in report["research_label"]


def test_sleeve_comparison_write_is_report_only(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _portfolio_fixture(store)
    version_path = next((store / "sleeves" / "slv_fixture" / "versions").glob("*.json"))
    before = version_path.read_text(encoding="utf-8")

    report = write_sleeve_comparison_report(store_root=store)

    assert (store / "portfolio_reports" / "sleeve_comparison" / f"{report['sleeve_comparison_report_id']}.json").exists()
    assert version_path.read_text(encoding="utf-8") == before
