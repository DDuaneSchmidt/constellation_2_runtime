from __future__ import annotations

from pathlib import Path

from research_lab.portfolio.backlog_priority import build_research_backlog_priority_report, write_research_backlog_priority_report
from research_lab.tests.test_evidence_inventory import _portfolio_fixture


def test_research_backlog_priority_recommends_more_observations(tmp_path: Path) -> None:
    store = tmp_path / "store"
    sleeve_id, _ = _portfolio_fixture(store)

    report = build_research_backlog_priority_report(store_root=store, created_at="2024-01-04T00:00:00Z")

    top = report["priority_rows"][0]
    assert top["sleeve_id"] == sleeve_id
    assert top["priority_bucket"] in {"high", "medium"}
    assert top["recommended_next_action"] == "record_more_observations"
    assert "active paper trial" in top["reason"].lower()


def test_research_backlog_priority_write_appends_registry(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _portfolio_fixture(store)

    report = write_research_backlog_priority_report(store_root=store)

    assert (store / "portfolio_reports" / "backlog_priority" / f"{report['research_backlog_priority_report_id']}.json").exists()
    assert (store / "registries" / "research_backlog_priority_reports.jsonl").exists()
