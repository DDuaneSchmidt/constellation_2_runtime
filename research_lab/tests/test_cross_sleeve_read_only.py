from __future__ import annotations

from pathlib import Path

from research_lab.portfolio.backlog_priority import write_research_backlog_priority_report
from research_lab.portfolio.evidence_inventory import write_evidence_inventory
from research_lab.portfolio.paper_trial_inventory import write_paper_trial_inventory
from research_lab.portfolio.sleeve_comparison import write_sleeve_comparison_report
from research_lab.tests.test_aegis_reference_boundary import test_aegis_reference_boundary_has_no_canonical_ohlcv_table
from research_lab.tests.test_evidence_inventory import _portfolio_fixture
from ops.aegis.research_lab.research_lab_routes import (
    research_lab_backlog_priority_v1,
    research_lab_evidence_inventory_v1,
    research_lab_paper_trial_inventory_v1,
    research_lab_sleeve_comparison_v1,
)


def test_cross_sleeve_reports_do_not_mutate_research_store_artifacts(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _portfolio_fixture(store)
    protected_paths = [
        store / "sleeves" / "slv_fixture" / "sleeve_definition.json",
        next((store / "sleeves" / "slv_fixture" / "versions").glob("*.json")),
        store / "candidate_batches" / "cb_fixture" / "generation_summary.json",
        store / "paper_trials" / "ptr_fixture" / "paper_trial_summary.json",
    ]
    before = {path: path.read_text(encoding="utf-8") for path in protected_paths}

    write_evidence_inventory(store_root=store)
    write_paper_trial_inventory(store_root=store)
    write_sleeve_comparison_report(store_root=store)
    write_research_backlog_priority_report(store_root=store)

    assert {path: path.read_text(encoding="utf-8") for path in protected_paths} == before


def test_cross_sleeve_reports_keep_aegis_reference_boundary() -> None:
    test_aegis_reference_boundary_has_no_canonical_ohlcv_table()


def test_cross_sleeve_aegis_routes_are_read_only(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _portfolio_fixture(store)
    write_evidence_inventory(store_root=store)
    write_paper_trial_inventory(store_root=store)
    write_sleeve_comparison_report(store_root=store)
    write_research_backlog_priority_report(store_root=store)
    protected_path = store / "paper_trials" / "ptr_fixture" / "paper_trial_summary.json"
    before = protected_path.read_text(encoding="utf-8")

    assert research_lab_evidence_inventory_v1(store_root=store)["read_only"] is True
    assert research_lab_paper_trial_inventory_v1(store_root=store)["read_only"] is True
    assert research_lab_sleeve_comparison_v1(store_root=store)["read_only"] is True
    assert research_lab_backlog_priority_v1(store_root=store)["read_only"] is True
    assert protected_path.read_text(encoding="utf-8") == before
