from __future__ import annotations

from pathlib import Path

from research_lab.portfolio.paper_trial_inventory import build_paper_trial_inventory, write_paper_trial_inventory
from research_lab.tests.test_evidence_inventory import _portfolio_fixture


def test_paper_trial_inventory_includes_active_trial(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _portfolio_fixture(store)

    inventory = build_paper_trial_inventory(store_root=store, created_at="2024-01-04T00:00:00Z")

    assert inventory["active_trials"] == 1
    assert inventory["trials"][0]["paper_trial_id"] == "ptr_fixture"
    assert inventory["trials"][0]["recommended_next_action"] == "record_more_observations"
    assert "No sleeve mutation" in inventory["research_label"]


def test_paper_trial_inventory_write_appends_registry(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _portfolio_fixture(store)

    inventory = write_paper_trial_inventory(store_root=store)

    assert (store / "portfolio_reports" / "paper_trial_inventory" / f"{inventory['paper_trial_inventory_id']}.json").exists()
    assert (store / "registries" / "paper_trial_inventories.jsonl").exists()
