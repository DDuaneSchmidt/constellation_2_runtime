from pathlib import Path

import pytest

from research_lab.research_intake.intake_registry import assess_hypothesis_readiness, convert_hypothesis_proposal_to_research_plan, review_hypothesis_proposal
from research_lab.research_intake.proposal_queue import hypothesis_proposal_queue
from research_lab.tests._packet30b_helpers import seed_minimum_dataset_and_cost_model, seed_oil_proposal


def test_queue_lists_proposed_oil_shock_and_conversion_gates(tmp_path: Path) -> None:
    store = tmp_path / "store"
    chain = seed_oil_proposal(store)
    seed_minimum_dataset_and_cost_model(store)
    queue = hypothesis_proposal_queue(status="proposed", store_root=store, audit_view=True)
    assert queue["read_only"] is True
    assert queue["hypothesis_proposals"][0]["hypothesis_proposal_id"] == chain["proposal"]["hypothesis_proposal_id"]
    with pytest.raises(RuntimeError, match="--approve true"):
        convert_hypothesis_proposal_to_research_plan(hypothesis_proposal_id=chain["proposal"]["hypothesis_proposal_id"], approve=False, store_root=store, actor="Tester")
    with pytest.raises(RuntimeError, match="accepted_for_research"):
        convert_hypothesis_proposal_to_research_plan(hypothesis_proposal_id=chain["proposal"]["hypothesis_proposal_id"], approve=True, store_root=store, actor="Tester")
    assess_hypothesis_readiness(hypothesis_proposal_id=chain["proposal"]["hypothesis_proposal_id"], store_root=store, actor="Tester")
    review_hypothesis_proposal(hypothesis_proposal_id=chain["proposal"]["hypothesis_proposal_id"], decision="watchlist", reason="Needs data.", reviewed_by="operator", store_root=store, actor="Tester")
    with pytest.raises(RuntimeError, match="accepted_for_research"):
        convert_hypothesis_proposal_to_research_plan(hypothesis_proposal_id=chain["proposal"]["hypothesis_proposal_id"], approve=True, store_root=store, actor="Tester")
