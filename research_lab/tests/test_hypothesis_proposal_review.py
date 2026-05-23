from pathlib import Path

import pytest

from research_lab.contracts.schemas import validate_contract
from research_lab.research_intake.intake_registry import assess_hypothesis_readiness, review_hypothesis_proposal
from research_lab.storage.manifest_io import read_json, read_jsonl
from research_lab.tests._packet30b_helpers import seed_minimum_dataset_and_cost_model, seed_oil_proposal


def test_hypothesis_proposal_review_schema_and_gate(tmp_path: Path) -> None:
    store = tmp_path / "store"
    chain = seed_oil_proposal(store)
    seed_minimum_dataset_and_cost_model(store)
    proposal_path = store / "event_intake" / "hypothesis_proposals" / f"{chain['proposal']['hypothesis_proposal_id']}.json"
    before = read_json(proposal_path)
    assess_hypothesis_readiness(hypothesis_proposal_id=chain["proposal"]["hypothesis_proposal_id"], store_root=store, actor="Tester")
    with pytest.raises(RuntimeError, match="ready_for_research=false"):
        review_hypothesis_proposal(hypothesis_proposal_id=chain["proposal"]["hypothesis_proposal_id"], decision="accept_for_research", reason="not ready", reviewed_by="operator", store_root=store, actor="Tester")
    result = review_hypothesis_proposal(hypothesis_proposal_id=chain["proposal"]["hypothesis_proposal_id"], decision="watchlist", reason="Interesting but needs data.", reviewed_by="operator", store_root=store, actor="Tester")
    review = result["hypothesis_proposal_review"]
    validate_contract("hypothesis_proposal_review", review)
    assert review["next_status"] == "watchlist"
    assert read_json(proposal_path) == before
    assert read_jsonl(store / "registries" / "hypothesis_proposal_reviews.jsonl")[-1]["review_decision"] == "watchlist"
