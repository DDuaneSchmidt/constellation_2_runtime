from pathlib import Path

from research_lab.contracts.schemas import validate_contract
from research_lab.research_intake.intake_registry import assess_hypothesis_readiness, proposal_context, max_confidence, score_hypothesis_proposal
from research_lab.research_intake.proposal_priority import build_proposal_priority_score
from research_lab.tests._packet30b_helpers import seed_minimum_dataset_and_cost_model, seed_oil_proposal


def test_proposal_priority_score_schema_validates_and_is_deterministic(tmp_path: Path) -> None:
    store = tmp_path / "store"
    chain = seed_oil_proposal(store)
    seed_minimum_dataset_and_cost_model(store)
    readiness = assess_hypothesis_readiness(hypothesis_proposal_id=chain["proposal"]["hypothesis_proposal_id"], store_root=store, actor="Tester")["readiness_assessment"]
    context = proposal_context(chain["proposal"]["hypothesis_proposal_id"], store_root=store)
    first = build_proposal_priority_score(proposal=context["proposal"], readiness=readiness, confidence_level=max_confidence(context["observations"]), scored_at="2026-05-20T00:00:00Z")
    second = build_proposal_priority_score(proposal=context["proposal"], readiness=readiness, confidence_level=max_confidence(context["observations"]), scored_at="2026-05-21T00:00:00Z")
    validate_contract("proposal_priority_score", first)
    assert first["content_hash"] == second["content_hash"]
    assert first["priority_bucket"] == "blocked"
