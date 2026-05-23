from pathlib import Path

from research_lab.contracts.schemas import validate_contract
from research_lab.research_intake.intake_registry import assess_hypothesis_readiness
from research_lab.tests._packet30b_helpers import seed_minimum_dataset_and_cost_model, seed_oil_proposal


def test_research_readiness_assessment_schema_validates(tmp_path: Path) -> None:
    store = tmp_path / "store"
    chain = seed_oil_proposal(store)
    seed_minimum_dataset_and_cost_model(store)
    result = assess_hypothesis_readiness(hypothesis_proposal_id=chain["proposal"]["hypothesis_proposal_id"], store_root=store, actor="Tester")
    assessment = result["readiness_assessment"]
    validate_contract("research_readiness_assessment", assessment)
    assert assessment["ready_for_research"] is False
    assert "missing_required_symbols" in assessment["blocking_items"]
    assert set(assessment["missing_symbols"]) == {"USO", "XLE", "XOP", "DBC"}
