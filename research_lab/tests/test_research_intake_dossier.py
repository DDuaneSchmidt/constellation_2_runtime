from pathlib import Path

from research_lab.contracts.schemas import validate_contract
from research_lab.research_intake.intake_dossier import RESEARCH_INTAKE_LABEL
from research_lab.research_intake.intake_registry import build_and_store_research_intake_dossier
from research_lab.tests._packet30b_helpers import seed_minimum_dataset_and_cost_model, seed_oil_proposal


def test_research_intake_dossier_schema_and_contents(tmp_path: Path) -> None:
    store = tmp_path / "store"
    chain = seed_oil_proposal(store)
    seed_minimum_dataset_and_cost_model(store)
    result = build_and_store_research_intake_dossier(hypothesis_proposal_id=chain["proposal"]["hypothesis_proposal_id"], store_root=store, actor="Tester")
    dossier = result["research_intake_dossier"]
    validate_contract("research_intake_dossier", dossier)
    assert dossier["event_observation_ids"] == [chain["observation"]["event_observation_id"]]
    assert dossier["intent_candidate_id"] == chain["intent"]["intent_candidate_id"]
    assert dossier["hypothesis"] == chain["proposal"]["hypothesis"]
    assert dossier["readiness_assessment_id"]
    assert dossier["proposal_priority_score_id"]
    assert dossier["research_label"] == RESEARCH_INTAKE_LABEL
    assert "No broker execution" in result["markdown"]
