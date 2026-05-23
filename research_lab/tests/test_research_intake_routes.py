from pathlib import Path

from ops.aegis.research_lab.research_lab_routes import (
    research_lab_hypothesis_proposal_dossier_v1,
    research_lab_hypothesis_proposal_v1,
    research_lab_hypothesis_proposals_v1,
    research_lab_research_intake_queue_v1,
)
from research_lab.tests._packet30b_helpers import seed_minimum_dataset_and_cost_model, seed_oil_proposal


def test_research_intake_routes_are_read_only(tmp_path: Path) -> None:
    store = tmp_path / "store"
    chain = seed_oil_proposal(store)
    seed_minimum_dataset_and_cost_model(store)
    queue = research_lab_research_intake_queue_v1(store_root=store)
    assert queue["ok"] is True
    assert queue["read_only"] is True
    assert queue["count"] == 1
    listing = research_lab_hypothesis_proposals_v1(store_root=store)
    assert any(row["hypothesis_proposal_id"] == chain["proposal"]["hypothesis_proposal_id"] for row in listing["hypothesis_proposals"])
    detail = research_lab_hypothesis_proposal_v1(hypothesis_proposal_id=chain["proposal"]["hypothesis_proposal_id"], store_root=store)
    assert detail["read_only"] is True
    dossier = research_lab_hypothesis_proposal_dossier_v1(hypothesis_proposal_id=chain["proposal"]["hypothesis_proposal_id"], store_root=store)
    assert dossier["read_only"] is True
    assert dossier["research_intake_dossier"]["hypothesis_proposal_id"] == chain["proposal"]["hypothesis_proposal_id"]


def test_no_execution_or_ohlcv_storage_code_added() -> None:
    text = "\n".join(path.read_text(encoding="utf-8") for path in Path("research_lab/src/research_lab/research_intake").glob("*.py"))
    for marker in ["broker_client", "place_order", "submit_order", "allocate_capital", "optimizer", "sleeve_created\": True", "trade_created\": True"]:
        assert marker not in text
    assert "canonical_daily_bars" not in text
