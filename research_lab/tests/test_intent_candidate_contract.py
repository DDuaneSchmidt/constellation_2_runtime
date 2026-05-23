import pytest

from research_lab.contracts.schemas import validate_contract
from research_lab.event_intake.event_cluster import build_event_cluster
from research_lab.event_intake.event_family import build_event_family
from research_lab.event_intake.event_hypothesis_templates import event_family_spec, intent_template
from research_lab.event_intake.event_observation import build_event_observation
from research_lab.event_intake.intent_candidate import build_intent_candidate


def _cluster(family_id: str):
    family = build_event_family(event_family_spec(family_id))
    observation = build_event_observation(
        event_family_id=family_id,
        title="Observation",
        description="Research intake observation.",
        source_type="operator_observation",
        symbols_mentioned=family["default_symbols"][:2],
        confidence_level="medium",
        observed_at="2026-05-20T00:00:00Z",
    )
    return build_event_cluster(event_family=family, observations=[observation], cluster_title="Cluster", created_at="2026-05-20T00:01:00Z")


def test_intent_candidate_schema_validates() -> None:
    cluster = _cluster("volatility_spike")
    intent = build_intent_candidate(event_cluster=cluster, template=intent_template("volatility_spike"), created_at="2026-05-20T00:02:00Z")
    validate_contract("intent_candidate", intent)
    assert intent["candidate_edge_type"] == "mean_reversion"


def test_lottery_events_require_extreme_fragility_and_stricter_governance_notes() -> None:
    cluster = _cluster("lottery_event")
    intent = build_intent_candidate(event_cluster=cluster, template=intent_template("lottery_event"), created_at="2026-05-20T00:02:00Z")
    assert intent["expected_fragility"] == "extreme"
    assert "stricter_review_required" in intent["governance_notes"]
    bad = dict(intent_template("lottery_event"), expected_fragility="high")
    with pytest.raises(ValueError, match="lottery event"):
        build_intent_candidate(event_cluster=cluster, template=bad, created_at="2026-05-20T00:03:00Z")
