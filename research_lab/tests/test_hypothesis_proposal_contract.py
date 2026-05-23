from research_lab.contracts.schemas import validate_contract
from research_lab.event_intake.event_cluster import build_event_cluster
from research_lab.event_intake.event_family import build_event_family
from research_lab.event_intake.event_hypothesis_templates import event_family_spec, intent_template
from research_lab.event_intake.event_observation import build_event_observation
from research_lab.event_intake.hypothesis_proposal import build_hypothesis_proposal
from research_lab.event_intake.intent_candidate import build_intent_candidate


def _proposal(family_id: str):
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
    cluster = build_event_cluster(event_family=family, observations=[observation], cluster_title="Cluster", created_at="2026-05-20T00:01:00Z")
    template = intent_template(family_id)
    intent = build_intent_candidate(event_cluster=cluster, template=template, created_at="2026-05-20T00:02:00Z")
    return build_hypothesis_proposal(intent_candidate=intent, event_family=family, template=template, created_at="2026-05-20T00:03:00Z")


def test_hypothesis_proposal_schema_validates() -> None:
    proposal = _proposal("oil_shock")
    validate_contract("hypothesis_proposal", proposal)
    assert proposal["proposal_status"] == "proposed"
    assert proposal["research_label"] == "RESEARCH_ONLY"
    assert proposal["non_approval_assertion"]["sleeve_created"] is False
