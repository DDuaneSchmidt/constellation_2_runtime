from pathlib import Path

from research_lab.event_intake.event_cluster import build_event_cluster
from research_lab.event_intake.event_family import build_event_family
from research_lab.event_intake.event_hypothesis_templates import event_family_spec, intent_template
from research_lab.event_intake.event_observation import build_event_observation
from research_lab.event_intake.hypothesis_proposal import build_hypothesis_proposal
from research_lab.event_intake.intent_candidate import build_intent_candidate


def test_event_intake_hashes_are_deterministic_except_timestamps() -> None:
    family = build_event_family(event_family_spec("oil_shock"))
    obs_a = build_event_observation(event_family_id="oil_shock", title="Obs", description="Desc", source_type="operator_observation", symbols_mentioned=["XLE", "USO"], confidence_level="medium", observed_at="2026-05-20T00:00:00Z")
    obs_b = build_event_observation(event_family_id="oil_shock", title="Obs", description="Desc", source_type="operator_observation", symbols_mentioned=["USO", "XLE"], confidence_level="medium", observed_at="2026-05-21T00:00:00Z")
    assert obs_a["content_hash"] == obs_b["content_hash"]
    cluster_a = build_event_cluster(event_family=family, observations=[obs_a], cluster_title="Cluster", created_at="2026-05-20T00:01:00Z")
    cluster_b = build_event_cluster(event_family=family, observations=[obs_a], cluster_title="Cluster", created_at="2026-05-21T00:01:00Z")
    assert cluster_a["content_hash"] == cluster_b["content_hash"]
    template = intent_template("oil_shock")
    intent_a = build_intent_candidate(event_cluster=cluster_a, template=template, created_at="2026-05-20T00:02:00Z")
    intent_b = build_intent_candidate(event_cluster=cluster_a, template=template, created_at="2026-05-21T00:02:00Z")
    assert intent_a["content_hash"] == intent_b["content_hash"]
    proposal_a = build_hypothesis_proposal(intent_candidate=intent_a, event_family=family, template=template, created_at="2026-05-20T00:03:00Z")
    proposal_b = build_hypothesis_proposal(intent_candidate=intent_a, event_family=family, template=template, created_at="2026-05-21T00:03:00Z")
    assert proposal_a["content_hash"] == proposal_b["content_hash"]
