import pytest

from research_lab.contracts.schemas import validate_contract
from research_lab.event_intake.event_observation import build_event_observation


def test_event_observation_schema_validates() -> None:
    observation = build_event_observation(
        event_family_id="oil_shock",
        title="Oil headline shock observation",
        description="Operator observed a large public-event oil dislocation.",
        source_type="operator_observation",
        symbols_mentioned=["USO", "XLE"],
        suspected_mechanism="headline overreaction",
        confidence_level="medium",
        observed_at="2026-05-20T00:00:00Z",
    )
    validate_contract("event_observation", observation)


def test_social_media_claim_defaults_and_validates_low_confidence() -> None:
    observation = build_event_observation(
        event_family_id="oil_shock",
        title="Viral oil claim",
        description="A social post made an unverified front-running claim.",
        source_type="social_media_claim",
        symbols_mentioned=["USO"],
        observed_at="2026-05-20T00:00:00Z",
    )
    assert observation["confidence_level"] == "low"
    assert "unverified_claim" in observation["notes"]
    validate_contract("event_observation", observation)


def test_social_media_claim_rejects_non_low_confidence() -> None:
    with pytest.raises(ValueError, match="low confidence"):
        build_event_observation(
            event_family_id="oil_shock",
            title="Viral oil claim",
            description="Unverified claim.",
            source_type="social_media_claim",
            confidence_level="high",
            observed_at="2026-05-20T00:00:00Z",
        )
