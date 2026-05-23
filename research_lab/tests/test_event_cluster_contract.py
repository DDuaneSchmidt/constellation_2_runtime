import pytest

from research_lab.contracts.schemas import validate_contract
from research_lab.event_intake.event_cluster import build_event_cluster
from research_lab.event_intake.event_family import build_event_family
from research_lab.event_intake.event_hypothesis_templates import event_family_spec
from research_lab.event_intake.event_observation import build_event_observation


def _family():
    return build_event_family(event_family_spec("oil_shock"))


def _observation():
    return build_event_observation(
        event_family_id="oil_shock",
        title="Oil shock",
        description="Observed oil ETF shock.",
        source_type="operator_observation",
        symbols_mentioned=["USO", "XLE"],
        suspected_mechanism="headline overreaction",
        confidence_level="medium",
        observed_at="2026-05-20T00:00:00Z",
    )


def test_event_cluster_schema_validates() -> None:
    cluster = build_event_cluster(event_family=_family(), observations=[_observation()], cluster_title="Oil shock cluster", created_at="2026-05-20T00:01:00Z")
    validate_contract("event_cluster", cluster)
    assert cluster["sample_observation_count"] == 1


def test_cluster_with_zero_observations_fails_closed() -> None:
    with pytest.raises(ValueError, match="zero observations"):
        build_event_cluster(event_family=_family(), observations=[], cluster_title="bad")
