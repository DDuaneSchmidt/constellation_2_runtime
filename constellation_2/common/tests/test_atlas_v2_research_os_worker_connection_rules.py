from constellation_2.common.atlas_v2_research_os.worker_adapters import AtlasClaimWorkerAdapter
from constellation_2.common.atlas_v2_research_os.worker_connection_rules import (
    is_output_artifact_allowed,
    is_side_effect_allowed,
    is_worker_connection_allowed,
)


def test_allowed_and_forbidden_artifacts_fail_closed():
    assert is_output_artifact_allowed("GeneratedResearchClaim") is True
    assert is_output_artifact_allowed("ResearchHypothesis") is True
    assert is_output_artifact_allowed("LiveTrade") is False
    assert is_output_artifact_allowed("") is False
    assert is_output_artifact_allowed({}) is False


def test_side_effect_rules_block_authority_expansion():
    assert is_side_effect_allowed("generate_research_artifact") is True
    assert is_side_effect_allowed("update_memory") is True
    assert is_side_effect_allowed("authorize_capital") is False
    assert is_side_effect_allowed("broker_execution") is False


def test_connected_worker_allowed():
    assert is_worker_connection_allowed(AtlasClaimWorkerAdapter()) is True
