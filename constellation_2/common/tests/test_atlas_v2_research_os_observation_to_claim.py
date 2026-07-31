from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.artifact_store import ArtifactStore
from constellation_2.common.atlas_v2_research_os.observation_to_claim import convert_observation_cluster_to_claim, create_backlog_items_from_observations

NOW = "2026-06-05T00:00:00Z"


def test_converts_observation_cluster_to_claim_seed() -> None:
    cluster = {
        "cluster_id": "obs_cluster_1",
        "mechanism": "OPENING_RANGE",
        "market_structure": "FAILED_BREAKOUT",
        "regime": "TRENDING",
        "symbols": ["SPY"],
        "timeframes": ["5m"],
        "source_observation_ids": ["obs-1", "obs-2"],
        "cluster_summary": "opening range observations",
        "confidence": 0.66,
        "metadata": {"measurement_only": True},
    }
    claim = convert_observation_cluster_to_claim(cluster)
    assert claim["source_observation_ids"] == ["obs-1", "obs-2"]
    assert claim["mechanism"] == "OPENING_RANGE"
    assert claim["market_structure"] == "FAILED_BREAKOUT"
    assert "FAILED_BREAKOUT" in claim["claim_text"]
    assert "TRENDING" in claim["claim_text"]


def test_creates_claim_investigation_backlog_items(tmp_path) -> None:
    claim = {
        "claim_seed_id": "obs_claim_abc123456789",
        "source_observation_ids": ["obs-1"],
        "mechanism": "BREAKOUT",
        "market_structure": "GAP_UP",
        "regime": "UNKNOWN",
        "symbols": ["SPY"],
        "timeframes": ["5m"],
        "cluster_summary": "breakout cluster",
        "claim_text": "BREAKOUT observations may show repeatable behavior in UNKNOWN regimes.",
        "confidence": 0.6,
        "metadata": {"measurement_only": True},
    }
    items = create_backlog_items_from_observations([claim], root=tmp_path, created_at=NOW)
    assert len(items) == 1
    assert items[0]["item_type"] == "CLAIM_INVESTIGATION"
    assert items[0]["state"] == "READY"
    assert items[0]["metadata"]["authority"] == "CLAIM_INVESTIGATION_ONLY"
    assert items[0]["metadata"]["source_observation_ids"] == ["obs-1"]
    assert items[0]["metadata"]["market_structure"] == "GAP_UP"
    assert items[0]["source_artifact_ids"] == ["obs_claim_abc123456789"]
    artifact = ArtifactStore(tmp_path).get_artifact("obs_claim_abc123456789")
    assert artifact["artifact_type"] == "Question"
    assert artifact["metadata"]["market_structure"] == "GAP_UP"
    assert artifact["metadata"]["observations_are_not_claims"] is True
    assert artifact["metadata"]["claims_are_not_recommendations"] is True
