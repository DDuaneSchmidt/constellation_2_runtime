from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.observation_deduplication import cluster_observations, deduplicate_observations, normalize_observation_text, timestamp_bucket


def test_normalize_observation_text() -> None:
    assert normalize_observation_text("Broke opening RANGE high!!!") == "broke opening range high"


def test_deduplicates_by_text_symbol_timeframe_mechanism_regime_and_bucket() -> None:
    records = [
        {"observation_id": "obs-1", "timestamp": "2026-06-05T09:45:00Z", "symbol": "SPY", "timeframe": "5m", "mechanism": "BREAKOUT", "regime": "TRENDING", "observation_text": "range breakout"},
        {"observation_id": "obs-2", "timestamp": "2026-06-05T09:55:00Z", "symbol": "SPY", "timeframe": "5m", "mechanism": "BREAKOUT", "regime": "TRENDING", "observation_text": "Range breakout!!!"},
        {"observation_id": "obs-3", "timestamp": "2026-06-05T10:05:00Z", "symbol": "SPY", "timeframe": "5m", "mechanism": "BREAKOUT", "regime": "TRENDING", "observation_text": "Range breakout!!!"},
    ]
    unique, duplicates = deduplicate_observations(records)
    assert len(unique) == 2
    assert len(duplicates) == 1
    assert timestamp_bucket("2026-06-05T09:45:00Z") == "2026-06-05T09"


def test_clusters_observations() -> None:
    records = [
        {"observation_id": "obs-1", "symbol": "SPY", "timeframe": "5m", "mechanism": "BREAKOUT", "regime": "TRENDING", "confidence": 0.6, "observation_text": "range breakout after compression"},
        {"observation_id": "obs-2", "symbol": "QQQ", "timeframe": "5m", "mechanism": "BREAKOUT", "regime": "TRENDING", "confidence": 0.8, "observation_text": "range breakout after compression"},
    ]
    clusters = cluster_observations(records)
    assert len(clusters) == 1
    assert clusters[0]["observation_count"] == 2
    assert clusters[0]["confidence"] == 0.7
