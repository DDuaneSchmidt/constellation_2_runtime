from __future__ import annotations

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_core import AtlasV2Ledger, validate_object
from ops.atlas.v2_high_volume_claim_pipeline import AtlasV2HighVolumeClaimPipeline

SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_high_volume_claim_pipeline.v1.schema.json"
NOW = "2026-06-04T00:00:00Z"


def _claim(index: int) -> dict[str, str]:
    variants = [
        "ORB Strategy: enter long when price breaks above the first 15 minute opening range high. Exit at 2R and stop below the opening range low.",
        "Sneaky Pivot setup: after the open, use the opening range high as the pivot and retest level. Stop below the range.",
        "Opening Range Breakout: buy a close above the opening range high and exit at the prior day high.",
        "Opening Range Retest: wait for the range high to break, retest, and hold before entry. Stop under the retest.",
        "Touch and Turn: short when price rejects prior resistance, exit at support, stop above rejection.",
        "VWAP reclaim: enter long when price reclaims VWAP after a pullback, exit near the morning high, stop below VWAP.",
    ]
    sources = ["TRANSCRIPT_INTAKE", "MANUAL_CLAIM_ENTRY", "HISTORICAL_RESEARCH_ARTIFACTS", "EXTERNAL_STRATEGY_CLAIMS"]
    return {
        "claim_id": f"claim-{index:05d}",
        "source_type": sources[index % len(sources)],
        "source_id": f"source-{index % 17:02d}",
        "claim_text": variants[index % len(variants)],
    }


def _run(size: int, tmp_path: Path):
    return AtlasV2HighVolumeClaimPipeline(AtlasV2Ledger(tmp_path)).process_claim_batch(
        batch_id=f"claim-batch-{size}",
        claims=(_claim(index) for index in range(size)),
        created_at=NOW,
    )


def test_1000_claim_batch_succeeds(tmp_path: Path) -> None:
    result = _run(1000, tmp_path)

    assert result.metrics["total_claims"] == 1000
    assert result.batch["batch_size_tier"] == "UP_TO_1000"
    assert result.metrics["unique_claims"] < 1000
    assert result.metrics["unique_mechanisms"] < 1000


def test_5000_claim_batch_succeeds(tmp_path: Path) -> None:
    result = _run(5000, tmp_path)

    assert result.metrics["total_claims"] == 5000
    assert result.batch["batch_size_tier"] == "UP_TO_5000"
    assert result.metrics["claims_deduped"] > 0


def test_10000_claim_simulated_batch_succeeds_without_10000_experiments(tmp_path: Path) -> None:
    result = _run(10000, tmp_path)

    assert result.metrics["total_claims"] == 10000
    assert result.batch["batch_size_tier"] == "UP_TO_10000"
    assert len(result.metrics["cheap_experiment_candidates"]) == result.metrics["unique_mechanisms"]
    assert len(result.metrics["cheap_experiment_candidates"]) < 10000
    assert result.metrics["mechanisms_reused"] > 0


def test_mechanism_clustering_reduces_opening_range_duplicates(tmp_path: Path) -> None:
    claims = [
        {"source_type": "MANUAL_CLAIM_ENTRY", "claim_text": "ORB Strategy: enter above the opening range high, stop below the range low."},
        {"source_type": "EXTERNAL_STRATEGY_CLAIMS", "claim_text": "Sneaky Pivot uses the opening range high after the open as a retest pivot."},
        {"source_type": "TRANSCRIPT_INTAKE", "claim_text": "Opening Range Breakout buys a close above the first 15 minute range high."},
        {"source_type": "HISTORICAL_RESEARCH_ARTIFACTS", "claim_text": "Opening Range Retest waits for a break and retest of the opening range high."},
    ]

    result = AtlasV2HighVolumeClaimPipeline(AtlasV2Ledger(tmp_path)).process_claim_batch(batch_id="orb-cluster", claims=claims, created_at=NOW)

    assert result.metrics["unique_mechanisms"] == 1
    assert result.metrics["mechanism_distribution"] == {"OPENING_RANGE": 4}
    assert result.mechanism_clusters[0]["mechanism_family"] == "OPENING_RANGE"
    assert result.mechanism_clusters[0]["claim_count"] == 4


def test_contrarian_coverage_is_positive_and_routing_is_bounded(tmp_path: Path) -> None:
    result = _run(1000, tmp_path)

    assert result.metrics["contrarian_coverage"] > 0
    assert result.metrics["cheap_experiment_coverage"] > 0
    assert len(result.metrics["cheap_experiment_candidates"]) <= result.metrics["unique_mechanisms"]
    assert len(result.metrics["cheap_experiment_candidates"]) <= 50


def test_high_volume_outputs_are_schema_valid_and_authority_bounded(tmp_path: Path) -> None:
    result = _run(1000, tmp_path)
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    records = [result.batch, result.metrics, result.cluster_summary, *result.mechanism_registry, *result.mechanism_clusters]

    for record in records:
        validate_object(record)
        validator.validate(record)

    assert result.batch["authority_boundary_acknowledged"] is True
    assert result.metrics["authority_boundary_acknowledged"] is True
    assert AtlasV2Ledger(tmp_path).audit_forbidden_artifacts().ok
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "ClaimBatch.jsonl",
        "ClaimBatchMetrics.jsonl",
        "ClaimClusterSummary.jsonl",
        "MechanismCluster.jsonl",
        "MechanismRegistry.jsonl",
    ]
