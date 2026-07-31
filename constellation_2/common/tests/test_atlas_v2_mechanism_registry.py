from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_claim_batch_processor import AtlasV2ClaimBatchProcessor
from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, validate_object
from ops.atlas.v2_mechanism_registry import AtlasV2MechanismRegistry

NOW = "2026-06-04T00:00:00Z"
LATER = "2026-06-04T01:00:00Z"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_mechanism_registry.v1.schema.json"


def _claim(index: int, family: str) -> dict[str, str]:
    source_types = ["TRANSCRIPT_INTAKE", "EXTERNAL_STRATEGY_CLAIMS", "HISTORICAL_RESEARCH_ARTIFACTS", "MANUAL_CLAIM_ENTRY"]
    templates = {
        "OPENING_RANGE": "ORB {index}: Mark the first 15 minute opening range on NQ. Enter long when price breaks above the opening range high. Exit at 2R and stop below the range low.",
        "BREAKOUT": "Breakout {index}: Enter long when price breaks above the prior range high. Exit at 2R and stop below the range low.",
        "MEAN_REVERSION": "Touch and turn {index}: Enter short when price touches resistance and rejects it. Exit at support and stop above the rejection high.",
        "VWAP": "VWAP reclaim {index}: Enter long when price reclaims VWAP after a pullback. Exit at the morning high and stop below VWAP.",
        "MOMENTUM": "Momentum {index}: Enter long after a strong impulse candle with trend confirmation. Exit at 2R and stop below the impulse low.",
        "UNKNOWN": "Unknown setup {index}: Enter long when Alpha X condition occurs. Exit after Beta Y condition and stop if Gamma Z fails.",
    }
    return {
        "claim_id": f"raw-{index:05d}",
        "source_id": f"src-{index % 137:03d}",
        "source_type": source_types[index % len(source_types)],
        "claim_text": templates[family].format(index=index),
    }


def _claims(count: int) -> list[dict[str, str]]:
    families = ["OPENING_RANGE", "BREAKOUT", "MEAN_REVERSION", "VWAP", "MOMENTUM"]
    return [_claim(index, families[index % len(families)]) for index in range(count)]


def _build_registry(tmp_path: Path, claims: list[dict[str, str]], *, batch_id: str = "batch"):
    ledger = AtlasV2Ledger(tmp_path)
    batch = AtlasV2ClaimBatchProcessor(ledger).process(claims, batch_id=batch_id, run_id=f"run-{batch_id}", created_at=NOW)
    registry = AtlasV2MechanismRegistry(ledger).build_from_claim_batch(batch_id=batch.batch["batch_id"], created_at=NOW)
    return ledger, batch, registry


def test_claim_batch_with_10000_claims_creates_small_durable_mechanism_registry(tmp_path: Path) -> None:
    ledger, batch, registry = _build_registry(tmp_path, _claims(10000), batch_id="ten-thousand")

    assert batch.metrics["total_claims"] == 10000
    assert len(registry.registry_entries) == 5
    assert len(registry.registry_entries) < 500
    assert {record["object_type"] for record in registry.registry_entries} == {"MechanismRegistryEntry"}
    assert len(ledger.records("CheapExperiment")) == 0
    for record in [*registry.registry_entries, *registry.clusters, *registry.lineages, *registry.metrics]:
        validate_object(record)


def test_duplicate_claims_increase_claim_count_not_mechanism_count(tmp_path: Path) -> None:
    claims = [_claim(index, "OPENING_RANGE") for index in range(1000)]
    _, _, registry = _build_registry(tmp_path, claims, batch_id="duplicates")

    assert len(registry.registry_entries) == 1
    entry = registry.registry_entries[0]
    assert entry["mechanism_family"] == "OPENING_RANGE"
    assert entry["claim_count"] == 1000
    assert registry.metrics[0]["claim_count"] == 1000


def test_mechanism_metrics_reports_duplicate_ratio(tmp_path: Path) -> None:
    _, _, registry = _build_registry(tmp_path, [_claim(index, "OPENING_RANGE") for index in range(100)], batch_id="ratio")

    metric = registry.metrics[0]
    assert metric["duplicate_ratio"] > 0.9
    assert metric["duplicate_ratio"] <= 1


def test_contrarian_and_cheap_experiment_coverage_are_calculated(tmp_path: Path) -> None:
    _, _, registry = _build_registry(tmp_path, _claims(250), batch_id="coverage")

    for metric in registry.metrics:
        assert metric["contrarian_coverage"] == 1
        assert metric["cheap_experiment_coverage"] in {0, 1}
    assert all(entry["contrarian_count"] >= 1 for entry in registry.registry_entries)
    assert all(entry["cheap_experiment_eligibility_count"] >= 1 for entry in registry.registry_entries)


def test_unknown_mechanism_is_retained_but_not_promoted(tmp_path: Path) -> None:
    _, _, registry = _build_registry(tmp_path, [_claim(index, "UNKNOWN") for index in range(20)], batch_id="unknown")

    entry = registry.registry_entries[0]
    metric = registry.metrics[0]
    assert entry["mechanism_family"] == "UNKNOWN"
    assert entry["status"] == "RETAINED_NOT_PROMOTED"
    assert entry["cheap_experiment_eligibility_count"] == 0
    assert metric["cheap_experiment_coverage"] == 0
    assert registry.clusters[0]["status"] == "RETAINED_NOT_PROMOTED"
    assert registry.lineages[0]["status"] == "UNKNOWN_RETAINED"


def test_append_only_behavior_preserves_prior_metrics(tmp_path: Path) -> None:
    ledger, batch, first = _build_registry(tmp_path, _claims(50), batch_id="append-only")
    initial_metric_count = len(ledger.records("MechanismMetrics"))

    second = AtlasV2MechanismRegistry(ledger).build_from_claim_batch(batch_id=batch.batch["batch_id"], created_at=LATER)

    assert len(ledger.records("MechanismMetrics")) == initial_metric_count + len(second.metrics)
    assert len(ledger.records("MechanismRegistryEntry")) == len(first.registry_entries) + len(second.registry_entries)
    assert all(record["last_updated"] == NOW for record in first.metrics)
    assert all(record["last_updated"] == LATER for record in second.metrics)


def test_mechanism_registry_rejects_authority_expansion_fields(tmp_path: Path) -> None:
    _, _, registry = _build_registry(tmp_path, _claims(10), batch_id="authority")

    forbidden = [
        (registry.registry_entries[0], {"trade_recommendation": "BUY"}),
        (registry.clusters[0], {"candidate_id": "candidate-forbidden"}),
        (registry.lineages[0], {"allocation_id": "allocation-forbidden"}),
        (registry.metrics[0], {"validation_authority": True}),
    ]
    for record, extra in forbidden:
        with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
            validate_object({**record, **extra})


def test_mechanism_registry_schema_validation_passes(tmp_path: Path) -> None:
    _, _, registry = _build_registry(tmp_path, _claims(25), batch_id="schema")
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    for record in [*registry.registry_entries, *registry.clusters, *registry.lineages, *registry.metrics]:
        validate_object(record)
        validator.validate(record)
