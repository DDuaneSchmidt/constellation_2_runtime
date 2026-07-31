from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_claim_batch_processor import AtlasV2ClaimBatchProcessor, PIPELINE_STEPS
from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, validate_object

NOW = "2026-06-04T00:00:00Z"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_claim_batch_processor.v1.schema.json"


def _claim(index: int, family: str) -> dict[str, str]:
    source_types = [
        "TRANSCRIPT_INTAKE",
        "EXTERNAL_STRATEGY_CLAIMS",
        "HISTORICAL_RESEARCH_ARTIFACTS",
        "MANUAL_CLAIM_ENTRY",
    ]
    templates = {
        "OPENING_RANGE": "ORB variant {index}: Mark the first 15 minute opening range on NQ. Enter long when price breaks above the opening range high. Exit at 2R and stop below the range low.",
        "BREAKOUT": "Breakout variant {index}: Enter long when price breaks above the prior range high. Exit at 2R and stop below the range low.",
        "MEAN_REVERSION": "Touch and turn variant {index}: Enter short when price touches resistance and rejects it. Exit at support and stop above the rejection high.",
        "VWAP": "VWAP reclaim variant {index}: Enter long when price reclaims VWAP after a pullback. Exit at the morning high and stop below VWAP.",
        "MOMENTUM": "Momentum variant {index}: Enter long after a strong impulse candle with trend confirmation. Exit at 2R and stop below the impulse low.",
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


def test_claim_batch_processor_compresses_1000_5000_and_10000_claims(tmp_path: Path) -> None:
    for count in (1000, 5000, 10000):
        ledger = AtlasV2Ledger(tmp_path / str(count))
        result = AtlasV2ClaimBatchProcessor(ledger).process(_claims(count), batch_id=f"batch-{count}", run_id=f"run-{count}", created_at=NOW)

        assert result.batch["claim_count"] == count
        assert result.run["pipeline_steps"] == PIPELINE_STEPS
        assert result.metrics["total_claims"] == count
        assert result.metrics["unique_mechanisms"] < 500
        assert result.metrics["mechanisms_reused"] == count - result.metrics["unique_mechanisms"]
        assert len(result.metrics["cheap_experiment_candidates"]) <= result.metrics["unique_mechanisms"]
        assert len(ledger.records("CheapExperiment")) == 0
        validate_object(result.batch)
        validate_object(result.run)
        validate_object(result.metrics)


def test_claim_batch_processor_emits_mechanism_granularity_records_only(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = AtlasV2ClaimBatchProcessor(ledger).process(_claims(250), batch_id="batch-mechanisms", run_id="run-mechanisms", created_at=NOW)

    unique_mechanisms = result.metrics["unique_mechanisms"]
    assert len(result.mechanism_records) == unique_mechanisms
    assert len(result.dedupe_records) == unique_mechanisms
    assert len(result.contrarian_records) == unique_mechanisms
    assert len(result.handoff_records) == unique_mechanisms
    assert len(result.registry_records) == unique_mechanisms
    assert result.run["emitted_record_counts"]["ExternalStrategyCheapExperimentHandoff"] == unique_mechanisms
    assert result.run["processed_claim_count"] == 250


def test_claim_batch_processor_rejects_authority_expansion_fields(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    result = AtlasV2ClaimBatchProcessor(ledger).process(_claims(10), created_at=NOW)

    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        validate_object({**result.batch, "trade_recommendation": "BUY"})
    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        validate_object({**result.run, "broker_order": "order-forbidden"})
    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        validate_object({**result.metrics, "allocation_id": "allocation-forbidden"})


def test_claim_batch_processor_rejects_more_than_10000_claims(tmp_path: Path) -> None:
    with pytest.raises(AtlasV2ValidationError, match="at most 10000"):
        AtlasV2ClaimBatchProcessor(AtlasV2Ledger(tmp_path)).process(_claims(10001), created_at=NOW)


def test_claim_batch_processor_schema_validation_passes(tmp_path: Path) -> None:
    result = AtlasV2ClaimBatchProcessor(AtlasV2Ledger(tmp_path)).process(_claims(25), created_at=NOW)
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    for record in [result.batch, result.run, result.metrics, *result.registry_records]:
        validate_object(record)
        validator.validate(record)
