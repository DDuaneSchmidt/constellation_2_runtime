from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, validate_object
from ops.atlas.v2_transcript_intake import AtlasV2TranscriptIntake

NOW = "2026-06-04T00:00:00Z"
SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_transcript_intake.v1.schema.json"

ORB_TRANSCRIPT = " ".join(
    [
        "For this ORB setup mark the first 15 minute opening range on NQ.",
        "Enter long when price breaks above the opening range high after a candle close.",
        "Use VWAP as a filter and only take the trade above VWAP.",
        "Exit at a 2R target or close when price returns inside the range.",
        "Place the stop below the opening range low.",
        "The presenter says it makes $10,000/month, but that is only a performance claim.",
    ]
)


def _intake(tmp_path: Path, text: str = ORB_TRANSCRIPT):
    return AtlasV2TranscriptIntake(AtlasV2Ledger(tmp_path)).intake(
        source_title="Manual ORB YouTube transcript",
        source_url="https://youtube.example/watch?v=manual-only",
        source_channel="Manual channel metadata",
        transcript_text=text,
        created_at=NOW,
    )


def test_orb_transcript_produces_claim_candidates(tmp_path: Path) -> None:
    result = _intake(tmp_path)

    assert result.request["object_type"] == "TranscriptIntakeRequest"
    assert result.request["status"] == "PARSED"
    assert any(candidate["status"] == "READY_FOR_EXTRACTION" for candidate in result.candidates)
    assert result.summary()["external_claims"] >= 1


def test_entry_rule_segments_are_identified(tmp_path: Path) -> None:
    result = _intake(tmp_path)

    entry_segments = [segment for segment in result.segments if segment["segment_type"] == "ENTRY_RULE"]
    assert entry_segments
    assert "breaks above" in entry_segments[0]["segment_text"].lower()


def test_exit_rule_segments_are_identified(tmp_path: Path) -> None:
    result = _intake(tmp_path)

    assert any(segment["segment_type"] == "EXIT_RULE" for segment in result.segments)


def test_performance_claim_segments_are_identified_and_not_candidate_evidence(tmp_path: Path) -> None:
    result = _intake(tmp_path)

    performance = [segment for segment in result.segments if segment["segment_type"] == "PERFORMANCE_CLAIM"]
    assert performance
    assert "$10,000/month" in performance[0]["segment_text"]
    assert all("$10,000" not in candidate["candidate_text"] for candidate in result.candidates)
    assert all("$10,000" not in claim["claim_text"] for external in result.external_results for claim in external.claims)


def test_transcript_produces_external_strategy_claim(tmp_path: Path) -> None:
    result = _intake(tmp_path)
    claims = [claim for external in result.external_results for claim in external.claims]

    assert claims
    assert claims[0]["object_type"] == "ExternalStrategyClaim"
    assert claims[0]["extraction_status"] == "EXTRACTED"
    validate_object(claims[0])


def test_mechanism_classification_runs(tmp_path: Path) -> None:
    result = _intake(tmp_path)
    mechanisms = [mechanism for external in result.external_results for mechanism in external.mechanisms]

    assert mechanisms
    assert mechanisms[0]["mechanism_family"] == "OPENING_RANGE"
    validate_object(mechanisms[0])


def test_newline_numbered_sneaky_pivot_transcript_routes_to_tier_1(tmp_path: Path) -> None:
    text = """"Sneaky Pivot"

Key rules extracted from transcript:

- Use a 15-minute chart only.
- Mark previous day high (range high).
- Mark previous day low (range low).
- Mark next swing high above range high.
- Mark next swing low below range low.
- Only trade at those four levels.
- Buy at range low or swing low.
- Sell at range high or swing high.
- Use a 3-candle framework:
    1. Opening range candle
    2. Sneaky confirmation candle
    3. Entry candle
- Enter when the third candle breaks the sneaky candle.
- Stop goes below buyer zone for longs.
- Stop goes above seller zone for shorts.
- Target is opposite range boundary.
- Strategy claims:
    - "easiest and fastest way"
    - "professional trader for 26 years"
    - "$10,000/month"
    - "consistently making trades"""
    result = _intake(tmp_path, text)

    segments_by_type = {segment["segment_type"] for segment in result.segments}
    assert {"ENTRY_RULE", "EXIT_RULE", "FILTER_RULE", "RISK_RULE", "MARKET_CONTEXT", "PERFORMANCE_CLAIM"} <= segments_by_type
    assert any(segment["segment_text"] == "Opening range candle" for segment in result.segments)
    assert all("$10,000" not in candidate["candidate_text"] for candidate in result.candidates)

    mechanisms = [mechanism for external in result.external_results for mechanism in external.mechanisms]
    theories = [theory for external in result.external_results for theory in external.contrarian_theories]
    handoffs = [handoff for external in result.external_results for handoff in external.handoffs]

    assert mechanisms[0]["mechanism_family"] == "OPENING_RANGE"
    assert {"REGIME_DEPENDENCE", "SLIPPAGE", "CHERRY_PICKING", "LOW_SAMPLE_SIZE"} <= set(theories[0]["primary_failure_modes"])
    assert handoffs[0]["recommended_tier"] == "TIER_1_SANITY"
    assert handoffs[0]["authority_boundary_acknowledged"] is True


def test_contrarian_theory_runs(tmp_path: Path) -> None:
    result = _intake(tmp_path)
    theories = [theory for external in result.external_results for theory in external.contrarian_theories]

    assert theories
    assert theories[0]["object_type"] == "ExternalStrategyContrarianTheory"
    assert theories[0]["primary_failure_modes"]
    validate_object(theories[0])


def test_cheap_experiment_handoff_runs(tmp_path: Path) -> None:
    result = _intake(tmp_path)
    handoffs = [handoff for external in result.external_results for handoff in external.handoffs]

    assert handoffs
    assert handoffs[0]["object_type"] == "ExternalStrategyCheapExperimentHandoff"
    assert handoffs[0]["authority_boundary_acknowledged"] is True
    assert handoffs[0]["recommended_tier"] in {"TIER_0_DEDUPE", "TIER_1_SANITY", "TIER_2_LIGHTWEIGHT_VALIDATION"}
    validate_object(handoffs[0])


def test_duplicate_segments_are_detected(tmp_path: Path) -> None:
    duplicate_text = (
        "Enter long when price breaks above the opening range high. "
        "Enter long when price breaks above the opening range high. "
        "Exit at 2R and stop below the opening range low."
    )
    result = _intake(tmp_path, duplicate_text)

    duplicates = [candidate for candidate in result.candidates if candidate["status"] == "DUPLICATE_SEGMENT"]
    assert duplicates
    assert len(duplicates[0]["segment_ids"]) == 2


def test_insufficient_detail_is_emitted_when_rules_are_vague(tmp_path: Path) -> None:
    result = _intake(tmp_path, "This video says to watch price action and take the best looking move when the market feels strong.")

    assert result.candidates[0]["status"] == "INSUFFICIENT_DETAIL"
    claims = [claim for external in result.external_results for claim in external.claims]
    assert claims[0]["extraction_status"] == "INSUFFICIENT_RULE_DETAIL"
    assert [handoff for external in result.external_results for handoff in external.handoffs][0]["eligible_for_cheap_experiment"] is False


def test_transcript_intake_schema_validation_passes(tmp_path: Path) -> None:
    result = _intake(tmp_path)
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    for record in [result.request, *result.segments, *result.candidates]:
        validate_object(record)
        validator.validate(record)


def test_transcript_intake_forbidden_authority_fields_fail(tmp_path: Path) -> None:
    result = _intake(tmp_path)

    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        validate_object({**result.request, "broker_execution": True})
    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        validate_object({**result.segments[0], "sleeve_id": "sleeve-forbidden"})
    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        validate_object({**result.candidates[0], "allocation_id": "allocation-forbidden"})
