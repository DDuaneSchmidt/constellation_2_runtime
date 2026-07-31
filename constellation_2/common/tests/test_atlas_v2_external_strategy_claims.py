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
from ops.atlas.v2_external_strategy_claim_extractor import (
    ALLOWED_HANDOFF_TIERS,
    AtlasV2ExternalStrategyClaimExtractor,
)

SCHEMA_PATH = REPO_ROOT / "governance/04_DATA/SCHEMAS/ATLAS/V2/atlas_v2_external_strategy_claims.v1.schema.json"
NOW = "2026-06-04T00:00:00Z"


def _extract(tmp_path: Path, text: str, *, title: str = "Manual ProRealAlgos-style ORB notes"):
    return AtlasV2ExternalStrategyClaimExtractor(AtlasV2Ledger(tmp_path)).extract(
        source_title=title,
        source_type="YOUTUBE_MANUAL_NOTES",
        source_url="https://youtube.example/watch?v=manual",
        source_channel="ProRealAlgos manual note",
        input_text=text,
        created_at=NOW,
    )


def test_pasted_orb_strategy_text_extracts_external_strategy_claim(tmp_path: Path) -> None:
    result = _extract(
        tmp_path,
        "Mark the first 15 minute opening range on NQ. Enter long when price breaks above the opening range high "
        "after a candle close. Use VWAP as a filter and exit at a 2R target or stop below the range low.",
    )

    claim = result.claims[0]
    theory = result.contrarian_theories[0]
    handoff = result.handoffs[0]
    assert claim["object_type"] == "ExternalStrategyClaim"
    assert claim["extraction_status"] == "EXTRACTED"
    assert "opening range" in claim["claim_text"].lower()
    assert claim["entry_rule"]
    assert claim["exit_rule"] or claim["risk_rule"]
    assert theory["object_type"] == "ExternalStrategyContrarianTheory"
    assert theory["claim_id"] == claim["claim_id"]
    assert theory["mechanism_id"] == result.mechanisms[0]["mechanism_id"]
    assert handoff["contrarian_id"] == theory["contrarian_id"]
    assert handoff["recommended_tier"] in {"TIER_1_SANITY", "TIER_2_LIGHTWEIGHT_VALIDATION"}
    validate_object(claim)
    validate_object(theory)
    validate_object(handoff)


def test_touch_and_turn_opening_range_classifies_as_opening_range_or_mean_reversion(tmp_path: Path) -> None:
    opening_range_result = _extract(
        tmp_path / "opening",
        "Draw the opening range high and low. If price touches the opening range high and turns down with rejection, "
        "enter short. Exit at the middle of the range and place the stop above the rejected high.",
    )
    touch_turn_result = _extract(
        tmp_path / "mean",
        "Touch and Turn setup: when price touches a prior resistance level and rejects it, enter short on the turn. "
        "Exit at the next support and use a stop above the rejection candle.",
    )

    assert opening_range_result.mechanisms[0]["mechanism_family"] == "OPENING_RANGE"
    assert touch_turn_result.mechanisms[0]["mechanism_family"] == "MEAN_REVERSION"


def test_missing_transcript_or_manual_notes_emits_transcript_required(tmp_path: Path) -> None:
    result = _extract(tmp_path, "", title="Video title without pasted transcript")

    assert result.source["status"] == "TRANSCRIPT_REQUIRED"
    assert result.claims[0]["extraction_status"] == "TRANSCRIPT_REQUIRED"
    assert result.handoffs[0]["eligible_for_cheap_experiment"] is False


def test_vague_strategy_emits_insufficient_rule_detail(tmp_path: Path) -> None:
    result = _extract(tmp_path, "This strategy uses price action to catch good moves when the market looks strong.")

    assert result.claims[0]["extraction_status"] == "INSUFFICIENT_RULE_DETAIL"
    assert result.handoffs[0]["eligible_for_cheap_experiment"] is False


def test_duplicate_claim_is_detected_by_claim_fingerprint(tmp_path: Path) -> None:
    text = (
        "Mark the first 5 minute opening range on ES. Enter long when price breaks above the opening range high. "
        "Exit at the prior day high and use a stop below the range low."
    )
    _extract(tmp_path, text, title="ORB note one")
    second = _extract(tmp_path, text, title="ORB note two")

    dedupe = second.deduplication_results[0]
    assert dedupe["is_duplicate"] is True
    assert dedupe["duplicate_reason"] == "CLAIM_FINGERPRINT_MATCH"
    assert dedupe["duplicate_of_claim_id"]
    assert second.handoffs[0]["recommended_tier"] == "TIER_0_DEDUPE"


def test_different_indicators_with_same_mechanism_produce_mechanism_level_duplicate(tmp_path: Path) -> None:
    first = _extract(
        tmp_path,
        "Mark the first 15 minute opening range on NQ. Enter long when price breaks above the opening range high "
        "with EMA confirmation. Exit at 2R and stop below the opening range low.",
        title="ORB EMA version",
    )
    second = _extract(
        tmp_path,
        "Mark the first 15 minute opening range on NQ. Enter long when price breaks above the opening range high "
        "with RSI confirmation. Exit at 2R and stop below the opening range low.",
        title="ORB RSI version",
    )

    assert first.deduplication_results[0]["is_duplicate"] is False
    assert second.deduplication_results[0]["is_duplicate"] is True
    assert second.deduplication_results[0]["duplicate_reason"] == "MECHANISM_FINGERPRINT_MATCH"


def test_cheap_experiment_handoff_only_recommends_allowed_tiers(tmp_path: Path) -> None:
    result = _extract(
        tmp_path,
        "After the London open, enter long when price reclaims VWAP after a pullback. Exit at the morning high "
        "and place a stop below VWAP.",
    )

    assert result.handoffs[0]["recommended_tier"] in ALLOWED_HANDOFF_TIERS
    assert set(ALLOWED_HANDOFF_TIERS) == {"TIER_0_DEDUPE", "TIER_1_SANITY", "TIER_2_LIGHTWEIGHT_VALIDATION"}
    assert result.handoffs[0]["recommended_tier"] != "TIER_3_ROBUST_VALIDATION"
    assert result.handoffs[0]["authority_boundary_acknowledged"] is True
    if result.handoffs[0]["recommended_tier"] in {"TIER_1_SANITY", "TIER_2_LIGHTWEIGHT_VALIDATION"}:
        assert result.handoffs[0]["contrarian_id"] == result.contrarian_theories[0]["contrarian_id"]


def test_forbidden_authority_fields_fail_validation(tmp_path: Path) -> None:
    result = _extract(
        tmp_path,
        "Enter long when price breaks above the opening range high. Exit at 2R and stop below the range low.",
    )
    bad_claim = {**result.claims[0], "candidate_id": "candidate-forbidden"}
    bad_handoff = {**result.handoffs[0], "broker_order": "order-forbidden"}

    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        validate_object(bad_claim)
    with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
        validate_object(bad_handoff)


def test_external_strategy_claim_schema_validation_passes(tmp_path: Path) -> None:
    result = _extract(
        tmp_path,
        "Enter short when price breaks below the first 15 minute opening range low. Use ATR as a filter. "
        "Exit at 2R and stop above the opening range high.",
    )
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    for record in [
        result.source,
        *result.claims,
        *result.mechanisms,
        *result.deduplication_results,
        *result.contrarian_theories,
        *result.handoffs,
    ]:
        validate_object(record)
        validator.validate(record)
