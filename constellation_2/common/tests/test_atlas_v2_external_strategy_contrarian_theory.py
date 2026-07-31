from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.atlas.v2_core import AtlasV2Ledger, AtlasV2ValidationError, validate_object
from ops.atlas.v2_external_strategy_claim_extractor import AtlasV2ExternalStrategyClaimExtractor

NOW = "2026-06-04T00:00:00Z"


def _extract(tmp_path: Path, text: str, **kwargs):
    return AtlasV2ExternalStrategyClaimExtractor(AtlasV2Ledger(tmp_path)).extract(
        source_title=kwargs.pop("source_title", "Manual external strategy notes"),
        source_type="YOUTUBE_MANUAL_NOTES",
        source_channel="ProRealAlgos manual note",
        source_url="https://youtube.example/manual",
        input_text=text,
        created_at=NOW,
        **kwargs,
    )


def test_orb_opening_range_claim_produces_plausible_failure_modes(tmp_path: Path) -> None:
    result = _extract(
        tmp_path,
        "Mark the first 15 minute opening range on NQ. Enter long when price breaks above the opening range high. "
        "Exit at a 2R target and stop below the range low.",
    )

    theory = result.contrarian_theories[0]
    assert theory["status"] == "GENERATED"
    assert {"REGIME_DEPENDENCE", "SLIPPAGE", "CHERRY_PICKING", "LOW_SAMPLE_SIZE"}.issubset(theory["primary_failure_modes"])
    assert theory["required_falsification_tests"]
    validate_object(theory)


def test_vague_claim_produces_vague_rules_and_requires_clarification(tmp_path: Path) -> None:
    result = _extract(tmp_path, "This strategy uses price action to catch good moves when conditions look right.")
    theory = result.contrarian_theories[0]

    assert theory["primary_failure_modes"] == ["VAGUE_RULES"]
    assert theory["status"] == "REQUIRES_CLARIFICATION"
    assert result.handoffs[0]["recommended_tier"] == "TIER_0_DEDUPE"


def test_contrarian_theory_required_before_tier_1_or_tier_2_handoff(tmp_path: Path) -> None:
    result = _extract(
        tmp_path,
        "Enter long when price breaks above the opening range high. Exit at 2R and stop below the opening range low.",
    )
    bad_handoff = {**result.handoffs[0]}
    bad_handoff.pop("contrarian_id")

    with pytest.raises(AtlasV2ValidationError, match="requires contrarian theory"):
        validate_object(bad_handoff)

    tier_2 = {**result.handoffs[0], "recommended_tier": "TIER_2_LIGHTWEIGHT_VALIDATION"}
    tier_2.pop("contrarian_id")
    with pytest.raises(AtlasV2ValidationError, match="requires contrarian theory"):
        validate_object(tier_2)


def test_contrarian_theory_does_not_block_tier_0_dedupe_handoff(tmp_path: Path) -> None:
    result = _extract(tmp_path, "This is too vague to test.")
    handoff = {**result.handoffs[0]}
    handoff.pop("contrarian_id", None)

    assert handoff["recommended_tier"] == "TIER_0_DEDUPE"
    validate_object(handoff)


def test_unknown_is_used_when_failure_mode_cannot_be_inferred(tmp_path: Path) -> None:
    ledger = AtlasV2Ledger(tmp_path)
    extractor = AtlasV2ExternalStrategyClaimExtractor(ledger)
    source_result = extractor.extract(
        source_title="Unknown mechanism but explicit rules",
        source_type="YOUTUBE_MANUAL_NOTES",
        input_text="Enter long when condition Alpha X occurs. Exit after condition Beta Y. Stop if condition Gamma Z fails.",
        created_at=NOW,
    )

    theory = source_result.contrarian_theories[0]
    assert source_result.mechanisms[0]["mechanism_family"] == "UNKNOWN"
    assert theory["primary_failure_modes"] == ["UNKNOWN"]
    assert theory["status"] == "UNKNOWN"


def test_prior_failure_match_preserves_provenance_when_supplied(tmp_path: Path) -> None:
    result = _extract(
        tmp_path,
        "Mark the first 15 minute opening range on ES. Enter long above the high. Exit at 2R and stop below the low.",
        prior_failure_matches=[
            {
                "provenance_reference": "atlas/v1/negative_knowledge/orb_slippage_2025.md",
                "summary": "Prior ORB replay was fragile to slippage.",
                "matched_failure_mode": "SLIPPAGE",
            }
        ],
    )

    match = result.contrarian_theories[0]["prior_failure_matches"][0]
    assert match["provenance_reference"] == "atlas/v1/negative_knowledge/orb_slippage_2025.md"
    assert match["matched_failure_mode"] == "SLIPPAGE"


def test_contrarian_theory_cannot_create_forbidden_authority(tmp_path: Path) -> None:
    result = _extract(
        tmp_path,
        "Enter short when price breaks below the opening range low. Exit at 2R and stop above the range high.",
    )
    forbidden = [
        {"validation_authority": True},
        {"recommendation_id": "rec-forbidden"},
        {"trade_recommendation": "SELL"},
        {"candidate_id": "candidate-forbidden"},
        {"sleeve_id": "sleeve-forbidden"},
        {"paper_position_id": "paper-position-forbidden"},
        {"allocation_id": "allocation-forbidden"},
    ]
    for extra in forbidden:
        with pytest.raises(AtlasV2ValidationError, match="prohibited authority"):
            validate_object({**result.contrarian_theories[0], **extra})
