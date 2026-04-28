from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_decision_pipeline_v1 import (  # noqa: E402
    REGIME_CHOPPY,
    REGIME_HIGH_VOL_UNSTABLE,
    REGIME_TRENDING,
    build_decision_trace_v1,
    build_eod_advisory_from_decision_trace_v1,
)
from constellation_2.common.aegis_structure_selection_v1 import (  # noqa: E402
    EXECUTION_STATUS_NOT_EVALUATED,
    RC_FALLBACK_USED,
    RC_REJECTED_HIGH_VOL,
    RC_REJECTED_REGIME_UNKNOWN,
    RC_REJECTED_WEAK_SIGNAL,
    RISK_STATUS_NOT_EVALUATED,
    STRUCTURE_CREDIT_SPREAD,
    STRUCTURE_IRON_CONDOR,
    STRUCTURE_NO_TRADE,
    STRUCTURE_VERTICAL_SPREAD,
    build_structure_selection_summary_v1,
    select_structure_for_candidate_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402


DAY = "2026-04-28"


def _candidate(**overrides: object) -> dict:
    base = {
        "candidate_id": "candidate-1",
        "timestamp": f"{DAY}T14:30:00Z",
        "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
        "symbol": "SPY",
        "direction": "LONG",
        "current_regime": REGIME_TRENDING,
        "confidence": "UNKNOWN",
        "raw_signal_payload": {"intent_id": "intent-1"},
    }
    base.update(overrides)
    return base


def _intent(engine_id: str, *, exposure_type: str = "LONG_EQUITY", intent_id: str = "intent-1", **extra: object) -> dict:
    payload = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": intent_id,
        "created_at_utc": f"{DAY}T00:00:00Z",
        "engine": {"engine_id": engine_id, "suite": "C2_HYBRID_V1", "mode": "PAPER"},
        "underlying": {"symbol": "SPY", "currency": "USD"},
        "exposure_type": exposure_type,
        "target_notional_pct": "0.01",
        "expected_holding_days": 3,
        "risk_class": "TEST",
        "constraints": {"max_risk_pct": "0.01"},
        "canonical_json_hash": None,
    }
    payload.update(extra)
    return payload


def test_unknown_regime_returns_no_trade() -> None:
    decision = select_structure_for_candidate_v1(_candidate(current_regime="UNKNOWN"))

    assert decision["selected_structure"] == STRUCTURE_NO_TRADE
    assert RC_REJECTED_REGIME_UNKNOWN in decision["reason_codes"]


def test_high_vol_unstable_returns_no_trade() -> None:
    decision = select_structure_for_candidate_v1(_candidate(current_regime=REGIME_HIGH_VOL_UNSTABLE))

    assert decision["selected_structure"] == STRUCTURE_NO_TRADE
    assert RC_REJECTED_HIGH_VOL in decision["reason_codes"]
    assert STRUCTURE_CREDIT_SPREAD in decision["rejected_structures"]
    assert STRUCTURE_IRON_CONDOR in decision["rejected_structures"]


def test_trending_bullish_defaults_to_vertical_spread_when_optional_fields_missing() -> None:
    decision = select_structure_for_candidate_v1(_candidate(direction="LONG", current_regime=REGIME_TRENDING))

    assert decision["selected_structure"] == STRUCTURE_VERTICAL_SPREAD
    assert decision["fallback_used"] is True
    assert RC_FALLBACK_USED in decision["reason_codes"]


def test_trending_bearish_defaults_to_vertical_spread_when_optional_fields_missing() -> None:
    decision = select_structure_for_candidate_v1(_candidate(direction="SHORT", current_regime=REGIME_TRENDING))

    assert decision["selected_structure"] == STRUCTURE_VERTICAL_SPREAD
    assert decision["fallback_used"] is True


def test_choppy_neutral_returns_iron_condor() -> None:
    decision = select_structure_for_candidate_v1(_candidate(direction="NEUTRAL", current_regime=REGIME_CHOPPY))

    assert decision["selected_structure"] == STRUCTURE_IRON_CONDOR


def test_choppy_directional_returns_credit_spread() -> None:
    bullish = select_structure_for_candidate_v1(_candidate(direction="LONG", current_regime=REGIME_CHOPPY))
    bearish = select_structure_for_candidate_v1(_candidate(direction="SHORT", current_regime=REGIME_CHOPPY))

    assert bullish["selected_structure"] == STRUCTURE_CREDIT_SPREAD
    assert bearish["selected_structure"] == STRUCTURE_CREDIT_SPREAD


def test_explicit_weak_signal_returns_no_trade() -> None:
    decision = select_structure_for_candidate_v1(_candidate(signal_strength="weak"))

    assert decision["selected_structure"] == STRUCTURE_NO_TRADE
    assert RC_REJECTED_WEAK_SIGNAL in decision["reason_codes"]


def test_structure_selection_preserves_direction_and_status_invariants() -> None:
    decision = select_structure_for_candidate_v1(_candidate(direction="SHORT"))

    assert decision["original_direction"] == "SHORT"
    assert decision["normalized_direction"] == "bearish"
    assert decision["raw_candidate_payload"]["direction"] == "SHORT"
    assert decision["risk_status"] == RISK_STATUS_NOT_EVALUATED
    assert decision["execution_status"] == EXECUTION_STATUS_NOT_EVALUATED


def test_same_input_produces_same_structure_decision_id() -> None:
    candidate = _candidate(direction="LONG", current_regime=REGIME_TRENDING)

    first = select_structure_for_candidate_v1(candidate)
    second = select_structure_for_candidate_v1(dict(candidate))

    assert first["structure_decision_id"] == second["structure_decision_id"]
    assert first == second


def test_structure_decision_validates_against_schema() -> None:
    decision = select_structure_for_candidate_v1(_candidate(direction="NEUTRAL", current_regime=REGIME_CHOPPY))

    validate_against_repo_schema_v1(
        decision,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_structure_decision.v1.schema.json",
    )


def test_eod_advisory_includes_structure_selection_summary() -> None:
    trace = build_decision_trace_v1(
        day_utc=DAY,
        raw_signal_payloads=[
            _intent("C2_TREND_EQ_PRIMARY_V1", exposure_type="LONG_EQUITY", intent_id="trend-source"),
            _intent("C2_MEAN_REVERSION_EQ_V1", exposure_type="LONG_EQUITY", intent_id="choppy-source"),
        ],
        regime_state={"regime": REGIME_TRENDING, "confidence": "HIGH"},
        produced_utc="2026-04-28T20:00:00Z",
    )
    advisory = build_eod_advisory_from_decision_trace_v1(
        decision_trace=trace,
        produced_utc="2026-04-28T21:00:00Z",
    )

    validate_against_repo_schema_v1(
        trace,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_decision_trace.v1.schema.json",
    )
    validate_against_repo_schema_v1(
        advisory,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_eod_advisory.v1.schema.json",
    )
    summary = advisory["structure_selection_summary"]
    assert summary["candidates_evaluated"] == 2
    assert summary["advisory_only"] is True
    assert summary["controls_broker_execution"] is False
    assert summary["controls_phasec_materialization"] is False
    assert summary["fallback_used_count"] == 2
    assert trace["candidates"][0]["structure_input"]["fallback_used"] is True
    assert trace["candidates"][0]["structure_decision"]["selected_structure"] == STRUCTURE_VERTICAL_SPREAD


def test_structure_selection_summary_counts_selected_and_rejected() -> None:
    selected = select_structure_for_candidate_v1(_candidate(direction="LONG", current_regime=REGIME_TRENDING))
    rejected = select_structure_for_candidate_v1(_candidate(candidate_id="candidate-2", current_regime="UNKNOWN"))

    summary = build_structure_selection_summary_v1([selected, rejected])

    assert summary["candidates_evaluated"] == 2
    assert summary["structures_selected_count"] == 1
    assert summary["structures_rejected_count"] == 1
    assert summary["selected_structure_counts"][STRUCTURE_NO_TRADE] == 1
    assert summary["rejection_reason_counts"][RC_REJECTED_REGIME_UNKNOWN] == 1
