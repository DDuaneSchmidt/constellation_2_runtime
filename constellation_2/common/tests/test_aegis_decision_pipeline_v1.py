from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_decision_pipeline_v1 import (
    RC_REGIME_COMPATIBLE,
    RC_REGIME_INCOMPATIBLE,
    RC_REGIME_UNKNOWN,
    RC_SLEEVE_COMPATIBILITY_UNKNOWN,
    REGIME_CHOPPY,
    REGIME_HIGH_VOL_UNSTABLE,
    REGIME_TRENDING,
    STATUS_REGIME_ALLOWED,
    STATUS_REGIME_REJECTED,
    build_decision_trace_v1,
    build_eod_advisory_from_decision_trace_v1,
    classify_market_regime_v1,
    evaluate_candidate_regime_compatibility_v1,
    load_compatibility_registry_v1,
    normalize_candidate_v1,
    write_decision_trace_v1,
    write_eod_advisory_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


DAY = "2026-04-28"

APPROVED_MAPPING = {
    "C2_TREND_EQ_PRIMARY_V1": {REGIME_TRENDING},
    "C2_CROSS_ASSET_TREND_V1": {REGIME_TRENDING},
    "C2_MEAN_REVERSION_EQ_V1": {REGIME_CHOPPY},
    "C2_MARKET_NEUTRAL_SPREAD_V1": {REGIME_CHOPPY},
    "C2_VOL_INCOME_DEFINED_RISK_V1": {REGIME_TRENDING},
    "C2_EVENT_DISLOCATION_V1": {REGIME_TRENDING, REGIME_HIGH_VOL_UNSTABLE},
    "C2_DEFENSIVE_TAIL_V1": {REGIME_HIGH_VOL_UNSTABLE},
}


def _intent(engine_id: str, *, symbol: str = "SPY", intent_id: str = "intent-1") -> dict:
    return {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": intent_id,
        "created_at_utc": f"{DAY}T00:00:00Z",
        "engine": {"engine_id": engine_id, "suite": "C2_HYBRID_V1", "mode": "PAPER"},
        "underlying": {"symbol": symbol, "currency": "USD"},
        "exposure_type": "LONG_EQUITY",
        "target_notional_pct": "0.01",
        "expected_holding_days": 3,
        "risk_class": "TEST",
        "constraints": {"max_risk_pct": "0.01"},
        "canonical_json_hash": None,
    }


def _write_json(path: Path, payload: dict) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def test_registry_has_exact_approved_sleeve_mapping_and_valid_schema() -> None:
    registry = load_compatibility_registry_v1()
    validate_against_repo_schema_v1(
        registry,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/REGISTRIES/aegis_sleeve_regime_compatibility.v1.schema.json",
    )

    actual = {
        str(row["sleeve_id"]): set(row["compatible_regimes"])
        for row in registry["sleeves"]
    }

    assert actual == APPROVED_MAPPING


def test_normalized_candidate_preserves_required_trace_fields() -> None:
    candidate = normalize_candidate_v1(
        raw_signal_payload=_intent("C2_TREND_EQ_PRIMARY_V1", symbol="QQQ"),
        current_regime=REGIME_TRENDING,
        candidate_id="candidate-1",
        timestamp="2026-04-28T14:30:00Z",
    )

    assert candidate["candidate_id"] == "candidate-1"
    assert candidate["timestamp"] == "2026-04-28T14:30:00Z"
    assert candidate["sleeve_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert candidate["symbol"] == "QQQ"
    assert candidate["direction"] == "LONG"
    assert candidate["entry_type"] == "LONG_EQUITY"
    assert candidate["current_regime"] == REGIME_TRENDING
    assert candidate["decision_status"] == "proposed"
    assert candidate["risk_status"] == "not_evaluated"
    assert candidate["execution_status"] == "not_attempted"
    assert candidate["raw_signal_payload"]["intent_id"] == "intent-1"


def test_unknown_sleeve_mapping_fails_closed() -> None:
    registry = load_compatibility_registry_v1()
    state = classify_market_regime_v1(trend_strength="STRONG", volatility_level="NORMAL")
    candidate = normalize_candidate_v1(
        raw_signal_payload=_intent("C2_UNKNOWN_SLEEVE_V1"),
        current_regime=state.regime,
    )

    decided = evaluate_candidate_regime_compatibility_v1(candidate=candidate, regime_state=state, registry=registry)

    assert decided["decision_status"] == STATUS_REGIME_REJECTED
    assert decided["regime_compatible"] is False
    assert decided["decision_reason_codes"] == [RC_SLEEVE_COMPATIBILITY_UNKNOWN]


def test_unknown_regime_fails_closed() -> None:
    registry = load_compatibility_registry_v1()
    candidate = normalize_candidate_v1(
        raw_signal_payload=_intent("C2_TREND_EQ_PRIMARY_V1"),
        current_regime="UNKNOWN",
    )

    decided = evaluate_candidate_regime_compatibility_v1(
        candidate=candidate,
        regime_state={"regime": "UNKNOWN", "confidence": "LOW"},
        registry=registry,
    )

    assert decided["decision_status"] == STATUS_REGIME_REJECTED
    assert decided["decision_reason_codes"] == [RC_REGIME_UNKNOWN]


def test_high_vol_unstable_overrides_trending_inputs() -> None:
    state = classify_market_regime_v1(
        trend_strength="STRONG",
        volatility_level="HIGH",
        breadth_participation="BROAD",
    )

    assert state.regime == REGIME_HIGH_VOL_UNSTABLE
    assert "HIGH_VOL_UNSTABLE_OVERRIDE" in state.reason_codes


def test_unknown_trend_does_not_classify_as_choppy() -> None:
    state = classify_market_regime_v1(
        trend_strength="UNKNOWN",
        volatility_level="NORMAL",
        breadth_participation="BROAD",
    )

    assert state.regime == "UNKNOWN"
    assert "TREND_STRENGTH_UNKNOWN" in state.reason_codes


def test_unknown_volatility_does_not_classify_as_trending_or_choppy() -> None:
    trending_input = classify_market_regime_v1(
        trend_strength="STRONG",
        volatility_level="UNKNOWN",
        breadth_participation="BROAD",
    )
    choppy_input = classify_market_regime_v1(
        trend_strength="LOW",
        volatility_level="UNKNOWN",
        breadth_participation="BROAD",
    )

    assert trending_input.regime == "UNKNOWN"
    assert choppy_input.regime == "UNKNOWN"
    assert "VOLATILITY_LEVEL_UNKNOWN" in trending_input.reason_codes
    assert "VOLATILITY_LEVEL_UNKNOWN" in choppy_input.reason_codes


def test_incompatible_regime_rejects_candidate_with_reason() -> None:
    registry = load_compatibility_registry_v1()
    state = classify_market_regime_v1(
        trend_strength="LOW",
        volatility_level="NORMAL",
        breadth_participation="NARROW",
    )
    candidate = normalize_candidate_v1(
        raw_signal_payload=_intent("C2_TREND_EQ_PRIMARY_V1"),
        current_regime=state.regime,
    )

    decided = evaluate_candidate_regime_compatibility_v1(candidate=candidate, regime_state=state, registry=registry)

    assert state.regime == REGIME_CHOPPY
    assert decided["decision_status"] == STATUS_REGIME_REJECTED
    assert decided["regime_compatible"] is False
    assert decided["decision_reason_codes"] == [RC_REGIME_INCOMPATIBLE]


def test_event_dislocation_is_compatible_with_trending_and_high_vol_unstable() -> None:
    registry = load_compatibility_registry_v1()
    event_payload = _intent("C2_EVENT_DISLOCATION_V1", symbol="GLD")

    trending = evaluate_candidate_regime_compatibility_v1(
        candidate=normalize_candidate_v1(raw_signal_payload=event_payload, current_regime=REGIME_TRENDING),
        regime_state={"regime": REGIME_TRENDING, "confidence": "HIGH"},
        registry=registry,
    )
    high_vol = evaluate_candidate_regime_compatibility_v1(
        candidate=normalize_candidate_v1(raw_signal_payload=event_payload, current_regime=REGIME_HIGH_VOL_UNSTABLE),
        regime_state={"regime": REGIME_HIGH_VOL_UNSTABLE, "confidence": "HIGH"},
        registry=registry,
    )

    assert trending["decision_status"] == STATUS_REGIME_ALLOWED
    assert high_vol["decision_status"] == STATUS_REGIME_ALLOWED
    assert trending["decision_reason_codes"] == [RC_REGIME_COMPATIBLE]
    assert high_vol["decision_reason_codes"] == [RC_REGIME_COMPATIBLE]


def test_regime_allowed_candidate_preserves_full_decision_trace() -> None:
    state = classify_market_regime_v1(
        trend_strength="STRONG",
        volatility_level="NORMAL",
        breadth_participation="BROAD",
    )

    trace = build_decision_trace_v1(
        day_utc=DAY,
        raw_signal_payloads=[_intent("C2_TREND_EQ_PRIMARY_V1", symbol="SPY", intent_id="trend-1")],
        regime_state=state,
        produced_utc="2026-04-28T20:00:00Z",
    )

    candidate = trace["candidates"][0]
    assert trace["schema_id"] == "C2_AEGIS_DECISION_TRACE_V1"
    assert trace["regime_state"]["regime"] == REGIME_TRENDING
    assert candidate["decision_status"] == STATUS_REGIME_ALLOWED
    assert candidate["regime_compatible"] is True
    assert candidate["decision_reason_codes"] == [RC_REGIME_COMPATIBLE]
    assert candidate["risk_status"] == "not_evaluated"
    assert candidate["execution_status"] == "not_attempted"
    assert candidate["raw_signal_payload"]["intent_id"] == "trend-1"
    assert trace["summary"]["candidates_generated"] == 1
    assert trace["summary"]["candidates_regime_allowed"] == 1
    assert trace["summary"]["candidates_rejected_by_regime"] == 0
    assert trace["canonical_json_hash"]


def test_regime_allowed_does_not_imply_risk_approval() -> None:
    state = classify_market_regime_v1(
        trend_strength="STRONG",
        volatility_level="NORMAL",
        breadth_participation="BROAD",
    )

    trace = build_decision_trace_v1(
        day_utc=DAY,
        raw_signal_payloads=[_intent("C2_TREND_EQ_PRIMARY_V1")],
        regime_state=state,
        produced_utc="2026-04-28T20:00:00Z",
    )
    candidate = trace["candidates"][0]

    assert candidate["decision_status"] == STATUS_REGIME_ALLOWED
    assert candidate["risk_status"] == "not_evaluated"
    assert "RISK_APPROVED" not in candidate["decision_reason_codes"]


def test_trace_includes_source_refs_and_sha_fields(tmp_path: Path) -> None:
    intent_path = tmp_path / "intents_v1" / "snapshots" / DAY / "intent.exposure_intent.v1.json"
    producer_path = tmp_path / "reports" / "trading_day_intent_generation_v1" / DAY / "trading_day_intent_generation.v1.json"
    regime_path = tmp_path / "monitoring_v1" / "aegis_regime_v1" / DAY / "regime.v1.json"
    intent_sha = _write_json(intent_path, _intent("C2_TREND_EQ_PRIMARY_V1", intent_id="source-ref-test"))
    producer_sha = _write_json(producer_path, {"status": "INTENTS_PRESENT"})
    regime_sha = _write_json(regime_path, {"regime": REGIME_TRENDING, "confidence": "HIGH"})

    raw = _intent("C2_TREND_EQ_PRIMARY_V1", intent_id="source-ref-test")
    raw["source_intent_path"] = str(intent_path)
    raw["source_intent_sha256"] = intent_sha
    state = classify_market_regime_v1(
        trend_strength="STRONG",
        volatility_level="NORMAL",
        breadth_participation="BROAD",
    )

    trace = build_decision_trace_v1(
        day_utc=DAY,
        raw_signal_payloads=[raw],
        regime_state=state,
        producer_result_path=producer_path,
        regime_source_path=regime_path,
        produced_utc="2026-04-28T20:00:00Z",
    )

    validate_against_repo_schema_v1(
        trace,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_decision_trace.v1.schema.json",
    )
    candidate = trace["candidates"][0]
    assert candidate["source_intent_path"] == str(intent_path)
    assert candidate["source_intent_sha256"] == intent_sha
    assert candidate["producer_result_path"] == str(producer_path)
    assert candidate["producer_result_sha256"] == producer_sha
    assert candidate["registry_path"].endswith("C2_AEGIS_SLEEVE_REGIME_COMPATIBILITY_V1.json")
    assert len(candidate["registry_sha256"]) == 64
    assert candidate["regime_source_path"] == str(regime_path)
    assert candidate["regime_source_sha256"] == regime_sha
    assert trace["trace_sources"]["producer_result_sha256"] == producer_sha
    assert trace["trace_sources"]["regime_source_sha256"] == regime_sha


def test_eod_advisory_summarizes_allowed_rejected_and_sources(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    producer_path = tmp_path / "reports" / "trading_day_intent_generation_v1" / DAY / "trading_day_intent_generation.v1.json"
    regime_path = tmp_path / "monitoring_v1" / "aegis_regime_v1" / DAY / "regime.v1.json"
    producer_sha = _write_json(producer_path, {"status": "INTENTS_PRESENT"})
    regime_sha = _write_json(regime_path, {"regime": REGIME_TRENDING, "confidence": "HIGH"})

    trend_intent_path = tmp_path / "intents" / "trend.json"
    mean_reversion_intent_path = tmp_path / "intents" / "mean_reversion.json"
    trend_sha = _write_json(trend_intent_path, _intent("C2_TREND_EQ_PRIMARY_V1", intent_id="trend-source"))
    mean_reversion_sha = _write_json(
        mean_reversion_intent_path,
        _intent("C2_MEAN_REVERSION_EQ_V1", intent_id="mr-source"),
    )

    trend_raw = _intent("C2_TREND_EQ_PRIMARY_V1", intent_id="trend-source")
    trend_raw["source_intent_path"] = str(trend_intent_path)
    trend_raw["source_intent_sha256"] = trend_sha
    mean_reversion_raw = _intent("C2_MEAN_REVERSION_EQ_V1", intent_id="mr-source")
    mean_reversion_raw["source_intent_path"] = str(mean_reversion_intent_path)
    mean_reversion_raw["source_intent_sha256"] = mean_reversion_sha
    state = classify_market_regime_v1(
        trend_strength="STRONG",
        volatility_level="NORMAL",
        breadth_participation="BROAD",
    )
    trace = build_decision_trace_v1(
        day_utc=DAY,
        raw_signal_payloads=[trend_raw, mean_reversion_raw],
        regime_state=state,
        producer_result_path=producer_path,
        regime_source_path=regime_path,
        produced_utc="2026-04-28T20:00:00Z",
    )
    trace_path = write_decision_trace_v1(truth_root=truth_root, day_utc=DAY, payload=trace)
    trace_sha = hashlib.sha256(trace_path.read_bytes()).hexdigest()

    advisory = build_eod_advisory_from_decision_trace_v1(
        decision_trace=trace,
        source_trace_path=trace_path,
        produced_utc="2026-04-28T21:00:00Z",
    )
    advisory_path = write_eod_advisory_v1(truth_root=truth_root, day_utc=DAY, payload=advisory)

    validate_against_repo_schema_v1(
        advisory,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_eod_advisory.v1.schema.json",
    )
    assert advisory_path.exists()
    assert advisory["current_regime"] == REGIME_TRENDING
    assert advisory["active_sleeves"] == ["C2_TREND_EQ_PRIMARY_V1"]
    assert advisory["disabled_sleeves"] == ["C2_MEAN_REVERSION_EQ_V1"]
    assert advisory["candidates_proposed"] == 2
    assert advisory["candidates_regime_allowed"] == 1
    assert advisory["candidates_regime_rejected"] == 1
    assert advisory["rejection_reasons"] == {RC_REGIME_INCOMPATIBLE: 1}
    assert advisory["risk_status_summary"] == {"not_evaluated": 2}
    assert advisory["execution_status_summary"] == {"not_attempted": 2}
    assert advisory["source_trace_refs"]["decision_trace_path"] == str(trace_path)
    assert advisory["source_trace_refs"]["decision_trace_sha256"] == trace_sha
    assert advisory["source_trace_refs"]["producer_result_sha256"] == producer_sha
    assert advisory["source_trace_refs"]["regime_source_sha256"] == regime_sha
    assert len(advisory["source_trace_refs"]["candidate_source_refs"]) == 2


def test_eod_advisory_is_observability_only_not_a_gate() -> None:
    state = classify_market_regime_v1(
        trend_strength="STRONG",
        volatility_level="NORMAL",
        breadth_participation="BROAD",
    )
    trace = build_decision_trace_v1(
        day_utc=DAY,
        raw_signal_payloads=[_intent("C2_TREND_EQ_PRIMARY_V1")],
        regime_state=state,
        produced_utc="2026-04-28T20:00:00Z",
    )

    advisory = build_eod_advisory_from_decision_trace_v1(
        decision_trace=trace,
        produced_utc="2026-04-28T21:00:00Z",
    )

    assert advisory["advisory_only"] is True
    assert advisory["controls_phasec_materialization"] is False
    assert advisory["controls_broker_execution"] is False
