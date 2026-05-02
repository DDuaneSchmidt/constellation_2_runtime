from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from ops.tools.run_ai_advisory_review_v1 import build_ai_advisory_review_v1
from ops.tools.run_decision_ledger_v1 import build_decision_ledger_v1
from ops.tools.run_insight_engine_v1 import build_insight_engine_v1, insight_engine_path

DAY = "2026-05-01"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _report(truth: Path, artifact: str, filename: str) -> Path:
    return truth / "reports" / artifact / DAY / filename


def _base_truth(tmp_path: Path, *, selected: bool = True, low_confidence: bool = False, decision_flip: bool = False) -> Path:
    truth = tmp_path / "truth"
    selected_intent_id = "intent_a" if selected else ""
    selected_sleeve_id = "SLEEVE_A" if selected else ""
    decision_ledger = {
        "schema_id": "decision_ledger",
        "day_utc": DAY,
        "selected_intent_id": selected_intent_id,
        "canonical_phase": "PORTFOLIO_SELECTION" if selected else "MARKET_OPEN_DATA_GATE",
        "canonical_blocker": "" if selected else "MARKET_NOT_OPEN",
        "final_status": "SELECTED" if selected else "PRE_MARKET_READY",
        "reason_codes": [] if selected else ["MARKET_NOT_OPEN"],
        "operator_next_action": "Observe.",
    }
    _write_json(_report(truth, "decision_ledger_v1", "decision_ledger.v1.json"), decision_ledger)
    rankings = [
        {
            "intent_id": "intent_a",
            "sleeve_id": "SLEEVE_A",
            "rank": 1 if selected else 0,
            "score_total": 51.0,
            "executable_eligible": selected,
            "portfolio_gate_decision": "ALLOW" if selected else "SUPPRESS",
            "lifecycle_decision": "INTENT_CREATED" if selected else "NO_INTENT",
            "lifecycle_reason_codes": ["NEW_SIGNAL"] if selected else ["NO_RAW_SIGNAL"],
            "score_components": {"regime_alignment": 20.0},
            "raw_regime_alignment_component": 20.0,
            "reason_codes": ["SCORING_EXECUTABLE_ELIGIBLE"] if selected else ["SCORING_NOT_EXECUTABLE_SUPPRESS"],
            "evidence_paths": [str(_report(truth, "portfolio_scoring_v1", "portfolio_scoring.v1.json"))],
        },
        {
            "intent_id": "intent_b",
            "sleeve_id": "SLEEVE_B",
            "rank": 2 if selected else 0,
            "score_total": 49.5,
            "executable_eligible": selected,
            "portfolio_gate_decision": "ALLOW" if selected else "SUPPRESS",
            "lifecycle_decision": "INTENT_CREATED" if selected else "NO_INTENT",
            "reason_codes": ["LOWER_SCORE_THAN_SELECTED"],
            "evidence_paths": [str(_report(truth, "portfolio_scoring_v1", "portfolio_scoring.v1.json"))],
        },
    ]
    _write_json(
        _report(truth, "portfolio_scoring_v1", "portfolio_scoring.v1.json"),
        {
            "schema_id": "portfolio_scoring",
            "day_utc": DAY,
            "status": "PASS",
            "selected_candidate_intent_id": selected_intent_id,
            "intents_scored_count": 2 if selected else 0,
            "rankings": rankings,
            "ranked_intents": rankings,
        },
    )
    _write_json(
        _report(truth, "portfolio_activation_gate_v1", "portfolio_activation_gate.v1.json"),
        {
            "schema_id": "portfolio_activation_gate",
            "day_utc": DAY,
            "status": "PASS",
            "decisions": [
                {"raw_intent_id": "intent_a", "sleeve_id": "SLEEVE_A", "portfolio_gate_decision": "ALLOW" if selected else "SUPPRESS"},
                {"raw_intent_id": "intent_b", "sleeve_id": "SLEEVE_B", "portfolio_gate_decision": "ALLOW" if selected else "SUPPRESS"},
            ],
        },
    )
    _write_json(
        _report(truth, "intent_lifecycle_state_v1", "intent_lifecycle_state.v1.json"),
        {
            "schema_id": "intent_lifecycle_state",
            "day_utc": DAY,
            "counts": {"INTENT_CREATED": 2 if selected else 0, "NO_INTENT": 0 if selected else 2, "BLOCKED": 0},
            "rows": [
                {"intent_id": "intent_a", "sleeve_id": "SLEEVE_A", "lifecycle_decision": "INTENT_CREATED" if selected else "NO_INTENT", "lifecycle_reason_codes": ["NEW_SIGNAL"] if selected else ["NO_RAW_SIGNAL"]},
                {"intent_id": "intent_b", "sleeve_id": "SLEEVE_B", "lifecycle_decision": "INTENT_CREATED" if selected else "NO_INTENT", "lifecycle_reason_codes": ["NEW_SIGNAL"] if selected else ["NO_RAW_SIGNAL"]},
            ],
        },
    )
    _write_json(_report(truth, "position_lifecycle_state_v1", "position_lifecycle_state.v1.json"), {"schema_id": "position_lifecycle_state", "day_utc": DAY, "status": "PASS", "rows": []})
    score_gap = 1.5 if low_confidence else 8.0
    _write_json(
        _report(truth, "selection_quality_v1", "selection_quality.v1.json"),
        {
            "schema_id": "selection_quality",
            "day_utc": DAY,
            "status": "PASS" if selected else "VALID_ZERO",
            "selected_intent_id": selected_intent_id,
            "selected_sleeve_id": selected_sleeve_id,
            "score_gap": score_gap,
            "confidence_level": "LOW" if low_confidence else ("MEDIUM" if selected else "UNKNOWN"),
            "defer_recommended": low_confidence,
            "artifact_path": str(_report(truth, "selection_quality_v1", "selection_quality.v1.json")),
            "no_trade_quality_reason": "" if selected else "NO_EXECUTABLE_INTENT",
            "selected_vs_alternatives": [{"intent_id": "intent_b", "sleeve_id": "SLEEVE_B", "score_total": 49.5}],
        },
    )
    _write_json(_report(truth, "edge_attribution_v1", "edge_attribution.v1.json"), {"schema_id": "edge_attribution", "day_utc": DAY, "status": "UNPROVEN", "sleeves": [{"sleeve_id": "SLEEVE_A", "edge_health": "UNPROVEN", "trades": 0}]})
    _write_json(_report(truth, "regime_confidence_v1", "regime_confidence.v1.json"), {"schema_id": "regime_confidence", "day_utc": DAY, "status": "PASS", "regime": "TREND", "confidence_level": "HIGH", "confidence_score": 0.8, "transition_risk": "LOW"})
    _write_json(_report(truth, "trade_outcome_v1", "trade_outcome.v1.json"), {"schema_id": "trade_outcome", "day_utc": DAY, "status": "VALID_ZERO", "outcome_status": "NO_TRADE" if not selected else "OPEN", "intent_id": selected_intent_id, "sleeve_id": selected_sleeve_id})
    _write_json(_report(truth, "missed_opportunity_v1", "missed_opportunity.v1.json"), {"schema_id": "missed_opportunity", "day_utc": DAY, "status": "PASS", "alternatives": [{"alternative_intent_id": "intent_b"}], "no_fabricated_trades": True})
    _write_json(_report(truth, "decision_consistency_v1", "decision_consistency.v1.json"), {"schema_id": "decision_consistency", "day_utc": DAY, "status": "DEGRADED" if decision_flip else "PASS", "ranking_stability": "FLIPPED" if decision_flip else "STABLE", "decision_flip_detected": decision_flip, "nondeterminism_suspected": False})
    return truth


def test_insight_engine_is_read_only_and_records_evidence(tmp_path: Path) -> None:
    truth = _base_truth(tmp_path)
    scoring_path = _report(truth, "portfolio_scoring_v1", "portfolio_scoring.v1.json")
    before = _sha(scoring_path)

    payload = build_insight_engine_v1(day_utc=DAY, truth_root=truth)

    assert _sha(scoring_path) == before
    assert payload["prohibited_actions_attempted"] is False
    assert payload["read_only_policy"]["thresholds_changed"] is False
    assert payload["read_only_policy"]["trades_submitted"] is False
    assert payload["evidence_paths"]
    assert insight_engine_path(truth_root=truth, day_utc=DAY).exists()


def test_selected_trade_explanation_uses_artifact_evidence(tmp_path: Path) -> None:
    truth = _base_truth(tmp_path)

    payload = build_insight_engine_v1(day_utc=DAY, truth_root=truth)

    assert payload["why_this_trade"]["selected_intent_id"] == "intent_a"
    assert payload["why_this_trade"]["portfolio_gate_decision"] == "ALLOW"
    assert payload["why_this_trade"]["score"] == 51.0
    assert payload["why_this_trade"]["regime_fit"]["regime"] == "TREND"
    assert payload["why_this_trade"]["evidence_paths"]


def test_no_trade_explanation_identifies_canonical_blocker(tmp_path: Path) -> None:
    truth = _base_truth(tmp_path, selected=False)

    payload = build_insight_engine_v1(day_utc=DAY, truth_root=truth)

    assert payload["selected_intent_id"] == ""
    assert payload["why_no_trade"]["canonical_blocker"] == "MARKET_NOT_OPEN"
    assert payload["why_no_trade"]["classification"] == "market_session_mode"


def test_alternatives_are_listed_without_creating_trades(tmp_path: Path) -> None:
    truth = _base_truth(tmp_path)

    payload = build_insight_engine_v1(day_utc=DAY, truth_root=truth)

    assert payload["alternatives_not_selected"][0]["intent_id"] == "intent_b"
    assert payload["missed_opportunity_summary"]["no_fabricated_trades"] is True
    assert payload["read_only_policy"]["trades_submitted"] is False


def test_near_misses_do_not_alter_thresholds(tmp_path: Path) -> None:
    truth = _base_truth(tmp_path, low_confidence=True)
    scoring_path = _report(truth, "portfolio_scoring_v1", "portfolio_scoring.v1.json")
    before = _sha(scoring_path)

    payload = build_insight_engine_v1(day_utc=DAY, truth_root=truth)

    assert _sha(scoring_path) == before
    near = payload["near_misses"][0]
    assert near["metric"] == "selection_score_gap"
    assert near["threshold"] == 3.0
    assert payload["read_only_policy"]["scoring_weights_changed"] is False


def test_drift_alerts_are_diagnostic_only(tmp_path: Path) -> None:
    truth = _base_truth(tmp_path, decision_flip=True)

    payload = build_insight_engine_v1(day_utc=DAY, truth_root=truth)

    assert any(row["metric"] == "selected_sleeve_mix" for row in payload["drift_alerts"])
    assert all(row["diagnostic_only"] is True for row in payload["drift_alerts"])


def test_missing_inputs_produce_degraded_not_fabricated_analysis(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_json(_report(truth, "decision_ledger_v1", "decision_ledger.v1.json"), {"schema_id": "decision_ledger", "day_utc": DAY, "selected_intent_id": "", "canonical_blocker": "INPUTS_MISSING"})

    payload = build_insight_engine_v1(day_utc=DAY, truth_root=truth)

    assert payload["status"] == "DEGRADED"
    assert "portfolio_scoring_v1" in payload["missing_required_inputs"]
    assert payload["alternatives_not_selected"] == []
    assert payload["confidence_calibration"]["status"] == "UNPROVEN"


def test_governance_required_when_recommendations_exist(tmp_path: Path) -> None:
    truth = _base_truth(tmp_path, low_confidence=True)

    payload = build_insight_engine_v1(day_utc=DAY, truth_root=truth)

    assert payload["governance_required"] is True
    assert payload["advisory_recommendations"]
    assert payload["advisory_recommendations"][0]["governance_route"].endswith("strategy_change_governance.v1.json")
    assert payload["prohibited_actions_attempted"] is False


def test_ai_advisory_consumes_insight_engine_recommendations(tmp_path: Path) -> None:
    truth = _base_truth(tmp_path, low_confidence=True)
    build_insight_engine_v1(day_utc=DAY, truth_root=truth)

    payload = build_ai_advisory_review_v1(day_utc=DAY, truth_root=truth)

    assert payload["status"] == "NO_GOVERNED_ADVISORY_INPUT"
    assert payload["advisory_evidence_packet_status"] == "MISSING"
    assert payload["recommendations"] == []


def test_decision_ledger_records_insight_engine_path(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    day_run = {"day_utc": DAY, "final_status": "PRE_MARKET_READY", "canonical_phase": "MARKET_OPEN_DATA_GATE", "canonical_blocker": "MARKET_NOT_OPEN"}

    payload = build_decision_ledger_v1(day_utc=DAY, truth_root=truth, aegis_day_payload=day_run)

    assert payload["insight_engine_path"].endswith("insight_engine.v1.json")
    assert payload["performance_intelligence_paths"]["insight_engine_path"].endswith("insight_engine.v1.json")
