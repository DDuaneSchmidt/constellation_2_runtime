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
from ops.tools.run_edge_attribution_v1 import build_edge_attribution_v1
from ops.tools.run_portfolio_scoring_v1 import build_portfolio_scoring_v1, portfolio_scoring_path
from ops.tools.run_regime_confidence_v1 import build_regime_confidence_v1
from ops.tools.run_selection_quality_v1 import build_selection_quality_v1
from ops.tools.run_strategy_change_governance_v1 import build_strategy_change_governance_v1


DAY = "2026-05-01"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _portfolio_state(truth: Path, *, regime: str = "TREND", status: str = "PASS", missing: list[str] | None = None) -> None:
    _write_json(
        truth / "reports" / "portfolio_state_v1" / DAY / "portfolio_state.v1.json",
        {
            "schema_id": "portfolio_state",
            "day_utc": DAY,
            "status": status,
            "regime": regime,
            "trend_strength": "HIGH" if regime != "UNKNOWN" else "UNKNOWN",
            "volatility_regime": "NORMAL",
            "correlation_regime": "NORMAL",
            "dispersion_regime": "NORMAL",
            "equity_beta_state": "NORMAL",
            "missing_inputs": missing or [],
            "degraded_inputs": [],
            "bootstrap_inputs": [],
            "inputs_used": [],
            "artifact_path": str(truth / "reports" / "portfolio_state_v1" / DAY / "portfolio_state.v1.json"),
        },
    )


def _scoring(truth: Path, rankings: list[dict]) -> None:
    _write_json(
        portfolio_scoring_path(truth_root=truth, day_utc=DAY),
        {
            "schema_id": "portfolio_scoring",
            "schema_version": "v1",
            "day_utc": DAY,
            "status": "PASS",
            "intents_scored_count": len([row for row in rankings if row.get("executable_eligible")]),
            "rankings": rankings,
            "ranked_intents": rankings,
        },
    )


def _selected_pointer(truth: Path, intent_id: str = "intent_a", sleeve_id: str = "SLEEVE_A") -> None:
    _write_json(
        truth / "pointers" / "selected_intent_pointer.v1.json",
        {"schema_id": "selected_intent_pointer", "day_utc": DAY, "status": "SELECTED", "selected_intent": {"intent_id": intent_id, "sleeve_id": sleeve_id}},
    )


def test_selection_quality_computes_score_gap_deterministically(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected_pointer(truth)
    _scoring(
        truth,
        [
            {"intent_id": "intent_a", "sleeve_id": "SLEEVE_A", "score_total": 42.0, "rank": 1, "executable_eligible": True},
            {"intent_id": "intent_b", "sleeve_id": "SLEEVE_B", "score_total": 39.5, "rank": 2, "executable_eligible": True},
        ],
    )

    payload = build_selection_quality_v1(day_utc=DAY, truth_root=truth)

    assert payload["score_gap"] == 2.5
    assert payload["confidence_level"] == "LOW"
    assert payload["defer_recommended"] is True


def test_selection_quality_classifies_no_trade(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_json(truth / "pointers" / "selected_intent_pointer.v1.json", {"status": "BLOCKED", "selected_intent": {}})
    _scoring(truth, [{"intent_id": "x", "score_total": 0.0, "executable_eligible": False, "reason_codes": ["SCORING_NOT_EXECUTABLE_SUPPRESS"]}])

    payload = build_selection_quality_v1(day_utc=DAY, truth_root=truth)

    assert payload["status"] == "VALID_ZERO"
    assert payload["no_trade_quality_reason"] == "NO_EXECUTABLE_INTENT"


def test_edge_attribution_uses_real_fills_only_and_unproven_for_short_history(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_json(
        truth / "fill_ledger_v1" / DAY / "fills.json",
        {
            "fills": [
                {"sleeve_id": "SLEEVE_A", "fill_id": "fill-1", "realized_pnl": 5.0, "notional": 100.0},
                {"sleeve_id": "SLEEVE_A", "fill_id": "proxy-1", "realized_pnl": 500.0, "notional": 100.0, "proxy": True},
            ]
        },
    )

    payload = build_edge_attribution_v1(day_utc=DAY, truth_root=truth)
    row = payload["sleeves"][0]

    assert row["trades"] == 1
    assert row["avg_return"] == 0.05
    assert row["edge_health"] == "UNPROVEN"
    assert payload["proxy_pnl_included"] is False


def test_regime_confidence_handles_degraded_inputs(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _portfolio_state(truth, regime="UNKNOWN", status="DEGRADED", missing=["regime_snapshot_v2"])

    payload = build_regime_confidence_v1(day_utc=DAY, truth_root=truth)

    assert payload["status"] == "DEGRADED"
    assert payload["confidence_level"] == "UNKNOWN"
    assert "regime_snapshot_v2" in payload["degraded_inputs"]


def test_low_regime_confidence_damps_scoring_regime_component(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _portfolio_state(truth, regime="TREND")
    _write_json(
        truth / "reports" / "regime_confidence_v1" / DAY / "regime_confidence.v1.json",
        {"schema_id": "regime_confidence", "day_utc": DAY, "confidence_score": 0.4, "confidence_level": "LOW"},
    )
    gate_path = truth / "reports" / "portfolio_activation_gate_v1" / DAY / "portfolio_activation_gate.v1.json"
    _write_json(
        gate_path,
        {
            "schema_id": "portfolio_activation_gate",
            "day_utc": DAY,
            "status": "PASS",
            "decisions": [
                {
                    "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                    "raw_intent_id": "intent_a",
                    "raw_intent_symbol": "SPY",
                    "raw_signal_status": "ACTIVE",
                    "portfolio_gate_decision": "ALLOW",
                    "allowed_by_portfolio_gate": True,
                    "reason_codes": [],
                }
            ],
            "artifact_path": str(gate_path),
        },
    )

    payload = build_portfolio_scoring_v1(day_utc=DAY, truth_root=truth, portfolio_gate_path_arg=gate_path)
    row = payload["rankings"][0]

    assert row["raw_regime_alignment_component"] == 25.0
    assert row["regime_confidence_multiplier"] == 0.4
    assert row["score_components"]["regime_alignment"] == 10.0


def _advisory_inputs(truth: Path) -> None:
    _write_json(truth / "reports" / "decision_ledger_v1" / DAY / "decision_ledger.v1.json", {"schema_id": "decision_ledger", "day_utc": DAY})
    _write_json(truth / "reports" / "selection_quality_v1" / DAY / "selection_quality.v1.json", {"status": "PASS", "confidence_level": "LOW", "score_gap": 1.0, "defer_recommended": True})
    _write_json(truth / "reports" / "edge_attribution_v1" / DAY / "edge_attribution.v1.json", {"status": "UNPROVEN", "sleeves": [{"sleeve_id": "SLEEVE_A", "edge_health": "UNPROVEN", "trades": 1}]})
    _write_json(truth / "reports" / "regime_confidence_v1" / DAY / "regime_confidence.v1.json", {"status": "DEGRADED", "regime": "UNKNOWN", "confidence_score": 0.2, "confidence_level": "UNKNOWN", "transition_risk": "HIGH"})
    _scoring(truth, [])
    _write_json(truth / "reports" / "portfolio_activation_gate_v1" / DAY / "portfolio_activation_gate.v1.json", {"status": "PASS", "decisions": []})
    _write_json(truth / "reports" / "intent_lifecycle_state_v1" / DAY / "intent_lifecycle_state.v1.json", {"counts": {"INTENT_CREATED": 0, "NO_INTENT": 1, "BLOCKED": 0}})
    _write_json(truth / "reports" / "position_lifecycle_state_v1" / DAY / "position_lifecycle_state.v1.json", {"rows": []})
    _write_json(truth / "reports" / "safety_state_authority_v1" / DAY / "safety_state_authority.v1.json", {"status": "PASS"})
    _write_json(truth / "reports" / "trading_day_readiness_authority_v1" / DAY / "trading_day_readiness_authority.v1.json", {"readiness_mode": "PREOPEN_BUILD"})


def test_ai_advisory_is_read_only_and_requires_human_review(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _advisory_inputs(truth)
    scoring_path = portfolio_scoring_path(truth_root=truth, day_utc=DAY)
    before = _sha(scoring_path)

    payload = build_ai_advisory_review_v1(day_utc=DAY, truth_root=truth)

    assert _sha(scoring_path) == before
    assert payload["requires_human_review"] is True
    assert payload["prohibited_actions_attempted"] is False
    assert payload["status"] == "NO_GOVERNED_ADVISORY_INPUT"
    assert payload["recommendations"] == []
    assert payload["authority"] == "ADVISORY_ONLY"


def test_strategy_governance_blocks_premature_changes(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _advisory_inputs(truth)
    build_ai_advisory_review_v1(day_utc=DAY, truth_root=truth)

    payload = build_strategy_change_governance_v1(day_utc=DAY, truth_root=truth)

    assert payload["status"] == "OBSERVE"
    assert payload["human_approval_required"] is True
    assert payload["automatic_deployment_allowed"] is False
    assert payload["minimum_evidence_window_met"] is False


def test_decision_ledger_records_performance_intelligence_paths(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_json(truth / "pointers" / "selected_intent_pointer.v1.json", {"status": "BLOCKED", "selected_intent": {}})
    _write_json(truth / "reports" / "portfolio_scoring_v1" / DAY / "portfolio_scoring.v1.json", {"rankings": []})
    day_run = {"day_utc": DAY, "final_status": "PRE_MARKET_READY", "canonical_phase": "MARKET_OPEN_DATA_GATE", "canonical_blocker": "MARKET_NOT_OPEN"}

    payload = build_decision_ledger_v1(day_utc=DAY, truth_root=truth, aegis_day_payload=day_run)

    assert payload["selection_quality_path"].endswith("selection_quality.v1.json")
    assert payload["edge_attribution_path"].endswith("edge_attribution.v1.json")
    assert payload["regime_confidence_path"].endswith("regime_confidence.v1.json")
    assert payload["ai_advisory_review_path"].endswith("ai_advisory_review.v1.json")
    assert payload["strategy_change_governance_path"].endswith("strategy_change_governance.v1.json")
