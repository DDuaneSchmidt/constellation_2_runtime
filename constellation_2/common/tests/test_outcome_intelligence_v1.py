from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from ops.tools.run_ai_advisory_review_v1 import build_ai_advisory_review_v1
from ops.tools.run_decision_consistency_v1 import build_decision_consistency_v1
from ops.tools.run_decision_ledger_v1 import build_decision_ledger_v1
from ops.tools.run_edge_attribution_v1 import build_edge_attribution_v1
from ops.tools.run_missed_opportunity_v1 import build_missed_opportunity_v1
from ops.tools.run_portfolio_scoring_v1 import portfolio_scoring_path
from ops.tools.run_trade_outcome_v1 import build_trade_outcome_v1

DAY = "2026-05-01"
PRIOR_DAY = "2026-04-30"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _selected_pointer(truth: Path, *, intent_id: str = "intent_a", sleeve_id: str = "SLEEVE_A", symbol: str = "IWM") -> None:
    _write_json(
        truth / "pointers" / "selected_intent_pointer.v1.json",
        {
            "schema_id": "selected_intent_pointer",
            "day_utc": DAY,
            "status": "SELECTED" if intent_id else "BLOCKED",
            "selected_intent": {"intent_id": intent_id, "sleeve_id": sleeve_id, "symbol": symbol} if intent_id else {},
        },
    )


def _scoring(truth: Path, day: str, rankings: list[dict]) -> None:
    _write_json(
        portfolio_scoring_path(truth_root=truth, day_utc=day),
        {
            "schema_id": "portfolio_scoring",
            "schema_version": "v1",
            "day_utc": day,
            "status": "PASS",
            "intents_scored_count": len([row for row in rankings if row.get("executable_eligible")]),
            "rankings": rankings,
            "ranked_intents": rankings,
        },
    )


def _position_lifecycle(truth: Path, row: dict) -> None:
    _write_json(
        truth / "reports" / "position_lifecycle_state_v1" / DAY / "position_lifecycle_state.v1.json",
        {"schema_id": "position_lifecycle_state", "day_utc": DAY, "status": "PASS", "rows": [row]},
    )


def test_no_trade_day_produces_no_trade_outcome(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected_pointer(truth, intent_id="")

    payload = build_trade_outcome_v1(day_utc=DAY, truth_root=truth)

    assert payload["status"] == "VALID_ZERO"
    assert payload["outcome_status"] == "NO_TRADE"
    assert payload["prohibited_actions_attempted"] is False


def test_open_trade_produces_open_outcome(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected_pointer(truth)
    _position_lifecycle(
        truth,
        {
            "intent_id": "intent_a",
            "sleeve_id": "SLEEVE_A",
            "symbol": "IWM",
            "lifecycle_state": "POSITION_OPEN",
            "avg_entry_price": 100.0,
            "current_mark": 102.5,
            "quantity_open": 2,
            "opened_at_utc": "2026-05-01T14:00:00Z",
        },
    )

    payload = build_trade_outcome_v1(day_utc=DAY, truth_root=truth)

    assert payload["outcome_status"] == "OPEN"
    assert payload["unrealized_pnl"] == 5.0
    assert payload["return_pct"] == 0.025


def test_closed_trade_computes_realized_pnl(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected_pointer(truth)
    _position_lifecycle(
        truth,
        {
            "intent_id": "intent_a",
            "sleeve_id": "SLEEVE_A",
            "symbol": "IWM",
            "lifecycle_state": "POSITION_CLOSED",
            "avg_entry_price": 100.0,
            "exit_price": 97.0,
            "quantity": 3,
            "opened_at_utc": "2026-05-01T14:00:00Z",
            "closed_at_utc": "2026-05-01T15:00:00Z",
        },
    )

    payload = build_trade_outcome_v1(day_utc=DAY, truth_root=truth)

    assert payload["outcome_status"] == "CLOSED"
    assert payload["realized_pnl"] == -9.0
    assert payload["return_pct"] == -0.03
    assert payload["holding_period"] == "3600s"


def test_edge_attribution_consumes_closed_trade_outcome(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected_pointer(truth)
    _position_lifecycle(
        truth,
        {
            "intent_id": "intent_a",
            "sleeve_id": "SLEEVE_A",
            "symbol": "IWM",
            "lifecycle_state": "POSITION_CLOSED",
            "avg_entry_price": 100.0,
            "exit_price": 110.0,
            "quantity": 1,
        },
    )
    build_trade_outcome_v1(day_utc=DAY, truth_root=truth)

    payload = build_edge_attribution_v1(day_utc=DAY, truth_root=truth)

    assert payload["sleeves"][0]["sleeve_id"] == "SLEEVE_A"
    assert payload["sleeves"][0]["trades"] == 1
    assert payload["sleeves"][0]["avg_return"] == 0.1


def test_decision_consistency_detects_rank_flip(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected_pointer(truth, intent_id="intent_a")
    _scoring(
        truth,
        PRIOR_DAY,
        [
            {"intent_id": "intent_b", "score_total": 51.0, "rank": 1, "executable_eligible": True},
            {"intent_id": "intent_a", "score_total": 49.0, "rank": 2, "executable_eligible": True},
        ],
    )
    _write_json(truth / "reports" / "decision_ledger_v1" / PRIOR_DAY / "decision_ledger.v1.json", {"selected_intent_id": "intent_b"})
    _scoring(
        truth,
        DAY,
        [
            {"intent_id": "intent_a", "score_total": 52.0, "rank": 1, "executable_eligible": True},
            {"intent_id": "intent_b", "score_total": 48.0, "rank": 2, "executable_eligible": True},
        ],
    )

    payload = build_decision_consistency_v1(day_utc=DAY, truth_root=truth)

    assert payload["decision_flip_detected"] is True
    assert payload["ranking_stability"] == "FLIPPED"
    assert payload["flip_reason"] == "RANK_ORDER_CHANGED"


def test_missed_opportunity_records_alternatives_without_creating_trades(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _write_json(
        truth / "reports" / "selection_quality_v1" / DAY / "selection_quality.v1.json",
        {
            "selected_intent_id": "intent_a",
            "selected_vs_alternatives": [{"intent_id": "intent_b", "sleeve_id": "SLEEVE_B", "score_total": 47.0}],
        },
    )
    _scoring(truth, DAY, [{"intent_id": "intent_b", "sleeve_id": "SLEEVE_B", "score_total": 47.0, "rank": 2, "reason_codes": []}])

    payload = build_missed_opportunity_v1(day_utc=DAY, truth_root=truth)

    assert payload["status"] == "PASS"
    assert payload["alternatives"][0]["alternative_intent_id"] == "intent_b"
    assert payload["alternatives"][0]["created_trade"] is False
    assert payload["alternatives"][0]["hypothetical_or_proxy"] is True


def _advisory_inputs(truth: Path) -> None:
    _write_json(truth / "reports" / "decision_ledger_v1" / DAY / "decision_ledger.v1.json", {"schema_id": "decision_ledger", "day_utc": DAY})
    _write_json(truth / "reports" / "selection_quality_v1" / DAY / "selection_quality.v1.json", {"status": "PASS", "confidence_level": "LOW", "score_gap": 1.0, "defer_recommended": True})
    _write_json(truth / "reports" / "edge_attribution_v1" / DAY / "edge_attribution.v1.json", {"status": "UNPROVEN", "sleeves": []})
    _write_json(truth / "reports" / "regime_confidence_v1" / DAY / "regime_confidence.v1.json", {"status": "PASS", "regime": "TREND", "confidence_score": 0.8, "confidence_level": "HIGH", "transition_risk": "LOW"})
    _write_json(truth / "reports" / "trade_outcome_v1" / DAY / "trade_outcome.v1.json", {"status": "PASS", "outcome_status": "CLOSED", "intent_id": "intent_a", "return_pct": -0.01, "realized_pnl": -10.0})
    _write_json(truth / "reports" / "decision_consistency_v1" / DAY / "decision_consistency.v1.json", {"status": "DEGRADED", "ranking_stability": "FLIPPED", "decision_flip_detected": True, "nondeterminism_suspected": False})
    _write_json(truth / "reports" / "missed_opportunity_v1" / DAY / "missed_opportunity.v1.json", {"status": "PASS", "alternatives": [{"alternative_intent_id": "intent_b"}], "no_fabricated_trades": True})
    _scoring(truth, DAY, [])
    _write_json(truth / "reports" / "portfolio_activation_gate_v1" / DAY / "portfolio_activation_gate.v1.json", {"status": "PASS", "decisions": []})
    _write_json(truth / "reports" / "intent_lifecycle_state_v1" / DAY / "intent_lifecycle_state.v1.json", {"counts": {"INTENT_CREATED": 0, "NO_INTENT": 1, "BLOCKED": 0}})
    _write_json(truth / "reports" / "position_lifecycle_state_v1" / DAY / "position_lifecycle_state.v1.json", {"rows": []})
    _write_json(truth / "reports" / "safety_state_authority_v1" / DAY / "safety_state_authority.v1.json", {"status": "PASS"})
    _write_json(truth / "reports" / "trading_day_readiness_authority_v1" / DAY / "trading_day_readiness_authority.v1.json", {"readiness_mode": "PREOPEN_BUILD"})


def test_ai_advisory_consumes_outcome_intelligence_read_only(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _advisory_inputs(truth)
    scoring_path = portfolio_scoring_path(truth_root=truth, day_utc=DAY)
    before = _sha(scoring_path)

    payload = build_ai_advisory_review_v1(day_utc=DAY, truth_root=truth)

    assert _sha(scoring_path) == before
    assert payload["status"] == "NO_GOVERNED_ADVISORY_INPUT"
    assert payload["advisory_evidence_packet_status"] == "MISSING"
    assert payload["recommendations"] == []
    assert payload["requires_human_review"] is True
    assert payload["prohibited_actions_attempted"] is False


def test_decision_ledger_records_outcome_intelligence_paths(tmp_path: Path) -> None:
    truth = tmp_path / "truth"
    _selected_pointer(truth, intent_id="")
    _scoring(truth, DAY, [])
    day_run = {"day_utc": DAY, "final_status": "PRE_MARKET_READY", "canonical_phase": "MARKET_OPEN_DATA_GATE", "canonical_blocker": "MARKET_NOT_OPEN"}

    payload = build_decision_ledger_v1(day_utc=DAY, truth_root=truth, aegis_day_payload=day_run)

    assert payload["trade_outcome_path"].endswith("trade_outcome.v1.json")
    assert payload["decision_consistency_path"].endswith("decision_consistency.v1.json")
    assert payload["missed_opportunity_path"].endswith("missed_opportunity.v1.json")
    assert payload["outcome_intelligence_paths"]["trade_outcome_path"].endswith("trade_outcome.v1.json")
