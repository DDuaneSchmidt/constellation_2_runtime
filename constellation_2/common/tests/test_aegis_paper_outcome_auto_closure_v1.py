from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.outcome_registry_v1 import build_outcome_registry_v1
from ops.aegis.paper_outcome_auto_closure_v1 import (
    AUTO_CLOSED_PAPER_OUTCOME,
    AUTO_CLOSURE_BLOCKED,
    AUTO_CLOSURE_NOT_ELIGIBLE,
    build_paper_outcome_auto_closure_v1,
    normalized_paper_outcome_auto_closure_v1,
    write_paper_outcome_auto_closure_v1,
)
from ops.aegis.validation_sample_generator_v1 import build_validation_samples_v1

DAY = "2026-06-01"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _position(candidate: str, symbol: str, *, mark: str = "112", certified: str = "CERTIFIED") -> dict:
    return {
        "position_id": f"paper-position:{candidate}",
        "candidate_id": candidate,
        "symbol": symbol,
        "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
        "hypothesis_id": "HYP_LARGE_CAP_EQUITY_MOMENTUM_20_60D_V1",
        "thesis_id": "THESIS_TREND_PERSISTENCE_V1",
        "entry_price": "100",
        "entry_time": "2026-06-01T14:00:00Z",
        "quantity": "1",
        "side": "BUY",
        "current_status": "OPEN",
        "current_certified_mark": mark,
        "mark_price": mark,
        "mark_certification_status": certified,
        "mark_freshness_status": "CURRENT",
        "mark_timestamp_utc": "2026-06-01T20:00:00Z",
        "mark_source_path": f"/tmp/{symbol}.json",
        "mark_source_hash": f"hash-{symbol}",
        "paper_only": True,
        "trade_advice_allowed": False,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _seed(root: Path, *, include_exit: bool = True, hold: bool = False, missing_mark: bool = False) -> None:
    positions = [
        _position("cand-exit", "BKSY", mark="112" if not missing_mark else ""),
        _position("cand-hold", "HOLD", mark="101"),
    ]
    _write(root / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json", {
        "day_utc": DAY,
        "positions": positions,
        "open_positions": positions,
        "closed_positions": [],
    })
    recommendations = [
        {
            "position_id": "paper-position:cand-exit",
            "candidate_id": "cand-exit",
            "symbol": "BKSY",
            "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
            "entry_price": "100",
            "current_mark": "112" if not missing_mark else "",
            "exit_recommendation": "HOLD" if hold else "EXIT_TAKE_PROFIT",
            "reason_codes": ["NO_EXIT_RULE_TRIGGERED"] if hold else ["TAKE_PROFIT_THRESHOLD_REACHED"],
            "policy": {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "max_hold_days": 20},
            "evidence_paths": [],
            "source_hashes": {},
            "automatic_exit_allowed": False,
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
        },
        {
            "position_id": "paper-position:cand-hold",
            "candidate_id": "cand-hold",
            "symbol": "HOLD",
            "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
            "entry_price": "100",
            "current_mark": "101",
            "exit_recommendation": "HOLD",
            "reason_codes": ["NO_EXIT_RULE_TRIGGERED"],
            "policy": {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "max_hold_days": 20},
            "automatic_exit_allowed": False,
            "trade_advice_allowed": False,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
        },
    ] if include_exit else []
    _write(root / "reports" / "aegis_exit_recommendations_v1" / DAY / "exit_recommendations.v1.json", {
        "schema_id": "aegis_exit_recommendations",
        "day_utc": DAY,
        "recommendations": recommendations,
        "rows": recommendations,
        "safety": {"trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False},
    })
    _write(root / "reports" / "aegis_runtime_truth_kernel_v1" / DAY / "runtime_truth_kernel.v1.json", {
        "day_utc": DAY,
        "trade_advice_allowed": False,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "live_trading_allowed": False,
    })


def test_deterministic_exit_recommendation_auto_closes_paper_outcome(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_paper_outcome_auto_closure_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["summary"]["positions_evaluated"] == 2
    assert payload["summary"]["auto_closed_count"] == 1
    closed = [row for row in payload["rows"] if row["auto_closure_state"] == AUTO_CLOSED_PAPER_OUTCOME][0]
    assert closed["symbol"] == "BKSY"
    assert closed["realized_return"] == 0.12
    assert closed["trade_advice_allowed"] is False
    assert closed["broker_execution_allowed"] is False


def test_hold_positions_remain_open_and_not_eligible(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_paper_outcome_auto_closure_v1(truth_root=tmp_path, day_utc=DAY)
    hold = [row for row in payload["rows"] if row["symbol"] == "HOLD"][0]
    assert hold["auto_closure_state"] == AUTO_CLOSURE_NOT_ELIGIBLE
    assert "HOLD_RECOMMENDATION" in hold["auto_closure_reason_codes"]


def test_missing_exit_mark_blocks_auto_closure(tmp_path: Path) -> None:
    _seed(tmp_path, missing_mark=True)
    payload = build_paper_outcome_auto_closure_v1(truth_root=tmp_path, day_utc=DAY)
    exit_row = [row for row in payload["rows"] if row["symbol"] == "BKSY"][0]
    assert exit_row["auto_closure_state"] == AUTO_CLOSURE_BLOCKED
    assert "MISSING_EXIT_MARK" in exit_row["auto_closure_reason_codes"]


def test_outcome_registry_and_validation_samples_include_auto_closed_outcome(tmp_path: Path) -> None:
    _seed(tmp_path)
    auto = build_paper_outcome_auto_closure_v1(truth_root=tmp_path, day_utc=DAY)
    write_paper_outcome_auto_closure_v1(truth_root=tmp_path, day_utc=DAY, payload=auto)
    outcomes = build_outcome_registry_v1(truth_root=tmp_path, day_utc=DAY)
    samples = build_validation_samples_v1(truth_root=tmp_path, day_utc=DAY, outcome_registry=outcomes)
    assert outcomes["summary"]["closed_outcomes"] == 1
    assert outcomes["summary"]["open_outcomes"] == 1
    included = [row for row in samples["samples"] if row["sample_state"] == "INCLUDED"]
    assert len(included) == 1
    assert included[0]["return_value"] == 0.12


def test_realized_return_is_rerun_stable_after_timestamp_normalization(tmp_path: Path) -> None:
    _seed(tmp_path)
    first = build_paper_outcome_auto_closure_v1(truth_root=tmp_path, day_utc=DAY)
    second = build_paper_outcome_auto_closure_v1(truth_root=tmp_path, day_utc=DAY)
    first_closed = [row for row in first["rows"] if row["auto_closure_state"] == AUTO_CLOSED_PAPER_OUTCOME][0]
    second_closed = [row for row in second["rows"] if row["auto_closure_state"] == AUTO_CLOSED_PAPER_OUTCOME][0]
    assert first_closed["realized_return"] == second_closed["realized_return"] == 0.12
    assert normalized_paper_outcome_auto_closure_v1(first) == normalized_paper_outcome_auto_closure_v1(second)


def test_manual_review_queue_is_separate_from_auto_closure_and_not_trade_advice(tmp_path: Path) -> None:
    _seed(tmp_path)
    payload = build_paper_outcome_auto_closure_v1(truth_root=tmp_path, day_utc=DAY)
    assert payload["summary"]["manual_review_queue_count"] == 1
    item = payload["manual_review_queue"][0]
    assert item["symbol"] == "BKSY"
    assert "not trade advice" in item["not_trade_advice_statement"].lower()
    assert item["broker_execution_allowed"] is False
    assert item["autonomous_execution_allowed"] is False


def test_ingested_auto_closure_remains_replayable_from_ledger_lineage(tmp_path: Path) -> None:
    _seed(tmp_path)
    first = build_paper_outcome_auto_closure_v1(truth_root=tmp_path, day_utc=DAY)
    closed_row = [row for row in first["rows"] if row["auto_closure_state"] == AUTO_CLOSED_PAPER_OUTCOME][0]
    closed_position = _position("cand-exit", "BKSY")
    closed_position.update({
        "current_status": "CLOSED",
        "exit_price": str(closed_row["exit_mark"]),
        "exit_time": closed_row["outcome_timestamp"],
        "realized_pnl": "12",
        "exit_source_receipt": {
            "receipt_type": "DERIVED_FROM_AEGIS_PAPER_OUTCOME_AUTO_CLOSURE_V1",
            "auto_closure_state": AUTO_CLOSED_PAPER_OUTCOME,
            "closure_id": closed_row["closure_id"],
            "exit_trigger": closed_row["exit_trigger"],
            "source_row": closed_row,
        },
        "closure_lineage": {
            "source_family": "aegis_paper_outcome_auto_closure_v1",
            "source_path": "/tmp/original-auto-closure.json",
            "source_hash": "source-hash",
            "closure_id": closed_row["closure_id"],
            "auto_closure_state": AUTO_CLOSED_PAPER_OUTCOME,
            "exit_trigger": closed_row["exit_trigger"],
            "realized_return": str(closed_row["realized_return"]),
            "deterministic_return_formula_version": closed_row["deterministic_return_formula_version"],
        },
    })
    hold_position = _position("cand-hold", "HOLD", mark="101")
    _write(root := tmp_path / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json", {
        "day_utc": DAY,
        "positions": [closed_position, hold_position],
        "open_positions": [hold_position],
        "closed_positions": [closed_position],
    })

    payload = build_paper_outcome_auto_closure_v1(truth_root=tmp_path, day_utc=DAY)
    replayed = [row for row in payload["rows"] if row["auto_closure_state"] == AUTO_CLOSED_PAPER_OUTCOME]

    assert payload["summary"]["auto_closed_count"] == 1
    assert payload["summary"]["manual_review_queue_count"] == 1
    assert replayed[0]["position_id"] == "paper-position:cand-exit"
    assert replayed[0]["persisted_from_paper_position_ledger"] is True
    assert replayed[0]["outcome_timestamp"] == closed_row["outcome_timestamp"]
    assert str(root) in replayed[0]["source_artifacts"]
