from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.trading_day_closure_authority_v1 import evaluate_trading_day_closure_authority_v1


DAY = "2026-04-27"
SID = "a" * 64


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _write_submission(root: Path, *, dry_run: bool = True, lifecycle_state: str = "DRY_RUN_COMPLETE", transmit: bool = False) -> None:
    subdir = root / "execution_evidence_v1" / "submissions" / DAY / SID
    _write_json(subdir / "broker_submit_attempt_v1.json", {"submission_id": SID, "dry_run": dry_run})
    _write_json(subdir / "broker_submission_record.v2.json", {"submission_id": SID, "broker_ids": {"order_id": 101 if transmit else None, "perm_id": 202 if transmit else None}})
    _write_json(
        root / "reports" / "execution_lifecycle_authority_v1" / DAY / "execution_lifecycle_authority.v1.json",
        {"submissions": [{"submission_id": SID, "current_lifecycle_state": lifecycle_state}]},
    )
    _write_json(
        root / "reports" / "trade_lineage_graph_v1" / DAY / "trade_lineage_graph.v1.json",
        {"lineages": [{"submission_id": SID, "dry_run": dry_run, "broker_transmit_enabled": transmit, "lifecycle_state": lifecycle_state, "identity_state": lifecycle_state}]},
    )


def _write_terminal_zero_fill_submission(root: Path) -> None:
    subdir = root / "execution_evidence_v1" / "submissions" / DAY / SID
    _write_json(subdir / "broker_submit_attempt_v1.json", {"submission_id": SID, "dry_run": False})
    _write_json(
        subdir / "broker_submission_record.v2.json",
        {
            "submission_id": SID,
            "status": "CANCELLED",
            "broker_ids": {"order_id": 101, "perm_id": 0},
            "error": {"code": "IB_ERROR_201", "message": "Riskless combination orders are not allowed."},
        },
    )
    _write_json(
        subdir / "execution_event_record.v1.json",
        {
            "submission_id": SID,
            "broker_order_id": "101",
            "perm_id": "0",
            "status": "CANCELLED",
            "filled_qty": 0,
        },
    )
    _write_json(
        root / "reports" / "execution_lifecycle_authority_v1" / DAY / "execution_lifecycle_authority.v1.json",
        {"submissions": [{"submission_id": SID, "current_lifecycle_state": "CANCELED"}]},
    )
    _write_json(
        root / "reports" / "trade_lineage_graph_v1" / DAY / "trade_lineage_graph.v1.json",
        {"lineages": [{"submission_id": SID, "dry_run": False, "broker_transmit_enabled": True, "lifecycle_state": "CANCELED", "identity_state": "CANCELED"}]},
    )


def test_no_submissions_closes_no_trades(tmp_path: Path) -> None:
    payload = evaluate_trading_day_closure_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["closure_state"] == "NO_TRADES_CLOSED"


def test_dry_run_submission_closes_without_fills(tmp_path: Path) -> None:
    _write_submission(tmp_path, dry_run=True, lifecycle_state="DRY_RUN_COMPLETE")

    payload = evaluate_trading_day_closure_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["closure_state"] == "DRY_RUN_CLOSED"


def test_dry_run_closure_ignores_later_stale_market_data(tmp_path: Path) -> None:
    _write_submission(tmp_path, dry_run=True, lifecycle_state="DRY_RUN_COMPLETE")
    _write_json(
        tmp_path / "reports" / "market_data_authority_v1" / DAY / "market_data_authority.v1.json",
        {"market_data_state": "STALE", "status": "WARN", "operator_impact": "POST_SUBMIT_DIAGNOSTIC"},
    )

    payload = evaluate_trading_day_closure_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["closure_state"] == "DRY_RUN_CLOSED"
    assert payload["status"] == "PASS"


def test_transmitted_open_order_blocks_closure(tmp_path: Path) -> None:
    _write_submission(tmp_path, dry_run=False, lifecycle_state="ACKNOWLEDGED_OPEN", transmit=True)

    payload = evaluate_trading_day_closure_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["closure_state"] == "OPEN_EXECUTIONS"
    assert payload["unresolved_submissions"][0]["submission_id"] == SID


def test_rejected_zero_fill_perm_zero_submission_closes_terminally(tmp_path: Path) -> None:
    _write_terminal_zero_fill_submission(tmp_path)

    payload = evaluate_trading_day_closure_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["status"] == "PASS"
    assert payload["closure_state"] == "NO_TRADES_CLOSED"
    assert payload["terminal_zero_fill_submission_count"] == 1
    assert payload["submissions"][0]["terminal_zero_fill"] is True


def test_fills_without_reconciliation_require_reconciliation(tmp_path: Path) -> None:
    _write_submission(tmp_path, dry_run=False, lifecycle_state="FILLED", transmit=True)
    _write_json(tmp_path / "fill_ledger_v1" / DAY / f"{SID}.fill_ledger.v1.json", {"submission_id": SID, "filled_qty": 1})

    payload = evaluate_trading_day_closure_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["closure_state"] == "RECONCILIATION_REQUIRED"


def test_reconciliation_present_closes_reconciled(tmp_path: Path) -> None:
    _write_submission(tmp_path, dry_run=False, lifecycle_state="FILLED", transmit=True)
    _write_json(tmp_path / "fill_ledger_v1" / DAY / f"{SID}.fill_ledger.v1.json", {"submission_id": SID, "filled_qty": 1})
    _write_json(tmp_path / "reports" / "execution_reconciliation_v1" / DAY / "execution_reconciliation.v1.json", {"status": "PASS", "semantic_status": "FULLY_OBSERVED_AND_CONFIRMED"})

    payload = evaluate_trading_day_closure_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["closure_state"] == "RECONCILED"


def test_missing_broker_record_is_closure_gap(tmp_path: Path) -> None:
    subdir = tmp_path / "execution_evidence_v1" / "submissions" / DAY / SID
    _write_json(subdir / "broker_submit_attempt_v1.json", {"submission_id": SID, "dry_run": True})

    payload = evaluate_trading_day_closure_authority_v1(day_utc=DAY, truth_root=tmp_path, execution_root=tmp_path)

    assert payload["closure_state"] == "CLOSURE_GAP"
