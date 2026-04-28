from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.execution_lifecycle_authority_v1 import evaluate_execution_lifecycle_authority_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


DAY = "2026-04-27"
SID = "a" * 64
SID2 = "b" * 64


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _broker_record(submission_id: str = SID, *, order_id=None, perm_id=None, status: str = "SUBMITTED", dry_run: bool = False) -> dict:
    payload = {
        "schema_id": "broker_submission_record",
        "schema_version": "v2",
        "submission_id": submission_id,
        "submitted_at_utc": f"{DAY}T14:00:00Z",
        "binding_hash": "c" * 64,
        "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
        "status": status,
        "broker_ids": {"order_id": order_id, "perm_id": perm_id},
        "canonical_json_hash": None,
    }
    if dry_run:
        payload["status"] = "PENDINGSUBMIT"
        payload["error"] = {
            "code": "DRY_RUN_NO_BROKER_ID",
            "message": "Dry-run submission has no broker callbacks.",
        }
    return payload


def _submit_attempt(submission_id: str = SID, *, dry_run: bool) -> dict:
    return {
        "schema_id": "broker_submit_attempt",
        "schema_version": "v1",
        "day_utc": DAY,
        "environment": "PAPER",
        "submission_id": submission_id,
        "attempted_at_utc": f"{DAY}T14:00:00Z",
        "attempt_state": "SUBMIT_ATTEMPTED",
        "dry_run": dry_run,
        "reason_codes": ["DRY_RUN_SUBMIT_ATTEMPT" if dry_run else "REAL_SUBMIT_ATTEMPT"],
        "evidence_artifacts": [],
    }


def _write_submission(root: Path, submission_id: str = SID, *, order_id=None, perm_id=None, status: str = "SUBMITTED", dry_run: bool = False) -> None:
    subdir = root / "execution_evidence_v1" / "submissions" / DAY / submission_id
    _write_json(subdir / "broker_submission_record.v2.json", _broker_record(submission_id, order_id=order_id, perm_id=perm_id, status=status, dry_run=dry_run))
    _write_json(subdir / "broker_submit_attempt_v1.json", _submit_attempt(submission_id, dry_run=dry_run))
    _write_json(
        subdir / "equity_order_plan.v1.json",
        {"schema_id": "equity_order_plan", "schema_version": "v1", "symbol": "SPY", "action": "BUY", "qty_shares": 2},
    )


def _write_fill(root: Path, submission_id: str = SID, *, order_qty: int = 2, filled_qty: int = 2) -> None:
    remaining = max(order_qty - filled_qty, 0)
    lifecycle = "FILLED" if filled_qty >= order_qty else ("PARTIALLY_FILLED" if filled_qty > 0 else "OPEN")
    _write_json(
        root / "fill_ledger_v1" / DAY / f"{submission_id}.fill_ledger.v1.json",
        {
            "schema_id": "C2_FILL_LEDGER_V1",
            "schema_version": 1,
            "day_utc": DAY,
            "produced_utc": f"{DAY}T14:05:00Z",
            "status": "OK",
            "submission_id": submission_id,
            "order_qty": order_qty,
            "filled_qty": filled_qty,
            "remaining_qty": remaining,
            "lifecycle_status": lifecycle,
        },
    )


def _evaluate(root: Path) -> dict:
    return evaluate_execution_lifecycle_authority_v1(
        day_utc=DAY,
        execution_root=root,
        canonical_truth_root=root,
        produced_utc=f"{DAY}T15:00:00Z",
    )


def test_dry_run_evidence_without_broker_ids_is_complete_diagnostic(tmp_path: Path) -> None:
    _write_submission(tmp_path, dry_run=True)

    payload = _evaluate(tmp_path)
    row = payload["submissions"][0]

    assert payload["current_lifecycle_state"] == "DRY_RUN_COMPLETE"
    assert row["was_simulated"] is True
    assert row["broker_transmit_expected"] is False
    assert row["missing_broker_ids_expected_diagnostic"] is True
    assert row["missing_broker_ids_failure"] is False


def test_transmit_enabled_evidence_without_broker_ids_is_lineage_gap(tmp_path: Path) -> None:
    _write_submission(tmp_path, dry_run=False)

    payload = _evaluate(tmp_path)
    row = payload["submissions"][0]

    assert payload["current_lifecycle_state"] in {"LINEAGE_GAP", "FAILED"}
    assert row["broker_transmit_expected"] is True
    assert row["missing_broker_ids_failure"] is True
    assert row["first_blocker_or_gap"] in {"BROKER_ORDER_ID_MISSING", "BROKER_PERM_ID_MISSING"}


def test_broker_ids_present_but_no_fill_is_acknowledged_open(tmp_path: Path) -> None:
    _write_submission(tmp_path, order_id=101, perm_id=202, dry_run=False)

    payload = _evaluate(tmp_path)
    row = payload["submissions"][0]

    assert payload["current_lifecycle_state"] == "ACKNOWLEDGED_OPEN"
    assert row["broker_order_id_assigned"] is True
    assert row["broker_perm_id_assigned"] is True
    assert row["fills_status"] == "MISSING"


def test_fill_ledger_present_classifies_filled_or_partial(tmp_path: Path) -> None:
    _write_submission(tmp_path, order_id=101, perm_id=202, dry_run=False)
    _write_fill(tmp_path, order_qty=2, filled_qty=1)

    partial = _evaluate(tmp_path)
    assert partial["current_lifecycle_state"] == "PARTIALLY_FILLED"
    assert partial["submissions"][0]["fills_status"] == "PARTIAL"

    _write_fill(tmp_path, order_qty=2, filled_qty=2)
    filled = _evaluate(tmp_path)
    assert filled["current_lifecycle_state"] == "FILLED"
    assert filled["submissions"][0]["fills_status"] == "COMPLETE"


def test_reconciliation_present_marks_reconciled(tmp_path: Path) -> None:
    _write_submission(tmp_path, order_id=101, perm_id=202, dry_run=False)
    _write_fill(tmp_path, order_qty=2, filled_qty=2)
    _write_json(
        tmp_path / "reports" / "execution_reconciliation_v1" / DAY / "execution_reconciliation.v1.json",
        {"schema_id": "C2_EXECUTION_RECONCILIATION_V1", "schema_version": 1, "day_utc": DAY, "status": "PASS", "semantic_status": "FULLY_OBSERVED_AND_CONFIRMED"},
    )

    payload = _evaluate(tmp_path)

    assert payload["current_lifecycle_state"] == "RECONCILED"
    assert payload["submissions"][0]["reconciliation_complete"] is True


def test_missing_submission_evidence_is_no_submission(tmp_path: Path) -> None:
    payload = _evaluate(tmp_path)

    assert payload["current_lifecycle_state"] == "NO_SUBMISSION"
    assert payload["submission_count"] == 0
    assert payload["first_blocker_or_gap"] == "NO_SUBMISSION_EVIDENCE"


def test_stale_conflicting_current_head_cannot_override_authority_state(tmp_path: Path) -> None:
    _write_submission(tmp_path, dry_run=True)
    _write_json(
        tmp_path / "execution_evidence_v1" / "current_head" / DAY / "current_head.v1.json",
        {
            "schema_version": "execution_evidence_current_head.v1",
            "day": DAY,
            "status": "PASS",
            "selected_attempt_id": SID2,
            "selected_artifact_path": "/tmp/stale",
        },
    )

    payload = _evaluate(tmp_path)

    assert payload["current_lifecycle_state"] == "DRY_RUN_COMPLETE"
    assert payload["projection_consistency"][0]["status"] == "CONFLICT"
    assert payload["projection_consistency"][0]["reason_code"] == "CURRENT_HEAD_SELECTED_ATTEMPT_NOT_IN_AUTHORITY"


def test_authority_payload_matches_schema(tmp_path: Path) -> None:
    _write_submission(tmp_path, dry_run=True)
    payload = _evaluate(tmp_path)

    validate_against_repo_schema_v1(
        payload,
        SOURCE_ROOT,
        "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_lifecycle_authority.v1.schema.json",
    )
