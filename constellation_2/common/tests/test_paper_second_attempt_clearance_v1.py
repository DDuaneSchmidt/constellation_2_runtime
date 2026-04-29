from __future__ import annotations

import json
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.paper_second_attempt_clearance_v1 import (
    OPERATOR_SCHEMA_ID,
    build_paper_second_attempt_clearance_v1,
    sha256_file_v1,
)


DAY = "2026-04-29"
ACCOUNT = "DUO847203"
SUBMISSION_ID = "prior-submission"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed_prior(tmp_path: Path, *, filled_qty: int = 0, open_lifecycle: bool = False) -> tuple[Path, Path, Path]:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    submission_dir = execution / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
    _write_json(
        submission_dir / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "day_utc": DAY,
            "submission_id": SUBMISSION_ID,
            "status": "CANCELLED",
            "broker_ids": {"order_id": 94, "perm_id": 0},
        },
    )
    _write_json(submission_dir / "broker_submit_attempt_v1.json", {"ib_account": ACCOUNT})
    _write_json(
        submission_dir / "broker_order_outcome_v1.json",
        {
            "outcome_state": "BROKER_REJECTED",
            "reason_codes": ["IB_ERROR_201_RISKLESS_COMBINATION"],
            "ib_account": ACCOUNT,
        },
    )
    _write_json(
        submission_dir / "execution_event_record.v1.json",
        {"status": "BROKER_REJECTED", "filled_qty": filled_qty, "broker_order_id": "94", "perm_id": "0"},
    )
    _write_json(submission_dir / "broker_acknowledgement_v1.json", {"ib_account": ACCOUNT})
    _write_json(
        submission_dir / "order_plan.v1.json",
        {"schema_id": "order_plan", "schema_version": "v1", "plan_hash": "a" * 64, "legs": []},
    )
    _write_json(
        truth / "reports" / "trading_day_closure_authority_v1" / DAY / "trading_day_closure_authority.v1.json",
        {"status": "PASS", "closure_state": "NO_TRADES_CLOSED", "unresolved_submissions": []},
    )
    _write_json(
        execution / "reports" / "execution_lifecycle_authority_v1" / DAY / "execution_lifecycle_authority.v1.json",
        {
            "submissions": [
                {
                    "submission_id": SUBMISSION_ID,
                    "current_lifecycle_state": "ACKNOWLEDGED_OPEN" if open_lifecycle else "BROKER_REJECTED",
                }
            ]
        },
    )
    op_path = tmp_path / "operator_inputs" / "operator_clearance.v1.json"
    _write_operator_clearance(op_path, truth=truth, execution=execution)
    return truth, execution, op_path


def _write_operator_clearance(path: Path, *, truth: Path, execution: Path, **overrides: object) -> None:
    submission_dir = execution / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
    closure_path = truth / "reports" / "trading_day_closure_authority_v1" / DAY / "trading_day_closure_authority.v1.json"
    payload = {
        "schema_id": OPERATOR_SCHEMA_ID,
        "schema_version": "v1",
        "environment": "PAPER",
        "day_utc": DAY,
        "prior_submission_id": SUBMISSION_ID,
        "ib_account": ACCOUNT,
        "broker_order_id": 94,
        "closure_artifact_sha256": sha256_file_v1(closure_path),
        "broker_submission_record_sha256": sha256_file_v1(submission_dir / "broker_submission_record.v2.json"),
        "broker_order_outcome_sha256": sha256_file_v1(submission_dir / "broker_order_outcome_v1.json"),
        "execution_event_record_sha256": sha256_file_v1(submission_dir / "execution_event_record.v1.json"),
        "approved_at_utc": "2026-04-29T15:00:00Z",
        "approval_reason": "Prior paper submit was rejected zero-fill; approve a changed second PAPER attempt.",
        "acknowledgements": {
            "prior_broker_submit_occurred": True,
            "prior_order_zero_fill": True,
            "second_attempt_risk_understood": True,
        },
    }
    payload.update(overrides)
    _write_json(path, payload)


def _build(truth: Path, execution: Path, op_path: Path, *, environment: str = "PAPER", submission_id: str = SUBMISSION_ID) -> dict:
    return build_paper_second_attempt_clearance_v1(
        truth_root=truth,
        execution_root=execution,
        day_utc=DAY,
        environment=environment,
        prior_submission_id=submission_id,
        operator_clearance_path=op_path,
    )


def test_valid_terminal_zero_fill_rejection_with_closure_and_operator_clearance_allows_new_attempt(tmp_path: Path) -> None:
    truth, execution, op_path = _seed_prior(tmp_path)

    payload = _build(truth, execution, op_path)

    assert payload["status"] == "CLEARED"
    assert payload["prior_submission"]["filled_qty"] == 0
    assert payload["policy"]["requires_ib_preview_pass"] is True


def test_missing_clearance_blocks(tmp_path: Path) -> None:
    truth, execution, _op_path = _seed_prior(tmp_path)

    payload = _build(truth, execution, tmp_path / "missing.json")

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "OPERATOR_CLEARANCE_MISSING"


def test_nonzero_fill_blocks(tmp_path: Path) -> None:
    truth, execution, op_path = _seed_prior(tmp_path, filled_qty=1)

    payload = _build(truth, execution, op_path)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "PRIOR_SUBMISSION_NONZERO_FILL"


def test_open_lifecycle_blocks(tmp_path: Path) -> None:
    truth, execution, op_path = _seed_prior(tmp_path, open_lifecycle=True)

    payload = _build(truth, execution, op_path)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "PRIOR_SUBMISSION_OPEN_LIFECYCLE"


def test_closure_hash_mismatch_blocks(tmp_path: Path) -> None:
    truth, execution, op_path = _seed_prior(tmp_path)
    _write_operator_clearance(op_path, truth=truth, execution=execution, closure_artifact_sha256="bad")

    payload = _build(truth, execution, op_path)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "OPERATOR_CLEARANCE_MISMATCH"
    assert "closure_artifact_sha256" in payload["operator_clearance"]["mismatches"]


def test_wrong_day_account_or_submission_blocks(tmp_path: Path) -> None:
    truth, execution, op_path = _seed_prior(tmp_path)
    _write_operator_clearance(op_path, truth=truth, execution=execution, day_utc="2026-04-28", ib_account="DU999999")

    payload = _build(truth, execution, op_path)

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "OPERATOR_CLEARANCE_MISMATCH"
    assert "day_utc" in payload["operator_clearance"]["mismatches"]
    assert "ib_account" in payload["operator_clearance"]["mismatches"]


def test_live_rejected(tmp_path: Path) -> None:
    truth, execution, op_path = _seed_prior(tmp_path)

    payload = _build(truth, execution, op_path, environment="LIVE")

    assert payload["status"] == "BLOCKED"
    assert payload["canonical_blocker"] == "LIVE_UNSUPPORTED"
