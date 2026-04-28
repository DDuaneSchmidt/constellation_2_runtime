from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.submission_index_v1 import evaluate_submission_index_v1


DAY = "2026-04-24"
SUBMISSION_ID = "a" * 64
ATTEMPT_ID = "attempt-001"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_latest_pointer(execution_root: Path, *, day: str, status: str) -> None:
    _write_json(
        execution_root / "execution_evidence_v1" / "latest_pointer.v1.json",
        {
            "day_utc": day,
            "status": status,
            "produced_utc": "2026-04-24T10:00:00Z",
            "pointers": {
                "submissions_day_dir": str((execution_root / "execution_evidence_v1" / "submissions" / DAY).resolve())
            },
        },
    )


def _seed_submission_record(execution_root: Path, *, attempt_id: str, order_id: int, perm_id: int, status: str = "PENDINGSUBMIT") -> Path:
    submission_dir = execution_root / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
    _write_json(
        submission_dir / "broker_submission_record.v2.json",
        {
            "submission_id": SUBMISSION_ID,
            "attempt_id": attempt_id,
            "status": status,
            "broker_ids": {"order_id": order_id, "perm_id": perm_id},
        },
    )
    _write_json(submission_dir / "broker_submit_attempt_v1.json", {"attempt_id": attempt_id, "submission_id": SUBMISSION_ID})
    return submission_dir


def _seed_stream_record(execution_root: Path, *, attempt_id: str, order_id: int, perm_id: int = 0) -> None:
    _write_json(
        execution_root / "execution_stream_v1" / DAY / "stream.execution_event_stream_record.v1.json",
        {
            "submission_id": SUBMISSION_ID,
            "attempt_id": attempt_id,
            "broker_ids": {"order_id": order_id, "perm_id": perm_id},
        },
    )


def _seed_fill_ledger(execution_root: Path, *, attempt_id: str, order_id: int) -> None:
    _write_json(
        execution_root / "fill_ledger_v1" / DAY / f"{SUBMISSION_ID}.fill_ledger.v1.json",
        {
            "submission_id": SUBMISSION_ID,
            "attempt_id": attempt_id,
            "broker_order_id": order_id,
        },
    )


def test_complete_matching_attempt_passes(tmp_path: Path) -> None:
    execution_root = tmp_path
    _seed_latest_pointer(execution_root, day=DAY, status="OK")
    _seed_submission_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90, perm_id=0)
    _seed_stream_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90)
    _seed_fill_ledger(execution_root, attempt_id=ATTEMPT_ID, order_id=90)
    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "PASS"
    assert payload["attempts"][0]["lineage_status"] == "PASS"
    assert payload["attempts"][0]["linkage_method"] == "ORDER_ID_ONLY_PENDING"
    assert payload["attempts"][0]["linkage_confidence"] == "DEGRADED"


def test_missing_submission_record_fails(tmp_path: Path) -> None:
    execution_root = tmp_path
    _seed_latest_pointer(execution_root, day=DAY, status="OK")
    submission_dir = execution_root / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
    submission_dir.mkdir(parents=True, exist_ok=True)
    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "SUBMISSION_RECORD_MISSING" for item in payload["blocking_evidence"])


def test_missing_fill_ledger_fails(tmp_path: Path) -> None:
    execution_root = tmp_path
    _seed_latest_pointer(execution_root, day=DAY, status="OK")
    _seed_submission_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90, perm_id=0)
    _seed_stream_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90)
    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "FILL_LEDGER_MISSING" for item in payload["blocking_evidence"])


def test_attempt_id_mismatch_fails(tmp_path: Path) -> None:
    execution_root = tmp_path
    _seed_latest_pointer(execution_root, day=DAY, status="OK")
    _seed_submission_record(execution_root, attempt_id="attempt-a", order_id=90, perm_id=0)
    _seed_stream_record(execution_root, attempt_id="attempt-b", order_id=90)
    _seed_fill_ledger(execution_root, attempt_id="attempt-a", order_id=90)
    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "ATTEMPT_ID_MISMATCH" for item in payload["blocking_evidence"])


def test_broker_order_id_mismatch_fails(tmp_path: Path) -> None:
    execution_root = tmp_path
    _seed_latest_pointer(execution_root, day=DAY, status="OK")
    _seed_submission_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90, perm_id=0)
    _seed_stream_record(execution_root, attempt_id=ATTEMPT_ID, order_id=91)
    _seed_fill_ledger(execution_root, attempt_id=ATTEMPT_ID, order_id=90)
    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "BROKER_ORDER_ID_MISMATCH" for item in payload["blocking_evidence"])


def test_latest_pointer_old_day_fails(tmp_path: Path) -> None:
    execution_root = tmp_path
    _seed_latest_pointer(execution_root, day="2026-04-23", status="OK")
    _seed_submission_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90, perm_id=0)
    _seed_stream_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90)
    _seed_fill_ledger(execution_root, attempt_id=ATTEMPT_ID, order_id=90)
    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "EXECUTION_POINTER_DAY_MISMATCH" for item in payload["blocking_evidence"])


def test_latest_pointer_failed_status_fails(tmp_path: Path) -> None:
    execution_root = tmp_path
    _seed_latest_pointer(execution_root, day=DAY, status="FAIL_SCHEMA_VIOLATION")
    _seed_submission_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90, perm_id=0)
    _seed_stream_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90)
    _seed_fill_ledger(execution_root, attempt_id=ATTEMPT_ID, order_id=90)
    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "FAIL"
    assert any(item["code"] == "STALE_EXECUTION_POINTER" for item in payload["blocking_evidence"])


def test_perm_id_zero_allowed_when_field_present_and_pending_status(tmp_path: Path) -> None:
    execution_root = tmp_path
    _seed_latest_pointer(execution_root, day=DAY, status="OK")
    _seed_submission_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90, perm_id=0, status="PENDINGSUBMIT")
    _seed_stream_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90)
    _seed_fill_ledger(execution_root, attempt_id=ATTEMPT_ID, order_id=90)
    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "PASS"


def test_perm_id_bridged_from_acknowledgement_artifact(tmp_path: Path) -> None:
    execution_root = tmp_path
    _seed_latest_pointer(execution_root, day=DAY, status="OK")
    submission_dir = _seed_submission_record(execution_root, attempt_id="", order_id=90, perm_id=0)
    _write_json(
        submission_dir / "broker_acknowledgement_v1.json",
        {
            "submission_id": SUBMISSION_ID,
            "broker_ids": {"order_id": 90, "perm_id": 764621016},
            "status": "PENDINGSUBMIT",
        },
    )
    _seed_stream_record(execution_root, attempt_id=SUBMISSION_ID, order_id=90, perm_id=764621016)
    _seed_fill_ledger(execution_root, attempt_id=SUBMISSION_ID, order_id=90)

    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "PASS"
    attempt = payload["attempts"][0]
    assert attempt["attempt_id"] == SUBMISSION_ID
    assert attempt["attempt_id_source"] == "SUBMISSION_ID_BRIDGE_PROVEN"
    assert attempt["broker_perm_id"] == 764621016
    assert attempt["broker_perm_id_source"] == "BROKER_ACKNOWLEDGEMENT"
    assert attempt["linkage_method"] == "PERM_ID_BACKFILL_FROM_ACK"
    assert attempt["linkage_confidence"] == "DERIVED_EXACT"
    assert not any(item["code"] == "ATTEMPT_ID_MISSING" for item in payload["blocking_evidence"])


def test_attempt_id_missing_without_cross_surface_bridge_fails(tmp_path: Path) -> None:
    execution_root = tmp_path
    _seed_latest_pointer(execution_root, day=DAY, status="OK")
    _seed_submission_record(execution_root, attempt_id="", order_id=90, perm_id=0)
    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "FAIL"
    attempt = payload["attempts"][0]
    assert attempt["attempt_id_source"] == "SUBMISSION_ID_BRIDGE_UNPROVEN"
    assert any(item["code"] == "ATTEMPT_ID_MISSING" for item in payload["blocking_evidence"])


def test_case_a_submission_perm_id_exact_match_passes(tmp_path: Path) -> None:
    execution_root = tmp_path
    _seed_latest_pointer(execution_root, day=DAY, status="OK")
    _seed_submission_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90, perm_id=764621016, status="SUBMITTED")
    _seed_stream_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90, perm_id=764621016)
    _seed_fill_ledger(execution_root, attempt_id=ATTEMPT_ID, order_id=90)
    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "PASS"
    attempt = payload["attempts"][0]
    assert attempt["broker_perm_id"] == 764621016
    assert attempt["linkage_method"] == "PERM_ID_EXACT_FROM_SUBMISSION"
    assert attempt["linkage_confidence"] == "EXACT"


def test_case_c_execution_perm_without_submission_or_ack_fails_closed(tmp_path: Path) -> None:
    execution_root = tmp_path
    _seed_latest_pointer(execution_root, day=DAY, status="OK")
    _seed_submission_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90, perm_id=0, status="PENDINGSUBMIT")
    _seed_stream_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90, perm_id=764621016)
    _seed_fill_ledger(execution_root, attempt_id=ATTEMPT_ID, order_id=90)
    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "FAIL"
    attempt = payload["attempts"][0]
    assert attempt["linkage_method"] == "FAIL_CLOSED"
    assert attempt["linkage_confidence"] == "NONE"
    assert any(item["code"] == "POST_SUBMIT_LINEAGE_GAP" for item in payload["blocking_evidence"])


def test_replay_stream_records_are_accepted_for_lineage(tmp_path: Path) -> None:
    execution_root = tmp_path
    _seed_latest_pointer(execution_root, day=DAY, status="OK")
    _seed_submission_record(execution_root, attempt_id=ATTEMPT_ID, order_id=90, perm_id=0)
    _write_json(
        execution_root
        / "execution_stream_v1"
        / "replays"
        / DAY
        / "replay_20260426_031517"
        / "stream.execution_event_stream_record.v1.json",
        {
            "submission_id": SUBMISSION_ID,
            "attempt_id": ATTEMPT_ID,
            "broker_ids": {"order_id": 90, "perm_id": 0},
        },
    )
    _seed_fill_ledger(execution_root, attempt_id=ATTEMPT_ID, order_id=90)

    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)
    assert payload["status"] == "PASS"
    assert payload["attempts"][0]["execution_stream_path"].endswith("stream.execution_event_stream_record.v1.json")


def test_dry_run_submission_without_broker_ids_is_diagnostic_not_failed_live(tmp_path: Path) -> None:
    execution_root = tmp_path
    submission_dir = execution_root / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
    _write_json(
        submission_dir / "broker_submission_record.v2.json",
        {
            "submission_id": SUBMISSION_ID,
            "status": "PENDINGSUBMIT",
            "broker_ids": {"order_id": None, "perm_id": None},
            "error": {"code": "DRY_RUN_NO_BROKER_ID"},
        },
    )
    _write_json(
        submission_dir / "broker_submit_attempt_v1.json",
        {"submission_id": SUBMISSION_ID, "dry_run": True, "reason_codes": ["DRY_RUN_SUBMIT_ATTEMPT"]},
    )

    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)

    assert payload["status"] == "PASS"
    assert payload["submit_mode_status"] == "DRY_RUN_COMPLETE"
    assert payload["broker_transmit_enabled"] is False
    assert payload["missing_broker_ids_blocker"] is False
    assert payload["missing_broker_ids_diagnostic"] is True
    assert not payload["blocking_evidence"]
    assert any(item["code"] == "BROKER_ORDER_ID_MISSING" for item in payload["diagnostic_evidence"])


def test_transmit_enabled_submission_without_broker_ids_fails_closed(tmp_path: Path) -> None:
    execution_root = tmp_path
    submission_dir = execution_root / "execution_evidence_v1" / "submissions" / DAY / SUBMISSION_ID
    _write_json(
        submission_dir / "broker_submission_record.v2.json",
        {
            "submission_id": SUBMISSION_ID,
            "status": "PENDINGSUBMIT",
            "broker_ids": {"order_id": None, "perm_id": None},
        },
    )
    _write_json(
        submission_dir / "broker_submit_attempt_v1.json",
        {"submission_id": SUBMISSION_ID, "dry_run": False, "reason_codes": ["REAL_SUBMIT_ATTEMPT"]},
    )

    payload = evaluate_submission_index_v1(day_utc=DAY, execution_root=execution_root)

    assert payload["status"] == "FAIL"
    assert payload["submit_mode_status"] == "DEGRADED"
    assert payload["broker_transmit_enabled"] is True
    assert payload["missing_broker_ids_blocker"] is True
    assert any(item["code"] == "BROKER_ORDER_ID_MISSING" for item in payload["blocking_evidence"])
