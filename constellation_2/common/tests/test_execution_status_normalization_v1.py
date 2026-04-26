from __future__ import annotations

from pathlib import Path

import pytest

from constellation_2.common.execution_status_normalization_v1 import (
    UNKNOWN_BROKER_ORDER_STATUS,
    normalize_broker_order_status_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_STREAM = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_event_stream_record.v1.schema.json"


def _valid_record_for_raw_status(raw_status: str) -> dict:
    normalized = normalize_broker_order_status_v1(raw_status)
    return {
        "schema_id": "C2_EXECUTION_EVENT_STREAM_RECORD_V1",
        "schema_version": 1,
        "produced_utc": "2026-04-24T00:00:00Z",
        "day_utc": "2026-04-24",
        "producer": {
            "repo": "constellation",
            "git_sha": "d7952e46d0dff4d86ba4eb9ddce64eb664c93315",
            "module": "ops/tools/run_execution_stream_snapshot_day_v1.py",
        },
        "status": "OK",
        "reason_codes": [],
        "submission_id": "a" * 64,
        "binding_hash": "b" * 64,
        "engine_id": "ENGINE_01",
        "source_intent_id": "intent-source-id-0001",
        "intent_sha256": "c" * 64,
        "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
        "event_type": "ORDER_STATUS",
        "event_time_utc": "2026-04-24T14:30:00Z",
        "observed_at_utc": "2026-04-24T14:30:00Z",
        "broker_ids": {"order_id": 90, "perm_id": 0},
        "event_attribution": {
            "raw_order_id": 90,
            "raw_perm_id": 0,
            "attribution_method": "DIRECT_BROKER_IDS",
            "attribution_confidence": "HIGH",
        },
        "order_state": {
            "status": normalized.normalized_lifecycle_status,
            "raw_broker_status": normalized.raw_broker_status,
            "normalized_lifecycle_status": normalized.normalized_lifecycle_status,
            "is_terminal": normalized.is_terminal,
            "filled_qty": 0,
            "remaining_qty": 1,
            "avg_fill_price": "0",
        },
        "fill": {"fill_qty": 0, "fill_price": "0", "commission": "0", "currency": "USD"},
        "canonical_json_hash": "d" * 64,
    }


@pytest.mark.parametrize(
    ("raw_status", "expected_normalized", "expected_terminal"),
    [
        ("PENDINGSUBMIT", "OPEN_PENDING_SUBMIT", False),
        ("PendingSubmit", "OPEN_PENDING_SUBMIT", False),
        ("Submitted", "OPEN_SUBMITTED", False),
        ("Filled", "TERMINAL_FILLED", True),
        ("Rejected", "TERMINAL_REJECTED", True),
        ("Cancelled", "TERMINAL_CANCELLED", True),
    ],
)
def test_normalized_statuses_produce_schema_valid_execution_stream_records(
    raw_status: str,
    expected_normalized: str,
    expected_terminal: bool,
) -> None:
    record = _valid_record_for_raw_status(raw_status)
    state = record["order_state"]
    assert state["normalized_lifecycle_status"] == expected_normalized
    assert state["is_terminal"] is expected_terminal
    validate_against_repo_schema_v1(record, REPO_ROOT, SCHEMA_STREAM)


def test_unknown_status_fails_closed_with_raw_status_preserved() -> None:
    with pytest.raises(ValueError) as excinfo:
        normalize_broker_order_status_v1("MysteryStatus")
    message = str(excinfo.value)
    assert UNKNOWN_BROKER_ORDER_STATUS in message
    assert "MysteryStatus" in message
