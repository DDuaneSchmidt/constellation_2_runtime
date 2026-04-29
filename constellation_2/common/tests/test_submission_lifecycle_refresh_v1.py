from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.constitutional_runtime_v1 import validate_governed_artifact_payload_v1  # noqa: E402
import ops.tools.run_fill_ledger_day_v1 as fill_ledger  # noqa: E402
import ops.tools.run_submission_lifecycle_refresh_v1 as lifecycle_refresh  # noqa: E402


DAY = "2026-04-13"
TARGET_SUBMISSION_ID = "f6b7a3dbbabf3f3bdd52a338ebc2b3cf72677be25e4f9de0ac7c39a26c44fe17"
OTHER_SUBMISSION_ID = "d50ff800edd387bdb8ef31f8d9355e8d6c4b8d957c81035d246963fcba5db0da"
TARGET_BINDING_HASH = "ed8bf1e2d6479e429ff0696515bec81cb855d96b373acaac01c772cd53a1f7cc"
OTHER_BINDING_HASH = "b13bd8662bfec5e2f072e28fc6720163bb010daf694eddd2e3bdeb7ee81ae678"
TARGET_BROKER_HASH = "a96e1385ea46105f996b2d8dea8f04ab55652f7a6f1db902c9ee4294b10edbc9"
OTHER_BROKER_HASH = "ed9ee845e7d456c8257ab90b95894ed84f0e8fd0a9a9440d33eb646a5ea6133f"
TARGET_INTENT_SHA = "f51cf9219c7a1bca3b1a6f21724f10517213cdbb27cd307ccf1ff84c0e236d14"
OTHER_INTENT_SHA = "00091fc97c0d1e953e652e0f2461c1b2b95492b8bb6891c04fbd0471adcf8ce0"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed_submission(
    truth_root: Path,
    *,
    submission_id: str,
    binding_hash: str,
    broker_hash: str,
    intent_sha: str,
    order_id: int,
    perm_id: int,
) -> Path:
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / submission_id
    _write_json(
        subdir / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "submission_id": submission_id,
            "submitted_at_utc": f"{DAY}T00:00:00Z",
            "binding_hash": binding_hash,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "status": "PRESUBMITTED",
            "broker_ids": {"order_id": order_id, "perm_id": perm_id},
            "error": None,
            "canonical_json_hash": broker_hash,
        },
    )
    _write_json(
        subdir / "binding_record.v2.json",
        {
            "schema_id": "binding_record",
            "schema_version": "v2",
            "submission_id": submission_id,
            "intent_id": "c2_trend_eq_spy_2026-04-13_v1",
            "intent_hash": intent_sha,
            "canonical_json_hash": binding_hash,
        },
    )
    _write_json(
        subdir / "mapping_ledger_record.v2.json",
        {
            "schema_id": "mapping_ledger_record",
            "schema_version": "v2",
            "submission_id": submission_id,
            "canonical_json_hash": "4" * 64,
            "broker_ids": {"order_id": order_id, "perm_id": perm_id},
        },
    )
    _write_json(
        subdir / "equity_order_plan.v2.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v2",
            "plan_id": "c2_trend_eq_spy_2026-04-13_v1",
            "created_at_utc": f"{DAY}T00:00:00Z",
            "intent_hash": intent_sha,
            "intent_sha256": intent_sha,
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "source_intent_id": "c2_trend_eq_spy_2026-04-13_v1",
            "lineage_envelope_ref": {"path": "lineage_envelope.v1.json", "sha256": "f" * 64},
            "structure": "EQUITY_SPOT",
            "symbol": "SPY",
            "currency": "USD",
            "action": "BUY",
            "qty_shares": 1,
            "order_terms": {"order_type": "LIMIT", "limit_price": "679.91", "time_in_force": "DAY"},
            "risk_proof": None,
            "canonical_json_hash": "b" * 64,
        },
    )
    return subdir


def _seed_execution_event_record(subdir: Path, *, order_id: str = "75", perm_id: str = "1870974300") -> None:
    _write_json(
        subdir / "execution_event_record.v1.json",
        {
            "schema_id": "execution_event_record",
            "schema_version": "v1",
            "created_at_utc": f"{DAY}T00:00:00Z",
            "event_time_utc": f"{DAY}T00:00:00Z",
            "binding_hash": TARGET_BINDING_HASH,
            "broker_submission_hash": TARGET_BROKER_HASH,
            "broker_order_id": order_id,
            "perm_id": perm_id,
            "status": "UNKNOWN",
            "filled_qty": 0,
            "avg_price": "0",
            "raw_broker_status": None,
            "raw_payload_digest": None,
            "sequence_num": None,
            "canonical_json_hash": "0cbe58f10f8a9dea8abc91bb05b511a775d40099c8afe24da8897cdb3e53ab36",
            "upstream_hash": None,
        },
    )


def _seed_stream_record(
    truth_root: Path,
    *,
    filename: str,
    submission_id: str,
    binding_hash: str,
    intent_sha: str,
    order_id: int,
    perm_id: int,
    event_time_utc: str,
    canonical_json_hash: str,
    order_status: str = "PRESUBMITTED",
    filled_qty: int = 0,
) -> None:
    _write_json(
        truth_root / "execution_stream_v1" / DAY / filename,
        {
            "schema_id": "C2_EXECUTION_EVENT_STREAM_RECORD_V1",
            "schema_version": 1,
            "produced_utc": f"{DAY}T00:00:00Z",
            "day_utc": DAY,
            "status": "OK",
            "reason_codes": [],
            "submission_id": submission_id,
            "binding_hash": binding_hash,
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "source_intent_id": "c2_trend_eq_spy_2026-04-13_v1",
            "intent_sha256": intent_sha,
            "event_type": "ORDER_STATUS",
            "event_time_utc": event_time_utc,
            "observed_at_utc": event_time_utc,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "broker_ids": {"order_id": order_id, "perm_id": perm_id},
            "fill": {"fill_qty": int(filled_qty), "fill_price": "0", "commission": "0", "currency": "USD"},
            "order_state": {"status": order_status, "filled_qty": int(filled_qty), "remaining_qty": max(0, 1 - int(filled_qty)), "avg_fill_price": "0"},
            "canonical_json_hash": canonical_json_hash,
        },
    )


def _append_broker_event(
    truth_root: Path,
    *,
    broker_day: str,
    event_type: str,
    args: list[str],
    received_utc: str,
) -> None:
    path = truth_root / "execution_evidence_v1" / "broker_events" / broker_day / "broker_event_log.v1.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "schema_id": "BROKER_EVENT_RAW",
        "schema_version": 1,
        "received_utc": received_utc,
        "event_type": event_type,
        "ib_fields": {"args": [{"value": value} for value in args]},
        "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER", "client_id": 7},
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


def test_submission_lifecycle_refresh_updates_execution_event_record_from_latest_stream(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    subdir = _seed_submission(
        truth_root,
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        broker_hash=TARGET_BROKER_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
    )
    _seed_execution_event_record(subdir)
    _seed_stream_record(
        truth_root,
        filename="old.execution_event_stream_record.v1.json",
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
        event_time_utc="2026-04-12T13:36:05Z",
        canonical_json_hash="08a165e9274ebfec0a0f3ee5c5a269f648da74e5319817e2b5490d58329fa923",
    )
    _seed_stream_record(
        truth_root,
        filename="latest.execution_event_stream_record.v1.json",
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
        event_time_utc="2026-04-12T14:23:13Z",
        canonical_json_hash="6d6cee1af914f86a26bb0f3f4957b2ad9912b2d18ea26691720b24c8963ca71d",
    )

    assert (
        lifecycle_refresh.main(
            ["--day_utc", DAY, "--truth_root", str(truth_root), "--submission_id", TARGET_SUBMISSION_ID]
        )
        == 0
    )

    refreshed = json.loads((subdir / "execution_event_record.v1.json").read_text(encoding="utf-8"))
    assert refreshed["event_time_utc"] == "2026-04-12T14:23:13Z"
    assert refreshed["raw_broker_status"] == "PRESUBMITTED"
    assert refreshed["upstream_hash"] == "6d6cee1af914f86a26bb0f3f4957b2ad9912b2d18ea26691720b24c8963ca71d"
    assert refreshed["filled_qty"] == 0
    attempt = json.loads((subdir / "broker_submit_attempt_v1.json").read_text(encoding="utf-8"))
    ack = json.loads((subdir / "broker_acknowledgement_v1.json").read_text(encoding="utf-8"))
    outcome = json.loads((subdir / "broker_order_outcome_v1.json").read_text(encoding="utf-8"))
    assert attempt["attempt_state"] == "SUBMIT_ATTEMPTED"
    assert ack["ack_state"] == "BROKER_ID_ASSIGNED"
    assert ack["identity_strength"] == "STRONG"
    assert outcome["outcome_state"] == "BROKER_ACCEPTED"
    assert outcome["status"] == "PRESUBMITTED"
    assert not (truth_root / "execution_kernel_v1" / "execution_state_records").exists()


def test_submission_lifecycle_refresh_emits_pending_when_stream_missing(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    subdir = _seed_submission(
        truth_root,
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        broker_hash=TARGET_BROKER_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
    )

    assert (
        lifecycle_refresh.main(
            ["--day_utc", DAY, "--truth_root", str(truth_root), "--submission_id", TARGET_SUBMISSION_ID]
        )
        == 0
    )

    assert not (subdir / "execution_event_record.v1.json").exists()
    outcome = json.loads((subdir / "broker_order_outcome_v1.json").read_text(encoding="utf-8"))
    assert outcome["outcome_state"] == "BROKER_ACCEPTED"
    assert "BROKER_ACCEPTED_FROM_STATUS" in outcome["reason_codes"]
    ledger_path = truth_root / "fill_ledger_v1" / DAY / f"{TARGET_SUBMISSION_ID}.fill_ledger.v1.json"
    assert ledger_path.exists()
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert ledger["submission_id"] == TARGET_SUBMISSION_ID
    assert ledger["filled_qty"] == 0
    assert ledger["lifecycle_status"] == "OPEN"


def test_submission_lifecycle_refresh_keeps_pending_when_stream_has_no_broker_ids(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    subdir = _seed_submission(
        truth_root,
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        broker_hash=TARGET_BROKER_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
    )
    broker_submission_path = subdir / "broker_submission_record.v2.json"
    broker_submission = json.loads(broker_submission_path.read_text(encoding="utf-8"))
    broker_submission["broker_ids"] = {"order_id": None, "perm_id": None}
    _write_json(broker_submission_path, broker_submission)
    _seed_stream_record(
        truth_root,
        filename="no-broker-ids.execution_event_stream_record.v1.json",
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=None,  # type: ignore[arg-type]
        perm_id=None,  # type: ignore[arg-type]
        event_time_utc="2026-04-13T14:23:13Z",
        canonical_json_hash="dbca6daa0de7b3143db98b51885076f6efd149f83476a7d619f2756cf6622db9",
    )

    assert (
        lifecycle_refresh.main(
            ["--day_utc", DAY, "--truth_root", str(truth_root), "--submission_id", TARGET_SUBMISSION_ID]
        )
        == 0
    )

    assert not (subdir / "execution_event_record.v1.json").exists()
    ledger_path = truth_root / "fill_ledger_v1" / DAY / f"{TARGET_SUBMISSION_ID}.fill_ledger.v1.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert ledger["submission_id"] == TARGET_SUBMISSION_ID
    assert ledger["filled_qty"] == 0
    assert ledger["lifecycle_status"] == "OPEN"


def test_submission_lifecycle_refresh_marks_dry_run_no_broker_id(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    truth_root = tmp_path / "truth"
    subdir = _seed_submission(
        truth_root,
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        broker_hash=TARGET_BROKER_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
    )
    broker_submission_path = subdir / "broker_submission_record.v2.json"
    broker_submission = json.loads(broker_submission_path.read_text(encoding="utf-8"))
    broker_submission["broker_ids"] = {"order_id": None, "perm_id": None}
    broker_submission["error"] = {
        "code": "DRY_RUN_NO_BROKER_ID",
        "message": "dry run execution without broker identifiers",
    }
    _write_json(broker_submission_path, broker_submission)
    _seed_stream_record(
        truth_root,
        filename="dry-run.execution_event_stream_record.v1.json",
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=None,  # type: ignore[arg-type]
        perm_id=None,  # type: ignore[arg-type]
        event_time_utc="2026-04-13T14:23:13Z",
        canonical_json_hash="d5e897dc4de4e798d96d97ebefb4f6db073d8ab05c973f8a67c3ce4c2cf8f7cd",
    )

    assert (
        lifecycle_refresh.main(
            ["--day_utc", DAY, "--truth_root", str(truth_root), "--submission_id", TARGET_SUBMISSION_ID]
        )
        == 0
    )

    stdout = capsys.readouterr().out
    assert "execution_event_action=DRY_RUN_NO_BROKER_ID" in stdout
    assert not (subdir / "execution_event_record.v1.json").exists()
    outcome = json.loads((subdir / "broker_order_outcome_v1.json").read_text(encoding="utf-8"))
    assert outcome["outcome_state"] == "UNKNOWN_PENDING"
    assert "BROKER_OUTCOME_NOT_RECONCILED" in outcome["reason_codes"]
    assert not (subdir / "broker_acknowledgement_v1.json").exists()
    ledger_path = truth_root / "fill_ledger_v1" / DAY / f"{TARGET_SUBMISSION_ID}.fill_ledger.v1.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert ledger["submission_id"] == TARGET_SUBMISSION_ID
    assert ledger["filled_qty"] == 0
    assert ledger["lifecycle_status"] == "OPEN"


def test_submission_lifecycle_refresh_maps_ib_error_201_to_broker_rejected(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    subdir = _seed_submission(
        truth_root,
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        broker_hash=TARGET_BROKER_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=90,
        perm_id=0,
    )
    _append_broker_event(
        truth_root,
        broker_day="2026-04-25",
        event_type="error",
        args=[
            "reqId=90",
            "errorCode=201",
            "errorString=Riskless combination orders are not allowed.",
            "advancedOrderRejectJson=",
        ],
        received_utc="2026-04-25T14:30:10Z",
    )

    assert lifecycle_refresh.main(["--day_utc", DAY, "--truth_root", str(truth_root), "--submission_id", TARGET_SUBMISSION_ID]) == 0

    ack = json.loads((subdir / "broker_acknowledgement_v1.json").read_text(encoding="utf-8"))
    outcome = json.loads((subdir / "broker_order_outcome_v1.json").read_text(encoding="utf-8"))
    assert ack["identity_strength"] == "WEAK"
    assert outcome["outcome_state"] == "BROKER_REJECTED"
    assert "IB_ERROR_201_RISKLESS_COMBINATION" in outcome["reason_codes"]
    assert outcome["error"]["code"] == "IB_ERROR_201"


def test_submission_lifecycle_refresh_maps_submission_record_ib_error_201_to_specific_rejection(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    subdir = _seed_submission(
        truth_root,
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        broker_hash=TARGET_BROKER_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=90,
        perm_id=0,
    )
    broker_record_path = subdir / "broker_submission_record.v2.json"
    broker_record = json.loads(broker_record_path.read_text(encoding="utf-8"))
    broker_record["status"] = "CANCELLED"
    broker_record["error"] = {"code": "IB_ERROR_201", "message": "Riskless combination orders are not allowed."}
    _write_json(broker_record_path, broker_record)

    assert lifecycle_refresh.main(["--day_utc", DAY, "--truth_root", str(truth_root), "--submission_id", TARGET_SUBMISSION_ID]) == 0

    outcome = json.loads((subdir / "broker_order_outcome_v1.json").read_text(encoding="utf-8"))
    assert outcome["outcome_state"] == "BROKER_REJECTED"
    assert "IB_ERROR_201_RISKLESS_COMBINATION" in outcome["reason_codes"]
    assert outcome["error"]["code"] == "IB_ERROR_201"


def test_submission_lifecycle_refresh_maps_filled_status_to_filled_outcome(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    subdir = _seed_submission(
        truth_root,
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        broker_hash=TARGET_BROKER_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
    )
    _seed_stream_record(
        truth_root,
        filename="filled.execution_event_stream_record.v1.json",
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
        event_time_utc="2026-04-13T14:23:13Z",
        canonical_json_hash="25f67d2975ca5d7f984a1366e5a3e9028f72e9be38a0636385f1fdbf361e4d8b",
        order_status="FILLED",
        filled_qty=1,
    )

    assert lifecycle_refresh.main(["--day_utc", DAY, "--truth_root", str(truth_root), "--submission_id", TARGET_SUBMISSION_ID]) == 0

    outcome = json.loads((subdir / "broker_order_outcome_v1.json").read_text(encoding="utf-8"))
    assert outcome["outcome_state"] == "FILLED"
    assert outcome["status"] == "FILLED"


def test_submission_lifecycle_refresh_maps_cancelled_status_to_cancelled_outcome(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    subdir = _seed_submission(
        truth_root,
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        broker_hash=TARGET_BROKER_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
    )
    _seed_stream_record(
        truth_root,
        filename="cancelled.execution_event_stream_record.v1.json",
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
        event_time_utc="2026-04-13T14:23:13Z",
        canonical_json_hash="e1ca239ad7f9e0d4791adfc0e00662a31f8b8086bdf95fa2c00906dbaec6dc6f",
        order_status="CANCELLED",
        filled_qty=0,
    )

    assert lifecycle_refresh.main(["--day_utc", DAY, "--truth_root", str(truth_root), "--submission_id", TARGET_SUBMISSION_ID]) == 0

    outcome = json.loads((subdir / "broker_order_outcome_v1.json").read_text(encoding="utf-8"))
    assert outcome["outcome_state"] == "BROKER_CANCELLED"
    assert outcome["status"] == "CANCELLED"


def test_submission_lifecycle_refresh_accepts_v2_payload_under_legacy_v1_plan_filename(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    subdir = _seed_submission(
        truth_root,
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        broker_hash=TARGET_BROKER_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
    )
    legacy_v1_plan = subdir / "equity_order_plan.v1.json"
    legacy_v1_plan.write_text((subdir / "equity_order_plan.v2.json").read_text(encoding="utf-8"), encoding="utf-8")
    (subdir / "equity_order_plan.v2.json").unlink()

    assert (
        lifecycle_refresh.main(
            ["--day_utc", DAY, "--truth_root", str(truth_root), "--submission_id", TARGET_SUBMISSION_ID]
        )
        == 0
    )
    ledger_path = truth_root / "fill_ledger_v1" / DAY / f"{TARGET_SUBMISSION_ID}.fill_ledger.v1.json"
    assert ledger_path.exists()
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert ledger["submission_id"] == TARGET_SUBMISSION_ID
    assert ledger["filled_qty"] == 0
    assert ledger["lifecycle_status"] == "OPEN"


def test_submission_lifecycle_refresh_updates_target_fill_ledger_only(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    target_subdir = _seed_submission(
        truth_root,
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        broker_hash=TARGET_BROKER_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
    )
    other_subdir = _seed_submission(
        truth_root,
        submission_id=OTHER_SUBMISSION_ID,
        binding_hash=OTHER_BINDING_HASH,
        broker_hash=OTHER_BROKER_HASH,
        intent_sha=OTHER_INTENT_SHA,
        order_id=70,
        perm_id=1870974295,
    )
    _seed_execution_event_record(target_subdir)
    _write_json(
        other_subdir / "execution_event_record.v1.json",
        {
            "schema_id": "execution_event_record",
            "schema_version": "v1",
            "created_at_utc": f"{DAY}T00:00:00Z",
            "event_time_utc": f"{DAY}T00:00:00Z",
            "binding_hash": OTHER_BINDING_HASH,
            "broker_submission_hash": OTHER_BROKER_HASH,
            "broker_order_id": "70",
            "perm_id": "1870974295",
            "status": "UNKNOWN",
            "filled_qty": 0,
            "avg_price": "0",
            "raw_broker_status": None,
            "raw_payload_digest": None,
            "sequence_num": None,
            "canonical_json_hash": "1" * 64,
            "upstream_hash": None,
        },
    )
    _seed_stream_record(
        truth_root,
        filename="target-old.execution_event_stream_record.v1.json",
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
        event_time_utc="2026-04-12T13:36:05Z",
        canonical_json_hash="08a165e9274ebfec0a0f3ee5c5a269f648da74e5319817e2b5490d58329fa923",
    )
    _seed_stream_record(
        truth_root,
        filename="target-latest.execution_event_stream_record.v1.json",
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
        event_time_utc="2026-04-12T14:23:13Z",
        canonical_json_hash="6d6cee1af914f86a26bb0f3f4957b2ad9912b2d18ea26691720b24c8963ca71d",
    )
    _seed_stream_record(
        truth_root,
        filename="other.execution_event_stream_record.v1.json",
        submission_id=OTHER_SUBMISSION_ID,
        binding_hash=OTHER_BINDING_HASH,
        intent_sha=OTHER_INTENT_SHA,
        order_id=70,
        perm_id=1870974295,
        event_time_utc="2026-04-12T13:36:05Z",
        canonical_json_hash="bd2d80a7c72addc8cad4c3c307fe1839d9d926167e4dbe4404437f98992002cc",
    )

    other_ledger_path = truth_root / "fill_ledger_v1" / DAY / f"{OTHER_SUBMISSION_ID}.fill_ledger.v1.json"
    other_ledger_path.parent.mkdir(parents=True, exist_ok=True)
    other_ledger_path.write_text("{\"conflict\":true}\n", encoding="utf-8")
    other_before = other_ledger_path.read_bytes()

    assert (
        lifecycle_refresh.main(
            ["--day_utc", DAY, "--truth_root", str(truth_root), "--submission_id", TARGET_SUBMISSION_ID]
        )
        == 0
    )

    target_ledger = json.loads(
        (truth_root / "fill_ledger_v1" / DAY / f"{TARGET_SUBMISSION_ID}.fill_ledger.v1.json").read_text(encoding="utf-8")
    )
    assert target_ledger["lifecycle_status"] == "OPEN"
    assert target_ledger["filled_qty"] == 0
    assert target_ledger["event_hashes"] == [
        "08a165e9274ebfec0a0f3ee5c5a269f648da74e5319817e2b5490d58329fa923",
        "6d6cee1af914f86a26bb0f3f4957b2ad9912b2d18ea26691720b24c8963ca71d",
    ]
    validated = validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id="fill_ledger_v1",
        payload=target_ledger,
        required_finality_states=["finalized", "corrected"],
    )
    assert target_ledger["closure_state"] == "COMPLETE"
    assert target_ledger["blocking_codes"] == []
    assert target_ledger["missing_dependency_artifacts"] == []
    assert validated["constitutional_lineage"]["artifact_type"] == "fill_ledger_v1"
    assert validated["constitutional_dependency_declaration"]["declared_dependency_artifacts"] == []
    assert other_ledger_path.read_bytes() == other_before


def test_submission_scoped_fill_ledger_refresh_ignores_unrelated_same_day_conflict(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    target_subdir = _seed_submission(
        truth_root,
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        broker_hash=TARGET_BROKER_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
    )
    _seed_execution_event_record(target_subdir)
    _seed_stream_record(
        truth_root,
        filename="latest.execution_event_stream_record.v1.json",
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
        event_time_utc="2026-04-12T14:23:13Z",
        canonical_json_hash="6d6cee1af914f86a26bb0f3f4957b2ad9912b2d18ea26691720b24c8963ca71d",
    )

    other_ledger_path = truth_root / "fill_ledger_v1" / DAY / f"{OTHER_SUBMISSION_ID}.fill_ledger.v1.json"
    other_ledger_path.parent.mkdir(parents=True, exist_ok=True)
    other_ledger_path.write_text("{\"conflict\":true}\n", encoding="utf-8")

    assert (
        fill_ledger.main(
            ["--day_utc", DAY, "--truth_root", str(truth_root), "--submission_id", TARGET_SUBMISSION_ID]
        )
        == 0
    )

    target_ledger = truth_root / "fill_ledger_v1" / DAY / f"{TARGET_SUBMISSION_ID}.fill_ledger.v1.json"
    assert target_ledger.exists()
    assert other_ledger_path.read_text(encoding="utf-8") == "{\"conflict\":true}\n"


def test_submission_lifecycle_refresh_fails_closed_on_target_conflict(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    subdir = _seed_submission(
        truth_root,
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        broker_hash=TARGET_BROKER_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
    )
    _seed_execution_event_record(subdir, order_id="999")
    _seed_stream_record(
        truth_root,
        filename="latest.execution_event_stream_record.v1.json",
        submission_id=TARGET_SUBMISSION_ID,
        binding_hash=TARGET_BINDING_HASH,
        intent_sha=TARGET_INTENT_SHA,
        order_id=75,
        perm_id=1870974300,
        event_time_utc="2026-04-12T14:23:13Z",
        canonical_json_hash="6d6cee1af914f86a26bb0f3f4957b2ad9912b2d18ea26691720b24c8963ca71d",
    )

    with pytest.raises(RuntimeError, match="REFUSE_OVERWRITE_DIFFERENT_BYTES"):
        lifecycle_refresh.main(
            ["--day_utc", DAY, "--truth_root", str(truth_root), "--submission_id", TARGET_SUBMISSION_ID]
        )
