from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest import mock


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseF.execution_evidence.run import run_execution_evidence_truth_day_v1 as exec_truth  # noqa: E402
from constellation_2.phaseF.positions.run import run_positions_effective_pointer_day_v1 as pos_eff  # noqa: E402
from constellation_2.phaseF.positions.run import run_positions_snapshot_day_v4 as pos_v4  # noqa: E402
import ops.tools.run_fill_ledger_day_v1 as fill_ledger  # noqa: E402
import ops.tools.run_orphan_submission_backfill_day_v1 as orphan_backfill  # noqa: E402


DAY = "2026-04-13"
ORPHAN_SUBMISSION_ID = "d50ff800edd387bdb8ef31f8d9355e8d6c4b8d957c81035d246963fcba5db0da"
INTENT_SHA = "00091fc97c0d1e953e652e0f2461c1b2b95492b8bb6891c04fbd0471adcf8ce0"
BINDING_HASH = "b13bd8662bfec5e2f072e28fc6720163bb010daf694eddd2e3bdeb7ee81ae678"
GIT_SHA = "a" * 40


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed_orphan_submission(truth_root: Path) -> None:
    subdir = truth_root / "execution_evidence_v1" / "submissions" / DAY / ORPHAN_SUBMISSION_ID
    _write_json(
        subdir / "equity_order_plan.v2.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v2",
            "plan_id": "c2_trend_eq_spy_2026-04-13_v1",
            "created_at_utc": f"{DAY}T00:00:00Z",
            "intent_hash": "79c8b0b7a54a4460d77a8cafd1f19e8a40f1a6db4cdbe8daa144943270f64a18",
            "intent_sha256": INTENT_SHA,
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
            "canonical_json_hash": "bc086f7af0901e9c6146a754d2196101aa039ac5c9c49fe30c48577d50b6592c",
        },
    )
    _write_json(
        subdir / "binding_record.v2.json",
        {
            "schema_id": "binding_record",
            "schema_version": "v2",
            "submission_id": ORPHAN_SUBMISSION_ID,
            "intent_id": "c2_trend_eq_spy_2026-04-13_v1",
            "intent_hash": "79c8b0b7a54a4460d77a8cafd1f19e8a40f1a6db4cdbe8daa144943270f64a18",
            "canonical_json_hash": BINDING_HASH,
        },
    )
    _write_json(
        subdir / "mapping_ledger_record.v2.json",
        {
            "schema_id": "mapping_ledger_record",
            "schema_version": "v2",
            "submission_id": ORPHAN_SUBMISSION_ID,
            "canonical_json_hash": "4f29528ca4c53bacd9d0c3de4dbba4a8e2b429b9d6869b3dee7e882171d69f55",
        },
    )
    _write_json(
        subdir / "veto_record.v1.json",
        {
            "schema_id": "veto_record",
            "schema_version": "v1",
            "boundary": "SUBMIT",
            "observed_at_utc": f"{DAY}T00:00:00Z",
            "inputs": {
                "chain_snapshot_hash": None,
                "freshness_cert_hash": None,
                "intent_hash": "79c8b0b7a54a4460d77a8cafd1f19e8a40f1a6db4cdbe8daa144943270f64a18",
                "plan_hash": None,
            },
            "pointers": [
                str(subdir / "equity_order_plan.v2.json"),
                str(subdir / "mapping_ledger_record.v2.json"),
                str(subdir / "binding_record.v2.json"),
            ],
            "reason_code": "C2_SUBMIT_FAIL_CLOSED_REQUIRED",
            "reason_detail": (
                "SUBMIT_FAILURE: SchemaValidationError("
                "\"SCHEMA_VALIDATION_FAILED: constellation_2/schemas/broker_submission_record.v2.schema.json "
                "$.status: 'PENDINGSUBMIT' is not one of [...]\""
                ")"
            ),
            "upstream_hash": BINDING_HASH,
            "canonical_json_hash": "f8c2484e4093ea15e6ed2d48099faf7078fab6705441ede2e8d135e47904fd9d",
        },
    )


def _seed_execution_stream(truth_root: Path) -> None:
    _write_json(
        truth_root / "execution_stream_v1" / DAY / "orphan.execution_event_stream_record.v1.json",
        {
            "schema_id": "C2_EXECUTION_EVENT_STREAM_RECORD_V1",
            "schema_version": 1,
            "produced_utc": f"{DAY}T00:00:00Z",
            "day_utc": DAY,
            "status": "OK",
            "reason_codes": ["ATTRIBUTED_BY_POST_HANDOFF_ORPHAN_FALLBACK"],
            "submission_id": ORPHAN_SUBMISSION_ID,
            "binding_hash": BINDING_HASH,
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "source_intent_id": "c2_trend_eq_spy_2026-04-13_v1",
            "intent_sha256": INTENT_SHA,
            "event_type": "ORDER_STATUS",
            "event_time_utc": "2026-04-12T13:36:05Z",
            "observed_at_utc": "2026-04-12T13:36:05Z",
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "broker_ids": {"order_id": 70, "perm_id": 1870974295},
            "fill": {"fill_qty": 0, "fill_price": "0", "commission": "0", "currency": "USD"},
            "order_state": {"status": "PRESUBMITTED", "filled_qty": 0, "remaining_qty": 1, "avg_fill_price": "0"},
            "canonical_json_hash": "bd2d80a7c72addc8cad4c3c307fe1839d9d926167e4dbe4404437f98992002cc",
        },
    )


def _run_positions_pipeline(canonical_truth: Path) -> None:
    exec_day_paths = mock.Mock(
        return_value=mock.Mock(submissions_day_dir=canonical_truth / "execution_evidence_v1" / "submissions" / DAY)
    )
    pos_v4_day_paths = mock.Mock(
        return_value=mock.Mock(
            snapshot_path=canonical_truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v4.json",
            failure_path=canonical_truth / "positions_v1" / "failures" / DAY / "failure_v4.json",
        )
    )
    pos_eff_day_paths = mock.Mock(
        return_value=mock.Mock(
            pointer_path=canonical_truth / "positions_v1" / "effective_v1" / "days" / DAY / "positions_effective_pointer.v1.json",
            failure_path=canonical_truth / "positions_v1" / "effective_v1" / "failures" / DAY / "failure.json",
        )
    )
    with (
        mock.patch.object(pos_v4, "exec_day_paths_v1", exec_day_paths),
        mock.patch.object(pos_v4, "day_paths_v4", pos_v4_day_paths),
        mock.patch.object(pos_eff, "day_paths_v4", pos_v4_day_paths),
        mock.patch.object(
            pos_eff,
            "day_paths_v3",
            mock.Mock(
                return_value=mock.Mock(snapshot_path=canonical_truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v3.json")
            ),
        ),
        mock.patch.object(
            pos_eff,
            "day_paths_v2",
            mock.Mock(
                return_value=mock.Mock(snapshot_path=canonical_truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json")
            ),
        ),
        mock.patch.object(pos_eff, "day_paths_effective_v1", pos_eff_day_paths),
    ):
        assert pos_v4.main(["--day_utc", DAY, "--producer_git_sha", GIT_SHA, "--producer_repo", "constellation"]) == 0
        assert pos_eff.main(["--day_utc", DAY, "--producer_git_sha", GIT_SHA, "--producer_repo", "constellation"]) == 0


def test_orphan_submission_is_backfilled_and_propagates_to_fill_ledger_and_positions(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _seed_orphan_submission(sleeve_truth)
    _seed_execution_stream(canonical_truth)

    assert orphan_backfill.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(sleeve_truth),
            "--stream_truth_root",
            str(canonical_truth),
        ]
    ) == 0

    sleeve_subdir = sleeve_truth / "execution_evidence_v1" / "submissions" / DAY / ORPHAN_SUBMISSION_ID
    sleeve_bsr = json.loads((sleeve_subdir / "broker_submission_record.v2.json").read_text(encoding="utf-8"))
    sleeve_eer = json.loads((sleeve_subdir / "execution_event_record.v1.json").read_text(encoding="utf-8"))
    assert sleeve_bsr["status"] == "PRESUBMITTED"
    assert sleeve_bsr["broker_ids"] == {"order_id": 70, "perm_id": 1870974295}
    assert sleeve_bsr["error"]["code"] == "RECONSTRUCTED_FROM_EXECUTION_STREAM_V1"
    assert sleeve_eer["status"] == "UNKNOWN"
    assert sleeve_eer["raw_broker_status"] == "PRESUBMITTED"
    assert sleeve_eer["upstream_hash"] == "bd2d80a7c72addc8cad4c3c307fe1839d9d926167e4dbe4404437f98992002cc"
    assert sleeve_eer["broker_submission_hash"] == sleeve_bsr["canonical_json_hash"]

    assert exec_truth.main(
        [
            "--day_utc",
            DAY,
            "--producer_git_sha",
            GIT_SHA,
            "--producer_repo",
            "constellation",
            "--truth_root",
            str(canonical_truth),
            "--source_truth_root",
            str(sleeve_truth),
        ]
    ) == 0
    assert orphan_backfill.main(
        [
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth),
            "--stream_truth_root",
            str(canonical_truth),
        ]
    ) == 0

    canonical_subdir = canonical_truth / "execution_evidence_v1" / "submissions" / DAY / ORPHAN_SUBMISSION_ID
    assert (canonical_subdir / "broker_submission_record.v2.json").read_bytes() == (sleeve_subdir / "broker_submission_record.v2.json").read_bytes()
    assert (canonical_subdir / "execution_event_record.v1.json").read_bytes() == (sleeve_subdir / "execution_event_record.v1.json").read_bytes()

    with mock.patch.object(
        sys,
        "argv",
        ["run_fill_ledger_day_v1.py", "--day_utc", DAY, "--truth_root", str(canonical_truth)],
    ):
        assert fill_ledger.main() == 0

    ledger_path = canonical_truth / "fill_ledger_v1" / DAY / f"{ORPHAN_SUBMISSION_ID}.fill_ledger.v1.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert ledger["lifecycle_status"] == "OPEN"
    assert ledger["filled_qty"] == 0
    assert ledger["remaining_qty"] == 1

    _run_positions_pipeline(canonical_truth)
    snapshot_path = canonical_truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v4.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    # Positions are projections only; zero-fill OPEN state does not create a position,
    # and orphan backfill restores submission/execution evidence rather than fill truth.
    assert snapshot["positions"]["items"] == []


def test_orphan_backfill_and_propagation_reruns_are_idempotent(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _seed_orphan_submission(sleeve_truth)
    _seed_execution_stream(canonical_truth)

    backfill_args = [
        "--day_utc",
        DAY,
        "--truth_root",
        str(sleeve_truth),
        "--stream_truth_root",
        str(canonical_truth),
    ]
    publish_args = [
        "--day_utc",
        DAY,
        "--producer_git_sha",
        GIT_SHA,
        "--producer_repo",
        "constellation",
        "--truth_root",
        str(canonical_truth),
        "--source_truth_root",
        str(sleeve_truth),
    ]
    assert orphan_backfill.main(backfill_args) == 0
    assert exec_truth.main(publish_args) == 0
    assert orphan_backfill.main(backfill_args) == 0
    assert exec_truth.main(publish_args) == 0

    canonical_subdir = canonical_truth / "execution_evidence_v1" / "submissions" / DAY / ORPHAN_SUBMISSION_ID
    sleeve_subdir = sleeve_truth / "execution_evidence_v1" / "submissions" / DAY / ORPHAN_SUBMISSION_ID
    assert len(
        [
            p
            for p in (canonical_truth / "execution_evidence_v1" / "submissions" / DAY).iterdir()
            if p.is_dir() and not p.name.startswith("__")
        ]
    ) == 1
    assert (canonical_subdir / "broker_submission_record.v2.json").read_bytes() == (sleeve_subdir / "broker_submission_record.v2.json").read_bytes()
    assert (canonical_subdir / "execution_event_record.v1.json").read_bytes() == (sleeve_subdir / "execution_event_record.v1.json").read_bytes()


def test_positions_snapshot_backfill_upgrade_accepts_superset_positions() -> None:
    existing = {
        "schema_id": "C2_POSITIONS_SNAPSHOT_V4",
        "day_utc": DAY,
        "positions": {
            "items": [
                {
                    "position_id": "a" * 64,
                    "instrument": {"kind": "EQUITY", "symbol": "SPY"},
                    "qty": 0,
                }
            ]
        },
    }
    candidate = {
        "schema_id": "C2_POSITIONS_SNAPSHOT_V4",
        "day_utc": DAY,
        "positions": {
            "items": [
                {
                    "position_id": "a" * 64,
                    "instrument": {"kind": "EQUITY", "symbol": "SPY"},
                    "qty": 0,
                },
                {
                    "position_id": "b" * 64,
                    "instrument": {"kind": "EQUITY", "symbol": "SPY"},
                    "qty": 0,
                },
            ]
        },
    }
    assert pos_v4._is_safe_backfill_upgrade(existing, candidate) is True
