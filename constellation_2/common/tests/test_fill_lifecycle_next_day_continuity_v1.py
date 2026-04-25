from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseF.positions.run import run_positions_snapshot_day_v2 as pos_v2  # noqa: E402
import ops.tools.run_paper_session_admission_v1 as session_admission  # noqa: E402


DAY = "2026-04-14"
INPUT_DAY = "2026-04-13"
GIT_SHA = "7d64db4a5e4d68af1d89a56edf64fb9024bb218a"
SUBMISSION_ID = "f6b7a3dbbabf3f3bdd52a338ebc2b3cf72677be25e4f9de0ac7c39a26c44fe17"
BINDING_HASH = "ed8bf1e2d6479e429ff0696515bec81cb855d96b373acaac01c772cd53a1f7cc"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed_empty_positions_v2(path: Path, day_utc: str) -> None:
    _write_json(
        path,
        {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V2",
            "schema_version": 2,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "day_utc": day_utc,
            "producer": {
                "repo": "constellation",
                "git_sha": GIT_SHA,
                "module": "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
            },
            "status": "OK",
            "reason_codes": ["NO_SUBMISSIONS_EMPTY_POSITIONS_V2"],
            "input_manifest": [
                {
                    "type": "execution_evidence",
                    "path": f"/tmp/{day_utc}",
                    "sha256": "0" * 64,
                    "day_utc": day_utc,
                    "producer": "execution_evidence_v1",
                }
            ],
            "positions": {
                "currency": "USD",
                "asof_utc": f"{day_utc}T00:00:00Z",
                "items": [],
                "notes": ["SAFE_IDLE: no submissions present for day; emitting empty positions snapshot (v2)"],
            },
        },
    )


def _seed_execution_submission(exec_dir: Path, day_utc: str, *, filled_qty: int = 0, avg_price: str = "0") -> None:
    subdir = exec_dir / day_utc / SUBMISSION_ID
    raw_status = "FILLED" if filled_qty > 0 else "PRESUBMITTED"
    lifecycle_status = "FILLED" if filled_qty > 0 else "UNKNOWN"
    _write_json(
        subdir / "execution_event_record.v1.json",
        {
            "schema_id": "execution_event_record",
            "schema_version": "v1",
            "created_at_utc": f"{day_utc}T00:00:00Z",
            "event_time_utc": f"{day_utc}T00:00:00Z",
            "binding_hash": BINDING_HASH,
            "broker_submission_hash": "a" * 64,
            "broker_order_id": "75",
            "perm_id": "1870974300",
            "status": lifecycle_status,
            "filled_qty": int(filled_qty),
            "avg_price": avg_price,
            "raw_broker_status": raw_status,
            "raw_payload_digest": None,
            "sequence_num": None,
            "canonical_json_hash": None,
            "upstream_hash": None,
        },
    )
    _write_json(
        subdir / "equity_order_plan.v2.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v2",
            "plan_id": "plan-0000000000000000000000000001",
            "created_at_utc": f"{day_utc}T00:00:00Z",
            "intent_hash": "a" * 64,
            "structure": "EQUITY_SPOT",
            "symbol": "SPY",
            "currency": "USD",
            "action": "BUY",
            "qty_shares": 1,
            "order_terms": {"order_type": "LIMIT", "limit_price": "679.91", "time_in_force": "DAY"},
            "risk_proof": None,
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "source_intent_id": "intent-0000000000000000000000000001",
            "intent_sha256": "b" * 64,
            "canonical_json_hash": "c" * 64,
        },
    )


def _seed_positions_snapshot_v2(path: Path, day_utc: str, *, qty: int, reason_codes: list[str]) -> None:
    _write_json(
        path,
        {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V2",
            "schema_version": 2,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "day_utc": day_utc,
            "producer": {
                "repo": "constellation",
                "git_sha": GIT_SHA,
                "module": "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
            },
            "status": "OK",
            "reason_codes": reason_codes,
            "input_manifest": [
                {
                    "type": "execution_evidence",
                    "path": f"/tmp/{day_utc}",
                    "sha256": "0" * 64,
                    "day_utc": day_utc,
                    "producer": "execution_evidence_v1",
                }
            ],
            "positions": {
                "currency": "USD",
                "asof_utc": f"{day_utc}T00:00:00Z",
                "notes": ["seeded"],
                "items": [
                    {
                        "position_id": BINDING_HASH,
                        "engine_id": "C2_TREND_EQ_PRIMARY_V1",
                        "instrument": {
                            "kind": "EQUITY",
                            "underlying": "SPY",
                            "expiry": None,
                            "strike": None,
                            "right": None,
                        },
                        "qty": int(qty),
                        "avg_cost_cents": 67991 if qty > 0 else 0,
                        "market_exposure_type": "UNDEFINED_RISK",
                        "max_loss_cents": None,
                        "opened_day_utc": day_utc,
                        "status": "OPEN",
                    }
                ],
            },
        },
    )


def _seed_previous_day_v4(path: Path, *, qty: int = 1) -> None:
    _write_json(
        path,
        {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V4",
            "schema_version": 4,
            "produced_utc": f"{INPUT_DAY}T00:00:00Z",
            "day_utc": INPUT_DAY,
            "producer": {
                "repo": "constellation",
                "git_sha": GIT_SHA,
                "module": "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v4.py",
            },
            "status": "OK",
            "reason_codes": ["INSTRUMENT_IDENTITY_FROM_EXECUTION_EVIDENCE_V4"],
            "input_manifest": [
                {
                    "type": "execution_evidence",
                    "path": "/tmp/input",
                    "sha256": "0" * 64,
                    "day_utc": INPUT_DAY,
                    "producer": "execution_evidence_v1",
                }
            ],
            "positions": {
                "currency": "USD",
                "asof_utc": f"{INPUT_DAY}T00:00:00Z",
                "notes": ["carry"],
                "items": [
                    {
                        "position_id": BINDING_HASH,
                        "engine_id": "unknown",
                        "instrument": {
                            "kind": "EQUITY",
                            "symbol": "SPY",
                            "currency": "USD",
                            "ib_conId": None,
                            "ib_localSymbol": None,
                        },
                        "qty": int(qty),
                        "avg_cost_cents": 67991 if qty > 0 else 0,
                        "market_exposure_type": "UNDEFINED_RISK",
                        "max_loss_cents": None,
                        "opened_day_utc": INPUT_DAY,
                        "status": "OPEN",
                    }
                ],
            },
        },
    )


def test_positions_snapshot_v2_ignores_zero_fill_submission_when_repairing_stale_snapshot(tmp_path: Path) -> None:
    exec_root = tmp_path / "truth" / "execution_evidence_v1" / "submissions"
    out_snapshot = tmp_path / "truth" / "positions_v1" / "snapshots" / INPUT_DAY / "positions_snapshot.v2.json"
    latest_path = tmp_path / "truth" / "positions_v1" / "latest_pointer.v2.json"
    failure_path = tmp_path / "truth" / "positions_v1" / "failures" / INPUT_DAY / "failure.json"
    _seed_execution_submission(exec_root, INPUT_DAY)
    _seed_positions_snapshot_v2(out_snapshot, INPUT_DAY, qty=0, reason_codes=["BOOTSTRAP_UNKNOWN_INSTRUMENT_V2"])

    with (
        mock.patch.object(pos_v2, "exec_day_paths_v1", return_value=SimpleNamespace(submissions_day_dir=exec_root / INPUT_DAY)),
        mock.patch.object(
            pos_v2,
            "day_paths_v2",
            return_value=SimpleNamespace(
                snapshot_path=out_snapshot,
                latest_path=latest_path,
                failure_path=failure_path,
            ),
        ),
    ):
        assert pos_v2.main(["--day_utc", INPUT_DAY, "--producer_git_sha", GIT_SHA, "--producer_repo", "constellation"]) == 0

    refreshed = json.loads(out_snapshot.read_text(encoding="utf-8"))
    assert refreshed["positions"]["items"] == []
    assert refreshed["reason_codes"] == ["NO_SUBMISSIONS_EMPTY_POSITIONS_V2"]


def test_positions_snapshot_v2_does_not_carry_forward_zero_qty_previous_open_positions(tmp_path: Path) -> None:
    exec_root = tmp_path / "truth" / "execution_evidence_v1" / "submissions"
    current_exec_day = exec_root / DAY
    current_exec_day.mkdir(parents=True, exist_ok=True)
    out_snapshot = tmp_path / "truth" / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json"
    latest_path = tmp_path / "truth" / "positions_v1" / "latest_pointer.v2.json"
    failure_path = tmp_path / "truth" / "positions_v1" / "failures" / DAY / "failure.json"
    prev_v4 = tmp_path / "truth" / "positions_v1" / "snapshots" / INPUT_DAY / "positions_snapshot.v4.json"
    _seed_previous_day_v4(prev_v4, qty=0)

    with (
        mock.patch.object(pos_v2, "exec_day_paths_v1", return_value=SimpleNamespace(submissions_day_dir=current_exec_day)),
        mock.patch.object(
            pos_v2,
            "day_paths_v2",
            return_value=SimpleNamespace(
                snapshot_path=out_snapshot,
                latest_path=latest_path,
                failure_path=failure_path,
            ),
        ),
        mock.patch.object(pos_v2, "day_paths_v4", return_value=SimpleNamespace(snapshot_path=prev_v4)),
    ):
        assert pos_v2.main(["--day_utc", DAY, "--producer_git_sha", GIT_SHA, "--producer_repo", "constellation"]) == 0

    carried = json.loads(out_snapshot.read_text(encoding="utf-8"))
    assert carried["reason_codes"] == ["NO_SUBMISSIONS_EMPTY_POSITIONS_V2"]
    assert carried["positions"]["items"] == []


def test_positions_snapshot_v2_carries_forward_previous_open_positions(tmp_path: Path) -> None:
    exec_root = tmp_path / "truth" / "execution_evidence_v1" / "submissions"
    current_exec_day = exec_root / DAY
    current_exec_day.mkdir(parents=True, exist_ok=True)
    out_snapshot = tmp_path / "truth" / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json"
    latest_path = tmp_path / "truth" / "positions_v1" / "latest_pointer.v2.json"
    failure_path = tmp_path / "truth" / "positions_v1" / "failures" / DAY / "failure.json"
    prev_v4 = tmp_path / "truth" / "positions_v1" / "snapshots" / INPUT_DAY / "positions_snapshot.v4.json"
    _seed_previous_day_v4(prev_v4)

    with (
        mock.patch.object(pos_v2, "exec_day_paths_v1", return_value=SimpleNamespace(submissions_day_dir=current_exec_day)),
        mock.patch.object(
            pos_v2,
            "day_paths_v2",
            return_value=SimpleNamespace(
                snapshot_path=out_snapshot,
                latest_path=latest_path,
                failure_path=failure_path,
            ),
        ),
        mock.patch.object(pos_v2, "day_paths_v4", return_value=SimpleNamespace(snapshot_path=prev_v4)),
    ):
        assert pos_v2.main(["--day_utc", DAY, "--producer_git_sha", GIT_SHA, "--producer_repo", "constellation"]) == 0

    carried = json.loads(out_snapshot.read_text(encoding="utf-8"))
    assert carried["reason_codes"] == ["CARRY_FORWARD_OPEN_POSITIONS_V2"]
    assert carried["positions"]["items"]
    assert carried["positions"]["items"][0]["instrument"]["kind"] == "EQUITY"
    assert carried["positions"]["items"][0]["instrument"]["underlying"] == "SPY"


def test_positions_snapshot_v2_carries_forward_from_canonical_when_sleeve_has_no_v4(tmp_path: Path) -> None:
    exec_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER" / "execution_evidence_v1" / "submissions"
    current_exec_day = exec_root / DAY
    current_exec_day.mkdir(parents=True, exist_ok=True)
    out_snapshot = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER" / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json"
    latest_path = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER" / "positions_v1" / "latest_pointer.v2.json"
    failure_path = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER" / "positions_v1" / "failures" / DAY / "failure.json"
    canonical_prev_v4 = tmp_path / "truth" / "positions_v1" / "snapshots" / INPUT_DAY / "positions_snapshot.v4.json"
    _seed_previous_day_v4(canonical_prev_v4)

    with (
        mock.patch.object(pos_v2, "exec_day_paths_v1", return_value=SimpleNamespace(submissions_day_dir=current_exec_day)),
        mock.patch.object(
            pos_v2,
            "day_paths_v2",
            return_value=SimpleNamespace(
                snapshot_path=out_snapshot,
                latest_path=latest_path,
                failure_path=failure_path,
            ),
        ),
        mock.patch.object(pos_v2, "day_paths_v4", return_value=SimpleNamespace(snapshot_path=tmp_path / "missing" / "positions_snapshot.v4.json")),
        mock.patch.object(pos_v2, "resolve_truth_root", return_value=tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"),
        mock.patch.object(pos_v2, "resolve_canonical_truth_root", return_value=tmp_path / "truth"),
    ):
        assert pos_v2.main(["--day_utc", DAY, "--producer_git_sha", GIT_SHA, "--producer_repo", "constellation"]) == 0

    carried = json.loads(out_snapshot.read_text(encoding="utf-8"))
    assert carried["reason_codes"] == ["CARRY_FORWARD_OPEN_POSITIONS_V2"]
    assert carried["positions"]["items"]
    assert carried["positions"]["items"][0]["instrument"]["underlying"] == "SPY"


def test_session_admission_positions_seed_repairs_empty_target_snapshot(tmp_path: Path) -> None:
    source_path = tmp_path / "truth" / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json"
    target_path = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER" / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v2.json"
    _seed_empty_positions_v2(target_path, DAY)
    _write_json(
        source_path,
        {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V2",
            "schema_version": 2,
            "produced_utc": f"{DAY}T00:00:00Z",
            "day_utc": DAY,
            "producer": {
                "repo": "constellation",
                "git_sha": GIT_SHA,
                "module": "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py",
            },
            "status": "OK",
            "reason_codes": ["CARRY_FORWARD_OPEN_POSITIONS_V2"],
            "input_manifest": [
                {
                    "type": "other",
                    "path": "/tmp/input",
                    "sha256": "0" * 64,
                    "day_utc": INPUT_DAY,
                    "producer": "positions_v1_snapshot_v4",
                }
            ],
            "positions": {
                "currency": "USD",
                "asof_utc": f"{DAY}T00:00:00Z",
                "items": [
                    {
                        "position_id": BINDING_HASH,
                        "engine_id": "unknown",
                        "instrument": {
                            "kind": "EQUITY",
                            "underlying": "SPY",
                            "expiry": None,
                            "strike": None,
                            "right": None,
                        },
                        "qty": 0,
                        "avg_cost_cents": 0,
                        "market_exposure_type": "UNDEFINED_RISK",
                        "max_loss_cents": None,
                        "opened_day_utc": INPUT_DAY,
                        "status": "OPEN",
                    }
                ],
                "notes": ["carry"],
            },
        },
    )

    result = session_admission._mirror_canonical_file(
        source_path=source_path,
        target_path=target_path,
        artifact_id="sleeve_positions_snapshot_v2",
    )

    assert result["status"] == "BACKFILL_REPAIRED"
    assert target_path.read_bytes() == source_path.read_bytes()


def test_positions_snapshot_v4_repairs_stale_zero_fill_positions_to_empty(tmp_path: Path) -> None:
    from constellation_2.phaseF.positions.run import run_positions_snapshot_day_v4 as pos_v4

    exec_root = tmp_path / "truth" / "execution_evidence_v1" / "submissions"
    out_snapshot = tmp_path / "truth" / "positions_v1" / "snapshots" / INPUT_DAY / "positions_snapshot.v4.json"
    failure_path = tmp_path / "truth" / "positions_v1" / "failures" / INPUT_DAY / "failure_v4.json"
    _seed_execution_submission(exec_root, INPUT_DAY)
    _seed_previous_day_v4(out_snapshot, qty=0)

    with (
        mock.patch.object(pos_v4, "exec_day_paths_v1", return_value=SimpleNamespace(submissions_day_dir=exec_root / INPUT_DAY)),
        mock.patch.object(
            pos_v4,
            "day_paths_v4",
            return_value=SimpleNamespace(
                snapshot_path=out_snapshot,
                failure_path=failure_path,
            ),
        ),
    ):
        assert pos_v4.main(["--day_utc", INPUT_DAY, "--producer_git_sha", GIT_SHA, "--producer_repo", "constellation"]) == 0

    refreshed = json.loads(out_snapshot.read_text(encoding="utf-8"))
    assert refreshed["positions"]["items"] == []


def test_positions_snapshot_v4_preserves_engine_identity_from_submission_plan(tmp_path: Path) -> None:
    from constellation_2.phaseF.positions.run import run_positions_snapshot_day_v4 as pos_v4

    exec_root = tmp_path / "truth" / "execution_evidence_v1" / "submissions"
    out_snapshot = tmp_path / "truth" / "positions_v1" / "snapshots" / INPUT_DAY / "positions_snapshot.v4.json"
    failure_path = tmp_path / "truth" / "positions_v1" / "failures" / INPUT_DAY / "failure_v4.json"
    _seed_execution_submission(exec_root, INPUT_DAY, filled_qty=1, avg_price="679.91")

    with (
        mock.patch.object(pos_v4, "exec_day_paths_v1", return_value=SimpleNamespace(submissions_day_dir=exec_root / INPUT_DAY)),
        mock.patch.object(
            pos_v4,
            "day_paths_v4",
            return_value=SimpleNamespace(
                snapshot_path=out_snapshot,
                failure_path=failure_path,
            ),
        ),
    ):
        assert pos_v4.main(["--day_utc", INPUT_DAY, "--producer_git_sha", GIT_SHA, "--producer_repo", "constellation"]) == 0

    refreshed = json.loads(out_snapshot.read_text(encoding="utf-8"))
    assert refreshed["positions"]["items"]
    assert refreshed["positions"]["items"][0]["engine_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert refreshed["positions"]["items"][0]["instrument"]["kind"] == "EQUITY"
    assert refreshed["positions"]["items"][0]["instrument"]["symbol"] == "SPY"


def test_positions_snapshot_v2_reads_v2_payload_from_v1_filename(tmp_path: Path) -> None:
    exec_root = tmp_path / "truth" / "execution_evidence_v1" / "submissions"
    out_snapshot = tmp_path / "truth" / "positions_v1" / "snapshots" / INPUT_DAY / "positions_snapshot.v2.json"
    latest_path = tmp_path / "truth" / "positions_v1" / "latest_pointer.v2.json"
    failure_path = tmp_path / "truth" / "positions_v1" / "failures" / INPUT_DAY / "failure.json"
    _seed_execution_submission(exec_root, INPUT_DAY, filled_qty=1, avg_price="679.91")
    legacy_v1_path = exec_root / INPUT_DAY / SUBMISSION_ID / "equity_order_plan.v1.json"
    legacy_v1_path.write_text((exec_root / INPUT_DAY / SUBMISSION_ID / "equity_order_plan.v2.json").read_text(encoding="utf-8"), encoding="utf-8")
    (exec_root / INPUT_DAY / SUBMISSION_ID / "equity_order_plan.v2.json").unlink()

    with (
        mock.patch.object(pos_v2, "exec_day_paths_v1", return_value=SimpleNamespace(submissions_day_dir=exec_root / INPUT_DAY)),
        mock.patch.object(
            pos_v2,
            "day_paths_v2",
            return_value=SimpleNamespace(
                snapshot_path=out_snapshot,
                latest_path=latest_path,
                failure_path=failure_path,
            ),
        ),
    ):
        assert pos_v2.main(["--day_utc", INPUT_DAY, "--producer_git_sha", GIT_SHA, "--producer_repo", "constellation"]) == 0

    refreshed = json.loads(out_snapshot.read_text(encoding="utf-8"))
    assert refreshed["positions"]["items"][0]["engine_id"] == "C2_TREND_EQ_PRIMARY_V1"
    assert refreshed["positions"]["items"][0]["instrument"]["underlying"] == "SPY"
