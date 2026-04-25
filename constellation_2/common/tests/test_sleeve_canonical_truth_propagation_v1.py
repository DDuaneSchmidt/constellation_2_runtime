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
from constellation_2.common.paper_session_path_alignment_v1 import resolve_canonical_lifecycle_closure_path  # noqa: E402
import ops.tools.run_canonical_lifecycle_closure_day_v1 as closure_tool  # noqa: E402
import ops.tools.run_execution_reconciliation_day_v1 as exec_recon  # noqa: E402
import ops.tools.run_fill_ledger_day_v1 as fill_ledger  # noqa: E402


DAY = "2026-04-13"
SID = "f6b7a3dbbabf3f3bdd52a338ebc2b3cf72677be25e4f9de0ac7c39a26c44fe17"
GIT_SHA = "a" * 40


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _seed_sleeve_submission(sleeve_truth: Path) -> None:
    subdir = sleeve_truth / "execution_evidence_v1" / "submissions" / DAY / SID
    _write_json(
        subdir / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "submission_id": SID,
            "submitted_at_utc": f"{DAY}T00:00:00Z",
            "binding_hash": "ed8bf1e2d6479e429ff0696515bec81cb855d96b373acaac01c772cd53a1f7cc",
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "status": "PRESUBMITTED",
            "broker_ids": {"order_id": 75, "perm_id": 1870974300},
            "error": None,
            "canonical_json_hash": "a96e1385ea46105f996b2d8dea8f04ab55652f7a6f1db902c9ee4294b10edbc9",
        },
    )
    _write_json(
        subdir / "execution_event_record.v1.json",
        {
            "schema_id": "execution_event_record",
            "schema_version": "v1",
            "created_at_utc": f"{DAY}T00:00:00Z",
            "event_time_utc": f"{DAY}T00:00:00Z",
            "binding_hash": "ed8bf1e2d6479e429ff0696515bec81cb855d96b373acaac01c772cd53a1f7cc",
            "broker_submission_hash": "a96e1385ea46105f996b2d8dea8f04ab55652f7a6f1db902c9ee4294b10edbc9",
            "broker_order_id": "75",
            "perm_id": "1870974300",
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
    _write_json(
        subdir / "equity_order_plan.v1.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v2",
            "plan_id": "c2_trend_eq_spy_2026-04-13_v1",
            "created_at_utc": f"{DAY}T00:00:00Z",
            "intent_hash": "79c8b0b7a54a4460d77a8cafd1f19e8a40f1a6db4cdbe8daa144943270f64a18",
            "intent_sha256": "f51cf9219c7a1bca3b1a6f21724f10517213cdbb27cd307ccf1ff84c0e236d14",
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
            "submission_id": SID,
            "intent_id": "c2_trend_eq_spy_2026-04-13_v1",
            "intent_hash": "79c8b0b7a54a4460d77a8cafd1f19e8a40f1a6db4cdbe8daa144943270f64a18",
            "canonical_json_hash": "ed8bf1e2d6479e429ff0696515bec81cb855d96b373acaac01c772cd53a1f7cc",
        },
    )
    _write_json(
        subdir / "mapping_ledger_record.v2.json",
        {
            "schema_id": "mapping_ledger_record",
            "schema_version": "v2",
            "submission_id": SID,
            "canonical_json_hash": "4f29528ca4c53bacd9d0c3de4dbba4a8e2b429b9d6869b3dee7e882171d69f55",
        },
    )


def test_sleeve_submission_propagates_to_canonical_reconciliation_and_exposes_explicit_pending_portfolio_state(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _seed_sleeve_submission(sleeve_truth)
    (canonical_truth / "fill_ledger_v1" / DAY).mkdir(parents=True, exist_ok=True)

    rc = exec_truth.main(
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
    )
    assert rc == 0

    canonical_submission = canonical_truth / "execution_evidence_v1" / "submissions" / DAY / SID
    assert (canonical_submission / "broker_submission_record.v2.json").exists()
    assert (canonical_submission / "execution_event_record.v1.json").exists()
    assert (canonical_submission / "binding_record.v2.json").exists()
    normalized_plan = json.loads((canonical_submission / "equity_order_plan.v1.json").read_text(encoding="utf-8"))
    assert normalized_plan["schema_version"] == "v1"
    assert "engine_id" not in normalized_plan
    propagated_v2_plan = json.loads((canonical_submission / "equity_order_plan.v2.json").read_text(encoding="utf-8"))
    assert propagated_v2_plan["schema_version"] == "v2"
    assert propagated_v2_plan["engine_id"] == "C2_TREND_EQ_PRIMARY_V1"

    with mock.patch.object(
        sys,
        "argv",
        [
            "run_fill_ledger_day_v1.py",
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth),
        ],
    ):
        rc = fill_ledger.main()
    assert rc == 0
    ledger_path = canonical_truth / "fill_ledger_v1" / DAY / f"{SID}.fill_ledger.v1.json"
    ledger_obj = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert ledger_obj["submission_id"] == SID

    with mock.patch.object(
        sys,
        "argv",
        [
            "run_execution_reconciliation_day_v1.py",
            "--day_utc",
            DAY,
            "--truth_root",
            str(canonical_truth),
        ],
    ):
        rc = exec_recon.main()
    assert rc == 0

    recon_path = canonical_truth / "reports" / "execution_reconciliation_v1" / DAY / "execution_reconciliation.v1.json"
    recon = json.loads(recon_path.read_text(encoding="utf-8"))
    assert recon["status"] == "PASS"
    assert "NO_SUBMISSIONS_FOUND" not in recon["reason_codes"]

    with mock.patch.object(closure_tool, "repo_git_sha_v1", return_value=GIT_SHA):
        rc = closure_tool.main(
            [
                "--day_utc",
                DAY,
                "--truth_root",
                str(canonical_truth),
                "--source_truth_root",
                str(sleeve_truth),
                "--submission_id",
                SID,
            ]
        )
    assert rc == 0

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
        rc = pos_v4.main(["--day_utc", DAY, "--producer_git_sha", GIT_SHA, "--producer_repo", "constellation"])
        assert rc == 0
        rc = pos_eff.main(["--day_utc", DAY, "--producer_git_sha", GIT_SHA, "--producer_repo", "constellation"])
        assert rc == 0

    pos_v4_path = canonical_truth / "positions_v1" / "snapshots" / DAY / "positions_snapshot.v4.json"
    pos_v4_obj = json.loads(pos_v4_path.read_text(encoding="utf-8"))
    assert pos_v4_obj["positions"]["items"] == []

    eff_path = canonical_truth / "positions_v1" / "effective_v1" / "days" / DAY / "positions_effective_pointer.v1.json"
    eff_obj = json.loads(eff_path.read_text(encoding="utf-8"))
    assert eff_obj["selection"]["selected_schema_version"] == 4
    assert eff_obj["pointers"]["snapshot_path"] == str(pos_v4_path)
    closure = json.loads(
        resolve_canonical_lifecycle_closure_path(
            truth_root=canonical_truth,
            day_utc=DAY,
            submission_id=SID,
        ).read_text(encoding="utf-8")
    )
    assert closure["canonical_lifecycle_status"] == "CANONICALIZED"
    assert closure["propagation_state"]["status"] == "PROPAGATION_PENDING"
    assert closure["propagation_state"]["blocker_chain"] == ["FILL_NOT_YET_OBSERVED"]
    assert closure["propagation_state"]["positions_snapshot_ref"]["exists"] is False


def test_propagation_rerun_is_idempotent_and_preserves_single_submission_dir(tmp_path: Path) -> None:
    canonical_truth = tmp_path / "truth"
    sleeve_truth = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    _seed_sleeve_submission(sleeve_truth)

    args = [
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
    assert exec_truth.main(args) == 0
    assert exec_truth.main(args) == 0

    sub_day = canonical_truth / "execution_evidence_v1" / "submissions" / DAY
    dirs = sorted([p.name for p in sub_day.iterdir() if p.is_dir() and not p.name.startswith("__")])
    assert dirs == [SID]

    latest_path = canonical_truth / "execution_evidence_v1" / "latest_pointer.v1.json"
    latest_obj = json.loads(latest_path.read_text(encoding="utf-8"))
    assert latest_obj["day_utc"] == DAY
    assert latest_obj["pointers"]["submissions_day_dir"] == str(sub_day)
