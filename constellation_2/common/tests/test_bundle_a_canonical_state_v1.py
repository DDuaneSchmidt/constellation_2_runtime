from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseF.positions.run import run_positions_snapshot_day_v2 as positions_v2  # noqa: E402
from constellation_2.phaseF.positions.run import run_positions_snapshot_day_v5 as positions_v5  # noqa: E402
import ops.tools.run_broker_reconciliation_day_v2 as broker_recon  # noqa: E402
import ops.tools.run_execution_positions_snapshot_v5_bridge_v1 as positions_v5_bridge  # noqa: E402
from constellation_2.common.artifact_authority_v1 import get_artifact_mirror_contract_v1  # noqa: E402
import ops.tools.run_position_lifecycle_snapshot_v2 as lifecycle_v2  # noqa: E402


ACCOUNT_ID = "DUO847203"
GIT_SHA = "7d64db4a5e4d68af1d89a56edf64fb9024bb218a"


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _cash_snapshot_obj(day_utc: str, *, cash_total_cents: int) -> dict:
    return {
        "schema_id": "C2_CASH_LEDGER_SNAPSHOT_V1",
        "schema_version": 1,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "authority_basis": "operator_statement",
        "producer": {"repo": "constellation", "git_sha": GIT_SHA, "module": "test_bundle_a_canonical_state_v1.py"},
        "status": "OK",
        "reason_codes": [],
        "input_manifest": [
            {
                "type": "operator_statement",
                "path": f"/tmp/{day_utc}/operator_statement.v1.json",
                "sha256": "1" * 64,
                "day_utc": day_utc,
                "producer": "test",
            }
        ],
        "snapshot": {
            "observed_at_utc": f"{day_utc}T00:00:00Z",
            "currency": "USD",
            "cash_total_cents": cash_total_cents,
            "nlv_total_cents": cash_total_cents,
            "available_funds_cents": cash_total_cents,
            "excess_liquidity_cents": cash_total_cents,
            "account_id": ACCOUNT_ID,
            "notes": [],
        },
    }


def _broker_statement_obj(day_utc: str, *, cash_end: str, positions: list[dict]) -> dict:
    return {
        "schema_id": "C2_BROKER_STATEMENT_NORMALIZED_V1",
        "schema_version": "1.0.0",
        "produced_utc": f"{day_utc}T00:00:00Z",
        "day_utc": day_utc,
        "producer": "test_bundle_a_canonical_state_v1.py",
        "source": "OTHER",
        "source_file_sha256": "2" * 64,
        "account_id": ACCOUNT_ID,
        "currency": "USD",
        "cash_end": cash_end,
        "fees_total": "0",
        "positions": positions,
        "notes": [],
    }


def _seed_cash_snapshot(truth_root: Path, day_utc: str, *, cash_total_cents: int) -> None:
    _write_json(
        truth_root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json",
        _cash_snapshot_obj(day_utc, cash_total_cents=cash_total_cents),
    )


def _seed_broker_statement(truth_root: Path, day_utc: str, *, cash_end: str, positions: list[dict]) -> None:
    _write_json(
        truth_root / "execution_evidence_v1" / "broker_statement_normalized_v1" / day_utc / "broker_statement_normalized.v1.json",
        _broker_statement_obj(day_utc, cash_end=cash_end, positions=positions),
    )


def _seed_submission_fill(
    truth_root: Path,
    *,
    day_utc: str,
    submission_id: str,
    action: str,
    qty_shares: int,
    filled_qty: int,
    avg_fill_price_weighted: str,
    source_intent_id: str,
    intent_sha256: str,
) -> None:
    subdir = truth_root / "execution_evidence_v1" / "submissions" / day_utc / submission_id
    _write_json(
        subdir / "broker_submission_record.v2.json",
        {
            "schema_id": "broker_submission_record",
            "schema_version": "v2",
            "submission_id": submission_id,
            "submitted_at_utc": f"{day_utc}T00:00:00Z",
            "binding_hash": "3" * 64,
            "broker": {"name": "INTERACTIVE_BROKERS", "environment": "PAPER"},
            "status": "FILLED",
            "broker_ids": {"order_id": 123, "perm_id": 456},
            "error": None,
            "canonical_json_hash": "4" * 64,
        },
    )
    _write_json(
        subdir / "equity_order_plan.v2.json",
        {
            "schema_id": "equity_order_plan",
            "schema_version": "v2",
            "plan_id": f"{submission_id}_plan",
            "created_at_utc": f"{day_utc}T00:00:00Z",
            "intent_hash": "5" * 64,
            "structure": "EQUITY_SPOT",
            "symbol": "SPY",
            "currency": "USD",
            "action": action,
            "qty_shares": qty_shares,
            "order_terms": {"order_type": "LIMIT", "limit_price": avg_fill_price_weighted, "time_in_force": "DAY"},
            "risk_proof": None,
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "source_intent_id": source_intent_id,
            "intent_sha256": intent_sha256,
            "lineage_envelope_ref": {"path": "lineage_envelope.v1.json", "sha256": "6" * 64},
            "canonical_json_hash": "7" * 64,
        },
    )
    _write_json(
        subdir / "execution_event_record.v1.json",
        {
            "schema_id": "execution_event_record",
            "schema_version": "v1",
            "created_at_utc": f"{day_utc}T00:00:00Z",
            "event_time_utc": f"{day_utc}T15:30:00Z",
            "binding_hash": "3" * 64,
            "broker_submission_hash": "8" * 64,
            "broker_order_id": "123",
            "perm_id": "456",
            "status": "FILLED",
            "filled_qty": filled_qty,
            "avg_price": avg_fill_price_weighted,
            "raw_broker_status": "Filled",
            "raw_payload_digest": None,
            "sequence_num": None,
            "canonical_json_hash": "9" * 64,
            "upstream_hash": None,
        },
    )
    _write_json(
        truth_root / "fill_ledger_v1" / day_utc / f"{submission_id}.fill_ledger.v1.json",
        {
            "schema_id": "C2_FILL_LEDGER_V1",
            "schema_version": 1,
            "produced_utc": f"{day_utc}T15:30:00Z",
            "day_utc": day_utc,
            "producer": {"repo": "constellation", "git_sha": GIT_SHA[:7], "module": "test_bundle_a_canonical_state_v1.py"},
            "status": "OK",
            "reason_codes": [],
            "submission_id": submission_id,
            "binding_hash": "3" * 64,
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "source_intent_id": source_intent_id,
            "intent_sha256": intent_sha256,
            "order_qty": qty_shares,
            "filled_qty": filled_qty,
            "remaining_qty": max(qty_shares - filled_qty, 0),
            "avg_fill_price_weighted": avg_fill_price_weighted,
            "lifecycle_status": "FILLED",
            "event_hashes": ["a" * 64],
            "canonical_json_hash": "b" * 64,
        },
    )


def _run_positions_v5(day_utc: str, truth_root: Path) -> Path:
    rc = positions_v5.main(
        [
            "--day_utc",
            day_utc,
            "--producer_git_sha",
            GIT_SHA,
            "--producer_repo",
            "constellation",
            "--truth_root",
            str(truth_root),
            "--ib_account",
            ACCOUNT_ID,
        ]
    )
    assert rc == 0
    return truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json"


def _run_positions_v2_bridge(day_utc: str, truth_root: Path) -> Path:
    rc = positions_v2.main(
        [
            "--day_utc",
            day_utc,
            "--producer_git_sha",
            GIT_SHA,
            "--producer_repo",
            "constellation",
            "--truth_root",
            str(truth_root),
        ]
    )
    assert rc == 0
    return truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json"


def _run_lifecycle_v2(day_utc: str, truth_root: Path, monkeypatch: pytest.MonkeyPatch, *, expected_rc: int = 0) -> Path:
    monkeypatch.setattr(lifecycle_v2, "_git_sha", lambda: GIT_SHA)
    monkeypatch.setattr(sys, "argv", ["run_position_lifecycle_snapshot_v2.py", "--day_utc", day_utc, "--truth_root", str(truth_root)])
    rc = lifecycle_v2.main()
    assert rc == expected_rc
    return truth_root / "position_lifecycle_v2" / day_utc / "position_lifecycle_snapshot.v2.json"


def _run_broker_reconciliation(day_utc: str, truth_root: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(broker_recon, "TRUTH_ROOT", truth_root)
    monkeypatch.setattr(sys, "argv", ["run_broker_reconciliation_day_v2.py", "--day_utc", day_utc, "--ib_account", ACCOUNT_ID, "--mode", "WRITE"])
    rc = broker_recon.main()
    assert rc == 0
    return truth_root / "reports" / "broker_reconciliation_v2" / day_utc / "broker_reconciliation.v2.json"


def _spy_broker_row(*, qty: str, avg_cost: str, market_value: str) -> dict:
    return {"symbol": "SPY", "sec_type": "STK", "qty": qty, "avg_cost": avg_cost, "market_value": market_value, "currency": "USD"}


def test_imported_position_ingestion_shares_canonical_lifecycle_model(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-14"
    _seed_cash_snapshot(truth_root, day_utc, cash_total_cents=1_000_000)
    _seed_broker_statement(
        truth_root,
        day_utc,
        cash_end="10000.00",
        positions=[_spy_broker_row(qty="5", avg_cost="100.00", market_value="500.00")],
    )

    snapshot_path = _run_positions_v5(day_utc, truth_root)
    lifecycle_path = _run_lifecycle_v2(day_utc, truth_root, monkeypatch)

    snapshot = _read_json(snapshot_path)
    assert snapshot["reason_codes"] == ["BUNDLE_A_CANONICAL_STATE_V5", "BROKER_STATEMENT_PRESENT"]
    assert len(snapshot["items"]) == 1
    position = snapshot["items"][0]
    assert position["origin"] == "IMPORTED"
    assert position["qty"] == 5
    assert position["status"] == "OPEN"
    assert position["lifecycle_state"] == "MANAGING"
    assert position["lifecycle_reason_code"] == "IMPORTED_FROM_BROKER_STATEMENT"
    assert position["lots"][0]["source_kind"] == "BROKER_AGGREGATE"

    lifecycle = _read_json(lifecycle_path)
    assert len(lifecycle["items"]) == 1
    assert lifecycle["items"][0]["position_id"] == position["position_id"]
    assert lifecycle["items"][0]["lifecycle_state"] == "MANAGING"
    assert lifecycle["items"][0]["lifecycle_reason_code"] == "IMPORTED_FROM_BROKER_STATEMENT"


def test_position_lifecycle_v2_script_runs_from_current_repo_for_canonical_truth(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-18"
    _seed_cash_snapshot(truth_root, day_utc, cash_total_cents=1_000_000)
    _seed_broker_statement(
        truth_root,
        day_utc,
        cash_end="10000.00",
        positions=[_spy_broker_row(qty="5", avg_cost="100.00", market_value="500.00")],
    )

    _run_positions_v5(day_utc, truth_root)
    script_path = REPO_ROOT / "ops" / "tools" / "run_position_lifecycle_snapshot_v2.py"
    proc = subprocess.run(
        [sys.executable, str(script_path), "--day_utc", day_utc, "--truth_root", str(truth_root)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
    snapshot_path = truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json"
    snapshot = _read_json(snapshot_path)
    lifecycle_path = truth_root / "position_lifecycle_v2" / day_utc / "position_lifecycle_snapshot.v2.json"
    lifecycle = _read_json(lifecycle_path)
    assert lifecycle["status"] == "OK"
    assert lifecycle["items"][0]["position_id"] == snapshot["items"][0]["position_id"]


def test_execution_root_positions_v5_bridge_copies_canonical_snapshot_explicitly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    canonical_truth = tmp_path / "truth"
    execution_truth_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    day_utc = "2026-04-19"
    _seed_cash_snapshot(canonical_truth, day_utc, cash_total_cents=1_000_000)
    _seed_broker_statement(
        canonical_truth,
        day_utc,
        cash_end="10000.00",
        positions=[_spy_broker_row(qty="5", avg_cost="100.00", market_value="500.00")],
    )
    canonical_path = _run_positions_v5(day_utc, canonical_truth)
    (tmp_path / "truth_sleeves").mkdir(parents=True, exist_ok=True)
    execution_truth_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(positions_v5_bridge, "resolve_canonical_truth_root", lambda: canonical_truth.resolve())
    monkeypatch.setattr(positions_v5_bridge, "resolve_truth_sleeves_root", lambda: (tmp_path / "truth_sleeves").resolve())

    rc = positions_v5_bridge.main(
        [
            "--day_utc",
            day_utc,
            "--source_truth_root",
            str(canonical_truth),
            "--truth_root",
            str(execution_truth_root),
        ]
    )
    assert rc == 0
    mirror = get_artifact_mirror_contract_v1(REPO_ROOT, "positions_snapshot_v5", "execution_positions_snapshot_v5")
    assert mirror["authoritative_bridge"] == "ops/tools/run_execution_positions_snapshot_v5_bridge_v1.py"

    execution_path = execution_truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json"
    assert execution_path.exists()
    assert execution_path.read_bytes() == canonical_path.read_bytes()


def test_native_partial_and_full_close_flow_is_authoritative_and_reconciled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"

    day_open = "2026-04-15"
    _seed_cash_snapshot(truth_root, day_open, cash_total_cents=1_000_000)
    _seed_broker_statement(
        truth_root,
        day_open,
        cash_end="10000.00",
        positions=[_spy_broker_row(qty="2", avg_cost="100.00", market_value="200.00")],
    )
    _seed_submission_fill(
        truth_root,
        day_utc=day_open,
        submission_id="1" * 64,
        action="BUY",
        qty_shares=2,
        filled_qty=2,
        avg_fill_price_weighted="100.00",
        source_intent_id="intent-open-000001",
        intent_sha256="c" * 64,
    )
    _run_positions_v5(day_open, truth_root)

    day_partial = "2026-04-16"
    _seed_cash_snapshot(truth_root, day_partial, cash_total_cents=1_000_000)
    _seed_broker_statement(
        truth_root,
        day_partial,
        cash_end="10000.00",
        positions=[_spy_broker_row(qty="1", avg_cost="100.00", market_value="100.00")],
    )
    _seed_submission_fill(
        truth_root,
        day_utc=day_partial,
        submission_id="2" * 64,
        action="SELL",
        qty_shares=1,
        filled_qty=1,
        avg_fill_price_weighted="110.00",
        source_intent_id="intent-reduce-0001",
        intent_sha256="d" * 64,
    )
    partial_snapshot_path = _run_positions_v5(day_partial, truth_root)
    partial_lifecycle_path = _run_lifecycle_v2(day_partial, truth_root, monkeypatch)
    partial_recon_path = _run_broker_reconciliation(day_partial, truth_root, monkeypatch)

    partial_snapshot = _read_json(partial_snapshot_path)
    assert len(partial_snapshot["items"]) == 1
    partial_position = partial_snapshot["items"][0]
    assert partial_position["origin"] == "NATIVE"
    assert partial_position["qty"] == 1
    assert partial_position["last_transition_type"] == "PARTIAL_CLOSE"
    assert partial_position["lifecycle_state"] == "MANAGING"
    assert partial_position["lifecycle_reason_code"] == "NATIVE_FILL_PARTIAL_CLOSE"
    assert partial_position["status"] == "OPEN"
    assert partial_position["lots"][0]["remaining_qty_abs"] == 1
    assert partial_position["reconciliation"]["status"] == "MATCH"

    partial_lifecycle = _read_json(partial_lifecycle_path)
    assert partial_lifecycle["items"][0]["position_id"] == partial_position["position_id"]
    assert partial_lifecycle["items"][0]["lifecycle_state"] == "MANAGING"

    partial_recon = _read_json(partial_recon_path)
    assert partial_recon["status"] == "PASS"
    assert partial_recon["fail_closed"] is False
    assert partial_recon["internal_positions_path"] == f"positions_v1/snapshots/{day_partial}/positions_snapshot.v5.json"
    assert partial_recon["position_mismatches"] == []

    day_full = "2026-04-17"
    _seed_cash_snapshot(truth_root, day_full, cash_total_cents=1_000_000)
    _seed_broker_statement(truth_root, day_full, cash_end="10000.00", positions=[])
    _seed_submission_fill(
        truth_root,
        day_utc=day_full,
        submission_id="3" * 64,
        action="SELL",
        qty_shares=1,
        filled_qty=1,
        avg_fill_price_weighted="120.00",
        source_intent_id="intent-close-00001",
        intent_sha256="e" * 64,
    )
    full_snapshot_path = _run_positions_v5(day_full, truth_root)
    full_lifecycle_path = _run_lifecycle_v2(day_full, truth_root, monkeypatch)
    full_recon_path = _run_broker_reconciliation(day_full, truth_root, monkeypatch)

    full_snapshot = _read_json(full_snapshot_path)
    assert len(full_snapshot["items"]) == 1
    full_position = full_snapshot["items"][0]
    assert full_position["qty"] == 0
    assert full_position["last_transition_type"] == "FULL_CLOSE"
    assert full_position["lifecycle_state"] == "CLOSED"
    assert full_position["lifecycle_reason_code"] == "NATIVE_FILL_FULL_CLOSE"
    assert full_position["status"] == "CLOSED"
    assert full_position["lots"] == []

    full_lifecycle = _read_json(full_lifecycle_path)
    assert full_lifecycle["items"][0]["position_id"] == full_position["position_id"]
    assert full_lifecycle["items"][0]["lifecycle_state"] == "CLOSED"

    full_recon = _read_json(full_recon_path)
    assert full_recon["status"] == "PASS"
    assert full_recon["fail_closed"] is False
    assert full_recon["internal_positions_path"] == f"positions_v1/snapshots/{day_full}/positions_snapshot.v5.json"
    assert full_recon["position_mismatches"] == []


def test_native_add_transition_is_preserved_in_canonical_state_and_lifecycle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"

    day_open = "2026-04-15"
    _seed_cash_snapshot(truth_root, day_open, cash_total_cents=1_000_000)
    _seed_broker_statement(
        truth_root,
        day_open,
        cash_end="10000.00",
        positions=[_spy_broker_row(qty="1", avg_cost="100.00", market_value="100.00")],
    )
    _seed_submission_fill(
        truth_root,
        day_utc=day_open,
        submission_id="a" * 64,
        action="BUY",
        qty_shares=1,
        filled_qty=1,
        avg_fill_price_weighted="100.00",
        source_intent_id="intent-open-add-0001",
        intent_sha256="1" * 64,
    )
    _run_positions_v5(day_open, truth_root)

    day_add = "2026-04-16"
    _seed_cash_snapshot(truth_root, day_add, cash_total_cents=1_000_000)
    _seed_broker_statement(
        truth_root,
        day_add,
        cash_end="10000.00",
        positions=[_spy_broker_row(qty="2", avg_cost="105.00", market_value="210.00")],
    )
    _seed_submission_fill(
        truth_root,
        day_utc=day_add,
        submission_id="b" * 64,
        action="BUY",
        qty_shares=1,
        filled_qty=1,
        avg_fill_price_weighted="110.00",
        source_intent_id="intent-add-000001",
        intent_sha256="2" * 64,
    )

    snapshot_path = _run_positions_v5(day_add, truth_root)
    lifecycle_path = _run_lifecycle_v2(day_add, truth_root, monkeypatch)
    snapshot = _read_json(snapshot_path)
    position = snapshot["items"][0]
    assert position["qty"] == 2
    assert position["last_transition_type"] == "ADD"
    assert position["lifecycle_state"] == "MANAGING"
    assert position["lifecycle_reason_code"] == "NATIVE_FILL_APPLIED"
    assert len(position["lots"]) == 2

    lifecycle = _read_json(lifecycle_path)
    assert lifecycle["status"] == "OK"
    assert lifecycle["items"][0]["position_id"] == position["position_id"]
    assert lifecycle["items"][0]["lifecycle_state"] == "MANAGING"


def test_bundle_a_replay_is_deterministic_for_positions_and_lifecycle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    day_utc = "2026-04-18"
    truth_root = tmp_path / "truth"
    _seed_cash_snapshot(truth_root, day_utc, cash_total_cents=1_000_000)
    _seed_broker_statement(
        truth_root,
        day_utc,
        cash_end="10000.00",
        positions=[_spy_broker_row(qty="1", avg_cost="100.00", market_value="100.00")],
    )
    _seed_submission_fill(
        truth_root,
        day_utc=day_utc,
        submission_id="4" * 64,
        action="BUY",
        qty_shares=1,
        filled_qty=1,
        avg_fill_price_weighted="100.00",
        source_intent_id="intent-deterministic-01",
        intent_sha256="f" * 64,
    )

    first_pos = _run_positions_v5(day_utc, truth_root)
    first_life = _run_lifecycle_v2(day_utc, truth_root, monkeypatch)
    first_pos_bytes = first_pos.read_bytes()
    first_life_bytes = first_life.read_bytes()

    first_pos.unlink()
    first_life.unlink()

    second_pos = _run_positions_v5(day_utc, truth_root)
    second_life = _run_lifecycle_v2(day_utc, truth_root, monkeypatch)

    assert first_pos_bytes == second_pos.read_bytes()
    assert first_life_bytes == second_life.read_bytes()


def test_positions_v2_bridge_is_explicit_and_non_authoritative(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-19"
    _seed_cash_snapshot(truth_root, day_utc, cash_total_cents=1_000_000)
    _seed_broker_statement(
        truth_root,
        day_utc,
        cash_end="10000.00",
        positions=[_spy_broker_row(qty="3", avg_cost="100.00", market_value="300.00")],
    )

    _run_positions_v5(day_utc, truth_root)
    bridge_path = _run_positions_v2_bridge(day_utc, truth_root)

    bridge = _read_json(bridge_path)
    assert bridge["reason_codes"] == ["COMPAT_BRIDGE_FROM_POSITIONS_V5"]
    assert bridge["input_manifest"][0]["producer"] == "positions_snapshot_v5"
    assert bridge["positions"]["notes"] == ["COMPATIBILITY BRIDGE: derived only from canonical positions_snapshot.v5"]


def test_lifecycle_v2_fails_closed_on_invalid_state_and_missing_attribution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    day_utc = "2026-04-20"
    monkeypatch.setattr(lifecycle_v2, "validate_against_repo_schema_v1", lambda *args, **kwargs: None)
    snapshot_path = truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json"
    _write_json(
        snapshot_path,
        {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V5",
            "schema_version": 5,
            "day_utc": day_utc,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "producer": {"repo": "constellation", "git_sha": GIT_SHA, "module": "test_bundle_a_canonical_state_v1.py"},
            "status": "OK",
            "reason_codes": ["BUNDLE_A_CANONICAL_STATE_V5"],
            "input_manifest": [],
            "accounts": [{"account_id": ACCOUNT_ID, "currency": "USD", "cash_total_cents": 1_000_000, "broker_cash_cents": 1_000_000, "cash_source": "CASH_LEDGER_ONLY", "reason_codes": []}],
            "items": [
                {
                    "position_id": "broken-spy",
                    "account_id": ACCOUNT_ID,
                    "origin": "NATIVE",
                    "engine_id": "",
                    "source_intent_id": "",
                    "intent_sha256": "",
                    "instrument": {"kind": "EQUITY", "symbol": "SPY", "currency": "USD"},
                    "qty": 1,
                    "avg_cost_cents": 10_000,
                    "opened_day_utc": day_utc,
                    "last_transition_utc": f"{day_utc}T00:00:00Z",
                    "last_transition_type": "OPEN",
                    "lifecycle_state": "NOT_A_REAL_STATE",
                    "lifecycle_reason_code": "",
                    "status": "OPEN",
                    "lots": [],
                    "reconciliation": {"broker_position_present": True, "broker_qty": "1", "status": "MATCH", "reason_codes": []},
                }
            ],
            "reconciliation": {
                "broker_statement_present": True,
                "broker_statement_path": "/tmp/broker.json",
                "cash_status": "MATCH",
                "cash_delta_cents": 0,
                "positions_status": "MATCH",
                "reason_codes": [],
                "position_mismatches": [],
            },
            "canonical_json_hash": "0" * 64,
        },
    )

    lifecycle_path = _run_lifecycle_v2(day_utc, truth_root, monkeypatch, expected_rc=2)
    lifecycle = _read_json(lifecycle_path)
    assert lifecycle["status"] == "FAIL"
    assert lifecycle["items"] == []
    assert "LIFECYCLE_ENGINE_ID_MISSING:position_id=broken-spy" in lifecycle["reason_codes"]
    assert "LIFECYCLE_STATE_INVALID:position_id=broken-spy:value=NOT_A_REAL_STATE" in lifecycle["reason_codes"]
    assert "LIFECYCLE_SOURCE_INTENT_ID_MISSING:position_id=broken-spy" in lifecycle["reason_codes"]
    assert "LIFECYCLE_INTENT_SHA256_INVALID:position_id=broken-spy" in lifecycle["reason_codes"]
