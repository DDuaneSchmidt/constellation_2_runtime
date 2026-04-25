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

import constellation_2.common.tests.test_phase2_immutable_record_adapters_v1 as phase2  # noqa: E402
import ops.tools.run_portfolio_governance_snapshot_day_v1 as portfolio_snapshot_writer  # noqa: E402
import ops.tools.run_sleeve_allocation_epoch_day_v1 as allocation_epoch_writer  # noqa: E402
import ops.tools.run_intent_authorization_ledger_day_v1 as authorization_ledger_writer  # noqa: E402
import ops.tools.run_exposure_ledger_day_v1 as exposure_ledger_writer  # noqa: E402
import ops.tools.run_epoch_ledger_reconciliation_day_v1 as epoch_reconciliation_writer  # noqa: E402


DAY = phase2.DAY_EXPOSURE
INTENT_HASH = phase2.INTENT_HASH_EXPOSURE
BINDING_HASH = phase2.BINDING_HASH
SUBMISSION_ID = phase2.SUBMISSION_ID


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _run_main(module, *argv: str, monkeypatch: pytest.MonkeyPatch) -> int:
    monkeypatch.setattr(sys, "argv", [module.__name__.split(".")[-1], *argv])
    return module.main()


def _materialize_epoch(truth_root: Path, *, monkeypatch: pytest.MonkeyPatch) -> None:
    assert _run_main(portfolio_snapshot_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(allocation_epoch_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0


def _seed_base_context(truth_root: Path) -> None:
    phase2._seed_capital_risk_envelope(truth_root, day_utc=DAY)
    phase2._seed_capital_allocation(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)
    phase2._seed_authorization_artifact(truth_root, day_utc=DAY, intent_hash=INTENT_HASH, status="AUTHORIZED")
    phase2._seed_equity_order_plan(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)


def _set_authorized_quantity(truth_root: Path, *, qty: int) -> None:
    auth_path = truth_root / "engine_activity_v1" / "authorization_v1" / DAY / f"{INTENT_HASH}.authorization.v1.json"
    auth_obj = _load_json(auth_path)
    auth_obj["authorization"]["authorized_quantity"] = qty
    phase2._write_json(auth_path, auth_obj)
    plan_path = truth_root / "phaseC_preflight_v1" / DAY / "attempt_A0001" / INTENT_HASH / "equity_order_plan.v2.json"
    plan_obj = _load_json(plan_path)
    plan_obj["qty_shares"] = qty
    plan_obj["order_terms"]["limit_price"] = "100.00"
    phase2._write_json(plan_path, plan_obj)


def _seed_fill(truth_root: Path, *, order_qty: int, filled_qty: int, remaining_qty: int, lifecycle_status: str) -> None:
    path = truth_root / "fill_ledger_v1" / DAY / f"{SUBMISSION_ID}.fill_ledger.v1.json"
    phase2._write_json(
        path,
        {
            "schema_id": "C2_FILL_LEDGER_V1",
            "schema_version": 1,
            "produced_utc": f"{DAY}T12:34:56Z",
            "day_utc": DAY,
            "producer": {"repo": "constellation", "git_sha": "7d64db4", "module": "test"},
            "status": "OK",
            "reason_codes": [],
            "submission_id": SUBMISSION_ID,
            "binding_hash": BINDING_HASH,
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "source_intent_id": f"intent_{DAY}",
            "intent_sha256": INTENT_HASH,
            "order_qty": order_qty,
            "filled_qty": filled_qty,
            "remaining_qty": remaining_qty,
            "avg_fill_price_weighted": "100.00",
            "lifecycle_status": lifecycle_status,
            "event_hashes": [phase2._sha(f"fill:{lifecycle_status}:{filled_qty}:{remaining_qty}")],
            "canonical_json_hash": phase2._sha(f"fill:{lifecycle_status}:{filled_qty}:{remaining_qty}:canonical"),
        },
    )


def _ledger_path(truth_root: Path) -> Path:
    return truth_root / "engine_activity_v1" / "intent_authorization_ledger_v1" / DAY / f"{INTENT_HASH}.intent_authorization_ledger.v1.jsonl"


def _validation_path(truth_root: Path) -> Path:
    return truth_root / "reports" / "intent_budget_validation_v1" / DAY / f"{INTENT_HASH}.intent_budget_validation.v1.json"


def test_reserved_to_committed_transition(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    _seed_base_context(truth_root)
    _materialize_epoch(truth_root, monkeypatch=monkeypatch)
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    _seed_fill(truth_root, order_qty=1, filled_qty=1, remaining_qty=0, lifecycle_status="FILLED")
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    events = _load_jsonl(_ledger_path(truth_root))
    assert [event["state"] for event in events][-1] == "COMMITTED"
    assert events[-1]["committed_quantity"] == 1
    assert events[-1]["remaining_reserved_quantity"] == 0


def test_partial_fill_keeps_remaining_reservation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    _seed_base_context(truth_root)
    _set_authorized_quantity(truth_root, qty=2)
    _materialize_epoch(truth_root, monkeypatch=monkeypatch)
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    _seed_fill(truth_root, order_qty=2, filled_qty=1, remaining_qty=1, lifecycle_status="PARTIALLY_FILLED")
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    events = _load_jsonl(_ledger_path(truth_root))
    assert [event["state"] for event in events][-1] == "COMMITTED"
    assert events[-1]["committed_quantity"] == 1
    assert events[-1]["remaining_reserved_quantity"] == 1
    assert "RELEASED" not in [event["state"] for event in events]


def test_release_frees_budget_on_terminal_cancel(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    _seed_base_context(truth_root)
    _set_authorized_quantity(truth_root, qty=2)
    _materialize_epoch(truth_root, monkeypatch=monkeypatch)
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    _seed_fill(truth_root, order_qty=2, filled_qty=1, remaining_qty=1, lifecycle_status="CANCELLED")
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    events = _load_jsonl(_ledger_path(truth_root))
    assert [event["state"] for event in events][-1] == "RELEASED"
    assert events[-1]["committed_quantity"] == 1
    assert events[-1]["released_quantity"] == 1
    assert events[-1]["remaining_reserved_quantity"] == 0


def test_exposure_matches_committed_risk(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    _seed_base_context(truth_root)
    _materialize_epoch(truth_root, monkeypatch=monkeypatch)
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    _seed_fill(truth_root, order_qty=1, filled_qty=1, remaining_qty=0, lifecycle_status="FILLED")
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    positions_path = phase2._seed_positions_snapshot(truth_root, day_utc=DAY, binding_hash=BINDING_HASH)
    positions_obj = _load_json(positions_path)
    positions_obj["positions"]["items"][0]["qty"] = 1
    phase2._write_json(positions_path, positions_obj)
    assert _run_main(exposure_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(epoch_reconciliation_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    report = _load_json(truth_root / "reports" / "epoch_ledger_reconciliation_v1" / DAY / "epoch_ledger_reconciliation.v1.json")
    assert "LIFECYCLE_INCONSISTENCY" not in report["reason_codes"]


def test_lifecycle_invariants_hold_after_release(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    _seed_base_context(truth_root)
    _set_authorized_quantity(truth_root, qty=2)
    _materialize_epoch(truth_root, monkeypatch=monkeypatch)
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    _seed_fill(truth_root, order_qty=2, filled_qty=1, remaining_qty=1, lifecycle_status="CANCELLED")
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    validation = _load_json(_validation_path(truth_root))
    assert validation["committed_quantity"] == 1
    assert validation["released_quantity"] == 1
    assert validation["remaining_reserved_quantity"] == 0
    assert _run_main(epoch_reconciliation_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    report = _load_json(truth_root / "reports" / "epoch_ledger_reconciliation_v1" / DAY / "epoch_ledger_reconciliation.v1.json")
    assert "LIFECYCLE_INCONSISTENCY" not in report["reason_codes"]
