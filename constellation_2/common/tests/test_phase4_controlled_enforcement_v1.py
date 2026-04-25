from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


REPO_ROOT = _repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.intent_risk_cents_v1 import load_intent_risk_context_v1  # noqa: E402
import constellation_2.common.tests.test_phase2_immutable_record_adapters_v1 as phase2  # noqa: E402
import ops.tools.run_portfolio_governance_snapshot_day_v1 as portfolio_snapshot_writer  # noqa: E402
import ops.tools.run_sleeve_allocation_epoch_day_v1 as allocation_epoch_writer  # noqa: E402
import ops.tools.run_intent_authorization_ledger_day_v1 as authorization_ledger_writer  # noqa: E402
import ops.tools.run_exposure_ledger_day_v1 as exposure_ledger_writer  # noqa: E402
import ops.tools.run_epoch_ledger_reconciliation_day_v1 as epoch_reconciliation_writer  # noqa: E402


DAY = "2026-04-14"
DAY_EXPOSURE = "2026-04-13"
INTENT_HASH = phase2.INTENT_HASH
INTENT_HASH_EXPOSURE = phase2.INTENT_HASH_EXPOSURE
BINDING_HASH = phase2.BINDING_HASH


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _run_main(module, *argv: str, monkeypatch: pytest.MonkeyPatch) -> int:
    monkeypatch.setattr(sys, "argv", [module.__name__.split(".")[-1], *argv])
    return module.main()


def _seed_base_budget_context(truth_root: Path, *, day_utc: str, intent_hash: str) -> None:
    phase2._seed_capital_risk_envelope(truth_root, day_utc=day_utc)
    phase2._seed_capital_allocation(truth_root, day_utc=day_utc, intent_hash=intent_hash)
    phase2._seed_authorization_artifact(truth_root, day_utc=day_utc, intent_hash=intent_hash, status="AUTHORIZED")
    phase2._seed_equity_order_plan(truth_root, day_utc=day_utc, intent_hash=intent_hash)
    assert (truth_root / "phaseC_preflight_v1" / day_utc / "attempt_A0001" / intent_hash / "equity_order_plan.v2.json").exists()


def test_risk_conversion_is_deterministic(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    phase2._seed_equity_order_plan(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)
    risk_ctx = load_intent_risk_context_v1(day_utc=DAY, intent_hash=INTENT_HASH, truth_root=truth_root, repo_root=REPO_ROOT)
    assert risk_ctx["risk_cents"] == 10000
    assert risk_ctx["requested_quantity"] == 1
    assert risk_ctx["risk_model"] == "EQUITY_LIMIT_NOTIONAL"


def test_shadow_mode_detects_violation_without_blocking(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    _seed_base_budget_context(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)
    allocation_path = truth_root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json"
    allocation_obj = _load_json(allocation_path)
    allocation_obj["per_sleeve"][0]["headroom_cents"] = 0
    phase2._write_json(allocation_path, allocation_obj)
    assert _run_main(portfolio_snapshot_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(allocation_epoch_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    monkeypatch.delenv("ENFORCE_BUDGET", raising=False)
    monkeypatch.delenv("ENFORCEMENT_MODE", raising=False)
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    ledger_path = truth_root / "engine_activity_v1" / "intent_authorization_ledger_v1" / DAY / f"{INTENT_HASH}.intent_authorization_ledger.v1.jsonl"
    validation_path = truth_root / "reports" / "intent_budget_validation_v1" / DAY / f"{INTENT_HASH}.intent_budget_validation.v1.json"
    lines = _load_jsonl(ledger_path)
    validation = _load_json(validation_path)
    assert [line["state"] for line in lines] == ["REQUESTED", "AUTHORIZED", "RESERVED"]
    assert "BUDGET_VIOLATION_DETECTED" in lines[-1]["reason_codes"]
    assert "WOULD_BLOCK_UNDER_ENFORCEMENT" in lines[-1]["reason_codes"]
    assert validation["would_block_under_enforcement"] is True
    assert validation["decision_shadow"] == "ALLOW"
    assert validation["enforcement_mode_effective"] == "SHADOW"
    assert validation["enforcement_decision"] == "ALLOW"
    assert validation["risk_cents"] == 10000


def test_enforcement_mode_soft_moves_ledger_decision_to_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    _seed_base_budget_context(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)
    allocation_path = truth_root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json"
    allocation_obj = _load_json(allocation_path)
    allocation_obj["per_sleeve"][0]["headroom_cents"] = 0
    phase2._write_json(allocation_path, allocation_obj)
    assert _run_main(portfolio_snapshot_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(allocation_epoch_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    monkeypatch.setenv("ENFORCE_BUDGET", "1")
    monkeypatch.setenv("ENFORCEMENT_MODE", "SOFT")
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    ledger_path = truth_root / "engine_activity_v1" / "intent_authorization_ledger_v1" / DAY / f"{INTENT_HASH}.intent_authorization_ledger.v1.jsonl"
    validation_path = truth_root / "reports" / "intent_budget_validation_v1" / DAY / f"{INTENT_HASH}.intent_budget_validation.v1.json"
    lines = _load_jsonl(ledger_path)
    validation = _load_json(validation_path)
    assert [line["state"] for line in lines] == ["REQUESTED", "REJECTED"]
    assert validation["decision_shadow"] == "WOULD_REJECT"
    assert validation["enforce_budget"] is True
    assert validation["enforcement_mode_effective"] == "SOFT"
    assert validation["enforcement_decision"] == "REJECT"
    assert validation["final_decision"] == "REJECT"
    assert "WOULD_REJECT_UNDER_ENFORCEMENT" in validation["reason_codes"]


def test_exposure_writer_marks_no_exposure_when_positions_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True, exist_ok=True)
    assert _run_main(exposure_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    build_status = _load_json(
        truth_root / "reports" / "exposure_ledger_build_v1" / DAY / "exposure_ledger_build.v1.json"
    )
    assert build_status["status"] == "NO_EXPOSURE"
    assert "NO_EXPOSURE" in build_status["reason_codes"]


def test_reconciliation_flags_hard_violations_with_risk_units(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    _seed_base_budget_context(truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH_EXPOSURE)
    assert _run_main(portfolio_snapshot_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(allocation_epoch_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(authorization_ledger_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    positions_path = phase2._seed_positions_snapshot(truth_root, day_utc=DAY_EXPOSURE, binding_hash=BINDING_HASH)
    positions_obj = _load_json(positions_path)
    positions_obj["positions"]["items"][0]["qty"] = 2
    phase2._write_json(positions_path, positions_obj)
    phase2._seed_fill_ledger(truth_root, day_utc=DAY_EXPOSURE, binding_hash=BINDING_HASH, intent_hash=INTENT_HASH_EXPOSURE)
    assert _run_main(exposure_ledger_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(epoch_reconciliation_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    report = _load_json(
        truth_root / "reports" / "epoch_ledger_reconciliation_v1" / DAY_EXPOSURE / "epoch_ledger_reconciliation.v1.json"
    )
    assert report["status"] == "HARD_VIOLATION"
    assert report["enforcement_recommendation"] == "BLOCK"
    assert "LIFECYCLE_INCONSISTENCY" in report["reason_codes"]
    assert "HARD_VIOLATION" in report["reason_codes"]
