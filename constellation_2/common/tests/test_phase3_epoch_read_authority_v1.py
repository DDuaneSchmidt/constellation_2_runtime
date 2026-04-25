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

from constellation_2.common.allocation_authority_v1 import read_allocation_authority_v1  # noqa: E402
from constellation_2.common.canonical_sleeve_identity_v1 import (  # noqa: E402
    canonical_sleeve_id_from_engine_id,
    canonical_sleeve_id_from_execution_scope,
)
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


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _run_main(module, *argv: str, monkeypatch: pytest.MonkeyPatch) -> int:
    monkeypatch.setattr(sys, "argv", [module.__name__.split(".")[-1], *argv])
    return module.main()


def test_all_governed_engines_map_to_canonical_sleeves_and_primary_needs_engine() -> None:
    engine_registry = _load_json(REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json")
    engine_ids = sorted(str(item["engine_id"]) for item in engine_registry["engines"])
    resolved = {engine_id: canonical_sleeve_id_from_engine_id(engine_id, repo_root=REPO_ROOT) for engine_id in engine_ids}
    assert resolved["C2_INTENT_SIMULATOR_V1"] == "C2_TREND_EQ_PRIMARY"
    assert resolved["C2_TREND_EQ_PRIMARY_V1"] == "C2_TREND_EQ_PRIMARY"
    with pytest.raises(ValueError, match="ENGINE_ID_MISSING"):
        canonical_sleeve_id_from_execution_scope("PRIMARY", "", repo_root=REPO_ROOT)


def test_allocation_authority_prefers_epoch_over_legacy_allocation_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    phase2._seed_capital_risk_envelope(truth_root, day_utc=DAY)
    phase2._seed_capital_allocation(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)
    assert _run_main(portfolio_snapshot_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(allocation_epoch_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    allocation_path = truth_root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json"
    allocation_obj = _load_json(allocation_path)
    allocation_obj["per_sleeve"][0]["allowed_capital_at_risk_cents"] = 1
    allocation_obj["per_sleeve"][0]["headroom_cents"] = 1
    _write_json(allocation_path, allocation_obj)
    authority = read_allocation_authority_v1(day_utc=DAY, truth_root=truth_root, repo_root=REPO_ROOT)
    assert authority["authority_source"] == "SLEEVE_ALLOCATION_EPOCH_V1"
    assert authority["obj"]["per_sleeve"][0]["assigned_budget_cents"] == 100000


def test_authorization_ledger_includes_epoch_id_on_every_event(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    phase2._seed_capital_risk_envelope(truth_root, day_utc=DAY)
    phase2._seed_capital_allocation(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)
    phase2._seed_authorization_artifact(truth_root, day_utc=DAY, intent_hash=INTENT_HASH, status="AUTHORIZED")
    phase2._seed_equity_order_plan(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)
    assert _run_main(portfolio_snapshot_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(allocation_epoch_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    ledger_path = truth_root / "engine_activity_v1" / "intent_authorization_ledger_v1" / DAY / f"{INTENT_HASH}.intent_authorization_ledger.v1.jsonl"
    lines = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert all(str(line.get("allocation_epoch_id") or "").strip() for line in lines)


def test_authorization_ledger_detects_budget_overrun_without_changing_decision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    phase2._seed_capital_risk_envelope(truth_root, day_utc=DAY)
    phase2._seed_capital_allocation(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)
    allocation_path = truth_root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json"
    allocation_obj = _load_json(allocation_path)
    allocation_obj["per_sleeve"][0]["allowed_capital_at_risk_cents"] = 0
    allocation_obj["per_sleeve"][0]["used_capital_at_risk_cents"] = 0
    allocation_obj["per_sleeve"][0]["headroom_cents"] = 0
    _write_json(allocation_path, allocation_obj)
    phase2._seed_authorization_artifact(truth_root, day_utc=DAY, intent_hash=INTENT_HASH, status="AUTHORIZED")
    phase2._seed_equity_order_plan(truth_root, day_utc=DAY, intent_hash=INTENT_HASH)
    assert _run_main(portfolio_snapshot_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(allocation_epoch_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(authorization_ledger_writer, "--day", DAY, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    ledger_path = truth_root / "engine_activity_v1" / "intent_authorization_ledger_v1" / DAY / f"{INTENT_HASH}.intent_authorization_ledger.v1.jsonl"
    lines = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert [line["state"] for line in lines] == ["REQUESTED", "AUTHORIZED", "RESERVED"]
    assert "BUDGET_VIOLATION_DETECTED" in lines[-1]["reason_codes"]
    assert lines[-1]["reserved_quantity"] == lines[-1]["authorized_quantity"] == 1


def test_epoch_ledger_reconciliation_emits_expected_mismatches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    truth_root = tmp_path / "truth"
    phase2._seed_capital_risk_envelope(truth_root, day_utc=DAY_EXPOSURE)
    phase2._seed_capital_allocation(truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH_EXPOSURE)
    allocation_path = truth_root / "allocation_v1" / "capital_authority_allocation_v1" / DAY_EXPOSURE / "capital_authority_allocation.v1.json"
    allocation_obj = _load_json(allocation_path)
    allocation_obj["per_sleeve"][0]["allowed_capital_at_risk_cents"] = 0
    allocation_obj["per_sleeve"][0]["used_capital_at_risk_cents"] = 0
    allocation_obj["per_sleeve"][0]["headroom_cents"] = 0
    _write_json(allocation_path, allocation_obj)
    phase2._seed_authorization_artifact(truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH_EXPOSURE, status="AUTHORIZED")
    phase2._seed_equity_order_plan(truth_root, day_utc=DAY_EXPOSURE, intent_hash=INTENT_HASH_EXPOSURE)
    positions_path = phase2._seed_positions_snapshot(truth_root, day_utc=DAY_EXPOSURE, binding_hash=BINDING_HASH)
    positions_obj = _load_json(positions_path)
    positions_obj["positions"]["items"][0]["qty"] = 2
    _write_json(positions_path, positions_obj)
    phase2._seed_fill_ledger(truth_root, day_utc=DAY_EXPOSURE, binding_hash=BINDING_HASH, intent_hash=INTENT_HASH_EXPOSURE)
    assert _run_main(portfolio_snapshot_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(allocation_epoch_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(authorization_ledger_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(exposure_ledger_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    assert _run_main(epoch_reconciliation_writer, "--day", DAY_EXPOSURE, "--truth_root", str(truth_root), monkeypatch=monkeypatch) == 0
    report_path = (
        truth_root / "reports" / "epoch_ledger_reconciliation_v1" / DAY_EXPOSURE / "epoch_ledger_reconciliation.v1.json"
    )
    report = _load_json(report_path)
    assert report["status"] == "FAIL"
    assert "RESERVED_EXCEEDS_ALLOCATION" in report["reason_codes"]
    assert "EXPOSURE_EXCEEDS_LEDGER" in report["reason_codes"]
