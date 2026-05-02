from __future__ import annotations

import json
from pathlib import Path

from ops.tools.run_risk_definition_contract_v1 import (
    build_risk_definition_contract_v1,
    load_valid_risk_definition_contract_v1,
    risk_contract_path_v1,
    validate_risk_definition_contract_v1,
)

DAY = "2026-05-01"
TREND_HASH = "a" * 64
VOL_HASH = "b" * 64
TREND_ID = "c2_trend_eq_spy_2026-05-01_v1"
VOL_ID = "c2_vol_income_iwm_2026-05-01_v1"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _seed_trend(root: Path, *, with_price: bool = True) -> None:
    _write(
        root / "intents_v1" / "snapshots" / DAY / f"{TREND_HASH}.exposure_intent.v1.json",
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": TREND_ID,
            "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "mode": "PAPER"},
            "exposure_type": "LONG_EQUITY",
            "underlying": {"symbol": "SPY", "currency": "USD"},
            "target_notional_pct": "0.01",
            "constraints": {"max_risk_pct": "0.01", "stop_loss_bps": 1000},
        },
    )
    if with_price:
        _write(
            root / "market_data_snapshot_v1" / "snapshots" / DAY / "SPY.market_data_snapshot.v1.json",
            {"schema_id": "C2_MARKET_DATA_SNAPSHOT_V1", "day_utc": DAY, "symbol": "SPY", "close": "500.00"},
        )


def _seed_vol(root: Path, *, defined: bool = False) -> None:
    _write(
        root / "intents_v1" / "snapshots" / DAY / f"{VOL_HASH}.exposure_intent.v1.json",
        {
            "schema_id": "exposure_intent",
            "schema_version": "v1",
            "intent_id": VOL_ID,
            "engine": {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "mode": "PAPER"},
            "exposure_type": "SHORT_VOL_DEFINED",
            "underlying": {"symbol": "IWM", "currency": "USD"},
            "target_notional_pct": "0.01",
            "constraints": {"max_risk_pct": "0.01"},
        },
    )
    if defined:
        phasec = root / "phaseC_preflight_v1" / DAY / "attempt_A0001" / VOL_HASH
        _write(
            phasec / "order_plan.v1.json",
            {
                "schema_id": "order_plan",
                "schema_version": "v1",
                "structure": "VERTICAL_SPREAD",
                "legs": [
                    {"action": "SELL", "right": "PUT", "strike": "190.00", "expiry_utc": "2026-05-15T00:00:00Z"},
                    {"action": "BUY", "right": "PUT", "strike": "185.00", "expiry_utc": "2026-05-15T00:00:00Z"},
                ],
                "risk_proof": {"defined_risk_proven": True, "contracts": 1, "max_loss_usd": "400.00"},
                "options_chain_ref": str(root / "options_chain_snapshot_v1" / DAY / "capture" / "options_chain_snapshot.v1.json"),
            },
        )


def test_trend_stop_loss_bps_creates_valid_stop_based_contract(tmp_path: Path) -> None:
    _seed_trend(tmp_path)
    payload = build_risk_definition_contract_v1(day_utc=DAY, truth_root=tmp_path, intent_hash=TREND_HASH)
    validate_risk_definition_contract_v1(payload)
    assert payload["validation_status"] == "PASS"
    assert payload["risk_type"] == "STOP_BASED"
    assert payload["reference_price"] == "500.00"
    assert payload["risk_per_unit"] == 5000
    assert payload["stop_loss_price"] == "450.00"


def test_missing_reference_price_blocks_stop_based_contract(tmp_path: Path) -> None:
    _seed_trend(tmp_path, with_price=False)
    payload = build_risk_definition_contract_v1(day_utc=DAY, truth_root=tmp_path, intent_hash=TREND_HASH)
    validate_risk_definition_contract_v1(payload)
    assert payload["validation_status"] == "FAIL"
    assert "RISK_CONTRACT_REFERENCE_PRICE_MISSING" in payload["blockers"]


def test_vol_missing_defined_risk_evidence_fails_closed(tmp_path: Path) -> None:
    _seed_vol(tmp_path)
    payload = build_risk_definition_contract_v1(day_utc=DAY, truth_root=tmp_path, intent_hash=VOL_HASH)
    validate_risk_definition_contract_v1(payload)
    assert payload["validation_status"] == "FAIL"
    assert "RISK_CONTRACT_DEFINED_RISK_ORDER_PLAN_MISSING" in payload["blockers"]


def test_valid_defined_risk_contract_can_pass_preconditions(tmp_path: Path) -> None:
    _seed_vol(tmp_path, defined=True)
    payload = build_risk_definition_contract_v1(day_utc=DAY, truth_root=tmp_path, intent_hash=VOL_HASH)
    validate_risk_definition_contract_v1(payload)
    assert payload["validation_status"] == "PASS"
    assert payload["risk_type"] == "DEFINED_RISK"
    assert payload["max_loss"] == 40000
    assert payload["quantity_basis"]["quantity"] == 1


def test_loader_rejects_missing_contract(tmp_path: Path) -> None:
    path, payload, blocker = load_valid_risk_definition_contract_v1(
        truth_root=tmp_path,
        day_utc=DAY,
        intent_hash=TREND_HASH,
        intent_id=TREND_ID,
    )
    assert path == risk_contract_path_v1(truth_root=tmp_path, day_utc=DAY, intent_hash=TREND_HASH)
    assert payload == {}
    assert blocker == "RISK_DEFINITION_CONTRACT_MISSING"
