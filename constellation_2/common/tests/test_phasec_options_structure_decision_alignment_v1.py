from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_phasec_options_identity_from_truth_day_v1 as phasec_options  # noqa: E402

DAY = "2026-04-29"
INTENT_ID = "c2_vol_income_spy_2026-04-29_v1"
INTENT_HASH = "a" * 64
SNAPSHOT_REL = Path("options_chain_snapshot_v1") / DAY / "capture_1" / "options_chain_snapshot.v1.json"
CERT_REL = Path("options_chain_snapshot_v1") / DAY / "capture_1" / "freshness_certificate.v1.json"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _roots(tmp_path: Path) -> tuple[Path, Path]:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    return truth_root, execution_root


def _intent() -> dict[str, object]:
    return {"intent_id": INTENT_ID, "underlying": {"symbol": "SPY"}, "exposure_type": "SHORT_VOL_DEFINED"}


def _snapshot(execution_root: Path) -> tuple[Path, Path, dict[str, object]]:
    snapshot_path = execution_root / SNAPSHOT_REL
    cert_path = execution_root / CERT_REL
    payload: dict[str, object] = {
        "schema_id": "options_chain_snapshot",
        "schema_version": "v1",
        "as_of_utc": "2026-04-29T15:51:09Z",
        "underlying": {"symbol": "SPY", "spot_price": "710.90", "spot_as_of_utc": "2026-04-29T15:51:09Z"},
        "contracts": [
            {
                "contract_key": "SPY|2026-04-30T00:00:00Z|PUT|692.00",
                "expiry_utc": "2026-04-30T00:00:00Z",
                "right": "PUT",
                "strike": "692.00",
                "bid": "0.19",
                "ask": "0.20",
                "open_interest": 0,
                "volume": 0,
                "ib": {"conId": 826250298, "exchange": "SMART", "localSymbol": "SPY   260430P00692000", "multiplier": 100},
            },
            {
                "contract_key": "SPY|2026-04-30T00:00:00Z|PUT|693.00",
                "expiry_utc": "2026-04-30T00:00:00Z",
                "right": "PUT",
                "strike": "693.00",
                "bid": "0.23",
                "ask": "0.24",
                "open_interest": 0,
                "volume": 0,
                "ib": {"conId": 826250332, "exchange": "SMART", "localSymbol": "SPY   260430P00693000", "multiplier": 100},
            },
        ],
        "canonical_json_hash": "b" * 64,
    }
    cert = {
        "schema_id": "freshness_certificate",
        "schema_version": "v1",
        "snapshot_as_of_utc": "2026-04-29T15:51:09Z",
        "valid_from_utc": "2026-04-29T15:51:09Z",
        "valid_until_utc": "2026-04-29T15:56:09Z",
        "snapshot_hash": "b" * 64,
        "canonical_json_hash": "c" * 64,
    }
    _write_json(snapshot_path, payload)
    _write_json(cert_path, cert)
    return snapshot_path, cert_path, payload


def _structure_supply(truth_root: Path, snapshot_path: Path, cert_path: Path, *, day: str = DAY, risk_defined: bool = True) -> Path:
    path = truth_root / "reports" / "structure_decision_supply_v1" / day / "structure_decision_supply.v1.json"
    payload = {
        "schema_id": "structure_decision_supply",
        "schema_version": "structure_decision_supply.v1",
        "day_utc": day,
        "environment": "PAPER",
        "status": "PASS",
        "canonical_blocker": "",
        "market_open_data": {
            "status": "PASS",
            "snapshot_path": str(snapshot_path.resolve()),
            "freshness_certificate_path": str(cert_path.resolve()),
        },
        "structure_decisions": [
            {
                "intent_id": INTENT_ID,
                "intent_hash": INTENT_HASH,
                "selected_structure": "VERTICAL_SPREAD",
                "risk_defined": risk_defined,
                "max_loss_known": risk_defined,
                "option_structure": {
                    "selected_structure": "VERTICAL_SPREAD",
                    "strategy_direction": "CREDIT",
                    "right": "PUT",
                    "legs": [
                        {
                            "action": "SELL",
                            "expiry_utc": "2026-04-30T00:00:00Z",
                            "right": "PUT",
                            "strike": "693.00",
                            "ratio": 1,
                            "ib_conId": 826250332,
                            "bid": "0.23",
                            "ask": "0.24",
                        },
                        {
                            "action": "BUY",
                            "expiry_utc": "2026-04-30T00:00:00Z",
                            "right": "PUT",
                            "strike": "692.00",
                            "ratio": 1,
                            "ib_conId": 826250298,
                            "bid": "0.19",
                            "ask": "0.20",
                        },
                    ],
                },
            }
        ],
    }
    _write_json(path, payload)
    return path


def _options_intent(path: Path) -> None:
    _write_json(
        path,
        {
            "schema_id": "options_intent",
            "schema_version": "v2",
            "canonical_json_hash": "d" * 64,
            "intent_id": INTENT_ID,
            "engine": {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "suite": "C2_OPTIONS_7", "mode": "PAPER"},
            "strategy": {"structure": "VERTICAL_SPREAD", "right": "PUT", "direction": "CREDIT"},
            "selection_policy": {
                "expiry_policy": {"mode": "DTE_WINDOW", "target_dte_min": 1, "target_dte_max": 7},
                "width_policy": {"width_points": "5.00"},
                "liquidity_policy": {"min_open_interest": 0, "min_volume": 0, "max_bid_ask_spread": "0.10"},
                "pricing_policy": {"limit_offset": "0.02", "tick_rounding": "ROUND_DOWN"},
            },
        },
    )


def test_phasec_consumes_structure_decision_selected_width_when_present(tmp_path: Path) -> None:
    truth_root, execution_root = _roots(tmp_path)
    snapshot_path, cert_path, snapshot = _snapshot(execution_root)
    _structure_supply(truth_root, snapshot_path, cert_path)
    supply_path, decision = phasec_options._load_structure_decision_for_intent(
        truth_root=execution_root,
        day_utc=DAY,
        intent_obj=_intent(),
        intent_hash=INTENT_HASH,
        snap_path=snapshot_path,
        cert_path=cert_path,
        snap_obj=snapshot,
    )
    options_intent_path = tmp_path / "options_intent.v2.json"
    _options_intent(options_intent_path)
    phasec_options._apply_structure_decision_to_options_intent(options_intent_path=options_intent_path, structure_decision=decision)
    adapted = json.loads(options_intent_path.read_text(encoding="utf-8"))
    assert supply_path == truth_root / "reports" / "structure_decision_supply_v1" / DAY / "structure_decision_supply.v1.json"
    assert adapted["selection_policy"]["width_policy"]["width_points"] == "1"
    assert adapted["selection_policy"]["expiry_policy"]["target_dte_min"] == 1
    assert adapted["selection_policy"]["expiry_policy"]["target_dte_max"] == 1
    assert adapted["selection_policy"]["governed_legs"] == [
        {"action": "SELL", "expiry_utc": "2026-04-30T00:00:00Z", "right": "PUT", "strike": "693.00", "ib_conId": 826250332},
        {"action": "BUY", "expiry_utc": "2026-04-30T00:00:00Z", "right": "PUT", "strike": "692.00", "ib_conId": 826250298},
    ]


def test_phasec_does_not_accept_mapper_output_that_remaps_away_from_governed_legs(tmp_path: Path) -> None:
    truth_root, execution_root = _roots(tmp_path)
    snapshot_path, cert_path, snapshot = _snapshot(execution_root)
    _structure_supply(truth_root, snapshot_path, cert_path)
    _, decision = phasec_options._load_structure_decision_for_intent(
        truth_root=execution_root,
        day_utc=DAY,
        intent_obj=_intent(),
        intent_hash=INTENT_HASH,
        snap_path=snapshot_path,
        cert_path=cert_path,
        snap_obj=snapshot,
    )
    order_plan = tmp_path / "order_plan.v1.json"
    _write_json(
        order_plan,
        {
            "legs": [
                {"action": "SELL", "expiry_utc": "2026-04-30T00:00:00Z", "right": "PUT", "strike": "693.00", "ib_conId": 826250332},
                {"action": "BUY", "expiry_utc": "2026-04-30T00:00:00Z", "right": "PUT", "strike": "688.00", "ib_conId": 1},
            ],
            "risk_proof": {"defined_risk_proven": True},
        },
    )
    with pytest.raises(phasec_options.OptionsIdentityError, match="OPTIONS_MAPPED_LEGS_DO_NOT_MATCH_STRUCTURE_DECISION"):
        phasec_options._validate_mapped_plan_matches_structure_decision(order_plan_path=order_plan, structure_decision=decision)


def test_missing_structure_decision_supply_fails_closed(tmp_path: Path) -> None:
    _, execution_root = _roots(tmp_path)
    snapshot_path, cert_path, snapshot = _snapshot(execution_root)
    with pytest.raises(phasec_options.OptionsIdentityError, match="STRUCTURE_DECISION_SUPPLY_MISSING"):
        phasec_options._load_structure_decision_for_intent(
            truth_root=execution_root,
            day_utc=DAY,
            intent_obj=_intent(),
            intent_hash=INTENT_HASH,
            snap_path=snapshot_path,
            cert_path=cert_path,
            snap_obj=snapshot,
        )


def test_wrong_day_structure_decision_supply_is_rejected(tmp_path: Path) -> None:
    truth_root, execution_root = _roots(tmp_path)
    snapshot_path, cert_path, snapshot = _snapshot(execution_root)
    _structure_supply(truth_root, snapshot_path, cert_path, day=DAY)
    payload_path = truth_root / "reports" / "structure_decision_supply_v1" / DAY / "structure_decision_supply.v1.json"
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["day_utc"] = "2026-04-28"
    _write_json(payload_path, payload)
    with pytest.raises(phasec_options.OptionsIdentityError, match="STRUCTURE_DECISION_SUPPLY_WRONG_DAY"):
        phasec_options._load_structure_decision_for_intent(
            truth_root=execution_root,
            day_utc=DAY,
            intent_obj=_intent(),
            intent_hash=INTENT_HASH,
            snap_path=snapshot_path,
            cert_path=cert_path,
            snap_obj=snapshot,
        )


def test_selected_legs_must_exist_in_accepted_snapshot(tmp_path: Path) -> None:
    truth_root, execution_root = _roots(tmp_path)
    snapshot_path, cert_path, snapshot = _snapshot(execution_root)
    _structure_supply(truth_root, snapshot_path, cert_path)
    payload_path = truth_root / "reports" / "structure_decision_supply_v1" / DAY / "structure_decision_supply.v1.json"
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload["structure_decisions"][0]["option_structure"]["legs"][0]["strike"] = "694.00"
    _write_json(payload_path, payload)
    with pytest.raises(phasec_options.OptionsIdentityError, match="STRUCTURE_DECISION_LEG_NOT_IN_ACCEPTED_SNAPSHOT"):
        phasec_options._load_structure_decision_for_intent(
            truth_root=execution_root,
            day_utc=DAY,
            intent_obj=_intent(),
            intent_hash=INTENT_HASH,
            snap_path=snapshot_path,
            cert_path=cert_path,
            snap_obj=snapshot,
        )


def test_defined_risk_proof_remains_required(tmp_path: Path) -> None:
    truth_root, execution_root = _roots(tmp_path)
    snapshot_path, cert_path, snapshot = _snapshot(execution_root)
    _structure_supply(truth_root, snapshot_path, cert_path, risk_defined=False)
    with pytest.raises(phasec_options.OptionsIdentityError, match="STRUCTURE_DECISION_DEFINED_RISK_NOT_PROVEN"):
        phasec_options._load_structure_decision_for_intent(
            truth_root=execution_root,
            day_utc=DAY,
            intent_obj=_intent(),
            intent_hash=INTENT_HASH,
            snap_path=snapshot_path,
            cert_path=cert_path,
            snap_obj=snapshot,
        )


def test_legacy_width_policy_is_not_used_when_governed_structure_is_missing(tmp_path: Path) -> None:
    _, execution_root = _roots(tmp_path)
    snapshot_path, cert_path, snapshot = _snapshot(execution_root)
    with pytest.raises(phasec_options.OptionsIdentityError, match="STRUCTURE_DECISION_SUPPLY_MISSING"):
        phasec_options._load_structure_decision_for_intent(
            truth_root=execution_root,
            day_utc=DAY,
            intent_obj=_intent(),
            intent_hash=INTENT_HASH,
            snap_path=snapshot_path,
            cert_path=cert_path,
            snap_obj=snapshot,
        )
