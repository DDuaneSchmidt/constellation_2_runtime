from __future__ import annotations

import json
import hashlib
from pathlib import Path

import pytest

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1

import ops.tools.run_phasec_options_identity_from_truth_day_v1 as phasec_options_module


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _contract(*, strike: str, bid: str, ask: str, conid: int) -> dict:
    return {
        "contract_key": f"SPY|2026-04-27T00:00:00Z|PUT|{strike}",
        "expiry_utc": "2026-04-27T00:00:00Z",
        "strike": strike,
        "right": "PUT",
        "bid": bid,
        "ask": ask,
        "open_interest": 0,
        "volume": 0,
        "ib": {
            "conId": conid,
            "localSymbol": f"SPY   260427P{strike.replace('.', '').rjust(8, '0')}",
            "tradingClass": "SPY",
            "exchange": "SMART",
            "currency": "USD",
            "multiplier": 100,
        },
    }


def _identity_write_fixture(tmp_path: Path) -> dict:
    day = "2026-04-24"
    intent_hash = "a" * 64
    out_day_dir = (
        tmp_path
        / "truth_sleeves"
        / "PRIMARY"
        / "PAPER"
        / "phaseC_preflight_v1"
        / day
        / "attempt_A0001"
    ).resolve()
    final_identity_dir = (out_day_dir / intent_hash).resolve()
    final_identity_dir.mkdir(parents=True, exist_ok=True)

    intent_path = (tmp_path / "intent.exposure_intent.v1.json").resolve()
    intent_obj = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": "test_short_vol_identity_v1",
        "day_utc": day,
        "environment": "PAPER",
        "underlying": {"symbol": "SPY", "currency": "USD"},
        "exposure_type": "SHORT_VOL_DEFINED",
    }
    _write_json(intent_path, intent_obj)

    order_plan = {
        "schema_id": "order_plan",
        "schema_version": "v1",
        "plan_id": "plan-identity-test-v1",
        "created_at_utc": f"{day}T20:00:00Z",
        "intent_hash": "b" * 64,
        "structure": "VERTICAL_SPREAD",
        "underlying": {"symbol": "SPY", "currency": "USD"},
        "legs": [
            {
                "action": "SELL",
                "ratio": 1,
                "right": "PUT",
                "expiry_utc": "2026-04-27T00:00:00Z",
                "strike": "712.00",
                "ib_conId": 873301879,
                "ib_localSymbol": "SPY   260427P00712000",
            },
            {
                "action": "BUY",
                "ratio": 1,
                "right": "PUT",
                "expiry_utc": "2026-04-27T00:00:00Z",
                "strike": "707.00",
                "ib_conId": 873301825,
                "ib_localSymbol": "SPY   260427P00707000",
            },
        ],
        "order_terms": {
            "order_type": "LIMIT",
            "limit_price": "1.12",
            "time_in_force": "DAY",
            "is_credit": True,
            "tick_rounding": "ROUND_DOWN",
        },
        "exit_policy_ref": {"policy_id": "exit-policy-test-v1"},
        "risk_proof": {
            "defined_risk_proven": True,
            "max_loss_usd": "388.00",
            "width_points": "5.00",
            "multiplier": 100,
            "contracts": 1,
        },
    }
    plan_hash = canonical_hash_for_c2_artifact_v1(order_plan)
    mapping = {
        "schema_id": "mapping_ledger_record",
        "schema_version": "v1",
        "record_id": "mapping-record-test-v1",
        "created_at_utc": f"{day}T20:00:01Z",
        "intent_hash": "b" * 64,
        "chain_snapshot_hash": "c" * 64,
        "freshness_cert_hash": "d" * 64,
        "plan_hash": plan_hash,
        "selection_trace": {
            "expiry_choice": {
                "policy": "TEST",
                "selected_expiry_utc": "2026-04-27T00:00:00Z",
                "candidates_considered": 1,
            },
            "strike_choice": {
                "width_points": "5.00",
                "short_leg_contract_key": "SPY|2026-04-27T00:00:00Z|PUT|712.00",
                "long_leg_contract_key": "SPY|2026-04-27T00:00:00Z|PUT|707.00",
            },
            "liquidity_filter": {
                "min_open_interest": 0,
                "min_volume": 0,
                "max_bid_ask_spread": "0.10",
            },
            "tie_breakers": ["TEST_TIE_BREAKER"],
        },
        "canonical_json_hash": "e" * 64,
    }
    mapping_hash = canonical_hash_for_c2_artifact_v1(mapping)
    binding = {
        "schema_id": "binding_record",
        "schema_version": "v1",
        "binding_id": "binding-record-test-v1",
        "created_at_utc": f"{day}T20:00:02Z",
        "plan_hash": plan_hash,
        "mapping_ledger_hash": mapping_hash,
        "freshness_cert_hash": "d" * 64,
        "broker_payload_digest": {
            "digest_sha256": "f" * 64,
            "format": "IB_BAG_ORDER_V1",
            "notes": "Bound vertical spread for unit test",
        },
        "preflight": {
            "validated_schema": True,
            "validated_invariants": True,
            "validated_freshness": True,
            "defined_risk_proven": True,
            "exit_policy_present": True,
        },
    }

    order_plan_path = (final_identity_dir / "order_plan.v1.json").resolve()
    mapping_path = (final_identity_dir / "mapping_ledger_record.v1.json").resolve()
    binding_path = (final_identity_dir / "binding_record.v1.json").resolve()
    decision_path = (final_identity_dir / "submit_preflight_decision.v1.json").resolve()
    options_intent_path = (final_identity_dir / "options_intent.v2.json").resolve()
    adapter_record_path = (final_identity_dir / "exposure_to_options_adapter_record.v1.json").resolve()

    _write_json(order_plan_path, order_plan)
    _write_json(mapping_path, mapping)
    _write_json(binding_path, binding)
    _write_json(decision_path, {"decision": "ALLOW"})
    _write_json(options_intent_path, {"intent_id": intent_obj["intent_id"]})
    _write_json(adapter_record_path, {"schema_id": "exposure_to_options_adapter_record", "schema_version": "v1"})

    return {
        "day": day,
        "intent_hash": intent_hash,
        "intent_path": intent_path,
        "intent_obj": intent_obj,
        "out_day_dir": out_day_dir,
        "final_identity_dir": final_identity_dir,
        "order_plan_path": order_plan_path,
        "mapping_path": mapping_path,
        "binding_path": binding_path,
        "decision_path": decision_path,
        "options_intent_path": options_intent_path,
        "adapter_record_path": adapter_record_path,
    }


def test_phasec_options_identity_proceeds_past_mapper_for_runtime_like_short_vol(tmp_path: Path) -> None:
    day = "2026-04-24"
    truth_root = (tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER").resolve()
    out_day_dir = (truth_root / "phaseC_preflight_v1" / day / "attempt_A0001").resolve()
    intent_path = (tmp_path / "intent.exposure_intent.v1.json").resolve()

    chain = {
        "schema_id": "options_chain_snapshot",
        "schema_version": "v1",
        "as_of_utc": "2026-04-24T21:06:56Z",
        "underlying": {"symbol": "SPY", "spot_price": "713.95", "spot_as_of_utc": "2026-04-24T21:06:56Z"},
        "contracts": [
            _contract(strike="700.00", bid="0.25", ask="0.26", conid=872979981),
            _contract(strike="703.00", bid="0.45", ask="0.46", conid=873301777),
            _contract(strike="705.00", bid="0.66", ask="0.67", conid=872979996),
            _contract(strike="709.00", bid="1.28", ask="1.30", conid=873301850),
            _contract(strike="715.00", bid="3.29", ask="3.39", conid=872980041),
        ],
        "provenance": {
            "source": "TEST_FIXTURE",
            "capture_method": "UNIT_TEST",
            "capture_host": "local",
            "capture_run_id": "phasec_options_identity_regression",
        },
        "canonical_json_hash": None,
    }
    chain_hash = canonical_hash_for_c2_artifact_v1(chain)
    chain["canonical_json_hash"] = chain_hash
    cert = {
        "schema_id": "freshness_certificate",
        "schema_version": "v1",
        "issued_at_utc": "2026-04-24T21:07:00Z",
        "valid_from_utc": "2026-04-24T21:06:56Z",
        "valid_until_utc": "2026-04-24T21:11:56Z",
        "snapshot_hash": chain_hash,
        "snapshot_as_of_utc": "2026-04-24T21:06:56Z",
        "source": "TEST_FIXTURE",
        "capture_method": "UNIT_TEST",
        "policy": {"max_age_seconds": 300, "clock_skew_tolerance_seconds": 5},
        "canonical_json_hash": None,
    }
    cert["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(cert)
    exposure_intent = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": "c2_vol_income_spy_2026-04-24_v1",
        "created_at_utc": "2026-04-24T00:00:00Z",
        "engine": {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "suite": "C2_HYBRID_V1", "mode": "PAPER"},
        "underlying": {"symbol": "SPY", "currency": "USD"},
        "exposure_type": "SHORT_VOL_DEFINED",
        "option": {"structure": "PUT", "direction": "SELL"},
        "target_notional_pct": "0.01",
        "expected_holding_days": 7,
        "risk_class": "VOL_INCOME_DEFINED",
        "constraints": {"max_risk_pct": "0.01"},
        "canonical_json_hash": None,
    }

    snap_dir = truth_root / "options_chain_snapshot_v1" / day / "capture_01"
    _write_json(snap_dir / "options_chain_snapshot.v1.json", chain)
    _write_json(snap_dir / "freshness_certificate.v1.json", cert)
    _write_json(intent_path, exposure_intent)

    status, output_path = phasec_options_module._materialize(
        truth_root=truth_root,
        day_utc=day,
        eval_time_utc="2026-04-24T21:07:33Z",
        intent_path=intent_path,
        out_day_dir=out_day_dir,
    )

    assert status == "RELEASED"
    identity_dir = Path(output_path).resolve()
    assert (identity_dir / "order_plan.v1.json").exists()
    assert (identity_dir / "mapping_ledger_record.v1.json").exists()
    assert (identity_dir / "binding_record.v1.json").exists()
    assert (identity_dir / "submit_preflight_decision.v1.json").exists()
    assert (identity_dir / "execution_identity_record.v1.json").exists()
    order_plan = json.loads((identity_dir / "order_plan.v1.json").read_text(encoding="utf-8"))
    assert order_plan["legs"][0]["action"] == "SELL"
    assert order_plan["legs"][0]["strike"] == "705.00"
    assert order_plan["legs"][1]["action"] == "BUY"
    assert order_plan["legs"][1]["strike"] == "700.00"
    execution_identity = json.loads((identity_dir / "execution_identity_record.v1.json").read_text(encoding="utf-8"))
    assert execution_identity["schema_id"] == "execution_identity_record"
    assert execution_identity["schema_version"] == "v1"
    assert execution_identity["day_utc"] == day
    assert execution_identity["environment"] == "PAPER"
    assert execution_identity["intent_hash"] == hashlib.sha256(intent_path.read_bytes()).hexdigest()
    ref_types = {row["ref_type"] for row in execution_identity["source_refs"]}
    assert "order_plan_ref" in ref_types
    assert "binding_record_ref" in ref_types
    assert "mapping_ledger_record_ref" in ref_types


def test_options_identity_writer_missing_order_plan_fails_closed(tmp_path: Path) -> None:
    fx = _identity_write_fixture(tmp_path)
    fx["order_plan_path"].unlink()
    with pytest.raises(phasec_options_module.OptionsIdentityError, match="ORDER_PLAN_MISSING"):
        phasec_options_module._write_execution_identity_record(
            day_utc=fx["day"],
            eval_time_utc=f"{fx['day']}T21:10:00Z",
            intent_hash=fx["intent_hash"],
            intent_path=fx["intent_path"],
            intent_obj=fx["intent_obj"],
            out_day_dir=fx["out_day_dir"],
            final_identity_dir=fx["final_identity_dir"],
            order_plan_path=fx["order_plan_path"],
            mapping_path=fx["mapping_path"],
            binding_path=fx["binding_path"],
            decision_path=fx["decision_path"],
            options_intent_path=fx["options_intent_path"],
            adapter_record_path=fx["adapter_record_path"],
        )


def test_options_identity_writer_missing_binding_fails_closed(tmp_path: Path) -> None:
    fx = _identity_write_fixture(tmp_path)
    fx["binding_path"].unlink()
    with pytest.raises(phasec_options_module.OptionsIdentityError, match="BINDING_RECORD_MISSING"):
        phasec_options_module._write_execution_identity_record(
            day_utc=fx["day"],
            eval_time_utc=f"{fx['day']}T21:10:01Z",
            intent_hash=fx["intent_hash"],
            intent_path=fx["intent_path"],
            intent_obj=fx["intent_obj"],
            out_day_dir=fx["out_day_dir"],
            final_identity_dir=fx["final_identity_dir"],
            order_plan_path=fx["order_plan_path"],
            mapping_path=fx["mapping_path"],
            binding_path=fx["binding_path"],
            decision_path=fx["decision_path"],
            options_intent_path=fx["options_intent_path"],
            adapter_record_path=fx["adapter_record_path"],
        )


def test_options_identity_writer_intent_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    fx = _identity_write_fixture(tmp_path)
    mapping = json.loads(fx["mapping_path"].read_text(encoding="utf-8"))
    mapping["intent_hash"] = "9" * 64
    _write_json(fx["mapping_path"], mapping)
    binding = json.loads(fx["binding_path"].read_text(encoding="utf-8"))
    binding["mapping_ledger_hash"] = canonical_hash_for_c2_artifact_v1(mapping)
    _write_json(fx["binding_path"], binding)
    with pytest.raises(phasec_options_module.OptionsIdentityError, match="OPTIONS_PLAN_MAPPING_INTENT_HASH_MISMATCH"):
        phasec_options_module._write_execution_identity_record(
            day_utc=fx["day"],
            eval_time_utc=f"{fx['day']}T21:10:02Z",
            intent_hash=fx["intent_hash"],
            intent_path=fx["intent_path"],
            intent_obj=fx["intent_obj"],
            out_day_dir=fx["out_day_dir"],
            final_identity_dir=fx["final_identity_dir"],
            order_plan_path=fx["order_plan_path"],
            mapping_path=fx["mapping_path"],
            binding_path=fx["binding_path"],
            decision_path=fx["decision_path"],
            options_intent_path=fx["options_intent_path"],
            adapter_record_path=fx["adapter_record_path"],
        )
