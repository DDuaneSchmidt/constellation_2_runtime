from __future__ import annotations

import json
from pathlib import Path

import pytest

import ops.tools.run_exposure_to_options_intent_adapter_v1 as adapter_module


def _policy_for_vol_income() -> dict:
    policy_path = (
        Path("/home/node/constellation/governance/02_REGISTRIES/C2_EXPOSURE_TO_OPTIONS_INTENT_POLICY_V1.json").resolve()
    )
    return json.loads(policy_path.read_text(encoding="utf-8"))


def _base_exposure(*, target_notional_pct: str = "0.01", risk_class: str = "VOL_INCOME_DEFINED") -> dict:
    return {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": "c2_vol_income_spy_2026-04-24_v1",
        "created_at_utc": "2026-04-24T00:00:00Z",
        "engine": {"engine_id": "C2_VOL_INCOME_DEFINED_RISK_V1", "suite": "C2_HYBRID_V1", "mode": "PAPER"},
        "underlying": {"symbol": "SPY", "currency": "USD"},
        "exposure_type": "SHORT_VOL_DEFINED",
        "option": {"structure": "PUT", "direction": "SELL"},
        "target_notional_pct": target_notional_pct,
        "expected_holding_days": 7,
        "risk_class": risk_class,
        "constraints": {"max_risk_pct": "0.01"},
        "canonical_json_hash": None,
    }


def test_adapter_fail_closed_when_option_block_missing() -> None:
    policy = _policy_for_vol_income()
    engine_policy = adapter_module._find_engine_policy(policy, "C2_VOL_INCOME_DEFINED_RISK_V1")
    exposure = _base_exposure()
    exposure.pop("option")

    with pytest.raises(adapter_module.AdapterError, match="EXPOSURE_OPTION_MISSING"):
        adapter_module._adapt(exposure, engine_policy)


def test_adapter_fail_closed_when_target_notional_pct_not_allowlisted() -> None:
    policy = _policy_for_vol_income()
    engine_policy = adapter_module._find_engine_policy(policy, "C2_VOL_INCOME_DEFINED_RISK_V1")
    exposure = _base_exposure(target_notional_pct="0.40")

    with pytest.raises(adapter_module.AdapterError, match="POLICY_REQUIREMENT_FAILED:target_notional_pct"):
        adapter_module._adapt(exposure, engine_policy)


def test_adapter_fail_closed_when_risk_class_not_allowlisted() -> None:
    policy = _policy_for_vol_income()
    engine_policy = adapter_module._find_engine_policy(policy, "C2_VOL_INCOME_DEFINED_RISK_V1")
    exposure = _base_exposure(risk_class="VRP_DEFINED")

    with pytest.raises(adapter_module.AdapterError, match="POLICY_REQUIREMENT_FAILED:risk_class"):
        adapter_module._adapt(exposure, engine_policy)


def test_adapter_builds_options_intent_when_exposure_matches_policy() -> None:
    policy = _policy_for_vol_income()
    engine_policy = adapter_module._find_engine_policy(policy, "C2_VOL_INCOME_DEFINED_RISK_V1")
    out = adapter_module._adapt(_base_exposure(), engine_policy)

    assert out["schema_id"] == "options_intent"
    assert out["schema_version"] == "v2"
    assert out["underlying"]["symbol"] == "SPY"
    assert out["strategy"]["structure"] == "VERTICAL_SPREAD"
    assert out["strategy"]["direction"] == "CREDIT"
    assert isinstance(out["canonical_json_hash"], str) and len(out["canonical_json_hash"]) == 64
