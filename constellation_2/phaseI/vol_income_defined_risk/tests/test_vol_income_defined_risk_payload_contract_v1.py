from __future__ import annotations

import constellation_2.phaseI.vol_income_defined_risk.run.run_vol_income_defined_risk_intents_day_v1 as vol_module


def test_build_exposure_intent_includes_short_vol_option_metadata() -> None:
    payload = vol_module._build_exposure_intent(
        day_utc="2026-04-24",
        mode="PAPER",
        symbol="SPY",
        target_pct="0.01",
        max_risk_pct="0.01",
    )
    assert payload["exposure_type"] == "SHORT_VOL_DEFINED"
    assert payload["option"] == {"structure": "PUT", "direction": "SELL"}


def test_build_exposure_intent_uses_policy_compatible_risk_class() -> None:
    payload = vol_module._build_exposure_intent(
        day_utc="2026-04-24",
        mode="PAPER",
        symbol="SPY",
        target_pct="0.01",
        max_risk_pct="0.01",
    )
    assert payload["risk_class"] == "VOL_INCOME_DEFINED"
