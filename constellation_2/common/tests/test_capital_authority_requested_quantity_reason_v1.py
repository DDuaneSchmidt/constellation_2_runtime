from __future__ import annotations

import sys
import json
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_capital_authority_allocation_day_v1 as allocation_module
from ops.tools.run_risk_definition_contract_v1 import build_risk_definition_contract_v1, risk_contract_path_v1


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _edge_policy() -> dict:
    return {
        "policy_version": "v1",
        "allocator_compatibility": {"allowed_calculation_versions": ["sleeve_edge_measurement_v1"]},
        "allocator_actions": {
            "MEASUREMENT_INVALID": {"capital_multiplier_bp": 0, "reason_code": "CAPAUTH_SLEEVE_MEASUREMENT_INVALID"},
            "INSUFFICIENT_DATA": {"capital_multiplier_bp": 0, "reason_code": "CAPAUTH_SLEEVE_INSUFFICIENT_DATA"},
            "QUALIFIED": {"capital_multiplier_bp": 10000, "reason_code": "CAPAUTH_SLEEVE_QUALIFIED"},
            "WATCHLIST": {"capital_multiplier_bp": 5000, "reason_code": "CAPAUTH_SLEEVE_WATCHLIST_THROTTLE"},
            "THROTTLED": {"capital_multiplier_bp": 2500, "reason_code": "CAPAUTH_SLEEVE_POLICY_THROTTLED"},
            "DISABLED": {"capital_multiplier_bp": 0, "reason_code": "CAPAUTH_SLEEVE_DISABLED"},
        },
    }


def _paper_discovery_policy() -> dict:
    return {
        "policy_id": "BOOTSTRAP_PAPER_TRADING",
        "enabled": True,
        "environments": ["PAPER"],
        "activation_triggers": [
            "SLEEVE_EDGE_SNAPSHOT_MISSING",
            "SLEEVE_EDGE_INSUFFICIENT_EVIDENCE",
        ],
        "min_prior_edge_snapshot_days_for_edge_governed": 3,
        "discovery_headroom_multiplier_bp": 10000,
        "max_sleeve_headroom_cents": 100000,
        "discovery_sleeve_governance_multiplier_bp": 10000,
        "discovery_portfolio_governance_multiplier_bp": 10000,
        "minimum_executable_trade_risk_cents": 100000,
        "reason_codes": [
            "PAPER_DISCOVERY_MODE_ACTIVE",
            "SLEEVE_EDGE_MEASUREMENT_BOOTSTRAP_ALLOWED",
            "PAPER_DISCOVERY_SLEEVE_THROTTLE_RELAXED",
            "PAPER_DISCOVERY_MIN_EXECUTABLE_HEADROOM_ALLOWED",
        ],
    }


def test_exposure_intent_missing_nav_total_has_specific_reason() -> None:
    intent = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "exposure_type": "LONG_EQUITY",
        "target_notional_pct": "0.01",
        "constraints": {"max_risk_pct": "0.01"},
    }
    reason_codes = allocation_module._specific_unproven_requested_quantity_reason_codes(
        intent,
        nav_total_cents=0,
    )
    assert "AUTHZ_MISSING_EXPOSURE_BUDGET_NAV_TOTAL_CENTS" in reason_codes


def test_exposure_intent_missing_constraints_has_specific_reason() -> None:
    intent = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "exposure_type": "LONG_EQUITY",
        "target_notional_pct": "0.01",
    }
    reason_codes = allocation_module._specific_unproven_requested_quantity_reason_codes(
        intent,
        nav_total_cents=10_000_000,
    )
    assert reason_codes == ["AUTHZ_MISSING_SIZING_INPUT"]


def test_options_intent_without_executable_contract_has_specific_reason() -> None:
    intent = {
        "schema_id": "options_intent",
        "schema_version": "v2",
        "risk": {"max_contracts": 0, "max_risk_usd": "25.00"},
    }
    reason_codes = allocation_module._specific_unproven_requested_quantity_reason_codes(
        intent,
        nav_total_cents=10_000_000,
    )
    assert reason_codes == ["RISK_DEFINITION_CONTRACT_MISSING"]


def test_valid_exposure_intent_returns_no_unproven_reason() -> None:
    intent = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "exposure_type": "LONG_EQUITY",
        "target_notional_pct": "0.01",
        "constraints": {"max_risk_pct": "0.01"},
    }
    reason_codes = allocation_module._specific_unproven_requested_quantity_reason_codes(
        intent,
        nav_total_cents=10_000_000,
    )
    assert reason_codes == []


def test_long_equity_stop_risk_uses_same_day_market_price(tmp_path: Path) -> None:
    day = "2026-05-01"
    _write(
        tmp_path / "market_data_snapshot_v1" / "snapshots" / day / "SPY.market_data_snapshot.v1.json",
        {
            "schema_id": "C2_MARKET_DATA_SNAPSHOT_V1",
            "schema_version": "v1",
            "day_utc": day,
            "symbol": "SPY",
            "close": "500.00",
        },
    )
    intent = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "intent_id": "trend-intent",
        "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1", "mode": "PAPER"},
        "exposure_type": "LONG_EQUITY",
        "underlying": {"symbol": "SPY"},
        "target_notional_pct": "0.01",
        "constraints": {"max_risk_pct": "0.01", "stop_loss_bps": 1000},
    }
    _write(tmp_path / "intents_v1" / "snapshots" / day / f"{'a' * 64}.exposure_intent.v1.json", intent)
    contract = build_risk_definition_contract_v1(day_utc=day, truth_root=tmp_path, intent_hash="a" * 64)
    _write(risk_contract_path_v1(truth_root=tmp_path, day_utc=day, intent_hash="a" * 64), contract)

    sizing = allocation_module._extract_quantity_and_risk_per_unit_cents(
        intent,
        nav_total_cents=10_000_000,
        day_utc=day,
        intent_hash="a" * 64,
        truth_root=tmp_path,
        environment="PAPER",
    )

    assert sizing == (1, 5_000)


def test_long_equity_stop_risk_fails_closed_without_price(tmp_path: Path) -> None:
    intent = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "exposure_type": "LONG_EQUITY",
        "underlying": {"symbol": "SPY"},
        "target_notional_pct": "0.01",
        "constraints": {"max_risk_pct": "0.01", "stop_loss_bps": 1000},
    }

    reason_codes = allocation_module._specific_unproven_requested_quantity_reason_codes(
        intent,
        nav_total_cents=10_000_000,
        day_utc="2026-05-01",
        truth_root=tmp_path,
        environment="PAPER",
    )

    assert "RISK_DEFINITION_CONTRACT_MISSING" in reason_codes
    assert "AUTHZ_MISSING_EQUITY_STOP_RISK_EVIDENCE" in reason_codes


def test_short_vol_defined_missing_max_loss_blocks_authorization_sizing(tmp_path: Path) -> None:
    day = "2026-05-01"
    intent_hash = "b" * 64
    candidate = tmp_path / "phaseC_preflight_v1" / day / "attempt_A0001" / intent_hash
    order_plan_path = candidate / "order_plan.v1.json"
    _write(
        order_plan_path,
        {
            "schema_id": "order_plan",
            "schema_version": "v1",
            "risk_proof": {"defined_risk_proven": True, "contracts": 1},
        },
    )
    _write(
        candidate / "execution_identity_record.v1.json",
        {
            "day_utc": day,
            "environment": "PAPER",
            "intent_hash": intent_hash,
            "source_refs": [{"ref_type": "order_plan_ref", "path": str(order_plan_path)}],
        },
    )
    intent = {
        "schema_id": "exposure_intent",
        "schema_version": "v1",
        "exposure_type": "SHORT_VOL_DEFINED",
        "underlying": {"symbol": "IWM"},
        "target_notional_pct": "0.01",
        "constraints": {"max_risk_pct": "0.01"},
    }

    sizing = allocation_module._extract_quantity_and_risk_per_unit_cents(
        intent,
        nav_total_cents=10_000_000,
        day_utc=day,
        intent_hash=intent_hash,
        truth_root=tmp_path,
        environment="PAPER",
    )
    reason_codes = allocation_module._specific_unproven_requested_quantity_reason_codes(
        intent,
        nav_total_cents=10_000_000,
        day_utc=day,
        intent_hash=intent_hash,
        truth_root=tmp_path,
        environment="PAPER",
    )

    assert sizing is None
    assert "RISK_DEFINITION_CONTRACT_MISSING" in reason_codes
    assert "AUTHZ_MISSING_DEFINED_RISK_EVIDENCE" in reason_codes


def test_headroom_metrics_rejects_when_required_risk_exceeds_available_headroom() -> None:
    metrics = allocation_module._headroom_authorization_metrics(  # noqa: SLF001
        requested_quantity=1,
        risk_per_unit_cents=100_000,
        available_sleeve_headroom_cents=0,
        available_portfolio_headroom_cents=0,
    )
    assert metrics["required_risk_cents"] == 100_000
    assert metrics["available_headroom_cents"] == 0
    assert metrics["authorized_quantity"] == 0


def test_headroom_metrics_allows_quantity_when_required_risk_within_headroom() -> None:
    metrics = allocation_module._headroom_authorization_metrics(  # noqa: SLF001
        requested_quantity=1,
        risk_per_unit_cents=100_000,
        available_sleeve_headroom_cents=200_000,
        available_portfolio_headroom_cents=200_000,
    )
    assert metrics["required_risk_cents"] == 100_000
    assert metrics["available_headroom_cents"] == 200_000
    assert metrics["authorized_quantity"] == 1


@pytest.mark.parametrize("day_utc", ["2026-04-23", "2026-04-24"])
def test_paper_discovery_mode_allows_capped_multiplier_when_snapshot_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    day_utc: str,
) -> None:
    def _missing_snapshot(**_: object) -> dict:
        raise ValueError(f"SLEEVE_EDGE_SNAPSHOT_MISSING:sleeve_id=C2_TREND_EQ_PRIMARY:day_utc={day_utc}")

    monkeypatch.setattr(allocation_module, "read_sleeve_edge_snapshot_for_day_v1", _missing_snapshot)

    meta = allocation_module._load_sleeve_edge_allocator_meta(
        truth_root=tmp_path,
        day_utc=day_utc,
        sleeve_id="C2_TREND_EQ_PRIMARY",
        sleeve_edge_policy=_edge_policy(),
        canonical_sequence_owner=allocation_module.CANONICAL_SEQUENCE_OWNER,
        environment="PAPER",
        paper_discovery_policy=_paper_discovery_policy(),
    )

    assert meta["capital_multiplier_bp"] == 10000
    assert meta["action_reason_code"] == "SLEEVE_EDGE_MEASUREMENT_BOOTSTRAP_ALLOWED"
    assert meta["discovery_mode_active"] is True
    assert meta["discovery_max_headroom_cents"] == 100000
    assert meta["discovery_sleeve_governance_multiplier_bp"] == 10000
    assert "PAPER_DISCOVERY_MODE_ACTIVE" in meta["reason_codes"]
    assert "SLEEVE_EDGE_MEASUREMENT_BOOTSTRAP_ALLOWED" in meta["reason_codes"]
    assert "PAPER_DISCOVERY_SLEEVE_THROTTLE_RELAXED" in meta["reason_codes"]
    assert "PAPER_DISCOVERY_MIN_EXECUTABLE_HEADROOM_ALLOWED" in meta["reason_codes"]


def test_paper_discovery_mode_never_applies_to_live_when_snapshot_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _missing_snapshot(**_: object) -> dict:
        raise ValueError("SLEEVE_EDGE_SNAPSHOT_MISSING:sleeve_id=C2_TREND_EQ_PRIMARY:day_utc=2026-04-23")

    monkeypatch.setattr(allocation_module, "read_sleeve_edge_snapshot_for_day_v1", _missing_snapshot)

    meta = allocation_module._load_sleeve_edge_allocator_meta(
        truth_root=tmp_path,
        day_utc="2026-04-23",
        sleeve_id="C2_TREND_EQ_PRIMARY",
        sleeve_edge_policy=_edge_policy(),
        canonical_sequence_owner=allocation_module.CANONICAL_SEQUENCE_OWNER,
        environment="LIVE",
        paper_discovery_policy=_paper_discovery_policy(),
    )

    assert meta["capital_multiplier_bp"] == 0
    assert meta["action_reason_code"] == "SLEEVE_EDGE_MEASUREMENT_INVALID"
    assert meta["discovery_mode_active"] is False
    assert meta["discovery_sleeve_governance_multiplier_bp"] == 0
    assert "PAPER_DISCOVERY_MODE_ACTIVE" not in meta["reason_codes"]


def test_paper_discovery_mode_turns_off_after_sufficient_prior_edge_days(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for prior_day in ("2026-04-20", "2026-04-21", "2026-04-22"):
        path = (
            tmp_path
            / "reports"
            / "sleeve_edge_snapshot_v1"
            / prior_day
            / "C2_TREND_EQ_PRIMARY"
            / "snapshot-1"
            / "sleeve_edge_snapshot.v1.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")

    def _missing_snapshot(**_: object) -> dict:
        raise ValueError("SLEEVE_EDGE_SNAPSHOT_MISSING:sleeve_id=C2_TREND_EQ_PRIMARY:day_utc=2026-04-23")

    monkeypatch.setattr(allocation_module, "read_sleeve_edge_snapshot_for_day_v1", _missing_snapshot)

    meta = allocation_module._load_sleeve_edge_allocator_meta(
        truth_root=tmp_path,
        day_utc="2026-04-23",
        sleeve_id="C2_TREND_EQ_PRIMARY",
        sleeve_edge_policy=_edge_policy(),
        canonical_sequence_owner=allocation_module.CANONICAL_SEQUENCE_OWNER,
        environment="PAPER",
        paper_discovery_policy=_paper_discovery_policy(),
    )

    assert meta["capital_multiplier_bp"] == 0
    assert meta["discovery_mode_active"] is False
    assert meta["discovery_sleeve_governance_multiplier_bp"] == 0
    assert "PAPER_DISCOVERY_MODE_ACTIVE" not in meta["reason_codes"]


def test_existing_sleeve_edge_snapshot_uses_normal_edge_governance_even_when_discovery_policy_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        allocation_module,
        "read_sleeve_edge_snapshot_for_day_v1",
        lambda **_: {
            "qualification": {
                "qualification_state": "QUALIFIED",
                "edge_band": "QUALIFIED_POSITIVE",
                "execution_health_band": "ACCEPTABLE",
                "sample_sufficiency_band": "SUFFICIENT",
                "drift_band": "STABLE",
                "reason_codes": ["SLEEVE_EDGE_QUALIFIED"],
            },
            "artifact_path": str((tmp_path / "reports" / "sleeve_edge_snapshot.v1.json").resolve()),
            "artifact_sha256": "2" * 64,
            "policy_version": "v1",
            "calculation_version": "sleeve_edge_measurement_v1",
        },
    )
    monkeypatch.setattr(
        allocation_module,
        "allocator_action_from_qualification_v1",
        lambda *_: {
            "reason_code": "CAPAUTH_SLEEVE_QUALIFIED",
            "capital_multiplier_bp": 10000,
        },
    )

    meta = allocation_module._load_sleeve_edge_allocator_meta(
        truth_root=tmp_path,
        day_utc="2026-04-23",
        sleeve_id="C2_TREND_EQ_PRIMARY",
        sleeve_edge_policy=_edge_policy(),
        canonical_sequence_owner=allocation_module.CANONICAL_SEQUENCE_OWNER,
        environment="PAPER",
        paper_discovery_policy=_paper_discovery_policy(),
    )

    assert meta["capital_multiplier_bp"] == 10000
    assert meta["action_reason_code"] == "CAPAUTH_SLEEVE_QUALIFIED"
    assert meta["discovery_mode_active"] is False
    assert meta["discovery_sleeve_governance_multiplier_bp"] == 0
    assert "PAPER_DISCOVERY_MODE_ACTIVE" not in meta["reason_codes"]


def test_discovery_mode_relaxes_sleeve_governance_multiplier_for_paper() -> None:
    effective, reason_codes = allocation_module._effective_sleeve_governance_multiplier_bp(
        governed_multiplier_bp=5000,
        environment="PAPER",
        qualification_meta={
            "discovery_mode_active": True,
            "discovery_sleeve_governance_multiplier_bp": 10000,
        },
    )
    assert effective == 10000
    assert reason_codes == ["PAPER_DISCOVERY_SLEEVE_THROTTLE_RELAXED"]


def test_discovery_mode_never_relaxes_sleeve_governance_multiplier_for_live() -> None:
    effective, reason_codes = allocation_module._effective_sleeve_governance_multiplier_bp(
        governed_multiplier_bp=5000,
        environment="LIVE",
        qualification_meta={
            "discovery_mode_active": True,
            "discovery_sleeve_governance_multiplier_bp": 10000,
        },
    )
    assert effective == 5000
    assert reason_codes == []


def test_discovery_mode_relaxes_portfolio_governance_multiplier_for_paper() -> None:
    effective, reason_codes = allocation_module._effective_portfolio_governance_multiplier_bp(
        governed_multiplier_bp=5000,
        environment="PAPER",
        discovery_mode_active=True,
        discovery_override_multiplier_bp=10000,
    )
    assert effective == 10000
    assert reason_codes == ["PAPER_DISCOVERY_MIN_EXECUTABLE_HEADROOM_ALLOWED"]


def test_discovery_mode_never_relaxes_portfolio_governance_multiplier_for_live() -> None:
    effective, reason_codes = allocation_module._effective_portfolio_governance_multiplier_bp(
        governed_multiplier_bp=5000,
        environment="LIVE",
        discovery_mode_active=True,
        discovery_override_multiplier_bp=10000,
    )
    assert effective == 5000
    assert reason_codes == []


def test_discovery_mode_min_executable_trade_headroom_allows_one_contract_risk() -> None:
    metrics = allocation_module._headroom_authorization_metrics(
        requested_quantity=1,
        risk_per_unit_cents=100_000,
        available_sleeve_headroom_cents=100_000,
        available_portfolio_headroom_cents=100_000,
    )
    assert metrics["required_risk_cents"] == 100_000
    assert metrics["available_headroom_cents"] == 100_000
    assert metrics["authorized_quantity"] == 1


def test_discovery_headroom_allows_small_trade_when_required_risk_fits_cap() -> None:
    metrics = allocation_module._headroom_authorization_metrics(
        requested_quantity=1,
        risk_per_unit_cents=20_000,
        available_sleeve_headroom_cents=30_000,
        available_portfolio_headroom_cents=100_000,
    )
    assert metrics["required_risk_cents"] == 20_000
    assert metrics["available_headroom_cents"] == 30_000
    assert metrics["authorized_quantity"] == 1


def test_discovery_headroom_rejects_trade_when_required_risk_exceeds_cap() -> None:
    metrics = allocation_module._headroom_authorization_metrics(
        requested_quantity=1,
        risk_per_unit_cents=100_000,
        available_sleeve_headroom_cents=30_000,
        available_portfolio_headroom_cents=100_000,
    )
    assert metrics["required_risk_cents"] == 100_000
    assert metrics["available_headroom_cents"] == 30_000
    assert metrics["authorized_quantity"] == 0
