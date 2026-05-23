from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import constellation_2.phaseG.allocation.run.run_allocation_day_v2 as allocation_v2
from ops.aegis.trade_lifecycle.readiness_domain_evaluation_v1 import build_readiness_domain_evaluations_v1

ENGINE_REGISTRY = ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json"
CAPITAL_POLICY = ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json"
DAY = "2026-05-20"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _active_engine_ids(registry: dict[str, Any]) -> set[str]:
    return {
        str(row.get("engine_id") or "").strip()
        for row in registry.get("engines", [])
        if str(row.get("activation_status") or "").strip().upper() == "ACTIVE"
    }


def _engine_policy_rows(policy: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for sleeve in policy.get("sleeves", []):
        for engine_id in sleeve.get("engine_ids", []):
            rows[str(engine_id)] = sleeve
    return rows


def _missing_or_zero_active_paper_headroom(registry: dict[str, Any], policy: dict[str, Any]) -> list[str]:
    rows = _engine_policy_rows(policy)
    missing: list[str] = []
    for engine_id in sorted(_active_engine_ids(registry)):
        row = rows.get(engine_id)
        limits = row.get("limits", {}) if isinstance(row, dict) else {}
        if not row:
            missing.append(f"{engine_id}:missing_policy_row")
            continue
        checks = {
            "paper_enabled": limits.get("paper_enabled") is True,
            "paper_test_capital_base": float(limits.get("paper_test_capital_base") or 0) > 0,
            "max_capital_at_risk_cents": int(limits.get("max_capital_at_risk_cents") or 0) > 0,
            "max_symbols": int(limits.get("max_symbols") or 0) > 0,
            "max_notional_per_trade": float(limits.get("max_notional_per_trade") or 0) > 0,
            "max_risk_per_trade": float(limits.get("max_risk_per_trade") or 0) > 0,
        }
        for name, ok in checks.items():
            if not ok:
                missing.append(f"{engine_id}:{name}")
    return missing


def test_every_active_engine_has_paper_headroom() -> None:
    registry = _read(ENGINE_REGISTRY)
    policy = _read(CAPITAL_POLICY)

    assert _active_engine_ids(registry) == {
        "C2_INTENT_SIMULATOR_V1",
        "C2_MEAN_REVERSION_EQ_V1",
        "C2_TREND_EQ_PRIMARY_V1",
        "C2_VOL_INCOME_DEFINED_RISK_V1",
        "C2_EVENT_DISLOCATION_V1",
        "C2_DEFENSIVE_TAIL_V1",
        "C2_CROSS_ASSET_TREND_V1",
        "C2_MARKET_NEUTRAL_SPREAD_V1",
    }
    assert _missing_or_zero_active_paper_headroom(registry, policy) == []


def test_live_and_prod_capital_remain_unaffected_by_paper_policy() -> None:
    policy = _read(CAPITAL_POLICY)
    scope = policy.get("scope", {})

    assert scope["environment"] == "PAPER"
    assert scope["paper_enabled"] is True
    assert scope["live_trading_allowed"] is False
    assert scope["broker_execution_allowed"] is False
    assert scope["real_capital_allocation_allowed"] is False
    assert scope["paper_submit_allowed"] is False


def test_inactive_engines_may_remain_zero_but_active_engines_may_not() -> None:
    registry = {
        "engines": [
            {"engine_id": "ACTIVE_ENGINE", "activation_status": "ACTIVE"},
            {"engine_id": "INACTIVE_ENGINE", "activation_status": "INACTIVE"},
        ]
    }
    policy = {
        "sleeves": [
            {"sleeve_id": "ACTIVE", "engine_ids": ["ACTIVE_ENGINE"], "limits": {"paper_enabled": True, "paper_test_capital_base": "1.00", "max_capital_at_risk_cents": 1, "max_symbols": 1, "max_notional_per_trade": "1.00", "max_risk_per_trade": "1.00"}},
            {"sleeve_id": "INACTIVE", "engine_ids": ["INACTIVE_ENGINE"], "limits": {"paper_enabled": False, "paper_test_capital_base": "0", "max_capital_at_risk_cents": 0, "max_symbols": 0, "max_notional_per_trade": "0", "max_risk_per_trade": "0"}},
        ]
    }

    assert _missing_or_zero_active_paper_headroom(registry, policy) == []


def test_capital_policy_mismatch_fails_helper() -> None:
    registry = {"engines": [{"engine_id": "ACTIVE_ENGINE", "activation_status": "ACTIVE"}]}
    policy = {"sleeves": [{"sleeve_id": "ACTIVE", "engine_ids": ["ACTIVE_ENGINE"], "limits": {"paper_enabled": True, "paper_test_capital_base": "0", "max_capital_at_risk_cents": 0, "max_symbols": 0, "max_notional_per_trade": "0", "max_risk_per_trade": "0"}}]}

    failures = _missing_or_zero_active_paper_headroom(registry, policy)
    assert "ACTIVE_ENGINE:max_capital_at_risk_cents" in failures
    assert "ACTIVE_ENGINE:max_symbols" in failures


def test_allocation_cap_table_covers_every_active_engine() -> None:
    active = _active_engine_ids(_read(ENGINE_REGISTRY))

    assert active <= set(allocation_v2.ENGINE_CAP_PCT)


def test_no_duplicate_capital_authority_policy_files_created() -> None:
    matches = [path for path in (ROOT / "governance/02_REGISTRIES").glob("*CAPITAL_AUTHORITY_POLICY*.json") if path.name != "C2_CAPITAL_AUTHORITY_POLICY_V1.json"]

    assert matches == []


def test_paper_nav_accounting_required_before_capital_domain_ready() -> None:
    construction = {
        "construction_id": "paper-trade-construction:test",
        "selected_exposure_intent_id": "intent:test",
        "symbol": "QQQ",
        "source_day": DAY,
        "market_data_latest_session": DAY,
        "entry_reference_price": "707.26",
        "suggested_quantity": None,
        "suggested_notional": "",
        "stop_price": "650.00",
        "risk_per_share": "57.26",
        "max_loss_estimate": "57.26",
        "capital_authority_source": "",
        "blocker_codes": ["CAPITAL_RISK_ENVELOPE_BLOCKED", "MISSING_SUGGESTED_QUANTITY"],
        "blocker_messages": ["Capital risk envelope did not pass."],
        "source_artifacts": [],
    }
    evaluations = build_readiness_domain_evaluations_v1(
        truth_root=ROOT,
        trade_lifecycle_case_id="case:test",
        selected_exposure_intent_id="intent:test",
        symbol="QQQ",
        source_day=DAY,
        paper_trade_construction=construction,
        evaluated_at=f"{DAY}T00:00:00Z",
    )
    statuses = {row["domain"]: row["status"] for row in evaluations}

    assert statuses["capital_authority"] == "BLOCKED"
    assert statuses["market_data"] == "READY"


def test_policy_contains_no_broker_order_or_allocation_side_effect_enablement() -> None:
    policy = _read(CAPITAL_POLICY)
    serialized = json.dumps(policy, sort_keys=True).lower()

    assert '"broker_execution_allowed": false' in serialized
    assert '"live_trading_allowed": false' in serialized
    assert '"real_capital_allocation_allowed": false' in serialized
    assert '"paper_submit_allowed": false' in serialized
    assert '"order_routing_allowed": true' not in serialized
    assert '"broker_execution_allowed": true' not in serialized
    assert '"live_trading_allowed": true' not in serialized
