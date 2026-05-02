from __future__ import annotations

import sys
import json
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
import ops.tools.run_correlation_envelope_gate_v1 as correlation_gate_module


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def test_fixed_decimal_str_renders_scientific_notation_as_plain_decimal() -> None:
    rendered = correlation_gate_module._fixed_decimal_str(
        Decimal("5.071627647789142889350095082E-8")
    )

    assert rendered == "0.00000005071627647789142889350095082"
    assert "E" not in rendered


def test_intent_scans_ignore_zero_cap_non_executable_engines(tmp_path: Path, monkeypatch) -> None:
    day = "2026-05-01"
    intents_root = tmp_path / "intents_v1" / "snapshots"
    monkeypatch.setattr(correlation_gate_module, "IN_INTENTS", intents_root)
    _write(
        intents_root / day / "active.json",
        {
            "engine": {"engine_id": "C2_TREND_EQ_PRIMARY_V1"},
            "underlying": {"symbol": "SPY"},
            "target_notional_pct": "0.01",
        },
    )
    _write(
        intents_root / day / "inactive.json",
        {
            "engine": {"engine_id": "C2_CROSS_ASSET_TREND_V1"},
            "underlying": {"symbol": "DBC"},
            "target_notional_pct": "0.10",
        },
    )

    active, counts = correlation_gate_module._scan_intents(
        day,
        executable_engine_ids={"C2_TREND_EQ_PRIMARY_V1"},
    )
    notionals = correlation_gate_module._scan_intents_notional_by_symbol(
        day,
        Decimal("1000000"),
        executable_engine_ids={"C2_TREND_EQ_PRIMARY_V1"},
    )

    assert active == ["C2_TREND_EQ_PRIMARY_V1"]
    assert counts == {"SPY": 1}
    assert notionals == {"SPY": Decimal("10000.00")}


def test_depth_liquidity_schema_accepts_fixed_decimal_impact_bps() -> None:
    payload = {
        "schema_id": "C2_DEPTH_LIQUIDITY_STRESS_V1",
        "schema_version": 1,
        "day_utc": "2026-04-09",
        "produced_utc": "2026-04-09T00:00:00Z",
        "producer": {
            "repo": "constellation_2_runtime",
            "git_sha": "a" * 40,
            "module": "ops/tools/run_correlation_envelope_gate_v1.py",
        },
        "status": "PASS",
        "fail_closed": False,
        "policy": {
            "policy_id": "C2_DEPTH_LIQUIDITY_STRESS_POLICY_V1",
            "path": "governance/02_REGISTRIES/C2_DEPTH_LIQUIDITY_STRESS_POLICY_V1.json",
            "sha256": "b" * 64,
        },
        "inputs": {
            "intents_root": "intents_v1/snapshots/2026-04-09",
            "liquidity_dataset_manifest_path": "market_data_snapshot_v1/dataset_manifest.json",
            "liquidity_dataset_manifest_sha256": "c" * 64,
            "nav_snapshot_path": "accounting_compat_v1/nav/2026-04-09/nav_snapshot.v1.json",
            "nav_snapshot_sha256": "d" * 64,
        },
        "regime_used": "LIQ_CONTRACTION",
        "aggregation": {
            "by_symbol": {
                "SPY": {
                    "intent_notional_dollar": "10000.00",
                    "price_close": "659.22",
                    "adv_shares": "67094252.5",
                    "adv_dollar": "44025589670.9025",
                    "depth_dollar_normal": "66038384.50635375",
                    "depth_dollar_stressed": "33019192.2531768750",
                    "spread_bps_stressed": "13.20",
                    "impact_bps": correlation_gate_module._fixed_decimal_str(
                        Decimal("5.071627647789142889350095082E-8")
                    ),
                    "total_cost_bps": "13.20000005071627647789142889",
                    "total_cost_dollar": "13.20000005071627647789142889",
                }
            },
            "portfolio": {
                "total_intent_notional_dollar": "10000.00",
                "total_cost_dollar": "13.20000005071627647789142889",
                "portfolio_cost_bps": "13.20000005071627647789142889",
                "max_symbol": "SPY",
                "max_symbol_cost_bps": "13.20000005071627647789142889",
            },
        },
        "enforcement": {
            "depth_scale_bp": 10000,
            "max_depth_portfolio_cost_bps": "25.0",
            "max_depth_symbol_cost_bps": "60.0",
            "portfolio_cost_bps_used": "13.20000005071627647789142889",
            "max_symbol_cost_bps_used": "13.20000005071627647789142889",
            "reason_codes": [],
        },
        "violations": [],
    }

    validate_against_repo_schema_v1(
        payload,
        REPO_ROOT,
        "governance/04_DATA/SCHEMAS/C2/REPORTS/depth_liquidity_stress.v1.schema.json",
    )
