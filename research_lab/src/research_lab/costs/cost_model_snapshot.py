from __future__ import annotations

from typing import Any

from research_lab.storage.hashing import content_hash


DEFAULT_COST_MODEL_ID = "cm_etf_research_default_v1"
DEFAULT_COST_MODEL_VERSION = "etf_research_costs_v1"


def build_default_cost_model_snapshot(*, created_at: str, created_by: str = "Aegis") -> dict[str, Any]:
    assumptions = {
        "commission_bps": 0,
        "default_half_spread_bps": 1,
        "default_slippage_bps": 2,
        "round_trip": True,
        "entry_cost_formula": "half_spread_bps + slippage_bps",
        "exit_cost_formula": "half_spread_bps + slippage_bps",
        "round_trip_cost_formula": "entry_cost_bps + exit_cost_bps",
    }
    overrides = {
        "SPY": {"half_spread_bps": 1, "slippage_bps": 1},
        "QQQ": {"half_spread_bps": 1, "slippage_bps": 1},
        "IWM": {"half_spread_bps": 2, "slippage_bps": 2},
        "TLT": {"half_spread_bps": 2, "slippage_bps": 2},
        "GLD": {"half_spread_bps": 2, "slippage_bps": 2},
    }
    asset_costs: dict[str, dict[str, float]] = {}
    for symbol, row in sorted(overrides.items()):
        entry = float(row["half_spread_bps"] + row["slippage_bps"])
        exit_cost = entry
        asset_costs[symbol] = {
            "half_spread_bps": float(row["half_spread_bps"]),
            "slippage_bps": float(row["slippage_bps"]),
            "entry_cost_bps": entry,
            "exit_cost_bps": exit_cost,
            "round_trip_cost_bps": entry + exit_cost,
        }
    snapshot = {
        "cost_model_snapshot_id": DEFAULT_COST_MODEL_ID,
        "name": "Default ETF research cost model",
        "cost_model_version": DEFAULT_COST_MODEL_VERSION,
        "created_at": created_at,
        "created_by": created_by,
        "assumptions": assumptions,
        "asset_costs": asset_costs,
        "content_hash": "",
        "schema_version": "cost_model_snapshot.v1",
    }
    snapshot["content_hash"] = cost_model_content_hash(snapshot)
    return snapshot


def cost_model_content_hash(snapshot: dict[str, Any]) -> str:
    return content_hash(snapshot, exclude={"created_at", "content_hash"}, sort_lists=True)

