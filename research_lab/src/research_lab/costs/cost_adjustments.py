from __future__ import annotations

from typing import Any


def round_trip_cost_bps_for_symbol(cost_model: dict[str, Any], symbol: str) -> float:
    clean = symbol.upper()
    if clean in cost_model.get("asset_costs", {}):
        return float(cost_model["asset_costs"][clean]["round_trip_cost_bps"])
    assumptions = cost_model.get("assumptions", {})
    half_spread = float(assumptions.get("default_half_spread_bps", 1))
    slippage = float(assumptions.get("default_slippage_bps", 2))
    return (half_spread + slippage) * 2


def apply_costs_to_forward_returns(forward_returns: list[dict[str, Any]], cost_model: dict[str, Any]) -> list[dict[str, Any]]:
    adjusted: list[dict[str, Any]] = []
    for row in forward_returns:
        clean = dict(row)
        gross = float(clean.get("gross_forward_return", clean.get("forward_return", 0.0)))
        cost_bps = round_trip_cost_bps_for_symbol(cost_model, str(clean["symbol"]))
        clean["gross_forward_return"] = gross
        clean["forward_return"] = gross
        clean["round_trip_cost_bps"] = cost_bps
        clean["post_cost_forward_return"] = gross - (cost_bps / 10000.0)
        clean["cost_model_snapshot_id"] = cost_model["cost_model_snapshot_id"]
        adjusted.append(clean)
    return adjusted

