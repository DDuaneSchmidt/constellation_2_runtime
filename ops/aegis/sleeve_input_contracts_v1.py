from __future__ import annotations

import csv
import hashlib
import json
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.candidate_generation_diagnostics_v1 import SIMULATOR_ENGINE_ID, _authoritative_sleeve_inventory
from ops.aegis.intelligence_common_v1 import now_utc_v1, write_json_v1


REPORT_FAMILY = "aegis_sleeve_input_contracts_v1"


def build_sleeve_input_contracts_v1(*, repo_root: Path, day_utc: str, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    inventory = _authoritative_sleeve_inventory(repo_root=Path(repo_root))
    contracts = []
    override_by_id = overrides if isinstance(overrides, dict) else {}
    for row in inventory.get("enabled_sleeves") or []:
        sleeve_id = str(row.get("sleeve_id") or "")
        if sleeve_id == SIMULATOR_ENGINE_ID:
            continue
        if sleeve_id in override_by_id:
            contract = dict(override_by_id[sleeve_id])
            contract.setdefault("sleeve_id", sleeve_id)
        else:
            contract = _default_contract(row)
        contracts.append(contract)
    missing = [row["sleeve_id"] for row in contracts if row.get("contract_status") == "CONTRACT_MISSING"]
    return {
        "schema_id": "aegis_sleeve_input_contracts",
        "schema_version": "v1",
        "artifact_id": "aegis_sleeve_input_contracts_v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "authoritative_sleeve_registry_path": str(inventory.get("registry_path") or ""),
        "total_sleeves_enabled": len([row for row in inventory.get("enabled_sleeves") or [] if row.get("sleeve_id") != SIMULATOR_ENGINE_ID]),
        "contract_count": len(contracts),
        "missing_contract_sleeve_ids": missing,
        "contracts": contracts,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def write_sleeve_input_contracts_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "sleeve_input_contracts.v1.json", payload)
    summary_path = out_dir / "sleeve_input_contracts.summary.txt"
    matrix_path = out_dir / "sleeve_input_contracts.matrix.csv"
    summary_path.write_text(render_sleeve_input_contracts_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_sleeve_input_contracts_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def contract_hash_v1(contract: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(contract, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()


def render_sleeve_input_contracts_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS SLEEVE INPUT CONTRACTS v1",
        f"day_utc: {payload.get('day_utc')}",
        f"total_sleeves_enabled: {payload.get('total_sleeves_enabled')}",
        f"contract_count: {payload.get('contract_count')}",
        f"missing_contract_sleeve_ids: {', '.join(payload.get('missing_contract_sleeve_ids') or [])}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
        "contracts:",
    ]
    for row in payload.get("contracts") or []:
        required = ",".join(item.get("data_item_id", "") for item in row.get("required_inputs") or [])
        optional = ",".join(item.get("data_item_id", "") for item in row.get("optional_inputs") or [])
        lines.append(f"- {row.get('sleeve_id')}: status={row.get('contract_status', 'OK')} required={required} optional={optional}")
    return "\n".join(lines) + "\n"


def render_sleeve_input_contracts_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=["sleeve_id", "enabled", "contract_status", "required_inputs", "optional_inputs", "allow_run_with_warnings", "contract_hash"])
    writer.writeheader()
    for row in payload.get("contracts") or []:
        writer.writerow(
            {
                "sleeve_id": row.get("sleeve_id", ""),
                "enabled": row.get("enabled", True),
                "contract_status": row.get("contract_status", "OK"),
                "required_inputs": "|".join(item.get("data_item_id", "") for item in row.get("required_inputs") or []),
                "optional_inputs": "|".join(item.get("data_item_id", "") for item in row.get("optional_inputs") or []),
                "allow_run_with_warnings": (row.get("candidate_generation_policy") or {}).get("allow_run_with_warnings", True),
                "contract_hash": contract_hash_v1(row),
            }
        )
    return out.getvalue()


def _default_contract(row: dict[str, Any]) -> dict[str, Any]:
    sleeve_id = str(row.get("sleeve_id") or "")
    symbols = [str(symbol).strip().upper() for symbol in row.get("allowed_symbols") or [] if str(symbol).strip()]
    required = [
        {
            "data_item_id": f"market.price.{symbol}",
            "required": True,
            "block_if_missing": True,
            "freshness_requirement": "CURRENT",
            "reason": f"{sleeve_id} requires current price data for its governed symbol {symbol}.",
        }
        for symbol in symbols
    ]
    optional = []
    if sleeve_id in {"C2_TREND_EQ_PRIMARY_V1", "C2_MEAN_REVERSION_EQ_V1", "C2_MARKET_NEUTRAL_SPREAD_V1", "C2_CROSS_ASSET_TREND_V1"}:
        optional.extend(
            [
                {"data_item_id": "market.breadth.down_pct", "required": False, "warn_if_missing": True, "reason": "Breadth improves context but is not a universal candidate-generation blocker."},
                {"data_item_id": "market.breadth.advance_decline_delta", "required": False, "warn_if_missing": True, "reason": "Breadth improves context but is not a universal candidate-generation blocker."},
            ]
        )
    if sleeve_id == "C2_VOL_INCOME_DEFINED_RISK_V1":
        required.append(
            {
                "data_item_id": "market.volatility.VIX",
                "required": True,
                "block_if_missing": True,
                "freshness_requirement": "CURRENT",
                "reason": "Defined-risk volatility income requires current volatility context.",
            }
        )
    elif sleeve_id in {"C2_DEFENSIVE_TAIL_V1", "C2_EVENT_DISLOCATION_V1", "C2_CROSS_ASSET_TREND_V1"}:
        optional.append({"data_item_id": "market.volatility.VIX", "required": False, "warn_if_missing": True, "reason": "VIX is contextual for this sleeve, not a hard input."})
    return {
        "sleeve_id": sleeve_id,
        "enabled": bool(row.get("enabled", True)),
        "contract_status": "OK",
        "required_inputs": required,
        "optional_inputs": optional,
        "fallbacks": [],
        "candidate_generation_policy": {
            "allow_run_with_warnings": True,
            "require_all_required_inputs": True,
            "never_fabricate_missing_inputs": True,
        },
    }
