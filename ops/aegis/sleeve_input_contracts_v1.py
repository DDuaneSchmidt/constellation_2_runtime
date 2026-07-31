from __future__ import annotations

import csv
import hashlib
import json
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.candidate_generation_diagnostics_v1 import SIMULATOR_ENGINE_ID, _authoritative_sleeve_inventory
from ops.aegis.context_requirement_profile_v1 import load_or_build_context_requirement_profile_v1, requirement_for_item_v1, severity_is_blocking_v1
from ops.aegis.hash_lineage_v1 import current_market_data_ref_v1
from ops.aegis.intelligence_common_v1 import now_utc_v1, write_json_v1
from ops.aegis.universe.canonical_symbol_universe_resolver_v1 import resolve_canonical_symbol_universe_v1


REPORT_FAMILY = "aegis_sleeve_input_contracts_v1"


def build_sleeve_input_contracts_v1(*, repo_root: Path, day_utc: str, truth_root: Path | None = None, overrides: dict[str, Any] | None = None, market_data_mode: str = "INTRADAY_OPERATIONAL") -> dict[str, Any]:
    repo = Path(repo_root)
    root = Path(truth_root).expanduser().resolve() if truth_root is not None else repo
    inventory = _authoritative_sleeve_inventory(repo_root=repo)
    context_profile = load_or_build_context_requirement_profile_v1(truth_root=root, day_utc=day_utc)
    generated_at = now_utc_v1()
    market_data_ref = current_market_data_ref_v1(truth_root=root, day_utc=day_utc)
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
            contract = _default_contract(row, repo_root=repo, truth_root=root, day_utc=day_utc, context_profile=context_profile, market_data_mode=market_data_mode)
        contract["generated_at_utc"] = generated_at
        contract["generated_by_command"] = f"TARGET_DAY={day_utc} npm run aegis:sleeve-input-contracts"
        contract["market_data_hash"] = str(market_data_ref.get("hash") or "")
        contract["market_data_artifact_path"] = str(market_data_ref.get("path") or "")
        contract["market_data_generated_at_utc"] = str(market_data_ref.get("generated_at_utc") or "")
        contract["market_data_generated_by_command"] = str(market_data_ref.get("command") or "")
        contracts.append(contract)
    missing = [row["sleeve_id"] for row in contracts if row.get("contract_status") == "CONTRACT_MISSING"]
    return {
        "schema_id": "aegis_sleeve_input_contracts",
        "schema_version": "v1",
        "artifact_id": "aegis_sleeve_input_contracts_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "generated_by_command": f"TARGET_DAY={day_utc} npm run aegis:sleeve-input-contracts",
        "market_data_hash": str(market_data_ref.get("hash") or ""),
        "market_data_artifact_path": str(market_data_ref.get("path") or ""),
        "market_data_generated_at_utc": str(market_data_ref.get("generated_at_utc") or ""),
        "market_data_generated_by_command": str(market_data_ref.get("command") or ""),
        "authoritative_sleeve_registry_path": str(inventory.get("registry_path") or ""),
        "active_context_requirement_profile_id": str(context_profile.get("active_profile_id") or ""),
        "context_requirement_profile_hash": str(context_profile.get("canonical_json_hash") or ""),
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


def _default_contract(row: dict[str, Any], *, repo_root: Path, truth_root: Path, day_utc: str, context_profile: dict[str, Any], market_data_mode: str) -> dict[str, Any]:
    sleeve_id = str(row.get("sleeve_id") or "")
    registry_symbols = [str(symbol).strip().upper() for symbol in row.get("allowed_symbols") or [] if str(symbol).strip()]
    symbol_resolution = _canonical_symbol_contract_resolution_v1(
        row=row,
        repo_root=repo_root,
        truth_root=truth_root,
        day_utc=day_utc,
        market_data_mode=market_data_mode,
    )
    symbols = list(symbol_resolution.get("allowed_symbols") or registry_symbols)
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
        vix_req = requirement_for_item_v1(context_profile, "market.volatility.VIX")
        vix_blocking = severity_is_blocking_v1(vix_req)
        vix_row = {
            "data_item_id": "market.volatility.VIX",
            "required": bool(vix_blocking),
            "block_if_missing": bool(vix_blocking),
            "block_if_stale": bool(vix_blocking),
            "warn_if_missing": not bool(vix_blocking),
            "freshness_requirement": str(vix_req.get("freshness_requirement") or ("CURRENT" if vix_blocking else "PRIOR_EOD_REFERENCE_ALLOWED")),
            "allowed_fallback_reference_types": list(vix_req.get("allowed_fallback_reference_types") or []),
            "blocker_severity": str(vix_req.get("blocker_severity") or ("BLOCKING" if vix_blocking else "NON_BLOCKING")),
            "active_context_requirement_profile_id": str(context_profile.get("active_profile_id") or ""),
            "reason": "Defined-risk volatility income VIX requirement follows the active context requirement profile.",
        }
        if vix_blocking:
            required.append(vix_row)
        else:
            optional.append(vix_row)
    elif sleeve_id in {"C2_DEFENSIVE_TAIL_V1", "C2_EVENT_DISLOCATION_V1", "C2_CROSS_ASSET_TREND_V1"}:
        optional.append({"data_item_id": "market.volatility.VIX", "required": False, "warn_if_missing": True, "reason": "VIX is contextual for this sleeve, not a hard input."})

    return {
        "sleeve_id": sleeve_id,
        "enabled": bool(row.get("enabled", True)),
        "contract_status": "OK",
        "required_inputs": required,
        "optional_inputs": optional,
        "allowed_symbols": symbols,
        "registry_allowed_symbols": registry_symbols,
        "symbol_universe_hash": _symbol_universe_hash_v1(symbol_resolution),
        "symbol_resolution": symbol_resolution,
        "fallbacks": [],
        "candidate_generation_policy": {
            "allow_run_with_warnings": True,
            "require_all_required_inputs": True,
            "never_fabricate_missing_inputs": True,
        },
    }


def _canonical_symbol_contract_resolution_v1(*, row: dict[str, Any], repo_root: Path, truth_root: Path, day_utc: str, market_data_mode: str) -> dict[str, Any]:
    sleeve_id = str(row.get("sleeve_id") or row.get("engine_id") or "")
    registry_symbols = [str(symbol).strip().upper() for symbol in row.get("allowed_symbols") or [] if str(symbol).strip()]
    engine_row = {**row, "engine_id": sleeve_id, "status": row.get("status") or ("ACTIVE" if row.get("enabled", True) else "DISABLED")}
    try:
        resolution = resolve_canonical_symbol_universe_v1(
            repo_root=repo_root,
            truth_root=truth_root,
            day_utc=day_utc,
            engine_id=sleeve_id,
            allow_deprecated_fallback=False,
            market_data_mode=market_data_mode,
        )
        symbols = [str(symbol).strip().upper() for symbol in resolution.symbols if str(symbol).strip()]
        uncapped_symbol_count = len(symbols)
        target_count = int(getattr(resolution, "policy_target_symbol_count", 0) or 0)
        dynamic_source = str(resolution.source or "") in {
            "engine_universe_candidate_basis_v1",
            "engine_universe_candidate_basis_v1.operational_latest_valid",
            "ranked_symbol_universe_v1",
            "market_data_snapshot_v1.dataset_manifest",
        }
        cap_applied = bool(dynamic_source and target_count > 0 and len(symbols) > target_count)
        if cap_applied:
            symbols = symbols[:target_count]
        return {
            "status": "RESOLVED" if symbols else "EMPTY",
            "engine_id": sleeve_id,
            "allowed_symbols": symbols,
            "registry_allowed_symbols": registry_symbols,
            "source": str(resolution.source or ""),
            "source_path": str(resolution.source_path or ""),
            "source_hash": _file_hash_if_exists_v1(Path(str(resolution.source_path or ""))),
            "source_day_utc": _source_day_from_path_v1(str(resolution.source_path or "")),
            "market_data_mode": str(market_data_mode or ""),
            "policy_target_symbol_count": target_count,
            "symbol_resolution_cap_applied": cap_applied,
            "uncapped_symbol_count": uncapped_symbol_count,
            "deprecated_registry_fallback_used": False,
        }
    except Exception as exc:
        return {
            "status": "BLOCKED",
            "engine_id": sleeve_id,
            "allowed_symbols": registry_symbols,
            "registry_allowed_symbols": registry_symbols,
            "source": "ENGINE_MODEL_REGISTRY_V1.allowed_symbols",
            "source_path": "",
            "source_hash": "",
            "source_day_utc": "",
            "market_data_mode": str(market_data_mode or ""),
            "deprecated_registry_fallback_used": True,
            "blocker": "CANONICAL_SYMBOL_UNIVERSE_UNAVAILABLE",
            "error": str(exc),
        }



def _file_hash_if_exists_v1(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() and path.is_file() else ""
    except Exception:
        return ""


def _source_day_from_path_v1(path_text: str) -> str:
    parts = Path(path_text).parts
    for part in parts:
        if len(part) == 10 and part[4] == "-" and part[7] == "-":
            return part
    return ""

def _symbol_universe_hash_v1(resolution: dict[str, Any]) -> str:
    payload = {
        "allowed_symbols": list(resolution.get("allowed_symbols") or []),
        "source": str(resolution.get("source") or ""),
        "source_path": str(resolution.get("source_path") or ""),
        "source_hash": str(resolution.get("source_hash") or ""),
        "source_day_utc": str(resolution.get("source_day_utc") or ""),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()
