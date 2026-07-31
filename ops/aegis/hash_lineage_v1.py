from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1

REPORT_FAMILY = "aegis_hash_lineage_v1"
REPAIR_COMMAND = "TARGET_DAY={day} npm run aegis:repair-input-contracts"


def sha256_file_v1(path: Path | None) -> str:
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest() if path else ""
    except Exception:
        return ""


def current_market_data_ref_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    path, payload = latest_json_v1(root, "aegis_market_data_v1", day_utc, "market_data.v1.json")
    if not path:
        return {"hash": "", "path": "", "generated_at_utc": "", "command": "TARGET_DAY={day} npm run aegis:refresh-market-data".format(day=day_utc)}
    return {
        "hash": str(payload.get("market_data_snapshot_hash") or sha256_file_v1(path)),
        "artifact_hash": sha256_file_v1(path),
        "snapshot_hash": str(payload.get("market_data_snapshot_hash") or ""),
        "snapshot_path": str(payload.get("market_data_snapshot_path") or ""),
        "path": str(path),
        "generated_at_utc": str(payload.get("generated_at_utc") or ""),
        "command": "TARGET_DAY={day} npm run aegis:refresh-market-data".format(day=day_utc),
    }


def artifact_ref_v1(*, truth_root: Path, family: str, day_utc: str, filename: str, command: str) -> dict[str, Any]:
    path, payload = latest_json_v1(Path(truth_root).expanduser().resolve(), family, day_utc, filename)
    return {
        "hash": sha256_file_v1(path),
        "path": str(path or ""),
        "generated_at_utc": str(payload.get("generated_at_utc") or payload.get("generated_at") or ""),
        "command": command,
        "found": bool(path and payload),
    }


def sleeve_contract_market_hashes_v1(payload: dict[str, Any]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in payload.get("contracts") if isinstance(payload.get("contracts"), list) else []:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("sleeve_id") or "")
        if not sleeve_id:
            continue
        out[sleeve_id] = {
            "contract_hash": hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest(),
            "market_data_hash": str(row.get("market_data_hash") or ""),
            "market_data_artifact_path": str(row.get("market_data_artifact_path") or ""),
            "generated_at_utc": str(row.get("generated_at_utc") or payload.get("generated_at_utc") or ""),
            "generated_by_command": str(row.get("generated_by_command") or ("TARGET_DAY=" + str(payload.get("day_utc") or "") + " npm run aegis:sleeve-input-contracts")),
        }
    return out


def _sleeve_evaluation_market_hash_mismatches_v1(payload: dict[str, Any]) -> list[dict[str, str]]:
    mismatches: list[dict[str, str]] = []
    rows = payload.get("outcomes") if isinstance(payload.get("outcomes"), list) else payload.get("rows")
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        check = row.get("market_data_manifest_check") if isinstance(row.get("market_data_manifest_check"), dict) else {}
        raw = check.get("hash_mismatches") if isinstance(check.get("hash_mismatches"), list) else []
        for item in raw:
            if not isinstance(item, dict):
                continue
            mismatches.append(
                {
                    "sleeve_id": str(row.get("engine_id") or row.get("sleeve_id") or ""),
                    "symbol": str(item.get("symbol") or ""),
                    "file": str(item.get("file") or ""),
                    "expected_sha256": str(item.get("expected_sha256") or ""),
                    "actual_sha256": str(item.get("actual_sha256") or ""),
                    "path": str(item.get("path") or ""),
                }
            )
    return mismatches


def _unique_actions(actions: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for action in actions:
        if action and action not in seen:
            seen.add(action)
            out.append(action)
    return out


def build_hash_lineage_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    market_data = current_market_data_ref_v1(truth_root=root, day_utc=day_utc)
    data_registry = artifact_ref_v1(
        truth_root=root,
        family="aegis_data_registry_v1",
        day_utc=day_utc,
        filename="data_registry.v1.json",
        command=f"TARGET_DAY={day_utc} npm run aegis:data-registry",
    )
    market_inputs = artifact_ref_v1(
        truth_root=root,
        family="market_data_inputs_v1",
        day_utc=day_utc,
        filename="market_data_inputs.v1.json",
        command=f"TARGET_DAY={day_utc} npm run aegis:market-data-inputs",
    )
    candidate_contracts = artifact_ref_v1(
        truth_root=root,
        family="aegis_candidate_contracts_v1",
        day_utc=day_utc,
        filename="candidate_contracts.v1.json",
        command=f"TARGET_DAY={day_utc} npm run aegis:candidate-contracts",
    )
    diagnostics = artifact_ref_v1(
        truth_root=root,
        family="aegis_candidate_generation_diagnostics_v1",
        day_utc=day_utc,
        filename="candidate_generation_diagnostics.v1.json",
        command=f"TARGET_DAY={day_utc} npm run aegis:candidate-diagnostics",
    )
    sleeve_eval_path, sleeve_eval_payload = latest_json_v1(root, "sleeve_evaluation_kernel_v1", day_utc, "sleeve_evaluation_rollup.v1.json")
    sleeve_eval = {
        "hash": sha256_file_v1(sleeve_eval_path),
        "path": str(sleeve_eval_path or ""),
        "generated_at_utc": str(sleeve_eval_payload.get("generated_at_utc") or sleeve_eval_payload.get("generated_at") or ""),
        "command": f"TARGET_DAY={day_utc} python3 ops/tools/run_sleeve_evaluation_kernel_v1.py",
        "found": bool(sleeve_eval_path and sleeve_eval_payload),
    }
    sleeve_path, sleeve_payload = latest_json_v1(root, "aegis_sleeve_input_contracts_v1", day_utc, "sleeve_input_contracts.v1.json")
    sleeve_hashes = sleeve_contract_market_hashes_v1(sleeve_payload)
    current_market_hash = str(market_data.get("hash") or "")
    stale_sleeves = sorted(
        sleeve_id
        for sleeve_id, row in sleeve_hashes.items()
        if current_market_hash and str(row.get("market_data_hash") or "") != current_market_hash
    )
    stale_downstream: list[dict[str, Any]] = []
    actions: list[str] = []
    if stale_sleeves:
        stale_downstream.append(
            {
                "artifact_family": "aegis_sleeve_input_contracts_v1",
                "artifact_path": str(sleeve_path or ""),
                "reason": "MARKET_DATA_HASH_CHANGED_AFTER_SLEEVE_INPUT_CONTRACT_GENERATION",
                "stale_sleeve_ids": stale_sleeves,
                "expected_market_data_hash": current_market_hash,
                "repair_action": f"TARGET_DAY={day_utc} npm run aegis:sleeve-input-contracts",
            }
        )
        actions.append(REPAIR_COMMAND.format(day=day_utc))
    sleeve_eval_mismatches = _sleeve_evaluation_market_hash_mismatches_v1(sleeve_eval_payload)
    if sleeve_eval_mismatches:
        stale_downstream.append(
            {
                "artifact_family": "sleeve_evaluation_kernel_v1",
                "artifact_path": str(sleeve_eval_path or ""),
                "reason": "SLEEVE_EVALUATION_ROLLUP_REFERENCES_STALE_MARKET_DATA_MANIFEST_HASHES",
                "hash_mismatches": sleeve_eval_mismatches,
                "repair_action": f"TARGET_DAY={day_utc} npm run aegis:repair-input-contracts",
            }
        )
        actions.append(f"TARGET_DAY={day_utc} npm run aegis:repair-input-contracts")
    generated_at_timestamps = {
        "market_data": str(market_data.get("generated_at_utc") or ""),
        "data_registry": str(data_registry.get("generated_at_utc") or ""),
        "market_data_inputs": str(market_inputs.get("generated_at_utc") or ""),
        "sleeve_input_contracts": str(sleeve_payload.get("generated_at_utc") or ""),
        "sleeve_evaluation_rollup": str(sleeve_eval.get("generated_at_utc") or ""),
        "candidate_contracts": str(candidate_contracts.get("generated_at_utc") or ""),
        "diagnostics": str(diagnostics.get("generated_at_utc") or ""),
    }
    return {
        "schema_id": "aegis_hash_lineage",
        "schema_version": "v1",
        "artifact_id": "aegis_hash_lineage_v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "market_data_hash": current_market_hash,
        "market_data": market_data,
        "data_registry_hash": data_registry.get("hash") or "",
        "data_registry": data_registry,
        "market_data_inputs_hash": market_inputs.get("hash") or "",
        "market_data_inputs": market_inputs,
        "sleeve_input_contract_hashes": sleeve_hashes,
        "sleeve_input_contracts": {
            "hash": sha256_file_v1(sleeve_path),
            "path": str(sleeve_path or ""),
            "generated_at_utc": str(sleeve_payload.get("generated_at_utc") or ""),
            "command": f"TARGET_DAY={day_utc} npm run aegis:sleeve-input-contracts",
        },
        "sleeve_evaluation_rollup_hash": sleeve_eval.get("hash") or "",
        "sleeve_evaluation_rollup": sleeve_eval,
        "candidate_contract_hash": candidate_contracts.get("hash") or "",
        "candidate_contracts": candidate_contracts,
        "diagnostics_hash": diagnostics.get("hash") or "",
        "diagnostics": diagnostics,
        "generated_at_timestamps": generated_at_timestamps,
        "stale_downstream_artifacts": stale_downstream,
        "required_regeneration_actions": _unique_actions(actions),
        "status": "STALE_DOWNSTREAM_ARTIFACTS" if stale_downstream else "PASS",
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }

def write_hash_lineage_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "hash_lineage.v1.json", payload)
    return {"json": str(json_path)}


def write_current_hash_lineage_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    payload = build_hash_lineage_v1(truth_root=truth_root, day_utc=day_utc)
    paths = write_hash_lineage_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    return {**payload, "paths": paths}


def stale_sleeve_contracts_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    return build_hash_lineage_v1(truth_root=truth_root, day_utc=day_utc)
