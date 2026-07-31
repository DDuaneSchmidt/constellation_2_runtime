from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from ops.aegis.hash_lineage_v1 import build_hash_lineage_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1

REPORT_FAMILY = "aegis_input_contract_reconciliation_v1"
REPAIR_COMMAND = "TARGET_DAY={day} npm run aegis:repair-input-contracts"
_HASH_RE = re.compile(r"(?:file=(?P<file>\S+)\s+expected=(?P<expected>[a-fA-F0-9]{32,})\s+got=(?P<actual>[a-fA-F0-9]{32,})|(?P<path>/\S+)\s+manifest=(?P<manifest>[a-fA-F0-9]{32,})\s+actual=(?P<actual2>[a-fA-F0-9]{32,}))")


def build_input_contract_reconciliation_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    source_specs = {
        "sleeve_evaluation_rollup": ("sleeve_evaluation_kernel_v1", "sleeve_evaluation_rollup.v1.json"),
        "candidate_generation_diagnostics": ("aegis_candidate_generation_diagnostics_v1", "candidate_generation_diagnostics.v1.json"),
        "allowed_symbol_source_report": ("allowed_symbol_source_report_v1", "allowed_symbol_source_report.v1.json"),
        "sleeve_input_contracts": ("aegis_sleeve_input_contracts_v1", "sleeve_input_contracts.v1.json"),
        "sleeve_readiness": ("aegis_sleeve_readiness_v1", "sleeve_readiness.v1.json"),
        "market_data_inputs": ("market_data_inputs_v1", "market_data_inputs.v1.json"),
        "data_registry": ("aegis_data_registry_v1", "data_registry.v1.json"),
        "context_requirement_profile": ("aegis_context_requirement_profile_v1", "context_requirement_profile.v1.json"),
        "candidate_contracts": ("aegis_candidate_contracts_v1", "candidate_contracts.v1.json"),
    }
    sources: dict[str, dict[str, Any]] = {}
    payloads: dict[str, dict[str, Any]] = {}
    for key, (family, filename) in source_specs.items():
        path, payload = latest_json_v1(root, family, day_utc, filename)
        payloads[key] = payload
        sources[key] = {"family": family, "filename": filename, "path": str(path or ""), "found": bool(path and payload), "hash": _sha256(path) if path else ""}

    hash_lineage = build_hash_lineage_v1(truth_root=root, day_utc=day_utc)
    rollup_rows = _rows(payloads["sleeve_evaluation_rollup"], "outcomes", "rows")
    diag_rows = _diagnostic_rows(payloads["candidate_generation_diagnostics"])
    contracts_by_id = {str(row.get("sleeve_id") or ""): row for row in _rows(payloads["sleeve_input_contracts"], "contracts")}
    readiness_by_id = {str(row.get("sleeve_id") or ""): row for row in _rows(payloads["sleeve_readiness"], "sleeves")}
    allowed_by_id = {str(row.get("sleeve_id") or row.get("engine_id") or ""): row for row in _rows(payloads["allowed_symbol_source_report"], "rows")}
    market_records = {str(row.get("data_item_id") or ""): row for row in _rows(payloads["market_data_inputs"], "input_records")}

    ids = sorted({str(row.get("engine_id") or row.get("sleeve_id") or "") for row in rollup_rows + diag_rows if str(row.get("engine_id") or row.get("sleeve_id") or "")})
    reconciliation_rows = []
    for sleeve_id in ids:
        rollup = _first([row for row in rollup_rows if str(row.get("engine_id") or row.get("sleeve_id") or "") == sleeve_id])
        diag = _first([row for row in diag_rows if str(row.get("engine_id") or row.get("sleeve_id") or "") == sleeve_id])
        contract = contracts_by_id.get(sleeve_id, {})
        readiness = readiness_by_id.get(sleeve_id, {})
        allowed = allowed_by_id.get(sleeve_id, {})
        reason_codes = sorted({str(item) for item in (rollup.get("reason_codes") or []) + (diag.get("reason_codes") or []) if str(item)})
        if rollup.get("canonical_blocker"):
            reason_codes.append(str(rollup.get("canonical_blocker")))
        if diag.get("canonical_blocker"):
            reason_codes.append(str(diag.get("canonical_blocker")))
        reason_codes = sorted(set(reason_codes))
        expected_symbols = _string_list(contract.get("allowed_symbols") or (contract.get("symbol_resolution") or {}).get("allowed_symbols") or allowed.get("canonical_allowed_symbols"))
        actual_symbols = _string_list(rollup.get("producer_requested_symbols") or diag.get("producer_requested_symbols") or rollup.get("registry_allowed_symbols") or allowed.get("deprecated_registry_symbols"))
        rejected_intents = _rows(rollup, "rejected_intents")
        stale_paths = [str(row.get("intent_path") or "") for row in rejected_intents if row.get("intent_path")]
        missing_required = _string_list(readiness.get("blocking_inputs"))
        hash_detail = _hash_detail(rollup.get("market_data_manifest_check"), str(rollup.get("stderr_summary") or diag.get("stderr_summary") or ""))
        contract_market_hash = str(contract.get("market_data_hash") or "")
        current_market_hash = str(hash_lineage.get("market_data_hash") or "")
        contract_market_stale = bool(current_market_hash and contract and contract_market_hash != current_market_hash)
        effective_hash_detail = hash_detail if hash_detail.get("status") == "MISMATCH" else ({"status": "MISMATCH", "expected_hash": contract_market_hash, "actual_hash": current_market_hash, "file": "market_data.v1.json", "path": str((hash_lineage.get("market_data") or {}).get("path") or ""), "source": "aegis_hash_lineage_v1"} if contract_market_stale else hash_detail)
        row = {
            "sleeve_id": sleeve_id,
            "producer_id": str(rollup.get("producer_id") or diag.get("producer_id") or rollup.get("engine_runner") or diag.get("source_generation_command") or ""),
            "contract_status": _contract_status(contract, readiness, reason_codes),
            "allowed_symbol_status": "MISMATCH" if set(expected_symbols) != set(actual_symbols) and actual_symbols else "OK",
            "market_data_hash_status": "MISMATCH" if contract_market_stale or hash_detail.get("status") == "MISMATCH" or any(code in {"MARKET_DATA_SHA_MISMATCH", "SHA256_MISMATCH"} for code in reason_codes) else "OK",
            "input_freshness_status": "BLOCKED" if missing_required else "OK",
            "stale_intent_status": "INVALIDATED_OR_REJECTED" if stale_paths or "STALE_MISMATCHED_INTENT_REJECTED" in reason_codes else "OK",
            "missing_required_inputs": missing_required,
            "repair_action": REPAIR_COMMAND.format(day=day_utc),
            "expected_input_artifact": str(sources["sleeve_input_contracts"].get("path") or ""),
            "actual_input_artifact": str(sources["sleeve_evaluation_rollup"].get("path") or sources["candidate_generation_diagnostics"].get("path") or ""),
            "expected_hash": str(contract_market_hash or contract.get("symbol_universe_hash") or contract.get("contract_hash") or sources["sleeve_input_contracts"].get("hash") or ""),
            "actual_hash": str(hash_detail.get("actual_hash") or current_market_hash or rollup.get("symbol_source_hash") or diag.get("actual_hash") or ""),
            "expected_market_data_hash": contract_market_hash or str(hash_detail.get("expected_hash") or ""),
            "actual_market_data_hash": str(hash_detail.get("actual_hash") or current_market_hash or ""),
            "expected_market_data_artifact_path": str(contract.get("market_data_artifact_path") or sources["sleeve_input_contracts"].get("path") or ""),
            "actual_market_data_artifact_path": str((hash_lineage.get("market_data") or {}).get("path") or hash_detail.get("path") or ""),
            "expected_market_data_generated_at_utc": str(contract.get("market_data_generated_at_utc") or contract.get("generated_at_utc") or payloads["sleeve_input_contracts"].get("generated_at_utc") or ""),
            "actual_market_data_generated_at_utc": str((hash_lineage.get("market_data") or {}).get("generated_at_utc") or ""),
            "expected_market_data_generated_by_command": str(contract.get("market_data_generated_by_command") or contract.get("generated_by_command") or f"TARGET_DAY={day_utc} npm run aegis:sleeve-input-contracts"),
            "actual_market_data_generated_by_command": str((hash_lineage.get("market_data") or {}).get("command") or f"TARGET_DAY={day_utc} npm run aegis:refresh-market-data"),
            "expected_allowed_symbols": expected_symbols,
            "actual_symbols": actual_symbols,
            "symbol_universe_hash": str(contract.get("symbol_universe_hash") or ""),
            "registry_allowed_symbols": _string_list(contract.get("registry_allowed_symbols") or rollup.get("registry_allowed_symbols")),
            "source_generation_command": str(rollup.get("command") or diag.get("source_generation_command") or ""),
            "stale_artifact_path": stale_paths[0] if stale_paths else "",
            "stale_artifact_paths": stale_paths,
            "current_artifact_path": str(sources["sleeve_evaluation_rollup"].get("path") or ""),
            "exact_mismatch_cause": _cause(reason_codes, readiness, effective_hash_detail),
            "blocking_current_day_valid_candidates": bool(contract_market_stale or _blocking(reason_codes, readiness, rollup, diag)),
            "reason_codes": reason_codes,
            "source_paths": {key: str(value.get("path") or "") for key, value in sources.items()},
            "source_hashes": {key: str(value.get("hash") or "") for key, value in sources.items()},
            "market_data_hash_detail": effective_hash_detail,
            "market_data_manifest_check": rollup.get("market_data_manifest_check") if isinstance(rollup.get("market_data_manifest_check"), dict) else {},
            "readiness_blocking_inputs": missing_required,
            "vix_context_requirement": _vix_requirement(payloads["context_requirement_profile"]),
            "market_input_records": [_market_record_summary(market_records.get(item, {})) for item in missing_required],
        }
        reconciliation_rows.append(row)
    blocking_rows = [row for row in reconciliation_rows if row.get("blocking_current_day_valid_candidates")]
    coverage_path, coverage_payload = latest_json_v1(root, "aegis_market_data_coverage_v1", day_utc, "market_data_coverage.v1.json")
    coverage_ready_hash_mismatch = bool(str(coverage_payload.get("status") or "").upper() == "READY" and any(row.get("market_data_hash_status") == "MISMATCH" for row in reconciliation_rows))
    return {
        "schema_id": "aegis_input_contract_reconciliation",
        "schema_version": "v1",
        "artifact_id": "aegis_input_contract_reconciliation_v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "status": "BLOCKED" if blocking_rows else "READY",
        "row_count": len(reconciliation_rows),
        "blocking_row_count": len(blocking_rows),
        "repair_command": REPAIR_COMMAND.format(day=day_utc),
        "coverage_ready_hash_mismatch_message": "Coverage is ready, but downstream sleeve contracts were generated against an older market-data hash. Run repair-input-contracts." if coverage_ready_hash_mismatch else "",
        "hash_lineage": hash_lineage,
        "rows": reconciliation_rows,
        "sources": sources,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }


def write_input_contract_reconciliation_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "input_contract_reconciliation.v1.json", payload)
    txt_path = out_dir / "input_contract_reconciliation.v1.txt"
    txt_path.write_text(render_input_contract_reconciliation_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "txt": str(txt_path)}


def render_input_contract_reconciliation_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS INPUT CONTRACT RECONCILIATION v1",
        f"day_utc: {payload.get('day_utc')}",
        f"status: {payload.get('status')}",
        f"blocking_row_count: {payload.get('blocking_row_count')}",
        f"repair_command: {payload.get('repair_command')}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "trade_advice_allowed: false",
        "",
        "rows:",
    ]
    for row in payload.get("rows") or []:
        lines.append(f"- {row.get('sleeve_id')}: contract={row.get('contract_status')} symbols={row.get('allowed_symbol_status')} market_hash={row.get('market_data_hash_status')} freshness={row.get('input_freshness_status')} stale_intent={row.get('stale_intent_status')} cause={row.get('exact_mismatch_cause')}")
    return "\n".join(lines) + "\n"


def _rows(payload: dict[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        value = payload.get(key) if isinstance(payload, dict) else None
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return []


def _diagnostic_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("failed_producers", "producer_rows", "rejected_candidates", "candidate_rejections"):
        rows.extend(_rows(payload, key))
    seen: set[str] = set()
    out = []
    for row in rows:
        ident = json.dumps(row, sort_keys=True, default=str)
        if ident not in seen:
            seen.add(ident)
            out.append(row)
    return out


def _first(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return rows[0] if rows else {}


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return sorted({str(item).strip().upper() for item in value if str(item).strip()})


def _sha256(path: Path | None) -> str:
    try:
        return __import__('hashlib').sha256(Path(path).read_bytes()).hexdigest() if path else ""
    except Exception:
        return ""


def _hash_detail(check: Any, stderr: str) -> dict[str, Any]:
    if isinstance(check, dict):
        mismatches = check.get("hash_mismatches") if isinstance(check.get("hash_mismatches"), list) else []
        if mismatches:
            first = mismatches[0]
            return {"status": "MISMATCH", "file": first.get("file", ""), "expected_hash": first.get("expected_sha256", ""), "actual_hash": first.get("actual_sha256", ""), "path": first.get("path", ""), "source": "market_data_manifest_check"}
    match = _HASH_RE.search(stderr or "")
    if not match:
        return {"status": "OK", "expected_hash": "", "actual_hash": ""}
    expected = match.group("expected") or match.group("manifest") or ""
    actual = match.group("actual") or match.group("actual2") or ""
    return {"status": "MISMATCH", "file": match.group("file") or "", "path": match.group("path") or "", "expected_hash": expected, "actual_hash": actual, "source": "stderr"}


def _contract_status(contract: dict[str, Any], readiness: dict[str, Any], reason_codes: list[str]) -> str:
    if not contract:
        return "MISSING"
    if "MISSING_REQUIRED_INPUTS" in reason_codes or readiness.get("blocking_inputs"):
        return "INPUTS_BLOCKED"
    return str(contract.get("contract_status") or readiness.get("contract_status") or "OK")


def _cause(reason_codes: list[str], readiness: dict[str, Any], hash_detail: dict[str, Any]) -> str:
    if hash_detail.get("status") == "MISMATCH":
        return f"MARKET_DATA_SHA_MISMATCH expected={hash_detail.get('expected_hash')} actual={hash_detail.get('actual_hash')} file={hash_detail.get('file') or hash_detail.get('path')}"
    if readiness.get("blocking_inputs"):
        return "MISSING_OR_STALE_REQUIRED_INPUTS: " + ",".join(str(item) for item in readiness.get("blocking_inputs") or [])
    if reason_codes:
        return ",".join(reason_codes)
    return "NO_MISMATCH_DETECTED"


def _blocking(reason_codes: list[str], readiness: dict[str, Any], rollup: dict[str, Any], diag: dict[str, Any]) -> bool:
    if str(rollup.get("status") or diag.get("status") or "").upper() == "BLOCKED":
        return True
    if readiness.get("blocking_inputs"):
        return True
    return any(code in {"ALLOWED_SYMBOL_MISMATCH", "MARKET_DATA_SHA_MISMATCH", "SHA256_MISMATCH", "MISSING_REQUIRED_INPUTS", "SLEEVE_INPUT_REQUIREMENT_BLOCKED"} for code in reason_codes)


def _vix_requirement(profile: dict[str, Any]) -> dict[str, Any]:
    active = profile.get("active_profile") if isinstance(profile.get("active_profile"), dict) else {}
    for row in active.get("requirements") if isinstance(active.get("requirements"), list) else []:
        if isinstance(row, dict) and str(row.get("required_evidence_item") or "") == "market.volatility.VIX":
            return row
    return {}


def _market_record_summary(row: dict[str, Any]) -> dict[str, Any]:
    if not row:
        return {}
    return {key: row.get(key) for key in ("data_item_id", "validation_status", "status", "reason", "source_timestamp_utc", "source_path", "raw_source_hash", "transformed_value_hash")}
