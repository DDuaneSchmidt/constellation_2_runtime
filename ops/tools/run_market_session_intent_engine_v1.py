#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, read_json_object_v1, resolve_fact_plane_truth_root_v1, resolve_paper_intent_truth_root_v1
from ops.tools.run_intent_arbitration_v1 import build_intent_arbitration, selected_intent_pointer_path
from ops.tools import run_sleeve_evaluation_kernel_v1 as sleeve_kernel

PAPER_MODE = "PAPER"
SCAN_SCHEMA_ID = "sleeve_scan_session"
SCAN_SCHEMA_VERSION = "v1"
MARKET_TZ = ZoneInfo("America/New_York")
SLEEVE_CONTRACTS_RELPATH = "governance/02_REGISTRIES/SLEEVE_CONTRACTS_V1.json"


def _now_dt() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _now_iso() -> str:
    return _now_dt().isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as f:
        f.write(_canonical_bytes(payload) + b"\n")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return read_json_object_v1(path)
    except Exception:
        return {}


def _duration_ms(started: datetime, completed: datetime) -> int:
    return max(0, int((completed - started).total_seconds() * 1000))


def _default_cycle_id() -> str:
    return "scan_" + _now_dt().strftime("%Y%m%dT%H%M%SZ")


def _is_after_market_close_now() -> bool:
    now_et = datetime.now(MARKET_TZ)
    return now_et.hour > 16 or (now_et.hour == 16 and now_et.minute >= 0)


def _compact_cycle_output(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": payload["status"],
        "canonical_blocker": payload["canonical_blocker"],
        "cycle_id": payload["cycle_id"],
        "artifact_root": payload["artifact_root"],
        "ledger_path": payload["ledger_path"],
        "operator_status_path": payload["operator_status_path"],
        "sleeve_scan_rollup_path": payload["sleeve_scan_rollup_path"],
        "intent_arbitration_path": payload["intent_arbitration_path"],
        "latest_scan_cycle_pointer_path": payload["latest_scan_cycle_pointer_path"],
        "selected_intent_pointer_path": payload["selected_intent_pointer_path"],
        "preflight_readiness_matrix_path": payload.get("preflight_readiness_matrix_path", ""),
        "operator_readiness_summary_path": payload.get("operator_readiness_summary_path", ""),
        "operator_status": payload["operator_status"],
    }


def _update_cycle_next_scan_at(payload: dict[str, Any], next_scan_at_utc: str | None) -> None:
    operator_status = payload.get("operator_status") if isinstance(payload.get("operator_status"), dict) else {}
    operator_path = Path(str(payload.get("operator_status_path") or ""))
    if not operator_status or not str(operator_path):
        return
    operator_status["next_scan_at_utc"] = next_scan_at_utc
    payload["operator_status"] = operator_status
    _write_json(operator_path, operator_status)


@dataclass(frozen=True)
class ScanCycleResult:
    cycle_id: str
    day_utc: str
    environment: str
    status: str
    sleeves_evaluated: int
    sleeves_expected: int
    selected_intent_id: str | None
    canonical_blocker: str | None
    artifact_root: str
    cycle_manifest_path: str
    scan_rollup_path: str
    arbitration_result_path: str
    operator_status_path: str
    preflight_readiness_matrix_path: str
    operator_readiness_summary_path: str
    selected_pointer_path: str
    latest_scan_pointer_path: str
    ledger_path: str


def sleeve_scan_cycle_root(*, truth_root: Path, day_utc: str, cycle_id: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "sleeve_scan_session_v1" / day_utc / cycle_id


def _unique_cycle_id(*, truth_root: Path, day_utc: str, requested_cycle_id: str) -> str:
    base = str(requested_cycle_id or "").strip() or _default_cycle_id()
    candidate = base
    idx = 1
    while sleeve_scan_cycle_root(truth_root=truth_root, day_utc=day_utc, cycle_id=candidate).exists():
        candidate = f"{base}_{idx:03d}"
        idx += 1
    return candidate


def cycle_manifest_path(*, truth_root: Path, day_utc: str, cycle_id: str) -> Path:
    return sleeve_scan_cycle_root(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id) / "cycle_manifest.v1.json"


def sleeve_scan_rollup_path(*, truth_root: Path, day_utc: str, cycle_id: str) -> Path:
    return sleeve_scan_cycle_root(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id) / "scan_rollup.v1.json"


def arbitration_result_path(*, truth_root: Path, day_utc: str, cycle_id: str) -> Path:
    return sleeve_scan_cycle_root(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id) / "arbitration_result.v1.json"


def operator_status_path(*, truth_root: Path, day_utc: str, cycle_id: str) -> Path:
    return sleeve_scan_cycle_root(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id) / "operator_status.v1.json"


def preflight_readiness_matrix_path(*, truth_root: Path, day_utc: str, cycle_id: str) -> Path:
    return sleeve_scan_cycle_root(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id) / "preflight_readiness_matrix.v1.json"


def operator_readiness_summary_path(*, truth_root: Path, day_utc: str, cycle_id: str) -> Path:
    return sleeve_scan_cycle_root(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id) / "operator_readiness_summary.v1.json"


def scan_ledger_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "scan_ledger_v1" / f"{day_utc}.scan_ledger.v1.jsonl"


def latest_scan_cycle_pointer_path(*, truth_root: Path) -> Path:
    return Path(truth_root).resolve() / "pointers" / "latest_scan_cycle_pointer.v1.json"


def sleeve_outcome_path(*, truth_root: Path, day_utc: str, cycle_id: str, sleeve_id: str) -> Path:
    return sleeve_scan_cycle_root(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id) / "sleeves" / sleeve_id / "sleeve_outcome.v1.json"


def _previous_cycle_outcome(*, truth_root: Path, day_utc: str, cycle_id: str, sleeve_id: str) -> dict[str, Any]:
    day_root = Path(truth_root).resolve() / "reports" / "sleeve_scan_session_v1" / day_utc
    if not day_root.exists():
        return {}
    current = str(cycle_id)
    candidates: list[Path] = []
    for path in day_root.glob(f"*/sleeves/{sleeve_id}/sleeve_outcome.v1.json"):
        if path.parts[-4] == current:
            continue
        candidates.append(path)
    if not candidates:
        return {}
    return _read_json(sorted(candidates, key=lambda path: str(path))[-1])


def _scan_registry_rows() -> list[dict[str, Any]]:
    registry = sleeve_kernel._load_engine_registry()
    rows: list[dict[str, Any]] = []
    for row in registry.get("engines") if isinstance(registry.get("engines"), list) else []:
        if not isinstance(row, dict):
            continue
        if str(row.get("engine_id") or "").strip() == sleeve_kernel.SIMULATOR_ENGINE_ID:
            continue
        rows.append(row)
    return rows


def _cycle_manifest_payload(
    *,
    day_utc: str,
    environment: str,
    truth_root: Path,
    sleeve_truth_root: Path,
    cycle_id: str,
    rows: list[dict[str, Any]],
    created_at_utc: str,
) -> dict[str, Any]:
    registry_path = sleeve_kernel._engine_registry_path()
    engine_ids = [str(row.get("engine_id") or "").strip() for row in rows]
    return {
        "schema_id": "cycle_manifest",
        "schema_version": "v1",
        "cycle_id": cycle_id,
        "day_utc": day_utc,
        "environment": environment,
        "created_at_utc": created_at_utc,
        "truth_root": str(Path(truth_root).resolve()),
        "sleeve_truth_root": str(Path(sleeve_truth_root).resolve()),
        "registry_path": str(registry_path),
        "registry_hash": sleeve_kernel._sha256_file(registry_path),
        "sleeves_expected": len(rows),
        "sleeve_engine_ids": engine_ids,
        "activation_status_by_engine": {engine_id: sleeve_kernel._status_from_registry(row) for engine_id, row in zip(engine_ids, rows, strict=False)},
        "allowed_symbols_by_engine": {engine_id: sleeve_kernel._allowed_symbols(row) for engine_id, row in zip(engine_ids, rows, strict=False)},
        "symbol_policy_source": "ENGINE_MODEL_REGISTRY_V1.allowed_symbols",
        "deprecated_symbol_universe_used": False,
        "submit_enabled": False,
        "artifact_root": str(sleeve_scan_cycle_root(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id)),
    }


def _adapt_outcome_for_scan(outcome: dict[str, Any], *, truth_root: Path, day_utc: str, cycle_id: str) -> dict[str, Any]:
    sleeve_id = str(outcome.get("sleeve_id") or outcome.get("engine_id") or "").strip()
    adapted = dict(outcome)
    adapted["schema_id"] = "sleeve_outcome"
    adapted["schema_version"] = "v1"
    adapted["cycle_id"] = cycle_id
    adapted["artifact_path"] = str(sleeve_outcome_path(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id, sleeve_id=sleeve_id))
    return adapted


def _sleeve_contracts_path() -> Path:
    return (REPO_ROOT / SLEEVE_CONTRACTS_RELPATH).resolve()


def _load_sleeve_contracts() -> dict[str, dict[str, Any]]:
    payload = _read_json(_sleeve_contracts_path())
    contracts: dict[str, dict[str, Any]] = {}
    for contract in payload.get("contracts") if isinstance(payload.get("contracts"), list) else []:
        if not isinstance(contract, dict):
            continue
        engine_id = str(contract.get("engine_id") or "").strip()
        if engine_id:
            contracts[engine_id] = contract
    return contracts


def _load_sleeve_dataset_readiness(*, truth_root: Path, day_utc: str) -> dict[str, dict[str, Any]]:
    path = (
        Path(truth_root).resolve()
        / "reports"
        / "sleeve_dataset_readiness_v1"
        / day_utc
        / "sleeve_dataset_readiness.v1.json"
    )
    payload = _read_json(path)
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    return {
        str(row.get("sleeve_id") or "").strip(): {**row, "validation_artifact_path": str(path)}
        for row in rows
        if isinstance(row, dict) and str(row.get("sleeve_id") or "").strip()
    }


def _contract_paths(
    *,
    input_contract: dict[str, Any],
    day_utc: str,
    allowed_symbols: list[str],
    sleeve_truth_root: Path,
    canonical_truth_root: Path,
) -> list[dict[str, Any]]:
    owner = str(input_contract.get("truth_owner") or "").strip()
    scope = str(input_contract.get("required_scope") or "").strip()
    template = str(input_contract.get("path_template") or "").strip()
    if not template:
        return []
    symbols = allowed_symbols if scope == "allowed_symbols" else [""]
    root = canonical_truth_root if owner == "canonical_truth" else sleeve_truth_root
    if owner == "registry":
        root = REPO_ROOT
    if owner not in {"canonical_truth", "sleeve_truth_root", "registry"}:
        return []
    out: list[dict[str, Any]] = []
    for symbol in symbols:
        rel = template.format(day=day_utc, symbol=symbol, year=day_utc[:4], environment=PAPER_MODE)
        path = (root / rel).resolve()
        out.append(
            {
                "name": str(input_contract.get("name") or ""),
                "truth_owner": owner,
                "required_scope": scope,
                "expected_artifact_type": str(input_contract.get("expected_artifact_type") or "unknown"),
                "symbol": symbol,
                "path": str(path),
                "exists": path.exists(),
                "required": bool(input_contract.get("required", True)),
            }
        )
    return out


def build_preflight_readiness_matrix_v1(
    *,
    day_utc: str,
    environment: str,
    truth_root: Path,
    sleeve_truth_root: Path,
    cycle_id: str,
    rows: list[dict[str, Any]],
    outcomes: list[dict[str, Any]],
) -> dict[str, Any]:
    contracts = _load_sleeve_contracts()
    dataset_readiness = _load_sleeve_dataset_readiness(truth_root=truth_root, day_utc=day_utc)
    canonical_truth_root = sleeve_kernel._canonical_truth_root()
    outcomes_by_engine = {str(row.get("engine_id") or "").strip(): row for row in outcomes if isinstance(row, dict)}
    matrix_rows: list[dict[str, Any]] = []
    for row in rows:
        engine_id = str(row.get("engine_id") or "").strip()
        activation_status = sleeve_kernel._status_from_registry(row)
        allowed_symbols = sleeve_kernel._allowed_symbols(row)
        contract = contracts.get(engine_id)
        dataset_row = dataset_readiness.get(engine_id, {})
        outcome = outcomes_by_engine.get(engine_id, {})
        candidate_intents = outcome.get("output_intents") if isinstance(outcome.get("output_intents"), list) else []
        required_inputs = contract.get("required_inputs") if isinstance(contract, dict) and isinstance(contract.get("required_inputs"), list) else []
        available_inputs: list[dict[str, Any]] = []
        missing_inputs: list[dict[str, Any]] = []
        unknown_inputs: list[dict[str, Any]] = []
        local_available = True
        canonical_available = True
        if activation_status != "ACTIVE":
            readiness_status = "DISABLED"
            readiness_reason = "ENGINE_INACTIVE"
        elif not contract:
            readiness_status = "UNKNOWN"
            readiness_reason = "SLEEVE_CONTRACT_MISSING"
        elif not required_inputs:
            readiness_status = "UNKNOWN"
            readiness_reason = "NO_REQUIRED_INPUT_CONTRACTS_DECLARED"
        else:
            for input_contract in required_inputs:
                expanded = _contract_paths(
                    input_contract=input_contract,
                    day_utc=day_utc,
                    allowed_symbols=allowed_symbols,
                    sleeve_truth_root=sleeve_truth_root,
                    canonical_truth_root=canonical_truth_root,
                )
                if not expanded:
                    unknown_inputs.append(
                        {
                            "name": str(input_contract.get("name") or ""),
                            "truth_owner": str(input_contract.get("truth_owner") or ""),
                            "unknown_reason": "UNRESOLVED_INPUT_CONTRACT_PATH",
                            "required": bool(input_contract.get("required", True)),
                        }
                    )
                    continue
                for item in expanded:
                    if item["exists"]:
                        available_inputs.append(item)
                    elif item["required"]:
                        missing_inputs.append(item)
                        if item["truth_owner"] == "sleeve_truth_root":
                            local_available = False
                        if item["truth_owner"] == "canonical_truth":
                            canonical_available = False
            if missing_inputs:
                readiness_status = "BLOCKED"
                readiness_reason = "REQUIRED_INPUT_MISSING"
            elif unknown_inputs:
                readiness_status = "UNKNOWN"
                readiness_reason = "UNKNOWN_INPUT_CONTRACT"
            elif str(outcome.get("status") or "") == "BLOCKED":
                readiness_status = "BLOCKED"
                readiness_reason = str(outcome.get("canonical_blocker") or "LATEST_SLEEVE_BLOCKED")
            else:
                readiness_status = "READY"
                readiness_reason = "REQUIRED_INPUTS_AVAILABLE"
        matrix_rows.append(
            {
                "engine_id": engine_id,
                "activation_status": activation_status,
                "allowed_symbols": allowed_symbols,
                "symbol_policy_source": "ENGINE_MODEL_REGISTRY_V1.allowed_symbols",
                "contract_found": bool(contract),
                "producer_entrypoint": str((contract or {}).get("producer_entrypoint") or row.get("engine_runner_path") or ""),
                "required_inputs": required_inputs,
                "available_inputs": available_inputs,
                "missing_inputs": missing_inputs,
                "unknown_inputs": unknown_inputs,
                "local_truth_available": local_available,
                "canonical_truth_available": canonical_available,
                "latest_sleeve_blocker": str(outcome.get("canonical_blocker") or ""),
                "candidate_intent_present": bool(candidate_intents),
                "dataset_ready": bool(dataset_row.get("dataset_ready")) if dataset_row else False,
                "missing_symbols": dataset_row.get("missing_symbols") if isinstance(dataset_row.get("missing_symbols"), list) else [],
                "stale_symbols": dataset_row.get("stale_symbols") if isinstance(dataset_row.get("stale_symbols"), list) else [],
                "insufficient_history_symbols": dataset_row.get("insufficient_history_symbols") if isinstance(dataset_row.get("insufficient_history_symbols"), list) else [],
                "validation_artifact_path": str(dataset_row.get("validation_artifact_path") or ""),
                "activation_blocker": str(dataset_row.get("activation_blocker") or ("SLEEVE_DATASET_READINESS_MISSING" if engine_id in {"C2_CROSS_ASSET_TREND_V1", "C2_MARKET_NEUTRAL_SPREAD_V1"} and not dataset_row else "")),
                "readiness_status": readiness_status,
                "readiness_reason": readiness_reason,
                "diagnostic_only": True,
                "affects_arbitration": False,
                "affects_execution": False,
            }
        )
    matrix_path = preflight_readiness_matrix_path(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id)
    payload = {
        "schema_id": "preflight_readiness_matrix",
        "schema_version": "v1",
        "cycle_id": cycle_id,
        "day_utc": day_utc,
        "environment": environment,
        "diagnostic_only": True,
        "affects_arbitration": False,
        "affects_execution": False,
        "contract_registry_path": str(_sleeve_contracts_path()),
        "contract_registry_sha256": sleeve_kernel._sha256_file(_sleeve_contracts_path()) if _sleeve_contracts_path().is_file() else "",
        "engine_registry_path": str(sleeve_kernel._engine_registry_path()),
        "engine_registry_sha256": sleeve_kernel._sha256_file(sleeve_kernel._engine_registry_path()),
        "sleeve_truth_root": str(Path(sleeve_truth_root).resolve()),
        "canonical_truth_root": str(canonical_truth_root) if str(canonical_truth_root) else "",
        "rows": matrix_rows,
        "artifact_path": str(matrix_path),
        "created_at_utc": _now_iso(),
    }
    _write_json(matrix_path, payload)
    return payload


def build_operator_readiness_summary_v1(
    *,
    day_utc: str,
    environment: str,
    truth_root: Path,
    cycle_id: str,
    matrix: dict[str, Any],
) -> dict[str, Any]:
    rows = matrix.get("rows") if isinstance(matrix.get("rows"), list) else []
    by_status: dict[str, list[str]] = {}
    blockers: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = str(row.get("readiness_status") or "UNKNOWN")
        engine_id = str(row.get("engine_id") or "")
        by_status.setdefault(status, []).append(engine_id)
        if status != "BLOCKED":
            continue
        missing = row.get("missing_inputs") if isinstance(row.get("missing_inputs"), list) else []
        if missing:
            for item in missing:
                key = str(item.get("name") or row.get("readiness_reason") or "UNKNOWN_BLOCKER")
                bucket = blockers.setdefault(key, {"blocker": key, "count": 0, "sleeves": [], "example_path": str(item.get("path") or "")})
                bucket["count"] += 1
                if engine_id not in bucket["sleeves"]:
                    bucket["sleeves"].append(engine_id)
        else:
            key = str(row.get("readiness_reason") or row.get("latest_sleeve_blocker") or "UNKNOWN_BLOCKER")
            bucket = blockers.setdefault(key, {"blocker": key, "count": 0, "sleeves": [], "example_path": ""})
            bucket["count"] += 1
            if engine_id not in bucket["sleeves"]:
                bucket["sleeves"].append(engine_id)
    top_blockers = sorted(blockers.values(), key=lambda item: (-int(item.get("count") or 0), str(item.get("blocker") or "")))
    summary_path = operator_readiness_summary_path(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id)
    payload = {
        "schema_id": "operator_readiness_summary",
        "schema_version": "v1",
        "cycle_id": cycle_id,
        "day_utc": day_utc,
        "environment": environment,
        "sleeves_total": len(rows),
        "ready_count": len(by_status.get("READY", [])),
        "blocked_count": len(by_status.get("BLOCKED", [])),
        "disabled_count": len(by_status.get("DISABLED", [])),
        "unknown_count": len(by_status.get("UNKNOWN", [])),
        "top_blockers": top_blockers,
        "sleeves_by_status": {key: sorted(value) for key, value in sorted(by_status.items())},
        "next_recommended_investigation": str(top_blockers[0].get("blocker") or "") if top_blockers else "",
        "diagnostic_only": True,
        "affects_arbitration": False,
        "affects_execution": False,
        "preflight_readiness_matrix_path": str(matrix.get("artifact_path") or ""),
        "artifact_path": str(summary_path),
        "created_at_utc": _now_iso(),
    }
    _write_json(summary_path, payload)
    return payload


def run_scan_cycle_v1(
    *,
    day_utc: str,
    truth_root: Path,
    environment: str = PAPER_MODE,
    cycle_id: str = "",
) -> dict[str, Any]:
    environment = str(environment or PAPER_MODE).strip().upper()
    cycle_id = _unique_cycle_id(truth_root=truth_root, day_utc=day_utc, requested_cycle_id=str(cycle_id or "").strip() or _default_cycle_id())
    started_dt = _now_dt()
    started = started_dt.isoformat().replace("+00:00", "Z")
    intent_truth_root = resolve_paper_intent_truth_root_v1(truth_root=truth_root, repo_root=REPO_ROOT)
    rows = _scan_registry_rows()
    sleeve_kernel.align_registry_market_data_symbols_v1(
        intent_truth_root=intent_truth_root,
        requested_symbols=sleeve_kernel._requested_registry_symbols(rows),
    )
    manifest_path = cycle_manifest_path(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id)
    _write_json(
        manifest_path,
        _cycle_manifest_payload(
            day_utc=day_utc,
            environment=environment,
            truth_root=truth_root,
            sleeve_truth_root=intent_truth_root,
            cycle_id=cycle_id,
            rows=rows,
            created_at_utc=started,
        ),
    )
    existing_by_engine = sleeve_kernel._existing_intents_by_engine(intent_truth_root=intent_truth_root, day_utc=day_utc)
    outcomes: list[dict[str, Any]] = []

    for row in rows:
        sleeve_id = str(row.get("engine_id") or "").strip()
        previous = _previous_cycle_outcome(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id, sleeve_id=sleeve_id)
        if sleeve_kernel._status_from_registry(row) != "ACTIVE":
            outcome = sleeve_kernel._outcome_for_inactive(row=row, day_utc=day_utc, environment=environment, truth_root=truth_root)
        else:
            outcome = sleeve_kernel._evaluate_active_engine(
                row=row,
                day_utc=day_utc,
                environment=environment,
                truth_root=truth_root,
                intent_truth_root=intent_truth_root,
                existing_by_engine=existing_by_engine,
            )
        outcome = _adapt_outcome_for_scan(outcome, truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id)
        outcome = sleeve_kernel._apply_state_memory(outcome, previous)
        if outcome["status"] not in sleeve_kernel.OUTCOMES:
            outcome["status"] = "BLOCKED"
            outcome["current_status"] = "BLOCKED"
            outcome["canonical_blocker"] = "INVALID_SLEEVE_SCAN_STATUS"
        _write_json(Path(outcome["artifact_path"]), outcome)
        outcomes.append(outcome)

    readiness_matrix = build_preflight_readiness_matrix_v1(
        day_utc=day_utc,
        environment=environment,
        truth_root=truth_root,
        sleeve_truth_root=intent_truth_root,
        cycle_id=cycle_id,
        rows=rows,
        outcomes=outcomes,
    )
    readiness_summary = build_operator_readiness_summary_v1(
        day_utc=day_utc,
        environment=environment,
        truth_root=truth_root,
        cycle_id=cycle_id,
        matrix=readiness_matrix,
    )

    rollup_path = sleeve_scan_rollup_path(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id)
    completed_dt = _now_dt()
    blockers = [row for row in outcomes if row.get("status") == "BLOCKED"]
    rollup = {
        "schema_id": SCAN_SCHEMA_ID,
        "schema_version": SCAN_SCHEMA_VERSION,
        "day_utc": day_utc,
        "cycle_id": cycle_id,
        "environment": environment,
        "status": "BLOCKED" if blockers else "PASS",
        "canonical_blocker": str(blockers[0].get("canonical_blocker") or "SLEEVE_SCAN_BLOCKED") if blockers else "",
        "engine_registry_path": str(sleeve_kernel._engine_registry_path()),
        "engine_registry_sha256": sleeve_kernel._sha256_file(sleeve_kernel._engine_registry_path()),
        "intent_truth_root": str(intent_truth_root),
        "sleeve_outcomes": outcomes,
        "outcomes": outcomes,
        "summary": {
            "configured_sleeve_count": len(outcomes),
            "active_sleeve_count": len([row for row in outcomes if row.get("activation_status") == "ACTIVE"]),
            "disabled_sleeve_count": len([row for row in outcomes if row.get("status") == "DISABLED"]),
            "intent_created_count": len([row for row in outcomes if row.get("status") == "INTENT_CREATED"]),
            "no_intent_count": len([row for row in outcomes if row.get("status") == "NO_INTENT"]),
            "blocked_count": len(blockers),
            "filtered_out_count": len([row for row in outcomes if row.get("status") == "FILTERED_OUT"]),
        },
        "started_at_utc": started,
        "completed_at_utc": completed_dt.isoformat().replace("+00:00", "Z"),
        "duration_ms": _duration_ms(started_dt, completed_dt),
        "artifact_path": str(rollup_path),
    }
    _write_json(rollup_path, rollup)

    arbitration_raw = build_intent_arbitration(
        day_utc=day_utc,
        truth_root=truth_root,
        environment=environment,
        cycle_id=cycle_id,
        source_rollup_path=rollup_path,
    )
    arbitration_path = arbitration_result_path(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id)
    arbitration = dict(arbitration_raw)
    arbitration["artifact_path"] = str(arbitration_path)
    _write_json(arbitration_path, arbitration)
    selected_pointer = _read_json(selected_intent_pointer_path(truth_root=truth_root, day_utc=day_utc))
    if selected_pointer:
        selected_pointer["source_arbitration_path"] = str(arbitration_path)
        selected_pointer["source_rollup_path"] = str(rollup_path)
        _write_json(selected_intent_pointer_path(truth_root=truth_root, day_utc=day_utc), selected_pointer)
    selected = arbitration.get("selected_intent") if isinstance(arbitration.get("selected_intent"), dict) else {}
    selected_intent_id = str(selected.get("intent_id") or "").strip() or None
    final_status = "BLOCKED" if rollup["status"] == "BLOCKED" or arbitration["status"] == "BLOCKED" else "PASS"
    final_blocker = str(rollup["canonical_blocker"] or arbitration.get("canonical_blocker") or "").strip() or None
    latest_pointer_path = latest_scan_cycle_pointer_path(truth_root=truth_root)
    ledger_path = scan_ledger_path(truth_root=truth_root, day_utc=day_utc)
    operator_path = operator_status_path(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id)
    result = ScanCycleResult(
        cycle_id=cycle_id,
        day_utc=day_utc,
        environment=environment,
        status=final_status,
        sleeves_evaluated=len(outcomes),
        sleeves_expected=len(rows),
        selected_intent_id=selected_intent_id,
        canonical_blocker=final_blocker,
        artifact_root=str(sleeve_scan_cycle_root(truth_root=truth_root, day_utc=day_utc, cycle_id=cycle_id)),
        cycle_manifest_path=str(manifest_path),
        scan_rollup_path=str(rollup_path),
        arbitration_result_path=str(arbitration_path),
        operator_status_path=str(operator_path),
        preflight_readiness_matrix_path=str(readiness_matrix.get("artifact_path") or ""),
        operator_readiness_summary_path=str(readiness_summary.get("artifact_path") or ""),
        selected_pointer_path=str(selected_intent_pointer_path(truth_root=truth_root, day_utc=day_utc)),
        latest_scan_pointer_path=str(latest_pointer_path),
        ledger_path=str(ledger_path),
    )
    operator_status = {
        "schema_id": "operator_status",
        "schema_version": "v1",
        "cycle_id": cycle_id,
        "day_utc": day_utc,
        "environment": environment,
        "status": final_status,
        "sleeves_evaluated": len(outcomes),
        "sleeves_expected": len(rows),
        "selected_intent_id": selected_intent_id,
        "selected_intent_present": bool(selected_intent_id),
        "canonical_blocker": final_blocker or "",
        "next_scan_at_utc": None,
        "submit_enabled": False,
        "execution_path_touched": False,
        "artifact_root": result.artifact_root,
        "preflight_readiness_matrix_path": result.preflight_readiness_matrix_path,
        "operator_readiness_summary_path": result.operator_readiness_summary_path,
        "created_at_utc": started,
        "completed_at_utc": _now_iso(),
    }
    _write_json(operator_path, operator_status)
    ledger_row = {
        "cycle_id": cycle_id,
        "day_utc": day_utc,
        "environment": environment,
        "status": final_status,
        "sleeves_evaluated": len(outcomes),
        "sleeves_expected": len(rows),
        "selected_intent_id": selected_intent_id,
        "canonical_blocker": final_blocker,
        "artifact_root": result.artifact_root,
        "arbitration_result_path": str(arbitration_path),
        "operator_status_path": str(operator_path),
        "created_at_utc": started,
        "completed_at_utc": operator_status["completed_at_utc"],
        "submit_enabled": False,
    }
    _append_jsonl(ledger_path, ledger_row)
    _write_json(
        latest_pointer_path,
        {
            "schema_id": "latest_scan_cycle_pointer",
            "schema_version": "v1",
            "cycle_id": cycle_id,
            "day_utc": day_utc,
            "environment": environment,
            "status": final_status,
            "canonical_blocker": final_blocker or "",
            "artifact_root": result.artifact_root,
            "operator_status_path": str(operator_path),
            "updated_at_utc": _now_iso(),
        },
    )
    payload = {
        "schema_id": "market_session_intent_engine",
        "schema_version": "v1",
        "day_utc": day_utc,
        "cycle_id": cycle_id,
        "environment": environment,
        "status": final_status,
        "canonical_blocker": final_blocker or "",
        "result": asdict(result),
        "cycle_manifest_path": str(manifest_path),
        "artifact_root": result.artifact_root,
        "sleeve_scan_rollup_path": str(rollup_path),
        "scan_rollup_path": str(rollup_path),
        "intent_arbitration_path": str(arbitration_path),
        "arbitration_result_path": str(arbitration_path),
        "operator_status_path": str(operator_path),
        "preflight_readiness_matrix_path": result.preflight_readiness_matrix_path,
        "operator_readiness_summary_path": result.operator_readiness_summary_path,
        "latest_scan_cycle_pointer_path": str(latest_pointer_path),
        "ledger_path": str(ledger_path),
        "selected_intent_pointer_path": str(selected_intent_pointer_path(truth_root=truth_root, day_utc=day_utc)),
        "sleeve_outcomes": outcomes,
        "arbitration": arbitration,
        "preflight_readiness_matrix": readiness_matrix,
        "operator_readiness_summary": readiness_summary,
        "operator_status": operator_status,
        "started_at_utc": started,
        "completed_at_utc": _now_iso(),
    }
    return payload


def build_market_session_intent_engine(
    *,
    day_utc: str,
    truth_root: Path,
    environment: str = PAPER_MODE,
    cycle_id: str = "",
) -> dict[str, Any]:
    return run_scan_cycle_v1(day_utc=day_utc, truth_root=truth_root, environment=environment, cycle_id=cycle_id)


def run_loop_v1(
    *,
    day_utc: str,
    truth_root: Path,
    environment: str = PAPER_MODE,
    cadence_seconds: int = 60,
    max_cycles: int = 0,
    stop_at_market_close: bool = False,
    emit_status: bool = False,
) -> dict[str, Any]:
    if cadence_seconds < 0:
        raise ValueError("cadence_seconds must be >= 0")
    if max_cycles < 0:
        raise ValueError("max_cycles must be >= 0")
    cycles: list[dict[str, Any]] = []
    while True:
        if stop_at_market_close and _is_after_market_close_now():
            break
        payload = run_scan_cycle_v1(day_utc=day_utc, truth_root=truth_root, environment=environment)
        cycles.append(payload)
        final_cycle = bool(max_cycles and len(cycles) >= max_cycles)
        next_scan_at_utc = None if final_cycle else (_now_dt() + timedelta(seconds=cadence_seconds)).isoformat().replace("+00:00", "Z")
        _update_cycle_next_scan_at(payload, next_scan_at_utc)
        if emit_status:
            print(json.dumps(_compact_cycle_output(payload), sort_keys=True), flush=True)
        if final_cycle:
            break
        if cadence_seconds > 0:
            time.sleep(cadence_seconds)
    last = cycles[-1] if cycles else {}
    return {
        "schema_id": "market_session_intent_engine_loop",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": str(last.get("status") or "STOPPED_MARKET_CLOSED"),
        "canonical_blocker": str(last.get("canonical_blocker") or ""),
        "cycles_run": len(cycles),
        "cycle_ids": [str(payload.get("cycle_id") or "") for payload in cycles],
        "last_cycle": _compact_cycle_output(last) if last else {},
        "cadence_seconds": cadence_seconds,
        "max_cycles": max_cycles,
        "stop_at_market_close": stop_at_market_close,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_market_session_intent_engine_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--once", action="store_true", help="Run exactly one market-session scan cycle.")
    parser.add_argument("--loop", action="store_true", help="Run repeated market-session scan cycles.")
    parser.add_argument("--cadence_seconds", type=int, default=60)
    parser.add_argument("--max_cycles", type=int, default=0)
    parser.add_argument("--stop_at_market_close", action="store_true")
    args = parser.parse_args(argv)
    if bool(args.once) == bool(args.loop):
        raise SystemExit("Specify exactly one of --once or --loop")
    if args.once and (int(args.cadence_seconds) != 60 or int(args.max_cycles) != 0 or bool(args.stop_at_market_close)):
        raise SystemExit("--cadence_seconds, --max_cycles, and --stop_at_market_close apply only with --loop")
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    environment = str(args.environment or PAPER_MODE).strip().upper()
    if args.loop:
        loop_payload = run_loop_v1(
            day_utc=day_utc,
            truth_root=truth_root,
            environment=environment,
            cadence_seconds=int(args.cadence_seconds),
            max_cycles=int(args.max_cycles),
            stop_at_market_close=bool(args.stop_at_market_close),
            emit_status=True,
        )
        if loop_payload["cycles_run"] == 0:
            print(json.dumps(loop_payload, sort_keys=True))
        return 0 if loop_payload["status"] == "PASS" else 2
    payload = build_market_session_intent_engine(day_utc=day_utc, truth_root=truth_root, environment=environment)
    print(
        json.dumps(
            _compact_cycle_output(payload),
            sort_keys=True,
        )
    )
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
