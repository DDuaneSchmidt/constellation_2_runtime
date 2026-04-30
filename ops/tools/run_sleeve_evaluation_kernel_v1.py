#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    collect_intent_files_v1,
    parse_day_utc_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
    resolve_paper_intent_truth_root_v1,
)

ENGINE_REGISTRY_RELPATH = "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json"
SIMULATOR_ENGINE_ID = "C2_INTENT_SIMULATOR_V1"
PAPER_MODE = "PAPER"

OUTCOMES = {"INTENT_CREATED", "NO_INTENT", "BLOCKED", "DEGRADED", "FILTERED_OUT", "DISABLED"}
_REASON_RE = re.compile(r"(?<![A-Z0-9_])([A-Z][A-Z0-9_]{2,})(?=:\s|$)")


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _mtime_iso(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except OSError:
        return ""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return read_json_object_v1(path)
    except Exception:
        return {}


def _engine_registry_path() -> Path:
    return (REPO_ROOT / ENGINE_REGISTRY_RELPATH).resolve()


def _load_engine_registry() -> dict[str, Any]:
    return read_json_object_v1(_engine_registry_path())


def _status_from_registry(row: dict[str, Any]) -> str:
    return str(row.get("activation_status") or "").strip().upper()


def _engine_runner_path(row: dict[str, Any]) -> Path:
    return (REPO_ROOT / str(row.get("engine_runner_path") or "")).resolve()


def _intent_symbol(payload: dict[str, Any]) -> str:
    underlying = payload.get("underlying")
    if isinstance(underlying, dict):
        return str(underlying.get("symbol") or "").strip().upper()
    return str(payload.get("symbol") or "").strip().upper()


def _intent_engine_id(payload: dict[str, Any]) -> str:
    engine = payload.get("engine") if isinstance(payload.get("engine"), dict) else {}
    return str(engine.get("engine_id") or payload.get("engine_id") or payload.get("strategy_id") or "").strip()


def _intent_hash(path: Path, payload: dict[str, Any]) -> str:
    return str(payload.get("intent_hash") or payload.get("canonical_json_hash") or path.name.split(".", 1)[0]).strip()


def _classify_nonzero(stdout: str, stderr: str) -> str:
    for block in (str(stderr or ""), str(stdout or "")):
        for line in block.splitlines():
            stripped = line.strip()
            for match in _REASON_RE.finditer(stripped):
                candidate = match.group(1)
                if candidate not in {"FAIL", "TRACEBACK"}:
                    return candidate
            lowered = stripped.lower()
            if "missing market data manifest:" in lowered:
                return "MARKET_DATA_MANIFEST_MISSING"
            if "missing required source" in lowered:
                return "MISSING_REQUIRED_INPUTS"
    return "PRODUCER_NONZERO_RC"


def _classify_no_intent(stdout: str) -> bool:
    text = str(stdout or "").strip()
    if "NO_INTENT" in text:
        return True
    try:
        obj = json.loads(text)
    except Exception:
        return False
    return isinstance(obj, dict) and str(obj.get("status") or "").strip().upper() == "NO_INTENT"


def _runner_supports_symbols_arg(row: dict[str, Any]) -> bool:
    path = _engine_runner_path(row)
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return False
    return "--symbols" in text


def _producer_symbol_invocations(row: dict[str, Any]) -> list[dict[str, Any]]:
    allowed = _allowed_symbols(row)
    if not allowed:
        return [{"arg_name": "", "arg_value": "", "symbols": []}]
    if len(allowed) == 1:
        return [{"arg_name": "--symbol", "arg_value": allowed[0], "symbols": allowed}]
    if _runner_supports_symbols_arg(row):
        return [{"arg_name": "--symbols", "arg_value": ",".join(allowed), "symbols": allowed}]
    return [{"arg_name": "--symbol", "arg_value": symbol, "symbols": [symbol]} for symbol in allowed]


def _base_engine_cmd(row: dict[str, Any], *, day_utc: str, intent_truth_root: Path) -> list[str]:
    return [
        sys.executable,
        str(_engine_runner_path(row)),
        "--day_utc",
        day_utc,
        "--mode",
        PAPER_MODE,
        "--truth_root",
        str(intent_truth_root),
    ]


def _run_one_engine_invocation(row: dict[str, Any], *, day_utc: str, intent_truth_root: Path, invocation: dict[str, Any]) -> dict[str, Any]:
    cmd = _base_engine_cmd(row, day_utc=day_utc, intent_truth_root=intent_truth_root)
    arg_name = str(invocation.get("arg_name") or "").strip()
    arg_value = str(invocation.get("arg_value") or "").strip()
    if arg_name and arg_value:
        cmd.extend([arg_name, arg_value])
    env = os.environ.copy()
    env["C2_TRUTH_ROOT"] = str(intent_truth_root)
    started = _now_iso()
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env)
    completed = _now_iso()
    return {
        "command": cmd,
        "return_code": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "started_at_utc": started,
        "completed_at_utc": completed,
    }


def _run_engine(row: dict[str, Any], *, day_utc: str, intent_truth_root: Path) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for invocation in _producer_symbol_invocations(row):
        results.append(_run_one_engine_invocation(row, day_utc=day_utc, intent_truth_root=intent_truth_root, invocation=invocation))
        if int(results[-1]["return_code"]) != 0:
            break
    first = results[0] if results else {"started_at_utc": _now_iso(), "completed_at_utc": _now_iso()}
    return {
        "command": results[-1]["command"] if results else _base_engine_cmd(row, day_utc=day_utc, intent_truth_root=intent_truth_root),
        "commands": [result["command"] for result in results],
        "return_code": next((int(result["return_code"]) for result in results if int(result["return_code"]) != 0), 0),
        "stdout": "\n".join(str(result["stdout"]) for result in results if str(result["stdout"])),
        "stderr": "\n".join(str(result["stderr"]) for result in results if str(result["stderr"])),
        "started_at_utc": str(first.get("started_at_utc") or ""),
        "completed_at_utc": str((results[-1] if results else first).get("completed_at_utc") or ""),
    }


def _existing_intents_by_engine(*, intent_truth_root: Path, day_utc: str) -> dict[str, list[tuple[Path, dict[str, Any]]]]:
    out: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
    for path in collect_intent_files_v1(truth_root=intent_truth_root, day_utc=day_utc):
        payload = _read_json(path)
        engine_id = _intent_engine_id(payload) or "UNKNOWN"
        out.setdefault(engine_id, []).append((path, payload))
    return out


def _intent_rows(paths_payloads: list[tuple[Path, dict[str, Any]]], *, artifact_source: str = "") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path, payload in sorted(paths_payloads, key=lambda item: str(item[0])):
        rows.append(
            {
                "intent_id": str(payload.get("intent_id") or "").strip(),
                "intent_hash": _intent_hash(path, payload),
                "intent_path": str(path.resolve()),
                "symbol": _intent_symbol(payload),
                "engine_id": _intent_engine_id(payload),
                "schema_id": str(payload.get("schema_id") or "").strip(),
                "schema_version": str(payload.get("schema_version") or "").strip(),
                "intent_artifact_mtime": _mtime_iso(path),
                "artifact_source": artifact_source,
            }
        )
    return rows


def _allowed_symbols(row: dict[str, Any]) -> list[str]:
    raw = row.get("allowed_symbols") if isinstance(row.get("allowed_symbols"), list) else []
    return [str(symbol).strip().upper() for symbol in raw if str(symbol).strip()]


def _stale_artifact_guard(*, rejected_intents: list[dict[str, Any]], active_symbol_universe: list[str]) -> dict[str, Any]:
    active = sorted({str(symbol).strip().upper() for symbol in active_symbol_universe if str(symbol).strip()})
    active_set = set(active)
    stale = [
        row
        for row in rejected_intents
        if str(row.get("symbol") or "").strip().upper() and active_set and str(row.get("symbol") or "").strip().upper() not in active_set
    ]
    stale_symbols = sorted({str(row.get("symbol") or "").strip().upper() for row in stale if str(row.get("symbol") or "").strip()})
    stale_sources = sorted({str(row.get("artifact_source") or "UNKNOWN") for row in stale})
    return {
        "stale_artifact_detected": bool(stale),
        "artifact_source": ",".join(stale_sources),
        "artifact_symbol": ",".join(stale_symbols),
        "active_symbol_universe": active,
        "stale_artifacts": stale,
    }


def _producer_requested_symbol(row: dict[str, Any]) -> str:
    allowed = _allowed_symbols(row)
    return allowed[0] if len(allowed) == 1 else ""


def _producer_requested_symbols(row: dict[str, Any]) -> list[str]:
    return _allowed_symbols(row)


def _symbol_source(row: dict[str, Any]) -> str:
    return "ENGINE_MODEL_REGISTRY_V1.allowed_symbols" if _allowed_symbols(row) else "NO_REGISTRY_SYMBOL_RESTRICTION"


def _manifest_symbols(root: Path) -> tuple[Path, set[str]]:
    manifest_path = Path(root).resolve() / "market_data_snapshot_v1" / "dataset_manifest.json"
    payload = _read_json(manifest_path)
    symbols: set[str] = set()
    for symbol in payload.get("symbols") if isinstance(payload.get("symbols"), list) else []:
        text = str(symbol).strip().upper()
        if text:
            symbols.add(text)
    for entry in payload.get("files") if isinstance(payload.get("files"), list) else []:
        if not isinstance(entry, dict):
            continue
        text = str(entry.get("symbol") or "").strip().upper()
        if text:
            symbols.add(text)
    return manifest_path, symbols


def _manifest_file_entries_by_symbol(root: Path) -> tuple[Path, dict[str, Any], dict[str, list[dict[str, Any]]]]:
    manifest_path = Path(root).resolve() / "market_data_snapshot_v1" / "dataset_manifest.json"
    payload = _read_json(manifest_path)
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for entry in payload.get("files") if isinstance(payload.get("files"), list) else []:
        if not isinstance(entry, dict):
            continue
        symbol = str(entry.get("symbol") or "").strip().upper()
        rel_file = str(entry.get("file") or "").strip()
        if symbol and rel_file:
            by_symbol.setdefault(symbol, []).append(dict(entry))
    return manifest_path, payload, by_symbol


def _canonical_truth_root() -> Path:
    try:
        from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root

        return Path(resolve_canonical_truth_root()).resolve()
    except Exception:
        return Path("")


def _requested_registry_symbols(rows: list[dict[str, Any]]) -> list[str]:
    requested: set[str] = set()
    for row in rows:
        if not isinstance(row, dict) or _status_from_registry(row) != "ACTIVE":
            continue
        requested.update(_allowed_symbols(row))
    return sorted(requested)


def align_registry_market_data_symbols_v1(*, intent_truth_root: Path, requested_symbols: list[str]) -> dict[str, Any]:
    requested = sorted({str(symbol).strip().upper() for symbol in requested_symbols if str(symbol).strip()})
    local_root = Path(intent_truth_root).resolve()
    canonical_root = _canonical_truth_root()
    local_manifest, local_payload, local_entries = _manifest_file_entries_by_symbol(local_root)
    canonical_manifest, _canonical_payload, canonical_entries = (
        _manifest_file_entries_by_symbol(canonical_root) if str(canonical_root) else (Path(""), {}, {})
    )
    copied: list[dict[str, Any]] = []
    missing_in_canonical: list[str] = []
    invalid_canonical: list[dict[str, str]] = []
    if not requested:
        return {
            "status": "PASS",
            "canonical_blocker": "",
            "requested_symbols": [],
            "copied_symbols": [],
            "missing_in_canonical_truth_root": [],
            "invalid_canonical_files": [],
            "local_manifest_path": str(local_manifest),
            "canonical_manifest_path": str(canonical_manifest) if str(canonical_manifest) else "",
        }

    for symbol in requested:
        entries = canonical_entries.get(symbol, [])
        if not entries:
            missing_in_canonical.append(symbol)
            continue
        copied_entries_for_symbol: list[dict[str, Any]] = []
        for entry in entries:
            rel_file = str(entry.get("file") or "").strip()
            expected_sha = str(entry.get("sha256") or "").strip().lower()
            source = canonical_root / "market_data_snapshot_v1" / rel_file
            target = local_root / "market_data_snapshot_v1" / rel_file
            if not source.is_file():
                invalid_canonical.append({"symbol": symbol, "file": rel_file, "reason": "CANONICAL_FILE_MISSING"})
                continue
            actual_sha = _sha256_file(source)
            if expected_sha and actual_sha.lower() != expected_sha:
                invalid_canonical.append({"symbol": symbol, "file": rel_file, "reason": "CANONICAL_FILE_HASH_MISMATCH"})
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())
            copied.append({"symbol": symbol, "file": rel_file, "sha256": expected_sha or actual_sha})
            copied_entries_for_symbol.append(dict(entry))
        if copied_entries_for_symbol and len(copied_entries_for_symbol) == len(entries):
            local_entries[symbol] = copied_entries_for_symbol

    blocker = ""
    if invalid_canonical:
        blocker = "CANONICAL_MARKET_DATA_MANIFEST_INVALID"
    elif missing_in_canonical:
        blocker = "MARKET_DATA_MANIFEST_SYMBOL_MISSING"

    if copied:
        merged_entries: list[dict[str, Any]] = []
        for symbol, entries in local_entries.items():
            for entry in entries:
                merged_entries.append(dict(entry))
        merged_entries.sort(key=lambda entry: (str(entry.get("symbol") or ""), str(entry.get("file") or "")))
        merged_payload = dict(local_payload) if isinstance(local_payload, dict) else {}
        if not merged_payload.get("dataset_version"):
            merged_payload["dataset_version"] = "v1"
        merged_payload["symbols"] = sorted({str(entry.get("symbol") or "").strip().upper() for entry in merged_entries if str(entry.get("symbol") or "").strip()})
        merged_payload["files"] = merged_entries
        _write_json(local_manifest, merged_payload)

    return {
        "status": "BLOCKED" if blocker else "PASS",
        "canonical_blocker": blocker,
        "requested_symbols": requested,
        "copied_symbols": sorted({row["symbol"] for row in copied}),
        "copied_files": copied,
        "missing_in_canonical_truth_root": missing_in_canonical,
        "invalid_canonical_files": invalid_canonical,
        "local_manifest_path": str(local_manifest),
        "canonical_manifest_path": str(canonical_manifest) if str(canonical_manifest) else "",
    }


def _market_data_symbol_preflight(*, intent_truth_root: Path, requested_symbols: list[str]) -> dict[str, Any]:
    local_manifest, local_symbols = _manifest_symbols(intent_truth_root)
    canonical_root = _canonical_truth_root()
    canonical_manifest, canonical_symbols = _manifest_symbols(canonical_root) if str(canonical_root) else (Path(""), set())
    requested = sorted({str(symbol).strip().upper() for symbol in requested_symbols if str(symbol).strip()})
    missing_local = sorted(symbol for symbol in requested if symbol not in local_symbols)
    present_canonical = sorted(symbol for symbol in missing_local if symbol in canonical_symbols)
    missing_everywhere = sorted(symbol for symbol in missing_local if symbol not in canonical_symbols)
    status = "PASS"
    blocker = ""
    if present_canonical:
        status = "BLOCKED"
        blocker = "TRUTH_ROOT_MARKET_DATA_ALIGNMENT_MISSING"
    elif missing_everywhere:
        status = "BLOCKED"
        blocker = "MARKET_DATA_MANIFEST_SYMBOL_MISSING"
    return {
        "status": status,
        "canonical_blocker": blocker,
        "requested_symbols": requested,
        "local_truth_root": str(Path(intent_truth_root).resolve()),
        "local_manifest_path": str(local_manifest),
        "local_manifest_symbols": sorted(local_symbols),
        "canonical_truth_root": str(canonical_root) if str(canonical_root) else "",
        "canonical_manifest_path": str(canonical_manifest) if str(canonical_manifest) else "",
        "canonical_manifest_symbols": sorted(canonical_symbols),
        "missing_in_local_truth_root": missing_local,
        "present_in_canonical_truth_root": present_canonical,
        "missing_in_both_truth_roots": missing_everywhere,
    }


def _allowed_symbol_mismatches(*, intents: list[dict[str, Any]], allowed_symbols: list[str]) -> list[str]:
    allowed = {str(symbol).strip().upper() for symbol in allowed_symbols if str(symbol).strip()}
    if not allowed:
        return []
    return [row["symbol"] for row in intents if row.get("symbol") and str(row.get("symbol")).upper() not in allowed]


def _partition_intents_by_allowed(
    *,
    paths_payloads: list[tuple[Path, dict[str, Any]]],
    allowed_symbols: list[str],
) -> tuple[list[tuple[Path, dict[str, Any]]], list[tuple[Path, dict[str, Any]]]]:
    allowed = {str(symbol).strip().upper() for symbol in allowed_symbols if str(symbol).strip()}
    if not allowed:
        return paths_payloads, []
    matching: list[tuple[Path, dict[str, Any]]] = []
    mismatched: list[tuple[Path, dict[str, Any]]] = []
    for path, payload in paths_payloads:
        symbol = _intent_symbol(payload)
        if symbol and symbol in allowed:
            matching.append((path, payload))
        else:
            mismatched.append((path, payload))
    return matching, mismatched


def _intent_signature(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    return sorted(
        {
            "intent_hash": str(row.get("intent_hash") or ""),
            "intent_id": str(row.get("intent_id") or ""),
            "symbol": str(row.get("symbol") or ""),
            "engine_id": str(row.get("engine_id") or ""),
        }
        for row in rows
        if isinstance(row, dict)
    )


def sleeve_evaluation_output_path(*, truth_root: Path, day_utc: str, sleeve_id: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "sleeve_evaluation_kernel_v1"
        / day_utc
        / sleeve_id
        / "sleeve_evaluation.v1.json"
    )


def sleeve_evaluation_rollup_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "sleeve_evaluation_kernel_v1"
        / day_utc
        / "sleeve_evaluation_rollup.v1.json"
    )


def _previous_outcome(*, truth_root: Path, day_utc: str, sleeve_id: str) -> dict[str, Any]:
    return _read_json(sleeve_evaluation_output_path(truth_root=truth_root, day_utc=day_utc, sleeve_id=sleeve_id))


def _apply_lifecycle_diagnostics(outcome: dict[str, Any], lifecycle_row: dict[str, Any], lifecycle_state_path: str) -> None:
    if not lifecycle_row:
        outcome.setdefault("unchanged_signal", False)
        outcome.setdefault("lifecycle_state_path", "")
        outcome.setdefault("lifecycle_decision", "")
        outcome.setdefault("lifecycle_reason_codes", [])
        outcome.setdefault("position_match_status", "")
        outcome.setdefault("order_match_status", "")
        outcome.setdefault("reentry_eligible", False)
        return
    outcome["unchanged_signal"] = bool(lifecycle_row.get("unchanged_signal"))
    outcome["lifecycle_state_path"] = str(lifecycle_state_path or "")
    outcome["lifecycle_decision"] = str(lifecycle_row.get("lifecycle_decision") or "").strip().upper()
    outcome["lifecycle_reason_codes"] = lifecycle_row.get("lifecycle_reason_codes") if isinstance(lifecycle_row.get("lifecycle_reason_codes"), list) else []
    outcome["position_match_status"] = str(lifecycle_row.get("matching_position_state") or "")
    outcome["order_match_status"] = str(lifecycle_row.get("matching_order_state") or "")
    outcome["reentry_eligible"] = bool(lifecycle_row.get("reentry_eligible"))


def _apply_state_memory(
    outcome: dict[str, Any],
    previous: dict[str, Any],
    *,
    lifecycle_row: dict[str, Any] | None = None,
    lifecycle_state_path: str = "",
) -> dict[str, Any]:
    previous_status = str(previous.get("current_status") or previous.get("status") or "").strip()
    raw_current_status = str(outcome.get("status") or "").strip()
    previous_signature = previous.get("intent_signature") if isinstance(previous.get("intent_signature"), list) else []
    current_intents = outcome.get("output_intents") if isinstance(outcome.get("output_intents"), list) else []
    current_signature = _intent_signature(current_intents)
    previous_signal = previous.get("signal_state") if isinstance(previous.get("signal_state"), dict) else {}
    raw_signal_state = "ACTIVE" if raw_current_status == "INTENT_CREATED" and current_signature else "INACTIVE"

    unchanged_signal = (
        raw_current_status == "INTENT_CREATED"
        and previous_status in {"INTENT_CREATED", "NO_INTENT"}
        and str(previous_signal.get("state") or "") == "ACTIVE"
        and bool(current_signature)
        and current_signature == previous_signature
    )
    _apply_lifecycle_diagnostics(outcome, lifecycle_row or {}, lifecycle_state_path)
    lifecycle_decision = str(lifecycle_row.get("lifecycle_decision") or "").strip().upper() if lifecycle_row else ""
    lifecycle_can_rewrite = bool(lifecycle_row) and raw_current_status == "INTENT_CREATED"
    if lifecycle_can_rewrite and lifecycle_decision in {"NO_INTENT", "BLOCKED", "DEGRADED"}:
        decision = str(lifecycle_row.get("lifecycle_decision") or "").strip().upper()
        reason_codes = outcome.get("reason_codes") if isinstance(outcome.get("reason_codes"), list) else []
        lifecycle_reasons = lifecycle_row.get("lifecycle_reason_codes") if isinstance(lifecycle_row.get("lifecycle_reason_codes"), list) else []
        outcome["status"] = decision
        outcome["current_status"] = decision
        outcome["canonical_blocker"] = str(lifecycle_reasons[0] if decision in {"BLOCKED", "DEGRADED"} and lifecycle_reasons else "")
        outcome["reason_codes"] = sorted(set([str(code) for code in reason_codes + lifecycle_reasons if str(code)]))
        outcome["output_intents"] = []
        outcome["output_intent_path"] = ""
        outcome["output_intent_id"] = ""
        outcome["output_intent_hash"] = ""
        signal_state = "ACTIVE" if str(lifecycle_row.get("signal_state") or "").strip().upper() == "ACTIVE" else raw_signal_state
    elif lifecycle_can_rewrite and lifecycle_decision == "INTENT_CREATED":
        outcome["status"] = "INTENT_CREATED"
        outcome["current_status"] = "INTENT_CREATED"
        outcome["canonical_blocker"] = ""
        reason_codes = outcome.get("reason_codes") if isinstance(outcome.get("reason_codes"), list) else []
        lifecycle_reasons = lifecycle_row.get("lifecycle_reason_codes") if isinstance(lifecycle_row.get("lifecycle_reason_codes"), list) else []
        outcome["reason_codes"] = sorted(set([str(code) for code in reason_codes + lifecycle_reasons if str(code)]))
        signal_state = "ACTIVE"
    elif unchanged_signal:
        outcome["status"] = "NO_INTENT"
        outcome["current_status"] = "NO_INTENT"
        outcome["canonical_blocker"] = ""
        reason_codes = outcome.get("reason_codes") if isinstance(outcome.get("reason_codes"), list) else []
        outcome["reason_codes"] = sorted(set([str(code) for code in reason_codes] + ["UNCHANGED_SIGNAL"]))
        outcome["output_intents"] = []
        outcome["output_intent_path"] = ""
        outcome["output_intent_id"] = ""
        outcome["output_intent_hash"] = ""
        signal_state = "ACTIVE"
    else:
        outcome["current_status"] = raw_current_status
        signal_state = raw_signal_state

    previous_duration = int(previous_signal.get("duration_cycles") or 0) if previous_signal else 0
    previous_state = str(previous_signal.get("state") or "").strip()
    duration = previous_duration + 1 if previous_state == signal_state else 1
    outcome["previous_status"] = previous_status
    outcome["changed"] = previous_status != str(outcome.get("current_status") or "")
    outcome["intent_signature"] = current_signature
    outcome["signal_state"] = {"state": signal_state, "duration_cycles": duration}
    return outcome


def _outcome_for_inactive(*, row: dict[str, Any], day_utc: str, environment: str, truth_root: Path) -> dict[str, Any]:
    engine_id = str(row.get("engine_id") or "").strip()
    return {
        "schema_id": "sleeve_evaluation_kernel",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "sleeve_id": engine_id,
        "engine_id": engine_id,
        "enabled": False,
        "activation_status": _status_from_registry(row),
        "expected_intent_type": "ExposureIntent",
        "allowed_symbols": row.get("allowed_symbols") if isinstance(row.get("allowed_symbols"), list) else [],
        "registry_allowed_symbols": row.get("allowed_symbols") if isinstance(row.get("allowed_symbols"), list) else [],
        "producer_requested_symbol": _producer_requested_symbol(row),
        "producer_requested_symbols": _producer_requested_symbols(row),
        "intent_symbol": "",
        "intent_artifact_path": "",
        "intent_artifact_mtime": "",
        "symbol_source": _symbol_source(row),
        "governing_policy_paths": [str(_engine_registry_path())],
        "status": "DISABLED",
        "canonical_blocker": "",
        "reason_codes": ["ENGINE_INACTIVE"],
        "input_artifacts": [],
        "market_data_manifest_check": {},
        "output_intents": [],
        "rejected_intents": [],
        "stale_artifact_detected": False,
        "artifact_source": "",
        "artifact_symbol": "",
        "active_symbol_universe": _allowed_symbols(row),
        "stale_artifacts": [],
        "output_intent_path": "",
        "output_intent_id": "",
        "output_intent_hash": "",
        "registry_constraints_checked": {
            "runner_hash_match": True,
            "allowed_symbol_match": True,
            "activation_status_valid": True,
        },
        "producer_command": "",
        "started_at_utc": _now_iso(),
        "completed_at_utc": _now_iso(),
        "duration_ms": 0,
        "exit_code": 0,
        "stdout_summary": "",
        "stderr_summary": "",
        "operator_next_action": "",
        "artifact_path": str(sleeve_evaluation_output_path(truth_root=truth_root, day_utc=day_utc, sleeve_id=engine_id)),
    }


def _evaluate_active_engine(
    *,
    row: dict[str, Any],
    day_utc: str,
    environment: str,
    truth_root: Path,
    intent_truth_root: Path,
    existing_by_engine: dict[str, list[tuple[Path, dict[str, Any]]]],
) -> dict[str, Any]:
    engine_id = str(row.get("engine_id") or "").strip()
    runner = _engine_runner_path(row)
    expected_sha = str(row.get("engine_runner_sha256") or "").strip().lower()
    actual_sha = _sha256_file(runner) if runner.exists() and runner.is_file() else ""
    allowed_symbols = row.get("allowed_symbols") if isinstance(row.get("allowed_symbols"), list) else []
    producer_requested_symbol = _producer_requested_symbol(row)
    producer_requested_symbols = _producer_requested_symbols(row)
    market_data_manifest_check = _market_data_symbol_preflight(intent_truth_root=intent_truth_root, requested_symbols=producer_requested_symbols)
    started = _now_iso()
    command = ""
    exit_code = 0
    stdout = ""
    stderr = ""
    reason_codes: list[str] = []
    blocker = ""
    status = "BLOCKED"

    if engine_id == SIMULATOR_ENGINE_ID:
        status = "FILTERED_OUT"
        reason_codes = ["ACTIVE_SIMULATOR_NOT_PRESTART_REQUIRED"]
        actual_intents: list[dict[str, Any]] = []
    elif not runner.exists() or not runner.is_file():
        blocker = "ACTIVE_ENGINE_RUNNER_MISSING"
        reason_codes = [blocker]
        actual_intents = []
    elif expected_sha and actual_sha.lower() != expected_sha:
        blocker = "ENGINE_RUNNER_SHA256_MISMATCH"
        reason_codes = [blocker]
        actual_intents = []
    elif market_data_manifest_check.get("status") == "BLOCKED":
        blocker = str(market_data_manifest_check.get("canonical_blocker") or "MARKET_DATA_MANIFEST_SYMBOL_MISSING")
        reason_codes = [blocker]
        preexisting = existing_by_engine.get(engine_id, [])
        _matching_preexisting, mismatched_preexisting = _partition_intents_by_allowed(paths_payloads=preexisting, allowed_symbols=allowed_symbols)
        rejected_intents = _intent_rows(mismatched_preexisting, artifact_source="PREEXISTING_INTENT_SNAPSHOT")
        actual_intents = []
    else:
        preexisting = existing_by_engine.get(engine_id, [])
        matching_preexisting, mismatched_preexisting = _partition_intents_by_allowed(paths_payloads=preexisting, allowed_symbols=allowed_symbols)
        rejected_intents = _intent_rows(mismatched_preexisting, artifact_source="PREEXISTING_INTENT_SNAPSHOT")
        before = {str(path) for path in collect_intent_files_v1(truth_root=intent_truth_root, day_utc=day_utc)}
        if matching_preexisting:
            actual_intents = _intent_rows(matching_preexisting)
            status = "INTENT_CREATED"
            reason_codes = ["EXISTING_ENGINE_INTENT_EVALUATED"]
        else:
            result = _run_engine(row, day_utc=day_utc, intent_truth_root=intent_truth_root)
            command = " ".join(result["command"])
            exit_code = int(result["return_code"])
            stdout = str(result["stdout"])
            stderr = str(result["stderr"])
            after_paths = collect_intent_files_v1(truth_root=intent_truth_root, day_utc=day_utc)
            new_paths = [path for path in after_paths if str(path) not in before]
            new_payloads = [(path, _read_json(path)) for path in new_paths]
            matching_new, mismatched_new = _partition_intents_by_allowed(paths_payloads=new_payloads, allowed_symbols=allowed_symbols)
            rejected_intents.extend(_intent_rows(mismatched_new, artifact_source="PRODUCER_OUTPUT"))
            if exit_code != 0:
                producer_blocker = _classify_nonzero(stdout, stderr)
                blocker = "ALLOWED_SYMBOL_MISMATCH" if rejected_intents else producer_blocker
                reason_codes = sorted(set([blocker, producer_blocker] + (["STALE_MISMATCHED_INTENT_REJECTED"] if rejected_intents else [])))
                status = "BLOCKED"
                actual_intents = _intent_rows(matching_new)
            elif len(new_paths) > 1:
                blocker = "PRODUCER_MULTIPLE_OUTPUTS_UNEXPECTED"
                reason_codes = [blocker]
                status = "BLOCKED"
                actual_intents = _intent_rows(matching_new)
            elif matching_new:
                status = "INTENT_CREATED"
                reason_codes = ["INTENT_OUTPUT_CREATED"]
                actual_intents = _intent_rows(matching_new)
            elif mismatched_new or rejected_intents:
                blocker = "ALLOWED_SYMBOL_MISMATCH"
                reason_codes = ["ALLOWED_SYMBOL_MISMATCH", "MISMATCHED_INTENT_REJECTED"]
                status = "BLOCKED"
                actual_intents = []
            elif _classify_no_intent(stdout):
                status = "NO_INTENT"
                reason_codes = ["NO_INTENT_DECLARED"]
                actual_intents = []
            else:
                blocker = "PRODUCER_ZERO_RC_WITHOUT_CANONICAL_OUTPUT"
                reason_codes = [blocker]
                status = "BLOCKED"
                actual_intents = []

        mismatches = _allowed_symbol_mismatches(intents=actual_intents if "actual_intents" in locals() else [], allowed_symbols=allowed_symbols)
        if mismatches:
            status = "BLOCKED"
            blocker = "ALLOWED_SYMBOL_MISMATCH"
            reason_codes = sorted(set(reason_codes + [blocker]))
            rejected_intents.extend([dict(row, artifact_source=str(row.get("artifact_source") or "OUTPUT_INTENT_MISMATCH")) for row in actual_intents])
            actual_intents = []

    runner_hash_match = bool(actual_sha and (not expected_sha or actual_sha.lower() == expected_sha))
    allowed_symbol_match = not _allowed_symbol_mismatches(
        intents=actual_intents if "actual_intents" in locals() else [],
        allowed_symbols=allowed_symbols,
    )
    completed = _now_iso()
    rejected_intents = rejected_intents if "rejected_intents" in locals() else []
    stale_artifact_guard = _stale_artifact_guard(rejected_intents=rejected_intents, active_symbol_universe=allowed_symbols)
    primary = actual_intents[0] if actual_intents else {}
    evidence_intent = primary or (rejected_intents[0] if rejected_intents else {})
    return {
        "schema_id": "sleeve_evaluation_kernel",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "sleeve_id": engine_id,
        "engine_id": engine_id,
        "enabled": True,
        "activation_status": _status_from_registry(row),
        "expected_intent_type": "ExposureIntent",
        "allowed_symbols": allowed_symbols,
        "registry_allowed_symbols": allowed_symbols,
        "producer_requested_symbol": producer_requested_symbol,
        "producer_requested_symbols": producer_requested_symbols,
        "symbol_source": _symbol_source(row),
        "governing_policy_paths": [str(_engine_registry_path())],
        "status": status,
        "canonical_blocker": blocker,
        "reason_codes": reason_codes,
        "input_artifacts": [{"artifact_type": "engine_registry", "path": str(_engine_registry_path()), "sha256": _sha256_file(_engine_registry_path())}],
        "market_data_manifest_check": market_data_manifest_check,
        "output_intents": actual_intents if "actual_intents" in locals() else [],
        "rejected_intents": rejected_intents,
        **stale_artifact_guard,
        "output_intent_path": str(primary.get("intent_path") or ""),
        "output_intent_id": str(primary.get("intent_id") or ""),
        "output_intent_hash": str(primary.get("intent_hash") or ""),
        "intent_symbol": str(evidence_intent.get("symbol") or ""),
        "intent_artifact_path": str(evidence_intent.get("intent_path") or ""),
        "intent_artifact_mtime": str(evidence_intent.get("intent_artifact_mtime") or ""),
        "registry_constraints_checked": {
            "runner_hash_match": runner_hash_match,
            "allowed_symbol_match": allowed_symbol_match,
            "activation_status_valid": True,
        },
        "producer_command": command,
        "started_at_utc": started,
        "completed_at_utc": completed,
        "duration_ms": 0,
        "exit_code": exit_code,
        "stdout_summary": stdout[-2000:],
        "stderr_summary": stderr[-2000:],
        "operator_next_action": "Resolve sleeve blocker before arbitration." if blocker else "",
        "artifact_path": str(sleeve_evaluation_output_path(truth_root=truth_root, day_utc=day_utc, sleeve_id=engine_id)),
    }


def build_sleeve_evaluation_kernel(*, day_utc: str, truth_root: Path, environment: str = PAPER_MODE) -> dict[str, Any]:
    registry = _load_engine_registry()
    intent_truth_root = resolve_paper_intent_truth_root_v1(truth_root=truth_root, repo_root=REPO_ROOT)
    align_registry_market_data_symbols_v1(intent_truth_root=intent_truth_root, requested_symbols=_requested_registry_symbols(registry.get("engines") if isinstance(registry.get("engines"), list) else []))
    existing_by_engine = _existing_intents_by_engine(intent_truth_root=intent_truth_root, day_utc=day_utc)
    raw_outcomes: list[dict[str, Any]] = []
    previous_by_engine: dict[str, dict[str, Any]] = {}
    for row in registry.get("engines") if isinstance(registry.get("engines"), list) else []:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("engine_id") or "").strip()
        previous = _previous_outcome(truth_root=truth_root, day_utc=day_utc, sleeve_id=sleeve_id)
        previous_by_engine[sleeve_id] = previous
        if _status_from_registry(row) != "ACTIVE":
            outcome = _outcome_for_inactive(row=row, day_utc=day_utc, environment=environment, truth_root=truth_root)
        else:
            outcome = _evaluate_active_engine(
                row=row,
                day_utc=day_utc,
                environment=environment,
                truth_root=truth_root,
                intent_truth_root=intent_truth_root,
                existing_by_engine=existing_by_engine,
            )
        raw_outcomes.append(outcome)

    from ops.tools.run_intent_lifecycle_state_v1 import build_intent_lifecycle_state_v1, rows_by_engine

    lifecycle = build_intent_lifecycle_state_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        environment=environment,
        intent_truth_root=intent_truth_root,
        outcomes=raw_outcomes,
        previous_by_engine=previous_by_engine,
    )
    lifecycle_by_engine = rows_by_engine(lifecycle)
    lifecycle_path = str(lifecycle.get("artifact_path") or "")
    outcomes: list[dict[str, Any]] = []
    for outcome in raw_outcomes:
        sleeve_id = str(outcome.get("sleeve_id") or outcome.get("engine_id") or "").strip()
        previous = previous_by_engine.get(sleeve_id, {})
        outcome = _apply_state_memory(
            outcome,
            previous,
            lifecycle_row=lifecycle_by_engine.get(sleeve_id, {}),
            lifecycle_state_path=lifecycle_path,
        )
        if outcome["status"] not in OUTCOMES:
            outcome["status"] = "BLOCKED"
            outcome["canonical_blocker"] = "INVALID_SLEEVE_EVALUATION_STATUS"
        _write_json(Path(outcome["artifact_path"]), outcome)
        outcomes.append(outcome)

    rollup_path = sleeve_evaluation_rollup_path(truth_root=truth_root, day_utc=day_utc)
    blockers = [row for row in outcomes if row.get("status") == "BLOCKED"]
    payload = {
        "schema_id": "sleeve_evaluation_kernel_rollup",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": "BLOCKED" if blockers else "PASS",
        "canonical_blocker": str(blockers[0].get("canonical_blocker") or "SLEEVE_EVALUATION_BLOCKED") if blockers else "",
        "engine_registry_path": str(_engine_registry_path()),
        "engine_registry_sha256": _sha256_file(_engine_registry_path()),
        "intent_truth_root": str(intent_truth_root),
        "outcomes": outcomes,
        "summary": {
            "configured_count": len(outcomes),
            "active_count": len([row for row in outcomes if row.get("activation_status") == "ACTIVE"]),
            "disabled_count": len([row for row in outcomes if row.get("status") == "DISABLED"]),
            "intent_created_count": len([row for row in outcomes if row.get("status") == "INTENT_CREATED"]),
            "no_intent_count": len([row for row in outcomes if row.get("status") == "NO_INTENT"]),
            "blocked_count": len(blockers),
        },
        "created_at_utc": _now_iso(),
        "updated_at_utc": _now_iso(),
        "artifact_path": str(rollup_path),
    }
    _write_json(rollup_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_sleeve_evaluation_kernel_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE)
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_sleeve_evaluation_kernel(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment or PAPER_MODE).strip().upper())
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "path": payload["artifact_path"]}, sort_keys=True))
    return 0 if payload["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
