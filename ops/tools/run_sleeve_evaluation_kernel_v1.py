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

OUTCOMES = {"INTENT_CREATED", "NO_INTENT", "BLOCKED", "FILTERED_OUT", "DISABLED"}
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


def _run_engine(row: dict[str, Any], *, day_utc: str, intent_truth_root: Path) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(_engine_runner_path(row)),
        "--day_utc",
        day_utc,
        "--mode",
        PAPER_MODE,
        "--truth_root",
        str(intent_truth_root),
    ]
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


def _existing_intents_by_engine(*, intent_truth_root: Path, day_utc: str) -> dict[str, list[tuple[Path, dict[str, Any]]]]:
    out: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
    for path in collect_intent_files_v1(truth_root=intent_truth_root, day_utc=day_utc):
        payload = _read_json(path)
        engine_id = _intent_engine_id(payload) or "UNKNOWN"
        out.setdefault(engine_id, []).append((path, payload))
    return out


def _intent_rows(paths_payloads: list[tuple[Path, dict[str, Any]]]) -> list[dict[str, Any]]:
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
            }
        )
    return rows


def _allowed_symbol_mismatches(*, intents: list[dict[str, Any]], allowed_symbols: list[str]) -> list[str]:
    allowed = {str(symbol).strip().upper() for symbol in allowed_symbols if str(symbol).strip()}
    if not allowed:
        return []
    return [row["symbol"] for row in intents if row.get("symbol") and str(row.get("symbol")).upper() not in allowed]


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


def _apply_state_memory(outcome: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
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
    if unchanged_signal:
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
        "governing_policy_paths": [str(_engine_registry_path())],
        "status": "DISABLED",
        "canonical_blocker": "",
        "reason_codes": ["ENGINE_INACTIVE"],
        "input_artifacts": [],
        "output_intents": [],
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
    else:
        preexisting = existing_by_engine.get(engine_id, [])
        before = {str(path) for path in collect_intent_files_v1(truth_root=intent_truth_root, day_utc=day_utc)}
        if preexisting:
            actual_intents = _intent_rows(preexisting)
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
            if exit_code != 0:
                blocker = _classify_nonzero(stdout, stderr)
                reason_codes = [blocker]
                status = "BLOCKED"
                actual_intents = _intent_rows(new_payloads)
            elif len(new_paths) > 1:
                blocker = "PRODUCER_MULTIPLE_OUTPUTS_UNEXPECTED"
                reason_codes = [blocker]
                status = "BLOCKED"
                actual_intents = _intent_rows(new_payloads)
            elif new_paths:
                status = "INTENT_CREATED"
                reason_codes = ["INTENT_OUTPUT_CREATED"]
                actual_intents = _intent_rows(new_payloads)
            elif _classify_no_intent(stdout):
                status = "NO_INTENT"
                reason_codes = ["NO_INTENT_DECLARED"]
                actual_intents = []
            else:
                blocker = "PRODUCER_ZERO_RC_WITHOUT_CANONICAL_OUTPUT"
                reason_codes = [blocker]
                status = "BLOCKED"
                actual_intents = []

        mismatches = _allowed_symbol_mismatches(intents=actual_intents, allowed_symbols=allowed_symbols)
        if mismatches:
            status = "BLOCKED"
            blocker = "ALLOWED_SYMBOL_MISMATCH"
            reason_codes = sorted(set(reason_codes + [blocker]))

    runner_hash_match = bool(actual_sha and (not expected_sha or actual_sha.lower() == expected_sha))
    allowed_symbol_match = not _allowed_symbol_mismatches(
        intents=actual_intents if "actual_intents" in locals() else [],
        allowed_symbols=allowed_symbols,
    )
    completed = _now_iso()
    primary = actual_intents[0] if actual_intents else {}
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
        "governing_policy_paths": [str(_engine_registry_path())],
        "status": status,
        "canonical_blocker": blocker,
        "reason_codes": reason_codes,
        "input_artifacts": [{"artifact_type": "engine_registry", "path": str(_engine_registry_path()), "sha256": _sha256_file(_engine_registry_path())}],
        "output_intents": actual_intents if "actual_intents" in locals() else [],
        "output_intent_path": str(primary.get("intent_path") or ""),
        "output_intent_id": str(primary.get("intent_id") or ""),
        "output_intent_hash": str(primary.get("intent_hash") or ""),
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
    existing_by_engine = _existing_intents_by_engine(intent_truth_root=intent_truth_root, day_utc=day_utc)
    outcomes: list[dict[str, Any]] = []
    for row in registry.get("engines") if isinstance(registry.get("engines"), list) else []:
        if not isinstance(row, dict):
            continue
        sleeve_id = str(row.get("engine_id") or "").strip()
        previous = _previous_outcome(truth_root=truth_root, day_utc=day_utc, sleeve_id=sleeve_id)
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
        outcome = _apply_state_memory(outcome, previous)
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
