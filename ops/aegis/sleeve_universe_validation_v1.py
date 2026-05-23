from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.common.aegis_lite_schedule_v1 import sleeve_schedule_metadata_v1

REPO_ROOT = Path(__file__).resolve().parents[2]
ENGINE_REGISTRY_PATH = REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json"
ENGINE_UNIVERSE_POLICY_PATH = REPO_ROOT / "governance/02_REGISTRIES/ENGINE_UNIVERSE_POLICY_V1.json"
REPORT_REL = Path("reports/sleeve_universe_validation_v1")


def canonical_json_bytes_v1(payload: Any) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def hash_payload_v1(payload: Any) -> str:
    return hashlib.sha256(canonical_json_bytes_v1(payload)).hexdigest()


def hash_file_v1(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json_object_v1(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def stable_symbols_v1(value: Any) -> list[str]:
    seen: set[str] = set()
    symbols: list[str] = []
    if not isinstance(value, list):
        return symbols
    for item in value:
        symbol = str(item or "").strip().upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
    return sorted(symbols)


def _now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_policy_by_engine() -> dict[str, dict[str, Any]]:
    payload = read_json_object_v1(ENGINE_UNIVERSE_POLICY_PATH)
    return {
        str(row.get("engine_id") or "").strip(): row
        for row in payload.get("policies", [])
        if isinstance(row, dict) and str(row.get("engine_id") or "").strip()
    }


def _load_active_engines() -> dict[str, dict[str, Any]]:
    payload = read_json_object_v1(ENGINE_REGISTRY_PATH)
    engines: dict[str, dict[str, Any]] = {}
    for row in payload.get("engines", []):
        if not isinstance(row, dict):
            continue
        engine_id = str(row.get("engine_id") or "").strip()
        status = str(row.get("activation_status") or "").strip().upper()
        if engine_id and status == "ACTIVE":
            engines[engine_id] = row
    return engines


def _runtime_evaluation_path(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "reports/aegis_runtime_truth_kernel_v1" / day_utc / "runtime_evaluation.v1.json"


def _sleeve_rollup_path(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "reports/sleeve_evaluation_kernel_v1" / day_utc / "sleeve_evaluation_rollup.v1.json"


def _sleeve_readiness_path(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "reports/aegis_sleeve_readiness_v1" / day_utc / "sleeve_readiness.v1.json"


def _ranked_universe_path(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "reports/ranked_symbol_universe_v1" / day_utc / "ranked_symbol_universe.v1.json"


def _capability_allowed(payload: dict[str, Any], capability: str) -> bool:
    value = (payload.get("capabilities") or {}).get(capability)
    if isinstance(value, dict):
        return bool(value.get("allowed"))
    return bool(value)

def _expected_symbols_from_source(source_path: Path, policy: dict[str, Any]) -> tuple[list[str], str, str]:
    source = str(policy.get("universe_mode") or "").strip().upper()
    if source in {"CURATED_SYMBOLS", "CURATED_PAIRS"}:
        return stable_symbols_v1(policy.get("curated_symbols")), "ENGINE_UNIVERSE_POLICY_V1.curated_symbols", str(ENGINE_UNIVERSE_POLICY_PATH)
    payload = read_json_object_v1(source_path)
    for key in ("candidate_symbols", "symbols", "selected_symbols", "allowed_symbols"):
        symbols = stable_symbols_v1(payload.get(key))
        if symbols:
            target_count = int(policy.get("target_symbol_count") or 0)
            if target_count > 0 and len(symbols) > target_count:
                symbols = symbols[:target_count]
            return symbols, str(payload.get("schema_id") or source_path.parent.name), str(source_path)
    return [], "EMPTY_SOURCE", str(source_path)


def _expected_dynamic_source_path(truth_root: Path, day_utc: str, engine_id: str, outcome: dict[str, Any]) -> Path:
    source_path = Path(str(outcome.get("symbol_source_path") or ""))
    if source_path.exists():
        return source_path
    basis = truth_root / "reports/engine_universe_candidate_basis_v1" / day_utc / engine_id / "engine_universe_candidate_basis.v1.json"
    if basis.exists():
        return basis
    return _ranked_universe_path(truth_root, day_utc)


def _readiness_by_sleeve(truth_root: Path, day_utc: str) -> dict[str, dict[str, Any]]:
    path = _sleeve_readiness_path(truth_root, day_utc)
    if not path.exists():
        return {}
    payload = read_json_object_v1(path)
    return {
        str(row.get("sleeve_id") or "").strip(): row
        for row in payload.get("sleeves", [])
        if isinstance(row, dict) and str(row.get("sleeve_id") or "").strip()
    }


def _outcomes_by_engine(truth_root: Path, day_utc: str) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    path = _sleeve_rollup_path(truth_root, day_utc)
    payload = read_json_object_v1(path)
    outcomes = {
        str(row.get("engine_id") or row.get("sleeve_id") or "").strip(): row
        for row in payload.get("outcomes", [])
        if isinstance(row, dict) and str(row.get("engine_id") or row.get("sleeve_id") or "").strip()
    }
    return outcomes, payload


def _source_day_status(source_path: str, source_payload: dict[str, Any], day_utc: str, same_day_required: bool) -> dict[str, Any]:
    source_day = str(source_payload.get("day_utc") or source_payload.get("basis_day_utc") or "").strip()
    stale = bool(same_day_required and source_day and source_day != day_utc)
    missing_day = bool(same_day_required and not source_day)
    return {
        "source_day_utc": source_day,
        "same_day_required": bool(same_day_required),
        "source_day_matches": bool(not same_day_required or source_day == day_utc),
        "stale_source": stale,
        "missing_source_day": missing_day,
        "source_path": source_path,
    }


def _validate_one(
    *,
    truth_root: Path,
    day_utc: str,
    engine_id: str,
    policy: dict[str, Any],
    outcome: dict[str, Any],
    readiness: dict[str, Any],
) -> dict[str, Any]:
    actual_symbols = stable_symbols_v1(outcome.get("allowed_symbols"))
    source = str(outcome.get("symbol_source") or "").strip()
    source_path = str(outcome.get("symbol_source_path") or "").strip()
    dynamic = str(policy.get("symbol_source_class") or "").strip().upper() == "DYNAMIC_SAME_DAY"
    if dynamic:
        expected_source_path = _expected_dynamic_source_path(truth_root, day_utc, engine_id, outcome)
    else:
        expected_source_path = ENGINE_UNIVERSE_POLICY_PATH
    expected_symbols, expected_source, expected_source_path_str = _expected_symbols_from_source(expected_source_path, policy)
    source_payload: dict[str, Any] = {}
    actual_source_path = Path(source_path) if source_path else expected_source_path
    if actual_source_path.exists() and actual_source_path.is_file():
        source_payload = read_json_object_v1(actual_source_path)
    source_day = _source_day_status(
        str(actual_source_path),
        source_payload,
        day_utc,
        bool(policy.get("same_day_symbol_basis_required")),
    )
    duplicate_symbols_removed = len(actual_symbols) == len(set(actual_symbols))
    missing = sorted(set(expected_symbols) - set(actual_symbols))
    unexpected = sorted(set(actual_symbols) - set(expected_symbols))
    fallback_used = bool(outcome.get("deprecated_symbol_fallback_used")) or source.startswith("DEPRECATED") or source in {"", "ENGINE_MODEL_REGISTRY_V1.allowed_symbols"}
    stale = bool(outcome.get("stale_artifact_detected")) or bool(source_day["stale_source"]) or bool(source_day["missing_source_day"])
    empty = len(actual_symbols) == 0
    status = "PASS"
    if fallback_used or stale or missing or unexpected or empty:
        status = "FAIL"
    if status == "PASS" and str(outcome.get("status") or "").strip().upper() == "BLOCKED":
        status = "PASS_BLOCKED_BY_READINESS"
    source_hash = ""
    if actual_source_path.exists() and actual_source_path.is_file():
        source_hash = hash_file_v1(actual_source_path)
    return {
        "sleeve_name": engine_id,
        "engine_id": engine_id,
        "execution_status": str(outcome.get("status") or "UNKNOWN"),
        "canonical_blocker": str(outcome.get("canonical_blocker") or ""),
        "started_at_utc": str(outcome.get("started_at_utc") or ""),
        "completed_at_utc": str(outcome.get("completed_at_utc") or ""),
        "expected_universe_source": expected_source,
        "expected_universe_source_path": expected_source_path_str,
        "actual_universe_source": source,
        "actual_universe_source_path": source_path,
        "source_artifact_hash": source_hash,
        "source_day_status": source_day,
        "symbol_count": len(actual_symbols),
        "expected_symbol_count": len(expected_symbols),
        "universe_hash": hash_payload_v1(actual_symbols),
        "expected_universe_hash": hash_payload_v1(expected_symbols),
        "symbols": actual_symbols,
        "missing_expected_symbols": missing,
        "unexpected_symbols": unexpected,
        "stale_symbols": [] if not stale else actual_symbols,
        "duplicate_symbols_removed": duplicate_symbols_removed,
        "deprecated_or_default_fallback_used": fallback_used,
        "empty_universe": empty,
        "readiness": str(readiness.get("readiness") or "UNKNOWN"),
        "blocking_inputs": list(readiness.get("blocking_inputs") or []),
        "warning_inputs": list(readiness.get("warning_inputs") or []),
        "validation_status": status,
    }


def build_sleeve_universe_validation_v1(*, truth_root: Path, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    truth_root = truth_root.resolve()
    runtime_path = _runtime_evaluation_path(truth_root, day_utc)
    runtime = read_json_object_v1(runtime_path)
    outcomes, rollup = _outcomes_by_engine(truth_root, day_utc)
    readiness = _readiness_by_sleeve(truth_root, day_utc)
    policies = _load_policy_by_engine()
    engines = _load_active_engines()
    sleeve_reports: list[dict[str, Any]] = []
    for engine_id in sorted(outcomes):
        if engine_id not in engines or engine_id not in policies:
            continue
        sleeve_reports.append(
            _validate_one(
                truth_root=truth_root,
                day_utc=day_utc,
                engine_id=engine_id,
                policy=policies[engine_id],
                outcome=outcomes[engine_id],
                readiness=readiness.get(engine_id, {}),
            )
        )
    failure_count = sum(1 for row in sleeve_reports if row["validation_status"] == "FAIL")
    blocked_count = sum(1 for row in sleeve_reports if row["execution_status"].upper() == "BLOCKED")
    fallback_count = sum(1 for row in sleeve_reports if row["deprecated_or_default_fallback_used"])
    stale_count = sum(1 for row in sleeve_reports if row["source_day_status"]["stale_source"] or row["source_day_status"]["missing_source_day"])
    duplicate_issue_count = sum(1 for row in sleeve_reports if not row["duplicate_symbols_removed"])
    overall = "PASS"
    if failure_count:
        overall = "FAIL"
    elif blocked_count:
        overall = "PASS_BLOCKED_BY_READINESS"
    return {
        "schema_id": "sleeve_universe_validation",
        "schema_version": "v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc or _now_utc_v1(),
        "truth_root": str(truth_root),
        "runtime_evaluation_path": str(runtime_path),
        "runtime_evaluation_hash": str(runtime.get("deterministic_output_hash") or ""),
        "runtime_truth_classification": str(runtime.get("runtime_truth_classification") or ""),
        "runtime_highest_readiness_layer": str(runtime.get("highest_readiness_layer") or ""),
        "trade_advice_allowed": _capability_allowed(runtime, "TRADE_ADVICE_ALLOWED"),
        "autonomous_execution_allowed": _capability_allowed(runtime, "AUTONOMOUS_EXECUTION_ALLOWED"),
        "schedule_metadata": sleeve_schedule_metadata_v1(),
        "sleeve_rollup_path": str(_sleeve_rollup_path(truth_root, day_utc)),
        "sleeve_rollup_hash": hash_file_v1(_sleeve_rollup_path(truth_root, day_utc)),
        "sleeve_rollup_status": str(rollup.get("status") or ""),
        "sleeve_rollup_canonical_blocker": str(rollup.get("canonical_blocker") or ""),
        "overall_status": overall,
        "sleeve_count": len(sleeve_reports),
        "blocked_sleeve_count": blocked_count,
        "universe_validation_failure_count": failure_count,
        "fallback_or_default_usage_count": fallback_count,
        "stale_source_count": stale_count,
        "duplicate_symbol_issue_count": duplicate_issue_count,
        "sleeves": sleeve_reports,
    }


def write_sleeve_universe_validation_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    report_dir = truth_root / REPORT_REL / day_utc
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / "sleeve_universe_validation.v1.json"
    txt_path = report_dir / "sleeve_universe_validation.v1.txt"
    tmp_json = json_path.with_suffix(".json.tmp")
    tmp_txt = txt_path.with_suffix(".txt.tmp")
    tmp_json.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    lines = [
        f"Sleeve universe validation: {payload['overall_status']}",
        f"day_utc: {payload['day_utc']}",
        f"runtime_evaluation_hash: {payload['runtime_evaluation_hash']}",
        f"schedule_times_utc: {','.join(payload['schedule_metadata']['times_utc'])}",
        f"sleeves: {payload['sleeve_count']}",
        f"blocked_sleeves: {payload['blocked_sleeve_count']}",
        f"fallback_or_default_usage_count: {payload['fallback_or_default_usage_count']}",
        f"stale_source_count: {payload['stale_source_count']}",
        "",
    ]
    for row in payload["sleeves"]:
        lines.append(
            f"{row['engine_id']}: {row['validation_status']} execution={row['execution_status']} "
            f"symbols={row['symbol_count']} source={row['actual_universe_source']} "
            f"hash={row['universe_hash']} blocker={row['canonical_blocker']}"
        )
    tmp_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(tmp_json, json_path)
    os.replace(tmp_txt, txt_path)
    return {"json_path": str(json_path), "txt_path": str(txt_path)}
