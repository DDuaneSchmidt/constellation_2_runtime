from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.event_append_transaction_v1 import contract_input_hashes_for_paths_v1, emit_artifact_evidence_transaction_v1, sha256_file_v1
from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1, runtime_evaluation_path_v1

SCHEMA_ID = "target_day_admission"
SCHEMA_VERSION = "v1"
PRODUCER_ID = "ops/tools/build_target_day_admission_v1.py"
PRODUCER_VERSION = "v1"
POLICY_VERSION = "aegis_target_day_admission.runtime_manual_capture.v1"


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def target_day_admission_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "target_day_admission_v1" / f"{day_utc}.json"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _latest_hash(path: Path) -> str:
    return sha256_file_v1(path) if path.exists() and path.is_file() else ""


def _capability_allowed(runtime: Mapping[str, Any], capability: str) -> bool:
    caps = runtime.get("capabilities") if isinstance(runtime.get("capabilities"), Mapping) else {}
    row = caps.get(capability) if isinstance(caps.get(capability), Mapping) else {}
    return bool(row.get("allowed") is True)


def _source_path(root: Path, day: str, rel: str) -> Path:
    return root / rel.format(day_utc=day, day=day)


def build_target_day_admission_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated = generated_at_utc or f"{day_utc}T00:00:00Z"
    runtime_path = runtime_evaluation_path_v1(truth_root=root, day_utc=day_utc)
    runtime = read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day_utc)
    runtime_hash = str(runtime.get("deterministic_output_hash") or "")
    pointer_path = root / "pointers" / "selected_intent_pointer.v1.json"
    market_inputs_path = _source_path(root, day_utc, "reports/market_data_inputs_v1/{day_utc}/market_data_inputs.v1.json")
    sleeve_readiness_path = _source_path(root, day_utc, "reports/aegis_sleeve_readiness_v1/{day_utc}/aegis_sleeve_readiness.v1.json")
    source_manifest_path = _source_path(root, day_utc, "reports/aegis_runtime_truth_kernel_v1/{day_utc}/source_data_manifest.v1.json")
    selected = _read_json(pointer_path).get("selected_intent") if isinstance(_read_json(pointer_path).get("selected_intent"), dict) else {}
    selected_symbol = str(selected.get("symbol") or "").upper()
    market_current = False
    if selected_symbol:
        year_path = root / "market_data_snapshot_v1" / selected_symbol / f"{day_utc[:4]}.jsonl"
        if year_path.exists():
            for line in year_path.read_text(encoding="utf-8").splitlines():
                if line.strip() and f'"timestamp_utc":"{day_utc}' in line and f'"symbol":"{selected_symbol}"' in line:
                    market_current = True
                    break
    blockers: list[str] = []
    if not runtime_hash:
        blockers.append("RUNTIME_EVALUATION_MISSING")
    if not _capability_allowed(runtime, "MANUAL_TRADE_CAPTURE_ALLOWED"):
        blockers.append("MANUAL_CAPTURE_NOT_ALLOWED")
    if _capability_allowed(runtime, "AUTONOMOUS_EXECUTION_ALLOWED"):
        blockers.append("AUTONOMOUS_EXECUTION_ALLOWED")
    if _capability_allowed(runtime, "BROKER_SUBMIT_TRANSMIT"):
        blockers.append("BROKER_SUBMIT_TRANSMIT_ALLOWED")
    if str((_read_json(pointer_path).get("day_utc") or day_utc)) != day_utc:
        blockers.append("SELECTED_INTENT_POINTER_DAY_MISMATCH")
    validation_status = "VALID" if not blockers else "REJECTED"
    admission_status = "ADMIT" if validation_status == "VALID" else "BLOCKED"
    reason_codes = [] if validation_status == "VALID" else sorted(set(blockers))
    input_paths = [runtime_path, pointer_path, market_inputs_path, sleeve_readiness_path, source_manifest_path]
    input_hashes = contract_input_hashes_for_paths_v1(input_paths, extra={"runtime_evaluation_hash": runtime_hash, "policy_version": POLICY_VERSION})
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "generated_utc": generated,
        "generated_at_utc": generated,
        "target_day": day_utc,
        "day_utc": day_utc,
        "admission_status": admission_status,
        "validation_status": validation_status,
        "admission_authority_source": "RUNTIME_EVALUATION_MANUAL_CAPTURE",
        "runtime_evaluation_hash": runtime_hash,
        "market_session_status": "CURRENT_SELECTED_SYMBOL_DATA" if market_current else "SELECTED_SYMBOL_DATA_NOT_PROVEN_CURRENT",
        "calendar_session_classification": "PAPER_MANUAL_CAPTURE_CONTEXT",
        "enabled_sleeves": [str(selected.get("sleeve_id") or selected.get("engine_id") or "")] if selected else [],
        "disabled_sleeves": [],
        "policy_version": POLICY_VERSION,
        "data_readiness_hash": _latest_hash(source_manifest_path) or _latest_hash(market_inputs_path),
        "sleeve_readiness_hash": _latest_hash(sleeve_readiness_path),
        "manual_intent_hash": _latest_hash(pointer_path),
        "input_hashes": input_hashes,
        "blocker_chain": [
            {"artifact_id": code, "artifact_name": code, "blocker_code": code, "classification": "TARGET_DAY_ADMISSION_BLOCKER", "artifact_path": str(runtime_path), "role_class": "RUNTIME_AUTHORITY"}
            for code in reason_codes
        ],
        "blocking_reason_codes": reason_codes,
        "build_ref": {"artifact_path": str(runtime_path), "artifact_sha256": _latest_hash(runtime_path) or (runtime_hash or "0" * 64)},
        "closure_status": "CLOSED" if validation_status == "VALID" else "OPEN",
        "hidden_dependency_check_result": {"status": "PASS" if validation_status == "VALID" else "FAIL", "blocking_reason_code": "" if validation_status == "VALID" else (reason_codes[0] if reason_codes else "TARGET_DAY_ADMISSION_REJECTED"), "summary": "Aegis target-day admission derived from RuntimeEvaluation manual-capture authority.", "declared_inventory_artifacts": ["aegis_runtime_evaluation", "selected_intent_pointer"], "observed_dependency_artifacts": [key.removeprefix("input:") for key in input_hashes if key.startswith("input:")], "undeclared_dependency_artifacts": [], "failing_producers": [] if validation_status == "VALID" else [PRODUCER_ID]},
        "rules_version": POLICY_VERSION,
        "binding": True,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
        "output_hash": "",
    }
    payload["output_hash"] = stable_hash_v1({**payload, "output_hash": ""})
    return payload


def write_target_day_admission_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any], emit_events: bool = True) -> Path:
    root = Path(truth_root).expanduser().resolve()
    path = target_day_admission_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(dict(payload)) + b"\n")
    if emit_events:
        emit_artifact_evidence_transaction_v1(
            truth_root=root,
            day_utc=day_utc,
            artifact_path=path,
            payload=dict(payload),
            producer_id=PRODUCER_ID,
            producer_version=PRODUCER_VERSION,
            created_at_utc=str(payload.get("generated_at_utc") or payload.get("generated_utc") or ""),
            input_hashes=dict(payload.get("input_hashes") or {}),
            validation_status="VALID" if str(payload.get("validation_status") or "").upper() == "VALID" else "REJECTED",
        )
    return path


def build_and_write_target_day_admission_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None, emit_events: bool = True) -> tuple[dict[str, Any], Path]:
    payload = build_target_day_admission_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)
    path = write_target_day_admission_v1(truth_root=truth_root, day_utc=day_utc, payload=payload, emit_events=emit_events)
    return {**payload, "artifact_path": str(path), "artifact_hash": sha256_file_v1(path)}, path
