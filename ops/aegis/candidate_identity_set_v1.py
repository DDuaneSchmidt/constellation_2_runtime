from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.event_append_transaction_v1 import contract_input_hashes_for_paths_v1, emit_artifact_evidence_transaction_v1, sha256_file_v1
from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1, runtime_evaluation_path_v1
from ops.aegis.capital_authority_allocation_v1 import allocation_path_v1

SCHEMA_ID = "candidate_identity_set"
SCHEMA_VERSION = "v1"
PRODUCER_ID = "ops/tools/build_candidate_identity_set_v1.py"
PRODUCER_VERSION = "v1"


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def candidate_identity_set_path_v1(*, truth_root: Path | str, day_utc: str, intent_hash: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "candidate_identity_set_v1" / day_utc / intent_hash.lower() / "candidate_identity_set.v1.json"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _selected_pointer_path(root: Path) -> Path:
    return root / "pointers" / "selected_intent_pointer.v1.json"


def _construction_path(root: Path, day: str) -> Path:
    return root / "reports" / "paper_trade_construction_v1" / day / "paper_trade_construction.v1.json"


def _risk_path(root: Path, day: str, intent_hash: str) -> Path:
    return root / "risk_definition_contract_v1" / day / intent_hash.lower() / "risk_definition_contract.v1.json"


def _conversion_latest(root: Path, day: str, intent_id: str) -> tuple[Path, dict[str, Any]]:
    base = root / "reports" / "exposure_intent_paper_submission_package_v1" / day
    matches: list[tuple[int, Path, dict[str, Any]]] = []
    if base.exists():
        for path in base.glob("*/exposure_intent_paper_submission_package.v1.json"):
            obj = _read_json(path)
            if str(obj.get("exposure_intent_id") or "") == intent_id:
                matches.append((path.stat().st_mtime_ns, path.resolve(), obj))
    if not matches:
        return Path(), {}
    _mtime, path, obj = sorted(matches, key=lambda row: (row[0], str(row[1])))[-1]
    return path, obj


def _source_hash(construction: Mapping[str, Any], artifact_id: str) -> str:
    rows = construction.get("source_artifacts") if isinstance(construction.get("source_artifacts"), list) else []
    for row in rows:
        if isinstance(row, Mapping) and str(row.get("artifact_id") or "") == artifact_id:
            return str(row.get("sha256") or "")
    return ""


def _actual_phasec_status(mirror_path: str) -> dict[str, Any]:
    if not mirror_path:
        return {"status": "NO_PHASEC_PATH", "order_plan_path": "", "order_plan_hash": "", "reason": ""}
    risk_path = Path(mirror_path).expanduser().resolve()
    intent_hash = risk_path.parent.name
    day = risk_path.parent.parent.name
    phasec = risk_path.parents[3] / "phaseC_preflight_v1" / day
    candidates = sorted(phasec.glob(f"attempt_*/{intent_hash}/equity_order_plan.v2.json")) if phasec.exists() else []
    if not candidates:
        return {"status": "MISSING", "order_plan_path": "", "order_plan_hash": "", "reason": "PHASE_C_ORDER_PLAN_MISSING"}
    path = candidates[-1].resolve()
    obj = _read_json(path)
    return {
        "status": "PRESENT",
        "order_plan_path": str(path),
        "order_plan_hash": sha256_file_v1(path),
        "source_intent_id": str(obj.get("source_intent_id") or ""),
        "intent_hash": str(obj.get("intent_hash") or obj.get("intent_sha256") or ""),
    }


def build_candidate_identity_set_v1(*, truth_root: Path | str, day_utc: str, intent_hash: str = "", generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    pointer_path = _selected_pointer_path(root)
    pointer = _read_json(pointer_path)
    selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    selected_hash = str(intent_hash or selected.get("intent_hash") or "").lower()
    selected_id = str(selected.get("intent_id") or "")
    selected_path = Path(str(selected.get("intent_path") or "")).expanduser().resolve() if selected.get("intent_path") else Path()
    intent = _read_json(selected_path) if selected_path.exists() else {}
    construction_path = _construction_path(root, day_utc)
    construction = _read_json(construction_path)
    allocation_path = allocation_path_v1(truth_root=root, day_utc=day_utc)
    allocation = _read_json(allocation_path)
    risk_path = _risk_path(root, day_utc, selected_hash)
    risk = _read_json(risk_path)
    runtime_path = runtime_evaluation_path_v1(truth_root=root, day_utc=day_utc)
    runtime = read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day_utc)
    runtime_hash = str(runtime.get("deterministic_output_hash") or "")
    conversion_path, conversion = _conversion_latest(root, day_utc, selected_id)
    allocation_hash = sha256_file_v1(allocation_path) if allocation_path.exists() else ""
    risk_hash = sha256_file_v1(risk_path) if risk_path.exists() else ""
    construction_hash = sha256_file_v1(construction_path) if construction_path.exists() else ""
    pointer_hash = sha256_file_v1(pointer_path) if pointer_path.exists() else ""
    source_candidate_hash = _source_hash(construction, "selected_exposure_intent") or (sha256_file_v1(selected_path) if selected_path.exists() else "")
    symbol = str(construction.get("symbol") or selected.get("symbol") or ((intent.get("underlying") or {}) if isinstance(intent.get("underlying"), dict) else {}).get("symbol") or "").upper()
    sleeve_id = str(construction.get("sleeve_id") or construction.get("engine_id") or selected.get("sleeve_id") or selected.get("engine_id") or "")
    side = "BUY" if str(construction.get("direction") or "LONG").upper() in {"LONG", "BUY"} else "SELL"
    actual_phasec = _actual_phasec_status(str(risk.get("execution_mirror_path") or ""))
    blockers: list[str] = []
    if str(pointer.get("day_utc") or "") != day_utc or str(pointer.get("status") or "").upper() != "SELECTED":
        blockers.append("SELECTED_INTENT_POINTER_MISMATCH")
    if not selected_hash or not selected_id:
        blockers.append("SELECTED_INTENT_POINTER_MISMATCH")
    if selected_path and not selected_path.exists():
        blockers.append("SOURCE_CANDIDATE_MISSING")
    if str(construction.get("selected_exposure_intent_id") or "") and str(construction.get("selected_exposure_intent_id") or "") != selected_id:
        blockers.append("CANDIDATE_IDENTITY_SET_MISMATCH")
    construction_selected_hash = str(construction.get("selected_exposure_intent_hash") or _source_hash(construction, "selected_exposure_intent") or "").lower()
    if construction_selected_hash and selected_hash and construction_selected_hash != selected_hash:
        blockers.append("CANDIDATE_IDENTITY_SET_MISMATCH")
    if runtime_hash and str(construction.get("runtime_evaluation_hash") or "") and str(construction.get("runtime_evaluation_hash") or "") != runtime_hash:
        blockers.append("RUNTIME_HASH_MISMATCH")
    if runtime_hash and str(allocation.get("runtime_evaluation_hash") or "") and str(allocation.get("runtime_evaluation_hash") or "") != runtime_hash:
        blockers.append("RUNTIME_HASH_MISMATCH")
    if runtime_hash and str(risk.get("runtime_evaluation_hash") or "") and str(risk.get("runtime_evaluation_hash") or "") != runtime_hash:
        blockers.append("RUNTIME_HASH_MISMATCH")
    if str(allocation.get("validation_status") or "").upper() != "VALID":
        blockers.append("CAPITAL_AUTHORITY_ALLOCATION_INVALID")
    if str(risk.get("validation_status") or "").upper() != "PASS":
        blockers.append("RISK_DEFINITION_CONTRACT_INVALID")
    if risk_hash and str(risk.get("capital_allocation_hash") or "") != allocation_hash:
        blockers.append("CANDIDATE_IDENTITY_SET_MISMATCH")
    if risk and str(risk.get("construction_contract_hash") or "") != str(construction.get("construction_contract_hash") or ""):
        blockers.append("CANDIDATE_IDENTITY_SET_MISMATCH")
    stale_order_plan_reason = ""
    if actual_phasec.get("status") == "PRESENT":
        if str(actual_phasec.get("source_intent_id") or "") != selected_id or str(actual_phasec.get("intent_hash") or "").lower() != selected_hash:
            stale_order_plan_reason = "PHASE_C_ORDER_PLAN_IDENTITY_MISMATCH"
            blockers.append("STALE_PHASE_C_ORDER_PLAN")
    status = "VALID" if not [b for b in blockers if b != "STALE_PHASE_C_ORDER_PLAN"] else "REJECTED"
    # A stale Phase C plan is regenerable when all upstream identity inputs are valid.
    if blockers == ["STALE_PHASE_C_ORDER_PLAN"]:
        status = "VALID"
    input_hashes = contract_input_hashes_for_paths_v1(
        [path for path in [pointer_path, selected_path, construction_path, allocation_path, risk_path, runtime_path, conversion_path] if path and Path(path).exists()],
        extra={"runtime_evaluation_hash": runtime_hash, "intent_hash": selected_hash},
    )
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": "",
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc or now_utc_v1(),
        "candidate_id": selected_id,
        "exposure_id": selected_id,
        "intent_hash": selected_hash,
        "sleeve_id": sleeve_id,
        "symbol": symbol,
        "side": side,
        "source_candidate_artifact_path": str(selected_path) if selected_path else "",
        "source_candidate_artifact_hash": source_candidate_hash,
        "selected_intent_pointer_path": str(pointer_path),
        "selected_intent_pointer_hash": pointer_hash,
        "runtime_evaluation_hash": runtime_hash,
        "allocation_path": str(allocation_path),
        "allocation_hash": allocation_hash,
        "risk_contract_path": str(risk_path),
        "risk_contract_hash": risk_hash,
        "construction_contract_hash": str(construction.get("construction_contract_hash") or ""),
        "paper_trade_construction_path": str(construction_path),
        "paper_trade_construction_hash": construction_hash,
        "conversion_artifact_path": str(conversion_path) if conversion_path and conversion_path.is_file() else "",
        "conversion_artifact_hash": sha256_file_v1(conversion_path) if conversion_path and conversion_path.is_file() else "",
        "expected_phasec_candidate_path": str(Path(str(risk.get("execution_mirror_path") or "")).parents[3] / "phaseC_preflight_v1" / day_utc / ("attempt_" + selected_hash[:12].upper()) / selected_hash) if risk.get("execution_mirror_path") else "",
        "actual_phasec_order_plan": actual_phasec,
        "stale_order_plan_reason": stale_order_plan_reason,
        "validation_status": status,
        "blocker_codes": sorted(set(blockers)),
        "input_hashes": input_hashes,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
        "output_hash": "",
    }
    payload["artifact_id"] = f"candidate_identity_set_v1:{day_utc}:{stable_hash_v1({**payload, 'artifact_id': '', 'output_hash': ''})[:24]}"
    payload["output_hash"] = stable_hash_v1({**payload, "output_hash": ""})
    return payload


def write_candidate_identity_set_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any], emit_events: bool = True) -> Path:
    root = Path(truth_root).expanduser().resolve()
    path = candidate_identity_set_path_v1(truth_root=root, day_utc=day_utc, intent_hash=str(payload.get("intent_hash") or ""))
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
            created_at_utc=str(payload.get("generated_at_utc") or ""),
            input_hashes=dict(payload.get("input_hashes") or {}),
            validation_status="VALID" if str(payload.get("validation_status") or "").upper() == "VALID" else "REJECTED",
        )
    return path


def build_and_write_candidate_identity_set_v1(*, truth_root: Path | str, day_utc: str, intent_hash: str = "", generated_at_utc: str | None = None, emit_events: bool = True) -> tuple[dict[str, Any], Path]:
    payload = build_candidate_identity_set_v1(truth_root=truth_root, day_utc=day_utc, intent_hash=intent_hash, generated_at_utc=generated_at_utc)
    path = write_candidate_identity_set_v1(truth_root=truth_root, day_utc=day_utc, payload=payload, emit_events=emit_events)
    return {**payload, "artifact_path": str(path), "artifact_hash": sha256_file_v1(path)}, path


def load_candidate_identity_set_v1(*, truth_root: Path | str, day_utc: str, intent_hash: str) -> tuple[Path, dict[str, Any], str]:
    path = candidate_identity_set_path_v1(truth_root=truth_root, day_utc=day_utc, intent_hash=intent_hash)
    if not path.exists():
        return path, {}, "CANDIDATE_IDENTITY_SET_MISSING"
    payload = _read_json(path)
    if str(payload.get("validation_status") or "").upper() != "VALID":
        blockers = ",".join(str(item) for item in payload.get("blocker_codes") or [] if str(item))
        if "STALE_PHASE_C_ORDER_PLAN" in blockers:
            return path, payload, "STALE_PHASE_C_ORDER_PLAN"
        if "SELECTED_INTENT_POINTER_MISMATCH" in blockers:
            return path, payload, "SELECTED_INTENT_POINTER_MISMATCH"
        return path, payload, "CANDIDATE_IDENTITY_SET_MISMATCH"
    return path, payload, ""
