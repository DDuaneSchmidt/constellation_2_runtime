from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.event_append_transaction_v1 import contract_input_hashes_for_paths_v1, emit_artifact_evidence_transaction_v1, sha256_file_v1
from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1, runtime_evaluation_path_v1

SCHEMA_ID = "engine_activity_authorization"
SCHEMA_VERSION = "v1"
PRODUCER_ID = "ops/tools/build_engine_activity_authorization_v1.py"
PRODUCER_VERSION = "v1"


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def engine_activity_authorization_path_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    intent_hash: str,
    sleeve_id: str = "PRIMARY",
    environment: str = "PAPER",
) -> Path:
    root = Path(truth_root).expanduser().resolve()
    execution_root = root.parent / "truth_sleeves" / sleeve_id.upper() / environment.upper()
    return execution_root / "engine_activity_v1" / "authorization_v1" / day_utc / f"{intent_hash.lower()}.authorization.v1.json"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _selected_pointer_path(root: Path) -> Path:
    return root / "pointers" / "selected_intent_pointer.v1.json"


def _candidate_identity_path(root: Path, day: str, intent_hash: str) -> Path:
    return root / "reports" / "candidate_identity_set_v1" / day / intent_hash.lower() / "candidate_identity_set.v1.json"


def _sleeve_readiness_path(root: Path, day: str) -> Path:
    return root / "reports" / "aegis_sleeve_readiness_v1" / day / "sleeve_readiness.v1.json"


def _market_inputs_path(root: Path, day: str) -> Path:
    return root / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json"


def _engine_registry_path(repo_root: Path) -> Path:
    return repo_root / "governance" / "02_REGISTRIES" / "ENGINE_MODEL_REGISTRY_V1.json"


def _registry_engine(registry: Mapping[str, Any], engine_id: str) -> dict[str, Any]:
    rows = registry.get("engines") if isinstance(registry.get("engines"), list) else []
    for row in rows:
        if isinstance(row, Mapping) and str(row.get("engine_id") or "").strip().upper() == engine_id.upper():
            return dict(row)
    return {}


def _sleeve_readiness_row(payload: Mapping[str, Any], engine_id: str) -> dict[str, Any]:
    rows = payload.get("sleeves") if isinstance(payload.get("sleeves"), list) else []
    for row in rows:
        if isinstance(row, Mapping) and str(row.get("sleeve_id") or "").strip().upper() == engine_id.upper():
            return dict(row)
    return {}


def _authorization_status(blockers: list[str]) -> str:
    if not blockers:
        return "AUTHORIZED"
    if any(code.endswith("_MISSING") or code == "MISSING_INPUT" for code in blockers):
        return "MISSING_INPUT"
    if "SLEEVE_POLICY_DISABLED" in blockers or "ENGINE_POLICY_DISABLED" in blockers:
        return "POLICY_DISABLED"
    if any("STALE" in code or "RUNTIME_HASH_MISMATCH" in code for code in blockers):
        return "STALE"
    return "REJECTED"


def build_engine_activity_authorization_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    intent_hash: str = "",
    sleeve_id: str = "PRIMARY",
    environment: str = "PAPER",
    repo_root: Path | str | None = None,
    engine_registry_path: Path | str | None = None,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    repo = Path(repo_root).expanduser().resolve() if repo_root else Path(__file__).resolve().parents[2]
    runtime_path = runtime_evaluation_path_v1(truth_root=root, day_utc=day_utc)
    runtime = read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day_utc)
    runtime_hash = str(runtime.get("deterministic_output_hash") or "")
    selected_pointer_path = _selected_pointer_path(root)
    selected_pointer = _read_json(selected_pointer_path)
    selected = selected_pointer.get("selected_intent") if isinstance(selected_pointer.get("selected_intent"), dict) else {}
    selected_hash = str(intent_hash or selected.get("intent_hash") or "").strip().lower()
    selected_path = Path(str(selected.get("intent_path") or "")).expanduser().resolve() if selected.get("intent_path") else Path()
    selected_intent = _read_json(selected_path) if selected_path and selected_path.exists() else {}
    candidate_identity_path = _candidate_identity_path(root, day_utc, selected_hash)
    candidate_identity = _read_json(candidate_identity_path)
    sleeve_readiness_path = _sleeve_readiness_path(root, day_utc)
    sleeve_readiness = _read_json(sleeve_readiness_path)
    market_inputs_path = _market_inputs_path(root, day_utc)
    market_inputs = _read_json(market_inputs_path)
    registry_path = Path(engine_registry_path).expanduser().resolve() if engine_registry_path else _engine_registry_path(repo)
    registry = _read_json(registry_path)

    engine_id = str(
        candidate_identity.get("sleeve_id")
        or selected.get("engine_id")
        or selected.get("sleeve_id")
        or selected_intent.get("engine_id")
        or selected_intent.get("sleeve_id")
        or ""
    ).strip()
    candidate_id = str(candidate_identity.get("candidate_id") or selected.get("intent_id") or selected_intent.get("intent_id") or "")
    exposure_id = str(candidate_identity.get("exposure_id") or candidate_id)
    symbol = str(candidate_identity.get("symbol") or selected.get("symbol") or selected_intent.get("symbol") or "").upper()
    side = str(candidate_identity.get("side") or selected_intent.get("side") or "BUY").upper()

    engine = _registry_engine(registry, engine_id)
    runner_rel = str(engine.get("engine_runner_path") or "")
    runner_path = (repo / runner_rel).resolve() if runner_rel else Path()
    expected_runner_hash = str(engine.get("engine_runner_sha256") or "").strip().lower()
    actual_runner_hash = sha256_file_v1(runner_path) if runner_path.exists() and runner_path.is_file() else ""
    readiness_row = _sleeve_readiness_row(sleeve_readiness, engine_id)
    readiness_status = str(readiness_row.get("readiness") or "").strip().upper()
    market_status = str(market_inputs.get("validation_status") or market_inputs.get("status") or "").strip().upper()

    blockers: list[str] = []
    if str(selected_pointer.get("day_utc") or "") != day_utc or str(selected_pointer.get("status") or "").upper() != "SELECTED":
        blockers.append("SELECTED_INTENT_POINTER_MISMATCH")
    if not selected_hash or not candidate_id:
        blockers.append("MISSING_INPUT")
    if str(candidate_identity.get("validation_status") or "").upper() != "VALID":
        blockers.append("STALE_CANDIDATE_IDENTITY" if candidate_identity else "CANDIDATE_IDENTITY_SET_MISSING")
    if runtime_hash and str(candidate_identity.get("runtime_evaluation_hash") or "") and str(candidate_identity.get("runtime_evaluation_hash") or "") != runtime_hash:
        blockers.append("RUNTIME_HASH_MISMATCH")
    if str(candidate_identity.get("intent_hash") or "").lower() not in {"", selected_hash}:
        blockers.append("CANDIDATE_IDENTITY_SET_MISMATCH")
    if not engine:
        blockers.append("ENGINE_POLICY_DISABLED")
    elif str(engine.get("activation_status") or "").upper() != "ACTIVE":
        blockers.append("ENGINE_POLICY_DISABLED")
    if expected_runner_hash and actual_runner_hash and expected_runner_hash != actual_runner_hash:
        blockers.append("ENGINE_RUNNER_SHA256_MISMATCH")
    if expected_runner_hash and not actual_runner_hash:
        blockers.append("ENGINE_RUNNER_MISSING")
    if readiness_status not in {"READY", "READY_WITH_WARNINGS", "VALID", "OK", "PASS"}:
        blockers.append("SLEEVE_READINESS_NOT_READY" if readiness_row else "SLEEVE_READINESS_MISSING")
    if market_status not in {"VALID", "OK", "READY", "PASS"}:
        blockers.append("MARKET_READINESS_MISSING" if not market_inputs else "MARKET_READINESS_INVALID")

    auth_status = _authorization_status(sorted(set(blockers)))
    validation_status = "VALID" if auth_status == "AUTHORIZED" else "REJECTED"
    input_paths = [
        runtime_path,
        selected_pointer_path,
        selected_path,
        candidate_identity_path,
        sleeve_readiness_path,
        market_inputs_path,
        registry_path,
        runner_path,
    ]
    input_hashes = contract_input_hashes_for_paths_v1(
        [path for path in input_paths if path and Path(path).exists()],
        extra={
            "runtime_evaluation_hash": runtime_hash,
            "intent_hash": selected_hash,
            "candidate_identity_hash": sha256_file_v1(candidate_identity_path) if candidate_identity_path.exists() else "",
            "source_candidate_identity_hash": sha256_file_v1(candidate_identity_path) if candidate_identity_path.exists() else "",
            "source_selected_intent_pointer_hash": sha256_file_v1(selected_pointer_path) if selected_pointer_path.exists() else "",
            "sleeve_readiness_hash": sha256_file_v1(sleeve_readiness_path) if sleeve_readiness_path.exists() else "",
            "market_readiness_hash": sha256_file_v1(market_inputs_path) if market_inputs_path.exists() else "",
        },
    )
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": "",
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc or now_utc_v1(),
        "runtime_evaluation_hash": runtime_hash,
        "sleeve_id": engine_id,
        "engine_id": engine_id,
        "exposure_id": exposure_id,
        "candidate_id": candidate_id,
        "intent_hash": selected_hash,
        "symbol": symbol,
        "side": side,
        "target_sleeve_id": sleeve_id.upper(),
        "target_environment": environment.upper(),
        "selected_intent_pointer_path": str(selected_pointer_path),
        "selected_intent_pointer_hash": sha256_file_v1(selected_pointer_path) if selected_pointer_path.exists() else "",
        "candidate_identity_path": str(candidate_identity_path),
        "candidate_identity_hash": sha256_file_v1(candidate_identity_path) if candidate_identity_path.exists() else "",
        "candidate_identity_runtime_evaluation_hash": str(candidate_identity.get("runtime_evaluation_hash") or ""),
        "sleeve_readiness_path": str(sleeve_readiness_path),
        "sleeve_readiness_hash": sha256_file_v1(sleeve_readiness_path) if sleeve_readiness_path.exists() else "",
        "sleeve_readiness_status": readiness_status,
        "engine_runner_path": str(runner_path) if runner_path else "",
        "engine_runner_sha256_expected": expected_runner_hash,
        "engine_runner_sha256_actual": actual_runner_hash,
        "engine_runner_hash_match": bool(expected_runner_hash and actual_runner_hash and expected_runner_hash == actual_runner_hash),
        "market_readiness_path": str(market_inputs_path),
        "market_readiness_hash": sha256_file_v1(market_inputs_path) if market_inputs_path.exists() else "",
        "market_readiness_status": market_status,
        "authorization_status": auth_status,
        "status": "AUTHORIZED" if auth_status == "AUTHORIZED" else "REJECTED",
        "authorization": {
            "decision": "AUTHORIZED" if auth_status == "AUTHORIZED" else "REJECTED",
            "authorized_quantity": 1 if auth_status == "AUTHORIZED" else 0,
            "scope": "CONSTRUCTION_AND_CONVERSION_ONLY",
        },
        "blocker_reasons": sorted(set(blockers)),
        "blocker_codes": sorted(set(blockers)),
        "validation_status": validation_status,
        "input_hashes": input_hashes,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
        "output_hash": "",
    }
    payload["artifact_id"] = f"engine_activity_authorization_v1:{day_utc}:{stable_hash_v1({**payload, 'artifact_id': '', 'output_hash': ''})[:24]}"
    payload["output_hash"] = stable_hash_v1({**payload, "output_hash": ""})
    return payload


def write_engine_activity_authorization_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    payload: Mapping[str, Any],
    sleeve_id: str = "PRIMARY",
    environment: str = "PAPER",
    emit_events: bool = True,
) -> Path:
    root = Path(truth_root).expanduser().resolve()
    path = engine_activity_authorization_path_v1(
        truth_root=root,
        day_utc=day_utc,
        intent_hash=str(payload.get("intent_hash") or ""),
        sleeve_id=sleeve_id,
        environment=environment,
    )
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


def build_and_write_engine_activity_authorization_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
    intent_hash: str = "",
    sleeve_id: str = "PRIMARY",
    environment: str = "PAPER",
    repo_root: Path | str | None = None,
    engine_registry_path: Path | str | None = None,
    generated_at_utc: str | None = None,
    emit_events: bool = True,
) -> tuple[dict[str, Any], Path]:
    payload = build_engine_activity_authorization_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        intent_hash=intent_hash,
        sleeve_id=sleeve_id,
        environment=environment,
        repo_root=repo_root,
        engine_registry_path=engine_registry_path,
        generated_at_utc=generated_at_utc,
    )
    path = write_engine_activity_authorization_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        payload=payload,
        sleeve_id=sleeve_id,
        environment=environment,
        emit_events=emit_events,
    )
    return {**payload, "artifact_path": str(path), "artifact_hash": sha256_file_v1(path)}, path
