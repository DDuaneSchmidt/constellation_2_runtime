#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from ops.tools.aegis_runtime_mode_v1 import runtime_mode_from_truth_root_v1

REPO_ROOT = Path(__file__).resolve().parents[2]
LEDGER_SCHEMA = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_artifact_ledger_record.v1.schema.json"
PROMOTION_SCHEMA = REPO_ROOT / "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_promotion_attestation.v1.schema.json"
LEDGER_SCHEMA_VERSION = "aegis_artifact_ledger_record.v1"
PROMOTION_SCHEMA_VERSION = "aegis_promotion_attestation.v1"
PROMOTION_POLICY_VERSION = "aegis_candidate_to_production_promotion.v1"


def now_iso_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def git_commit_v1() -> str:
    proc = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return str(proc.stdout or "").strip() if proc.returncode == 0 else ""


def canonical_bytes_v1(payload: Any) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def canonical_hash_v1(payload: Any) -> str:
    return hashlib.sha256(canonical_bytes_v1(payload)).hexdigest()


def artifact_hash_v1(path: Path) -> str:
    resolved = Path(path).expanduser().resolve()
    digest = hashlib.sha256()
    with resolved.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json_v1(path: Path) -> dict[str, Any]:
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return obj


def write_json_v1(path: Path, payload: dict[str, Any]) -> None:
    resolved = Path(path).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def ledger_path_v1(*, truth_root: Path, day: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_artifact_ledger_v1" / day / "artifact_ledger.v1.jsonl"


def promotion_attestation_path_v1(*, truth_root: Path, day: str, artifact_id: str) -> Path:
    safe_id = str(artifact_id or "").strip().replace("/", "_")
    return (
        Path(truth_root).expanduser().resolve()
        / "reports"
        / "aegis_promotion_attestation_v1"
        / day
        / f"{safe_id}.promotion_attestation.v1.json"
    )


def infer_runtime_root_v1(truth_root: Path) -> Path:
    resolved = Path(truth_root).expanduser().resolve()
    parts = list(resolved.parts)
    if "truth_sleeves" in parts:
        idx = parts.index("truth_sleeves")
        return Path(*parts[:idx]).resolve()
    if resolved.name in {"production_truth", "candidate_truth", "truth"}:
        return resolved.parent.resolve()
    return resolved.parent.resolve()


def _validate_schema(payload: dict[str, Any], schema_path: Path) -> None:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=lambda err: list(err.path))
    if errors:
        raise ValueError("AEGIS_LEDGER_SCHEMA_INVALID:" + ";".join(str(err.message) for err in errors[:3]))


def _path_is_under(path: Path, root: Path) -> bool:
    resolved = Path(path).expanduser().resolve()
    root_resolved = Path(root).expanduser().resolve()
    return resolved == root_resolved or root_resolved in resolved.parents


def _payload_day(payload: dict[str, Any]) -> str:
    for key in ("day", "day_utc", "target_day", "active_day", "trading_day"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value[:10]
    for key in ("generated_utc", "generated_at", "generated_at_utc", "created_at_utc"):
        value = str(payload.get(key) or "").strip()
        if len(value) >= 10:
            return value[:10]
    return ""


def _producer_contract(payload: dict[str, Any]) -> dict[str, Any]:
    contract = payload.get("producer_contract_v1")
    return contract if isinstance(contract, dict) else {}


def _producer_module(payload: dict[str, Any]) -> str:
    contract = _producer_contract(payload)
    producer = payload.get("producer") if isinstance(payload.get("producer"), dict) else {}
    return (
        str(contract.get("producer_name") or "").strip()
        or str(producer.get("module") or "").strip()
        or str(payload.get("producer") or "").strip()
        or str((payload.get("constitutional_lineage") or {}).get("producer_id") if isinstance(payload.get("constitutional_lineage"), dict) else "").strip()
    )


def _producer_git_commit(payload: dict[str, Any]) -> str:
    contract = _producer_contract(payload)
    producer = payload.get("producer") if isinstance(payload.get("producer"), dict) else {}
    return (
        str(contract.get("code_version_git_commit") or "").strip()
        or str(producer.get("git_sha") or "").strip()
        or str(payload.get("git_commit") or payload.get("source_git_commit") or "").strip()
        or str((payload.get("constitutional_lineage") or {}).get("code_version") if isinstance(payload.get("constitutional_lineage"), dict) else "").strip()
    )


def _input_refs(payload: dict[str, Any]) -> list[dict[str, Any]]:
    contract = _producer_contract(payload)
    refs = contract.get("input_artifacts") if isinstance(contract.get("input_artifacts"), list) else []
    return [row for row in refs if isinstance(row, dict)]


def _input_hashes(refs: list[dict[str, Any]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for row in refs:
        path = str(row.get("path") or "").strip()
        if not path:
            continue
        sha = str(row.get("sha256") or "").strip()
        if not sha:
            ref_path = Path(path).expanduser()
            sha = artifact_hash_v1(ref_path) if ref_path.exists() and ref_path.is_file() else ""
        out.append({"path": path, "sha256": sha})
    return out


def _status(payload: dict[str, Any]) -> str:
    for key in ("validation_status", "final_status", "status", "closure_status"):
        value = str(payload.get(key) or "").strip().upper()
        if value:
            return value
    return "UNKNOWN"


def _blockers(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("blockers", "reason_codes", "blocking_codes"):
        rows = payload.get(key)
        if isinstance(rows, list):
            values.extend(str(row) for row in rows if str(row or "").strip())
    blocker = str(payload.get("canonical_blocker") or payload.get("blocker") or "").strip()
    if blocker:
        values.append(blocker)
    return list(dict.fromkeys(values))


def build_artifact_ledger_record_v1(
    *,
    artifact_path: Path,
    artifact_type: str,
    truth_root: Path,
    runtime_root: Path,
    runtime_mode: str,
    day: str,
    recovery_command: str = "",
    account: str = "",
    sleeve: str = "",
    artifact_id: str = "",
) -> dict[str, Any]:
    path = Path(artifact_path).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"ARTIFACT_LEDGER_ARTIFACT_MISSING:{path}")
    payload = read_json_v1(path)
    truth = Path(truth_root).expanduser().resolve()
    runtime = Path(runtime_root).expanduser().resolve()
    mode = str(runtime_mode or runtime_mode_from_truth_root_v1(truth)).strip().upper()
    artifact_day = _payload_day(payload) or str(day or "").strip()
    if artifact_day != str(day).strip():
        raise ValueError(f"ARTIFACT_LEDGER_DAY_MISMATCH:artifact_day={artifact_day}:day={day}")
    if not _path_is_under(path, truth):
        raise ValueError(f"ARTIFACT_LEDGER_TRUTH_ROOT_MISMATCH:path={path}:truth_root={truth}")
    if not _path_is_under(truth, runtime):
        raise ValueError(f"ARTIFACT_LEDGER_RUNTIME_ROOT_MISMATCH:truth_root={truth}:runtime_root={runtime}")
    payload_truth = str(payload.get("truth_root") or "").strip()
    if payload_truth and Path(payload_truth).expanduser().resolve() != truth:
        raise ValueError(f"ARTIFACT_LEDGER_PAYLOAD_TRUTH_ROOT_MISMATCH:{payload_truth}")
    payload_runtime = str(payload.get("runtime_root") or "").strip()
    if payload_runtime and Path(payload_runtime).expanduser().resolve() != runtime:
        raise ValueError(f"ARTIFACT_LEDGER_PAYLOAD_RUNTIME_ROOT_MISMATCH:{payload_runtime}")
    payload_mode = str(payload.get("runtime_mode") or mode).strip().upper()
    if payload_mode != mode:
        raise ValueError(f"ARTIFACT_LEDGER_RUNTIME_MODE_MISMATCH:payload={payload_mode}:expected={mode}")
    producer_module = _producer_module(payload)
    producer_git_commit = _producer_git_commit(payload)
    if not producer_module or not producer_git_commit:
        raise ValueError("ARTIFACT_LEDGER_PRODUCER_METADATA_MISSING")
    digest = artifact_hash_v1(path)
    refs = _input_refs(payload)
    record_artifact_id = artifact_id or f"{artifact_type}:{day}:{digest}"
    record = {
        "schema_id": "aegis_artifact_ledger_record",
        "schema_version": LEDGER_SCHEMA_VERSION,
        "ledger_record_id": canonical_hash_v1({"artifact_id": record_artifact_id, "artifact_hash": digest, "artifact_path": str(path)}),
        "artifact_id": record_artifact_id,
        "artifact_type": artifact_type,
        "artifact_path": str(path),
        "artifact_hash": digest,
        "day": day,
        "account": account or str(payload.get("account") or payload.get("ib_account") or ""),
        "sleeve": sleeve or str(payload.get("sleeve") or payload.get("sleeve_id") or ""),
        "runtime_mode": mode,
        "truth_root": str(truth),
        "runtime_root": str(runtime),
        "schema_id_ref": str(payload.get("schema_id") or ""),
        "schema_version_ref": str(payload.get("schema_version") or ""),
        "producer_module": producer_module,
        "producer_git_commit": producer_git_commit,
        "producer_contract_ref": _producer_contract(payload),
        "input_artifact_refs": refs,
        "input_artifact_hashes": _input_hashes(refs),
        "output_artifact_hash": digest,
        "generated_at": str(payload.get("generated_at_utc") or payload.get("generated_at") or payload.get("generated_utc") or now_iso_v1()),
        "validation_status": _status(payload),
        "blockers": _blockers(payload),
        "recovery_command": recovery_command,
        "append_only": True,
    }
    _validate_schema(record, LEDGER_SCHEMA)
    return record


def _ledger_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def append_artifact_ledger_record_v1(*, record: dict[str, Any], ledger_path: Path) -> Path:
    path = Path(ledger_path).expanduser().resolve()
    _validate_schema(record, LEDGER_SCHEMA)
    for existing in _ledger_records(path):
        if str(existing.get("artifact_id") or "") != str(record.get("artifact_id") or ""):
            continue
        if str(existing.get("artifact_hash") or "") != str(record.get("artifact_hash") or ""):
            raise ValueError(f"ARTIFACT_LEDGER_DUPLICATE_ARTIFACT_ID_DIFFERENT_HASH:{record['artifact_id']}")
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n")
    return path


def write_artifact_ledger_record_v1(
    *,
    artifact_path: Path,
    artifact_type: str,
    truth_root: Path,
    runtime_root: Path,
    runtime_mode: str,
    day: str,
    recovery_command: str = "",
    account: str = "",
    sleeve: str = "",
    artifact_id: str = "",
) -> tuple[Path, dict[str, Any]]:
    record = build_artifact_ledger_record_v1(
        artifact_path=artifact_path,
        artifact_type=artifact_type,
        truth_root=truth_root,
        runtime_root=runtime_root,
        runtime_mode=runtime_mode,
        day=day,
        recovery_command=recovery_command,
        account=account,
        sleeve=sleeve,
        artifact_id=artifact_id,
    )
    path = append_artifact_ledger_record_v1(record=record, ledger_path=ledger_path_v1(truth_root=truth_root, day=day))
    return path, record


def find_artifact_ledger_record_v1(*, truth_root: Path, day: str, artifact_path: Path | None = None, artifact_id: str = "") -> dict[str, Any]:
    target_path = str(Path(artifact_path).expanduser().resolve()) if artifact_path is not None else ""
    target_id = str(artifact_id or "").strip()
    matches = []
    for row in _ledger_records(ledger_path_v1(truth_root=truth_root, day=day)):
        if target_id and str(row.get("artifact_id") or "") == target_id:
            matches.append(row)
        elif target_path and str(row.get("artifact_path") or "") == target_path:
            matches.append(row)
    return matches[-1] if matches else {}


def verify_artifact_ledger_record_v1(
    *,
    artifact_path: Path,
    artifact_type: str,
    truth_root: Path,
    runtime_root: Path,
    runtime_mode: str,
    day: str,
) -> list[dict[str, str]]:
    path = Path(artifact_path).expanduser().resolve()
    issues: list[dict[str, str]] = []
    record = find_artifact_ledger_record_v1(truth_root=truth_root, day=day, artifact_path=path)
    if not record:
        return [{"code": "ARTIFACT_LEDGER_RECORD_MISSING", "path": str(path)}]
    actual_hash = artifact_hash_v1(path) if path.exists() and path.is_file() else ""
    expected = {
        "artifact_type": artifact_type,
        "artifact_path": str(path),
        "artifact_hash": actual_hash,
        "day": day,
        "truth_root": str(Path(truth_root).expanduser().resolve()),
        "runtime_root": str(Path(runtime_root).expanduser().resolve()),
        "runtime_mode": str(runtime_mode).strip().upper(),
    }
    for key, value in expected.items():
        if str(record.get(key) or "") != str(value):
            issues.append({"code": f"ARTIFACT_LEDGER_{key.upper()}_MISMATCH", "path": str(path), "ledger_value": str(record.get(key) or ""), "expected": str(value)})
    if not str(record.get("producer_module") or "").strip() or not str(record.get("producer_git_commit") or "").strip():
        issues.append({"code": "ARTIFACT_LEDGER_PRODUCER_METADATA_MISSING", "path": str(path)})
    return issues


def build_promotion_attestation_v1(
    *,
    artifact_id: str,
    artifact_type: str,
    source_candidate_artifact_path: Path,
    source_ledger_record: dict[str, Any],
    destination_production_path: Path,
    validation_status: str,
    blockers: list[str] | None = None,
) -> dict[str, Any]:
    source = Path(source_candidate_artifact_path).expanduser().resolve()
    destination = Path(destination_production_path).expanduser().resolve()
    source_hash = artifact_hash_v1(source)
    destination_hash = artifact_hash_v1(destination)
    payload = {
        "schema_id": "aegis_promotion_attestation",
        "schema_version": PROMOTION_SCHEMA_VERSION,
        "attestation_id": canonical_hash_v1({"artifact_id": artifact_id, "source": str(source), "destination": str(destination), "destination_hash": destination_hash}),
        "artifact_id": artifact_id,
        "artifact_type": artifact_type,
        "source_candidate_artifact_path": str(source),
        "source_candidate_artifact_hash": source_hash,
        "source_ledger_record_ref": {
            "ledger_record_id": str(source_ledger_record.get("ledger_record_id") or ""),
            "artifact_id": str(source_ledger_record.get("artifact_id") or ""),
            "artifact_hash": str(source_ledger_record.get("artifact_hash") or ""),
        },
        "destination_production_path": str(destination),
        "destination_production_hash": destination_hash,
        "promotion_policy_version": PROMOTION_POLICY_VERSION,
        "producer_module": "ops/tools/run_candidate_to_production_promotion_v1.py",
        "producer_git_commit": git_commit_v1(),
        "promoted_at": now_iso_v1(),
        "validation_status": validation_status,
        "blockers": blockers or [],
    }
    _validate_schema(payload, PROMOTION_SCHEMA)
    return payload


def write_promotion_attestation_v1(*, truth_root: Path, day: str, attestation: dict[str, Any]) -> Path:
    path = promotion_attestation_path_v1(truth_root=truth_root, day=day, artifact_id=str(attestation.get("artifact_id") or "unknown"))
    write_json_v1(path, attestation)
    return path


def verify_promotion_attestation_v1(
    *,
    truth_root: Path,
    day: str,
    artifact_path: Path,
    artifact_type: str,
    artifact_id: str = "",
) -> list[dict[str, str]]:
    path = Path(artifact_path).expanduser().resolve()
    record = find_artifact_ledger_record_v1(truth_root=truth_root, day=day, artifact_path=path)
    resolved_artifact_id = artifact_id or str(record.get("artifact_id") or "")
    if not resolved_artifact_id:
        return [{"code": "PROMOTION_ATTESTATION_ARTIFACT_ID_MISSING", "path": str(path)}]
    attestation_path = promotion_attestation_path_v1(truth_root=truth_root, day=day, artifact_id=resolved_artifact_id)
    if not attestation_path.exists():
        return [{"code": "PROMOTION_ATTESTATION_MISSING", "path": str(path), "attestation_path": str(attestation_path)}]
    try:
        attestation = read_json_v1(attestation_path)
        _validate_schema(attestation, PROMOTION_SCHEMA)
    except Exception as exc:
        return [{"code": "PROMOTION_ATTESTATION_INVALID", "path": str(path), "error": type(exc).__name__}]
    issues: list[dict[str, str]] = []
    if str(attestation.get("validation_status") or "").upper() != "PASS":
        issues.append({"code": "PROMOTION_ATTESTATION_NOT_PASS", "path": str(path)})
    expected_hash = artifact_hash_v1(path) if path.exists() and path.is_file() else ""
    expected = {
        "artifact_type": artifact_type,
        "destination_production_path": str(path),
        "destination_production_hash": expected_hash,
        "producer_git_commit": git_commit_v1(),
    }
    for key, value in expected.items():
        if str(attestation.get(key) or "") != str(value):
            issues.append({"code": f"PROMOTION_ATTESTATION_{key.upper()}_MISMATCH", "path": str(path), "attestation_value": str(attestation.get(key) or ""), "expected": str(value)})
    if record and str(attestation.get("artifact_id") or "") != str(record.get("artifact_id") or ""):
        issues.append({"code": "PROMOTION_ATTESTATION_LEDGER_ARTIFACT_ID_MISMATCH", "path": str(path)})
    return issues
