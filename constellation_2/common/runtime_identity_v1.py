from __future__ import annotations

import hashlib
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict

from constellation_2.common.execution_identity_binding_v1 import (
    EXECUTION_IDENTITY_BINDING_OWNER,
    resolve_governed_execution_identity_v1,
)
from constellation_2.common.paper_execution_authority_v1 import (
    resolve_governed_paper_execution_roots,
)
from constellation_2.common.runtime_contract_v1 import (
    ACTIVE_RUNTIME_CONTRACT_PATH,
    load_active_runtime_contract_or_fail,
    resolve_release_provenance,
)
from constellation_2.common.runtime_path_authority_v1 import (
    require_authoritative_repo_runtime_v1,
    resolve_runtime_path_authority_snapshot_v1,
)
from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    validate_against_repo_schema_v1,
)


RUNTIME_STARTUP_IDENTITY_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/runtime_startup_identity.v1.schema.json"
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_now_compact() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def _sha256_hex(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _require_text(value: Any, *, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise SystemExit(f"FAIL: runtime_identity_{label}_missing")
    return text


def _sanitize_token(value: str) -> str:
    raw = _require_text(value, label="token").lower()
    sanitized = "".join(ch if ch.isalnum() else "_" for ch in raw).strip("_")
    sanitized = "_".join(part for part in sanitized.split("_") if part)
    return sanitized or "runtime_startup"


def _resolve_python_executable(requested_python: str, resolved_python_executable: str = "") -> str:
    explicit = str(resolved_python_executable or "").strip()
    if explicit:
        return str(Path(explicit).expanduser().resolve())
    requested = _require_text(requested_python, label="requested_python")
    located = shutil.which(requested)
    if not located:
        raise SystemExit(f"FAIL: runtime_identity_python_unresolved requested={requested!r}")
    return str(Path(located).resolve())


def _load_active_runtime_identity_snapshot_legacy_v1(*, repo_root: Path | None = None) -> Dict[str, Any]:
    authoritative_repo_root = require_authoritative_repo_runtime_v1(repo_root)
    contract = load_active_runtime_contract_or_fail()
    contract_path = ACTIVE_RUNTIME_CONTRACT_PATH.resolve()
    contract_repo_root = Path(
        _require_text(contract.get("authoritative_repo_root"), label="authoritative_repo_root")
    ).resolve()
    if contract_repo_root != authoritative_repo_root:
        raise SystemExit(
            "FAIL: runtime_identity_authoritative_repo_root_mismatch:"
            f" contract={contract_repo_root} runtime={authoritative_repo_root}"
        )

    runtime_environment = _require_text(
        contract.get("runtime_environment"),
        label="runtime_environment",
    ).upper()
    primary_execution_ref = contract.get("primary_execution_identity_ref")
    if not isinstance(primary_execution_ref, dict):
        raise SystemExit("FAIL: runtime_identity_primary_execution_identity_ref_invalid")

    authority_owner = _require_text(
        primary_execution_ref.get("authority_owner"),
        label="primary_execution_identity_ref.authority_owner",
    )
    if authority_owner != EXECUTION_IDENTITY_BINDING_OWNER:
        raise SystemExit(
            "FAIL: runtime_identity_primary_execution_identity_ref_owner_invalid:"
            f" expected={EXECUTION_IDENTITY_BINDING_OWNER} actual={authority_owner}"
        )
    primary_sleeve_id = _require_text(
        primary_execution_ref.get("sleeve_id"),
        label="primary_execution_identity_ref.sleeve_id",
    ).upper()

    execution_identity = resolve_governed_execution_identity_v1(
        repo_root=authoritative_repo_root,
        environment=runtime_environment,
        sleeve_id=primary_sleeve_id,
    )
    execution_roots = resolve_governed_paper_execution_roots(
        repo_root=authoritative_repo_root,
        environment=execution_identity.environment,
        ib_account=execution_identity.account_id,
        sleeve_id=execution_identity.sleeve_id,
    )
    release_provenance = resolve_release_provenance()

    return {
        "contract_path": str(contract_path),
        "contract_sha256": _sha256_hex(contract_path),
        "schema_id": str(contract.get("schema_id") or "").strip(),
        "schema_version": str(contract.get("schema_version") or "").strip(),
        "status": str(contract.get("status") or "").strip(),
        "generated_at_utc": str(contract.get("generated_at_utc") or "").strip(),
        "authoritative_repo_root": str(contract_repo_root),
        "release_id": str(contract.get("release_id") or "").strip(),
        "git_sha": str(contract.get("git_sha") or "").strip(),
        "release_root": str(Path(_require_text(contract.get("release_root"), label="release_root")).resolve()),
        "runtime_data_root": str(
            Path(_require_text(contract.get("runtime_data_root"), label="runtime_data_root")).resolve()
        ),
        "canonical_truth_root": str(
            Path(_require_text(contract.get("canonical_truth_root"), label="canonical_truth_root")).resolve()
        ),
        "truth_sleeves_root": str(
            Path(_require_text(contract.get("truth_sleeves_root"), label="truth_sleeves_root")).resolve()
        ),
        "pointer_index_family": str(contract.get("pointer_index_family") or "").strip(),
        "allowed_truth_roots": [str(Path(str(item)).resolve()) for item in contract.get("allowed_truth_roots") or []],
        "provenance_mode": str(contract.get("provenance_mode") or "").strip(),
        "runtime_environment": runtime_environment,
        "primary_execution_identity_ref": {
            "authority_owner": authority_owner,
            "sleeve_id": primary_sleeve_id,
        },
        "resolved_execution_identity": {
            "authority_owner": execution_identity.authority_owner,
            "environment": execution_identity.environment,
            "sleeve_id": execution_identity.sleeve_id,
            "account_id": execution_identity.account_id,
            "client_id_orders": execution_identity.client_id_orders,
            "client_id_observer": execution_identity.client_id_observer,
            "host": execution_identity.host,
            "port": execution_identity.port,
            "sleeve_registry_path": str(execution_identity.sleeve_registry_path.resolve()),
            "account_registry_path": str(execution_identity.account_registry_path.resolve()),
        },
        "resolved_execution_roots": {
            "authority_owner": execution_roots.authority_owner,
            "environment": execution_roots.environment,
            "ib_account": execution_roots.ib_account,
            "sleeve_id": execution_roots.sleeve_id,
            "mode": execution_roots.mode,
            "truth_partition": execution_roots.truth_partition,
            "execution_root_path": str(execution_roots.execution_root_path.resolve()),
            "truth_sleeves_root": str(execution_roots.truth_sleeves_root.resolve()),
            "sleeve_registry_path": str(execution_roots.sleeve_registry_path.resolve()),
        },
        "path_authority_snapshot": resolve_runtime_path_authority_snapshot_v1(
            repo_root=authoritative_repo_root
        ),
        "release_provenance": release_provenance,
    }


def load_active_runtime_identity_snapshot_v1(*, repo_root: Path | None = None) -> Dict[str, Any]:
    from constellation_2.common.runtime_identity_bridge_v1 import (
        load_active_runtime_identity_snapshot_bridge_v1,
    )

    return load_active_runtime_identity_snapshot_bridge_v1(
        repo_root=repo_root,
        caller="runtime_identity_v1.load_active_runtime_identity_snapshot_v1",
    )


def derive_runtime_startup_identity_payload(
    *,
    repo_root: Path,
    entrypoint_name: str,
    entrypoint_path: Path,
    service_name: str,
    requested_python: str,
    resolved_python_executable: str = "",
) -> Dict[str, Any]:
    runtime_identity = load_active_runtime_identity_snapshot_v1(repo_root=repo_root)
    generated_at_utc = _utc_now_iso()
    entry_name = _require_text(entrypoint_name, label="entrypoint_name")
    startup_id = (
        f"{_utc_now_compact()}__{_sanitize_token(entry_name)}__pid{int(os.getpid())}"
    )
    resolved_python = _resolve_python_executable(
        requested_python=requested_python,
        resolved_python_executable=resolved_python_executable,
    )

    return {
        "schema_id": "runtime_startup_identity.v1",
        "schema_version": "v1",
        "startup_id": startup_id,
        "status": "CAPTURED",
        "generated_at_utc": generated_at_utc,
        "entrypoint_name": entry_name,
        "entrypoint_path": str(Path(entrypoint_path).resolve()),
        "service_name": str(service_name or "").strip(),
        "requested_python": _require_text(requested_python, label="requested_python"),
        "resolved_python_executable": resolved_python,
        "cwd": str(Path.cwd().resolve()),
        "pid": int(os.getpid()),
        "runtime_identity": runtime_identity,
    }


def resolve_runtime_startup_identity_receipt_path(
    *,
    runtime_data_root: Path,
    startup_id: str,
) -> Path:
    return (
        Path(runtime_data_root).resolve()
        / "runtime_startup_identity_v1"
        / _require_text(startup_id, label="startup_id")
        / "runtime_startup_identity.v1.json"
    ).resolve()


def write_runtime_startup_identity_receipt_v1(
    *,
    repo_root: Path,
    payload: Dict[str, Any],
) -> Path:
    runtime_identity = payload.get("runtime_identity")
    if not isinstance(runtime_identity, dict):
        raise SystemExit("FAIL: runtime_startup_identity_runtime_identity_invalid")
    runtime_data_root = Path(
        _require_text(runtime_identity.get("runtime_data_root"), label="runtime_identity.runtime_data_root")
    ).resolve()
    path = resolve_runtime_startup_identity_receipt_path(
        runtime_data_root=runtime_data_root,
        startup_id=_require_text(payload.get("startup_id"), label="startup_id"),
    )
    validate_against_repo_schema_v1(payload, Path(repo_root).resolve(), RUNTIME_STARTUP_IDENTITY_SCHEMA_RELPATH)
    path.parent.mkdir(parents=True, exist_ok=False)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path
