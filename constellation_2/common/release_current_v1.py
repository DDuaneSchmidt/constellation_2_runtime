from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping

from constellation_2.common.control_plane_read_gateway_v1 import ControlPlaneReadRefV1, read_control_plane_surface_v1
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    now_utc_iso_v1,
    repo_git_sha_v1,
    resolve_fact_plane_truth_root_v1,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/RUNTIME/release_current.v1.schema.json"
WRITER_ID = "truth_kernel_release_current_reducer_v1"
CURRENT_LOCK_NAME = ".release_current_v1.current.lock"


def _stable_hash(seed: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(dict(seed), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _require_text(value: Any, code: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(code)
    return text


def _require_hex64(value: Any, code: str) -> str:
    text = _require_text(value, code)
    if len(text) != 64 or any(ch not in "0123456789abcdef" for ch in text.lower()):
        raise ValueError(code)
    return text.lower()


def _surface_input_ref(ref: ControlPlaneReadRefV1) -> dict[str, str]:
    schema_relpath = _require_text(ref.schema_relpath, f"RELEASE_CURRENT_SCHEMA_RELPATH_MISSING:{ref.surface}")
    return {
        "domain": _require_text(ref.domain, f"RELEASE_CURRENT_DOMAIN_MISSING:{ref.surface}"),
        "surface": _require_text(ref.surface, "RELEASE_CURRENT_SURFACE_MISSING"),
        "path": str(Path(ref.path).resolve()),
        "sha256": _require_hex64(ref.sha256, f"RELEASE_CURRENT_SHA256_INVALID:{ref.surface}"),
        "schema_relpath": schema_relpath,
    }


def _artifact_ref(value: Any, code: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValueError(code)
    return {
        "artifact_id": _require_text(value.get("artifact_id"), f"{code}:artifact_id"),
        "path": _require_text(value.get("path"), f"{code}:path"),
        "sha256": _require_hex64(value.get("sha256"), f"{code}:sha256"),
        "artifact_class": _require_text(value.get("artifact_class"), f"{code}:artifact_class"),
        "finality_state": _require_text(value.get("finality_state"), f"{code}:finality_state"),
    }


def _artifact_instance_id_from_path(*, path: str, family: str, code: str) -> str:
    parts = Path(path).resolve().parts
    try:
        family_index = parts.index(family)
    except ValueError as exc:
        raise ValueError(code) from exc
    if family_index + 1 >= len(parts):
        raise ValueError(code)
    return _require_hex64(parts[family_index + 1], code)


def _require_allowed_truth_roots(
    runtime_payload: Mapping[str, Any],
    *,
    canonical_truth_root: str,
    truth_sleeves_root: str,
) -> list[str]:
    raw = runtime_payload.get("allowed_truth_roots")
    if not isinstance(raw, list) or len(raw) < 2:
        raise ValueError("RELEASE_CURRENT_ALLOWED_TRUTH_ROOTS_INVALID")
    roots = [_require_text(item, "RELEASE_CURRENT_ALLOWED_TRUTH_ROOTS_INVALID") for item in raw]
    resolved_roots = {str(Path(item).resolve()) for item in roots}
    required_roots = {
        str(Path(canonical_truth_root).resolve()),
        str(Path(truth_sleeves_root).resolve()),
    }
    if not required_roots.issubset(resolved_roots):
        raise ValueError("RELEASE_CURRENT_ALLOWED_TRUTH_ROOTS_MISMATCH")
    return roots


def _primary_execution_identity_ref(runtime_payload: Mapping[str, Any]) -> dict[str, str]:
    value = runtime_payload.get("primary_execution_identity_ref")
    if not isinstance(value, Mapping):
        raise ValueError("RELEASE_CURRENT_PRIMARY_EXECUTION_IDENTITY_REF_INVALID")
    authority_owner = _require_text(
        value.get("authority_owner"),
        "RELEASE_CURRENT_PRIMARY_EXECUTION_IDENTITY_REF_INVALID:authority_owner",
    )
    if authority_owner != "execution_identity_binding_v1":
        raise ValueError("RELEASE_CURRENT_PRIMARY_EXECUTION_IDENTITY_REF_OWNER_MISMATCH")
    sleeve_id = _require_text(
        value.get("sleeve_id"),
        "RELEASE_CURRENT_PRIMARY_EXECUTION_IDENTITY_REF_INVALID:sleeve_id",
    )
    return {
        "authority_owner": authority_owner,
        "sleeve_id": sleeve_id,
    }


def _published_release_current_path(*, truth_root: Path) -> Path:
    return (Path(truth_root).resolve() / "release_current_v1" / "current.json").resolve()


def _published_state_lock_path(*, current_path: Path) -> Path:
    return (current_path.parent / CURRENT_LOCK_NAME).resolve()


@contextmanager
def _hold_release_current_publication_lock(*, current_path: Path):
    lock_path = _published_state_lock_path(current_path=current_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_fd: int | None = None
    lock_acquired = False
    try:
        try:
            lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise ValueError(f"RELEASE_CURRENT_LOCK_BUSY:path={lock_path}") from exc
        lock_acquired = True
        os.write(
            lock_fd,
            (
                f"writer_id={WRITER_ID}\n"
                f"pid={os.getpid()}\n"
                f"target={current_path.resolve()}\n"
            ).encode("utf-8"),
        )
        os.fsync(lock_fd)
        yield lock_path
    finally:
        if lock_fd is not None:
            try:
                os.close(lock_fd)
            except Exception:
                pass
        if lock_acquired and lock_path.exists():
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass


def build_release_current_v1(
    *,
    release_manifest_ref: ControlPlaneReadRefV1,
    active_runtime_contract_ref: ControlPlaneReadRefV1,
    configuration_state_ref: ControlPlaneReadRefV1,
    generated_at_utc: str,
    producer_module: str,
) -> dict[str, Any]:
    manifest_payload = dict(release_manifest_ref.payload)
    runtime_payload = dict(active_runtime_contract_ref.payload)
    state_payload = dict(configuration_state_ref.payload)

    manifest_release_id = _require_text(
        manifest_payload.get("release_id"),
        "RELEASE_CURRENT_RELEASE_MANIFEST_RELEASE_ID_MISSING",
    )
    runtime_release_id = _require_text(
        runtime_payload.get("release_id"),
        "RELEASE_CURRENT_RUNTIME_CONTRACT_RELEASE_ID_MISSING",
    )
    if manifest_release_id != runtime_release_id:
        raise ValueError("RELEASE_CURRENT_RELEASE_ID_MISMATCH")

    manifest_git_sha = _require_text(
        manifest_payload.get("git_sha"),
        "RELEASE_CURRENT_RELEASE_MANIFEST_GIT_SHA_MISSING",
    ).lower()
    runtime_git_sha = _require_text(
        runtime_payload.get("git_sha"),
        "RELEASE_CURRENT_RUNTIME_CONTRACT_GIT_SHA_MISSING",
    ).lower()
    if manifest_git_sha != runtime_git_sha:
        raise ValueError("RELEASE_CURRENT_GIT_SHA_MISMATCH")

    manifest_release_root = _require_text(
        manifest_payload.get("release_root"),
        "RELEASE_CURRENT_RELEASE_MANIFEST_RELEASE_ROOT_MISSING",
    )
    runtime_release_root = _require_text(
        runtime_payload.get("release_root"),
        "RELEASE_CURRENT_RUNTIME_CONTRACT_RELEASE_ROOT_MISSING",
    )
    if str(Path(manifest_release_root).resolve()) != str(Path(runtime_release_root).resolve()):
        raise ValueError("RELEASE_CURRENT_RELEASE_ROOT_MISMATCH")
    if _require_text(runtime_payload.get("status"), "RELEASE_CURRENT_RUNTIME_CONTRACT_STATUS_MISSING").upper() != "ACTIVE":
        raise ValueError("RELEASE_CURRENT_RUNTIME_CONTRACT_NOT_ACTIVE")

    if _require_text(state_payload.get("status"), "RELEASE_CURRENT_CONFIGURATION_STATE_STATUS_MISSING").upper() != "ACTIVE":
        raise ValueError("RELEASE_CURRENT_CONFIGURATION_STATE_NOT_ACTIVE")

    configuration_state_id = _require_hex64(
        state_payload.get("configuration_state_id"),
        "RELEASE_CURRENT_CONFIGURATION_STATE_ID_INVALID",
    )
    canonical_truth_root = _require_text(
        runtime_payload.get("canonical_truth_root"),
        "RELEASE_CURRENT_CANONICAL_TRUTH_ROOT_MISSING",
    )
    truth_sleeves_root = _require_text(
        runtime_payload.get("truth_sleeves_root"),
        "RELEASE_CURRENT_TRUTH_SLEEVES_ROOT_MISSING",
    )
    allowed_truth_roots = _require_allowed_truth_roots(
        runtime_payload,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    primary_execution_identity_ref = _primary_execution_identity_ref(runtime_payload)
    compiled_ref = _artifact_ref(
        state_payload.get("compiled_active_config_ref"),
        "RELEASE_CURRENT_COMPILED_ACTIVE_CONFIG_REF_INVALID",
    )
    activation_ref = _artifact_ref(
        state_payload.get("configuration_activation_transaction_ref"),
        "RELEASE_CURRENT_CONFIGURATION_ACTIVATION_TRANSACTION_REF_INVALID",
    )
    compiled_config_id = _artifact_instance_id_from_path(
        path=compiled_ref["path"],
        family="compiled_active_config_v1",
        code="RELEASE_CURRENT_COMPILED_CONFIG_ID_INVALID",
    )
    activation_transaction_id = _artifact_instance_id_from_path(
        path=activation_ref["path"],
        family="configuration_activation_transaction_v1",
        code="RELEASE_CURRENT_ACTIVATION_TRANSACTION_ID_INVALID",
    )

    release_manifest_input_ref = _surface_input_ref(release_manifest_ref)
    runtime_input_ref = _surface_input_ref(active_runtime_contract_ref)
    configuration_state_input_ref = _surface_input_ref(configuration_state_ref)

    release_current_id = _stable_hash(
        {
            "artifact_id": "release_current_v1",
            "release_id": manifest_release_id,
            "git_sha": manifest_git_sha,
            "release_root": str(Path(runtime_release_root).resolve()),
            "configuration_state_id": configuration_state_id,
            "compiled_config_id": compiled_config_id,
            "activation_transaction_id": activation_transaction_id,
        }
    )

    return {
        "schema_id": "release_current.v1",
        "schema_version": "v1",
        "release_current_id": release_current_id,
        "reducer_owner": WRITER_ID,
        "activation_state": "SHADOW_ONLY",
        "status": "ACTIVE_SHADOW",
        "generated_at_utc": _require_text(generated_at_utc, "RELEASE_CURRENT_GENERATED_AT_UTC_MISSING"),
        "release_id": manifest_release_id,
        "git_sha": manifest_git_sha,
        "authoritative_repo_root": _require_text(
            runtime_payload.get("authoritative_repo_root"),
            "RELEASE_CURRENT_AUTHORITATIVE_REPO_ROOT_MISSING",
        ),
        "release_root": str(Path(runtime_release_root).resolve()),
        "runtime_data_root": _require_text(runtime_payload.get("runtime_data_root"), "RELEASE_CURRENT_RUNTIME_DATA_ROOT_MISSING"),
        "canonical_truth_root": canonical_truth_root,
        "truth_sleeves_root": truth_sleeves_root,
        "pointer_index_family": _require_text(
            runtime_payload.get("pointer_index_family"),
            "RELEASE_CURRENT_POINTER_INDEX_FAMILY_MISSING",
        ),
        "allowed_truth_roots": allowed_truth_roots,
        "runtime_environment": _require_text(
            runtime_payload.get("runtime_environment"),
            "RELEASE_CURRENT_RUNTIME_ENVIRONMENT_MISSING",
        ),
        "primary_execution_identity_ref": primary_execution_identity_ref,
        "configuration_state_id": configuration_state_id,
        "compiled_config_id": compiled_config_id,
        "activation_transaction_id": activation_transaction_id,
        "release_manifest_ref": release_manifest_input_ref,
        "active_runtime_contract_ref": runtime_input_ref,
        "configuration_state_ref": configuration_state_input_ref,
        "compiled_active_config_ref": compiled_ref,
        "configuration_activation_transaction_ref": activation_ref,
        "lineage": {
            "producer_module": _require_text(producer_module, "RELEASE_CURRENT_PRODUCER_MODULE_MISSING"),
            "code_version": repo_git_sha_v1(),
            "replay_input_refs": [
                release_manifest_input_ref,
                runtime_input_ref,
                configuration_state_input_ref,
            ],
            "shadow_only_reason_code": "CUTOVER_BOUNDARY_NOT_ESTABLISHED",
        },
    }


def write_release_current_v1(*, truth_root: str | Path, payload: dict[str, Any]) -> SurfaceRefV1:
    current_path = _published_release_current_path(truth_root=resolve_fact_plane_truth_root_v1(truth_root))
    with _hold_release_current_publication_lock(current_path=current_path):
        return atomic_write_idempotent_validated_json_v1(
            path=current_path,
            payload=payload,
            schema_relpath=SCHEMA_RELPATH_V1,
            volatile_field_names=("generated_at_utc",),
        )


def reduce_release_current_v1(
    *,
    truth_root: str | Path | None = None,
    generated_at_utc: str | None = None,
    producer_module: str = "ops/tools/run_release_current_reducer_v1.py",
) -> SurfaceRefV1:
    active_runtime_contract_ref = read_control_plane_surface_v1(
        domain="release",
        surface="active_runtime_contract",
    )
    canonical_truth_root = resolve_fact_plane_truth_root_v1(
        truth_root
        if truth_root is not None and str(truth_root).strip()
        else str(active_runtime_contract_ref.payload.get("canonical_truth_root") or "")
    )
    contract_truth_root = resolve_fact_plane_truth_root_v1(
        str(active_runtime_contract_ref.payload.get("canonical_truth_root") or "")
    )
    if canonical_truth_root != contract_truth_root:
        raise ValueError("RELEASE_CURRENT_CANONICAL_TRUTH_ROOT_MISMATCH")

    release_manifest_ref = read_control_plane_surface_v1(
        domain="release",
        surface="release_manifest_active",
    )
    configuration_state_ref = read_control_plane_surface_v1(
        domain="policy",
        surface="configuration_state_current",
        truth_root=canonical_truth_root,
    )

    payload = build_release_current_v1(
        release_manifest_ref=release_manifest_ref,
        active_runtime_contract_ref=active_runtime_contract_ref,
        configuration_state_ref=configuration_state_ref,
        generated_at_utc=generated_at_utc or now_utc_iso_v1(),
        producer_module=producer_module,
    )
    return write_release_current_v1(truth_root=canonical_truth_root, payload=payload)
