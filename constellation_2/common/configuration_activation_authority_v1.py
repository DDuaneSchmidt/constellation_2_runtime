from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

from constellation_2.common.constitutional_runtime_v1 import (
    FINALITY_FINALIZED,
    FINALITY_PROVISIONAL,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_frozen_decision_input_bundle_v1,
    build_governed_artifact_lineage_v1,
    build_governed_dependency_ref_v1,
    resolve_constitutional_artifact_path_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.configuration_activation_family_validator_v1 import (
    ConfigurationActivationFamilyRefsV1,
    load_validated_configuration_activation_family_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    atomic_write_validated_json_v1,
    read_validated_surface_v1,
    repo_git_sha_v1,
    resolve_fact_plane_truth_root_v1,
    sha256_file_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
WRITER_ID = "configuration_activation_authority_v1"

POLICY_SNAPSHOT_ARTIFACT_ID = "configuration_policy_snapshot_v1"
VALIDATION_RESULT_ARTIFACT_ID = "configuration_validation_result_v1"
COMPILED_ACTIVE_CONFIG_ARTIFACT_ID = "compiled_active_config_v1"
COMPILE_RESULT_ARTIFACT_ID = "configuration_compile_result_v1"
REVIEW_DIFF_ARTIFACT_ID = "configuration_review_diff_v1"
ACTIVATION_TRANSACTION_ARTIFACT_ID = "configuration_activation_transaction_v1"
CONFIGURATION_STATE_ARTIFACT_ID = "configuration_state_v1"

POLICY_SNAPSHOT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_policy_snapshot.v1.schema.json"
VALIDATION_RESULT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_validation_result.v1.schema.json"
COMPILED_ACTIVE_CONFIG_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/compiled_active_config.v1.schema.json"
COMPILE_RESULT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_compile_result.v1.schema.json"
REVIEW_DIFF_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_review_diff.v1.schema.json"
ACTIVATION_TRANSACTION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_activation_transaction.v1.schema.json"
CONFIGURATION_STATE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/configuration_state.v1.schema.json"
CONFIGURATION_STATE_CURRENT_LOCK_NAME = ".configuration_state_v1.current.lock"

REQUIRED_GOVERNANCE_UTILITY_RELPATHS = (
    "governance/00_MANIFEST.yaml",
    "governance/02_REGISTRIES/C2_ARTIFACT_AUTHORITY_REGISTRY_V1.json",
    "governance/05_CONTRACTS/C2/constitutional_runtime_architecture_v1.contract.md",
    "governance/05_CONTRACTS/C2/constitutional_artifact_taxonomy_v1.contract.md",
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/constitutional_artifact_authority_registry.v1.schema.json",
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/artifact_dependency_declaration.v1.schema.json",
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/governed_artifact_lineage.v1.schema.json",
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/frozen_decision_input_bundle.v1.schema.json",
)
FAMILY_ARTIFACT_IDS = (
    VALIDATION_RESULT_ARTIFACT_ID,
    COMPILED_ACTIVE_CONFIG_ARTIFACT_ID,
    COMPILE_RESULT_ARTIFACT_ID,
    REVIEW_DIFF_ARTIFACT_ID,
    ACTIVATION_TRANSACTION_ARTIFACT_ID,
    CONFIGURATION_STATE_ARTIFACT_ID,
)


class ConfigurationActivationError(RuntimeError):
    pass


@dataclass(frozen=True)
class ConfigurationActivationRunResultV1:
    policy_snapshot_ref: SurfaceRefV1
    validation_result_ref: SurfaceRefV1
    compiled_active_config_ref: SurfaceRefV1 | None
    compile_result_ref: SurfaceRefV1 | None
    review_diff_ref: SurfaceRefV1 | None
    activation_transaction_ref: SurfaceRefV1 | None
    configuration_state_ref: SurfaceRefV1 | None


def _surface_ref(path: Path, schema_relpath: str) -> SurfaceRefV1:
    return read_validated_surface_v1(path=path, schema_relpath=schema_relpath)


def _governed_ref(artifact_id: str, ref: SurfaceRefV1, *, finality_state: str = FINALITY_FINALIZED) -> Dict[str, Any]:
    return build_governed_dependency_ref_v1(
        repo_root=REPO_ROOT,
        artifact_id=artifact_id,
        path=ref.path,
        sha256=ref.sha256,
        finality_state=finality_state,
    )


def _ordered_policy_snapshot_refs(rows: Sequence[Mapping[str, Any]]) -> list[Dict[str, Any]]:
    normalized: list[Dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "logical_name": str(row.get("logical_name") or "").strip(),
                "path": str(Path(str(row.get("path") or "")).resolve()),
                "sha256": str(row.get("sha256") or "").strip(),
            }
        )
    return sorted(normalized, key=lambda item: (item["logical_name"], item["path"], item["sha256"]))


def _ordered_source_document_refs(rows: Sequence[Mapping[str, Any]]) -> list[Dict[str, Any]]:
    normalized: list[Dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                "logical_name": str(row.get("logical_name") or "").strip(),
                "path": str(Path(str(row.get("path") or "")).resolve()),
                "sha256": str(row.get("sha256") or "").strip(),
                "document_class": str(row.get("document_class") or "").strip(),
            }
        )
    return sorted(
        normalized,
        key=lambda item: (item["logical_name"], item["document_class"], item["path"], item["sha256"]),
    )


def _required_governance_utility_refs() -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for relpath in REQUIRED_GOVERNANCE_UTILITY_RELPATHS:
        path = (REPO_ROOT / relpath).resolve()
        if not path.exists() or not path.is_file():
            raise ConfigurationActivationError(f"MISSING_GOVERNANCE_UTILITY:path={path}")
        rows.append(
            {
                "logical_name": relpath,
                "path": str(path),
                "sha256": sha256_file_v1(path),
            }
        )
    return rows


def _artifact_writing_convention_status(*, truth_root: Path, day_utc: str) -> str:
    try:
        for artifact_id in FAMILY_ARTIFACT_IDS:
            assert_constitutional_writer_allowed_v1(REPO_ROOT, artifact_id, WRITER_ID)
        resolve_constitutional_artifact_path_v1(
            repo_root=REPO_ROOT,
            artifact_id=COMPILED_ACTIVE_CONFIG_ARTIFACT_ID,
            day_utc=day_utc,
            canonical_truth_root=truth_root,
            extra_variables={"compiled_config_id": "0" * 64},
        )
        resolve_constitutional_artifact_path_v1(
            repo_root=REPO_ROOT,
            artifact_id=ACTIVATION_TRANSACTION_ARTIFACT_ID,
            day_utc=day_utc,
            canonical_truth_root=truth_root,
            extra_variables={"activation_transaction_id": "0" * 64},
        )
        resolve_constitutional_artifact_path_v1(
            repo_root=REPO_ROOT,
            artifact_id=CONFIGURATION_STATE_ARTIFACT_ID,
            day_utc=day_utc,
            canonical_truth_root=truth_root,
        )
    except Exception:
        return "FAIL"
    return "PASS"


def _write_governed_surface(
    *,
    truth_root: Path,
    day_utc: str,
    artifact_id: str,
    schema_relpath: str,
    artifact_key: str,
    artifact_value: str,
    payload: Dict[str, Any],
) -> SurfaceRefV1:
    path = resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id=artifact_id,
        day_utc=day_utc,
        canonical_truth_root=truth_root,
        extra_variables={artifact_key: artifact_value},
    )
    ref = atomic_write_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=schema_relpath,
    )
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id=artifact_id,
        payload=ref.payload,
        required_finality_states=[FINALITY_FINALIZED, FINALITY_PROVISIONAL],
    )
    return ref


def _published_state_path(*, truth_root: Path, day_utc: str) -> Path:
    return resolve_constitutional_artifact_path_v1(
        repo_root=REPO_ROOT,
        artifact_id=CONFIGURATION_STATE_ARTIFACT_ID,
        day_utc=day_utc,
        canonical_truth_root=truth_root,
    )


def _published_state_lock_path(*, current_path: Path) -> Path:
    return (current_path.parent / CONFIGURATION_STATE_CURRENT_LOCK_NAME).resolve()


@contextmanager
def _hold_configuration_state_publication_lock(*, current_path: Path):
    lock_path = _published_state_lock_path(current_path=current_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_fd: int | None = None
    lock_acquired = False
    try:
        try:
            lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise ConfigurationActivationError(f"CONFIGURATION_STATE_CURRENT_LOCK_BUSY:path={lock_path}") from exc
        lock_acquired = True
        lock_payload = (
            f"writer_id={WRITER_ID}\n"
            f"pid={os.getpid()}\n"
            f"target={current_path.resolve()}\n"
        ).encode("utf-8")
        os.write(lock_fd, lock_payload)
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


def _validate_policy_snapshot_ref(policy_snapshot_path: Path) -> SurfaceRefV1:
    ref = _surface_ref(policy_snapshot_path, POLICY_SNAPSHOT_SCHEMA)
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id=POLICY_SNAPSHOT_ARTIFACT_ID,
        payload=ref.payload,
        required_finality_states=[FINALITY_FINALIZED],
    )
    return ref


def _validation_payload(
    *,
    policy_snapshot_ref: SurfaceRefV1,
    governance_utility_refs: Sequence[Mapping[str, Any]],
    artifact_writing_convention_status: str,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, VALIDATION_RESULT_ARTIFACT_ID, WRITER_ID)
    policy_snapshot_dep = _governed_ref(POLICY_SNAPSHOT_ARTIFACT_ID, policy_snapshot_ref)
    blocking_reason_codes: list[str] = []
    validation_status = "PASS"
    if str(policy_snapshot_ref.payload.get("closure_state") or "").strip().upper() != "COMPLETE":
        blocking_reason_codes.append("POLICY_SNAPSHOT_NOT_COMPLETE")
    if artifact_writing_convention_status != "PASS":
        blocking_reason_codes.append("ARTIFACT_WRITING_CONVENTION_UNPROVEN")
    if _ordered_policy_snapshot_refs(policy_snapshot_ref.payload.get("governance_utility_refs") or []) != _ordered_policy_snapshot_refs(governance_utility_refs):
        blocking_reason_codes.append("GOVERNANCE_UTILITY_SET_MISMATCH")
    if blocking_reason_codes:
        validation_status = "FAIL"
    generated_at_utc = str(policy_snapshot_ref.payload.get("generated_at_utc") or "").strip()
    validation_result_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": VALIDATION_RESULT_ARTIFACT_ID,
            "policy_snapshot_id": str(policy_snapshot_ref.payload.get("policy_snapshot_id") or "").strip(),
            "governance_utility_refs": _ordered_policy_snapshot_refs(governance_utility_refs),
            "validation_status": validation_status,
            "artifact_writing_convention_status": artifact_writing_convention_status,
            "blocking_reason_codes": sorted(blocking_reason_codes),
        }
    )
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type=VALIDATION_RESULT_ARTIFACT_ID,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=VALIDATION_RESULT_ARTIFACT_ID,
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=[policy_snapshot_dep],
    )
    lineage = build_governed_artifact_lineage_v1(
        artifact_type=VALIDATION_RESULT_ARTIFACT_ID,
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=VALIDATION_RESULT_ARTIFACT_ID,
        producer_id=WRITER_ID,
        generated_at_utc=generated_at_utc,
        effective_at_utc=str(policy_snapshot_ref.payload.get("effective_at_utc") or "").strip(),
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=[policy_snapshot_dep],
        policy_snapshot_refs=[policy_snapshot_dep],
        code_version=repo_git_sha_v1(),
        run_id=f"configuration_validation:{validation_result_id}",
    )
    return {
        "schema_id": "configuration_validation_result.v1",
        "schema_version": "v1",
        "validation_result_id": validation_result_id,
        "authority_owner": WRITER_ID,
        "generated_at_utc": generated_at_utc,
        "policy_snapshot_ref": policy_snapshot_dep,
        "governance_utility_refs": [dict(row) for row in governance_utility_refs],
        "validation_status": validation_status,
        "artifact_writing_convention_status": artifact_writing_convention_status,
        "validated_document_count": len(policy_snapshot_ref.payload.get("source_document_refs") or []),
        "required_governance_utility_count": len(governance_utility_refs),
        "blocking_reason_codes": sorted(blocking_reason_codes),
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _resolved_configuration_refs(policy_snapshot_ref: SurfaceRefV1) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    for row in policy_snapshot_ref.payload.get("source_document_refs") or []:
        rows.append(
            {
                "logical_name": str(row.get("logical_name") or "").strip(),
                "path": str(Path(str(row.get("path") or "")).resolve()),
                "sha256": str(row.get("sha256") or "").strip(),
                "document_class": str(row.get("document_class") or "").strip(),
                "required_for_runtime": True,
            }
        )
    rows.sort(key=lambda item: (item["logical_name"], item["document_class"], item["path"], item["sha256"]))
    if not rows:
        raise ConfigurationActivationError("NO_RESOLVED_CONFIGURATION_REFS")
    return rows


def _compiled_config_payload(
    *,
    policy_snapshot_ref: SurfaceRefV1,
    validation_result_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, COMPILED_ACTIVE_CONFIG_ARTIFACT_ID, WRITER_ID)
    if str(validation_result_ref.payload.get("validation_status") or "").strip().upper() != "PASS":
        raise ConfigurationActivationError("VALIDATION_NOT_PASS")
    policy_snapshot_dep = _governed_ref(POLICY_SNAPSHOT_ARTIFACT_ID, policy_snapshot_ref)
    validation_result_dep = _governed_ref(VALIDATION_RESULT_ARTIFACT_ID, validation_result_ref)
    resolved_configuration_refs = _resolved_configuration_refs(policy_snapshot_ref)
    compiled_digest_sha256 = canonical_hash_for_c2_artifact_v1(
        {
            "policy_snapshot_id": str(policy_snapshot_ref.payload.get("policy_snapshot_id") or "").strip(),
            "validation_result_id": str(validation_result_ref.payload.get("validation_result_id") or "").strip(),
            "effective_at_utc": str(policy_snapshot_ref.payload.get("effective_at_utc") or "").strip(),
            "resolved_configuration_refs": resolved_configuration_refs,
        }
    )
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type=COMPILED_ACTIVE_CONFIG_ARTIFACT_ID,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=COMPILED_ACTIVE_CONFIG_ARTIFACT_ID,
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=[policy_snapshot_dep, validation_result_dep],
    )
    lineage = build_governed_artifact_lineage_v1(
        artifact_type=COMPILED_ACTIVE_CONFIG_ARTIFACT_ID,
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=COMPILED_ACTIVE_CONFIG_ARTIFACT_ID,
        producer_id=WRITER_ID,
        generated_at_utc=str(policy_snapshot_ref.payload.get("generated_at_utc") or "").strip(),
        effective_at_utc=str(policy_snapshot_ref.payload.get("effective_at_utc") or "").strip(),
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=[policy_snapshot_dep, validation_result_dep],
        policy_snapshot_refs=[policy_snapshot_dep],
        code_version=repo_git_sha_v1(),
        run_id=f"compiled_active_config:{compiled_digest_sha256}",
    )
    return {
        "schema_id": "compiled_active_config.v1",
        "schema_version": "v1",
        "compiled_config_id": compiled_digest_sha256,
        "authority_owner": WRITER_ID,
        "generated_at_utc": str(policy_snapshot_ref.payload.get("generated_at_utc") or "").strip(),
        "effective_at_utc": str(policy_snapshot_ref.payload.get("effective_at_utc") or "").strip(),
        "policy_snapshot_ref": policy_snapshot_dep,
        "validation_result_ref": validation_result_dep,
        "compile_status": "COMPILED",
        "compiled_digest_sha256": compiled_digest_sha256,
        "resolved_configuration_refs": resolved_configuration_refs,
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _compile_result_payload(
    *,
    policy_snapshot_ref: SurfaceRefV1,
    validation_result_ref: SurfaceRefV1,
    compiled_active_config_ref: SurfaceRefV1,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, COMPILE_RESULT_ARTIFACT_ID, WRITER_ID)
    policy_snapshot_dep = _governed_ref(POLICY_SNAPSHOT_ARTIFACT_ID, policy_snapshot_ref)
    validation_result_dep = _governed_ref(VALIDATION_RESULT_ARTIFACT_ID, validation_result_ref)
    compiled_dep = _governed_ref(COMPILED_ACTIVE_CONFIG_ARTIFACT_ID, compiled_active_config_ref)
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type=COMPILE_RESULT_ARTIFACT_ID,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=COMPILE_RESULT_ARTIFACT_ID,
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=[policy_snapshot_dep, validation_result_dep, compiled_dep],
    )
    lineage = build_governed_artifact_lineage_v1(
        artifact_type=COMPILE_RESULT_ARTIFACT_ID,
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=COMPILE_RESULT_ARTIFACT_ID,
        producer_id=WRITER_ID,
        generated_at_utc=str(compiled_active_config_ref.payload.get("generated_at_utc") or "").strip(),
        effective_at_utc=str(compiled_active_config_ref.payload.get("effective_at_utc") or "").strip(),
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=[policy_snapshot_dep, validation_result_dep, compiled_dep],
        policy_snapshot_refs=[policy_snapshot_dep],
        code_version=repo_git_sha_v1(),
        run_id=f"configuration_compile:{compiled_dep['sha256']}",
    )
    compile_result_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": COMPILE_RESULT_ARTIFACT_ID,
            "policy_snapshot_id": str(policy_snapshot_ref.payload.get("policy_snapshot_id") or "").strip(),
            "validation_result_id": str(validation_result_ref.payload.get("validation_result_id") or "").strip(),
            "compiled_config_id": str(compiled_active_config_ref.payload.get("compiled_config_id") or "").strip(),
            "compile_status": "COMPILED",
        }
    )
    resolved_refs = compiled_active_config_ref.payload.get("resolved_configuration_refs") or []
    return {
        "schema_id": "configuration_compile_result.v1",
        "schema_version": "v1",
        "compile_result_id": compile_result_id,
        "authority_owner": WRITER_ID,
        "generated_at_utc": str(compiled_active_config_ref.payload.get("generated_at_utc") or "").strip(),
        "policy_snapshot_ref": policy_snapshot_dep,
        "validation_result_ref": validation_result_dep,
        "compiled_active_config_ref": compiled_dep,
        "compile_status": "COMPILED",
        "compiled_entry_count": len(resolved_refs),
        "required_runtime_entry_count": sum(1 for row in resolved_refs if bool(row.get("required_for_runtime"))),
        "blocking_reason_codes": [],
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _load_prior_configuration_state(*, truth_root: Path, day_utc: str) -> SurfaceRefV1 | None:
    current_path = _published_state_path(truth_root=truth_root, day_utc=day_utc)
    if not current_path.exists() or not current_path.is_file():
        return None
    ref = _surface_ref(current_path, CONFIGURATION_STATE_SCHEMA)
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id=CONFIGURATION_STATE_ARTIFACT_ID,
        payload=ref.payload,
        required_finality_states=[FINALITY_PROVISIONAL, FINALITY_FINALIZED],
    )
    return ref


def _load_prior_configuration_family(
    *,
    truth_root: Path,
    day_utc: str,
) -> ConfigurationActivationFamilyRefsV1 | None:
    current_path = _published_state_path(truth_root=truth_root, day_utc=day_utc)
    if not current_path.exists() or not current_path.is_file():
        return None
    return load_validated_configuration_activation_family_v1(truth_root=truth_root)


def _load_prior_compiled_ref(prior_state_ref: SurfaceRefV1) -> SurfaceRefV1:
    compiled_path = Path(str(prior_state_ref.payload.get("compiled_active_config_ref", {}).get("path") or "")).resolve()
    ref = _surface_ref(compiled_path, COMPILED_ACTIVE_CONFIG_SCHEMA)
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id=COMPILED_ACTIVE_CONFIG_ARTIFACT_ID,
        payload=ref.payload,
        required_finality_states=[FINALITY_FINALIZED],
    )
    return ref


def _ref_index(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("logical_name") or "").strip()
        if key:
            out[key] = dict(row)
    return out


def _review_diff_payload(
    *,
    compiled_active_config_ref: SurfaceRefV1,
    prior_configuration_state_ref: SurfaceRefV1 | None,
    prior_compiled_active_config_ref: SurfaceRefV1 | None,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, REVIEW_DIFF_ARTIFACT_ID, WRITER_ID)
    candidate_compiled_dep = _governed_ref(COMPILED_ACTIVE_CONFIG_ARTIFACT_ID, compiled_active_config_ref)
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type=REVIEW_DIFF_ARTIFACT_ID,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=REVIEW_DIFF_ARTIFACT_ID,
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=[candidate_compiled_dep],
    )
    review_kind = "FIRST_ACTIVATION" if prior_configuration_state_ref is None else "SUPERSEDING_ACTIVATION"
    prior_state_dep = (
        None
        if prior_configuration_state_ref is None
        else _governed_ref(CONFIGURATION_STATE_ARTIFACT_ID, prior_configuration_state_ref, finality_state=FINALITY_PROVISIONAL)
    )
    prior_compiled_dep = (
        None
        if prior_compiled_active_config_ref is None
        else _governed_ref(COMPILED_ACTIVE_CONFIG_ARTIFACT_ID, prior_compiled_active_config_ref)
    )
    candidate_index = _ref_index(compiled_active_config_ref.payload.get("resolved_configuration_refs") or [])
    prior_index = _ref_index(
        []
        if prior_compiled_active_config_ref is None
        else prior_compiled_active_config_ref.payload.get("resolved_configuration_refs") or []
    )
    added = 0
    removed = 0
    changed = 0
    unchanged = 0
    for logical_name, row in candidate_index.items():
        prior_row = prior_index.get(logical_name)
        if prior_row is None:
            added += 1
        elif prior_row != row:
            changed += 1
        else:
            unchanged += 1
    for logical_name in prior_index:
        if logical_name not in candidate_index:
            removed += 1
    diff_summary = {
        "added_entry_count": added,
        "removed_entry_count": removed,
        "changed_entry_count": changed,
        "unchanged_entry_count": unchanged,
    }
    review_diff_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": REVIEW_DIFF_ARTIFACT_ID,
            "review_kind": review_kind,
            "candidate_compiled_config_id": str(compiled_active_config_ref.payload.get("compiled_config_id") or "").strip(),
            "prior_configuration_state_id": (
                None if prior_configuration_state_ref is None else str(prior_configuration_state_ref.payload.get("configuration_state_id") or "").strip()
            ),
            "prior_compiled_config_id": (
                None if prior_compiled_active_config_ref is None else str(prior_compiled_active_config_ref.payload.get("compiled_config_id") or "").strip()
            ),
            "diff_summary": diff_summary,
        }
    )
    lineage = build_governed_artifact_lineage_v1(
        artifact_type=REVIEW_DIFF_ARTIFACT_ID,
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=REVIEW_DIFF_ARTIFACT_ID,
        producer_id=WRITER_ID,
        generated_at_utc=str(compiled_active_config_ref.payload.get("generated_at_utc") or "").strip(),
        effective_at_utc=str(compiled_active_config_ref.payload.get("effective_at_utc") or "").strip(),
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=[candidate_compiled_dep],
        policy_snapshot_refs=[],
        code_version=repo_git_sha_v1(),
        run_id=f"configuration_review_diff:{review_diff_id}",
    )
    return {
        "schema_id": "configuration_review_diff.v1",
        "schema_version": "v1",
        "review_diff_id": review_diff_id,
        "authority_owner": WRITER_ID,
        "generated_at_utc": str(compiled_active_config_ref.payload.get("generated_at_utc") or "").strip(),
        "review_kind": review_kind,
        "review_status": "REVIEW_READY",
        "candidate_compiled_active_config_ref": candidate_compiled_dep,
        "prior_configuration_state_ref": prior_state_dep,
        "prior_compiled_active_config_ref": prior_compiled_dep,
        "diff_summary": diff_summary,
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _activation_transaction_payload(
    *,
    policy_snapshot_ref: SurfaceRefV1,
    validation_result_ref: SurfaceRefV1,
    compile_result_ref: SurfaceRefV1,
    review_diff_ref: SurfaceRefV1,
    compiled_active_config_ref: SurfaceRefV1,
    prior_configuration_state_ref: SurfaceRefV1 | None,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, ACTIVATION_TRANSACTION_ARTIFACT_ID, WRITER_ID)
    policy_snapshot_dep = _governed_ref(POLICY_SNAPSHOT_ARTIFACT_ID, policy_snapshot_ref)
    validation_result_dep = _governed_ref(VALIDATION_RESULT_ARTIFACT_ID, validation_result_ref)
    compile_result_dep = _governed_ref(COMPILE_RESULT_ARTIFACT_ID, compile_result_ref)
    review_diff_dep = _governed_ref(REVIEW_DIFF_ARTIFACT_ID, review_diff_ref)
    compiled_dep = _governed_ref(COMPILED_ACTIVE_CONFIG_ARTIFACT_ID, compiled_active_config_ref)
    prior_state_dep = (
        None
        if prior_configuration_state_ref is None
        else _governed_ref(CONFIGURATION_STATE_ARTIFACT_ID, prior_configuration_state_ref, finality_state=FINALITY_PROVISIONAL)
    )
    prior_activation_dep = None
    if prior_configuration_state_ref is not None:
        prior_activation_path = Path(
            str(prior_configuration_state_ref.payload.get("configuration_activation_transaction_ref", {}).get("path") or "")
        ).resolve()
        prior_activation_ref = _surface_ref(prior_activation_path, ACTIVATION_TRANSACTION_SCHEMA)
        prior_activation_dep = _governed_ref(ACTIVATION_TRANSACTION_ARTIFACT_ID, prior_activation_ref)
    activation_kind = "FIRST_ACTIVATION" if prior_configuration_state_ref is None else "SUPERSEDING_ACTIVATION"
    dependency_refs = [validation_result_dep, compile_result_dep, review_diff_dep, compiled_dep]
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type=ACTIVATION_TRANSACTION_ARTIFACT_ID,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=ACTIVATION_TRANSACTION_ARTIFACT_ID,
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    bundle = build_frozen_decision_input_bundle_v1(
        artifact_type=ACTIVATION_TRANSACTION_ARTIFACT_ID,
        authority_id=ACTIVATION_TRANSACTION_ARTIFACT_ID,
        generated_at_utc=str(compiled_active_config_ref.payload.get("generated_at_utc") or "").strip(),
        effective_at_utc=str(compiled_active_config_ref.payload.get("effective_at_utc") or "").strip(),
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[policy_snapshot_dep],
        run_id=f"configuration_activation:{str(compiled_active_config_ref.payload.get('compiled_config_id') or '').strip()}",
        reason_codes=["CONFIGURATION_ACTIVATION_PROMOTED"],
    )
    activation_transaction_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": ACTIVATION_TRANSACTION_ARTIFACT_ID,
            "activation_kind": activation_kind,
            "policy_snapshot_id": str(policy_snapshot_ref.payload.get("policy_snapshot_id") or "").strip(),
            "validation_result_id": str(validation_result_ref.payload.get("validation_result_id") or "").strip(),
            "compile_result_id": str(compile_result_ref.payload.get("compile_result_id") or "").strip(),
            "review_diff_id": str(review_diff_ref.payload.get("review_diff_id") or "").strip(),
            "compiled_config_id": str(compiled_active_config_ref.payload.get("compiled_config_id") or "").strip(),
            "prior_configuration_state_id": (
                None if prior_configuration_state_ref is None else str(prior_configuration_state_ref.payload.get("configuration_state_id") or "").strip()
            ),
        }
    )
    lineage = build_governed_artifact_lineage_v1(
        artifact_type=ACTIVATION_TRANSACTION_ARTIFACT_ID,
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=ACTIVATION_TRANSACTION_ARTIFACT_ID,
        producer_id=WRITER_ID,
        generated_at_utc=str(compiled_active_config_ref.payload.get("generated_at_utc") or "").strip(),
        effective_at_utc=str(compiled_active_config_ref.payload.get("effective_at_utc") or "").strip(),
        finality_state=FINALITY_FINALIZED,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[policy_snapshot_dep],
        code_version=repo_git_sha_v1(),
        run_id=f"configuration_activation:{activation_transaction_id}",
    )
    candidate_artifacts = [policy_snapshot_dep, validation_result_dep, compile_result_dep, review_diff_dep, compiled_dep]
    return {
        "schema_id": "configuration_activation_transaction.v1",
        "schema_version": "v1",
        "activation_transaction_id": activation_transaction_id,
        "authority_owner": WRITER_ID,
        "generated_at_utc": str(compiled_active_config_ref.payload.get("generated_at_utc") or "").strip(),
        "activation_kind": activation_kind,
        "activation_status": "PROMOTED",
        "configuration_policy_snapshot_ref": policy_snapshot_dep,
        "configuration_validation_result_ref": validation_result_dep,
        "configuration_compile_result_ref": compile_result_dep,
        "configuration_review_diff_ref": review_diff_dep,
        "compiled_active_config_ref": compiled_dep,
        "prior_configuration_state_ref": prior_state_dep,
        "prior_activation_transaction_ref": prior_activation_dep,
        "candidate_artifacts": candidate_artifacts,
        "promoted_artifacts": candidate_artifacts,
        "blocked_reason_codes": [],
        "frozen_input_bundle": bundle,
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def _configuration_state_payload(
    *,
    policy_snapshot_ref: SurfaceRefV1,
    validation_result_ref: SurfaceRefV1,
    compile_result_ref: SurfaceRefV1,
    review_diff_ref: SurfaceRefV1,
    compiled_active_config_ref: SurfaceRefV1,
    activation_transaction_ref: SurfaceRefV1,
    prior_configuration_state_ref: SurfaceRefV1 | None,
) -> Dict[str, Any]:
    contract = assert_constitutional_writer_allowed_v1(REPO_ROOT, CONFIGURATION_STATE_ARTIFACT_ID, WRITER_ID)
    policy_snapshot_dep = _governed_ref(POLICY_SNAPSHOT_ARTIFACT_ID, policy_snapshot_ref)
    validation_result_dep = _governed_ref(VALIDATION_RESULT_ARTIFACT_ID, validation_result_ref)
    compile_result_dep = _governed_ref(COMPILE_RESULT_ARTIFACT_ID, compile_result_ref)
    review_diff_dep = _governed_ref(REVIEW_DIFF_ARTIFACT_ID, review_diff_ref)
    compiled_dep = _governed_ref(COMPILED_ACTIVE_CONFIG_ARTIFACT_ID, compiled_active_config_ref)
    activation_dep = _governed_ref(ACTIVATION_TRANSACTION_ARTIFACT_ID, activation_transaction_ref)
    prior_state_dep = (
        None
        if prior_configuration_state_ref is None
        else _governed_ref(CONFIGURATION_STATE_ARTIFACT_ID, prior_configuration_state_ref, finality_state=FINALITY_PROVISIONAL)
    )
    activation_kind = str(activation_transaction_ref.payload.get("activation_kind") or "").strip()
    dependency_refs = [policy_snapshot_dep, compiled_dep, activation_dep]
    dependency_decl = build_artifact_dependency_declaration_v1(
        artifact_type=CONFIGURATION_STATE_ARTIFACT_ID,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=CONFIGURATION_STATE_ARTIFACT_ID,
        declared_dependency_artifacts=list(contract.get("required_upstream_dependencies") or []),
        dependency_refs=dependency_refs,
    )
    configuration_state_id = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_id": CONFIGURATION_STATE_ARTIFACT_ID,
            "activation_kind": activation_kind,
            "policy_snapshot_id": str(policy_snapshot_ref.payload.get("policy_snapshot_id") or "").strip(),
            "compiled_config_id": str(compiled_active_config_ref.payload.get("compiled_config_id") or "").strip(),
            "activation_transaction_id": str(activation_transaction_ref.payload.get("activation_transaction_id") or "").strip(),
            "prior_configuration_state_id": (
                None if prior_configuration_state_ref is None else str(prior_configuration_state_ref.payload.get("configuration_state_id") or "").strip()
            ),
        }
    )
    lineage = build_governed_artifact_lineage_v1(
        artifact_type=CONFIGURATION_STATE_ARTIFACT_ID,
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=CONFIGURATION_STATE_ARTIFACT_ID,
        producer_id=WRITER_ID,
        generated_at_utc=str(activation_transaction_ref.payload.get("generated_at_utc") or "").strip(),
        effective_at_utc=str(compiled_active_config_ref.payload.get("effective_at_utc") or "").strip(),
        finality_state=FINALITY_PROVISIONAL,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[policy_snapshot_dep],
        code_version=repo_git_sha_v1(),
        run_id=f"configuration_state:{configuration_state_id}",
    )
    return {
        "schema_id": "configuration_state.v1",
        "schema_version": "v1",
        "configuration_state_id": configuration_state_id,
        "authority_owner": WRITER_ID,
        "generated_at_utc": str(activation_transaction_ref.payload.get("generated_at_utc") or "").strip(),
        "status": "ACTIVE",
        "activation_kind": activation_kind,
        "configuration_policy_snapshot_ref": policy_snapshot_dep,
        "configuration_validation_result_ref": validation_result_dep,
        "configuration_compile_result_ref": compile_result_dep,
        "configuration_review_diff_ref": review_diff_dep,
        "compiled_active_config_ref": compiled_dep,
        "configuration_activation_transaction_ref": activation_dep,
        "prior_configuration_state_ref": prior_state_dep,
        "active_compiled_config_sha256": compiled_active_config_ref.sha256,
        "constitutional_dependency_declaration": dependency_decl,
        "constitutional_lineage": lineage,
    }


def run_configuration_activation_authority_v1(
    *,
    policy_snapshot_path: str | Path,
    truth_root: str | Path | None = None,
) -> ConfigurationActivationRunResultV1:
    canonical_truth_root = resolve_fact_plane_truth_root_v1(truth_root)
    policy_snapshot_ref = _validate_policy_snapshot_ref(Path(policy_snapshot_path).resolve())
    day_utc = str(policy_snapshot_ref.payload.get("effective_at_utc") or "")[:10]
    if len(day_utc) != 10:
        raise ConfigurationActivationError("POLICY_SNAPSHOT_EFFECTIVE_AT_UTC_INVALID")
    governance_utility_refs = _required_governance_utility_refs()
    artifact_writing_convention_status = _artifact_writing_convention_status(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
    )
    validation_payload = _validation_payload(
        policy_snapshot_ref=policy_snapshot_ref,
        governance_utility_refs=governance_utility_refs,
        artifact_writing_convention_status=artifact_writing_convention_status,
    )
    validation_result_ref = _write_governed_surface(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
        artifact_id=VALIDATION_RESULT_ARTIFACT_ID,
        schema_relpath=VALIDATION_RESULT_SCHEMA,
        artifact_key="validation_result_id",
        artifact_value=str(validation_payload["validation_result_id"]),
        payload=validation_payload,
    )
    if str(validation_result_ref.payload.get("validation_status") or "").strip().upper() != "PASS":
        return ConfigurationActivationRunResultV1(
            policy_snapshot_ref=policy_snapshot_ref,
            validation_result_ref=validation_result_ref,
            compiled_active_config_ref=None,
            compile_result_ref=None,
            review_diff_ref=None,
            activation_transaction_ref=None,
            configuration_state_ref=None,
        )

    compiled_payload = _compiled_config_payload(
        policy_snapshot_ref=policy_snapshot_ref,
        validation_result_ref=validation_result_ref,
    )
    compiled_active_config_ref = _write_governed_surface(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
        artifact_id=COMPILED_ACTIVE_CONFIG_ARTIFACT_ID,
        schema_relpath=COMPILED_ACTIVE_CONFIG_SCHEMA,
        artifact_key="compiled_config_id",
        artifact_value=str(compiled_payload["compiled_config_id"]),
        payload=compiled_payload,
    )
    compile_result_payload = _compile_result_payload(
        policy_snapshot_ref=policy_snapshot_ref,
        validation_result_ref=validation_result_ref,
        compiled_active_config_ref=compiled_active_config_ref,
    )
    compile_result_ref = _write_governed_surface(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
        artifact_id=COMPILE_RESULT_ARTIFACT_ID,
        schema_relpath=COMPILE_RESULT_SCHEMA,
        artifact_key="compile_result_id",
        artifact_value=str(compile_result_payload["compile_result_id"]),
        payload=compile_result_payload,
    )
    prior_family = _load_prior_configuration_family(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
    )
    prior_configuration_state_ref = None if prior_family is None else prior_family.configuration_state_ref
    prior_compiled_active_config_ref = None if prior_family is None else prior_family.compiled_active_config_ref
    if (
        prior_family is not None
        and str(prior_family.policy_snapshot_ref.path) == str(policy_snapshot_ref.path)
        and str(prior_family.policy_snapshot_ref.sha256) == str(policy_snapshot_ref.sha256)
        and str(prior_family.compiled_active_config_ref.path) == str(compiled_active_config_ref.path)
        and str(prior_family.compiled_active_config_ref.sha256) == str(compiled_active_config_ref.sha256)
    ):
        return ConfigurationActivationRunResultV1(
            policy_snapshot_ref=policy_snapshot_ref,
            validation_result_ref=validation_result_ref,
            compiled_active_config_ref=compiled_active_config_ref,
            compile_result_ref=compile_result_ref,
            review_diff_ref=prior_family.review_diff_ref,
            activation_transaction_ref=prior_family.activation_transaction_ref,
            configuration_state_ref=prior_family.configuration_state_ref,
        )
    review_diff_payload = _review_diff_payload(
        compiled_active_config_ref=compiled_active_config_ref,
        prior_configuration_state_ref=prior_configuration_state_ref,
        prior_compiled_active_config_ref=prior_compiled_active_config_ref,
    )
    review_diff_ref = _write_governed_surface(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
        artifact_id=REVIEW_DIFF_ARTIFACT_ID,
        schema_relpath=REVIEW_DIFF_SCHEMA,
        artifact_key="review_diff_id",
        artifact_value=str(review_diff_payload["review_diff_id"]),
        payload=review_diff_payload,
    )
    activation_transaction_payload = _activation_transaction_payload(
        policy_snapshot_ref=policy_snapshot_ref,
        validation_result_ref=validation_result_ref,
        compile_result_ref=compile_result_ref,
        review_diff_ref=review_diff_ref,
        compiled_active_config_ref=compiled_active_config_ref,
        prior_configuration_state_ref=prior_configuration_state_ref,
    )
    activation_transaction_ref = _write_governed_surface(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
        artifact_id=ACTIVATION_TRANSACTION_ARTIFACT_ID,
        schema_relpath=ACTIVATION_TRANSACTION_SCHEMA,
        artifact_key="activation_transaction_id",
        artifact_value=str(activation_transaction_payload["activation_transaction_id"]),
        payload=activation_transaction_payload,
    )
    configuration_state_payload = _configuration_state_payload(
        policy_snapshot_ref=policy_snapshot_ref,
        validation_result_ref=validation_result_ref,
        compile_result_ref=compile_result_ref,
        review_diff_ref=review_diff_ref,
        compiled_active_config_ref=compiled_active_config_ref,
        activation_transaction_ref=activation_transaction_ref,
        prior_configuration_state_ref=prior_configuration_state_ref,
    )
    current_path = _published_state_path(truth_root=canonical_truth_root, day_utc=day_utc)
    with _hold_configuration_state_publication_lock(current_path=current_path):
        ref = atomic_write_idempotent_validated_json_v1(
            path=current_path,
            payload=configuration_state_payload,
            schema_relpath=CONFIGURATION_STATE_SCHEMA,
            volatile_field_names=("generated_at_utc",),
        )
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id=CONFIGURATION_STATE_ARTIFACT_ID,
        payload=ref.payload,
        required_finality_states=[FINALITY_PROVISIONAL, FINALITY_FINALIZED],
    )
    configuration_state_ref = SurfaceRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)
    return ConfigurationActivationRunResultV1(
        policy_snapshot_ref=policy_snapshot_ref,
        validation_result_ref=validation_result_ref,
        compiled_active_config_ref=compiled_active_config_ref,
        compile_result_ref=compile_result_ref,
        review_diff_ref=review_diff_ref,
        activation_transaction_ref=activation_transaction_ref,
        configuration_state_ref=configuration_state_ref,
    )
