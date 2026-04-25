from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Sequence

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_RELPATH = "governance/02_REGISTRIES/C2_ARTIFACT_AUTHORITY_REGISTRY_V1.json"
REGISTRY_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/constitutional_artifact_authority_registry.v1.schema.json"
)
LINEAGE_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RUNTIME/governed_artifact_lineage.v1.schema.json"
FROZEN_INPUT_BUNDLE_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/frozen_decision_input_bundle.v1.schema.json"
)
DEPENDENCY_DECLARATION_SCHEMA_RELPATH = (
    "governance/04_DATA/SCHEMAS/C2/RUNTIME/artifact_dependency_declaration.v1.schema.json"
)

ARTIFACT_CLASS_SOURCE_FACT = "source_fact"
ARTIFACT_CLASS_COMPILED_STATE = "compiled_state"
ARTIFACT_CLASS_POLICY_SNAPSHOT = "policy_snapshot"
ARTIFACT_CLASS_POLICY_INTERPRETATION = "policy_interpretation"
ARTIFACT_CLASS_DECISION_PROPOSAL = "decision_proposal"
ARTIFACT_CLASS_ADMISSION_RESULT = "admission_result"
ARTIFACT_CLASS_EXECUTION_RESULT = "execution_result"
ARTIFACT_CLASS_OUTCOME_RECORD = "outcome_record"
ARTIFACT_CLASS_OVERRIDE_RECORD = "override_record"
ARTIFACT_CLASS_EXCEPTION_RECORD = "exception_record"
ARTIFACT_CLASS_READ_MODEL = "read_model"

FINALITY_PROVISIONAL = "provisional"
FINALITY_FINALIZED = "finalized"
FINALITY_CORRECTED = "corrected"
FINALITY_SUPERSEDED = "superseded"
FINALITY_ARCHIVED = "archived"

CLOSURE_STATE_COMPLETE = "COMPLETE"
CLOSURE_STATE_BLOCKED = "BLOCKED"
CLOSURE_STATE_DEGRADED = "DEGRADED"
CLOSURE_STATE_OPEN = "OPEN"

ARTIFACT_CLASSES = {
    ARTIFACT_CLASS_SOURCE_FACT,
    ARTIFACT_CLASS_COMPILED_STATE,
    ARTIFACT_CLASS_POLICY_SNAPSHOT,
    ARTIFACT_CLASS_POLICY_INTERPRETATION,
    ARTIFACT_CLASS_DECISION_PROPOSAL,
    ARTIFACT_CLASS_ADMISSION_RESULT,
    ARTIFACT_CLASS_EXECUTION_RESULT,
    ARTIFACT_CLASS_OUTCOME_RECORD,
    ARTIFACT_CLASS_OVERRIDE_RECORD,
    ARTIFACT_CLASS_EXCEPTION_RECORD,
    ARTIFACT_CLASS_READ_MODEL,
}

FINALITY_STATES = {
    FINALITY_PROVISIONAL,
    FINALITY_FINALIZED,
    FINALITY_CORRECTED,
    FINALITY_SUPERSEDED,
    FINALITY_ARCHIVED,
}

CLOSURE_STATES = {
    CLOSURE_STATE_COMPLETE,
    CLOSURE_STATE_BLOCKED,
    CLOSURE_STATE_DEGRADED,
    CLOSURE_STATE_OPEN,
}

ALLOWED_DEPENDENCY_CLASSES: dict[str, set[str]] = {
    ARTIFACT_CLASS_SOURCE_FACT: set(),
    ARTIFACT_CLASS_COMPILED_STATE: {
        ARTIFACT_CLASS_SOURCE_FACT,
        ARTIFACT_CLASS_COMPILED_STATE,
        ARTIFACT_CLASS_OVERRIDE_RECORD,
        ARTIFACT_CLASS_EXCEPTION_RECORD,
    },
    ARTIFACT_CLASS_POLICY_SNAPSHOT: {
        ARTIFACT_CLASS_SOURCE_FACT,
        ARTIFACT_CLASS_COMPILED_STATE,
        ARTIFACT_CLASS_POLICY_SNAPSHOT,
        ARTIFACT_CLASS_OVERRIDE_RECORD,
        ARTIFACT_CLASS_EXCEPTION_RECORD,
    },
    ARTIFACT_CLASS_POLICY_INTERPRETATION: {
        ARTIFACT_CLASS_SOURCE_FACT,
        ARTIFACT_CLASS_COMPILED_STATE,
        ARTIFACT_CLASS_POLICY_SNAPSHOT,
        ARTIFACT_CLASS_POLICY_INTERPRETATION,
        ARTIFACT_CLASS_OVERRIDE_RECORD,
        ARTIFACT_CLASS_EXCEPTION_RECORD,
    },
    ARTIFACT_CLASS_DECISION_PROPOSAL: {
        ARTIFACT_CLASS_SOURCE_FACT,
        ARTIFACT_CLASS_COMPILED_STATE,
        ARTIFACT_CLASS_POLICY_SNAPSHOT,
        ARTIFACT_CLASS_POLICY_INTERPRETATION,
        ARTIFACT_CLASS_DECISION_PROPOSAL,
        ARTIFACT_CLASS_ADMISSION_RESULT,
        ARTIFACT_CLASS_OUTCOME_RECORD,
        ARTIFACT_CLASS_OVERRIDE_RECORD,
        ARTIFACT_CLASS_EXCEPTION_RECORD,
    },
    ARTIFACT_CLASS_ADMISSION_RESULT: {
        ARTIFACT_CLASS_SOURCE_FACT,
        ARTIFACT_CLASS_COMPILED_STATE,
        ARTIFACT_CLASS_POLICY_SNAPSHOT,
        ARTIFACT_CLASS_POLICY_INTERPRETATION,
        ARTIFACT_CLASS_DECISION_PROPOSAL,
        ARTIFACT_CLASS_ADMISSION_RESULT,
        ARTIFACT_CLASS_OUTCOME_RECORD,
        ARTIFACT_CLASS_OVERRIDE_RECORD,
        ARTIFACT_CLASS_EXCEPTION_RECORD,
    },
    ARTIFACT_CLASS_EXECUTION_RESULT: {
        ARTIFACT_CLASS_SOURCE_FACT,
        ARTIFACT_CLASS_COMPILED_STATE,
        ARTIFACT_CLASS_POLICY_SNAPSHOT,
        ARTIFACT_CLASS_POLICY_INTERPRETATION,
        ARTIFACT_CLASS_DECISION_PROPOSAL,
        ARTIFACT_CLASS_ADMISSION_RESULT,
        ARTIFACT_CLASS_EXECUTION_RESULT,
        ARTIFACT_CLASS_OUTCOME_RECORD,
        ARTIFACT_CLASS_OVERRIDE_RECORD,
        ARTIFACT_CLASS_EXCEPTION_RECORD,
    },
    ARTIFACT_CLASS_OUTCOME_RECORD: {
        ARTIFACT_CLASS_SOURCE_FACT,
        ARTIFACT_CLASS_COMPILED_STATE,
        ARTIFACT_CLASS_POLICY_SNAPSHOT,
        ARTIFACT_CLASS_POLICY_INTERPRETATION,
        ARTIFACT_CLASS_DECISION_PROPOSAL,
        ARTIFACT_CLASS_ADMISSION_RESULT,
        ARTIFACT_CLASS_EXECUTION_RESULT,
        ARTIFACT_CLASS_OUTCOME_RECORD,
        ARTIFACT_CLASS_OVERRIDE_RECORD,
        ARTIFACT_CLASS_EXCEPTION_RECORD,
    },
    ARTIFACT_CLASS_OVERRIDE_RECORD: ARTIFACT_CLASSES - {ARTIFACT_CLASS_READ_MODEL},
    ARTIFACT_CLASS_EXCEPTION_RECORD: ARTIFACT_CLASSES - {ARTIFACT_CLASS_READ_MODEL},
    ARTIFACT_CLASS_READ_MODEL: ARTIFACT_CLASSES - {ARTIFACT_CLASS_READ_MODEL},
}

READ_MODEL_SURFACE_KINDS = {"projection", "composition"}
READ_MODEL_FORBIDDEN_AUTHORITY_FIELDS = {
    "authoritative_writer",
    "authoritative_root_type",
    "authority_id",
}


class ConstitutionalRuntimeError(ValueError):
    pass


def _repo_root(repo_root: Path | str) -> Path:
    return Path(repo_root).resolve()


def _read_json_object(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ConstitutionalRuntimeError(f"json root must be object: {path}")
    return obj


def _validated_ref_rows(rows: Sequence[Mapping[str, Any]]) -> list[Dict[str, Any]]:
    validated: list[Dict[str, Any]] = []
    for row in rows:
        artifact_id = str(row.get("artifact_id") or "").strip()
        path = str(row.get("path") or "").strip()
        sha256 = str(row.get("sha256") or "").strip()
        if not artifact_id or not path or not sha256:
            raise ConstitutionalRuntimeError("artifact ref requires artifact_id, path, sha256")
        validated.append(
            {
                "artifact_id": artifact_id,
                "path": path,
                "sha256": sha256,
                "artifact_class": str(row.get("artifact_class") or "").strip(),
                "finality_state": str(row.get("finality_state") or "").strip(),
            }
        )
    return validated


def validate_artifact_contract_rows_v1(rows: Sequence[Mapping[str, Any]]) -> None:
    seen_ids: set[str] = set()
    domain_ids = {
        str(row.get("domain_id") or "").strip()
        for row in rows
        if isinstance(row, Mapping) and str(row.get("domain_id") or "").strip()
    }
    by_id: dict[str, Mapping[str, Any]] = {}

    for row in rows:
        artifact_id = str(row.get("artifact_id") or "").strip()
        if not artifact_id:
            raise ConstitutionalRuntimeError("artifact contract missing artifact_id")
        if artifact_id in seen_ids:
            raise ConstitutionalRuntimeError(f"duplicate artifact authority contract: {artifact_id}")
        seen_ids.add(artifact_id)
        by_id[artifact_id] = row

        artifact_class = str(row.get("artifact_class") or "").strip()
        if artifact_class not in ARTIFACT_CLASSES:
            raise ConstitutionalRuntimeError(f"invalid artifact class: artifact_id={artifact_id} artifact_class={artifact_class}")

        domain_id = str(row.get("authoritative_domain") or "").strip()
        if domain_ids and domain_id and domain_id not in domain_ids:
            raise ConstitutionalRuntimeError(f"artifact references unknown domain: artifact_id={artifact_id} domain_id={domain_id}")

        initial_finality = str(row.get("initial_finality_state") or "").strip()
        allowed_finalities = {
            str(value).strip()
            for value in (row.get("allowed_finality_states") or [])
            if str(value).strip()
        }
        if initial_finality not in FINALITY_STATES:
            raise ConstitutionalRuntimeError(
                f"invalid initial finality state: artifact_id={artifact_id} state={initial_finality}"
            )
        if not allowed_finalities:
            raise ConstitutionalRuntimeError(f"artifact contract missing allowed finality states: {artifact_id}")
        if not allowed_finalities.issubset(FINALITY_STATES):
            raise ConstitutionalRuntimeError(f"artifact contract has invalid finality set: {artifact_id}")
        if initial_finality not in allowed_finalities:
            raise ConstitutionalRuntimeError(
                f"initial finality state not allowed: artifact_id={artifact_id} state={initial_finality}"
            )

        root_policy = str(row.get("root_policy") or "").strip()
        mirror_paths = row.get("mirror_paths") or []
        if mirror_paths and root_policy != "mirrored":
            raise ConstitutionalRuntimeError(
                f"mirror paths only allowed for mirrored artifacts: artifact_id={artifact_id} root_policy={root_policy}"
            )

        if artifact_class == ARTIFACT_CLASS_READ_MODEL and str(row.get("authoritative_root_type") or "").strip() in {
            "canonical_truth_root",
            "execution_truth_root",
            "mixed",
        }:
            raise ConstitutionalRuntimeError(
                f"read_model cannot register as canonical writer: artifact_id={artifact_id}"
            )

    for artifact_id, row in by_id.items():
        artifact_class = str(row.get("artifact_class") or "").strip()
        allowed = ALLOWED_DEPENDENCY_CLASSES.get(artifact_class, set())
        for dep_id in row.get("required_upstream_dependencies") or []:
            dep_text = str(dep_id).strip()
            dep_row = by_id.get(dep_text)
            if dep_row is None:
                raise ConstitutionalRuntimeError(
                    f"artifact declares unknown dependency: artifact_id={artifact_id} dependency_id={dep_text}"
                )
            dep_class = str(dep_row.get("artifact_class") or "").strip()
            if dep_class not in allowed:
                raise ConstitutionalRuntimeError(
                    "illegal upward dependency: "
                    f"artifact_id={artifact_id} artifact_class={artifact_class} "
                    f"dependency_id={dep_text} dependency_class={dep_class}"
                )


@lru_cache(maxsize=8)
def _load_registry_cached(repo_root_str: str) -> Dict[str, Any]:
    repo_root = Path(repo_root_str).resolve()
    path = (repo_root / REGISTRY_RELPATH).resolve()
    obj = _read_json_object(path)
    validate_against_repo_schema_v1(obj, repo_root, REGISTRY_SCHEMA_RELPATH)
    rows = obj.get("artifacts")
    if not isinstance(rows, list):
        raise ConstitutionalRuntimeError(f"artifact registry missing artifacts list: {path}")
    validate_artifact_contract_rows_v1(rows)
    return obj


def load_constitutional_artifact_authority_registry_v1(repo_root: Path | str) -> Dict[str, Any]:
    return dict(_load_registry_cached(str(_repo_root(repo_root))))


def iter_constitutional_artifact_contracts_v1(repo_root: Path | str) -> Iterable[Dict[str, Any]]:
    registry = _load_registry_cached(str(_repo_root(repo_root)))
    for row in registry.get("artifacts") or []:
        if isinstance(row, dict):
            yield dict(row)


def get_constitutional_artifact_contract_v1(repo_root: Path | str, artifact_id: str) -> Dict[str, Any]:
    artifact_text = str(artifact_id or "").strip()
    for row in iter_constitutional_artifact_contracts_v1(repo_root):
        if str(row.get("artifact_id") or "").strip() == artifact_text:
            return row
    raise ConstitutionalRuntimeError(f"unknown constitutional artifact contract: {artifact_text}")


def assert_constitutional_consumer_allowed_v1(repo_root: Path | str, artifact_id: str, consumer_id: str) -> Dict[str, Any]:
    contract = get_constitutional_artifact_contract_v1(repo_root, artifact_id)
    legal_consumers = {
        str(value).strip()
        for value in (contract.get("legal_consumers") or [])
        if str(value).strip()
    }
    consumer_text = str(consumer_id or "").strip()
    if consumer_text not in legal_consumers:
        raise ConstitutionalRuntimeError(
            f"artifact consumer not allowed by constitutional contract: artifact_id={artifact_id} consumer_id={consumer_text}"
        )
    return contract


def assert_constitutional_writer_allowed_v1(repo_root: Path | str, artifact_id: str, writer_id: str) -> Dict[str, Any]:
    contract = get_constitutional_artifact_contract_v1(repo_root, artifact_id)
    expected_writer = str(contract.get("authoritative_writer") or "").strip()
    writer_text = str(writer_id or "").strip()
    if writer_text != expected_writer:
        raise ConstitutionalRuntimeError(
            f"artifact writer mismatch: artifact_id={artifact_id} writer_id={writer_text} expected={expected_writer}"
        )
    return contract


def get_constitutional_artifact_mirror_contract_v1(repo_root: Path | str, artifact_id: str, mirror_id: str) -> Dict[str, Any]:
    contract = get_constitutional_artifact_contract_v1(repo_root, artifact_id)
    for row in (contract.get("mirror_paths") or []):
        if not isinstance(row, dict):
            continue
        if str(row.get("mirror_id") or "").strip() == str(mirror_id or "").strip():
            return dict(row)
    raise ConstitutionalRuntimeError(
        f"unknown constitutional mirror contract: artifact_id={artifact_id} mirror_id={mirror_id}"
    )


def _render_path_pattern(pattern: str, variables: Mapping[str, Any]) -> Path:
    rendered = str(pattern)
    for key, value in variables.items():
        rendered = rendered.replace("{" + str(key) + "}", str(value))
    unresolved = [part for part in rendered.split("{") if "}" in part]
    if unresolved:
        raise ConstitutionalRuntimeError(f"unresolved artifact path placeholders remain: {pattern}")
    return Path(rendered).resolve()


def resolve_constitutional_artifact_path_v1(
    *,
    repo_root: Path | str,
    artifact_id: str,
    day_utc: str,
    canonical_truth_root: Path | str | None = None,
    execution_truth_root: Path | str | None = None,
    path_role: str = "authoritative",
    mirror_id: str = "",
    extra_variables: Mapping[str, Any] | None = None,
) -> Path:
    contract = get_constitutional_artifact_contract_v1(repo_root, artifact_id)
    if path_role == "authoritative":
        pattern = str(contract.get("authoritative_path_pattern") or "").strip()
    elif path_role == "mirror":
        mirror = get_constitutional_artifact_mirror_contract_v1(repo_root, artifact_id, mirror_id)
        pattern = str(mirror.get("path_pattern") or "").strip()
    else:
        raise ConstitutionalRuntimeError(f"unsupported artifact path role: {path_role}")

    variables: Dict[str, Any] = {
        "day_utc": str(day_utc).strip(),
        "canonical_truth_root": str(Path(canonical_truth_root).resolve()) if canonical_truth_root is not None else "",
        "execution_truth_root": str(Path(execution_truth_root).resolve()) if execution_truth_root is not None else "",
    }
    if extra_variables:
        variables.update({str(k): v for k, v in extra_variables.items()})
    return _render_path_pattern(pattern, variables)


def build_artifact_dependency_declaration_v1(
    *,
    artifact_type: str,
    artifact_class: str,
    authority_id: str,
    declared_dependency_artifacts: Sequence[str],
    dependency_refs: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    if artifact_class not in ARTIFACT_CLASSES:
        raise ConstitutionalRuntimeError(f"invalid artifact class for dependency declaration: {artifact_class}")
    payload = {
        "schema_id": "artifact_dependency_declaration",
        "schema_version": "v1",
        "artifact_type": str(artifact_type).strip(),
        "artifact_class": artifact_class,
        "authority_id": str(authority_id).strip(),
        "declared_dependency_artifacts": [
            str(item).strip() for item in declared_dependency_artifacts if str(item).strip()
        ],
        "dependency_refs": _validated_ref_rows(list(dependency_refs)),
    }
    validate_against_repo_schema_v1(payload, REPO_ROOT, DEPENDENCY_DECLARATION_SCHEMA_RELPATH)
    return payload


def build_governed_dependency_ref_v1(
    *,
    repo_root: Path | str,
    artifact_id: str,
    path: str | Path,
    sha256: str,
    finality_state: str,
) -> Dict[str, Any]:
    contract = get_constitutional_artifact_contract_v1(repo_root, artifact_id)
    return {
        "artifact_id": str(artifact_id).strip(),
        "path": str(Path(path).resolve()),
        "sha256": str(sha256 or "").strip(),
        "artifact_class": str(contract.get("artifact_class") or "").strip(),
        "finality_state": str(finality_state or "").strip(),
    }


def build_governed_artifact_lineage_v1(
    *,
    artifact_type: str,
    artifact_version: str,
    artifact_class: str,
    authority_id: str,
    producer_id: str,
    generated_at_utc: str,
    effective_at_utc: str,
    finality_state: str,
    input_artifact_refs: Sequence[Mapping[str, Any]],
    policy_snapshot_refs: Sequence[Mapping[str, Any]] | None = None,
    code_version: str = "",
    run_id: str = "",
    corrected_from_ref: Mapping[str, Any] | None = None,
    supersedes_ref: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    if artifact_class not in ARTIFACT_CLASSES:
        raise ConstitutionalRuntimeError(f"invalid artifact class for lineage: {artifact_class}")
    if finality_state not in FINALITY_STATES:
        raise ConstitutionalRuntimeError(f"invalid finality state for lineage: {finality_state}")
    if finality_state == FINALITY_CORRECTED and not corrected_from_ref:
        raise ConstitutionalRuntimeError("corrected lineage requires corrected_from_ref")
    if finality_state == FINALITY_SUPERSEDED and not supersedes_ref:
        raise ConstitutionalRuntimeError("superseded lineage requires supersedes_ref")

    payload = {
        "schema_id": "governed_artifact_lineage",
        "schema_version": "v1",
        "artifact_type": str(artifact_type).strip(),
        "artifact_version": str(artifact_version).strip(),
        "artifact_class": artifact_class,
        "authority_id": str(authority_id).strip(),
        "producer_id": str(producer_id).strip(),
        "generated_at_utc": str(generated_at_utc).strip(),
        "effective_at_utc": str(effective_at_utc).strip(),
        "finality_state": finality_state,
        "input_artifact_refs": _validated_ref_rows(list(input_artifact_refs)),
        "policy_snapshot_refs": _validated_ref_rows(list(policy_snapshot_refs or [])),
        "code_version": str(code_version or "").strip(),
        "run_id": str(run_id or "").strip(),
        "corrected_from_ref": (dict(corrected_from_ref) if corrected_from_ref else None),
        "supersedes_ref": (dict(supersedes_ref) if supersedes_ref else None),
    }
    validate_against_repo_schema_v1(payload, REPO_ROOT, LINEAGE_SCHEMA_RELPATH)
    return payload


def build_frozen_decision_input_bundle_v1(
    *,
    artifact_type: str,
    authority_id: str,
    generated_at_utc: str,
    effective_at_utc: str,
    input_artifact_refs: Sequence[Mapping[str, Any]],
    policy_snapshot_refs: Sequence[Mapping[str, Any]] | None = None,
    run_id: str = "",
    reason_codes: Sequence[str] | None = None,
) -> Dict[str, Any]:
    ref_rows = _validated_ref_rows(list(input_artifact_refs))
    payload = {
        "schema_id": "frozen_decision_input_bundle",
        "schema_version": "v1",
        "bundle_id": "",
        "artifact_type": str(artifact_type).strip(),
        "artifact_class": ARTIFACT_CLASS_DECISION_PROPOSAL,
        "authority_id": str(authority_id).strip(),
        "generated_at_utc": str(generated_at_utc).strip(),
        "effective_at_utc": str(effective_at_utc).strip(),
        "frozen": True,
        "input_artifact_refs": ref_rows,
        "policy_snapshot_refs": _validated_ref_rows(list(policy_snapshot_refs or [])),
        "run_id": str(run_id or "").strip(),
        "reason_codes": [str(code).strip() for code in (reason_codes or []) if str(code).strip()],
        "input_bundle_hash": "",
    }
    bundle_hash = canonical_hash_for_c2_artifact_v1(
        {
            "artifact_type": payload["artifact_type"],
            "authority_id": payload["authority_id"],
            "generated_at_utc": payload["generated_at_utc"],
            "effective_at_utc": payload["effective_at_utc"],
            "input_artifact_refs": payload["input_artifact_refs"],
            "policy_snapshot_refs": payload["policy_snapshot_refs"],
            "run_id": payload["run_id"],
            "reason_codes": payload["reason_codes"],
        }
    )
    payload["bundle_id"] = bundle_hash
    payload["input_bundle_hash"] = bundle_hash
    validate_against_repo_schema_v1(payload, REPO_ROOT, FROZEN_INPUT_BUNDLE_SCHEMA_RELPATH)
    return payload


def build_machine_blocker_envelope_v1(
    *,
    closure_state: str,
    reason_codes: Sequence[str] | None = None,
    first_blocker_code: str = "",
    missing_dependency_artifacts: Sequence[str] | None = None,
) -> Dict[str, Any]:
    normalized_closure_state = str(closure_state or "").strip().upper()
    if normalized_closure_state not in CLOSURE_STATES:
        raise ConstitutionalRuntimeError(
            f"invalid closure state for blocker envelope: {closure_state!r}"
        )
    blocking_codes: list[str] = []
    seen_codes: set[str] = set()
    for code in (reason_codes or []):
        text = str(code or "").strip()
        if text and text not in seen_codes:
            seen_codes.add(text)
            blocking_codes.append(text)
    missing_dependency_rows: list[str] = []
    seen_missing: set[str] = set()
    for artifact_id in (missing_dependency_artifacts or []):
        text = str(artifact_id or "").strip()
        if text and text not in seen_missing:
            seen_missing.add(text)
            missing_dependency_rows.append(text)
    normalized_first_blocker = str(first_blocker_code or "").strip()
    if not normalized_first_blocker and blocking_codes:
        normalized_first_blocker = blocking_codes[0]
    if normalized_closure_state == CLOSURE_STATE_COMPLETE and (
        blocking_codes or missing_dependency_rows or normalized_first_blocker
    ):
        raise ConstitutionalRuntimeError(
            "complete blocker envelope must not carry blockers or missing dependencies"
        )
    if normalized_closure_state in {CLOSURE_STATE_BLOCKED, CLOSURE_STATE_DEGRADED} and not (
        blocking_codes or missing_dependency_rows or normalized_first_blocker
    ):
        raise ConstitutionalRuntimeError(
            "blocked/degraded blocker envelope requires explicit blocker semantics"
        )
    return {
        "closure_state": normalized_closure_state,
        "blocking_codes": blocking_codes,
        "first_blocker_code": normalized_first_blocker,
        "missing_dependency_artifacts": missing_dependency_rows,
    }


def _raw_sha256_file(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_artifact_sha256_file(path: Path) -> str:
    return canonical_hash_for_c2_artifact_v1(json.loads(path.read_text(encoding="utf-8")))


def _artifact_sha_candidates(path: Path) -> set[str]:
    candidates = {_raw_sha256_file(path)}
    try:
        candidates.add(_canonical_artifact_sha256_file(path))
    except Exception:
        pass
    return candidates


def _normalized_ref_rows(rows: Sequence[Mapping[str, Any]]) -> list[Dict[str, Any]]:
    normalized = _validated_ref_rows(rows)
    return [
        {
            "artifact_id": str(row.get("artifact_id") or "").strip(),
            "path": str(row.get("path") or "").strip(),
            "sha256": str(row.get("sha256") or "").strip(),
            "artifact_class": str(row.get("artifact_class") or "").strip(),
            "finality_state": str(row.get("finality_state") or "").strip(),
        }
        for row in normalized
    ]


def validate_governed_artifact_payload_v1(
    *,
    repo_root: Path | str,
    artifact_id: str,
    payload: Mapping[str, Any],
    consumer_id: str = "",
    required_finality_states: Sequence[str] | None = None,
) -> Dict[str, Any]:
    repo_root_path = _repo_root(repo_root)
    artifact_text = str(artifact_id or "").strip()
    if not isinstance(payload, Mapping):
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_PAYLOAD_NOT_OBJECT:artifact_id={artifact_text}"
        )
    contract = get_constitutional_artifact_contract_v1(repo_root_path, artifact_text)
    if consumer_id:
        try:
            assert_constitutional_consumer_allowed_v1(repo_root_path, artifact_text, consumer_id)
        except ConstitutionalRuntimeError as exc:
            raise ConstitutionalRuntimeError(
                f"CONSTITUTIONAL_ARTIFACT_CONSUMER_NOT_ALLOWED:artifact_id={artifact_text}:consumer_id={consumer_id}"
            ) from exc

    schema_relpath = str(contract.get("schema_relpath") or "").strip()
    if schema_relpath:
        try:
            validate_against_repo_schema_v1(dict(payload), repo_root_path, schema_relpath)
        except Exception as exc:
            raise ConstitutionalRuntimeError(
                f"CONSTITUTIONAL_ARTIFACT_SCHEMA_INVALID:artifact_id={artifact_text}:reason={type(exc).__name__}"
            ) from exc

    dependency_declaration = payload.get("constitutional_dependency_declaration")
    if not isinstance(dependency_declaration, Mapping):
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_DEPENDENCY_DECLARATION_MISSING:artifact_id={artifact_text}"
        )
    lineage = payload.get("constitutional_lineage")
    if not isinstance(lineage, Mapping):
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_LINEAGE_MISSING:artifact_id={artifact_text}"
        )

    try:
        validate_against_repo_schema_v1(
            dict(dependency_declaration),
            repo_root_path,
            DEPENDENCY_DECLARATION_SCHEMA_RELPATH,
        )
    except Exception as exc:
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_DEPENDENCY_DECLARATION_INVALID:artifact_id={artifact_text}:reason={type(exc).__name__}"
        ) from exc
    try:
        validate_against_repo_schema_v1(
            dict(lineage),
            repo_root_path,
            LINEAGE_SCHEMA_RELPATH,
        )
    except Exception as exc:
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_LINEAGE_INVALID:artifact_id={artifact_text}:reason={type(exc).__name__}"
        ) from exc

    contract_artifact_class = str(contract.get("artifact_class") or "").strip()
    contract_required_deps = [
        str(value).strip()
        for value in (contract.get("required_upstream_dependencies") or [])
        if str(value).strip()
    ]
    declared_deps = [
        str(value).strip()
        for value in (dependency_declaration.get("declared_dependency_artifacts") or [])
        if str(value).strip()
    ]
    if dependency_declaration.get("artifact_type") != artifact_text:
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_DEPENDENCY_TYPE_MISMATCH:artifact_id={artifact_text}"
        )
    if str(dependency_declaration.get("artifact_class") or "").strip() != contract_artifact_class:
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_DEPENDENCY_CLASS_MISMATCH:artifact_id={artifact_text}"
        )
    if str(dependency_declaration.get("authority_id") or "").strip() != artifact_text:
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_DEPENDENCY_AUTHORITY_ID_MISMATCH:artifact_id={artifact_text}"
        )
    if declared_deps != contract_required_deps:
        raise ConstitutionalRuntimeError(
            "CONSTITUTIONAL_ARTIFACT_DECLARED_DEPENDENCIES_MISMATCH:"
            f"artifact_id={artifact_text}:declared={declared_deps}:expected={contract_required_deps}"
        )

    closure_state = str(payload.get("closure_state") or "").strip().upper()
    missing_dependency_artifacts = [
        str(value).strip()
        for value in (payload.get("missing_dependency_artifacts") or [])
        if str(value).strip()
    ]
    missing_dependency_set = set(missing_dependency_artifacts)
    if closure_state == CLOSURE_STATE_COMPLETE and missing_dependency_artifacts:
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_COMPLETE_WITH_MISSING_DEPENDENCIES:artifact_id={artifact_text}"
        )
    if missing_dependency_artifacts and closure_state not in {
        CLOSURE_STATE_BLOCKED,
        CLOSURE_STATE_DEGRADED,
        CLOSURE_STATE_OPEN,
    }:
        raise ConstitutionalRuntimeError(
            "CONSTITUTIONAL_ARTIFACT_MISSING_DEPENDENCIES_WITHOUT_BLOCKED_STATE:"
            f"artifact_id={artifact_text}:closure_state={closure_state}"
        )
    if any(dep_id not in contract_required_deps for dep_id in missing_dependency_set):
        raise ConstitutionalRuntimeError(
            "CONSTITUTIONAL_ARTIFACT_UNKNOWN_MISSING_DEPENDENCY:"
            f"artifact_id={artifact_text}:missing={sorted(missing_dependency_set)}"
        )

    normalized_dependency_refs = _normalized_ref_rows(
        list(dependency_declaration.get("dependency_refs") or [])
    )
    dependency_ref_ids = [row["artifact_id"] for row in normalized_dependency_refs]
    expected_dependency_ref_ids = [
        dep_id for dep_id in contract_required_deps if dep_id not in missing_dependency_set
    ]
    if dependency_ref_ids != expected_dependency_ref_ids:
        raise ConstitutionalRuntimeError(
            "CONSTITUTIONAL_ARTIFACT_DEPENDENCY_REF_SET_MISMATCH:"
            f"artifact_id={artifact_text}:ref_ids={dependency_ref_ids}:expected={expected_dependency_ref_ids}"
        )
    for row in normalized_dependency_refs:
        ref_path = Path(row["path"]).resolve()
        if not ref_path.exists() or not ref_path.is_file():
            raise ConstitutionalRuntimeError(
                f"CONSTITUTIONAL_ARTIFACT_DEPENDENCY_REF_MISSING:artifact_id={artifact_text}:dependency_id={row['artifact_id']}:path={ref_path}"
            )
        expected_sha = str(row.get("sha256") or "").strip()
        if expected_sha and expected_sha not in _artifact_sha_candidates(ref_path):
            raise ConstitutionalRuntimeError(
                "CONSTITUTIONAL_ARTIFACT_DEPENDENCY_REF_SHA256_MISMATCH:"
                f"artifact_id={artifact_text}:dependency_id={row['artifact_id']}:path={ref_path}"
            )

    normalized_input_refs = _normalized_ref_rows(list(lineage.get("input_artifact_refs") or []))
    if normalized_input_refs != normalized_dependency_refs:
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_LINEAGE_INPUT_REFS_MISMATCH:artifact_id={artifact_text}"
        )
    if str(lineage.get("artifact_type") or "").strip() != artifact_text:
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_LINEAGE_TYPE_MISMATCH:artifact_id={artifact_text}"
        )
    if str(lineage.get("artifact_version") or "").strip() != str(payload.get("schema_version") or "").strip():
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_LINEAGE_VERSION_MISMATCH:artifact_id={artifact_text}"
        )
    if str(lineage.get("artifact_class") or "").strip() != contract_artifact_class:
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_LINEAGE_CLASS_MISMATCH:artifact_id={artifact_text}"
        )
    if str(lineage.get("authority_id") or "").strip() != artifact_text:
        raise ConstitutionalRuntimeError(
            f"CONSTITUTIONAL_ARTIFACT_LINEAGE_AUTHORITY_ID_MISMATCH:artifact_id={artifact_text}"
        )
    expected_writer = str(contract.get("authoritative_writer") or "").strip()
    producer_id = str(lineage.get("producer_id") or "").strip()
    if producer_id != expected_writer:
        raise ConstitutionalRuntimeError(
            "CONSTITUTIONAL_ARTIFACT_WRITER_MISMATCH:"
            f"artifact_id={artifact_text}:producer_id={producer_id}:expected={expected_writer}"
        )

    finality_state = str(lineage.get("finality_state") or "").strip()
    allowed_finality_states = {
        str(value).strip()
        for value in (contract.get("allowed_finality_states") or [])
        if str(value).strip()
    }
    if finality_state not in allowed_finality_states:
        raise ConstitutionalRuntimeError(
            "CONSTITUTIONAL_ARTIFACT_FINALITY_NOT_ALLOWED:"
            f"artifact_id={artifact_text}:finality_state={finality_state}"
        )
    if required_finality_states:
        required_set = {
            str(value).strip()
            for value in required_finality_states
            if str(value).strip()
        }
        if finality_state not in required_set:
            raise ConstitutionalRuntimeError(
                "CONSTITUTIONAL_ARTIFACT_FINALITY_INVALID:"
                f"artifact_id={artifact_text}:finality_state={finality_state}:required={sorted(required_set)}"
            )

    return {
        "artifact_id": artifact_text,
        "contract": contract,
        "constitutional_dependency_declaration": dict(dependency_declaration),
        "constitutional_lineage": dict(lineage),
    }


def assert_constitutional_completeness_v1(
    *,
    repo_root: Path | str,
    consumer_id: str,
    required_artifacts: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    validated: list[Dict[str, Any]] = []
    for row in required_artifacts:
        artifact_id = str(row.get("artifact_id") or "").strip()
        artifact_path = Path(str(row.get("path") or "").strip()).resolve()
        expected_sha256 = str(row.get("sha256") or "").strip()
        required_finality_states = list(row.get("required_finality_states") or [])
        if not artifact_id:
            raise ConstitutionalRuntimeError(
                "CONSTITUTIONAL_COMPLETENESS_ARTIFACT_ID_MISSING"
            )
        if not artifact_path.exists() or not artifact_path.is_file():
            raise ConstitutionalRuntimeError(
                f"CONSTITUTIONAL_COMPLETENESS_ARTIFACT_MISSING:artifact_id={artifact_id}:path={artifact_path}"
            )
        if expected_sha256 and expected_sha256 not in _artifact_sha_candidates(artifact_path):
            raise ConstitutionalRuntimeError(
                f"CONSTITUTIONAL_COMPLETENESS_ARTIFACT_SHA256_MISMATCH:artifact_id={artifact_id}:path={artifact_path}"
            )
        payload = _read_json_object(artifact_path)
        validated_row = validate_governed_artifact_payload_v1(
            repo_root=repo_root,
            artifact_id=artifact_id,
            payload=payload,
            consumer_id=consumer_id,
            required_finality_states=required_finality_states,
        )
        validated.append(
            {
                "artifact_id": artifact_id,
                "path": str(artifact_path),
                "sha256": expected_sha256 or _raw_sha256_file(artifact_path),
                "finality_state": str(
                    validated_row["constitutional_lineage"].get("finality_state") or ""
                ).strip(),
            }
        )
    return {"ok": True, "artifacts": validated}


def validate_read_model_payload_v1(payload: Mapping[str, Any]) -> Dict[str, Any]:
    surface_kind = str(payload.get("surface_kind") or "").strip()
    errors: list[str] = []
    if surface_kind not in READ_MODEL_SURFACE_KINDS:
        errors.append("read_model surface_kind must be projection or composition")
    for field in READ_MODEL_FORBIDDEN_AUTHORITY_FIELDS:
        if field in payload:
            errors.append(f"read_model payload must not declare {field}")
    artifact_class = str(payload.get("artifact_class") or "").strip()
    if artifact_class and artifact_class != ARTIFACT_CLASS_READ_MODEL:
        errors.append("read_model payload must not claim governed artifact_class")
    finality_state = str(payload.get("finality_state") or "").strip()
    if finality_state:
        errors.append("read_model payload must not claim governed finality_state")
    return {"ok": not errors, "errors": errors}
