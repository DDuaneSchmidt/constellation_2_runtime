from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence

from constellation_2.common.constitutional_runtime_v1 import (
    FINALITY_FINALIZED,
    FINALITY_PROVISIONAL,
    build_governed_dependency_ref_v1,
    resolve_constitutional_artifact_path_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    read_validated_surface_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_semantic_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]

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
FROZEN_INPUT_BUNDLE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/RUNTIME/frozen_decision_input_bundle.v1.schema.json"


class ConfigurationActivationFamilyValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class ConfigurationActivationFamilyRefsV1:
    truth_root: Path
    current_path: Path
    configuration_state_ref: SurfaceRefV1
    policy_snapshot_ref: SurfaceRefV1
    validation_result_ref: SurfaceRefV1
    compiled_active_config_ref: SurfaceRefV1
    compile_result_ref: SurfaceRefV1
    review_diff_ref: SurfaceRefV1
    activation_transaction_ref: SurfaceRefV1


def _artifact_summary(ref: SurfaceRefV1) -> Dict[str, str]:
    return {"path": str(ref.path), "sha256": ref.sha256}


def _governed_ref_from_surface(
    artifact_id: str,
    ref: SurfaceRefV1,
    *,
    finality_state: str = FINALITY_FINALIZED,
) -> Dict[str, Any]:
    return build_governed_dependency_ref_v1(
        repo_root=REPO_ROOT,
        artifact_id=artifact_id,
        path=ref.path,
        sha256=ref.sha256,
        finality_state=finality_state,
    )


def _load_surface(
    *,
    path: Path,
    schema_relpath: str,
    artifact_id: str,
    required_finality_states: Sequence[str],
) -> SurfaceRefV1:
    ref = read_validated_surface_v1(path=path, schema_relpath=schema_relpath)
    validate_governed_artifact_payload_v1(
        repo_root=REPO_ROOT,
        artifact_id=artifact_id,
        payload=ref.payload,
        required_finality_states=required_finality_states,
    )
    return ref


def _ref_row_path(row: Mapping[str, Any]) -> Path:
    return Path(str(row.get("path") or "")).resolve()


def _compare_artifact_ref(
    *,
    errors: list[str],
    label: str,
    row: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> None:
    for key in ("path", "sha256", "artifact_class"):
        left = str(row.get(key) or "").strip()
        right = str(expected.get(key) or "").strip()
        if left != right:
            errors.append(f"{label}_{key.upper()}_MISMATCH")


def _optional_ref_payload(
    row: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if not isinstance(row, Mapping):
        return None
    path_text = str(row.get("path") or "").strip()
    return row if path_text else None


def validate_configuration_activation_family_v1(
    *,
    truth_root: str | Path | None = None,
) -> Dict[str, Any]:
    canonical_truth_root = resolve_fact_plane_truth_root_v1(truth_root)
    semantic_view = read_control_plane_semantic_v1(
        domain="policy",
        surface="configuration_activation_family",
        truth_root=canonical_truth_root,
    )
    artifact_rows = dict(semantic_view.payload.get("artifacts") or {})
    current_path = Path(str(artifact_rows.get("configuration_state_v1", {}).get("path") or "")).resolve()
    report: Dict[str, Any] = {
        "ok": False,
        "validator_id": "configuration_activation_family_validator_v1",
        "truth_root": str(canonical_truth_root),
        "current_path": str(current_path),
        "errors": [],
        "artifacts": {},
    }
    errors: list[str] = []
    if not str(current_path):
        report["errors"] = ["CONFIGURATION_STATE_CURRENT_MISSING"]
        return report
    if not current_path.exists() or not current_path.is_file():
        report["errors"] = ["CONFIGURATION_STATE_CURRENT_MISSING"]
        return report

    try:
        configuration_state_ref = _load_surface(
            path=current_path,
            schema_relpath=CONFIGURATION_STATE_SCHEMA,
            artifact_id=CONFIGURATION_STATE_ARTIFACT_ID,
            required_finality_states=[FINALITY_PROVISIONAL, FINALITY_FINALIZED],
        )
    except Exception as exc:
        report["errors"] = [f"CONFIGURATION_STATE_INVALID:{type(exc).__name__}"]
        return report

    try:
        policy_snapshot_ref = _load_surface(
            path=Path(str(artifact_rows.get("configuration_policy_snapshot", {}).get("path") or "")).resolve(),
            schema_relpath=POLICY_SNAPSHOT_SCHEMA,
            artifact_id=POLICY_SNAPSHOT_ARTIFACT_ID,
            required_finality_states=[FINALITY_FINALIZED],
        )
        validation_result_ref = _load_surface(
            path=Path(str(artifact_rows.get("configuration_validation_result", {}).get("path") or "")).resolve(),
            schema_relpath=VALIDATION_RESULT_SCHEMA,
            artifact_id=VALIDATION_RESULT_ARTIFACT_ID,
            required_finality_states=[FINALITY_FINALIZED],
        )
        compiled_active_config_ref = _load_surface(
            path=Path(str(artifact_rows.get("compiled_active_config", {}).get("path") or "")).resolve(),
            schema_relpath=COMPILED_ACTIVE_CONFIG_SCHEMA,
            artifact_id=COMPILED_ACTIVE_CONFIG_ARTIFACT_ID,
            required_finality_states=[FINALITY_FINALIZED],
        )
        compile_result_ref = _load_surface(
            path=Path(str(artifact_rows.get("configuration_compile_result", {}).get("path") or "")).resolve(),
            schema_relpath=COMPILE_RESULT_SCHEMA,
            artifact_id=COMPILE_RESULT_ARTIFACT_ID,
            required_finality_states=[FINALITY_FINALIZED],
        )
        review_diff_ref = _load_surface(
            path=Path(str(artifact_rows.get("configuration_review_diff", {}).get("path") or "")).resolve(),
            schema_relpath=REVIEW_DIFF_SCHEMA,
            artifact_id=REVIEW_DIFF_ARTIFACT_ID,
            required_finality_states=[FINALITY_FINALIZED],
        )
        activation_transaction_ref = _load_surface(
            path=Path(str(artifact_rows.get("configuration_activation_transaction", {}).get("path") or "")).resolve(),
            schema_relpath=ACTIVATION_TRANSACTION_SCHEMA,
            artifact_id=ACTIVATION_TRANSACTION_ARTIFACT_ID,
            required_finality_states=[FINALITY_FINALIZED],
        )
    except Exception as exc:
        report["errors"] = [f"CONFIGURATION_CHAIN_READ_INVALID:{type(exc).__name__}"]
        return report

    expected_current_path = str(current_path.resolve())
    if str(configuration_state_ref.path) != expected_current_path:
        errors.append("CONFIGURATION_STATE_PATH_NOT_CURRENT")
    if str(configuration_state_ref.payload.get("status") or "").strip().upper() != "ACTIVE":
        errors.append("CONFIGURATION_STATE_STATUS_NOT_ACTIVE")
    if str(activation_transaction_ref.payload.get("activation_status") or "").strip().upper() != "PROMOTED":
        errors.append("ACTIVATION_TRANSACTION_NOT_PROMOTED")
    if str(compiled_active_config_ref.payload.get("compile_status") or "").strip().upper() != "COMPILED":
        errors.append("COMPILED_ACTIVE_CONFIG_STATUS_NOT_COMPILED")
    if str(compile_result_ref.payload.get("compile_status") or "").strip().upper() != "COMPILED":
        errors.append("COMPILE_RESULT_STATUS_NOT_COMPILED")
    if str(review_diff_ref.payload.get("review_status") or "").strip().upper() != "REVIEW_READY":
        errors.append("REVIEW_DIFF_STATUS_NOT_REVIEW_READY")

    _compare_artifact_ref(
        errors=errors,
        label="STATE_POLICY_REF",
        row=configuration_state_ref.payload["configuration_policy_snapshot_ref"],
        expected=_governed_ref_from_surface(POLICY_SNAPSHOT_ARTIFACT_ID, policy_snapshot_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="STATE_VALIDATION_REF",
        row=configuration_state_ref.payload["configuration_validation_result_ref"],
        expected=_governed_ref_from_surface(VALIDATION_RESULT_ARTIFACT_ID, validation_result_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="STATE_COMPILE_REF",
        row=configuration_state_ref.payload["configuration_compile_result_ref"],
        expected=_governed_ref_from_surface(COMPILE_RESULT_ARTIFACT_ID, compile_result_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="STATE_REVIEW_REF",
        row=configuration_state_ref.payload["configuration_review_diff_ref"],
        expected=_governed_ref_from_surface(REVIEW_DIFF_ARTIFACT_ID, review_diff_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="STATE_COMPILED_REF",
        row=configuration_state_ref.payload["compiled_active_config_ref"],
        expected=_governed_ref_from_surface(COMPILED_ACTIVE_CONFIG_ARTIFACT_ID, compiled_active_config_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="STATE_ACTIVATION_REF",
        row=configuration_state_ref.payload["configuration_activation_transaction_ref"],
        expected=_governed_ref_from_surface(ACTIVATION_TRANSACTION_ARTIFACT_ID, activation_transaction_ref),
    )

    if str(configuration_state_ref.payload.get("active_compiled_config_sha256") or "").strip() != compiled_active_config_ref.sha256:
        errors.append("STATE_ACTIVE_COMPILED_SHA256_MISMATCH")

    _compare_artifact_ref(
        errors=errors,
        label="COMPILED_POLICY_REF",
        row=compiled_active_config_ref.payload["policy_snapshot_ref"],
        expected=_governed_ref_from_surface(POLICY_SNAPSHOT_ARTIFACT_ID, policy_snapshot_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="COMPILED_VALIDATION_REF",
        row=compiled_active_config_ref.payload["validation_result_ref"],
        expected=_governed_ref_from_surface(VALIDATION_RESULT_ARTIFACT_ID, validation_result_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="VALIDATION_POLICY_REF",
        row=validation_result_ref.payload["policy_snapshot_ref"],
        expected=_governed_ref_from_surface(POLICY_SNAPSHOT_ARTIFACT_ID, policy_snapshot_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="COMPILE_POLICY_REF",
        row=compile_result_ref.payload["policy_snapshot_ref"],
        expected=_governed_ref_from_surface(POLICY_SNAPSHOT_ARTIFACT_ID, policy_snapshot_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="COMPILE_VALIDATION_REF",
        row=compile_result_ref.payload["validation_result_ref"],
        expected=_governed_ref_from_surface(VALIDATION_RESULT_ARTIFACT_ID, validation_result_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="COMPILE_COMPILED_REF",
        row=compile_result_ref.payload["compiled_active_config_ref"],
        expected=_governed_ref_from_surface(COMPILED_ACTIVE_CONFIG_ARTIFACT_ID, compiled_active_config_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="REVIEW_COMPILED_REF",
        row=review_diff_ref.payload["candidate_compiled_active_config_ref"],
        expected=_governed_ref_from_surface(COMPILED_ACTIVE_CONFIG_ARTIFACT_ID, compiled_active_config_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="ACTIVATION_POLICY_REF",
        row=activation_transaction_ref.payload["configuration_policy_snapshot_ref"],
        expected=_governed_ref_from_surface(POLICY_SNAPSHOT_ARTIFACT_ID, policy_snapshot_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="ACTIVATION_VALIDATION_REF",
        row=activation_transaction_ref.payload["configuration_validation_result_ref"],
        expected=_governed_ref_from_surface(VALIDATION_RESULT_ARTIFACT_ID, validation_result_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="ACTIVATION_COMPILE_REF",
        row=activation_transaction_ref.payload["configuration_compile_result_ref"],
        expected=_governed_ref_from_surface(COMPILE_RESULT_ARTIFACT_ID, compile_result_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="ACTIVATION_REVIEW_REF",
        row=activation_transaction_ref.payload["configuration_review_diff_ref"],
        expected=_governed_ref_from_surface(REVIEW_DIFF_ARTIFACT_ID, review_diff_ref),
    )
    _compare_artifact_ref(
        errors=errors,
        label="ACTIVATION_COMPILED_REF",
        row=activation_transaction_ref.payload["compiled_active_config_ref"],
        expected=_governed_ref_from_surface(COMPILED_ACTIVE_CONFIG_ARTIFACT_ID, compiled_active_config_ref),
    )

    try:
        validate_against_repo_schema_v1(
            dict(activation_transaction_ref.payload["frozen_input_bundle"]),
            REPO_ROOT,
            FROZEN_INPUT_BUNDLE_SCHEMA,
        )
    except Exception:
        errors.append("ACTIVATION_FROZEN_INPUT_BUNDLE_INVALID")
    else:
        frozen_input_bundle = activation_transaction_ref.payload["frozen_input_bundle"]
        expected_bundle_ref_ids = [row["artifact_id"] for row in activation_transaction_ref.payload["constitutional_dependency_declaration"]["dependency_refs"]]
        actual_bundle_ref_ids = [str(row.get("artifact_id") or "").strip() for row in (frozen_input_bundle.get("input_artifact_refs") or [])]
        if actual_bundle_ref_ids != expected_bundle_ref_ids:
            errors.append("ACTIVATION_FROZEN_INPUT_BUNDLE_REF_SET_MISMATCH")
        bundle_policy_refs = frozen_input_bundle.get("policy_snapshot_refs") or []
        if len(bundle_policy_refs) != 1:
            errors.append("ACTIVATION_FROZEN_INPUT_BUNDLE_POLICY_REF_MISSING")
        else:
            _compare_artifact_ref(
                errors=errors,
                label="ACTIVATION_FROZEN_POLICY_REF",
                row=bundle_policy_refs[0],
                expected=_governed_ref_from_surface(POLICY_SNAPSHOT_ARTIFACT_ID, policy_snapshot_ref),
            )

    review_kind = str(review_diff_ref.payload.get("review_kind") or "").strip().upper()
    activation_kind = str(activation_transaction_ref.payload.get("activation_kind") or "").strip().upper()
    current_kind = str(configuration_state_ref.payload.get("activation_kind") or "").strip().upper()
    if review_kind != activation_kind or activation_kind != current_kind:
        errors.append("ACTIVATION_KIND_MISMATCH")

    prior_state_row = _optional_ref_payload(configuration_state_ref.payload.get("prior_configuration_state_ref"))
    review_prior_state_row = _optional_ref_payload(review_diff_ref.payload.get("prior_configuration_state_ref"))
    activation_prior_state_row = _optional_ref_payload(activation_transaction_ref.payload.get("prior_configuration_state_ref"))
    if review_kind == "FIRST_ACTIVATION":
        if prior_state_row or review_prior_state_row or activation_prior_state_row:
            errors.append("FIRST_ACTIVATION_HAS_PRIOR_STATE")
    elif review_kind == "SUPERSEDING_ACTIVATION":
        if not (prior_state_row and review_prior_state_row and activation_prior_state_row):
            errors.append("SUPERSEDING_ACTIVATION_MISSING_PRIOR_STATE")
        else:
            try:
                prior_state_ref = _load_surface(
                    path=_ref_row_path(prior_state_row),
                    schema_relpath=CONFIGURATION_STATE_SCHEMA,
                    artifact_id=CONFIGURATION_STATE_ARTIFACT_ID,
                    required_finality_states=[FINALITY_PROVISIONAL, FINALITY_FINALIZED],
                )
                expected_prior_state = _governed_ref_from_surface(
                    CONFIGURATION_STATE_ARTIFACT_ID,
                    prior_state_ref,
                    finality_state=FINALITY_PROVISIONAL,
                )
                _compare_artifact_ref(errors=errors, label="STATE_PRIOR_STATE_REF", row=prior_state_row, expected=expected_prior_state)
                _compare_artifact_ref(errors=errors, label="REVIEW_PRIOR_STATE_REF", row=review_prior_state_row, expected=expected_prior_state)
                _compare_artifact_ref(errors=errors, label="ACTIVATION_PRIOR_STATE_REF", row=activation_prior_state_row, expected=expected_prior_state)
            except Exception:
                errors.append("SUPERSEDING_ACTIVATION_PRIOR_STATE_INVALID")

        review_prior_compiled_row = _optional_ref_payload(review_diff_ref.payload.get("prior_compiled_active_config_ref"))
        if review_prior_compiled_row is None:
            errors.append("SUPERSEDING_ACTIVATION_MISSING_PRIOR_COMPILED")
        else:
            try:
                prior_compiled_ref = _load_surface(
                    path=_ref_row_path(review_prior_compiled_row),
                    schema_relpath=COMPILED_ACTIVE_CONFIG_SCHEMA,
                    artifact_id=COMPILED_ACTIVE_CONFIG_ARTIFACT_ID,
                    required_finality_states=[FINALITY_FINALIZED],
                )
                _compare_artifact_ref(
                    errors=errors,
                    label="REVIEW_PRIOR_COMPILED_REF",
                    row=review_prior_compiled_row,
                    expected=_governed_ref_from_surface(COMPILED_ACTIVE_CONFIG_ARTIFACT_ID, prior_compiled_ref),
                )
            except Exception:
                errors.append("SUPERSEDING_ACTIVATION_PRIOR_COMPILED_INVALID")

    report["artifacts"] = {
        "configuration_state_v1": _artifact_summary(configuration_state_ref),
        "configuration_policy_snapshot_v1": _artifact_summary(policy_snapshot_ref),
        "configuration_validation_result_v1": _artifact_summary(validation_result_ref),
        "compiled_active_config_v1": _artifact_summary(compiled_active_config_ref),
        "configuration_compile_result_v1": _artifact_summary(compile_result_ref),
        "configuration_review_diff_v1": _artifact_summary(review_diff_ref),
        "configuration_activation_transaction_v1": _artifact_summary(activation_transaction_ref),
    }
    report["errors"] = errors
    report["ok"] = not errors
    return report


def load_validated_configuration_activation_family_v1(
    *,
    truth_root: str | Path | None = None,
) -> ConfigurationActivationFamilyRefsV1:
    report = validate_configuration_activation_family_v1(truth_root=truth_root)
    if not bool(report.get("ok")):
        raise ConfigurationActivationFamilyValidationError("|".join(str(item) for item in (report.get("errors") or [])))
    canonical_truth_root = resolve_fact_plane_truth_root_v1(truth_root)
    current_path = Path(str(report["artifacts"]["configuration_state_v1"]["path"])).resolve()
    configuration_state_ref = _load_surface(
        path=current_path,
        schema_relpath=CONFIGURATION_STATE_SCHEMA,
        artifact_id=CONFIGURATION_STATE_ARTIFACT_ID,
        required_finality_states=[FINALITY_PROVISIONAL, FINALITY_FINALIZED],
    )
    policy_snapshot_ref = _load_surface(
        path=Path(str(report["artifacts"]["configuration_policy_snapshot_v1"]["path"])).resolve(),
        schema_relpath=POLICY_SNAPSHOT_SCHEMA,
        artifact_id=POLICY_SNAPSHOT_ARTIFACT_ID,
        required_finality_states=[FINALITY_FINALIZED],
    )
    validation_result_ref = _load_surface(
        path=Path(str(report["artifacts"]["configuration_validation_result_v1"]["path"])).resolve(),
        schema_relpath=VALIDATION_RESULT_SCHEMA,
        artifact_id=VALIDATION_RESULT_ARTIFACT_ID,
        required_finality_states=[FINALITY_FINALIZED],
    )
    compiled_active_config_ref = _load_surface(
        path=Path(str(report["artifacts"]["compiled_active_config_v1"]["path"])).resolve(),
        schema_relpath=COMPILED_ACTIVE_CONFIG_SCHEMA,
        artifact_id=COMPILED_ACTIVE_CONFIG_ARTIFACT_ID,
        required_finality_states=[FINALITY_FINALIZED],
    )
    compile_result_ref = _load_surface(
        path=Path(str(report["artifacts"]["configuration_compile_result_v1"]["path"])).resolve(),
        schema_relpath=COMPILE_RESULT_SCHEMA,
        artifact_id=COMPILE_RESULT_ARTIFACT_ID,
        required_finality_states=[FINALITY_FINALIZED],
    )
    review_diff_ref = _load_surface(
        path=Path(str(report["artifacts"]["configuration_review_diff_v1"]["path"])).resolve(),
        schema_relpath=REVIEW_DIFF_SCHEMA,
        artifact_id=REVIEW_DIFF_ARTIFACT_ID,
        required_finality_states=[FINALITY_FINALIZED],
    )
    activation_transaction_ref = _load_surface(
        path=Path(str(report["artifacts"]["configuration_activation_transaction_v1"]["path"])).resolve(),
        schema_relpath=ACTIVATION_TRANSACTION_SCHEMA,
        artifact_id=ACTIVATION_TRANSACTION_ARTIFACT_ID,
        required_finality_states=[FINALITY_FINALIZED],
    )
    return ConfigurationActivationFamilyRefsV1(
        truth_root=canonical_truth_root,
        current_path=current_path,
        configuration_state_ref=configuration_state_ref,
        policy_snapshot_ref=policy_snapshot_ref,
        validation_result_ref=validation_result_ref,
        compiled_active_config_ref=compiled_active_config_ref,
        compile_result_ref=compile_result_ref,
        review_diff_ref=review_diff_ref,
        activation_transaction_ref=activation_transaction_ref,
    )
