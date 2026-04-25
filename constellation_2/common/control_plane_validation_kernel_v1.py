from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping

from constellation_2.common.day_activation_authority_v1 import (
    compute_day_activation_context_hash_v1,
)
from constellation_2.common.global_context_authority_v1 import (
    compute_global_context_hash_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    read_validated_surface_v1,
)
from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1
from constellation_2.common.runtime_contract_v1 import (
    resolve_canonical_truth_root,
    resolve_truth_sleeves_root,
)
from constellation_2.common.session_authority_v1 import (
    resolve_active_session_path,
    resolve_target_day_admission_path,
    resolve_target_day_build_path,
)
from constellation_2.common.session_promotion_gate_v1 import (
    resolve_session_promotion_decision_path_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import (
    validate_against_repo_schema_v1,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
VALIDATOR_VERSION = "control_plane_validation_kernel_v1"
LEGACY_RUNTIME_ROOT = Path("/home/node/constellation_2_runtime").resolve()

DAY_ACTIVATION_BUILD_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/day_activation_build.v1.schema.json"
DAY_ACTIVATION_PACKAGE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/CONTEXT/day_activation_package.v1.schema.json"
GLOBAL_CONTEXT_BUILD_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/global_context_build.v1.schema.json"
GLOBAL_CONTEXT_PACKAGE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/CONTEXT/global_context_package.v1.schema.json"
TARGET_DAY_BUILD_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_build.v1.schema.json"
TARGET_DAY_ADMISSION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_admission.v1.schema.json"
ACTIVE_SESSION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/active_session.v1.schema.json"
SESSION_PROMOTION_DECISION_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/session_promotion_decision.v1.schema.json"
EXECUTION_BUILD_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/execution_build.v1.schema.json"
EXECUTION_PACKAGE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXECUTION/execution_package.v1.schema.json"


@dataclass(frozen=True)
class ControlPlaneScopeV1:
    repo_root: Path
    canonical_truth_root: Path
    truth_sleeves_root: Path
    execution_truth_root: Path
    day_utc: str
    sleeve_id: str
    environment: str
    ib_account: str
    operation_type: str
    context_hash: str
    scope_id: str
    candidate_path: Path | None = None
    submission_id: str = ""


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _forbidden_roots(repo_root: Path) -> tuple[Path, ...]:
    return (
        (repo_root / "constellation_2" / "runtime" / "truth").resolve(),
        (repo_root / "constellation_2" / "runtime" / "truth_sleeves").resolve(),
        LEGACY_RUNTIME_ROOT,
    )


def _derived_surface_paths(scope: ControlPlaneScopeV1) -> dict[str, Path]:
    return {
        "session_authority_status_v1": (scope.canonical_truth_root / "session_authority_status_v1" / "current.json").resolve(),
        "session_authority_alert_v1": (scope.canonical_truth_root / "session_authority_alert_v1" / "current.json").resolve(),
        "startup_proof_validation_v1": (
            scope.canonical_truth_root
            / "reports"
            / "startup_proof_validation_v1"
            / scope.day_utc
            / "startup_proof_validation.v1.json"
        ).resolve(),
    }


def _normalized_ref_path(row: Mapping[str, Any] | None) -> Path | None:
    if not isinstance(row, Mapping):
        return None
    raw = str(row.get("path") or row.get("artifact_path") or "").strip()
    if not raw:
        return None
    return Path(raw).resolve()


def _normalized_ref_sha(row: Mapping[str, Any] | None) -> str:
    if not isinstance(row, Mapping):
        return ""
    return str(row.get("sha256") or row.get("artifact_sha256") or "").strip()


def _unique_sorted(values: list[str]) -> list[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _new_report(validator_id: str, scope: ControlPlaneScopeV1 | None) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "validator_id": validator_id,
        "validator_version": VALIDATOR_VERSION,
        "ok": False,
        "errors": [],
        "warnings": [],
        "invariants_checked": [],
        "validated_artifacts": [],
        "validated_current_surfaces": [],
        "forbidden_path_hits": [],
    }
    if scope is not None:
        report["target_day"] = scope.day_utc
        report["scope_id"] = scope.scope_id
        report["operation_type"] = scope.operation_type
        report["sleeve_id"] = scope.sleeve_id
        report["environment"] = scope.environment
        report["ib_account"] = scope.ib_account
        report["canonical_truth_root"] = str(scope.canonical_truth_root)
        report["execution_truth_root"] = str(scope.execution_truth_root)
    return report


def _push_unique(report: Dict[str, Any], field: str, value: str) -> None:
    normalized = str(value).strip()
    if not normalized:
        return
    bucket = report.setdefault(field, [])
    if normalized not in bucket:
        bucket.append(normalized)


def _record_invariant(report: Dict[str, Any], invariant_id: str) -> None:
    _push_unique(report, "invariants_checked", invariant_id)


def _record_artifact(
    report: Dict[str, Any],
    *,
    artifact_id: str,
    path: Path,
    sha256: str,
    surface_class: str,
    root_type: str,
    is_current_surface: bool = False,
) -> None:
    row = {
        "artifact_id": artifact_id,
        "path": str(path.resolve()),
        "sha256": str(sha256).strip(),
        "surface_class": surface_class,
        "root_type": root_type,
    }
    key = "validated_current_surfaces" if is_current_surface else "validated_artifacts"
    report.setdefault(key, []).append(row)


def _check_path_boundary(
    report: Dict[str, Any],
    *,
    scope: ControlPlaneScopeV1,
    label: str,
    path: Path,
    expected_root_type: str,
    reject_derived: bool,
) -> None:
    resolved = path.resolve()
    for forbidden_root in _forbidden_roots(scope.repo_root):
        if resolved == forbidden_root or _is_under(resolved, forbidden_root):
            _push_unique(report, "errors", f"{label}_FORBIDDEN_ROOT")
            _push_unique(report, "forbidden_path_hits", str(resolved))
            return
    if reject_derived:
        for _, derived_path in _derived_surface_paths(scope).items():
            if resolved == derived_path:
                _push_unique(report, "errors", f"{label}_DERIVED_SURFACE_FORBIDDEN")
                _push_unique(report, "forbidden_path_hits", str(resolved))
                return
    expected_root = scope.canonical_truth_root if expected_root_type == "canonical_truth_root" else scope.execution_truth_root
    if not (resolved == expected_root or _is_under(resolved, expected_root)):
        _push_unique(report, "errors", f"{label}_OUTSIDE_{expected_root_type.upper()}")


def _load_surface(
    report: Dict[str, Any],
    *,
    scope: ControlPlaneScopeV1,
    artifact_id: str,
    path: Path,
    schema_relpath: str,
    surface_class: str,
    root_type: str,
    reject_derived: bool = False,
    is_current_surface: bool = False,
) -> SurfaceRefV1 | None:
    _check_path_boundary(
        report,
        scope=scope,
        label=artifact_id.upper(),
        path=path,
        expected_root_type=root_type,
        reject_derived=reject_derived,
    )
    if not path.exists() or not path.is_file():
        _push_unique(report, "errors", f"{artifact_id.upper()}_MISSING")
        return None
    try:
        ref = read_validated_surface_v1(path=path, schema_relpath=schema_relpath)
    except Exception:
        _push_unique(report, "errors", f"{artifact_id.upper()}_SCHEMA_INVALID")
        return None
    _record_artifact(
        report,
        artifact_id=artifact_id,
        path=ref.path,
        sha256=ref.sha256,
        surface_class=surface_class,
        root_type=root_type,
        is_current_surface=is_current_surface,
    )
    return ref


def _load_gateway_surface(
    report: Dict[str, Any],
    *,
    scope: ControlPlaneScopeV1,
    artifact_id: str,
    domain: str,
    surface: str,
    surface_class: str,
    root_type: str,
    reject_derived: bool = False,
    is_current_surface: bool = False,
    context_hash: str = "",
    submission_id: str = "",
) -> SurfaceRefV1 | None:
    try:
        ref = read_control_plane_surface_v1(
            domain=domain,
            surface=surface,
            truth_root=scope.canonical_truth_root,
            truth_sleeves_root=scope.truth_sleeves_root,
            day_utc=scope.day_utc,
            sleeve_id=scope.sleeve_id,
            environment=scope.environment,
            ib_account=scope.ib_account,
            context_hash=context_hash,
            submission_id=submission_id,
        )
    except Exception:
        _push_unique(report, "errors", f"{artifact_id.upper()}_MISSING")
        return None
    _check_path_boundary(
        report,
        scope=scope,
        label=artifact_id.upper(),
        path=ref.path,
        expected_root_type=root_type,
        reject_derived=reject_derived,
    )
    _record_artifact(
        report,
        artifact_id=artifact_id,
        path=ref.path,
        sha256=ref.sha256,
        surface_class=surface_class,
        root_type=root_type,
        is_current_surface=is_current_surface,
    )
    return ref


def _candidate_submission_id(candidate_path: Path) -> str:
    for filename in ("binding_record.v2.json", "binding_record.v1.json"):
        path = (candidate_path / filename).resolve()
        if not path.exists():
            continue
        obj = json.loads(path.read_text(encoding="utf-8"))
        submission_id = str(obj.get("submission_id") or "").strip()
        if submission_id:
            return submission_id
    identity_path = (candidate_path / "execution_identity_record.v1.json").resolve()
    if identity_path.exists():
        obj = json.loads(identity_path.read_text(encoding="utf-8"))
        submission_id = str(obj.get("submission_id") or "").strip()
        if submission_id:
            return submission_id
    raise ValueError("EXECUTION_BUILD_SUBMISSION_ID_MISSING")


def _build_scope(
    *,
    repo_root: Path | str = REPO_ROOT,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str,
    candidate_path: Path | str | None = None,
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
) -> ControlPlaneScopeV1:
    repo_root_path = Path(repo_root).resolve()
    canonical_root = Path(canonical_truth_root).resolve() if canonical_truth_root is not None else resolve_canonical_truth_root().resolve()
    sleeves_root = Path(truth_sleeves_root).resolve() if truth_sleeves_root is not None else resolve_truth_sleeves_root().resolve()
    day_text = str(day_utc).strip()
    sleeve_text = str(sleeve_id).strip().upper()
    env_text = str(environment).strip().upper()
    account_text = str(ib_account).strip().upper()
    op_text = str(operation_type).strip()
    context_hash = compute_global_context_hash_v1(
        day_utc=day_text,
        sleeve_id=sleeve_text,
        environment=env_text,
        ib_account=account_text,
        operation_type=op_text,
    )
    execution_root = (sleeves_root / sleeve_text / env_text).resolve()
    submission_id = ""
    scope_id = context_hash
    candidate = None
    if candidate_path is not None and str(candidate_path).strip():
        candidate = Path(candidate_path).resolve()
        rel = candidate.relative_to(sleeves_root)
        if len(rel.parts) < 6 or rel.parts[0].upper() != sleeve_text or rel.parts[1].upper() != env_text:
            raise ValueError("EXECUTION_BUILD_CANDIDATE_PATH_OUTSIDE_SCOPE")
        if rel.parts[2] != "phaseC_preflight_v1" or rel.parts[3] != day_text:
            raise ValueError("EXECUTION_BUILD_CANDIDATE_PATH_INVALID")
        submission_id = _candidate_submission_id(candidate)
        scope_id = submission_id
    return ControlPlaneScopeV1(
        repo_root=repo_root_path,
        canonical_truth_root=canonical_root,
        truth_sleeves_root=sleeves_root,
        execution_truth_root=execution_root,
        day_utc=day_text,
        sleeve_id=sleeve_text,
        environment=env_text,
        ib_account=account_text,
        operation_type=op_text,
        context_hash=context_hash,
        scope_id=scope_id,
        candidate_path=candidate,
        submission_id=submission_id,
    )


def _finalize_report(report: Dict[str, Any]) -> Dict[str, Any]:
    report["errors"] = _unique_sorted(list(report.get("errors") or []))
    report["warnings"] = _unique_sorted(list(report.get("warnings") or []))
    report["invariants_checked"] = _unique_sorted(list(report.get("invariants_checked") or []))
    report["forbidden_path_hits"] = _unique_sorted(list(report.get("forbidden_path_hits") or []))
    report["ok"] = not report["errors"]
    return report


def _plain_ref_row(*, artifact_id: str, path: Path, sha256: str, root_type: str) -> Dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "path": str(path.resolve()),
        "sha256": str(sha256).strip(),
        "root_type": root_type,
    }


def validate_day_activation_family_v1(
    *,
    repo_root: Path | str = REPO_ROOT,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
) -> Dict[str, Any]:
    try:
        scope = _build_scope(
            repo_root=repo_root,
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            canonical_truth_root=canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root,
        )
    except Exception:
        report = _new_report("day_activation_family_validator_v1", None)
        _push_unique(report, "errors", "CONTROL_PLANE_SCOPE_INVALID")
        return _finalize_report(report)

    report = _new_report("day_activation_family_validator_v1", scope)
    build_ref = _load_gateway_surface(
        report,
        scope=scope,
        artifact_id="day_activation_build_v1",
        domain="session",
        surface="day_activation_build",
        surface_class="authoritative_immutable_artifact",
        root_type="canonical_truth_root",
        context_hash=scope.context_hash,
    )
    package_ref = _load_gateway_surface(
        report,
        scope=scope,
        artifact_id="day_activation_package_v1",
        domain="session",
        surface="day_activation_package",
        surface_class="authoritative_immutable_artifact",
        root_type="execution_truth_root",
        context_hash=scope.context_hash,
    )
    if build_ref is not None:
        _record_invariant(report, "DAY_BUILD_CONTEXT_MATCH")
        if any(
            str(build_ref.payload.get(field) or "").strip() != expected
            for field, expected in (
                ("day_utc", scope.day_utc),
                ("sleeve_id", scope.sleeve_id),
                ("mode", scope.environment),
                ("account_id", scope.ib_account),
                ("operation_type", scope.operation_type),
                ("context_hash", scope.context_hash),
            )
        ):
            _push_unique(report, "errors", "DAY_BUILD_CONTEXT_MISMATCH")
        _record_invariant(report, "DAY_BUILD_COMPLETE")
        if str(build_ref.payload.get("closure_status") or "").strip().upper() != "COMPLETE":
            _push_unique(report, "errors", "DAY_BUILD_NOT_COMPLETE")
        for row in build_ref.payload.get("dependency_results") or []:
            dep_path = _normalized_ref_path({"path": row.get("path")})
            if dep_path is not None:
                _check_path_boundary(
                    report,
                    scope=scope,
                    label="DAY_BUILD_DEPENDENCY",
                    path=dep_path,
                    expected_root_type=(
                        "canonical_truth_root"
                        if dep_path == scope.canonical_truth_root or _is_under(dep_path, scope.canonical_truth_root)
                        else "execution_truth_root"
                    ),
                    reject_derived=False,
                )
    if package_ref is not None:
        _record_invariant(report, "DAY_PACKAGE_CONTEXT_MATCH")
        if any(
            str(package_ref.payload.get(field) or "").strip() != expected
            for field, expected in (
                ("day_utc", scope.day_utc),
                ("sleeve_id", scope.sleeve_id),
                ("mode", scope.environment),
                ("account_id", scope.ib_account),
                ("operation_type", scope.operation_type),
                ("context_hash", scope.context_hash),
            )
        ):
            _push_unique(report, "errors", "DAY_PACKAGE_CONTEXT_MISMATCH")
        _record_invariant(report, "DAY_PACKAGE_SEALED")
        if package_ref.payload.get("sealed") is not True:
            _push_unique(report, "errors", "DAY_PACKAGE_NOT_SEALED")
        build_ref_path = _normalized_ref_path(package_ref.payload.get("build_ref"))
        build_ref_sha = _normalized_ref_sha(package_ref.payload.get("build_ref"))
        _record_invariant(report, "DAY_PACKAGE_BUILD_REF_MATCH")
        if build_ref is None or build_ref_path != build_ref.path or build_ref_sha != build_ref.sha256:
            _push_unique(report, "errors", "DAY_PACKAGE_BUILD_REF_MISMATCH")
        dependency_ids = {
            str((row or {}).get("dependency_id") or "").strip()
            for row in package_ref.payload.get("dependency_refs") or []
            if isinstance(row, Mapping)
        }
        _record_invariant(report, "DAY_PACKAGE_REQUIRED_REFS_PRESENT")
        required_ids = {
            "target_day_admission_v1",
            "canonical_authority_head_v1",
            "authorization_gate_verdict_v1",
        }
        if not required_ids.issubset(dependency_ids):
            _push_unique(report, "errors", "DAY_PACKAGE_REQUIRED_REFS_MISSING")
        for row in package_ref.payload.get("dependency_refs") or []:
            ref_path = _normalized_ref_path(row)
            if ref_path is None:
                _push_unique(report, "errors", "DAY_PACKAGE_REF_PATH_MISSING")
                continue
            _check_path_boundary(
                report,
                scope=scope,
                label="DAY_PACKAGE_REF",
                path=ref_path,
                expected_root_type=(
                    "canonical_truth_root"
                    if ref_path == scope.canonical_truth_root or _is_under(ref_path, scope.canonical_truth_root)
                    else "execution_truth_root"
                ),
                reject_derived=False,
            )
    report["validated_family_refs"] = []
    if build_ref is not None:
        report["validated_family_refs"].append(
            _plain_ref_row(
                artifact_id="day_activation_build_v1",
                path=build_ref.path,
                sha256=build_ref.sha256,
                root_type="canonical_truth_root",
            )
        )
    if package_ref is not None:
        report["validated_family_refs"].append(
            _plain_ref_row(
                artifact_id="day_activation_package_v1",
                path=package_ref.path,
                sha256=package_ref.sha256,
                root_type="execution_truth_root",
            )
        )
    return _finalize_report(report)


def validate_global_context_family_v1(
    *,
    repo_root: Path | str = REPO_ROOT,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
) -> Dict[str, Any]:
    try:
        scope = _build_scope(
            repo_root=repo_root,
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            canonical_truth_root=canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root,
        )
    except Exception:
        report = _new_report("global_context_family_validator_v1", None)
        _push_unique(report, "errors", "CONTROL_PLANE_SCOPE_INVALID")
        return _finalize_report(report)

    report = _new_report("global_context_family_validator_v1", scope)
    package_path = (
        scope.execution_truth_root
        / "global_context_package_v1"
        / scope.day_utc
        / scope.context_hash
        / "global_context_package.v1.json"
    ).resolve()
    day_package_path = (
        scope.execution_truth_root
        / "day_activation_package_v1"
        / scope.day_utc
        / scope.context_hash
        / "day_activation_package.v1.json"
    ).resolve()
    build_ref = _load_gateway_surface(
        report,
        scope=scope,
        artifact_id="global_context_build_v1",
        domain="session",
        surface="global_context_build",
        surface_class="authoritative_immutable_artifact",
        root_type="canonical_truth_root",
        context_hash=scope.context_hash,
    )
    package_ref = _load_gateway_surface(
        report,
        scope=scope,
        artifact_id="global_context_package_v1",
        domain="session",
        surface="global_context_package",
        surface_class="authoritative_immutable_artifact",
        root_type="execution_truth_root",
        context_hash=scope.context_hash,
    )
    if build_ref is not None:
        _record_invariant(report, "GLOBAL_CONTEXT_BUILD_CONTEXT_MATCH")
        if any(
            str(build_ref.payload.get(field) or "").strip() != expected
            for field, expected in (
                ("day_utc", scope.day_utc),
                ("sleeve_id", scope.sleeve_id),
                ("mode", scope.environment),
                ("account_id", scope.ib_account),
                ("operation_type", scope.operation_type),
                ("context_hash", scope.context_hash),
            )
        ):
            _push_unique(report, "errors", "GLOBAL_CONTEXT_BUILD_CONTEXT_MISMATCH")
        _record_invariant(report, "GLOBAL_CONTEXT_BUILD_COMPLETE")
        if str(build_ref.payload.get("closure_status") or "").strip().upper() != "COMPLETE":
            _push_unique(report, "errors", "GLOBAL_CONTEXT_BUILD_NOT_COMPLETE")
    if package_ref is not None:
        _record_invariant(report, "GLOBAL_CONTEXT_PACKAGE_CONTEXT_MATCH")
        if any(
            str(package_ref.payload.get(field) or "").strip() != expected
            for field, expected in (
                ("day_utc", scope.day_utc),
                ("sleeve_id", scope.sleeve_id),
                ("mode", scope.environment),
                ("account_id", scope.ib_account),
                ("operation_type", scope.operation_type),
                ("context_hash", scope.context_hash),
            )
        ):
            _push_unique(report, "errors", "GLOBAL_CONTEXT_PACKAGE_CONTEXT_MISMATCH")
        _record_invariant(report, "GLOBAL_CONTEXT_PACKAGE_SEALED")
        if package_ref.payload.get("sealed") is not True:
            _push_unique(report, "errors", "GLOBAL_CONTEXT_PACKAGE_NOT_SEALED")
        build_ref_path = _normalized_ref_path(package_ref.payload.get("build_ref"))
        build_ref_sha = _normalized_ref_sha(package_ref.payload.get("build_ref"))
        _record_invariant(report, "GLOBAL_CONTEXT_PACKAGE_BUILD_REF_MATCH")
        if build_ref is None or build_ref_path != build_ref.path or build_ref_sha != build_ref.sha256:
            _push_unique(report, "errors", "GLOBAL_CONTEXT_PACKAGE_BUILD_REF_MISMATCH")
        day_package_ref = _normalized_ref_path(package_ref.payload.get("day_activation_package_ref"))
        _record_invariant(report, "GLOBAL_CONTEXT_PACKAGE_DAY_STAGE_REF_MATCH")
        if day_package_ref != day_package_path:
            _push_unique(report, "errors", "GLOBAL_CONTEXT_PACKAGE_DAY_STAGE_REF_MISMATCH")
        for row in package_ref.payload.get("dependency_refs") or []:
            ref_path = _normalized_ref_path(row)
            if ref_path is None:
                _push_unique(report, "errors", "GLOBAL_CONTEXT_PACKAGE_REF_PATH_MISSING")
                continue
            _check_path_boundary(
                report,
                scope=scope,
                label="GLOBAL_CONTEXT_PACKAGE_REF",
                path=ref_path,
                expected_root_type="execution_truth_root",
                reject_derived=False,
            )
    report["validated_family_refs"] = []
    if build_ref is not None:
        report["validated_family_refs"].append(
            _plain_ref_row(
                artifact_id="global_context_build_v1",
                path=build_ref.path,
                sha256=build_ref.sha256,
                root_type="canonical_truth_root",
            )
        )
    if package_ref is not None:
        report["validated_family_refs"].append(
            _plain_ref_row(
                artifact_id="global_context_package_v1",
                path=package_ref.path,
                sha256=package_ref.sha256,
                root_type="execution_truth_root",
            )
        )
    return _finalize_report(report)


def validate_session_authority_family_v1(
    *,
    repo_root: Path | str = REPO_ROOT,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
) -> Dict[str, Any]:
    try:
        scope = _build_scope(
            repo_root=repo_root,
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            canonical_truth_root=canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root,
        )
    except Exception:
        report = _new_report("session_authority_family_validator_v1", None)
        _push_unique(report, "errors", "CONTROL_PLANE_SCOPE_INVALID")
        return _finalize_report(report)

    report = _new_report("session_authority_family_validator_v1", scope)
    build_ref = _load_gateway_surface(
        report,
        scope=scope,
        artifact_id="target_day_build_v1",
        domain="session",
        surface="target_day_build",
        surface_class="authoritative_immutable_artifact",
        root_type="canonical_truth_root",
    )
    admission_ref = _load_gateway_surface(
        report,
        scope=scope,
        artifact_id="target_day_admission_v1",
        domain="session",
        surface="target_day_admission",
        surface_class="authoritative_immutable_artifact",
        root_type="canonical_truth_root",
    )
    promotion_ref = _load_gateway_surface(
        report,
        scope=scope,
        artifact_id="session_promotion_decision_v1",
        domain="session",
        surface="session_promotion_decision",
        surface_class="authoritative_immutable_artifact",
        root_type="canonical_truth_root",
    )
    active_ref = _load_gateway_surface(
        report,
        scope=scope,
        artifact_id="active_session_v1",
        domain="session",
        surface="active_session_current",
        surface_class="authoritative_current_state_surface",
        root_type="canonical_truth_root",
        reject_derived=True,
        is_current_surface=True,
    )
    if build_ref is not None:
        _record_invariant(report, "TARGET_DAY_BUILD_COMPLETE")
        if str(build_ref.payload.get("build_status") or "").strip().upper() != "COMPLETE":
            _push_unique(report, "errors", "TARGET_DAY_BUILD_NOT_COMPLETE")
        _record_invariant(report, "TARGET_DAY_BUILD_CLOSED")
        if str(build_ref.payload.get("closure_status") or "").strip().upper() != "CLOSED":
            _push_unique(report, "errors", "TARGET_DAY_BUILD_NOT_CLOSED")
        _record_invariant(report, "TARGET_DAY_BUILD_DAY_MATCH")
        if str(build_ref.payload.get("target_day") or "").strip() != scope.day_utc:
            _push_unique(report, "errors", "TARGET_DAY_BUILD_DAY_MISMATCH")
    if admission_ref is not None:
        _record_invariant(report, "TARGET_DAY_ADMISSION_ADMIT")
        if str(admission_ref.payload.get("admission_status") or "").strip().upper() != "ADMIT":
            _push_unique(report, "errors", "TARGET_DAY_ADMISSION_NOT_ADMIT")
        _record_invariant(report, "TARGET_DAY_ADMISSION_BINDING_TRUE")
        if admission_ref.payload.get("binding") is not True:
            _push_unique(report, "errors", "TARGET_DAY_ADMISSION_NOT_BINDING")
        _record_invariant(report, "TARGET_DAY_ADMISSION_DAY_MATCH")
        if str(admission_ref.payload.get("target_day") or "").strip() != scope.day_utc:
            _push_unique(report, "errors", "TARGET_DAY_ADMISSION_DAY_MISMATCH")
        _record_invariant(report, "TARGET_DAY_ADMISSION_BUILD_REF_MATCH")
        admission_build_path = _normalized_ref_path(admission_ref.payload.get("build_ref"))
        admission_build_sha = _normalized_ref_sha(admission_ref.payload.get("build_ref"))
        if build_ref is None or admission_build_path != build_ref.path or admission_build_sha != build_ref.sha256:
            _push_unique(report, "errors", "TARGET_DAY_ADMISSION_BUILD_REF_MISMATCH")
    if promotion_ref is not None:
        _record_invariant(report, "SESSION_PROMOTION_PROMOTED")
        if str(promotion_ref.payload.get("promotion_state") or "").strip().upper() != "PROMOTED":
            _push_unique(report, "errors", "SESSION_PROMOTION_NOT_PROMOTED")
        _record_invariant(report, "SESSION_PROMOTION_DAY_MATCH")
        if str(promotion_ref.payload.get("target_day") or "").strip() != scope.day_utc:
            _push_unique(report, "errors", "SESSION_PROMOTION_DAY_MISMATCH")
        _record_invariant(report, "SESSION_PROMOTION_ADMISSION_REF_MATCH")
        promotion_admission_path = _normalized_ref_path(promotion_ref.payload.get("target_day_admission_ref"))
        promotion_admission_sha = _normalized_ref_sha(promotion_ref.payload.get("target_day_admission_ref"))
        if admission_ref is None or promotion_admission_path != admission_ref.path or promotion_admission_sha != admission_ref.sha256:
            _push_unique(report, "errors", "SESSION_PROMOTION_ADMISSION_REF_MISMATCH")
        promoted_rows = promotion_ref.payload.get("promoted_artifacts") or []
        promoted_paths = {
            str(_normalized_ref_path(row) or "")
            for row in promoted_rows
            if isinstance(row, Mapping)
        }
        _record_invariant(report, "SESSION_PROMOTION_ACTIVE_SESSION_TARGET_PRESENT")
        expected_active_path = str(resolve_active_session_path(truth_root=scope.canonical_truth_root))
        if expected_active_path not in promoted_paths:
            _push_unique(report, "errors", "SESSION_PROMOTION_ACTIVE_SESSION_TARGET_MISSING")
    if active_ref is not None:
        _record_invariant(report, "ACTIVE_SESSION_DAY_MATCH")
        if str(active_ref.payload.get("target_day") or "").strip() != scope.day_utc:
            _push_unique(report, "errors", "ACTIVE_SESSION_TARGET_DAY_MISMATCH")
        _record_invariant(report, "ACTIVE_SESSION_ACTIVE_DAY_MATCH")
        if str(active_ref.payload.get("active_day") or "").strip() != scope.day_utc:
            _push_unique(report, "errors", "ACTIVE_SESSION_ACTIVE_DAY_MISMATCH")
        _record_invariant(report, "ACTIVE_SESSION_PROMOTION_STATE_PROMOTED")
        if str(active_ref.payload.get("promotion_state") or "").strip().upper() != "PROMOTED":
            _push_unique(report, "errors", "ACTIVE_SESSION_NOT_PROMOTED")
        _record_invariant(report, "ACTIVE_SESSION_ROLLOVER_NOT_WITHHELD")
        if str(active_ref.payload.get("rollover_status") or "").strip().upper() == "ROLLOVER_WITHHELD":
            _push_unique(report, "errors", "ACTIVE_SESSION_ROLLOVER_WITHHELD")
        _record_invariant(report, "ACTIVE_SESSION_BUILD_REF_MATCH")
        active_build_path = _normalized_ref_path(active_ref.payload.get("target_day_build_ref"))
        active_build_sha = _normalized_ref_sha(active_ref.payload.get("target_day_build_ref"))
        if build_ref is None or active_build_path != build_ref.path or active_build_sha != build_ref.sha256:
            _push_unique(report, "errors", "ACTIVE_SESSION_BUILD_REF_MISMATCH")
        _record_invariant(report, "ACTIVE_SESSION_ADMISSION_REF_MATCH")
        active_admission_path = Path(str(active_ref.payload.get("target_day_admission_ref") or "")).resolve()
        if admission_ref is None or active_admission_path != admission_ref.path:
            _push_unique(report, "errors", "ACTIVE_SESSION_ADMISSION_REF_MISMATCH")
    report["validated_family_refs"] = []
    for artifact_id, ref in (
        ("target_day_build_v1", build_ref),
        ("target_day_admission_v1", admission_ref),
        ("session_promotion_decision_v1", promotion_ref),
        ("active_session_v1", active_ref),
    ):
        if ref is None:
            continue
        report["validated_family_refs"].append(
            _plain_ref_row(
                artifact_id=artifact_id,
                path=ref.path,
                sha256=ref.sha256,
                root_type="canonical_truth_root",
            )
        )
    return _finalize_report(report)


def validate_execution_build_family_v1(
    *,
    repo_root: Path | str = REPO_ROOT,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    candidate_path: Path | str,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
) -> Dict[str, Any]:
    try:
        scope = _build_scope(
            repo_root=repo_root,
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            candidate_path=candidate_path,
            canonical_truth_root=canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root,
        )
    except Exception:
        report = _new_report("execution_build_family_validator_v1", None)
        _push_unique(report, "errors", "CONTROL_PLANE_SCOPE_INVALID")
        return _finalize_report(report)

    report = _new_report("execution_build_family_validator_v1", scope)
    expected_global_context_path = (
        scope.execution_truth_root
        / "global_context_package_v1"
        / scope.day_utc
        / scope.context_hash
        / "global_context_package.v1.json"
    ).resolve()
    expected_economic_path = (
        scope.execution_truth_root
        / "economic_state_package_v1"
        / scope.day_utc
        / scope.context_hash
        / "economic_state_package.v1.json"
    ).resolve()
    build_ref = _load_gateway_surface(
        report,
        scope=scope,
        artifact_id="execution_build_v1",
        domain="execution",
        surface="execution_build",
        surface_class="authoritative_immutable_artifact",
        root_type="canonical_truth_root",
        submission_id=scope.submission_id,
    )
    package_ref = _load_gateway_surface(
        report,
        scope=scope,
        artifact_id="execution_package_v1",
        domain="execution",
        surface="execution_package",
        surface_class="authoritative_immutable_artifact",
        root_type="execution_truth_root",
        submission_id=scope.submission_id,
    )
    if build_ref is not None:
        _record_invariant(report, "EXECUTION_BUILD_COMPLETE")
        if str(build_ref.payload.get("closure_status") or "").strip().upper() != "COMPLETE":
            _push_unique(report, "errors", "EXECUTION_BUILD_NOT_COMPLETE")
        _record_invariant(report, "EXECUTION_BUILD_SUBMISSION_MATCH")
        if str(build_ref.payload.get("submission_id") or "").strip() != scope.submission_id:
            _push_unique(report, "errors", "EXECUTION_BUILD_SUBMISSION_ID_MISMATCH")
        _record_invariant(report, "EXECUTION_BUILD_GLOBAL_CONTEXT_REF_MATCH")
        build_global_context_path = _normalized_ref_path(build_ref.payload.get("global_context_package_ref"))
        if build_global_context_path != expected_global_context_path:
            _push_unique(report, "errors", "EXECUTION_BUILD_GLOBAL_CONTEXT_REF_MISMATCH")
        _record_invariant(report, "EXECUTION_BUILD_ECONOMIC_REF_MATCH")
        build_economic_path = _normalized_ref_path(build_ref.payload.get("economic_state_package_ref"))
        if build_economic_path != expected_economic_path:
            _push_unique(report, "errors", "EXECUTION_BUILD_ECONOMIC_REF_MISMATCH")
        for row in build_ref.payload.get("dependency_results") or []:
            dep_id = str((row or {}).get("dependency_id") or "").strip()
            status = str((row or {}).get("status") or "").strip().upper()
            required = bool((row or {}).get("required") is True)
            advisory_only = bool((row or {}).get("advisory_only") is True)
            post_submit_only = bool((row or {}).get("post_submit_only") is True)
            if required and not advisory_only and not post_submit_only and status != "PRESENT":
                _push_unique(report, "errors", f"EXECUTION_BUILD_DEPENDENCY_NOT_PRESENT:{dep_id}")
            dep_path = _normalized_ref_path({"path": row.get("path")})
            if dep_path is not None:
                _check_path_boundary(
                    report,
                    scope=scope,
                    label="EXECUTION_BUILD_DEPENDENCY",
                    path=dep_path,
                    expected_root_type=(
                        "canonical_truth_root"
                        if dep_path == scope.canonical_truth_root or _is_under(dep_path, scope.canonical_truth_root)
                        else "execution_truth_root"
                    ),
                    reject_derived=False,
                )
    if package_ref is not None:
        _record_invariant(report, "EXECUTION_PACKAGE_SEALED")
        if package_ref.payload.get("sealed") is not True:
            _push_unique(report, "errors", "EXECUTION_PACKAGE_NOT_SEALED")
        _record_invariant(report, "EXECUTION_PACKAGE_BUILD_REF_MATCH")
        package_build_path = _normalized_ref_path(package_ref.payload.get("build_ref"))
        package_build_sha = _normalized_ref_sha(package_ref.payload.get("build_ref"))
        if build_ref is None or package_build_path != build_ref.path or package_build_sha != build_ref.sha256:
            _push_unique(report, "errors", "EXECUTION_PACKAGE_BUILD_REF_MISMATCH")
        _record_invariant(report, "EXECUTION_PACKAGE_GLOBAL_CONTEXT_REF_MATCH")
        package_global_context_path = _normalized_ref_path(package_ref.payload.get("global_context_package_ref"))
        if package_global_context_path != expected_global_context_path:
            _push_unique(report, "errors", "EXECUTION_PACKAGE_GLOBAL_CONTEXT_REF_MISMATCH")
        _record_invariant(report, "EXECUTION_PACKAGE_ECONOMIC_REF_MATCH")
        package_economic_path = _normalized_ref_path(package_ref.payload.get("economic_state_package_ref"))
        if package_economic_path != expected_economic_path:
            _push_unique(report, "errors", "EXECUTION_PACKAGE_ECONOMIC_REF_MISMATCH")
        for row in package_ref.payload.get("dependency_refs") or []:
            ref_path = _normalized_ref_path(row)
            if ref_path is None:
                _push_unique(report, "errors", "EXECUTION_PACKAGE_REF_PATH_MISSING")
                continue
            _check_path_boundary(
                report,
                scope=scope,
                label="EXECUTION_PACKAGE_REF",
                path=ref_path,
                expected_root_type=(
                    "canonical_truth_root"
                    if ref_path == scope.canonical_truth_root or _is_under(ref_path, scope.canonical_truth_root)
                    else "execution_truth_root"
                ),
                reject_derived=False,
            )
    report["validated_family_refs"] = []
    if build_ref is not None:
        report["validated_family_refs"].append(
            _plain_ref_row(
                artifact_id="execution_build_v1",
                path=build_ref.path,
                sha256=build_ref.sha256,
                root_type="canonical_truth_root",
            )
        )
    if package_ref is not None:
        report["validated_family_refs"].append(
            _plain_ref_row(
                artifact_id="execution_package_v1",
                path=package_ref.path,
                sha256=package_ref.sha256,
                root_type="execution_truth_root",
            )
        )
    return _finalize_report(report)


def validate_control_plane_boundary_v1(
    *,
    repo_root: Path | str = REPO_ROOT,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    candidate_path: Path | str | None = None,
    operation_type: str = "fresh_paper_entry_v1",
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
) -> Dict[str, Any]:
    try:
        scope = _build_scope(
            repo_root=repo_root,
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            candidate_path=candidate_path,
            canonical_truth_root=canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root,
        )
    except Exception:
        report = _new_report("control_plane_boundary_validator_v1", None)
        _push_unique(report, "errors", "CONTROL_PLANE_SCOPE_INVALID")
        return _finalize_report(report)

    report = _new_report("control_plane_boundary_validator_v1", scope)
    canonical_roots = [
        scope.canonical_truth_root,
        resolve_target_day_build_path(truth_root=scope.canonical_truth_root, target_day=scope.day_utc),
        resolve_target_day_admission_path(truth_root=scope.canonical_truth_root, target_day=scope.day_utc),
        resolve_session_promotion_decision_path_v1(truth_root=scope.canonical_truth_root, day_utc=scope.day_utc),
        resolve_active_session_path(truth_root=scope.canonical_truth_root),
        (scope.canonical_truth_root / "reports" / "day_activation_build_v1" / scope.day_utc / scope.context_hash / "day_activation_build.v1.json").resolve(),
        (scope.canonical_truth_root / "reports" / "global_context_build_v1" / scope.day_utc / scope.context_hash / "global_context_build.v1.json").resolve(),
    ]
    execution_roots = [
        scope.execution_truth_root,
        (scope.execution_truth_root / "day_activation_package_v1" / scope.day_utc / scope.context_hash / "day_activation_package.v1.json").resolve(),
        (scope.execution_truth_root / "global_context_package_v1" / scope.day_utc / scope.context_hash / "global_context_package.v1.json").resolve(),
    ]
    if scope.submission_id:
        canonical_roots.append(
            (scope.canonical_truth_root / "reports" / "execution_build_v1" / scope.day_utc / scope.submission_id / "execution_build.v1.json").resolve()
        )
        execution_roots.append(
            (scope.execution_truth_root / "execution_package_v1" / scope.day_utc / scope.submission_id / "execution_package.v1.json").resolve()
        )
    for path in canonical_roots:
        _record_invariant(report, "BOUNDARY_CANONICAL_PATH_UNDER_CANONICAL_TRUTH_ROOT")
        _check_path_boundary(
            report,
            scope=scope,
            label="CANONICAL_PATH",
            path=path,
            expected_root_type="canonical_truth_root",
            reject_derived=False,
        )
    for path in execution_roots:
        _record_invariant(report, "BOUNDARY_EXECUTION_PATH_UNDER_EXECUTION_TRUTH_ROOT")
        _check_path_boundary(
            report,
            scope=scope,
            label="EXECUTION_PATH",
            path=path,
            expected_root_type="execution_truth_root",
            reject_derived=False,
        )
    for derived_id, derived_path in _derived_surface_paths(scope).items():
        _record_artifact(
            report,
            artifact_id=derived_id,
            path=derived_path,
            sha256="",
            surface_class="derived_non_authoritative_surface",
            root_type="canonical_truth_root",
            is_current_surface=derived_id.endswith("_current"),
        )
        _record_invariant(report, "BOUNDARY_DERIVED_SURFACE_CLASSIFIED_NON_AUTHORITATIVE")
        if derived_path == resolve_active_session_path(truth_root=scope.canonical_truth_root):
            _push_unique(report, "errors", "BOUNDARY_ACTIVE_SESSION_COLLIDES_WITH_DERIVED_SURFACE")
    return _finalize_report(report)
