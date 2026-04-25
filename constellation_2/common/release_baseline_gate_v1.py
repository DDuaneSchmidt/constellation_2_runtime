from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.common.configuration_activation_family_validator_v1 import (
    validate_configuration_activation_family_v1,
)
from constellation_2.common.control_plane_trust_surface_validator_v1 import (
    validate_control_plane_trust_surfaces_v1,
)
from constellation_2.common.control_plane_validation_kernel_v1 import (
    validate_control_plane_boundary_v1,
    validate_day_activation_family_v1,
    validate_execution_build_family_v1,
    validate_global_context_family_v1,
    validate_session_authority_family_v1,
)
from constellation_2.common.release_baseline_common_v1 import (
    forbidden_root_hits_in_active_paths_v1,
    read_json_object_v1,
    resolve_release_baseline_roots_v1,
)
from constellation_2.common.runtime_state_readiness_validator_v1 import (
    validate_runtime_state_readiness_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOYMENT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/deployment_state_machine.v1.schema.json"


def evaluate_release_baseline_gate_v1(
    *,
    repo_root: Path | str = REPO_ROOT,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str = "fresh_paper_entry_v1",
    candidate_path: Path | str | None = None,
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
) -> dict[str, Any]:
    roots = resolve_release_baseline_roots_v1(
        repo_root,
        canonical_truth_root=canonical_truth_root,
        truth_sleeves_root=truth_sleeves_root,
    )
    deployment_path = (
        roots.canonical_truth_root / "reports" / "deployment_state_machine_v1" / day_utc / "deployment_state_machine.v1.json"
    ).resolve()
    forbidden_hits = forbidden_root_hits_in_active_paths_v1(
        roots.repo_root,
        canonical_truth_root=roots.canonical_truth_root,
        truth_sleeves_root=roots.truth_sleeves_root,
    )
    blocking_errors: list[str] = []
    deployment_payload: dict[str, Any] = {}
    try:
        deployment_payload = read_json_object_v1(deployment_path)
        validate_against_repo_schema_v1(deployment_payload, REPO_ROOT, DEPLOYMENT_SCHEMA)
    except Exception as exc:
        blocking_errors.append(f"DEPLOYMENT_BASELINE_INVALID:{type(exc).__name__}")
    baseline_status = {
        "status": "UNKNOWN",
        "deployment_artifact_path": str(deployment_path),
        "deployment_decision": "",
        "authoritative_cleanliness_status": "",
    }
    if deployment_payload:
        baseline_status = {
            "status": "PASS",
            "deployment_artifact_path": str(deployment_path),
            "deployment_decision": str(deployment_payload.get("final_deployment_decision") or "").strip(),
            "authoritative_cleanliness_status": str(((deployment_payload.get("authoritative_source") or {}).get("authoritative_cleanliness_status")) or "").strip(),
        }
        if baseline_status["deployment_decision"] not in {"DEPLOY_ACTIVE", "DEPLOY_READY"}:
            baseline_status["status"] = "FAIL"
            blocking_errors.append(f"DEPLOYMENT_DECISION_BLOCKING:{baseline_status['deployment_decision'] or 'UNKNOWN'}")
        if baseline_status["authoritative_cleanliness_status"] != "CLEAN":
            baseline_status["status"] = "FAIL"
            blocking_errors.append("AUTHORITATIVE_BASELINE_NOT_CLEAN")

    validator_reports: dict[str, dict[str, Any]] = {
        "configuration_activation_family": validate_configuration_activation_family_v1(truth_root=roots.canonical_truth_root),
        "day_activation_family": validate_day_activation_family_v1(
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            canonical_truth_root=canonical_truth_root or roots.canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root or roots.truth_sleeves_root,
        ),
        "global_context_family": validate_global_context_family_v1(
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            canonical_truth_root=canonical_truth_root or roots.canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root or roots.truth_sleeves_root,
        ),
        "session_authority_family": validate_session_authority_family_v1(
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            canonical_truth_root=canonical_truth_root or roots.canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root or roots.truth_sleeves_root,
        ),
        "control_plane_boundary": validate_control_plane_boundary_v1(
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            candidate_path=candidate_path,
            canonical_truth_root=canonical_truth_root or roots.canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root or roots.truth_sleeves_root,
        ),
        "runtime_state_readiness": validate_runtime_state_readiness_v1(
            repo_root=roots.repo_root,
            canonical_truth_root=canonical_truth_root or roots.canonical_truth_root,
        ),
        "control_plane_trust_surfaces": validate_control_plane_trust_surfaces_v1(
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            canonical_truth_root=canonical_truth_root or roots.canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root or roots.truth_sleeves_root,
        ),
    }
    if candidate_path is not None and str(candidate_path).strip():
        validator_reports["execution_build_family"] = validate_execution_build_family_v1(
            day_utc=day_utc,
            sleeve_id=sleeve_id,
            environment=environment,
            ib_account=ib_account,
            operation_type=operation_type,
            candidate_path=candidate_path,
            canonical_truth_root=canonical_truth_root or roots.canonical_truth_root,
            truth_sleeves_root=truth_sleeves_root or roots.truth_sleeves_root,
        )
    else:
        validator_reports["execution_build_family"] = {
            "validator_id": "execution_build_family_validator_v1",
            "ok": False,
            "skipped": True,
            "required": False,
            "errors": ["EXECUTION_BUILD_VALIDATOR_SKIPPED_REQUIRES_CANDIDATE_PATH"],
        }

    validator_statuses: dict[str, dict[str, Any]] = {}
    for name, payload in validator_reports.items():
        required = not bool(payload.get("skipped")) or bool(payload.get("required"))
        ok = bool(payload.get("ok"))
        validator_statuses[name] = {
            "ok": ok,
            "required": required,
            "skipped": bool(payload.get("skipped")),
            "errors": list(payload.get("errors") or []),
            "summary": dict(payload.get("summary") or {}),
            "operator_summary": dict(payload.get("operator_summary") or {}),
            "timeline_summary": dict(payload.get("timeline_summary") or {}),
        }
        if required and not ok:
            blocking_errors.append(f"VALIDATOR_FAILED:{name}")

    if forbidden_hits:
        blocking_errors.append("FORBIDDEN_ACTIVE_ROOT_HITS_PRESENT")

    readiness_summary = {
        "deployment_decision": baseline_status.get("deployment_decision") or "UNKNOWN",
        "platform_readiness_state": str(((validator_reports["runtime_state_readiness"].get("summary") or {}).get("platform_readiness_state")) or "UNKNOWN"),
        "operator_status": str(((validator_reports["control_plane_trust_surfaces"].get("operator_summary") or {}).get("current_operator_status")) or "UNKNOWN"),
        "configuration_state": "VALID" if validator_reports["configuration_activation_family"].get("ok") else "INVALID",
    }
    governing_refs = [
        "governance/05_CONTRACTS/C2/release_baseline_readiness_v1.contract.md",
        "governance/05_CONTRACTS/C2/runtime_authority_closure_v1.contract.md",
        "governance/05_CONTRACTS/C2/critical_validator_coverage_v1.contract.md",
        str(deployment_path),
    ]
    report = {
        "gate_id": "release_baseline_gate_v1",
        "authority_label": "governed_release_gate",
        "ok": not blocking_errors,
        "baseline_status": baseline_status,
        "canonical_root_status": {
            "ok": not forbidden_hits,
            "forbidden_root_hits": forbidden_hits,
        },
        "validator_statuses": validator_statuses,
        "forbidden_root_hits": forbidden_hits,
        "blocking_errors": sorted(set(blocking_errors)),
        "governing_refs": governing_refs,
        "readiness_summary": readiness_summary,
    }
    return report


def build_certified_operational_readiness_v1(
    *,
    repo_root: Path | str = REPO_ROOT,
    day_utc: str,
    sleeve_id: str,
    environment: str,
    ib_account: str,
    operation_type: str = "fresh_paper_entry_v1",
    candidate_path: Path | str | None = None,
    canonical_truth_root: Path | str | None = None,
    truth_sleeves_root: Path | str | None = None,
) -> dict[str, Any]:
    gate = evaluate_release_baseline_gate_v1(
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
    trust = gate["validator_statuses"]["control_plane_trust_surfaces"]
    runtime = gate["validator_statuses"]["runtime_state_readiness"]
    return {
        "surface_id": "certified_operational_readiness_v1",
        "authority_label": "governed_release_projection",
        "ok": bool(gate.get("ok")),
        "day_utc": day_utc,
        "scope": {
            "sleeve_id": sleeve_id,
            "environment": environment,
            "ib_account": ib_account,
            "operation_type": operation_type,
        },
        "baseline_gate": gate,
        "current_family_statuses": {
            "configuration_activation_family": gate["validator_statuses"]["configuration_activation_family"],
            "day_activation_family": gate["validator_statuses"]["day_activation_family"],
            "global_context_family": gate["validator_statuses"]["global_context_family"],
            "session_authority_family": gate["validator_statuses"]["session_authority_family"],
            "control_plane_boundary": gate["validator_statuses"]["control_plane_boundary"],
            "runtime_state_readiness": runtime,
            "control_plane_trust_surfaces": trust,
        },
        "transition_timeline_summary": dict(trust.get("timeline_summary") or {}),
        "blocked_state": {
            "blocked": not bool(gate.get("ok")),
            "blocking_errors": list(gate.get("blocking_errors") or []),
        },
        "readiness_summary": dict(gate.get("readiness_summary") or {}),
        "governing_refs": [
            "governance/05_CONTRACTS/C2/certified_operational_readiness_v1.contract.md",
            *list(gate.get("governing_refs") or []),
        ],
    }
