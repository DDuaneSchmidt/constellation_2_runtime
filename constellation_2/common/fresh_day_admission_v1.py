from __future__ import annotations

from datetime import date, timedelta
import json
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    sha256_file_v1,
)
from constellation_2.common.control_plane_read_gateway_v1 import (
    ControlPlaneSemanticViewV1,
    read_control_plane_semantic_v1,
    read_control_plane_surface_v1,
)
from constellation_2.common.next_day_readiness_probe_v1 import resolve_next_day_readiness_probe_path
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_operator_statement_path,
    resolve_paper_capital_seed_path,
)
from constellation_2.common.runtime_contract_v1 import resolve_release_provenance
from constellation_2.common.runtime_authority_snapshot_bridge_v1 import (
    load_runtime_path_authority_bridge_v1,
    resolve_runtime_path_authority_snapshot_bridge_v1,
)
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1


FRESH_DAY_ADMISSION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/fresh_day_admission.v1.schema.json"
NEXT_DAY_READINESS_PROBE_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/next_day_readiness_probe.v1.schema.json"
PAPER_POLICY_VERDICT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_policy_verdict.v1.schema.json"
TRADE_SUBMIT_READINESS_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/READINESS/trade_submit_readiness.status.v1.schema.json"
TRADING_DAY_STATE_MACHINE_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_state_machine.v1.schema.json"
CAPITAL_RISK_ENVELOPE_V2_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/capital_risk_envelope.v2.schema.json"
PAPER_BOOTSTRAP_MODE = "PAPER_BOOTSTRAP"
PAPER_BOOTSTRAP_REASON = "PAPER_BOOTSTRAP_SESSION_ADMISSION"


def resolve_fresh_day_admission_path(*, truth_root: Path, target_day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "fresh_day_admission_v1"
        / str(target_day_utc).strip()
        / "fresh_day_admission.v1.json"
    ).resolve()


def _day_minus_one(day_utc: str) -> str:
    return (date.fromisoformat(str(day_utc).strip()) - timedelta(days=1)).isoformat()


def _artifact_ref(path: Path) -> Dict[str, str]:
    return {
        "artifact_path": str(path),
        "artifact_sha256": sha256_file_v1(path),
    }

def _read_optional_json_dict(path: Path) -> Dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _find_previous_day_complete_economic_state_build_path(*, truth_root: Path, target_day_utc: str) -> Path | None:
    previous_day = _day_minus_one(target_day_utc)
    build_day_root = (Path(truth_root).resolve() / "reports" / "economic_state_build_v1" / previous_day).resolve()
    if not build_day_root.exists() or not build_day_root.is_dir():
        return None
    for candidate in sorted(build_day_root.glob("*/economic_state_build.v1.json")):
        payload = _read_optional_json_dict(candidate)
        if not isinstance(payload, dict):
            continue
        closure_status = str(payload.get("closure_status") or "").strip().upper()
        if closure_status == "COMPLETE":
            return candidate.resolve()
    return None


def evaluate_paper_bootstrap_admission_v1(
    *,
    repo_root: Path,
    target_day_utc: str,
    environment: str,
) -> Dict[str, Any]:
    normalized_environment = str(environment).strip().upper()
    result: Dict[str, Any] = {
        "eligible": False,
        "mode": PAPER_BOOTSTRAP_MODE,
        "reason": PAPER_BOOTSTRAP_REASON,
        "artifacts": [],
        "blocking_reason_codes": [],
    }
    if normalized_environment != "PAPER":
        result["blocking_reason_codes"] = ["PAPER_BOOTSTRAP_ENVIRONMENT_NOT_PAPER"]
        return result

    resolved_repo_root = Path(repo_root).resolve()
    operator_input_root = (resolved_repo_root / "constellation_2").resolve()
    authority = load_runtime_path_authority_bridge_v1(
        repo_root=resolved_repo_root,
        caller="constellation_2/common/fresh_day_admission_v1.py",
    )
    prior_day_complete_build_path = _find_previous_day_complete_economic_state_build_path(
        truth_root=authority.canonical_runtime_truth_root,
        target_day_utc=target_day_utc,
    )
    if prior_day_complete_build_path is not None:
        result["artifacts"].append(
            {
                "artifact_id": "previous_day_economic_state_build_v1",
                "artifact_path": str(prior_day_complete_build_path),
                "artifact_sha256": sha256_file_v1(prior_day_complete_build_path),
                "artifact_status": "PASS",
            }
        )
        result["blocking_reason_codes"].append("PAPER_BOOTSTRAP_PRIOR_DAY_CONTINUITY_PRESENT")
        return result
    seed_path = resolve_paper_capital_seed_path(operator_input_root=operator_input_root, day_utc=target_day_utc)
    operator_statement_path = resolve_operator_statement_path(
        operator_input_root=operator_input_root,
        day_utc=target_day_utc,
    )

    seed_payload = _read_optional_json_dict(seed_path)
    if seed_payload is None:
        result["blocking_reason_codes"].append("PAPER_BOOTSTRAP_SEED_MISSING_OR_INVALID")
    else:
        seed_day = str(seed_payload.get("day_utc") or "").strip()
        seed_environment = str(seed_payload.get("environment") or "").strip().upper()
        seed_cash_total = str(seed_payload.get("cash_total") or "").strip()
        seed_nlv_total = str(seed_payload.get("nlv_total") or "").strip()
        if (
            seed_day != str(target_day_utc).strip()
            or seed_environment != "PAPER"
            or not seed_cash_total
            or not seed_nlv_total
        ):
            result["blocking_reason_codes"].append("PAPER_BOOTSTRAP_SEED_INVALID")
        else:
            result["artifacts"].append(
                {
                    "artifact_id": "paper_capital_seed_v1",
                    "artifact_path": str(seed_path),
                    "artifact_sha256": sha256_file_v1(seed_path),
                    "artifact_status": "PASS",
                }
            )

    operator_statement_payload = _read_optional_json_dict(operator_statement_path)
    if operator_statement_payload is None:
        result["blocking_reason_codes"].append("PAPER_BOOTSTRAP_OPERATOR_STATEMENT_MISSING_OR_INVALID")
    else:
        operator_cash_total = str(operator_statement_payload.get("cash_total") or "").strip()
        operator_nlv_total = str(operator_statement_payload.get("nlv_total") or "").strip()
        seed_cash_total = str(seed_payload.get("cash_total") or "").strip() if isinstance(seed_payload, dict) else ""
        seed_nlv_total = str(seed_payload.get("nlv_total") or "").strip() if isinstance(seed_payload, dict) else ""
        if (
            not operator_cash_total
            or not operator_nlv_total
            or (seed_cash_total and operator_cash_total != seed_cash_total)
            or (seed_nlv_total and operator_nlv_total != seed_nlv_total)
        ):
            result["blocking_reason_codes"].append("PAPER_BOOTSTRAP_OPERATOR_STATEMENT_INVALID")
        else:
            result["artifacts"].append(
                {
                    "artifact_id": "operator_statement_v1",
                    "artifact_path": str(operator_statement_path),
                    "artifact_sha256": sha256_file_v1(operator_statement_path),
                    "artifact_status": "PASS",
                }
            )

    try:
        cap_env_ref = read_control_plane_surface_v1(
            domain="execution",
            surface="capital_risk_envelope",
            truth_root=Path(authority.canonical_runtime_truth_sleeves_root).resolve(),
            day_utc=target_day_utc,
            truth_sleeves_root=authority.canonical_runtime_truth_sleeves_root,
            environment=normalized_environment,
            sleeve_id="PRIMARY",
        )
    except Exception:
        cap_env_ref = None
    if cap_env_ref is not None:
        cap_env_status = str(cap_env_ref.payload.get("status") or "").strip().upper()
        if cap_env_status == "PASS":
            result["artifacts"].append(
                _materialized_artifact_row(
                    artifact_id="capital_risk_envelope_v2",
                    ref=cap_env_ref,
                    artifact_status=cap_env_status,
                )
            )

    result["eligible"] = not bool(result["blocking_reason_codes"])
    return result

def _materialized_artifact_row(*, artifact_id: str, ref: SurfaceRefV1, artifact_status: str) -> Dict[str, str]:
    return {
        "artifact_id": artifact_id,
        "artifact_path": str(ref.path),
        "artifact_sha256": ref.sha256,
        "artifact_status": artifact_status,
    }


def _normalize_materialized_target_day_artifacts(rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    for row in rows:
        artifact_id = str(row.get("artifact_id") or "").strip()
        artifact_ref = row.get("artifact_ref") if isinstance(row.get("artifact_ref"), dict) else {}
        artifact_path = str(row.get("artifact_path") or artifact_ref.get("artifact_path") or artifact_ref.get("path") or "").strip()
        artifact_sha256 = str(
            row.get("artifact_sha256")
            or artifact_ref.get("artifact_sha256")
            or artifact_ref.get("sha256")
            or ""
        ).strip()
        artifact_status = str(row.get("artifact_status") or "").strip().upper()
        if artifact_status not in {"PASS", "FAIL", "UNKNOWN"}:
            artifact_status = "UNKNOWN"

        if not artifact_id or not artifact_path or len(artifact_sha256) != 64:
            raise ValueError(
                "FRESH_DAY_ADMISSION_MATERIALIZED_ARTIFACT_INVALID:"
                f"artifact_id={artifact_id or 'MISSING'}:artifact_path={artifact_path or 'MISSING'}"
            )

        normalized.append(
            {
                "artifact_id": artifact_id,
                "artifact_path": artifact_path,
                "artifact_sha256": artifact_sha256,
                "artifact_status": artifact_status,
            }
        )
    return normalized


def derive_fresh_day_admission_payload(
    *,
    repo_root: Path,
    truth_root: Path,
    target_day_utc: str,
    environment: str,
    ib_account: str,
) -> Dict[str, Any]:
    resolved_truth_root = resolve_decision_truth_root_v1(truth_root, repo_root=repo_root)
    authority = load_runtime_path_authority_bridge_v1(
        repo_root=repo_root,
        caller="constellation_2/common/fresh_day_admission_v1.py",
    )
    authority_snapshot = resolve_runtime_path_authority_snapshot_bridge_v1(
        repo_root=repo_root,
        caller="constellation_2/common/fresh_day_admission_v1.py",
    )
    reference_day_utc = _day_minus_one(target_day_utc)
    semantic_view: ControlPlaneSemanticViewV1 = read_control_plane_semantic_v1(
        domain="session",
        surface="fresh_day_admission_inputs",
        truth_root=resolved_truth_root,
        day_utc=target_day_utc,
        environment=environment,
        ib_account=ib_account,
    )
    semantic_payload = dict(semantic_view.payload)
    required_target_day_artifacts = list(semantic_payload.get("required_target_day_artifacts") or [])
    materialized_target_day_artifacts = list(semantic_payload.get("materialized_target_day_artifacts") or [])
    blocking_items = list(semantic_payload.get("blocking_items") or [])
    missing_required_artifacts = list(semantic_payload.get("missing_required_artifacts") or [])
    required_artifacts_ready = bool(semantic_payload.get("required_artifacts_ready") is True)
    probe_status = str(semantic_payload.get("probe_status") or "MISSING").strip().upper()
    probe_path = resolve_next_day_readiness_probe_path(
        truth_root=resolved_truth_root,
        target_day_utc=target_day_utc,
    )

    bootstrap_result = evaluate_paper_bootstrap_admission_v1(
        repo_root=repo_root,
        target_day_utc=target_day_utc,
        environment=environment,
    )
    admission_status = "ADMIT" if required_artifacts_ready else "BLOCKED"
    if bootstrap_result["eligible"]:
        admission_status = "ADMIT"
        required_target_day_artifacts = [
            {
                "artifact_id": str(row.get("artifact_id") or "").strip(),
                "artifact_path": str(row.get("artifact_path") or "").strip(),
                "requirement_class": "REQUIRED_PRE_ADMISSION",
            }
            for row in bootstrap_result["artifacts"]
        ]
        materialized_target_day_artifacts = [dict(row) for row in bootstrap_result["artifacts"]]
        blocking_items = []
        missing_required_artifacts = []

    materialized_target_day_artifacts = _normalize_materialized_target_day_artifacts(materialized_target_day_artifacts)

    release = resolve_release_provenance()
    return {
        "schema_id": "fresh_day_admission",
        "schema_version": "v1",
        "target_day_utc": str(target_day_utc).strip(),
        "reference_day_utc": reference_day_utc,
        "admission_status": admission_status,
        "probe_status": probe_status,
        "probe_artifact_path": str(probe_path),
        "blocking_items": blocking_items,
        "required_target_day_artifacts": required_target_day_artifacts,
        "materialized_target_day_artifacts": materialized_target_day_artifacts,
        "missing_required_artifacts": missing_required_artifacts,
        "path_authority_snapshot": authority_snapshot,
        "release_id": str(release.get("release_id") or "").strip(),
        "git_sha": str(release.get("git_sha") or "").strip(),
        "truth_root": str(resolved_truth_root),
        "truth_sleeves_root": str(authority.canonical_runtime_truth_sleeves_root),
        "evaluated_at_utc": f"{target_day_utc}T00:00:00Z",
    }


def write_fresh_day_admission_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_fresh_day_admission_path(
            truth_root=truth_root,
            target_day_utc=str(payload.get("target_day_utc") or "").strip(),
        ),
        payload=payload,
        schema_relpath=FRESH_DAY_ADMISSION_SCHEMA_RELPATH,
        volatile_field_names=("evaluated_at_utc",),
    )
