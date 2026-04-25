from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import hashlib
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping
from zoneinfo import ZoneInfo

from constellation_2.common.attempt_history_v1 import (
    build_attempt_id_v1,
    resolve_day_attempt_artifact_path_v1,
)
from constellation_2.common.fresh_day_admission_v1 import (
    PAPER_BOOTSTRAP_MODE,
    PAPER_BOOTSTRAP_REASON,
    evaluate_paper_bootstrap_admission_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_validated_json_v1,
    atomic_write_idempotent_validated_json_v1,
)
from constellation_2.common.constitutional_runtime_v1 import (
    CLOSURE_STATE_BLOCKED,
    CLOSURE_STATE_COMPLETE,
    CLOSURE_STATE_OPEN,
    FINALITY_PROVISIONAL,
    assert_constitutional_writer_allowed_v1,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    build_governed_dependency_ref_v1,
    build_machine_blocker_envelope_v1,
    get_constitutional_artifact_contract_v1,
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1
from constellation_2.common.session_promotion_gate_v1 import (
    PROMOTION_STATE_PROMOTED,
    SessionPromotionDecisionRefV1,
)


TARGET_DAY_BUILD_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_build.v1.schema.json"
TARGET_DAY_ADMISSION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_admission.v1.schema.json"
ACTIVE_SESSION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/active_session.v1.schema.json"
SESSION_AUTHORITY_OWNER = "session_authority_v1"
ACTIVE_SESSION_ARTIFACT_FAMILY = "active_session_v1"
TARGET_DAY_ADMISSION_ARTIFACT_FAMILY = "target_day_admission_v1"
CLOSURE_STATUS_CLOSED = "CLOSED"
CLOSURE_STATUS_OPEN = "OPEN"
HIDDEN_DEPENDENCY_STATUS_PASS = "PASS"
HIDDEN_DEPENDENCY_STATUS_FAIL = "FAIL"
ROLLOVER_REASON_CODE_WITHHELD = "ROLLOVER_WITHHELD"


@dataclass(frozen=True)
class SessionAuthorityRefV1:
    path: Path
    payload: Dict[str, Any]
    sha256: str


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical_ref(path: Path, payload: Dict[str, Any]) -> SessionAuthorityRefV1:
    raw = (json_dumps(payload) + "\n").encode("utf-8")
    return SessionAuthorityRefV1(path=path, payload=payload, sha256=_sha256_bytes(raw))


def json_dumps(payload: Dict[str, Any]) -> str:
    import json

    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _read_surface_ref(path: Path, schema_relpath: str) -> SessionAuthorityRefV1:
    resolved_path = Path(path).resolve()
    if schema_relpath == TARGET_DAY_BUILD_SCHEMA_RELPATH:
        ref = read_control_plane_surface_v1(
            domain="session",
            surface="target_day_build",
            truth_root=resolved_path.parents[1],
            day_utc=resolved_path.stem,
        )
        return SessionAuthorityRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)
    if schema_relpath == TARGET_DAY_ADMISSION_SCHEMA_RELPATH:
        ref = read_control_plane_surface_v1(
            domain="session",
            surface="target_day_admission",
            truth_root=resolved_path.parents[1],
            day_utc=resolved_path.stem,
        )
        return SessionAuthorityRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)
    if schema_relpath == ACTIVE_SESSION_SCHEMA_RELPATH:
        ref = read_control_plane_surface_v1(
            domain="session",
            surface="active_session_current",
            truth_root=resolved_path.parents[1],
        )
        return SessionAuthorityRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)
    raise ValueError(f"SESSION_AUTHORITY_UNSUPPORTED_SURFACE_READ:schema={schema_relpath}:path={resolved_path}")


def resolve_session_authority_target_day_v1(
    target_day_utc: str | None = None,
    *,
    now: datetime | None = None,
) -> str:
    explicit = str(target_day_utc or "").strip()
    if explicit:
        return date.fromisoformat(explicit).isoformat()
    current = now or datetime.now(ZoneInfo("America/New_York"))
    if current.tzinfo is None:
        current = current.replace(tzinfo=ZoneInfo("America/New_York"))
    return current.astimezone(ZoneInfo("America/New_York")).date().isoformat()



def resolve_session_authority_tomorrow_target_day_v1(*, now: datetime | None = None) -> str:
    return _day_plus_one(resolve_session_authority_target_day_v1(now=now))

def resolve_target_day_build_path(*, truth_root: Path, target_day: str) -> Path:
    return (Path(truth_root).resolve() / "target_day_build_v1" / str(target_day).strip()).with_suffix(".json")


def resolve_target_day_build_attempt_path(*, truth_root: Path, target_day: str, attempt_id: str) -> Path:
    return resolve_day_attempt_artifact_path_v1(
        family_root=Path(truth_root).resolve() / "target_day_build_v1",
        day_utc=str(target_day).strip(),
        attempt_id=attempt_id,
        filename="target_day_build.v1.json",
    )


def resolve_target_day_admission_path(*, truth_root: Path, target_day: str) -> Path:
    return (Path(truth_root).resolve() / "target_day_admission_v1" / str(target_day).strip()).with_suffix(".json")


def resolve_target_day_admission_attempt_path(*, truth_root: Path, target_day: str, attempt_id: str) -> Path:
    return resolve_day_attempt_artifact_path_v1(
        family_root=Path(truth_root).resolve() / "target_day_admission_v1",
        day_utc=str(target_day).strip(),
        attempt_id=attempt_id,
        filename="target_day_admission.v1.json",
    )


def resolve_active_session_path(*, truth_root: Path) -> Path:
    return (Path(truth_root).resolve() / "active_session_v1" / "current.json").resolve()


def _build_ref(path: Path, sha256: str) -> Dict[str, str]:
    return {"artifact_path": str(path), "artifact_sha256": sha256}


def _constitutional_ref_from_dict(
    *,
    artifact_id: str,
    ref_dict: Mapping[str, Any] | None,
    finality_state: str = FINALITY_PROVISIONAL,
) -> Dict[str, Any] | None:
    path_text = str((ref_dict or {}).get("artifact_path") or "").strip()
    sha256 = str((ref_dict or {}).get("artifact_sha256") or "").strip()
    if not path_text or not sha256:
        return None
    return build_governed_dependency_ref_v1(
        repo_root=Path(__file__).resolve().parents[2],
        artifact_id=artifact_id,
        path=path_text,
        sha256=sha256,
        finality_state=finality_state,
    )


def _constitutional_ref_from_surface(
    *,
    artifact_id: str,
    ref: SessionAuthorityRefV1 | None,
    finality_state: str = FINALITY_PROVISIONAL,
) -> Dict[str, Any] | None:
    if ref is None:
        return None
    return build_governed_dependency_ref_v1(
        repo_root=Path(__file__).resolve().parents[2],
        artifact_id=artifact_id,
        path=ref.path,
        sha256=ref.sha256,
        finality_state=finality_state,
    )


def _finalize_target_day_admission_payload_v1(payload: Mapping[str, Any]) -> Dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[2]
    contract = get_constitutional_artifact_contract_v1(repo_root, TARGET_DAY_ADMISSION_ARTIFACT_FAMILY)
    build_dependency_ref = _constitutional_ref_from_dict(
        artifact_id="target_day_build_v1",
        ref_dict=payload.get("build_ref") if isinstance(payload.get("build_ref"), Mapping) else None,
    )
    missing_dependency_artifacts: List[str] = []
    if build_dependency_ref is None:
        missing_dependency_artifacts.append("target_day_build_v1")
    blocking_codes = [
        str(code).strip()
        for code in (payload.get("blocking_reason_codes") or [])
        if str(code).strip()
    ]
    admission_status = str(payload.get("admission_status") or "").strip().upper()
    hidden_dependency = payload.get("hidden_dependency_check_result")
    hidden_dependency_status = (
        str(hidden_dependency.get("status") or "").strip().upper()
        if isinstance(hidden_dependency, Mapping)
        else HIDDEN_DEPENDENCY_STATUS_FAIL
    )
    closure_state = CLOSURE_STATE_COMPLETE
    if missing_dependency_artifacts:
        closure_state = CLOSURE_STATE_BLOCKED
    elif str(payload.get("closure_status") or "").strip().upper() != CLOSURE_STATUS_CLOSED:
        closure_state = CLOSURE_STATE_OPEN
    elif admission_status != "ADMIT" or hidden_dependency_status != HIDDEN_DEPENDENCY_STATUS_PASS:
        closure_state = CLOSURE_STATE_BLOCKED
    blocker_envelope = build_machine_blocker_envelope_v1(
        closure_state=closure_state,
        reason_codes=blocking_codes,
        missing_dependency_artifacts=missing_dependency_artifacts,
    )
    dependency_refs = [build_dependency_ref] if build_dependency_ref is not None else []
    constitutional_dependency_declaration = build_artifact_dependency_declaration_v1(
        artifact_type=TARGET_DAY_ADMISSION_ARTIFACT_FAMILY,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=TARGET_DAY_ADMISSION_ARTIFACT_FAMILY,
        declared_dependency_artifacts=[
            str(item).strip()
            for item in (contract.get("required_upstream_dependencies") or [])
            if str(item).strip()
        ],
        dependency_refs=dependency_refs,
    )
    generated_at = str(payload.get("generated_utc") or "").strip()
    constitutional_lineage = build_governed_artifact_lineage_v1(
        artifact_type=TARGET_DAY_ADMISSION_ARTIFACT_FAMILY,
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=TARGET_DAY_ADMISSION_ARTIFACT_FAMILY,
        producer_id="constellation_2.common.session_authority_v1",
        generated_at_utc=generated_at,
        effective_at_utc=generated_at,
        finality_state=FINALITY_PROVISIONAL,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[],
        code_version="",
        run_id=f"target_day_admission:{str(payload.get('target_day') or '').strip()}",
    )
    normalized = dict(payload)
    normalized.update(blocker_envelope)
    normalized["constitutional_dependency_declaration"] = constitutional_dependency_declaration
    normalized["constitutional_lineage"] = constitutional_lineage
    return normalized


def _finalize_active_session_payload_v1(payload: Mapping[str, Any]) -> Dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[2]
    contract = get_constitutional_artifact_contract_v1(repo_root, ACTIVE_SESSION_ARTIFACT_FAMILY)
    admission_ref: SessionAuthorityRefV1 | None = None
    target_day_admission_path = str(payload.get("target_day_admission_ref") or "").strip()
    if target_day_admission_path:
        try:
            admission_ref = _read_surface_ref(Path(target_day_admission_path), TARGET_DAY_ADMISSION_SCHEMA_RELPATH)
        except Exception:
            admission_ref = None
    admission_dependency_ref = _constitutional_ref_from_surface(
        artifact_id="target_day_admission_v1",
        ref=admission_ref,
    )
    build_dependency_ref = _constitutional_ref_from_dict(
        artifact_id="target_day_build_v1",
        ref_dict=payload.get("target_day_build_ref") if isinstance(payload.get("target_day_build_ref"), Mapping) else None,
    )
    dependency_refs: List[Dict[str, Any]] = []
    if admission_dependency_ref is not None:
        dependency_refs.append(admission_dependency_ref)
    if build_dependency_ref is not None:
        dependency_refs.append(build_dependency_ref)
    missing_dependency_artifacts: List[str] = []
    if admission_dependency_ref is None:
        missing_dependency_artifacts.append("target_day_admission_v1")
    if build_dependency_ref is None:
        missing_dependency_artifacts.append("target_day_build_v1")
    blocking_codes = [
        str(code).strip()
        for code in (payload.get("blocked_reason_codes") or [])
        if str(code).strip()
    ]
    if not blocking_codes:
        rollover_reason_code = str(payload.get("rollover_reason_code") or "").strip()
        if rollover_reason_code:
            blocking_codes.append(rollover_reason_code)
    closure_state = CLOSURE_STATE_COMPLETE
    if missing_dependency_artifacts:
        closure_state = CLOSURE_STATE_BLOCKED
    elif str(payload.get("promotion_state") or "").strip().upper() != PROMOTION_STATE_PROMOTED:
        closure_state = CLOSURE_STATE_BLOCKED
    blocker_envelope = build_machine_blocker_envelope_v1(
        closure_state=closure_state,
        reason_codes=blocking_codes,
        missing_dependency_artifacts=missing_dependency_artifacts,
    )
    constitutional_dependency_declaration = build_artifact_dependency_declaration_v1(
        artifact_type=ACTIVE_SESSION_ARTIFACT_FAMILY,
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=ACTIVE_SESSION_ARTIFACT_FAMILY,
        declared_dependency_artifacts=[
            str(item).strip()
            for item in (contract.get("required_upstream_dependencies") or [])
            if str(item).strip()
        ],
        dependency_refs=dependency_refs,
    )
    generated_at = str(payload.get("generated_utc") or "").strip()
    constitutional_lineage = build_governed_artifact_lineage_v1(
        artifact_type=ACTIVE_SESSION_ARTIFACT_FAMILY,
        artifact_version="v1",
        artifact_class=str(contract.get("artifact_class") or "").strip(),
        authority_id=ACTIVE_SESSION_ARTIFACT_FAMILY,
        producer_id="constellation_2.common.session_authority_v1",
        generated_at_utc=generated_at,
        effective_at_utc=generated_at,
        finality_state=FINALITY_PROVISIONAL,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[],
        code_version="",
        run_id=f"active_session:{str(payload.get('target_day') or '').strip()}",
    )
    normalized = dict(payload)
    normalized.update(blocker_envelope)
    normalized["constitutional_dependency_declaration"] = constitutional_dependency_declaration
    normalized["constitutional_lineage"] = constitutional_lineage
    return normalized


def _day_plus_one(day_utc: str) -> str:
    return (date.fromisoformat(str(day_utc).strip()) + timedelta(days=1)).isoformat()


def _day_minus_one(day_utc: str) -> str:
    return (date.fromisoformat(str(day_utc).strip()) - timedelta(days=1)).isoformat()


def _normalized_blocker_codes(row: Mapping[str, Any]) -> List[str]:
    codes = [str(code).strip() for code in (row.get("blocker_codes") or []) if str(code).strip()]
    if codes:
        return codes
    artifact_id = str(row.get("artifact_id") or "target_day_artifact").strip().upper()
    result_status = str(row.get("result_status") or "FAIL").strip().upper()
    return [f"{artifact_id}_{result_status}"]


def _normalized_provenance_summary(row: Mapping[str, Any]) -> Dict[str, Any]:
    summary = row.get("provenance_summary")
    if isinstance(summary, dict):
        return {
            "required": bool(summary.get("required", False)),
            "present": bool(summary.get("present", False)),
            "fields_present": [
                str(item).strip()
                for item in (summary.get("fields_present") or [])
                if str(item).strip()
            ],
            "source": str(summary.get("source") or "").strip(),
        }
    return {
        "required": bool(row.get("provenance_required", False)),
        "present": False,
        "fields_present": [],
        "source": "",
    }


def _normalized_observed_dependencies(row: Mapping[str, Any]) -> List[str]:
    return sorted(
        {
            str(item).strip()
            for item in (row.get("observed_dependency_artifacts") or [])
            if str(item).strip()
        }
    )


def _normalized_hidden_dependency_check_result(
    hidden_dependency_check_result: Mapping[str, Any] | None,
    *,
    declared_artifacts: Iterable[str],
) -> Dict[str, Any]:
    payload = dict(hidden_dependency_check_result or {})
    status = str(payload.get("status") or HIDDEN_DEPENDENCY_STATUS_PASS).strip().upper()
    if status not in {HIDDEN_DEPENDENCY_STATUS_PASS, HIDDEN_DEPENDENCY_STATUS_FAIL}:
        status = HIDDEN_DEPENDENCY_STATUS_FAIL
    undeclared = sorted(
        {
            str(item).strip()
            for item in (payload.get("undeclared_dependency_artifacts") or [])
            if str(item).strip()
        }
    )
    failing_producers = [
        str(item).strip()
        for item in (payload.get("failing_producers") or [])
        if str(item).strip()
    ]
    return {
        "status": status if not undeclared else HIDDEN_DEPENDENCY_STATUS_FAIL,
        "blocking_reason_code": str(payload.get("blocking_reason_code") or "HIDDEN_DEPENDENCY_DETECTED").strip(),
        "summary": str(payload.get("summary") or "").strip(),
        "declared_inventory_artifacts": sorted({str(item).strip() for item in declared_artifacts if str(item).strip()}),
        "observed_dependency_artifacts": sorted(
            {
                str(item).strip()
                for item in (payload.get("observed_dependency_artifacts") or [])
                if str(item).strip()
            }
        ),
        "undeclared_dependency_artifacts": undeclared,
        "failing_producers": list(dict.fromkeys(failing_producers)),
    }


def _source_ref_blocks_build(ref: Mapping[str, Any]) -> bool:
    if not isinstance(ref, Mapping):
        return False
    if str(ref.get("script") or "").strip() in {"", "source_ref"}:
        return False
    if bool(ref.get("required_for_closure") is False):
        return False
    return int(ref.get("return_code") or 0) != 0


def _normalize_artifact_result(row: Mapping[str, Any], *, target_day: str) -> Dict[str, Any]:
    artifact_id = str(row.get("artifact_id") or row.get("artifact_name") or "").strip()
    canonical_path = str(row.get("canonical_path") or row.get("authority_path") or "").strip()
    target_day_expected = str(row.get("target_day_expected") or target_day).strip()
    target_day_observed = str(
        row.get("target_day_observed")
        or row.get("date_binding_value")
        or row.get("target_day")
        or row.get("day_utc")
        or ""
    ).strip()
    blocking_reason_code = str(row.get("blocking_reason_code") or "").strip()
    normalized = {
        "artifact_id": artifact_id,
        "artifact_name": str(row.get("artifact_name") or artifact_id).strip(),
        "required": bool(row.get("required", True)),
        "role_class": str(row.get("role_class") or "REQUIRED_DERIVED_GATE").strip(),
        "classification": str(row.get("classification") or "REQUIRED_ARTIFACT_BLOCKER").strip(),
        "canonical_path": canonical_path,
        "authority_path": canonical_path,
        "path_family": str(row.get("path_family") or "").strip(),
        "observed_status": str(row.get("observed_status") or "").strip(),
        "result_status": str(row.get("result_status") or "").strip().upper() or "FAIL",
        "blocker_codes": _normalized_blocker_codes(row),
        "blocking_reason_code": blocking_reason_code or (_normalized_blocker_codes(row)[0] if _normalized_blocker_codes(row) else ""),
        "schema_status": str(row.get("schema_status") or "").strip() or "UNKNOWN",
        "schema_ref": str(row.get("schema_ref") or "").strip(),
        "freshness_rule": str(row.get("freshness_rule") or "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT").strip(),
        "freshness_status": str(row.get("freshness_status") or "").strip() or "UNKNOWN",
        "target_day_expected": target_day_expected,
        "target_day_observed": target_day_observed,
        "date_binding_status": str(row.get("date_binding_status") or "").strip() or "UNKNOWN",
        "date_binding_value": target_day_observed,
        "provenance_required": bool(row.get("provenance_required", False)),
        "provenance_summary": _normalized_provenance_summary(row),
        "closure_status": str(row.get("closure_status") or "").strip().upper() or CLOSURE_STATUS_OPEN,
        "producer": dict(row.get("producer") or {"module": "", "git_sha": ""}),
        "source_refs": [dict(item) for item in (row.get("source_refs") or []) if isinstance(item, dict)],
        "observed_dependency_artifacts": _normalized_observed_dependencies(row),
    }
    return normalized


def derive_target_day_build_payload_v1(
    *,
    truth_root: Path,
    target_day: str,
    artifact_results: Iterable[Mapping[str, Any]],
    source_refs: Iterable[Mapping[str, Any]] | None = None,
    hidden_dependency_check_result: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    normalized_target_day = resolve_session_authority_target_day_v1(target_day)
    normalized_results = [_normalize_artifact_result(row, target_day=normalized_target_day) for row in artifact_results]
    declared_required_artifacts = [
        str(row.get("artifact_id") or "").strip()
        for row in normalized_results
        if bool(row.get("required", True))
    ]
    required_artifacts = [
        {
            "artifact_id": str(row.get("artifact_id") or "").strip(),
            "artifact_name": str(row.get("artifact_name") or row.get("artifact_id") or "").strip(),
            "role_class": str(row.get("role_class") or "").strip(),
            "required_status": "PASS",
            "canonical_path": str(row.get("canonical_path") or row.get("authority_path") or "").strip(),
            "authority_path": str(row.get("canonical_path") or row.get("authority_path") or "").strip(),
            "path_family": str(row.get("path_family") or "").strip(),
            "target_day_expected": str(row.get("target_day_expected") or normalized_target_day).strip(),
            "freshness_rule": str(row.get("freshness_rule") or "").strip(),
            "provenance_required": bool(row.get("provenance_required", False)),
        }
        for row in normalized_results
        if bool(row.get("required", True))
    ]
    blocker_chain: List[Dict[str, Any]] = []
    for row in normalized_results:
        if (
            str(row.get("result_status") or "").strip().upper() == "PASS"
            and str(row.get("closure_status") or "").strip().upper() == CLOSURE_STATUS_CLOSED
        ):
            continue
        if not bool(row.get("required", True)):
            continue
        blocker_codes = [str(row.get("blocking_reason_code") or "").strip()] if str(row.get("blocking_reason_code") or "").strip() else _normalized_blocker_codes(row)
        for blocker_code in blocker_codes:
            blocker_chain.append(
                {
                    "artifact_id": str(row.get("artifact_id") or "").strip(),
                    "artifact_name": str(row.get("artifact_name") or row.get("artifact_id") or "").strip(),
                    "blocker_code": blocker_code,
                    "classification": str(row.get("classification") or "REQUIRED_ARTIFACT_BLOCKER").strip(),
                    "artifact_path": str(row.get("canonical_path") or row.get("authority_path") or "").strip(),
                    "role_class": str(row.get("role_class") or "").strip(),
                }
            )
    partial_build = any(_source_ref_blocks_build(row) for row in (source_refs or []))
    hidden_dependency_result = _normalized_hidden_dependency_check_result(
        hidden_dependency_check_result,
        declared_artifacts=declared_required_artifacts,
    )
    if hidden_dependency_result["status"] != HIDDEN_DEPENDENCY_STATUS_PASS:
        blocker_chain.append(
            {
                "artifact_id": "target_day_build_v1",
                "artifact_name": "target_day_build_v1",
                "blocker_code": str(hidden_dependency_result["blocking_reason_code"]).strip() or "HIDDEN_DEPENDENCY_DETECTED",
                "classification": "CONTROL_BOUNDARY_FAILURE",
                "artifact_path": str(resolve_target_day_build_path(truth_root=truth_root, target_day=normalized_target_day)),
                "role_class": "REQUIRED_EXECUTION_BOUNDARY",
            }
        )
    if partial_build:
        blocker_chain.append(
            {
                "artifact_id": "target_day_build_v1",
                "artifact_name": "target_day_build_v1",
                "blocker_code": "PARTIAL_BUILD",
                "classification": "CONTROL_BOUNDARY_FAILURE",
                "artifact_path": str(resolve_target_day_build_path(truth_root=truth_root, target_day=normalized_target_day)),
                "role_class": "REQUIRED_EXECUTION_BOUNDARY",
            }
        )
    completeness_result = (
        "COMPLETE"
        if all(
            str(row.get("result_status") or "").strip().upper() == "PASS"
            for row in normalized_results
            if bool(row.get("required", True))
        )
        else "INCOMPLETE"
    )
    closure_status = (
        CLOSURE_STATUS_CLOSED
        if completeness_result == "COMPLETE"
        and hidden_dependency_result["status"] == HIDDEN_DEPENDENCY_STATUS_PASS
        and all(
            str(row.get("closure_status") or "").strip().upper() == CLOSURE_STATUS_CLOSED
            for row in normalized_results
            if bool(row.get("required", True))
        )
        else CLOSURE_STATUS_OPEN
    )
    return {
        "schema_id": "target_day_build",
        "schema_version": "v1",
        "generated_utc": _utc_now(),
        "target_day": normalized_target_day,
        "authority_root": str(Path(truth_root).resolve()),
        "build_status": "COMPLETE" if completeness_result == "COMPLETE" and closure_status == CLOSURE_STATUS_CLOSED else "BLOCKED",
        "required_artifacts": required_artifacts,
        "artifact_results": normalized_results,
        "blocker_chain": blocker_chain,
        "completeness_result": completeness_result,
        "closure_status": closure_status,
        "hidden_dependency_check_result": hidden_dependency_result,
        "source_refs": [dict(row) for row in (source_refs or [])],
    }


def write_target_day_build_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SessionAuthorityRefV1:
    target_day = str(payload.get("target_day") or "").strip()
    attempt_id = build_attempt_id_v1(payload=payload)
    atomic_write_validated_json_v1(
        path=resolve_target_day_build_attempt_path(
            truth_root=truth_root,
            target_day=target_day,
            attempt_id=attempt_id,
        ),
        payload=payload,
        schema_relpath=TARGET_DAY_BUILD_SCHEMA_RELPATH,
    )
    path = resolve_target_day_build_path(
        truth_root=truth_root,
        target_day=target_day,
    )
    ref = atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=TARGET_DAY_BUILD_SCHEMA_RELPATH,
        volatile_field_names=("generated_utc",),
    )
    return SessionAuthorityRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)


def read_target_day_build_ref_v1(*, truth_root: Path, target_day: str) -> SessionAuthorityRefV1:
    ref = read_control_plane_surface_v1(
        domain="session",
        surface="target_day_build",
        truth_root=Path(truth_root).resolve(),
        day_utc=target_day,
    )
    return SessionAuthorityRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)


def derive_target_day_admission_payload_v1(
    *,
    truth_root: Path,
    target_day: str,
    build_ref: SessionAuthorityRefV1,
    prior_active_session_ref: SessionAuthorityRefV1 | None = None,
    enforce_consistency_gate: bool = False,
    rules_version: str = "canonical_session_authority_v1",
    environment: str = "",
    repo_root: Path | None = None,
) -> Dict[str, Any]:
    from constellation_2.common.next_day_readiness_consistency_gate_v1 import (
        CONSISTENCY_GATE_FAILURE,
        CONSISTENCY_GATE_STATUS_FAIL,
        evaluate_next_day_readiness_consistency_gate_v1,
    )

    build_payload = dict(build_ref.payload)
    blocker_chain = [dict(row) for row in (build_payload.get("blocker_chain") or []) if isinstance(row, dict)]
    completeness = str(build_payload.get("completeness_result") or "").strip().upper()
    closure_status = str(build_payload.get("closure_status") or "").strip().upper()
    hidden_dependency_result = dict(build_payload.get("hidden_dependency_check_result") or {})
    hidden_dependency_status = str(hidden_dependency_result.get("status") or "").strip().upper()
    normal_admission_status = (
        "ADMIT"
        if completeness == "COMPLETE"
        and closure_status == CLOSURE_STATUS_CLOSED
        and hidden_dependency_status == HIDDEN_DEPENDENCY_STATUS_PASS
        and not blocker_chain
        else "BLOCKED"
    )
    bootstrap_result: Dict[str, Any] = {"eligible": False}
    if normal_admission_status != "ADMIT":
        bootstrap_result = evaluate_paper_bootstrap_admission_v1(
            repo_root=Path(repo_root).resolve() if repo_root is not None else Path(__file__).resolve().parents[2],
            target_day_utc=resolve_session_authority_target_day_v1(target_day),
            environment=environment,
        )
    bootstrap_admission = bool(bootstrap_result.get("eligible"))
    admission_status = "ADMIT" if normal_admission_status == "ADMIT" or bootstrap_admission else "BLOCKED"
    blocking_reason_codes = (
        []
        if bootstrap_admission
        else sorted(
            {
                str(row.get("blocker_code") or "").strip()
                for row in blocker_chain
                if str(row.get("blocker_code") or "").strip()
            }
        )
    )
    if admission_status != "ADMIT" and not blocking_reason_codes:
        blocking_reason_codes = ["ADMISSION_RULE_BLOCKED"]
    payload = {
        "schema_id": "target_day_admission",
        "schema_version": "v1",
        "generated_utc": _utc_now(),
        "target_day": resolve_session_authority_target_day_v1(target_day),
        "admission_status": admission_status,
        "blocker_chain": [] if bootstrap_admission else blocker_chain,
        "blocking_reason_codes": blocking_reason_codes,
        "build_ref": _build_ref(build_ref.path, build_ref.sha256),
        "closure_status": closure_status or CLOSURE_STATUS_OPEN,
        "hidden_dependency_check_result": hidden_dependency_result,
        "rules_version": str(rules_version).strip(),
        "binding": True,
    }
    if bootstrap_admission:
        payload["mode"] = PAPER_BOOTSTRAP_MODE
        payload["reason"] = PAPER_BOOTSTRAP_REASON
        return payload
    if not enforce_consistency_gate or admission_status != "ADMIT":
        return payload

    prior_active_ref = prior_active_session_ref
    if prior_active_ref is None:
        try:
            prior_active_ref = read_active_session_ref_v1(truth_root=truth_root)
        except Exception:
            prior_active_ref = None
    candidate_admission_ref = _canonical_ref(
        resolve_target_day_admission_path(truth_root=truth_root, target_day=payload["target_day"]),
        payload,
    )
    candidate_active_payload = derive_active_session_payload_v1(
        truth_root=truth_root,
        target_day=payload["target_day"],
        admission_ref=candidate_admission_ref,
        prior_active_session_ref=prior_active_ref,
    )
    consistency_result = evaluate_next_day_readiness_consistency_gate_v1(
        truth_root=Path(truth_root).resolve(),
        day_utc=str(payload["target_day"]),
        surface_scope="admission",
        build_payload=build_payload,
        admission_payload=payload,
        active_session_payload=candidate_active_payload,
    )
    if consistency_result.status != CONSISTENCY_GATE_STATUS_FAIL:
        return payload

    first_issue = consistency_result.issues[0] if consistency_result.issues else None
    blocker_chain.append(
        {
            "artifact_id": "next_day_readiness_consistency_gate_v1",
            "artifact_name": "next_day_readiness_consistency_gate_v1",
            "blocker_code": CONSISTENCY_GATE_FAILURE,
            "classification": "CONTROL_BOUNDARY_FAILURE",
            "artifact_path": first_issue.artifact_path if first_issue is not None and first_issue.artifact_path else str(candidate_admission_ref.path),
            "role_class": "REQUIRED_EXECUTION_BOUNDARY",
        }
    )
    blocking_reason_codes = sorted(
        {
            *blocking_reason_codes,
            CONSISTENCY_GATE_FAILURE,
        }
    )
    payload["admission_status"] = "BLOCKED"
    payload["blocker_chain"] = blocker_chain
    payload["blocking_reason_codes"] = blocking_reason_codes
    return payload


def write_target_day_admission_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SessionAuthorityRefV1:
    assert_constitutional_writer_allowed_v1(
        Path(__file__).resolve().parents[2],
        TARGET_DAY_ADMISSION_ARTIFACT_FAMILY,
        "constellation_2.common.session_authority_v1",
    )
    payload = _finalize_target_day_admission_payload_v1(payload)
    validate_governed_artifact_payload_v1(
        repo_root=Path(__file__).resolve().parents[2],
        artifact_id=TARGET_DAY_ADMISSION_ARTIFACT_FAMILY,
        payload=payload,
        required_finality_states=["provisional", "finalized", "corrected"],
    )
    target_day = str(payload.get("target_day") or "").strip()
    attempt_id = build_attempt_id_v1(payload=payload)
    atomic_write_validated_json_v1(
        path=resolve_target_day_admission_attempt_path(
            truth_root=truth_root,
            target_day=target_day,
            attempt_id=attempt_id,
        ),
        payload=payload,
        schema_relpath=TARGET_DAY_ADMISSION_SCHEMA_RELPATH,
    )
    path = resolve_target_day_admission_path(
        truth_root=truth_root,
        target_day=target_day,
    )
    ref = atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=TARGET_DAY_ADMISSION_SCHEMA_RELPATH,
        volatile_field_names=("generated_utc",),
    )
    return SessionAuthorityRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)


def read_target_day_admission_ref_v1(*, truth_root: Path, target_day: str) -> SessionAuthorityRefV1:
    ref = read_control_plane_surface_v1(
        domain="session",
        surface="target_day_admission",
        truth_root=Path(truth_root).resolve(),
        day_utc=target_day,
    )
    return SessionAuthorityRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)


def read_active_session_ref_v1(*, truth_root: Path) -> SessionAuthorityRefV1:
    ref = read_control_plane_surface_v1(
        domain="session",
        surface="active_session_current",
        truth_root=Path(truth_root).resolve(),
    )
    return SessionAuthorityRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)


def derive_active_session_payload_v1(
    *,
    truth_root: Path,
    target_day: str,
    admission_ref: SessionAuthorityRefV1,
    prior_active_session_ref: SessionAuthorityRefV1 | None = None,
    authority_owner: str = SESSION_AUTHORITY_OWNER,
    promotion_ref: SessionPromotionDecisionRefV1 | None = None,
) -> Dict[str, Any]:
    normalized_target_day = resolve_session_authority_target_day_v1(target_day)
    prior_payload = dict(prior_active_session_ref.payload) if prior_active_session_ref is not None else {}
    prior_active_day = str(prior_payload.get("active_day") or "").strip()
    prior_prior_day = str(prior_payload.get("prior_day") or "").strip()
    prior_active_day_admission_ref = str(prior_payload.get("active_day_admission_ref") or "").strip()
    prior_active_day_build_ref = dict(prior_payload.get("active_day_build_ref") or {})
    admission_status = str(admission_ref.payload.get("admission_status") or "").strip().upper()
    blocker_chain = [dict(row) for row in (admission_ref.payload.get("blocker_chain") or []) if isinstance(row, dict)]
    build_ref = dict(admission_ref.payload.get("build_ref") or {})
    promotion_payload = dict(promotion_ref.payload) if promotion_ref is not None else {}
    promotion_state = str(promotion_payload.get("promotion_state") or "").strip().upper()
    rollover_reason_code = blocker_chain[0]["blocker_code"] if blocker_chain else ""
    rollover_reason_summary = rollover_reason_code
    blocked_reason_codes = sorted(
        {
            str(code).strip()
            for code in (admission_ref.payload.get("blocking_reason_codes") or [])
            if str(code).strip()
        }
    )
    if not promotion_state:
        promotion_state = PROMOTION_STATE_PROMOTED if admission_status == "ADMIT" else "BLOCKED"
    promotion_blocking_reason_codes = sorted(
        {
            str(code).strip()
            for code in (promotion_payload.get("blocked_reason_codes") or [])
            if str(code).strip()
        }
    )
    if promotion_state == PROMOTION_STATE_PROMOTED:
        active_day = normalized_target_day
        prior_day = prior_active_day if prior_active_day and prior_active_day != active_day else _day_minus_one(active_day)
        rollover_status = "ACTIVE_SESSION_CONFIRMED" if prior_active_day == active_day else "ROLLOVER_COMPLETED"
        active_day_admission_ref = str(admission_ref.path)
        active_day_build_ref = build_ref
        rollover_reason_code = ""
        rollover_reason_summary = ""
        blocked_target_day = ""
        blocked_admission_ref = ""
        blocked_reason_codes = []
    else:
        active_day = prior_active_day
        prior_day = prior_prior_day or (_day_minus_one(prior_active_day) if prior_active_day else "")
        rollover_status = "ROLLOVER_WITHHELD"
        active_day_admission_ref = prior_active_day_admission_ref
        active_day_build_ref = prior_active_day_build_ref or build_ref
        blocked_target_day = normalized_target_day
        blocked_admission_ref = str(admission_ref.path)
        blocked_reason_codes = promotion_blocking_reason_codes or blocked_reason_codes
        if blocked_reason_codes:
            rollover_reason_code = blocked_reason_codes[0]
        if not rollover_reason_code:
            rollover_reason_code = ROLLOVER_REASON_CODE_WITHHELD
        rollover_reason_summary = rollover_reason_code
    return {
        "schema_id": "active_session",
        "schema_version": "v1",
        "generated_utc": _utc_now(),
        "target_day": normalized_target_day,
        "active_day": active_day,
        "prior_day": prior_day,
        "next_target_day": _day_plus_one(normalized_target_day) if admission_status == "ADMIT" else normalized_target_day,
        "active_day_admission_ref": active_day_admission_ref,
        "active_day_build_ref": active_day_build_ref,
        "target_day_admission_ref": str(admission_ref.path),
        "target_day_build_ref": build_ref,
        "target_day_admission_status": admission_status,
        "rollover_status": rollover_status,
        "rollover_reason": rollover_reason_summary,
        "rollover_reason_code": rollover_reason_code,
        "rollover_reason_summary": rollover_reason_summary,
        "blocked_target_day": blocked_target_day,
        "blocked_admission_ref": blocked_admission_ref,
        "blocked_reason_codes": blocked_reason_codes,
        "authority_owner": str(authority_owner).strip() or SESSION_AUTHORITY_OWNER,
        "promotion_state": promotion_state,
        "promotion_decision_ref": str(promotion_ref.path) if promotion_ref is not None else "",
    }


def write_active_session_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SessionAuthorityRefV1:
    assert_constitutional_writer_allowed_v1(
        Path(__file__).resolve().parents[2],
        ACTIVE_SESSION_ARTIFACT_FAMILY,
        "constellation_2.common.session_authority_v1",
    )
    payload = _finalize_active_session_payload_v1(payload)
    validate_governed_artifact_payload_v1(
        repo_root=Path(__file__).resolve().parents[2],
        artifact_id=ACTIVE_SESSION_ARTIFACT_FAMILY,
        payload=payload,
        required_finality_states=["provisional", "finalized", "corrected"],
    )
    path = resolve_active_session_path(truth_root=truth_root)
    ref = atomic_write_idempotent_validated_json_v1(
        path=path,
        payload=payload,
        schema_relpath=ACTIVE_SESSION_SCHEMA_RELPATH,
        volatile_field_names=("generated_utc",),
    )
    return SessionAuthorityRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)
