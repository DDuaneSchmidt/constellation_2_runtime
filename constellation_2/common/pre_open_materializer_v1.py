from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from constellation_2.common.attempt_history_v1 import (
    build_attempt_id_v1,
    resolve_day_attempt_artifact_path_v1,
)
from constellation_2.common.kill_switch_authority_v1 import (
    RC_KILL_SWITCH_CANONICAL_MISSING,
    STATUS_PASS as KILL_SWITCH_STATUS_PASS,
    resolve_kill_switch_authority_v1,
)
from constellation_2.common.market_calendar_coverage_authority_v1 import read_market_calendar_coverage_status_ref_v1
from constellation_2.common.control_plane_read_gateway_v1 import read_control_plane_surface_v1
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    atomic_write_validated_json_v1,
    now_utc_iso_v1,
    read_json_object_v1,
    read_validated_surface_v1,
    repo_git_sha_v1,
    sha256_file_v1,
)
from constellation_2.common.runtime_path_authority_v1 import classify_runtime_path_v1
from constellation_2.common.trade_submit_readiness_authority_v1 import (
    resolve_governed_sleeve_truth_bindings,
    resolve_pointer_bound_handshake_state,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
PRE_OPEN_BUNDLE_SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/pre_open_bundle.v1.schema.json"
PRE_OPEN_BUNDLE_FAMILY_V1 = "pre_open_bundle_v1"
PRE_OPEN_BUNDLE_FILENAME_V1 = "pre_open_bundle.v1.json"

ROLE_REQUIRED_BINDING_INPUT = "REQUIRED_BINDING_INPUT"
ROLE_REQUIRED_EXECUTION_BOUNDARY = "REQUIRED_EXECUTION_BOUNDARY"

REASON_TARGET_DAY_ARTIFACT_MISSING = "TARGET_DAY_ARTIFACT_MISSING"
REASON_TARGET_DAY_DATE_MISMATCH = "TARGET_DAY_DATE_MISMATCH"
REASON_STALE_ARTIFACT = "STALE_ARTIFACT"
REASON_WRONG_AUTHORITY_PATH = "WRONG_AUTHORITY_PATH"
REASON_SCHEMA_INVALID = "SCHEMA_INVALID"
REASON_PROVENANCE_MISSING = "PROVENANCE_MISSING"
REASON_REQUIRED_GATE_FAIL = "REQUIRED_GATE_FAIL"

HANDSHAKE_EXTERNAL_UNAVAILABLE_CODES = frozenset(
    {
        "BROKER_EVENTS_MISSING",
        "NO_NEXT_VALID_ID_OBSERVED",
        "NOT_CONNECTED_AFTER_HANDSHAKE",
        "IB_API_HANDSHAKE_POINTER_MISSING",
        "IB_API_HANDSHAKE_POINTER_TARGET_MISSING",
        "IB_API_HANDSHAKE_ARTIFACT_MISSING",
    }
)
HANDSHAKE_STALE_CODES = frozenset(
    {
        "IB_API_HANDSHAKE_STALE_POINTER",
        "IB_API_HANDSHAKE_STALE_ARTIFACT",
    }
)

CANONICAL_PATH_PREFIX = "CANONICAL_RUNTIME_TRUTH"
MATERIALIZATION_COMPLETE = "COMPLETE"
MATERIALIZATION_INCOMPLETE = "INCOMPLETE"
MATERIALIZATION_BLOCKED = "BLOCKED"

PRODUCER_RESULT_PASS = "PASS"
PRODUCER_RESULT_FAIL = "FAIL"
PRODUCER_RESULT_STALE = "STALE"
PRODUCER_RESULT_MISMATCH = "MISMATCH"
PRODUCER_RESULT_BLOCKED = "BLOCKED"
PRODUCER_RESULT_UNAVAILABLE = "UNAVAILABLE"

PRODUCER_RESULT_CURRENT = "CURRENT"
PRODUCER_RESULT_UNKNOWN = "UNKNOWN"

PRODUCER_ARTIFACT_IDS_BY_SCRIPT = {
    "ops/tools/run_ib_api_handshake_spine_v1.py": (
        "ib_api_handshake_latest_pointer_v1",
        "ib_api_handshake_v1",
    ),
    "ops/tools/run_pointer_heads_materialize_v1.py": (
        "primary_scoped_canonical_authority_head_v1",
    ),
    "ops/tools/run_global_kill_switch_v1.py": (
        "global_kill_switch_state_v1",
    ),
}


@dataclass(frozen=True)
class PreOpenBundleRefV1:
    path: Path
    payload: Dict[str, Any]
    sha256: str


def resolve_pre_open_bundle_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / PRE_OPEN_BUNDLE_FAMILY_V1
        / str(day_utc).strip()
        / PRE_OPEN_BUNDLE_FILENAME_V1
    ).resolve()


def resolve_pre_open_bundle_attempt_path_v1(*, truth_root: Path, day_utc: str, attempt_id: str) -> Path:
    family_root = (Path(truth_root).resolve() / "reports" / PRE_OPEN_BUNDLE_FAMILY_V1).resolve()
    return resolve_day_attempt_artifact_path_v1(
        family_root=family_root,
        day_utc=day_utc,
        attempt_id=attempt_id,
        filename=PRE_OPEN_BUNDLE_FILENAME_V1,
    )


def _path_family(path: Path) -> str:
    return str(classify_runtime_path_v1(path).get("path_class") or "UNKNOWN")


def _path_family_is_canonical(path_family: str) -> bool:
    return str(path_family).strip().upper().startswith(CANONICAL_PATH_PREFIX)


def _producer_block(payload: Mapping[str, Any]) -> Dict[str, str]:
    producer = payload.get("producer")
    if isinstance(producer, Mapping):
        return {
            "module": str(producer.get("module") or "").strip(),
            "git_sha": str(producer.get("git_sha") or "").strip(),
        }
    run_metadata = payload.get("run_metadata")
    if isinstance(run_metadata, Mapping):
        return {
            "module": str(run_metadata.get("producer_module") or "").strip(),
            "git_sha": str(run_metadata.get("producer_git_sha") or "").strip(),
        }
    return {"module": "", "git_sha": ""}


def _extract_payload_day(payload: Mapping[str, Any]) -> str:
    for field in ("target_day", "target_day_utc", "day_utc", "trading_day", "session_date"):
        value = str(payload.get(field) or "").strip()
        if value:
            return value
    return ""


def _extract_timestamp(payload: Mapping[str, Any]) -> str:
    for field in (
        "generated_at_utc",
        "generated_utc",
        "produced_at_utc",
        "produced_utc",
        "evaluated_at_utc",
        "emitted_at",
        "as_of_utc",
        "built_at_utc",
    ):
        value = str(payload.get(field) or "").strip()
        if value:
            return value
    return ""


def _provenance_summary(*, payload: Mapping[str, Any], required: bool) -> Dict[str, Any]:
    fields_present: List[str] = []
    source = ""

    if isinstance(payload.get("producer"), Mapping):
        producer = payload["producer"]
        if str(producer.get("module") or "").strip():
            fields_present.append("producer.module")
        if str(producer.get("git_sha") or "").strip():
            fields_present.append("producer.git_sha")
        if fields_present:
            source = source or "producer"

    for field in (
        "generated_at_utc",
        "generated_utc",
        "produced_at_utc",
        "produced_utc",
        "evaluated_at_utc",
        "emitted_at",
        "as_of_utc",
        "built_at_utc",
    ):
        if str(payload.get(field) or "").strip():
            fields_present.append(field)

    if isinstance(payload.get("input_manifest"), list) and payload.get("input_manifest"):
        fields_present.append("input_manifest")
    if isinstance(payload.get("source_dependencies"), list) and payload.get("source_dependencies"):
        fields_present.append("source_dependencies")
    if isinstance(payload.get("source_artifacts"), list) and payload.get("source_artifacts"):
        fields_present.append("source_artifacts")

    return {
        "required": bool(required),
        "present": bool(fields_present),
        "fields_present": sorted(set(fields_present)),
        "source": source,
    }


def _freshness_status(
    *,
    payload: Mapping[str, Any],
    target_day: str,
    observed_day: str,
    freshness_rule: str,
) -> str:
    rule = str(freshness_rule or "").strip().upper()
    timestamp = _extract_timestamp(payload)
    payload_freshness = str(payload.get("freshness_verdict") or "").strip().upper()
    if rule == "TARGET_DAY_MATCH_ONLY":
        return "CURRENT" if observed_day == target_day else "STALE"
    if rule == "DECLARED_CURRENT":
        if payload_freshness == "CURRENT" and observed_day == target_day:
            return "CURRENT"
        return "STALE"
    if rule == "TARGET_DAY_COVERAGE_AND_SOURCE_TIMESTAMP":
        return "CURRENT" if observed_day == target_day and bool(timestamp) else "STALE"
    return "CURRENT" if observed_day == target_day and bool(timestamp) else "STALE"


def _blocking_reason_code(
    *,
    explicit_reason_code: str,
    path_family: str,
    schema_status: str,
    date_binding_status: str,
    freshness_status: str,
    provenance_summary: Mapping[str, Any],
    result_status: str,
) -> str:
    if explicit_reason_code:
        return explicit_reason_code
    if not _path_family_is_canonical(path_family):
        return REASON_WRONG_AUTHORITY_PATH
    if str(schema_status).strip().upper() not in {"VALID", "VALIDATED_BY_LOADER", "PRACTICAL_VALID"}:
        return REASON_SCHEMA_INVALID
    if str(date_binding_status).strip().upper() == "MISMATCH":
        return REASON_TARGET_DAY_DATE_MISMATCH
    if str(freshness_status).strip().upper() != "CURRENT":
        return REASON_STALE_ARTIFACT
    if bool(provenance_summary.get("required", False)) and not bool(provenance_summary.get("present", False)):
        return REASON_PROVENANCE_MISSING
    if str(result_status).strip().upper() != "PASS":
        return REASON_REQUIRED_GATE_FAIL
    return ""


def _closure_status_for_row(
    *,
    path_family: str,
    schema_status: str,
    date_binding_status: str,
    freshness_status: str,
    provenance_summary: Mapping[str, Any],
    result_status: str,
) -> str:
    if not _path_family_is_canonical(path_family):
        return "OPEN"
    if str(schema_status).strip().upper() not in {"VALID", "VALIDATED_BY_LOADER", "PRACTICAL_VALID"}:
        return "OPEN"
    if str(date_binding_status).strip().upper() != "MATCH":
        return "OPEN"
    if str(freshness_status).strip().upper() != "CURRENT":
        return "OPEN"
    if bool(provenance_summary.get("required", False)) and not bool(provenance_summary.get("present", False)):
        return "OPEN"
    if str(result_status).strip().upper() != "PASS":
        return "OPEN"
    return "CLOSED"


def _result_row(
    *,
    artifact_id: str,
    required: bool,
    role_class: str,
    classification: str,
    authority_path: Path,
    observed_status: str,
    result_status: str,
    blocker_codes: Iterable[str],
    schema_status: str,
    schema_ref: str,
    freshness_rule: str,
    freshness_status: str,
    target_day_expected: str,
    target_day_observed: str,
    date_binding_status: str,
    provenance_required: bool,
    provenance_summary: Dict[str, Any],
    producer: Dict[str, str] | None = None,
    source_refs: Iterable[Mapping[str, Any]] | None = None,
    observed_dependency_artifacts: Iterable[str] | None = None,
    explicit_reason_code: str = "",
) -> Dict[str, Any]:
    path_family = _path_family(authority_path)
    blocking_reason_code = _blocking_reason_code(
        explicit_reason_code=explicit_reason_code,
        path_family=path_family,
        schema_status=schema_status,
        date_binding_status=date_binding_status,
        freshness_status=freshness_status,
        provenance_summary=provenance_summary,
        result_status=result_status,
    )
    return {
        "artifact_id": artifact_id,
        "artifact_name": artifact_id,
        "required": bool(required),
        "role_class": role_class,
        "classification": classification,
        "canonical_path": str(authority_path),
        "authority_path": str(authority_path),
        "path_family": path_family,
        "observed_status": observed_status,
        "result_status": result_status,
        "blocker_codes": sorted({str(code).strip() for code in blocker_codes if str(code).strip()}),
        "blocking_reason_code": blocking_reason_code,
        "schema_status": schema_status,
        "schema_ref": schema_ref,
        "freshness_rule": freshness_rule,
        "freshness_status": freshness_status,
        "target_day_expected": target_day_expected,
        "target_day_observed": target_day_observed,
        "date_binding_status": date_binding_status,
        "date_binding_value": target_day_observed,
        "provenance_required": bool(provenance_required),
        "provenance_summary": provenance_summary,
        "closure_status": _closure_status_for_row(
            path_family=path_family,
            schema_status=schema_status,
            date_binding_status=date_binding_status,
            freshness_status=freshness_status,
            provenance_summary=provenance_summary,
            result_status=result_status,
        ),
        "producer": producer or {"module": "", "git_sha": ""},
        "source_refs": [dict(item) for item in (source_refs or [])],
        "observed_dependency_artifacts": sorted(
            {str(item).strip() for item in (observed_dependency_artifacts or []) if str(item).strip()}
        ),
    }


def _normalized_codes(values: Iterable[str]) -> List[str]:
    return sorted({str(value).strip() for value in values if str(value).strip()})


def _producer_artifact_refs(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, str]]:
    refs_by_path: Dict[str, Dict[str, str]] = {}
    for row in rows:
        authority_path = str(row.get("authority_path") or "").strip()
        if authority_path:
            refs_by_path.setdefault(authority_path, {"artifact_path": authority_path, "artifact_sha256": ""})
        for ref in row.get("source_refs") or []:
            if not isinstance(ref, Mapping):
                continue
            artifact_path = str(ref.get("artifact_path") or "").strip()
            artifact_sha256 = str(ref.get("artifact_sha256") or "").strip()
            if not artifact_path:
                continue
            existing = refs_by_path.get(artifact_path)
            if existing is None or (artifact_sha256 and not existing.get("artifact_sha256")):
                refs_by_path[artifact_path] = {
                    "artifact_path": artifact_path,
                    "artifact_sha256": artifact_sha256,
                }
    return list(refs_by_path.values())


def _producer_reason_codes(rows: Iterable[Mapping[str, Any]]) -> List[str]:
    codes: List[str] = []
    for row in rows:
        blocking_reason_code = str(row.get("blocking_reason_code") or "").strip()
        if blocking_reason_code:
            codes.append(blocking_reason_code)
        codes.extend(str(code).strip() for code in (row.get("blocker_codes") or []) if str(code).strip())
    return _normalized_codes(codes)


def _producer_freshness_status(rows: Iterable[Mapping[str, Any]]) -> str:
    freshness_values = {
        str(row.get("freshness_status") or "").strip().upper()
        for row in rows
        if str(row.get("freshness_status") or "").strip()
    }
    if "STALE" in freshness_values:
        return PRODUCER_RESULT_STALE
    if "CURRENT" in freshness_values:
        return PRODUCER_RESULT_CURRENT
    return PRODUCER_RESULT_UNKNOWN


def _producer_target_day_observed(rows: Iterable[Mapping[str, Any]]) -> str:
    observed_days = {
        str(row.get("target_day_observed") or "").strip()
        for row in rows
        if str(row.get("target_day_observed") or "").strip()
    }
    if len(observed_days) == 1:
        return next(iter(observed_days))
    return ""


def _producer_result_state(*, rows: List[Mapping[str, Any]], return_code: int) -> str:
    reason_codes = set(_producer_reason_codes(rows))
    freshness_statuses = {
        str(row.get("freshness_status") or "").strip().upper()
        for row in rows
        if str(row.get("freshness_status") or "").strip()
    }
    date_binding_statuses = {
        str(row.get("date_binding_status") or "").strip().upper()
        for row in rows
        if str(row.get("date_binding_status") or "").strip()
    }
    observed_statuses = {
        str(row.get("observed_status") or "").strip().upper()
        for row in rows
        if str(row.get("observed_status") or "").strip()
    }
    result_statuses = {
        str(row.get("result_status") or "").strip().upper()
        for row in rows
        if str(row.get("result_status") or "").strip()
    }

    if reason_codes & HANDSHAKE_EXTERNAL_UNAVAILABLE_CODES:
        return PRODUCER_RESULT_UNAVAILABLE
    if reason_codes & HANDSHAKE_STALE_CODES:
        return PRODUCER_RESULT_STALE
    if REASON_TARGET_DAY_DATE_MISMATCH in reason_codes or "MISMATCH" in date_binding_statuses:
        return PRODUCER_RESULT_MISMATCH
    if REASON_REQUIRED_GATE_FAIL in reason_codes or "ACTIVE" in observed_statuses or "BLOCKED" in observed_statuses:
        return PRODUCER_RESULT_BLOCKED
    if REASON_TARGET_DAY_ARTIFACT_MISSING in reason_codes or "MISSING" in observed_statuses or "UNAVAILABLE" in observed_statuses:
        return PRODUCER_RESULT_UNAVAILABLE
    if REASON_STALE_ARTIFACT in reason_codes or "STALE" in freshness_statuses:
        return PRODUCER_RESULT_STALE
    if PRODUCER_RESULT_FAIL in result_statuses or return_code != 0:
        return PRODUCER_RESULT_FAIL
    return PRODUCER_RESULT_PASS


def _handshake_blocker_codes_from_exception(exc: Exception) -> List[str]:
    raw = str(exc or "").strip()
    if not raw:
        return [f"IB_API_HANDSHAKE_UNAVAILABLE:{type(exc).__name__}"]
    prefix = raw.split(":", 1)[0].strip()
    if prefix.startswith("IB_API_HANDSHAKE_"):
        return [prefix]
    return [f"IB_API_HANDSHAKE_UNAVAILABLE:{type(exc).__name__}"]


def _handshake_pointer_row_from_payload(
    *,
    pointer_path: Path,
    pointer_payload: Mapping[str, Any],
    target_day: str,
    source_refs: List[Dict[str, str]],
    blocker_codes: List[str],
) -> Dict[str, Any]:
    pointer_day = str(pointer_payload.get("day_utc") or "").strip()
    schema_ok = (
        str(pointer_payload.get("schema_id") or "").strip() == "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1"
        and int(pointer_payload.get("schema_version") or 0) == 1
    )
    pointers = pointer_payload.get("pointers")
    pointer_target_path = ""
    if isinstance(pointers, Mapping):
        pointer_target_path = str(pointers.get("handshake_path") or "").strip()
    explicit_reason_code = ""
    if not schema_ok:
        explicit_reason_code = REASON_SCHEMA_INVALID
    elif not pointer_day:
        explicit_reason_code = REASON_TARGET_DAY_ARTIFACT_MISSING
    elif pointer_day != target_day:
        explicit_reason_code = REASON_TARGET_DAY_DATE_MISMATCH
    elif not pointer_target_path:
        explicit_reason_code = REASON_TARGET_DAY_ARTIFACT_MISSING
    return _result_row(
        artifact_id="ib_api_handshake_latest_pointer_v1",
        required=True,
        role_class=ROLE_REQUIRED_BINDING_INPUT,
        classification="PRE_OPEN_PREREQUISITE",
        authority_path=pointer_path,
        observed_status=str(pointer_payload.get("status") or ("OK" if schema_ok else "INVALID")).strip().upper() or "UNKNOWN",
        result_status="PASS" if schema_ok and pointer_day == target_day and bool(pointer_target_path) else "FAIL",
        blocker_codes=blocker_codes + list(pointer_payload.get("reason_codes") or []),
        schema_status="PRACTICAL_VALID" if schema_ok else "INVALID",
        schema_ref="governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/ib_api_handshake_latest_pointer.v1.schema.json",
        freshness_rule="TARGET_DAY_MATCH_ONLY",
        freshness_status="CURRENT" if pointer_day == target_day else "STALE",
        target_day_expected=target_day,
        target_day_observed=pointer_day,
        date_binding_status="MATCH" if pointer_day == target_day else ("MISSING" if not pointer_day else "MISMATCH"),
        provenance_required=True,
        provenance_summary=_provenance_summary(payload=pointer_payload, required=True),
        producer={"module": "ops/tools/run_ib_api_handshake_spine_v1.py", "git_sha": ""},
        source_refs=source_refs,
        explicit_reason_code=explicit_reason_code,
    )


def _handshake_artifact_row_from_payload(
    *,
    artifact_path: Path,
    handshake_payload: Mapping[str, Any],
    target_day: str,
    source_refs: List[Dict[str, str]],
    blocker_codes: List[str],
) -> Dict[str, Any]:
    handshake_day = str(handshake_payload.get("day_utc") or "").strip()
    handshake_status = str(handshake_payload.get("status") or "").strip().upper()
    schema_ok = (
        str(handshake_payload.get("schema_id") or "").strip() == "C2_IB_API_HANDSHAKE_V1"
        and int(handshake_payload.get("schema_version") or 0) == 1
    )
    explicit_reason_code = ""
    if not schema_ok:
        explicit_reason_code = REASON_SCHEMA_INVALID
    elif handshake_day != target_day:
        explicit_reason_code = REASON_TARGET_DAY_DATE_MISMATCH if handshake_day else REASON_TARGET_DAY_ARTIFACT_MISSING
    return _result_row(
        artifact_id="ib_api_handshake_v1",
        required=True,
        role_class=ROLE_REQUIRED_BINDING_INPUT,
        classification="PRE_OPEN_PREREQUISITE",
        authority_path=artifact_path,
        observed_status=handshake_status or "UNKNOWN",
        result_status="PASS" if schema_ok and handshake_status == "OK" and handshake_day == target_day else "FAIL",
        blocker_codes=blocker_codes + list(handshake_payload.get("reason_codes") or []) + ([] if handshake_status == "OK" else ["IB_API_HANDSHAKE_NOT_OK"]),
        schema_status="PRACTICAL_VALID" if schema_ok else "INVALID",
        schema_ref="governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/ib_api_handshake.v1.schema.json",
        freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        freshness_status=_freshness_status(
            payload=handshake_payload,
            target_day=target_day,
            observed_day=handshake_day,
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        ),
        target_day_expected=target_day,
        target_day_observed=handshake_day,
        date_binding_status="MATCH" if handshake_day == target_day else ("MISSING" if not handshake_day else "MISMATCH"),
        provenance_required=True,
        provenance_summary=_provenance_summary(payload=handshake_payload, required=True),
        producer={"module": "ops/tools/run_ib_api_handshake_spine_v1.py", "git_sha": ""},
        source_refs=source_refs,
        explicit_reason_code=explicit_reason_code,
    )


def _normalize_producer_result_row(
    *,
    raw_row: Mapping[str, Any],
    related_rows: List[Mapping[str, Any]],
    target_day: str,
) -> Dict[str, Any]:
    script = str(raw_row.get("script") or "").strip()
    return_code = int(raw_row.get("return_code") or 0)
    reason_codes = _producer_reason_codes(related_rows)
    if return_code != 0 and not reason_codes:
        reason_codes = [f"PRE_OPEN_PRODUCER_NONZERO_EXIT:{script}"]
    result_state = _producer_result_state(rows=related_rows, return_code=return_code)
    return {
        "script": script,
        "command": list(raw_row.get("command") or []),
        "return_code": return_code,
        "stdout": str(raw_row.get("stdout") or "").strip(),
        "stderr": str(raw_row.get("stderr") or "").strip(),
        "required_for_completion": bool(raw_row.get("required_for_completion", True)),
        "result_state": result_state,
        "reason_codes": reason_codes,
        "freshness_status": _producer_freshness_status(related_rows),
        "target_day_expected": target_day,
        "target_day_observed": _producer_target_day_observed(related_rows),
        "artifact_refs": _producer_artifact_refs(related_rows),
    }


def _normalize_pre_open_producer_results(
    *,
    checks: List[Mapping[str, Any]],
    producer_results: Iterable[Mapping[str, Any]],
    target_day: str,
) -> List[Dict[str, Any]]:
    rows_by_artifact_id = {
        str(row.get("artifact_id") or "").strip(): dict(row)
        for row in checks
        if str(row.get("artifact_id") or "").strip()
    }
    normalized_rows: List[Dict[str, Any]] = []
    for raw_row in producer_results:
        script = str(raw_row.get("script") or "").strip()
        related_rows = [
            rows_by_artifact_id[artifact_id]
            for artifact_id in PRODUCER_ARTIFACT_IDS_BY_SCRIPT.get(script, ())
            if artifact_id in rows_by_artifact_id
        ]
        normalized_rows.append(
            _normalize_producer_result_row(
                raw_row=raw_row,
                related_rows=related_rows,
                target_day=target_day,
            )
        )
    return normalized_rows


def _resolve_primary_binding(*, repo_root: Path, environment: str, ib_account: str):
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=repo_root,
        environment=environment,
        requested_ib_account=ib_account,
    )
    for binding in bindings:
        if str(binding.sleeve_id).strip().upper() == "PRIMARY":
            return binding
    return bindings[0]


def _current_active_day_observed(*, truth_root: Path) -> str:
    try:
        active_ref = read_control_plane_surface_v1(
            domain="session",
            surface="active_session_current",
            truth_root=truth_root,
        )
    except Exception:
        return ""
    return str(active_ref.payload.get("active_day") or "").strip()


def _market_calendar_status(*, truth_root: Path) -> Dict[str, Any]:
    try:
        ref = read_market_calendar_coverage_status_ref_v1(truth_root=truth_root)
    except Exception:
        return {
            "available": False,
            "artifact_path": "",
            "artifact_sha256": "",
            "severity": "",
            "required_target_day": "",
            "reason_codes": [],
        }
    payload = dict(ref.payload)
    return {
        "available": True,
        "artifact_path": str(ref.path),
        "artifact_sha256": str(ref.sha256),
        "severity": str(payload.get("severity") or "").strip(),
        "required_target_day": str(payload.get("required_target_day") or "").strip(),
        "reason_codes": [str(code).strip() for code in (payload.get("reason_codes") or []) if str(code).strip()],
    }


def _collect_handshake_rows(*, truth_root: Path, environment: str, ib_account: str, target_day: str) -> List[Dict[str, Any]]:
    binding = _resolve_primary_binding(repo_root=REPO_ROOT, environment=environment, ib_account=ib_account)
    handshake_truth_root = Path(binding.truth_root).resolve()
    try:
        handshake_state = resolve_pointer_bound_handshake_state(
            environment=environment,
            ib_account=ib_account,
            day_utc=target_day,
            truth_root=handshake_truth_root,
        )
        pointer_payload = read_json_object_v1(handshake_state.pointer_path)
        handshake_payload = read_json_object_v1(handshake_state.handshake_path)
        pointer_resolved_for_target_day = (
            handshake_state.pointer_day_utc != target_day
            and handshake_state.handshake_day_utc == target_day
        )
        pointer_observed_day = target_day if pointer_resolved_for_target_day else handshake_state.pointer_day_utc
        pointer_date_binding_status = "MATCH" if pointer_observed_day == target_day else "MISMATCH"
        pointer_freshness_status = "CURRENT" if pointer_date_binding_status == "MATCH" else "STALE"
        pointer_row = _result_row(
            artifact_id="ib_api_handshake_latest_pointer_v1",
            required=True,
            role_class=ROLE_REQUIRED_BINDING_INPUT,
            classification="PRE_OPEN_PREREQUISITE",
            authority_path=handshake_state.pointer_path,
            observed_status="STALE_POINTER_RESOLVED_TARGET_DAY_HANDSHAKE" if pointer_resolved_for_target_day else "OK",
            result_status="PASS",
            blocker_codes=[],
            schema_status="PRACTICAL_VALID",
            schema_ref="governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/ib_api_handshake_latest_pointer.v1.schema.json",
            freshness_rule="TARGET_DAY_MATCH_ONLY",
            freshness_status=pointer_freshness_status,
            target_day_expected=target_day,
            target_day_observed=pointer_observed_day,
            date_binding_status=pointer_date_binding_status,
            provenance_required=True,
            provenance_summary=_provenance_summary(payload=pointer_payload, required=True),
            producer={"module": "ops/tools/run_ib_api_handshake_spine_v1.py", "git_sha": ""},
            source_refs=[
                {"artifact_path": str(handshake_state.pointer_path), "artifact_sha256": handshake_state.pointer_sha256},
                {"artifact_path": str(handshake_state.handshake_path), "artifact_sha256": handshake_state.handshake_sha256},
            ],
        )
        handshake_status = str(handshake_payload.get("status") or "").strip().upper()
        handshake_row = _result_row(
            artifact_id="ib_api_handshake_v1",
            required=True,
            role_class=ROLE_REQUIRED_BINDING_INPUT,
            classification="PRE_OPEN_PREREQUISITE",
            authority_path=handshake_state.handshake_path,
            observed_status=handshake_status or "UNKNOWN",
            result_status="PASS" if handshake_status == "OK" else "FAIL",
            blocker_codes=[] if handshake_status == "OK" else _normalized_codes(
                list(handshake_payload.get("reason_codes") or []) + ["IB_API_HANDSHAKE_NOT_OK"]
            ),
            schema_status="PRACTICAL_VALID",
            schema_ref="governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/ib_api_handshake.v1.schema.json",
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            freshness_status=_freshness_status(
                payload=handshake_payload,
                target_day=target_day,
                observed_day=handshake_state.handshake_day_utc,
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            ),
            target_day_expected=target_day,
            target_day_observed=handshake_state.handshake_day_utc,
            date_binding_status="MATCH" if handshake_state.handshake_day_utc == target_day else "MISMATCH",
            provenance_required=True,
            provenance_summary=_provenance_summary(payload=handshake_payload, required=True),
            producer={"module": "ops/tools/run_ib_api_handshake_spine_v1.py", "git_sha": ""},
            source_refs=[{"artifact_path": str(handshake_state.handshake_path), "artifact_sha256": handshake_state.handshake_sha256}],
        )
        return [pointer_row, handshake_row]
    except Exception as exc:
        pointer_path = (handshake_truth_root / "ib_api_handshake" / "latest_pointer.v1.json").resolve()
        artifact_path = (handshake_truth_root / "ib_api_handshake" / target_day / "ib_api_handshake.v1.json").resolve()
        exception_codes = _handshake_blocker_codes_from_exception(exc)
        source_refs: List[Dict[str, str]] = []
        pointer_payload: Mapping[str, Any] | None = None
        handshake_payload: Mapping[str, Any] | None = None
        if pointer_path.exists() and pointer_path.is_file():
            try:
                pointer_payload = read_json_object_v1(pointer_path)
                source_refs.append({"artifact_path": str(pointer_path), "artifact_sha256": sha256_file_v1(pointer_path)})
            except Exception:
                pointer_payload = None
        if artifact_path.exists() and artifact_path.is_file():
            try:
                handshake_payload = read_json_object_v1(artifact_path)
                source_refs.append({"artifact_path": str(artifact_path), "artifact_sha256": sha256_file_v1(artifact_path)})
            except Exception:
                handshake_payload = None

        pointer_row = (
            _handshake_pointer_row_from_payload(
                pointer_path=pointer_path,
                pointer_payload=pointer_payload,
                target_day=target_day,
                source_refs=source_refs,
                blocker_codes=exception_codes,
            )
            if pointer_payload is not None
            else _result_row(
                artifact_id="ib_api_handshake_latest_pointer_v1",
                required=True,
                role_class=ROLE_REQUIRED_BINDING_INPUT,
                classification="PRE_OPEN_PREREQUISITE",
                authority_path=pointer_path,
                observed_status="MISSING",
                result_status="FAIL",
                blocker_codes=exception_codes,
                schema_status="MISSING",
                schema_ref="governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/ib_api_handshake_latest_pointer.v1.schema.json",
                freshness_rule="TARGET_DAY_MATCH_ONLY",
                freshness_status="STALE",
                target_day_expected=target_day,
                target_day_observed="",
                date_binding_status="MISSING",
                provenance_required=True,
                provenance_summary={"required": True, "present": False, "fields_present": [], "source": ""},
                explicit_reason_code=REASON_TARGET_DAY_ARTIFACT_MISSING,
            )
        )
        handshake_row = (
            _handshake_artifact_row_from_payload(
                artifact_path=artifact_path,
                handshake_payload=handshake_payload,
                target_day=target_day,
                source_refs=source_refs,
                blocker_codes=exception_codes,
            )
            if handshake_payload is not None
            else _result_row(
                artifact_id="ib_api_handshake_v1",
                required=True,
                role_class=ROLE_REQUIRED_BINDING_INPUT,
                classification="PRE_OPEN_PREREQUISITE",
                authority_path=artifact_path,
                observed_status="MISSING",
                result_status="FAIL",
                blocker_codes=exception_codes,
                schema_status="MISSING",
                schema_ref="governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/ib_api_handshake.v1.schema.json",
                freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
                freshness_status="STALE",
                target_day_expected=target_day,
                target_day_observed="",
                date_binding_status="MISSING",
                provenance_required=True,
                provenance_summary={"required": True, "present": False, "fields_present": [], "source": ""},
                explicit_reason_code=REASON_TARGET_DAY_ARTIFACT_MISSING,
            )
        )
        return [pointer_row, handshake_row]


def _collect_kill_switch_row(*, truth_root: Path, target_day: str) -> Dict[str, Any]:
    kill_switch_result = resolve_kill_switch_authority_v1(canonical_truth_root=truth_root, day_utc=target_day)
    canonical_path = kill_switch_result.canonical_path
    if kill_switch_result.status != KILL_SWITCH_STATUS_PASS:
        reason_code = str(kill_switch_result.reason_code or "").strip()
        target_day_observed = ""
        provenance_summary = {"required": True, "present": False, "fields_present": [], "source": ""}
        if isinstance(kill_switch_result.payload, dict):
            target_day_observed = str(kill_switch_result.payload.get("day_utc") or "").strip()
            provenance_summary = _provenance_summary(payload=kill_switch_result.payload, required=True)
        return _result_row(
            artifact_id="global_kill_switch_state_v1",
            required=True,
            role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
            classification="PRE_OPEN_PREREQUISITE",
            authority_path=canonical_path,
            observed_status="MISSING" if reason_code == RC_KILL_SWITCH_CANONICAL_MISSING else "FAIL",
            result_status="FAIL",
            blocker_codes=[reason_code] if reason_code else [],
            schema_status="MISSING" if reason_code == RC_KILL_SWITCH_CANONICAL_MISSING else "PRACTICAL_VALID",
            schema_ref="governance/04_DATA/SCHEMAS/C2/RISK/global_kill_switch_state.v1.schema.json",
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            freshness_status="UNKNOWN" if reason_code == RC_KILL_SWITCH_CANONICAL_MISSING else "CURRENT",
            target_day_expected=target_day,
            target_day_observed=target_day_observed,
            date_binding_status="MISSING" if reason_code == RC_KILL_SWITCH_CANONICAL_MISSING else "MATCH",
            provenance_required=True,
            provenance_summary=provenance_summary,
            producer=_producer_block(kill_switch_result.payload or {}),
            source_refs=(
                [{"artifact_path": str(canonical_path), "artifact_sha256": kill_switch_result.canonical_sha256}]
                if kill_switch_result.canonical_sha256
                else []
            ),
            explicit_reason_code=reason_code,
        )

    payload = dict(kill_switch_result.payload or {})
    observed_day = str(payload.get("day_utc") or "").strip()
    state = str(kill_switch_result.state or "").strip().upper()
    allow_entries = bool(kill_switch_result.allow_entries is True)
    result_status = "PASS" if state == "INACTIVE" and allow_entries else "FAIL"
    blocker_codes = [str(code).strip() for code in (kill_switch_result.reason_codes or ()) if str(code).strip()]
    if result_status != "PASS" and not blocker_codes:
        blocker_codes = [
            f"GLOBAL_KILL_SWITCH_STATE:{state or 'UNKNOWN'}",
            "GLOBAL_KILL_SWITCH_ALLOW_ENTRIES_FALSE" if not allow_entries else "",
        ]
    return _result_row(
        artifact_id="global_kill_switch_state_v1",
        required=True,
        role_class=ROLE_REQUIRED_EXECUTION_BOUNDARY,
        classification="PRE_OPEN_PREREQUISITE",
        authority_path=canonical_path,
        observed_status=state or "UNKNOWN",
        result_status=result_status,
        blocker_codes=[str(code).strip() for code in blocker_codes if str(code).strip()],
        schema_status="PRACTICAL_VALID",
        schema_ref="governance/04_DATA/SCHEMAS/C2/RISK/global_kill_switch_state.v1.schema.json",
        freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        freshness_status=_freshness_status(
            payload=payload,
            target_day=target_day,
            observed_day=observed_day,
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        ),
        target_day_expected=target_day,
        target_day_observed=observed_day,
        date_binding_status="MATCH" if observed_day == target_day else "MISMATCH",
        provenance_required=True,
        provenance_summary=_provenance_summary(payload=payload, required=True),
        producer=_producer_block(payload),
        source_refs=[{"artifact_path": str(canonical_path), "artifact_sha256": kill_switch_result.canonical_sha256}],
    )


def _collect_primary_scoped_authority_head_row(*, repo_root: Path, environment: str, ib_account: str, target_day: str) -> Dict[str, Any]:
    binding = _resolve_primary_binding(repo_root=repo_root, environment=environment, ib_account=ib_account)
    head_path = (binding.truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()
    if not head_path.exists() or not head_path.is_file():
        return _result_row(
            artifact_id="primary_scoped_canonical_authority_head_v1",
            required=True,
            role_class=ROLE_REQUIRED_BINDING_INPUT,
            classification="PRE_OPEN_PREREQUISITE",
            authority_path=head_path,
            observed_status="MISSING",
            result_status="FAIL",
            blocker_codes=["PRIMARY_SCOPED_CANONICAL_AUTHORITY_HEAD_MISSING"],
            schema_status="MISSING",
            schema_ref="",
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
            freshness_status="STALE",
            target_day_expected=target_day,
            target_day_observed="",
            date_binding_status="MISSING",
            provenance_required=True,
            provenance_summary={"required": True, "present": False, "fields_present": [], "source": ""},
            explicit_reason_code=REASON_TARGET_DAY_ARTIFACT_MISSING,
        )

    head_payload = read_json_object_v1(head_path)
    head_day = str(head_payload.get("day_utc") or "").strip()
    head_status = str(head_payload.get("status") or "").strip().upper()
    head_reason_codes = _normalized_codes(head_payload.get("reason_codes") or [])
    schema_ok = (
        str(head_payload.get("schema_id") or "").strip() == "c2_run_pointer_canonical_authority_head"
        and str(head_payload.get("schema_version") or "").strip() == "v1"
    )
    explicit_reason_code = ""
    date_binding_status = "MATCH" if head_day == target_day else ("MISSING" if not head_day else "MISMATCH")
    if not schema_ok:
        explicit_reason_code = REASON_SCHEMA_INVALID
    elif not head_day and head_status in {"UNAVAILABLE", "MISSING", "FAIL"}:
        explicit_reason_code = REASON_TARGET_DAY_ARTIFACT_MISSING
    return _result_row(
        artifact_id="primary_scoped_canonical_authority_head_v1",
        required=True,
        role_class=ROLE_REQUIRED_BINDING_INPUT,
        classification="PRE_OPEN_PREREQUISITE",
        authority_path=head_path,
        observed_status=head_status or "UNKNOWN",
        result_status="PASS"
        if schema_ok and head_day == target_day and head_status == "PASS"
        else "FAIL",
        blocker_codes=head_reason_codes if schema_ok else ["PRIMARY_SCOPED_CANONICAL_AUTHORITY_HEAD_SCHEMA_INVALID"],
        schema_status="PRACTICAL_VALID" if schema_ok else "INVALID",
        schema_ref="",
        freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        freshness_status=_freshness_status(
            payload=head_payload,
            target_day=target_day,
            observed_day=head_day,
            freshness_rule="TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        ),
        target_day_expected=target_day,
        target_day_observed=head_day,
        date_binding_status=date_binding_status,
        provenance_required=True,
        provenance_summary=_provenance_summary(payload=head_payload, required=True),
        producer={"module": "ops/tools/run_pointer_heads_materialize_v1.py", "git_sha": str(head_payload.get("producer_git_sha") or "").strip()},
        source_refs=[{"artifact_path": str(head_path), "artifact_sha256": sha256_file_v1(head_path)}],
        explicit_reason_code=explicit_reason_code,
    )


def _apply_paper_active_session_alignment_override(
    *,
    checks: List[Dict[str, Any]],
    environment: str,
    target_day: str,
    active_day_observed: str,
) -> List[Dict[str, Any]]:
    if str(environment or "").strip().upper() != "PAPER":
        return checks
    if str(active_day_observed or "").strip() != str(target_day or "").strip():
        return checks
    overridden: List[Dict[str, Any]] = []
    for row in checks:
        if str(row.get("artifact_id") or "").strip() != "primary_scoped_canonical_authority_head_v1":
            overridden.append(row)
            continue
        patched = dict(row)
        patched["result_status"] = "PASS"
        patched["blocking_reason_code"] = ""
        patched["blocker_codes"] = []
        patched["closure_status"] = "CLOSED"
        patched["date_binding_status"] = "MATCH"
        patched["date_binding_value"] = target_day
        patched["target_day_observed"] = target_day
        patched["freshness_status"] = "CURRENT"
        patched["observed_status"] = "ACTIVE_SESSION_PROMOTED"
        overridden.append(patched)
    return overridden


def collect_pre_open_prerequisite_checks_v1(
    *,
    truth_root: Path,
    target_day: str,
    environment: str,
    ib_account: str,
    repo_root: Path | None = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    rows.extend(
        _collect_handshake_rows(
            truth_root=truth_root,
            environment=environment,
            ib_account=ib_account,
            target_day=target_day,
        )
    )
    rows.append(_collect_kill_switch_row(truth_root=truth_root, target_day=target_day))
    rows.append(
        _collect_primary_scoped_authority_head_row(
            repo_root=Path(repo_root or REPO_ROOT).resolve(),
            environment=environment,
            ib_account=ib_account,
            target_day=target_day,
        )
    )
    return rows


def apply_trading_day_readiness_policy_to_pre_open_checks_v1(
    *,
    checks: List[Dict[str, Any]],
    readiness_payload: Mapping[str, Any] | None,
) -> List[Dict[str, Any]]:
    readiness = readiness_payload if isinstance(readiness_payload, Mapping) else {}
    readiness_mode = str(readiness.get("readiness_mode") or "").strip().upper()
    requires_same_day_broker_event_log = readiness.get("requires_same_day_broker_event_log")
    requires_live_account_truth = readiness.get("requires_live_account_truth")
    if readiness_mode not in {"PREOPEN_BUILD", "PREOPEN_ADMISSION"}:
        return checks
    if requires_same_day_broker_event_log is not False and requires_live_account_truth is not False:
        return checks

    adjusted: List[Dict[str, Any]] = []
    for row in checks:
        item = dict(row)
        artifact_id = str(item.get("artifact_id") or "").strip()
        if artifact_id in {"ib_api_handshake_latest_pointer_v1", "ib_api_handshake_v1"}:
            item.update(
                {
                    "required": False,
                    "observed_status": f"NOT_REQUIRED_BY_{readiness_mode}",
                    "result_status": "PASS",
                    "blocker_codes": [],
                    "blocking_reason_code": "",
                    "freshness_rule": "TRADING_DAY_READINESS_POLICY",
                    "freshness_status": "NOT_REQUIRED",
                    "date_binding_status": "NOT_REQUIRED",
                    "provenance_required": False,
                    "provenance_summary": {"required": False, "present": False, "fields_present": [], "source": "trading_day_readiness_authority_v1"},
                    "closure_status": "CLOSED",
                }
            )
            refs = list(item.get("source_refs") or [])
            readiness_path = str(readiness.get("path") or readiness.get("artifact_path") or "").strip()
            if readiness_path:
                refs.append({"artifact_path": readiness_path, "artifact_sha256": ""})
            item["source_refs"] = refs
        adjusted.append(item)
    return adjusted


def derive_pre_open_bundle_payload_v1(
    *,
    truth_root: Path,
    target_day: str,
    environment: str,
    ib_account: str,
    producer_results: Iterable[Mapping[str, Any]],
    owner_tool: str,
    repo_root: Path | None = None,
    readiness_payload: Mapping[str, Any] | None = None,
    readiness_authority_path: str = "",
) -> Dict[str, Any]:
    normalized_truth_root = Path(truth_root).resolve()
    normalized_target_day = str(target_day).strip()
    checks = collect_pre_open_prerequisite_checks_v1(
        truth_root=normalized_truth_root,
        target_day=normalized_target_day,
        environment=environment,
        ib_account=ib_account,
        repo_root=repo_root,
    )
    checks = apply_trading_day_readiness_policy_to_pre_open_checks_v1(
        checks=checks,
        readiness_payload=readiness_payload,
    )
    active_day_observed = _current_active_day_observed(truth_root=normalized_truth_root)
    checks = _apply_paper_active_session_alignment_override(
        checks=checks,
        environment=environment,
        target_day=normalized_target_day,
        active_day_observed=active_day_observed,
    )
    active_day_alignment_status = (
        "UNKNOWN"
        if not active_day_observed
        else ("MATCH" if active_day_observed == normalized_target_day else "MISMATCH")
    )
    normalized_producer_results = _normalize_pre_open_producer_results(
        checks=checks,
        producer_results=producer_results,
        target_day=normalized_target_day,
    )
    producer_failure_codes = sorted(
        {
            f"PRE_OPEN_PRODUCER_FAILED:{str(row.get('script') or '').strip()}"
            for row in normalized_producer_results
            if str(row.get("script") or "").strip() and int(row.get("return_code") or 0) != 0
        }
    )
    required_checks = [row for row in checks if bool(row.get("required", True))]
    completion_state = MATERIALIZATION_COMPLETE if all(str(row.get("result_status") or "").strip().upper() == "PASS" for row in required_checks) else MATERIALIZATION_INCOMPLETE
    materialization_state = (
        MATERIALIZATION_COMPLETE
        if completion_state == MATERIALIZATION_COMPLETE and not producer_failure_codes
        else (MATERIALIZATION_BLOCKED if producer_failure_codes else MATERIALIZATION_INCOMPLETE)
    )
    blocking_reason_codes = sorted(
        {
            str(row.get("blocking_reason_code") or "").strip()
            for row in required_checks
            if str(row.get("blocking_reason_code") or "").strip()
        }
        | {
            str(code).strip()
            for row in normalized_producer_results
            if str(row.get("result_state") or "").strip().upper() != PRODUCER_RESULT_PASS
            for code in (row.get("reason_codes") or [])
            if str(code).strip()
        }
        | set(producer_failure_codes)
    )
    readiness = readiness_payload if isinstance(readiness_payload, Mapping) else {}
    payload = {
        "schema_id": "pre_open_bundle",
        "schema_version": "v1",
        "target_day": normalized_target_day,
        "active_day_observed": active_day_observed,
        "active_day_alignment_status": active_day_alignment_status,
        "owner_tool": str(owner_tool).strip(),
        "materialization_state": materialization_state,
        "completion_state": completion_state,
        "blocking_reason_codes": blocking_reason_codes,
        "prerequisite_checks": checks,
        "producer_results": normalized_producer_results,
        "market_calendar_status": _market_calendar_status(truth_root=normalized_truth_root),
        "built_at_utc": now_utc_iso_v1(),
        "producer": {
            "repo": str(Path(repo_root or REPO_ROOT).resolve()),
            "module": "constellation_2/common/pre_open_materializer_v1.py",
            "git_sha": repo_git_sha_v1(),
        },
    }
    if readiness:
        payload.update(
            {
                "readiness_authority_path": str(readiness_authority_path or readiness.get("path") or readiness.get("artifact_path") or "").strip(),
                "readiness_mode": str(readiness.get("readiness_mode") or "").strip(),
                "evidence_policy_used": readiness.get("evidence_policy") if isinstance(readiness.get("evidence_policy"), Mapping) else {},
                "carry_forward_source_used": "T_MINUS_1" if str(readiness.get("readiness_mode") or "").strip().upper() in {"PREOPEN_BUILD", "PREOPEN_ADMISSION"} else "",
                "mode_specific_blocker": str(readiness.get("canonical_blocker") or "").strip() == "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE",
            }
        )
    return payload


def write_pre_open_bundle_v1(*, truth_root: Path, payload: Dict[str, Any]) -> PreOpenBundleRefV1:
    target_day = str(payload.get("target_day") or "").strip()
    attempt_id = build_attempt_id_v1(payload=payload)
    atomic_write_validated_json_v1(
        path=resolve_pre_open_bundle_attempt_path_v1(
            truth_root=truth_root,
            day_utc=target_day,
            attempt_id=attempt_id,
        ),
        payload=payload,
        schema_relpath=PRE_OPEN_BUNDLE_SCHEMA_RELPATH_V1,
    )
    ref = atomic_write_idempotent_validated_json_v1(
        path=resolve_pre_open_bundle_path_v1(truth_root=truth_root, day_utc=target_day),
        payload=payload,
        schema_relpath=PRE_OPEN_BUNDLE_SCHEMA_RELPATH_V1,
        volatile_field_names=("built_at_utc",),
    )
    return PreOpenBundleRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)


def read_pre_open_bundle_ref_v1(*, truth_root: Path, target_day: str) -> PreOpenBundleRefV1:
    path = resolve_pre_open_bundle_path_v1(truth_root=truth_root, day_utc=target_day)
    ref: SurfaceRefV1 = read_validated_surface_v1(path=path, schema_relpath=PRE_OPEN_BUNDLE_SCHEMA_RELPATH_V1)
    return PreOpenBundleRefV1(path=ref.path, payload=ref.payload, sha256=ref.sha256)
