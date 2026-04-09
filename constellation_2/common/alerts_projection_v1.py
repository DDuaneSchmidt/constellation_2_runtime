from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterable, Mapping

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_validated_json_v1,
    producer_block_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import resolve_alerts_projection_path
from constellation_2.phaseC.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/alerts_projection.v1.schema.json"
SEVERITY_VALUES_V1 = ("INFO", "ACTION_REQUIRED", "DEGRADED", "CRITICAL_BLOCK")
ROOT_CAUSE_FAMILIES_V1 = (
    "UPSTREAM_MISSING_INPUT",
    "POLICY_BLOCK",
    "SYSTEM_DEFECT",
    "DEPLOYMENT_BLOCK",
)
REQUIRED_CURRENT_PROJECTION_FIELDS_V1 = (
    "day_utc",
    "projection_id",
    "projection_ref",
    "generated_at_utc",
    "git_sha",
    "first_true_blocker_code",
    "first_true_blocker_source",
    "operator_action_summary",
    "contradiction_status",
    "contradictions",
)


def _stable_id(seed: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json_bytes_v1(dict(seed))).hexdigest()[:16]


def _require_nonempty_string(value: Any, code: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(code)
    return text


def validate_current_projection_input_v1(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("ALERTS_PROJECTION_INPUT_NOT_OBJECT")
    obj = dict(payload)
    if str(obj.get("authority_scope") or "").strip() != "DERIVED_ONLY_CURRENT_SYSTEM_PROJECTION":
        raise ValueError("ALERTS_PROJECTION_INPUT_AUTHORITY_SCOPE_INVALID")
    for field_name in REQUIRED_CURRENT_PROJECTION_FIELDS_V1:
        if field_name not in obj:
            raise ValueError(f"ALERTS_PROJECTION_REQUIRED_FIELD_MISSING:{field_name}")
    _require_nonempty_string(obj.get("day_utc"), "ALERTS_PROJECTION_REQUIRED_FIELD_MISSING:day_utc")
    _require_nonempty_string(obj.get("projection_id"), "ALERTS_PROJECTION_REQUIRED_FIELD_MISSING:projection_id")
    _require_nonempty_string(obj.get("projection_ref"), "ALERTS_PROJECTION_REQUIRED_FIELD_MISSING:projection_ref")
    _require_nonempty_string(obj.get("generated_at_utc"), "ALERTS_PROJECTION_REQUIRED_FIELD_MISSING:generated_at_utc")
    git_sha = _require_nonempty_string(obj.get("git_sha"), "ALERTS_PROJECTION_REQUIRED_FIELD_MISSING:git_sha").lower()
    if len(git_sha) != 40 or any(ch not in "0123456789abcdef" for ch in git_sha):
        raise ValueError("ALERTS_PROJECTION_GIT_SHA_INVALID")
    contradiction_status = str(obj.get("contradiction_status") or "").strip().upper()
    if contradiction_status not in {"NONE", "PRESENT"}:
        raise ValueError("ALERTS_PROJECTION_CONTRADICTION_STATUS_INVALID")
    contradictions = obj.get("contradictions")
    if not isinstance(contradictions, list):
        raise ValueError("ALERTS_PROJECTION_CONTRADICTIONS_INVALID")
    for row in contradictions:
        if not isinstance(row, Mapping):
            raise ValueError("ALERTS_PROJECTION_CONTRADICTION_ROW_INVALID")
        _require_nonempty_string(
            row.get("contradiction_code"),
            "ALERTS_PROJECTION_CONTRADICTION_CODE_MISSING",
        )
        _require_nonempty_string(
            row.get("summary"),
            "ALERTS_PROJECTION_CONTRADICTION_SUMMARY_MISSING",
        )
        if not isinstance(row.get("source_paths"), list):
            raise ValueError("ALERTS_PROJECTION_CONTRADICTION_SOURCE_PATHS_INVALID")
    return obj


def _classify_root_cause_family(code: str) -> str:
    text = str(code or "").strip().upper()
    if (
        text.startswith("DEPLOY")
        or "RUNTIME_COPY" in text
        or "ACTIVE_POINTER" in text
        or "AUTHORITATIVE_WORKTREE" in text
        or "BUILD_BLOCKED" in text
        or text.startswith("RELEASE_")
    ):
        return "DEPLOYMENT_BLOCK"
    if "CONTRADICTION" in text or "DEFECT" in text or "INVALID" in text:
        return "SYSTEM_DEFECT"
    if "MISSING" in text or "UNUSABLE" in text or "INCOMPLETE" in text:
        return "UPSTREAM_MISSING_INPUT"
    return "POLICY_BLOCK"


def _classify_severity(root_cause_family: str, blocker_present: bool) -> str:
    if not blocker_present:
        return "INFO"
    if root_cause_family in {"DEPLOYMENT_BLOCK", "SYSTEM_DEFECT"}:
        return "CRITICAL_BLOCK"
    if root_cause_family == "UPSTREAM_MISSING_INPUT":
        return "ACTION_REQUIRED"
    return "DEGRADED"


def _alert_row(*, alert_code: str, summary: str, source_ref: str, blocker_present: bool) -> dict[str, Any]:
    family = _classify_root_cause_family(alert_code)
    severity = _classify_severity(family, blocker_present)
    if severity not in SEVERITY_VALUES_V1:
        raise ValueError(f"ALERTS_PROJECTION_INVALID_SEVERITY:{severity}")
    if family not in ROOT_CAUSE_FAMILIES_V1:
        raise ValueError(f"ALERTS_PROJECTION_INVALID_ROOT_CAUSE:{family}")
    return {
        "alert_code": str(alert_code).strip(),
        "severity": severity,
        "root_cause_family": family,
        "summary": str(summary).strip(),
        "source_ref": str(source_ref).strip(),
        "blocking": bool(blocker_present),
    }


def _dedupe_alerts(rows: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    active: list[dict[str, Any]] = []
    suppressed: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        key = (str(row["root_cause_family"]), str(row["alert_code"]))
        if key in seen:
            suppressed.append(dict(row))
            continue
        seen.add(key)
        active.append(dict(row))
    return active, suppressed


def build_alerts_projection_v1(
    *,
    current_projection_payload: Mapping[str, Any],
    generated_at_utc: str,
    producer_module: str,
) -> dict[str, Any]:
    validated_projection = validate_current_projection_input_v1(current_projection_payload)
    blocker_code = str(validated_projection.get("first_true_blocker_code") or "").strip()
    blocker_source = str(validated_projection.get("first_true_blocker_source") or "").strip()
    contradiction_status = str(validated_projection.get("contradiction_status") or "").strip().upper()
    contradictions = list(validated_projection.get("contradictions") or [])

    candidates: list[dict[str, Any]] = []
    if blocker_code:
        candidates.append(
            _alert_row(
                alert_code=blocker_code,
                summary=str(validated_projection.get("operator_action_summary") or "").strip(),
                source_ref=blocker_source,
                blocker_present=True,
            )
        )
    for contradiction in contradictions:
        if not isinstance(contradiction, Mapping):
            continue
        code = str(contradiction.get("contradiction_code") or "").strip()
        if not code:
            continue
        candidates.append(
            _alert_row(
                alert_code=code,
                summary=str(contradiction.get("summary") or "").strip(),
                source_ref=blocker_source,
                blocker_present=True,
            )
        )
    if not candidates:
        candidates.append(
            _alert_row(
                alert_code="SYSTEM_READY",
                summary="Current deployment, startup, ledger, and day-start projection are healthy.",
                source_ref=str(validated_projection.get("journal_ref") or "").strip(),
                blocker_present=False,
            )
        )

    active_alerts, suppressed = _dedupe_alerts(candidates)
    first_actionable = dict(active_alerts[0])
    payload = {
        "schema_id": "alerts_projection",
        "schema_version": "v1",
        "authority_scope": "DERIVED_ONLY_ALERTS_PROJECTION",
        "day_utc": str(validated_projection.get("day_utc") or "").strip(),
        "projection_id": "",
        "generated_at_utc": str(generated_at_utc).strip(),
        "current_system_projection_ref": str(validated_projection.get("projection_ref") or "").strip(),
        "current_system_projection_id": str(validated_projection.get("projection_id") or "").strip(),
        "first_true_blocker_code": blocker_code,
        "active_alerts": active_alerts,
        "first_actionable_alert": first_actionable,
        "suppressed_or_secondary_alerts": suppressed,
        "operator_summary": str(validated_projection.get("operator_action_summary") or "").strip(),
        "source_artifacts": [
            {
                "logical_name": "current_system_projection_v1",
                "path": str(validated_projection.get("projection_ref") or "").strip(),
                "generated_at_utc": str(validated_projection.get("generated_at_utc") or "").strip(),
                "status": contradiction_status or "NONE",
            }
        ],
        "non_authority_notice": (
            "Derived from current_system_projection_v1 only. "
            "Do not treat this alert map as an independent authority surface."
        ),
        "producer": producer_block_v1(
            module=producer_module,
            git_sha=str(validated_projection.get("git_sha") or "").strip(),
        ),
    }
    payload["projection_id"] = f"alerts_projection:{payload['day_utc']}:{_stable_id(payload)}"
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH_V1)
    return payload


def write_alerts_projection_v1(*, truth_root: str | Path, payload: Mapping[str, Any]) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    return atomic_write_validated_json_v1(
        path=resolve_alerts_projection_path(truth_root=root, day_utc=str(payload["day_utc"])),
        payload=dict(payload),
        schema_relpath=SCHEMA_RELPATH_V1,
    )
