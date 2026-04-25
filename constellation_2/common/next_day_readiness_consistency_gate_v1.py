from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Tuple

from constellation_2.common.kill_switch_authority_v1 import (
    RC_KILL_SWITCH_AUTHORITY_MISMATCH,
    RC_KILL_SWITCH_CANONICAL_MISSING,
    STATUS_FAIL_CLOSED as KILL_SWITCH_FAIL_CLOSED,
    resolve_kill_switch_authority_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import read_validated_surface_v1
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_paper_day_control_plane_path,
    resolve_paper_session_ledger_path,
    resolve_submit_boundary_status_path,
)
from constellation_2.common.session_authority_monitor_v1 import (
    RC_SESSION_AUTHORITY_CANONICAL_MISMATCH,
    RC_SESSION_AUTHORITY_STATUS_STALE,
    read_session_authority_status_ref_v1,
)


CONSISTENCY_GATE_FAILURE = "CONSISTENCY_GATE_FAILURE"
CONSISTENCY_GATE_STATUS_PASS = "PASS"
CONSISTENCY_GATE_STATUS_FAIL = "FAIL"
CONSISTENCY_GATE_SURFACE_SCOPE_FULL = "full"
CONSISTENCY_GATE_SURFACE_SCOPE_ADMISSION = "admission"

TARGET_DAY_BUILD_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_build.v1.schema.json"
TARGET_DAY_ADMISSION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_admission.v1.schema.json"
ACTIVE_SESSION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/active_session.v1.schema.json"
_STARTUP_PHASEC_VETO_FAIL_CLOSED_CODE = "STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:C2_SUBMIT_FAIL_CLOSED_REQUIRED"


@dataclass(frozen=True)
class ConsistencyGateIssueV1:
    reason_code: str
    summary: str
    surface_name: str
    artifact_path: str
    observed: str
    expected: str


@dataclass(frozen=True)
class NextDayReadinessConsistencyGateResultV1:
    status: str
    day_utc: str
    blocking_reason_codes: Tuple[str, ...]
    issues: Tuple[ConsistencyGateIssueV1, ...]
    kill_switch_reason_codes: Tuple[str, ...]


def _parse_utc(text: str) -> datetime | None:
    value = str(text or "").strip()
    if not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _build_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "target_day_build_v1" / f"{day_utc}.json").resolve()


def _admission_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "target_day_admission_v1" / f"{day_utc}.json").resolve()


def _active_session_path(*, truth_root: Path) -> Path:
    return (truth_root / "active_session_v1" / "current.json").resolve()


def _read_surface_payload(
    *,
    truth_root: Path,
    day_utc: str,
    override_payload: Mapping[str, Any] | None,
    path: Path,
    schema_relpath: str,
) -> tuple[dict[str, Any] | None, str, str]:
    if override_payload is not None:
        return dict(override_payload), str(path), ""
    try:
        ref = read_validated_surface_v1(path=path, schema_relpath=schema_relpath)
    except Exception:
        return None, str(path), ""
    return dict(ref.payload), str(ref.path), str(ref.sha256)


def _status_day(payload: Mapping[str, Any]) -> str:
    for key in ("target_day", "blocked_target_day", "active_day", "next_target_day"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def _build_allows(payload: Mapping[str, Any]) -> bool:
    hidden_dependency_status = str(
        (payload.get("hidden_dependency_check_result") or {}).get("status") or ""
    ).strip().upper()
    return (
        str(payload.get("build_status") or "").strip().upper() == "COMPLETE"
        and str(payload.get("closure_status") or "").strip().upper() == "CLOSED"
        and hidden_dependency_status == "PASS"
    )


def _admission_allows(payload: Mapping[str, Any]) -> bool:
    return str(payload.get("admission_status") or "").strip().upper() == "ADMIT"


def _active_session_allows(payload: Mapping[str, Any], *, day_utc: str) -> tuple[bool | None, str]:
    target_day = str(payload.get("target_day") or "").strip()
    blocked_target_day = str(payload.get("blocked_target_day") or "").strip()
    active_day = str(payload.get("active_day") or "").strip()
    admission_status = str(payload.get("target_day_admission_status") or "").strip().upper()

    if target_day == day_utc:
        return admission_status == "ADMIT", "target_day"
    if blocked_target_day == day_utc:
        return False, "blocked_target_day"
    if active_day == day_utc:
        return admission_status == "ADMIT", "active_day"
    return None, ""


def _status_allows(payload: Mapping[str, Any]) -> bool:
    if "submission_authorized" in payload:
        return bool(payload.get("submission_authorized") is True)
    projection = payload.get("paper_authority_projection")
    if isinstance(projection, Mapping):
        if bool(projection.get("paper_open_allowed") is True):
            return True
        authority_status = str(projection.get("authority_status") or "").strip().upper()
        if authority_status:
            return authority_status == "GRANTED"
    return bool(payload.get("submission_authorized") is True)


def _boundary_allows(payload: Mapping[str, Any]) -> bool:
    return bool(payload.get("submission_authorized") is True)


def _ledger_authority_status(payload: Mapping[str, Any]) -> str:
    control_state = payload.get("control_state") or {}
    return str(control_state.get("authority_status") or payload.get("authority_status") or "").strip().upper()


def _ledger_allows(payload: Mapping[str, Any]) -> bool:
    authority_status = _ledger_authority_status(payload)
    if authority_status in {"GRANTED", "DENIED"}:
        return authority_status == "GRANTED"
    control_state = payload.get("control_state") or {}
    return bool(control_state.get("submission_authorized") is True)


def _control_plane_authority_status(payload: Mapping[str, Any]) -> str:
    authority_result = payload.get("authority_result") or {}
    return str(authority_result.get("ledger_authority_status") or "").strip().upper()


def _control_plane_allows(payload: Mapping[str, Any]) -> bool:
    return str(payload.get("final_start_decision") or "").strip().upper() == "READY_NOW"


def _issue(
    *,
    reason_code: str,
    summary: str,
    surface_name: str,
    artifact_path: str,
    observed: str,
    expected: str,
) -> ConsistencyGateIssueV1:
    return ConsistencyGateIssueV1(
        reason_code=str(reason_code).strip(),
        summary=str(summary).strip(),
        surface_name=str(surface_name).strip(),
        artifact_path=str(artifact_path).strip(),
        observed=str(observed).strip(),
        expected=str(expected).strip(),
    )


def _paper_startup_submit_divergence_is_allowed(
    *,
    control_payload: Mapping[str, Any],
    boundary_payload: Mapping[str, Any],
) -> bool:
    final_decision = str(control_payload.get("final_start_decision") or "").strip().upper()
    if final_decision != "READY_NOW":
        return False
    if _control_plane_authority_status(control_payload) != "GRANTED":
        return False
    if bool(boundary_payload.get("submission_authorized") is True):
        return False
    boundary_status = str(boundary_payload.get("boundary_status") or "").strip().upper()
    if boundary_status not in {"BLOCKED", "DENIED"}:
        return False
    return True


def _dedupe_issues(issues: Iterable[ConsistencyGateIssueV1]) -> Tuple[ConsistencyGateIssueV1, ...]:
    seen: set[tuple[str, str, str, str, str, str]] = set()
    ordered: list[ConsistencyGateIssueV1] = []
    for issue in issues:
        key = (
            issue.reason_code,
            issue.summary,
            issue.surface_name,
            issue.artifact_path,
            issue.observed,
            issue.expected,
        )
        if key in seen:
            continue
        seen.add(key)
        ordered.append(issue)
    return tuple(ordered)


def evaluate_next_day_readiness_consistency_gate_v1(
    *,
    truth_root: Path,
    day_utc: str,
    surface_scope: str = CONSISTENCY_GATE_SURFACE_SCOPE_FULL,
    build_payload: Mapping[str, Any] | None = None,
    admission_payload: Mapping[str, Any] | None = None,
    active_session_payload: Mapping[str, Any] | None = None,
    session_authority_status_payload: Mapping[str, Any] | None = None,
    submit_boundary_payload: Mapping[str, Any] | None = None,
    paper_session_ledger_payload: Mapping[str, Any] | None = None,
    paper_day_control_plane_payload: Mapping[str, Any] | None = None,
) -> NextDayReadinessConsistencyGateResultV1:
    root = Path(truth_root).resolve()
    day = str(day_utc).strip()
    scope = str(surface_scope or "").strip().lower() or CONSISTENCY_GATE_SURFACE_SCOPE_FULL
    if scope not in {CONSISTENCY_GATE_SURFACE_SCOPE_FULL, CONSISTENCY_GATE_SURFACE_SCOPE_ADMISSION}:
        scope = CONSISTENCY_GATE_SURFACE_SCOPE_FULL
    issues: list[ConsistencyGateIssueV1] = []

    build_obj, build_path, build_sha = _read_surface_payload(
        truth_root=root,
        day_utc=day,
        override_payload=build_payload,
        path=_build_path(truth_root=root, day_utc=day),
        schema_relpath=TARGET_DAY_BUILD_SCHEMA_RELPATH,
    )
    admission_obj, admission_path, admission_sha = _read_surface_payload(
        truth_root=root,
        day_utc=day,
        override_payload=admission_payload,
        path=_admission_path(truth_root=root, day_utc=day),
        schema_relpath=TARGET_DAY_ADMISSION_SCHEMA_RELPATH,
    )
    active_obj, active_path, active_sha = _read_surface_payload(
        truth_root=root,
        day_utc=day,
        override_payload=active_session_payload,
        path=_active_session_path(truth_root=root),
        schema_relpath=ACTIVE_SESSION_SCHEMA_RELPATH,
    )

    status_obj: dict[str, Any] | None
    status_path = str((root / "session_authority_status_v1" / "current.json").resolve())
    status_sha = ""
    if session_authority_status_payload is not None:
        status_obj = dict(session_authority_status_payload)
    else:
        try:
            status_ref = read_session_authority_status_ref_v1(truth_root=root)
            status_obj = dict(status_ref.payload)
            status_path = str(status_ref.path)
            status_sha = str(status_ref.sha256)
        except Exception:
            status_obj = None

    boundary_obj, boundary_path, boundary_sha = _read_surface_payload(
        truth_root=root,
        day_utc=day,
        override_payload=submit_boundary_payload,
        path=resolve_submit_boundary_status_path(truth_root=root, day_utc=day),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/submit_boundary_status.v1.schema.json",
    )
    ledger_obj, ledger_path, ledger_sha = _read_surface_payload(
        truth_root=root,
        day_utc=day,
        override_payload=paper_session_ledger_payload,
        path=resolve_paper_session_ledger_path(truth_root=root, day_utc=day),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_ledger.v1.schema.json",
    )
    control_obj, control_path, control_sha = _read_surface_payload(
        truth_root=root,
        day_utc=day,
        override_payload=paper_day_control_plane_payload,
        path=resolve_paper_day_control_plane_path(truth_root=root, day_utc=day),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/paper_day_control_plane.v1.schema.json",
    )

    required = [
        ("target_day_build_v1", build_obj, build_path),
        ("target_day_admission_v1", admission_obj, admission_path),
        ("active_session_v1", active_obj, active_path),
    ]
    if scope == CONSISTENCY_GATE_SURFACE_SCOPE_FULL:
        required.extend(
            (
                ("session_authority_status_v1", status_obj, status_path),
                ("submit_boundary_status_v1", boundary_obj, boundary_path),
                ("paper_session_ledger_v1", ledger_obj, ledger_path),
                ("paper_day_control_plane_v1", control_obj, control_path),
            )
        )
    for surface_name, payload, artifact_path in required:
        if payload is None:
            issues.append(
                _issue(
                    reason_code=CONSISTENCY_GATE_FAILURE,
                    summary=f"Missing required surface {surface_name} for day={day}.",
                    surface_name=surface_name,
                    artifact_path=artifact_path,
                    observed="MISSING",
                    expected="PRESENT",
                )
            )

    kill_result = resolve_kill_switch_authority_v1(canonical_truth_root=root, day_utc=day)
    kill_switch_reason_codes = tuple(str(code).strip() for code in kill_result.reason_codes if str(code).strip())
    kill_switch_allows = (
        kill_result.status != KILL_SWITCH_FAIL_CLOSED
        and str(kill_result.state or "").strip().upper() == "INACTIVE"
        and bool(kill_result.allow_entries is True)
    )
    if scope == CONSISTENCY_GATE_SURFACE_SCOPE_FULL and kill_result.status == KILL_SWITCH_FAIL_CLOSED:
        issues.append(
            _issue(
                reason_code=kill_result.reason_code or CONSISTENCY_GATE_FAILURE,
                summary="Kill-switch authority did not pass.",
                surface_name="kill_switch_authority_v1",
                artifact_path=str(kill_result.canonical_path),
                observed=kill_result.status,
                expected="PASS",
            )
        )

    if issues:
        deduped = _dedupe_issues(issues)
        return NextDayReadinessConsistencyGateResultV1(
            status=CONSISTENCY_GATE_STATUS_FAIL,
            day_utc=day,
            blocking_reason_codes=tuple(
                code
                for code in (
                    CONSISTENCY_GATE_FAILURE,
                    *[issue.reason_code for issue in deduped if issue.reason_code],
                )
                if code
            ),
            issues=deduped,
            kill_switch_reason_codes=kill_switch_reason_codes,
        )

    assert build_obj is not None
    assert admission_obj is not None
    assert active_obj is not None
    if scope == CONSISTENCY_GATE_SURFACE_SCOPE_FULL:
        assert status_obj is not None
        assert boundary_obj is not None
        assert ledger_obj is not None
        assert control_obj is not None

    paper_ready_now = False
    if scope == CONSISTENCY_GATE_SURFACE_SCOPE_FULL:
        paper_ready_now = (
            _control_plane_allows(control_obj)
            and _control_plane_authority_status(control_obj) == "GRANTED"
            and _ledger_authority_status(ledger_obj) == "GRANTED"
        )

    if scope == CONSISTENCY_GATE_SURFACE_SCOPE_FULL:
        status_day_value = _status_day(status_obj)
        paper_ready_status_day_mismatch = (
            paper_ready_now
            and bool(status_day_value)
            and status_day_value != day
        )
        status_reason_codes = [
            str(code).strip()
            for code in (status_obj.get("top_blocker_reason_codes") or [])
            if str(code).strip()
        ]
        first_status_code = str(status_obj.get("first_real_blocker_code") or "").strip()
        if (
            not paper_ready_status_day_mismatch
            and first_status_code in {RC_SESSION_AUTHORITY_STATUS_STALE, RC_SESSION_AUTHORITY_CANONICAL_MISMATCH}
        ):
            issues.append(
                _issue(
                    reason_code=first_status_code,
                    summary="session_authority_status_v1/current.json is already invalidated.",
                    surface_name="session_authority_status_v1",
                    artifact_path=status_path,
                    observed=first_status_code,
                    expected="TRUSTED",
                )
            )
        elif (
            not paper_ready_status_day_mismatch
            and any(code in {RC_SESSION_AUTHORITY_STATUS_STALE, RC_SESSION_AUTHORITY_CANONICAL_MISMATCH} for code in status_reason_codes)
        ):
            issues.append(
                _issue(
                    reason_code=RC_SESSION_AUTHORITY_CANONICAL_MISMATCH if RC_SESSION_AUTHORITY_CANONICAL_MISMATCH in status_reason_codes else RC_SESSION_AUTHORITY_STATUS_STALE,
                    summary="session_authority_status_v1/current.json carries stale or mismatch blocker codes.",
                    surface_name="session_authority_status_v1",
                    artifact_path=status_path,
                    observed=",".join(status_reason_codes),
                    expected="TRUSTED",
                )
            )

        latest_canonical_dt = max(
            (
                dt
                for dt in (
                    _parse_utc(str(boundary_obj.get("produced_at_utc") or "").strip()),
                    _parse_utc(str(ledger_obj.get("evaluated_at_utc") or "").strip()),
                    _parse_utc(str(control_obj.get("evaluated_at_utc") or "").strip()),
                )
                if dt is not None
            ),
            default=None,
        )
        status_generated_dt = _parse_utc(str(status_obj.get("generated_utc") or "").strip())
        if (
            not paper_ready_status_day_mismatch
            and latest_canonical_dt is not None
            and status_generated_dt is not None
            and status_generated_dt < latest_canonical_dt
        ):
            issues.append(
                _issue(
                    reason_code=RC_SESSION_AUTHORITY_STATUS_STALE,
                    summary="session_authority_status_v1/current.json is older than the newest canonical readiness artifacts.",
                    surface_name="session_authority_status_v1",
                    artifact_path=status_path,
                    observed=str(status_obj.get("generated_utc") or "").strip(),
                    expected=latest_canonical_dt.isoformat().replace("+00:00", "Z"),
                )
            )

        if not paper_ready_status_day_mismatch and status_day_value != day:
            issues.append(
                _issue(
                    reason_code=CONSISTENCY_GATE_FAILURE,
                    summary="session_authority_status_v1/current.json does not resolve to the requested target day.",
                    surface_name="session_authority_status_v1",
                    artifact_path=status_path,
                    observed=status_day_value,
                    expected=day,
                )
            )

    build_allows = _build_allows(build_obj)
    admission_allows = _admission_allows(admission_obj)
    active_allows, active_relation = _active_session_allows(active_obj, day_utc=day)
    if active_allows is None:
        issues.append(
            _issue(
                reason_code=CONSISTENCY_GATE_FAILURE,
                summary="active_session_v1/current.json does not describe the requested target day.",
                surface_name="active_session_v1",
                artifact_path=active_path,
                observed="UNRELATED_DAY",
                expected=day,
            )
        )
        active_allows = False
    else:
        active_target_day = str(active_obj.get("target_day") or "").strip()
        paper_ready_historical_active = (
            paper_ready_now
            and active_relation == "active_day"
            and bool(active_target_day)
            and active_target_day != day
        )
        if not paper_ready_historical_active:
            expected_build_ref_path = build_path
            if active_relation == "active_day":
                observed_build_ref_path = str((active_obj.get("active_day_build_ref") or {}).get("artifact_path") or "").strip()
                observed_build_sha = str((active_obj.get("active_day_build_ref") or {}).get("artifact_sha256") or "").strip()
            else:
                observed_build_ref_path = str((active_obj.get("target_day_build_ref") or {}).get("artifact_path") or "").strip()
                observed_build_sha = str((active_obj.get("target_day_build_ref") or {}).get("artifact_sha256") or "").strip()
            if observed_build_ref_path != expected_build_ref_path:
                issues.append(
                    _issue(
                        reason_code=CONSISTENCY_GATE_FAILURE,
                        summary="active_session_v1/current.json build reference does not match target_day_build_v1.",
                        surface_name="active_session_v1",
                        artifact_path=active_path,
                        observed=observed_build_ref_path,
                        expected=expected_build_ref_path,
                    )
                )
            expected_build_sha = build_sha
            if expected_build_sha and observed_build_sha != expected_build_sha:
                issues.append(
                    _issue(
                        reason_code=CONSISTENCY_GATE_FAILURE,
                        summary="active_session_v1/current.json build sha256 does not match target_day_build_v1.",
                        surface_name="active_session_v1",
                        artifact_path=active_path,
                        observed=observed_build_sha,
                        expected=expected_build_sha,
                    )
                )
            expected_admission_path = admission_path
            if active_relation == "blocked_target_day":
                observed_admission_path = str(active_obj.get("blocked_admission_ref") or "").strip()
            elif active_relation == "active_day":
                observed_admission_path = str(active_obj.get("active_day_admission_ref") or "").strip()
            else:
                observed_admission_path = str(active_obj.get("target_day_admission_ref") or "").strip()
            if observed_admission_path != expected_admission_path:
                issues.append(
                    _issue(
                        reason_code=CONSISTENCY_GATE_FAILURE,
                        summary="active_session_v1/current.json admission reference does not match target_day_admission_v1.",
                        surface_name="active_session_v1",
                        artifact_path=active_path,
                        observed=observed_admission_path,
                        expected=expected_admission_path,
                    )
                )

    boolean_views = {
        "target_day_build_v1": build_allows,
        "target_day_admission_v1": admission_allows,
        "active_session_v1": active_allows,
    }
    if scope == CONSISTENCY_GATE_SURFACE_SCOPE_FULL:
        status_allows = _status_allows(status_obj)
        boundary_allows = _boundary_allows(boundary_obj)
        ledger_status = _ledger_authority_status(ledger_obj)
        ledger_allows = _ledger_allows(ledger_obj)
        control_plane_ledger_status = _control_plane_authority_status(control_obj)
        control_plane_allows = _control_plane_allows(control_obj)
        boolean_views.update(
            {
                "session_authority_status_v1": status_allows,
                "submit_boundary_status_v1": boundary_allows,
                "paper_session_ledger_v1": ledger_allows,
                "paper_day_control_plane_v1": control_plane_allows,
            }
        )
    expected_allow = True if paper_ready_now else build_allows
    paper_submit_divergence_allowed = (
        scope == CONSISTENCY_GATE_SURFACE_SCOPE_FULL
        and _paper_startup_submit_divergence_is_allowed(
            control_payload=control_obj,
            boundary_payload=boundary_obj,
        )
    )
    tolerated_divergence_surfaces = {
        "paper_day_control_plane_v1",
        "session_authority_status_v1",
        "submit_boundary_status_v1",
        "paper_session_ledger_v1",
    }
    if paper_ready_now:
        tolerated_divergence_surfaces.update(
            {
                "target_day_build_v1",
                "target_day_admission_v1",
                "active_session_v1",
            }
        )
    for surface_name, surface_allows in boolean_views.items():
        if paper_submit_divergence_allowed and surface_name in tolerated_divergence_surfaces:
            continue
        if surface_allows != expected_allow:
            artifact_path = {
                "target_day_build_v1": build_path,
                "target_day_admission_v1": admission_path,
                "active_session_v1": active_path,
                "session_authority_status_v1": status_path,
                "submit_boundary_status_v1": boundary_path,
                "paper_session_ledger_v1": ledger_path,
                "paper_day_control_plane_v1": control_path,
                "kill_switch_authority_v1": str(kill_result.canonical_path),
            }[surface_name]
            issues.append(
                _issue(
                    reason_code=CONSISTENCY_GATE_FAILURE,
                    summary=f"{surface_name} disagrees with the target_day_build_v1 proceed/no-proceed state.",
                    surface_name=surface_name,
                    artifact_path=artifact_path,
                    observed="ALLOW" if surface_allows else "BLOCK",
                    expected="ALLOW" if expected_allow else "BLOCK",
                )
            )

    if scope == CONSISTENCY_GATE_SURFACE_SCOPE_FULL:
        guarded_authorization_surfaces = {
            "target_day_admission_v1": admission_allows,
            "active_session_v1": active_allows,
            "session_authority_status_v1": status_allows,
            "submit_boundary_status_v1": boundary_allows,
            "paper_session_ledger_v1": ledger_allows,
            "paper_day_control_plane_v1": control_plane_allows,
        }
        if not kill_switch_allows:
            allowing_surfaces = sorted(surface_name for surface_name, surface_allows in guarded_authorization_surfaces.items() if surface_allows)
            if allowing_surfaces:
                issues.append(
                    _issue(
                        reason_code=CONSISTENCY_GATE_FAILURE,
                        summary="kill_switch_authority_v1 blocks submission while another readiness surface still allows it.",
                        surface_name="kill_switch_authority_v1",
                        artifact_path=str(kill_result.canonical_path),
                        observed="BLOCK",
                        expected="ALLOW",
                    )
                )

        if control_plane_allows and ledger_status != "GRANTED":
            issues.append(
                _issue(
                    reason_code=CONSISTENCY_GATE_FAILURE,
                    summary="paper_day_control_plane_v1 reports READY_NOW while paper_session_ledger_v1 authority is not GRANTED.",
                    surface_name="paper_session_ledger_v1",
                    artifact_path=ledger_path,
                    observed=ledger_status,
                    expected="GRANTED",
                )
            )
        if control_plane_allows and control_plane_ledger_status != "GRANTED":
            issues.append(
                _issue(
                    reason_code=CONSISTENCY_GATE_FAILURE,
                    summary="paper_day_control_plane_v1 reports READY_NOW while authority_result.ledger_authority_status is not GRANTED.",
                    surface_name="paper_day_control_plane_v1",
                    artifact_path=control_path,
                    observed=control_plane_ledger_status,
                    expected="GRANTED",
                )
            )
        if ledger_status != control_plane_ledger_status:
            issues.append(
                _issue(
                    reason_code=CONSISTENCY_GATE_FAILURE,
                    summary="paper_session_ledger_v1 authority_status disagrees with paper_day_control_plane_v1 authority_result.",
                    surface_name="paper_day_control_plane_v1",
                    artifact_path=control_path,
                    observed=control_plane_ledger_status,
                    expected=ledger_status,
                )
            )

    deduped = _dedupe_issues(issues)
    status = CONSISTENCY_GATE_STATUS_PASS if not deduped else CONSISTENCY_GATE_STATUS_FAIL
    blocking_reason_codes = (
        ()
        if not deduped
        else tuple(
            code
            for code in (
                CONSISTENCY_GATE_FAILURE,
                *[issue.reason_code for issue in deduped if issue.reason_code],
            )
            if code
        )
    )
    return NextDayReadinessConsistencyGateResultV1(
        status=status,
        day_utc=day,
        blocking_reason_codes=blocking_reason_codes,
        issues=deduped,
        kill_switch_reason_codes=kill_switch_reason_codes,
    )
