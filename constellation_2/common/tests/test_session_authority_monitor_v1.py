from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.session_authority_monitor_v1 import (
    ALERT_STATUS_ALERT,
    ALERT_STATUS_CLEAR,
    ALERT_STATUS_DEDUPED,
    ALERT_STATUS_ESCALATED,
    ALERT_STATUS_HEALTHY,
    RC_SESSION_AUTHORITY_CANONICAL_MISMATCH,
    RC_SESSION_AUTHORITY_STATUS_STALE,
    STATUS_SEVERITY_CRITICAL,
    STATUS_SEVERITY_ERROR,
    STATUS_SEVERITY_INFO,
    STATUS_SEVERITY_WARNING,
    build_session_authority_status_payload_v1,
    derive_session_authority_alert_payload_v1,
    read_session_authority_status_ref_v1,
    render_session_authority_status_summary_v1,
    resolve_session_authority_status_day_path,
    write_session_authority_alert_v1,
    write_session_authority_status_v1,
)
import constellation_2.common.session_authority_monitor_v1 as session_authority_monitor_module
from constellation_2.common.constitutional_runtime_v1 import (
    FINALITY_PROVISIONAL,
    build_artifact_dependency_declaration_v1,
    build_governed_artifact_lineage_v1,
    build_machine_blocker_envelope_v1,
)
from constellation_2.common.market_calendar_coverage_authority_v1 import (
    COVERAGE_STATUS_BLOCKED,
    COVERAGE_STATUS_HEALTHY,
    SEVERITY_CRITICAL,
    SEVERITY_INFO,
    write_market_calendar_coverage_status_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.common.constitutional_runtime_v1 import (
    validate_governed_artifact_payload_v1,
)
from constellation_2.common.session_authority_v1 import (
    derive_active_session_payload_v1,
    derive_target_day_admission_payload_v1,
    derive_target_day_build_payload_v1,
    write_active_session_v1,
    write_target_day_admission_v1,
    write_target_day_build_v1,
)
from constellation_2.phaseL.ui_api.operations_read_model import _build_readiness_ladder
from constellation_2.common.tests.test_paper_day_control_plane_v1 import (
    _producer as _canonical_producer,
    _write_boundary as _write_submit_boundary,
    _write_ledger as _write_paper_session_ledger,
    _write_startup as _write_startup_materialization,
    _write_startup_proof as _write_startup_proof_validation,
)

DAY = "2026-04-10"


def _coverage_payload(*, tmp_path: Path, artifact_rows: list[dict], target_day: str) -> dict:
    has_source_not_extended = any(
        str(row.get("blocking_reason_code") or "").strip() == "SOURCE_NOT_EXTENDED"
        for row in artifact_rows
    )
    return {
        "schema_id": "market_calendar_coverage_status",
        "schema_version": "v1",
        "generated_utc": "2026-04-09T21:43:49Z",
        "source_coverage_start": "2026-04-09",
        "source_coverage_end": target_day if not has_source_not_extended else "2026-04-09",
        "runtime_coverage_start": "2026-04-09",
        "runtime_coverage_end": target_day if not has_source_not_extended else "2026-04-09",
        "required_target_day": target_day,
        "required_forward_coverage_policy": {
            "policy_id": "MARKET_CALENDAR_FORWARD_COVERAGE_POLICY_V1",
            "policy_mode": "SESSION_AUTHORITY_DAY_PLUS_CALENDAR_BUFFER",
            "base_session_authority_day": "2026-04-09",
            "minimum_required_offset_calendar_days": 1,
            "buffer_calendar_days": 1,
            "warning_target_day": target_day,
        },
        "warning_target_day": target_day,
        "source_status": COVERAGE_STATUS_HEALTHY if not has_source_not_extended else COVERAGE_STATUS_BLOCKED,
        "runtime_status": COVERAGE_STATUS_HEALTHY if not has_source_not_extended else COVERAGE_STATUS_BLOCKED,
        "source_required_target_day_covered": not has_source_not_extended,
        "runtime_required_target_day_covered": not has_source_not_extended,
        "source_warning_target_day_covered": not has_source_not_extended,
        "runtime_warning_target_day_covered": not has_source_not_extended,
        "coverage_status": COVERAGE_STATUS_HEALTHY if not has_source_not_extended else COVERAGE_STATUS_BLOCKED,
        "reason_codes": ["SOURCE_NOT_EXTENDED"] if has_source_not_extended else [],
        "severity": SEVERITY_INFO if not has_source_not_extended else SEVERITY_CRITICAL,
        "operator_action_code": "NONE" if not has_source_not_extended else "EXTEND_GOVERNED_SOURCE",
        "recommended_action": "No operator action required."
        if not has_source_not_extended
        else "Extend the governed market-calendar source dataset through the required target day, then rerun market-calendar coverage authority and Session Authority.",
        "status_summary": "required_target_day="
        f"{target_day} warning_target_day={target_day} "
        f"source_range=2026-04-09..{target_day if not has_source_not_extended else '2026-04-09'} "
        f"runtime_range=2026-04-09..{target_day if not has_source_not_extended else '2026-04-09'} "
        f"source_required_day={'YES' if not has_source_not_extended else 'NO'} "
        f"runtime_required_day={'YES' if not has_source_not_extended else 'NO'} "
        f"source_buffer_day={'YES' if not has_source_not_extended else 'NO'} "
        f"runtime_buffer_day={'YES' if not has_source_not_extended else 'NO'}",
        "source_manifest_ref": {"artifact_path": str((tmp_path / "source_manifest.json").resolve()), "artifact_sha256": ""},
        "runtime_manifest_ref": {"artifact_path": str((tmp_path / "runtime_manifest.json").resolve()), "artifact_sha256": ""},
        "source_root": str((tmp_path / "source").resolve()),
        "truth_root": str(tmp_path.resolve()),
        "writer_owner": "market_calendar_coverage_authority_v1",
        "refresh_actions": [],
    }


def _artifact_row(
    tmp_path: Path,
    artifact_id: str,
    *,
    result_status: str = "PASS",
    role_class: str = "REQUIRED_DERIVED_GATE",
    classification: str = "TEST",
    blocking_reason_code: str = "",
    blocker_codes: list[str] | None = None,
    required: bool = True,
    path_family: str = "CANONICAL_RUNTIME_TRUTH_SUBPATH",
    target_day_expected: str = DAY,
    target_day_observed: str = DAY,
    date_binding_status: str = "MATCH",
    freshness_status: str = "CURRENT",
    provenance_present: bool = True,
    closure_status: str | None = None,
    observed_dependency_artifacts: list[str] | None = None,
) -> dict:
    if closure_status is None:
        closure_status = (
            "CLOSED"
            if result_status == "PASS"
            and path_family.startswith("CANONICAL_RUNTIME_TRUTH")
            and date_binding_status == "MATCH"
            and freshness_status == "CURRENT"
            and provenance_present
            else "OPEN"
        )
    return {
        "artifact_id": artifact_id,
        "artifact_name": artifact_id,
        "required": required,
        "role_class": role_class,
        "classification": classification,
        "canonical_path": str((tmp_path / f"{artifact_id}.json").resolve()),
        "authority_path": str((tmp_path / f"{artifact_id}.json").resolve()),
        "path_family": path_family,
        "observed_status": result_status,
        "result_status": result_status,
        "blocker_codes": blocker_codes or ([blocking_reason_code] if blocking_reason_code else []),
        "blocking_reason_code": blocking_reason_code,
        "schema_status": "VALID",
        "schema_ref": "governance/test.schema.json",
        "freshness_rule": "TARGET_DAY_MATCH_AND_TIMESTAMP_PRESENT",
        "freshness_status": freshness_status,
        "target_day_expected": target_day_expected,
        "target_day_observed": target_day_observed,
        "date_binding_status": date_binding_status,
        "date_binding_value": target_day_observed,
        "provenance_required": True,
        "provenance_summary": {
            "required": True,
            "present": provenance_present,
            "fields_present": ["producer.module", "generated_utc"] if provenance_present else [],
            "source": "test" if provenance_present else "",
        },
        "closure_status": closure_status,
        "producer": {"module": "test", "git_sha": "abc123"},
        "source_refs": [],
        "observed_dependency_artifacts": list(observed_dependency_artifacts or []),
    }


def _operator_summary_dossier(
    *,
    day_utc: str,
    truth_root: Path | None = None,
    current_state: str = "READY",
    submission_authorization_status: str = "AUTHORIZED",
    submission_authorized: bool = True,
    first_blocker_code: str = "",
    first_blocker_summary: str = "",
    ambiguity_status: str = "CLEAR",
    ambiguity_reason_codes: list[str] | None = None,
    ambiguity_conflicting_authorities: list[str] | None = None,
    recommended_operator_action: str = "No operator action required.",
    advisory_only_signals: list[dict] | None = None,
) -> dict:
    root = Path(truth_root or SOURCE_ROOT).resolve()
    dependency_rows = [
        ("active_session_v1", root / "active_session_v1" / "current.json"),
        ("target_day_admission_v1", root / "target_day_admission_v1" / f"{day_utc}.json"),
        ("target_day_build_v1", root / "target_day_build_v1" / f"{day_utc}.json"),
        (
            "submit_boundary_status_v1",
            root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json",
        ),
    ]
    dependency_refs = []
    missing_dependencies = []
    for artifact_id, path in dependency_rows:
        if path.exists() and path.is_file():
            dependency_refs.append(
                {
                    "artifact_id": artifact_id,
                    "path": str(path.resolve()),
                    "sha256": canonical_hash_for_c2_artifact_v1(json.loads(path.read_text(encoding="utf-8"))),
                    "artifact_class": "admission_result",
                    "finality_state": FINALITY_PROVISIONAL,
                }
            )
        else:
            missing_dependencies.append(artifact_id)

    payload = {
        "schema_id": "operator_summary_dossier",
        "schema_version": "v1",
        "generated_utc": "2026-04-10T12:00:00Z",
        "day_utc": day_utc,
        "subsystem_id": "operator_summary_authority",
        "current_state": current_state,
        "owner_ref": {"artifact_path": "", "artifact_sha256": ""},
        "first_blocker": {
            "reason_code": first_blocker_code,
            "summary": first_blocker_summary,
            "source": "operator_summary_authority_v1",
        },
        "upstream_evidence_refs": [],
        "ambiguity_state": {
            "status": ambiguity_status,
            "summary": first_blocker_summary if ambiguity_status == "AMBIGUOUS_AUTHORITY" else "",
            "reason_codes": list(ambiguity_reason_codes or []),
            "conflicting_authorities": list(ambiguity_conflicting_authorities or []),
        },
        "recommended_operator_action": recommended_operator_action,
        "authority_owner": "session_authority_status_v1",
        "active_day": day_utc,
        "admission_status": "ADMIT",
        "submission_authorization_status": submission_authorization_status,
        "submission_authorized": submission_authorized,
        "advisory_only_signals": list(advisory_only_signals or []),
        "is_canonical": False,
        "authority_level": "derived",
        "derived_from": [
            "active_session_v1",
            "target_day_admission_v1",
            "target_day_build_v1",
            "submit_boundary_status_v1",
        ],
        "legacy_surface_classification": [],
        "subsystem_dossier_refs": [],
    }
    blocker_envelope = build_machine_blocker_envelope_v1(
        closure_state=("COMPLETE" if not missing_dependencies else "BLOCKED"),
        reason_codes=(
            []
            if not missing_dependencies
            else [f"MISSING_GOVERNED_DEPENDENCY:{artifact_id}" for artifact_id in missing_dependencies]
        ),
        missing_dependency_artifacts=missing_dependencies,
    )
    payload["blocking_codes"] = list(blocker_envelope["blocking_codes"])
    payload["closure_state"] = str(blocker_envelope["closure_state"])
    payload["first_blocker_code"] = str(blocker_envelope["first_blocker_code"])
    payload["missing_dependency_artifacts"] = list(blocker_envelope["missing_dependency_artifacts"])
    payload["constitutional_dependency_declaration"] = build_artifact_dependency_declaration_v1(
        artifact_type="operator_summary_dossier_v1",
        artifact_class="admission_result",
        authority_id="operator_summary_dossier_v1",
        declared_dependency_artifacts=[
            "active_session_v1",
            "target_day_admission_v1",
            "target_day_build_v1",
            "submit_boundary_status_v1",
        ],
        dependency_refs=dependency_refs,
    )
    payload["constitutional_lineage"] = build_governed_artifact_lineage_v1(
        artifact_type="operator_summary_dossier_v1",
        artifact_version="v1",
        artifact_class="admission_result",
        authority_id="operator_summary_dossier_v1",
        producer_id="constellation_2.common.subsystem_authority_v1",
        generated_at_utc="2026-04-10T12:00:00Z",
        effective_at_utc="2026-04-10T12:00:00Z",
        finality_state=FINALITY_PROVISIONAL,
        input_artifact_refs=dependency_refs,
        policy_snapshot_refs=[],
        code_version="a" * 40,
        run_id=f"operator_summary_dossier:{day_utc}",
    )
    if truth_root is not None:
        out_path = root / "reports" / "operator_summary_dossier_v1" / day_utc / "operator_summary_dossier.v1.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return payload


@pytest.fixture(autouse=True)
def _stub_operator_summary_dossier(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "constellation_2.common.session_authority_monitor_v1.build_operator_summary_dossier_v1",
        lambda **kwargs: _operator_summary_dossier(
            day_utc=str(kwargs.get("day_utc") or DAY),
            truth_root=Path(kwargs.get("truth_root") or SOURCE_ROOT),
        ),
    )


def _write_state(
    tmp_path: Path,
    *,
    artifact_rows: list[dict],
    target_day: str = DAY,
    prior_active: bool = False,
    tamper_build_ref: bool = False,
    paper_authority_status: str = "GRANTED",
    paper_submission_authorized: bool = True,
    paper_degraded_mode: bool = False,
    paper_advisory_reason_codes: list[str] | None = None,
    paper_blocking_reason_codes: list[str] | None = None,
):
    write_market_calendar_coverage_status_v1(
        truth_root=tmp_path,
        payload=_coverage_payload(tmp_path=tmp_path, artifact_rows=artifact_rows, target_day=target_day),
    )
    build_ref = write_target_day_build_v1(
        truth_root=tmp_path,
        payload=derive_target_day_build_payload_v1(
            truth_root=tmp_path,
            target_day=target_day,
            artifact_results=artifact_rows,
            source_refs=[],
        ),
    )
    admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(
            truth_root=tmp_path,
            target_day=target_day,
            build_ref=build_ref,
        ),
    )
    prior_active_ref = None
    if prior_active:
        prior_build_ref = write_target_day_build_v1(
            truth_root=tmp_path,
            payload=derive_target_day_build_payload_v1(
                truth_root=tmp_path,
                target_day="2026-04-09",
                artifact_results=[
                    _artifact_row(
                        tmp_path,
                        "trading_day_state_machine_v1",
                        role_class="REQUIRED_EXECUTION_BOUNDARY",
                        target_day_expected="2026-04-09",
                        target_day_observed="2026-04-09",
                    )
                ],
                source_refs=[],
            ),
        )
        prior_admission_ref = write_target_day_admission_v1(
            truth_root=tmp_path,
            payload=derive_target_day_admission_payload_v1(
                truth_root=tmp_path,
                target_day="2026-04-09",
                build_ref=prior_build_ref,
            ),
        )
        prior_active_ref = write_active_session_v1(
            truth_root=tmp_path,
            payload=derive_active_session_payload_v1(
                truth_root=tmp_path,
                target_day="2026-04-09",
                admission_ref=prior_admission_ref,
            ),
        )

    active_payload = derive_active_session_payload_v1(
        truth_root=tmp_path,
        target_day=target_day,
        admission_ref=admission_ref,
        prior_active_session_ref=prior_active_ref,
    )
    active_ref = write_active_session_v1(truth_root=tmp_path, payload=active_payload)
    if tamper_build_ref:
        tampered_payload = json.loads(active_ref.path.read_text(encoding="utf-8"))
        tampered_payload["target_day_build_ref"] = {
            "artifact_path": str(build_ref.path),
            "artifact_sha256": "0" * 64,
        }
        active_ref.path.write_text(json.dumps(tampered_payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    _write_submit_boundary(tmp_path, target_day, authorized=True)
    _write_paper_session_authority(
        tmp_path,
        day_utc=target_day,
        authority_status=paper_authority_status,
        submission_authorized=paper_submission_authorized,
        degraded_mode=paper_degraded_mode,
        advisory_reason_codes=paper_advisory_reason_codes,
        blocking_reason_codes=paper_blocking_reason_codes,
    )
    return build_ref, admission_ref, active_ref


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def _write_paper_session_authority(
    truth_root: Path,
    *,
    day_utc: str,
    authority_status: str = "GRANTED",
    submission_authorized: bool = True,
    degraded_mode: bool = False,
    advisory_reason_codes: list[str] | None = None,
    blocking_reason_codes: list[str] | None = None,
    produced_utc: str | None = None,
) -> None:
    normalized_status = str(authority_status or "").strip().upper()
    if normalized_status not in {"GRANTED", "DENIED"}:
        raise ValueError(f"unsupported authority_status for test fixture: {authority_status!r}")
    blocker_codes = [] if normalized_status == "GRANTED" else list(blocking_reason_codes or ["PAPER_SESSION_AUTHORITY_DENIED"])
    advisory_codes = [str(code).strip() for code in (advisory_reason_codes or []) if str(code).strip()]
    open_allowed = normalized_status == "GRANTED"
    _write_json(
        truth_root / "reports" / "paper_session_authority_v1" / day_utc / "paper_session_authority.v1.json",
        {
            "schema_id": "paper_session_authority",
            "schema_version": "v1",
            "authority_scope": "CANONICAL_PAPER_SESSION_AUTHORITY",
            "day_utc": day_utc,
            "produced_utc": produced_utc or f"{day_utc}T00:00:00Z",
            "mode": "PAPER",
            "authority_status": normalized_status,
            "paper_open_allowed": open_allowed,
            "blocking_reason_codes": blocker_codes,
            "blocking_reason_details": (
                []
                if not blocker_codes
                else [
                    {
                        "reason_code": blocker_codes[0],
                        "blocker_class": "SAFETY_CRITICAL",
                        "check_id": "PRE_OPEN_BUNDLE_COMPLETE",
                        "summary": "BLOCKED",
                        "artifact_path": str(
                            truth_root / "reports" / "pre_open_bundle_v1" / day_utc / "pre_open_bundle.v1.json"
                        ),
                    }
                ]
            ),
            "safety_checks": [
                {
                    "check_id": "PAPER_CAPITAL_SEED_READY",
                    "status": "PASS",
                    "reason_code": "",
                    "summary": "OK",
                    "artifact_path": "",
                },
                {
                    "check_id": "OPERATOR_STATEMENT_READY",
                    "status": "PASS",
                    "reason_code": "",
                    "summary": "OK",
                    "artifact_path": "",
                },
                {
                    "check_id": "PRE_OPEN_BUNDLE_COMPLETE",
                    "status": "PASS" if open_allowed else "FAIL",
                    "reason_code": "" if open_allowed else blocker_codes[0],
                    "summary": "COMPLETE" if open_allowed else "BLOCKED",
                    "artifact_path": str(
                        truth_root / "reports" / "pre_open_bundle_v1" / day_utc / "pre_open_bundle.v1.json"
                    ),
                },
                {
                    "check_id": "CANONICAL_KILL_SWITCH_PRESENT",
                    "status": "PASS",
                    "reason_code": "",
                    "summary": "PRESENT",
                    "artifact_path": "",
                },
                {
                    "check_id": "CANONICAL_KILL_SWITCH_INACTIVE",
                    "status": "PASS",
                    "reason_code": "",
                    "summary": "INACTIVE",
                    "artifact_path": "",
                },
            ],
            "advisory_checks": [
                {
                    "check_id": "ADVISORY_ONLY",
                    "status": "ADVISORY",
                    "reason_code": code,
                    "summary": "advisory",
                    "artifact_path": "",
                }
                for code in advisory_codes
            ],
            "degraded_mode": bool(degraded_mode),
            "submission_authorized": bool(submission_authorized),
            "upstream_refs": {
                "paper_session_bootstrap_v1": str(
                    truth_root / "reports" / "paper_session_bootstrap_v1" / day_utc / "paper_session_bootstrap.v1.json"
                ),
                "paper_capital_seed": "",
                "operator_statement": "",
                "pre_open_bundle_v1": str(
                    truth_root / "reports" / "pre_open_bundle_v1" / day_utc / "pre_open_bundle.v1.json"
                ),
                "canonical_kill_switch_v1": "",
            },
            "producer": _canonical_producer(),
        },
    )


def _write_paper_day_control_plane(
    truth_root: Path,
    *,
    day_utc: str,
    control_plane_ready: bool,
    final_start_decision: str,
    authority_status: str,
    evaluated_at_utc: str,
) -> None:
    blocker_code = "" if control_plane_ready or authority_status == "GRANTED" else "LEDGER_BLOCKED"
    _write_json(
        truth_root / "reports" / "paper_day_control_plane_v1" / day_utc / "paper_day_control_plane.v1.json",
        {
            "schema_id": "paper_day_control_plane",
            "schema_version": "v1",
            "authority_scope": "SUPPORTING_DAY_CONTROL_ARTIFACT",
            "day_utc": day_utc,
            "startup_attempt_id": f"paper_day_start_attempt:{day_utc}:test",
            "evaluated_at_utc": evaluated_at_utc,
            "control_plane_id": f"paper_day_control_plane:{day_utc}:test",
            "producer": _canonical_producer(),
            "prerequisite_gate": {
                "prerequisite_status": "PASS",
                "prerequisite_blocking_codes": [],
                "prerequisite_artifact_refs": [],
                "first_missing_prerequisite": "",
            },
            "canonical_regeneration_results": [
                {
                    "logical_name": "submit_boundary_status_v1",
                    "path": str(
                        truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"
                    ),
                    "status": "AUTHORIZED" if authority_status == "GRANTED" else "DENIED",
                    "return_code": 0 if authority_status == "GRANTED" else 2,
                    "digest": "3" * 64,
                    "produced_at": evaluated_at_utc,
                },
                {
                    "logical_name": "paper_session_ledger_v1",
                    "path": str(
                        truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"
                    ),
                    "status": authority_status,
                    "return_code": 0 if authority_status == "GRANTED" else 2,
                    "digest": "4" * 64,
                    "produced_at": evaluated_at_utc,
                },
            ],
            "authority_result": {
                "ledger_path": str(
                    truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"
                ),
                "ledger_id": f"paper_session_ledger:{day_utc}:test",
                "ledger_authority_status": authority_status,
                "ledger_evidence_status": "READY" if authority_status == "GRANTED" else "DENY",
                "first_true_blocker_code": blocker_code if not control_plane_ready else "",
                "first_true_blocker_artifact_path": ""
                if control_plane_ready
                else str(
                    truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"
                ),
            },
            "startup_proof_result": {
                "startup_proof_validation_path": str(
                    truth_root / "reports" / "startup_proof_validation_v1" / day_utc / "startup_proof_validation.v1.json"
                ),
                "startup_proof_validation_status": "STARTUP_READY" if authority_status == "GRANTED" else "STARTUP_BLOCKED",
                "agreement_with_ledger": True,
            },
            "blocking_codes": [] if control_plane_ready else ["CONTROL_PLANE_BLOCKED"],
            "final_start_decision": final_start_decision,
            "human_readable_summary": "Supporting paper-day control plane for session authority monitor tests.",
            "ignored_legacy_surfaces": [
                {"logical_name": "trading_day_state_v1", "path": "/tmp/trading_day_state.v1.json", "exists": False}
            ],
            "suppression_reason": "NON_AUTHORITATIVE_FOR_STARTUP",
        },
    )


def _write_canonical_readiness_stack(
    truth_root: Path,
    *,
    day_utc: str,
    submit_authorized: bool,
    ledger_granted: bool,
    control_plane_ready: bool,
    submit_ts: str,
    ledger_ts: str,
    control_ts: str,
    paper_authority_status: str | None = None,
    paper_submission_authorized: bool | None = None,
) -> None:
    _write_startup_materialization(truth_root, day_utc, status="SUCCESS")
    _write_startup_proof_validation(truth_root, day_utc, ready=ledger_granted)
    _write_submit_boundary(truth_root, day_utc, authorized=submit_authorized)
    submit_path = truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"
    submit_payload = json.loads(submit_path.read_text(encoding="utf-8"))
    submit_payload["produced_at_utc"] = submit_ts
    _write_json(submit_path, submit_payload)

    _write_paper_session_ledger(truth_root, day_utc, authority_status="GRANTED" if ledger_granted else "DENIED")
    ledger_path = truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json"
    ledger_payload = json.loads(ledger_path.read_text(encoding="utf-8"))
    ledger_payload["evaluated_at_utc"] = ledger_ts
    _write_json(ledger_path, ledger_payload)

    _write_paper_day_control_plane(
        truth_root,
        day_utc=day_utc,
        control_plane_ready=control_plane_ready,
        final_start_decision="READY_NOW" if control_plane_ready else "BLOCKED_VALID",
        authority_status="GRANTED" if ledger_granted else "DENIED",
        evaluated_at_utc=control_ts,
    )
    resolved_authority_status = str(paper_authority_status or ("GRANTED" if ledger_granted else "DENIED")).strip().upper()
    _write_paper_session_authority(
        truth_root,
        day_utc=day_utc,
        authority_status=resolved_authority_status,
        submission_authorized=(
            bool(submit_authorized)
            if paper_submission_authorized is None and resolved_authority_status == "GRANTED"
            else bool(paper_submission_authorized)
        ),
        blocking_reason_codes=["PAPER_SESSION_AUTHORITY_DENIED"] if resolved_authority_status != "GRANTED" else [],
        produced_utc=control_ts,
    )


def _write_day_run_ledger(
    truth_root: Path,
    *,
    day_utc: str,
    final_status: str = "NOT_READY",
    canonical_phase: str = "MARKET_OPEN_DATA_GATE",
    canonical_blocker: str = "MARKET_CLOSED",
    updated_at_utc: str = "2026-04-11T22:12:00Z",
) -> None:
    _write_json(
        truth_root / "reports" / "aegis_day_run_v1" / day_utc / "day_run.v1.json",
        {
            "schema_id": "aegis_day_run.v1",
            "schema_version": "aegis_day_run.v1",
            "day_utc": day_utc,
            "environment": "PAPER",
            "final_status": final_status,
            "canonical_phase": canonical_phase,
            "canonical_blocker": canonical_blocker,
            "root_cause_chain": [canonical_blocker] if canonical_blocker else [],
            "downstream_consequences": [],
            "phase_order": [],
            "phase_results": {},
            "created_at_utc": updated_at_utc,
            "updated_at_utc": updated_at_utc,
            "source_repo_status": {},
            "operator_next_action": "",
        },
    )


def _write_execution_mode_authority(
    truth_root: Path,
    *,
    day_utc: str,
    mode_state: str = "PAPER_TRANSMIT_ENABLED",
    produced_utc: str = "2026-04-11T22:12:01Z",
) -> None:
    _write_json(
        truth_root / "reports" / "execution_mode_authority_v1" / day_utc / "execution_mode_authority.v1.json",
        {
            "schema_id": "C2_EXECUTION_MODE_AUTHORITY_V1",
            "schema_version": 1,
            "day_utc": day_utc,
            "environment": "PAPER",
            "status": "PASS",
            "mode_state": mode_state,
            "mode": mode_state,
            "broker_transmit_enabled": mode_state == "PAPER_TRANSMIT_ENABLED",
            "produced_utc": produced_utc,
            "first_blocker": "",
        },
    )


def _canonical_surface_map(payload: dict) -> dict[str, dict]:
    checks = [
        row
        for row in payload.get("monitoring_checks", [])
        if isinstance(row, dict) and row.get("check_name") == "canonical_readiness_authority"
    ]
    assert checks
    surfaces = checks[-1]["details"]["canonical_surfaces"]
    return {str(surface["surface_name"]): surface for surface in surfaces}


def _build_and_write_status(
    tmp_path: Path,
    *,
    current_state: str,
    submission_authorization_status: str,
    submission_authorized: bool,
    first_blocker_code: str = "",
    first_blocker_summary: str = "",
    generated_at: datetime,
    monkeypatch: pytest.MonkeyPatch,
) -> dict:
    monkeypatch.setattr(
        "constellation_2.common.session_authority_monitor_v1.build_operator_summary_dossier_v1",
        lambda **kwargs: _operator_summary_dossier(
            day_utc=str(kwargs.get("day_utc") or DAY),
            current_state=current_state,
            submission_authorization_status=submission_authorization_status,
            submission_authorized=submission_authorized,
            first_blocker_code=first_blocker_code,
            first_blocker_summary=first_blocker_summary,
        ),
    )
    payload = build_session_authority_status_payload_v1(
        truth_root=tmp_path,
        environment="PAPER",
        now=generated_at,
    )
    write_session_authority_status_v1(truth_root=tmp_path, payload=payload)
    return payload


def test_status_reports_healthy_admit_state(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")
    assert payload["semantic_status"] == "FULLY_OBSERVED_AND_CONFIRMED"
    assert payload["status_severity"] == STATUS_SEVERITY_INFO
    assert payload["target_day_admission_status"] == "ADMIT"
    assert payload["traceability_status"] == "VALID"


def test_paper_session_status_includes_runtime_ladder_canonical_surfaces(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    _write_canonical_readiness_stack(
        tmp_path,
        day_utc=DAY,
        submit_authorized=True,
        ledger_granted=True,
        control_plane_ready=True,
        submit_ts="2026-04-11T22:11:51Z",
        ledger_ts="2026-04-11T22:11:54Z",
        control_ts="2026-04-11T22:11:50Z",
    )
    _write_day_run_ledger(
        tmp_path,
        day_utc=DAY,
        final_status="NOT_READY",
        canonical_blocker="MARKET_CLOSED",
        updated_at_utc="2026-04-11T22:11:55Z",
    )
    _write_execution_mode_authority(
        tmp_path,
        day_utc=DAY,
        mode_state="PAPER_TRANSMIT_ENABLED",
        produced_utc="2026-04-11T22:11:56Z",
    )

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")
    surfaces = _canonical_surface_map(payload)

    assert surfaces["submit_boundary_status_v1"]["status_value"] == "AUTHORIZED"
    assert surfaces["aegis_day_run_v1"]["status_value"] == "NOT_READY"
    assert surfaces["paper_day_control_plane_v1"]["status_value"] == "READY_NOW"
    assert surfaces["execution_mode_authority_v1"]["status_value"] == "PAPER_TRANSMIT_ENABLED"


def test_runtime_readiness_ladder_uses_boundary_day_run_and_execution_mode_surfaces(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    _write_canonical_readiness_stack(
        tmp_path,
        day_utc=DAY,
        submit_authorized=True,
        ledger_granted=True,
        control_plane_ready=True,
        submit_ts="2026-04-11T22:11:51Z",
        ledger_ts="2026-04-11T22:11:54Z",
        control_ts="2026-04-11T22:11:50Z",
    )
    _write_day_run_ledger(tmp_path, day_utc=DAY, final_status="NOT_READY", updated_at_utc="2026-04-11T22:11:55Z")
    _write_execution_mode_authority(tmp_path, day_utc=DAY, mode_state="PAPER_TRANSMIT_ENABLED")

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")
    rows = {row["key"]: row for row in _build_readiness_ladder(payload, {})}

    assert rows["boundary"]["status"] == "AUTHORIZED"
    assert rows["ledger"]["status"] == "NOT_READY"
    assert rows["ledger"]["artifact_ref"]["surface_name"] == "aegis_day_run_v1"
    assert rows["control"]["status"] == "PAPER_TRANSMIT_ENABLED"
    assert rows["control"]["artifact_ref"]["surface_name"] == "execution_mode_authority_v1"


def test_runtime_readiness_ladder_unknown_only_when_runtime_surface_missing(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    submit_path = tmp_path / "reports" / "submit_boundary_status_v1" / DAY / "submit_boundary_status.v1.json"
    if submit_path.exists():
        submit_path.unlink()

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")
    surfaces = _canonical_surface_map(payload)
    rows = {row["key"]: row for row in _build_readiness_ladder(payload, {})}

    assert "submit_boundary_status_v1" not in surfaces
    assert "aegis_day_run_v1" not in surfaces
    assert "execution_mode_authority_v1" not in surfaces
    assert rows["boundary"]["status"] == "UNKNOWN"
    assert rows["ledger"]["status"] == "UNKNOWN"
    assert rows["control"]["status"] == "UNKNOWN"


def test_status_reports_withheld_rollover_state(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        prior_active=True,
        artifact_rows=[
            _artifact_row(
                tmp_path,
                "market_calendar_day",
                result_status="FAIL",
                role_class="REQUIRED_BINDING_INPUT",
                blocking_reason_code="SOURCE_NOT_EXTENDED",
                blocker_codes=["MARKET_CALENDAR_DAY_MISSING"],
                freshness_status="STALE",
                provenance_present=False,
                closure_status="OPEN",
            )
        ],
    )
    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")
    assert payload["semantic_status"] == "BLOCKED_BY_UPSTREAM_PREREQUISITE"
    assert payload["status_severity"] == STATUS_SEVERITY_CRITICAL
    assert payload["rollover_status"] == "ROLLOVER_WITHHELD"
    assert payload["blocked_target_day"] == DAY
    assert "SOURCE_NOT_EXTENDED" in payload["top_blocker_reason_codes"]


def test_status_escalates_traceability_failure_to_critical(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        tamper_build_ref=True,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")
    assert payload["semantic_status"] == "MATERIALIZED_AND_FAILED"
    assert payload["status_severity"] == STATUS_SEVERITY_CRITICAL
    assert payload["traceability_status"] == "BROKEN"
    assert "TRACEABILITY_BROKEN" in payload["top_blocker_reason_codes"]


def test_render_status_summary_includes_semantic_status_and_severity(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")

    summary = render_session_authority_status_summary_v1(payload)

    assert "semantic_status=FULLY_OBSERVED_AND_CONFIRMED" in summary
    assert "severity=INFO" in summary


def test_alert_emits_blocker_reason_codes_correctly(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        prior_active=True,
        artifact_rows=[
            _artifact_row(
                tmp_path,
                "trade_submit_readiness_c2_v1",
                freshness_status="STALE",
                blocking_reason_code="STALE_ARTIFACT",
                closure_status="OPEN",
            )
        ],
    )
    status_ref = write_session_authority_status_v1(
        truth_root=tmp_path,
        payload=build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER"),
    )
    alert_payload = derive_session_authority_alert_payload_v1(
        truth_root=tmp_path,
        environment="PAPER",
        status_ref=status_ref,
    )
    assert alert_payload["alert_status"] == ALERT_STATUS_ALERT
    assert "STALE_ARTIFACT" in alert_payload["alert_reason_codes"]


def test_alert_dedupe_for_unchanged_blocker_state(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        prior_active=True,
        artifact_rows=[
            _artifact_row(
                tmp_path,
                "market_calendar_day",
                result_status="FAIL",
                role_class="REQUIRED_BINDING_INPUT",
                blocking_reason_code="SOURCE_NOT_EXTENDED",
                blocker_codes=["MARKET_CALENDAR_DAY_MISSING"],
                freshness_status="STALE",
                provenance_present=False,
                closure_status="OPEN",
            )
        ],
    )
    status_ref = write_session_authority_status_v1(
        truth_root=tmp_path,
        payload=build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER"),
    )
    with pytest.MonkeyPatch.context() as mp:
        timestamps = iter(
            [
                "2026-04-11T00:00:01Z",
                "2026-04-11T00:00:02Z",
            ]
        )
        mp.setattr(session_authority_monitor_module, "_utc_now", lambda: next(timestamps))
        first_alert_ref = write_session_authority_alert_v1(
            truth_root=tmp_path,
            payload=derive_session_authority_alert_payload_v1(
                truth_root=tmp_path,
                environment="PAPER",
                status_ref=status_ref,
            ),
        )
        second_alert_payload = derive_session_authority_alert_payload_v1(
            truth_root=tmp_path,
            environment="PAPER",
            status_ref=status_ref,
            prior_alert_ref=first_alert_ref,
        )
    assert second_alert_payload["alert_status"] == ALERT_STATUS_DEDUPED
    assert second_alert_payload["dedupe_key"] == first_alert_ref.payload["dedupe_key"]
    assert second_alert_payload["repeat_count"] == 2
    assert second_alert_payload["first_seen_utc"] == "2026-04-11T00:00:01Z"
    assert second_alert_payload["last_seen_utc"] == "2026-04-11T00:00:02Z"


def test_alert_escalates_on_third_occurrence_for_same_day_and_warning_key(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        prior_active=True,
        artifact_rows=[
            _artifact_row(
                tmp_path,
                "market_calendar_day",
                result_status="FAIL",
                role_class="REQUIRED_BINDING_INPUT",
                blocking_reason_code="SOURCE_NOT_EXTENDED",
                blocker_codes=["MARKET_CALENDAR_DAY_MISSING"],
                freshness_status="STALE",
                provenance_present=False,
                closure_status="OPEN",
            )
        ],
    )
    status_ref = write_session_authority_status_v1(
        truth_root=tmp_path,
        payload=build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER"),
    )
    with pytest.MonkeyPatch.context() as mp:
        timestamps = iter(
            [
                "2026-04-11T00:00:01Z",
                "2026-04-11T00:00:02Z",
                "2026-04-11T00:00:03Z",
            ]
        )
        mp.setattr(session_authority_monitor_module, "_utc_now", lambda: next(timestamps))
        first_alert_ref = write_session_authority_alert_v1(
            truth_root=tmp_path,
            payload=derive_session_authority_alert_payload_v1(
                truth_root=tmp_path,
                environment="PAPER",
                status_ref=status_ref,
            ),
        )
        second_alert_ref = write_session_authority_alert_v1(
            truth_root=tmp_path,
            payload=derive_session_authority_alert_payload_v1(
                truth_root=tmp_path,
                environment="PAPER",
                status_ref=status_ref,
                prior_alert_ref=first_alert_ref,
            ),
        )
        third_alert_payload = derive_session_authority_alert_payload_v1(
            truth_root=tmp_path,
            environment="PAPER",
            status_ref=status_ref,
            prior_alert_ref=second_alert_ref,
        )
    assert third_alert_payload["alert_status"] == ALERT_STATUS_ESCALATED
    assert third_alert_payload["severity"] == STATUS_SEVERITY_ERROR
    assert third_alert_payload["repeat_count"] == 3
    assert third_alert_payload["first_seen_utc"] == "2026-04-11T00:00:01Z"
    assert third_alert_payload["last_seen_utc"] == "2026-04-11T00:00:03Z"


def test_alert_recovery_emits_clear_state(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        prior_active=True,
        artifact_rows=[
            _artifact_row(
                tmp_path,
                "trade_submit_readiness_c2_v1",
                freshness_status="STALE",
                blocking_reason_code="STALE_ARTIFACT",
                closure_status="OPEN",
            )
        ],
    )
    blocked_status_ref = write_session_authority_status_v1(
        truth_root=tmp_path,
        payload=build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER"),
    )
    prior_alert_ref = write_session_authority_alert_v1(
        truth_root=tmp_path,
        payload=derive_session_authority_alert_payload_v1(
            truth_root=tmp_path,
            environment="PAPER",
            status_ref=blocked_status_ref,
        ),
    )
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    healthy_status_ref = write_session_authority_status_v1(
        truth_root=tmp_path,
        payload=build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER"),
    )
    recovery_alert_payload = derive_session_authority_alert_payload_v1(
        truth_root=tmp_path,
        environment="PAPER",
        status_ref=healthy_status_ref,
        prior_alert_ref=prior_alert_ref,
    )
    assert recovery_alert_payload["alert_status"] == ALERT_STATUS_CLEAR


def test_status_surfaces_startup_input_convergence_failures(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        prior_active=True,
        artifact_rows=[
            _artifact_row(
                tmp_path,
                "paper_startup_intent_input_convergence_v1",
                result_status="FAIL",
                role_class="REQUIRED_BINDING_INPUT",
                blocking_reason_code="REQUIRED_GATE_FAIL",
                closure_status="OPEN",
            ),
            _artifact_row(
                tmp_path,
                "startup_materialization_input_convergence_v1",
                result_status="FAIL",
                role_class="REQUIRED_BINDING_INPUT",
                blocking_reason_code="REQUIRED_GATE_FAIL",
                closure_status="OPEN",
            ),
        ],
    )
    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")
    reason_codes = {row["reason_code"] for row in payload["monitoring_checks"] if row["reason_code"]}
    assert "PAPER_STARTUP_INTENT_INPUT_CONVERGENCE_BLOCKED" in reason_codes
    assert "STARTUP_MATERIALIZATION_INPUT_CONVERGENCE_BLOCKED" in reason_codes


def test_status_surfaces_precise_execution_root_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "constellation_2.common.session_authority_monitor_v1.build_operator_summary_dossier_v1",
        lambda **kwargs: _operator_summary_dossier(
            day_utc=str(kwargs.get("day_utc") or DAY),
            current_state="BLOCKED",
            submission_authorization_status="BLOCKED",
            submission_authorized=False,
            first_blocker_code="EXECUTION_ROOT_PATH_MISMATCH",
            first_blocker_summary="Submit/runtime path does not match the canonical sleeve execution root.",
            ambiguity_status="CLEAR",
            recommended_operator_action="Correct the runtime/submit path so it matches truth/sleeves/<sleeve_id>/<mode>/ before trusting execution readiness.",
        ),
    )
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")
    assert payload["status_severity"] == STATUS_SEVERITY_CRITICAL
    assert "EXECUTION_ROOT_PATH_MISMATCH" in payload["top_blocker_reason_codes"]
    assert payload["paper_authority_projection"]["authority_status"] == "GRANTED"
    assert payload["ambiguity_state"]["status"] == "CLEAR"


def test_status_surfaces_precise_execution_identity_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "constellation_2.common.session_authority_monitor_v1.build_operator_summary_dossier_v1",
        lambda **kwargs: _operator_summary_dossier(
            day_utc=str(kwargs.get("day_utc") or DAY),
            current_state="BLOCKED",
            submission_authorization_status="BLOCKED",
            submission_authorized=False,
            first_blocker_code="EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH",
            first_blocker_summary="Runtime client_id_orders does not match the governed execution identity.",
            ambiguity_status="CLEAR",
            recommended_operator_action="Correct the runtime orders client ID so it exactly matches the governed sleeve execution identity.",
        ),
    )
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")
    assert payload["status_severity"] == STATUS_SEVERITY_CRITICAL
    assert "EXECUTION_IDENTITY_CLIENT_ID_ORDERS_MISMATCH" in payload["top_blocker_reason_codes"]
    assert payload["paper_authority_projection"]["authority_status"] == "GRANTED"
    assert payload["ambiguity_state"]["status"] == "CLEAR"


def test_status_surfaces_readiness_bootstrap_ibapi_failure(tmp_path: Path) -> None:
    build_payload = derive_target_day_build_payload_v1(
        truth_root=tmp_path,
        target_day=DAY,
        artifact_results=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
        source_refs=[
            {
                "script": "ops/tools/run_session_readiness_refresh_v1.py",
                "command": [],
                "return_code": 2,
                "stdout": "{\"results\":{\"broker_events_bootstrap\":{\"returncode\":1,\"reason_codes\":[\"READINESS_BOOTSTRAP_IBAPI_IMPORT_FAILED\"]}}}",
                "stderr": "",
            }
        ],
    )
    build_ref = write_target_day_build_v1(truth_root=tmp_path, payload=build_payload)
    admission_ref = write_target_day_admission_v1(
        truth_root=tmp_path,
        payload=derive_target_day_admission_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            build_ref=build_ref,
        ),
    )
    write_active_session_v1(
        truth_root=tmp_path,
        payload=derive_active_session_payload_v1(
            truth_root=tmp_path,
            target_day=DAY,
            admission_ref=admission_ref,
        ),
    )
    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")
    reason_codes = {row["reason_code"] for row in payload["monitoring_checks"] if row["reason_code"]}
    assert "READINESS_BOOTSTRAP_IBAPI_IMPORT_FAILED" in reason_codes


def test_status_and_alert_traceability_refs_are_valid(tmp_path: Path) -> None:
    build_ref, admission_ref, active_ref = _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    status_ref = write_session_authority_status_v1(
        truth_root=tmp_path,
        payload=build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER"),
    )
    alert_ref = write_session_authority_alert_v1(
        truth_root=tmp_path,
        payload=derive_session_authority_alert_payload_v1(
            truth_root=tmp_path,
            environment="PAPER",
            status_ref=status_ref,
        ),
    )
    assert status_ref.payload["active_session_ref"]["artifact_path"] == str(active_ref.path)
    assert status_ref.payload["target_day_admission_ref"]["artifact_path"] == str(admission_ref.path)
    assert status_ref.payload["target_day_build_ref"]["artifact_path"] == str(build_ref.path)
    assert alert_ref.payload["source_status_ref"]["artifact_path"] == str(status_ref.path)
    assert alert_ref.payload["source_admission_ref"]["artifact_path"] == str(admission_ref.path)
    assert alert_ref.payload["source_build_ref"]["artifact_path"] == str(build_ref.path)


def test_alert_healthy_state_without_prior_alert_is_healthy(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    status_ref = write_session_authority_status_v1(
        truth_root=tmp_path,
        payload=build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER"),
    )
    alert_payload = derive_session_authority_alert_payload_v1(
        truth_root=tmp_path,
        environment="PAPER",
        status_ref=status_ref,
    )
    assert alert_payload["alert_status"] == ALERT_STATUS_HEALTHY


def test_wrapper_remains_session_authority_consumer_only() -> None:
    wrapper = Path("/home/node/constellation/ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh").read_text(encoding="utf-8")
    assert 'DAY="$(TZ=America/New_York date +%F)"' not in wrapper
    assert "run_session_authority_v1.py" in wrapper
    assert "run_session_authority_status_v1.py" not in wrapper
    assert "c2_session_authority_monitor_v1.sh" in wrapper


def test_read_status_invalidates_stale_current_when_newer_granted_canonical_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    _build_and_write_status(
        tmp_path,
        current_state="BLOCKED",
        submission_authorization_status="BLOCKED",
        submission_authorized=False,
        first_blocker_code="C2_KILL_SWITCH_ACTIVE",
        first_blocker_summary="Submit boundary has not granted submission authorization for the requested day.",
        generated_at=datetime(2026, 4, 11, 3, 53, 22, tzinfo=UTC),
        monkeypatch=monkeypatch,
    )
    _write_canonical_readiness_stack(
        tmp_path,
        day_utc=DAY,
        submit_authorized=True,
        ledger_granted=True,
        control_plane_ready=True,
        submit_ts="2026-04-11T22:11:51Z",
        ledger_ts="2026-04-11T22:11:54Z",
        control_ts="2026-04-11T22:11:50Z",
    )

    ref = read_session_authority_status_ref_v1(truth_root=tmp_path)
    persisted = json.loads(
        (tmp_path / "session_authority_status_v1" / "current.json").read_text(encoding="utf-8")
    )

    assert ref.payload["submission_authorized"] is False
    assert ref.payload["submission_authorization_status"] == "UNKNOWN"
    assert ref.payload["first_real_blocker_code"] == RC_SESSION_AUTHORITY_STATUS_STALE
    assert RC_SESSION_AUTHORITY_STATUS_STALE in ref.payload["top_blocker_reason_codes"]
    assert persisted["first_real_blocker_code"] == RC_SESSION_AUTHORITY_STATUS_STALE


def test_read_status_invalidates_stale_current_when_newer_blocked_canonical_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    _build_and_write_status(
        tmp_path,
        current_state="READY",
        submission_authorization_status="AUTHORIZED",
        submission_authorized=True,
        generated_at=datetime(2026, 4, 11, 3, 53, 22, tzinfo=UTC),
        monkeypatch=monkeypatch,
    )
    _write_canonical_readiness_stack(
        tmp_path,
        day_utc=DAY,
        submit_authorized=False,
        ledger_granted=False,
        control_plane_ready=False,
        submit_ts="2026-04-11T22:11:51Z",
        ledger_ts="2026-04-11T22:11:54Z",
        control_ts="2026-04-11T22:11:50Z",
    )

    ref = read_session_authority_status_ref_v1(truth_root=tmp_path)
    persisted = json.loads(
        (tmp_path / "session_authority_status_v1" / "current.json").read_text(encoding="utf-8")
    )

    assert ref.payload["submission_authorized"] is False
    assert ref.payload["submission_authorization_status"] == "UNKNOWN"
    assert ref.payload["first_real_blocker_code"] == RC_SESSION_AUTHORITY_STATUS_STALE
    assert RC_SESSION_AUTHORITY_STATUS_STALE in ref.payload["top_blocker_reason_codes"]
    assert persisted["first_real_blocker_code"] == RC_SESSION_AUTHORITY_STATUS_STALE


def test_build_status_uses_canonical_paper_authority_when_submit_boundary_disagrees(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    _write_canonical_readiness_stack(
        tmp_path,
        day_utc=DAY,
        submit_authorized=False,
        ledger_granted=False,
        control_plane_ready=False,
        submit_ts="2026-04-11T12:11:51Z",
        ledger_ts="2026-04-11T12:11:54Z",
        control_ts="2026-04-11T12:11:50Z",
        paper_authority_status="GRANTED",
        paper_submission_authorized=True,
    )
    monkeypatch.setattr(
        "constellation_2.common.session_authority_monitor_v1.build_operator_summary_dossier_v1",
        lambda **kwargs: _operator_summary_dossier(
            day_utc=str(kwargs.get("day_utc") or DAY),
            current_state="READY",
            submission_authorization_status="AUTHORIZED",
            submission_authorized=True,
        ),
    )

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")

    assert payload["submission_authorized"] is True
    assert payload["submission_authorization_status"] == "AUTHORIZED"
    assert payload["first_real_blocker_code"] == ""
    assert payload["paper_authority_projection"]["authority_status"] == "GRANTED"
    assert payload["paper_authority_projection"]["paper_open_allowed"] is True
    assert payload["paper_authority_projection"]["blocker_reason_codes"] == []
    assert RC_SESSION_AUTHORITY_CANONICAL_MISMATCH not in payload["top_blocker_reason_codes"]


def test_build_status_uses_canonical_paper_authority_when_ledger_disagrees(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    _write_canonical_readiness_stack(
        tmp_path,
        day_utc=DAY,
        submit_authorized=True,
        ledger_granted=False,
        control_plane_ready=False,
        submit_ts="2026-04-11T12:11:51Z",
        ledger_ts="2026-04-11T12:11:54Z",
        control_ts="2026-04-11T12:11:50Z",
        paper_authority_status="GRANTED",
        paper_submission_authorized=True,
    )
    monkeypatch.setattr(
        "constellation_2.common.session_authority_monitor_v1.build_operator_summary_dossier_v1",
        lambda **kwargs: _operator_summary_dossier(
            day_utc=str(kwargs.get("day_utc") or DAY),
            current_state="READY",
            submission_authorization_status="AUTHORIZED",
            submission_authorized=True,
        ),
    )

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")

    assert payload["submission_authorized"] is True
    assert payload["submission_authorization_status"] == "AUTHORIZED"
    assert payload["first_real_blocker_code"] == ""
    assert payload["paper_authority_projection"]["authority_status"] == "GRANTED"
    assert payload["paper_authority_projection"]["paper_open_allowed"] is True
    assert payload["paper_authority_projection"]["blocker_reason_codes"] == []
    assert RC_SESSION_AUTHORITY_CANONICAL_MISMATCH not in payload["top_blocker_reason_codes"]


def test_build_status_keeps_submission_visibility_when_paper_authority_grants_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    _write_canonical_readiness_stack(
        tmp_path,
        day_utc=DAY,
        submit_authorized=True,
        ledger_granted=True,
        control_plane_ready=False,
        submit_ts="2026-04-11T12:11:51Z",
        ledger_ts="2026-04-11T12:11:54Z",
        control_ts="2026-04-11T12:11:50Z",
        paper_authority_status="GRANTED",
        paper_submission_authorized=False,
    )
    monkeypatch.setattr(
        "constellation_2.common.session_authority_monitor_v1.build_operator_summary_dossier_v1",
        lambda **kwargs: _operator_summary_dossier(
            day_utc=str(kwargs.get("day_utc") or DAY),
            current_state="READY",
            submission_authorization_status="AUTHORIZED",
            submission_authorized=True,
        ),
    )

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")

    assert payload["submission_authorized"] is False
    assert payload["submission_authorization_status"] == "DENIED"
    assert payload["first_real_blocker_code"] == ""
    assert payload["paper_authority_projection"]["authority_status"] == "GRANTED"
    assert payload["paper_authority_projection"]["paper_open_allowed"] is True
    assert payload["paper_authority_projection"]["submission_authorized"] is False
    assert RC_SESSION_AUTHORITY_CANONICAL_MISMATCH not in payload["top_blocker_reason_codes"]


def test_build_status_passes_when_aligned_with_latest_canonical_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    _write_canonical_readiness_stack(
        tmp_path,
        day_utc=DAY,
        submit_authorized=True,
        ledger_granted=True,
        control_plane_ready=True,
        submit_ts="2026-04-11T12:11:51Z",
        ledger_ts="2026-04-11T12:11:54Z",
        control_ts="2026-04-11T12:11:50Z",
    )
    monkeypatch.setattr(
        "constellation_2.common.session_authority_monitor_v1.build_operator_summary_dossier_v1",
        lambda **kwargs: _operator_summary_dossier(
            day_utc=str(kwargs.get("day_utc") or DAY),
            current_state="READY",
            submission_authorization_status="AUTHORIZED",
            submission_authorized=True,
        ),
    )

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")

    assert payload["submission_authorized"] is True
    assert payload["submission_authorization_status"] == "AUTHORIZED"
    assert payload["first_real_blocker_code"] == ""
    assert RC_SESSION_AUTHORITY_CANONICAL_MISMATCH not in payload["top_blocker_reason_codes"]
    assert RC_SESSION_AUTHORITY_STATUS_STALE not in payload["top_blocker_reason_codes"]


def test_build_status_surfaces_degraded_mode_from_canonical_paper_authority(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
        paper_authority_status="GRANTED",
        paper_submission_authorized=True,
        paper_degraded_mode=True,
        paper_advisory_reason_codes=["LEGACY_CONTINUITY_MISSING"],
    )

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")

    projection = payload["paper_authority_projection"]
    assert projection["authority_status"] == "GRANTED"
    assert projection["paper_open_allowed"] is True
    assert projection["degraded_mode"] is True
    assert projection["advisory_count"] == 1
    assert projection["advisory_reason_codes"] == ["LEGACY_CONTINUITY_MISSING"]
    assert projection["advisory_reason_details"] == [
        {
            "reason_code": "LEGACY_CONTINUITY_MISSING",
            "severity": "INFO",
            "check_id": "ADVISORY_ONLY",
            "summary": "advisory",
            "artifact_path": "",
        }
    ]
    assert projection["highest_advisory_severity"] == "INFO"
    assert projection["blocker_reason_codes"] == []
    assert payload["top_blocker_reason_codes"] == ["LEGACY_CONTINUITY_MISSING"]
    assert payload["status_severity"] == STATUS_SEVERITY_WARNING


def test_build_status_surfaces_advisory_severity_tiers_without_changing_open_semantics(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
        paper_authority_status="GRANTED",
        paper_submission_authorized=False,
        paper_degraded_mode=True,
        paper_advisory_reason_codes=[
            "LEGACY_CONTINUITY_MISSING",
            "SOURCE_NOT_EXTENDED",
            "HIDDEN_DEPENDENCY_DETECTED",
        ],
    )

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")

    projection = payload["paper_authority_projection"]
    advisory_details = {
        str(row["reason_code"]): str(row["severity"])
        for row in projection["advisory_reason_details"]
    }
    assert projection["authority_status"] == "GRANTED"
    assert projection["paper_open_allowed"] is True
    assert projection["degraded_mode"] is True
    assert projection["submission_authorized"] is False
    assert advisory_details["LEGACY_CONTINUITY_MISSING"] == "INFO"
    assert advisory_details["SOURCE_NOT_EXTENDED"] == "WARNING"
    assert advisory_details["HIDDEN_DEPENDENCY_DETECTED"] == "CRITICAL"
    assert projection["highest_advisory_severity"] == "CRITICAL"
    assert payload["first_real_blocker_code"] == ""
    assert payload["submission_authorization_status"] == "DENIED"


def test_build_status_carries_canonical_blocker_reason_codes_unchanged(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
        paper_authority_status="DENIED",
        paper_submission_authorized=False,
        paper_blocking_reason_codes=["CANONICAL_KILL_SWITCH_ACTIVE"],
    )

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")

    projection = payload["paper_authority_projection"]
    assert projection["authority_status"] == "DENIED"
    assert projection["paper_open_allowed"] is False
    assert projection["blocker_reason_codes"] == ["CANONICAL_KILL_SWITCH_ACTIVE"]
    assert projection["advisory_reason_details"] == []
    assert projection["highest_advisory_severity"] == "NONE"
    assert payload["first_real_blocker_code"] == "CANONICAL_KILL_SWITCH_ACTIVE"
    assert payload["top_blocker_reason_codes"] == ["CANONICAL_KILL_SWITCH_ACTIVE"]
    assert payload["status_severity"] == STATUS_SEVERITY_CRITICAL


def test_build_status_operator_projection_remains_canonical_derivation_only(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
        paper_authority_status="DENIED",
        paper_submission_authorized=False,
        paper_blocking_reason_codes=["CANONICAL_KILL_SWITCH_ACTIVE"],
    )

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")

    projection = payload["paper_authority_projection"]
    projection_checks = [
        row
        for row in payload["monitoring_checks"]
        if row.get("check_name") == "paper_session_authority_projection"
    ]
    assert projection_checks
    assert projection_checks[-1]["status"] == "PASS"
    assert projection_checks[-1]["artifact_ref"] == projection["authority_artifact_ref"]
    assert projection["blocker_reason_codes"] == ["CANONICAL_KILL_SWITCH_ACTIVE"]
    assert payload["first_real_blocker_code"] == projection["first_blocker_code"]
    assert payload["submission_authorized"] == projection["submission_authorized"]
    assert payload["operator_summary_authority_owner"] == "paper_session_authority_v1"


def test_build_status_fails_closed_when_paper_session_authority_missing(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    authority_path = (
        tmp_path
        / "reports"
        / "paper_session_authority_v1"
        / DAY
        / "paper_session_authority.v1.json"
    )
    authority_path.unlink()

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")

    projection = payload["paper_authority_projection"]
    assert projection["authority_status"] == "DENIED"
    assert projection["paper_open_allowed"] is False
    assert "paper_session_authority_v1" in payload["missing_dependency_artifacts"]
    assert payload["first_real_blocker_code"] == "SESSION_AUTHORITY_ARTIFACT_MISSING"
    assert "SESSION_AUTHORITY_ARTIFACT_MISSING" in payload["top_blocker_reason_codes"]
    projection_checks = [
        row for row in payload["monitoring_checks"] if row.get("check_name") == "paper_session_authority_projection"
    ]
    assert projection_checks
    assert projection_checks[-1]["status"] == "FAIL"


def test_live_environment_keeps_paper_projection_contract_unused(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="LIVE")

    projection = payload["paper_authority_projection"]
    assert projection["contract_used"] is False
    assert projection["authority_status"] == "UNKNOWN"
    assert projection["open_state"] == "UNKNOWN"
    assert projection["advisory_reason_details"] == []
    assert projection["highest_advisory_severity"] == "NONE"
    assert payload["derived_from"][:3] == [
        "submit_boundary_status_v1",
        "paper_session_ledger_v1",
        "paper_day_control_plane_v1",
    ]


def test_live_contradiction_class_regression_blocks_trusted_current_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    _build_and_write_status(
        tmp_path,
        current_state="BLOCKED",
        submission_authorization_status="BLOCKED",
        submission_authorized=False,
        first_blocker_code="C2_KILL_SWITCH_ACTIVE",
        first_blocker_summary="Submit boundary has not granted submission authorization for the requested day.",
        generated_at=datetime(2026, 4, 11, 3, 53, 22, tzinfo=UTC),
        monkeypatch=monkeypatch,
    )
    _write_canonical_readiness_stack(
        tmp_path,
        day_utc=DAY,
        submit_authorized=True,
        ledger_granted=True,
        control_plane_ready=True,
        submit_ts="2026-04-11T22:11:51Z",
        ledger_ts="2026-04-11T22:11:54Z",
        control_ts="2026-04-11T22:11:50Z",
    )

    ref = read_session_authority_status_ref_v1(truth_root=tmp_path)

    assert ref.payload["submission_authorized"] is False
    assert ref.payload["submission_authorization_status"] == "UNKNOWN"
    assert ref.payload["first_real_blocker_code"] == RC_SESSION_AUTHORITY_STATUS_STALE


def test_session_authority_status_writes_per_day_history_and_current_matches_day_file(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")
    ref = write_session_authority_status_v1(truth_root=tmp_path, payload=payload)
    day_path = resolve_session_authority_status_day_path(truth_root=tmp_path, day_utc=DAY)

    assert day_path.exists()
    day_payload = json.loads(day_path.read_text(encoding="utf-8"))
    current_payload = json.loads((tmp_path / "session_authority_status_v1" / "current.json").read_text(encoding="utf-8"))
    assert ref.payload == day_payload
    assert current_payload == day_payload


def test_session_authority_status_history_is_retained_across_days(tmp_path: Path) -> None:
    first_day = "2026-04-14"
    second_day = "2026-04-15"

    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
        target_day=first_day,
    )
    write_session_authority_status_v1(
        truth_root=tmp_path,
        payload=build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER"),
    )
    first_day_path = resolve_session_authority_status_day_path(truth_root=tmp_path, day_utc=first_day)
    first_day_bytes = first_day_path.read_bytes()

    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT", target_day_expected=second_day, target_day_observed=second_day),
            _artifact_row(tmp_path, "paper_policy_verdict_v1", target_day_expected=second_day, target_day_observed=second_day),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1", target_day_expected=second_day, target_day_observed=second_day),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY", target_day_expected=second_day, target_day_observed=second_day),
        ],
        target_day=second_day,
    )
    second_ref = write_session_authority_status_v1(
        truth_root=tmp_path,
        payload=build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER"),
    )
    second_day_path = resolve_session_authority_status_day_path(truth_root=tmp_path, day_utc=second_day)

    assert first_day_path.read_bytes() == first_day_bytes
    assert second_day_path.exists()
    assert second_ref.payload == json.loads(second_day_path.read_text(encoding="utf-8"))
    assert second_ref.payload == json.loads((tmp_path / "session_authority_status_v1" / "current.json").read_text(encoding="utf-8"))


def test_session_authority_status_includes_audit_labels_and_ready_state_has_no_blocker_codes(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")

    assert payload["is_canonical"] is False
    assert payload["authority_level"] == "derived"
    assert payload["derived_from"][:3] == [
        "paper_session_authority_v1",
        "active_session_v1",
        "target_day_admission_v1",
    ]
    assert payload["submission_authorization_status"] == "AUTHORIZED"
    assert payload["submission_authorized"] is True
    assert payload["first_real_blocker_code"] == ""
    assert payload["top_blocker_reason_codes"] == []
    assert payload["semantic_status"] == "FULLY_OBSERVED_AND_CONFIRMED"


def test_session_authority_status_emits_constitutional_metadata_when_ready(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")

    validated = validate_governed_artifact_payload_v1(
        repo_root=SOURCE_ROOT,
        artifact_id="session_authority_status_v1",
        payload=payload,
    )

    assert payload["closure_state"] == "COMPLETE"
    assert payload["blocking_codes"] == []
    assert payload["missing_dependency_artifacts"] == []
    assert payload["constitutional_lineage"]["artifact_type"] == "session_authority_status_v1"
    assert validated["constitutional_dependency_declaration"]["declared_dependency_artifacts"] == [
        "active_session_v1",
        "target_day_admission_v1",
        "target_day_build_v1",
        "operator_summary_dossier_v1",
        "paper_session_authority_v1",
    ]
    dependency_refs = validated["constitutional_dependency_declaration"]["dependency_refs"]
    dependency_by_id = {str(row["artifact_id"]): row for row in dependency_refs}
    assert "paper_session_authority_v1" in dependency_by_id
    assert dependency_by_id["paper_session_authority_v1"]["path"] == payload["paper_authority_projection"]["authority_artifact_ref"]["artifact_path"]
    assert dependency_by_id["paper_session_authority_v1"]["sha256"] == payload["paper_authority_projection"]["authority_artifact_ref"]["artifact_sha256"]


def test_session_authority_status_blocks_with_missing_governed_build_dependency(tmp_path: Path) -> None:
    _write_state(
        tmp_path,
        artifact_rows=[
            _artifact_row(tmp_path, "market_calendar_day", role_class="REQUIRED_BINDING_INPUT"),
            _artifact_row(tmp_path, "paper_policy_verdict_v1"),
            _artifact_row(tmp_path, "trade_submit_readiness_c2_v1"),
            _artifact_row(tmp_path, "trading_day_state_machine_v1", role_class="REQUIRED_EXECUTION_BOUNDARY"),
        ],
    )
    active_payload = json.loads((tmp_path / "active_session_v1" / "current.json").read_text(encoding="utf-8"))
    build_ref = dict(active_payload.get("target_day_build_ref") or {})
    build_path = Path(str(build_ref.get("artifact_path") or ""))
    build_path.unlink()

    payload = build_session_authority_status_payload_v1(truth_root=tmp_path, environment="PAPER")
    validated = validate_governed_artifact_payload_v1(
        repo_root=SOURCE_ROOT,
        artifact_id="session_authority_status_v1",
        payload=payload,
    )

    assert payload["closure_state"] == "BLOCKED"
    assert payload["missing_dependency_artifacts"] == ["target_day_build_v1"]
    assert payload["first_blocker_code"] != ""
    assert validated["constitutional_lineage"]["artifact_type"] == "session_authority_status_v1"
    assert validated["constitutional_dependency_declaration"]["declared_dependency_artifacts"] == [
        "active_session_v1",
        "target_day_admission_v1",
        "target_day_build_v1",
        "operator_summary_dossier_v1",
        "paper_session_authority_v1",
    ]
