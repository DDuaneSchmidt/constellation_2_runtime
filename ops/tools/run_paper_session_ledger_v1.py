#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Callable

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
import sys

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    build_constitutional_fact_bundle_v1,
    build_constitutional_surface_fact_record_v1,
    canonical_paper_session_id_v1,
    now_utc_iso_v1,
    producer_block_v1,
    read_json_object_v1,
    read_paper_trading_posture_ref_v1,
    read_startup_materialization_ref_v1,
    read_submit_boundary_status_ref_v1,
    read_sleeve_rollup_ref_v1,
    resolve_fact_plane_truth_root_v1,
    resolve_paper_intent_truth_root_v1,
    resolve_market_calendar_record_v1,
    sha256_file_v1,
)
from constellation_2.common.paper_session_authority_v1 import read_paper_session_authority_ref_v1
from constellation_2.common.constitutional_decision_v1 import evaluate_constitutional_decision_v1
from constellation_2.common.constitutional_proposal_v1 import (
    build_session_submission_proposal_v1,
    proposal_hash_v1,
)
from constellation_2.common.pre_open_materializer_v1 import (
    read_pre_open_bundle_ref_v1,
    resolve_pre_open_bundle_path_v1,
)
from constellation_2.common.paper_session_ledger_v1 import (
    build_paper_session_ledger_v1,
    write_paper_session_ledger_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_paper_session_authority_path,
    resolve_paper_session_ledger_path,
    resolve_paper_trading_posture_path,
    resolve_sleeve_rollup_path,
    resolve_startup_materialization_path,
    resolve_submit_boundary_status_path,
)
from constellation_2.common.execution_evidence_current_head_v1 import current_head_output_path
from constellation_2.common.submission_index_v1 import submission_index_output_path
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1

GATE_HIERARCHY_POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/GATE_HIERARCHY_V1.json").resolve()
CONSTITUTIONAL_SHADOW_POLICY_VERSION = "constitutional_shadow_v1"


def _blocking_codes(*codes: str) -> list[str]:
    return sorted({str(code).strip() for code in codes if str(code).strip()})


def _safe_read_surface(
    *,
    logical_name: str,
    day_utc: str,
    session_id: str,
    resolver: Callable[..., Path],
    reader: Callable[..., SurfaceRefV1],
    truth_root: Path,
    required_for_authority: bool,
) -> tuple[dict[str, Any], SurfaceRefV1 | None]:
    target_path = resolver(truth_root=truth_root, day_utc=day_utc)
    lookup_evidence = [f"resolved_path={target_path}"]
    try:
        ref = reader(truth_root=truth_root, day_utc=day_utc)
    except Exception as exc:
        error_text = f"{type(exc).__name__}:{exc}"
        path_exists = target_path.exists() and target_path.is_file()
        return (
            {
                "logical_name": logical_name,
                "required_for_authority": bool(required_for_authority),
                "absolute_path": str(target_path),
                "schema_id": "",
                "schema_version": "",
                "producer": {"repo": "", "module": "", "git_sha": ""},
                "artifact_timestamp_utc": "",
                "artifact_day_utc": "",
                "artifact_session_id": "",
                "content_hash": sha256_file_v1(target_path) if path_exists else "",
                "presence_verdict": "PRESENT" if path_exists else "MISSING",
                "schema_verdict": "INVALID" if path_exists else "UNKNOWN",
                "linkage_verdict": "UNKNOWN",
                "freshness_verdict": "UNKNOWN",
                "duplicate_resolution_verdict": "SINGLE_CANONICAL_PATH",
                "blocking_codes": _blocking_codes(f"LEDGER_INPUT_UNUSABLE:{logical_name}"),
                "lookup_evidence": lookup_evidence + [error_text],
                "fact_snapshot": {},
            },
            None,
        )
    payload = dict(ref.payload)
    producer = payload.get("producer") if isinstance(payload.get("producer"), dict) else {}
    if logical_name == "paper_session_authority_v1":
        payload_day = str(payload.get("day_utc") or "").strip()
        payload_session = session_id
        produced_at = str(payload.get("produced_utc") or payload.get("produced_at_utc") or "").strip()
    elif logical_name == "pre_open_bundle_v1":
        payload_day = str(payload.get("target_day") or payload.get("day_utc") or "").strip()
        payload_session = str(
            payload.get("session_id")
            or payload.get("admitted_session_id")
            or canonical_paper_session_id_v1(payload_day or day_utc)
        ).strip()
        produced_at = str(payload.get("built_at_utc") or payload.get("produced_at_utc") or payload.get("produced_utc") or "").strip()
    else:
        payload_day = str(payload.get("day_utc") or "").strip()
        payload_session = str(payload.get("session_id") or payload.get("admitted_session_id") or "").strip()
        produced_at = str(payload.get("produced_at_utc") or payload.get("produced_utc") or "").strip()
    linkage = str(payload.get("linkage_verdict") or ("LINKED" if payload_session == session_id else "UNLINKED")).strip().upper()
    freshness = str(payload.get("freshness_verdict") or ("CURRENT" if payload_day == day_utc else "STALE")).strip().upper()
    schema_id = str(payload.get("schema_id") or "").strip()
    schema_version = str(payload.get("schema_version") or "").strip()
    row = {
        "logical_name": logical_name,
        "required_for_authority": bool(required_for_authority),
        "absolute_path": str(ref.path),
        "schema_id": schema_id,
        "schema_version": schema_version,
        "producer": {
            "repo": str(producer.get("repo") or "").strip(),
            "module": str(producer.get("module") or "").strip(),
            "git_sha": str(producer.get("git_sha") or "").strip(),
        },
        "artifact_timestamp_utc": produced_at,
        "artifact_day_utc": payload_day,
        "artifact_session_id": payload_session,
        "content_hash": str(ref.sha256),
        "presence_verdict": "PRESENT",
        "schema_verdict": "VALID",
        "linkage_verdict": linkage if payload_session else "UNKNOWN",
        "freshness_verdict": freshness if produced_at else "UNKNOWN",
        "duplicate_resolution_verdict": "SINGLE_CANONICAL_PATH",
        "blocking_codes": _blocking_codes(
            *list(payload.get("blocking_codes") or []),
            *list(payload.get("blocking_reason_codes") or []),
        ),
        "lookup_evidence": lookup_evidence,
        "fact_snapshot": {},
    }
    if logical_name == "paper_session_authority_v1":
        row["fact_snapshot"] = {
            "authority_status": str(payload.get("authority_status") or "").strip().upper(),
            "paper_open_allowed": bool(payload.get("paper_open_allowed") is True),
            "degraded_mode": bool(payload.get("degraded_mode") is True),
            "submission_authorized": bool(payload.get("submission_authorized") is True),
            "advisory_reason_codes": sorted(
                {
                    str(item.get("reason_code") or "").strip()
                    for item in (payload.get("advisory_checks") or [])
                    if isinstance(item, dict) and str(item.get("reason_code") or "").strip()
                }
            ),
        }
    elif logical_name == "pre_open_bundle_v1":
        row["fact_snapshot"] = {
            "materialization_state": str(payload.get("materialization_state") or "").strip().upper(),
            "completion_state": str(payload.get("completion_state") or "").strip().upper(),
        }
    elif logical_name == "startup_materialization_v1":
        row["fact_snapshot"] = {"status": str(payload.get("status") or "").strip().upper()}
    elif logical_name == "paper_trading_posture_v1":
        row["fact_snapshot"] = {
            "posture_status": str(payload.get("posture_status") or "").strip().upper(),
            "system_ready": bool(payload.get("system_ready") is True),
        }
    elif logical_name == "submit_boundary_status_v1":
        row["fact_snapshot"] = {
            "boundary_status": str(payload.get("boundary_status") or "").strip().upper(),
            "submission_authorized": bool(payload.get("submission_authorized") is True),
        }
    elif logical_name == "sleeve_rollup_v1":
        row["fact_snapshot"] = {"status": str(payload.get("status") or "").strip().upper()}
    return row, ref


def _evidence_freeze_from_rows(*, rows: list[dict[str, Any]]) -> dict[str, Any]:
    authority_rows = [dict(row) for row in rows if bool(row.get("required_for_authority") is True)]
    strict_blocking_logical_names = {"paper_session_authority_v1"}
    blocking_codes: list[str] = []
    for row in authority_rows:
        logical_name = str(row["logical_name"])
        if logical_name in strict_blocking_logical_names:
            if row.get("presence_verdict") != "PRESENT":
                blocking_codes.append(f"LEDGER_EVIDENCE_INPUT_MISSING:{logical_name}")
            if row.get("schema_verdict") != "VALID":
                blocking_codes.append(f"LEDGER_EVIDENCE_SCHEMA_INVALID:{logical_name}")
            if row.get("linkage_verdict") != "LINKED":
                blocking_codes.append(f"LEDGER_EVIDENCE_LINKAGE_INVALID:{logical_name}")
            if row.get("freshness_verdict") != "CURRENT":
                blocking_codes.append(f"LEDGER_EVIDENCE_FRESHNESS_INVALID:{logical_name}")
            blocking_codes.extend(str(code).strip() for code in (row.get("blocking_codes") or []) if str(code).strip())
    freeze_inputs = sorted(authority_rows, key=lambda row: str(row["logical_name"]))
    evidence_digest = __import__("hashlib").sha256(
        json.dumps(freeze_inputs, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "evidence_digest": evidence_digest,
        "overall_evidence_status": "READY" if not blocking_codes else "DENY",
        "blocking_codes": sorted(set(blocking_codes)),
        "inputs": freeze_inputs,
    }


def _control_from_rows(
    *,
    evidence_freeze: dict[str, Any],
    rows_by_name: dict[str, dict[str, Any]],
) -> tuple[str, bool, bool, list[str], list[str]]:
    blocking_codes = [str(code).strip() for code in (evidence_freeze.get("blocking_codes") or []) if str(code).strip()]
    advisory_codes: list[str] = []
    if str(evidence_freeze.get("overall_evidence_status") or "").strip().upper() != "READY":
        return (
            "DENIED",
            False,
            False,
            sorted(set(blocking_codes + ["PAPER_SESSION_LEDGER_EVIDENCE_DENIED"])),
            advisory_codes,
        )
    authority = rows_by_name["paper_session_authority_v1"]
    authority_snapshot = authority.get("fact_snapshot") if isinstance(authority.get("fact_snapshot"), dict) else {}
    authority_status = str(authority_snapshot.get("authority_status") or "").strip().upper()
    paper_open_allowed = bool(authority_snapshot.get("paper_open_allowed") is True)
    advisory_codes.extend(
        str(code).strip()
        for code in (authority_snapshot.get("advisory_reason_codes") or [])
        if str(code).strip()
    )
    if authority_status != "GRANTED" or not paper_open_allowed:
        blocking_codes.append("PAPER_SESSION_AUTHORITY_DENIED")
        return ("DENIED", False, False, sorted(set(blocking_codes)), sorted(set(advisory_codes)))

    pre_open = rows_by_name["pre_open_bundle_v1"]
    startup = rows_by_name["startup_materialization_v1"]
    posture = rows_by_name["paper_trading_posture_v1"]
    boundary = rows_by_name["submit_boundary_status_v1"]

    pre_open_complete = str(pre_open.get("fact_snapshot", {}).get("materialization_state") or "").strip().upper() == "COMPLETE"
    startup_ok = str(startup.get("fact_snapshot", {}).get("status") or "").strip().upper() == "SUCCESS"
    posture_enabled = str(posture.get("fact_snapshot", {}).get("posture_status") or "").strip().upper() == "ENABLED"
    posture_ready = bool(posture.get("fact_snapshot", {}).get("system_ready") is True)
    boundary_ok = str(boundary.get("fact_snapshot", {}).get("boundary_status") or "").strip().upper() == "AUTHORIZED"
    submission_authorized = boundary_ok and bool(boundary.get("fact_snapshot", {}).get("submission_authorized") is True)

    if not pre_open_complete:
        advisory_codes.append("PAPER_SESSION_LEDGER_ADVISORY_PRE_OPEN_NOT_COMPLETE")
    if not startup_ok:
        advisory_codes.append("PAPER_SESSION_LEDGER_ADVISORY_STARTUP_NOT_READY")
    if not (posture_enabled and posture_ready):
        advisory_codes.append("PAPER_SESSION_LEDGER_ADVISORY_POSTURE_NOT_READY")
    if not submission_authorized:
        advisory_codes.append("PAPER_SESSION_LEDGER_ADVISORY_SUBMIT_BOUNDARY_DENIED")
    if blocking_codes:
        return ("DENIED", False, False, sorted(set(blocking_codes)), sorted(set(advisory_codes)))
    return ("GRANTED", True, bool(submission_authorized), [], sorted(set(advisory_codes)))


def _submit_lifecycle_from_rollup(*, rollup_row: dict[str, Any] | None, authority_status: str) -> dict[str, Any]:
    if str(authority_status).strip().upper() != "GRANTED":
        return {
            "submit_attempt_status": "SKIPPED",
            "submit_attempted": False,
            "submit_result_status": "NOT_AUTHORIZED",
            "reason_codes": ["PAPER_SESSION_LEDGER_AUTHORITY_DENIED"],
            "submit_evidence_refs": [],
            "finalization_status": "OPEN",
        }
    if rollup_row is None or rollup_row.get("presence_verdict") != "PRESENT":
        return {
            "submit_attempt_status": "NOT_OBSERVED_AT_EVALUATION",
            "submit_attempted": False,
            "submit_result_status": "NOT_OBSERVED",
            "reason_codes": ["PAPER_SESSION_LEDGER_SUBMIT_NOT_OBSERVED_AT_EVALUATION"],
            "submit_evidence_refs": [],
            "finalization_status": "OPEN",
        }
    return {
        "submit_attempt_status": "ATTEMPTED",
        "submit_attempted": True,
        "submit_result_status": str(rollup_row.get("fact_snapshot", {}).get("status") or "UNKNOWN").strip().upper(),
        "reason_codes": sorted(
            set([str(rollup_row.get("fact_snapshot", {}).get("status") or "").strip().upper()] + list(rollup_row.get("blocking_codes") or []))
        ),
        "submit_evidence_refs": [str(rollup_row.get("absolute_path") or "")],
        "finalization_status": "OPEN",
    }


def _post_submit_lifecycle(
    *,
    truth_root: Path,
    day_utc: str,
    rollup_row: dict[str, Any] | None,
) -> dict[str, Any]:
    execution_root = resolve_paper_intent_truth_root_v1(
        truth_root=Path(truth_root).resolve(),
        repo_root=REPO_ROOT,
    )
    execution_evidence_root = (execution_root / "execution_evidence_v1").resolve()
    submissions_dir = (execution_evidence_root / "submissions" / day_utc).resolve()
    latest_path = (execution_evidence_root / "latest_pointer.v1.json").resolve()
    current_head_path = current_head_output_path(execution_root=execution_root, day_utc=day_utc)
    submission_index_path = submission_index_output_path(execution_root=execution_root, day_utc=day_utc)
    reconciliation_path = (Path(truth_root).resolve() / "reports" / "execution_reconciliation_v1" / day_utc / "execution_reconciliation.v1.json").resolve()
    submissions_present = submissions_dir.exists() and submissions_dir.is_dir() and any(submissions_dir.iterdir())
    latest_present = latest_path.exists() and latest_path.is_file()
    current_head_present = current_head_path.exists() and current_head_path.is_file()
    submission_index_present = submission_index_path.exists() and submission_index_path.is_file()
    reconciliation_present = reconciliation_path.exists() and reconciliation_path.is_file()
    latest_payload: dict[str, Any] = {}
    if latest_present:
        try:
            latest_payload = read_json_object_v1(latest_path)
        except Exception:
            latest_payload = {}
    current_head_payload: dict[str, Any] = {}
    if current_head_present:
        try:
            current_head_payload = read_json_object_v1(current_head_path)
        except Exception:
            current_head_payload = {}
    submission_index_payload: dict[str, Any] = {}
    if submission_index_present:
        try:
            submission_index_payload = read_json_object_v1(submission_index_path)
        except Exception:
            submission_index_payload = {}
    latest_day_utc = str(latest_payload.get("day_utc") or latest_payload.get("asof_day_utc") or "").strip() if latest_present else ""
    latest_day_matches = latest_day_utc == str(day_utc).strip() if latest_day_utc else False
    current_head_status = str(current_head_payload.get("status") or "").strip().upper() if current_head_present else ""
    submission_index_status = str(submission_index_payload.get("status") or "").strip().upper() if submission_index_present else ""
    selected_attempt_id = str(current_head_payload.get("selected_attempt_id") or "").strip() if current_head_present else ""

    submission_index_attempt_ids: set[str] = set()
    if isinstance(submission_index_payload.get("attempts"), list):
        for item in submission_index_payload.get("attempts") or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("lineage_status") or "").strip().upper() != "PASS":
                continue
            attempt_id = str(item.get("attempt_id") or "").strip()
            if attempt_id:
                submission_index_attempt_ids.add(attempt_id)
    attempt_lineage_match = bool(selected_attempt_id) and selected_attempt_id in submission_index_attempt_ids

    if rollup_row is None or rollup_row.get("presence_verdict") != "PRESENT":
        status = "NOT_OBSERVED_AT_EVALUATION"
        gap_codes = ["PAPER_SESSION_LEDGER_POST_SUBMIT_NOT_OBSERVED"]
    elif current_head_status == "PASS" and submission_index_status == "PASS" and attempt_lineage_match:
        status = "BOUND"
        gap_codes = []
    elif submissions_present and latest_present and latest_day_matches:
        status = "BOUND"
        gap_codes = []
    else:
        status = "GAP"
        gap_codes = []
        if not submissions_present:
            gap_codes.append("PAPER_SESSION_LEDGER_EXECUTION_SUBMISSIONS_MISSING")
        if not latest_present:
            gap_codes.append("PAPER_SESSION_LEDGER_EXECUTION_LATEST_POINTER_MISSING")
        elif not latest_payload:
            gap_codes.append("PAPER_SESSION_LEDGER_EXECUTION_LATEST_POINTER_MALFORMED")
        elif not latest_day_matches:
            gap_codes.append("PAPER_SESSION_LEDGER_EXECUTION_LATEST_POINTER_DAY_MISMATCH")
        if not current_head_present:
            gap_codes.append("PAPER_SESSION_LEDGER_EXECUTION_CURRENT_HEAD_MISSING")
        elif not current_head_payload:
            gap_codes.append("PAPER_SESSION_LEDGER_EXECUTION_CURRENT_HEAD_MALFORMED")
        elif current_head_status != "PASS":
            gap_codes.append("PAPER_SESSION_LEDGER_EXECUTION_CURRENT_HEAD_NOT_PASS")
        if not submission_index_present:
            gap_codes.append("PAPER_SESSION_LEDGER_SUBMISSION_INDEX_MISSING")
        elif not submission_index_payload:
            gap_codes.append("PAPER_SESSION_LEDGER_SUBMISSION_INDEX_MALFORMED")
        elif submission_index_status != "PASS":
            gap_codes.append("PAPER_SESSION_LEDGER_SUBMISSION_INDEX_NOT_PASS")
        elif not selected_attempt_id:
            gap_codes.append("PAPER_SESSION_LEDGER_CURRENT_HEAD_ATTEMPT_ID_MISSING")
        elif selected_attempt_id not in submission_index_attempt_ids:
            gap_codes.append("PAPER_SESSION_LEDGER_CURRENT_HEAD_SUBMISSION_INDEX_ATTEMPT_MISMATCH")
        if not reconciliation_present:
            gap_codes.append("PAPER_SESSION_LEDGER_EXECUTION_RECONCILIATION_MISSING")
    refs = []
    if rollup_row is not None and str(rollup_row.get("absolute_path") or "").strip():
        refs.append(str(rollup_row["absolute_path"]))
    if latest_present:
        refs.append(str(latest_path))
    if reconciliation_present:
        refs.append(str(reconciliation_path))
    if submissions_present:
        refs.append(str(submissions_dir))
    if current_head_present:
        refs.append(str(current_head_path))
    if submission_index_present:
        refs.append(str(submission_index_path))
    authoritative_lineage_path = current_head_path if current_head_present else latest_path
    authoritative_lineage_present = current_head_present or latest_present
    return {
        "lineage_status": status,
        "latest_authoritative_lineage_ref": str(authoritative_lineage_path) if authoritative_lineage_present else "",
        "latest_authoritative_lineage_sha256": (
            sha256_file_v1(authoritative_lineage_path) if authoritative_lineage_present else ""
        ),
        "execution_evidence_refs": sorted(set(refs)),
        "reconciliation_refs": [str(reconciliation_path)] if reconciliation_present else [],
        "gap_codes": sorted(set(gap_codes)),
    }


def _operator_summary_state(
    *,
    authority_status: str,
    submit_lifecycle: dict[str, Any],
    post_submit_lifecycle: dict[str, Any],
) -> str:
    if str(authority_status).strip().upper() != "GRANTED":
        return "AUTHORITY_DENIED"
    submit_attempt_status = str(submit_lifecycle.get("submit_attempt_status") or "").strip().upper()
    submit_result_status = str(submit_lifecycle.get("submit_result_status") or "").strip().upper()
    if submit_attempt_status == "ATTEMPTED":
        return f"SUBMIT_{submit_result_status or 'UNKNOWN'}"
    lineage_status = str(post_submit_lifecycle.get("lineage_status") or "").strip().upper()
    if submit_attempt_status == "NOT_OBSERVED_AT_EVALUATION" or lineage_status == "NOT_OBSERVED_AT_EVALUATION":
        return "AUTHORIZED_PRE_SUBMIT"
    if lineage_status == "GAP":
        return "POST_SUBMIT_GAP"
    return "AUTHORIZED_TO_PROCEED"


def _fact_type_for_logical_name(logical_name: str) -> str:
    mapping = {
        "paper_session_authority_v1": "execution_capability_fact",
        "pre_open_bundle_v1": "execution_capability_fact",
        "startup_materialization_v1": "execution_capability_fact",
        "paper_trading_posture_v1": "market_state_fact",
        "submit_boundary_status_v1": "dependency_health_fact",
        "sleeve_rollup_v1": "sleeve_state_fact",
    }
    return mapping.get(str(logical_name).strip(), "execution_capability_fact")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_paper_session_ledger_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    truth_root = resolve_decision_truth_root_v1(args.truth_root, repo_root=REPO_ROOT)
    session_id = canonical_paper_session_id_v1(day_utc)
    evaluated_at_utc = now_utc_iso_v1()

    rows: list[dict[str, Any]] = []
    refs_by_name: dict[str, SurfaceRefV1 | None] = {}
    for logical_name, resolver, reader, required_for_authority in (
        (
            "paper_session_authority_v1",
            resolve_paper_session_authority_path,
            read_paper_session_authority_ref_v1,
            True,
        ),
        (
            "pre_open_bundle_v1",
            resolve_pre_open_bundle_path_v1,
            lambda **kwargs: read_pre_open_bundle_ref_v1(
                truth_root=kwargs["truth_root"],
                target_day=kwargs["day_utc"],
            ),
            True,
        ),
        ("startup_materialization_v1", resolve_startup_materialization_path, read_startup_materialization_ref_v1, True),
        ("paper_trading_posture_v1", resolve_paper_trading_posture_path, read_paper_trading_posture_ref_v1, True),
        ("submit_boundary_status_v1", resolve_submit_boundary_status_path, read_submit_boundary_status_ref_v1, False),
        ("sleeve_rollup_v1", resolve_sleeve_rollup_path, read_sleeve_rollup_ref_v1, False),
    ):
        row, ref = _safe_read_surface(
            logical_name=logical_name,
            day_utc=day_utc,
            session_id=session_id,
            resolver=resolver,
            reader=reader,
            truth_root=truth_root,
            required_for_authority=required_for_authority,
        )
        rows.append(row)
        refs_by_name[logical_name] = ref

    evidence_freeze = _evidence_freeze_from_rows(rows=rows)
    rows_by_name = {str(row["logical_name"]): row for row in rows}
    authority_status, system_ready, submission_authorized, control_codes, advisory_codes = _control_from_rows(
        evidence_freeze=evidence_freeze,
        rows_by_name=rows_by_name,
    )
    submit_lifecycle = _submit_lifecycle_from_rollup(
        rollup_row=rows_by_name.get("sleeve_rollup_v1"),
        authority_status=authority_status,
    )
    post_submit_lifecycle = _post_submit_lifecycle(
        truth_root=truth_root,
        day_utc=day_utc,
        rollup_row=rows_by_name.get("sleeve_rollup_v1"),
    )
    ledger_path = resolve_paper_session_ledger_path(truth_root=truth_root, day_utc=day_utc)
    operator_summary = {
        "authority_scope": "DERIVED_ONLY_VIEW",
        "ledger_ref": str(ledger_path),
        "summary_state": _operator_summary_state(
            authority_status=authority_status,
            submit_lifecycle=submit_lifecycle,
            post_submit_lifecycle=post_submit_lifecycle,
        ),
        "authority_status": authority_status,
        "submission_authorized": bool(submission_authorized),
        "non_authority_notice": (
            "Derived from paper_session_ledger_v1 only. Do not treat this summary as an independent authority surface."
            if not advisory_codes
            else (
                "Derived from paper_session_ledger_v1 only. Do not treat this summary as an independent authority surface. "
                f"Non-blocking paper-open advisories={','.join(advisory_codes)}"
            )
        ),
        "blocking_codes": sorted(set(control_codes)),
    }
    source_artifact_hashes = [
        {
            "artifact_ref": str(row.get("absolute_path") or ""),
            "sha256": str(row.get("content_hash") or "").strip().lower(),
        }
        for row in rows
        if str(row.get("absolute_path") or "").strip() and str(row.get("content_hash") or "").strip()
    ]
    if GATE_HIERARCHY_POLICY_PATH.exists() and GATE_HIERARCHY_POLICY_PATH.is_file():
        source_artifact_hashes.append(
            {
                "artifact_ref": str(GATE_HIERARCHY_POLICY_PATH),
                "sha256": sha256_file_v1(GATE_HIERARCHY_POLICY_PATH),
            }
        )
    proposal = build_session_submission_proposal_v1(
        day_utc=day_utc,
        source_subsystem="paper_session_ledger_v1",
        source_reasoning_reference="paper_session_ledger_v1",
        source_policy_bindings=[str(GATE_HIERARCHY_POLICY_PATH)] if GATE_HIERARCHY_POLICY_PATH.exists() else [],
        source_artifact_hashes=source_artifact_hashes,
        target_entities=["PRIMARY", session_id],
    )
    proposal_hash = proposal_hash_v1(proposal)
    fact_bundle = build_constitutional_fact_bundle_v1(
        day_utc=day_utc,
        session_id=session_id,
        policy_version=CONSTITUTIONAL_SHADOW_POLICY_VERSION,
        required_fact_types=list(proposal.get("required_fact_types") or []),
        fact_records=[
            build_constitutional_surface_fact_record_v1(
                row=row,
                fact_type=_fact_type_for_logical_name(str(row.get("logical_name") or "")),
                source_system=str(row.get("logical_name") or ""),
            )
            for row in rows
        ],
    )
    constitutional_decision = evaluate_constitutional_decision_v1(
        proposal=proposal,
        proposal_hash=proposal_hash,
        fact_bundle=fact_bundle,
        fact_bundle_hash=str(fact_bundle.get("fact_bundle_hash") or "").strip(),
        policy_version=CONSTITUTIONAL_SHADOW_POLICY_VERSION,
        scope_authorities={
            "global": "REQUIRE_HUMAN_REVIEW",
            "domain": "REQUIRE_HUMAN_REVIEW",
            "account": "REQUIRE_HUMAN_REVIEW",
            "sleeve": "REQUIRE_HUMAN_REVIEW",
            "action_class": "REQUIRE_HUMAN_REVIEW",
        },
        hard_envelope_ok=str(evidence_freeze.get("overall_evidence_status") or "").strip().upper() == "READY",
        policy_blockers=list(control_codes),
        persistence_ok=True,
        evaluated_at=evaluated_at_utc,
    )
    constitutional_context = {
        "proposal_hash": proposal_hash,
        "fact_bundle_hash": str(fact_bundle.get("fact_bundle_hash") or "").strip(),
        "decision_enum": str(constitutional_decision.get("decision_enum") or "").strip(),
        "effective_scope": dict(constitutional_decision.get("effective_scope") or {}),
        "blocker_rules": list(constitutional_decision.get("blocker_rules") or []),
        "rule_provenance": list(constitutional_decision.get("rule_provenance") or []),
        "negative_evidence": list(constitutional_decision.get("negative_evidence") or []),
        "policy_version": CONSTITUTIONAL_SHADOW_POLICY_VERSION,
    }

    ledger = build_paper_session_ledger_v1(
        day_utc=day_utc,
        session_id=session_id,
        evaluated_at_utc=evaluated_at_utc,
        provenance=producer_block_v1(module="ops/tools/run_paper_session_ledger_v1.py"),
        fact_refs=sorted(rows, key=lambda row: str(row["logical_name"])),
        evidence_freeze=evidence_freeze,
        authority_status=authority_status,
        system_ready=system_ready,
        submission_authorized=submission_authorized,
        control_blocking_codes=control_codes,
        submit_lifecycle=submit_lifecycle,
        post_submit_lifecycle=post_submit_lifecycle,
        operator_summary=operator_summary,
        constitutional_context=constitutional_context,
    )
    path = write_paper_session_ledger_v1(truth_root=truth_root, ledger=ledger)
    print(
        json.dumps(
            {
                "path": str(path),
                "ledger_id": ledger.ledger_id,
                "authority_status": ledger.control_state["authority_status"],
                "system_ready": ledger.control_state["system_ready"],
                "submission_authorized": ledger.control_state["submission_authorized"],
                "evidence_status": ledger.evidence_freeze["overall_evidence_status"],
            },
            sort_keys=True,
        )
    )
    return 0 if ledger.control_state["authority_status"] == "GRANTED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
