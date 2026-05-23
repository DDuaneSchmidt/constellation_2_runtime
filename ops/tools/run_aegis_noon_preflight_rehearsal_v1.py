#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import json
import smtplib
import sys
from datetime import UTC, datetime, timedelta
from email.message import EmailMessage
from io import StringIO
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_chatgpt_control_packet_v1 import (  # noqa: E402
    build_aegis_chatgpt_control_packet_v1,
    write_aegis_chatgpt_control_packet_v1,
)
from constellation_2.common.aegis_eod_artifact_contract_v1 import (  # noqa: E402
    build_market_snapshot_authority_v1,
    build_promoted_sleeve_manifest_v1,
    validate_eod_input_contract_v1,
)
from constellation_2.common.aegis_event_monitoring_v1 import DEFAULT_EVENT_RULES_REGISTRY_PATH  # noqa: E402
from constellation_2.common.aegis_lite_eod_v1 import artifact_ref_v1  # noqa: E402
from constellation_2.common.aegis_trade_sizing_engine_v1 import build_trade_sizing_guidance_v1  # noqa: E402
from constellation_2.common.paper_session_fact_plane_v1 import (  # noqa: E402
    parse_day_utc_v1,
    resolve_fact_plane_truth_root_v1,
    resolve_market_calendar_record_v1,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1  # noqa: E402
from ops.aegis.data_remediation_v1 import run_data_remediation_v1  # noqa: E402
from ops.aegis.run_context_v1 import child_run_context_v1, run_context_from_env_v1, step_allowed_v1  # noqa: E402


ARTIFACT_FAMILY = "aegis_noon_preflight_rehearsal_v1"
NOON_PREFLIGHT_FAMILY = "aegis_noon_preflight_v1"
ALERT_SUBJECT = "[Aegis Preflight FAIL] action needed before 09:50 UTC and 14:50 UTC sleeve run"
NOON_PREFLIGHT_ALERT_SUBJECT = "Aegis noon preflight failed — candidate generation blocked"
ALERT_DELIVERY_DRY_RUN = "DRY_RUN_MESSAGE_BODY_ONLY"
EMAIL_TRANSPORT_STATUS = "GATE_ONLY_NO_TRANSPORT"
STALE_AFTER = timedelta(hours=6)


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_run_id_v1(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip())
    return cleaned or "aegis_noon_preflight_rehearsal_v1"


def preflight_dir_v1(*, truth_root: Path, day_utc: str, run_id: str) -> Path:
    return Path(truth_root).resolve() / "reports" / ARTIFACT_FAMILY / day_utc / safe_run_id_v1(run_id)


def preflight_path_v1(*, truth_root: Path, day_utc: str, run_id: str, filename: str) -> Path:
    return preflight_dir_v1(truth_root=truth_root, day_utc=day_utc, run_id=run_id) / filename


def write_preflight_artifact_v1(*, truth_root: Path, day_utc: str, run_id: str, filename: str, payload: dict[str, Any]) -> Path:
    payload["canonical_json_hash"] = None
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    path = preflight_path_v1(truth_root=truth_root, day_utc=day_utc, run_id=run_id, filename=filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def run_noon_preflight_rehearsal_v1(
    *,
    day_utc: str,
    truth_root: Path,
    run_id: str,
    generated_at_utc: str,
    candidate_input_path: str = "",
    promoted_sleeve_library_path: str = "",
    event_rules_registry_path: str = "",
    refresh_control_packet: bool = True,
) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    validations: list[dict[str, Any]] = []
    blockers: list[str] = []
    warnings: list[str] = []
    candidate_rows: list[dict[str, Any]] = []
    source_refs: list[dict[str, Any]] = []

    session_state, calendar_ref = _calendar_session_state(root=root, day_utc=day_utc)
    if calendar_ref:
        source_refs.append(calendar_ref)

    if session_state == "NON_TRADING_DAY":
        manifest_path, diagnostic_path, status_path = _write_non_trading_day(
            truth_root=root,
            day_utc=day_utc,
            run_id=run_id,
            generated_at_utc=generated_at_utc,
            source_refs=source_refs,
        )
        return _read_json(status_path) | {
            "manifest_path": str(manifest_path),
            "diagnostic_dry_run_path": str(diagnostic_path),
            "status_path": str(status_path),
        }

    if session_state == "UNKNOWN_CALENDAR_STATE":
        _add_validation(validations, blockers, "market_calendar", "FAIL", "NYSE_MARKET_CALENDAR_MISSING", str(calendar_ref.get("path") if calendar_ref else ""))

    _validate_event_rules_registry(
        validations=validations,
        blockers=blockers,
        source_refs=source_refs,
        event_rules_registry_path=event_rules_registry_path,
    )
    promoted = _validate_promoted_sleeve_library(
        validations=validations,
        blockers=blockers,
        source_refs=source_refs,
        truth_root=root,
        explicit_path=promoted_sleeve_library_path,
    )
    market_context = _validate_market_context(
        validations=validations,
        blockers=blockers,
        source_refs=source_refs,
        truth_root=root,
        day_utc=day_utc,
        generated_at_utc=generated_at_utc,
    )
    candidate_payload, candidate_rows = _load_candidate_input(
        validations=validations,
        blockers=blockers,
        source_refs=source_refs,
        candidate_input_path=candidate_input_path,
    )
    candidate_readiness = _validate_candidate_generation_readiness(
        validations=validations,
        blockers=blockers,
        source_refs=source_refs,
        truth_root=root,
        day_utc=day_utc,
    )
    sizing_results = _validate_sizing_engine(
        validations=validations,
        blockers=blockers,
        candidates=candidate_rows,
        promoted_library=promoted,
        market_context=market_context,
    )
    eod_contract = _validate_eod_contract(
        validations=validations,
        blockers=blockers,
        truth_root=root,
        day_utc=day_utc,
        run_id=run_id,
        generated_at_utc=generated_at_utc,
        candidate_payload=candidate_payload,
        promoted_library=promoted,
        promoted_sleeve_library_path=_source_path(promoted),
    )
    control_refresh = _refresh_control_packet_if_safe(
        truth_root=root,
        day_utc=day_utc,
        generated_at_utc=generated_at_utc,
        enabled=refresh_control_packet,
    )
    candidate_partial_ready = bool(candidate_readiness.get("candidate_generation_ready")) and str(candidate_readiness.get("operator_interpretation") or "").upper() == "PARTIAL_RUN"
    if control_refresh["status"] == "REFRESHED" and control_refresh.get("runtime_truth_classification") == "PARTIAL_CONTEXT" and candidate_partial_ready:
        _add_validation(
            validations,
            blockers,
            "control_packet_refresh",
            "WARN",
            "PARTIAL_CONTEXT_AFTER_REFRESH",
            str(control_refresh.get("path") or ""),
        )
        warnings.append("PARTIAL_CONTEXT_AFTER_REFRESH")
    elif control_refresh["status"] == "REFRESHED" and control_refresh.get("runtime_truth_classification") == "PARTIAL_CONTEXT":
        _add_validation(
            validations,
            blockers,
            "control_packet_refresh",
            "FAIL",
            "PARTIAL_CONTEXT_AFTER_REFRESH",
            str(control_refresh.get("path") or ""),
        )
    elif control_refresh["status"] == "FAILED":
        _add_validation(validations, blockers, "control_packet_refresh", "WARN", "CONTROL_PACKET_REFRESH_FAILED", "")
        warnings.append("CONTROL_PACKET_REFRESH_FAILED")

    alert_transport = _alert_transport_configuration()
    if not bool(alert_transport.get("configured")):
        _add_validation(validations, blockers, "alert_transport", "FAIL", "ALERT_TRANSPORT_UNAVAILABLE", "")
    else:
        _add_validation(validations, blockers, "alert_transport", "PASS", "", "")

    result = _result_from_validations(validations=validations, blockers=blockers, warnings=warnings)
    readiness_status = _preflight_readiness_status(result=result, candidate_readiness=candidate_readiness)
    canonical_eod_at_risk = result in {"FAIL", "WARN"} and any(_canonical_eod_risk_code(code) for code in blockers + warnings)
    next_action = _next_action(result=result, blockers=blockers, canonical_eod_at_risk=canonical_eod_at_risk)

    diagnostic = {
        "schema_id": "preflight_diagnostic_dry_run",
        "schema_version": "v1",
        "artifact_id": "preflight_diagnostic_dry_run_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "diagnostic_only": True,
        "trade_advice_enabled": False,
        "manual_trade_capture_allowed": False,
        "broker_submit_required": False,
        "ib_automation_required": False,
        "canonical_eod_state_mutated": False,
        "real_runtime_packet_created": False,
        "candidate_count": len(candidate_rows),
        "sizing_engine_validation": sizing_results,
        "eod_pipeline_contract_validation": eod_contract,
        "control_packet_refresh": control_refresh,
        "validations": validations,
        "canonical_json_hash": None,
    }
    diagnostic_path = write_preflight_artifact_v1(
        truth_root=root,
        day_utc=day_utc,
        run_id=run_id,
        filename="diagnostic_dry_run.v1.json",
        payload=diagnostic,
    )

    alert_required = _alert_required(result=result, blockers=blockers, warnings=warnings)
    ledger_path = ""
    alert_subject = ""
    alert_body = ""
    noon_email_body = ""
    delivery_status = "NOT_SENT"
    operator_alert_status = "no alert needed"
    email_attempt = {
        "email_attempted": False,
        "email_status": "DISABLED",
        "email_error": None,
        "email_alert_sent": False,
        "email_transport_proven": False,
    }
    if alert_required:
        expected_manifest_path = preflight_path_v1(truth_root=root, day_utc=day_utc, run_id=run_id, filename="preflight_run_manifest.v1.json")
        expected_status_path = preflight_path_v1(truth_root=root, day_utc=day_utc, run_id=run_id, filename="preflight_status.v1.json")
        expected_ledger_path = preflight_path_v1(truth_root=root, day_utc=day_utc, run_id=run_id, filename="preflight_alert_ledger.v1.json")
        alert_subject = ALERT_SUBJECT
        alert_body = _alert_body(
            result=result,
            validations=validations,
            blockers=blockers,
            paths={
                "preflight_run_manifest": str(expected_manifest_path),
                "preflight_status": str(expected_status_path),
                "diagnostic_dry_run": str(diagnostic_path),
                "preflight_alert_ledger": str(expected_ledger_path),
                "control_packet": str(control_refresh.get("path") or ""),
            },
            next_action=next_action,
            canonical_eod_at_risk=canonical_eod_at_risk,
        )
        delivery_status = ALERT_DELIVERY_DRY_RUN
        operator_alert_status = "alert not live"
        noon_email_body = _noon_email_body(
            result=result,
            readiness_status=readiness_status,
            blockers=blockers,
            next_action=next_action,
            candidate_readiness=candidate_readiness,
        )
        email_attempt = _deliver_noon_preflight_email(
            alert_required=True,
            alert_transport=alert_transport,
            subject=NOON_PREFLIGHT_ALERT_SUBJECT,
            body=noon_email_body,
        )
        delivery_status = _delivery_status_from_email_attempt(email_attempt)
        operator_alert_status = _operator_alert_status_from_email_attempt(email_attempt)

    manifest = {
        "schema_id": "preflight_run_manifest",
        "schema_version": "v1",
        "artifact_id": "preflight_run_manifest_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "result": result,
        "readiness_status": readiness_status,
        "source_artifacts_used": source_refs,
        "diagnostic_dry_run_artifact_path": str(diagnostic_path),
        "control_packet_refresh": control_refresh,
        "alert_required": alert_required,
        "candidate_generation_ready": bool(candidate_readiness.get("candidate_generation_ready")),
        "candidate_readiness": candidate_readiness,
        "canonical_eod_at_risk": canonical_eod_at_risk,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_required": False,
        "canonical_eod_state_mutated": False,
        "production_mutation_allowed": False,
        "real_runtime_packet_created": False,
        "canonical_json_hash": None,
    }
    manifest_path = write_preflight_artifact_v1(
        truth_root=root,
        day_utc=day_utc,
        run_id=run_id,
        filename="preflight_run_manifest.v1.json",
        payload=manifest,
    )

    if alert_required:
        ledger = _alert_ledger(
            day_utc=day_utc,
            run_id=run_id,
            generated_at_utc=generated_at_utc,
            result=result,
            delivery_status=delivery_status,
            subject=alert_subject,
            body=alert_body,
            manifest_path=str(manifest_path),
            diagnostic_path=str(diagnostic_path),
            control_packet_path=str(control_refresh.get("path") or ""),
        )
        ledger_path = str(
            write_preflight_artifact_v1(
                truth_root=root,
                day_utc=day_utc,
                run_id=run_id,
                filename="preflight_alert_ledger.v1.json",
                payload=ledger,
            )
        )

    status = {
        "schema_id": "preflight_status",
        "schema_version": "v1",
        "artifact_id": "preflight_status_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "result": result,
        "readiness_status": readiness_status,
        "failed_validations": [row for row in validations if row["status"] == "FAIL"],
        "warnings": [row for row in validations if row["status"] == "WARN"],
        "blockers": sorted(set(blockers)),
        "next_action": next_action,
        "canonical_eod_at_risk": canonical_eod_at_risk,
        "email_alert_required": alert_required,
        "email_alert_sent": bool(email_attempt.get("email_alert_sent")),
        "email_transport_proven": bool(email_attempt.get("email_transport_proven")),
        "email_transport_status": str(email_attempt.get("email_status") or EMAIL_TRANSPORT_STATUS),
        "email_error": email_attempt.get("email_error"),
        "delivery_status": delivery_status,
        "operator_alert_status": operator_alert_status,
        "alert_subject": alert_subject,
        "alert_body": alert_body,
        "noon_email_body": noon_email_body,
        "alert_ledger_path": ledger_path,
        "manifest_path": str(manifest_path),
        "diagnostic_dry_run_path": str(diagnostic_path),
        "candidate_readiness": candidate_readiness,
        "control_packet_refresh": control_refresh,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_required": False,
        "canonical_eod_state_mutated": False,
        "production_mutation_allowed": False,
        "trade_advice_enabled": False,
        "canonical_json_hash": None,
    }
    status_path = write_preflight_artifact_v1(
        truth_root=root,
        day_utc=day_utc,
        run_id=run_id,
        filename="preflight_status.v1.json",
        payload=status,
    )
    noon_payload = _build_noon_preflight_report(
        day_utc=day_utc,
        generated_at_utc=generated_at_utc,
        status=status,
        validations=validations,
        blockers=blockers,
        next_action=next_action,
        candidate_readiness=candidate_readiness,
        email_attempt=email_attempt,
    )
    noon_paths = write_noon_preflight_report_v1(truth_root=root, day_utc=day_utc, payload=noon_payload)
    return {**status, "status_path": str(status_path), "noon_preflight_path": noon_paths["json"], "noon_preflight_paths": noon_paths}


def _write_non_trading_day(
    *,
    truth_root: Path,
    day_utc: str,
    run_id: str,
    generated_at_utc: str,
    source_refs: list[dict[str, Any]],
) -> tuple[Path, Path, Path]:
    diagnostic = {
        "schema_id": "preflight_diagnostic_dry_run",
        "schema_version": "v1",
        "artifact_id": "preflight_diagnostic_dry_run_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "diagnostic_only": True,
        "session_state": "NON_TRADING_DAY",
        "broker_submit_required": False,
        "ib_automation_required": False,
        "canonical_eod_state_mutated": False,
        "canonical_json_hash": None,
    }
    diagnostic_path = write_preflight_artifact_v1(truth_root=truth_root, day_utc=day_utc, run_id=run_id, filename="diagnostic_dry_run.v1.json", payload=diagnostic)
    manifest = {
        "schema_id": "preflight_run_manifest",
        "schema_version": "v1",
        "artifact_id": "preflight_run_manifest_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "result": "SKIPPED_NON_TRADING_DAY",
        "source_artifacts_used": source_refs,
        "diagnostic_dry_run_artifact_path": str(diagnostic_path),
        "alert_required": False,
        "canonical_eod_at_risk": False,
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_required": False,
        "canonical_eod_state_mutated": False,
        "production_mutation_allowed": False,
        "real_runtime_packet_created": False,
        "canonical_json_hash": None,
    }
    manifest_path = write_preflight_artifact_v1(truth_root=truth_root, day_utc=day_utc, run_id=run_id, filename="preflight_run_manifest.v1.json", payload=manifest)
    status = {
        "schema_id": "preflight_status",
        "schema_version": "v1",
        "artifact_id": "preflight_status_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "result": "SKIPPED_NON_TRADING_DAY",
        "failed_validations": [],
        "warnings": [],
        "blockers": [],
        "next_action": "No operator action. NYSE is not in session.",
        "canonical_eod_at_risk": False,
        "email_alert_required": False,
        "email_alert_sent": False,
        "email_transport_proven": False,
        "email_transport_status": EMAIL_TRANSPORT_STATUS,
        "delivery_status": "NOT_SENT",
        "operator_alert_status": "no alert needed",
        "alert_ledger_path": "",
        "manifest_path": str(manifest_path),
        "diagnostic_dry_run_path": str(diagnostic_path),
        "control_packet_refresh": {"status": "SKIPPED_NON_TRADING_DAY", "path": "", "runtime_truth_classification": ""},
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_required": False,
        "canonical_eod_state_mutated": False,
        "production_mutation_allowed": False,
        "trade_advice_enabled": False,
        "canonical_json_hash": None,
    }
    status_path = write_preflight_artifact_v1(truth_root=truth_root, day_utc=day_utc, run_id=run_id, filename="preflight_status.v1.json", payload=status)
    return manifest_path, diagnostic_path, status_path


def _calendar_session_state(*, root: Path, day_utc: str) -> tuple[str, dict[str, Any]]:
    calendar_path = root / "market_calendar_v1" / "NYSE" / f"{day_utc[:4]}.jsonl"
    ref = artifact_ref_v1(calendar_path, artifact_type="market_calendar_v1:NYSE")
    try:
        resolved = resolve_market_calendar_record_v1(truth_root=root, day_utc=day_utc)
    except Exception:
        return "UNKNOWN_CALENDAR_STATE", ref
    record = resolved.get("record") if isinstance(resolved, dict) else None
    if isinstance(record, dict) and record.get("is_trading_session") is True:
        return "TRADING_SESSION", ref
    if isinstance(record, dict) and record.get("is_trading_session") is False:
        return "NON_TRADING_DAY", ref
    return "UNKNOWN_CALENDAR_STATE", ref


def _validate_event_rules_registry(
    *,
    validations: list[dict[str, Any]],
    blockers: list[str],
    source_refs: list[dict[str, Any]],
    event_rules_registry_path: str,
) -> dict[str, Any]:
    path = Path(event_rules_registry_path).expanduser().resolve() if event_rules_registry_path else DEFAULT_EVENT_RULES_REGISTRY_PATH
    if not path.exists() or not path.is_file():
        _add_validation(validations, blockers, "event_rules_registry", "FAIL", "EVENT_RULES_REGISTRY_MISSING", str(path))
        return {}
    payload = _read_json(path)
    source_refs.append(artifact_ref_v1(path, artifact_type="event_rules_registry_v1"))
    rules = payload.get("event_rules") if isinstance(payload.get("event_rules"), list) else []
    if not rules:
        _add_validation(validations, blockers, "event_rules_registry", "FAIL", "EVENT_RULES_REGISTRY_EMPTY", str(path))
    else:
        _add_validation(validations, blockers, "event_rules_registry", "PASS", "", str(path))
    return payload


def _validate_promoted_sleeve_library(
    *,
    validations: list[dict[str, Any]],
    blockers: list[str],
    source_refs: list[dict[str, Any]],
    truth_root: Path,
    explicit_path: str,
) -> dict[str, Any]:
    path = Path(explicit_path).expanduser().resolve() if explicit_path else _latest_file(truth_root / "reports" / "promoted_sleeve_library_v1", "promoted_sleeve_library.v1.json")
    if path is None or not path.exists():
        _add_validation(validations, blockers, "promoted_sleeve_library", "FAIL", "PROMOTED_SLEEVE_LIBRARY_MISSING", str(path or ""))
        return {}
    payload = _read_json(path)
    payload["_preflight_source_path"] = str(path)
    source_refs.append(artifact_ref_v1(path, artifact_type="promoted_sleeve_library_v1"))
    sleeves = payload.get("promoted_sleeves") if isinstance(payload.get("promoted_sleeves"), list) else payload.get("sleeves")
    promoted = [row for row in sleeves if isinstance(row, dict) and str(row.get("promotion_status") or "").lower() == "promoted"] if isinstance(sleeves, list) else []
    if not promoted:
        _add_validation(validations, blockers, "promoted_sleeve_library", "FAIL", "PROMOTED_SLEEVE_LIBRARY_NO_PROMOTED_SLEEVES", str(path))
    else:
        _add_validation(validations, blockers, "promoted_sleeve_library", "PASS", "", str(path))
    return payload


def _validate_market_context(
    *,
    validations: list[dict[str, Any]],
    blockers: list[str],
    source_refs: list[dict[str, Any]],
    truth_root: Path,
    day_utc: str,
    generated_at_utc: str,
) -> dict[str, Any]:
    path = _latest_file(truth_root / "reports" / "event_market_snapshot_v1" / day_utc, "event_market_snapshot.v1.json")
    if path is None:
        _add_validation(validations, blockers, "market_context_snapshot", "FAIL", "MARKET_CONTEXT_SNAPSHOT_MISSING", "")
        return {}
    payload = _read_json(path)
    payload["_preflight_source_path"] = str(path)
    source_refs.append(artifact_ref_v1(path, artifact_type="event_market_snapshot_v1"))
    stale_status = str(payload.get("stale_data_status") or "").upper()
    generated_dt = _parse_utc(generated_at_utc)
    snapshot_dt = _parse_utc(str(payload.get("generated_at_utc") or payload.get("timestamp_utc") or ""))
    too_old = bool(snapshot_dt and generated_dt and generated_dt - snapshot_dt > STALE_AFTER)
    if stale_status in {"STALE", "MISSING_INPUT"} or too_old:
        readiness = _latest_file(truth_root / "reports" / "aegis_sleeve_readiness_v1" / day_utc, "sleeve_readiness.v1.json")
        readiness_payload = _read_json(readiness) if readiness else {}
        ready_count = int(readiness_payload.get("ready_count") or 0) + int(readiness_payload.get("ready_with_warnings_count") or 0)
        status = "WARN" if ready_count > 0 else "FAIL"
        _add_validation(validations, blockers, "market_context_snapshot", status, "MARKET_CONTEXT_SNAPSHOT_STALE", str(path))
    else:
        _add_validation(validations, blockers, "market_context_snapshot", "PASS", "", str(path))
    return payload


def _load_candidate_input(
    *,
    validations: list[dict[str, Any]],
    blockers: list[str],
    source_refs: list[dict[str, Any]],
    candidate_input_path: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not candidate_input_path:
        _add_validation(validations, blockers, "candidate_input", "PASS", "", "")
        return {"candidates": [], "raw_candidate_absent_reason": "NO_TRADE_CANDIDATES"}, []
    path = Path(candidate_input_path).expanduser().resolve()
    if not path.exists():
        _add_validation(validations, blockers, "candidate_input", "FAIL", "CANDIDATE_INPUT_MISSING", str(path))
        return {}, []
    payload = _read_json(path)
    source_refs.append(artifact_ref_v1(path, artifact_type="aegis_lite_candidate_input_v1"))
    rows = [row for row in payload.get("candidates", []) if isinstance(row, dict)] if isinstance(payload.get("candidates"), list) else []
    _add_validation(validations, blockers, "candidate_input", "PASS", "", str(path))
    if not rows:
        payload["raw_candidate_absent_reason"] = str(payload.get("raw_candidate_absent_reason") or "NO_TRADE_CANDIDATES")
    return payload, rows


def _validate_candidate_generation_readiness(
    *,
    validations: list[dict[str, Any]],
    blockers: list[str],
    source_refs: list[dict[str, Any]],
    truth_root: Path,
    day_utc: str,
) -> dict[str, Any]:
    path = _candidate_diagnostics_path(truth_root=truth_root, day_utc=day_utc)
    if path is None:
        _add_validation(validations, blockers, "candidate_generation_readiness", "FAIL", "CANDIDATE_DIAGNOSTICS_MISSING", "")
        return {
            "diagnostics_found": False,
            "diagnostics_path": "",
            "candidate_generation_ready": False,
            "data_ready": False,
            "candidate_generation_status": "UNKNOWN",
            "operator_interpretation": "PARTIAL_CONTEXT",
            "sleeves_expected": 0,
            "sleeves_run": 0,
            "total_raw_signals": 0,
            "total_candidates_generated": 0,
            "blocking_codes": ["CANDIDATE_DIAGNOSTICS_MISSING", "CANDIDATE_GENERATION_NOT_READY"],
            "recommended_action": ["npm run aegis:candidate-diagnostics"],
        }
    payload = _read_json(path)
    source_refs.append(artifact_ref_v1(path, artifact_type="aegis_candidate_generation_diagnostics_v1"))
    status = str(payload.get("candidate_generation_status") or "UNKNOWN").strip().upper()
    interpretation = str(payload.get("operator_interpretation") or "UNKNOWN").strip().upper()
    sleeves_expected = int(payload.get("total_sleeves_expected") or 0)
    sleeves_enabled = int(payload.get("total_sleeves_enabled") or 0)
    sleeves_expected_today = int(payload.get("total_sleeves_expected_today") or sleeves_expected)
    sleeves_run = int(payload.get("total_sleeves_run") or 0)
    total_raw_signals = int(payload.get("total_raw_signals") or 0)
    total_candidates = int(payload.get("total_candidates_generated") or 0)
    total_rejected = int(payload.get("total_candidates_rejected") or 0)
    raw_signal_rejections = payload.get("raw_signal_rejections") if isinstance(payload.get("raw_signal_rejections"), list) else []
    selected_intent_promotion = payload.get("selected_intent_promotion") if isinstance(payload.get("selected_intent_promotion"), dict) else {}
    sleeves = payload.get("sleeves") if isinstance(payload.get("sleeves"), list) else []
    blocked_sleeves = [
        {
            "sleeve_id": row.get("sleeve_id") or "UNKNOWN",
            "blocker": row.get("canonical_blocker") or ", ".join(str(item) for item in row.get("blocking_inputs") or []) or row.get("reason_no_candidate") or "",
            "reason": row.get("reason_no_candidate") or "",
        }
        for row in sleeves
        if isinstance(row, dict) and str(row.get("run_status") or "").upper() == "BLOCKED"
    ]
    ready_total = int(payload.get("total_sleeves_ready") or 0) + int(payload.get("total_sleeves_ready_with_warnings") or 0)
    blocked_total = int(payload.get("total_sleeves_blocked") or 0)
    trigger = payload.get("trigger_evaluation") if isinstance(payload.get("trigger_evaluation"), dict) else {}
    market_data_summary = payload.get("market_data_summary") if isinstance(payload.get("market_data_summary"), dict) else {}
    missing_symbol_explanations = market_data_summary.get("missing_symbol_explanations") if isinstance(market_data_summary.get("missing_symbol_explanations"), dict) else {}
    market_provider_config = market_data_summary.get("provider_config") if isinstance(market_data_summary.get("provider_config"), dict) else {}
    input_artifacts = payload.get("input_artifacts") if isinstance(payload.get("input_artifacts"), dict) else {}
    data_blocked = interpretation == "DATA_BLOCKED"
    blocking_codes: list[str] = []
    warning_codes: list[str] = []
    partial_run_acceptable = interpretation == "PARTIAL_RUN" and sleeves_run > 0 and ready_total > 0
    if not input_artifacts.get("data_registry"):
        blocking_codes.append("DATA_REGISTRY_MISSING")
    if not input_artifacts.get("sleeve_input_contracts"):
        blocking_codes.append("SLEEVE_INPUT_CONTRACTS_MISSING")
    if not input_artifacts.get("sleeve_readiness"):
        blocking_codes.append("SLEEVE_READINESS_MISSING")
    if status in {"UNKNOWN", "NOT_RUN"} or (status == "PARTIAL" and not partial_run_acceptable):
        blocking_codes.append("CANDIDATE_GENERATION_NOT_READY")
        blocking_codes.append(f"CANDIDATE_GENERATION_STATUS_{status}")
    elif status == "PARTIAL":
        warning_codes.append("CANDIDATE_GENERATION_STATUS_PARTIAL")
    if sleeves_enabled > 0 and sleeves_expected_today < sleeves_enabled:
        blocking_codes.append("CANDIDATE_DIAGNOSTICS_INCOMPLETE")
        blocking_codes.append("ENABLED_SLEEVES_NOT_ENUMERATED")
    if sleeves_expected_today > 0 and len(sleeves) < sleeves_expected_today:
        blocking_codes.append("CANDIDATE_DIAGNOSTICS_INCOMPLETE")
        blocking_codes.append("EXPECTED_SLEEVE_ROWS_MISSING")
    if any(
        isinstance(row, dict)
        and row.get("expected_today") is True
        and str(row.get("run_status") or "").strip().upper() == "UNKNOWN"
        for row in sleeves
    ):
        blocking_codes.append("ENABLED_SLEEVE_RUN_STATUS_UNKNOWN")
    if sleeves_expected > 0 and sleeves_run == 0:
        blocking_codes.append("SLEEVE_RUNS_MISSING")
    if interpretation == "DATA_BLOCKED":
        blocking_codes.append("DATA_BLOCKED")
        blocking_codes.append("CANDIDATE_REQUIRED_DATA_MISSING_OR_STALE")
    elif interpretation == "CONTRACT_BLOCKED":
        blocking_codes.append("CONTRACT_BLOCKED")
    elif interpretation in {"ENGINE_NOT_RUN", "PARTIAL_CONTEXT"}:
        blocking_codes.append(interpretation)
    if ready_total == 0 and blocked_total > 0:
        blocking_codes.append("ALL_SLEEVES_BLOCKED")
    if market_data_summary:
        failure_reason = str(market_data_summary.get("failure_reason") or "").strip().upper()
        if failure_reason:
            blocking_codes.append(failure_reason)
        if not bool(market_provider_config.get("configured")):
            blocking_codes.append("MARKET_DATA_PROVIDER_NOT_CONFIGURED")
        if market_data_summary.get("missing_symbols") and not partial_run_acceptable:
            blocking_codes.append("SYMBOL_DATA_MISSING")
        elif market_data_summary.get("missing_symbols"):
            warning_codes.append("SYMBOL_DATA_MISSING")
        if market_data_summary.get("stale_symbols") and not partial_run_acceptable:
            blocking_codes.append("MARKET_DATA_STALE")
        elif market_data_summary.get("stale_symbols"):
            warning_codes.append("MARKET_DATA_STALE")
        if market_data_summary.get("mapping_missing_symbols"):
            blocking_codes.append("SYMBOL_MAPPING_MISSING")
    if data_blocked:
        blocking_codes.append("CANDIDATE_REQUIRED_DATA_MISSING_OR_STALE")
    if trigger and trigger.get("ran") is not True and not partial_run_acceptable:
        blocking_codes.append("EVENT_REGIME_TRIGGERS_NOT_READY")
    elif trigger and trigger.get("ran") is not True:
        warning_codes.append("EVENT_REGIME_TRIGGERS_NOT_READY")
    blocking_codes = sorted(set(code for code in blocking_codes if code))
    warning_codes = sorted(set(code for code in warning_codes if code))
    if blocking_codes:
        _add_validation(validations, blockers, "candidate_generation_readiness", "FAIL", "CANDIDATE_GENERATION_NOT_READY", str(path))
        for code in blocking_codes:
            if code != "CANDIDATE_GENERATION_NOT_READY":
                blockers.append(code)
    elif partial_run_acceptable or blocked_total > 0:
        _add_validation(validations, blockers, "candidate_generation_readiness", "WARN", "PARTIAL_SLEEVE_READINESS", str(path))
    else:
        _add_validation(validations, blockers, "candidate_generation_readiness", "PASS", "", str(path))
    data_ready = not data_blocked
    return {
        "diagnostics_found": True,
        "diagnostics_path": str(path),
        "candidate_generation_ready": not blocking_codes,
        "data_ready": data_ready,
        "candidate_generation_status": status,
        "operator_interpretation": interpretation,
        "sleeves_enabled": sleeves_enabled,
        "sleeves_expected_today": sleeves_expected_today,
        "sleeves_expected": sleeves_expected,
        "sleeves_run": sleeves_run,
        "total_raw_signals": total_raw_signals,
        "total_candidates_generated": total_candidates,
        "total_candidates_rejected": total_rejected,
        "zero_candidate_explanation": str(payload.get("zero_candidate_explanation") or ""),
        "raw_signal_rejections": raw_signal_rejections,
        "selected_intent_promotion": selected_intent_promotion,
        "blocked_sleeves": blocked_sleeves,
        "trigger_evaluation_ran": bool(trigger.get("ran") is True),
        "market_data_summary": market_data_summary,
        "missing_symbol_explanations": missing_symbol_explanations,
        "blocked_sleeve_explanations": _blocked_sleeve_explanations(blocked_sleeves=blocked_sleeves, missing_symbol_explanations=missing_symbol_explanations),
        "blocking_codes": blocking_codes,
        "warning_codes": warning_codes,
        "recommended_action": _candidate_readiness_actions(blocking_codes=blocking_codes, data_ready=data_ready),
    }


def _candidate_diagnostics_path(*, truth_root: Path, day_utc: str) -> Path | None:
    exact = truth_root / "reports" / "aegis_candidate_generation_diagnostics_v1" / day_utc / "candidate_generation_diagnostics.v1.json"
    if exact.exists():
        return exact
    return _latest_file(truth_root / "reports" / "aegis_candidate_generation_diagnostics_v1" / day_utc, "candidate_generation_diagnostics.v1.json")


def _candidate_readiness_actions(*, blocking_codes: list[str], data_ready: bool) -> list[str]:
    if not blocking_codes:
        return ["No action needed."]
    actions = ["npm run aegis:candidate-diagnostics"]
    if not data_ready:
        if "MARKET_DATA_PROVIDER_NOT_CONFIGURED" in blocking_codes:
            actions.append("Configure AEGIS_MARKET_DATA_PROVIDER_PRIMARY or AEGIS_MARKET_DATA_PROVIDER_FALLBACK.")
            actions.append("Place manual CSV drop files in /home/node/constellation_runtime_data/market_data/manual_drop if using MANUAL_CSV_DROP.")
        actions.append("Refresh runtime evidence.")
        actions.append("Check missing data feed.")
    if "SLEEVE_RUNS_MISSING" in blocking_codes or "ENGINE_NOT_RUN" in blocking_codes:
        actions.append("Investigate sleeve generator.")
    if "EVENT_REGIME_TRIGGERS_NOT_READY" in blocking_codes:
        actions.append("Refresh event/regime trigger evidence.")
    return actions


def _blocked_sleeve_explanations(*, blocked_sleeves: list[dict[str, Any]], missing_symbol_explanations: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    vix = missing_symbol_explanations.get("VIX") if isinstance(missing_symbol_explanations.get("VIX"), dict) else {}
    for sleeve in blocked_sleeves:
        if not isinstance(sleeve, dict):
            continue
        reason = str(sleeve.get("reason") or sleeve.get("blocker") or "")
        if "VIX" not in reason:
            continue
        out.append(
            {
                "sleeve_id": str(sleeve.get("sleeve_id") or "UNKNOWN"),
                "required_symbol": "VIX",
                "required_input": "market.volatility.VIX",
                "explanation": vix.get("operator_message") or "Blocked sleeve requires canonical VIX; no valid real VIX source was available.",
                "fail_closed": True,
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            }
        )
    return out


def _validate_sizing_engine(
    *,
    validations: list[dict[str, Any]],
    blockers: list[str],
    candidates: list[dict[str, Any]],
    promoted_library: dict[str, Any],
    market_context: dict[str, Any],
) -> list[dict[str, Any]]:
    if not candidates:
        _add_validation(validations, blockers, "sizing_engine", "PASS", "", "")
        return []
    promoted_ids = _promoted_sleeve_ids(promoted_library)
    out: list[dict[str, Any]] = []
    failed = False
    for row in candidates:
        candidate = {**row, "runtime_truth_classification": "REAL_RUNTIME", "market_context": market_context}
        source_valid = str(row.get("sleeve_id") or "").strip() in promoted_ids
        sizing = build_trade_sizing_guidance_v1(candidate=candidate, promoted_sleeve_source_valid=source_valid)
        out.append({"candidate_id": str(row.get("candidate_id") or row.get("recommended_trade_id") or ""), "sizing": sizing})
        if sizing.get("sizing_blockers"):
            failed = True
    if failed:
        _add_validation(validations, blockers, "sizing_engine", "FAIL", "SIZING_ENGINE_VALIDATION_FAILED", "")
    else:
        _add_validation(validations, blockers, "sizing_engine", "PASS", "", "")
    return out


def _validate_eod_contract(
    *,
    validations: list[dict[str, Any]],
    blockers: list[str],
    truth_root: Path,
    day_utc: str,
    run_id: str,
    generated_at_utc: str,
    candidate_payload: dict[str, Any],
    promoted_library: dict[str, Any],
    promoted_sleeve_library_path: str,
) -> dict[str, Any]:
    candidates = candidate_payload.get("candidates") if isinstance(candidate_payload.get("candidates"), list) else []
    raw_absent_reason = str(candidate_payload.get("raw_candidate_absent_reason") or ("NO_TRADE_CANDIDATES" if not candidates else ""))
    input_payload = {
        **candidate_payload,
        "raw_candidates": candidate_payload.get("raw_candidates", candidates),
        "raw_candidate_absent_reason": raw_absent_reason,
        "promoted_sleeve_library": promoted_library,
        "promoted_sleeve_library_path": promoted_sleeve_library_path,
        "enforce_eod_input_contract": bool(candidates),
        "manual_execution_expected": bool(candidates),
    }
    market_authority = build_market_snapshot_authority_v1(
        truth_root=truth_root,
        trading_date=day_utc,
        run_id=run_id,
        created_at_utc=generated_at_utc,
        required_symbols=_required_symbols(candidates),
        source_artifact_lineage=[],
    )
    promoted_manifest = build_promoted_sleeve_manifest_v1(
        trading_date=day_utc,
        run_id=run_id,
        generated_at_utc=generated_at_utc,
        promoted_sleeve_library=promoted_library,
        source_artifact=promoted_sleeve_library_path,
    )
    contract = validate_eod_input_contract_v1(
        trading_date=day_utc,
        input_payload=input_payload,
        market_snapshot_authority=market_authority,
        promoted_sleeve_manifest=promoted_manifest,
        upstream_candidate_manifest_path=truth_root / "reports" / "candidate_generation_manifest_v1" / day_utc / "missing.json",
    )
    if str(contract.get("status") or "") == "PASS":
        _add_validation(validations, blockers, "eod_pipeline_contract", "PASS", "", "")
    else:
        _add_validation(validations, blockers, "eod_pipeline_contract", "FAIL", "EOD_PIPELINE_CONTRACT_VALIDATION_FAILED", "")
    return contract


def _refresh_control_packet_if_safe(*, truth_root: Path, day_utc: str, generated_at_utc: str, enabled: bool) -> dict[str, Any]:
    if not enabled:
        return {"status": "SKIPPED_DISABLED", "path": "", "runtime_truth_classification": ""}
    try:
        packet = build_aegis_chatgpt_control_packet_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)
    except Exception as exc:
        return {"status": "FAILED", "path": "", "runtime_truth_classification": "", "error": f"{type(exc).__name__}:{exc}"}
    runtime_truth = str(packet.get("runtime_truth_classification") or "")
    if runtime_truth == "REAL_RUNTIME":
        return {
            "status": "SKIPPED_REAL_RUNTIME_PROTECTED",
            "path": "",
            "runtime_truth_classification": runtime_truth,
            "real_runtime_packet_created": False,
        }
    path = write_aegis_chatgpt_control_packet_v1(truth_root=truth_root, payload=packet)
    return {
        "status": "REFRESHED",
        "path": str(path),
        "runtime_truth_classification": runtime_truth,
        "trade_advice_allowed": bool(packet.get("trade_advice_allowed", False)),
        "manual_trade_capture_allowed": bool(packet.get("manual_trade_capture_allowed", False)),
        "real_runtime_packet_created": False,
    }


def _alert_transport_configuration() -> dict[str, Any]:
    required = (
        "C2_EMAIL_SMTP_HOST",
        "C2_EMAIL_SMTP_PORT",
        "C2_EMAIL_USERNAME",
        "C2_EMAIL_PASSWORD",
        "C2_EMAIL_FROM",
        "C2_EMAIL_TO",
    )
    missing = [name for name in required if not str(os.environ.get(name) or "").strip()]
    return {
        "configured": not missing,
        "missing": missing,
        "smtp_host": str(os.environ.get("C2_EMAIL_SMTP_HOST") or "").strip(),
        "smtp_port": str(os.environ.get("C2_EMAIL_SMTP_PORT") or "").strip(),
        "username": str(os.environ.get("C2_EMAIL_USERNAME") or "").strip(),
        "from": str(os.environ.get("C2_EMAIL_FROM") or "").strip(),
        "to": str(os.environ.get("C2_EMAIL_TO") or "").strip(),
        "use_tls": str(os.environ.get("C2_EMAIL_USE_TLS") or "1").strip().lower() not in {"0", "false", "no"},
    }


def _deliver_noon_preflight_email(
    *,
    alert_required: bool,
    alert_transport: dict[str, Any],
    subject: str,
    body: str,
) -> dict[str, Any]:
    if not alert_required:
        return {
            "email_attempted": False,
            "email_status": "DISABLED",
            "email_error": None,
            "email_alert_sent": False,
            "email_transport_proven": False,
        }
    if not bool(alert_transport.get("configured")):
        missing = ",".join(alert_transport.get("missing") or [])
        return {
            "email_attempted": False,
            "email_status": "NOT_CONFIGURED",
            "email_error": f"EMAIL_NOT_CONFIGURED:{missing}",
            "email_alert_sent": False,
            "email_transport_proven": False,
        }
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = str(alert_transport.get("from") or "")
    message["To"] = str(alert_transport.get("to") or "")
    message.set_content(body)
    try:
        with smtplib.SMTP(str(alert_transport.get("smtp_host") or ""), int(alert_transport.get("smtp_port") or 587), timeout=20) as smtp:
            if bool(alert_transport.get("use_tls")):
                smtp.starttls()
            smtp.login(str(alert_transport.get("username") or ""), str(os.environ.get("C2_EMAIL_PASSWORD") or ""))
            smtp.send_message(message)
    except Exception as exc:
        return {
            "email_attempted": True,
            "email_status": "FAILED",
            "email_error": f"{type(exc).__name__}:{exc}",
            "email_alert_sent": False,
            "email_transport_proven": False,
        }
    return {
        "email_attempted": True,
        "email_status": "SENT",
        "email_error": None,
        "email_alert_sent": True,
        "email_transport_proven": True,
    }


def _delivery_status_from_email_attempt(email_attempt: dict[str, Any]) -> str:
    status = str(email_attempt.get("email_status") or "").upper()
    if status == "SENT":
        return "LIVE_EMAIL_SENT"
    if status == "FAILED":
        return "EMAIL_FAILED"
    if status == "NOT_CONFIGURED":
        return "EMAIL_NOT_CONFIGURED"
    return ALERT_DELIVERY_DRY_RUN


def _operator_alert_status_from_email_attempt(email_attempt: dict[str, Any]) -> str:
    status = str(email_attempt.get("email_status") or "").upper()
    if status == "SENT":
        return "email sent"
    if status == "FAILED":
        return "email delivery failed"
    if status == "NOT_CONFIGURED":
        return "email not configured"
    return "alert not live"


def _noon_email_body(
    *,
    result: str,
    readiness_status: str = "",
    blockers: list[str],
    next_action: str,
    candidate_readiness: dict[str, Any],
) -> str:
    market_data = candidate_readiness.get("market_data_summary") if isinstance(candidate_readiness.get("market_data_summary"), dict) else {}
    provider_config = market_data.get("provider_config") if isinstance(market_data.get("provider_config"), dict) else {}
    missing_symbols = ", ".join(market_data.get("missing_symbols") or []) or "none"
    stale_symbols = ", ".join(market_data.get("stale_symbols") or []) or "none"
    raw_rejections = candidate_readiness.get("raw_signal_rejections") if isinstance(candidate_readiness.get("raw_signal_rejections"), list) else []
    selected_intent_promotion = candidate_readiness.get("selected_intent_promotion") if isinstance(candidate_readiness.get("selected_intent_promotion"), dict) else {}
    blocked_sleeves = candidate_readiness.get("blocked_sleeves") if isinstance(candidate_readiness.get("blocked_sleeves"), list) else []
    rejection_lines = [
        f"- {row.get('sleeve_id') or 'UNKNOWN'} {row.get('raw_signal_id') or ''}: {row.get('rejection_stage') or 'UNKNOWN'} {row.get('rejection_reason') or ''} - {row.get('human_readable_explanation') or ''}"
        for row in raw_rejections[:5]
        if isinstance(row, dict)
    ]
    blocked_lines = [
        f"- {row.get('sleeve_id') or 'UNKNOWN'}: {row.get('blocker') or row.get('reason') or 'blocked'}"
        for row in blocked_sleeves[:5]
        if isinstance(row, dict)
    ]
    explanations = candidate_readiness.get("missing_symbol_explanations") if isinstance(candidate_readiness.get("missing_symbol_explanations"), dict) else {}
    explanation_lines = [
        f"- {symbol}: {row.get('operator_message') or row.get('blocker_reason') or ''}"
        for symbol, row in sorted(explanations.items())
        if isinstance(row, dict)
    ]
    return "\n".join(
        [
            "Aegis noon preflight requires operator review." if readiness_status == "READY_PARTIAL" else "Aegis noon preflight failed.",
            f"status: {result}",
            f"readiness_status: {readiness_status or 'UNKNOWN'}",
            f"operator_interpretation: {candidate_readiness.get('operator_interpretation') or 'UNKNOWN'}",
            f"zero_opportunity_explanation: {candidate_readiness.get('zero_candidate_explanation') or 'UNKNOWN'}",
            f"blocker: {', '.join(sorted(set(blockers))) or 'none'}",
            f"market_data_status: {market_data.get('status') or 'UNKNOWN'}",
            f"primary_provider: {provider_config.get('primary') or 'NOT_CONFIGURED'}",
            f"fallback_provider: {provider_config.get('fallback') or 'NOT_CONFIGURED'}",
            f"missing_symbols: {missing_symbols}",
            f"stale_symbols: {stale_symbols}",
            f"sleeves_expected: {candidate_readiness.get('sleeves_expected', 0)}",
            f"sleeves_run: {candidate_readiness.get('sleeves_run', 0)}",
            f"raw_signals: {candidate_readiness.get('total_raw_signals', 0)}",
            f"candidates_generated: {candidate_readiness.get('total_candidates_generated', 0)}",
            f"candidates_rejected: {candidate_readiness.get('total_candidates_rejected', 0)}",
            f"selected_intent_promotion_status: {selected_intent_promotion.get('status') or 'UNKNOWN'}",
            f"selected_intent_promotion_candidate_count: {selected_intent_promotion.get('promoted_candidate_count', 0)}",
            f"selected_intent_promotion_missing_fields: {', '.join(selected_intent_promotion.get('missing_contract_fields') or []) or 'none'}",
            f"data_ready: {str(candidate_readiness.get('data_ready') is True).lower()}",
            "raw_signal_rejections:",
            *(rejection_lines or ["- none"]),
            "blocked_sleeves:",
            *(blocked_lines or ["- none"]),
            "missing_symbol_explanations:",
            *(explanation_lines or ["- none"]),
            "recommended_next_commands:",
            "- npm run aegis:candidate-diagnostics",
            "- npm run aegis:noon-preflight",
            f"- {next_action}",
            "warning: no broker execution, autonomous execution, live trading, automatic approval, or automatic sleeve mutation was performed or authorized.",
        ]
    )


def _build_noon_preflight_report(
    *,
    day_utc: str,
    generated_at_utc: str,
    status: dict[str, Any],
    validations: list[dict[str, Any]],
    blockers: list[str],
    next_action: str,
    candidate_readiness: dict[str, Any],
    email_attempt: dict[str, Any],
) -> dict[str, Any]:
    failed = str(status.get("result") or "").upper() not in {"PASS", "WARN", "SKIPPED_NON_TRADING_DAY"}
    readiness_status = str(status.get("readiness_status") or _preflight_readiness_status(result=str(status.get("result") or ""), candidate_readiness=candidate_readiness))
    blocking_items = _blocking_items(validations=validations, blockers=blockers, candidate_readiness=candidate_readiness, failed=failed)
    email_status = str(email_attempt.get("email_status") or "DISABLED").upper()
    alert_reason = _alert_reason(candidate_readiness=candidate_readiness, blockers=blockers, failed=failed, email_status=email_status)
    recommended = _dedupe_strings(
        [
            *(candidate_readiness.get("recommended_action") if isinstance(candidate_readiness.get("recommended_action"), list) else []),
            next_action,
            *("Configure C2_EMAIL_SMTP_HOST, C2_EMAIL_SMTP_PORT, C2_EMAIL_USERNAME, C2_EMAIL_PASSWORD, C2_EMAIL_FROM, and C2_EMAIL_TO." for _ in [0] if email_status == "NOT_CONFIGURED"),
        ]
    )
    operator_alert_required = failed or readiness_status == "READY_PARTIAL"
    return {
        "schema_id": "aegis_noon_preflight",
        "schema_version": "v1",
        "artifact_id": "aegis_noon_preflight_v1",
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc,
        "status": "FAILED" if failed else ("PARTIAL" if str(status.get("result") or "").upper() == "WARN" else "PASSED"),
        "readiness_status": readiness_status,
        "operator_alert_required": operator_alert_required,
        "alert_severity": "CRITICAL" if failed else ("WARNING" if readiness_status == "READY_PARTIAL" else "NONE"),
        "alert_reason": alert_reason,
        "candidate_generation_ready": bool(candidate_readiness.get("candidate_generation_ready")),
        "data_ready": bool(candidate_readiness.get("data_ready")),
        "sleeves_expected": int(candidate_readiness.get("sleeves_expected") or 0),
        "sleeves_run": int(candidate_readiness.get("sleeves_run") or 0),
        "operator_interpretation": str(candidate_readiness.get("operator_interpretation") or "UNKNOWN"),
        "candidate_generation_status": str(candidate_readiness.get("candidate_generation_status") or "UNKNOWN"),
        "warning_codes": candidate_readiness.get("warning_codes") if isinstance(candidate_readiness.get("warning_codes"), list) else [],
        "missing_symbol_explanations": candidate_readiness.get("missing_symbol_explanations") if isinstance(candidate_readiness.get("missing_symbol_explanations"), dict) else {},
        "blocked_sleeve_explanations": candidate_readiness.get("blocked_sleeve_explanations") if isinstance(candidate_readiness.get("blocked_sleeve_explanations"), list) else [],
        "blocking_items": blocking_items,
        "recommended_action": recommended or ["No action needed."],
        "email_attempted": bool(email_attempt.get("email_attempted")),
        "email_status": email_status,
        "email_error": email_attempt.get("email_error"),
        "next_safe_command": "npm run aegis:candidate-diagnostics",
        "classifications": _dedupe_strings(
            [
                *("PREFLIGHT_FAILED" for _ in [0] if failed),
                *("READY_FULL" for _ in [0] if readiness_status == "READY_FULL"),
                *("READY_PARTIAL" for _ in [0] if readiness_status == "READY_PARTIAL"),
                *("BLOCKED" for _ in [0] if readiness_status == "BLOCKED"),
                *("DATA_BLOCKED" for _ in [0] if str(candidate_readiness.get("operator_interpretation") or "").upper() == "DATA_BLOCKED"),
                *("CANDIDATE_GENERATION_NOT_READY" for _ in [0] if not bool(candidate_readiness.get("candidate_generation_ready"))),
                *("OPERATOR_ALERT_REQUIRED" for _ in [0] if operator_alert_required),
            ]
        ),
        "safety": {
            "manual_execution_only": True,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "live_trading_allowed": False,
            "automatic_approval_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
        },
    }


def _blocking_items(
    *,
    validations: list[dict[str, Any]],
    blockers: list[str],
    candidate_readiness: dict[str, Any],
    failed: bool,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if failed:
        items.append({"code": "PREFLIGHT_FAILED", "check": "noon_preflight", "path": ""})
    if not bool(candidate_readiness.get("candidate_generation_ready")):
        items.append({"code": "CANDIDATE_GENERATION_NOT_READY", "check": "candidate_generation_readiness", "path": str(candidate_readiness.get("diagnostics_path") or "")})
    for code in candidate_readiness.get("blocking_codes") or []:
        items.append({"code": str(code), "check": "candidate_generation_readiness", "path": str(candidate_readiness.get("diagnostics_path") or "")})
    for row in validations:
        if not isinstance(row, dict) or row.get("status") != "FAIL":
            continue
        code = str(row.get("reason_code") or "").strip()
        if code:
            items.append({"code": code, "check": str(row.get("validation_id") or ""), "path": str(row.get("artifact_path") or "")})
    for code in blockers:
        items.append({"code": str(code), "check": "preflight", "path": ""})
    return _dedupe_blocking_items(items)


def _alert_reason(*, candidate_readiness: dict[str, Any], blockers: list[str], failed: bool, email_status: str) -> str:
    if str(candidate_readiness.get("operator_interpretation") or "").upper() == "DATA_BLOCKED":
        return "DATA_BLOCKED: required candidate-generation data is missing or stale."
    if not bool(candidate_readiness.get("candidate_generation_ready")):
        return "CANDIDATE_GENERATION_NOT_READY: candidate diagnostics show the pipeline did not fully run."
    if str(candidate_readiness.get("operator_interpretation") or "").upper() == "PARTIAL_RUN":
        return "READY_PARTIAL: candidate generation ran for ready sleeves; one or more sleeves remain blocked or warned."
    if email_status == "NOT_CONFIGURED":
        return "ALERT_TRANSPORT_UNAVAILABLE: operator email destination is not configured."
    if failed:
        return f"PREFLIGHT_FAILED: {', '.join(sorted(set(blockers))) or 'unknown blocker'}"
    return "No alert required."


def write_noon_preflight_report_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / NOON_PREFLIGHT_FAMILY / day_utc
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "noon_preflight.v1.json"
    summary_path = out_dir / "noon_preflight.summary.txt"
    matrix_path = out_dir / "noon_preflight.matrix.csv"
    payload["canonical_json_hash"] = None
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    json_path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    summary_path.write_text(render_noon_preflight_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_noon_preflight_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_noon_preflight_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS NOON PREFLIGHT v1",
        f"day_utc: {payload.get('day_utc')}",
        f"status: {payload.get('status')}",
        f"readiness_status: {payload.get('readiness_status')}",
        f"operator_alert_required: {str(payload.get('operator_alert_required') is True).lower()}",
        f"alert_reason: {payload.get('alert_reason')}",
        f"candidate_generation_ready: {str(payload.get('candidate_generation_ready') is True).lower()}",
        f"data_ready: {str(payload.get('data_ready') is True).lower()}",
        f"sleeves_expected: {payload.get('sleeves_expected')}",
        f"sleeves_run: {payload.get('sleeves_run')}",
        f"operator_interpretation: {payload.get('operator_interpretation')}",
        f"email_attempted: {str(payload.get('email_attempted') is True).lower()}",
        f"email_status: {payload.get('email_status')}",
        f"next_safe_command: {payload.get('next_safe_command')}",
        "",
        "blocking_items:",
    ]
    for row in payload.get("blocking_items") or []:
        lines.append(f"- {row.get('code')}: check={row.get('check')} path={row.get('path')}")
    explanations = payload.get("missing_symbol_explanations") if isinstance(payload.get("missing_symbol_explanations"), dict) else {}
    if explanations:
        lines.extend(["", "missing_symbol_explanations:"])
        for symbol, row in sorted(explanations.items()):
            if isinstance(row, dict):
                lines.append(f"- {symbol}: {row.get('operator_message') or row.get('blocker_reason') or ''}")
    blocked_explanations = payload.get("blocked_sleeve_explanations") if isinstance(payload.get("blocked_sleeve_explanations"), list) else []
    if blocked_explanations:
        lines.extend(["", "blocked_sleeve_explanations:"])
        for row in blocked_explanations:
            if isinstance(row, dict):
                lines.append(f"- {row.get('sleeve_id')}: {row.get('explanation') or ''}")
    lines.append("")
    lines.append("recommended_action:")
    for item in payload.get("recommended_action") or []:
        lines.append(f"- {item}")
    lines.extend(["", "broker_execution_allowed: false", "autonomous_execution_allowed: false", "automatic_sleeve_mutation_allowed: false", ""])
    return "\n".join(lines)


def render_noon_preflight_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=["code", "check", "path", "status", "operator_alert_required", "email_status"])
    writer.writeheader()
    rows = payload.get("blocking_items") if isinstance(payload.get("blocking_items"), list) else []
    if not rows:
        rows = [{"code": "NO_BLOCKERS", "check": "noon_preflight", "path": ""}]
    for row in rows:
        writer.writerow(
            {
                "code": row.get("code", ""),
                "check": row.get("check", ""),
                "path": row.get("path", ""),
                "status": payload.get("status", ""),
                "operator_alert_required": str(payload.get("operator_alert_required") is True).lower(),
                "email_status": payload.get("email_status", ""),
            }
        )
    return out.getvalue()


def _dedupe_blocking_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, Any]] = []
    for item in items:
        key = (str(item.get("code") or ""), str(item.get("check") or ""), str(item.get("path") or ""))
        if not key[0] or key in seen:
            continue
        seen.add(key)
        out.append({"code": key[0], "check": key[1], "path": key[2]})
    return out


def _dedupe_strings(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _alert_required(*, result: str, blockers: list[str], warnings: list[str]) -> bool:
    if result in {"PASS", "SKIPPED_NON_TRADING_DAY"}:
        return False
    alert_codes = {
        "PARTIAL_CONTEXT_AFTER_REFRESH",
        "CANDIDATE_INPUT_MISSING",
        "PROMOTED_SLEEVE_LIBRARY_MISSING",
        "PROMOTED_SLEEVE_LIBRARY_NO_PROMOTED_SLEEVES",
        "EVENT_RULES_REGISTRY_MISSING",
        "EVENT_RULES_REGISTRY_EMPTY",
        "MARKET_CONTEXT_SNAPSHOT_MISSING",
        "MARKET_CONTEXT_SNAPSHOT_STALE",
        "CANDIDATE_DIAGNOSTICS_MISSING",
        "CANDIDATE_GENERATION_NOT_READY",
        "CANDIDATE_GENERATION_STATUS_UNKNOWN",
        "CANDIDATE_GENERATION_STATUS_NOT_RUN",
        "CANDIDATE_GENERATION_STATUS_PARTIAL",
        "SLEEVE_RUNS_MISSING",
        "DATA_BLOCKED",
        "ENGINE_NOT_RUN",
        "PARTIAL_CONTEXT",
        "CANDIDATE_REQUIRED_DATA_MISSING_OR_STALE",
        "EVENT_REGIME_TRIGGERS_NOT_READY",
        "ALERT_TRANSPORT_UNAVAILABLE",
        "SIZING_ENGINE_VALIDATION_FAILED",
        "EOD_PIPELINE_CONTRACT_VALIDATION_FAILED",
        "NYSE_MARKET_CALENDAR_MISSING",
    }
    return any(code in alert_codes for code in blockers + warnings) or result == "FAIL"


def _alert_body(
    *,
    result: str,
    validations: list[dict[str, Any]],
    blockers: list[str],
    paths: dict[str, str],
    next_action: str,
    canonical_eod_at_risk: bool,
) -> str:
    failed = [row for row in validations if row["status"] == "FAIL"]
    path_lines = [f"- {name}: {path}" for name, path in paths.items() if path]
    return "\n".join(
        [
            "Aegis Noon Preflight Rehearsal requires operator action.",
            f"result: {result}",
            "failed_validations:",
            *[f"- {row['validation_id']}: {row['reason_code']} path={row.get('artifact_path') or ''}" for row in failed],
            "blockers:",
            *[f"- {code}" for code in sorted(set(blockers))],
            "exact_artifact_paths:",
            *path_lines,
            f"recommended_operator_action: {next_action}",
            f"canonical_eod_at_risk: {str(canonical_eod_at_risk).lower()}",
            "no_trades_or_broker_automation_occurred: true",
            "broker_submit_required: false",
            "ib_automation_required: false",
            "canonical_eod_state_mutated: false",
            "email_transport_status: alert not live; delivery_status=DRY_RUN_MESSAGE_BODY_ONLY",
        ]
    )


def _alert_ledger(
    *,
    day_utc: str,
    run_id: str,
    generated_at_utc: str,
    result: str,
    delivery_status: str,
    subject: str,
    body: str,
    manifest_path: str,
    diagnostic_path: str,
    control_packet_path: str,
) -> dict[str, Any]:
    return {
        "schema_id": "preflight_alert_ledger",
        "schema_version": "v1",
        "artifact_id": "preflight_alert_ledger_v1",
        "run_id": run_id,
        "day_utc": day_utc,
        "generated_at_utc": generated_at_utc,
        "email_transport_proven": False,
        "operator_status": "alert not live",
        "delivery_status": delivery_status,
        "alert_attempts": [
            {
                "alert_id": f"preflight-alert:{safe_run_id_v1(run_id)}",
                "result": result,
                "delivery_status": delivery_status,
                "email_transport_proven": False,
                "subject": subject,
                "body": body,
                "artifact_paths": {
                    "preflight_run_manifest": manifest_path,
                    "diagnostic_dry_run": diagnostic_path,
                    "control_packet": control_packet_path,
                },
            }
        ],
        "manual_execution_only": True,
        "broker_submit_required": False,
        "ib_automation_required": False,
        "canonical_eod_state_mutated": False,
        "canonical_json_hash": None,
    }


def _add_validation(
    validations: list[dict[str, Any]],
    blockers: list[str],
    validation_id: str,
    status: str,
    reason_code: str,
    artifact_path: str,
) -> None:
    validations.append(
        {
            "validation_id": validation_id,
            "status": status,
            "reason_code": reason_code,
            "artifact_path": artifact_path,
            "operator_action_required": status == "FAIL",
        }
    )
    if status == "FAIL" and reason_code:
        blockers.append(reason_code)


def _result_from_validations(*, validations: list[dict[str, Any]], blockers: list[str], warnings: list[str]) -> str:
    if blockers:
        return "FAIL"
    if warnings or any(row["status"] == "WARN" for row in validations):
        return "WARN"
    return "PASS"


def _preflight_readiness_status(*, result: str, candidate_readiness: dict[str, Any]) -> str:
    normalized_result = str(result or "").upper()
    interpretation = str(candidate_readiness.get("operator_interpretation") or "").upper()
    candidate_ready = bool(candidate_readiness.get("candidate_generation_ready"))
    sleeves_expected = int(candidate_readiness.get("sleeves_expected") or 0)
    sleeves_run = int(candidate_readiness.get("sleeves_run") or 0)
    sleeves_enabled = int(candidate_readiness.get("sleeves_enabled") or sleeves_expected or 0)
    if not candidate_ready or normalized_result == "FAIL":
        return "BLOCKED"
    if interpretation == "PARTIAL_RUN" or normalized_result == "WARN" or (sleeves_enabled and sleeves_run < sleeves_enabled):
        return "READY_PARTIAL"
    return "READY_FULL"


def _next_action(*, result: str, blockers: list[str], canonical_eod_at_risk: bool) -> str:
    if result == "PASS":
        return "No operator action. Wait for canonical 09:50 UTC and 14:50 UTC sleeve run."
    if result == "WARN" and not blockers:
        return "Review READY_PARTIAL warning details; resolve blocked sleeve inputs only if full readiness is required."
    if "PROMOTED_SLEEVE_LIBRARY_MISSING" in blockers:
        return "Restore or promote the governed sleeve library, then rerun noon preflight before 09:50 UTC and 14:50 UTC sleeve run."
    if any(code.startswith("MARKET_CONTEXT") for code in blockers):
        return "Refresh the market context snapshot, then rerun noon preflight before 09:50 UTC and 14:50 UTC sleeve run."
    if "EVENT_RULES_REGISTRY_MISSING" in blockers:
        return "Restore the event rules registry, then rerun noon preflight before 09:50 UTC and 14:50 UTC sleeve run."
    if canonical_eod_at_risk:
        return "Resolve listed blockers and rerun noon preflight before canonical 09:50 UTC and 14:50 UTC sleeve run."
    return "Review listed WARN items before canonical 09:50 UTC and 14:50 UTC sleeve run."


def _canonical_eod_risk_code(code: str) -> bool:
    return code in {
        "PARTIAL_CONTEXT_AFTER_REFRESH",
        "CANDIDATE_INPUT_MISSING",
        "PROMOTED_SLEEVE_LIBRARY_MISSING",
        "PROMOTED_SLEEVE_LIBRARY_NO_PROMOTED_SLEEVES",
        "EVENT_RULES_REGISTRY_MISSING",
        "EVENT_RULES_REGISTRY_EMPTY",
        "MARKET_CONTEXT_SNAPSHOT_MISSING",
        "MARKET_CONTEXT_SNAPSHOT_STALE",
        "CANDIDATE_DIAGNOSTICS_MISSING",
        "CANDIDATE_GENERATION_NOT_READY",
        "CANDIDATE_GENERATION_STATUS_UNKNOWN",
        "CANDIDATE_GENERATION_STATUS_NOT_RUN",
        "CANDIDATE_GENERATION_STATUS_PARTIAL",
        "SLEEVE_RUNS_MISSING",
        "DATA_BLOCKED",
        "ENGINE_NOT_RUN",
        "PARTIAL_CONTEXT",
        "CANDIDATE_REQUIRED_DATA_MISSING_OR_STALE",
        "EVENT_REGIME_TRIGGERS_NOT_READY",
        "ALERT_TRANSPORT_UNAVAILABLE",
        "SIZING_ENGINE_VALIDATION_FAILED",
        "EOD_PIPELINE_CONTRACT_VALIDATION_FAILED",
        "NYSE_MARKET_CALENDAR_MISSING",
    }


def _latest_file(root: Path, filename: str) -> Path | None:
    if not root.exists():
        return None
    paths = sorted(path for path in root.rglob(filename) if path.is_file())
    return paths[-1] if paths else None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _source_path(payload: dict[str, Any]) -> str:
    return str(payload.get("_preflight_source_path") or "")


def _promoted_sleeve_ids(library: dict[str, Any]) -> set[str]:
    sleeves = library.get("promoted_sleeves") if isinstance(library.get("promoted_sleeves"), list) else library.get("sleeves")
    return {
        str(row.get("sleeve_id") or "").strip()
        for row in sleeves
        if isinstance(row, dict) and str(row.get("promotion_status") or "").lower() == "promoted"
    } if isinstance(sleeves, list) else set()


def _required_symbols(rows: list[dict[str, Any]]) -> list[str]:
    symbols: set[str] = set()
    for row in rows:
        symbol = str(row.get("symbol") or row.get("symbol_or_pair") or "").strip().upper()
        if symbol:
            symbols.add(symbol)
    return sorted(symbols)


def _parse_utc(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_noon_preflight_rehearsal_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--run_id", default="")
    parser.add_argument("--generated_at_utc", default="")
    parser.add_argument("--candidate_input", default="")
    parser.add_argument("--promoted_sleeve_library", default="")
    parser.add_argument("--event_rules_registry", default="")
    parser.add_argument("--skip-control-packet-refresh", action="store_true")
    parser.add_argument("--skip-self-heal", action="store_true")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    context = run_context_from_env_v1("noon-preflight")
    if not args.skip_self_heal:
        allowed, _reason = step_allowed_v1(context, "self_heal")
        if allowed:
            run_data_remediation_v1(
                truth_root=truth_root,
                repo_root=REPO_ROOT,
                day_utc=day_utc,
                run_context=child_run_context_v1(
                    context,
                    "self-heal-data",
                    allow_market_data_refresh=False,
                    allow_self_heal=True,
                    allow_projection_rebuild=True,
                ),
            )
    generated_at_utc = str(args.generated_at_utc or now_utc_v1())
    run_id = str(args.run_id or f"aegis_noon_preflight:{day_utc}")
    status = run_noon_preflight_rehearsal_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        run_id=run_id,
        generated_at_utc=generated_at_utc,
        candidate_input_path=str(args.candidate_input or ""),
        promoted_sleeve_library_path=str(args.promoted_sleeve_library or ""),
        event_rules_registry_path=str(args.event_rules_registry or ""),
        refresh_control_packet=not bool(args.skip_control_packet_refresh),
    )
    print(
        json.dumps(
            {
                "result": status["result"],
                "readiness_status": status.get("readiness_status", ""),
                "status_path": status["status_path"],
                "noon_preflight_path": status.get("noon_preflight_path", ""),
                "blockers": status["blockers"],
                "email_alert_required": status["email_alert_required"],
                "email_alert_sent": status.get("email_alert_sent", False),
                "email_transport_status": status.get("email_transport_status", ""),
                "delivery_status": status["delivery_status"],
                "canonical_eod_at_risk": status["canonical_eod_at_risk"],
                "candidate_generation_ready": bool((status.get("candidate_readiness") or {}).get("candidate_generation_ready")),
                "broker_submit_required": False,
                "ib_automation_required": False,
            },
            sort_keys=True,
        )
    )
    return 0 if status["result"] in {"PASS", "WARN", "SKIPPED_NON_TRADING_DAY"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
