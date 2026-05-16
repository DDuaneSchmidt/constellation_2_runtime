#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
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


ARTIFACT_FAMILY = "aegis_noon_preflight_rehearsal_v1"
ALERT_SUBJECT = "[Aegis Preflight FAIL] action needed before 15:50 EOD"
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
    if control_refresh["status"] == "REFRESHED" and control_refresh.get("runtime_truth_classification") == "PARTIAL_CONTEXT":
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

    result = _result_from_validations(validations=validations, blockers=blockers, warnings=warnings)
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
    delivery_status = "NOT_SENT"
    operator_alert_status = "no alert needed"
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

    manifest = {
        "schema_id": "preflight_run_manifest",
        "schema_version": "v1",
        "artifact_id": "preflight_run_manifest_v1",
        "day_utc": day_utc,
        "run_id": run_id,
        "generated_at_utc": generated_at_utc,
        "result": result,
        "source_artifacts_used": source_refs,
        "diagnostic_dry_run_artifact_path": str(diagnostic_path),
        "control_packet_refresh": control_refresh,
        "alert_required": alert_required,
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
        "failed_validations": [row for row in validations if row["status"] == "FAIL"],
        "warnings": [row for row in validations if row["status"] == "WARN"],
        "blockers": sorted(set(blockers)),
        "next_action": next_action,
        "canonical_eod_at_risk": canonical_eod_at_risk,
        "email_alert_required": alert_required,
        "email_alert_sent": False,
        "email_transport_proven": False,
        "email_transport_status": EMAIL_TRANSPORT_STATUS,
        "delivery_status": delivery_status,
        "operator_alert_status": operator_alert_status,
        "alert_subject": alert_subject,
        "alert_body": alert_body,
        "alert_ledger_path": ledger_path,
        "manifest_path": str(manifest_path),
        "diagnostic_dry_run_path": str(diagnostic_path),
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
    return {**status, "status_path": str(status_path)}


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
        _add_validation(validations, blockers, "market_context_snapshot", "FAIL", "MARKET_CONTEXT_SNAPSHOT_STALE", str(path))
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


def _next_action(*, result: str, blockers: list[str], canonical_eod_at_risk: bool) -> str:
    if result == "PASS":
        return "No operator action. Wait for canonical 15:50 EOD."
    if "PROMOTED_SLEEVE_LIBRARY_MISSING" in blockers:
        return "Restore or promote the governed sleeve library, then rerun noon preflight before 15:50 EOD."
    if any(code.startswith("MARKET_CONTEXT") for code in blockers):
        return "Refresh the market context snapshot, then rerun noon preflight before 15:50 EOD."
    if "EVENT_RULES_REGISTRY_MISSING" in blockers:
        return "Restore the event rules registry, then rerun noon preflight before 15:50 EOD."
    if canonical_eod_at_risk:
        return "Resolve listed blockers and rerun noon preflight before canonical 15:50 EOD."
    return "Review listed WARN items before canonical 15:50 EOD."


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
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
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
                "status_path": status["status_path"],
                "blockers": status["blockers"],
                "email_alert_required": status["email_alert_required"],
                "delivery_status": status["delivery_status"],
                "canonical_eod_at_risk": status["canonical_eod_at_risk"],
                "broker_submit_required": False,
                "ib_automation_required": False,
            },
            sort_keys=True,
        )
    )
    return 0 if status["result"] in {"PASS", "WARN", "SKIPPED_NON_TRADING_DAY"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
