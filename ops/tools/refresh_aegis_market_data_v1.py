#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import replace
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.intelligence_common_v1 import latest_json_v1, write_json_v1  # noqa: E402
from ops.aegis.market_data.freshness_policy_v1 import classify_market_data_freshness_v1  # noqa: E402
from ops.aegis.market_data.market_data_provider_v1 import fetch_market_data_v1, finalization_window_v1, provider_capability_report_v1, provider_config_from_env_v1  # noqa: E402
from ops.aegis.market_data.market_data_mode_v1 import FINAL_EOD_CERTIFIED, INTRADAY_OPERATIONAL, normalize_market_data_mode_v1  # noqa: E402
from ops.aegis.market_data.market_data_snapshot_plane_v1 import build_market_data_snapshot_v1, write_market_data_snapshot_v1  # noqa: E402
from ops.aegis.market_data.symbol_alias_registry_v1 import (  # noqa: E402
    canonicalize_symbol_list_v1,
    market_data_item_id_v1,
    symbol_resolution_row_v1,
)
from ops.aegis.market_data.symbol_map_v1 import build_symbol_map_v1, symbol_map_entry_for_symbol_v1, write_symbol_map_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402
from ops.aegis.run_context_v1 import child_run_context_v1, run_context_from_env_v1, step_allowed_v1  # noqa: E402


REPORT_FAMILY = "aegis_market_data_v1"
INTRADAY_REPORT_FAMILY = "market_data_intraday_operational_v1"
FINAL_EOD_REPORT_FAMILY = "market_data_final_eod_v1"


def build_market_data_report_v1(*, truth_root: Path, day_utc: str, symbol_map_override: dict[str, Any] | None = None, timeout_diagnostic: dict[str, Any] | None = None, market_data_mode: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    requested_mode = normalize_market_data_mode_v1(market_data_mode or os.environ.get("AEGIS_MARKET_DATA_MODE"))
    symbol_map = symbol_map_override if isinstance(symbol_map_override, dict) else build_symbol_map_v1(repo_root=REPO_ROOT, day_utc=day_utc)
    paths = write_symbol_map_v1(truth_root=root, day_utc=day_utc, payload=symbol_map)
    symbol_map_path = Path(paths["json"])
    requested_symbols = _requested_symbols(symbol_map)
    config = provider_config_from_env_v1()
    requested_mode = normalize_market_data_mode_v1(market_data_mode or config.market_data_mode or requested_mode)
    config = replace(config, market_data_mode=requested_mode)
    result = fetch_market_data_v1(truth_root=root, day_utc=day_utc, symbols=requested_symbols, symbol_map=symbol_map, config_override=config)
    capability_report = provider_capability_report_v1(config)
    symbols = result.symbols
    missing_fields: list[str] = []
    stale_fields: list[str] = []
    mapping_missing = list(result.mapping_missing_symbols)
    for symbol in requested_symbols:
        row = symbols.get(symbol)
        item_id = market_data_item_id_v1(symbol)
        if symbol in mapping_missing:
            missing_fields.append(item_id)
        if not row:
            missing_fields.append(item_id)
        elif str(row.get("freshness_status") or "").upper() == "STALE":
            stale_fields.append(item_id)
    breadth = result.breadth or {}
    for field in ("breadth_down_pct", "advance_decline_delta"):
        if config.require_breadth and breadth.get(field) in {None, ""}:
            missing_fields.append(f"market.breadth.{field.replace('breadth_', '') if field == 'breadth_down_pct' else field}")
    fetched_symbols = sorted(symbols)
    missing_symbols = sorted(set(requested_symbols) - set(fetched_symbols))
    stale_symbols = sorted(symbol for symbol, row in symbols.items() if str(row.get("freshness_status") or "").upper() == "STALE")
    provider_attempts = list(result.provider_attempts)
    final_eod_symbols = sorted(symbol for symbol, row in symbols.items() if str(row.get("freshness_status") or "").upper() == "CURRENT" and str(row.get("data_finality") or "").upper() == "FINAL_EOD")
    provisional_intraday_symbols = sorted(symbol for symbol, row in symbols.items() if str(row.get("data_finality") or "").upper() == "PROVISIONAL_INTRADAY")
    required_contract_items, optional_contract_items = _contract_market_item_sets(root=root, day_utc=day_utc)
    gap_classification = _classify_market_gaps(
        requested_symbols=requested_symbols,
        missing_symbols=missing_symbols,
        stale_symbols=stale_symbols,
        required_contract_items=required_contract_items,
        optional_contract_items=optional_contract_items,
    )
    sleeve_critical_missing = _symbols_by_gap_class(gap_classification, status="MISSING", classification="SLEEVE_CRITICAL")
    sleeve_critical_stale = _symbols_by_gap_class(gap_classification, status="STALE", classification="SLEEVE_CRITICAL")
    runtime_critical_missing = _symbols_by_gap_class(gap_classification, status="MISSING", classification="RUNTIME_CRITICAL")
    runtime_critical_stale = _symbols_by_gap_class(gap_classification, status="STALE", classification="RUNTIME_CRITICAL")
    advisory_only_missing = _symbols_by_gap_class(gap_classification, status="MISSING", classification="ADVISORY_ONLY")
    advisory_only_stale = _symbols_by_gap_class(gap_classification, status="STALE", classification="ADVISORY_ONLY")
    legacy_optional_missing = _symbols_by_gap_class(gap_classification, status="MISSING", classification="LEGACY_OPTIONAL")
    legacy_optional_stale = _symbols_by_gap_class(gap_classification, status="STALE", classification="LEGACY_OPTIONAL")
    future_required_missing = _symbols_by_gap_class(gap_classification, status="MISSING", classification="FUTURE_REQUIRED")
    future_required_stale = _symbols_by_gap_class(gap_classification, status="STALE", classification="FUTURE_REQUIRED")
    blocking_missing = sorted(set(sleeve_critical_missing) | set(runtime_critical_missing) | set(future_required_missing))
    blocking_stale = sorted(set(sleeve_critical_stale) | set(runtime_critical_stale) | set(future_required_stale))
    required_symbols_for_final = {symbol for item in required_contract_items if (symbol := _symbol_from_market_item_id(item))}
    if not required_symbols_for_final:
        required_symbols_for_final = set(requested_symbols)
    blocking_provisional_symbols = sorted(symbol for symbol in required_symbols_for_final if symbol in provisional_intraday_symbols)
    intraday_blocking_provisional_symbols = [] if requested_mode == INTRADAY_OPERATIONAL else blocking_provisional_symbols
    final_eod_missing_symbols = sorted(symbol for symbol in required_symbols_for_final if symbol not in final_eod_symbols)
    legacy_report_status = _status(request_status=result.request_status, missing_fields=missing_fields, stale_fields=stale_fields)
    status = _classified_status(request_status=result.request_status, blocking_missing=blocking_missing, blocking_stale=blocking_stale or intraday_blocking_provisional_symbols, any_missing=missing_symbols, any_stale=stale_symbols or ([] if requested_mode == INTRADAY_OPERATIONAL else provisional_intraday_symbols))
    current_sleeve_market_readiness_status = "READY" if not (sleeve_critical_missing or sleeve_critical_stale or intraday_blocking_provisional_symbols) else "BLOCKED"
    final_eod_ready = bool(required_symbols_for_final) and not final_eod_missing_symbols and not blocking_missing and not blocking_stale and not blocking_provisional_symbols and status in {"CURRENT", "CURRENT_WITH_WARNINGS"}
    intraday_operational_ready = bool(required_symbols_for_final) and not blocking_missing and not blocking_stale and current_sleeve_market_readiness_status == "READY" and any(
        bool((symbols.get(symbol) or {}).get("candidate_generation_eligible") is True)
        for symbol in required_symbols_for_final
    )
    operator_market_data_state = _operator_market_data_state(
        provider_attempts=provider_attempts,
        final_eod_ready=final_eod_ready,
        intraday_operational_ready=intraday_operational_ready,
        requested_mode=requested_mode,
        fetched_symbols=fetched_symbols,
        missing_symbols=missing_symbols,
        stale_symbols=stale_symbols,
        provisional_symbols=provisional_intraday_symbols,
    )
    current_session_symbols = sorted(
        symbol
        for symbol, row in symbols.items()
        if isinstance(row, dict)
        and str(row.get("freshness_status") or "").upper() == "CURRENT"
        and str(row.get("market_session_date") or row.get("returned_data_date") or result.returned_data_date or day_utc) == day_utc
    )
    freshness_decision = classify_market_data_freshness_v1(
        truth_root=root,
        day_utc=day_utc,
        required_symbols=sorted(required_symbols_for_final or set(requested_symbols)),
        fetched_symbols=fetched_symbols,
        missing_symbols=blocking_missing or missing_symbols,
        stale_symbols=blocking_stale or stale_symbols,
        current_session_symbols=current_session_symbols,
        as_of_utc=result.timestamp_utc,
        provider_status=status,
        provider_error=result.failure_reason or "",
    )
    market_certification_state = "CERTIFIED" if final_eod_ready else freshness_decision.certification_state
    symbol_resolution = {
        symbol: symbol_resolution_row_v1(symbol_map, symbol, [config.primary, config.fallback])
        for symbol in requested_symbols
        if symbol == "VIX"
    }
    missing_symbol_explanations = _missing_symbol_explanations(
        missing_symbols=missing_symbols,
        stale_symbols=stale_symbols,
        symbol_resolution=symbol_resolution,
        provider_results=list(result.provider_results),
        config={"primary": config.primary, "fallback": config.fallback},
    )
    timeout_symbols = sorted({
        str(row.get("symbol") or "").upper()
        for row in provider_attempts
        if str(row.get("status") or "").upper() in {"TIMEOUT", "NOT_ATTEMPTED_DEADLINE_EXHAUSTED", "RATE_LIMITED"}
        and str(row.get("symbol") or "")
    })
    provider_coverage_plan = result.provider_coverage_plan or {}
    provider_coverage_action_item = {
        "status": "OK" if final_eod_ready else "PROVIDER_COVERAGE_INCOMPLETE",
        "message": "Provider coverage complete." if final_eod_ready else "Provider coverage incomplete",
        "required_universe_size": len(requested_symbols),
        "covered_count": len(final_eod_symbols) if requested_mode == FINAL_EOD_CERTIFIED else len(fetched_symbols),
        "unsupported_symbols": list(provider_coverage_plan.get("unsupported_symbols") or []),
        "timeout_symbols": timeout_symbols,
        "stale_symbols": blocking_stale or stale_symbols,
        "missing_symbols": missing_symbols,
        "recommended_fix": "" if final_eod_ready else "Configure a reliable certification-eligible EOD provider, add symbol mappings/fallback provider, reduce/split the governed universe, or rerun provider refresh after resolving timeouts.",
    }
    payload = {
        "schema_id": "aegis_market_data",
        "schema_version": "v1",
        "artifact_id": "aegis_market_data_v1",
        "day_utc": day_utc,
        "market_session_date": result.returned_data_date or day_utc,
        "generated_at_utc": result.timestamp_utc,
        "status": status,
        "market_data_mode": requested_mode,
        "certification_mode": "FINAL_EOD_CERTIFICATION" if requested_mode == FINAL_EOD_CERTIFIED else "INTRADAY_OPERATIONAL_RUN",
        "legacy_market_report_status": legacy_report_status,
        "status_scope": "CURRENT_SLEEVE_MARKET_READINESS" if status in {"CURRENT", "CURRENT_WITH_WARNINGS"} else "BLOCKING_MARKET_READINESS",
        "current_sleeve_market_readiness_status": current_sleeve_market_readiness_status,
        "source": result.provider,
        "provider_capability_report": capability_report,
        "provider_coverage_plan": provider_coverage_plan,
        "provider_coverage_action_item": provider_coverage_action_item,
        "provider_config": {
            "primary": config.primary,
            "fallback": config.fallback,
            "allow_delayed": config.allow_delayed,
            "require_current_session": config.require_current_session,
            "require_breadth": config.require_breadth,
            "timeout_seconds": config.timeout_seconds,
            "per_symbol_timeout_seconds": config.per_symbol_timeout_seconds,
            "total_timeout_seconds": config.total_timeout_seconds,
            "stooq_retries": config.stooq_retries,
            "stooq_backoff_seconds": config.stooq_backoff_seconds,
            "stooq_chunk_size": config.stooq_chunk_size,
            "cache_ttl_seconds": config.cache_ttl_seconds,
            "configured": bool(config.primary or config.fallback),
            "market_data_mode": requested_mode,
        },
        "symbol_map_path": str(symbol_map_path or ""),
        "runtime_universe_mode": symbol_map.get("runtime_universe_mode") or "",
        "production_scan_dataset_id": symbol_map.get("production_scan_dataset_id") or "",
        "dataset_snapshot_id": symbol_map.get("dataset_snapshot_id") or symbol_map.get("production_scan_dataset_id") or "",
        "production_scan_universe_count": int(symbol_map.get("production_scan_universe_count") or 0),
        "sleeve_required_symbol_count": int(symbol_map.get("sleeve_required_symbol_count") or 0),
        "total_requested_symbol_count": len(requested_symbols),
        "requested_symbols_source": symbol_map.get("requested_symbols_source") or "",
        "minimum_viable_runtime_reference_removed": bool(symbol_map.get("minimum_viable_runtime_reference_removed")),
        "provider_results": list(result.provider_results)
        or [
            {
                "provider": result.provider,
                "request_status": result.request_status,
                "timestamp_utc": result.timestamp_utc,
                "returned_data_date": result.returned_data_date,
                "failure_reason": result.failure_reason,
                "fetched_symbols": fetched_symbols,
                "missing_symbols": missing_symbols,
                "stale_symbols": stale_symbols,
            }
        ],
        "provider_attempts": provider_attempts,
        "provider_attempts_artifact": "",
        "finalization_window": finalization_window_v1(day_utc),
        "freshness_state": freshness_decision.freshness_state,
        "validation_status": freshness_decision.validation_status,
        "certification_state": market_certification_state,
        "market_calendar": freshness_decision.market_calendar,
        "expected_current_data_after_utc": freshness_decision.expected_current_data_after_utc,
        "next_retry_utc": freshness_decision.next_retry_utc,
        "last_attempt": freshness_decision.last_attempt,
        "freshness_reason": freshness_decision.reason,
        "freshness_message": freshness_decision.message,
        "legacy_operator_market_data_state": operator_market_data_state,
        "operator_market_data_state": operator_market_data_state,
        "final_eod_ready": final_eod_ready,
        "intraday_operational_ready": intraday_operational_ready,
        "final_eod_certification_status": "VALID" if final_eod_ready else ("PENDING" if intraday_operational_ready else "UNAVAILABLE"),
        "final_eod_symbols": final_eod_symbols,
        "final_eod_missing_symbols": final_eod_missing_symbols,
        "provisional_intraday_symbols": provisional_intraday_symbols,
        "blocking_provisional_symbols": intraday_blocking_provisional_symbols,
        "final_eod_pending_symbols": blocking_provisional_symbols,
        "normalized_records": list(result.normalized_records),
        "symbols": symbols,
        "breadth": {
            "advance_decline_delta": breadth.get("advance_decline_delta"),
            "breadth_down_pct": breadth.get("breadth_down_pct"),
            "source": breadth.get("source"),
            "market_session_date": breadth.get("market_session_date"),
            "data_timestamp_utc": breadth.get("data_timestamp_utc"),
            "freshness_status": str(breadth.get("freshness_status") or "MISSING").upper(),
            "quality": breadth.get("quality") or "UNKNOWN",
        },
        "missing_fields": missing_fields,
        "stale_fields": stale_fields,
        "requested_symbols": requested_symbols,
        "fetched_symbols": fetched_symbols,
        "missing_symbols": missing_symbols,
        "stale_symbols": stale_symbols,
        "symbol_resolution": symbol_resolution,
        "missing_symbol_explanations": missing_symbol_explanations,
        "market_gap_classification": gap_classification,
        "gap_summary": _gap_summary(gap_classification),
        "required_contract_market_item_ids": required_contract_items,
        "optional_contract_market_item_ids": optional_contract_items,
        "sleeve_critical_missing_symbols": sleeve_critical_missing,
        "sleeve_critical_stale_symbols": sleeve_critical_stale,
        "runtime_critical_missing_symbols": runtime_critical_missing,
        "runtime_critical_stale_symbols": runtime_critical_stale,
        "advisory_only_missing_symbols": advisory_only_missing,
        "advisory_only_stale_symbols": advisory_only_stale,
        "legacy_optional_missing_symbols": legacy_optional_missing,
        "legacy_optional_stale_symbols": legacy_optional_stale,
        "future_required_missing_symbols": future_required_missing,
        "future_required_stale_symbols": future_required_stale,
        "blocking_missing_symbols": blocking_missing,
        "blocking_stale_symbols": blocking_stale,
        "mapping_missing_symbols": mapping_missing,
        "provider_failed_symbols": sorted(result.provider_failed_symbols),
        "usable_for_candidate_generation": bool(freshness_decision.usable_for_candidate_visibility),
        "usable_for_candidate_visibility": bool(freshness_decision.usable_for_candidate_visibility),
        "usable_for_execution_candidate_generation": bool(final_eod_ready),
        "usable_for_current_sleeve_candidate_generation": bool(freshness_decision.usable_for_candidate_visibility),
        "candidate_generation_label": "FINAL_EOD_CERTIFIED" if final_eod_ready else ("NON_CERTIFIED" if freshness_decision.usable_for_candidate_visibility else "BLOCKED"),
        "candidate_lane": "CERTIFIED" if final_eod_ready else "PROVISIONAL",
        "final_eod_certification_pending": bool(requested_mode == INTRADAY_OPERATIONAL and freshness_decision.usable_for_candidate_visibility and not final_eod_ready),
        "failure_reason": result.failure_reason or None,
        "timeout_diagnostic": timeout_diagnostic or {},
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }
    snapshot = build_market_data_snapshot_v1(truth_root=root, day_utc=day_utc, market_report=payload, captured_at_utc=result.timestamp_utc)
    snapshot_paths = write_market_data_snapshot_v1(truth_root=root, snapshot=snapshot)
    payload["market_data_snapshot_id"] = snapshot_paths["snapshot_id"]
    payload["market_data_snapshot_path"] = snapshot_paths["json"]
    payload["market_data_snapshot_hash"] = snapshot_paths["hash"]
    payload["input_market_data_snapshot_ids"] = [snapshot_paths["snapshot_id"]]
    return payload


def write_market_data_report_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    out_dir = root / "reports" / REPORT_FAMILY / day_utc
    attempts = payload.get("provider_attempts") if isinstance(payload.get("provider_attempts"), list) else []
    attempts_json_path = out_dir / "market_data_provider_attempts.v1.json"
    capability_json_path = out_dir / "market_data_provider_capabilities.v1.json"
    missing_config_json_path = out_dir / "market_data_provider_missing_config.v1.json"
    attempts_txt_path = out_dir / "market_data_provider_attempts.v1.txt"
    payload["provider_attempts_artifact"] = str(attempts_json_path)
    json_path = write_json_v1(out_dir / "market_data.v1.json", payload)
    summary_path = out_dir / "market_data.summary.txt"
    matrix_path = out_dir / "market_data.matrix.csv"
    attempts_payload = {
        "schema_id": "market_data_provider_attempts",
        "schema_version": "v1",
        "day_utc": day_utc,
        "generated_at_utc": payload.get("generated_at_utc") or "",
        "finalization_window": payload.get("finalization_window") if isinstance(payload.get("finalization_window"), dict) else {},
        "attempt_count": len(attempts),
        "attempts": attempts,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }
    write_json_v1(attempts_json_path, attempts_payload)
    capability_payload = payload.get("provider_capability_report") if isinstance(payload.get("provider_capability_report"), dict) else {}
    write_json_v1(capability_json_path, capability_payload)
    if str(capability_payload.get("status") or "") != "VALID":
        write_json_v1(missing_config_json_path, {"schema_id": "market_data_provider_missing_config", "schema_version": "v1", "day_utc": day_utc, "status": capability_payload.get("status") or "UNKNOWN", "message": "No intraday market data provider configured." if payload.get("market_data_mode") == INTRADAY_OPERATIONAL else "No final EOD market data provider configured.", "required_configuration": capability_payload.get("required_configuration") or "", "broker_execution_allowed": False, "autonomous_execution_allowed": False})
    attempts_txt_path.write_text(render_provider_attempts_summary(attempts_payload), encoding="utf-8")
    summary_path.write_text(render_summary(payload), encoding="utf-8")
    matrix_path.write_text(render_matrix(payload), encoding="utf-8")
    mode = normalize_market_data_mode_v1(payload.get("market_data_mode"))
    if mode == FINAL_EOD_CERTIFIED:
        specific_dir = root / "reports" / FINAL_EOD_REPORT_FAMILY / day_utc
        specific_name = "market_data_final_eod.v1.json"
    else:
        specific_dir = root / "reports" / INTRADAY_REPORT_FAMILY / day_utc
        specific_name = "market_data_intraday_operational.v1.json"
    specific_json = write_json_v1(specific_dir / specific_name, payload)
    specific_txt = specific_dir / specific_name.replace(".json", ".txt")
    specific_txt.write_text(render_summary(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path), "provider_attempts_json": str(attempts_json_path), "provider_attempts_txt": str(attempts_txt_path), "provider_capabilities_json": str(capability_json_path), "provider_missing_config_json": str(missing_config_json_path) if str((payload.get("provider_capability_report") or {}).get("status") or "") != "VALID" else "", "mode_json": str(specific_json), "mode_summary": str(specific_txt)}


def _operator_market_data_state(*, provider_attempts: list[dict[str, Any]], final_eod_ready: bool, intraday_operational_ready: bool, requested_mode: str, fetched_symbols: list[str], missing_symbols: list[str], stale_symbols: list[str], provisional_symbols: list[str]) -> str:
    statuses = {str(row.get("status") or "").upper() for row in provider_attempts if isinstance(row, dict)}
    if final_eod_ready:
        return "FINAL_EOD_READY"
    if requested_mode == INTRADAY_OPERATIONAL and intraday_operational_ready:
        return "INTRADAY_OPERATIONAL_READY"
    if "SOURCE_NOT_FINALIZED" in statuses:
        return "MARKET_NOT_FINALIZED_YET"
    if "TIMEOUT" in statuses or "NOT_ATTEMPTED_DEADLINE_EXHAUSTED" in statuses:
        return "PARTIAL_PROVIDER_SUCCESS" if fetched_symbols else "PROVIDER_TIMEOUT"
    if "SOURCE_UNAVAILABLE" in statuses or "NETWORK_ERROR" in statuses:
        return "PARTIAL_PROVIDER_SUCCESS" if fetched_symbols else "PROVIDER_SOURCE_UNAVAILABLE"
    if provisional_symbols:
        return "PARTIAL_PROVIDER_SUCCESS"
    if stale_symbols:
        return "CURRENT_DAY_DATA_STALE"
    if missing_symbols:
        return "PARTIAL_PROVIDER_SUCCESS" if fetched_symbols else "PROVIDER_SOURCE_UNAVAILABLE"
    return "FINAL_EOD_READY" if final_eod_ready else "PROVIDER_SOURCE_UNAVAILABLE"


def render_provider_attempts_summary(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS MARKET DATA PROVIDER ATTEMPTS v1",
        f"day_utc: {payload.get('day_utc')}",
        f"generated_at_utc: {payload.get('generated_at_utc')}",
        f"attempt_count: {payload.get('attempt_count')}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
    ]
    window = payload.get("finalization_window") if isinstance(payload.get("finalization_window"), dict) else {}
    if window:
        lines.extend([
            f"finalization_window_start_utc: {window.get('start_utc') or ''}",
            f"finalization_window_end_utc: {window.get('end_utc') or ''}",
            "",
        ])
    for row in payload.get("attempts", []) if isinstance(payload.get("attempts"), list) else []:
        if not isinstance(row, dict):
            continue
        lines.append(
            " | ".join(
                [
                    f"symbol={row.get('symbol') or ''}",
                    f"provider={row.get('provider') or ''}",
                    f"status={row.get('status') or ''}",
                    f"duration_ms={row.get('duration_ms') or 0}",
                    f"source_timestamp={row.get('source_timestamp') or ''}",
                    f"raw={row.get('raw_output_path') or ''}",
                    f"exception={row.get('exception_class') or ''}",
                    f"accepted={row.get('accepted_reason') or ''}",
                    f"rejected={row.get('rejected_reason') or ''}",
                ]
            )
        )
    return "\n".join(lines) + "\n"


def render_summary(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS MARKET DATA v1",
        f"day_utc: {payload.get('day_utc')}",
        f"status: {payload.get('status')}",
        f"market_data_mode: {payload.get('market_data_mode') or ''}",
        f"legacy_market_report_status: {payload.get('legacy_market_report_status') or payload.get('status')}",
        f"status_scope: {payload.get('status_scope') or ''}",
        f"current_sleeve_market_readiness_status: {payload.get('current_sleeve_market_readiness_status') or ''}",
        f"source: {payload.get('source') or 'NOT_CONFIGURED'}",
        f"provider_capability_status: {((payload.get('provider_capability_report') or {}).get('status') if isinstance(payload.get('provider_capability_report'), dict) else '')}",
        f"operator_market_data_state: {payload.get('operator_market_data_state') or ''}",
        f"freshness_state: {payload.get('freshness_state') or ''}",
        f"validation_status: {payload.get('validation_status') or ''}",
        f"final_eod_ready: {str(payload.get('final_eod_ready') is True).lower()}",
        f"intraday_operational_ready: {str(payload.get('intraday_operational_ready') is True).lower()}",
        f"final_eod_certification_status: {payload.get('final_eod_certification_status') or ''}",
        f"final_eod_symbols: {', '.join(payload.get('final_eod_symbols') or [])}",
        f"final_eod_missing_symbols: {', '.join(payload.get('final_eod_missing_symbols') or [])}",
        f"provisional_intraday_symbols: {', '.join(payload.get('provisional_intraday_symbols') or [])}",
        f"usable_for_candidate_generation: {str(payload.get('usable_for_candidate_generation') is True).lower()}",
        f"usable_for_current_sleeve_candidate_generation: {str(payload.get('usable_for_current_sleeve_candidate_generation') is True).lower()}",
        f"failure_reason: {payload.get('failure_reason') or ''}",
        f"universe_mode: {payload.get('runtime_universe_mode') or ''}",
        f"dataset_snapshot_id: {payload.get('production_scan_dataset_id') or payload.get('dataset_snapshot_id') or ''}",
        f"production_scan_universe_count: {payload.get('production_scan_universe_count') or 0}",
        f"sleeve_required_symbol_count: {payload.get('sleeve_required_symbol_count') or 0}",
        f"total_requested_symbol_count: {payload.get('total_requested_symbol_count') or len(payload.get('requested_symbols') or [])}",
        f"requested_symbols_source: {payload.get('requested_symbols_source') or ''}",
        f"requested_symbols: {', '.join(payload.get('requested_symbols') or [])}",
        f"fetched_symbols: {', '.join(payload.get('fetched_symbols') or [])}",
        f"missing_symbols: {', '.join(payload.get('missing_symbols') or [])}",
        f"stale_symbols: {', '.join(payload.get('stale_symbols') or [])}",
        f"mapping_missing_symbols: {', '.join(payload.get('mapping_missing_symbols') or [])}",
        f"provider_failed_symbols: {', '.join(payload.get('provider_failed_symbols') or [])}",
        f"missing_fields: {', '.join(payload.get('missing_fields') or [])}",
        f"stale_fields: {', '.join(payload.get('stale_fields') or [])}",
        f"blocking_missing_symbols: {', '.join(payload.get('blocking_missing_symbols') or [])}",
        f"blocking_stale_symbols: {', '.join(payload.get('blocking_stale_symbols') or [])}",
        f"legacy_optional_missing_symbols: {', '.join(payload.get('legacy_optional_missing_symbols') or [])}",
        f"advisory_only_missing_symbols: {', '.join(payload.get('advisory_only_missing_symbols') or [])}",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
    ]
    explanations = payload.get("missing_symbol_explanations") if isinstance(payload.get("missing_symbol_explanations"), dict) else {}
    if explanations:
        lines.extend(["", "missing_symbol_explanations:"])
        for symbol, row in sorted(explanations.items()):
            lines.append(f"- {symbol}: {row.get('operator_message') or row.get('blocker_reason') or ''}")
    return "\n".join(lines) + "\n"


def render_matrix(payload: dict[str, Any]) -> str:
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=["item", "status", "provider", "market_session_date", "timestamp"])
    writer.writeheader()
    for symbol, row in (payload.get("symbols") or {}).items():
        writer.writerow({"item": symbol, "status": row.get("freshness_status", ""), "provider": row.get("source", ""), "market_session_date": row.get("market_session_date", ""), "timestamp": row.get("data_timestamp_utc", "")})
    breadth = payload.get("breadth") or {}
    writer.writerow({"item": "breadth", "status": breadth.get("freshness_status", ""), "provider": breadth.get("source", ""), "market_session_date": payload.get("market_session_date", ""), "timestamp": ""})
    return out.getvalue()


def _requested_symbols(symbol_map: dict[str, Any]) -> list[str]:
    raw = symbol_map.get("required_symbols") if isinstance(symbol_map.get("required_symbols"), list) else []
    if raw:
        return canonicalize_symbol_list_v1(raw)
    symbols = symbol_map.get("symbols") if isinstance(symbol_map.get("symbols"), dict) else {}
    return canonicalize_symbol_list_v1(list(symbols))


def _contract_market_item_sets(*, root: Path, day_utc: str) -> tuple[list[str], list[str]]:
    _path, contracts = latest_json_v1(root, "aegis_sleeve_input_contracts_v1", day_utc, "sleeve_input_contracts.v1.json")
    required: set[str] = set()
    optional: set[str] = set()
    for contract in contracts.get("contracts", []) if isinstance(contracts.get("contracts"), list) else []:
        if not isinstance(contract, dict):
            continue
        for key, bucket in (("required_inputs", required), ("optional_inputs", optional)):
            for row in contract.get(key, []) if isinstance(contract.get(key), list) else []:
                item_id = str(row.get("data_item_id") or "").strip() if isinstance(row, dict) else ""
                if item_id.startswith(("market.price.", "market.volatility.")):
                    bucket.add(item_id)
    return sorted(required), sorted(optional - required)


def _item_id_for_symbol(symbol: str) -> str:
    return market_data_item_id_v1(symbol)


def _symbol_from_market_item_id(item_id: str) -> str:
    if item_id == "market.volatility.VIX":
        return "VIX"
    if item_id.startswith("market.price."):
        return item_id.rsplit(".", 1)[-1].upper()
    return ""


def _classify_market_gaps(*, requested_symbols: list[str], missing_symbols: list[str], stale_symbols: list[str], required_contract_items: list[str], optional_contract_items: list[str]) -> dict[str, dict[str, Any]]:
    required_symbols = {symbol for item in required_contract_items if (symbol := _symbol_from_market_item_id(item))}
    optional_symbols = {symbol for item in optional_contract_items if (symbol := _symbol_from_market_item_id(item))} - required_symbols
    no_contract_authority = not required_contract_items and not optional_contract_items
    out: dict[str, dict[str, Any]] = {}
    for symbol in sorted(set(missing_symbols) | set(stale_symbols)):
        if symbol in required_symbols:
            classification = "SLEEVE_CRITICAL"
            effect = "BLOCKS"
            reason = "Required by current sleeve input contracts."
        elif symbol in optional_symbols:
            classification = "ADVISORY_ONLY"
            effect = "WARNS"
            reason = "Optional in current sleeve input contracts."
        elif no_contract_authority and symbol in set(requested_symbols):
            classification = "RUNTIME_CRITICAL"
            effect = "BLOCKS"
            reason = "No sleeve contract artifact was available, so requested market symbols fail closed."
        else:
            classification = "LEGACY_OPTIONAL"
            effect = "WARNS"
            reason = "Requested by legacy production-scan market coverage, not by current sleeve-critical input contracts."
        out[symbol] = {
            "symbol": symbol,
            "data_item_id": _item_id_for_symbol(symbol),
            "status": "STALE" if symbol in stale_symbols else "MISSING",
            "classification": classification,
            "readiness_effect": effect,
            "reason": reason,
        }
    return out


def _symbols_by_gap_class(gap_classification: dict[str, dict[str, Any]], *, status: str, classification: str) -> list[str]:
    return sorted(
        symbol
        for symbol, row in gap_classification.items()
        if str(row.get("status") or "") == status and str(row.get("classification") or "") == classification
    )


def _gap_summary(gap_classification: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    summary = {
        "SLEEVE_CRITICAL": [],
        "RUNTIME_CRITICAL": [],
        "ADVISORY_ONLY": [],
        "LEGACY_OPTIONAL": [],
        "FUTURE_REQUIRED": [],
    }
    for symbol, row in sorted(gap_classification.items()):
        classification = str(row.get("classification") or "LEGACY_OPTIONAL")
        summary.setdefault(classification, []).append(symbol)
    return {key: sorted(value) for key, value in summary.items()}


def _classified_status(*, request_status: str, blocking_missing: list[str], blocking_stale: list[str], any_missing: list[str], any_stale: list[str]) -> str:
    request = str(request_status).upper()
    if request in {"FAILED", "TIMEOUT"}:
        return "FAILED"
    if blocking_stale:
        return "STALE"
    if request != "SUCCESS" and not (any_missing or any_stale):
        return "FAILED"
    if blocking_missing:
        return "PARTIAL"
    if any_missing or any_stale:
        return "CURRENT_WITH_WARNINGS"
    return "CURRENT"


def _missing_symbol_explanations(*, missing_symbols: list[str], stale_symbols: list[str], symbol_resolution: dict[str, Any], provider_results: list[dict[str, Any]], config: dict[str, str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for symbol in sorted(set(missing_symbols) | set(stale_symbols)):
        if symbol != "VIX":
            continue
        attempts = [
            {
                "provider": row.get("provider"),
                "request_status": row.get("request_status"),
                "failure_reason": row.get("failure_reason"),
                "missing_symbols": row.get("missing_symbols") if isinstance(row.get("missing_symbols"), list) else [],
                "stale_symbols": row.get("stale_symbols") if isinstance(row.get("stale_symbols"), list) else [],
            }
            for row in provider_results
            if isinstance(row, dict)
        ]
        stale_providers = [
            str(row.get("provider") or "")
            for row in attempts
            if symbol in {str(item).upper() for item in row.get("stale_symbols", [])}
        ]
        missing_providers = [
            str(row.get("provider") or "")
            for row in attempts
            if symbol in {str(item).upper() for item in row.get("missing_symbols", [])}
        ]
        if stale_providers or symbol in stale_symbols:
            symbol_status = "STALE"
            blocker_status = "STALE"
            operator_message = (
                "VIX is present from "
                + ", ".join(sorted(set(stale_providers)))
                + " but is stale under the current-session policy, and fallback provider lookup did not produce current VIX. "
                "C2_VOL_INCOME_DEFINED_RISK_V1 remains blocked and the run remains READY_PARTIAL. "
                "Other ready sleeves may proceed to operator review. No execution is authorized."
            )
        else:
            symbol_status = "MISSING"
            blocker_status = "UNAVAILABLE_EXTERNAL_SOURCE"
            missing_from = ", ".join(sorted(set(missing_providers))) or "configured providers"
            operator_message = (
                f"VIX is missing from {missing_from} canonical/alias paths and fallback provider lookup failed; "
                "C2_VOL_INCOME_DEFINED_RISK_V1 remains blocked and the run remains READY_PARTIAL. "
                "Other ready sleeves may proceed to operator review. No execution is authorized."
            )
        resolution = symbol_resolution.get(symbol, {})
        out[symbol] = {
            "canonical_symbol": "VIX",
            "symbol_status": symbol_status,
            "blocker_status": blocker_status,
            "blocked_sleeves": ["C2_VOL_INCOME_DEFINED_RISK_V1"],
            "blocker_reason": "C2_VOL_INCOME_DEFINED_RISK_V1 requires canonical VIX input.",
            "primary_provider": config.get("primary") or "",
            "fallback_provider": config.get("fallback") or "",
            "accepted_aliases": resolution.get("aliases") if isinstance(resolution.get("aliases"), list) else ["VIX", "^VIX", "$VIX", "vix", "VIXCLS"],
            "provider_mappings_attempted": resolution.get("provider_mappings") if isinstance(resolution.get("provider_mappings"), dict) else {},
            "provider_attempts": attempts,
            "operator_message": operator_message,
            "fail_closed": True,
            "synthetic_data_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        }
    return out


def _status(*, request_status: str, missing_fields: list[str], stale_fields: list[str]) -> str:
    if stale_fields:
        return "STALE"
    if str(request_status).upper() != "SUCCESS":
        return "FAILED"
    if missing_fields:
        return "PARTIAL"
    return "CURRENT"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="refresh_aegis_market_data_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--market-data-mode", choices=[INTRADAY_OPERATIONAL, FINAL_EOD_CERTIFIED], default=INTRADAY_OPERATIONAL)
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbol override for scoped governed refreshes.")
    args = parser.parse_args(argv)
    os.environ["AEGIS_MARKET_DATA_MODE"] = str(args.market_data_mode)
    context = run_context_from_env_v1("refresh-market-data", default_allow_self_heal=False, default_allow_projection_rebuild=False)
    allowed, guard_reason = step_allowed_v1(context, "market_data_refresh")
    if not allowed:
        symbol_map = build_symbol_map_v1(repo_root=REPO_ROOT, day_utc=str(args.day_utc))
        payload = {
            "schema_id": "aegis_market_data",
            "schema_version": "v1",
            "artifact_id": "aegis_market_data_v1",
            "day_utc": str(args.day_utc),
            "market_session_date": str(args.day_utc),
            "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "status": "FAILED",
            "market_data_mode": str(args.market_data_mode),
            "source": "SKIPPED_BY_RUN_CONTEXT",
            "provider_config": {"configured": False},
            "runtime_universe_mode": symbol_map.get("runtime_universe_mode") or "",
            "production_scan_dataset_id": symbol_map.get("production_scan_dataset_id") or "",
            "dataset_snapshot_id": symbol_map.get("dataset_snapshot_id") or symbol_map.get("production_scan_dataset_id") or "",
            "production_scan_universe_count": int(symbol_map.get("production_scan_universe_count") or 0),
            "sleeve_required_symbol_count": int(symbol_map.get("sleeve_required_symbol_count") or 0),
            "total_requested_symbol_count": len(_requested_symbols(symbol_map)),
            "requested_symbols_source": symbol_map.get("requested_symbols_source") or "",
            "minimum_viable_runtime_reference_removed": bool(symbol_map.get("minimum_viable_runtime_reference_removed")),
            "provider_results": [],
            "normalized_records": [],
            "symbols": {},
            "breadth": {"freshness_status": "MISSING", "source": None},
            "missing_fields": [],
            "stale_fields": [],
            "requested_symbols": _requested_symbols(symbol_map),
            "fetched_symbols": [],
            "missing_symbols": _requested_symbols(symbol_map),
            "stale_symbols": [],
            "symbol_resolution": {},
            "missing_symbol_explanations": {},
            "mapping_missing_symbols": [],
            "provider_failed_symbols": _requested_symbols(symbol_map),
            "usable_for_candidate_generation": False,
            "failure_reason": guard_reason,
            "timeout_diagnostic": {"diagnostic_type": "market_data_refresh_skipped", "reason": guard_reason, "run_context": context.to_dict()},
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        }
    else:
        context = child_run_context_v1(context, "refresh-market-data", allow_self_heal=False, allow_projection_rebuild=False, add_step="market_data_refresh")
        symbol_override = None
        if str(args.symbols or "").strip():
            symbol_override = build_symbol_map_v1(repo_root=REPO_ROOT, day_utc=str(args.day_utc))
            override_symbols = canonicalize_symbol_list_v1(str(args.symbols).replace(";", ",").split(","))
            symbol_override["required_symbols"] = override_symbols
            symbol_override["requested_symbols"] = override_symbols
            symbol_override["runtime_symbols"] = override_symbols
            symbol_override["total_requested_symbol_count"] = len(override_symbols)
            symbols_map = symbol_override.get("symbols") if isinstance(symbol_override.get("symbols"), dict) else {}
            missing = []
            for symbol in override_symbols:
                try:
                    symbols_map[symbol] = symbol_map_entry_for_symbol_v1(symbol)
                except Exception:
                    missing.append(symbol)
            symbol_override["symbols"] = symbols_map
            symbol_override["mapping_missing_symbols"] = missing
            symbol_override["requested_symbols_source"] = "CLI_SYMBOL_OVERRIDE"
        payload = build_market_data_report_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), symbol_map_override=symbol_override, timeout_diagnostic={"run_context": context.to_dict()}, market_data_mode=str(args.market_data_mode))
    paths = write_market_data_report_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({**paths, "status": payload["status"], "usable_for_candidate_generation": payload["usable_for_candidate_generation"], "failure_reason": payload.get("failure_reason"), "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
