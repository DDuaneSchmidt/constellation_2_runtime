from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from ops.aegis.operator_state.current_operator_truth_resolver_v1 import resolve_current_operator_truth_v1
from ops.aegis.candidate_intent_plane_v1 import build_and_write_candidate_intent_plane_v1
from ops.aegis.operator_state.runtime_timeline_projection_v1 import build_runtime_timeline_projection_v1
from ops.aegis.operator_state.trade_candidate_projection_v1 import (
    build_trade_candidate_projection_v1,
    manual_capture_view_from_projection_v1,
    write_trade_candidate_projection_v1,
)
from ops.aegis.trade_lifecycle.paper_trade_construction_v1 import (
    build_and_write_paper_trade_construction_v1,
    paper_trade_construction_view_v1,
)
from ops.aegis.trade_lifecycle.trade_lifecycle_case_v1 import build_and_write_trade_lifecycle_case_v1
from ops.aegis.trade_lifecycle.trade_case_projection_v1 import trade_case_projection_v1
from ops.aegis.submit_boundary_precheck_v1 import build_and_write_submit_boundary_precheck_v1
from ops.aegis.trade_ticket_lineage_v1 import build_trade_ticket_lineage_v1, write_ticket_evidence_set_v1, write_trade_ticket_lineage_v1
from ops.aegis.trade_lifecycle.captured_ticket_history_v1 import (
    build_captured_ticket_history_v1,
    captured_ticket_projection_v1,
    write_captured_ticket_history_v1,
    write_captured_ticket_projection_v1,
)
from ops.aegis.trade_lifecycle.trade_ticket_projection_v1 import (
    trade_ticket_projection_v1,
    write_trade_ticket_projection_v1,
)
from ops.aegis.thesis_graph.thesis_graph_v1 import build_thesis_graph_projection_v1

REPORT_FAMILY = "operator_state_snapshot_v1"
REPORT_FILENAME = "operator_state_snapshot.v1.json"
ALT_RUNTIME_TRUTH_ROOT = Path("/home/node/constellation_2_runtime/constellation_2/runtime/truth")
SCHEMA_ID = "operator_state_snapshot"
SCHEMA_VERSION = "v1"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_read_json(path: Path, *, artifact_id: str, errors: list[dict[str, Any]], required: bool = False) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        errors.append({"code": f"{artifact_id.upper()}_MISSING", "message": f"Artifact missing: {path}", "source_path": str(path), "recoverable": True, "required": required})
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        errors.append({"code": f"{artifact_id.upper()}_MALFORMED", "message": f"Artifact JSON is malformed: {exc}", "source_path": str(path), "recoverable": True, "required": required})
        return {}
    except Exception as exc:  # noqa: BLE001
        errors.append({"code": f"{artifact_id.upper()}_READ_FAILED", "message": f"Artifact read failed: {type(exc).__name__}: {exc}", "source_path": str(path), "recoverable": True, "required": required})
        return {}
    if not isinstance(payload, dict):
        errors.append({"code": f"{artifact_id.upper()}_TOP_LEVEL_NOT_OBJECT", "message": "Artifact top level is not an object.", "source_path": str(path), "recoverable": True, "required": required})
        return {}
    return payload


def _source_ref(path: Path, artifact_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    exists = path.exists() and path.is_file()
    return {
        "artifact_id": artifact_id,
        "path": str(path),
        "exists": exists,
        "sha256": _sha256_file(path) if exists else "",
        "schema_id": str((payload or {}).get("schema_id") or ""),
    }


def _resolve_effective_truth_root(root: Path, day_utc: str) -> Path:
    root = Path(root).expanduser().resolve()
    if str(root).startswith("/tmp/"):
        return root
    if _candidate_diagnostics_path(root, day_utc).exists() or _portfolio_gate_candidate_report_path(root, day_utc).exists() or _latest_conversion_path(root, day_utc) is not None:
        return root
    if ALT_RUNTIME_TRUTH_ROOT.exists() and (
        _candidate_diagnostics_path(ALT_RUNTIME_TRUTH_ROOT, day_utc).exists()
        or _portfolio_gate_candidate_report_path(ALT_RUNTIME_TRUTH_ROOT, day_utc).exists()
        or _latest_conversion_path(ALT_RUNTIME_TRUTH_ROOT, day_utc) is not None
    ):
        return ALT_RUNTIME_TRUTH_ROOT.resolve()
    return root


def _references_external_tmp_path(value: Any, *, allowed_root: Path) -> bool:
    if isinstance(value, dict):
        return any(_references_external_tmp_path(item, allowed_root=allowed_root) for item in value.values())
    if isinstance(value, list):
        return any(_references_external_tmp_path(item, allowed_root=allowed_root) for item in value)
    if isinstance(value, str):
        text = value.strip()
        if not text.startswith("/tmp/"):
            return False
        try:
            return not Path(text).expanduser().resolve().is_relative_to(allowed_root)
        except Exception:
            return True
    return False


def operator_state_snapshot_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def _selected_pointer_path(root: Path) -> Path:
    return root / "pointers" / "selected_intent_pointer.v1.json"


def _candidate_diagnostics_path(root: Path, day_utc: str) -> Path:
    return root / "reports" / "paper_intent_candidate_diagnostics_v1" / day_utc / "paper_intent_candidate_diagnostics.v1.json"


def _portfolio_gate_candidate_report_path(root: Path, day_utc: str) -> Path:
    return root / "reports" / "portfolio_gate_candidate_report_v1" / day_utc / "portfolio_gate_candidate_report.v1.json"


def _latest_conversion_path(root: Path, day_utc: str) -> Path | None:
    base = root / "reports" / "exposure_intent_paper_submission_package_v1" / day_utc
    if not base.exists() or not base.is_dir():
        return None
    candidates = sorted(base.glob("*/exposure_intent_paper_submission_package.v1.json"), key=lambda p: (p.stat().st_mtime_ns, str(p)))
    return candidates[-1].resolve() if candidates else None


def _latest_matching_conversion_path(root: Path, day_utc: str, selected_id: str) -> tuple[Path | None, bool]:
    base = root / "reports" / "exposure_intent_paper_submission_package_v1" / day_utc
    if not base.exists() or not base.is_dir():
        return None, False
    candidates = sorted(base.glob("*/exposure_intent_paper_submission_package.v1.json"), key=lambda p: (p.stat().st_mtime_ns, str(p)), reverse=True)
    if not selected_id:
        return (candidates[0].resolve() if candidates else None), False
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(payload.get("exposure_intent_id") or payload.get("selected_exposure_intent_id") or "") == selected_id:
            return path.resolve(), False
    return (candidates[0].resolve() if candidates else None), bool(candidates)


def _market_manifest_path(root: Path) -> Path:
    return root / "market_data_snapshot_v1" / "dataset_manifest.json"


def _research_status_path(root: Path, day_utc: str) -> Path:
    return root / "reports" / "aegis_research_lab_execution_loop_v1" / day_utc / "research_lab_execution_loop.v1.json"



def _thesis_graph_projection(root: Path, day_utc: str, errors: list[dict[str, Any]]) -> dict[str, Any]:
    try:
        return build_thesis_graph_projection_v1(truth_root=root, day_utc=day_utc)
    except Exception as exc:  # noqa: BLE001
        errors.append({
            "code": "THESIS_GRAPH_PROJECTION_UNAVAILABLE",
            "message": f"Thesis graph projection unavailable: {type(exc).__name__}: {exc}",
            "recoverable": True,
            "required": False,
        })
        return {"schema_id": "aegis_thesis_graph_projection", "thesis_count": 0, "thesis_cards": [], "mission_control_thesis_summary": {}, "runtime_timeline_events": []}


def _latest_captured_ticket_projection(root: Path, before_or_on_day: str) -> dict[str, Any]:
    base = root / "reports" / "captured_ticket_projection_v1"
    if not base.exists() or not base.is_dir():
        return {}
    paths = []
    for path in base.glob("*/*/captured_ticket_projection.v1.json"):
        day = path.parts[-3] if len(path.parts) >= 3 else ""
        if day and day <= before_or_on_day:
            paths.append(path)
    for path in sorted(paths, key=lambda item: (item.parts[-3], item.stat().st_mtime_ns, str(item)), reverse=True):
        errors: list[dict[str, Any]] = []
        payload = _safe_read_json(path, artifact_id="captured_ticket_projection", errors=errors, required=False)
        if payload:
            payload = {**payload, "artifact_path": str(path)}
            return payload
    return {}


def _runtime_truth_kernel_path(root: Path, day_utc: str) -> Path:
    return root / "reports" / "aegis_runtime_truth_kernel_v1" / day_utc / "runtime_truth_kernel.v1.json"


def _legacy_module_runtime_quarantine(root: Path, day_utc: str, trade_ticket_projection: dict[str, Any]) -> dict[str, Any]:
    runtime_path = _runtime_truth_kernel_path(root, day_utc)
    runtime_errors: list[dict[str, Any]] = []
    runtime = _safe_read_json(runtime_path, artifact_id="aegis_runtime_truth_kernel", errors=runtime_errors, required=False)
    ticket_manual_ready = bool(trade_ticket_projection.get("manual_capture_ready") is True)
    runtime_manual_allowed = bool(runtime.get("manual_trade_capture_allowed") is True)
    blocked_capabilities = [str(code or "") for code in runtime.get("blocked_capabilities", [])] if isinstance(runtime.get("blocked_capabilities"), list) else []
    conflict = bool(ticket_manual_ready and not runtime_manual_allowed and "MANUAL_TRADE_CAPTURE_ALLOWED" in blocked_capabilities)
    quarantined_items = []
    if conflict:
        quarantined_items.append(
            {
                "item_id": "legacy_runtime_truth_manual_capture_permission",
                "source_path": str(runtime_path),
                "status": "QUARANTINED_INACTIVE_FOR_TRADE_LIFECYCLE_V1",
                "reason": "Runtime truth kernel manual_capture_permission is a legacy advisory-runtime gate and may not override TradeLifecycleCase_v1/ReadinessDomainEvaluation_v1 manual-capture readiness.",
                "legacy_value": False,
                "active_authority": "trade_ticket_projection_v1.manual_capture_ready",
            }
        )
    return {
        "schema_id": "legacy_module_runtime_quarantine",
        "schema_version": "v1",
        "day_utc": day_utc,
        "modules": {
            "bond_sleeve": {
                "status": "INACTIVE_FOR_CORE_AEGIS_RUNTIME",
                "classification": "stale_deprecated_or_test_only",
                "may_block_aegis_readiness": False,
                "may_block_paper_cycle": False,
                "may_block_trade_lifecycle_case": False,
                "may_block_manual_capture_readiness": False,
            },
            "advisory": {
                "status": "INACTIVE_FOR_TRADE_LIFECYCLE_READINESS",
                "classification": "advisory_surfaces_are_read_only_or_legacy_mode_labels",
                "may_block_aegis_readiness": False,
                "may_block_paper_cycle": False,
                "may_block_trade_lifecycle_case": False,
                "may_block_manual_capture_readiness": False,
            },
            "tax": {
                "status": "INACTIVE_FOR_CORE_AEGIS_RUNTIME",
                "classification": "separate_planning_module_not_trade_lifecycle_input",
                "may_block_aegis_readiness": False,
                "may_block_paper_cycle": False,
                "may_block_trade_lifecycle_case": False,
                "may_block_manual_capture_readiness": False,
            },
        },
        "active_manual_capture_authority": "trade_ticket_projection_v1",
        "active_trade_lifecycle_authority": "trade_lifecycle_case_v1",
        "runtime_truth_kernel_path": str(runtime_path),
        "runtime_truth_kernel_present": bool(runtime),
        "runtime_truth_manual_capture_allowed": runtime_manual_allowed if runtime else None,
        "trade_ticket_manual_capture_ready": ticket_manual_ready,
        "hidden_blocker_risk_detected": conflict,
        "hidden_blocker_risk_quarantined": conflict,
        "quarantined_items": quarantined_items,
        "read_errors": runtime_errors,
    }


def _first_evidence_path(row: dict[str, Any]) -> str:
    paths = row.get("evidence_paths") if isinstance(row.get("evidence_paths"), list) else []
    for path in paths:
        text = str(path or "").strip()
        if text:
            return text
    return str(row.get("intent_path") or "").strip()


def _diagnostics_from_portfolio_gate_report(report: dict[str, Any]) -> dict[str, Any]:
    rows = report.get("candidate_rows") if isinstance(report.get("candidate_rows"), list) else []
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        selected = str(row.get("selected_by_gate") or "").upper() == "YES" or str(row.get("portfolio_gate_decision") or "").upper() == "ALLOW"
        reason_codes = [str(code or "") for code in row.get("reason_codes", [])] if isinstance(row.get("reason_codes"), list) else []
        suppression_code = str(row.get("suppression_code") or "").strip()
        blocker_code = "" if selected else "PORTFOLIO_GATE_SUPPRESSED"
        normalized_rows.append(
            {
                **row,
                "candidate_id": str(row.get("candidate_id") or row.get("intent_candidate_id") or ""),
                "symbol": str(row.get("symbol") or "").upper(),
                "sleeve_id": str(row.get("sleeve_id") or ""),
                "engine_id": str(row.get("engine_id") or row.get("sleeve_id") or ""),
                "proposed_direction": str(row.get("proposed_direction") or row.get("direction") or ""),
                "score": row.get("score"),
                "confidence": row.get("confidence"),
                "rank": row.get("rank_before_arbitration"),
                "blocker_code": blocker_code,
                "blocker_message": "" if selected else str(row.get("suppression_reason") or "Suppressed by portfolio activation gate policy."),
                "portfolio_gate_decision": "ALLOW" if selected else "SUPPRESS",
                "portfolio_gate_reason_codes": reason_codes or ([suppression_code.upper()] if suppression_code else []),
                "final_disposition": "SELECTED_BY_ARBITRATION_NO_PAPER_TRADE_INTENT_CREATED" if selected else "SUPPRESSED_BY_PORTFOLIO_GATE",
                "intent_path": _first_evidence_path(row),
                "source_report_family": "portfolio_gate_candidate_report_v1",
            }
        )
    selected_id = str(report.get("selected_candidate_id") or "")
    return {
        "schema_id": "paper_intent_candidate_diagnostics",
        "schema_version": "v1_from_portfolio_gate_candidate_report",
        "day_utc": str(report.get("day_utc") or ""),
        "selected_exposure_intent_id": selected_id,
        "candidates_found": int(report.get("candidate_count") or len(normalized_rows)),
        "selected_count": int(report.get("selected_count") or (1 if selected_id else 0)),
        "suppressed_count": int(report.get("suppressed_count") or len([row for row in normalized_rows if row.get("blocker_code") == "PORTFOLIO_GATE_SUPPRESSED"])),
        "suppression_code_counts": report.get("suppression_code_counts") if isinstance(report.get("suppression_code_counts"), dict) else {},
        "candidate_rows": normalized_rows,
        "paper_trade_intent_created": False,
        "source_report_family": "portfolio_gate_candidate_report_v1",
        "source_report_path": str(report.get("artifact_path") or ""),
    }



def _diagnostics_from_intent_arbitration(arbitration: dict[str, Any]) -> dict[str, Any]:
    selected = arbitration.get("selected_intent") if isinstance(arbitration.get("selected_intent"), dict) else {}
    selected_id = str(selected.get("intent_id") or "")
    raw_rows = arbitration.get("raw_candidate_intents") if isinstance(arbitration.get("raw_candidate_intents"), list) else []
    rejected_rows = arbitration.get("rejected_or_filtered_intents") if isinstance(arbitration.get("rejected_or_filtered_intents"), list) else []
    by_id: dict[str, dict[str, Any]] = {}
    for row in raw_rows + rejected_rows + ([selected] if selected else []):
        if not isinstance(row, dict):
            continue
        candidate_id = str(row.get("intent_id") or row.get("candidate_id") or row.get("raw_intent_id") or "")
        if not candidate_id:
            continue
        by_id[candidate_id] = {**by_id.get(candidate_id, {}), **row}
    normalized_rows: list[dict[str, Any]] = []
    for candidate_id, row in sorted(by_id.items()):
        is_selected = candidate_id == selected_id
        reason_codes = row.get("portfolio_gate_reason_codes") if isinstance(row.get("portfolio_gate_reason_codes"), list) else row.get("reason_codes") if isinstance(row.get("reason_codes"), list) else []
        rejection_reason = str(row.get("rejection_reason") or "")
        decision = str(row.get("portfolio_gate_decision") or ("ALLOW" if is_selected else "SUPPRESS")).upper()
        normalized_rows.append(
            {
                **row,
                "candidate_id": candidate_id,
                "symbol": str(row.get("symbol") or row.get("raw_intent_symbol") or "").upper(),
                "sleeve_id": str(row.get("sleeve_id") or row.get("engine_id") or ""),
                "engine_id": str(row.get("engine_id") or row.get("sleeve_id") or ""),
                "proposed_direction": str(row.get("proposed_direction") or row.get("direction") or ""),
                "score": row.get("portfolio_score_total") if row.get("portfolio_score_total") is not None else row.get("score"),
                "confidence": row.get("confidence"),
                "rank": row.get("portfolio_score_rank") or row.get("rank"),
                "blocker_code": "" if is_selected else "PORTFOLIO_GATE_SUPPRESSED",
                "blocker_message": "" if is_selected else (rejection_reason or "Suppressed by portfolio activation/scoring policy."),
                "portfolio_gate_decision": "ALLOW" if is_selected else (decision if decision else "SUPPRESS"),
                "portfolio_gate_reason_codes": [str(code) for code in reason_codes],
                "final_disposition": "SELECTED_BY_ARBITRATION" if is_selected else (rejection_reason or "SUPPRESSED_BY_PORTFOLIO_GATE"),
                "intent_path": str(row.get("intent_path") or row.get("raw_intent_path") or ""),
                "source_report_family": "intent_arbitration_v1",
            }
        )
    return {
        "schema_id": "paper_intent_candidate_diagnostics",
        "schema_version": "v1_from_intent_arbitration",
        "day_utc": str(arbitration.get("day_utc") or ""),
        "selected_exposure_intent_id": selected_id,
        "candidates_found": len(normalized_rows),
        "selected_count": 1 if selected_id else 0,
        "suppressed_count": len([row for row in normalized_rows if row.get("candidate_id") != selected_id]),
        "suppression_code_counts": {},
        "candidate_rows": normalized_rows,
        "paper_trade_intent_created": False,
        "source_report_family": "intent_arbitration_v1",
        "source_report_path": str(arbitration.get("artifact_path") or ""),
    }

def _regime_bucket_ranking_path(root: Path, day_utc: str) -> Path:
    return root / "reports" / "regime_bucket_candidate_ranking_v1" / day_utc / "regime_bucket_candidate_ranking.v1.json"


def _ranking_by_candidate(ranking: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    buckets = ranking.get("buckets") if isinstance(ranking.get("buckets"), list) else []
    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        candidates = bucket.get("candidates") if isinstance(bucket.get("candidates"), list) else []
        for row in candidates:
            if not isinstance(row, dict):
                continue
            candidate_id = str(row.get("candidate_id") or row.get("intent_id") or "").strip()
            if candidate_id:
                out[candidate_id] = row
    return out


def _first_selected_row(diagnostics: dict[str, Any], selected_id: str) -> dict[str, Any]:
    rows = diagnostics.get("candidate_rows") if isinstance(diagnostics.get("candidate_rows"), list) else []
    for row in rows:
        if isinstance(row, dict) and str(row.get("candidate_id") or row.get("intent_candidate_id") or "") == selected_id:
            return row
    return {}


def _selected_from_pointer(pointer: dict[str, Any]) -> dict[str, Any]:
    return pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}


def _selected_from_gate_report(report: dict[str, Any]) -> dict[str, Any]:
    selected_id = str(report.get("selected_candidate_id") or "").strip()
    rows = report.get("candidate_rows") if isinstance(report.get("candidate_rows"), list) else []
    selected_row: dict[str, Any] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_id = str(row.get("candidate_id") or row.get("intent_candidate_id") or "").strip()
        selected_by_gate = str(row.get("selected_by_gate") or "").upper() == "YES" or str(row.get("portfolio_gate_decision") or "").upper() == "ALLOW"
        if (selected_id and row_id == selected_id) or (not selected_id and selected_by_gate):
            selected_row = row
            selected_id = row_id
            break
    if not selected_id:
        return {}
    return {
        "intent_id": selected_id,
        "intent_path": _first_evidence_path(selected_row),
        "engine_id": str(selected_row.get("engine_id") or selected_row.get("sleeve_id") or ""),
        "sleeve_id": str(selected_row.get("sleeve_id") or selected_row.get("engine_id") or ""),
        "symbol": str(selected_row.get("symbol") or "").upper(),
        "selection_reason": "PORTFOLIO_GATE_SELECTED",
        "arbitration_reason": "PORTFOLIO_GATE_SELECTED",
        "portfolio_score_total": selected_row.get("score"),
        "portfolio_score_components": {"signal_strength": selected_row.get("confidence")},
        "source_day": str(report.get("day_utc") or ""),
        "source_report_family": "portfolio_gate_candidate_report_v1",
        "source_report_path": str(report.get("artifact_path") or ""),
    }


def _direction(row: dict[str, Any], intent: dict[str, Any] | None = None) -> str:
    for key in ("proposed_direction", "direction", "candidate_direction"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    exposure = str((intent or {}).get("exposure_type") or "").upper()
    if exposure == "LONG_EQUITY":
        return "LONG"
    return ""


def _selected_intent_payload(selected: dict[str, Any], errors: list[dict[str, Any]]) -> tuple[dict[str, Any], str]:
    path_text = str(selected.get("intent_path") or "").strip()
    if not path_text:
        return {}, ""
    path = Path(path_text).expanduser().resolve()
    return _safe_read_json(path, artifact_id="selected_exposure_intent", errors=errors, required=False), str(path)


def _json_scalar(value: Any) -> Any:
    if isinstance(value, float):
        return str(value)
    return value


def _json_safe(value: Any) -> Any:
    if isinstance(value, float):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _manual_capture_candidate(
    *,
    selected: dict[str, Any],
    selected_row: dict[str, Any],
    selected_intent: dict[str, Any],
    conversion: dict[str, Any],
    diagnostics_path: str,
    pointer_path: str,
    selected_intent_path: str,
    source_day: str,
    latest_report_day: str,
    source_run_id: str,
    source_mismatch_warning: str = "",
    suppressed_count: int = 0,
) -> dict[str, Any]:
    selected_id = str(selected.get("intent_id") or selected_row.get("candidate_id") or "").strip()
    symbol = str(selected.get("symbol") or selected_row.get("symbol") or ((selected_intent.get("underlying") or {}) if isinstance(selected_intent.get("underlying"), dict) else {}).get("symbol") or "").strip().upper()
    engine_id = str(selected.get("engine_id") or selected_row.get("engine_id") or ((selected_intent.get("engine") or {}) if isinstance(selected_intent.get("engine"), dict) else {}).get("engine_id") or "").strip()
    sleeve_id = str(selected.get("sleeve_id") or selected_row.get("sleeve_id") or engine_id).strip()
    market = conversion.get("market_data_status") if isinstance(conversion.get("market_data_status"), dict) else {}
    blocker_code = str(conversion.get("blocker_code") or selected_row.get("blocker_code") or "").strip()
    blocker_message = str(conversion.get("blocker_message") or selected_row.get("blocker_message") or "").strip()
    paper_created = bool(conversion.get("paper_trade_intent_created") is True)
    stale = str(market.get("status") or "").upper() in {"STALE_OR_MISSING", "STALE_OR_INVALID"} or blocker_code == "STALE_MARKET_DATA_BLOCKS_CONVERSION"
    blocked = bool(blocker_code)
    if stale or blocked:
        label = "Review only — blocked"
    elif paper_created:
        label = "Manual paper capture candidate — no broker execution"
    elif selected_id:
        label = "Manual paper capture candidate — no broker execution"
    else:
        label = "No manual capture candidate"
    return {
        "schema_id": "manual_capture_candidate",
        "schema_version": "v1",
        "candidate_available": bool(selected_id),
        "source_day": source_day,
        "latest_report_day": latest_report_day,
        "source_run_id": source_run_id,
        "stale_source": bool(source_day and latest_report_day and source_day != latest_report_day),
        "source_mismatch_warning": source_mismatch_warning,
        "selected_exposure_intent_id": selected_id,
        "symbol": symbol,
        "sleeve_id": sleeve_id,
        "engine_id": engine_id,
        "direction": _direction(selected_row, selected_intent),
        "score": _json_scalar(selected_row.get("score") if selected_row else selected.get("portfolio_score_total")),
        "confidence": _json_scalar(selected_row.get("confidence") if selected_row else selected.get("portfolio_score_components", {}).get("signal_strength") if isinstance(selected.get("portfolio_score_components"), dict) else None),
        "selected_by_arbitration": bool(selected_id and (selected.get("selection_reason") or selected.get("arbitration_reason") or selected_row.get("final_disposition"))),
        "selected_reason": str(selected.get("selection_reason") or selected.get("arbitration_reason") or ""),
        "paper_trade_intent_created": paper_created,
        "paper_intent_created": paper_created,
        "paper_trade_intent_id": conversion.get("paper_trade_intent_id"),
        "conversion_status": str(conversion.get("status") or ("BLOCKED" if blocker_code else "UNKNOWN")),
        "blocker_code": blocker_code,
        "blocker_message": blocker_message,
        "latest_market_session": str(market.get("observed_session") or ""),
        "required_market_session": str(market.get("expected_session") or conversion.get("day_utc") or ""),
        "stale_market_data": bool(stale),
        "suppressed_count": suppressed_count,
        "source_artifact_paths": [path for path in [pointer_path, diagnostics_path, selected_intent_path, str(conversion.get("artifact_path") or "")] if path],
        "operator_action_label": label,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "order_routing_allowed": False,
    }


def _suppression_code(row: dict[str, Any]) -> str:
    reasons = row.get("portfolio_gate_reason_codes") if isinstance(row.get("portfolio_gate_reason_codes"), list) else []
    for reason in reasons:
        text = str(reason or "").strip()
        if text.endswith("SUPPRESSED"):
            return text.lower()
    return str(row.get("blocker_code") or "suppressed").strip().lower()


def _suppressed_watchlist(diagnostics: dict[str, Any], selected_id: str, ranking: dict[str, Any] | None = None) -> dict[str, Any]:
    rows = diagnostics.get("candidate_rows") if isinstance(diagnostics.get("candidate_rows"), list) else []
    ranking_rows = _ranking_by_candidate(ranking or {})
    suppressed: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("blocker_code") or "") != "PORTFOLIO_GATE_SUPPRESSED" and str(row.get("portfolio_gate_decision") or "").upper() != "SUPPRESS":
            continue
        code = _suppression_code(row)
        candidate_id = str(row.get("candidate_id") or row.get("intent_candidate_id") or "")
        rank_row = ranking_rows.get(candidate_id, {})
        counts[code] += 1
        suppressed.append(
            {
                "candidate_id": candidate_id,
                "symbol": str(row.get("symbol") or "").upper(),
                "sleeve_id": str(row.get("sleeve_id") or ""),
                "engine_id": str(row.get("engine_id") or ""),
                "direction": _direction(row),
                "score": _json_scalar(row.get("score")),
                "confidence": _json_scalar(row.get("confidence")),
                "suppression_code": code,
                "suppression_reason": str(row.get("blocker_message") or ""),
                "competing_selected_candidate_id": selected_id,
                "pre_suppression_score_total": _json_scalar(rank_row.get("pre_suppression_score_total")),
                "pre_suppression_rank_in_bucket": rank_row.get("pre_suppression_rank_in_bucket"),
                "pre_suppression_score_components": _json_safe(rank_row.get("pre_suppression_score_components") if isinstance(rank_row.get("pre_suppression_score_components"), dict) else {}),
                "selected_by_gate": bool(rank_row.get("selected_by_gate")),
                "ranking_metric_available": bool(rank_row),
                "watchlist_only": True,
                "source_artifact_path": str(row.get("intent_path") or rank_row.get("raw_intent_path") or ""),
            }
        )
    suppressed = sorted(suppressed, key=lambda row: (int(row.get("pre_suppression_rank_in_bucket") or 999999), str(row.get("symbol") or ""), str(row.get("candidate_id") or "")))
    return {
        "schema_id": "suppressed_candidate_watchlist",
        "schema_version": "v1",
        "suppressed_count": len(suppressed),
        "suppression_code_counts": dict(sorted(counts.items())),
        "competing_selected_candidate_id": selected_id,
        "ranking_report_path": str((ranking or {}).get("artifact_path") or ""),
        "ranking_order_dependency_detected": bool((ranking or {}).get("order_dependency_detected")),
        "ranking_top_candidate_id": str((ranking or {}).get("top_ranked_candidate_id") or ""),
        "ranking_selected_candidate_rank": (ranking or {}).get("selected_candidate_rank"),
        "candidates": suppressed,
    }


def _paper_status(conversion: dict[str, Any]) -> dict[str, Any]:
    return {
        "paper_trade_intent_created": bool(conversion.get("paper_trade_intent_created") is True),
        "paper_trade_intent_id": conversion.get("paper_trade_intent_id"),
        "paper_submit_attempted": bool(conversion.get("paper_submit_attempted") is True),
        "paper_submit_rc": conversion.get("paper_submit_rc"),
        "execution_package_created": bool(conversion.get("execution_package_created") is True),
        "submission_record_created": bool(conversion.get("submission_record_created") is True),
        "read_only_projection": True,
    }


def _runtime_status(errors: list[dict[str, Any]], source_artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "status": "DEGRADED" if errors else "READY",
        "environment": "PRODUCTION",
        "governance_mode": "Governed",
        "backend_reachable": True,
        "source_artifact_count": len([row for row in source_artifacts if row.get("exists")]),
        "broker_execution_allowed": False,
        "live_trading_allowed": False,
    }


def _blockers(manual: dict[str, Any], errors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if manual.get("blocker_code"):
        out.append(
            {
                "code": manual.get("blocker_code"),
                "message": manual.get("blocker_message"),
                "symbol": manual.get("symbol"),
                "sleeve_id": manual.get("sleeve_id"),
                "severity": "HIGH" if manual.get("stale_market_data") else "MEDIUM",
                "recoverable": True,
            }
        )
    for err in errors:
        if err.get("required"):
            out.append({"code": err.get("code"), "message": err.get("message"), "severity": "MEDIUM", "recoverable": True})
    return out


def _research_summary(research_payload: dict[str, Any], errors: list[dict[str, Any]]) -> dict[str, Any]:
    if not research_payload:
        return {"status": "DEGRADED", "summary": "Research Lab projection unavailable or not generated.", "counts": {}, "read_only": True}
    return {
        "status": str(research_payload.get("status") or "AVAILABLE"),
        "summary": str(research_payload.get("summary") or research_payload.get("operator_summary") or "Research Lab projection available."),
        "counts": research_payload.get("counts") if isinstance(research_payload.get("counts"), dict) else {},
        "read_only": True,
    }


def build_operator_state_snapshot_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    current_truth = resolve_current_operator_truth_v1(truth_root=truth_root, day_utc=str(day_utc), generated_at_utc=generated_at_utc)
    root = Path(str(current_truth.get("truth_root") or truth_root)).expanduser().resolve()
    day = str(current_truth.get("source_day") or day_utc)
    current_day_status = current_truth.get("current_day_status") if isinstance(current_truth.get("current_day_status"), dict) else {}
    market_data_state = str(current_day_status.get("market_data_state") or current_truth.get("market_data_state") or "MARKET_DATA_PENDING")
    candidate_certification_state = str(current_day_status.get("candidate_certification_state") or current_truth.get("candidate_certification_state") or "CANDIDATES_PROVISIONAL")
    execution_eligibility_state = str(current_day_status.get("execution_eligibility_state") or current_truth.get("execution_eligibility_state") or ("EXECUTION_ELIGIBLE_CERTIFIED_ONLY" if candidate_certification_state == "CANDIDATES_CERTIFIED" else "EXECUTION_LOCKED_NON_CERTIFIED"))
    operator_state_semantics = {
        "market_data_state": market_data_state,
        "candidate_certification_state": candidate_certification_state,
        "execution_eligibility_state": execution_eligibility_state,
    }
    historical_fallback = current_truth.get("historical_fallback") if isinstance(current_truth.get("historical_fallback"), dict) else {}
    errors: list[dict[str, Any]] = []
    errors.extend(current_truth.get("errors") if isinstance(current_truth.get("errors"), list) else [])
    generated = generated_at_utc or _now_iso()
    candidate_intent_plane, candidate_intent_plane_path = build_and_write_candidate_intent_plane_v1(
        truth_root=root,
        day_utc=day,
        now_utc=generated,
        write_histories=True,
    )
    intent_rows = candidate_intent_plane.get("intent_snapshots") if isinstance(candidate_intent_plane.get("intent_snapshots"), list) else []
    intent_by_id = {str(row.get("intent_id") or ""): row for row in intent_rows if isinstance(row, dict) and str(row.get("intent_id") or "")}
    if isinstance(current_day_status.get("candidate_rows"), list):
        merged_rows = []
        for candidate_row in current_day_status.get("candidate_rows", []):
            if not isinstance(candidate_row, dict):
                merged_rows.append(candidate_row)
                continue
            intent_row = intent_by_id.get(str(candidate_row.get("raw_intent_id") or candidate_row.get("intent_id") or candidate_row.get("candidate_id") or ""), {})
            if intent_row:
                candidate_row = {
                    **candidate_row,
                    "intent_id": str(intent_row.get("intent_id") or candidate_row.get("intent_id") or ""),
                    "intent_state": str(intent_row.get("intent_state") or candidate_row.get("intent_state") or "DISCOVERED"),
                    "confidence_score": intent_row.get("confidence_score"),
                    "stability_score": intent_row.get("stability_score"),
                    "certification_convergence_score": intent_row.get("certification_convergence_score"),
                    "data_completeness_score": intent_row.get("data_completeness_score"),
                    "capture_guidance": str(intent_row.get("capture_guidance") or "NO_USER_ACTION"),
                    "capture_guidance_reason": str(intent_row.get("capture_guidance_reason") or ""),
                    "recommendation_mode": str(intent_row.get("recommendation_mode") or "NONE"),
                    "intent_execution_eligibility_state": str(intent_row.get("execution_eligibility_state") or "BROKER_AUTOMATION_DISABLED"),
                    "next_expected_lifecycle_transition": "final certification convergence" if str(intent_row.get("capture_guidance") or "") == "AWAIT_CERTIFICATION" else "operator may record manual IB capture" if str(intent_row.get("capture_guidance") or "") == "MANUAL_IB_CAPTURE_RECOMMENDED" else "continue observation",
                    "next_action": str(intent_row.get("capture_guidance_reason") or candidate_row.get("next_action") or ""),
                }
            merged_rows.append(candidate_row)
        current_day_status["candidate_rows"] = merged_rows
    current_day_status["candidate_intent_plane"] = candidate_intent_plane
    current_day_status["intent_lifecycle_summary"] = {
        "intent_count": int(candidate_intent_plane.get("intent_count") or len(intent_rows)),
        "selected_intent_count": int(candidate_intent_plane.get("selected_intent_count") or 0),
        "manual_ib_capture_recommended_count": int(candidate_intent_plane.get("manual_ib_capture_recommended_count") or 0),
        "certification_diverged_count": int(candidate_intent_plane.get("certification_diverged_count") or 0),
    }

    pointer_path = _selected_pointer_path(root)
    diagnostics_path = _candidate_diagnostics_path(root, day)
    gate_report_path = _portfolio_gate_candidate_report_path(root, day)
    manifest_path = _market_manifest_path(root)
    research_path = _research_status_path(root, day)
    ranking_path = _regime_bucket_ranking_path(root, day)

    pointer = _safe_read_json(pointer_path, artifact_id="selected_intent_pointer", errors=[], required=False)
    gate_report = current_truth.get("portfolio_gate_report") if isinstance(current_truth.get("portfolio_gate_report"), dict) else {}
    pointer_arbitration_path = ""
    for artifact in current_truth.get("source_artifacts") if isinstance(current_truth.get("source_artifacts"), list) else []:
        if isinstance(artifact, dict) and str(artifact.get("artifact_id") or "") == "intent_arbitration":
            pointer_arbitration_path = str(artifact.get("path") or "")
            break
    arbitration_payload = _safe_read_json(Path(pointer_arbitration_path), artifact_id="intent_arbitration", errors=[], required=False) if pointer_arbitration_path else {}
    if gate_report and str(current_truth.get("current_truth_status") or "") in {"CURRENT_OK", "CURRENT"}:
        diagnostics = _diagnostics_from_portfolio_gate_report(gate_report)
    elif arbitration_payload and str(current_truth.get("current_truth_status") or "") in {"CURRENT_OK", "CURRENT"}:
        diagnostics = _diagnostics_from_intent_arbitration(arbitration_payload)
    else:
        diagnostics = {"schema_id": "paper_intent_candidate_diagnostics", "schema_version": "v1_from_current_truth", "day_utc": day, "selected_exposure_intent_id": "", "candidates_found": 0, "candidate_rows": [], "paper_trade_intent_created": False}
        errors.append({"code": "CURRENT_OPERATOR_TRUTH_UNAVAILABLE", "message": str(current_truth.get("status_reason") or "Current operator truth unavailable."), "source_path": str(current_truth.get("portfolio_gate_report_path") or ""), "recoverable": True, "required": False})
    selected_exposure = current_truth.get("selected_exposure") if isinstance(current_truth.get("selected_exposure"), dict) else {}
    selected = {
        "intent_id": str(selected_exposure.get("candidate_id") or selected_exposure.get("selected_exposure_intent_id") or ""),
        "intent_path": str(selected_exposure.get("source_artifact_path") or ""),
        "engine_id": str(selected_exposure.get("engine_id") or ""),
        "sleeve_id": str(selected_exposure.get("sleeve_id") or ""),
        "symbol": str(selected_exposure.get("symbol") or "").upper(),
        "selection_reason": str(selected_exposure.get("selected_reason") or ("SELECTED_BY_ARBITRATION" if selected_exposure else "")),
        "arbitration_reason": str(selected_exposure.get("selected_reason") or ("SELECTED_BY_ARBITRATION" if selected_exposure else "")),
        "portfolio_score_total": selected_exposure.get("score"),
        "portfolio_score_components": {"signal_strength": selected_exposure.get("confidence")},
        "source_day": str(current_truth.get("source_day") or ""),
        "source_report_family": "selected_intent_pointer_v1" if current_truth.get("portfolio_gate_report") == {} else "portfolio_gate_candidate_report_v1",
        "source_report_path": str(current_truth.get("portfolio_gate_report_path") or ""),
    }
    selected_id = str(selected.get("intent_id") or diagnostics.get("selected_exposure_intent_id") or "")
    conversion_path_text = str(current_truth.get("conversion_path") or "")
    conversion_path = Path(conversion_path_text).expanduser().resolve() if conversion_path_text else None
    conversion = current_truth.get("conversion") if isinstance(current_truth.get("conversion"), dict) else {}
    source_mismatch_warning = str(current_truth.get("source_mismatch_warning") or "")
    manifest = _safe_read_json(manifest_path, artifact_id="market_data_manifest", errors=errors, required=False)
    research = _safe_read_json(research_path, artifact_id="research_lab_summary", errors=errors, required=False)
    ranking = _safe_read_json(ranking_path, artifact_id="regime_bucket_candidate_ranking", errors=errors, required=False) if ranking_path.exists() else {}
    latest_captured_projection = _latest_captured_ticket_projection(root, str(day_utc))

    selected_intent, selected_intent_path = _selected_intent_payload(selected, errors)
    selected_row = _first_selected_row(diagnostics, selected_id)

    manual = _manual_capture_candidate(
        selected=selected,
        selected_row=selected_row,
        selected_intent=selected_intent,
        conversion=conversion,
        diagnostics_path=str(diagnostics_path),
        pointer_path=str(pointer_path),
        selected_intent_path=selected_intent_path,
        source_day=str(selected.get("source_day") or diagnostics.get("day_utc") or ""),
        latest_report_day=day,
        source_run_id=str(current_truth.get("source_run_id") or ""),
        source_mismatch_warning=source_mismatch_warning,
    )
    trade_projection = build_trade_candidate_projection_v1(
        truth_root=root,
        day_utc=day,
        current_operator_truth=current_truth,
        generated_at_utc=generated,
    )
    write_trade_candidate_projection_v1(truth_root=root, day_utc=str(trade_projection.get("day_utc") or day), payload=trade_projection)
    paper_construction, paper_construction_path = build_and_write_paper_trade_construction_v1(
        truth_root=root,
        day_utc=day,
        current_operator_truth=current_truth,
        generated_at_utc=generated,
    )
    has_manual_ticket = bool(str(paper_construction.get("selected_exposure_intent_id") or "").strip())
    if has_manual_ticket:
        ticket_evidence_set = write_ticket_evidence_set_v1(truth_root=root, construction=paper_construction, generated_at_utc=generated)
        submit_boundary_precheck, submit_boundary_precheck_path, trade_ticket_lineage = build_and_write_submit_boundary_precheck_v1(
            truth_root=root,
            construction=paper_construction,
            generated_at_utc=generated,
        )
    else:
        ticket_evidence_set = {"ticket_id": "", "evidences": {}, "paths": {}}
        submit_boundary_precheck = {}
        submit_boundary_precheck_path = root / "reports" / "submit_boundary_precheck_v1" / day / "NO_ACTIVE_TICKET" / "submit_boundary_precheck.v1.json"
        trade_ticket_lineage = {}
    trade_lifecycle_case, trade_lifecycle_case_path = build_and_write_trade_lifecycle_case_v1(
        truth_root=root,
        day_utc=day,
        current_operator_truth=current_truth,
        paper_trade_construction=paper_construction,
        generated_at_utc=generated,
    )
    trade_lifecycle_case["trade_ticket_lineage_v1"] = trade_ticket_lineage
    trade_lifecycle_case["submit_boundary_precheck_v1"] = submit_boundary_precheck
    trade_case_projection = trade_case_projection_v1(trade_lifecycle_case)
    trade_ticket_projection = trade_ticket_projection_v1(trade_lifecycle_case)
    trade_ticket_projection_path = write_trade_ticket_projection_v1(
        truth_root=root,
        day_utc=str(trade_ticket_projection.get("source_day") or day),
        payload=trade_ticket_projection,
    )
    legacy_module_quarantine = _legacy_module_runtime_quarantine(root, day, trade_ticket_projection)
    construction_manual = paper_trade_construction_view_v1(paper_construction)
    construction_manual["source_mismatch_warning"] = source_mismatch_warning
    construction_manual["score"] = manual.get("score")
    construction_manual["confidence"] = manual.get("confidence")
    construction_manual["selected_reason"] = manual.get("selected_reason")
    construction_manual["paper_trade_intent_id"] = manual.get("paper_trade_intent_id")
    construction_manual["source_artifact_paths"] = manual.get("source_artifact_paths")
    construction_manual["paper_trade_construction_id"] = paper_construction.get("construction_id")
    construction_manual["paper_trade_construction_path"] = str(paper_construction_path)
    construction_manual["trade_candidate_projection_id"] = str(trade_projection.get("projection_id") or "")
    construction_manual["blocker_code"] = str(trade_projection.get("conversion_blocker_code") or "")
    construction_manual["blocker_message"] = str(trade_projection.get("conversion_blocker_message") or "")
    construction_manual["conversion_status"] = str(trade_projection.get("conversion_status") or "")
    construction_manual["stale_source"] = False
    construction_manual["stale_market_data"] = str(construction_manual.get("trade_construction_status") or "") == "blocked_missing_market_data"
    construction_manual["paper_trade_intent_created"] = trade_projection.get("paper_intent_status") == "CREATED"
    construction_manual["paper_intent_created"] = construction_manual["paper_trade_intent_created"]
    construction_manual["paper_intent_status"] = str(trade_projection.get("paper_intent_status") or "")
    construction_manual["submit_boundary_status"] = str(paper_construction.get("submit_boundary_status") or "")
    legacy_freshness = trade_projection.get("data_freshness_status") if isinstance(trade_projection.get("data_freshness_status"), dict) else {}
    if not construction_manual.get("latest_market_session") and legacy_freshness.get("observed_session"):
        construction_manual["latest_market_session"] = str(legacy_freshness.get("observed_session") or "")
    if not construction_manual.get("required_market_session") and legacy_freshness.get("expected_session"):
        construction_manual["required_market_session"] = str(legacy_freshness.get("expected_session") or "")
    manual = {**trade_case_projection, **trade_ticket_projection, **construction_manual}
    manual["ticket_id"] = trade_ticket_lineage.get("ticket_id")
    manual["trade_ticket_lineage_v1"] = trade_ticket_lineage
    manual["lineage_status"] = trade_ticket_lineage.get("lineage_status")
    manual["lineage_hash"] = trade_ticket_lineage.get("lineage_hash")
    manual["ticket_lineage_hash"] = trade_ticket_lineage.get("lineage_hash")
    manual["trade_ticket_lineage_path"] = str(trade_ticket_lineage.get("artifact_path") or "")
    manual_records = manual.get("manual_capture_records") if isinstance(manual.get("manual_capture_records"), list) else []
    manual_capture_record = manual.get("manual_capture_record") if isinstance(manual.get("manual_capture_record"), dict) else (manual_records[-1] if manual_records and isinstance(manual_records[-1], dict) else {})
    if manual_capture_record:
        manual["manual_capture_record"] = manual_capture_record
    captured_statuses = {"captured_manually", "partial"}
    already_captured = str(manual_capture_record.get("capture_status") or "") in captured_statuses or trade_ticket_lineage.get("lineage_status") == "CAPTURED_HISTORICAL"
    manual["captured_read_only"] = already_captured
    if already_captured:
        captured_history = build_captured_ticket_history_v1(truth_root=root, day_utc=day, capture_record=manual_capture_record)
        captured_projection = captured_ticket_projection_v1(captured_history)
        captured_history_path = write_captured_ticket_history_v1(truth_root=root, payload=captured_history)
        captured_projection_path = write_captured_ticket_projection_v1(truth_root=root, payload=captured_projection)
        manual["captured_ticket_history_v1"] = captured_history
        manual["captured_ticket_projection_v1"] = captured_projection
        manual["captured_ticket_history_path"] = str(captured_history_path)
        manual["captured_ticket_projection_path"] = str(captured_projection_path)
        manual["historical_state"] = "CAPTURED_HISTORICAL"
        manual["lifecycle_state"] = "CAPTURED_HISTORICAL"
        manual["current_state"] = "CAPTURED_HISTORICAL"
        manual["lineage_status"] = "CAPTURED_HISTORICAL"
        manual["submit_boundary_status"] = "CAPTURE_TIME_VALIDATED"
        manual["editable"] = False
        manual["read_only"] = True
        manual["editing_policy_reason"] = "This ticket has an immutable manual capture record and is historical/read-only."
        manual["capture_editing_disabled"] = True
        manual["manual_capture_ready"] = False
        manual["capture_save_ready"] = False
    else:
        manual["editable"] = trade_ticket_lineage.get("lineage_status") == "ACTIVE_CURRENT"
        manual["read_only"] = trade_ticket_lineage.get("lineage_status") != "ACTIVE_CURRENT"
    manual["submit_boundary_precheck_v1"] = submit_boundary_precheck
    manual["submit_boundary_status"] = "CAPTURE_TIME_VALIDATED" if already_captured else submit_boundary_precheck.get("validation_status")
    manual["submit_boundary_id"] = submit_boundary_precheck.get("submit_boundary_id")
    manual["submit_boundary_hash"] = submit_boundary_precheck.get("submit_boundary_hash")
    manual["market_freshness_status"] = trade_ticket_lineage.get("evidence_status", {}).get("market_freshness") if isinstance(trade_ticket_lineage.get("evidence_status"), dict) else "MISSING"
    manual["paper_intent_status"] = trade_ticket_lineage.get("evidence_status", {}).get("paper_intent") if isinstance(trade_ticket_lineage.get("evidence_status"), dict) else "MISSING"
    manual["conversion_evidence_status"] = trade_ticket_lineage.get("evidence_status", {}).get("conversion") if isinstance(trade_ticket_lineage.get("evidence_status"), dict) else "MISSING"
    manual["allocation_status"] = trade_ticket_lineage.get("evidence_status", {}).get("allocation") if isinstance(trade_ticket_lineage.get("evidence_status"), dict) else "MISSING"
    manual["allocation_artifact_path"] = trade_ticket_lineage.get("allocation_artifact_path")
    manual["allocation_artifact_hash"] = trade_ticket_lineage.get("allocation_artifact_hash")
    manual["allocation_limit_used"] = trade_ticket_lineage.get("allocation_limit_used") if isinstance(trade_ticket_lineage.get("allocation_limit_used"), dict) else {}
    manual["risk_contract_status"] = trade_ticket_lineage.get("evidence_status", {}).get("risk_contract") if isinstance(trade_ticket_lineage.get("evidence_status"), dict) else "MISSING"
    manual["risk_contract_path"] = trade_ticket_lineage.get("risk_contract_path")
    manual["risk_contract_hash"] = trade_ticket_lineage.get("risk_contract_hash")
    manual["risk_measure"] = trade_ticket_lineage.get("risk_measure")
    manual["risk_measure_definition"] = trade_ticket_lineage.get("risk_measure_definition") if isinstance(trade_ticket_lineage.get("risk_measure_definition"), dict) else {}
    manual["risk_limit_used"] = trade_ticket_lineage.get("risk_limit_used") if isinstance(trade_ticket_lineage.get("risk_limit_used"), dict) else {}
    manual["candidate_identity_status"] = trade_ticket_lineage.get("evidence_status", {}).get("candidate_identity") if isinstance(trade_ticket_lineage.get("evidence_status"), dict) else "MISSING"
    manual["target_day_admission_status"] = trade_ticket_lineage.get("evidence_status", {}).get("target_day_admission") if isinstance(trade_ticket_lineage.get("evidence_status"), dict) else "MISSING"
    manual["target_day_admission_path"] = trade_ticket_lineage.get("target_day_admission_path")
    manual["target_day_admission_hash"] = trade_ticket_lineage.get("target_day_admission_hash")
    manual["day_activation_status"] = trade_ticket_lineage.get("evidence_status", {}).get("day_activation") if isinstance(trade_ticket_lineage.get("evidence_status"), dict) else "MISSING"
    manual["day_activation_path"] = trade_ticket_lineage.get("day_activation_path")
    manual["day_activation_hash"] = trade_ticket_lineage.get("day_activation_hash")
    manual["global_context_status"] = trade_ticket_lineage.get("evidence_status", {}).get("global_context") if isinstance(trade_ticket_lineage.get("evidence_status"), dict) else "MISSING"
    manual["global_context_path"] = trade_ticket_lineage.get("global_context_path")
    manual["global_context_hash"] = trade_ticket_lineage.get("global_context_hash")
    manual["economic_state_status"] = trade_ticket_lineage.get("evidence_status", {}).get("economic_state") if isinstance(trade_ticket_lineage.get("evidence_status"), dict) else "MISSING"
    manual["economic_state_path"] = trade_ticket_lineage.get("economic_state_path")
    manual["economic_state_hash"] = trade_ticket_lineage.get("economic_state_hash")
    manual["economic_state_build_path"] = trade_ticket_lineage.get("economic_state_build_path")
    manual["cash_ledger_path"] = trade_ticket_lineage.get("cash_ledger_path")
    manual["cash_ledger_hash"] = trade_ticket_lineage.get("cash_ledger_hash")
    manual["cash_ledger_source_type"] = trade_ticket_lineage.get("cash_ledger_source_type")
    manual["positions_snapshot_path"] = trade_ticket_lineage.get("positions_snapshot_path")
    manual["positions_snapshot_hash"] = trade_ticket_lineage.get("positions_snapshot_hash")
    manual["positions_source_type"] = trade_ticket_lineage.get("positions_source_type")
    manual["position_lifecycle_path"] = trade_ticket_lineage.get("position_lifecycle_path")
    manual["position_lifecycle_hash"] = trade_ticket_lineage.get("position_lifecycle_hash")
    manual["accounting_nav_path"] = trade_ticket_lineage.get("accounting_nav_path")
    manual["accounting_nav_hash"] = trade_ticket_lineage.get("accounting_nav_hash")
    manual["accounting_nav_source_type"] = trade_ticket_lineage.get("accounting_nav_source_type")
    manual["candidate_identity_set_path"] = trade_ticket_lineage.get("candidate_identity_set_path")
    manual["candidate_identity_set_hash"] = trade_ticket_lineage.get("candidate_identity_set_hash")
    manual["expected_candidate_id"] = trade_ticket_lineage.get("expected_candidate_id")
    manual["actual_phasec_candidate_id"] = trade_ticket_lineage.get("actual_phasec_candidate_id")
    manual["actual_phasec_order_plan_path"] = trade_ticket_lineage.get("actual_phasec_order_plan_path")
    manual["stale_order_plan_reason"] = trade_ticket_lineage.get("stale_order_plan_reason")
    manual["runtime_evaluation_hash"] = trade_ticket_lineage.get("runtime_evaluation_hash")
    manual["trade_case_projection_v1"] = trade_case_projection
    manual["trade_ticket_projection_v1"] = trade_ticket_projection
    manual["trade_lifecycle_case_v1"] = trade_lifecycle_case
    watchlist = _suppressed_watchlist(diagnostics, manual.get("selected_exposure_intent_id") or selected_id, ranking)
    manual["suppressed_count"] = int(watchlist.get("suppressed_count") or 0)
    source_artifacts = [
        _source_ref(pointer_path, "selected_intent_pointer", pointer),
        _source_ref(diagnostics_path, "paper_intent_candidate_diagnostics", diagnostics),
        _source_ref(gate_report_path, "portfolio_gate_candidate_report", gate_report),
        _source_ref(conversion_path, "exposure_intent_conversion", conversion) if conversion_path else {"artifact_id": "exposure_intent_conversion", "path": "", "exists": False, "sha256": "", "schema_id": ""},
        _source_ref(manifest_path, "market_data_manifest", manifest),
        _source_ref(research_path, "research_lab_summary", research),
        _source_ref(ranking_path, "regime_bucket_candidate_ranking", ranking),
    ]
    if selected_intent_path:
        source_artifacts.append(_source_ref(Path(selected_intent_path), "selected_exposure_intent", selected_intent))
    source_artifacts.append(_source_ref(paper_construction_path, "paper_trade_construction_v1", paper_construction))
    source_artifacts.append(_source_ref(trade_lifecycle_case_path, "trade_lifecycle_case_v1", trade_lifecycle_case))
    source_artifacts.append(_source_ref(trade_ticket_projection_path, "trade_ticket_projection_v1", trade_ticket_projection))
    source_artifacts.append(_source_ref(submit_boundary_precheck_path, "submit_boundary_precheck_v1", submit_boundary_precheck))
    lineage_path = Path(str(trade_ticket_lineage.get("artifact_path") or ""))
    if str(lineage_path):
        source_artifacts.append(_source_ref(lineage_path, "trade_ticket_lineage_v1", trade_ticket_lineage))
    for evidence_name, evidence_path_text in (ticket_evidence_set.get("paths") or {}).items():
        evidence_payload = (ticket_evidence_set.get("evidences") or {}).get(evidence_name, {}) if isinstance(ticket_evidence_set.get("evidences"), dict) else {}
        source_artifacts.append(_source_ref(Path(str(evidence_path_text)), f"{evidence_name}_evidence_v1", evidence_payload if isinstance(evidence_payload, dict) else {}))
    source_artifacts.append(_source_ref(candidate_intent_plane_path, "candidate_intent_plane_v1", candidate_intent_plane))
    source_artifacts.append(_source_ref(_runtime_truth_kernel_path(root, day), "aegis_runtime_truth_kernel_v1", {}))

    market = conversion.get("market_data_status") if isinstance(conversion.get("market_data_status"), dict) else {}
    market_freshness = {
        "symbol": manual.get("symbol"),
        "required_market_session": manual.get("required_market_session") or day,
        "latest_market_session": manual.get("latest_market_session"),
        "stale_market_data": bool(manual.get("stale_market_data")),
        "status": str(market.get("status") or ("STALE_OR_MISSING" if manual.get("stale_market_data") else "UNKNOWN")),
        "source_path": str(market.get("path") or ""),
        "manifest_symbol_count": len(manifest.get("symbols") or []) if isinstance(manifest.get("symbols"), list) else 0,
    }
    paper = _paper_status(conversion)
    blockers = _blockers(manual, errors)
    degraded_sections = []
    if errors:
        degraded_sections.append("source_artifacts")
    if manual.get("stale_market_data") or manual.get("blocker_code"):
        degraded_sections.extend(["manual_capture_candidate", "market_data_freshness", "conversion_status"])
    if not research:
        degraded_sections.append("research_lab_summary")
    degraded_sections = sorted(set(degraded_sections))
    selected_count = int(current_day_status.get("selected_candidate_count") or (1 if manual.get("candidate_available") else 0))
    capture_ready_ticket_count = int(current_day_status.get("capture_ready_ticket_count") or 0)
    blocked_selected_candidate_count = int(current_day_status.get("blocked_selected_candidate_count") or 0)
    candidate_pipeline_observability = current_day_status.get("candidate_pipeline_observability") if isinstance(current_day_status.get("candidate_pipeline_observability"), dict) else {}
    candidate_funnel_projection = current_day_status.get("candidate_funnel_projection") if isinstance(current_day_status.get("candidate_funnel_projection"), dict) else {}
    thesis_graph_projection = _thesis_graph_projection(root, day, errors)
    mission_control_thesis_summary = thesis_graph_projection.get("mission_control_thesis_summary") if isinstance(thesis_graph_projection.get("mission_control_thesis_summary"), dict) else {}
    candidate_pipeline_alerts = candidate_pipeline_observability.get("alerts") if isinstance(candidate_pipeline_observability.get("alerts"), list) else []
    suppressed_count = int(watchlist.get("suppressed_count") or 0)
    candidate_rows = diagnostics.get("candidate_rows") if isinstance(diagnostics.get("candidate_rows"), list) else []
    candidate_count = int(diagnostics.get("candidates_found") or len(candidate_rows) or current_day_status.get("candidate_count") or 0)
    current_day_blocked_count = int(current_day_status.get("blocked_candidate_count") or 0)
    unique_sleeves = sorted(
        {
            str(row.get("sleeve_id") or row.get("engine_id") or "")
            for row in candidate_rows
            if isinstance(row, dict) and str(row.get("sleeve_id") or row.get("engine_id") or "")
        }
    )
    blocked_conversion_count = blocked_selected_candidate_count or (1 if manual.get("blocker_code") else 0)
    eod_status = "BLOCKED_CONVERSION" if blocked_conversion_count else ("SELECTED_EXPOSURE_AVAILABLE" if selected_count else "NO_CANDIDATES")
    current_truth_status_text = str(current_truth.get("current_truth_status") or "")
    degraded_read_only_states = {"PENDING_VENDOR_DATA", "PARTIAL_DATA_AVAILABLE", "READ_ONLY_PRIOR_DAY_FALLBACK", "STALE"}
    degraded_read_only = current_truth_status_text in degraded_read_only_states
    current_day_visible_states = {"CURRENT_OK", "CURRENT", "INTRADAY_OPERATIONAL_READY", "PROVISIONAL_INTRADAY", "VALIDATED_CURRENT_DAY", "CERTIFICATION_PENDING", "CERTIFIED", "CURRENT_DAY_BLOCKED", "FINAL_EOD_READY"}
    current_day_visible = current_truth_status_text in current_day_visible_states or str(current_truth.get("runtime_mode") or "") in {"OPEN_INTRADAY", "CERTIFICATION_PENDING", "CERTIFIED_READY"}
    displayed_artifact_day = str(current_truth.get("displayed_artifact_day") or (day if current_day_visible else "") or historical_fallback.get("source_day") or day)
    current_intraday_candidate_count = int(current_day_status.get("current_intraday_candidate_count") or current_day_status.get("candidate_created_count") or 0) if current_day_visible else 0
    current_day_generated_candidate_count = int(current_day_status.get("candidate_count") or 0) if current_day_visible else 0
    final_eod_certified_candidate_count = int(current_day_status.get("final_eod_certified_candidate_count") or 0) if current_day_visible else 0
    current_day_candidate_rows = current_day_status.get("candidate_rows") if current_day_visible and isinstance(current_day_status.get("candidate_rows"), list) else []
    runtime_mode = str(current_truth.get("runtime_mode") or current_day_status.get("runtime_mode") or current_day_status.get("market_data_mode") or "UNKNOWN")
    certification_state_text = str(current_day_status.get("certification_state") or "").upper()
    if certification_state_text == "CERTIFIED" or runtime_mode == "CERTIFIED_READY":
        projected_operational_mode = "CERTIFIED_READY"
    elif certification_state_text in {"CERTIFICATION_PENDING", "PROVISIONAL_INTRADAY"} or runtime_mode in {"OPEN_INTRADAY", "CERTIFICATION_PENDING"}:
        projected_operational_mode = "CERTIFICATION_PENDING" if certification_state_text == "CERTIFICATION_PENDING" else "OPEN_INTRADAY"
    elif certification_state_text == "INVALID" or runtime_mode == "CERTIFICATION_FAILED":
        projected_operational_mode = "CERTIFICATION_FAILED"
    else:
        projected_operational_mode = "DEGRADED_READ_ONLY" if degraded_read_only else runtime_mode
    snapshot_id = canonical_hash_for_c2_artifact_v1(
        {
            "day_utc": day,
            "selected": manual.get("selected_exposure_intent_id"),
            "conversion_blocker": manual.get("blocker_code"),
            "suppressed_count": watchlist.get("suppressed_count"),
            "source_hashes": [row.get("sha256") for row in source_artifacts],
        }
    )
    payload = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "artifact_id": f"operator_state_snapshot_v1:{day}:{snapshot_id[:16]}",
        "generated_at": generated,
        "generated_at_utc": generated,
        "day_utc": day,
        "source_day": str(current_truth.get("source_day") or day),
        "current_truth_status": str(current_truth.get("current_truth_status") or "UNAVAILABLE"),
        "runtime_mode": projected_operational_mode,
        "market_data_state": market_data_state,
        "candidate_certification_state": candidate_certification_state,
        "execution_eligibility_state": execution_eligibility_state,
        "operator_state_semantics": operator_state_semantics,
        "current_operator_truth": _json_safe(current_truth),
        "current_day_status": _json_safe(current_day_status),
        "historical_fallback": _json_safe(historical_fallback),
        "displayed_artifact_day": displayed_artifact_day,
        "requested_day": str(current_truth.get("requested_day") or day_utc),
        "truth_root": str(root),
        "read_only": True,
        "safety": {"broker_execution_allowed": False, "broker_submit_transmit_allowed": False, "autonomous_execution_allowed": False, "automatic_approval_allowed": False, "live_trading_allowed": False},
        "metadata": {
            "snapshot_id": snapshot_id,
            "artifact_id": f"operator_state_snapshot_v1:{day}:{snapshot_id[:16]}",
            "generated_at": generated,
            "day_utc": day,
            "read_only": True,
            "projection_family": REPORT_FAMILY,
            "projection_version": SCHEMA_VERSION,
            "source_run_id": str(conversion.get("remediation_attempt_id") or conversion.get("ad_hoc_run_id") or diagnostics.get("ad_hoc_run_id") or ""),
            "source_fingerprint": snapshot_id,
            "safety": {"broker_execution_allowed": False, "live_trading_allowed": False, "order_routing_allowed": False, "capital_allocation_allowed": False},
        },
        "runtime_status": _runtime_status(errors, source_artifacts),
        "readiness": {"status": "DEGRADED" if blockers or errors else "READY", "blocked": bool(blockers), "blocker_count": len(blockers), "stale_market_data_count": 1 if manual.get("stale_market_data") else 0},
        "selected_exposure_intent_id": manual.get("selected_exposure_intent_id") or selected_id,
        "latest_run_summary": {
            "selected_exposure_intent_id": manual.get("selected_exposure_intent_id"),
            "selected_symbol": manual.get("symbol"),
            "selected_sleeve": manual.get("sleeve_id"),
            "candidates_found": candidate_count,
            "selected_candidate_count": selected_count,
            "capture_ready_ticket_count": capture_ready_ticket_count,
            "blocked_selected_candidate_count": blocked_selected_candidate_count,
            "suppressed_count": suppressed_count,
            "blocked_conversion_count": blocked_conversion_count,
            "sleeves_ran": len(unique_sleeves),
            "sleeves_run": len(unique_sleeves),
            "paper_trade_intent_created": paper.get("paper_trade_intent_created"),
            "blocker": manual.get("blocker_code") or current_day_status.get("blocker"),
            "current_day_status": current_day_status.get("status") or str(current_truth.get("current_truth_status") or ""),
            "market_data_state": market_data_state,
            "candidate_certification_state": candidate_certification_state,
            "execution_eligibility_state": execution_eligibility_state,
            "current_day_candidate_count": current_intraday_candidate_count,
            "current_day_generated_candidate_count": current_day_generated_candidate_count,
            "current_day_blocked_candidate_count": 0 if degraded_read_only else current_day_blocked_count,
            "runtime_mode": projected_operational_mode,
            "displayed_artifact_day": displayed_artifact_day,
            "market_data_mode": str(current_day_status.get("market_data_mode") or ""),
            "final_eod_certification_status": str(current_day_status.get("final_eod_certification_status") or ""),
            "intraday_operational_ready": bool(current_day_status.get("intraday_operational_ready") is True),
        },
        "selected_exposure": {"intent_id": manual.get("selected_exposure_intent_id") or selected_id, "symbol": manual.get("symbol"), "sleeve_id": manual.get("sleeve_id"), "engine_id": manual.get("engine_id"), "direction": manual.get("direction"), "score": manual.get("score"), "confidence": manual.get("confidence"), "selected_by_arbitration": True, "selected_reason": manual.get("selected_reason") or selected.get("selection_reason"), "source_artifact_path": selected_intent_path},
        "current_day_candidate_rows": _json_safe(current_day_candidate_rows),
        "current_day_candidates": _json_safe(current_day_candidate_rows),
        "candidate_pipeline_observability": _json_safe(candidate_pipeline_observability),
        "candidate_pipeline_alerts": _json_safe(candidate_pipeline_alerts),
        "candidate_funnel_projection": _json_safe(candidate_funnel_projection),
        "thesis_graph_projection": _json_safe(thesis_graph_projection),
        "mission_control_thesis_summary": _json_safe(mission_control_thesis_summary),
        "candidate_intent_plane": _json_safe(candidate_intent_plane),
        "intent_lifecycle_summary": _json_safe(current_day_status.get("intent_lifecycle_summary") if isinstance(current_day_status.get("intent_lifecycle_summary"), dict) else {
            "intent_count": candidate_intent_plane.get("intent_count", 0),
            "selected_intent_count": candidate_intent_plane.get("selected_intent_count", 0),
            "manual_ib_capture_recommended_count": candidate_intent_plane.get("manual_ib_capture_recommended_count", 0),
        }),
        "latest_captured_trade_projection": _json_safe(latest_captured_projection),
        "historical_captures": _json_safe([latest_captured_projection] if latest_captured_projection else []),
        "manual_capture_candidate": manual,
        "manual_capture_candidate_v1": manual,
        "trade_lifecycle_case": trade_lifecycle_case,
        "trade_lifecycle_case_v1": trade_lifecycle_case,
        "trade_case_projection": trade_case_projection,
        "trade_case_projection_v1": trade_case_projection,
        "trade_ticket_projection": trade_ticket_projection,
        "trade_ticket_projection_v1": trade_ticket_projection,
        "trade_ticket_projection_path": str(trade_ticket_projection_path),
        "trade_ticket_lineage": trade_ticket_lineage,
        "trade_ticket_lineage_v1": trade_ticket_lineage,
        "submit_boundary_precheck": submit_boundary_precheck,
        "submit_boundary_precheck_v1": submit_boundary_precheck,
        "ticket_evidence_set_v1": ticket_evidence_set,
        "readiness_domain_evaluations": trade_lifecycle_case.get("readiness_domain_evaluations") if isinstance(trade_lifecycle_case.get("readiness_domain_evaluations"), list) else [],
        "readiness_domain_evaluations_v1": trade_lifecycle_case.get("readiness_domain_evaluations") if isinstance(trade_lifecycle_case.get("readiness_domain_evaluations"), list) else [],
        "domain_statuses": trade_ticket_projection.get("domain_statuses") if isinstance(trade_ticket_projection.get("domain_statuses"), dict) else {},
        "blockers_by_domain": trade_ticket_projection.get("blockers_by_domain") if isinstance(trade_ticket_projection.get("blockers_by_domain"), dict) else {},
        "legacy_module_runtime_quarantine": legacy_module_quarantine,
        "paper_trade_construction": paper_construction,
        "paper_trade_construction_v1": paper_construction,
        "paper_trade_construction_path": str(paper_construction_path),
        "trade_candidate_projection": trade_projection,
        "trade_candidate_projection_v1": trade_projection,
        "suppressed_candidate_watchlist": watchlist,
        "suppressed_candidate_watchlist_v1": watchlist,
        "regime_bucket_candidate_ranking": _json_safe(ranking),
        "paper_intent_status": paper,
        "conversion_status": {"status": manual.get("conversion_status"), "blocker_code": manual.get("blocker_code"), "blocker_message": manual.get("blocker_message"), "execution_package_created": paper.get("execution_package_created"), "submission_record_created": paper.get("submission_record_created"), "paper_submit_attempted": paper.get("paper_submit_attempted")},
        "market_data_freshness": market_freshness,
        "blockers": blockers,
        "research_lab_summary": _research_summary(research, errors),
        "source_artifacts": source_artifacts,
        "errors": errors,
        "degraded_sections": degraded_sections,
        "next_actions": [manual.get("operator_action_label") or "Review canonical operator state."],
        "active_opportunity_projection": {"snapshot_id": snapshot_id, "candidates": [], "research_observations": [], "manual_capture_candidate": manual, "suppressed_candidate_watchlist": watchlist, "selected_exposure": manual, "selected_candidate_count": selected_count, "suppressed_count": suppressed_count, "blocked_conversion_count": blocked_conversion_count},
        "operator_today_projection": {
            "snapshot_id": snapshot_id,
            "readiness_status": "DEGRADED_READ_ONLY" if degraded_read_only else str(current_day_status.get("status") or ("DEGRADED" if blockers or errors else "READY")),
            "current_runtime_day": str(current_truth.get("requested_day") or day_utc),
            "displayed_artifact_day": displayed_artifact_day,
            "runtime_mode": projected_operational_mode,
            "operational_mode": projected_operational_mode,
            "current_day_run_status": str(current_day_status.get("status") or current_truth.get("current_truth_status") or "UNKNOWN"),
            "market_data_state": market_data_state,
            "candidate_certification_state": candidate_certification_state,
            "execution_eligibility_state": execution_eligibility_state,
            "candidate_count": current_intraday_candidate_count,
            "sleeve_candidate_count": current_intraday_candidate_count,
            "current_day_candidate_count": current_intraday_candidate_count,
            "current_intraday_candidate_count": current_intraday_candidate_count,
            "current_day_generated_candidate_count": current_day_generated_candidate_count,
            "final_eod_certified_candidate_count": final_eod_certified_candidate_count,
            "analytical_state_counts": current_day_status.get("analytical_state_counts") if isinstance(current_day_status.get("analytical_state_counts"), dict) else {},
            "operator_task_state_counts": current_day_status.get("operator_task_state_counts") if isinstance(current_day_status.get("operator_task_state_counts"), dict) else {},
            "operator_affordance_counts": current_day_status.get("operator_affordance_counts") if isinstance(current_day_status.get("operator_affordance_counts"), dict) else {},
            "execution_state_counts": current_day_status.get("execution_state_counts") if isinstance(current_day_status.get("execution_state_counts"), dict) else {},
            "user_task_available_count": int(current_day_status.get("user_task_available_count") or current_day_status.get("operator_action_available_count") or 0),
            "operator_action_available_count": int(current_day_status.get("operator_action_available_count") or 0),
            "manual_ib_capture_ready_count": int(current_day_status.get("manual_ib_capture_ready_count") or current_day_status.get("manual_capture_available_count") or 0),
            "manual_capture_available_count": int(current_day_status.get("manual_capture_available_count") or 0),
            "system_repair_required_count": int(current_day_status.get("system_repair_required_count") or 0),
            "execution_eligible_row_count": int(current_day_status.get("execution_eligible_row_count") or 0),
            "execution_locked_non_certified_count": int(current_day_status.get("execution_locked_non_certified_count") or 0),
            "candidate_pipeline_observability": _json_safe(candidate_pipeline_observability),
            "candidate_pipeline_alerts": _json_safe(candidate_pipeline_alerts),
            "candidate_funnel_projection": _json_safe(candidate_funnel_projection),
            "thesis_graph_projection": _json_safe(thesis_graph_projection),
            "mission_control_thesis_summary": _json_safe(mission_control_thesis_summary),
            "capture_ready_ticket_count": capture_ready_ticket_count,
            "blocked_selected_candidate_count": blocked_selected_candidate_count,
            "market_data_mode": str(current_day_status.get("market_data_mode") or ""),
            "final_eod_certification_status": str(current_day_status.get("final_eod_certification_status") or ""),
            "final_eod_certification_pending": bool(current_day_status.get("final_eod_certification_pending") is True),
            "intraday_operational_ready": bool(current_day_status.get("intraday_operational_ready") is True),
            "market_data_last_updated_at": str(current_day_status.get("market_data_last_updated_at") or ""),
            "candidate_snapshot_generated_at": str(current_day_status.get("candidate_snapshot_generated_at") or ""),
            "last_certification_attempt_at": str(current_day_status.get("last_certification_attempt_at") or ""),
            "final_eod_certification_completed_at": str(current_day_status.get("final_eod_certification_completed_at") or ""),
            "eod_certification_timing": _json_safe(current_day_status.get("eod_certification_timing") if isinstance(current_day_status.get("eod_certification_timing"), dict) else {}),
            "historical_candidate_count": int(historical_fallback.get("candidate_count") or 0),
            "historical_latest_capture_day": str(historical_fallback.get("source_day") or ""),
            "latest_historical_capture_label": str(historical_fallback.get("source_day") or ""),
            "selected_candidate_count": selected_count,
            "capture_ready_ticket_count": capture_ready_ticket_count,
            "blocked_selected_candidate_count": blocked_selected_candidate_count,
            "suppressed_count": suppressed_count,
            "blocked_candidate_count": 0 if degraded_read_only else current_day_blocked_count,
            "blocked_conversion_count": 0 if degraded_read_only else blocked_conversion_count,
            "sleeves_ran": len(unique_sleeves),
            "sleeves_run": len(unique_sleeves),
            "blocked_sleeves": 0 if degraded_read_only else (1 if blockers or current_day_blocked_count else 0),
            "data_warnings": len(current_day_status.get("critical_missing_symbols") or current_day_status.get("missing_symbols") or []) or (1 if manual.get("stale_market_data") else 0),
            "completed_capture_count": 0,
            "historical_completed_capture_count": 1 if historical_fallback else 0,
        },
        "system_diagnostic_projection": {"snapshot_id": snapshot_id, "diagnostics": blockers},
        "operator_task_projection": {"snapshot_id": snapshot_id, "tasks": []},
        "passive_health_projection": {"snapshot_id": snapshot_id, "status": "DEGRADED_READ_ONLY" if degraded_read_only else ("DEGRADED" if blockers or errors else "READY")},
        "what_changed_projection": {"snapshot_id": snapshot_id, "changes": []},
        "eod_outcome_projection": {"snapshot_id": snapshot_id, "status": eod_status, "sleeves_expected": len(unique_sleeves), "sleeves_ran": len(unique_sleeves), "sleeves_run": len(unique_sleeves), "selected_candidate_count": selected_count, "suppressed_candidate_count": suppressed_count, "blocked_conversion_count": blocked_conversion_count, "manual_capture_count": 0},
    }
    payload["runtime_timeline_projection"] = build_runtime_timeline_projection_v1(
        truth_root=root,
        day_utc=day,
        operator_snapshot=payload,
        now_utc=generated,
    )
    domain_certification = payload["runtime_timeline_projection"].get("domain_certification") if isinstance(payload["runtime_timeline_projection"].get("domain_certification"), dict) else {}
    payload["domain_certification"] = domain_certification
    if domain_certification:
        summary = domain_certification.get("summary") if isinstance(domain_certification.get("summary"), dict) else {}
        payload["passive_health_projection"]["health"] = [
            {"health_type": "domain_certification", "label": "Certified domains", "value": int(summary.get("certified_domains") or 0)},
            {"health_type": "domain_certification", "label": "Delayed domains", "value": int(summary.get("delayed_domains") or 0)},
            {"health_type": "domain_certification", "label": "Blocked sleeves", "value": int(summary.get("blocked_sleeves") or 0)},
        ]
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1({**payload, "canonical_json_hash": None})
    return payload


def write_operator_state_snapshot_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any]) -> Path:
    effective_root = _resolve_effective_truth_root(Path(truth_root).expanduser().resolve(), str(day_utc))
    path = operator_state_snapshot_path_v1(truth_root=effective_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_and_write_operator_state_snapshot_v1(*, truth_root: Path | str, day_utc: str) -> tuple[dict[str, Any], Path]:
    payload = build_operator_state_snapshot_v1(truth_root=truth_root, day_utc=day_utc)
    path = write_operator_state_snapshot_v1(truth_root=truth_root, day_utc=day_utc, payload=payload)
    payload["artifact_path"] = str(path)
    return payload, path


def read_operator_state_snapshot_v1(*, truth_root: Path | str, day_utc: str) -> tuple[dict[str, Any], Path, list[dict[str, Any]]]:
    effective_root = _resolve_effective_truth_root(Path(truth_root).expanduser().resolve(), str(day_utc))
    path = operator_state_snapshot_path_v1(truth_root=effective_root, day_utc=day_utc)
    errors: list[dict[str, Any]] = []
    payload = _safe_read_json(path, artifact_id="operator_state_snapshot", errors=errors, required=False)
    return payload, path, errors


def api_envelope_v1(data: dict[str, Any], errors: list[dict[str, Any]] | None = None, *, next_action: str = "") -> dict[str, Any]:
    all_errors = list(errors or []) + list(data.get("errors") if isinstance(data.get("errors"), list) else [])
    degraded = bool(all_errors or data.get("degraded_sections") or data.get("blockers"))
    return {"ok": True, "degraded": degraded, "data": data, "errors": all_errors, "next_action": next_action or (data.get("next_actions") or ["Review canonical operator state."])[0]}


def _snapshot_needs_refresh(payload: dict[str, Any], *, truth_root: Path | str, day_utc: str) -> bool:
    if not payload:
        return True
    root = _resolve_effective_truth_root(Path(truth_root).expanduser().resolve(), str(day_utc))
    if _references_external_tmp_path(payload, allowed_root=root):
        return True
    current_truth = resolve_current_operator_truth_v1(truth_root=root, day_utc=str(day_utc))
    gate_report_exists = _portfolio_gate_candidate_report_path(root, str(day_utc)).exists()
    conversion_exists = _latest_conversion_path(root, str(day_utc)) is not None
    manual = payload.get("manual_capture_candidate") if isinstance(payload.get("manual_capture_candidate"), dict) else {}
    watchlist = payload.get("suppressed_candidate_watchlist") if isinstance(payload.get("suppressed_candidate_watchlist"), dict) else {}
    latest = payload.get("latest_run_summary") if isinstance(payload.get("latest_run_summary"), dict) else {}
    today_projection = payload.get("operator_today_projection") if isinstance(payload.get("operator_today_projection"), dict) else {}
    current_status_text = str(current_truth.get("current_truth_status") or "")
    if current_status_text in {"PENDING_VENDOR_DATA", "PARTIAL_DATA_AVAILABLE", "READ_ONLY_PRIOR_DAY_FALLBACK", "STALE"} and str(today_projection.get("operational_mode") or "") != "DEGRADED_READ_ONLY":
        return True
    current_selected = str((current_truth.get("selected_exposure") if isinstance(current_truth.get("selected_exposure"), dict) else {}).get("candidate_id") or "")
    if current_selected and str(manual.get("selected_exposure_intent_id") or "") != current_selected:
        return True
    if str(current_truth.get("current_truth_status") or "") in {"CURRENT_OK", "CURRENT"} and str(manual.get("source_day") or "") != str(current_truth.get("source_day") or ""):
        return True
    if "trade_candidate_projection_v1" not in payload:
        return True
    if "trade_case_projection_v1" not in payload:
        return True
    if "trade_ticket_projection_v1" not in payload:
        return True
    if "runtime_timeline_projection" not in payload:
        return True
    if "candidate_intent_plane" not in payload:
        return True
    if "current_truth_status" not in payload:
        return True
    if str(payload.get("current_truth_status") or "") != str(current_truth.get("current_truth_status") or ""):
        return True
    if gate_report_exists and int(watchlist.get("suppressed_count") or 0) == 0:
        return True
    if conversion_exists and not manual.get("selected_exposure_intent_id"):
        return True
    if gate_report_exists and int(latest.get("candidates_found") or 0) == 0:
        return True
    return False


def load_or_build_operator_state_snapshot_response_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    payload, path, read_errors = read_operator_state_snapshot_v1(truth_root=truth_root, day_utc=day_utc)
    if not payload or _snapshot_needs_refresh(payload, truth_root=truth_root, day_utc=day_utc):
        try:
            payload, path = build_and_write_operator_state_snapshot_v1(truth_root=truth_root, day_utc=day_utc)
            read_errors = []
        except Exception as exc:  # noqa: BLE001
            fallback = {"schema_id": SCHEMA_ID, "schema_version": SCHEMA_VERSION, "day_utc": day_utc, "status": "DEGRADED", "errors": [{"code": "OPERATOR_STATE_REBUILD_FAILED", "message": f"{type(exc).__name__}: {exc}", "source_path": str(path), "recoverable": True}], "next_actions": ["Inspect operator state builder inputs."]}
            return api_envelope_v1(fallback, read_errors, next_action="Inspect operator state builder inputs.")
    payload = {**payload, "artifact_path": str(path)}
    return {**api_envelope_v1(payload, read_errors), **payload}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="canonical_operator_state_builder_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", required=True)
    parser.add_argument("--day", "--day-utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)
    payload, path = build_and_write_operator_state_snapshot_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    print(json.dumps({"path": str(path), "snapshot_id": payload.get("snapshot_id"), "degraded_sections": payload.get("degraded_sections"), "manual_capture_candidate_available": payload.get("manual_capture_candidate", {}).get("candidate_available"), "suppressed_watchlist_count": payload.get("suppressed_candidate_watchlist", {}).get("suppressed_count"), "broker_execution_allowed": False, "live_trading_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
