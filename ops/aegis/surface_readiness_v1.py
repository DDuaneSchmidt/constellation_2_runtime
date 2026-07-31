from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import now_utc_v1, read_json_v1, write_json_v1
from ops.aegis.semantic_invariants_v1 import blocking_failures_for_surface_v1

REPORT_FAMILY = "aegis_surface_readiness_v1"
REPORT_FILENAME = "surface_readiness.v1.json"
SURFACES = (
    "command_center",
    "engineering",
    "positions",
    "history",
    "performance",
    "position_review",
    "sleeve_analytics",
    "research",
    "ask_aegis",
)
STATUSES = {"READY", "DEGRADED", "BLOCKED", "UNAVAILABLE", "HISTORICAL", "INCONSISTENT"}

SAFETY = {
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "live_trading_allowed": False,
    "autonomous_live_trading_allowed": False,
}


def surface_readiness_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_surface_readiness_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated_at = now_utc_v1()
    rows = [_build_surface_row(root=root, requested_day=day, surface_id=surface_id, generated_at=generated_at) for surface_id in SURFACES]
    by_id = {row["surface_id"]: row for row in rows}
    summary = {
        "surface_count": len(rows),
        "ready_count": sum(1 for row in rows if row["surface_status"] == "READY"),
        "blocked_count": sum(1 for row in rows if row["surface_status"] == "BLOCKED"),
        "degraded_count": sum(1 for row in rows if row["surface_status"] == "DEGRADED"),
        "unavailable_count": sum(1 for row in rows if row["surface_status"] == "UNAVAILABLE"),
        "inconsistent_count": sum(1 for row in rows if row["surface_status"] == "INCONSISTENT"),
        "actions_allowed_count": sum(1 for row in rows if row["actions_allowed"]),
        "blocked_surfaces": [row["surface_id"] for row in rows if row["surface_status"] in {"BLOCKED", "UNAVAILABLE", "INCONSISTENT"}],
    }
    return {
        "schema_id": REPORT_FAMILY,
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "requested_day": day,
        "generated_at": generated_at,
        "summary": summary,
        "surfaces": rows,
        "surface_by_id": by_id,
        "safety": dict(SAFETY),
        **SAFETY,
    }


def write_surface_readiness_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    body = dict(payload or build_surface_readiness_v1(truth_root=truth_root, day_utc=day_utc))
    return write_json_v1(surface_readiness_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def run_surface_readiness_self_check_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    payload = read_json_v1(surface_readiness_path_v1(truth_root=root, day_utc=day))
    if not payload:
        payload = build_surface_readiness_v1(truth_root=root, day_utc=day)
        write_surface_readiness_v1(truth_root=root, day_utc=day, payload=payload)
    rows = payload.get("surfaces") if isinstance(payload.get("surfaces"), list) else []
    failures: list[dict[str, str]] = []
    seen = {str(row.get("surface_id") or "") for row in rows if isinstance(row, Mapping)}
    for surface_id in SURFACES:
        if surface_id not in seen:
            failures.append({"check": "registered_surface_has_row", "surface_id": surface_id, "reason": "missing surface readiness row"})
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        sid = str(row.get("surface_id") or "")
        status = str(row.get("surface_status") or "")
        if status not in STATUSES:
            failures.append({"check": "valid_surface_status", "surface_id": sid, "reason": f"invalid status {status}"})
        if row.get("requested_day") != day:
            failures.append({"check": "requested_day_matches", "surface_id": sid, "reason": f"requested_day={row.get('requested_day')} expected={day}"})
        if row.get("actions_allowed") and status != "READY":
            failures.append({"check": "actions_only_when_ready", "surface_id": sid, "reason": f"actions_allowed with status {status}"})
        if row.get("actions_allowed") and row.get("blocking_reasons"):
            failures.append({"check": "actions_have_no_blockers", "surface_id": sid, "reason": "actions_allowed while blocking_reasons present"})
        if str(row.get("source_day") or day) != day and status not in {"HISTORICAL", "INCONSISTENT"}:
            failures.append({"check": "day_mismatch_status", "surface_id": sid, "reason": f"source_day={row.get('source_day')} status={status}"})
        if sid == "ask_aegis" and row.get("context_day") and row.get("context_day") != day and status not in {"DEGRADED", "BLOCKED", "INCONSISTENT"}:
            failures.append({"check": "ask_aegis_context_day_gated", "surface_id": sid, "reason": f"context_day={row.get('context_day')}"})
        if sid == "sleeve_analytics":
            details = row.get("details") if isinstance(row.get("details"), Mapping) else {}
            if details.get("artifact_status") == "NOT_CANONICAL" and details.get("data_quality_status") == "PASS":
                failures.append({"check": "not_canonical_not_pass", "surface_id": sid, "reason": "NOT_CANONICAL with PASS data quality"})
    return {
        "ok": not failures,
        "status": "PASS" if not failures else "FAIL",
        "day_utc": day,
        "path": str(surface_readiness_path_v1(truth_root=root, day_utc=day)),
        "surface_count": len(rows),
        "failures": failures,
        "safety": dict(SAFETY),
    }


def _build_surface_row(*, root: Path, requested_day: str, surface_id: str, generated_at: str) -> dict[str, Any]:
    specs = _surface_specs(root, requested_day)[surface_id]
    artifacts = [_artifact_ref(spec["id"], spec["path"], bool(spec.get("required", True))) for spec in specs]
    missing = [row for row in artifacts if not row["exists"] and row.get("required", True)]
    wrong_day = [row for row in artifacts if row["exists"] and row.get("artifact_day") and row.get("artifact_day") != requested_day]
    source_days = sorted({row.get("artifact_day") for row in artifacts if row.get("artifact_day")})
    source_day = source_days[0] if len(source_days) == 1 else requested_day if not source_days else "MIXED"
    context_day = _context_day(surface_id, artifacts)
    blocking: list[str] = []
    warnings: list[str] = []
    details: dict[str, Any] = {}
    if missing:
        blocking.extend(f"MISSING_ARTIFACT:{row['artifact_id']}:{row['path']}" for row in missing)
    if wrong_day:
        blocking.extend(f"WRONG_DAY_ARTIFACT:{row['artifact_id']}:{row.get('artifact_day')}!={requested_day}" for row in wrong_day)
    if source_day not in {requested_day, ""}:
        blocking.append(f"SOURCE_DAY_MISMATCH:{source_day}!={requested_day}")
    if context_day and context_day != requested_day:
        blocking.append(f"CONTEXT_DAY_MISMATCH:{context_day}!={requested_day}")

    _apply_surface_specific_rules(surface_id, artifacts, requested_day, blocking, warnings, details)
    _apply_semantic_invariant_gate(root, requested_day, surface_id, blocking, details)

    if source_day not in {requested_day, ""}:
        status = "INCONSISTENT" if source_day == "MIXED" else "HISTORICAL"
    elif missing and len(missing) == len([row for row in artifacts if row.get("required", True)]):
        status = "UNAVAILABLE"
    elif blocking:
        status = "BLOCKED"
    elif warnings:
        status = "DEGRADED"
    else:
        status = "READY"

    actions_allowed = status == "READY" and surface_id in {"command_center"} and bool(details.get("operator_action_required_count", 0))
    render_allowed = True
    return {
        "surface_id": surface_id,
        "requested_day": requested_day,
        "source_day": source_day,
        "artifact_days": {row["artifact_id"]: row.get("artifact_day") for row in artifacts},
        "context_day": context_day,
        "render_allowed": render_allowed,
        "actions_allowed": actions_allowed,
        "surface_status": status,
        "blocking_reasons": blocking,
        "warning_reasons": warnings,
        "source_artifacts": artifacts,
        "source_artifact_hashes": {row["artifact_id"]: row.get("content_hash") for row in artifacts if row.get("content_hash")},
        "generated_at": generated_at,
        "details": details,
        "safety": dict(SAFETY),
    }


def _surface_specs(root: Path, day: str) -> dict[str, list[dict[str, Any]]]:
    reports = root / "reports"
    return {
        "command_center": [
            _spec("canonical_operator_state", reports / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json"),
            _spec("command_center_queue_audit", reports / "aegis_command_center_queue_audit_v1" / day / "command_center_queue_audit.v1.json"),
        ],
        "engineering": [
            _spec("engineering_priority_queue", reports / "aegis_engineering_priority_queue_v1" / day / "engineering_priority_queue.v1.json"),
            _spec("runtime_truth_kernel", reports / "aegis_runtime_truth_kernel_v1" / day / "runtime_truth_kernel.v1.json"),
            _spec("verified_runtime_graph", reports / "aegis_verified_runtime_graph_v1" / day / "verified_runtime_graph.v1.json"),
        ],
        "positions": [
            _spec("paper_position_ledger", reports / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json"),
            _spec("canonical_operator_state", reports / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json"),
        ],
        "history": [
            _spec("canonical_operator_state", reports / "aegis_canonical_operator_state_v1" / day / "canonical_operator_state.v1.json"),
            _spec("daily_paper_performance", reports / "aegis_daily_paper_performance_v1" / day / "daily_paper_performance.v1.json", required=False),
        ],
        "performance": [
            _spec("paper_pnl_report", reports / "aegis_paper_pnl_report_v1" / day / "paper_pnl_report.v1.json"),
            _spec("daily_paper_performance", reports / "aegis_daily_paper_performance_v1" / day / "daily_paper_performance.v1.json"),
        ],
        "position_review": [
            _spec("position_review_context", reports / "aegis_position_review_context_v1" / day / "position_review_context.v1.json"),
            _spec("position_review_score", reports / "aegis_position_review_score_v1" / day / "position_review_score.v1.json"),
            _spec("position_review_brief", reports / "aegis_position_review_brief_v1" / day / "position_review_brief.v1.json"),
        ],
        "sleeve_analytics": [
            _spec("sleeve_analytics", reports / "aegis_sleeve_analytics_v1" / day / "sleeve_analytics.v1.json"),
        ],
        "research": [
            _spec("research_doctor", reports / "aegis_research_doctor_v1" / day / "research_doctor.v1.json", required=False),
            _spec("hypothesis_qualification", reports / "aegis_hypothesis_qualification_v1" / day / "hypothesis_qualification.v1.json", required=False),
        ],
        "ask_aegis": [
            _spec("ai_operations_context", reports / "aegis_ai_operations_context_v1" / day / "ai_operations_context.v1.json"),
            _spec("ai_operations_response", reports / "aegis_ai_operations_response_v1" / day / "ai_operations_response.v1.json", required=False),
        ],
    }


def _spec(artifact_id: str, path: Path, required: bool = True) -> dict[str, Any]:
    return {"id": artifact_id, "path": path, "required": required}


def _artifact_ref(artifact_id: str, path: Path, required: bool = True) -> dict[str, Any]:
    payload = read_json_v1(path)
    exists = path.exists() and bool(payload)
    day = str(payload.get("day_utc") or payload.get("requested_day") or path.parent.name if exists else "")
    status = str((payload.get("status") or payload.get("surface_status") or payload.get("data_quality_status") or "AVAILABLE") if exists else "MISSING")
    return {
        "artifact_id": artifact_id,
        "path": str(path),
        "exists": exists,
        "required": required,
        "artifact_day": day,
        "status": status,
        "content_hash": _sha256(path) if exists else "",
        "generated_at": payload.get("generated_at") or payload.get("generated_at_utc") or payload.get("as_of") if exists else "",
        "summary": payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {},
        "payload": payload,
    }


def _context_day(surface_id: str, artifacts: list[dict[str, Any]]) -> str:
    if surface_id != "ask_aegis":
        return ""
    for row in artifacts:
        if row["artifact_id"] == "ai_operations_context" and row.get("payload"):
            payload = row["payload"]
            return str(payload.get("day_utc") or payload.get("source_day") or "")
    return ""


def _apply_surface_specific_rules(surface_id: str, artifacts: list[dict[str, Any]], requested_day: str, blocking: list[str], warnings: list[str], details: dict[str, Any]) -> None:
    payloads = {row["artifact_id"]: row.get("payload") if isinstance(row.get("payload"), Mapping) else {} for row in artifacts}
    if surface_id == "command_center":
        queue = payloads.get("command_center_queue_audit", {})
        summary = queue.get("summary") if isinstance(queue.get("summary"), Mapping) else {}
        counts = queue.get("current_command_center_counts") if isinstance(queue.get("current_command_center_counts"), Mapping) else {}
        op_count = int(summary.get("operator_action_required_count") or 0)
        details["operator_action_required_count"] = op_count
        details["awaiting_review_metric"] = int(counts.get("awaiting_review_metric") or op_count)
        if int(summary.get("incorrectly_shown_as_awaiting_review_count") or 0):
            blocking.append("COMMAND_CENTER_QUEUE_MISMATCH:awaiting_review_contains_non_actionable_rows")
        for row in queue.get("rows", []) if isinstance(queue.get("rows"), list) else []:
            if isinstance(row, Mapping) and row.get("classification") == "OPERATOR_ACTION_REQUIRED" and str(row.get("day_boundary_status") or "CURRENT_DAY_SESSION") != "CURRENT_DAY_SESSION":
                blocking.append(f"COMMAND_CENTER_WRONG_DAY_ACTIONABLE:{row.get('symbol') or row.get('candidate_id')}")
                break
    elif surface_id == "sleeve_analytics":
        sleeve = payloads.get("sleeve_analytics", {})
        summary = sleeve.get("summary") if isinstance(sleeve.get("summary"), Mapping) else {}
        status = str(sleeve.get("status") or "")
        dq = str(summary.get("data_quality_status") or (sleeve.get("data_quality") or {}).get("data_quality_status") or "")
        details["artifact_status"] = status
        details["data_quality_status"] = dq
        if status == "NOT_CANONICAL":
            blocking.append("SLEEVE_ANALYTICS_NOT_CANONICAL")
        if status == "NOT_CANONICAL" and dq == "PASS":
            blocking.append("SLEEVE_ANALYTICS_STATUS_CONTRADICTION:NOT_CANONICAL_WITH_PASS")
        if status in {"PARTIAL", "DEGRADED"} or dq in {"PARTIAL", "DEGRADED", "BLOCKED"}:
            warnings.append(f"SLEEVE_ANALYTICS_DEGRADED:{status or dq}")
    elif surface_id == "position_review":
        brief = payloads.get("position_review_brief", {})
        details["brief_count"] = len(brief.get("briefs") or []) if isinstance(brief.get("briefs"), list) else 0
        if brief and str(brief.get("day_utc") or "") != requested_day:
            blocking.append(f"POSITION_REVIEW_DAY_MISMATCH:{brief.get('day_utc')}!={requested_day}")
    elif surface_id == "ask_aegis":
        ctx = payloads.get("ai_operations_context", {})
        rsp = payloads.get("ai_operations_response", {})
        latest = rsp.get("latest_response") if isinstance(rsp.get("latest_response"), Mapping) else {}
        details["context_day"] = str(ctx.get("day_utc") or "")
        details["response_context_day"] = str(latest.get("context_day") or "")
        details["response_source_day"] = str(latest.get("source_day") or "")
        if ctx and str(ctx.get("day_utc") or "") != requested_day:
            blocking.append(f"ASK_AEGIS_CONTEXT_DAY_MISMATCH:{ctx.get('day_utc')}!={requested_day}")
        if latest and (str(latest.get("context_day") or requested_day) != requested_day or str(latest.get("source_day") or requested_day) != requested_day):
            blocking.append("ASK_AEGIS_RESPONSE_DAY_MISMATCH")
    elif surface_id == "engineering":
        kernel = payloads.get("runtime_truth_kernel", {})
        if str(kernel.get("highest_readiness_layer") or "").upper() == "BLOCKED":
            warnings.append("RUNTIME_TRUTH_BLOCKED:engineering_surface_remains_renderable_for_repair")
            details["runtime_truth_classification"] = kernel.get("runtime_truth_classification")
            details["missing_or_stale_source_count"] = kernel.get("missing_or_stale_source_count")
    elif surface_id == "performance":
        for artifact_id, payload in payloads.items():
            if not payload:
                continue
            raw_status = str(payload.get("status") or payload.get("data_quality_status") or "").upper()
            overview = payload.get("overview") if isinstance(payload.get("overview"), Mapping) else {}
            full_status = str(overview.get("full_portfolio_pnl_status") or payload.get("full_portfolio_pnl_status") or "").upper()
            if raw_status in {"NOT_CANONICAL", "BLOCKED"}:
                blocking.append(f"PERFORMANCE_INPUT_NOT_CANONICAL:{artifact_id}")
            if raw_status.startswith("PARTIAL") or raw_status.startswith("DEGRADED") or full_status == "NOT_CANONICAL":
                warnings.append(f"PERFORMANCE_INPUT_DEGRADED:{artifact_id}:{full_status or raw_status}")
    elif surface_id == "research":
        if not any(row.get("exists") for row in artifacts):
            warnings.append("RESEARCH_READINESS_ARTIFACTS_NOT_PRESENT")



def _apply_semantic_invariant_gate(root: Path, requested_day: str, surface_id: str, blocking: list[str], details: dict[str, Any]) -> None:
    if surface_id not in {"performance", "sleeve_analytics"}:
        return
    failures = blocking_failures_for_surface_v1(truth_root=root, day_utc=requested_day, surface_id=surface_id)
    details["semantic_invariant_failure_count"] = len(failures)
    details["semantic_invariant_failures"] = failures
    for row in failures:
        invariant_id = str(row.get("invariant_id") or "unknown")
        reason = str(row.get("reason") or "semantic invariant failed")
        blocking.append(f"SEMANTIC_INVARIANT_FAILED:{invariant_id}:{reason}")

def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""
