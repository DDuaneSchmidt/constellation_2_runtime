from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import now_utc_v1, read_json_v1, write_json_v1
from ops.aegis.semantic_invariants_v1 import build_semantic_invariants_v1, semantic_invariants_path_v1, write_semantic_invariants_v1
from ops.aegis.surface_readiness_v1 import SURFACES, build_surface_readiness_v1, surface_readiness_path_v1, write_surface_readiness_v1

REPORT_FAMILY = "aegis_operator_surface_contract_v1"
REPORT_FILENAME = "operator_surface_contract.v1.json"
STATUSES = {"READY", "DEGRADED", "BLOCKED", "UNAVAILABLE", "HISTORICAL", "INCONSISTENT"}
SAFETY = {
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "live_trading_allowed": False,
    "autonomous_live_trading_allowed": False,
}

SURFACE_MESSAGES = {
    "command_center": ("What requires operator attention?", "Why are there no actionable items today?"),
    "engineering": ("What is broken and how do I repair it?", "Why is runtime blocked?"),
    "positions": ("What positions are active today?", "Why are positions unavailable?"),
    "history": ("What historical records exist today?", "Why is history unavailable?"),
    "performance": ("How did the portfolio perform?", "Why are analytics unavailable?"),
    "position_review": ("What matters most for this position?", "Why is Position Review unavailable?"),
    "sleeve_analytics": ("How are sleeves performing?", "Why are sleeve analytics unavailable?"),
    "research": ("What is being validated?", "Why is this hypothesis waiting?"),
    "ask_aegis": ("What can Aegis explain?", "What should be fixed first?"),
}


def operator_surface_contract_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_operator_surface_contract_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated_at = now_utc_v1()
    readiness = _load_or_build_surface_readiness(root, day)
    semantics = _load_or_build_semantic_invariants(root, day)
    rows = [_contract_row(surface_id=surface_id, readiness=readiness, semantics=semantics, generated_at=generated_at, day=day) for surface_id in SURFACES]
    by_id = {row["surface_id"]: row for row in rows}
    return {
        "schema_id": REPORT_FAMILY,
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "requested_day": day,
        "generated_at": generated_at,
        "summary": {
            "surface_count": len(rows),
            "ready_count": sum(1 for row in rows if row["status"] == "READY"),
            "degraded_count": sum(1 for row in rows if row["status"] == "DEGRADED"),
            "blocked_count": sum(1 for row in rows if row["status"] == "BLOCKED"),
            "unavailable_count": sum(1 for row in rows if row["status"] == "UNAVAILABLE"),
            "historical_count": sum(1 for row in rows if row["status"] == "HISTORICAL"),
            "inconsistent_count": sum(1 for row in rows if row["status"] == "INCONSISTENT"),
            "actions_allowed_count": sum(1 for row in rows if row["actions_allowed"]),
            "metrics_allowed_count": sum(1 for row in rows if row["metrics_allowed"]),
        },
        "surfaces": rows,
        "surface_by_id": by_id,
        "contract_by_id": by_id,
        "source_artifacts": {
            "surface_readiness": str(surface_readiness_path_v1(truth_root=root, day_utc=day)),
            "semantic_invariants": str(semantic_invariants_path_v1(truth_root=root, day_utc=day)),
        },
        "source_artifact_hashes": {
            "surface_readiness": _sha256(surface_readiness_path_v1(truth_root=root, day_utc=day)),
            "semantic_invariants": _sha256(semantic_invariants_path_v1(truth_root=root, day_utc=day)),
        },
        "safety": dict(SAFETY),
        **SAFETY,
    }


def write_operator_surface_contract_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    body = dict(payload or build_operator_surface_contract_v1(truth_root=truth_root, day_utc=day_utc))
    return write_json_v1(operator_surface_contract_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def run_operator_surface_contract_self_check_v1(*, truth_root: Path | str, day_utc: str, repo_root: Path | str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    payload = read_json_v1(operator_surface_contract_path_v1(truth_root=root, day_utc=day))
    if not payload:
        payload = build_operator_surface_contract_v1(truth_root=root, day_utc=day)
        write_operator_surface_contract_v1(truth_root=root, day_utc=day, payload=payload)
    failures: list[dict[str, Any]] = []
    rows = payload.get("surfaces") if isinstance(payload.get("surfaces"), list) else []
    by_id = {str(row.get("surface_id")): row for row in rows if isinstance(row, Mapping)}
    required_fields = {"surface_id", "requested_day", "source_day", "context_day", "status", "render_allowed", "actions_allowed", "primary_message", "reason", "impact", "next_step", "metrics_allowed", "diagnostics_allowed", "ask_aegis_prompt", "evidence_refs", "artifact_refs", "surface_readiness_status", "semantic_invariant_status", "generated_at"}
    for surface_id in SURFACES:
        if surface_id not in by_id:
            failures.append({"check": "required_surface_present", "surface_id": surface_id, "reason": "missing contract row"})
            continue
        row = by_id[surface_id]
        missing = sorted(required_fields - set(row))
        if missing:
            failures.append({"check": "required_contract_fields", "surface_id": surface_id, "reason": f"missing fields: {', '.join(missing)}"})
        if row.get("status") not in STATUSES:
            failures.append({"check": "valid_status", "surface_id": surface_id, "reason": f"invalid status {row.get('status')}"})
        if row.get("actions_allowed") and row.get("surface_readiness_status") != "READY":
            failures.append({"check": "actions_allowed_readiness", "surface_id": surface_id, "reason": "actions allowed while readiness is not READY"})
        if row.get("status") in {"BLOCKED", "UNAVAILABLE", "INCONSISTENT"} and row.get("actions_allowed"):
            failures.append({"check": "blocked_no_actions", "surface_id": surface_id, "reason": "blocked/unavailable/inconsistent surface exposes actions"})
        if row.get("status") == "INCONSISTENT" and row.get("render_allowed") and not str(row.get("primary_message") or "").startswith("Surface consistency check failed"):
            failures.append({"check": "inconsistent_explains_failure", "surface_id": surface_id, "reason": "inconsistent surface does not explain consistency failure"})
        if not row.get("ask_aegis_prompt"):
            failures.append({"check": "ask_aegis_prompt_present", "surface_id": surface_id, "reason": "ask_aegis_prompt missing"})
        if row.get("metrics_allowed") is False and surface_id in {"performance", "sleeve_analytics"} and row.get("status") == "READY":
            failures.append({"check": "ready_metrics_allowed", "surface_id": surface_id, "reason": "READY analytics surface has metrics disabled"})
    ui_failures = _ui_contract_failures(Path(repo_root).resolve() if repo_root else None)
    failures.extend(ui_failures)
    return {
        "ok": not failures,
        "status": "PASS" if not failures else "FAIL",
        "day_utc": day,
        "path": str(operator_surface_contract_path_v1(truth_root=root, day_utc=day)),
        "surface_count": len(rows),
        "failures": failures,
        "summary": payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {},
        "safety": dict(SAFETY),
    }


def _contract_row(*, surface_id: str, readiness: Mapping[str, Any], semantics: Mapping[str, Any], generated_at: str, day: str) -> dict[str, Any]:
    readiness_by_id = readiness.get("surface_by_id") if isinstance(readiness.get("surface_by_id"), Mapping) else {}
    r = readiness_by_id.get(surface_id) if isinstance(readiness_by_id.get(surface_id), Mapping) else {}
    semantic_surface = (semantics.get("surfaces") or {}).get(surface_id) if isinstance(semantics.get("surfaces"), Mapping) else {}
    semantic_failures = semantic_surface.get("failed_invariants") if isinstance(semantic_surface, Mapping) and isinstance(semantic_surface.get("failed_invariants"), list) else []
    blocking_semantic = [row for row in semantic_failures if isinstance(row, Mapping) and row.get("blocking") is True]
    status = str(r.get("surface_status") or "UNAVAILABLE")
    if status not in STATUSES:
        status = "UNAVAILABLE"
    if blocking_semantic:
        status = "BLOCKED"
    primary, prompt = SURFACE_MESSAGES.get(surface_id, ("What is this surface showing?", "Why is this surface unavailable?"))
    reasons = [str(x) for x in (r.get("blocking_reasons") or []) if x]
    warnings = [str(x) for x in (r.get("warning_reasons") or []) if x]
    if blocking_semantic:
        reasons = [f"SEMANTIC_INVARIANT_FAILED:{row.get('invariant_id')}:{row.get('reason')}" for row in blocking_semantic] + reasons
    reason = _reason_for(status, reasons, warnings)
    actions_allowed = bool(r.get("actions_allowed")) and status == "READY"
    render_allowed = bool(r.get("render_allowed", True))
    metrics_allowed = _metrics_allowed(surface_id, status, blocking_semantic, reason)
    diagnostics_allowed = status not in {"INCONSISTENT"}
    if status == "HISTORICAL":
        primary = "Viewing historical operational session."
    elif status == "INCONSISTENT":
        primary = "Surface consistency check failed."
    elif status == "UNAVAILABLE":
        primary = "Unavailable"
    artifacts = r.get("source_artifacts") if isinstance(r.get("source_artifacts"), list) else []
    return {
        "surface_id": surface_id,
        "requested_day": str(r.get("requested_day") or day),
        "source_day": str(r.get("source_day") or day),
        "context_day": str(r.get("context_day") or ""),
        "status": status,
        "render_allowed": render_allowed,
        "actions_allowed": actions_allowed,
        "primary_message": primary,
        "reason": reason,
        "impact": _impact_for(surface_id, status, metrics_allowed, actions_allowed),
        "next_step": _next_step_for(surface_id, status, reason),
        "metrics_allowed": metrics_allowed,
        "diagnostics_allowed": diagnostics_allowed,
        "ask_aegis_prompt": prompt,
        "evidence_refs": _evidence_refs(r, blocking_semantic),
        "artifact_refs": _artifact_refs(artifacts),
        "surface_readiness_status": str(r.get("surface_status") or "UNAVAILABLE"),
        "semantic_invariant_status": str(semantic_surface.get("status") if isinstance(semantic_surface, Mapping) else "NOT_APPLICABLE"),
        "generated_at": generated_at,
        "blocking_reasons": reasons,
        "warning_reasons": warnings,
        "semantic_invariant_failures": blocking_semantic,
        "details": r.get("details") if isinstance(r.get("details"), Mapping) else {},
        "safety": dict(SAFETY),
    }


def _load_or_build_surface_readiness(root: Path, day: str) -> dict[str, Any]:
    path = surface_readiness_path_v1(truth_root=root, day_utc=day)
    payload = read_json_v1(path)
    if not payload:
        payload = build_surface_readiness_v1(truth_root=root, day_utc=day)
        write_surface_readiness_v1(truth_root=root, day_utc=day, payload=payload)
    return payload


def _load_or_build_semantic_invariants(root: Path, day: str) -> dict[str, Any]:
    path = semantic_invariants_path_v1(truth_root=root, day_utc=day)
    payload = read_json_v1(path)
    if not payload:
        payload = build_semantic_invariants_v1(truth_root=root, day_utc=day)
        write_semantic_invariants_v1(truth_root=root, day_utc=day, payload=payload)
    return payload


def _reason_for(status: str, reasons: list[str], warnings: list[str]) -> str:
    if status == "READY":
        return "Surface checks passed for the requested day."
    if status == "DEGRADED":
        return warnings[0] if warnings else "Surface is usable with degraded evidence."
    if status == "BLOCKED":
        return reasons[0] if reasons else "Surface is blocked by readiness or semantic checks."
    if status == "UNAVAILABLE":
        return reasons[0] if reasons else "Required surface artifacts are unavailable."
    if status == "HISTORICAL":
        return reasons[0] if reasons else "Requested day and source day differ."
    if status == "INCONSISTENT":
        return reasons[0] if reasons else "Surface consistency check failed."
    return "Surface status is unavailable."


def _metrics_allowed(surface_id: str, status: str, blocking_semantic: list[Mapping[str, Any]], reason: str) -> bool:
    if surface_id not in {"performance", "sleeve_analytics"}:
        return False
    if blocking_semantic or status in {"BLOCKED", "UNAVAILABLE", "INCONSISTENT", "HISTORICAL"}:
        return False
    return True


def _impact_for(surface_id: str, status: str, metrics_allowed: bool, actions_allowed: bool) -> str:
    if status == "READY":
        return "Normal workflow may render."
    if surface_id in {"performance", "sleeve_analytics"} and not metrics_allowed:
        return "Analytics cards are hidden until canonical inputs and semantic invariants agree."
    if not actions_allowed:
        return "Operator actions are hidden or disabled for this surface."
    return "Surface is rendered with a degraded or explanatory state."


def _next_step_for(surface_id: str, status: str, reason: str) -> str:
    if status == "READY":
        return "Continue normal workflow."
    if status == "DEGRADED":
        return "Review the surface message and expand diagnostics if needed."
    if surface_id == "engineering":
        return "Use Engineering Fix First and Ask Aegis to inspect the repair path."
    if surface_id in {"performance", "sleeve_analytics"}:
        return "Run semantic invariants, surface readiness, and the relevant analytics producer before trusting metrics."
    if surface_id == "ask_aegis":
        return "Refresh the AI operations context for the requested day."
    return "Open Ask Aegis with the surface prompt or inspect the named artifacts."


def _evidence_refs(readiness_row: Mapping[str, Any], semantic_failures: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for reason in (readiness_row.get("blocking_reasons") or []) + (readiness_row.get("warning_reasons") or []):
        refs.append({"kind": "surface_readiness_reason", "value": reason})
    for row in semantic_failures:
        refs.append({"kind": "semantic_invariant", "value": row.get("invariant_id"), "reason": row.get("reason"), "field_values": row.get("field_values")})
    return refs


def _artifact_refs(artifacts: list[Any]) -> list[dict[str, Any]]:
    refs = []
    for row in artifacts:
        if isinstance(row, Mapping):
            refs.append({"artifact_id": row.get("artifact_id"), "path": row.get("path"), "artifact_day": row.get("artifact_day"), "status": row.get("status"), "content_hash": row.get("content_hash")})
    return refs


def _ui_contract_failures(repo_root: Path | None) -> list[dict[str, Any]]:
    if repo_root is None:
        return []
    pages = repo_root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
    domain = repo_root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "domain_client" / "index.js"
    text = _read_text(pages)
    dtext = _read_text(domain)
    failures: list[dict[str, Any]] = []
    if "fetchAegisOperatorSurfaceContract" not in dtext:
        failures.append({"check": "ui_contract_fetcher", "reason": "domain client lacks fetchAegisOperatorSurfaceContract"})
    if "surfaceContractRow" not in text:
        failures.append({"check": "ui_contract_helper", "reason": "operator shell lacks central surfaceContractRow helper"})
    if "if open_positions == 0" in text:
        failures.append({"check": "page_specific_empty_state_logic", "reason": "Python-style page-specific open position empty-state logic detected"})
    return failures


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""
