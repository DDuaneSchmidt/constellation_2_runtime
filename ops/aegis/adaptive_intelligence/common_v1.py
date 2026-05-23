from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import ai_evidence_v1, latest_json_v1, now_utc_v1


QUALITY_ORDER = {"UNKNOWN": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}


def artifact_status_v1(path: Path | None, payload: dict[str, Any]) -> dict[str, Any]:
    if not path:
        return {"status": "NOT_FOUND", "path": ""}
    if not payload:
        return {"status": "INVALID", "path": str(path)}
    return {"status": "AVAILABLE", "path": str(path)}


def latest_input_v1(truth_root: Path, family: str, day_utc: str, filename: str) -> tuple[Path | None, dict[str, Any], dict[str, Any]]:
    path, payload = latest_json_v1(truth_root, family, day_utc, filename)
    return path, payload, artifact_status_v1(path, payload)


def evidence_quality_v1(statuses: dict[str, dict[str, Any]]) -> str:
    if not statuses:
        return "UNKNOWN"
    available = [row for row in statuses.values() if isinstance(row, dict) and row.get("status") == "AVAILABLE"]
    if not available:
        return "UNKNOWN"
    ratio = len(available) / max(1, len(statuses))
    if ratio >= 0.8:
        return "HIGH"
    if ratio >= 0.5:
        return "MEDIUM"
    return "LOW"


def standard_payload_v1(
    *,
    engine_name: str,
    truth_root: Path,
    repo_root: Path,
    day_utc: str,
    input_artifacts: dict[str, str],
    input_artifact_status: dict[str, dict[str, Any]],
    conclusions: list[dict[str, Any]],
    recommendations: list[dict[str, Any]],
    unknowns: list[str],
    next_operator_actions: list[str],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ai = ai_evidence_v1(repo_root)
    payload: dict[str, Any] = {
        "schema_id": engine_name,
        "schema_version": "v1",
        "artifact_id": f"{engine_name}_v1",
        "engine_name": engine_name,
        "generated_at": now_utc_v1(),
        "generated_at_utc": None,
        "day_utc": day_utc,
        "truth_root": str(Path(truth_root).resolve()),
        "ai_used": ai["ai_used"],
        "deterministic_fallback": ai["deterministic_fallback"],
        "model_used": ai["model_used"],
        "live_ai_call_path_found": ai["live_ai_call_path_found"],
        "input_artifacts": input_artifacts,
        "input_artifact_status": input_artifact_status,
        "evidence_quality": evidence_quality_v1(input_artifact_status),
        "conclusions": conclusions,
        "recommendations": [_safe_recommendation(row) for row in recommendations],
        "human_approval_required": True,
        "execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "unknowns": sorted(set(str(item) for item in unknowns if str(item))),
        "next_operator_actions": next_operator_actions,
        "safety": {
            "advisory_only": True,
            "human_approval_required": True,
            "execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
        },
    }
    payload["generated_at_utc"] = payload["generated_at"]
    if extra:
        payload.update(extra)
    return payload


def render_standard_summary_v1(payload: dict[str, Any], *, title: str) -> str:
    lines = [
        title,
        f"day_utc: {payload.get('day_utc')}",
        f"engine_name: {payload.get('engine_name')}",
        f"evidence_quality: {payload.get('evidence_quality')}",
        f"ai_used: {str(payload.get('ai_used')).lower()}",
        f"deterministic_fallback: {str(payload.get('deterministic_fallback')).lower()}",
        f"recommendation_count: {len(payload.get('recommendations') or [])}",
        f"unknowns: {', '.join(payload.get('unknowns') or []) or 'NONE'}",
        "execution_allowed: false",
        "broker_submit_transmit_allowed: false",
        "autonomous_execution_allowed: false",
        "",
        "conclusions:",
    ]
    for row in payload.get("conclusions") or [{"type": "NONE", "summary": "No conclusions generated."}]:
        lines.append(f"- {row.get('type') or row.get('classification') or 'CONCLUSION'}: {row.get('summary') or row.get('reason') or row.get('status') or 'UNKNOWN'}")
    lines.extend(["", "recommendations:"])
    for row in payload.get("recommendations") or [{"type": "NO_ACTION", "reason": "No recommendation generated."}]:
        lines.append(f"- {row.get('type') or row.get('recommendation')}: target={row.get('target') or row.get('sleeve_id') or row.get('item_id') or 'SYSTEM'} confidence={row.get('confidence') or 'UNKNOWN'} reason={row.get('reason') or row.get('recommended_action') or 'UNKNOWN'}")
    lines.extend(["", "next_operator_actions:"])
    lines.extend(f"- {item}" for item in payload.get("next_operator_actions") or ["Review generated JSON for evidence details."])
    lines.append("")
    return "\n".join(lines)


def recommendation_v1(
    rec_type: str,
    *,
    target: str = "SYSTEM",
    evidence: list[str] | None = None,
    confidence: str = "UNKNOWN",
    expected_impact: str = "UNKNOWN",
    risk: str = "UNKNOWN",
    reason: str = "INSUFFICIENT_EVIDENCE",
) -> dict[str, Any]:
    return {
        "type": rec_type,
        "target": target,
        "evidence": evidence or [],
        "confidence": confidence,
        "expected_impact": expected_impact,
        "risk": risk,
        "reason": reason,
        "human_approval_required": True,
        "automated_change_allowed": False,
        "execution_allowed": False,
    }


def _safe_recommendation(row: dict[str, Any]) -> dict[str, Any]:
    safe = dict(row)
    safe.setdefault("human_approval_required", True)
    safe.setdefault("automated_change_allowed", False)
    safe.setdefault("execution_allowed", False)
    return safe


def get_path(statuses: dict[str, dict[str, Any]], name: str) -> str:
    row = statuses.get(name) if isinstance(statuses.get(name), dict) else {}
    return str(row.get("path") or "")
