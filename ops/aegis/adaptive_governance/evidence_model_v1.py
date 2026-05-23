from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import ai_evidence_v1, latest_json_v1, now_utc_v1, write_json_v1


def latest_input_v1(truth_root: Path, family: str, day_utc: str, filename: str) -> tuple[Path | None, dict[str, Any], dict[str, Any]]:
    path, payload = latest_json_v1(truth_root, family, day_utc, filename)
    if not path:
        return None, {}, {"status": "NOT_FOUND", "path": ""}
    if not payload:
        return path, {}, {"status": "INVALID", "path": str(path)}
    return path, payload, {"status": "AVAILABLE", "path": str(path)}


def evidence_quality_v1(input_status: dict[str, dict[str, Any]]) -> str:
    if not input_status:
        return "UNKNOWN"
    available = [row for row in input_status.values() if isinstance(row, dict) and row.get("status") == "AVAILABLE"]
    if not available:
        return "UNKNOWN"
    ratio = len(available) / max(1, len(input_status))
    if ratio >= 0.8:
        return "HIGH"
    if ratio >= 0.5:
        return "MEDIUM"
    return "LOW"


def fact_v1(name: str, value: Any, *, evidence: str, status: str = "OBSERVED") -> dict[str, Any]:
    return {"name": name, "value": value, "evidence": evidence, "status": status}


def metric_v1(name: str, value: Any, *, inputs: list[str], method: str, status: str = "CALCULATED") -> dict[str, Any]:
    return {"name": name, "value": value, "inputs": inputs, "method": method, "status": status}


def interpretation_v1(name: str, summary: str, *, evidence: list[str], metrics_used: list[str], confidence: str) -> dict[str, Any]:
    return {"name": name, "summary": summary, "evidence": evidence, "metrics_used": metrics_used, "confidence": confidence}


def recommendation_v1(
    rec_type: str,
    target: str,
    *,
    evidence: list[str],
    metrics_used: list[str] | None = None,
    interpretation: str = "",
    confidence: str = "UNKNOWN",
    expected_impact: str = "UNKNOWN",
    risk: str = "UNKNOWN",
) -> dict[str, Any]:
    return {
        "recommendation_id": f"{rec_type}:{target}",
        "type": rec_type,
        "target": target,
        "evidence": [item for item in evidence if item],
        "metrics_used": metrics_used or [],
        "interpretation": interpretation or "INSUFFICIENT_EVIDENCE",
        "confidence": confidence,
        "expected_impact": expected_impact,
        "risk": risk,
        "human_approval_required": True,
        "automated_change_allowed": False,
    }


def report_v1(
    *,
    engine_name: str,
    truth_root: Path,
    repo_root: Path,
    day_utc: str,
    input_artifacts: dict[str, str],
    input_artifact_status: dict[str, dict[str, Any]],
    facts: list[dict[str, Any]],
    metrics: list[dict[str, Any]],
    interpretations: list[dict[str, Any]],
    recommendations: list[dict[str, Any]],
    unknowns: list[str],
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
        "input_artifacts": input_artifacts,
        "input_artifact_status": input_artifact_status,
        "evidence_quality": evidence_quality_v1(input_artifact_status),
        "ai_used": ai["ai_used"],
        "deterministic_fallback": ai["deterministic_fallback"],
        "model_used": ai["model_used"],
        "facts": facts,
        "metrics": metrics,
        "interpretations": interpretations,
        "conclusions": interpretations,
        "recommendations": [_safe_recommendation(row) for row in recommendations],
        "unknowns": sorted(set(str(item) for item in unknowns if str(item))),
        "human_approval_required": True,
        "execution_allowed": False,
        "automated_change_allowed": False,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "actions": [],
        "next_operator_actions": [
            "Review facts, metrics, and interpretations before acting.",
            "Approve or reject recommendations manually.",
            "Do not treat adaptive governance output as trade execution.",
        ],
    }
    payload["generated_at_utc"] = payload["generated_at"]
    if extra:
        payload.update(extra)
    return payload


def write_report_v1(*, truth_root: Path, day_utc: str, family: str, filename: str, summary_filename: str, payload: dict[str, Any], title: str) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / family / day_utc
    json_path = write_json_v1(out_dir / filename, payload)
    summary_path = out_dir / summary_filename
    summary_path.write_text(render_summary_v1(payload, title=title), encoding="utf-8")
    return {"path": str(json_path), "summary_path": str(summary_path)}


def render_summary_v1(payload: dict[str, Any], *, title: str) -> str:
    lines = [
        title,
        f"day_utc: {payload.get('day_utc')}",
        f"engine_name: {payload.get('engine_name')}",
        f"evidence_quality: {payload.get('evidence_quality')}",
        f"ai_used: {str(payload.get('ai_used')).lower()}",
        f"deterministic_fallback: {str(payload.get('deterministic_fallback')).lower()}",
        f"facts: {len(payload.get('facts') or [])}",
        f"metrics: {len(payload.get('metrics') or [])}",
        f"interpretations: {len(payload.get('interpretations') or [])}",
        f"recommendations: {len(payload.get('recommendations') or [])}",
        f"unknowns: {', '.join(payload.get('unknowns') or []) or 'NONE'}",
        "human_approval_required: true",
        "automated_change_allowed: false",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
        "recommendations:",
    ]
    for row in payload.get("recommendations") or [{"type": "NO_ACTION", "target": "SYSTEM", "confidence": "UNKNOWN", "interpretation": "No recommendation."}]:
        lines.append(f"- {row.get('type')}: target={row.get('target')} confidence={row.get('confidence')} interpretation={row.get('interpretation')}")
    lines.append("")
    return "\n".join(lines)


def _safe_recommendation(row: dict[str, Any]) -> dict[str, Any]:
    safe = dict(row)
    safe.setdefault("human_approval_required", True)
    safe.setdefault("automated_change_allowed", False)
    return safe
