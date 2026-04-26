from __future__ import annotations

from typing import Any, Mapping

from .schema_v1 import (
    IB_RECONCILIATION_AI_PACKET_SCHEMA_VERSION,
    IB_RECONCILIATION_AI_REVIEW_SCHEMA_VERSION,
    REVIEW_STATUS_VALUES,
    utc_now_z_v1,
    validate_ai_packet_v1,
    validate_ai_review_v1,
)


AI_PACKET_QUESTIONS = [
    "Is this safe to ignore?",
    "Is this likely expected broker behavior?",
    "Is this likely Aegis execution bug?",
    "Is this recurring?",
    "Should Codex investigate?",
    "Should David be alerted?",
]


def _first_text(*values: Any, default: str = "") -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return default


def build_ai_exception_packet_v1(*, day_utc: str, reconciliation_payload: Mapping[str, Any], reconciliation_path: str) -> dict[str, Any]:
    mismatches_raw = reconciliation_payload.get("mismatches")
    mismatches = [dict(item) for item in mismatches_raw if isinstance(item, Mapping)] if isinstance(mismatches_raw, list) else []
    payload = {
        "schema_version": IB_RECONCILIATION_AI_PACKET_SCHEMA_VERSION,
        "day_utc": day_utc,
        "reconciliation_status": _first_text(reconciliation_payload.get("status"), default="FAIL"),
        "reconciliation_path": str(reconciliation_path),
        "high_mismatch_count": len([m for m in mismatches if _first_text(m.get("severity")) == "HIGH"]),
        "medium_mismatch_count": len([m for m in mismatches if _first_text(m.get("severity")) == "MEDIUM"]),
        "questions": list(AI_PACKET_QUESTIONS),
        "mismatches": mismatches,
        "guardrails": [
            "AI reviews exceptions only.",
            "AI cannot override deterministic match outcomes.",
            "AI cannot change PASS/WARN/FAIL status.",
            "AI cannot place trades.",
            "If reconciliation status is FAIL, AI cannot mark the result safe with NO_ACTION.",
        ],
        "generated_utc": utc_now_z_v1(),
    }
    validate_ai_packet_v1(payload)
    return payload


def render_ai_exception_packet_markdown_v1(packet_payload: Mapping[str, Any]) -> str:
    lines = [
        "# IB Reconciliation AI Exception Packet v1",
        "",
        f"- day_utc: {_first_text(packet_payload.get('day_utc'))}",
        f"- reconciliation_status: {_first_text(packet_payload.get('reconciliation_status'))}",
        f"- reconciliation_path: {_first_text(packet_payload.get('reconciliation_path'))}",
        f"- high_mismatch_count: {_first_text(packet_payload.get('high_mismatch_count'))}",
        f"- medium_mismatch_count: {_first_text(packet_payload.get('medium_mismatch_count'))}",
        "",
        "## Required Review Questions",
    ]
    for idx, question in enumerate(packet_payload.get("questions", []), start=1):
        lines.append(f"{idx}. {question}")

    lines.extend(["", "## Deterministic Mismatches"])
    mismatches = packet_payload.get("mismatches")
    if isinstance(mismatches, list) and mismatches:
        for row in mismatches:
            if not isinstance(row, Mapping):
                continue
            lines.append(
                "- "
                + " | ".join(
                    [
                        _first_text(row.get("mismatch_id"), default="UNKNOWN_ID"),
                        _first_text(row.get("type"), default="UNKNOWN_TYPE"),
                        _first_text(row.get("severity"), default="UNKNOWN_SEVERITY"),
                        _first_text(row.get("symbol"), default="UNKNOWN_SYMBOL"),
                        _first_text(row.get("description"), default=""),
                    ]
                )
            )
    else:
        lines.append("- No mismatches in reconciliation payload.")

    lines.extend(["", "## Guardrails"])
    for item in packet_payload.get("guardrails", []):
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def default_ai_review_template_v1(day_utc: str) -> dict[str, Any]:
    payload = {
        "schema_version": IB_RECONCILIATION_AI_REVIEW_SCHEMA_VERSION,
        "day_utc": day_utc,
        "review_status": "MONITOR",
        "primary_risk": "UNASSESSED",
        "findings": [],
        "recommended_action": "Review deterministic mismatches and decide operational response.",
        "codex_task": None,
        "human_alert_required": False,
        "generated_utc": utc_now_z_v1(),
    }
    validate_ai_review_v1(payload)
    return payload


def enforce_ai_review_guardrails_v1(*, reconciliation_payload: Mapping[str, Any], ai_review_payload: Mapping[str, Any]) -> None:
    validate_ai_review_v1(ai_review_payload)
    review_status = _first_text(ai_review_payload.get("review_status"), default="")
    if review_status not in REVIEW_STATUS_VALUES:
        raise RuntimeError(f"AI_REVIEW_STATUS_INVALID:{review_status}")
    reconciliation_status = _first_text(reconciliation_payload.get("status"), default="FAIL")
    if reconciliation_status == "FAIL" and review_status == "NO_ACTION":
        raise RuntimeError("AI_REVIEW_FORBIDDEN_NO_ACTION_ON_FAIL")
    if "deterministic_status_override" in ai_review_payload:
        raise RuntimeError("AI_REVIEW_STATUS_OVERRIDE_FORBIDDEN")
    if "match_override" in ai_review_payload:
        raise RuntimeError("AI_REVIEW_MATCH_OVERRIDE_FORBIDDEN")


def build_alert_candidate_v1(
    *,
    day_utc: str,
    reconciliation_payload: Mapping[str, Any],
    ai_review_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    mismatches_raw = reconciliation_payload.get("mismatches")
    mismatches = [dict(item) for item in mismatches_raw if isinstance(item, Mapping)] if isinstance(mismatches_raw, list) else []
    has_high = any(_first_text(row.get("severity")) == "HIGH" for row in mismatches)
    reconciliation_status = _first_text(reconciliation_payload.get("status"), default="FAIL")

    ai_action_required = False
    human_alert_required = has_high or reconciliation_status == "FAIL"
    if isinstance(ai_review_payload, Mapping):
        enforce_ai_review_guardrails_v1(reconciliation_payload=reconciliation_payload, ai_review_payload=ai_review_payload)
        ai_action_required = _first_text(ai_review_payload.get("review_status")) == "ACTION_REQUIRED"
        human_alert_required = bool(ai_review_payload.get("human_alert_required")) or human_alert_required

    if not (reconciliation_status == "FAIL" or has_high or ai_action_required):
        return None

    severity = "HIGH" if reconciliation_status == "FAIL" or has_high else "MEDIUM"
    evidence_paths = sorted(
        {
            str(path).strip()
            for row in mismatches
            if isinstance(row, Mapping)
            for path in (row.get("evidence_paths") or [])
            if str(path).strip()
        }
    )
    if not evidence_paths:
        evidence_paths.append(_first_text(reconciliation_payload.get("aegis_expected_path"), default=""))
    return {
        "day_utc": day_utc,
        "severity": severity,
        "subject": f"IB Reconciliation {reconciliation_status} for {day_utc}",
        "body": (
            f"Deterministic IB reconciliation status is {reconciliation_status}. "
            f"High mismatches: {len([m for m in mismatches if _first_text(m.get('severity')) == 'HIGH'])}. "
            f"AI action required: {'yes' if ai_action_required else 'no'}."
        ),
        "evidence_paths": [p for p in evidence_paths if p],
        "human_alert_required": bool(human_alert_required),
    }
