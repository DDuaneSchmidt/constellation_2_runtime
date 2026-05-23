from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1


def build_risk_governance_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    kernel_path, kernel = latest_json_v1(truth_root, "aegis_runtime_truth_kernel_v1", day_utc, "runtime_truth_kernel.v1.json")
    attribution_path, attribution = latest_json_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json")
    event_path, event_snapshot = latest_json_v1(truth_root, "event_market_snapshot_v1", day_utc, "event_market_snapshot.v1.json")
    manual_path, manual = latest_json_v1(truth_root, "manual_execution_receipt_v1", day_utc, "manual_execution_receipt.v1.json")
    reasons: list[str] = []
    affected: list[str] = []
    missing: list[str] = []
    if not kernel:
        missing.append("runtime_truth_kernel")
    if not attribution:
        missing.append("sleeve_attribution")
    if not event_snapshot:
        missing.append("event_market_snapshot")
    for sleeve in attribution.get("sleeves", []) if isinstance(attribution.get("sleeves"), list) else []:
        status = str(sleeve.get("status") or "UNKNOWN")
        if status in {"DEGRADED", "SUSPEND_CANDIDATE", "UNKNOWN"}:
            affected.append(str(sleeve.get("sleeve_id") or "UNKNOWN"))
            reasons.append(f"SLEEVE_STATUS:{status}")
    if kernel and kernel.get("runtime_truth_classification") != "REAL_RUNTIME":
        reasons.append("RUNTIME_TRUTH_NOT_REAL_RUNTIME")
    if missing:
        risk_state = "UNKNOWN"
    elif any("SUSPEND_CANDIDATE" in reason for reason in reasons):
        risk_state = "BLOCKED"
    elif any("DEGRADED" in reason for reason in reasons):
        risk_state = "ELEVATED"
    elif reasons:
        risk_state = "WATCH"
    else:
        risk_state = "NORMAL"
    return {
        "schema_id": "aegis_risk_governance",
        "schema_version": "v1",
        "artifact_id": "aegis_risk_governance_v1",
        "day_utc": day_utc,
        "generated_at_utc": now_utc_v1(),
        "risk_state": risk_state,
        "risk_reasons": sorted(set(reasons)) or ["NO_RISK_WARNINGS_FOUND"],
        "sleeves_affected": sorted(set(affected)),
        "advisory_restrictions": _restrictions(risk_state),
        "manual_capture_notes": "Manual capture remains journaling/audit only.",
        "recommended_operator_caution": _caution(risk_state),
        "missing_risk_evidence": missing,
        "input_artifacts": {
            "runtime_truth_kernel": str(kernel_path or ""),
            "sleeve_attribution": str(attribution_path or ""),
            "event_market_snapshot": str(event_path or ""),
            "manual_execution_receipt": str(manual_path or ""),
        },
        "manual_receipt_status": manual.get("result") or "UNKNOWN",
        "safety": {
            "may_block_advisory_when_blocked": True,
            "may_enable_trading": False,
            "may_execute_trades": False,
            "broker_submit_required": False,
            "autonomous_execution_allowed": False,
        },
    }


def render_risk_governance_summary_v1(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "AEGIS RISK GOVERNANCE v1",
            f"day_utc: {payload.get('day_utc')}",
            f"risk_state: {payload.get('risk_state')}",
            f"risk_reasons: {', '.join(payload.get('risk_reasons') or [])}",
            f"sleeves_affected: {', '.join(payload.get('sleeves_affected') or []) or 'NONE'}",
            f"advisory_restrictions: {', '.join(payload.get('advisory_restrictions') or []) or 'NONE'}",
            f"missing_risk_evidence: {', '.join(payload.get('missing_risk_evidence') or []) or 'NONE'}",
            "Safety: report only; may not enable trading; may not execute anything.",
            "",
        ]
    )


def _restrictions(risk_state: str) -> list[str]:
    if risk_state == "BLOCKED":
        return ["BLOCK_NEW_ADVISORY_RECOMMENDATIONS_UNTIL_REVIEWED"]
    if risk_state in {"ELEVATED", "WATCH", "UNKNOWN"}:
        return ["HUMAN_REVIEW_REQUIRED", "DO_NOT_INCREASE_RISK"]
    return []


def _caution(risk_state: str) -> str:
    if risk_state == "NORMAL":
        return "Continue normal human-approved advisory review."
    if risk_state == "UNKNOWN":
        return "Resolve missing risk evidence before increasing reliance."
    if risk_state == "BLOCKED":
        return "Do not use advisory candidates until risk blockers are reviewed."
    return "Use reduced confidence and review affected sleeves."
