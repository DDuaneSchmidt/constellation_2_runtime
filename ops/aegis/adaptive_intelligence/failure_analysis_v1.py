from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.adaptive_intelligence.common_v1 import latest_input_v1, recommendation_v1, standard_payload_v1


FAILURE_TYPES = {"REGIME_MISMATCH", "SIGNAL_DECAY", "OVERFIT_RISK", "EXECUTION_DRIFT", "OPERATOR_OVERRIDE", "EVENT_SHOCK", "DATA_QUALITY", "INSUFFICIENT_EVIDENCE", "UNKNOWN"}


def build_failure_analysis_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    inputs = {
        "sleeve_attribution": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json"),
        "manual_execution_receipt": latest_input_v1(truth_root, "manual_execution_receipt_v1", day_utc, "manual_execution_receipt.v1.json"),
        "eod_intelligence": latest_input_v1(truth_root, "aegis_eod_intelligence_v1", day_utc, "eod_intelligence.v1.json"),
        "eow_intelligence": latest_input_v1(truth_root, "aegis_eow_intelligence_v1", day_utc, "eow_intelligence.v1.json"),
        "regime_detection": latest_input_v1(truth_root, "regime_detection_v1", day_utc, "regime_detection.v1.json"),
    }
    attribution = inputs["sleeve_attribution"][1]
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    patterns = []
    for sleeve in attribution.get("sleeves", []) if isinstance(attribution.get("sleeves"), list) else []:
        if not isinstance(sleeve, dict):
            continue
        status = str(sleeve.get("status") or "UNKNOWN")
        if status in {"DEGRADED", "SUSPEND_CANDIDATE"}:
            kind = "SIGNAL_DECAY"
        elif status == "UNKNOWN":
            kind = "INSUFFICIENT_EVIDENCE"
        else:
            continue
        patterns.append(_pattern(kind, [str(sleeve.get("sleeve_id") or "UNKNOWN")], [], [artifacts.get("sleeve_attribution", "")], "Needs investigation; causality is not proven.", "LOW" if kind == "SIGNAL_DECAY" else "UNKNOWN"))
    if not patterns:
        patterns.append(_pattern("INSUFFICIENT_EVIDENCE", [], [], [path for path in artifacts.values() if path], "No proven failure pattern found.", "UNKNOWN"))
    recommendations = [recommendation_v1("INVESTIGATE_FAILURE", target=",".join(row["affected_sleeves"]) or "SYSTEM", evidence=row["evidence"], confidence=row["confidence"], reason=row["suspected_cause"]) for row in patterns]
    unknowns = [key for key, row in statuses.items() if row["status"] != "AVAILABLE"]
    return standard_payload_v1(
        engine_name="failure_analysis",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        conclusions=[{"type": row["failure_type"], "summary": row["suspected_cause"], "confidence": row["confidence"]} for row in patterns],
        recommendations=recommendations,
        unknowns=unknowns,
        next_operator_actions=["Review suspected failures separately from proven causality.", "Open research tasks for failure patterns with LOW or UNKNOWN confidence."],
        extra={"failure_patterns": patterns, "failure_type_catalog": sorted(FAILURE_TYPES), "causality_claim_policy": "DO_NOT_CLAIM_CAUSALITY_WITHOUT_DIRECT_EVIDENCE"},
    )


def _pattern(kind: str, sleeves: list[str], hypotheses: list[str], evidence: list[str], cause: str, confidence: str) -> dict[str, Any]:
    return {
        "failure_type": kind,
        "affected_sleeves": sleeves,
        "affected_hypotheses": hypotheses,
        "evidence": [item for item in evidence if item],
        "suspected_cause": cause,
        "proven_cause": "NOT_PROVEN",
        "confidence": confidence,
        "recommended_action": "CREATE_RESEARCH_TASK" if kind != "INSUFFICIENT_EVIDENCE" else "COLLECT_MORE_EVIDENCE",
        "research_task_candidate": kind != "UNKNOWN",
    }
