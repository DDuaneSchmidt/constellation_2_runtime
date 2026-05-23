from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.adaptive_intelligence.common_v1 import latest_input_v1, recommendation_v1, standard_payload_v1


def build_cross_sleeve_interaction_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    inputs = {
        "sleeve_attribution": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json"),
        "sleeve_health_scores": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_health_scores.v1.json"),
        "regime_detection": latest_input_v1(truth_root, "regime_detection_v1", day_utc, "regime_detection.v1.json"),
        "research_memory_graph": latest_input_v1(truth_root, "research_memory_graph_v1", day_utc, "research_memory_graph.v1.json"),
    }
    attribution = inputs["sleeve_attribution"][1]
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    sleeves = [row for row in attribution.get("sleeves", []) if isinstance(row, dict)] if isinstance(attribution.get("sleeves"), list) else []
    warnings = []
    if len(sleeves) < 2:
        warnings.append({"type": "CORRELATION_UNKNOWN", "confidence": "UNKNOWN", "reason": "Fewer than two sleeves with attribution evidence; no cross-sleeve correlation can be inferred."})
    unknowns = [key for key, row in statuses.items() if row["status"] != "AVAILABLE"]
    if not warnings:
        warnings.append({"type": "INSUFFICIENT_CORRELATION_EVIDENCE", "confidence": "UNKNOWN", "reason": "No pairwise return series was found; correlations are not computed."})
    return standard_payload_v1(
        engine_name="cross_sleeve_interaction",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        conclusions=[{"type": row["type"], "summary": row["reason"], "confidence": row["confidence"]} for row in warnings],
        recommendations=[recommendation_v1("REVIEW_CROSS_SLEEVE_RISK", target="SLEEVE_SET", confidence="UNKNOWN", reason="Do not infer redundancy or correlation without pairwise evidence.")],
        unknowns=unknowns + ["PAIRWISE_CORRELATION_DATA"],
        next_operator_actions=["Treat cross-sleeve correlations as UNKNOWN until pairwise data exists.", "Review concentration manually before increasing sleeve trust."],
        extra={
            "redundant_sleeves": [],
            "conflicting_signals": [],
            "correlated_failure_patterns": [],
            "regime_overlap": "UNKNOWN",
            "concentration_risk": "UNKNOWN",
            "sleeves_that_should_not_be_trusted_together": [],
            "complementary_sleeves": [],
            "interaction_warnings": warnings,
        },
    )
