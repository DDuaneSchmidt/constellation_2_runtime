from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.adaptive_intelligence.common_v1 import latest_input_v1, recommendation_v1, standard_payload_v1


def build_event_interpretation_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    inputs = {
        "event_monitoring_status": latest_input_v1(truth_root, "event_monitoring_status_v1", day_utc, "event_monitoring_status.v1.json"),
        "event_validity_gate": latest_input_v1(truth_root, "event_validity_gate_v1", day_utc, "event_validity_gate.v1.json"),
        "event_market_snapshot": latest_input_v1(truth_root, "event_market_snapshot_v1", day_utc, "event_market_snapshot.v1.json"),
        "regime_detection": latest_input_v1(truth_root, "regime_detection_v1", day_utc, "regime_detection.v1.json"),
        "sleeve_attribution": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json"),
    }
    validity = inputs["event_validity_gate"][1]
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    if validity.get("validity_status") == "NO_EVENT_PACKET":
        event_importance = "NONE"
        expected = "NONE"
        advisory = "NO_EVENT_DRIVEN_ACTION"
        risk = "NO_EVENT_PACKET_EVALUATED"
        research = "No event-driven research task required from this packet."
        recommendation = "NO_ACTION"
        confidence = "MEDIUM"
    elif validity.get("validity_status") == "VALID":
        event_importance = "MEDIUM"
        expected = "MIXED"
        advisory = "HUMAN_REVIEW_REQUIRED_BEFORE_ANY_ADVISORY_USE"
        risk = "EVENT_CONTEXT_ACTIVE"
        research = "Review post-event sleeve outcomes."
        recommendation = "REVIEW_EVENT_CONTEXT"
        confidence = "LOW"
    elif validity:
        event_importance = "UNKNOWN"
        expected = "UNKNOWN"
        advisory = "EVENT_CONTEXT_INVALID_OR_STALE"
        risk = "DO_NOT_USE_EVENT_CONTEXT_FOR_ADVICE"
        research = "Investigate event validity evidence."
        recommendation = "REQUIRE_MORE_EVIDENCE"
        confidence = "UNKNOWN"
    else:
        event_importance = "UNKNOWN"
        expected = "UNKNOWN"
        advisory = "EVENT_CONTEXT_NOT_FOUND"
        risk = "DO_NOT_USE_EVENT_CONTEXT_FOR_ADVICE"
        research = "Generate event validity evidence."
        recommendation = "REQUIRE_MORE_EVIDENCE"
        confidence = "UNKNOWN"
    unknowns = [key for key, row in statuses.items() if row["status"] != "AVAILABLE"]
    details = {
        "event_importance": event_importance,
        "expected_market_impact": expected,
        "affected_sleeves": [],
        "advisory_implication": advisory,
        "risk_implication": risk,
        "research_implication": research,
        "trade_advice_generated": False,
    }
    return standard_payload_v1(
        engine_name="event_interpretation",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        conclusions=[{"type": "EVENT_INTERPRETATION", "summary": advisory, "confidence": confidence}],
        recommendations=[recommendation_v1(recommendation, target="EVENT_CONTEXT", evidence=[path for path in artifacts.values() if path], confidence=confidence, reason=advisory)],
        unknowns=unknowns,
        next_operator_actions=["Do not generate trade advice from event interpretation alone.", "Use NO_EVENT_PACKET as evaluated absence, not as an actionable event."],
        extra=details,
    )
