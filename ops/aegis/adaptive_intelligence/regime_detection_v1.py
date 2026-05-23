from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.adaptive_intelligence.common_v1 import latest_input_v1, recommendation_v1, standard_payload_v1


def build_regime_detection_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    inputs = {
        "event_market_snapshot": latest_input_v1(truth_root, "event_market_snapshot_v1", day_utc, "event_market_snapshot.v1.json"),
        "event_monitoring_status": latest_input_v1(truth_root, "event_monitoring_status_v1", day_utc, "event_monitoring_status.v1.json"),
        "sleeve_attribution": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json"),
        "runtime_truth_snapshot": latest_input_v1(truth_root, "aegis_runtime_state_snapshots_v1", day_utc, "runtime_state_snapshot.v1.json"),
        "eod_intelligence": latest_input_v1(truth_root, "aegis_eod_intelligence_v1", day_utc, "eod_intelligence.v1.json"),
        "eow_intelligence": latest_input_v1(truth_root, "aegis_eow_intelligence_v1", day_utc, "eow_intelligence.v1.json"),
    }
    payloads = {key: value[1] for key, value in inputs.items()}
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    unknowns = [key for key, row in statuses.items() if row["status"] != "AVAILABLE"]
    event_status = _event_regime(payloads.get("event_monitoring_status", {}), payloads.get("event_market_snapshot", {}))
    classifications = {
        "volatility_regime": _classification("volatility_regime", "UNKNOWN", "No explicit volatility metric was found.", unknowns, []),
        "trend_regime": _classification("trend_regime", "UNKNOWN", "No explicit trend or breadth metric was found.", unknowns, []),
        "liquidity_regime": _classification("liquidity_regime", "UNKNOWN", "No explicit liquidity stress metric was found.", unknowns, []),
        "event_regime": event_status,
        "breadth_regime": _classification("breadth_regime", "UNKNOWN", "No explicit breadth metric was found.", unknowns, []),
    }
    confidence = "LOW" if event_status["classification"] != "UNKNOWN" else "UNKNOWN"
    conclusions = [
        {"type": key, "classification": row["classification"], "summary": row["reason"], "confidence": confidence if key == "event_regime" else "UNKNOWN"}
        for key, row in classifications.items()
    ]
    return standard_payload_v1(
        engine_name="regime_detection",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        conclusions=conclusions,
        recommendations=[recommendation_v1("REVIEW_REGIME_CONTEXT", evidence=[path for path in artifacts.values() if path], confidence=confidence, reason="Regime labels are context only and must be reviewed before advisory reliance.")],
        unknowns=unknowns,
        next_operator_actions=["Review regime classifications before interpreting sleeve behavior.", "Do not treat UNKNOWN regime fields as evidence.", "Run event interpretation after regime detection."],
        extra={"regime_classifications": classifications, "confidence": confidence},
    )


def _event_regime(event_monitor: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    triggered = _count(event_monitor.get("triggered_events")) if isinstance(event_monitor, dict) else 0
    blocked = _count(event_monitor.get("blocked_events")) if isinstance(event_monitor, dict) else 0
    if triggered > 0:
        value = "EVENT_ACTIVE"
        reason = "Event monitor reported triggered events."
    elif blocked > 0:
        value = "EVENT_RISK"
        reason = "Event monitor reported blocked events."
    elif event_monitor or snapshot:
        value = "QUIET"
        reason = "Event evidence exists and no triggered/blocked event count was reported."
    else:
        value = "UNKNOWN"
        reason = "No event monitor or market snapshot evidence was found."
    return _classification("event_regime", value, reason, [] if value != "UNKNOWN" else ["event_monitoring_status", "event_market_snapshot"], [])


def _classification(name: str, value: str, reason: str, missing: list[str], impact: list[str]) -> dict[str, Any]:
    return {"field": name, "classification": value, "evidence": [], "reason": reason, "missing_data": missing, "impact_on_sleeves": impact}


def _count(value: Any) -> int:
    if isinstance(value, list):
        return len(value)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
