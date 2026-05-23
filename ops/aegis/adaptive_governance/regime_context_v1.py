from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.adaptive_governance.evidence_model_v1 import fact_v1, interpretation_v1, latest_input_v1, metric_v1, recommendation_v1, report_v1


def build_regime_context_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    inputs = {
        "event_market_snapshot": latest_input_v1(truth_root, "event_market_snapshot_v1", day_utc, "event_market_snapshot.v1.json"),
        "event_monitoring_status": latest_input_v1(truth_root, "event_monitoring_status_v1", day_utc, "event_monitoring_status.v1.json"),
        "runtime_truth": latest_input_v1(truth_root, "aegis_runtime_truth_kernel_v1", day_utc, "runtime_truth_kernel.v1.json"),
        "sleeve_performance": latest_input_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json"),
        "eod": latest_input_v1(truth_root, "aegis_eod_intelligence_v1", day_utc, "eod_intelligence.v1.json"),
        "eow": latest_input_v1(truth_root, "aegis_eow_intelligence_v1", day_utc, "eow_intelligence.v1.json"),
    }
    statuses = {key: value[2] for key, value in inputs.items()}
    artifacts = {key: str(value[0] or "") for key, value in inputs.items()}
    event_monitor = inputs["event_monitoring_status"][1]
    triggered = _count(event_monitor.get("triggered_events")) if event_monitor else 0
    blocked = _count(event_monitor.get("blocked_events")) if event_monitor else 0
    event_regime = "EVENT_ACTIVE" if triggered else "EVENT_RISK" if blocked else "QUIET" if event_monitor else "UNKNOWN"
    facts = [
        fact_v1("event_monitor_present", bool(event_monitor), evidence=artifacts["event_monitoring_status"]),
        fact_v1("runtime_truth_present", bool(inputs["runtime_truth"][1]), evidence=artifacts["runtime_truth"]),
    ]
    metrics = [
        metric_v1("triggered_event_count", triggered if event_monitor else "INSUFFICIENT_DATA", inputs=[artifacts["event_monitoring_status"]], method="count triggered_events"),
        metric_v1("blocked_event_count", blocked if event_monitor else "INSUFFICIENT_DATA", inputs=[artifacts["event_monitoring_status"]], method="count blocked_events"),
    ]
    classifications = {
        "volatility_regime": "UNKNOWN",
        "trend_regime": "UNKNOWN",
        "liquidity_regime": "UNKNOWN",
        "breadth_regime": "UNKNOWN",
        "event_regime": event_regime,
    }
    interpretations = [
        interpretation_v1("event_regime", f"event_regime={event_regime}", evidence=[artifacts["event_monitoring_status"]], metrics_used=["triggered_event_count", "blocked_event_count"], confidence="LOW" if event_regime != "UNKNOWN" else "UNKNOWN"),
        interpretation_v1("market_context_unknowns", "Volatility, trend, liquidity, and breadth remain UNKNOWN without explicit market metrics.", evidence=[path for path in artifacts.values() if path], metrics_used=[], confidence="UNKNOWN"),
    ]
    unknowns = [key for key, row in statuses.items() if row["status"] != "AVAILABLE"] + [key for key, value in classifications.items() if value == "UNKNOWN"]
    return report_v1(
        engine_name="regime_context",
        truth_root=truth_root,
        repo_root=repo_root,
        day_utc=day_utc,
        input_artifacts=artifacts,
        input_artifact_status=statuses,
        facts=facts,
        metrics=metrics,
        interpretations=interpretations,
        recommendations=[recommendation_v1("REQUIRE_MORE_EVIDENCE", "MARKET_CONTEXT", evidence=[path for path in artifacts.values() if path], metrics_used=["triggered_event_count", "blocked_event_count"], interpretation="Regime context is partial unless explicit market metrics exist.", confidence="LOW")],
        unknowns=unknowns,
        extra={"regime_classifications": classifications},
    )


def _count(value: Any) -> int:
    if isinstance(value, list):
        return len(value)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
