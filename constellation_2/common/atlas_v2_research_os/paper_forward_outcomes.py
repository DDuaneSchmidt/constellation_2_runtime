from __future__ import annotations

import hashlib
import json
import statistics
from datetime import datetime, timezone
from typing import Any

from .paper_forward_outcome_governance import validate_paper_forward_outcome_allowed
from .paper_forward_outcome_models import (
    PAPER_FORWARD_EVIDENCE_LEVEL,
    CandidateSurvivalAnalytics,
    PaperForwardObservationPlan,
    PaperForwardObservationResult,
)

MIN_PAPER_FORWARD_SAMPLE_SIZE = 5


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_id(prefix: str, values: list[Any]) -> str:
    material = json.dumps(values, sort_keys=True, default=str)
    return f"{prefix}_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:16]}"


def create_paper_forward_observation_plan(
    *,
    candidate_id: str,
    observation_start: str,
    observation_end: str,
    regime_context: dict[str, Any] | None = None,
    source_artifact_ids: list[str] | None = None,
    mechanism_tags: list[str] | None = None,
    minimum_sample_size: int = MIN_PAPER_FORWARD_SAMPLE_SIZE,
    plan_id: str | None = None,
    created_at: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plan = PaperForwardObservationPlan(
        plan_id=plan_id or stable_id("paper_forward_plan", [candidate_id, observation_start, observation_end]),
        candidate_id=candidate_id,
        observation_start=observation_start,
        observation_end=observation_end,
        regime_context=dict(regime_context or {}),
        source_artifact_ids=list(source_artifact_ids or []),
        mechanism_tags=sorted(set(mechanism_tags or [])),
        minimum_sample_size=minimum_sample_size,
        status="PENDING",
        created_at=created_at or now_utc(),
        metadata={"measurement_only": True, "research_only": True, **dict(metadata or {})},
    ).to_dict()
    validate_paper_forward_outcome_allowed({"metadata": plan["metadata"]})
    return plan


def activate_paper_forward_observation_plan(plan: dict[str, Any]) -> dict[str, Any]:
    row = dict(plan)
    row["status"] = "ACTIVE_OBSERVATION"
    return PaperForwardObservationPlan(**row).to_dict()


def record_paper_forward_observation_result(
    plan: dict[str, Any] | PaperForwardObservationPlan,
    samples: list[dict[str, Any]],
    *,
    created_at: str | None = None,
    notes: list[str] | None = None,
    status_override: str | None = None,
) -> dict[str, Any]:
    row = plan.to_dict() if isinstance(plan, PaperForwardObservationPlan) else dict(plan)
    validate_paper_forward_outcome_allowed({"metadata": row.get("metadata", {})})
    metrics = calculate_paper_forward_metrics(samples)
    sample_size = int(metrics["sample_size"])
    minimum_sample_size = int(row.get("minimum_sample_size") or MIN_PAPER_FORWARD_SAMPLE_SIZE)
    status, hypothesis_supported, hypothesis_weakened, hypothesis_falsified, reasons = classify_paper_forward_outcome(
        metrics,
        minimum_sample_size=minimum_sample_size,
    )
    if status_override is not None:
        status = status_override
    result = PaperForwardObservationResult(
        candidate_id=row["candidate_id"],
        plan_id=row["plan_id"],
        observation_start=row["observation_start"],
        observation_end=row["observation_end"],
        sample_size=sample_size,
        wins=int(metrics["wins"]),
        losses=int(metrics["losses"]),
        average_return=metrics["average_return"],
        expectancy=metrics["expectancy"],
        max_drawdown=metrics["max_drawdown"],
        profit_factor=metrics["profit_factor"],
        regime_context=dict(row.get("regime_context", {})),
        hypothesis_supported=hypothesis_supported,
        hypothesis_weakened=hypothesis_weakened,
        hypothesis_falsified=hypothesis_falsified,
        notes=list(notes or []) + reasons,
        status=status,
        created_at=created_at or now_utc(),
        source_artifact_ids=list(row.get("source_artifact_ids", [])) + [row["plan_id"]],
        mechanism_tags=list(row.get("mechanism_tags", [])),
        evidence_level=PAPER_FORWARD_EVIDENCE_LEVEL,
        metadata={
            "measurement_only": True,
            "research_only": True,
            "minimum_sample_size": minimum_sample_size,
            "metrics": metrics,
            **dict(row.get("metadata", {})),
        },
    ).to_dict()
    validate_paper_forward_outcome_allowed(result)
    return result


def calculate_paper_forward_metrics(samples: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [_sample_return(row) for row in samples if _sample_return(row) is not None]
    sample_size = len(returns)
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    total_win = sum(wins)
    total_loss = abs(sum(losses))
    regime_groups: dict[str, list[float]] = {}
    for row in samples:
        value = _sample_return(row)
        if value is None:
            continue
        regime = str(row.get("regime") or row.get("regime_context") or "UNKNOWN")
        regime_groups.setdefault(regime, []).append(value)
    return {
        "sample_size": sample_size,
        "wins": len(wins),
        "losses": len(losses),
        "average_return": _average(returns),
        "expectancy": _average(returns),
        "max_drawdown": _max_drawdown(returns),
        "profit_factor": round(total_win / total_loss, 6) if total_loss > 0 else (None if total_win == 0 else round(total_win, 6)),
        "survival_rate": _rate(len(wins), sample_size),
        "failure_rate": _rate(len(losses), sample_size),
        "regime_specific_performance": {
            regime: {
                "sample_size": len(values),
                "average_return": _average(values),
                "expectancy": _average(values),
                "survival_rate": _rate(sum(1 for value in values if value > 0), len(values)),
                "failure_rate": _rate(sum(1 for value in values if value < 0), len(values)),
            }
            for regime, values in sorted(regime_groups.items())
        },
    }


def classify_paper_forward_outcome(
    metrics: dict[str, Any],
    *,
    minimum_sample_size: int = MIN_PAPER_FORWARD_SAMPLE_SIZE,
) -> tuple[str, bool, bool, bool, list[str]]:
    sample_size = int(metrics.get("sample_size") or 0)
    expectancy = float(metrics.get("expectancy") or 0.0)
    failure_rate = float(metrics.get("failure_rate") or 0.0)
    survival_rate = float(metrics.get("survival_rate") or 0.0)
    max_drawdown = float(metrics.get("max_drawdown") or 0.0)
    if sample_size == 0:
        return "PENDING", False, False, False, ["no paper-forward samples recorded"]
    if sample_size < minimum_sample_size:
        return "NEEDS_MORE_DATA", False, False, False, [f"sample_size below {minimum_sample_size}"]
    if expectancy > 0 and survival_rate >= 0.55 and failure_rate <= 0.45:
        return "SURVIVED", True, False, False, ["paper-forward observation supports continued research tracking"]
    if expectancy < 0 and failure_rate >= 0.70:
        return "FALSIFIED", False, False, True, ["paper-forward observation falsified the hypothesis in this sample"]
    if expectancy < 0 or max_drawdown <= -0.05 or failure_rate >= 0.55:
        return "WEAKENED", False, True, False, ["paper-forward observation weakened the hypothesis"]
    return "NEEDS_MORE_DATA", False, False, False, ["paper-forward observation is mixed"]


def build_candidate_survival_analytics(result: dict[str, Any] | PaperForwardObservationResult) -> dict[str, Any]:
    row = result.to_dict() if isinstance(result, PaperForwardObservationResult) else dict(result)
    validate_paper_forward_outcome_allowed(row)
    metrics = dict(row.get("metadata", {}).get("metrics", {}))
    analytics = CandidateSurvivalAnalytics(
        candidate_id=row["candidate_id"],
        plan_id=row["plan_id"],
        status=row["status"],
        sample_size=int(row["sample_size"]),
        survival_rate=metrics.get("survival_rate"),
        failure_rate=metrics.get("failure_rate"),
        expectancy=row.get("expectancy"),
        average_return=row.get("average_return"),
        max_drawdown=row.get("max_drawdown"),
        profit_factor=row.get("profit_factor"),
        hypothesis_supported=bool(row.get("hypothesis_supported")),
        hypothesis_weakened=bool(row.get("hypothesis_weakened")),
        hypothesis_falsified=bool(row.get("hypothesis_falsified")),
        regime_context=dict(row.get("regime_context", {})),
        notes=list(row.get("notes", [])),
        metadata={
            "measurement_only": True,
            "source_outcome_status": row["status"],
            "candidate_id": row["candidate_id"],
        },
    ).to_dict()
    validate_paper_forward_outcome_allowed(analytics)
    return analytics


def _sample_return(row: dict[str, Any]) -> float | None:
    for key in ["return", "return_pct", "pnl_pct", "outcome_return", "paper_return"]:
        if row.get(key) is not None:
            return float(row[key])
    outcome = row.get("outcome")
    if outcome == "WIN":
        return 0.01
    if outcome == "LOSS":
        return -0.01
    return None


def _average(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def _max_drawdown(returns: list[float]) -> float | None:
    if not returns:
        return None
    equity = 0.0
    peak = 0.0
    drawdowns = []
    for value in returns:
        equity += value
        peak = max(peak, equity)
        drawdowns.append(equity - peak)
    return round(min(drawdowns), 6)
