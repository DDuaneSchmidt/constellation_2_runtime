from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_quality_metrics import CANDIDATE_QUALITY_EVIDENCE_WEIGHTS
from .failure_patterns import get_repeated_failures
from .learning_validation_governance import validate_learning_validation_allowed
from .learning_validation_models import (
    CERTIFICATION_RESULTS,
    CONTINUOUS_LEARNING_VALIDATION_CERTIFICATION,
    LearningImprovement,
    LearningPlateau,
    LearningRegression,
    LearningTrend,
    LearningValidationSnapshot,
    TIME_HORIZONS,
)
from .memory_index import list_memory_objects
from .regime_context import list_regime_contexts
from .semantic_deduplication import find_potential_duplicates

DECREASING_IS_BETTER = {"failure_reduction_rate", "duplicate_reduction_rate", "regime_gap_reduction_rate"}
INCREASING_IS_BETTER = {"evidence_maturity_growth", "hypothesis_survival_growth", "candidate_quality_growth"}
METRIC_NAMES = sorted(DECREASING_IS_BETTER | INCREASING_IS_BETTER)
PLATEAU_EPSILON = 0.000001


def build_learning_validation_snapshot(root: str | Path = DEFAULT_STORE_ROOT, *, snapshot_id: str = "learning-validation-snapshot", observed_at: str | None = None) -> dict[str, Any]:
    observed = observed_at or _now()
    memory_rows = list_memory_objects(root)
    repeated_failures = len(get_repeated_failures(root, 2))
    duplicates = len([row for row in find_potential_duplicates(root) if row.get("status") == "LIKELY_DUPLICATE"])
    regime_by_id = {row["regime_context_id"]: row for row in list_regime_contexts(root)}
    regime_gaps = 0
    for memory in memory_rows:
        contexts = [regime_by_id.get(context_id, {}) for context_id in memory.get("regime_context_ids", [])]
        if not contexts or any("UNKNOWN" in context.get("labels", []) for context in contexts):
            regime_gaps += 1
    evidence_score = _average([CANDIDATE_QUALITY_EVIDENCE_WEIGHTS.get(row.get("evidence_level", ""), 0.0) for row in memory_rows])
    return LearningValidationSnapshot(
        snapshot_id=snapshot_id,
        observed_at=observed,
        repeated_failures=float(repeated_failures),
        duplicate_ideas=float(duplicates),
        regime_gaps=float(regime_gaps),
        evidence_maturity_score=evidence_score,
        hypothesis_survival_rate=_latest_candidate_quality_value(root, "hypothesis_survival_rate"),
        candidate_quality_score=_latest_candidate_quality_score(root),
        source_artifact_ids=sorted({artifact_id for row in memory_rows for artifact_id in row.get("source_artifact_ids", [])}),
        source_memory_ids=sorted({row["memory_id"] for row in memory_rows}),
        metadata={"measurement_only": True, "authority_boundary": "NO_TRADING_OR_PROMOTION_AUTHORITY"},
    ).to_dict()


def validate_learning_over_time(snapshots: list[dict[str, Any]], *, horizon: str = "30_day", as_of: str | None = None) -> dict[str, Any]:
    if horizon not in TIME_HORIZONS:
        raise ValueError(f"invalid horizon: {horizon}")
    scoped = filter_snapshots_for_horizon(snapshots, horizon=horizon, as_of=as_of)
    metrics = compute_learning_trend_metrics(scoped)
    outputs = classify_learning_outputs(metrics)
    trend_direction = certify_learning_validation(metrics, sample_count=len(scoped))["result"]
    confidence = _confidence(len(scoped), metrics)
    limitations = []
    if len(scoped) < 2:
        limitations.append("At least two learning validation snapshots are required.")
    if horizon != "lifetime":
        limitations.append(f"Trend is scoped to {horizon}; longer horizons may differ.")
    report = LearningTrend(horizon=horizon, trend_direction=trend_direction, metrics=metrics, confidence=confidence, limitations=limitations).to_dict()
    payload = {
        "schema_id": "atlas_v2_research_os_learning_validation.v1",
        "schema_version": "v1",
        "certification": certify_learning_validation(metrics, sample_count=len(scoped)),
        "trend": report,
        "trend_direction": trend_direction,
        "improvements": outputs["improvements"],
        "regressions": outputs["regressions"],
        "plateaus": outputs["plateaus"],
        "confidence": confidence,
        "limitations": limitations,
        "snapshot_count": len(scoped),
        "questions_answered": {
            "are_repeated_failures_decreasing": _answer(metrics.get("failure_reduction_rate")),
            "are_duplicate_ideas_decreasing": _answer(metrics.get("duplicate_reduction_rate")),
            "are_regime_gaps_decreasing": _answer(metrics.get("regime_gap_reduction_rate")),
            "is_evidence_maturity_increasing": _answer(metrics.get("evidence_maturity_growth")),
            "is_hypothesis_survival_improving": _answer(metrics.get("hypothesis_survival_growth")),
            "is_candidate_quality_improving": _answer(metrics.get("candidate_quality_growth")),
        },
        "metadata": {"measurement_only": True, "no_authority_created": True},
    }
    validate_learning_validation_allowed({**payload, "certification_result": trend_direction})
    return payload


def compute_learning_trend_metrics(snapshots: list[dict[str, Any]]) -> dict[str, float | None]:
    rows = sorted(snapshots, key=lambda row: row["observed_at"])
    if len(rows) < 2:
        return {name: None for name in METRIC_NAMES}
    first = rows[0]
    last = rows[-1]
    return {
        "failure_reduction_rate": _reduction(first.get("repeated_failures"), last.get("repeated_failures")),
        "duplicate_reduction_rate": _reduction(first.get("duplicate_ideas"), last.get("duplicate_ideas")),
        "regime_gap_reduction_rate": _reduction(first.get("regime_gaps"), last.get("regime_gaps")),
        "evidence_maturity_growth": _growth(first.get("evidence_maturity_score"), last.get("evidence_maturity_score")),
        "hypothesis_survival_growth": _growth(first.get("hypothesis_survival_rate"), last.get("hypothesis_survival_rate")),
        "candidate_quality_growth": _growth(first.get("candidate_quality_score"), last.get("candidate_quality_score")),
    }


def classify_learning_outputs(metrics: dict[str, float | None]) -> dict[str, list[dict[str, Any]]]:
    improvements: list[dict[str, Any]] = []
    regressions: list[dict[str, Any]] = []
    plateaus: list[dict[str, Any]] = []
    for name, value in metrics.items():
        if value is None:
            plateaus.append(LearningPlateau(name, None, "insufficient comparable data").to_dict())
        elif value > PLATEAU_EPSILON:
            improvements.append(LearningImprovement(name, value, "metric moved in the learning-improving direction").to_dict())
        elif value < -PLATEAU_EPSILON:
            regressions.append(LearningRegression(name, value, "metric moved against learning quality").to_dict())
        else:
            plateaus.append(LearningPlateau(name, value, "metric was effectively unchanged").to_dict())
    return {"improvements": improvements, "regressions": regressions, "plateaus": plateaus}


def certify_learning_validation(metrics: dict[str, float | None], *, sample_count: int) -> dict[str, Any]:
    if sample_count < 2 or any(value is None for value in metrics.values()):
        result = "INSUFFICIENT_DATA"
    else:
        positives = sum(1 for value in metrics.values() if value is not None and value > PLATEAU_EPSILON)
        negatives = sum(1 for value in metrics.values() if value is not None and value < -PLATEAU_EPSILON)
        if negatives > positives:
            result = "REGRESSING"
        elif positives > negatives:
            result = "IMPROVING"
        else:
            result = "STABLE"
    return {
        "certification_type": CONTINUOUS_LEARNING_VALIDATION_CERTIFICATION,
        "result": result,
        "authority_boundary": "Measurement only; no trading, capital, candidate promotion, sleeve, portfolio, or position sizing authority.",
    }


def filter_snapshots_for_horizon(snapshots: list[dict[str, Any]], *, horizon: str, as_of: str | None = None) -> list[dict[str, Any]]:
    if horizon == "lifetime":
        return list(snapshots)
    days = {"7_day": 7, "30_day": 30, "90_day": 90}[horizon]
    end = _parse_time(as_of or max((row["observed_at"] for row in snapshots), default=_now()))
    start = end - timedelta(days=days)
    return [row for row in snapshots if start <= _parse_time(row["observed_at"]) <= end]


def _reduction(first: Any, last: Any) -> float | None:
    if first is None or last is None:
        return None
    first_f = float(first)
    last_f = float(last)
    if first_f == 0:
        return 0.0 if last_f == 0 else round(-last_f, 6)
    return round((first_f - last_f) / abs(first_f), 6)


def _growth(first: Any, last: Any) -> float | None:
    if first is None or last is None:
        return None
    return round(float(last) - float(first), 6)


def _confidence(sample_count: int, metrics: dict[str, float | None]) -> float:
    available = sum(1 for value in metrics.values() if value is not None)
    return round(min(1.0, (sample_count / 4.0) * (available / max(1, len(metrics)))), 6)


def _answer(value: float | None) -> str:
    if value is None:
        return "INSUFFICIENT_DATA"
    if value > PLATEAU_EPSILON:
        return "YES"
    if value < -PLATEAU_EPSILON:
        return "NO"
    return "STABLE"


def _average(values: list[float]) -> float:
    return round(sum(values) / len(values), 6) if values else 0.0


def _latest_candidate_quality_value(root: str | Path, metric_name: str) -> float:
    latest = _latest_candidate_quality_evaluation(root)
    if not latest:
        return 0.0
    return float(latest.get("evaluation", {}).get("metric_set", {}).get("treatment", {}).get(metric_name, {}).get("value") or 0.0)


def _latest_candidate_quality_score(root: str | Path) -> float:
    latest = _latest_candidate_quality_evaluation(root)
    if not latest:
        return 0.0
    metrics = latest.get("evaluation", {}).get("metric_set", {}).get("treatment", {})
    values = [
        metrics.get("candidate_conversion_rate", {}).get("value"),
        metrics.get("portfolio_scoring_pass_rate", {}).get("value"),
        metrics.get("evidence_maturity_score", {}).get("value"),
        metrics.get("hypothesis_survival_rate", {}).get("value"),
    ]
    return _average([float(value) for value in values if value is not None])


def _latest_candidate_quality_evaluation(root: str | Path) -> dict[str, Any] | None:
    quality_root = Path(root) / "candidate_quality"
    candidates = sorted(quality_root.rglob("*.json")) if quality_root.exists() else []
    for path in reversed(candidates):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict) and "evaluation" in payload:
            return payload
    return None


def _parse_time(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
