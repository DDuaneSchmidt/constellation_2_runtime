from __future__ import annotations

import hashlib
import statistics
from datetime import datetime, timezone
from typing import Any

from .historical_replay_governance import validate_historical_replay_allowed
from .historical_replay_models import (
    HISTORICAL_REPLAY_EVIDENCE_LEVEL,
    HISTORICAL_REPLAY_INPUT_TYPES,
    HISTORICAL_REPLAY_WINDOWS,
    HistoricalReplayCertification,
    HistoricalReplayEvidence,
    HistoricalReplayRequest,
    HistoricalReplayResult,
    HistoricalReplaySummary,
)

MIN_REPLAY_SAMPLE_SIZE = 5


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_id(prefix: str, values: list[Any]) -> str:
    material = "|".join(str(value) for value in values)
    return f"{prefix}_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:16]}"


def create_historical_replay_request(
    *,
    hypothesis_id: str,
    mechanism_tags: list[str],
    regime_context: dict[str, Any] | None = None,
    time_window: str = "1y",
    source_artifact_ids: list[str] | None = None,
    input_type: str = "ResearchHypothesis",
    replay_id: str | None = None,
    created_at: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if input_type not in HISTORICAL_REPLAY_INPUT_TYPES:
        raise ValueError(f"invalid historical replay input_type: {input_type}")
    if time_window not in HISTORICAL_REPLAY_WINDOWS:
        raise ValueError(f"invalid historical replay time_window: {time_window}")
    row = HistoricalReplayRequest(
        replay_id=replay_id or stable_id("hist_replay", [hypothesis_id, mechanism_tags, time_window, source_artifact_ids or []]),
        hypothesis_id=hypothesis_id,
        mechanism_tags=sorted(set(mechanism_tags)),
        regime_context=dict(regime_context or {}),
        time_window=time_window,
        sample_size=0,
        result_count=0,
        evidence_level=HISTORICAL_REPLAY_EVIDENCE_LEVEL,
        created_at=created_at or now_utc(),
        source_artifact_ids=list(source_artifact_ids or []),
        metadata={"input_type": input_type, "research_only": True, **dict(metadata or {})},
        input_type=input_type,
    ).to_dict()
    validate_historical_replay_allowed(row)
    return row


def request_from_replay_input(payload: dict[str, Any], *, time_window: str = "1y", replay_id: str | None = None, created_at: str | None = None) -> dict[str, Any]:
    input_type = str(payload.get("input_type") or payload.get("artifact_type") or "ResearchHypothesis")
    if input_type == "Mechanism Variation":
        input_type = "MechanismVariation"
    if input_type == "Failure Analysis":
        input_type = "FailureAnalysis"
    return create_historical_replay_request(
        hypothesis_id=str(payload.get("hypothesis_id") or payload.get("candidate_id") or payload.get("experiment_id") or payload.get("failure_id") or "unknown-hypothesis"),
        mechanism_tags=list(payload.get("mechanism_tags", [])),
        regime_context=payload.get("regime_context", {}) if isinstance(payload.get("regime_context", {}), dict) else {"label": payload.get("regime_context")},
        time_window=time_window,
        source_artifact_ids=list(payload.get("source_artifact_ids", [])),
        input_type=input_type,
        replay_id=replay_id,
        created_at=created_at,
        metadata={"source_input_type": input_type, **dict(payload.get("metadata", {}) if isinstance(payload.get("metadata"), dict) else {})},
    )


def run_historical_replay(request: dict[str, Any] | HistoricalReplayRequest, samples: list[dict[str, Any]], *, created_at: str | None = None) -> dict[str, Any]:
    row = request.to_dict() if isinstance(request, HistoricalReplayRequest) else dict(request)
    validate_historical_replay_allowed(row)
    metrics = calculate_replay_metrics(samples)
    result_count = len(samples)
    sample_size = int(metrics["sample_size"])
    created = created_at or row.get("created_at") or now_utc()
    evidence = HistoricalReplayEvidence(
        replay_id=row["replay_id"],
        hypothesis_id=row["hypothesis_id"],
        mechanism_tags=list(row.get("mechanism_tags", [])),
        regime_context=dict(row.get("regime_context", {})),
        time_window=row["time_window"],
        sample_size=sample_size,
        result_count=result_count,
        evidence_level=HISTORICAL_REPLAY_EVIDENCE_LEVEL,
        created_at=created,
        source_artifact_ids=list(row.get("source_artifact_ids", [])),
        metadata={"research_only": True, "sample_count_input": len(samples), **dict(row.get("metadata", {}))},
        metrics=metrics,
    ).to_dict()
    certification = certify_historical_replay(evidence, metrics=metrics, created_at=created)
    result = HistoricalReplayResult(
        replay_id=row["replay_id"],
        hypothesis_id=row["hypothesis_id"],
        mechanism_tags=list(row.get("mechanism_tags", [])),
        regime_context=dict(row.get("regime_context", {})),
        time_window=row["time_window"],
        sample_size=sample_size,
        result_count=result_count,
        evidence_level=HISTORICAL_REPLAY_EVIDENCE_LEVEL,
        created_at=created,
        source_artifact_ids=list(row.get("source_artifact_ids", [])),
        metadata={"research_only": True, "input_type": row.get("input_type"), **dict(row.get("metadata", {}))},
        metrics=metrics,
        evidence=evidence,
        certification=certification,
    ).to_dict()
    validate_historical_replay_allowed(result)
    return result


def calculate_replay_metrics(samples: list[dict[str, Any]]) -> dict[str, Any]:
    returns = [_sample_return(row) for row in samples if _sample_return(row) is not None]
    sample_size = len(returns)
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    regime_groups: dict[str, list[float]] = {}
    for row in samples:
        value = _sample_return(row)
        if value is None:
            continue
        regime = str(row.get("regime") or row.get("regime_context") or "UNKNOWN")
        regime_groups.setdefault(regime, []).append(value)
    regime_performance = {
        regime: {
            "sample_size": len(values),
            "win_rate": _rate(sum(1 for value in values if value > 0), len(values)),
            "average_return": _average(values),
            "expectancy": _average(values),
        }
        for regime, values in sorted(regime_groups.items())
    }
    total_win = sum(wins)
    total_loss = abs(sum(losses))
    metrics = {
        "sample_size": sample_size,
        "win_rate": _rate(len(wins), sample_size),
        "expectancy": _average(returns),
        "profit_factor": round(total_win / total_loss, 6) if total_loss > 0 else (None if total_win == 0 else round(total_win, 6)),
        "average_return": _average(returns),
        "median_return": round(statistics.median(returns), 6) if returns else None,
        "max_drawdown": _max_drawdown(returns),
        "regime_specific_performance": regime_performance,
        "survival_rate": _rate(len(wins), sample_size),
        "failure_rate": _rate(len(losses), sample_size),
    }
    metrics["regime_consistency"] = _regime_consistency(regime_performance)
    metrics["historical_replay_score"] = score_historical_replay(metrics)
    return metrics


def score_historical_replay(metrics: dict[str, Any]) -> float:
    sample_size = int(metrics.get("sample_size") or 0)
    if sample_size <= 0:
        return 0.0
    sample_score = min(1.0, sample_size / 50.0)
    expectancy_score = _clamp((float(metrics.get("expectancy") or 0.0) + 0.03) / 0.08)
    win_score = _clamp(metrics.get("win_rate"))
    profit_factor = metrics.get("profit_factor")
    profit_score = 0.5 if profit_factor is None else _clamp(float(profit_factor) / 2.0)
    consistency_score = _clamp(metrics.get("regime_consistency"))
    failure_inverse = 1.0 - _clamp(metrics.get("failure_rate"))
    return round(
        0.20 * sample_score
        + 0.25 * expectancy_score
        + 0.15 * win_score
        + 0.15 * profit_score
        + 0.15 * consistency_score
        + 0.10 * failure_inverse,
        6,
    )


def certify_historical_replay(evidence: dict[str, Any], *, metrics: dict[str, Any] | None = None, created_at: str | None = None) -> dict[str, Any]:
    row = dict(evidence)
    metrics = metrics or row.get("metrics", {})
    sample_size = int(metrics.get("sample_size") or row.get("sample_size") or 0)
    score = float(metrics.get("historical_replay_score") or 0.0)
    expectancy = float(metrics.get("expectancy") or 0.0)
    failure_rate = float(metrics.get("failure_rate") or 0.0)
    reasons: list[str] = []
    if sample_size < MIN_REPLAY_SAMPLE_SIZE:
        status = "INSUFFICIENT_SAMPLE"
        reasons.append(f"sample_size below {MIN_REPLAY_SAMPLE_SIZE}")
    elif score >= 0.65 and expectancy > 0 and failure_rate <= 0.45:
        status = "REPLAY_POSITIVE"
        reasons.append("historical replay score, expectancy, and failure rate support further review")
    elif score <= 0.35 or (expectancy < 0 and failure_rate >= 0.55):
        status = "REPLAY_NEGATIVE"
        reasons.append("historical replay score, expectancy, or failure rate shows weakness")
    else:
        status = "REPLAY_NEUTRAL"
        reasons.append("historical replay evidence is mixed")
    cert = HistoricalReplayCertification(
        replay_id=row["replay_id"],
        hypothesis_id=row["hypothesis_id"],
        mechanism_tags=list(row.get("mechanism_tags", [])),
        regime_context=dict(row.get("regime_context", {})),
        time_window=row["time_window"],
        sample_size=sample_size,
        result_count=int(row.get("result_count", sample_size)),
        evidence_level=HISTORICAL_REPLAY_EVIDENCE_LEVEL,
        created_at=created_at or row.get("created_at") or now_utc(),
        source_artifact_ids=list(row.get("source_artifact_ids", [])),
        status=status,
        reasons=reasons,
        metadata={"historical_replay_score": score, "research_only": True},
    ).to_dict()
    validate_historical_replay_allowed(cert)
    return cert


def summarize_historical_replay(result: dict[str, Any]) -> dict[str, Any]:
    metrics = result.get("metrics", {})
    certification = result.get("certification", {})
    return HistoricalReplaySummary(
        replay_id=result["replay_id"],
        hypothesis_id=result["hypothesis_id"],
        mechanism_tags=list(result.get("mechanism_tags", [])),
        regime_context=dict(result.get("regime_context", {})),
        time_window=result["time_window"],
        sample_size=int(result.get("sample_size", 0)),
        result_count=int(result.get("result_count", 0)),
        evidence_level=HISTORICAL_REPLAY_EVIDENCE_LEVEL,
        created_at=result["created_at"],
        source_artifact_ids=list(result.get("source_artifact_ids", [])),
        metadata={"research_only": True},
        status=str(certification.get("status", "INSUFFICIENT_SAMPLE")),
        score=float(metrics.get("historical_replay_score") or 0.0),
        metrics=dict(metrics),
    ).to_dict()


def _sample_return(row: dict[str, Any]) -> float | None:
    for key in ["return", "return_pct", "pnl_pct", "outcome_return"]:
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
    equity = 1.0
    peak = 1.0
    drawdown = 0.0
    for value in returns:
        equity *= 1.0 + value
        peak = max(peak, equity)
        drawdown = min(drawdown, (equity - peak) / peak)
    return round(drawdown, 6)


def _regime_consistency(regime_performance: dict[str, Any]) -> float:
    if not regime_performance:
        return 0.0
    positive = 0
    measured = 0
    for row in regime_performance.values():
        expectancy = row.get("expectancy")
        if expectancy is None:
            continue
        measured += 1
        if float(expectancy) >= 0:
            positive += 1
    return round(positive / measured, 6) if measured else 0.0


def _clamp(value: Any) -> float:
    if value is None:
        return 0.0
    return min(1.0, max(0.0, float(value)))
