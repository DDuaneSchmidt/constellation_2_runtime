from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from .edge_qualification_models import EDGE_ELIGIBILITY_THRESHOLD, EDGE_SCORE_WEIGHTS, EDGE_SCORE_WEIGHTS_WITH_HISTORICAL_REPLAY, EdgeQualificationInput, EdgeQualificationResult, EdgeQualificationScore
from .paper_trade_candidate_governance import validate_candidate_governance


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_id(prefix: str, values: list[str]) -> str:
    material = "|".join(str(value) for value in values)
    return f"{prefix}_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:16]}"


def compute_edge_score(edge_input: EdgeQualificationInput | dict[str, Any]) -> dict[str, Any]:
    row = _as_input(edge_input)
    candidate_quality_signal = _clamp((row.candidate_quality_trend + row.learning_validation_trend) / 2.0)
    components = {
        "evidence_maturity": _clamp(row.evidence_maturity),
        "research_effectiveness": _clamp(row.research_effectiveness),
        "hypothesis_survival": _clamp(row.hypothesis_survival),
        "candidate_quality_signal": candidate_quality_signal,
        "regime_coverage": _clamp(row.regime_coverage),
        "failure_penalty_inverse": 1.0 - _clamp(row.failure_history),
        "duplicate_penalty_inverse": 1.0 - _clamp(row.duplicate_risk),
        "lineage_completeness": 1.0 if row.lineage_complete else 0.0,
    }
    weights = EDGE_SCORE_WEIGHTS
    if row.historical_replay_score is not None:
        components["historical_replay"] = _historical_replay_component(row)
        weights = EDGE_SCORE_WEIGHTS_WITH_HISTORICAL_REPLAY
    score = round(sum(weights[key] * value for key, value in components.items()), 6)
    return EdgeQualificationScore(edge_score=score, components=components, weights=dict(weights), explanation=explain_edge_score(components, score)).to_dict()


def explain_edge_score(edge_input_or_components: EdgeQualificationInput | dict[str, Any], edge_score: float | None = None) -> list[str]:
    if isinstance(edge_input_or_components, dict) and "edge_score" not in edge_input_or_components:
        components = dict(edge_input_or_components)
        weights = EDGE_SCORE_WEIGHTS_WITH_HISTORICAL_REPLAY if "historical_replay" in components else EDGE_SCORE_WEIGHTS
    else:
        score = compute_edge_score(edge_input_or_components)
        components = score["components"]
        weights = score.get("weights", EDGE_SCORE_WEIGHTS)
        edge_score = score["edge_score"]
    lines = [f"edge_score={edge_score}"]
    for key in weights:
        if key in components:
            lines.append(f"{key} contribution={round(weights[key] * components[key], 6)}")
    if "historical_replay" in components:
        lines.append("historical replay contributed through score, sample size, expectancy, regime consistency, and failure rate")
    return lines


def qualify_edge(edge_input: EdgeQualificationInput | dict[str, Any], *, qualification_id: str | None = None, created_at: str | None = None) -> dict[str, Any]:
    row = _as_input(edge_input)
    validate_candidate_governance(
        {
            "artifact_type": "PaperTradeCandidateQualification",
            "authority_level": "HUMAN_REVIEWED_PAPER_TESTING_CONSIDERATION",
            "metadata": row.metadata,
            "forbidden_artifacts": row.forbidden_artifacts,
            "paper_trade_eligible": False,
        }
    )
    score = compute_edge_score(row)
    reasons: list[str] = []
    disqualifications: list[str] = []
    if score["edge_score"] >= EDGE_ELIGIBILITY_THRESHOLD:
        reasons.append(f"edge_score >= {EDGE_ELIGIBILITY_THRESHOLD}")
    else:
        disqualifications.append(f"edge_score below {EDGE_ELIGIBILITY_THRESHOLD}")
    checks = [
        (row.lineage_complete, "lineage complete", "lineage incomplete"),
        (row.governance_pass, "governance passed", "governance failed"),
        (not row.forbidden_artifacts, "forbidden artifact audit passed", "forbidden artifacts present"),
        (not row.generated_only, "not generated-only", "generated-only evidence"),
        (not row.mock_only, "not mock-only", "mock-only evidence"),
        (not row.quarantined, "not quarantined", "quarantined lifecycle state"),
        (not row.retired, "not retired", "retired lifecycle state"),
    ]
    for passed, passed_reason, failed_reason in checks:
        if passed:
            reasons.append(passed_reason)
        else:
            disqualifications.append(failed_reason)
    eligible = not disqualifications
    input_summary = {
        "source_artifact_ids": row.source_artifact_ids,
        "source_hypothesis_ids": row.source_hypothesis_ids,
        "source_experiment_ids": row.source_experiment_ids,
        "source_memory_ids": row.source_memory_ids,
        "mechanism_tags": row.mechanism_tags,
        "regime_context": row.regime_context,
        "evidence_level": row.evidence_level,
        "lifecycle_state": row.lifecycle_state,
        "historical_replay_score": row.historical_replay_score,
        "historical_sample_size": row.historical_sample_size,
        "historical_expectancy": row.historical_expectancy,
        "historical_regime_consistency": row.historical_regime_consistency,
        "historical_failure_rate": row.historical_failure_rate,
    }
    return EdgeQualificationResult(
        qualification_id=qualification_id or stable_id("edge_qual", row.source_artifact_ids + row.source_hypothesis_ids + [str(score["edge_score"])]),
        created_at=created_at or now_utc(),
        input_summary=input_summary,
        score=score,
        eligible=eligible,
        qualification_reasons=reasons,
        disqualification_reasons=disqualifications,
        governance_pass=row.governance_pass,
        lineage_complete=row.lineage_complete,
        metadata={"threshold": EDGE_ELIGIBILITY_THRESHOLD, "research_only": True, **row.metadata},
    ).to_dict()


def _as_input(edge_input: EdgeQualificationInput | dict[str, Any]) -> EdgeQualificationInput:
    if isinstance(edge_input, EdgeQualificationInput):
        return edge_input
    return EdgeQualificationInput(**edge_input)


def _clamp(value: float | int | None) -> float:
    if value is None:
        return 0.0
    return min(1.0, max(0.0, float(value)))


def _historical_replay_component(row: EdgeQualificationInput) -> float:
    if int(row.historical_sample_size or 0) <= 0:
        return 0.0
    sample_component = min(1.0, float(row.historical_sample_size) / 50.0)
    expectancy_component = _clamp(((row.historical_expectancy or 0.0) + 0.03) / 0.08)
    failure_inverse = 1.0 - _clamp(row.historical_failure_rate)
    return round(
        0.40 * _clamp(row.historical_replay_score)
        + 0.20 * sample_component
        + 0.15 * expectancy_component
        + 0.15 * _clamp(row.historical_regime_consistency)
        + 0.10 * failure_inverse,
        6,
    )
