from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_survival_analytics import compute_candidate_survival
from .failure_patterns import increment_failure_repetition, list_failure_patterns, record_failure_pattern
from .learning_validation import validate_learning_over_time
from .memory_index import add_memory_object, list_memory_objects
from .memory_models import MemoryEvidenceMaturity, MemoryLifecycleState, MemoryType, create_memory_object
from .paper_trade_outcome_governance import validate_paper_trade_outcome_allowed
from .paper_trade_outcome_models import PaperTradeFeedbackSignal, PaperTradeOutcome
from .research_backlog import BacklogError, ResearchBacklog
from .research_effectiveness import evaluate_research_effectiveness


def derive_feedback_from_outcome(outcome: dict[str, Any] | PaperTradeOutcome, survival: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    outcome = _outcome_row(outcome)
    validate_paper_trade_outcome_allowed(outcome)
    survival_row = survival or compute_candidate_survival(outcome)
    state = survival_row["survival_state"]
    if state == "SURVIVED_INITIAL_TEST":
        specs = [
            ("SUPPORT_MECHANISM", "RESEARCH_MEMORY", "INCREASE", "paper outcome survived initial test"),
            ("RECORD_SUCCESS_PATTERN", "RESEARCH_MEMORY", "INCREASE", "record paper success pattern"),
            ("INCREASE_RESEARCH_EFFECTIVENESS", "RESEARCH_EFFECTIVENESS", "INCREASE", "paper outcome improved research path"),
            ("UPDATE_LEARNING_VALIDATION", "LEARNING_VALIDATION", "INCREASE", "paper outcome adds positive validation signal"),
        ]
    elif state in {"WEAKENED", "FALSIFIED"}:
        specs = [
            ("RECORD_FAILURE_PATTERN", "RESEARCH_MEMORY", "DECREASE", "record paper failure pattern"),
            ("WEAKEN_HYPOTHESIS", "RESEARCH_MEMORY", "DECREASE", "weaken hypothesis from paper outcome"),
            ("CREATE_FAILURE_ANALYSIS_BACKLOG", "RESEARCH_PRIORITIZATION", "INCREASE", "create research failure-analysis priority"),
            ("REDUCE_RESEARCH_EFFECTIVENESS", "RESEARCH_EFFECTIVENESS", "DECREASE", "paper outcome reduced research path effectiveness"),
            ("UPDATE_LEARNING_VALIDATION", "LEARNING_VALIDATION", "DECREASE", "paper outcome adds negative validation signal"),
        ]
    elif state == "NEEDS_MORE_DATA":
        specs = [("NEEDS_MORE_DATA", "RESEARCH_PRIORITIZATION", "NEUTRAL", "insufficient paper sample; do not over-interpret")]
    else:
        specs = [("UPDATE_LEARNING_VALIDATION", "LEARNING_VALIDATION", "NEUTRAL", "paper outcome inconclusive")]
    return [
        PaperTradeFeedbackSignal(
            feedback_id=f"ptf-{_stable_id([outcome['outcome_id'], feedback_type, target])}",
            outcome_id=outcome["outcome_id"],
            candidate_id=outcome["candidate_id"],
            feedback_type=feedback_type,
            target=target,
            direction=direction,
            reason=reason,
            source_artifact_ids=list(outcome.get("source_artifact_ids", [])),
            metadata={"paper_only": True, "survival_state": state, "influence_target": target},
        ).to_dict()
        for feedback_type, target, direction, reason in specs
    ]


def update_memory_from_paper_outcome(root: str | Path = DEFAULT_STORE_ROOT, outcome: dict[str, Any] | PaperTradeOutcome | None = None, survival: dict[str, Any] | None = None) -> dict[str, Any]:
    if outcome is None:
        raise ValueError("outcome is required")
    outcome = _outcome_row(outcome)
    survival_row = survival or compute_candidate_survival(outcome)
    state = survival_row["survival_state"]
    memory_state = MemoryLifecycleState.SUPPORTED.value if state == "SURVIVED_INITIAL_TEST" else MemoryLifecycleState.WEAKENED.value if state == "WEAKENED" else MemoryLifecycleState.FALSIFIED.value if state == "FALSIFIED" else MemoryLifecycleState.NEW.value
    memory_id = f"mem-paper-outcome-{outcome['outcome_id']}"
    existing = {row["memory_id"] for row in list_memory_objects(root)}
    if memory_id not in existing:
        add_memory_object(create_memory_object(
            memory_id=memory_id,
            memory_type=MemoryType.LEARNING_NODE.value,
            created_at=outcome["created_at"],
            source_artifact_ids=outcome.get("source_artifact_ids", []) or [outcome["outcome_id"]],
            evidence_level=MemoryEvidenceMaturity.PAPER_FORWARD_OBSERVATION.value,
            lifecycle_state=memory_state,
            confidence=_confidence(outcome),
            labels=["paper_trade_outcome", state.lower()],
            metadata={"candidate_id": outcome["candidate_id"], "outcome_id": outcome["outcome_id"], "paper_only": True, "survival_state": state},
        ), root)
    failure_pattern = None
    backlog_item = None
    if state in {"WEAKENED", "FALSIFIED"}:
        failure_pattern = _record_or_increment_failure(root, outcome)
        backlog_item = _create_failure_backlog_item(root, outcome, failure_pattern)
    return {"memory_id": memory_id, "survival_state": state, "failure_pattern": failure_pattern, "backlog_item": backlog_item}


def update_research_effectiveness_from_paper_outcome(root: str | Path = DEFAULT_STORE_ROOT, outcome: dict[str, Any] | PaperTradeOutcome | None = None, survival: dict[str, Any] | None = None) -> dict[str, Any]:
    if outcome is None:
        raise ValueError("outcome is required")
    outcome = _outcome_row(outcome)
    survival_row = survival or compute_candidate_survival(outcome)
    positive = survival_row["survival_state"] == "SURVIVED_INITIAL_TEST"
    activity = {
        "activity_id": f"paper-outcome-effectiveness-{outcome['outcome_id']}",
        "created_at": outcome["created_at"],
        "mechanism": outcome.get("metadata", {}).get("mechanism", "UNKNOWN"),
        "worker": "paper_trade_outcome_capture",
        "backlog_type": "HYPOTHESIS_VALIDATION",
        "experiment_type": "PAPER_TRADE_OBSERVATION",
        "failure_category": ",".join(outcome.get("failure_reasons", [])) or "NONE",
        "regime": outcome.get("regime_context", "UNKNOWN"),
        "evidence_maturity": MemoryEvidenceMaturity.PAPER_FORWARD_OBSERVATION.value,
        "cost_estimate": max(float(outcome.get("sample_size", 0)), 1.0),
        "input_uncertainty": 0.7,
        "output_uncertainty": 0.3 if positive else 0.8,
        "failures_before": 1,
        "failures_after": 0 if positive else 2,
        "candidate_quality_before": 0.4,
        "candidate_quality_after": 0.7 if positive else 0.2,
        "hypotheses_tested_before": 1,
        "hypotheses_survived_before": 0,
        "hypotheses_tested_after": 1,
        "hypotheses_survived_after": 1 if positive else 0,
        "source_artifact_ids": outcome.get("source_artifact_ids", []),
        "metadata": {"paper_only": True, "outcome_id": outcome["outcome_id"], "influence_target": "RESEARCH_PRIORITIZATION"},
    }
    report = evaluate_research_effectiveness([activity])
    if not positive:
        for contribution in report["contributions"]:
            contribution["metadata"]["paper_outcome_reduced_path_effectiveness"] = True
    return report


def update_learning_validation_from_paper_outcome(root: str | Path = DEFAULT_STORE_ROOT, outcome: dict[str, Any] | PaperTradeOutcome | None = None, survival: dict[str, Any] | None = None) -> dict[str, Any]:
    if outcome is None:
        raise ValueError("outcome is required")
    outcome = _outcome_row(outcome)
    survival_row = survival or compute_candidate_survival(outcome)
    positive = survival_row["survival_state"] == "SURVIVED_INITIAL_TEST"
    first = {
        "snapshot_id": f"paper-before-{outcome['outcome_id']}",
        "observed_at": outcome["observation_start"] + "T00:00:00Z" if len(outcome["observation_start"]) == 10 else outcome["observation_start"],
        "repeated_failures": 1.0,
        "duplicate_ideas": 0.0,
        "regime_gaps": 1.0,
        "evidence_maturity_score": 0.4,
        "hypothesis_survival_rate": 0.0,
        "candidate_quality_score": 0.4,
        "source_artifact_ids": outcome.get("source_artifact_ids", []),
        "source_memory_ids": [],
        "metadata": {"paper_only": True},
    }
    last = dict(first)
    last.update({
        "snapshot_id": f"paper-after-{outcome['outcome_id']}",
        "observed_at": outcome["observation_end"] + "T00:00:00Z" if len(outcome["observation_end"]) == 10 else outcome["observation_end"],
        "repeated_failures": 0.0 if positive else 2.0,
        "regime_gaps": 0.0 if positive else 1.0,
        "evidence_maturity_score": 0.7,
        "hypothesis_survival_rate": 1.0 if positive else 0.0,
        "candidate_quality_score": 0.7 if positive else 0.2,
    })
    return validate_learning_over_time([first, last], horizon="lifetime")


def _record_or_increment_failure(root: str | Path, outcome: dict[str, Any]) -> dict[str, Any]:
    failure_id = f"paper-failure-{_stable_id([outcome['candidate_id'], outcome.get('test_plan_id')])}"
    for row in list_failure_patterns(root):
        if row["failure_id"] == failure_id:
            return increment_failure_repetition(root, failure_id, source_artifact_id=(outcome.get("source_artifact_ids") or [None])[0], seen_at=outcome["created_at"])
    return record_failure_pattern(
        root=root,
        failure_id=failure_id,
        failure_type=(outcome.get("failure_reasons") or ["PAPER_OUTCOME_FAILURE"])[0],
        source_artifact_ids=outcome.get("source_artifact_ids", []) or [outcome["outcome_id"]],
        mechanism_tags=[outcome.get("metadata", {}).get("mechanism", "EVENT_REACTION")],
        reason="; ".join(outcome.get("failure_reasons", [])) or "paper outcome weakened or falsified candidate",
        evidence_level=MemoryEvidenceMaturity.PAPER_FORWARD_OBSERVATION.value,
        first_seen_at=outcome["created_at"],
        metadata={"paper_only": True, "candidate_id": outcome["candidate_id"], "outcome_id": outcome["outcome_id"]},
    )


def _create_failure_backlog_item(root: str | Path, outcome: dict[str, Any], failure: dict[str, Any]) -> dict[str, Any]:
    backlog = ResearchBacklog(root)
    backlog_item_id = f"paper-failure-analysis-{_stable_id([outcome['outcome_id']])}"
    existing = backlog.get(backlog_item_id, required=False)
    if existing:
        return existing
    try:
        return backlog.create_backlog_item(
            backlog_item_id=backlog_item_id,
            item_type="FAILURE_ANALYSIS",
            title=f"Analyze paper outcome failure for {outcome['candidate_id']}",
            description="Paper trade outcome weakened or falsified the candidate; research analysis required.",
            created_at=outcome["created_at"],
            created_by="atlas_v2_paper_trade_feedback",
            source_artifact_ids=outcome.get("source_artifact_ids", []) or [outcome["outcome_id"]],
            state="READY",
            expected_learning_value=0.6,
            failure_reduction_score=0.8,
            priority_reasons=["paper_trade_outcome_failure", failure["failure_id"]],
        )
    except BacklogError:
        return backlog.get(backlog_item_id)


def _confidence(outcome: dict[str, Any]) -> float:
    return round(min(1.0, int(outcome.get("sample_size", 0)) / 20.0), 6)


def _stable_id(parts: list[Any]) -> str:
    return hashlib.sha1(json.dumps(parts, sort_keys=True).encode("utf-8")).hexdigest()[:12]


def _outcome_row(outcome: dict[str, Any] | PaperTradeOutcome) -> dict[str, Any]:
    if isinstance(outcome, PaperTradeOutcome):
        return outcome.to_dict()
    return outcome
