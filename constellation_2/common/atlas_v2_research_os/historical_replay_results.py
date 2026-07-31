from __future__ import annotations

from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .edge_qualification_models import EdgeQualificationInput
from .historical_replay_engine import now_utc, stable_id, summarize_historical_replay
from .historical_replay_governance import validate_historical_replay_allowed
from .historical_replay_models import HISTORICAL_REPLAY_EVIDENCE_LEVEL
from .memory_index import add_memory_object, list_memory_objects
from .memory_models import MemoryEvidenceMaturity, MemoryLifecycleState, MemoryType, create_memory_object
from .research_backlog import ResearchBacklog


def edge_input_with_historical_replay(edge_input: EdgeQualificationInput | dict[str, Any], replay_result: dict[str, Any]) -> EdgeQualificationInput:
    base = edge_input.to_dict() if isinstance(edge_input, EdgeQualificationInput) else dict(edge_input)
    summary = summarize_historical_replay(replay_result)
    metrics = summary["metrics"]
    source_artifact_ids = sorted(set(base.get("source_artifact_ids", []) + summary.get("source_artifact_ids", []) + [summary["replay_id"]]))
    mechanism_tags = sorted(set(base.get("mechanism_tags", []) + summary.get("mechanism_tags", [])))
    metadata = {
        **dict(base.get("metadata", {})),
        "historical_replay_summary": summary,
        "historical_replay_certification": replay_result.get("certification", {}),
        "evidence_only": True,
    }
    historical_sample_size = int(metrics.get("sample_size") or summary.get("sample_size") or 0)
    base.update(
        {
            "source_artifact_ids": source_artifact_ids,
            "mechanism_tags": mechanism_tags,
            "evidence_level": HISTORICAL_REPLAY_EVIDENCE_LEVEL,
            "evidence_maturity": max(float(base.get("evidence_maturity") or 0.0), 0.65 if historical_sample_size else 0.35),
            "generated_only": False if historical_sample_size else bool(base.get("generated_only", False)),
            "historical_replay_score": float(metrics.get("historical_replay_score") or summary.get("score") or 0.0),
            "historical_sample_size": historical_sample_size,
            "historical_expectancy": float(metrics.get("expectancy") or 0.0),
            "historical_regime_consistency": float(metrics.get("regime_consistency") or 0.0),
            "historical_failure_rate": float(metrics.get("failure_rate") or 0.0),
            "metadata": metadata,
        }
    )
    return EdgeQualificationInput(**base)


def route_historical_replay_backlog_items(replay_result: dict[str, Any], *, root: str | Path = DEFAULT_STORE_ROOT, created_at: str | None = None) -> list[dict[str, Any]]:
    validate_historical_replay_allowed(replay_result)
    status = replay_result.get("certification", {}).get("status", "INSUFFICIENT_SAMPLE")
    if status == "REPLAY_NEGATIVE":
        return [create_historical_replay_failure_analysis_item(replay_result, root=root, created_at=created_at)]
    if status == "REPLAY_POSITIVE":
        return [create_historical_replay_edge_review_item(replay_result, root=root, created_at=created_at)]
    return []


def create_historical_replay_failure_analysis_item(replay_result: dict[str, Any], *, root: str | Path = DEFAULT_STORE_ROOT, created_at: str | None = None) -> dict[str, Any]:
    return _create_replay_backlog_item(
        replay_result,
        root=root,
        created_at=created_at,
        item_type="FAILURE_ANALYSIS",
        title=f"Analyze historical replay weakness {replay_result['replay_id']}",
        description="Historical replay showed weakness; analyze failure modes before further qualification.",
        scores={"expected_learning_value": 0.7, "failure_reduction_score": 0.9, "evidence_gap_score": 0.2},
    )


def create_historical_replay_edge_review_item(replay_result: dict[str, Any], *, root: str | Path = DEFAULT_STORE_ROOT, created_at: str | None = None) -> dict[str, Any]:
    return _create_replay_backlog_item(
        replay_result,
        root=root,
        created_at=created_at,
        item_type="EDGE_QUALIFICATION_REVIEW",
        title=f"Review historically supported edge {replay_result['replay_id']}",
        description="Historical replay showed support; review edge qualification before any paper-test consideration.",
        scores={"expected_learning_value": 0.5, "candidate_impact_estimate": 0.7, "evidence_gap_score": 0.1},
    )


def record_historical_replay_to_memory(replay_result: dict[str, Any], *, root: str | Path = DEFAULT_STORE_ROOT, created_at: str | None = None) -> dict[str, Any]:
    validate_historical_replay_allowed(replay_result)
    summary = summarize_historical_replay(replay_result)
    memory_id = f"mem-historical-replay-{summary['replay_id']}"
    existing = {row["memory_id"] for row in list_memory_objects(root)}
    lifecycle = MemoryLifecycleState.SUPPORTED.value if summary["status"] == "REPLAY_POSITIVE" else MemoryLifecycleState.WEAKENED.value if summary["status"] == "REPLAY_NEGATIVE" else MemoryLifecycleState.NEW.value
    memory = create_memory_object(
        memory_id=memory_id,
        memory_type=MemoryType.EVIDENCE_TRAIL.value,
        created_at=created_at or replay_result.get("created_at") or now_utc(),
        source_artifact_ids=summary.get("source_artifact_ids", []) or [summary["replay_id"]],
        mechanism_tags=summary.get("mechanism_tags", []),
        regime_context_ids=[],
        evidence_level=MemoryEvidenceMaturity.HISTORICAL_REPLAY.value,
        lifecycle_state=lifecycle,
        confidence=float(summary.get("score") or 0.0),
        labels=["historical_replay", summary["status"].lower()],
        metadata={
            "historical_replay_summary": summary,
            "survived_replay": summary["status"] == "REPLAY_POSITIVE",
            "failed_replay": summary["status"] == "REPLAY_NEGATIVE",
            "research_only": True,
        },
    )
    if memory_id in existing:
        return next(row for row in list_memory_objects(root) if row["memory_id"] == memory_id)
    return add_memory_object(memory, root)


def _create_replay_backlog_item(
    replay_result: dict[str, Any],
    *,
    root: str | Path,
    created_at: str | None,
    item_type: str,
    title: str,
    description: str,
    scores: dict[str, float],
) -> dict[str, Any]:
    backlog = ResearchBacklog(root)
    issue_key = replay_result["replay_id"]
    for row in backlog.list_backlog_items(item_type=item_type):
        if row.get("state") in {"NEW", "READY", "IN_PROGRESS", "BLOCKED"} and row.get("metadata", {}).get("historical_replay_id") == issue_key:
            return row
    return backlog.create_backlog_item(
        backlog_item_id=f"hr-{item_type.lower().replace('_', '-')}-{stable_id('item', [issue_key])[-12:]}",
        item_type=item_type,
        title=title,
        description=description,
        created_at=created_at or now_utc(),
        created_by="atlas_historical_replay",
        source_artifact_ids=list(replay_result.get("source_artifact_ids", [])) + [replay_result["replay_id"]],
        state="READY",
        expected_learning_value=scores.get("expected_learning_value", 0.0),
        candidate_impact_estimate=scores.get("candidate_impact_estimate", 0.0),
        failure_reduction_score=scores.get("failure_reduction_score", 0.0),
        evidence_gap_score=scores.get("evidence_gap_score", 0.0),
        mechanism_tags=list(replay_result.get("mechanism_tags", [])),
        regime_context=str(replay_result.get("regime_context", {}).get("label", "UNKNOWN")),
        metadata={
            "historical_replay_id": issue_key,
            "historical_replay_status": replay_result.get("certification", {}).get("status"),
            "historical_replay_score": replay_result.get("metrics", {}).get("historical_replay_score"),
            "evidence_only": True,
        },
    )
