from __future__ import annotations

from pathlib import Path
from typing import Any

from .artifact_models import ArtifactType
from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .autonomous_research_execution import find_compatible_worker_for_backlog_item
from .funnel_progression_models import FunnelBalancingPolicy, FunnelSelectionResult, FunnelStageGroup, stage_group_for_item_type


def select_next_funnel_balanced_item(
    ready_backlog_items: list[dict[str, Any]],
    *,
    runs_completed: int,
    runs_remaining: int,
    stage_counts_so_far: dict[str, int] | None = None,
    profile: Any | None = None,
    root: str | Path = DEFAULT_STORE_ROOT,
    policy: FunnelBalancingPolicy | None = None,
) -> FunnelSelectionResult:
    policy_value = policy or FunnelBalancingPolicy()
    stage_counts = dict(stage_counts_so_far or {})
    store = ArtifactStore(root)
    compatible = [_with_stage(row, store) for row in ready_backlog_items]
    compatible = [row for row in compatible if row.get("compatible")]
    compatible.sort(key=lambda row: (-_effective_priority(row, policy_value), str(row.get("backlog_item_id", ""))))
    if not compatible:
        return FunnelSelectionResult(None, "NO_COMPATIBLE_READY_BACKLOG_ITEM", FunnelStageGroup.UNKNOWN.value, compatible_item_count=0, skipped_item_count=len(ready_backlog_items))

    downstream_continuations = [row for row in compatible if _is_same_session_continuation(row) and row["stage_group"] in {FunnelStageGroup.HYPOTHESIS_STAGE.value, FunnelStageGroup.QUALIFICATION_STAGE.value, FunnelStageGroup.PAPER_REVIEW_STAGE.value}]
    if policy_value.prefer_downstream_continuations and downstream_continuations:
        selected = downstream_continuations[0]
        return FunnelSelectionResult(selected, "PREFER_DOWNSTREAM_SAME_SESSION_CONTINUATION", selected["stage_group"], len(compatible), len(ready_backlog_items) - len(compatible), downstream_starvation_detected=_downstream_starvation_detected(stage_counts, compatible, runs_completed, policy_value))

    qualification_items = [row for row in compatible if row["stage_group"] == FunnelStageGroup.QUALIFICATION_STAGE.value]
    if qualification_items and _needs_min_share(FunnelStageGroup.QUALIFICATION_STAGE.value, stage_counts, runs_completed, policy_value.min_qualification_stage_share_if_available):
        selected = qualification_items[0]
        return FunnelSelectionResult(selected, "RESERVE_QUALIFICATION_STAGE_CAPACITY", selected["stage_group"], len(compatible), len(ready_backlog_items) - len(compatible), downstream_starvation_detected=True)

    hypothesis_items = [row for row in compatible if row["stage_group"] == FunnelStageGroup.HYPOTHESIS_STAGE.value]
    if hypothesis_items and _needs_min_share(FunnelStageGroup.HYPOTHESIS_STAGE.value, stage_counts, runs_completed, policy_value.min_hypothesis_stage_share_if_available):
        selected = hypothesis_items[0]
        return FunnelSelectionResult(selected, "RESERVE_HYPOTHESIS_STAGE_CAPACITY", selected["stage_group"], len(compatible), len(ready_backlog_items) - len(compatible), downstream_starvation_detected=True)

    claim_items = [row for row in compatible if row["stage_group"] == FunnelStageGroup.CLAIM_STAGE.value]
    non_claim_items = [row for row in compatible if row["stage_group"] != FunnelStageGroup.CLAIM_STAGE.value]
    if claim_items and non_claim_items and _claim_share_at_or_above_limit(stage_counts, runs_completed, policy_value):
        selected = non_claim_items[0]
        return FunnelSelectionResult(selected, "MAX_CLAIM_STAGE_SHARE_ENFORCED", selected["stage_group"], len(compatible), len(ready_backlog_items) - len(compatible), downstream_starvation_detected=True)

    selected = compatible[0]
    return FunnelSelectionResult(selected, "HIGHEST_PRIORITY_COMPATIBLE_ITEM", selected["stage_group"], len(compatible), len(ready_backlog_items) - len(compatible), downstream_starvation_detected=_downstream_starvation_detected(stage_counts, compatible, runs_completed, policy_value))


def _with_stage(item: dict[str, Any], store: ArtifactStore) -> dict[str, Any]:
    row = dict(item)
    row["stage_group"] = stage_group_for_item_type(str(row.get("item_type") or ""))
    artifacts = []
    try:
        for artifact_id in row.get("source_artifact_ids", []):
            artifacts.append(store.get_artifact(str(artifact_id)))
    except Exception:
        row["compatible"] = False
        return row
    if row.get("item_type") == "EDGE_QUALIFICATION_REVIEW":
        has_hypothesis = any(artifact.get("artifact_type") == ArtifactType.RESEARCH_HYPOTHESIS.value for artifact in artifacts)
        has_replay = any(artifact.get("artifact_type") == "HistoricalReplayResult" for artifact in artifacts)
        metadata = row.get("metadata", {}) or {}
        replay_exception = metadata.get("historical_replay_unavailable") or metadata.get("historical_replay_skipped") or metadata.get("historical_replay_insufficient_data")
        row["compatible"] = has_hypothesis and (has_replay or bool(replay_exception))
    elif row.get("item_type") == "HISTORICAL_REPLAY_REVIEW":
        row["compatible"] = any(artifact.get("artifact_type") == ArtifactType.RESEARCH_HYPOTHESIS.value for artifact in artifacts)
    else:
        row["compatible"] = find_compatible_worker_for_backlog_item(row, artifacts) is not None
    return row


def _effective_priority(item: dict[str, Any], policy: FunnelBalancingPolicy) -> float:
    priority = float(item.get("priority_score") or 0.0)
    if policy.same_session_continuation_boost and _is_same_session_continuation(item):
        priority += 100.0 + float(item.get("metadata", {}).get("continuation_depth") or 0)
    return priority


def _is_same_session_continuation(item: dict[str, Any]) -> bool:
    metadata = item.get("metadata", {}) or {}
    return bool(metadata.get("same_session_eligible") and metadata.get("priority_boost_reason") == "FUNNEL_CONTINUATION")


def _needs_min_share(stage_group: str, stage_counts: dict[str, int], runs_completed: int, minimum: float) -> bool:
    if runs_completed <= 0:
        return True
    return (float(stage_counts.get(stage_group, 0)) / float(runs_completed)) < float(minimum)


def _claim_share_at_or_above_limit(stage_counts: dict[str, int], runs_completed: int, policy: FunnelBalancingPolicy) -> bool:
    if runs_completed <= 0:
        return False
    return (float(stage_counts.get(FunnelStageGroup.CLAIM_STAGE.value, 0)) / float(runs_completed)) >= float(policy.max_claim_stage_share)


def _downstream_starvation_detected(stage_counts: dict[str, int], compatible: list[dict[str, Any]], runs_completed: int, policy: FunnelBalancingPolicy) -> bool:
    if not compatible or runs_completed <= 0:
        return False
    has_hypothesis = any(row["stage_group"] == FunnelStageGroup.HYPOTHESIS_STAGE.value for row in compatible)
    has_qualification = any(row["stage_group"] == FunnelStageGroup.QUALIFICATION_STAGE.value for row in compatible)
    if has_hypothesis and _needs_min_share(FunnelStageGroup.HYPOTHESIS_STAGE.value, stage_counts, runs_completed, policy.min_hypothesis_stage_share_if_available):
        return True
    if has_qualification and _needs_min_share(FunnelStageGroup.QUALIFICATION_STAGE.value, stage_counts, runs_completed, policy.min_qualification_stage_share_if_available):
        return True
    return False
