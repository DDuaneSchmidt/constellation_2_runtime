from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any


class FunnelStageGroup(str, Enum):
    CLAIM_STAGE = "CLAIM_STAGE"
    HYPOTHESIS_STAGE = "HYPOTHESIS_STAGE"
    REPLAY_STAGE = "REPLAY_STAGE"
    QUALIFICATION_STAGE = "QUALIFICATION_STAGE"
    PAPER_REVIEW_STAGE = "PAPER_REVIEW_STAGE"
    UNKNOWN = "UNKNOWN"


CLAIM_STAGE_TYPES = {"CLAIM_INVESTIGATION", "RESEARCH_QUESTION", "MECHANISM_VARIATION"}
HYPOTHESIS_STAGE_TYPES = {"HYPOTHESIS_VALIDATION"}
REPLAY_STAGE_TYPES = {"HISTORICAL_REPLAY_REVIEW", "EVIDENCE_GAP"}
QUALIFICATION_STAGE_TYPES = {"EDGE_QUALIFICATION_REVIEW"}
PAPER_REVIEW_STAGE_TYPES = {"PAPER_TRADE_CANDIDATE_REVIEW"}


@dataclass(frozen=True)
class FunnelBalancingPolicy:
    max_claim_stage_share: float = 0.50
    min_hypothesis_stage_share_if_available: float = 0.25
    min_qualification_stage_share_if_available: float = 0.10
    prefer_downstream_continuations: bool = True
    same_session_continuation_boost: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FunnelSelectionResult:
    selected_backlog_item: dict[str, Any] | None
    selection_reason: str
    stage_group: str
    compatible_item_count: int = 0
    skipped_item_count: int = 0
    downstream_starvation_detected: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "selected_backlog_item": self.selected_backlog_item,
            "selection_reason": self.selection_reason,
            "stage_group": self.stage_group,
            "compatible_item_count": self.compatible_item_count,
            "skipped_item_count": self.skipped_item_count,
            "downstream_starvation_detected": self.downstream_starvation_detected,
        }


def stage_group_for_item_type(item_type: str) -> str:
    if item_type in CLAIM_STAGE_TYPES:
        return FunnelStageGroup.CLAIM_STAGE.value
    if item_type in HYPOTHESIS_STAGE_TYPES:
        return FunnelStageGroup.HYPOTHESIS_STAGE.value
    if item_type in REPLAY_STAGE_TYPES:
        return FunnelStageGroup.REPLAY_STAGE.value
    if item_type in QUALIFICATION_STAGE_TYPES:
        return FunnelStageGroup.QUALIFICATION_STAGE.value
    if item_type in PAPER_REVIEW_STAGE_TYPES:
        return FunnelStageGroup.PAPER_REVIEW_STAGE.value
    return FunnelStageGroup.UNKNOWN.value


def mark_same_session_continuation_metadata(metadata: dict[str, Any] | None, *, execution_id: str, continuation_depth: int) -> dict[str, Any]:
    value = dict(metadata or {})
    value["continuation_of_execution_id"] = execution_id
    value["continuation_depth"] = continuation_depth
    value["same_session_eligible"] = True
    value["priority_boost_reason"] = "FUNNEL_CONTINUATION"
    return value
