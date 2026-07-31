from __future__ import annotations

import random
from typing import Any

from .learning_lifecycle import can_influence_priority
from .research_backlog import ResearchBacklog, compute_priority_score, priority_reasons_for_scores
from .artifact_store import ArtifactStore
from .research_effectiveness import research_effectiveness_priority_adjustment


def score_item(item: dict[str, Any], research_effectiveness_report: dict[str, Any] | None = None) -> float:
    base_score = compute_priority_score(
        expected_learning_value=float(item.get("expected_learning_value", 0.0)),
        novelty_score=float(item.get("novelty_score", 0.0)),
        candidate_impact_estimate=float(item.get("candidate_impact_estimate", 0.0)),
        failure_reduction_score=float(item.get("failure_reduction_score", 0.0)),
        evidence_gap_score=float(item.get("evidence_gap_score", 0.0)),
        regime_gap_score=float(item.get("regime_gap_score", 0.0)),
        cost_estimate=float(item.get("cost_estimate", 0.0)),
    )
    return round(base_score + research_effectiveness_priority_adjustment(item, research_effectiveness_report), 6)


class PriorityEngine:
    def __init__(self, backlog: ResearchBacklog, artifact_store: ArtifactStore | None = None, research_effectiveness_report: dict[str, Any] | None = None) -> None:
        self.backlog = backlog
        self.artifact_store = artifact_store
        self.research_effectiveness_report = research_effectiveness_report

    def select_next_items(self, limit: int, exploration_rate: float = 0.15, seed: int | None = None) -> list[dict[str, Any]]:
        ready = [item for item in self.backlog.get_ready_items() if self._sources_can_influence(item)]
        for item in ready:
            item["priority_score"] = score_item(item, self.research_effectiveness_report)
            item["priority_reasons"] = priority_reasons_for_scores(
                item.get("expected_learning_value", 0.0), item.get("novelty_score", 0.0), item.get("candidate_impact_estimate", 0.0), item.get("failure_reduction_score", 0.0), item.get("evidence_gap_score", 0.0), item.get("regime_gap_score", 0.0), item.get("cost_estimate", 0.0)
            )
            if research_effectiveness_priority_adjustment(item, self.research_effectiveness_report):
                item["priority_reasons"].append("research_effectiveness_adjustment=research_priority_only")
        ranked = sorted(ready, key=lambda row: (-float(row["priority_score"]), row["backlog_item_id"]))
        if exploration_rate <= 0 or not ranked:
            return [selection(row, "PRIORITY", "highest deterministic priority") for row in ranked[:limit]]
        rng = random.Random(seed)
        selected: list[dict[str, Any]] = []
        pool = ranked[:]
        while pool and len(selected) < limit:
            use_exploration = rng.random() < exploration_rate
            if use_exploration:
                idx = rng.randrange(len(pool))
                mode = "EXPLORATION"
                reason = "seeded exploration among READY items"
            else:
                idx = 0
                mode = "PRIORITY"
                reason = "highest deterministic priority"
            row = pool.pop(idx)
            selected.append(selection(row, mode, reason))
        return selected

    def _sources_can_influence(self, item: dict[str, Any]) -> bool:
        if self.artifact_store is None:
            return True
        for artifact_id in item.get("source_artifact_ids", []):
            if not can_influence_priority(self.artifact_store, artifact_id):
                return False
        return True


def selection(item: dict[str, Any], mode: str, reason: str) -> dict[str, Any]:
    return {
        "backlog_item_id": item["backlog_item_id"],
        "priority_score": float(item.get("priority_score", score_item(item))),
        "selection_reason": reason,
        "selection_mode": mode,
    }
