from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .artifact_models import BacklogItem

BACKLOG_ITEM_TYPES = {
    "RESEARCH_QUESTION",
    "CLAIM_INVESTIGATION",
    "HYPOTHESIS_VALIDATION",
    "FAILURE_ANALYSIS",
    "EVIDENCE_GAP",
    "REGIME_GAP",
    "DUPLICATE_REVIEW",
    "STALE_LEARNING_REVIEW",
    "MECHANISM_VARIATION",
    "EDGE_QUALIFICATION_REVIEW",
    "HISTORICAL_REPLAY_REVIEW",
    "PAPER_TRADE_CANDIDATE_REVIEW",
    "FUNDAMENTAL_CLAIM_INVESTIGATION",
}
BACKLOG_STATES = {"NEW", "READY", "READY_IF_DATA_AVAILABLE", "IN_PROGRESS", "BLOCKED", "COMPLETED", "REJECTED", "RETIRED"}


class BacklogError(ValueError):
    pass


class ResearchBacklog:
    def __init__(self, root: str | Path = DEFAULT_STORE_ROOT) -> None:
        self.root = Path(root)
        self.path = self.root / "research_backlog.json"
        self.root.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write([])

    def create_backlog_item(
        self,
        *,
        backlog_item_id: str,
        item_type: str,
        title: str,
        description: str,
        created_at: str,
        created_by: str,
        source_artifact_ids: list[str] | None = None,
        state: str = "NEW",
        blocked_reason: str = "",
        cost_estimate: float = 0.0,
        expected_learning_value: float = 0.0,
        candidate_impact_estimate: float = 0.0,
        novelty_score: float = 0.0,
        failure_reduction_score: float = 0.0,
        evidence_gap_score: float = 0.0,
        regime_gap_score: float = 0.0,
        priority_reasons: list[str] | None = None,
        mechanism_tags: list[str] | None = None,
        regime_context: str = "UNKNOWN",
        source_memory_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if item_type not in BACKLOG_ITEM_TYPES:
            raise BacklogError(f"invalid backlog item_type: {item_type}")
        if state not in BACKLOG_STATES:
            raise BacklogError(f"invalid backlog state: {state}")
        if self.get(backlog_item_id, required=False):
            raise BacklogError(f"backlog item exists: {backlog_item_id}")
        score = compute_priority_score(
            expected_learning_value=expected_learning_value,
            novelty_score=novelty_score,
            candidate_impact_estimate=candidate_impact_estimate,
            failure_reduction_score=failure_reduction_score,
            evidence_gap_score=evidence_gap_score,
            regime_gap_score=regime_gap_score,
            cost_estimate=cost_estimate,
        )
        item = BacklogItem(
            backlog_item_id=backlog_item_id,
            item_type=item_type,
            title=title,
            description=description,
            created_at=created_at,
            created_by=created_by,
            source_artifact_ids=list(source_artifact_ids or []),
            priority_score=score,
            priority_reasons=list(priority_reasons or priority_reasons_for_scores(expected_learning_value, novelty_score, candidate_impact_estimate, failure_reduction_score, evidence_gap_score, regime_gap_score, cost_estimate)),
            state=state,
            blocked_reason=blocked_reason,
            cost_estimate=float(cost_estimate),
            expected_learning_value=float(expected_learning_value),
            candidate_impact_estimate=float(candidate_impact_estimate),
            novelty_score=float(novelty_score),
            failure_reduction_score=float(failure_reduction_score),
            evidence_gap_score=float(evidence_gap_score),
            regime_gap_score=float(regime_gap_score),
            mechanism_tags=sorted(set(mechanism_tags or [])),
            regime_context=str(regime_context or "UNKNOWN"),
            source_memory_ids=sorted(set(source_memory_ids or [])),
            metadata=dict(metadata or {}),
        ).to_dict()
        rows = self._read()
        rows.append(item)
        self._write(rows)
        return item

    def update_backlog_state(self, backlog_item_id: str, *, state: str, blocked_reason: str = "") -> dict[str, Any]:
        if state not in BACKLOG_STATES:
            raise BacklogError(f"invalid backlog state: {state}")
        rows = self._read()
        for row in rows:
            if row["backlog_item_id"] == backlog_item_id:
                row["state"] = state
                row["blocked_reason"] = blocked_reason
                self._write(rows)
                return row
        raise BacklogError(f"backlog item not found: {backlog_item_id}")

    def list_backlog_items(self, state: str | None = None, item_type: str | None = None) -> list[dict[str, Any]]:
        rows = self._read()
        if state is not None:
            rows = [row for row in rows if row["state"] == state]
        if item_type is not None:
            rows = [row for row in rows if row["item_type"] == item_type]
        return rows

    def get_ready_items(self) -> list[dict[str, Any]]:
        return self.list_backlog_items(state="READY")

    def get_top_priority_items(self, limit: int) -> list[dict[str, Any]]:
        return sorted(self.list_backlog_items(), key=lambda row: (-float(row["priority_score"]), row["backlog_item_id"]))[:limit]

    def link_backlog_item_to_artifact(self, backlog_item_id: str, artifact_id: str) -> dict[str, Any]:
        rows = self._read()
        for row in rows:
            if row["backlog_item_id"] == backlog_item_id:
                linked = list(row.get("linked_artifact_ids", []))
                if artifact_id not in linked:
                    linked.append(artifact_id)
                row["linked_artifact_ids"] = linked
                if artifact_id not in row.get("source_artifact_ids", []):
                    row.setdefault("source_artifact_ids", []).append(artifact_id)
                self._write(rows)
                return row
        raise BacklogError(f"backlog item not found: {backlog_item_id}")

    def get(self, backlog_item_id: str, *, required: bool = True) -> dict[str, Any] | None:
        for row in self._read():
            if row["backlog_item_id"] == backlog_item_id:
                return row
        if required:
            raise BacklogError(f"backlog item not found: {backlog_item_id}")
        return None

    def _read(self) -> list[dict[str, Any]]:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _write(self, rows: list[dict[str, Any]]) -> None:
        self.path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def compute_priority_score(*, expected_learning_value: float, novelty_score: float, candidate_impact_estimate: float, failure_reduction_score: float, evidence_gap_score: float, regime_gap_score: float, cost_estimate: float) -> float:
    return round(float(expected_learning_value) + float(novelty_score) + float(candidate_impact_estimate) + float(failure_reduction_score) + float(evidence_gap_score) + float(regime_gap_score) - float(cost_estimate), 6)


def priority_reasons_for_scores(expected_learning_value: float, novelty_score: float, candidate_impact_estimate: float, failure_reduction_score: float, evidence_gap_score: float, regime_gap_score: float, cost_estimate: float) -> list[str]:
    fields = {
        "expected_learning_value": expected_learning_value,
        "novelty_score": novelty_score,
        "candidate_impact_estimate": candidate_impact_estimate,
        "failure_reduction_score": failure_reduction_score,
        "evidence_gap_score": evidence_gap_score,
        "regime_gap_score": regime_gap_score,
        "cost_estimate": -float(cost_estimate),
    }
    return [f"{name}={value}" for name, value in fields.items() if float(value) != 0.0]
