from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

CANDIDATE_REVIEW_ALLOWED_SCOPE = "review for paper-forward observation"
CANDIDATE_REVIEW_LIMITATION = (
    "Candidate review is for paper-forward observation only; it does not authorize trade "
    "recommendations, capital allocation, position sizing, live trading, broker execution, "
    "automatic paper placement, or production promotion."
)
CANDIDATE_REVIEW_FORBIDDEN_ACTIONS = [
    "trade recommendation",
    "capital allocation",
    "position sizing",
    "live trading",
    "broker execution",
]


@dataclass(frozen=True)
class CandidateReviewItem:
    rank: int
    candidate_id: str
    mechanism: str
    edge_score: float
    replay_score: float
    hypothesis: str
    entry_condition: str
    exit_condition: str
    invalidating_condition: str
    evidence_summary: str
    why_interesting: str
    why_risky: str
    human_action_required: str
    replay_status: str
    regime: str
    source_hypothesis_id: str
    source_replay_id: str
    source_artifact_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        if int(row["rank"]) < 1:
            raise ValueError("candidate review rank must be positive")
        if not row["candidate_id"]:
            raise ValueError("candidate review requires candidate_id")
        if not row["mechanism"]:
            raise ValueError("candidate review requires mechanism")
        if row["replay_status"] not in {"REPLAY_POSITIVE", "REPLAY_NEUTRAL", "REPLAY_NEGATIVE", "INSUFFICIENT_SAMPLE"}:
            raise ValueError(f"invalid candidate review replay_status: {row['replay_status']}")
        return row


@dataclass(frozen=True)
class CandidateReviewSection:
    section_name: str
    candidates: list[dict[str, Any]]
    rationale: str

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        if not row["section_name"]:
            raise ValueError("candidate review section requires section_name")
        return row


@dataclass(frozen=True)
class CandidateReviewReport:
    schema_id: str
    schema_version: str
    day: str
    created_at: str
    source_report_path: str
    candidates_reviewed: int
    top_candidates: list[dict[str, Any]]
    near_threshold_candidates: list[dict[str, Any]]
    rejected_candidates_worth_monitoring: list[dict[str, Any]]
    mechanism_clusters: list[dict[str, Any]]
    common_risks: list[str]
    recommended_next_human_reviews: list[str]
    authority_boundary: dict[str, Any]
    guardrails: dict[str, Any]
    sections: dict[str, Any]
    limitations: list[str] = field(default_factory=lambda: [CANDIDATE_REVIEW_LIMITATION])

    def to_dict(self) -> dict[str, Any]:
        row = asdict(self)
        if row["schema_id"] != "atlas_v2_research_os_candidate_review_report_v1":
            raise ValueError(f"invalid candidate review schema_id: {row['schema_id']}")
        if row["schema_version"] != "v1":
            raise ValueError(f"invalid candidate review schema_version: {row['schema_version']}")
        if row["candidates_reviewed"] != len(row["top_candidates"]):
            raise ValueError("candidates_reviewed must match top_candidates length")
        if row["authority_boundary"].get("allowed_scope") != CANDIDATE_REVIEW_ALLOWED_SCOPE:
            raise ValueError("candidate review authority boundary must be paper-forward observation only")
        return row
