from __future__ import annotations

from typing import Any


RANKING_POLICY_VERSION = "candidate_ranking_policy_v1"


def evidence_quality_bonus(evidence_quality: str) -> float:
    return {
        "moderate": 2.0,
        "preliminary": 1.0,
        "insufficient_sample": 0.0,
        "research_simulation": 0.0,
    }.get(str(evidence_quality or "unknown"), 0.0)


def regime_bonus(risk_regime: str) -> float:
    return {"risk_on": 1.0, "mixed": 0.0, "risk_off": -1.0, "unknown": -0.5}.get(
        str(risk_regime or "unknown"),
        -0.5,
    )


def score_candidate(*, signal_return: float, evidence_quality: str, risk_regime: str) -> float:
    return abs(float(signal_return)) * 100.0 + evidence_quality_bonus(evidence_quality) + regime_bonus(risk_regime)


def assign_ranking_buckets(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = sorted(candidates, key=lambda row: (-float(row["ranking_score"]), row["symbol"], row["candidate_id"]))
    total = len(ranked)
    if total == 0:
        return []
    for index, row in enumerate(ranked):
        item = row
        if index == 0:
            item["ranking_bucket"] = "top"
        elif index < max(2, total // 2 + total % 2):
            item["ranking_bucket"] = "middle"
        else:
            item["ranking_bucket"] = "low"
    return ranked

