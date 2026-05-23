from __future__ import annotations

from research_lab.candidates.candidate_scoring import assign_ranking_buckets, score_candidate


def test_ranking_score_deterministic() -> None:
    assert score_candidate(signal_return=-0.03, evidence_quality="moderate", risk_regime="risk_on") == 6.0
    assert score_candidate(signal_return=-0.03, evidence_quality="moderate", risk_regime="risk_off") == 4.0


def test_tie_break_deterministic() -> None:
    candidates = [
        {"candidate_id": "b", "symbol": "QQQ", "ranking_score": 5.0},
        {"candidate_id": "a", "symbol": "SPY", "ranking_score": 5.0},
    ]
    ranked = assign_ranking_buckets(candidates)

    assert [row["symbol"] for row in ranked] == ["QQQ", "SPY"]
    assert ranked[0]["ranking_bucket"] == "top"

