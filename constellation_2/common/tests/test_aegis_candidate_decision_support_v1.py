from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.candidate_decision_support_v1 import (
    build_candidate_decision_support_brief_v1,
    build_candidate_decision_support_payload_v1,
)


def _cockpit() -> dict[str, object]:
    return {
        "source_paths": {
            "canonical_operator_state": "/truth/canonical_operator_state.v1.json",
            "operator_brief": "/truth/operator_brief.v1.json",
        },
        "runtime": {"runtime_truth_classification": "ADVISORY_ONLY"},
        "opportunities": {
            "market_data_summary": {"status": "READY", "missing_symbols": [], "stale_symbols": []},
            "sleeve_run_summary": [],
        },
        "sleeve_warnings": {},
        "top_candidates": [],
        "candidate_decisions_corrections": {},
    }


def test_candidate_decision_support_supported_when_evidence_complete() -> None:
    candidate = {
        "candidate_id": "cand-supported",
        "symbol": "QQQ",
        "direction": "LONG",
        "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
        "why_now": "Cross-asset trend alignment triggered bullish QQQ setup after volatility compression.",
        "historical_expectancy": "post-cost expectancy +0.42%",
        "event_study_summary": "event study positive across 120 events",
        "drift_state": "stable",
        "fragility_state": "normal",
        "sleeve_health": "healthy",
        "observation_count": 84,
        "paper_trial_status": "active",
    }

    brief = build_candidate_decision_support_brief_v1(candidate, _cockpit(), generated_at="2026-05-19T12:00:00Z")

    assert brief["trust_classification"] == "supported"
    assert brief["decision_guidance"] == "reasonable_for_manual_review"
    assert brief["recommended_operator_action"] == "review_and_decide"
    assert brief["execution_allowed"] is False
    assert "Manual capture may be reasonable after review." in brief["direct_answer"]


def test_candidate_decision_support_partial_when_some_evidence_missing() -> None:
    candidate = {
        "candidate_id": "cand-partial",
        "symbol": "QQQ",
        "direction": "LONG",
        "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
        "why_now": "Cross-asset trend alignment triggered bullish QQQ setup.",
        "historical_expectancy": "post-cost expectancy +0.21%",
        "event_study_summary": "event study positive across 55 events",
        "drift_state": "stable",
    }

    brief = build_candidate_decision_support_brief_v1(candidate, _cockpit(), generated_at="2026-05-19T12:00:00Z")

    assert brief["trust_classification"] == "partially_supported"
    assert brief["decision_guidance"] == "caution_manual_review_only"
    assert any(item["item"] == "Fragility status" for item in brief["missing_evidence"])


def test_candidate_decision_support_unsupported_explains_missing_evidence_without_fabrication() -> None:
    candidate = {
        "candidate_id": "cand-unsupported",
        "symbol": "QQQ",
        "direction": "LONG",
        "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
    }

    brief = build_candidate_decision_support_brief_v1(candidate, _cockpit(), generated_at="2026-05-19T12:00:00Z")

    assert brief["trust_classification"] == "unsupported"
    assert brief["decision_guidance"] == "not_recommended_due_to_missing_evidence"
    assert brief["recommended_operator_action"] == "request_more_evidence"
    assert "Manual capture is not recommended because supporting evidence is incomplete." in brief["direct_answer"]
    assert "No candidate-linked expectancy artifact was found" in str(brief["missing_evidence"])
    assert "post-cost expectancy +" not in str(brief)


def test_candidate_decision_support_blocked_when_candidate_market_data_missing() -> None:
    cockpit = _cockpit()
    cockpit["opportunities"] = {
        "market_data_summary": {"status": "PARTIAL", "missing_symbols": ["QQQ"], "stale_symbols": []},
        "sleeve_run_summary": [],
    }
    candidate = {
        "candidate_id": "cand-blocked",
        "symbol": "QQQ",
        "direction": "LONG",
        "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
        "why_now": "Cross-asset trend alignment triggered bullish QQQ setup.",
    }

    brief = build_candidate_decision_support_brief_v1(candidate, cockpit, generated_at="2026-05-19T12:00:00Z")

    assert brief["trust_classification"] == "blocked"
    assert brief["decision_guidance"] == "blocked_do_not_capture"
    assert brief["manual_capture_allowed"] is False
    assert "Do not capture; candidate is blocked." in brief["direct_answer"]


def test_candidate_decision_support_is_deterministic_for_same_inputs() -> None:
    candidate = {
        "candidate_id": "cand-deterministic",
        "symbol": "QQQ",
        "direction": "LONG",
        "sleeve_id": "C2_CROSS_ASSET_TREND_V1",
    }

    first = build_candidate_decision_support_brief_v1(candidate, _cockpit(), generated_at="2026-05-19T12:00:00Z")
    second = build_candidate_decision_support_brief_v1(candidate, _cockpit(), generated_at="2026-05-19T12:00:00Z")

    assert first["content_hash"] == second["content_hash"]
    assert first == second


def test_candidate_decision_support_payload_is_read_only_and_indexed() -> None:
    cockpit = _cockpit()
    cockpit["top_candidates"] = [
        {"candidate_id": "cand-indexed", "symbol": "QQQ", "direction": "LONG", "sleeve_id": "C2"}
    ]

    payload = build_candidate_decision_support_payload_v1(cockpit)

    assert payload["read_only"] is True
    assert payload["safety"]["broker_execution_allowed"] is False
    assert "cand-indexed" in payload["by_candidate_id"]
