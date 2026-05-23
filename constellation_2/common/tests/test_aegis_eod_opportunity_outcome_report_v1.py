from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.eod_opportunity_outcome_report_v1 import (
    build_eod_opportunity_outcome_report_v1,
    classify_sleeve_eod_outcome_v1,
    longitudinal_eod_accumulation_v1,
    read_eod_opportunity_outcome_report_v1,
    write_eod_opportunity_outcome_report_v1,
)
from ops.aegis.operator_command_v1 import active_opportunity_projection_v1


def _supported_candidate() -> dict[str, object]:
    return {
        "candidate_id": "cand-supported",
        "symbol": "SPY",
        "direction": "LONG",
        "sleeve_id": "SLEEVE_ELIGIBLE",
        "decision_support_brief": {
            "candidate_id": "cand-supported",
            "direct_answer": "Manual capture may be reasonable after review.",
            "trust_classification": "supported",
            "trust_score_label": "Supported",
            "decision_guidance": "reasonable_for_manual_review",
            "why_triggered": "Supported setup.",
            "historical_support": [{"item": "event study", "status": "present"}],
            "weakening_factors": [],
            "sleeve_health_context": {"overall_health": "watch"},
            "expected_holding_window": "5 sessions",
            "invalidation_conditions": ["candidate expires"],
            "missing_evidence": [],
            "source_artifacts": ["ev_supported"],
        },
    }


def _unsupported_candidate() -> dict[str, object]:
    return {
        "candidate_id": "cand-unsupported",
        "symbol": "QQQ",
        "direction": "LONG",
        "sleeve_id": "SLEEVE_UNSUPPORTED",
    }


def _cockpit() -> dict[str, object]:
    cockpit = {
        "day_utc": "2026-05-19",
        "source_paths": {"canonical_operator_state": "/truth/canonical.json"},
        "runtime": {"runtime_truth_classification": "READY", "highest_readiness_layer": "ADVISORY_ONLY"},
        "opportunities": {
            "market_data_summary": {"missing_symbols": ["VIX"], "stale_symbols": []},
            "sleeve_run_summary": [
                {"sleeve_id": "SLEEVE_BLOCKED_VIX", "run_status": "BLOCKED", "blocking_inputs": ["VIX"], "canonical_blocker": "SLEEVE_INPUT_REQUIREMENT_BLOCKED"},
                {"sleeve_id": "SLEEVE_NO_SIGNAL", "run_status": "RAN", "raw_signal_count": 0},
                {"sleeve_id": "SLEEVE_RAW_REJECTED", "run_status": "RAN", "raw_signal_count": 2},
                {"sleeve_id": "SLEEVE_CANDIDATE_REJECTED", "run_status": "RAN", "raw_signal_count": 2, "rejected_candidate_count": 1},
                {"sleeve_id": "SLEEVE_UNSUPPORTED", "run_status": "RAN", "raw_signal_count": 1},
                {"sleeve_id": "SLEEVE_ELIGIBLE", "run_status": "RAN", "raw_signal_count": 1},
            ],
        },
        "top_candidates": [_unsupported_candidate(), _supported_candidate()],
        "candidate_decisions_corrections": {},
    }
    cockpit["active_opportunity_projection"] = active_opportunity_projection_v1(cockpit)
    return cockpit


def test_one_primary_bucket_per_sleeve_and_bucket_examples() -> None:
    report = build_eod_opportunity_outcome_report_v1(_cockpit(), trading_session="2026-05-19", generated_at="2026-05-19T21:00:00Z")
    buckets = {row["sleeve_id"]: row["primary_outcome_bucket"] for row in report["sleeve_outcomes"]}

    assert all(row["primary_outcome_bucket"] for row in report["sleeve_outcomes"])
    assert buckets["SLEEVE_BLOCKED_VIX"] == "BLOCKED_DATA_OR_INPUT"
    assert buckets["SLEEVE_NO_SIGNAL"] == "NO_RAW_SIGNAL"
    assert buckets["SLEEVE_RAW_REJECTED"] == "RAW_SIGNAL_REJECTED"
    assert buckets["SLEEVE_CANDIDATE_REJECTED"] == "CANDIDATE_REJECTED"
    assert buckets["SLEEVE_UNSUPPORTED"] == "UNSUPPORTED_RESEARCH_OBSERVATION"
    assert buckets["SLEEVE_ELIGIBLE"] == "OPERATOR_ELIGIBLE_OPPORTUNITY"


def test_manual_capture_bucket_takes_highest_lifecycle_for_running_sleeve() -> None:
    cockpit = _cockpit()
    cockpit["top_candidates"] = [{**_supported_candidate(), "review_state": "MANUAL_EXTERNAL_CAPTURE_RECORDED"}]
    cockpit["opportunities"]["sleeve_run_summary"] = [{"sleeve_id": "SLEEVE_ELIGIBLE", "run_status": "RAN", "raw_signal_count": 1}]
    cockpit["active_opportunity_projection"] = active_opportunity_projection_v1(cockpit)

    outcome = classify_sleeve_eod_outcome_v1("SLEEVE_ELIGIBLE", cockpit)
    assert outcome["primary_outcome_bucket"] == "MANUAL_EXTERNAL_CAPTURE_RECORDED"
    assert outcome["manual_capture_count"] == 1


def test_blocked_data_not_counted_as_strategy_failure() -> None:
    outcome = classify_sleeve_eod_outcome_v1("SLEEVE_BLOCKED_VIX", _cockpit())

    assert outcome["primary_outcome_bucket"] == "BLOCKED_DATA_OR_INPUT"
    assert outcome["strategy_failure_counted"] is False
    assert "VIX_MISSING" in outcome["secondary_flags"]


def test_opportunities_only_receives_eligible_subset_and_unsupported_is_separate() -> None:
    projection = _cockpit()["active_opportunity_projection"]

    assert [row["sleeve_id"] for row in projection["candidates"]] == ["SLEEVE_ELIGIBLE"]
    assert [row["sleeve_id"] for row in projection["research_observations"]] == ["SLEEVE_UNSUPPORTED"]


def test_eod_report_persistence_and_replay_determinism(tmp_path: Path) -> None:
    report = build_eod_opportunity_outcome_report_v1(_cockpit(), trading_session="2026-05-19", generated_at="2026-05-19T21:00:00Z")
    written = write_eod_opportunity_outcome_report_v1(truth_root=tmp_path, report=report)
    read_back = read_eod_opportunity_outcome_report_v1(truth_root=tmp_path, trading_session="2026-05-19")
    rebuilt = build_eod_opportunity_outcome_report_v1(_cockpit(), trading_session="2026-05-19", generated_at="2026-05-20T01:00:00Z")

    assert written["eod_outcome_report_id"] == report["eod_outcome_report_id"]
    assert read_back is not None
    assert read_back["determinism_fingerprint"] == report["determinism_fingerprint"]
    assert rebuilt["determinism_fingerprint"] == report["determinism_fingerprint"]


def test_longitudinal_eod_accumulation_tracks_data_blocks_separately() -> None:
    report = build_eod_opportunity_outcome_report_v1(_cockpit(), trading_session="2026-05-19", generated_at="2026-05-19T21:00:00Z")
    accumulation = longitudinal_eod_accumulation_v1([report])
    blocked = next(row for row in accumulation["sleeves"] if row["sleeve_id"] == "SLEEVE_BLOCKED_VIX")

    assert blocked["data_block_rate"] == 1
    assert blocked["strategy_failure_count"] == 0
    assert accumulation["determinism_fingerprint"]


def test_report_governance_disables_execution_paths() -> None:
    report = build_eod_opportunity_outcome_report_v1(_cockpit(), trading_session="2026-05-19", generated_at="2026-05-19T21:00:00Z")

    assert report["research_label"] == "RESEARCH_ONLY"
    assert report["advisory_label"] == "ADVISORY_ONLY"
    assert report["governance"]["broker_execution_allowed"] is False
    assert report["governance"]["order_routing_allowed"] is False
    assert report["governance"]["autonomous_execution_allowed"] is False
