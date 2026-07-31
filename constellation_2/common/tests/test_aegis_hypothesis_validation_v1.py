from __future__ import annotations

from pathlib import Path
import sys

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ops.aegis.research_lab.hypothesis_validation_v1 import (
    QUALIFIED,
    BLOCKED,
    MORE_RESEARCH,
    build_hypothesis_qualification_v1,
    build_paper_testing_sleeve_v1,
    hypothesis_validation_self_check_v1,
    write_hypothesis_qualification_v1,
    write_paper_testing_sleeve_v1,
)


def _qualified_review() -> dict:
    return {
        "briefs": [
            {
                "hypothesis_id": "rh-qualified-semiconductors-v1",
                "research_run_id": "RR-QUALIFIED-001",
                "title": "Semiconductor Momentum Follow Through",
                "status": "RECOMMENDATION_READY",
                "confidence": "MEDIUM",
                "confidence_reason": "Governed event study and cross-ETF confirmation are present.",
                "affected_symbols": ["SMH", "SOXX", "NVDA"],
                "evidence_summary": "Completed governed validation found testable follow-through behavior.",
                "key_evidence": ["governed event study complete", "cross-sleeve confirmation present"],
                "risks": ["open P&L is not proof of edge"],
                "decision_needed": "Paper validation can start under research-only safety gates.",
                "staleness": {"status": "CURRENT"},
                "diagnostics": {"raw_result_status": "RECOMMENDATION_READY"},
            }
        ]
    }


def test_qualified_hypothesis_creates_paper_testing_sleeve_with_linked_symbols(tmp_path):
    qualification = build_hypothesis_qualification_v1(truth_root=tmp_path, day_utc="2026-05-29", review_brief_payload=_qualified_review())

    assert qualification["summary"]["qualified"] == 1
    row = qualification["qualifications"][0]
    assert row["qualification_status"] == QUALIFIED
    assert row["paper_validation_allowed"] is True
    assert row["safety_gates_checked"]["trade_advice_allowed"] is False

    sleeves = build_paper_testing_sleeve_v1(truth_root=tmp_path, day_utc="2026-05-29", qualification_payload=qualification)

    assert sleeves["summary"]["paper_testing_sleeves_active"] == 1
    sleeve = sleeves["sleeves"][0]
    assert sleeve["sleeve_type"] == "PAPER_TESTING"
    assert sleeve["lifecycle_state"] == "PAPER_VALIDATION_ACTIVE"
    assert {link["symbol"] for link in sleeve["linked_symbols"]} == {"SMH", "SOXX", "NVDA"}
    assert all(link["link_status"] == "LINKED" for link in sleeve["linked_symbols"])
    assert all(link["evidence_reference"] for link in sleeve["linked_symbols"])
    assert sleeves["safety"]["live_trading_allowed"] is False
    assert sleeves["safety"]["broker_submit_transmit_allowed"] is False


def test_unqualified_and_blocked_states_are_explicit(tmp_path):
    review = {
        "briefs": [
            {
                "hypothesis_id": "rh-more-research-v1",
                "research_run_id": "RR-MORE-001",
                "title": "Low Evidence Hypothesis",
                "status": "RECOMMENDATION_READY",
                "confidence": "LOW",
                "affected_symbols": ["QQQ"],
                "key_evidence": ["INCONCLUSIVE sample size"],
                "evidence_summary": "INCONCLUSIVE sample size.",
                "staleness": {"status": "CURRENT"},
            },
            {
                "hypothesis_id": "rh-blocked-test-v1",
                "research_run_id": "RR-BLOCKED-001",
                "title": "Missing Test Implementation",
                "status": "RECOMMENDATION_READY",
                "confidence": "MEDIUM",
                "affected_symbols": ["SPY"],
                "key_evidence": ["TEST_NOT_IMPLEMENTED"],
                "evidence_summary": "TEST_NOT_IMPLEMENTED",
                "staleness": {"status": "CURRENT"},
            },
        ]
    }

    qualification = build_hypothesis_qualification_v1(truth_root=tmp_path, day_utc="2026-05-29", review_brief_payload=review)
    by_id = {row["hypothesis_id"]: row for row in qualification["qualifications"]}

    assert by_id["rh-more-research-v1"]["qualification_status"] == MORE_RESEARCH
    assert by_id["rh-more-research-v1"]["required_follow_up"]
    assert by_id["rh-blocked-test-v1"]["qualification_status"] == BLOCKED
    assert by_id["rh-blocked-test-v1"]["blocker_reasons"]

    sleeves = build_paper_testing_sleeve_v1(truth_root=tmp_path, day_utc="2026-05-29", qualification_payload=qualification)
    assert sleeves["summary"]["paper_testing_sleeves_active"] == 0
    assert sleeves["summary"]["excluded_hypotheses"] == 2
    assert {row["hypothesis_id"] for row in sleeves["excluded_hypotheses"]} == {"rh-more-research-v1", "rh-blocked-test-v1"}


def test_hypothesis_validation_self_check_accepts_qualified_sleeve_and_safety_gates(tmp_path):
    qualification = build_hypothesis_qualification_v1(truth_root=tmp_path, day_utc="2026-05-29", review_brief_payload=_qualified_review())
    write_hypothesis_qualification_v1(truth_root=tmp_path, day_utc="2026-05-29", payload=qualification)
    sleeves = build_paper_testing_sleeve_v1(truth_root=tmp_path, day_utc="2026-05-29", qualification_payload=qualification)
    write_paper_testing_sleeve_v1(truth_root=tmp_path, day_utc="2026-05-29", payload=sleeves)

    report = hypothesis_validation_self_check_v1(truth_root=tmp_path, day_utc="2026-05-29")

    assert report["ok"] is True
    assert report["qualified"] == 1
    assert report["paper_testing_sleeves_active"] == 1
    assert report["linked_symbols"] == 3
    assert all(check["ok"] for check in report["checks"])


def test_data_needed_research_result_becomes_more_research_not_blocked(tmp_path):
    review = {
        "briefs": [
            {
                "hypothesis_id": "rh-process-test-etf-drop-mean-reversion-v1",
                "research_run_id": "RR-DATA-NEEDED-001",
                "title": "ETF mean-reversion after sharp 1-day drop",
                "status": "RECOMMENDATION_READY",
                "confidence": "LOW",
                "affected_symbols": ["SPY", "QQQ", "IWM", "VIX"],
                "key_evidence": ["DATA_NEEDED: Waiting for current intraday market data."],
                "evidence_summary": "DATA_NEEDED: Waiting for current intraday market data.",
                "staleness": {"status": "CURRENT"},
            }
        ]
    }

    qualification = build_hypothesis_qualification_v1(truth_root=tmp_path, day_utc="2026-05-29", review_brief_payload=review)
    row = qualification["qualifications"][0]

    assert row["qualification_status"] == MORE_RESEARCH
    assert row["blocker_reasons"] == []
    assert row["required_follow_up"] == ["Refresh or bind required research inputs, then rerun the governed research test."]


def test_research_validation_samples_report_excludes_non_trigger_observations(tmp_path):
    from ops.aegis.research_lab.research_validation_samples_v1 import build_research_validation_samples_v1

    day = "2026-05-29"
    result_path = tmp_path / "reports" / "aegis_research_test_results_v1" / day / "rh-process-test-etf-drop-mean-reversion-v1" / "research_test_result.v1.json"
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(
        '{"minimum_sample_size":20,"event_data_availability":{"observed_rows":[{"symbol":"SPY","daily_return":0.001,"vix_filter_pass":true}],"event_rows":[]}}',
        encoding="utf-8",
    )

    report = build_research_validation_samples_v1(truth_root=tmp_path, day_utc=day)

    assert report["hypothesis_id"] == "rh-process-test-etf-drop-mean-reversion-v1"
    assert report["required_samples"] == 20
    assert report["current_samples"] == 0
    assert report["missing_samples"] == 20
    assert "next valid trigger event" in report["next_sample_expected_at"]
    assert report["exclusion_reasons"] == {"NO_TRIGGER_EVENT": 1}
    assert report["should_rerun_qualification"] is False
    assert report["safety"]["trade_advice_allowed"] is False


def test_research_validation_samples_accumulates_valid_forward_returns(tmp_path):
    from ops.aegis.research_lab.research_validation_samples_v1 import build_research_validation_samples_v1

    hypothesis = "rh-process-test-etf-drop-mean-reversion-v1"
    event_day = "2026-05-28"
    target_day = "2026-05-29"
    result_path = tmp_path / "reports" / "aegis_research_test_results_v1" / event_day / hypothesis / "research_test_result.v1.json"
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(
        '{"minimum_sample_size":20,"event_data_availability":{"event_rows":[{"symbol":"SPY","close":100,"daily_return":-0.02,"vix_filter_pass":true}],"observed_rows":[]}}',
        encoding="utf-8",
    )
    market_path = tmp_path / "reports" / "aegis_market_data_v1" / target_day / "market_data.v1.json"
    market_path.parent.mkdir(parents=True, exist_ok=True)
    market_path.write_text('{"normalized_records":[{"symbol":"SPY","close":103}]}', encoding="utf-8")

    report = build_research_validation_samples_v1(truth_root=tmp_path, day_utc=target_day)

    assert report["current_samples"] == 1
    assert report["missing_samples"] == 19
    assert report["samples"][0]["forward_return_1d"] == 0.03
    assert report["samples"][0]["sample_status"] == "VALID"
    assert report["sample_count_monotonic"] is True
    assert report["should_rerun_qualification"] is False
    assert report["safety"]["paper_testing_sleeve_creation_allowed"] is False


def test_research_validation_samples_preserves_prior_samples_monotonically(tmp_path):
    from ops.aegis.research_lab.research_validation_samples_v1 import build_research_validation_samples_v1, research_validation_samples_path_v1

    hypothesis = "rh-process-test-etf-drop-mean-reversion-v1"
    prior_path = research_validation_samples_path_v1(truth_root=tmp_path, day_utc="2026-05-28")
    prior_path.parent.mkdir(parents=True, exist_ok=True)
    prior_path.write_text(
        '{"hypothesis_id":"rh-process-test-etf-drop-mean-reversion-v1","samples":[{"sample_id":"prior-1","hypothesis_id":"rh-process-test-etf-drop-mean-reversion-v1","symbol":"SPY","event_day":"2026-05-27","forward_close_day":"2026-05-28","sample_timestamp":"2026-05-28T20:00:00Z","forward_return_1d":0.01}]}',
        encoding="utf-8",
    )

    report = build_research_validation_samples_v1(truth_root=tmp_path, day_utc="2026-05-29", hypothesis_id=hypothesis)

    assert report["current_samples"] == 1
    assert report["samples"][0]["sample_id"] == "prior-1"
    assert report["sample_count_reset_reason"] == ""


def test_research_validation_samples_excludes_pending_forward_window(tmp_path):
    from ops.aegis.research_lab.research_validation_samples_v1 import build_research_validation_samples_v1

    hypothesis = "rh-process-test-etf-drop-mean-reversion-v1"
    day = "2026-05-29"
    result_path = tmp_path / "reports" / "aegis_research_test_results_v1" / day / hypothesis / "research_test_result.v1.json"
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(
        '{"minimum_sample_size":20,"event_data_availability":{"event_rows":[{"symbol":"SPY","close":100,"daily_return":-0.02,"vix_filter_pass":true}],"observed_rows":[]}}',
        encoding="utf-8",
    )

    report = build_research_validation_samples_v1(truth_root=tmp_path, day_utc=day)

    assert report["current_samples"] == 0
    assert report["exclusion_reasons"] == {"FORWARD_WINDOW_PENDING": 1}
    assert report["excluded_samples"][0]["exclusion_reason"] == "FORWARD_WINDOW_PENDING"


def test_research_validation_self_check_includes_sample_accumulation_guard(tmp_path):
    from ops.aegis.research_lab.hypothesis_validation_v1 import research_validation_self_check_v1, write_paper_testing_sleeve_v1
    from ops.aegis.research_lab.research_review_brief_v1 import research_review_brief_path_v1

    day = "2026-05-29"
    review_path = research_review_brief_path_v1(truth_root=tmp_path, day_utc=day)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.write_text(
        '{"briefs":[{"hypothesis_id":"rh-process-test-etf-drop-mean-reversion-v1","validation_ui":{"state_label":"Collecting Evidence","required_samples":20,"current_samples":0,"missing_samples":20,"next_sample_expected_at":"next market close","operator_action_required":false},"diagnostics":{},"decision_needed":"No operator action required."}]}',
        encoding="utf-8",
    )
    write_paper_testing_sleeve_v1(truth_root=tmp_path, day_utc=day, payload={"sleeves": []})
    result_path = tmp_path / "reports" / "aegis_research_test_results_v1" / day / "rh-process-test-etf-drop-mean-reversion-v1" / "research_test_result.v1.json"
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(
        '{"minimum_sample_size":20,"event_data_availability":{"observed_rows":[{"symbol":"SPY","daily_return":0.001,"vix_filter_pass":true}],"event_rows":[]}}',
        encoding="utf-8",
    )

    report = research_validation_self_check_v1(truth_root=tmp_path, day_utc=day)

    assert report["ok"] is True
    assert report["research_validation_samples"]["current_samples"] == 0
    assert report["research_validation_samples"]["exclusion_reasons"] == {"NO_TRIGGER_EVENT": 1}
    assert all(check["ok"] for check in report["checks"])
