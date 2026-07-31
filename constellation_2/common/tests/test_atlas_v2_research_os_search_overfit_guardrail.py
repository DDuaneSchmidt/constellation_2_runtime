from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.search_overfit_guardrail import (
    build_search_overfit_guardrail_report,
    write_search_overfit_guardrail_report,
)

NOW = "2026-06-06T00:00:00Z"


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name / "latest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_inputs(root: Path) -> None:
    _write_latest(
        root,
        "observation_cluster_split_experiment",
        {
            "report_type": "OBSERVATION_CLUSTER_SPLIT_EXPERIMENT",
            "post_split": {
                "observations": 1000,
                "clusters": 100,
                "claims": 100,
                "hypotheses": 100,
                "backtest_supported_candidates": 25,
                "eligible_candidates": 0,
                "paper_forward_ready_candidates": 0,
            },
            "data_source": {"symbol": "SPY"},
        },
    )
    _write_latest(
        root,
        "expanded_search_trial",
        {
            "report_type": "EXPANDED_SEARCH_TRIAL",
            "profile": "EXPANDED_OBSERVATION_TRIAL_5000",
            "comparison_against_prior_baseline": {
                "new_hypotheses": 770,
                "new_backtest_supported_candidates": 427,
                "new_eligible_candidates": 0,
                "supported_diagnostic_families": 427,
                "supported_duplicate_families": 12,
                "promising_but_data_blocked_families": 427,
            },
            "final_qualification": {"summary": {"hypotheses_generated": 770, "backtest_supported_candidates": 427, "final_eligible_candidates": 0}},
        },
    )
    _write_latest(
        root,
        "expanded_search_gate_audit",
        {
            "report_type": "EXPANDED_SEARCH_GATE_AUDIT",
            "near_threshold_expanded_candidates": [{"candidate_id": f"c{i}", "edge_score": 0.695} for i in range(21)],
        },
    )
    _write_latest(
        root,
        "expanded_search_narrowing",
        {
            "report_type": "EXPANDED_SEARCH_NARROWING_RECOMMENDATION",
            "summary": {"values_evaluated": 72, "quality_assessment": "MIXED_DATA_BLOCKED"},
            "recommended_next_search_profile": {
                "profile_id": "EXPANDED_OBSERVATION_TRIAL_FOCUSED",
                "dimensions": {"regime": ["CHOP"], "mechanism": ["BREAKOUT"], "session_context": []},
            },
        },
    )
    _write_latest(
        root,
        "final_candidate_ranking",
        {
            "report_type": "FINAL_CANDIDATE_RANKING",
            "summary": {
                "candidates_evaluated": 600,
                "ranked_eligible_candidates": 110,
                "top_20_count": 20,
                "campaign_candidate_count": 8,
                "biggest_remaining_risk": "All selected candidates still depend on SPY daily proxy evidence.",
            },
            "excluded_candidates": [{"final_score": 0.69}, {"final_score": 0.5}],
        },
    )
    _write_latest(
        root,
        "candidate_family_discovery",
        {
            "report_type": "CANDIDATE_FAMILY_DISCOVERY",
            "summary": {"unique_candidate_families": 2, "near_duplicate_group_count": 1},
            "families": [
                {
                    "family_id": "family-a",
                    "family_name": "Breakout A",
                    "classification": "HIGH_PRIORITY_FAMILY",
                    "best_rank": 1,
                    "candidate_count": 3,
                    "requires_direct_data_validation": True,
                    "duplicate_or_distinct_assessment": "REPEATED_FAMILY_VARIANTS",
                    "classification_counts": {"READY_FOR_PAPER_FORWARD_OBSERVATION": 3},
                },
                {
                    "family_id": "family-b",
                    "family_name": "Confirmed B",
                    "classification": "PROMISING_FAMILY",
                    "best_rank": 2,
                    "candidate_count": 1,
                    "requires_direct_data_validation": False,
                    "duplicate_or_distinct_assessment": "GENUINELY_DISTINCT",
                    "classification_counts": {"READY_FOR_PAPER_FORWARD_OBSERVATION": 1},
                },
            ],
        },
    )
    _write_latest(
        root,
        "edge_magnitude_estimation",
        {
            "report_type": "EDGE_MAGNITUDE_ESTIMATION",
            "families": [
                {
                    "family_id": "family-a",
                    "classification": "SMALL_BUT_INTERESTING",
                    "direct_validation_status": "mostly_insufficient",
                    "proxy_dependence": "HIGH",
                    "data_blockers": ["proxy data dependence remains; direct candidate data validation required", "duplicate review effort risk"],
                    "contribution_estimates": {"conservative": {"net_expectancy_estimate": 0.001}},
                },
                {
                    "family_id": "family-b",
                    "classification": "POTENTIALLY_MATERIAL",
                    "direct_validation_status": "confirmed",
                    "proxy_dependence": "REDUCED_BY_DIRECT_CONFIRMATION",
                    "data_blockers": [],
                    "contribution_estimates": {"conservative": {"net_expectancy_estimate": 0.001}},
                },
            ],
        },
    )
    _write_latest(root, "portfolio_relevance_estimate", {"report_type": "PORTFOLIO_RELEVANCE_ESTIMATE", "summary": {"independent_family_estimate_base": 2}})


def test_search_overfit_guardrail_builds_profile_and_family_reviews(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = build_search_overfit_guardrail_report(root=tmp_path, created_at=NOW)
    profiles = {row["profile_id"]: row for row in report["trial_profile_reviews"]}
    families = {row["family_id"]: row for row in report["candidate_family_reviews"]}

    assert report["report_type"] == "SEARCH_OVERFIT_MULTIPLE_TESTING_GUARDRAIL"
    assert profiles["EXPANDED_OBSERVATION_TRIAL_5000"]["estimated_false_discovery_risk"] == "UNACCEPTABLE_OVERFIT_RISK"
    assert profiles["EXPANDED_OBSERVATION_TRIAL_5000"]["recommendation"] == "REJECT_PROFILE"
    assert families["family-a"]["recommendation"] == "REQUIRE_FAMILY_DEDUPLICATION"
    assert "REQUIRE_DIRECT_DATA" in families["family-a"]["required_actions"]
    assert families["family-b"]["recommendation"] == "PROCEED_TO_FORWARD_OBSERVATION"
    assert report["mandatory_rules"]["confidence_increase_from_search_selection_allowed"] is False
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["broker_execution_authorized"] is False


def test_search_overfit_guardrail_writes_requested_outputs(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_search_overfit_guardrail_report(root=tmp_path, created_at=NOW)

    paths = write_search_overfit_guardrail_report(report, root=tmp_path)

    for path in paths.values():
        assert path.exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["schema_id"] == "atlas_v2_research_os_search_overfit_guardrail_v1"
    assert paths["json"].name == "search_overfit_guardrail_report.json"
    assert paths["summary"].name == "search_overfit_guardrail_summary.md"
    assert "Search Overfit Guardrail" in paths["latest_summary"].read_text(encoding="utf-8")
