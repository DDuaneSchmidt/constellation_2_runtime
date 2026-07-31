from __future__ import annotations

import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.atlas_v2_research_os.expanded_search_gate_audit import (
    FAILURE_CAUSES,
    build_expanded_search_gate_audit_report,
    write_expanded_search_gate_audit_report,
)

NOW = "2026-06-05T00:00:00Z"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_sources(root: Path) -> None:
    _write_json(root / "expanded_search_trial" / "latest.json", {
        "final_qualification": {
            "summary": {
                "hypotheses_generated": 770,
                "backtest_supported_candidates": 427,
                "edge_eligible_candidates": 0,
                "final_eligible_candidates": 0,
                "average_supported_expectancy": 0.001467,
                "average_supported_profit_factor": 1.212903,
            },
            "backtest_supported_examples": [
                {
                    "candidate_id": "ptc_expanded_a",
                    "mechanism": "BREAKOUT",
                    "regime": "BULL",
                    "edge_score": 0.695373,
                    "backtest_classification": "BACKTEST_SUPPORTED",
                    "paper_forward_ready": False,
                }
            ],
        },
        "comparison_against_prior_baseline": {
            "new_families": 0,
            "robust_families": 0,
            "promising_but_data_blocked_families": 427,
            "supported_diagnostic_families": 427,
            "supported_duplicate_families": 12,
            "best_new_regimes": ["BULL"],
            "best_market_structures": ["FAILED_BREAKOUT"],
            "best_session_contexts": ["OPEN"],
            "quality_vs_baseline": "MIXED_DATA_BLOCKED",
            "diversity_vs_baseline": "IMPROVED_DIAGNOSTIC_DIVERSITY_ONLY",
        },
        "observation_dimensions": {"regimes_covered": 8, "market_structures_covered": 9, "session_contexts_covered": 8},
        "family_discovery": {
            "backtest_supported_families": [
                {
                    "family_id": "expanded_family_a",
                    "family_name": "BREAKOUT / BULL",
                    "candidate_ids": ["ptc_expanded_a"],
                }
            ]
        },
    })
    _write_json(root / "backtest_aware_final_qualification" / "latest.json", {
        "summary": {
            "candidates_evaluated": 600,
            "backtest_supported_candidates": 228,
            "final_eligible_candidates": 110,
            "average_final_score": 0.664326,
            "median_final_score": 0.666771,
            "average_supported_final_score": 0.693511,
        },
        "backtest_supported_examples": [
            {
                "candidate_id": "ptc_base_a",
                "final_score": 0.720111,
                "expectancy": 0.004375,
                "profit_factor": 2.149729,
                "sample_size": 102,
                "penalties": {
                    "proxy_data_penalty": 0.025,
                    "missing_evidence_penalty": 0.0,
                    "intraday_daily_mismatch_penalty": 0.0,
                },
            }
        ],
        "final_eligible_examples": [],
    })
    _write_json(root / "final_candidate_ranking" / "latest.json", {"campaign_candidate_preview": []})
    _write_json(root / "family_robustness_review" / "latest.json", {"classification_counts": {"ROBUST_ENOUGH_TO_OBSERVE": 2}})
    _write_json(root / "candidate_family_discovery" / "latest.json", {"summary": {"family_count": 30}})
    _write_json(root / "edge_qualification_gate_audit" / "latest.json", {
        "edge_score_distribution_backtest_supported": {"count": 228, "average": 0.678768},
        "component_suppression_rank": [{"component": "evidence_maturity", "average_suppression_vs_max": 0.0595}],
        "daily_proxy_limitations": {"correctly_penalized_in_edge_qualification": False},
        "sample_size_penalties": {"historical_replay_sample_size": 12.0},
        "regime_penalties": {"regime_coverage_average": 0.55},
    })


def test_expanded_search_gate_audit_classifies_gate_failure(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    report = build_expanded_search_gate_audit_report(root=tmp_path, created_at=NOW)
    comparison = report["expanded_vs_split_search"]
    assert comparison["expanded_hypotheses"] == 770
    assert comparison["expanded_backtest_supported"] == 427
    assert comparison["expanded_final_eligible"] == 0
    assert comparison["baseline_final_eligible"] == 110
    causes = report["failure_cause_classification"]["primary_failure_causes"]
    assert "PENALTY_DOMINATED" in causes
    assert "DATA_BLOCKED" in causes
    assert "SCORING_PIPELINE_MISMATCH" in causes
    assert set(causes).issubset(set(FAILURE_CAUSES))
    assert report["near_threshold_expanded_candidates"][0]["candidate_id"] == "ptc_expanded_a"
    assert report["authority_boundary"]["live_trading_authorized"] is False
    assert report["authority_boundary"]["capital_authorized"] is False


def test_expanded_search_gate_audit_writes_requested_outputs(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    paths = write_expanded_search_gate_audit_report(root=tmp_path, day="2026-06-05", created_at=NOW)
    for path in paths.values():
        assert path.exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["report_type"] == "EXPANDED_SEARCH_GATE_AUDIT"
    assert paths["json"].name == "expanded_search_gate_audit_report.json"
    assert paths["summary"].name == "expanded_search_gate_audit_summary.md"
    assert "Expanded Search Gate Audit" in paths["latest_summary"].read_text(encoding="utf-8")
