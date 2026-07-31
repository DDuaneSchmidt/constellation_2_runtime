from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.research_allocation_decisions_v1 import build_research_allocation_decisions_v1
from ops.aegis.research_capital_allocation_self_check_v1 import research_capital_allocation_failures_v1
from ops.aegis.research_capital_scoring_v1 import build_research_capital_scoring_v1
from ops.aegis.research_program_registry_v1 import build_research_program_registry_v1

DAY = "2026-05-31"


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed(root: Path, *, closed_samples: int = 0) -> dict:
    portfolio = {
        "schema_id": "aegis_research_portfolio",
        "day_utc": DAY,
        "theses": [
            {"thesis_id": "THESIS_TREND_PERSISTENCE_V1", "name": "Trend Persistence", "description": "Trend evidence."},
            {"thesis_id": "THESIS_EVENT_DISLOCATION_V1", "name": "Event Dislocation", "description": "Event evidence."},
        ],
        "hypotheses": [
            {
                "hypothesis_id": "HYP_TREND_20_60D_V1",
                "thesis_id": "THESIS_TREND_PERSISTENCE_V1",
                "name": "20-60 day trend",
                "state": "ACCUMULATING_EVIDENCE",
                "linked_candidates": ["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12"],
                "linked_paper_positions": ["p1", "p2"],
            },
            {
                "hypothesis_id": "HYP_EVENT_REACTION_V1",
                "thesis_id": "THESIS_EVENT_DISLOCATION_V1",
                "name": "Event reaction",
                "state": "ACCUMULATING_EVIDENCE",
                "linked_candidates": [],
                "linked_paper_positions": [],
            },
        ],
        "sleeve_implementations": [
            {"sleeve_id": "C2_TREND_EQ_PRIMARY_V1", "thesis_id": "THESIS_TREND_PERSISTENCE_V1", "hypothesis_id": "HYP_TREND_20_60D_V1"},
            {"sleeve_id": "C2_EVENT_REACTION_V1", "thesis_id": "THESIS_EVENT_DISLOCATION_V1", "hypothesis_id": "HYP_EVENT_REACTION_V1"},
        ],
    }
    suff = {
        "hypotheses": [
            {"hypothesis_id": "HYP_TREND_20_60D_V1", "usable_sample_count": closed_samples, "next_evidence_needed": max(0, 30 - closed_samples), "expectancy": 0.04 if closed_samples else None, "max_drawdown": -0.01 if closed_samples else None},
            {"hypothesis_id": "HYP_EVENT_REACTION_V1", "usable_sample_count": 0, "next_evidence_needed": 30, "expectancy": None, "max_drawdown": None},
        ]
    }
    lineage = {"evidence_coverage_panel": {"candidate_coverage_pct": 100, "sleeve_attribution_pct": 100, "mark_coverage_pct": 100, "validation_coverage_pct": 100}}
    _write(root / "reports" / "aegis_research_portfolio_v1" / DAY / "research_portfolio.v1.json", portfolio)
    _write(root / "reports" / "aegis_statistical_sufficiency_v1" / DAY / "statistical_sufficiency.v1.json", suff)
    _write(root / "reports" / "aegis_evidence_lineage_integrity_v1" / DAY / "evidence_lineage_integrity.v1.json", lineage)
    return portfolio


def test_research_program_registry_generation(tmp_path: Path) -> None:
    portfolio = _seed(tmp_path)
    registry = build_research_program_registry_v1(truth_root=tmp_path, day_utc=DAY, portfolio=portfolio)
    assert registry["summary"]["research_program_count"] == 2
    assert {row["research_program_id"] for row in registry["programs"]} == {"PROGRAM_TREND_PERSISTENCE_V1", "PROGRAM_EVENT_DISLOCATION_V1"}


def test_hypothesis_to_program_mapping(tmp_path: Path) -> None:
    portfolio = _seed(tmp_path)
    registry = build_research_program_registry_v1(truth_root=tmp_path, day_utc=DAY, portfolio=portfolio)
    trend = next(row for row in registry["programs"] if row["research_program_id"] == "PROGRAM_TREND_PERSISTENCE_V1")
    assert trend["linked_hypothesis_ids"] == ["HYP_TREND_20_60D_V1"]


def test_sleeve_to_program_mapping(tmp_path: Path) -> None:
    portfolio = _seed(tmp_path)
    registry = build_research_program_registry_v1(truth_root=tmp_path, day_utc=DAY, portfolio=portfolio)
    event = next(row for row in registry["programs"] if row["research_program_id"] == "PROGRAM_EVENT_DISLOCATION_V1")
    assert event["linked_sleeve_ids"] == ["C2_EVENT_REACTION_V1"]


def test_score_component_generation(tmp_path: Path) -> None:
    portfolio = _seed(tmp_path)
    registry = build_research_program_registry_v1(truth_root=tmp_path, day_utc=DAY, portfolio=portfolio)
    scoring = build_research_capital_scoring_v1(truth_root=tmp_path, day_utc=DAY, registry=registry)
    components = scoring["scores"][0]["score_components"]
    for key in ["candidate_yield_score", "validation_progress_score", "evidence_quality_score", "expected_value_signal_score", "time_to_decision_score", "diversification_value_score", "resource_efficiency_score", "staleness_dormancy_penalty", "duplication_penalty", "risk_drawdown_penalty"]:
        assert key in components


def test_recommendation_rule_behavior(tmp_path: Path) -> None:
    portfolio = _seed(tmp_path)
    registry = build_research_program_registry_v1(truth_root=tmp_path, day_utc=DAY, portfolio=portfolio)
    scoring = build_research_capital_scoring_v1(truth_root=tmp_path, day_utc=DAY, registry=registry)
    decisions = build_research_allocation_decisions_v1(truth_root=tmp_path, day_utc=DAY, registry=registry, scoring=scoring)
    trend = next(row for row in decisions["decisions"] if row["research_program_id"] == "PROGRAM_TREND_PERSISTENCE_V1")
    assert trend["recommendation"] in {"INVESTIGATE_MORE", "MAINTAIN"}
    assert trend["recommendation"] != "INCREASE"
    assert "INSUFFICIENT_CLOSED_OUTCOMES" in trend["reason_codes"]


def test_insufficient_outcomes_handling(tmp_path: Path) -> None:
    portfolio = _seed(tmp_path, closed_samples=0)
    registry = build_research_program_registry_v1(truth_root=tmp_path, day_utc=DAY, portfolio=portfolio)
    scoring = build_research_capital_scoring_v1(truth_root=tmp_path, day_utc=DAY, registry=registry)
    trend = next(row for row in scoring["scores"] if row["research_program_id"] == "PROGRAM_TREND_PERSISTENCE_V1")
    assert trend["outcome_proof_status"] == "INSUFFICIENT_OUTCOMES"
    assert trend["score_components"]["expected_value_signal_score"] == 0.0


def test_reason_code_requirements(tmp_path: Path) -> None:
    portfolio = _seed(tmp_path)
    registry = build_research_program_registry_v1(truth_root=tmp_path, day_utc=DAY, portfolio=portfolio)
    scoring = build_research_capital_scoring_v1(truth_root=tmp_path, day_utc=DAY, registry=registry)
    decisions = build_research_allocation_decisions_v1(truth_root=tmp_path, day_utc=DAY, registry=registry, scoring=scoring)
    assert all(row["reason_codes"] for row in decisions["decisions"])


def test_no_increase_for_retired_or_disproven_programs() -> None:
    failures = research_capital_allocation_failures_v1(
        {"programs": [{"research_program_id": "p", "linked_hypothesis_ids": ["h"], "linked_sleeve_ids": ["s"]}]},
        {"scores": [{"research_program_id": "p", "closed_sample_count": 1, "score_components": {"candidate_yield_score": 1, "validation_progress_score": 1, "evidence_quality_score": 1, "expected_value_signal_score": 0, "expected_value_signal_status": "AVAILABLE", "time_to_decision_score": 1, "diversification_value_score": 1, "resource_efficiency_score": 1, "staleness_dormancy_penalty": 0, "duplication_penalty": 0, "risk_drawdown_penalty": 0}}]},
        {"decisions": [{"allocation_decision_id": "d", "research_program_id": "p", "recommendation": "INCREASE", "reason_codes": ["VALIDATION_FAILURE"], "supporting_artifacts": ["x"]}]},
    )
    assert any(row["failure_code"] == "RETIRED_PROGRAM_INCREASED" for row in failures)


def test_deterministic_rerun_stability(tmp_path: Path) -> None:
    portfolio = _seed(tmp_path)
    first = build_research_program_registry_v1(truth_root=tmp_path, day_utc=DAY, portfolio=portfolio)
    second = build_research_program_registry_v1(truth_root=tmp_path, day_utc=DAY, portfolio=portfolio)
    assert first["content_hash"] == second["content_hash"]
    assert first["programs"] == second["programs"]


def test_self_check_success_path(tmp_path: Path) -> None:
    portfolio = _seed(tmp_path)
    registry = build_research_program_registry_v1(truth_root=tmp_path, day_utc=DAY, portfolio=portfolio)
    scoring = build_research_capital_scoring_v1(truth_root=tmp_path, day_utc=DAY, registry=registry)
    decisions = build_research_allocation_decisions_v1(truth_root=tmp_path, day_utc=DAY, registry=registry, scoring=scoring)
    assert research_capital_allocation_failures_v1(registry, scoring, decisions) == []


def test_self_check_failure_path() -> None:
    failures = research_capital_allocation_failures_v1(
        {"programs": [{"research_program_id": "p", "linked_hypothesis_ids": [], "linked_sleeve_ids": []}]},
        {"scores": [{"research_program_id": "p", "closed_sample_count": 0, "score_components": {"expected_value_signal_score": 2, "expected_value_signal_status": "AVAILABLE"}}]},
        {"decisions": [{"allocation_decision_id": "d", "research_program_id": "p", "recommendation": "BAD", "reason_codes": [], "supporting_artifacts": []}]},
    )
    codes = {row["failure_code"] for row in failures}
    assert {"MISSING_RESEARCH_PROGRAM_MAPPING", "MISSING_SCORE_COMPONENT", "OUTCOME_PROOF_OVERCLAIM", "MISSING_REASON_CODES", "MISSING_SOURCE_ARTIFACTS", "INVALID_RECOMMENDATION"}.issubset(codes)


def test_ui_data_shape(tmp_path: Path) -> None:
    portfolio = _seed(tmp_path)
    registry = build_research_program_registry_v1(truth_root=tmp_path, day_utc=DAY, portfolio=portfolio)
    scoring = build_research_capital_scoring_v1(truth_root=tmp_path, day_utc=DAY, registry=registry)
    decisions = build_research_allocation_decisions_v1(truth_root=tmp_path, day_utc=DAY, registry=registry, scoring=scoring)
    decision = decisions["decisions"][0]
    assert decision["allocation_type"] == "RESEARCH_ATTENTION_ONLY"
    assert {"research_program_id", "prior_allocation_units", "recommended_allocation_units", "recommendation", "allocation_score", "reason_codes"}.issubset(decision.keys())
