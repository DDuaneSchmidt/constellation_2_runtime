from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest

from constellation_2.common.atlas_v2_research_os.edge_qualification import compute_edge_score, explain_edge_score, qualify_edge
from constellation_2.common.atlas_v2_research_os.edge_qualification_models import EdgeQualificationInput


def strong_input() -> EdgeQualificationInput:
    return EdgeQualificationInput(
        source_artifact_ids=["artifact-1"],
        source_hypothesis_ids=["hyp-1"],
        source_experiment_ids=["exp-1"],
        source_memory_ids=["mem-1"],
        evidence_maturity=0.90,
        research_effectiveness=0.85,
        hypothesis_survival=0.80,
        failure_history=0.10,
        duplicate_risk=0.10,
        regime_coverage=0.80,
        candidate_quality_trend=0.80,
        learning_validation_trend=0.80,
        lineage_complete=True,
        governance_pass=True,
        evidence_level="PAPER_FORWARD_OBSERVATION",
        lifecycle_state="SUPPORTED",
    )


def test_compute_edge_score_uses_weighted_formula() -> None:
    score = compute_edge_score(strong_input())
    assert score["edge_score"] == pytest.approx(0.855)
    assert score["components"]["failure_penalty_inverse"] == pytest.approx(0.9)
    assert score["components"]["duplicate_penalty_inverse"] == pytest.approx(0.9)


def test_explain_edge_score_lists_component_contributions() -> None:
    explanation = explain_edge_score(strong_input())
    assert any("evidence_maturity contribution" in line for line in explanation)
    assert any("lineage_completeness contribution" in line for line in explanation)


def test_qualify_edge_accepts_strong_governed_lineage_complete_input() -> None:
    result = qualify_edge(strong_input(), qualification_id="edge-1", created_at="2026-06-05T00:00:00Z")
    assert result["eligible"] is True
    assert result["disqualification_reasons"] == []
    assert "governance passed" in result["qualification_reasons"]


def test_qualify_edge_disqualifies_generated_only_artifact_even_with_high_score() -> None:
    row = strong_input().to_dict()
    row["generated_only"] = True
    result = qualify_edge(row)
    assert result["eligible"] is False
    assert "generated-only evidence" in result["disqualification_reasons"]


def test_qualify_edge_disqualifies_incomplete_lineage() -> None:
    row = strong_input().to_dict()
    row["lineage_complete"] = False
    result = qualify_edge(row)
    assert result["eligible"] is False
    assert "lineage incomplete" in result["disqualification_reasons"]
