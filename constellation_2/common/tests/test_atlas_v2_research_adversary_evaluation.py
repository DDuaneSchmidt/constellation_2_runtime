from __future__ import annotations

import json
from pathlib import Path

import pytest

from constellation_2.common.atlas_v2_research_os.research_adversary_evaluation import (
    ResearchAdversaryEvaluationError,
    build_research_adversary_evaluation,
    write_research_adversary_evaluation_report,
)


def _write_review(root: Path, payload: dict) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / "research_adversary_review.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_failure(root: Path, *, failure_id: str = "FAIL_TEST") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{failure_id}.yaml"
    path.write_text(
        "\n".join(
            [
                f"id: {failure_id}",
                "what_we_expected: Regime context and source evidence would support the claim.",
                "what_failed: Regime mismatch and duplicate observations weakened the hypothesis.",
                "why_failed: Source lineage could not reconstruct the mechanism.",
                "what_to_try_next: Test regime controls and duplicate observation filters.",
                "status: OPEN",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _review(*, extra_assumption: str | None = None) -> dict:
    assumptions = [
        {"assumption_id": "a1", "statement": "Regime mismatch and duplicate observations are controlled."},
    ]
    if extra_assumption:
        assumptions.append({"assumption_id": "a2", "statement": extra_assumption})
    return {
        "review_id": "review-001",
        "assumptions": assumptions,
        "constraints": [
            {"constraint_id": "c1", "description": "Source lineage must reconstruct the mechanism."},
        ],
        "falsification_tests": [
            {"test_id": "f1", "description": "Reject if regime controls and duplicate filters remove the effect."},
        ],
        "supporting_evidence": ["Historical observation artifact mentions regime controls."],
        "weakening_evidence": ["Duplicate observations may explain the pattern."],
        "authority_boundary": {
            "research_only": True,
            "live_trading_authorized": False,
            "broker_execution_authorized": False,
            "capital_authorized": False,
            "position_sizing_authorized": False,
            "candidate_promotion_authorized": False,
            "trade_recommendation_authorized": False,
            "replay_override_authorized": False,
            "qualification_override_authorized": False,
        },
    }


def test_evaluation_accepts_valid_research_adversary_review_artifacts(tmp_path: Path) -> None:
    reviews = tmp_path / "reviews"
    failures = tmp_path / "failures"
    _write_review(reviews, _review())
    _write_failure(failures)

    report = build_research_adversary_evaluation(reviews=reviews, failures=failures, created_at="2026-06-05T00:00:00Z")

    assert report["summary"]["cases_evaluated"] == 1
    assert report["results"][0]["assumption_count"] == 1
    assert report["results"][0]["constraint_count"] == 1
    assert report["results"][0]["falsification_test_count"] == 1
    assert report["authority_boundary"]["evaluation_only"] is True


def test_evaluation_compares_assumptions_against_known_failures(tmp_path: Path) -> None:
    reviews = tmp_path / "reviews"
    failures = tmp_path / "failures"
    _write_review(reviews, _review())
    _write_failure(failures, failure_id="FAIL_REGIME_DUPLICATE")

    report = build_research_adversary_evaluation(reviews=reviews, failures=failures)
    result = report["results"][0]

    assert result["known_failure_mode_detected"] is True
    assert result["failure_mode_recall"] == 1.0
    assert result["assumption_recall"] == 1.0


def test_evaluation_records_missed_known_failures(tmp_path: Path) -> None:
    reviews = tmp_path / "reviews"
    failures = tmp_path / "failures"
    _write_review(reviews, _review())
    _write_failure(failures, failure_id="FAIL_REGIME_DUPLICATE")
    (failures / "FAIL_LATENCY.yaml").write_text(
        "id: FAIL_LATENCY\nwhat_failed: Latency timestamp sequencing broke event ordering.\nwhy_failed: Clock drift was not modeled.\n",
        encoding="utf-8",
    )

    report = build_research_adversary_evaluation(reviews=reviews, failures=failures)

    assert "FAIL_LATENCY" in report["results"][0]["missed_known_failure_modes"]
    assert report["summary"]["missed_known_failure_modes"] >= 1


def test_evaluation_records_false_positives(tmp_path: Path) -> None:
    reviews = tmp_path / "reviews"
    failures = tmp_path / "failures"
    _write_review(reviews, _review(extra_assumption="Lunar phase entropy controls the mechanism."))
    _write_failure(failures)

    report = build_research_adversary_evaluation(reviews=reviews, failures=failures)

    assert report["results"][0]["false_positive_count"] >= 1
    assert any("Lunar phase" in row for row in report["results"][0]["false_positives"])


def test_evaluation_rejects_authority_violating_language(tmp_path: Path) -> None:
    reviews = tmp_path / "reviews"
    failures = tmp_path / "failures"
    review = _review()
    review["assumptions"][0]["statement"] = "This should recommend a trade after review."
    _write_review(reviews, review)
    _write_failure(failures)

    with pytest.raises(ResearchAdversaryEvaluationError, match="authority-violating language rejected"):
        build_research_adversary_evaluation(reviews=reviews, failures=failures)


def test_markdown_includes_authority_boundary_verification(tmp_path: Path) -> None:
    reviews = tmp_path / "reviews"
    failures = tmp_path / "failures"
    out = tmp_path / "out"
    _write_review(reviews, _review())
    _write_failure(failures)

    report = build_research_adversary_evaluation(reviews=reviews, failures=failures)
    paths = write_research_adversary_evaluation_report(report, out)
    markdown = paths["markdown"].read_text(encoding="utf-8")

    assert "## Authority boundary verification" in markdown
    assert "no_trade_recommendations: True" in markdown
