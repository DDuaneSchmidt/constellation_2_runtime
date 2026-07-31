from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.cli import main
from constellation_2.common.atlas_v2_research_os.evidence_review_board import (
    FINAL_CLASSIFICATIONS,
    classify_family_review,
    run_evidence_review_board,
)

NOW = "2026-06-06T00:00:00Z"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_promising_evidence(root: Path) -> None:
    _write_json(
        root / "exact_replay_without_fallback" / "latest.json",
        {
            "report_type": "EXACT_REPLAY_WITHOUT_FALLBACK",
            "confidence_impact": "NONE",
            "summary": {"exact_replays_run": 3, "exact_blocked": 0},
            "family_repeatability": [
                {"family_id": "family_alpha", "family_classification": "EXACT_REPEATABLE_STRONG", "mechanism": "REVERSAL"}
            ],
        },
    )
    _write_json(
        root / "holdout_replay_validation" / "latest.json",
        {
            "report_type": "HOLDOUT_REPLAY_VALIDATION",
            "summary": {"families_survived_holdout": ["family_alpha"], "families_data_blocked": [], "methodology_confidence_impact": "NONE"},
            "family_validations": [
                {
                    "family_id": "family_alpha",
                    "classification": "HOLDOUT_SURVIVED",
                    "family_definition": {"mechanism": "REVERSAL", "regime": "TRENDING", "timeframes": ["30m"]},
                }
            ],
        },
    )
    _write_json(
        root / "holdout_readiness_audit" / "latest.json",
        {"report_type": "HOLDOUT_READINESS_AUDIT", "summary": {"readiness_counts": {"READY": 1}, "confidence_impact": "NONE"}},
    )
    _write_json(
        root / "family_stability_analysis" / "latest.json",
        {
            "report_type": "FAMILY_STABILITY_ANALYSIS",
            "summary": {"families_analyzed": 1, "classification_counts": {"STABLE_STRONG": 1}},
            "family_rows": [
                {"family_id": "family_alpha", "classification": "STABLE_STRONG", "mechanism": "REVERSAL", "regime": "TRENDING", "timeframe": "30M"}
            ],
        },
    )
    _write_json(
        root / "net_of_cost_evidence" / "latest.json",
        {
            "report_type": "NET_OF_COST_EVIDENCE",
            "confidence_impact": "NONE",
            "summary": {"net_surviving_families": 1, "cost_eroded_families": 0, "confidence_impact": "NONE"},
            "family_results": [
                {"family_id": "family_alpha", "cost_scenario": "10bps", "family_classification": "NET_SURVIVES_STRONG"}
            ],
        },
    )
    _write_json(
        root / "forward_observation_starter" / "latest.json",
        {
            "report_type": "FORWARD_OBSERVATION_STARTER",
            "summary": {"target_family": "family_alpha", "target_status": "OBSERVATION_QUEUE_ACTIVE", "confidence_impact": "NONE"},
            "target_family": {"family_id": "family_alpha"},
            "forward_observation_status": [
                {"family_id": "family_alpha", "candidate_id": "cand", "status_summary": "OBSERVATION_QUEUE_ACTIVE"}
            ],
        },
    )
    _write_json(
        root / "evidence_lineage_graph" / "evidence_lineage_graph.json",
        {
            "report_type": "EVIDENCE_LINEAGE_GRAPH",
            "summary": {"families_analyzed": 1, "edge_count": 4, "confidence_impact": "NONE"},
            "families": [{"family_id": "family_alpha", "evidence_density": 1.0}],
        },
    )


def test_build_119_reviews_all_evidence_and_writes_requested_outputs(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _seed_promising_evidence(root)

    report = run_evidence_review_board(root=root, created_at=NOW)

    assert report["build"] == "119"
    assert report["final_classification_values"] == list(FINAL_CLASSIFICATIONS)
    assert report["final_report"]["overall_conclusion"] == "RESEARCH_PROMISING"
    assert report["final_report"]["strongest_family"] == "family_alpha"
    assert report["required_questions"]["is_a_signal_likely_present"] == "YES"
    assert report["required_questions"]["is_the_signal_robust"] == "YES"
    assert report["required_questions"]["is_the_signal_economically_meaningful"] == "YES"
    assert report["authority_boundary"]["trade_recommendation_authorized"] is False
    assert report["authority_boundary"]["confidence_impact"] == "NONE"

    out_dir = root / "evidence_review_board"
    for name in ["latest.json", "latest_summary.md", "evidence_scorecard.csv", "family_review.csv", "unresolved_questions.csv"]:
        assert (out_dir / name).exists(), name
    with (out_dir / "family_review.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["family_id"] == "family_alpha"
    assert rows[0]["final_classification"] == "RESEARCH_PROMISING"


def test_build_119_missing_evidence_is_inconclusive_not_promoted(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _write_json(
        root / "family_stability_analysis" / "latest.json",
        {"family_rows": [{"family_id": "family_beta", "classification": "INSUFFICIENT_EVIDENCE"}], "summary": {"confidence_impact": "NONE"}},
    )

    report = run_evidence_review_board(root=root, created_at=NOW)

    assert report["final_report"]["overall_conclusion"] == "RESEARCH_INCONCLUSIVE"
    assert report["family_review"][0]["final_classification"] == "RESEARCH_INCONCLUSIVE"
    assert report["unresolved_questions"]
    assert "missing evidence layers" in report["final_report"]["confidence_statement"]
    assert classify_family_review("EXACT_NOT_REPEATABLE", "HOLDOUT_FAILED", "NET_FAILED", "UNSTABLE") == "RESEARCH_REJECTED"


def test_cli_writes_evidence_review_board(tmp_path: Path, capsys) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _seed_promising_evidence(root)

    rc = main(["--root", str(root), "--evidence-review-board"])

    captured = capsys.readouterr()
    assert rc == 0
    assert "evidence_review_board/latest.json" in captured.out
    assert (root / "evidence_review_board" / "latest.json").exists()
