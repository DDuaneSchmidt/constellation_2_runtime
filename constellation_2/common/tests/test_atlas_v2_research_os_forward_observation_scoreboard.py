from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.forward_observation_scoreboard import (
    build_forward_observation_scoreboard,
    write_forward_observation_scoreboard,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _base_inputs(root: Path) -> None:
    _write_json(
        root / "exact_replay_without_fallback" / "latest.json",
        {
            "family_repeatability": [
                {
                    "family_id": "family_strong",
                    "family_classification": "EXACT_REPEATABLE_STRONG",
                    "confidence_impact": "SMALL_INCREASE",
                },
                {
                    "family_id": "family_empty",
                    "family_classification": "EXACT_REPEATABLE_WEAK",
                    "confidence_impact": "SMALL_INCREASE",
                },
                {
                    "family_id": "family_failed_exact",
                    "family_classification": "EXACT_NOT_REPEATABLE",
                    "confidence_impact": "DECREASE",
                },
            ],
            "candidate_results": [
                {"candidate_id": "candidate_a", "family_id": "family_strong"},
                {"candidate_id": "candidate_b", "family_id": "family_empty"},
                {"candidate_id": "candidate_c", "family_id": "family_failed_exact"},
            ],
        },
    )
    _write_json(
        root / "family_paper_forward_observation" / "latest.json",
        {
            "plans": [
                {
                    "family_id": "family_strong",
                    "family_name": "Strong Family",
                    "minimum_sample_size": 5,
                    "representative_candidates": [{"candidate_id": "candidate_a"}],
                },
                {
                    "family_id": "family_empty",
                    "family_name": "Empty Family",
                    "minimum_sample_size": 5,
                    "representative_candidates": [{"candidate_id": "candidate_b"}],
                },
            ]
        },
    )


def test_forward_observation_scoreboard_classifies_mapped_forward_evidence(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _base_inputs(root)
    _write_json(
        root / "paper_forward_outcomes" / "latest.json",
        {
            "outcomes": [
                {
                    "candidate_id": "candidate_a",
                    "status": "SURVIVED",
                    "sample_size": 6,
                    "expectancy": 0.01,
                    "profit_factor": 2.0,
                    "wins": 5,
                    "losses": 1,
                    "observation_start": "2026-06-01",
                    "observation_end": "2026-06-05",
                    "notes": ["supported"],
                    "metadata": {"minimum_sample_size": 5, "metrics": {"sample_size": 6}},
                }
            ]
        },
    )

    report = build_forward_observation_scoreboard(root=root, created_at="2026-06-06T00:00:00Z")

    family_by_id = {row["family_id"]: row for row in report["family_forward_scoreboard"]}
    candidate_by_id = {row["candidate_id"]: row for row in report["candidate_forward_scoreboard"]}
    assert family_by_id["family_strong"]["forward_classification"] == "FORWARD_STRONG"
    assert family_by_id["family_empty"]["forward_classification"] == "FORWARD_INSUFFICIENT_SAMPLE"
    assert candidate_by_id["candidate_a"]["forward_classification"] == "FORWARD_STRONG"
    assert candidate_by_id["candidate_b"]["source_status"] == "NO_FORWARD_OUTCOME"
    assert report["summary"]["strongest_family"] == "family_strong"
    assert "family_strong" in report["summary"]["families_improving"]


def test_forward_observation_scoreboard_reports_unmapped_outcomes_without_family_fabrication(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _base_inputs(root)
    _write_json(
        root / "paper_forward_outcomes" / "latest.json",
        {
            "outcomes": [
                {"candidate_id": "unknown", "status": "WEAKENED", "sample_size": 6, "expectancy": -0.02, "profit_factor": 0.2, "metadata": {"minimum_sample_size": 5}}
            ]
        },
    )

    report = build_forward_observation_scoreboard(root=root, created_at="2026-06-06T00:00:00Z")

    family_by_id = {row["family_id"]: row for row in report["family_forward_scoreboard"]}
    assert family_by_id["UNMAPPED"]["forward_classification"] == "FORWARD_FAILED"
    assert family_by_id["UNMAPPED"]["direction"] == "WEAKENING"
    assert any(row["candidate_id"] == "unknown" and row["family_id"] == "UNMAPPED" for row in report["candidate_forward_scoreboard"])


def test_write_forward_observation_scoreboard_outputs_required_files(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _base_inputs(root)
    _write_json(root / "paper_forward_outcomes" / "latest.json", {"outcomes": []})
    report = build_forward_observation_scoreboard(root=root, created_at="2026-06-06T00:00:00Z")

    paths = write_forward_observation_scoreboard(report, root=root)

    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
    assert paths["family_forward_scoreboard"].exists()
    assert paths["candidate_forward_scoreboard"].exists()
