from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.evidence_lineage_graph import build_evidence_lineage_graph, write_evidence_lineage_graph


NOW = "2026-06-05T00:00:00Z"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _seed_root(root: Path) -> None:
    artifacts = [
        {"artifact_id": "claim_seed_1", "artifact_type": "Question"},
        {"artifact_id": "claim_1", "artifact_type": "GeneratedResearchClaim"},
        {"artifact_id": "hyp_1", "artifact_type": "ResearchHypothesis"},
    ]
    _write_json(root / "artifact_index.json", {"artifacts": artifacts})
    _write_json(
        root / "artifacts" / "claim_seed_1.json",
        {
            "artifact_id": "claim_seed_1",
            "artifact_type": "Question",
            "metadata": {
                "claim_text": "Breakout observations may persist.",
                "mechanism_family": "BREAKOUT",
                "regime_context": "CHOP",
                "source_observation_ids": ["obs_1", "obs_2"],
            },
            "source_artifact_ids": [],
        },
    )
    _write_json(
        root / "artifacts" / "claim_1.json",
        {
            "artifact_id": "claim_1",
            "artifact_type": "GeneratedResearchClaim",
            "metadata": {"atlas_component_payload": {"claim_text": "Breakout claim.", "mechanism_family": "BREAKOUT"}},
            "source_artifact_ids": ["claim_seed_1"],
        },
    )
    _write_json(
        root / "artifacts" / "hyp_1.json",
        {
            "artifact_id": "hyp_1",
            "artifact_type": "ResearchHypothesis",
            "metadata": {"atlas_component_payload": {"mechanism_family": "BREAKOUT", "hypothesis_text": "Breakout hypothesis."}},
            "source_artifact_ids": ["claim_1"],
        },
    )
    _write_json(
        root / "candidate_backtests" / "latest.json",
        {
            "candidates": [
                {
                    "candidate_id": "cand_1",
                    "classification": "BACKTEST_SUPPORTED",
                    "mechanism": "BREAKOUT",
                    "backtest_spec": {"source_hypothesis_id": "hyp_1"},
                    "historical_replay_result": {"replay_id": "bt_1", "certification": {"status": "REPLAY_POSITIVE", "replay_id": "bt_1"}},
                }
            ]
        },
    )
    _write_json(
        root / "final_candidate_ranking" / "latest.json",
        {
            "top_20_robust_candidates": [
                {
                    "candidate_id": "cand_1",
                    "classification": "READY_FOR_PAPER_FORWARD_OBSERVATION",
                    "mechanism": "BREAKOUT",
                    "regime": "CHOP",
                    "candidate_source_observation_ids": ["obs_1"],
                }
            ],
            "campaign_candidate_preview": [],
        },
    )
    _write_json(
        root / "candidate_family_discovery" / "latest.json",
        {"families": [{"family_id": "family_1", "candidate_ids": ["cand_1"], "mechanism": "BREAKOUT", "regime": "CHOP"}]},
    )


def test_graph_created_and_lineage_links_valid(tmp_path: Path) -> None:
    _seed_root(tmp_path)
    report = build_evidence_lineage_graph(tmp_path, created_at=NOW)

    assert report["summary"]["observations_analyzed"] == 2
    assert report["summary"]["claims_analyzed"] == 2
    assert report["summary"]["families_analyzed"] == 1
    node_ids = set(report["nodes"])
    assert all(edge["source"] in node_ids and edge["target"] in node_ids for edge in report["edges"])
    assert any(edge for edge in report["edges"] if edge["source"] == "obs_1" and edge["target"] == "cand_1")


def test_productivity_reports_written(tmp_path: Path) -> None:
    _seed_root(tmp_path)
    paths = write_evidence_lineage_graph(tmp_path, created_at=NOW)

    assert paths["json"].name == "evidence_lineage_graph.json"
    assert paths["summary"].name == "latest_summary.md"
    for key in ["claim_productivity", "observation_productivity", "family_lineage_summary"]:
        assert paths[key].exists()
        with paths[key].open(newline="", encoding="utf-8") as handle:
            assert list(csv.DictReader(handle))


def test_rankings_are_deterministic(tmp_path: Path) -> None:
    _seed_root(tmp_path)
    first = build_evidence_lineage_graph(tmp_path, created_at=NOW)["rankings"]
    second = build_evidence_lineage_graph(tmp_path, created_at=NOW)["rankings"]
    assert first == second


def test_no_promotion_authority_emitted(tmp_path: Path) -> None:
    _seed_root(tmp_path)
    report = build_evidence_lineage_graph(tmp_path, created_at=NOW)

    assert report["summary"]["no_promotion_authority_emitted"] is True
    assert report["authority_boundary"]["candidate_generation_authorized"] is False
    assert report["authority_boundary"]["candidate_promotion_authorized"] is False
    assert report["authority_boundary"]["ranking_changes_authorized"] is False
