from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.expansion_evidence_review import (
    ALLOWED_DECISIONS,
    build_expansion_evidence_review,
    write_expansion_evidence_review,
)

NOW = "2026-06-06T00:00:00Z"


def test_expansion_evidence_review_continues_tsla_only_when_neighbors_are_missing(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "final_research_verdict" / "latest.json",
        {
            "summary": {"final_verdict": "CONTINUE_TSLA_ONLY", "confidence_impact": "NONE"},
            "evidence_state": {
                "exact_classification": "EXACT_CONFIRMED_STRONG",
                "exact_tsla_samples": 338,
                "tsla_cost_classification": "POSSIBLY_VIABLE",
                "tsla_net_expectancy_10bps": 0.000496,
            },
        },
    )
    _write_json(
        tmp_path / "controlled_similar_symbol_expansion" / "latest.json",
        {
            "summary": {"symbols_confirmed": 1, "symbols_weak": 1, "symbols_failed": 1, "symbols_blocked": 3},
            "similar_symbol_comparison": [
                {"symbol": "TSLA", "classification": "SIMILAR_SYMBOL_CONFIRMED"},
                {"symbol": "META", "classification": "SIMILAR_SYMBOL_WEAK"},
            ],
        },
    )
    _write_json(tmp_path / "exact_replay_without_fallback" / "latest.json", {"candidate_results": []})
    _write_json(tmp_path / "net_of_cost_evidence" / "latest.json", {"summary": {"net_surviving_families": 1}})

    report = build_expansion_evidence_review(root=tmp_path, created_at=NOW)

    assert report["summary"]["decision"] == "CONTINUE_TSLA_ONLY"
    assert report["allowed_decisions"] == ALLOWED_DECISIONS
    assert report["approved_next_surface"][0]["symbol"] == "TSLA"
    rejected_surfaces = {row["surface"] for row in report["rejected_surfaces"]}
    assert "similar_symbols" in rejected_surfaces
    assert "reversal_neighborhood" in rejected_surfaces
    assert "timeframe_neighborhood" in rejected_surfaces
    assert "regime_boundary" in rejected_surfaces
    assert report["summary"]["trading_authority"] is False
    assert report["summary"]["promotion_authority"] is False


def test_expansion_evidence_review_writes_requested_outputs(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "final_research_verdict" / "latest.json",
        {"summary": {"final_verdict": "CONTINUE_TSLA_ONLY", "confidence_impact": "NONE"}, "evidence_state": {}},
    )

    paths = write_expansion_evidence_review(build_expansion_evidence_review(root=tmp_path, created_at=NOW), root=tmp_path)

    assert paths["latest_json"].name == "latest.json"
    assert paths["latest_summary"].name == "latest_summary.md"
    assert paths["scorecard"].name == "expansion_scorecard.csv"
    assert paths["approved"].name == "approved_next_surface.csv"
    assert paths["rejected"].name == "rejected_surfaces.csv"
    assert paths["next_phase"].name == "next_phase_decision.csv"
    for path in paths.values():
        assert path.exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["schema_id"] == "atlas_v2_research_os_expansion_evidence_review"
    assert "Expansion Evidence Review" in paths["latest_summary"].read_text(encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
