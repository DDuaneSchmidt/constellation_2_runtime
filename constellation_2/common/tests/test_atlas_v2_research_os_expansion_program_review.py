from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.expansion_program_review import (
    FINAL_DECISIONS,
    build_expansion_program_review,
    write_expansion_program_review,
)

NOW = "2026-06-06T00:00:00Z"


def test_expansion_program_review_synthesizes_tsla_only_without_expansion(tmp_path: Path) -> None:
    _seed_sources(tmp_path)

    report = build_expansion_program_review(root=tmp_path, created_at=NOW)

    assert report["build"] == "183-186"
    assert report["final_decisions"] == FINAL_DECISIONS
    assert report["summary"]["program_decision"] == "NO_EXPANSION_JUSTIFIED"
    assert report["summary"]["strongest_supported_decision"] == "TSLA_ONLY"
    assert report["summary"]["did_discover_tsla_anomaly"] == "YES_RESEARCH_ONLY"
    assert report["summary"]["did_discover_high_volatility_phenomenon"] == "NO"
    assert report["summary"]["did_discover_growth_stock_phenomenon"] == "NO"
    assert report["summary"]["did_discover_broader_market_phenomenon"] == "NO"
    assert report["summary"]["trading_authority"] is False
    assert report["summary"]["recommendation_authority"] is False

    decisions = {row["question_id"]: row for row in report["expansion_decision"]}
    assert decisions["Q1"]["final_decision"] == "TSLA_ONLY"
    assert decisions["Q2"]["final_decision"] == "NO_EXPANSION_JUSTIFIED"
    assert decisions["Q3"]["final_decision"] == "NO_EXPANSION_JUSTIFIED"
    assert decisions["Q4"]["final_decision"] == "NO_EXPANSION_JUSTIFIED"
    assert decisions["Q5"]["final_decision"] == "NO_EXPANSION_JUSTIFIED"

    rankings = report["survivor_rankings"]
    assert rankings[0]["decision"] == "TSLA_ONLY"
    assert rankings[0]["support_level"] == "SUPPORTED_RESEARCH_ONLY"
    assert {row["surface"] for row in rankings[1:]} == {
        "HIGH_VOLATILITY_CLUSTER",
        "LARGE_CAP_GROWTH_CLUSTER",
        "BROADER_REVERSAL_SURFACE",
    }


def test_expansion_program_review_writes_requested_outputs_only(tmp_path: Path) -> None:
    _seed_sources(tmp_path)

    paths = write_expansion_program_review(build_expansion_program_review(root=tmp_path, created_at=NOW), root=tmp_path)

    assert sorted(path.name for path in paths.values()) == [
        "expansion_decision.csv",
        "expansion_scorecard.csv",
        "latest_summary.md",
        "survivor_inventory.csv",
        "survivor_rankings.csv",
    ]
    for path in paths.values():
        assert path.exists()

    decision_rows = _read_csv(paths["decision"])
    assert decision_rows[-1]["final_decision"] == "NO_EXPANSION_JUSTIFIED"
    assert "Program decision: NO_EXPANSION_JUSTIFIED" in paths["summary"].read_text(encoding="utf-8")


def _seed_sources(root: Path) -> None:
    _write_json(
        root / "final_research_verdict" / "latest.json",
        {
            "summary": {
                "final_verdict": "CONTINUE_TSLA_ONLY",
                "confidence_impact": "NONE",
                "target_family_id": "family_59cc928bca30cc44",
            },
            "evidence_state": {
                "exact_classification": "EXACT_CONFIRMED_STRONG",
                "exact_tsla_samples": 338,
                "exact_tsla_expectancy": 0.001496,
                "tsla_cost_classification": "POSSIBLY_VIABLE",
                "tsla_net_expectancy_10bps": 0.000496,
                "generalization_classification": "FRAGILE",
                "surviving_symbol": "TSLA",
                "surviving_symbol_classification": "SYMBOL_STRONG",
            },
        },
    )
    _write_json(
        root / "controlled_similar_symbol_expansion" / "latest.json",
        {
            "summary": {
                "symbols_confirmed": 1,
                "symbols_weak": 1,
                "symbols_failed": 1,
                "symbols_blocked": 3,
                "expansion_conclusion": "No similar-symbol expansion confirmed beyond TSLA.",
            },
            "similar_symbol_comparison": [
                {"symbol": "TSLA", "classification": "SIMILAR_SYMBOL_CONFIRMED"},
                {"symbol": "META", "classification": "SIMILAR_SYMBOL_WEAK"},
            ],
        },
    )
    _write_json(
        root / "volatility_profile_expansion" / "latest.json",
        {
            "summary": {
                "overall_classification": "VOLATILITY_WEAK",
                "key_question_answer": "Volatility may contribute, but high-bucket evidence is incomplete or not confirmed across multiple symbols.",
            },
            "bucket_results": [
                {
                    "volatility_bucket": "HIGH_VOLATILITY",
                    "bucket_classification": "VOLATILITY_WEAK",
                    "symbols_replayed": 1,
                    "mean_net_expectancy_10bps": 0.000496,
                }
            ],
        },
    )
    _write_json(
        root / "large_cap_growth_expansion" / "latest.json",
        {
            "summary": {
                "top_symbol": "TSLA",
                "expansion_conclusion": "No non-TSLA confirmation; weak positive evidence exists for MSFT, META, AAPL and blocked symbols are GOOGL, NFLX, NVDA.",
                "symbols_weak": 3,
                "symbols_failed": 4,
            },
            "cross_symbol_ranking": [
                {"rank": 1, "symbol": "TSLA", "classification": "EXPANSION_CONFIRMED"},
                {"rank": 2, "symbol": "MSFT", "classification": "EXPANSION_WEAK"},
            ],
        },
    )
    _write_json(
        root / "reversal_neighborhood_expansion" / "latest.json",
        {"summary": {"top_classification": "VARIANT_FRAGILE"}},
    )
    _write_json(root / "exact_replay_without_fallback" / "latest.json", {"summary": {}})
    _write_json(root / "net_of_cost_evidence" / "latest.json", {"summary": {}})
    _write_json(root / "expansion_evidence_review" / "latest.json", {"summary": {"decision": "CONTINUE_TSLA_ONLY"}})
    _write_json(root / "controlled_surface_expansion_gate" / "latest.json", {"summary": {"decision": "EXPAND_NARROW_TSLA_ONLY"}})


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))
