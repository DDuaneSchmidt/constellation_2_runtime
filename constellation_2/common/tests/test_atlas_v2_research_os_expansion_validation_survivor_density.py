from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.expansion_validation_survivor_density import (
    build_expansion_validation_survivor_density,
    run_expansion_validation_survivor_density,
)


def test_survivor_density_cost_robustness_and_blocked_distinction(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "mechanism_expansion_program" / "latest.json",
        {
            "target_mechanisms": [{"mechanism": "REVERSAL", "regime": "TRENDING"}],
            "target_symbols": ["AAA", "BBB"],
            "timeframes_tested": ["30m"],
            "blocked_mechanism_tests": [
                {"candidate_id": "blocked", "family_id": "fam", "mechanism": "REVERSAL", "symbol": "CCC", "timeframe": "30m", "regime": "TRENDING", "blocker": "MISSING_EXACT_FILE"}
            ],
            "exact_replay_results": [
                _surface("b", "BREAKOUT", "AAA", "1h", 100, 0.004, 2.0),
                _surface("a", "REVERSAL", "AAA", "30m", 100, 0.003, 1.8),
                _surface("c", "REVERSAL", "BBB", "30m", 100, -0.001, 0.8),
                _surface("d", "REVERSAL", "DDD", "30m", 10, 0.01, 4.0),
            ],
            "cost_adjusted_mechanism_results": [],
        },
    )

    report = build_expansion_validation_survivor_density(tmp_path, created_at="2026-06-06T00:00:00Z")

    decision = report["expansion_validation_decision"][0]
    assert decision["total_surfaces_reviewed"] == 4
    assert decision["exact_survivors"] == 2
    assert decision["net_survivors"] == 2
    assert decision["cost_robust_survivors"] == 2
    assert decision["survivor_density"] == 0.5
    assert report["authority_boundary"]["live_trading"] is False
    assert report["authority_boundary"]["candidate_promotion"] is False

    inventory = report["cost_robust_survivor_inventory"]
    assert {row["candidate_id"] for row in inventory if row["survives_25bps"] == "true"} == {"a", "b"}
    assert all(row["candidate_id"] != "d" for row in inventory)

    reversal = next(row for row in report["mechanism_level_ranking"] if row["value"] == "REVERSAL")
    assert reversal["blocked_count"] == 1
    failures = {row["failure_mode"] for row in report["failure_mode_analysis"]}
    assert "gross negative" in failures
    assert "sample too small" in failures
    assert "blocked" in failures


def test_rankings_are_deterministic_and_required_files_are_written(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "mechanism_expansion_program" / "latest.json",
        {
            "target_mechanisms": [{"mechanism": "REVERSAL", "regime": "TRENDING"}],
            "target_symbols": ["AAA", "BBB"],
            "timeframes_tested": ["30m"],
            "blocked_mechanism_tests": [],
            "exact_replay_results": [
                _surface("z", "REVERSAL", "BBB", "30m", 100, 0.002, 1.4),
                _surface("a", "BREAKOUT", "AAA", "1h", 100, 0.004, 2.0),
            ],
            "cost_adjusted_mechanism_results": [],
        },
    )

    first = build_expansion_validation_survivor_density(tmp_path, created_at="2026-06-06T00:00:00Z")
    second = run_expansion_validation_survivor_density(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert [row["value"] for row in first["mechanism_level_ranking"][:2]] == [row["value"] for row in second["mechanism_level_ranking"][:2]]
    assert first["source_status"]["mechanism_survivor_audit"]["exists"] is False
    out_dir = tmp_path / "expansion_validation_survivor_density"
    for filename in [
        "latest.json",
        "latest_summary.md",
        "survivor_density_baseline.csv",
        "mechanism_level_ranking.csv",
        "symbol_level_ranking.csv",
        "timeframe_level_ranking.csv",
        "regime_level_ranking.csv",
        "interaction_map.csv",
        "cost_robust_survivor_inventory.csv",
        "failure_mode_analysis.csv",
        "candidate_pipeline_yield.csv",
        "expansion_validation_decision.csv",
    ]:
        assert (out_dir / filename).exists()


def _surface(candidate_id: str, mechanism: str, symbol: str, timeframe: str, sample_size: int, expectancy: float, profit_factor: float) -> dict:
    return {
        "candidate_id": candidate_id,
        "family_id": f"family_{mechanism.lower()}",
        "mechanism": mechanism,
        "regime": "TRENDING",
        "symbol": symbol,
        "timeframe": timeframe,
        "sample_size": sample_size,
        "expectancy": expectancy,
        "profit_factor": profit_factor,
        "classification": "EXACT_CONFIRMED_STRONG" if expectancy > 0 else "EXACT_FAILED",
        "fallback_used": "false",
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
