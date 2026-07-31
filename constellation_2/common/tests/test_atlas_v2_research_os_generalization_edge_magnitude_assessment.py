from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.generalization_edge_magnitude_assessment import (
    TARGET_FAMILY_ID,
    build_generalization_edge_magnitude_assessment,
    run_generalization_edge_magnitude_assessment,
)


NOW = "2026-06-06T00:00:00Z"


def test_generalization_assessment_loads_required_reports_and_writes_outputs(tmp_path: Path) -> None:
    _write_inputs(
        tmp_path,
        exact_rows=[
            _exact("cand_a", "AAPL", "30m", "TRENDING", 100, 0.002, 1.6),
            _exact("cand_b", "MSFT", "30m", "TRENDING", 100, 0.002, 1.6),
        ],
        net_rows=[
            _net("cand_a", 100, 0.002, 1.6, 0.001, 1.5, "NET_SURVIVES_STRONG"),
            _net("cand_b", 100, 0.002, 1.6, 0.001, 1.5, "NET_SURVIVES_STRONG"),
        ],
    )

    report = run_generalization_edge_magnitude_assessment(tmp_path, created_at=NOW)

    assert report["source_input_status"]["exact_replay_without_fallback"]["loaded"] is True
    assert report["source_input_status"]["net_of_cost_evidence"]["loaded"] is True
    assert report["source_input_status"]["family_stability_analysis"]["loaded"] is True
    assert report["source_input_status"]["evidence_lineage_graph"]["loaded"] is True
    assert report["source_input_status"]["final_evidence_synthesis"]["loaded"] is True
    assert report["confidence_impact"] == "SMALL_INCREASE"
    assert report["overall_classification"] == "GENERALIZES_WEAKLY"
    assert "No parameter optimization is performed." in report["guardrails"]
    assert report["authority_boundary"]["research_only"] is True
    assert "live trading" in report["authority_boundary"]["forbidden_actions"]

    out_dir = tmp_path / "generalization_edge_magnitude_assessment"
    for filename in [
        "latest.json",
        "latest_summary.md",
        "cross_symbol_generalization.csv",
        "cross_regime_generalization.csv",
        "cross_timeframe_generalization.csv",
        "parameter_stability.csv",
        "portfolio_interaction_analysis.csv",
        "edge_magnitude_assessment.csv",
    ]:
        assert (out_dir / filename).exists()


def test_dimension_summaries_are_deterministic_and_no_optimization_is_performed(tmp_path: Path) -> None:
    _write_inputs(
        tmp_path,
        exact_rows=[
            _exact("cand_b", "MSFT", "30m", "TRENDING", 100, 0.002, 1.6),
            _exact("cand_a", "AAPL", "30m", "TRENDING", 100, 0.002, 1.6),
        ],
        net_rows=[
            _net("cand_b", 100, 0.002, 1.6, 0.001, 1.5, "NET_SURVIVES_STRONG"),
            _net("cand_a", 100, 0.002, 1.6, 0.001, 1.5, "NET_SURVIVES_STRONG"),
        ],
    )

    report = build_generalization_edge_magnitude_assessment(tmp_path, created_at=NOW)

    assert [row["symbol"] for row in report["cross_symbol_generalization"]] == ["AAPL", "MSFT"]
    assert report["cross_timeframe_generalization"] == [
        {
            "timeframe": "30m",
            "sample_count": 200,
            "expectancy": 0.002,
            "profit_factor": 1.6,
            "net_expectancy": 0.001,
            "classification": "TIMEFRAME_STRONG",
        }
    ]
    assert report["parameter_stability"][0]["classification"] == "PARAMETER_STABLE"
    assert report["guardrails"][1] == "No parameter optimization is performed."


def test_confidence_impact_none_when_net_edge_is_too_small_and_concentrated(tmp_path: Path) -> None:
    _write_inputs(
        tmp_path,
        exact_rows=[
            _exact("cand_a", "TSLA", "30m", "TRENDING", 100, 0.0015, 1.5),
            _exact("cand_b", "TSLA", "30m", "TRENDING", 100, 0.0015, 1.5),
            _exact("cand_c", "SPY", "30m", "TRENDING", 100, 0.0002, 1.2),
        ],
        net_rows=[
            _net("cand_a", 100, 0.0015, 1.5, 0.0005, 1.4, "NET_SURVIVES_STRONG"),
            _net("cand_b", 100, 0.0015, 1.5, 0.0005, 1.4, "NET_SURVIVES_STRONG"),
            _net("cand_c", 100, 0.0002, 1.2, -0.0008, 1.1, "COST_ERODED"),
        ],
    )

    report = build_generalization_edge_magnitude_assessment(tmp_path, created_at=NOW)

    assert report["confidence_impact"] == "NONE"
    assert any(row["diversification_status"] == "CONCENTRATED" for row in report["portfolio_interaction_analysis"])
    assert report["authority_boundary"]["research_only"] is True
    assert "candidate promotion" in report["authority_boundary"]["forbidden_actions"]


def _write_inputs(tmp_path: Path, *, exact_rows: list[dict], net_rows: list[dict]) -> None:
    _write_latest(tmp_path / "exact_replay_without_fallback", {"candidate_results": exact_rows})
    _write_latest(tmp_path / "net_of_cost_evidence", {"candidate_results": net_rows})
    _write_latest(tmp_path / "family_stability_analysis", {"family_stability_rankings": []})
    _write_json(tmp_path / "evidence_lineage_graph" / "evidence_lineage_graph.json", {"nodes": [], "edges": []})
    _write_latest(tmp_path / "final_evidence_synthesis", {"family_final_assessment": []})


def _write_latest(path: Path, payload: dict) -> None:
    path.mkdir(parents=True, exist_ok=True)
    _write_json(path / "latest.json", payload)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _exact(candidate_id: str, symbol: str, timeframe: str, regime: str, sample_size: int, expectancy: float, profit_factor: float) -> dict:
    return {
        "candidate_id": candidate_id,
        "family_id": TARGET_FAMILY_ID,
        "symbol": symbol,
        "timeframe": timeframe,
        "sample_size": sample_size,
        "expectancy": expectancy,
        "profit_factor": profit_factor,
        "fallback_used": False,
        "regime_bridged": regime,
        "classification": "EXACT_CONFIRMED_STRONG",
    }


def _net(candidate_id: str, sample_size: int, gross_expectancy: float, gross_profit_factor: float, net_expectancy: float, net_profit_factor: float, classification: str) -> dict:
    return {
        "candidate_id": candidate_id,
        "family_id": TARGET_FAMILY_ID,
        "cost_scenario": "10bps",
        "sample_size": sample_size,
        "gross_expectancy": gross_expectancy,
        "gross_profit_factor": gross_profit_factor,
        "net_expectancy": net_expectancy,
        "net_profit_factor": net_profit_factor,
        "classification": classification,
    }
