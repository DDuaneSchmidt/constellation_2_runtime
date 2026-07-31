from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.final_research_verdict import run_final_research_verdict


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _seed_verdict_inputs(root: Path) -> None:
    _write_json(root / "exact_replay_without_fallback" / "latest.json", {
        "summary": {"exact_blocked": 0, "exact_confirmed_strong": 18, "confidence_impact": "SMALL_INCREASE"}
    })
    _write_csv(root / "exact_replay_without_fallback" / "exact_replay_results.csv", [
        {
            "candidate_id": "ptc_backtest_final_651cd169dd508c4e",
            "family_id": "family_59cc928bca30cc44",
            "symbol": "TSLA",
            "timeframe": "30m",
            "sample_size": 338,
            "expectancy": 0.001496,
            "profit_factor": 1.498734,
            "classification": "EXACT_CONFIRMED_STRONG",
        }
    ])
    _write_json(root / "net_of_cost_evidence" / "latest.json", {
        "summary": {"net_survives_strong": 69, "confidence_impact": "NONE"}
    })
    _write_json(root / "execution_cost_failure_analysis" / "latest.json", {
        "summary": {
            "overall_classification": "EXECUTION_FRAGILE",
            "tsla_viability": {"classification": "POSSIBLY_VIABLE", "surviving_realistic_rows": 3},
        }
    })
    _write_csv(root / "execution_cost_failure_analysis" / "tsla_execution_viability.csv", [
        {
            "candidate_id": "ptc_backtest_final_651cd169dd508c4e",
            "family_id": "family_59cc928bca30cc44",
            "symbol": "TSLA",
            "timeframe": "30m",
            "sample_size": 338,
            "gross_expectancy": 0.001496,
            "break_even_cost_bps": 14.96,
            "net_expectancy_10bps": 0.000496,
            "net_profit_factor_10bps": 1.398734,
            "classification_10bps": "NET_SURVIVES_STRONG",
            "survives_realistic_assumptions": "true",
            "classification": "POSSIBLY_VIABLE",
        }
    ])
    _write_json(root / "generalization_edge_magnitude_assessment" / "latest.json", {
        "summary": {"overall_classification": "FRAGILE", "surviving_symbol_count": 1}
    })
    _write_csv(root / "generalization_edge_magnitude_assessment" / "cross_symbol_generalization.csv", [
        {"symbol": "TSLA", "sample_count": 1014, "net_expectancy": 0.000496, "classification": "SYMBOL_STRONG"},
        {"symbol": "AAPL", "sample_count": 1098, "net_expectancy": -0.000892, "classification": "SYMBOL_FAILED"},
    ])
    _write_json(root / "execution_realism_economic_viability" / "latest.json", {
        "summary": {
            "overall_classification": "FRAGILE_EDGE",
            "execution_classification": "EXECUTION_VIABLE",
            "liquidity_classification": "LOW_LIQUIDITY",
            "slippage_classification": "SLIPPAGE_SENSITIVE",
        }
    })
    _write_json(root / "forward_observation_loop" / "latest.json", {
        "summary": {
            "forward_classification": "FORWARD_NOT_STARTED",
            "new_signals": 0,
            "pending_observations": 0,
            "measured_outcomes": 0,
        }
    })
    _write_json(root / "holdout_replay" / "latest.json", {
        "summary": {"families_tested": 0, "survived": 0, "failed": 0, "confidence_impact": "NONE"}
    })
    _write_json(root / "holdout_readiness_after_backfill" / "latest.json", {
        "summary": {"remaining_blockers": 64}
    })
    _write_json(root / "research_decision_review" / "latest.json", {
        "summary": {
            "conclusion": {
                "decision": "CONTINUE_NARROW",
                "edge_durable": "NOT_PROVEN",
                "edge_exploitable": "FRAGILE_NOT_PROVEN_FOR_DEPLOYMENT",
            }
        }
    })
    _write_json(root / "controlled_surface_expansion_gate" / "latest.json", {
        "summary": {"decision": "EXPAND_NARROW_TSLA_ONLY"}
    })


def test_builds_147_150_final_verdict_continues_tsla_only(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _seed_verdict_inputs(root)

    report = run_final_research_verdict(root=root, created_at="2026-06-06T00:00:00Z")

    summary = report["summary"]
    assert report["build"] == "147-150"
    assert summary["final_verdict"] == "CONTINUE_TSLA_ONLY"
    assert summary["confidence_impact"] == "NONE"
    assert summary["signal_likely_exists"] == "YES"
    assert summary["edge_economically_meaningful"] == "PARTIAL_TSLA_ONLY"
    assert summary["edge_robust"] == "PARTIAL_NOT_BROADLY_ROBUST"
    assert summary["edge_likely_exploitable"] == "NOT_YET_PROVEN"
    assert summary["research_continue"] == "YES_TSLA_ONLY"
    assert summary["no_promotion_authority"] is True
    assert summary["no_trading_authority"] is True
    assert "No promotion authority" in report["authority_boundary"]
    assert (root / "final_research_verdict" / "latest.json").exists()
    assert (root / "final_research_verdict" / "latest_summary.md").exists()
    assert (root / "final_research_verdict" / "verdict_scorecard.csv").exists()
    assert (root / "final_research_verdict" / "evidence_chain.csv").exists()
    assert (root / "final_research_verdict" / "recommended_next_phase.csv").exists()


def test_builds_147_150_final_verdict_fails_closed_without_evidence(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"

    report = run_final_research_verdict(root=root, created_at="2026-06-06T00:00:00Z")

    assert report["summary"]["final_verdict"] == "INSUFFICIENT_EVIDENCE"
    assert report["summary"]["confidence_impact"] == "NONE"
    assert report["summary"]["research_continue"] == "NO"
    assert all(row["classification"] == "INSUFFICIENT_EVIDENCE" for row in report["verdict_scorecard"][:4])
    assert report["summary"]["no_promotion_authority"] is True
    assert report["summary"]["no_trading_authority"] is True
