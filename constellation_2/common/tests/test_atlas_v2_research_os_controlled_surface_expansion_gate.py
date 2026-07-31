from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.controlled_surface_expansion_gate import run_controlled_surface_expansion_gate


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _seed_gate_inputs(root: Path, *, strongest_family: str = "family_59cc928bca30cc44") -> None:
    _write_json(root / "final_evidence_synthesis" / "latest.json", {
        "final_report": {
            "overall_conclusion": "RESEARCH_PROMISING",
            "strongest_family": strongest_family,
            "remaining_blockers": ["holdout data blocked", "net-of-cost evidence blocked"],
        }
    })
    _write_json(root / "cost_robustness_expansion" / "latest.json", {
        "family_cost_robustness": [
            {"family_id": "family_59cc928bca30cc44", "fragility_classification": "COST_SENSITIVE", "break_even_cost_bps": 10.0}
        ]
    })
    _write_json(root / "generalization_edge_magnitude_assessment" / "latest.json", {
        "overall_classification": "FRAGILE",
        "summary": {"symbol_count": 2, "surviving_symbol_count": 1, "timeframe_count": 1, "regime_count": 1},
        "cross_symbol_generalization": [
            {"symbol": "TSLA", "classification": "SYMBOL_STRONG", "net_expectancy": 0.000496},
            {"symbol": "AAPL", "classification": "SYMBOL_FAILED", "net_expectancy": -0.001},
        ],
    })
    _write_json(root / "execution_realism_economic_viability" / "latest.json", {
        "summary": {
            "overall_classification": "FRAGILE_EDGE",
            "execution_classification": "EXECUTION_VIABLE",
            "liquidity_classification": "LOW_LIQUIDITY",
            "slippage_classification": "SLIPPAGE_SENSITIVE",
        }
    })
    _write_csv(root / "databento_download_symbols" / "databento_download_symbols.csv", [
        {"symbol": "TSLA", "timeframes": "30m;1h"}
    ])


def test_builds_138_140_gate_approves_only_narrow_tsla_surface(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _seed_gate_inputs(root)

    report = run_controlled_surface_expansion_gate(root=root, created_at="2026-06-06T00:00:00Z")

    assert report["build"] == "138-140"
    assert report["summary"]["decision"] == "EXPAND_NARROW_TSLA_ONLY"
    assert report["summary"]["candidate_promotion"] is False
    assert report["summary"]["trading_authority"] is False
    assert report["approved_expansion_surface"] == [
        {
            "surface_id": "approved_001",
            "family_id": "family_59cc928bca30cc44",
            "dimension": "surviving_symbol_baseline_surface",
            "symbol": "TSLA",
            "timeframe": "30m",
            "mechanism": "REVERSAL",
            "regime": "TRENDING",
            "variant": "BASELINE_REVERSAL_ONLY",
            "approval_status": "APPROVED_FOR_RESEARCH_OBSERVATION_ONLY",
            "guardrail": "No new candidates, no promotion, no trading; collect TSLA-only direct evidence and rerun exact/net/holdout gates before any wider expansion.",
        }
    ]
    blocked_dimensions = {row["dimension"] for row in report["blocked_expansion_surface"]}
    assert blocked_dimensions >= {"nearby_symbols", "nearby_timeframes", "related_reversal_variants", "neighboring_regimes", "similar_high_liquidity_symbols", "tsla_like_volatility_profiles", "broad_surface"}
    assert {row["risk_level"] for row in report["expansion_risk_report"]} >= {"HIGH", "MEDIUM"}
    assert "No candidate promotion" in report["authority_boundary"]
    assert (root / "controlled_surface_expansion_gate" / "latest.json").exists()
    assert (root / "controlled_surface_expansion_gate" / "expansion_gate_decision.csv").exists()
    assert (root / "controlled_surface_expansion_gate" / "approved_expansion_surface.csv").exists()
    assert (root / "controlled_surface_expansion_gate" / "blocked_expansion_surface.csv").exists()
    assert (root / "controlled_surface_expansion_gate" / "expansion_risk_report.csv").exists()


def test_builds_138_140_gate_fails_closed_when_target_family_not_surviving(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _seed_gate_inputs(root, strongest_family="other_family")

    report = run_controlled_surface_expansion_gate(root=root, created_at="2026-06-06T00:00:00Z")

    assert report["summary"]["decision"] == "DO_NOT_EXPAND"
    assert report["approved_expansion_surface"] == []
    assert all(row["blocked_decision"] == "DO_NOT_EXPAND" for row in report["blocked_expansion_surface"])
    assert report["summary"]["candidate_promotion"] is False
    assert report["summary"]["trading_authority"] is False
