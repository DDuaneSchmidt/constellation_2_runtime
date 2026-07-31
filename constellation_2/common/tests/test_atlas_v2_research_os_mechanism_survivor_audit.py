from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.mechanism_survivor_audit import (
    run_mechanism_survivor_audit,
)


def test_mechanism_survivor_audit_writes_reports_and_keeps_research_only(tmp_path: Path) -> None:
    _seed_source_report(tmp_path)

    report = run_mechanism_survivor_audit(tmp_path, created_at="2026-06-06T00:00:00Z")

    assert report["families_audited"] == 3
    assert report["strong_survivors_before_audit"] == 2
    assert report["confidence_impact"] == "NONE"
    assert report["authority_boundary"]["research_only"] is True
    forbidden = set(report["authority_boundary"]["forbidden_actions"])
    assert "trade_recommendations" in forbidden
    assert "automatic_paper_placement" in forbidden
    assert "candidate_promotion" in forbidden
    for filename in [
        "latest.json",
        "latest_summary.md",
        "survivor_integrity_audit.csv",
        "survivor_deduplication.csv",
        "survivor_cost_recheck.csv",
        "leakage_lookahead_audit.csv",
        "top_survivor_selection.csv",
        "top_survivor_deep_dive.csv",
        "top_survivor_null_controls.csv",
        "survivor_audit_decision.csv",
    ]:
        assert (tmp_path / "mechanism_survivor_audit" / filename).exists()


def test_duplicate_detection_cost_checks_and_leakage_audit_run(tmp_path: Path) -> None:
    _seed_source_report(tmp_path)

    run_mechanism_survivor_audit(tmp_path, created_at="2026-06-06T00:00:00Z")

    dedup_rows = _read_csv(tmp_path / "mechanism_survivor_audit" / "survivor_deduplication.csv")
    assert any(row["classification"] == "DUPLICATIVE_SURVIVOR" for row in dedup_rows)
    assert any(row["duplicate_signal_timestamps"] == "NOT_AVAILABLE_SOURCE_HAS_NO_EVENT_TIMESTAMPS" for row in dedup_rows)

    cost_rows = _read_csv(tmp_path / "mechanism_survivor_audit" / "survivor_cost_recheck.csv")
    assert "20" in {row["cost_bps"] for row in cost_rows}
    assert {row["cost_classification"] for row in cost_rows} <= {
        "COST_ROBUST",
        "COST_SENSITIVE",
        "COST_ERODED",
        "INSUFFICIENT_COST_MARGIN",
    }

    leakage_rows = _read_csv(tmp_path / "mechanism_survivor_audit" / "leakage_lookahead_audit.csv")
    assert all(row["leakage_classification"] == "NO_LEAKAGE_DETECTED" for row in leakage_rows)

    integrity_rows = _read_csv(tmp_path / "mechanism_survivor_audit" / "survivor_integrity_audit.csv")
    assert all(row["fallback_used_count"] == "0" for row in integrity_rows)


def test_null_controls_are_deterministic(tmp_path: Path) -> None:
    _seed_source_report(tmp_path)

    run_mechanism_survivor_audit(tmp_path, created_at="2026-06-06T00:00:00Z")
    first = (tmp_path / "mechanism_survivor_audit" / "top_survivor_null_controls.csv").read_text()
    run_mechanism_survivor_audit(tmp_path, created_at="2026-06-06T00:00:00Z")
    second = (tmp_path / "mechanism_survivor_audit" / "top_survivor_null_controls.csv").read_text()

    assert first == second


def _seed_source_report(root: Path) -> None:
    source = root / "mechanism_expansion_program"
    source.mkdir()
    (source / "latest.json").write_text(
        json.dumps(
            {
                "builds": "187-190",
                "summary": {"fallback_used_count": 0, "blocked_tests": 0},
                "fallback_policy": {
                    "daily_fallback_allowed": False,
                    "alternate_symbol_fallback_allowed": False,
                    "alternate_timeframe_fallback_allowed": False,
                },
                "cost_scenarios_bps": [1, 2, 5, 10, 15, 25],
            }
        )
    )
    _write_csv(
        source / "family_mechanism_results.csv",
        [
            {
                "family_id": "family_anchor",
                "mechanism": "REVERSAL",
                "regime": "TRENDING",
                "timeframe": "30m",
                "candidate_count": "1",
                "symbols_tested": "3",
                "exact_replays_run": "3",
                "best_net_cost_bps": "10",
                "survivor_count_at_best_cost": "3",
                "strong_survivors_at_best_cost": "3",
                "weak_survivors_at_best_cost": "0",
                "survivor_density_at_best_cost": "1",
                "family_classification": "MECHANISM_SURVIVOR_STRONG",
            },
            {
                "family_id": "family_duplicate",
                "mechanism": "REVERSAL",
                "regime": "TRENDING",
                "timeframe": "30m",
                "candidate_count": "1",
                "symbols_tested": "3",
                "exact_replays_run": "3",
                "best_net_cost_bps": "10",
                "survivor_count_at_best_cost": "3",
                "strong_survivors_at_best_cost": "3",
                "weak_survivors_at_best_cost": "0",
                "survivor_density_at_best_cost": "1",
                "family_classification": "MECHANISM_SURVIVOR_STRONG",
            },
            {
                "family_id": "family_weak",
                "mechanism": "BREAKOUT",
                "regime": "CHOP",
                "timeframe": "1h",
                "candidate_count": "1",
                "symbols_tested": "3",
                "exact_replays_run": "3",
                "best_net_cost_bps": "10",
                "survivor_count_at_best_cost": "2",
                "strong_survivors_at_best_cost": "0",
                "weak_survivors_at_best_cost": "2",
                "survivor_density_at_best_cost": ".667",
                "family_classification": "MECHANISM_SURVIVOR_WEAK",
            },
        ],
    )
    cost_rows = []
    for family_id, candidate_id, mechanism, regime, timeframe, gross, pf in [
        ("family_anchor", "candidate_anchor", "REVERSAL", "TRENDING", "30m", 0.0032, 1.8),
        ("family_duplicate", "candidate_duplicate", "REVERSAL", "TRENDING", "30m", 0.0031, 1.75),
        ("family_weak", "candidate_weak", "BREAKOUT", "CHOP", "1h", 0.0014, 1.15),
    ]:
        for symbol in ["TSLA", "NVDA", "META"]:
            cost_rows.append(
                {
                    "cost_bps": "1",
                    "candidate_id": candidate_id,
                    "family_id": family_id,
                    "mechanism": mechanism,
                    "regime": regime,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "sample_size": "120",
                    "gross_expectancy": str(gross),
                    "net_expectancy": str(gross - 0.0001),
                    "gross_profit_factor": str(pf),
                    "net_profit_factor": str(pf - 0.01),
                    "classification": "MECHANISM_SURVIVOR_STRONG",
                }
            )
    _write_csv(source / "cost_adjusted_mechanism_results.csv", cost_rows)
    _write_csv(source / "blocked_mechanism_tests.csv", [])


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        path.write_text("status,notes\n", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))

