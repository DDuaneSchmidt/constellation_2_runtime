from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.exact_data_repair_loop import run_exact_data_repair_loop
from constellation_2.common.atlas_v2_research_os.net_of_cost_evidence import classify_net_result, run_net_of_cost_evidence
from constellation_2.common.atlas_v2_research_os.cost_robustness_expansion import run_cost_robustness_expansion
from constellation_2.common.atlas_v2_research_os.final_evidence_synthesis import classify_final, run_final_evidence_synthesis
from constellation_2.common.atlas_v2_research_os import exact_replay_without_fallback as exact_replay


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_build_107_repairs_sort_and_duplicate_only_without_replay(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    raw = tmp_path / "imports" / "SPY_30m.csv"
    _write_csv(raw, [
        {"timestamp": "2023-01-03T10:00:00", "open": "10", "high": "11", "low": "9", "close": "10", "volume": "100"},
        {"timestamp": "2023-01-03T09:30:00", "open": "9", "high": "10", "low": "8", "close": "9", "volume": "100"},
        {"timestamp": "2023-01-03T09:30:00", "open": "9", "high": "10", "low": "8", "close": "9", "volume": "100"},
    ])
    matrix = [
        {"expected_filename": "SPY_30m.csv", "actual_file": str(raw), "symbol": "SPY", "timeframe": "30m", "candidate_id": "cand", "family_id": "fam", "rows_found": "3", "validation_status": "REJECTED", "failure_reasons": "NO_DUPLICATE_TIMESTAMPS;SORTED_ASCENDING"},
        {"expected_filename": "QQQ_30m.csv", "actual_file": "", "symbol": "QQQ", "timeframe": "30m", "candidate_id": "miss", "family_id": "fam", "rows_found": "0", "validation_status": "MISSING_FILE", "failure_reasons": "MISSING_FILE"},
    ]
    _write_json(root / "exact_coverage_import_validator" / "latest.json", {"import_validation_matrix": matrix})
    _write_csv(root / "exact_coverage_import_specification" / "required_exact_files.csv", [{"expected_filename": "QQQ_30m.csv", "symbol": "QQQ", "timeframe": "30m", "candidate_id": "miss", "family_id": "fam"}])

    report = run_exact_data_repair_loop(root=root, created_at="2026-06-06T00:00:00Z", cache_dir=tmp_path / "cache")

    assert report["build"] == "107"
    assert report["summary"]["repairable_files"] == 1
    assert report["summary"]["unrepaired_files"] == 1
    assert report["revalidation_matrix"][0]["validation_status"] == "VALID_READY"
    assert Path(report["repairable_files"][0]["repaired_file"]).exists()
    assert report["repair_policy"]["replay_run"] is False
    assert (root / "exact_data_repair_loop" / "repairable_files.csv").exists()


def test_build_108_can_consume_build_107_revalidation_matrix(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    exact_file = tmp_path / "cache" / "SPY_30m.csv"
    _write_csv(exact_file, [{"timestamp": "2023-01-03T09:30:00", "open": 10, "high": 11, "low": 9, "close": 10, "volume": 100}])
    _write_json(root / "exact_data_repair_loop" / "latest.json", {"revalidation_matrix": [{"symbol": "SPY", "timeframe": "30m", "validation_status": "VALID_READY", "revalidated_file": str(exact_file)}]})
    _write_csv(root / "reversal_trending_exact_coverage_plan" / "coverage_matrix.csv", [{"candidate_id": "cand", "family_id": "family_55443d63b32328bd", "mechanism": "REVERSAL", "regime": "TRENDING", "timeframe": "30m", "symbol": "SPY", "available_exact_file": str(exact_file), "fallback_used": "false"}])
    rows = [{"timestamp": f"2023-01-{(i % 28) + 1:02d}T09:30:00Z", "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 1000} for i in range(90)]
    monkeypatch.setattr(exact_replay, "normalize_market_data_csv", lambda path, symbol, timeframe: rows)
    monkeypatch.setattr(exact_replay, "run_candidate_backtest_spec", lambda spec, data_rows, created_at=None: {"metrics": {"sample_size": 60, "expectancy": 0.01, "profit_factor": 1.3, "max_drawdown": -0.02}, "warnings": [], "missing_data": []})

    report = exact_replay.run_exact_replay_without_fallback(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)

    assert report["build"] == "108"
    assert report["candidate_results"][0]["fallback_used"] is False
    assert report["source_inputs"]["build_098_or_107_validation"].endswith("exact_data_repair_loop/latest.json")


def test_build_109_cost_adjusts_exact_and_holdout_evidence(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _write_json(root / "exact_replay_without_fallback" / "latest.json", {"candidate_results": [{"candidate_id": "cand", "family_id": "fam", "sample_size": 60, "expectancy": 0.01, "profit_factor": 1.4}]})
    _write_json(root / "holdout_replay_validation" / "latest.json", {"family_validations": [{"family_id": "fam2", "holdout_sample_size": 60, "holdout_expectancy": 0.0001, "holdout_profit_factor": 1.1}]})

    report = run_net_of_cost_evidence(root=root, created_at="2026-06-06T00:00:00Z", cost_bps=8.0)

    assert report["summary"]["rows_evaluated"] == 10
    assert [row["cost_scenario"] for row in report["sensitivity_matrix"]] == ["0bps", "1bps", "2bps", "5bps", "10bps"]
    assert report["cost_assumptions"]["share_cost_scenarios"] == []
    assert report["cost_assumptions"]["share_cost_blocker"] == "SHARE_BASED_MODELING_NOT_SUPPORTED_BY_SOURCE_ROWS"
    assert {row["classification"] for row in report["candidate_results"]} >= {"NET_SURVIVES_STRONG", "COST_ERODED"}
    assert classify_net_result(10, 0.01, 0.009, 1.2) == "NET_INSUFFICIENT_SAMPLE"
    assert classify_net_result(60, -0.01, -0.011, 0.9) == "NET_FAILED"
    assert (root / "net_of_cost_evidence" / "sensitivity_matrix.csv").exists()


def test_build_118_expands_cost_robustness_from_build_109(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    build_109_rows = [
        {"source": "exact_replay", "cost_scenario": "0bps", "cost_model": "fixed_bps", "cost_bps": 0.0, "share_cost": "", "candidate_id": "robust", "family_id": "fam_robust", "sample_size": 80, "gross_expectancy": 0.004, "net_expectancy": 0.004, "gross_profit_factor": 2.0, "net_profit_factor": 2.0, "cost_erosion": 0.0, "classification": "NET_SURVIVES_STRONG"},
        {"source": "exact_replay", "cost_scenario": "0bps", "cost_model": "fixed_bps", "cost_bps": 0.0, "share_cost": "", "candidate_id": "sensitive", "family_id": "fam_sensitive", "sample_size": 80, "gross_expectancy": 0.0006, "net_expectancy": 0.0006, "gross_profit_factor": 1.4, "net_profit_factor": 1.4, "cost_erosion": 0.0, "classification": "NET_SURVIVES_STRONG"},
        {"source": "holdout_replay", "cost_scenario": "0bps", "cost_model": "fixed_bps", "cost_bps": 0.0, "share_cost": "", "candidate_id": "", "family_id": "fam_blocked", "sample_size": 0, "gross_expectancy": None, "net_expectancy": None, "gross_profit_factor": None, "net_profit_factor": None, "cost_erosion": 0.0, "classification": "NET_BLOCKED"},
    ]
    _write_json(root / "net_of_cost_evidence" / "latest.json", {"build": "109", "cost_assumptions": {"slippage_model": "conservative_fixed_bps"}, "candidate_results": build_109_rows})

    report = run_cost_robustness_expansion(root=root, created_at="2026-06-06T00:00:00Z")

    assert report["build"] == "118"
    assert [row["cost_scenario"] for row in report["cost_sensitivity_grid"]] == ["0bps", "1bps", "2bps", "5bps", "10bps", "15bps", "20bps", "25bps"]
    assert report["summary"]["strongest_family"] == "fam_robust"
    assert report["summary"]["break_even_cost_bps"] == 25.0
    assert report["summary"]["cost_robust_families"] == ["fam_robust"]
    assert report["summary"]["cost_sensitive_families"] == ["fam_sensitive"]
    by_family = {row["family_id"]: row for row in report["family_cost_robustness"]}
    assert by_family["fam_robust"]["fragility_classification"] == "COST_ROBUST"
    assert by_family["fam_sensitive"]["break_even_cost_bps"] == 5.0
    assert by_family["fam_blocked"]["fragility_classification"] == "COST_BLOCKED"
    assert (root / "cost_robustness_expansion" / "cost_sensitivity_grid.csv").exists()
    assert (root / "cost_robustness_expansion" / "family_cost_robustness.csv").exists()


def test_build_110_synthesizes_research_only_final_assessment(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _write_json(root / "exact_replay_without_fallback" / "latest.json", {"confidence_impact": "NONE", "summary": {"exact_blocked": 0, "exact_replays_run": 2}, "family_repeatability": [{"family_id": "fam", "family_classification": "EXACT_REPEATABLE_STRONG"}]})
    _write_json(root / "holdout_replay_validation" / "latest.json", {"summary": {"families_survived_holdout": 1, "families_data_blocked": 0}, "family_validations": [{"family_id": "fam", "classification": "SURVIVED_HOLDOUT"}]})
    _write_json(root / "family_stability_analysis" / "latest.json", {"family_rows": [{"family_id": "fam", "classification": "STABLE_WEAK"}]})
    _write_json(root / "net_of_cost_evidence" / "latest.json", {"confidence_impact": "NONE", "summary": {"cost_eroded": 0}, "family_results": [{"family_id": "fam", "family_classification": "NET_SURVIVES_STRONG"}]})
    _write_json(root / "evidence_lineage_graph" / "evidence_lineage_graph.json", {"schema_id": "lineage"})

    report = run_final_evidence_synthesis(root=root, created_at="2026-06-06T00:00:00Z")

    assert report["final_report"]["overall_conclusion"] == "RESEARCH_PROMISING"
    assert report["required_conclusions"]["exploitable_edge"] is True
    assert "No trading authority" in report["authority_boundary"]
    assert classify_final("EXACT_BLOCKED_INSUFFICIENT_DATA", "DATA_BLOCKED", "NET_BLOCKED") == "RESEARCH_BLOCKED"
    assert (root / "final_evidence_synthesis" / "family_final_assessment.csv").exists()
