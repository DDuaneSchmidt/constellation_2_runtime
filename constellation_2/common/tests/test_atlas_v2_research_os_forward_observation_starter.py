from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.forward_observation_starter import (
    OUTCOME_COLUMNS,
    QUEUE_COLUMNS,
    RULE_COLUMNS,
    STATUS_COLUMNS,
    TARGET_FAMILY_ID,
    build_forward_observation_starter,
    deterministic_observation_id,
    write_forward_observation_starter,
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _seed_evidence(root: Path) -> None:
    exact_fields = ["candidate_id", "family_id", "symbol", "timeframe", "data_file", "sample_size", "expectancy", "profit_factor", "max_drawdown", "classification", "fallback_used", "regime_original", "regime_bridged", "notes"]
    exact_rows = [
        {"candidate_id": "candidate_alpha", "family_id": TARGET_FAMILY_ID, "symbol": "BAC", "timeframe": "30m", "data_file": "data/manual_intraday_import/BAC_30m.csv", "sample_size": "237", "expectancy": "0.0006", "profit_factor": "1.4", "max_drawdown": "-0.04", "classification": "EXACT_CONFIRMED_STRONG", "fallback_used": "false", "regime_original": "TRENDING", "regime_bridged": "TRENDING", "notes": "seed"},
        {"candidate_id": "candidate_alpha", "family_id": TARGET_FAMILY_ID, "symbol": "AAPL", "timeframe": "30m", "data_file": "data/manual_intraday_import/AAPL_30m.csv", "sample_size": "200", "expectancy": "0.0002", "profit_factor": "1.1", "max_drawdown": "-0.05", "classification": "EXACT_BACKTEST_WEAK", "fallback_used": "false", "regime_original": "TRENDING", "regime_bridged": "TRENDING", "notes": "seed"},
        {"candidate_id": "candidate_other", "family_id": "family_other", "symbol": "BAC", "timeframe": "30m", "data_file": "", "sample_size": "1", "expectancy": "0", "profit_factor": "0", "max_drawdown": "0", "classification": "EXACT_FAILED", "fallback_used": "false", "regime_original": "CHOP", "regime_bridged": "CHOP", "notes": "ignore"},
    ]
    _write_csv(root / "exact_replay_without_fallback" / "exact_replay_results.csv", exact_fields, exact_rows)
    net_fields = ["source", "cost_scenario", "cost_model", "cost_bps", "share_cost", "candidate_id", "family_id", "sample_size", "gross_expectancy", "net_expectancy", "gross_profit_factor", "net_profit_factor", "cost_erosion", "classification"]
    net_rows = [
        {"source": "exact_replay", "cost_scenario": "10bps", "cost_model": "fixed_bps", "cost_bps": "10.0", "share_cost": "", "candidate_id": "candidate_alpha", "family_id": TARGET_FAMILY_ID, "sample_size": "237", "gross_expectancy": "0.0006", "net_expectancy": "0.0005", "gross_profit_factor": "1.4", "net_profit_factor": "1.3", "cost_erosion": "0.0001", "classification": "NET_SURVIVES_STRONG"}
    ]
    _write_csv(root / "net_of_cost_evidence" / "cost_adjusted_candidate_results.csv", net_fields, net_rows)


def test_strongest_family_loaded_and_rules_written_with_zero_signal_queue(tmp_path: Path) -> None:
    _seed_evidence(tmp_path)

    report = build_forward_observation_starter(tmp_path, created_at="2026-06-06T00:00:00Z", now="2026-06-06T00:00:00Z")

    assert report["target_family"]["family_id"] == TARGET_FAMILY_ID
    assert report["target_family"]["exact_replay"] == "EXACT_REPEATABLE_STRONG"
    assert report["summary"]["target_status"] == "READY_NO_SIGNALS_OBSERVED"
    assert report["summary"]["candidates_enabled"] == 1
    assert report["summary"]["observations_created"] == 0
    assert report["forward_observation_queue"] == []
    assert report["forward_observation_outcomes"] == []
    assert report["forward_observation_rules"][0]["family_id"] == TARGET_FAMILY_ID
    assert report["forward_observation_rules"][0]["observation_enabled"] == "true"
    assert report["summary"]["confidence_impact"] == "NONE"


def test_signal_observation_is_pending_and_outcome_not_due_before_window(tmp_path: Path) -> None:
    _seed_evidence(tmp_path)
    _write_csv(
        tmp_path / "forward_observation_starter" / "current_signal_events.csv",
        ["family_id", "candidate_id", "symbol", "timeframe", "regime", "mechanism", "signal_timestamp"],
        [{"family_id": TARGET_FAMILY_ID, "candidate_id": "candidate_alpha", "symbol": "BAC", "timeframe": "30m", "regime": "TRENDING", "mechanism": "REVERSAL", "signal_timestamp": "2026-06-06T14:00:00Z"}],
    )

    report = build_forward_observation_starter(tmp_path, created_at="2026-06-06T14:00:00Z", now="2026-06-06T14:05:00Z")
    queue = report["forward_observation_queue"]
    outcomes = report["forward_observation_outcomes"]

    assert len(queue) == 1
    assert queue[0]["status"] == "PENDING_OUTCOME"
    assert queue[0]["outcome_due_at"] == "2026-06-06T14:30:00Z"
    assert outcomes[0]["outcome_status"] == "NOT_DUE"
    assert outcomes[0]["entry_reference_price"] == ""
    assert outcomes[0]["exit_reference_price"] == ""
    assert outcomes[0]["return_observed"] == ""


def test_deterministic_observation_ids_are_stable() -> None:
    first = deterministic_observation_id(TARGET_FAMILY_ID, "candidate_alpha", "BAC", "30m", "2026-06-06T14:00:00Z", "30m")
    second = deterministic_observation_id(TARGET_FAMILY_ID, "candidate_alpha", "BAC", "30m", "2026-06-06T14:00:00Z", "30m")
    changed = deterministic_observation_id(TARGET_FAMILY_ID, "candidate_alpha", "AAPL", "30m", "2026-06-06T14:00:00Z", "30m")

    assert first == second
    assert first.startswith("fobs_")
    assert first != changed


def test_required_files_written_and_no_forbidden_authority_fields(tmp_path: Path) -> None:
    _seed_evidence(tmp_path)
    report = build_forward_observation_starter(tmp_path, created_at="2026-06-06T00:00:00Z", now="2026-06-06T00:00:00Z")
    paths = write_forward_observation_starter(report, tmp_path)

    for key in ["latest_json", "latest_summary", "queue", "outcomes", "rules", "status"]:
        assert paths[key].exists(), key
    expected = {"queue": QUEUE_COLUMNS, "outcomes": OUTCOME_COLUMNS, "rules": RULE_COLUMNS, "status": STATUS_COLUMNS}
    for key, columns in expected.items():
        with paths[key].open(newline="", encoding="utf-8") as handle:
            assert csv.DictReader(handle).fieldnames == columns
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    serialized = json.dumps(payload, sort_keys=True)
    assert "trade_recommendation" not in serialized
    assert "position_sizing" in serialized
    assert payload["authority_boundary"]["position_sizing"] is False
    assert payload["authority_boundary"]["automatic_paper_placement"] is False
    assert payload["authority_boundary"]["candidate_promotion"] is False
    assert payload["summary"]["confidence_impact"] == "NONE"
    summary = paths["latest_summary"].read_text(encoding="utf-8")
    for section in [
        "# Build 114 — Forward Observation Starter",
        "## Executive Summary",
        "## Target Family",
        "## Observation Rules",
        "## Queue Status",
        "## Outcome Measurement Policy",
        "## What This Build Does Not Do",
        "## Confidence Impact",
        "## Authority Boundary",
        "## Recommended Next Build",
    ]:
        assert section in summary
