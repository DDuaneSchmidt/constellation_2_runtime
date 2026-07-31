from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os import tsla_full_surface_expansion as mod
from constellation_2.common.atlas_v2_research_os.tsla_full_surface_expansion import run_tsla_full_surface_expansion


NOW = "2026-06-06T00:00:00Z"
REQUIRED_FILES = {
    "latest.json",
    "latest_summary.md",
    "tsla_surface_inventory.csv",
    "tsla_survivor_inventory.csv",
    "tsla_failed_surface.csv",
    "tsla_blocked_surface.csv",
    "tsla_mechanism_rankings.csv",
    "tsla_timeframe_rankings.csv",
    "tsla_regime_rankings.csv",
    "tsla_entry_rule_rankings.csv",
    "tsla_exit_window_rankings.csv",
    "tsla_filter_rankings.csv",
    "tsla_cost_adjusted_results.csv",
    "tsla_null_control_results.csv",
    "tsla_surface_decision.csv",
}


def _small_matrix(monkeypatch) -> None:
    monkeypatch.setattr(mod, "TIMEFRAMES", {"1m": 1, "5m": 5, "30m": 30})
    monkeypatch.setattr(mod, "EXIT_WINDOWS", {"next_bar": 1, "15m": 15, "end_of_session": -1})
    monkeypatch.setattr(mod, "ACTIVE_REGIMES", {"UNKNOWN", "TRENDING", "RANGE_BOUND", "HIGH_VOLATILITY", "LOW_VOLATILITY", "REGULAR_HOURS"})


def _write_tsla_1m(path: Path, *, days: int = 3, minutes_per_day: int = 90) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        for day in range(1, days + 1):
            price = 200.0 + day
            for minute in range(minutes_per_day):
                hour = 9 + ((30 + minute) // 60)
                clock_minute = (30 + minute) % 60
                drift = ((minute % 17) - 8) * 0.015 + (0.04 if minute % 29 == 0 else 0.0)
                open_price = price
                close = max(1.0, open_price + drift)
                high = max(open_price, close) + 0.08
                low = min(open_price, close) - 0.08
                volume = 1000 + (minute % 23) * 100 + (5000 if minute % 31 == 0 else 0)
                writer.writerow(
                    {
                        "timestamp": f"2023-01-{day:02d}T{hour:02d}:{clock_minute:02d}:00Z",
                        "open": f"{open_price:.4f}",
                        "high": f"{high:.4f}",
                        "low": f"{low:.4f}",
                        "close": f"{close:.4f}",
                        "volume": volume,
                    }
                )
                price = close


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_tsla_full_surface_expansion_outputs_and_governance(monkeypatch, tmp_path: Path) -> None:
    _small_matrix(monkeypatch)
    _small_matrix(monkeypatch)
    repo = tmp_path
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _write_tsla_1m(repo / "data" / "manual_intraday_import" / "TSLA_1m.csv")

    report = run_tsla_full_surface_expansion(root=root, repo_root=repo, created_at=NOW)
    out = root / "tsla_full_surface_expansion"

    assert {path.name for path in out.iterdir() if path.is_file()} >= REQUIRED_FILES
    assert report["symbol"] == "TSLA"
    assert report["source_inputs"]["fallback_data_allowed"] is False
    assert report["authority_boundary"]["trade_recommendations"] is False
    assert report["authority_boundary"]["automatic_paper_placement"] is False
    assert report["authority_boundary"]["candidate_promotion"] is False
    assert report["authority_boundary"]["production_promotion"] is False

    inventory = _read_csv(out / "tsla_surface_inventory.csv")
    assert inventory
    assert {row["symbol"] for row in inventory} == {"TSLA"}
    assert {"net_expectancy_1bps", "net_expectancy_10bps", "net_expectancy_25bps", "break_even_cost_bps"} <= set(inventory[0])
    assert {row["classification"] for row in inventory} <= {
        "TSLA_SURFACE_STRONG",
        "TSLA_SURFACE_PROMISING",
        "TSLA_SURFACE_WEAK",
        "TSLA_SURFACE_COST_ERODED",
        "TSLA_SURFACE_FAILED",
        "TSLA_SURFACE_INSUFFICIENT_SAMPLE",
        "TSLA_SURFACE_BLOCKED",
    }

    blocked = _read_csv(out / "tsla_blocked_surface.csv")
    assert blocked
    assert {row["symbol"] for row in blocked} == {"TSLA"}
    assert any("MARKET_CONTEXT_DATA_BLOCKED" in row["notes"] for row in blocked)

    cost_rows = _read_csv(out / "tsla_cost_adjusted_results.csv")
    assert cost_rows
    assert "10" in json.loads(cost_rows[0]["net_expectancy_by_cost"])

    null_rows = _read_csv(out / "tsla_null_control_results.csv")
    assert null_rows
    assert {"random_timestamp_expectancy", "shuffled_label_expectancy", "beats_controls"} <= set(null_rows[0])

    decision = _read_csv(out / "tsla_surface_decision.csv")[0]
    assert decision["overall_classification"] in {
        "TSLA_RICH_SURFACE",
        "TSLA_MULTIPLE_FRAGILE_SURFACES",
        "TSLA_NARROW_SURFACE",
        "TSLA_ONE_OFF_SURVIVOR",
        "TSLA_NO_EXPANSION_CONFIRMED",
        "INSUFFICIENT_EVIDENCE",
    }


def test_tsla_full_surface_expansion_is_deterministic(monkeypatch, tmp_path: Path) -> None:
    repo = tmp_path
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _write_tsla_1m(repo / "data" / "manual_intraday_import" / "TSLA_1m.csv")

    first = run_tsla_full_surface_expansion(root=root, repo_root=repo, created_at=NOW)
    first_json = (root / "tsla_full_surface_expansion" / "latest.json").read_text(encoding="utf-8")
    second = run_tsla_full_surface_expansion(root=root, repo_root=repo, created_at=NOW)
    second_json = (root / "tsla_full_surface_expansion" / "latest.json").read_text(encoding="utf-8")

    assert first["summary"] == second["summary"]
    assert first["rankings"] == second["rankings"]
    assert first["null_control_results"] == second["null_control_results"]
    assert first_json == second_json
