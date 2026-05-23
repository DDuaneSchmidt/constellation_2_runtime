from __future__ import annotations

from pathlib import Path

import yaml

from research_lab.datasets.ohlcv_builder import build_ohlcv_dataset_snapshot
from research_lab.research.research_plan import build_research_plan
from research_lab.research.research_plan_registry import store_research_plan
from research_lab.runners.batch_event_study_runner import run_batch_event_study
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import store_universe_snapshot


def _write_csv(root: Path, symbol: str, prices: list[float]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    lines = ["timestamp,open,high,low,close,volume"]
    for index, close in enumerate(prices, start=1):
        lines.append(f"2024-01-0{index}T00:00:00Z,{close},{close + 1},{close - 1},{close},1000")
    (root / f"{symbol}.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _store_universe(tmp_path: Path) -> str:
    source = tmp_path / "universe.yaml"
    source.write_text(
        yaml.safe_dump(
            {
                "selection_policy": "fixture",
                "source_notes": "fixture",
                "symbols": [
                    {"symbol": "SPY", "asset_type": "ETF", "category": "broad", "active": True, "min_start_date": "2024-01-01", "notes": ""},
                    {"symbol": "QQQ", "asset_type": "ETF", "category": "growth", "active": True, "min_start_date": "2024-01-01", "notes": ""},
                ],
            }
        ),
        encoding="utf-8",
    )
    snapshot = build_universe_snapshot(name="batch_fixture", version="v1", input_path=source)
    store_universe_snapshot(snapshot, store_root=tmp_path / "store")
    return snapshot["universe_snapshot_id"]


def test_multi_symbol_event_study_extracts_events_and_groups_summary(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "ohlcv"
    _write_csv(root, "SPY", [100, 97, 98, 96, 99, 100])
    _write_csv(root, "QQQ", [100, 101, 98, 100, 97, 99])
    monkeypatch.setenv("LOCAL_CSV_OHLCV_ROOT", str(root))
    universe_id = _store_universe(tmp_path)
    dataset = build_ohlcv_dataset_snapshot(
        dataset_type="ohlcv",
        provider_name="local_csv",
        interval="1d",
        bar_policy_version="bp_daily_ohlcv_local_csv_v1",
        universe_snapshot_id=universe_id,
        start_date="2024-01-01",
        end_date="2024-01-06",
        store_root=tmp_path / "store",
        allow_test_parquet_fallback=True,
    )["dataset_snapshot"]
    plan = build_research_plan(
        hypothesis_id="hyp_multi_symbol_drop",
        title="multi symbol drop",
        dataset_snapshot_id=dataset["dataset_snapshot_id"],
        universe_snapshot_id=universe_id,
        symbols=["SPY", "QQQ"],
        start="2024-01-01",
        end="2024-01-06",
        event_definition={"type": "daily_return_below_threshold", "params": {"return_column": "adj_close", "threshold": -0.02}},
        forward_return_windows=[1, 2],
        created_at="2024-01-01T00:00:00Z",
    )
    store_research_plan(plan, store_root=tmp_path / "store")

    result = run_batch_event_study(
        research_plan_id=plan["research_plan_id"],
        store_root=tmp_path / "store",
        actor="pytest",
        allow_json_fallback=True,
    )

    summary = result["summary"]
    assert summary["event_count"] >= 2
    assert "overall" in summary
    assert set(summary["by_symbol"]) == {"QQQ", "SPY"}
    assert set(summary["by_category"]) == {"broad", "growth"}
    assert summary["sample_size_quality"]["evidence_quality"] == result["evidence_manifest"]["evidence_quality"]
