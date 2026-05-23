from __future__ import annotations

from pathlib import Path

import yaml
import pytest

from research_lab.acquisition.csv_readiness import build_csv_readiness_report
from research_lab.acquisition.csv_readiness import write_csv_readiness_report
from research_lab.acquisition.csv_template import create_csv_template
from research_lab.datasets.ohlcv_builder import build_ohlcv_dataset_snapshot
from research_lab.universes.universe_builder import build_universe_snapshot
from research_lab.universes.universe_registry import store_universe_snapshot


def _universe(tmp_path: Path) -> str:
    source = tmp_path / "universe.yaml"
    source.write_text(
        yaml.safe_dump(
            {
                "selection_policy": "fixture",
                "source_notes": "fixture",
                "symbols": [
                    {"symbol": "SPY", "asset_type": "ETF", "category": "equity", "active": True, "min_start_date": "2020-01-01", "notes": ""},
                    {"symbol": "QQQ", "asset_type": "ETF", "category": "equity", "active": True, "min_start_date": "2020-01-01", "notes": ""},
                ],
            }
        ),
        encoding="utf-8",
    )
    snapshot = build_universe_snapshot(name="readiness_fixture", version="v1", input_path=source)
    store_universe_snapshot(snapshot, store_root=tmp_path / "store")
    return snapshot["universe_snapshot_id"]


def test_csv_readiness_blocks_no_symbols_present(tmp_path: Path) -> None:
    csv_root = tmp_path / "csv"
    csv_root.mkdir()
    create_csv_template(symbol="SPY", output_path=csv_root / "SPY_template.csv")
    report = build_csv_readiness_report(
        universe_snapshot_id=_universe(tmp_path),
        csv_root=csv_root,
        start="2024-01-01",
        end="2024-01-31",
        allow_missing_symbols=True,
        store_root=tmp_path / "store",
    )

    assert report["ready_to_build"] is False
    assert "no_symbols_present" in report["blocking_errors"]
    assert report["template_files_ignored"] == ["SPY_template.csv"]


def test_csv_readiness_ready_with_warnings_when_adj_close_missing(tmp_path: Path) -> None:
    csv_root = tmp_path / "csv"
    csv_root.mkdir()
    (csv_root / "SPY.csv").write_text("date,open,high,low,close,volume\n2024-01-02,100,101,99,100.5,1000\n", encoding="utf-8")

    report = build_csv_readiness_report(
        universe_snapshot_id=_universe(tmp_path),
        csv_root=csv_root,
        start="2024-01-01",
        end="2024-01-31",
        allow_missing_symbols=True,
        store_root=tmp_path / "store",
    )

    assert report["ready_to_build"] is True
    assert report["overall_status"] == "ready_with_warnings"
    assert "SPY:adj_close_missing_will_use_close" in report["warnings"]


def test_build_ohlcv_dataset_fails_if_required_readiness_report_missing(tmp_path: Path, monkeypatch) -> None:
    csv_root = tmp_path / "csv"
    csv_root.mkdir()
    (csv_root / "SPY.csv").write_text("date,open,high,low,close,adj_close,volume\n2024-01-02,100,101,99,100.5,100.5,1000\n", encoding="utf-8")
    monkeypatch.setenv("LOCAL_CSV_OHLCV_ROOT", str(csv_root))

    with pytest.raises(RuntimeError, match="Readiness report required"):
        build_ohlcv_dataset_snapshot(
            dataset_type="ohlcv",
            provider_name="local_csv",
            interval="1d",
            bar_policy_version="bp_daily_ohlcv_local_csv_v1",
            universe_snapshot_id=_universe(tmp_path),
            start_date="2024-01-01",
            end_date="2024-01-31",
            store_root=tmp_path / "store",
            allow_test_parquet_fallback=True,
            allow_missing_symbols=True,
            require_readiness_report=True,
            readiness_report_path=csv_root / "missing_report.json",
        )


def test_build_ohlcv_dataset_fails_if_readiness_report_mismatches_universe(tmp_path: Path, monkeypatch) -> None:
    csv_root = tmp_path / "csv"
    csv_root.mkdir()
    (csv_root / "SPY.csv").write_text("date,open,high,low,close,adj_close,volume\n2024-01-02,100,101,99,100.5,100.5,1000\n", encoding="utf-8")
    monkeypatch.setenv("LOCAL_CSV_OHLCV_ROOT", str(csv_root))
    universe_id = _universe(tmp_path)
    report = build_csv_readiness_report(
        universe_snapshot_id=universe_id,
        csv_root=csv_root,
        start="2024-01-01",
        end="2024-01-31",
        allow_missing_symbols=True,
        store_root=tmp_path / "store",
    )
    report["universe_snapshot_id"] = "wrong_universe"
    report_path = csv_root / "csv_readiness_report.json"
    write_csv_readiness_report(report, report_path)

    with pytest.raises(RuntimeError, match="universe_snapshot_id mismatch"):
        build_ohlcv_dataset_snapshot(
            dataset_type="ohlcv",
            provider_name="local_csv",
            interval="1d",
            bar_policy_version="bp_daily_ohlcv_local_csv_v1",
            universe_snapshot_id=universe_id,
            start_date="2024-01-01",
            end_date="2024-01-31",
            store_root=tmp_path / "store",
            allow_test_parquet_fallback=True,
            allow_missing_symbols=True,
            require_readiness_report=True,
            readiness_report_path=report_path,
        )
