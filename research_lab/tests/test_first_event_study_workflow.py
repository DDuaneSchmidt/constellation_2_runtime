from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.workflows.first_dataset_workflow import build_from_present_csvs
from research_lab.workflows.first_event_study_workflow import (
    create_standard_event_study,
    evidence_readiness_for_dataset,
    run_standard_event_study,
)


def _write_csv(root: Path, symbol: str, *, years: int = 4) -> None:
    root.mkdir(parents=True, exist_ok=True)
    rows = ["date,open,high,low,close,adj_close,volume"]
    price = 100.0
    row_index = 0
    for year in range(2020, 2020 + years):
        for month in range(1, 13):
            row_index += 1
            if row_index % 6 == 0:
                price *= 0.965
            else:
                price *= 1.004
            rows.append(f"{year}-{month:02d}-03,{price:.2f},{price + 1:.2f},{price - 1:.2f},{price:.2f},{price:.2f},1000")
    (root / f"{symbol}.csv").write_text("\n".join(rows) + "\n", encoding="utf-8")


def _build_dataset(tmp_path: Path, symbols: list[str], *, years: int = 4) -> str:
    csv_root = tmp_path / "csv"
    for symbol in symbols:
        _write_csv(csv_root, symbol, years=years)
    result = build_from_present_csvs(
        universe="local_etf_minimum_viable_v1",
        csv_root=csv_root,
        start="2020-01-01",
        end="2024-12-31",
        store_root=tmp_path / "store",
        allow_test_parquet_fallback=True,
    )
    return result["dataset_build"]["dataset_snapshot"]["dataset_snapshot_id"]


def test_create_standard_event_study_uses_all_loaded_dataset_symbols(tmp_path: Path) -> None:
    dataset_id = _build_dataset(tmp_path, ["SPY", "QQQ", "IWM"])

    result = create_standard_event_study(
        dataset_snapshot_id=dataset_id,
        hypothesis_id="hyp_etf_drop_reversion_v1",
        threshold=-0.02,
        forward_windows=[1, 2, 5, 10],
        store_root=tmp_path / "store",
    )

    assert result["research_plan"]["symbols"] == ["IWM", "QQQ", "SPY"]
    assert result["research_plan"]["event_definition"]["params"]["threshold"] == -0.02


def test_create_standard_event_study_fails_if_dataset_has_fewer_than_3_symbols(tmp_path: Path) -> None:
    csv_root = tmp_path / "csv"
    for symbol in ["SPY", "QQQ"]:
        _write_csv(csv_root, symbol)
    with pytest.raises(RuntimeError, match="Fewer than 3"):
        build_from_present_csvs(
            universe="local_etf_minimum_viable_v1",
            csv_root=csv_root,
            start="2020-01-01",
            end="2024-12-31",
            store_root=tmp_path / "store",
            allow_test_parquet_fallback=True,
        )


def test_evidence_readiness_blocks_poor_coverage(tmp_path: Path) -> None:
    dataset_id = _build_dataset(tmp_path, ["SPY", "QQQ", "IWM"], years=1)

    readiness = evidence_readiness_for_dataset(dataset_id, store_root=tmp_path / "store")

    assert readiness["ready"] is False
    assert "at_least_3_symbols_need_3_years_of_data" in readiness["missing_requirements"]


def test_run_standard_event_study_writes_evidence_package(tmp_path: Path) -> None:
    dataset_id = _build_dataset(tmp_path, ["SPY", "QQQ", "IWM"])
    plan = create_standard_event_study(
        dataset_snapshot_id=dataset_id,
        hypothesis_id="hyp_etf_drop_reversion_v1",
        threshold=-0.02,
        forward_windows=[1, 2, 5, 10],
        store_root=tmp_path / "store",
    )["research_plan"]

    result = run_standard_event_study(
        research_plan_id=plan["research_plan_id"],
        store_root=tmp_path / "store",
        actor="pytest",
        allow_json_fallback=True,
    )

    manifest = result["event_study"]["evidence_manifest"]
    assert manifest["evidence_package_id"].startswith("ev_hyp_etf_drop_reversion_v1_")
    assert manifest["event_count"] > 0
    assert (tmp_path / "store" / "evidence_packages" / manifest["evidence_package_id"]).exists()
