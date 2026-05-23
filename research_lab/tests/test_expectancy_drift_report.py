from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.contracts.schemas import validate_contract
from research_lab.stability.expectancy_drift import build_expectancy_drift_report, write_expectancy_drift_report
from research_lab.storage.manifest_io import read_jsonl, write_json
from research_lab.storage.parquet_io import write_parquet_records


SLEEVE_ID = "slv_fixture"
SLEEVE_VERSION_ID = "slvv_fixture"


def _write_run(store: Path, rows: list[dict], run_id: str = "lcr_fixture") -> str:
    root = store / "longitudinal_runs" / run_id
    write_json(
        root / "longitudinal_run.json",
        {
            "longitudinal_run_id": run_id,
            "sleeve_id": SLEEVE_ID,
            "sleeve_version_id": SLEEVE_VERSION_ID,
            "schema_version": "longitudinal_candidate_run.v1",
        },
        overwrite=False,
    )
    write_parquet_records(root / "candidate_batch_index.parquet", [{"candidate_count": len({row["candidate_id"] for row in rows})}], allow_json_fallback=True)
    write_parquet_records(root / "outcome_index.parquet", rows, allow_json_fallback=True)
    return run_id


def _rows(values: list[float], *, window: str = "5d") -> list[dict]:
    return [
        {
            "candidate_id": f"cand_{idx:03d}",
            "as_of_date": f"2024-01-{(idx % 28) + 1:02d}",
            "outcome_window": window,
            "post_cost_return": value,
            "excess_return": value / 2,
            "outcome_status": "measured",
            "risk_regime": "risk_on",
        }
        for idx, value in enumerate(values)
    ]


def test_expectancy_drift_report_schema_validates(tmp_path: Path) -> None:
    run_id = _write_run(tmp_path / "store", _rows([0.01] * 45))

    report = build_expectancy_drift_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        longitudinal_run_id=run_id,
        store_root=tmp_path / "store",
    )

    validate_contract("expectancy_drift_report", report)
    assert report["schema_version"] == "expectancy_drift_report.v1"
    assert "No broker execution" in report["research_label"]


def test_drift_report_returns_insufficient_data_under_40_measured_candidates(tmp_path: Path) -> None:
    run_id = _write_run(tmp_path / "store", _rows([0.01] * 39))

    report = build_expectancy_drift_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        longitudinal_run_id=run_id,
        store_root=tmp_path / "store",
    )

    assert report["measured_candidate_count"] == 39
    assert report["drift_status"] == "insufficient_data"
    assert report["recommended_action"] == "collect_more_candidates"


def test_drift_report_detects_degrading_series(tmp_path: Path) -> None:
    values = [0.02] * 25 + [-0.01] * 25
    run_id = _write_run(tmp_path / "store", _rows(values))

    report = build_expectancy_drift_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        longitudinal_run_id=run_id,
        store_root=tmp_path / "store",
    )

    assert report["drift_status"] == "degrading"
    assert report["recommended_action"] == "challenge_sleeve"


def test_drift_report_detects_improving_series(tmp_path: Path) -> None:
    values = [-0.01] * 25 + [0.02] * 25
    run_id = _write_run(tmp_path / "store", _rows(values))

    report = build_expectancy_drift_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        longitudinal_run_id=run_id,
        store_root=tmp_path / "store",
    )

    assert report["drift_status"] == "improving"
    assert report["recommended_action"] == "continue_research"


def test_expectancy_drift_registry_appends_and_reports_are_immutable(tmp_path: Path) -> None:
    store = tmp_path / "store"
    run_id = _write_run(store, _rows([0.01] * 45))

    report = write_expectancy_drift_report(
        sleeve_id=SLEEVE_ID,
        sleeve_version_id=SLEEVE_VERSION_ID,
        longitudinal_run_id=run_id,
        store_root=store,
    )

    rows = read_jsonl(store / "registries" / "expectancy_drift_reports.jsonl")
    assert rows[-1]["expectancy_drift_report_id"] == report["expectancy_drift_report_id"]
    with pytest.raises(FileExistsError):
        write_expectancy_drift_report(
            sleeve_id=SLEEVE_ID,
            sleeve_version_id=SLEEVE_VERSION_ID,
            longitudinal_run_id=run_id,
            store_root=store,
        )
