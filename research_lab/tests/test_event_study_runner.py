from __future__ import annotations

from pathlib import Path

import pytest

from research_lab.audit.audit_log import audit_events
from research_lab.evidence.evidence_registry import list_evidence_packages
from research_lab.research.research_plan import build_research_plan
from research_lab.research.research_plan_registry import store_research_plan
from research_lab.runners.event_study_runner import run_event_study
from research_lab.storage.hashing import content_hash
from research_lab.storage.manifest_io import write_json
from research_lab.storage.parquet_io import read_parquet_records, write_parquet_records


def _write_dataset(store: Path) -> str:
    dataset_id = "ds_fixture_event_study"
    dataset_root = store / "datasets" / dataset_id
    write_json(
        store / "universes" / "us_fixture" / "universe_snapshot.json",
        {
            "universe_snapshot_id": "us_fixture",
            "universe_name": "fixture",
            "universe_version": "v1",
            "created_at": "2024-01-01T00:00:00Z",
            "created_by": "pytest",
            "symbols": [{"symbol": "SPY", "asset_type": "ETF", "category": "fixture", "active": True, "min_start_date": "2024-01-01", "notes": ""}],
            "symbol_count": 1,
            "selection_policy": "fixture",
            "source_notes": "fixture",
            "content_hash": "fixture_content_hash",
            "schema_version": "dataset_snapshot.v1",
        },
        overwrite=False,
    )
    canonical = dataset_root / "data" / "canonical" / "daily_ohlcv.parquet"
    rows = [
        {"date": "2024-01-01", "symbol": "SPY", "open": 100, "high": 101, "low": 99, "close": 100, "adj_close": 100, "volume": 1000, "provider": "fixture", "provider_version": "v1", "bar_policy_version": "bp_fixture", "dataset_snapshot_id": dataset_id},
        {"date": "2024-01-02", "symbol": "SPY", "open": 100, "high": 101, "low": 97, "close": 98, "adj_close": 98, "volume": 1000, "provider": "fixture", "provider_version": "v1", "bar_policy_version": "bp_fixture", "dataset_snapshot_id": dataset_id},
        {"date": "2024-01-03", "symbol": "SPY", "open": 98, "high": 104, "low": 98, "close": 103, "adj_close": 103, "volume": 1000, "provider": "fixture", "provider_version": "v1", "bar_policy_version": "bp_fixture", "dataset_snapshot_id": dataset_id},
        {"date": "2024-01-04", "symbol": "SPY", "open": 103, "high": 103, "low": 101, "close": 102, "adj_close": 102, "volume": 1000, "provider": "fixture", "provider_version": "v1", "bar_policy_version": "bp_fixture", "dataset_snapshot_id": dataset_id},
    ]
    write_parquet_records(canonical, rows, allow_json_fallback=True)
    snapshot = {
        "dataset_snapshot_id": dataset_id,
        "dataset_type": "ohlcv",
        "provider": "fixture",
        "provider_version": "v1",
        "interval": "1d",
        "bar_policy_version": "bp_fixture",
        "universe_snapshot_id": "us_fixture",
        "start_date": "2024-01-01",
        "end_date": "2024-01-04",
        "symbol_count": 1,
        "symbols": ["SPY"],
        "storage_uri": f"research://datasets/{dataset_id}",
        "canonical_format": "parquet",
        "row_count": len(rows),
        "quality_status": "pass",
        "quality_report_uri": f"research://datasets/{dataset_id}/quality_report.json",
        "content_hash": content_hash({"dataset": dataset_id, "rows": rows}),
        "source_hash": content_hash({"source": "fixture"}),
        "created_at": "2024-01-01T00:00:00Z",
        "created_by": "pytest",
        "schema_version": "dataset_snapshot.v1",
    }
    write_json(dataset_root / "dataset_snapshot.json", snapshot, overwrite=False)
    write_json(dataset_root / "quality_report.json", {"quality_status": "pass"}, overwrite=False)
    return dataset_id


def _store_plan(store: Path, dataset_id: str) -> str:
    plan = build_research_plan(
        hypothesis_id="hyp_spy_drop_fixture",
        title="SPY drop fixture",
        dataset_snapshot_id=dataset_id,
        universe_snapshot_id="us_fixture",
        symbols=["SPY"],
        start="2024-01-01",
        end="2024-01-04",
        event_definition={"type": "daily_return_below_threshold", "params": {"return_column": "adj_close", "threshold": -0.01}},
        forward_return_windows=[1, 2],
        created_at="2024-01-01T00:00:00Z",
    )
    store_research_plan(plan, store_root=store)
    return plan["research_plan_id"]


def test_event_study_runner_writes_artifacts_registry_and_audit(tmp_path: Path) -> None:
    store = tmp_path / "store"
    plan_id = _store_plan(store, _write_dataset(store))

    result = run_event_study(research_plan_id=plan_id, store_root=store, actor="pytest", allow_json_fallback=True)
    manifest = result["evidence_manifest"]
    package_root = store / "evidence_packages" / manifest["evidence_package_id"]

    assert (package_root / "event_table.parquet").exists()
    assert (package_root / "forward_returns.parquet").exists()
    assert (package_root / "summary.json").exists()
    assert (package_root / "summary.md").exists()
    assert manifest["research_plan_hash"]
    assert manifest["dataset_snapshot_hash"]
    assert manifest["event_table_hash"]
    assert manifest["forward_returns_hash"]
    assert list_evidence_packages(store_root=store)[-1]["evidence_package_id"] == manifest["evidence_package_id"]
    assert any(event["action"] == "event_study_completed" for event in audit_events(store_root=store))
    assert read_parquet_records(package_root / "event_table.parquet")
    assert read_parquet_records(package_root / "forward_returns.parquet")


def test_same_input_produces_same_event_and_forward_hashes(tmp_path: Path) -> None:
    store_a = tmp_path / "a"
    store_b = tmp_path / "b"
    plan_a = _store_plan(store_a, _write_dataset(store_a))
    plan_b = _store_plan(store_b, _write_dataset(store_b))

    result_a = run_event_study(research_plan_id=plan_a, store_root=store_a, actor="pytest", allow_json_fallback=True)
    result_b = run_event_study(research_plan_id=plan_b, store_root=store_b, actor="pytest", allow_json_fallback=True)

    assert result_a["evidence_manifest"]["event_table_hash"] == result_b["evidence_manifest"]["event_table_hash"]
    assert result_a["evidence_manifest"]["forward_returns_hash"] == result_b["evidence_manifest"]["forward_returns_hash"]


def test_existing_evidence_package_path_cannot_be_overwritten(tmp_path: Path) -> None:
    store = tmp_path / "store"
    plan_id = _store_plan(store, _write_dataset(store))
    run_event_study(research_plan_id=plan_id, store_root=store, actor="pytest", allow_json_fallback=True)

    with pytest.raises(FileExistsError):
        run_event_study(research_plan_id=plan_id, store_root=store, actor="pytest", allow_json_fallback=True)


def test_evidence_registry_appends_only_after_successful_run(tmp_path: Path) -> None:
    store = tmp_path / "store"
    dataset_id = _write_dataset(store)
    plan = build_research_plan(
        hypothesis_id="hyp_no_events",
        title="No events fixture",
        dataset_snapshot_id=dataset_id,
        universe_snapshot_id="us_fixture",
        symbols=["SPY"],
        start="2024-01-01",
        end="2024-01-04",
        event_definition={"type": "daily_return_below_threshold", "params": {"return_column": "adj_close", "threshold": -0.90}},
        forward_return_windows=[1],
        created_at="2024-01-01T00:00:00Z",
    )
    store_research_plan(plan, store_root=store)

    with pytest.raises(RuntimeError, match="Event count is zero"):
        run_event_study(research_plan_id=plan["research_plan_id"], store_root=store, actor="pytest", allow_json_fallback=True)

    assert list_evidence_packages(store_root=store) == []
    assert any(event["action"] == "event_study_failed" for event in audit_events(store_root=store))
