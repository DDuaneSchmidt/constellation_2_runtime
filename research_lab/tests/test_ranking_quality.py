from __future__ import annotations

from pathlib import Path

from research_lab.longitudinal.ranking_quality import build_ranking_quality_report
from research_lab.storage.manifest_io import write_json
from research_lab.storage.parquet_io import write_parquet_records


def _write_longitudinal_fixture(store: Path, rows: list[dict]) -> str:
    run_id = "lcr_fixture"
    root = store / "longitudinal_runs" / run_id
    write_json(
        root / "longitudinal_run.json",
        {
            "longitudinal_run_id": run_id,
            "sleeve_id": "slv_fixture",
            "sleeve_version_id": "slvv_fixture",
            "schema_version": "longitudinal_candidate_run.v1",
        },
        overwrite=False,
    )
    write_parquet_records(root / "candidate_batch_index.parquet", [{"candidate_count": len({row["candidate_id"] for row in rows})}], allow_json_fallback=True)
    write_parquet_records(root / "outcome_index.parquet", rows, allow_json_fallback=True)
    return run_id


def test_ranking_bucket_metrics_and_status_rules(tmp_path: Path) -> None:
    store = tmp_path / "store"
    rows = []
    for idx in range(30):
        rows.append({"candidate_id": f"top_{idx}", "ranking_score": 100 - idx, "ranking_bucket": "top", "post_cost_return": 0.02, "excess_return": 0.01, "outcome_window": "5d", "outcome_status": "measured"})
    for idx in range(30):
        rows.append({"candidate_id": f"low_{idx}", "ranking_score": 10 - idx, "ranking_bucket": "low", "post_cost_return": -0.01, "excess_return": -0.02, "outcome_window": "5d", "outcome_status": "measured"})
    run_id = _write_longitudinal_fixture(store, rows)

    report = build_ranking_quality_report(run_id, store_root=store, created_at="2024-01-01T00:00:00Z")

    assert report["metrics_by_window"]["5d"]["top_minus_low_spread"] > 0
    assert report["metrics_by_window"]["5d"]["spearman_rank_correlation"] > 0
    assert report["ranking_quality_status_by_window"]["5d"] == "useful"
    assert report["metrics_by_window"]["5d"]["deciles"]["decile_status"] == "computed"


def test_deciles_require_minimum_measured_count(tmp_path: Path) -> None:
    store = tmp_path / "store"
    rows = [{"candidate_id": "one", "ranking_score": 1, "ranking_bucket": "top", "post_cost_return": 0.01, "excess_return": 0.0, "outcome_window": "1d", "outcome_status": "measured"}]
    report = build_ranking_quality_report(_write_longitudinal_fixture(store, rows), store_root=store)

    assert report["ranking_quality_status_by_window"]["1d"] == "insufficient_sample"
    assert report["metrics_by_window"]["1d"]["deciles"]["decile_status"] == "insufficient_sample"

