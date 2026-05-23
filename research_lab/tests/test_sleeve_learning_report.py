from __future__ import annotations

from pathlib import Path

from research_lab.longitudinal.sleeve_learning_report import build_sleeve_learning_report
from research_lab.storage.manifest_io import append_jsonl, write_json
from research_lab.storage.parquet_io import write_parquet_records


def _write_learning_fixture(store: Path, *, total_candidates: int, ranking_status: str) -> None:
    run_id = "lcr_fixture"
    root = store / "longitudinal_runs" / run_id
    write_json(root / "longitudinal_run.json", {"longitudinal_run_id": run_id}, overwrite=False)
    write_parquet_records(root / "candidate_batch_index.parquet", [{"candidate_count": total_candidates}], allow_json_fallback=True)
    write_parquet_records(root / "outcome_index.parquet", [], allow_json_fallback=True)
    append_jsonl(store / "registries" / "longitudinal_candidate_runs.jsonl", {"longitudinal_run_id": run_id, "sleeve_id": "slv_fixture", "sleeve_version_id": "slvv_fixture", "frequency": "weekly"})
    append_jsonl(store / "registries" / "ranking_quality_reports.jsonl", {"longitudinal_run_id": run_id, "overall_ranking_quality_status": ranking_status})


def test_sleeve_learning_collects_more_candidates_when_sample_small(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _write_learning_fixture(store, total_candidates=10, ranking_status="weak")

    report = build_sleeve_learning_report("slv_fixture", "slvv_fixture", store_root=store)

    assert report["recommended_next_action"] == "collect_more_candidates"


def test_sleeve_learning_revises_ranking_policy_for_weak_ranking(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _write_learning_fixture(store, total_candidates=100, ranking_status="weak")

    report = build_sleeve_learning_report("slv_fixture", "slvv_fixture", store_root=store)

    assert report["recommended_next_action"] == "revise_ranking_policy"

