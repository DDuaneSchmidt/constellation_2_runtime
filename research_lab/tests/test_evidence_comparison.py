from __future__ import annotations

from pathlib import Path

from research_lab.evidence.evidence_comparison import compare_evidence_packages
from research_lab.storage.manifest_io import append_jsonl, write_json


def _write_package(store: Path, package_id: str, event_count: int) -> None:
    root = store / "evidence_packages" / package_id
    summary = {
        "forward_windows": [1],
        "mean_forward_return_by_window": {"1": 0.01},
        "median_forward_return_by_window": {"1": 0.02},
        "win_rate_by_window": {"1": 0.5},
    }
    manifest = {
        "evidence_package_id": package_id,
        "research_plan_id": f"rp_{package_id}",
        "hypothesis_id": f"hyp_{package_id}",
        "dataset_snapshot_id": "ds_fixture",
        "universe_snapshot_id": "us_fixture",
        "runner_name": "event_study_v1",
        "runner_version": "event_study_v1.0",
        "runner_input_hash": "input",
        "runner_output_hash": "output",
        "artifact_uris": [],
        "summary_uri": f"research://evidence/{package_id}/summary.json",
        "manifest_hash": f"hash_{package_id}",
        "event_count": event_count,
        "evidence_quality": "insufficient_sample",
        "created_at": "2024-01-01T00:00:00Z",
        "created_by": "pytest",
        "schema_version": "evidence_package.v1",
    }
    write_json(root / "summary.json", summary, overwrite=False)
    write_json(root / "evidence_manifest.json", manifest, overwrite=False)
    append_jsonl(store / "registries" / "evidence_packages.jsonl", {"evidence_package_id": package_id})


def test_evidence_comparison_summarizes_multiple_packages(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _write_package(store, "ev_a", 5)
    _write_package(store, "ev_b", 10)

    comparison = compare_evidence_packages(["ev_a", "ev_b"], store_root=store)

    assert comparison["package_count"] == 2
    assert [row["event_count"] for row in comparison["comparison_rows"]] == [5, 10]

