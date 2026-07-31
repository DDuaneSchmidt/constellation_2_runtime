from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os import null_model_randomized_control as build168


NOW = "2026-06-06T00:00:00Z"


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    (root / "exact_coverage_import_validator").mkdir(parents=True)
    return root


def _rows() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    signal_rows = [
        {
            "bar_index": index,
            "timestamp": f"2023-01-{(index % 28) + 1:02d}T09:30:00Z",
            "date": "2023-01-01",
            "regime": build168.TARGET_REGIME,
            "return": 0.02,
            "is_signal_bar": True,
        }
        for index in range(40)
    ]
    non_signal_rows = [
        {
            "bar_index": index,
            "timestamp": f"2023-02-{(index % 28) + 1:02d}T09:30:00Z",
            "date": "2023-02-01",
            "regime": build168.TARGET_REGIME if index % 2 == 0 else "RANGE_BOUND",
            "return": -0.002 if index % 3 else 0.001,
            "is_signal_bar": False,
        }
        for index in range(40, 140)
    ]
    return signal_rows, signal_rows + non_signal_rows


def _patch_inputs(monkeypatch) -> None:
    signal_rows, eligible_rows = _rows()
    monkeypatch.setattr(
        build168,
        "_validated_exact_file",
        lambda root, repo: {"data_file": "data/manual_intraday_import/TSLA_30m.csv", "status": "VALID_READY"},
    )
    monkeypatch.setattr(
        build168,
        "_load_feature_rows",
        lambda exact_file: ([], {"status": "VALID_READY", "rows": 140, "data_file": exact_file["data_file"]}),
    )
    monkeypatch.setattr(build168, "_sample_universe", lambda features, horizon: (signal_rows, eligible_rows))


def test_null_model_randomized_control_is_deterministic(monkeypatch, tmp_path: Path) -> None:
    root = _root(tmp_path)
    _patch_inputs(monkeypatch)

    first = build168.build_null_model_randomized_control(root=root, created_at=NOW, repo_root=tmp_path)
    second = build168.build_null_model_randomized_control(root=root, created_at=NOW, repo_root=tmp_path)

    assert first["deterministic_seeds"] == build168.CONTROL_SEEDS
    assert first["signal_metrics"] == second["signal_metrics"]
    assert first["control_comparison"] == second["control_comparison"]
    assert first["control_samples"] == second["control_samples"]
    assert {row["control_name"] for row in first["control_comparison"]} == set(build168.CONTROL_SEEDS)
    assert first["authority_boundary"]["fallback_data_allowed"] is False
    assert first["authority_boundary"]["trade_recommendation_authorized"] is False


def test_null_model_randomized_control_writes_required_outputs(monkeypatch, tmp_path: Path) -> None:
    root = _root(tmp_path)
    _patch_inputs(monkeypatch)

    report = build168.run_null_model_randomized_control(root=root, created_at=NOW, repo_root=tmp_path)
    out = root / build168.REPORT_DIRNAME

    assert report["summary"]["controls_tested"] == 4
    assert (out / "latest.json").exists()
    assert (out / "latest_summary.md").exists()
    assert (out / "control_comparison.csv").exists()
    assert (out / "control_samples.csv").exists()
    assert (out / "2026-06-06" / "null_model_randomized_control_report.json").exists()
    assert (out / "2026-06-06" / "null_model_randomized_control_summary.md").exists()
    with (out / "control_comparison.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["control_name"] for row in rows} == set(build168.CONTROL_SEEDS)
    latest = json.loads((out / "latest.json").read_text(encoding="utf-8"))
    assert latest["report_type"] == "NULL_MODEL_RANDOMIZED_CONTROL"


def test_classify_null_result_thresholds() -> None:
    strong = [{"percentile_rank": 96.0, "signal_not_better_than_null": False} for _ in build168.CONTROL_SEEDS]
    weak = [{"percentile_rank": 80.0, "signal_not_better_than_null": False} for _ in build168.CONTROL_SEEDS]
    failed = [{"percentile_rank": 96.0, "signal_not_better_than_null": False} for _ in build168.CONTROL_SEEDS]
    failed[0]["signal_not_better_than_null"] = True

    assert build168.classify_null_result(40, strong) == "BEATS_NULL_STRONGLY"
    assert build168.classify_null_result(40, weak) == "BEATS_NULL_WEAKLY"
    assert build168.classify_null_result(40, failed) == "DOES_NOT_BEAT_NULL"
    assert build168.classify_null_result(10, strong) == "INSUFFICIENT_SAMPLE"
