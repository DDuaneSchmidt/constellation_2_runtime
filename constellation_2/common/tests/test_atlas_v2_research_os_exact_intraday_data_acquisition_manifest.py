from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.exact_intraday_data_acquisition_manifest import (
    MANIFEST_COLUMNS,
    build_exact_intraday_data_acquisition_manifest,
    run_exact_intraday_data_acquisition_manifest,
)


def _write_csv(path: Path, rows: list[dict[str, str]], columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = columns or list(rows[0])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _seed_inputs(root: Path) -> None:
    required_columns = [
        "priority",
        "family_id",
        "candidate_id",
        "symbol",
        "timeframe",
        "required_start",
        "required_end",
        "expected_filename",
        "blocks_exact_validation",
        "fallback_used",
        "fallback_file",
        "reason",
    ]
    _write_csv(
        root / "exact_coverage_import_specification" / "required_exact_files.csv",
        [
            {
                "priority": "P1",
                "family_id": "family_b",
                "candidate_id": "candidate_3",
                "symbol": "SPY",
                "timeframe": "1h",
                "required_start": "2021-01-01",
                "required_end": "2026-06-05",
                "expected_filename": "SPY_1h.csv",
                "blocks_exact_validation": "true",
                "fallback_used": "false",
                "fallback_file": "",
                "reason": "exact 1h file missing",
            },
            {
                "priority": "P0",
                "family_id": "family_a",
                "candidate_id": "candidate_1",
                "symbol": "QQQ",
                "timeframe": "30m",
                "required_start": "2021-06-07",
                "required_end": "2026-06-05",
                "expected_filename": "QQQ_30m.csv",
                "blocks_exact_validation": "true",
                "fallback_used": "true",
                "fallback_file": "QQQ_daily.csv",
                "reason": "daily fallback exists but exact 30m file is missing",
            },
            {
                "priority": "P0",
                "family_id": "family_a",
                "candidate_id": "candidate_2",
                "symbol": "QQQ",
                "timeframe": "30m",
                "required_start": "2021-06-07",
                "required_end": "2026-06-05",
                "expected_filename": "QQQ_30m.csv",
                "blocks_exact_validation": "true",
                "fallback_used": "true",
                "fallback_file": "QQQ_daily.csv",
                "reason": "daily fallback exists but exact 30m file is missing",
            },
        ],
        required_columns,
    )
    _write_csv(
        root / "exact_coverage_import_validator" / "import_validation_matrix.csv",
        [
            {
                "expected_filename": "QQQ_30m.csv",
                "actual_file": "",
                "priority": "P0",
                "family_id": "family_a",
                "candidate_id": "candidate_1",
                "symbol": "QQQ",
                "timeframe": "30m",
                "required_start": "2021-06-07",
                "required_end": "2026-06-05",
                "validation_status": "MISSING_FILE",
                "failure_reasons": "missing exact file",
            },
            {
                "expected_filename": "SPY_1h.csv",
                "actual_file": "/tmp/SPY_1h.csv",
                "priority": "P1",
                "family_id": "family_b",
                "candidate_id": "candidate_3",
                "symbol": "SPY",
                "timeframe": "1h",
                "required_start": "2021-01-01",
                "required_end": "2026-06-05",
                "validation_status": "INVALID_EXACT_FILE",
                "failure_reasons": "timestamp timezone missing",
            },
        ],
    )
    _write_csv(
        root / "exact_replay_without_fallback" / "blocked_exact_replay.csv",
        [
            {
                "candidate_id": "candidate_1",
                "family_id": "family_a",
                "symbol": "QQQ",
                "timeframe": "30m",
                "blocker": "FALLBACK_REQUIRED",
                "required_file": "QQQ_30m.csv",
                "reason": "Exact replay would require fallback, which is disabled.",
            },
            {
                "candidate_id": "candidate_2",
                "family_id": "family_a",
                "symbol": "QQQ",
                "timeframe": "30m",
                "blocker": "FALLBACK_REQUIRED",
                "required_file": "QQQ_30m.csv",
                "reason": "Exact replay would require fallback, which is disabled.",
            },
            {
                "candidate_id": "candidate_3",
                "family_id": "family_b",
                "symbol": "SPY",
                "timeframe": "1h",
                "blocker": "INVALID_EXACT_FILE",
                "required_file": "SPY_1h.csv",
                "reason": "Exact file exists locally but Build 098 did not mark it valid.",
            },
        ],
    )
    synthesis_dir = root / "final_evidence_synthesis"
    synthesis_dir.mkdir(parents=True, exist_ok=True)
    (synthesis_dir / "latest.json").write_text(
        json.dumps({"schema_id": "atlas_v2_research_os_final_evidence_synthesis", "report_type": "FINAL_EVIDENCE_SYNTHESIS", "confidence_impact": "NONE"}),
        encoding="utf-8",
    )


def test_required_files_load_and_manifest_deduplicates_expected_filenames(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _seed_inputs(root)
    report = build_exact_intraday_data_acquisition_manifest(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    rows = report["exact_intraday_acquisition_manifest"]
    assert report["summary"]["unique_files_required"] == 2
    assert len({row["expected_filename"] for row in rows}) == len(rows)
    assert rows[0]["expected_filename"] == "QQQ_30m.csv"
    assert rows[0]["candidate_ids_blocked"] == "candidate_1;candidate_2"


def test_p0_rows_rank_before_p1_and_required_columns_are_exact(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _seed_inputs(root)
    report = build_exact_intraday_data_acquisition_manifest(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    rows = report["exact_intraday_acquisition_manifest"]
    assert [row["priority"] for row in rows] == ["P0", "P1"]
    assert rows[0]["minimum_required_columns"] == "timestamp,open,high,low,close,volume"
    assert "do not aggregate from daily bars" in rows[0]["session_policy"]


def test_no_replay_or_validation_is_executed_and_confidence_is_none(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _seed_inputs(root)
    report = build_exact_intraday_data_acquisition_manifest(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    boundary = report["authority_boundary"]
    assert boundary["exact_replay_run"] is False
    assert boundary["validation_run"] is False
    assert boundary["external_api_calls"] is False
    assert boundary["paid_data_acquisition"] is False
    assert report["confidence_impact"] == "NONE"
    assert report["summary"]["confidence_impact"] == "NONE"


def test_outputs_are_written_with_required_columns_and_are_deterministic(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _seed_inputs(root)
    run_exact_intraday_data_acquisition_manifest(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    out_dir = root / "exact_intraday_data_acquisition_manifest"
    required_paths = [
        out_dir / "latest.json",
        out_dir / "latest_summary.md",
        out_dir / "exact_intraday_acquisition_manifest.csv",
        out_dir / "vendor_request_template.md",
        out_dir / "post_upload_validation_steps.md",
    ]
    assert all(path.exists() for path in required_paths)
    with (out_dir / "exact_intraday_acquisition_manifest.csv").open(encoding="utf-8", newline="") as handle:
        assert next(csv.reader(handle)) == MANIFEST_COLUMNS
    before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in required_paths}
    run_exact_intraday_data_acquisition_manifest(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    after = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in required_paths}
    assert before == after


def test_vendor_and_post_upload_docs_keep_authority_boundary(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    _seed_inputs(root)
    run_exact_intraday_data_acquisition_manifest(root=root, created_at="2026-06-06T00:00:00Z", repo_root=tmp_path)
    out_dir = root / "exact_intraday_data_acquisition_manifest"
    vendor = (out_dir / "vendor_request_template.md").read_text(encoding="utf-8")
    steps = (out_dir / "post_upload_validation_steps.md").read_text(encoding="utf-8")
    assert "timezone-aware" in vendor
    assert "Do not provide adjusted OHLC unless the adjustment is clearly marked" in vendor
    assert "Include volume" in vendor
    assert "Do not run exact replay" in steps
