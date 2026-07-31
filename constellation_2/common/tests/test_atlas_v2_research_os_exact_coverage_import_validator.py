from __future__ import annotations

import csv
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.exact_coverage_import_validator import (
    build_exact_coverage_import_validator,
    run_exact_coverage_import_validator,
)


def _write_csv(path: Path, rows: list[dict[str, str]], columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = columns or list(rows[0])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_spec(root: Path, *, expected_filename: str = "SPY_1h.csv", symbol: str = "SPY", timeframe: str = "1h", required_start: str = "2023-01-03", required_end: str = "2023-01-04") -> None:
    spec_dir = root / "exact_coverage_import_specification"
    required = {
        "priority": "P0",
        "family_id": "family_test",
        "candidate_id": "candidate_test",
        "symbol": symbol,
        "timeframe": timeframe,
        "required_start": required_start,
        "required_end": required_end,
        "expected_filename": expected_filename,
        "blocks_exact_validation": "true",
        "fallback_used": "true",
        "fallback_file": "",
        "reason": "test",
    }
    contract = {
        "expected_filename": expected_filename,
        "symbol": symbol,
        "timeframe": timeframe,
        "required_columns": "timestamp,open,high,low,close,volume",
        "accepted_aliases": '{"datetime":"timestamp","o":"open","h":"high","l":"low","c":"close","v":"volume"}',
        "timestamp_timezone_required": "true",
        "regular_hours_policy": "preserve_and_flag",
        "extended_hours_policy": "preserve_and_flag",
        "duplicate_policy": "reject_or_dedupe_with_report",
        "gap_policy": "allow_but_report",
        "sort_required": "true",
        "output_normalized_path": f"data/manual_intraday_import/{expected_filename}",
    }
    rules = [
        ("REQUIRED_COLUMNS_PRESENT", "Required columns present", "ERROR", "required", "REJECT"),
        ("TIMESTAMP_PARSEABLE", "Timestamp parseable", "ERROR", "parse", "REJECT"),
        ("TIMESTAMP_TIMEZONE_PRESENT_OR_DECLARED", "Timezone present", "ERROR", "tz", "REJECT"),
        ("OHLC_NUMERIC", "OHLC numeric", "ERROR", "numeric", "REJECT"),
        ("VOLUME_NUMERIC", "Volume numeric", "ERROR", "numeric", "REJECT"),
        ("HIGH_GTE_LOW", "High >= low", "ERROR", "ohlc", "REJECT"),
        ("HIGH_GTE_OPEN_CLOSE", "High bounds", "ERROR", "ohlc", "REJECT"),
        ("LOW_LTE_OPEN_CLOSE", "Low bounds", "ERROR", "ohlc", "REJECT"),
        ("NO_DUPLICATE_TIMESTAMPS", "No duplicates", "ERROR", "dupes", "REJECT"),
        ("SORTED_ASCENDING", "Sorted", "ERROR", "sort", "REJECT"),
        ("DATE_RANGE_OVERLAPS_REQUIREMENT", "Overlap", "ERROR", "overlap", "REJECT"),
        ("TIMEFRAME_INTERVAL_MATCH", "Interval", "ERROR", "interval", "REJECT"),
        ("GAPS_REPORTED", "Gaps", "WARNING", "gaps", "ACCEPT_WITH_WARNING"),
        ("SESSION_COVERAGE_REPORTED", "Session", "INFO", "session", "REPORT_ONLY"),
    ]
    _write_csv(spec_dir / "required_exact_files.csv", [required])
    _write_csv(spec_dir / "import_contract.csv", [contract])
    _write_csv(
        spec_dir / "validation_rules.csv",
        [{"rule_id": row[0], "rule_name": row[1], "severity": row[2], "description": row[3], "failure_action": row[4]} for row in rules],
    )


def _valid_rows() -> list[dict[str, str]]:
    return [
        {"timestamp": "2023-01-03T09:30:00-05:00", "open": "10", "high": "11", "low": "9", "close": "10.5", "volume": "100"},
        {"timestamp": "2023-01-03T10:30:00-05:00", "open": "10.5", "high": "11.2", "low": "10", "close": "11", "volume": "200"},
        {"timestamp": "2023-01-03T11:30:00-05:00", "open": "11", "high": "12", "low": "10.8", "close": "11.5", "volume": "300"},
    ]


def _build(tmp_path: Path, rows: list[dict[str, str]] | None, *, filename: str = "SPY_1h.csv") -> dict:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    import_dir = tmp_path / "data" / "manual_intraday_import"
    cache_dir = tmp_path / "data" / "cache" / "exact_coverage_import_validator"
    _write_spec(root, expected_filename=filename)
    if rows is not None:
        _write_csv(import_dir / filename, rows)
    return build_exact_coverage_import_validator(root=root, import_dir=import_dir, cache_dir=cache_dir, created_at="2026-06-06T00:00:00Z")


def test_required_files_are_loaded_from_build_097_output(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    import_dir = tmp_path / "data" / "manual_intraday_import"
    cache_dir = tmp_path / "data" / "cache" / "exact_coverage_import_validator"
    _write_spec(root)
    report = run_exact_coverage_import_validator(root=root, import_dir=import_dir, cache_dir=cache_dir, created_at="2026-06-06T00:00:00Z")
    assert report["summary"]["required_files"] == 1
    assert report["inputs"]["required_exact_files"].endswith("exact_coverage_import_specification/required_exact_files.csv")
    assert (root / "exact_coverage_import_validator" / "latest.json").exists()


def test_missing_files_produce_missing_file(tmp_path: Path) -> None:
    report = _build(tmp_path, None)
    assert report["import_validation_matrix"][0]["validation_status"] == "MISSING_FILE"


def test_blocked_exact_replay_rows_extend_required_validation_matrix(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    import_dir = tmp_path / "data" / "manual_intraday_import"
    cache_dir = tmp_path / "data" / "cache" / "exact_coverage_import_validator"
    _write_spec(root, expected_filename="AAPL_1h.csv", symbol="AAPL", timeframe="1h")
    _write_csv(import_dir / "AAPL_1h.csv", _valid_rows())
    _write_csv(
        root / "exact_replay_without_fallback" / "blocked_exact_replay.csv",
        [
            {
                "candidate_id": "blocked_spy",
                "family_id": "family_59cc928bca30cc44",
                "symbol": "SPY",
                "timeframe": "30m",
                "blocker": "INVALID_EXACT_FILE",
                "required_file": "/tmp/SPY_30m.csv",
                "reason": "not in validator matrix",
            }
        ],
    )
    _write_csv(
        root / "reversal_trending_exact_coverage_plan" / "coverage_matrix.csv",
        [
            {
                "candidate_id": "blocked_spy",
                "family_id": "family_59cc928bca30cc44",
                "symbol": "SPY",
                "timeframe": "30m",
                "required_start": "2023-01-03",
                "required_end": "2023-01-03",
                "priority": "P1",
            }
        ],
    )
    _write_csv(
        import_dir / "SPY_30m.csv",
        [
            {"timestamp": "2023-01-03T09:30:00Z", "open": "10", "high": "11", "low": "9", "close": "10.5", "volume": "100"},
            {"timestamp": "2023-01-03T10:00:00Z", "open": "10.5", "high": "11.2", "low": "10", "close": "11", "volume": "200"},
        ],
    )

    report = build_exact_coverage_import_validator(root=root, import_dir=import_dir, cache_dir=cache_dir, created_at="2026-06-06T00:00:00Z")

    by_file = {row["expected_filename"]: row for row in report["import_validation_matrix"]}
    assert report["summary"]["required_files"] == 2
    assert by_file["SPY_30m.csv"]["candidate_id"] == "blocked_spy"
    assert by_file["SPY_30m.csv"]["validation_status"] == "VALID_READY"


def test_exact_available_coverage_plan_rows_extend_required_validation_matrix_when_blockers_are_empty(tmp_path: Path) -> None:
    root = tmp_path / "reports" / "atlas_v2_research_os"
    import_dir = tmp_path / "data" / "manual_intraday_import"
    cache_dir = tmp_path / "data" / "cache" / "exact_coverage_import_validator"
    _write_spec(root, expected_filename="AAPL_1h.csv", symbol="AAPL", timeframe="1h")
    _write_csv(import_dir / "AAPL_1h.csv", _valid_rows())
    _write_csv(root / "exact_replay_without_fallback" / "blocked_exact_replay.csv", [], columns=["candidate_id", "family_id", "symbol", "timeframe", "blocker", "required_file", "reason"])
    _write_csv(
        root / "reversal_trending_exact_coverage_plan" / "coverage_matrix.csv",
        [
            {
                "candidate_id": "ptc_backtest_final_651cd169dd508c4e",
                "family_id": "family_59cc928bca30cc44",
                "symbol": "SPY",
                "timeframe": "30m",
                "required_start": "2023-01-03",
                "required_end": "2023-01-03",
                "available_exact_file": str(import_dir / "SPY_30m.csv"),
                "available_fallback_file": "/tmp/SPY_daily.csv",
                "fallback_used": "false",
                "coverage_status": "EXACT_COVERAGE_AVAILABLE",
                "priority": "P1",
            }
        ],
    )
    _write_csv(
        import_dir / "SPY_30m.csv",
        [
            {"timestamp": "2023-01-03T09:30:00Z", "open": "10", "high": "11", "low": "9", "close": "10.5", "volume": "100"},
            {"timestamp": "2023-01-03T10:00:00Z", "open": "10.5", "high": "11.2", "low": "10", "close": "11", "volume": "200"},
        ],
    )

    report = build_exact_coverage_import_validator(root=root, import_dir=import_dir, cache_dir=cache_dir, created_at="2026-06-06T00:00:00Z")

    by_file = {row["expected_filename"]: row for row in report["import_validation_matrix"]}
    assert report["summary"]["required_files"] == 2
    assert by_file["SPY_30m.csv"]["candidate_id"] == "ptc_backtest_final_651cd169dd508c4e"
    assert by_file["SPY_30m.csv"]["validation_status"] == "VALID_READY"


def test_alias_columns_normalize_correctly(tmp_path: Path) -> None:
    rows = [
        {"datetime": "2023-01-03 09:30:00", "o": "10", "h": "11", "l": "9", "c": "10.5", "v": "100"},
        {"datetime": "2023-01-03 10:30:00", "o": "10.5", "h": "11.2", "l": "10", "c": "11", "v": "200"},
    ]
    root = tmp_path / "reports" / "atlas_v2_research_os"
    import_dir = tmp_path / "data" / "manual_intraday_import"
    cache_dir = tmp_path / "data" / "cache" / "exact_coverage_import_validator"
    _write_spec(root)
    _write_csv(import_dir / "SPY_1h.csv", rows)
    (import_dir / "SPY_1h.csv.timezone").write_text("America/New_York\n", encoding="utf-8")
    report = build_exact_coverage_import_validator(root=root, import_dir=import_dir, cache_dir=cache_dir)
    assert report["import_validation_matrix"][0]["validation_status"] == "VALID_READY"
    assert report["normalized_file_manifest"][0]["rows_written"] == "2"
    with Path(report["normalized_file_manifest"][0]["normalized_file"]).open(encoding="utf-8") as handle:
        assert next(csv.reader(handle)) == ["timestamp", "open", "high", "low", "close", "volume"]


def test_naive_timestamps_fail_unless_timezone_declaration_exists(tmp_path: Path) -> None:
    rows = [{"timestamp": "2023-01-03 09:30:00", "open": "10", "high": "11", "low": "9", "close": "10", "volume": "100"}]
    report = _build(tmp_path, rows)
    row = report["import_validation_matrix"][0]
    assert row["validation_status"] == "REJECTED"
    assert "TIMESTAMP_TIMEZONE_PRESENT_OR_DECLARED" in row["failure_reasons"]


def test_ohlc_integrity_violations_reject_file(tmp_path: Path) -> None:
    rows = _valid_rows()
    rows[0]["high"] = "8"
    report = _build(tmp_path, rows)
    assert report["import_validation_matrix"][0]["validation_status"] == "REJECTED"
    assert "HIGH_GTE_OPEN_CLOSE" in report["import_validation_matrix"][0]["failure_reasons"]


def test_duplicate_timestamps_are_detected(tmp_path: Path) -> None:
    rows = _valid_rows()
    rows[1]["timestamp"] = rows[0]["timestamp"]
    report = _build(tmp_path, rows)
    row = report["import_validation_matrix"][0]
    assert row["duplicate_timestamp_count"] == "1"
    assert "NO_DUPLICATE_TIMESTAMPS" in row["failure_reasons"]


def test_unsorted_timestamps_are_detected(tmp_path: Path) -> None:
    rows = [_valid_rows()[1], _valid_rows()[0]]
    report = _build(tmp_path, rows)
    row = report["import_validation_matrix"][0]
    assert row["sorted_ascending"] == "false"
    assert "SORTED_ASCENDING" in row["failure_reasons"]


def test_date_range_overlap_is_required(tmp_path: Path) -> None:
    rows = _valid_rows()
    for row in rows:
        row["timestamp"] = row["timestamp"].replace("2023-01-03", "2020-01-03")
    report = _build(tmp_path, rows)
    assert "DATE_RANGE_OVERLAPS_REQUIREMENT" in report["import_validation_matrix"][0]["failure_reasons"]


def test_timeframe_interval_mismatch_rejects_according_to_rules(tmp_path: Path) -> None:
    rows = _valid_rows()
    rows[1]["timestamp"] = "2023-01-03T11:00:00-05:00"
    report = _build(tmp_path, rows)
    assert "TIMEFRAME_INTERVAL_MATCH" in report["import_validation_matrix"][0]["failure_reasons"]


def test_valid_files_write_normalized_manifest_rows(tmp_path: Path) -> None:
    report = _build(tmp_path, _valid_rows())
    assert report["summary"]["valid_ready"] == 1
    manifest = report["normalized_file_manifest"][0]
    assert manifest["normalized_file"].endswith("SPY_1h.csv")
    assert Path(manifest["normalized_file"]).exists()


def test_no_replay_or_candidate_validation_is_executed_or_reported(tmp_path: Path) -> None:
    report = _build(tmp_path, _valid_rows())
    payload = str(report).lower()
    assert "exact replay" not in " ".join(report.keys()).lower()
    assert "candidate validation" not in " ".join(report.keys()).lower()
    assert "run_holdout_replay_validation" not in payload
    assert "run_direct_candidate_data_validation" not in payload


def test_confidence_impact_is_always_none(tmp_path: Path) -> None:
    assert _build(tmp_path, _valid_rows())["confidence_impact"] == "NONE"
    assert _build(tmp_path / "missing", None)["confidence_impact"] == "NONE"


def test_forbidden_authority_fields_are_not_emitted(tmp_path: Path) -> None:
    report = _build(tmp_path, _valid_rows())
    forbidden_key_fragments = ["promotion", "production", "live_trading", "broker", "capital", "paper_placement", "position_sizing"]

    def keys(value):
        if isinstance(value, dict):
            for key, nested in value.items():
                yield key
                yield from keys(nested)
        elif isinstance(value, list):
            for nested in value:
                yield from keys(nested)

    emitted_keys = {key.lower() for key in keys(report)}
    assert not any(fragment in key for fragment in forbidden_key_fragments for key in emitted_keys)
