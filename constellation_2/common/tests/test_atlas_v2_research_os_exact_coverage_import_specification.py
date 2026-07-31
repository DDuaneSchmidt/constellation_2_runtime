from __future__ import annotations

import csv
import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.exact_coverage_import_specification import (
    ACCEPTED_ALIASES,
    CANONICAL_COLUMNS,
    IMPORT_CONTRACT_COLUMNS,
    REQUIRED_EXACT_FILES_COLUMNS,
    VALIDATION_RULE_COLUMNS,
    build_exact_coverage_import_specification,
    normalize_required_columns,
    normalize_timeframe_label,
    write_exact_coverage_import_specification,
)


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _seed_build_090(root: Path) -> None:
    source = root / "reversal_trending_exact_coverage_plan"
    source.mkdir(parents=True, exist_ok=True)
    (source / "latest.json").write_text(
        json.dumps(
            {
                "summary": {
                    "families_reviewed": 3,
                    "missing_data_rows": 45,
                    "confidence_impact": "NONE",
                }
            }
        ),
        encoding="utf-8",
    )
    shopping_rows = [
        {
            "priority": "P0",
            "family_id": "family_55443d63b32328bd",
            "candidate_id": "ptc_backtest_final_4df2e8e80685a054",
            "symbol": "BAC",
            "timeframe": "1h",
            "required_start": "2021-06-07",
            "required_end": "2026-06-05",
            "reason": "daily fallback exists but exact 1h file is missing",
            "suggested_file_name": "BAC_1h.csv",
            "blocks_exact_validation": "true",
        },
        {
            "priority": "P1",
            "family_id": "family_59cc928bca30cc44",
            "candidate_id": "variant_30m",
            "symbol": "SPY",
            "timeframe": "30M",
            "required_start": "2021-06-07",
            "required_end": "2026-06-05",
            "reason": "daily fallback exists but exact 30m file is missing",
            "suggested_file_name": "SPY_30M.csv",
            "blocks_exact_validation": "true",
        },
        {
            "priority": "P2",
            "family_id": "family_future",
            "candidate_id": "future",
            "symbol": "QQQ",
            "timeframe": "30m",
            "required_start": "2021-06-07",
            "required_end": "2026-06-05",
            "reason": "future breadth only",
            "suggested_file_name": "QQQ_30m.csv",
            "blocks_exact_validation": "false",
        },
    ]
    _write_csv(
        source / "missing_data_shopping_list.csv",
        [
            "priority",
            "family_id",
            "candidate_id",
            "symbol",
            "timeframe",
            "required_start",
            "required_end",
            "reason",
            "suggested_file_name",
            "blocks_exact_validation",
        ],
        shopping_rows,
    )
    _write_csv(
        source / "coverage_matrix.csv",
        [
            "candidate_id",
            "family_id",
            "mechanism",
            "regime",
            "timeframe",
            "source",
            "symbol",
            "universe",
            "required_start",
            "required_end",
            "available_exact_file",
            "available_exact_rows",
            "available_exact_start",
            "available_exact_end",
            "available_fallback_file",
            "fallback_used",
            "coverage_status",
            "coverage_gap_reason",
            "priority",
        ],
        [
            {
                "candidate_id": "ptc_backtest_final_4df2e8e80685a054",
                "family_id": "family_55443d63b32328bd",
                "mechanism": "REVERSAL",
                "regime": "TRENDING",
                "timeframe": "1h",
                "source": "SCREEN_REPLAY",
                "symbol": "BAC",
                "universe": "BAC",
                "required_start": "2021-06-07",
                "required_end": "2026-06-05",
                "available_exact_file": "",
                "available_exact_rows": "",
                "available_exact_start": "",
                "available_exact_end": "",
                "available_fallback_file": "data/cache/BAC_tiingo_adjusted_daily.csv",
                "fallback_used": "true",
                "coverage_status": "FALLBACK_ONLY",
                "coverage_gap_reason": "daily fallback exists but exact 1h file is missing",
                "priority": "P0",
            },
            {
                "candidate_id": "variant_30m",
                "family_id": "family_59cc928bca30cc44",
                "mechanism": "REVERSAL",
                "regime": "TRENDING",
                "timeframe": "30m",
                "source": "UNKNOWN",
                "symbol": "SPY",
                "universe": "SPY",
                "required_start": "2021-06-07",
                "required_end": "2026-06-05",
                "available_exact_file": "",
                "available_exact_rows": "",
                "available_exact_start": "",
                "available_exact_end": "",
                "available_fallback_file": "data/cache/SPY_tiingo_adjusted_daily.csv",
                "fallback_used": "true",
                "coverage_status": "FALLBACK_ONLY",
                "coverage_gap_reason": "daily fallback exists but exact 30m file is missing",
                "priority": "P1",
            },
        ],
    )
    _write_csv(
        source / "fallback_usage.csv",
        [
            "candidate_id",
            "family_id",
            "symbol",
            "required_timeframe",
            "fallback_file",
            "fallback_timeframe",
            "fallback_reason",
            "fallback_sample_size",
            "fallback_expectancy",
            "fallback_profit_factor",
            "fallback_max_drawdown",
            "evidence_strength",
        ],
        [],
    )


def test_import_spec_loads_p0_p1_and_emits_required_files(tmp_path: Path) -> None:
    _seed_build_090(tmp_path)
    report = build_exact_coverage_import_specification(tmp_path, created_at="2026-06-06T00:00:00Z")

    rows = report["required_exact_files"]
    assert len(rows) == 2
    assert report["summary"]["P0_files"] == 1
    assert report["summary"]["P1_files"] == 1
    assert {row["family_id"] for row in rows} == {"family_55443d63b32328bd", "family_59cc928bca30cc44"}
    assert rows[0]["expected_filename"] == "BAC_1h.csv"
    assert rows[1]["expected_filename"] == "SPY_30m.csv"
    assert rows[0]["fallback_used"] == "true"
    assert rows[0]["fallback_file"] == "data/cache/BAC_tiingo_adjusted_daily.csv"
    assert report["summary"]["confidence_impact"] == "NONE"
    assert report["confidence_impact"] == "NONE"


def test_import_contract_columns_aliases_and_validation_rules(tmp_path: Path) -> None:
    _seed_build_090(tmp_path)
    report = build_exact_coverage_import_specification(tmp_path, created_at="2026-06-06T00:00:00Z")

    contract = report["import_contract"]
    assert {row["expected_filename"] for row in contract} == {"BAC_1h.csv", "SPY_30m.csv"}
    assert all(row["required_columns"] == ",".join(CANONICAL_COLUMNS) for row in contract)
    assert all(row["timestamp_timezone_required"] == "true" for row in contract)
    assert all(row["output_normalized_path"].startswith("data/manual_intraday_import/") for row in contract)
    assert normalize_timeframe_label("1H") == "1h"
    assert normalize_timeframe_label("30M") == "30m"
    assert normalize_required_columns(["datetime", "o", "h", "l", "c", "v"]) == CANONICAL_COLUMNS
    assert ACCEPTED_ALIASES["datetime"] == "timestamp"

    rules = {row["rule_id"]: row for row in report["validation_rules"]}
    for rule_id in [
        "TIMESTAMP_PARSEABLE",
        "TIMESTAMP_TIMEZONE_PRESENT_OR_DECLARED",
        "OHLC_NUMERIC",
        "VOLUME_NUMERIC",
        "NO_DUPLICATE_TIMESTAMPS",
        "GAPS_REPORTED",
        "TIMEFRAME_INTERVAL_MATCH",
    ]:
        assert rule_id in rules
    assert rules["TIMESTAMP_TIMEZONE_PRESENT_OR_DECLARED"]["failure_action"] == "REJECT"
    assert "Naive timestamps are accepted only when a timezone declaration is supplied" in report["vendor_format_examples"]


def test_import_spec_writes_outputs_and_no_authority_fields(tmp_path: Path) -> None:
    _seed_build_090(tmp_path)
    report = build_exact_coverage_import_specification(tmp_path, created_at="2026-06-06T00:00:00Z")
    paths = write_exact_coverage_import_specification(report, root=tmp_path)

    for key in [
        "latest_json",
        "latest_summary",
        "required_exact_files",
        "import_contract",
        "validation_rules",
        "vendor_format_examples",
    ]:
        assert paths[key].exists()

    with paths["required_exact_files"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == REQUIRED_EXACT_FILES_COLUMNS
    with paths["import_contract"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == IMPORT_CONTRACT_COLUMNS
    with paths["validation_rules"].open(newline="", encoding="utf-8") as handle:
        assert csv.DictReader(handle).fieldnames == VALIDATION_RULE_COLUMNS

    summary = paths["latest_summary"].read_text(encoding="utf-8")
    assert "# Build 097 — Exact Coverage Import Specification" in summary
    assert "## Authority Boundary" in summary
    assert report["authority_boundary"]["research_only"] is True
    assert not any(key.endswith("_authorized") for key in report["authority_boundary"])
    assert "candidate promotion" in report["authority_boundary"]["forbidden_actions"]
