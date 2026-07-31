from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.market_data_coverage_report import (
    STATUS_MISSING_SYMBOL,
    STATUS_READY,
    build_market_data_coverage_report,
    check_candidate_data_coverage,
)
from constellation_2.common.atlas_v2_research_os.market_data_schema_validation import validate_market_data_schema


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_csv(path: Path, rows: int = 40) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["timestamp,open,high,low,close,volume"]
    for index in range(rows):
        lines.append(f"2026-01-{(index % 28) + 1:02d}T09:30:00,100,101,99,{100 + index / 100:.2f},{1000 + index}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_check_candidate_data_coverage_ready_when_required_files_exist(tmp_path: Path) -> None:
    path = tmp_path / "data" / "cache" / "QQQ_30m.csv"
    _write_csv(path)
    validation = validate_market_data_schema(path).to_dict()

    row = check_candidate_data_coverage({"candidate_id": "ptc", "candidate_symbols": ["QQQ"], "candidate_timeframes": ["30m"]}, [validation])

    assert row["coverage_status"] == STATUS_READY
    assert row["available_symbols"] == ["QQQ"]
    assert row["row_count"] >= 30


def test_market_data_coverage_report_blocks_missing_symbols(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_latest(
        tmp_path,
        "candidate_symbol_attribution",
        {
            "candidate_symbol_attributions": [
                {
                    "candidate_id": "ptc",
                    "candidate_symbols": ["QQQ", "DIA"],
                    "candidate_timeframes": ["30m"],
                }
            ]
        },
    )
    _write_csv(tmp_path / "data" / "cache" / "QQQ_30m.csv")

    report = build_market_data_coverage_report(tmp_path, created_at="2026-06-05T00:00:00Z")
    row = report["candidate_coverage"][0]

    assert row["coverage_status"] == STATUS_MISSING_SYMBOL
    assert row["missing_symbols"] == ["DIA"]
    assert report["summary"]["ready_for_direct_replay"] == 0
    assert report["authority_boundary"]["broker_execution_authorized"] is False
