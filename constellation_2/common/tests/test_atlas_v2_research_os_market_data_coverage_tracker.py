from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.market_data_coverage_tracker import (
    STATUS_FULL,
    STATUS_NONE,
    STATUS_PARTIAL,
    build_market_data_coverage_dashboard,
    write_market_data_coverage_dashboard,
)


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_csv(path: Path, rows: int = 40) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["date,open,high,low,close,volume"]
    price = 100.0
    for index in range(rows):
        price *= 1.001
        lines.append(f"2025-01-{(index % 28) + 1:02d},{price:.2f},{price + 1:.2f},{price - 1:.2f},{price:.2f},{1000 + index}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_market_data_coverage_tracker_reports_core_metrics(tmp_path: Path) -> None:
    _write_latest(
        tmp_path,
        "candidate_symbol_attribution",
        {
            "candidate_symbol_attributions": [
                {"candidate_id": "ptc_1", "mechanism": "BREAKOUT", "regime": "CHOP", "candidate_symbols": ["ABC", "XYZ"]},
                {"candidate_id": "ptc_2", "mechanism": "REVERSAL", "regime": "TRENDING", "candidate_symbols": ["ABC"]},
                {"candidate_id": "ptc_3", "mechanism": "MEAN_REVERSION", "regime": "CHOP", "candidate_symbols": ["MNO"]},
            ]
        },
    )
    _write_csv(tmp_path / "data" / "cache" / "ABC.csv")

    dashboard = build_market_data_coverage_dashboard(tmp_path, created_at="2026-06-05T00:00:00Z", base_dir=tmp_path)

    assert dashboard["coverage_percent"] == 33.33
    assert dashboard["summary"]["candidate_symbol_count"] == 3
    assert dashboard["summary"]["symbols_with_data"] == 1
    assert dashboard["full_candidate_coverage"]["count"] == 1
    assert dashboard["full_candidate_coverage"]["percent"] == 33.33
    assert dashboard["validation_block_rate"] == 66.67
    by_id = {row["candidate_id"]: row for row in dashboard["candidate_symbol_coverage"]}
    assert by_id["ptc_1"]["coverage_status"] == STATUS_PARTIAL
    assert by_id["ptc_2"]["coverage_status"] == STATUS_FULL
    assert by_id["ptc_3"]["coverage_status"] == STATUS_NONE
    assert dashboard["authority_boundary"]["live_trading_authorized"] is False
    assert dashboard["authority_boundary"]["replay_override_authorized"] is False


def test_market_data_coverage_dashboard_writes_requested_artifacts(tmp_path: Path) -> None:
    dashboard = build_market_data_coverage_dashboard(tmp_path, created_at="2026-06-05T00:00:00Z", base_dir=tmp_path)
    paths = write_market_data_coverage_dashboard(dashboard, root=tmp_path)

    assert paths["json"].name == "coverage_dashboard.json"
    assert paths["summary"].name == "coverage_dashboard.md"
    assert paths["latest_json"].exists()
    assert paths["latest_summary"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["report_type"] == "MARKET_DATA_COVERAGE_TRACKER"
