from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.market_data_acquisition_plan import (
    build_market_data_acquisition_plan,
    run_market_data_acquisition_dry_run,
    write_required_market_data_download_manifest,
)


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def test_market_data_acquisition_plan_prioritizes_unblocking_first_candidates(tmp_path: Path) -> None:
    _write_latest(
        tmp_path,
        "market_data_import",
        {
            "candidate_coverage": [
                {"candidate_id": "ptc1", "missing_symbols": ["DIA", "QQQ"], "missing_timeframes": ["DIA:30m", "QQQ:30m", "SPY:30m"]},
                {"candidate_id": "ptc2", "missing_symbols": ["DIA", "QQQ"], "missing_timeframes": ["DIA:5m", "QQQ:5m", "SPY:5m"]},
            ]
        },
    )
    # The report is normally named latest_coverage.json, so write that too.
    (tmp_path / "market_data_import" / "latest_coverage.json").write_text((tmp_path / "market_data_import" / "latest.json").read_text(encoding="utf-8"), encoding="utf-8")

    report = build_market_data_acquisition_plan(tmp_path, created_at="2026-06-05T00:00:00Z")
    priority_1 = {(row["symbol"], row["timeframe"]) for row in report["acquisition_items"] if row["priority"] == 1}

    assert {("DIA", "30m"), ("QQQ", "30m"), ("SPY", "30m"), ("DIA", "5m"), ("QQQ", "5m"), ("SPY", "5m")} <= priority_1
    assert report["summary"]["priority_1_items"] == 6
    assert report["authority_boundary"]["external_api_called"] is False


def test_market_data_acquisition_manifest_and_dry_run_are_non_fetching(tmp_path: Path) -> None:
    _write_latest(
        tmp_path,
        "candidate_symbol_attribution",
        {
            "candidate_symbol_attributions": [
                {"candidate_id": "ptc1", "candidate_symbols": ["TLT", "USO"], "candidate_timeframes": ["15m"]}
            ]
        },
    )
    report = build_market_data_acquisition_plan(tmp_path, created_at="2026-06-05T00:00:00Z")
    manifest = write_required_market_data_download_manifest(report, tmp_path)
    dry_run = run_market_data_acquisition_dry_run(tmp_path, created_at="2026-06-05T00:00:00Z")

    assert manifest.exists()
    assert "TLT,15m" in manifest.read_text(encoding="utf-8")
    assert dry_run["dry_run"] is True
    assert dry_run["authority_boundary"]["live_trading_authorized"] is False
