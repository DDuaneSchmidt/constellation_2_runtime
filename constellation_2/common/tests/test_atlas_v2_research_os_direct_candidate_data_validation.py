from __future__ import annotations

import json
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.direct_candidate_data_validation import (
    CLASS_INSUFFICIENT_DATA,
    build_direct_candidate_data_validation_report,
    write_direct_candidate_data_validation_report,
)


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def _candidate(symbol: str | None = None) -> dict:
    row = {
        "candidate_id": "ptc_direct",
        "campaign_rank": 1,
        "mechanism": "MEAN_REVERSION",
        "regime": "TRENDING",
        "final_score": 0.72,
        "expectancy": 0.003,
        "profit_factor": 1.4,
        "sample_size": 100,
        "max_drawdown": -0.12,
        "backtest_classification": "BACKTEST_SUPPORTED",
    }
    if symbol:
        row["candidate_symbol"] = symbol
    return row


def _write_csv(path: Path, rows: int = 260) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["date,open,high,low,close,volume"]
    price = 100.0
    for index in range(rows):
        price *= 1.0 + (0.002 if index % 7 else -0.01)
        open_price = price * 1.001
        high = open_price * 1.01
        low = open_price * 0.99
        close = price
        lines.append(f"2025-01-{(index % 28) + 1:02d},{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},{1000000 + index}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_direct_validation_reports_exact_gap_when_symbol_missing(tmp_path: Path) -> None:
    _write_latest(tmp_path, "focused_observation_campaign", {"campaign_candidates": [_candidate()]})
    _write_latest(tmp_path, "candidate_data_validation_plan", {"candidate_data_validation_plans": []})

    report = build_direct_candidate_data_validation_report(tmp_path, created_at="2026-06-05T00:00:00Z")
    row = report["candidate_validations"][0]

    assert row["classification"] == CLASS_INSUFFICIENT_DATA
    assert row["data_exists_locally"] is False
    assert "No structured candidate symbol" in row["data_gap"]
    assert row["authority_boundary"]["live_trading_authorized"] is False
    assert row["authority_boundary"]["position_sizing_authorized"] is False


def test_direct_validation_runs_replay_when_explicit_local_symbol_exists(tmp_path: Path) -> None:
    _write_latest(tmp_path, "focused_observation_campaign", {"campaign_candidates": [_candidate("ABC")]})
    _write_latest(tmp_path, "candidate_data_validation_plan", {"candidate_data_validation_plans": [{"candidate_id": "ptc_direct", "required_timeframe": "daily bars"}]})
    _write_csv(tmp_path / "data" / "cache" / "ABC.csv")

    report = build_direct_candidate_data_validation_report(tmp_path, created_at="2026-06-05T00:00:00Z")
    row = report["candidate_validations"][0]

    assert row["data_exists_locally"] is True
    assert row["direct_replay_ran"] is True
    assert row["direct_result"]["symbol"] == "ABC"
    assert row["classification"] in {"CONFIRMED", "WEAKENED", "INSUFFICIENT_DATA"}


def test_direct_validation_report_writes_latest(tmp_path: Path) -> None:
    _write_latest(tmp_path, "focused_observation_campaign", {"campaign_candidates": [_candidate()]})
    _write_latest(tmp_path, "candidate_data_validation_plan", {"candidate_data_validation_plans": []})
    report = build_direct_candidate_data_validation_report(tmp_path, created_at="2026-06-05T00:00:00Z")
    paths = write_direct_candidate_data_validation_report(report, root=tmp_path)

    assert paths["latest_json"].exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["report_type"] == "DIRECT_CANDIDATE_DATA_VALIDATION"
