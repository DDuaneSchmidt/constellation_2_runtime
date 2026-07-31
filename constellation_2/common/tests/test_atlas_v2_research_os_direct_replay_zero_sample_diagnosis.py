from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

from constellation_2.common.atlas_v2_research_os.direct_replay_zero_sample_diagnosis import (
    build_direct_replay_zero_sample_diagnosis_report,
    write_direct_replay_zero_sample_diagnosis_report,
)


CID1 = "ptc_backtest_final_469607b8340421b7"
CID2 = "ptc_backtest_final_3a4ac24107c77136"


def _write_latest(root: Path, name: str, payload: dict) -> None:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    (path / "latest.json").write_text(json.dumps(payload), encoding="utf-8")


def _candidate(candidate_id: str, rank: int, timeframe: str) -> dict:
    return {
        "candidate_id": candidate_id,
        "campaign_rank": rank,
        "mechanism": "BREAKOUT",
        "regime": "CHOP",
        "candidate_symbols": ["DIA"],
        "candidate_timeframes": [timeframe],
    }


def _write_breakout_csv(path: Path, rows: int = 90) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["timestamp,open,high,low,close,volume"]
    price = 100.0
    for index in range(rows):
        day = date(2023, 1, 1) + timedelta(days=index)
        price += 1.0
        open_price = price - 0.25
        high = price + 0.5
        low = price - 0.75
        lines.append(f"{day.isoformat()}T09:30:00,{open_price:.2f},{high:.2f},{low:.2f},{price:.2f},{1000 + index}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _seed_inputs(root: Path) -> None:
    candidates = [_candidate(CID1, 1, "30m"), _candidate(CID2, 2, "5m")]
    _write_latest(root, "focused_observation_campaign", {"campaign_candidates": candidates})
    _write_latest(
        root,
        "candidate_data_validation_plan",
        {"candidate_data_validation_plans": [{"candidate_id": row["candidate_id"], "mechanism": "BREAKOUT", "regime": "CHOP"} for row in candidates]},
    )
    _write_latest(root, "candidate_symbol_attribution", {"candidate_symbol_attributions": candidates})
    coverage_rows = [
        {"candidate_id": CID1, "coverage_status": "READY_FOR_DIRECT_REPLAY", "blockers": [], "required_timeframes": ["30m"]},
        {"candidate_id": CID2, "coverage_status": "READY_FOR_DIRECT_REPLAY", "blockers": [], "required_timeframes": ["5m"]},
    ]
    coverage_dir = root / "market_data_import"
    coverage_dir.mkdir(parents=True, exist_ok=True)
    (coverage_dir / "latest_coverage.json").write_text(json.dumps({"candidate_coverage": coverage_rows}), encoding="utf-8")
    _write_latest(
        root,
        "direct_candidate_data_validation",
        {"candidate_validations": [{"candidate_id": CID1, "classification": "INSUFFICIENT_DATA", "direct_result": {"sample_size": 0}}, {"candidate_id": CID2, "classification": "INSUFFICIENT_DATA", "direct_result": {"sample_size": 0}}]},
    )
    _write_breakout_csv(root / "data" / "cache" / "DIA_30m.csv")
    _write_breakout_csv(root / "data" / "cache" / "DIA_5m.csv")


def test_zero_sample_diagnosis_uses_regime_bridge_to_avoid_chop_range_bound_zero_sample(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)

    report = build_direct_replay_zero_sample_diagnosis_report(tmp_path, created_at="2026-06-06T00:00:00Z")
    rows = {row["candidate_id"]: row for row in report["candidate_diagnoses"]}

    assert rows[CID1]["data_coverage_sufficient"] is True
    assert rows[CID1]["first_failing_filter"] == "NONE"
    assert rows[CID1]["step_counts"]["trigger_events_before_filters"] > 0
    assert rows[CID1]["step_counts"]["final_sample_size"] > 0
    assert rows[CID1]["candidate_rule_parsed"]["allowed_replay_regimes"] == ["RANGE_BOUND"]
    assert rows[CID2]["first_failing_filter"] == "NONE"
    assert rows[CID2]["step_counts"]["final_sample_size"] > 0
    assert report["authority_boundary"]["live_trading_authorized"] is False
    assert report["authority_boundary"]["position_sizing_authorized"] is False


def test_zero_sample_diagnosis_writes_latest(tmp_path: Path) -> None:
    _seed_inputs(tmp_path)
    report = build_direct_replay_zero_sample_diagnosis_report(tmp_path, created_at="2026-06-06T00:00:00Z")
    paths = write_direct_replay_zero_sample_diagnosis_report(report, root=tmp_path)

    assert paths["latest_json"].exists()
    payload = json.loads(paths["latest_json"].read_text(encoding="utf-8"))
    assert payload["report_type"] == "DIRECT_REPLAY_ZERO_SAMPLE_DIAGNOSIS"
    assert paths["latest_summary"].exists()
