from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.trend_eq_realized_vs_unrealized_diagnostic_v1 import (  # noqa: E402
    build_trend_eq_realized_vs_unrealized_diagnostic_v1,
    write_trend_eq_realized_vs_unrealized_diagnostic_v1,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _seed_sources(tmp_path: Path, *, open_positions: list[dict], closed_positions: list[dict]) -> None:
    root = tmp_path / "truth"
    day = "2026-06-03"
    _write_json(
        root / "reports" / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json",
        {
            "open_positions": open_positions,
            "closed_positions": closed_positions,
            "positions": open_positions + closed_positions,
        },
    )
    _write_json(
        root / "reports" / "aegis_sleeve_performance_truth_v1" / day / "sleeve_performance_truth.v1.json",
        {
            "sleeves": [
                {
                    "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                    "factory_classification": "TECHNICAL_STRATEGY",
                }
            ]
        },
    )
    _write_json(
        root / "reports" / "aegis_sleeve_evidence_certification_v1" / day / "sleeve_evidence_certification.v1.json",
        {
            "sleeves": [
                {
                    "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
                    "sample_status": "BUILDING_SAMPLE",
                    "evidence_status": "UNDERPOWERED",
                }
            ]
        },
    )


def _open(position_id: str, symbol: str, unrealized_pnl: float, entry_day: str = "2026-05-01") -> dict:
    return {
        "position_id": position_id,
        "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
        "symbol": symbol,
        "entry_time": f"{entry_day}T14:30:00Z",
        "mark_timestamp_utc": "2026-06-03T20:00:00Z",
        "entry_price": 100,
        "mark_price": 105,
        "quantity": 1,
        "side": "BUY",
        "unrealized_pnl": unrealized_pnl,
        "current_status": "OPEN",
    }


def _closed(position_id: str, realized_pnl: float, entry_day: str = "2026-06-01", exit_day: str = "2026-06-02") -> dict:
    return {
        "position_id": position_id,
        "sleeve_id": "C2_TREND_EQ_PRIMARY_V1",
        "symbol": "AAL",
        "entry_time": f"{entry_day}T14:30:00Z",
        "exit_time": f"{exit_day}T20:00:00Z",
        "entry_price": 100,
        "exit_price": 99,
        "quantity": 1,
        "side": "BUY",
        "realized_pnl": realized_pnl,
        "current_status": "CLOSED",
        "closure_lineage": {"exit_trigger": "STOP_LOSS"},
    }


def test_negative_realized_sample_flags_possible_bad_entries(tmp_path: Path) -> None:
    _seed_sources(
        tmp_path,
        open_positions=[_open("o1", "AAPL", 10)],
        closed_positions=[_closed("c1", -5), _closed("c2", -6), _closed("c3", 2)],
    )

    payload = build_trend_eq_realized_vs_unrealized_diagnostic_v1(
        truth_root=tmp_path / "truth", day_utc="2026-06-03"
    )

    assert payload["diagnostics"]["bad_entries"]["status"] == "POSSIBLE_BAD_ENTRIES_OR_ADVERSE_SELECTION"
    assert "REALIZED_SAMPLE_NEGATIVE" in payload["summary"]["diagnostic_conclusion"]


def test_concentration_in_few_open_winners_is_reported(tmp_path: Path) -> None:
    _seed_sources(
        tmp_path,
        open_positions=[_open("o1", "AAPL", 90), _open("o2", "MSFT", 5), _open("o3", "NVDA", 5)],
        closed_positions=[_closed("c1", -1), _closed("c2", 1)],
    )

    payload = build_trend_eq_realized_vs_unrealized_diagnostic_v1(
        truth_root=tmp_path / "truth", day_utc="2026-06-03"
    )

    concentration = payload["diagnostics"]["concentration_in_few_winners"]
    assert concentration["status"] == "CONCENTRATED_IN_FEW_WINNERS"
    assert concentration["top_one_positive_unrealized_share"] == "0.900000"


def test_short_closed_holding_period_is_flagged_for_trend_eq_horizon(tmp_path: Path) -> None:
    _seed_sources(
        tmp_path,
        open_positions=[_open("o1", "AAPL", 2)],
        closed_positions=[_closed("c1", 1), _closed("c2", -1)],
    )

    payload = build_trend_eq_realized_vs_unrealized_diagnostic_v1(
        truth_root=tmp_path / "truth", day_utc="2026-06-03"
    )

    holding = payload["diagnostics"]["too_short_holding_period"]
    assert holding["status"] == "TOO_SHORT_FOR_20_60D_TREND_REVIEW"
    assert "MEDIAN_CLOSED_HOLDING_PERIOD_BELOW_20D_TREND_HORIZON" in holding["reasons"]


def test_positive_unrealized_does_not_override_underpowered_realized_evidence(tmp_path: Path) -> None:
    _seed_sources(
        tmp_path,
        open_positions=[_open("o1", "AAPL", 100), _open("o2", "MSFT", 25)],
        closed_positions=[_closed("c1", -5), _closed("c2", -4)],
    )

    payload = build_trend_eq_realized_vs_unrealized_diagnostic_v1(
        truth_root=tmp_path / "truth", day_utc="2026-06-03"
    )

    noise = payload["diagnostics"]["temporary_unrealized_mark_to_market_noise"]
    assert noise["status"] == "MTM_DOMINATED_NOT_REALIZED_EVIDENCE"
    assert payload["summary"]["sample_status"] == "BUILDING_SAMPLE"
    assert payload["no_trade_recommendations"] is True
    assert payload["no_investable_edge_claims"] is True


def test_write_creates_json_and_markdown_artifacts(tmp_path: Path) -> None:
    _seed_sources(
        tmp_path,
        open_positions=[_open("o1", "AAPL", 10)],
        closed_positions=[_closed("c1", -2)],
    )

    paths = write_trend_eq_realized_vs_unrealized_diagnostic_v1(
        truth_root=tmp_path / "truth", day_utc="2026-06-03"
    )

    assert paths["json"].exists()
    assert paths["markdown"].exists()
    body = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert body["schema_id"] == "aegis_trend_eq_realized_vs_unrealized_diagnostic"
