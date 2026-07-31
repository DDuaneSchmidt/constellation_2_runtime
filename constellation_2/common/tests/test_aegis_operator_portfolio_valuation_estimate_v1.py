from __future__ import annotations

import json
from pathlib import Path

from ops.aegis.operator_portfolio_valuation_estimate_v1 import build_operator_portfolio_valuation_estimate_v1


DAY = "2026-05-30"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _seed_ledger(root: Path, *, certified: bool = False) -> None:
    row = {
        "position_id": "paper-position:test-aapl",
        "candidate_id": "test-aapl",
        "symbol": "AAPL",
        "side": "BUY",
        "quantity": "10",
        "entry_price": "100",
        "current_certified_mark": "111" if certified else "",
        "mark_price": "111" if certified else "",
        "mark_certification_status": "CERTIFIED_CURRENT" if certified else "MISSING_MARK",
        "unrealized_pnl_status": "AVAILABLE" if certified else "NOT_CANONICAL",
        "unrealized_pnl": "110" if certified else "",
    }
    _write_json(
        root / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json",
        {"schema_id": "aegis_paper_position_ledger", "day_utc": DAY, "open_position_count": 1, "open_positions": [row]},
    )


def _seed_market(root: Path, *, symbols: dict | None = None) -> None:
    _write_json(
        root / "reports" / "market_data_intraday_operational_v1" / DAY / "market_data_intraday_operational.v1.json",
        {
            "schema_id": "market_data_intraday_operational",
            "day_utc": DAY,
            "validation_status": "READ_ONLY_PRIOR_DAY_FALLBACK",
            "symbols": symbols if symbols is not None else {
                "AAPL": {
                    "symbol": "AAPL",
                    "last_price": "110",
                    "market_session_date": "2026-05-29",
                    "source": "LOCAL_CACHE",
                    "source_hash": "abc",
                    "source_url_or_path": "/truth/market_data_snapshot_v1/AAPL/2026.jsonl",
                    "freshness_status": "STALE",
                    "market_data_mode": "STALE_PRIOR_DAY",
                    "usable_for": {"final_eod_certification": False},
                }
            },
        },
    )


def test_non_trading_day_latest_marks_create_read_only_estimate_without_canonical_pnl(tmp_path: Path) -> None:
    _seed_ledger(tmp_path)
    _seed_market(tmp_path)

    payload = build_operator_portfolio_valuation_estimate_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["target_is_trading_day"] is False
    assert payload["valuation_status"] == "ESTIMATED_NOT_CERTIFIED_FOR_TARGET_DAY"
    assert payload["estimate_reason"] == "NON_TRADING_DAY_PRIOR_SESSION_MARKS"
    assert payload["latest_available_market_session"] == "2026-05-29"
    assert payload["estimated_portfolio_value"] == "1100"
    assert payload["estimated_unrealized_pnl"] == "100"
    assert payload["canonical_pnl_allowed"] is False
    assert payload["trade_advice_allowed"] is False
    assert payload["broker_execution_allowed"] is False
    assert payload["autonomous_execution_allowed"] is False
    assert payload["positions"][0]["certification_status"] == "NOT_CERTIFIED_FOR_TARGET_DAY"


def test_estimate_unavailable_when_latest_marks_are_missing(tmp_path: Path) -> None:
    _seed_ledger(tmp_path)
    _seed_market(tmp_path, symbols={})

    payload = build_operator_portfolio_valuation_estimate_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["valuation_status"] == "ESTIMATE_UNAVAILABLE"
    assert payload["estimate_reason"] == "ESTIMATE_UNAVAILABLE"
    assert payload["marked_position_count"] == 0
    assert payload["missing_estimate_count"] == 1
    assert payload["estimated_portfolio_value"] == ""


def test_canonical_certified_marks_remain_separate_from_operator_estimate(tmp_path: Path) -> None:
    _seed_ledger(tmp_path, certified=True)
    _seed_market(tmp_path)

    payload = build_operator_portfolio_valuation_estimate_v1(truth_root=tmp_path, day_utc=DAY)

    assert payload["valuation_status"] == "ESTIMATED_NOT_CERTIFIED_FOR_TARGET_DAY"
    assert payload["positions"][0]["latest_mark_price"] == "110"
    assert payload["positions"][0]["canonical_pnl_allowed"] is False
