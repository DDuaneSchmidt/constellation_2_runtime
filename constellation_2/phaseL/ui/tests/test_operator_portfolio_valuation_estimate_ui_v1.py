from __future__ import annotations

import json
from pathlib import Path

from constellation_2.phaseL.ui.server import run_ops_dashboard_v1 as server
from ops.aegis.engineering_priority_queue_v1 import build_engineering_priority_queue_v1
from ops.aegis.operator_portfolio_valuation_estimate_v1 import build_operator_portfolio_valuation_estimate_v1


DAY = "2026-05-30"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _seed_sources(root: Path, *, market_symbols: dict | None = None) -> None:
    _write_json(
        root / "reports" / "aegis_paper_position_ledger_v1" / DAY / "paper_position_ledger.v1.json",
        {
            "schema_id": "aegis_paper_position_ledger",
            "day_utc": DAY,
            "open_position_count": 1,
            "open_positions": [{
                "position_id": "paper-position:test-aapl",
                "candidate_id": "test-aapl",
                "symbol": "AAPL",
                "side": "BUY",
                "quantity": "10",
                "entry_price": "100",
                "current_certified_mark": "",
                "mark_price": "",
                "mark_certification_status": "MISSING_MARK",
                "unrealized_pnl_status": "NOT_CANONICAL",
            }],
        },
    )
    _write_json(
        root / "reports" / "market_data_intraday_operational_v1" / DAY / "market_data_intraday_operational.v1.json",
        {
            "schema_id": "market_data_intraday_operational",
            "day_utc": DAY,
            "validation_status": "READ_ONLY_PRIOR_DAY_FALLBACK",
            "symbols": market_symbols if market_symbols is not None else {
                "AAPL": {
                    "symbol": "AAPL",
                    "last_price": "110",
                    "market_session_date": "2026-05-29",
                    "source": "LOCAL_CACHE",
                    "freshness_status": "STALE",
                    "market_data_mode": "STALE_PRIOR_DAY",
                    "usable_for": {"final_eod_certification": False},
                }
            },
        },
    )
    _write_json(
        root / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json",
        {
            "schema_id": "aegis_paper_pnl_report",
            "day_utc": DAY,
            "open_position_count": 1,
            "unrealized_pnl": "NOT_CANONICAL",
            "total_paper_pnl": "NOT_CANONICAL",
            "full_portfolio_pnl_status": "NOT_CANONICAL",
            "mark_coverage": {"open_position_count": 1, "marked_position_count": 0, "missing_mark_position_count": 1, "mark_coverage_by_position_pct": 0.0},
            "open_positions": [],
            "closed_positions": [],
        },
    )


def test_positions_payload_attaches_estimates_without_certifying_marks(tmp_path: Path) -> None:
    _seed_sources(tmp_path)
    _write_json(
        tmp_path / "reports" / "aegis_candidate_lifecycle_projection_v1" / DAY / "candidate_lifecycle_projection.v1.json",
        {
            "day_utc": DAY,
            "paper_session_id": "PAPER",
            "current_session_candidates": [],
            "open_paper_positions": [{"position_id": "paper-position:test-aapl", "candidate_id": "test-aapl", "symbol": "AAPL", "quantity": "10", "entry_price": "100"}],
            "closed_paper_positions": [],
            "summary": {},
        },
    )

    payload = server._positions_lightweight_payload_v1(truth_root=tmp_path, day_utc=DAY)

    estimate = payload["operator_portfolio_valuation_estimate_v1"]
    assert estimate["valuation_status"] == "ESTIMATED_NOT_CERTIFIED_FOR_TARGET_DAY"
    assert estimate["canonical_pnl_allowed"] is False
    assert payload["open_positions"][0]["estimated_mark_price"] == "110"
    assert payload["open_positions"][0]["estimated_certification_status"] == "NOT_CERTIFIED_FOR_TARGET_DAY"


def test_system_health_non_trading_prior_session_marks_are_expected_limitation_not_repair(tmp_path: Path) -> None:
    _seed_sources(tmp_path)

    queue = build_engineering_priority_queue_v1(truth_root=tmp_path, day_utc=DAY)

    summary = queue["summary"]
    assert summary["certified_same_day_marks"] == "UNAVAILABLE"
    assert summary["latest_estimated_marks"] == "AVAILABLE"
    assert summary["pricing_limitation"] == "EXPECTED_NON_TRADING_DAY_LIMITATION_NOT_REPAIR"
    assert summary["operator_actions_required"] == 0
    assert not any(row["issue"] == "Latest estimated marks unavailable" for row in queue["issues"])


def test_system_health_missing_latest_marks_are_repair_needed(tmp_path: Path) -> None:
    _seed_sources(tmp_path, market_symbols={})

    queue = build_engineering_priority_queue_v1(truth_root=tmp_path, day_utc=DAY)

    assert queue["summary"]["latest_estimated_marks"] == "UNAVAILABLE"
    assert queue["summary"]["pricing_limitation"] == "LATEST_ESTIMATE_DATA_REPAIR_NEEDED"
    assert any(row["issue"] == "Latest estimated marks unavailable" for row in queue["issues"])


def test_estimate_artifact_does_not_populate_canonical_pnl(tmp_path: Path) -> None:
    _seed_sources(tmp_path)

    estimate = build_operator_portfolio_valuation_estimate_v1(truth_root=tmp_path, day_utc=DAY)
    paper_pnl = json.loads((tmp_path / "reports" / "aegis_paper_pnl_report_v1" / DAY / "paper_pnl_report.v1.json").read_text(encoding="utf-8"))

    assert estimate["estimated_unrealized_pnl"] == "100"
    assert paper_pnl["unrealized_pnl"] == "NOT_CANONICAL"
    assert paper_pnl["total_paper_pnl"] == "NOT_CANONICAL"
