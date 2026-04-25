from __future__ import annotations

import json
from pathlib import Path

import ops.tools.run_monthly_performance_summary_v1 as monthly_module


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _nav_payload(*, day_utc: str, nav_total: int, realized: int, unrealized: int) -> dict:
    return {
        "schema_id": "C2_ACCOUNTING_NAV_V2",
        "schema_version": 2,
        "day_utc": day_utc,
        "nav": {
            "nav_total": nav_total,
            "realized_pnl_to_date": realized,
            "unrealized_pnl": unrealized,
        },
    }


def _fill_payload(*, filled_qty: int) -> dict:
    return {
        "schema_id": "C2_FILL_LEDGER_V1",
        "schema_version": 1,
        "filled_qty": filled_qty,
    }


def test_monthly_summary_adjusts_for_external_flows(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(
        truth_root / "accounting_v2" / "nav" / "2026-03-31" / "nav.v2.json",
        _nav_payload(day_utc="2026-03-31", nav_total=100, realized=0, unrealized=0),
    )
    _write_json(
        truth_root / "accounting_v2" / "nav" / "2026-04-01" / "nav.v2.json",
        _nav_payload(day_utc="2026-04-01", nav_total=120, realized=10, unrealized=0),
    )
    _write_json(
        truth_root / "accounting_v2" / "nav" / "2026-04-02" / "nav.v2.json",
        _nav_payload(day_utc="2026-04-02", nav_total=130, realized=15, unrealized=0),
    )
    _write_json(
        truth_root / "accounting_v2" / "nav" / "2026-04-03" / "nav.v2.json",
        _nav_payload(day_utc="2026-04-03", nav_total=126, realized=12, unrealized=2),
    )

    _write_json(
        truth_root / "fill_ledger_v1" / "2026-04-02" / "aaa.fill_ledger.v1.json",
        _fill_payload(filled_qty=1),
    )
    _write_json(
        truth_root / "fill_ledger_v1" / "2026-04-03" / "bbb.fill_ledger.v1.json",
        _fill_payload(filled_qty=0),
    )

    rc = monthly_module.main(["--month_utc", "2026-04", "--truth_root", str(truth_root)])
    assert rc == 0

    out_path = truth_root / "reports" / "monthly_performance_summary_v1" / "2026-04" / "monthly_performance_summary.v1.json"
    obj = json.loads(out_path.read_text(encoding="utf-8"))
    rpt = obj["report"]

    assert obj["status"] == "DEGRADED"
    assert rpt["starting_nav"] == 100.0
    assert rpt["ending_nav"] == 126.0
    assert rpt["net_pnl"] == 14.0
    assert rpt["deposits_additions"] == 15.0
    assert rpt["withdrawals"] == 3.0
    assert rpt["net_external_flow"] == 12.0
    assert rpt["monthly_return_pct"] == "0.26000000"
    assert rpt["return_excluding_external_flows"] == "0.14000000"
    assert rpt["realized_pnl"] == 12.0
    assert rpt["unrealized_pnl"] == 2.0
    assert rpt["max_drawdown_for_month"] == "-0.030769"
    assert rpt["trading_days_count"] == 3
    assert rpt["trades_count"] == 1
    assert rpt["winning_days_count"] == 2
    assert rpt["losing_days_count"] == 1
    assert rpt["rolling_3_month_return_pct"] is None


def test_monthly_summary_blocks_when_month_has_no_nav(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    _write_json(
        truth_root / "accounting_v2" / "nav" / "2026-03-31" / "nav.v2.json",
        _nav_payload(day_utc="2026-03-31", nav_total=100, realized=0, unrealized=0),
    )

    rc = monthly_module.main(["--month_utc", "2026-04", "--truth_root", str(truth_root)])
    assert rc == 0

    out_path = truth_root / "reports" / "monthly_performance_summary_v1" / "2026-04" / "monthly_performance_summary.v1.json"
    obj = json.loads(out_path.read_text(encoding="utf-8"))

    assert obj["status"] == "BLOCKED"
    assert obj["data_quality_status"] == "BLOCKED"
    assert "MISSING_NAV_DAYS_FOR_MONTH" in obj["blocker_codes"]
    assert obj["report"]["starting_nav"] is None
    assert obj["report"]["ending_nav"] is None
