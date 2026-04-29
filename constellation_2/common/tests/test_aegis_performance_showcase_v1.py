from __future__ import annotations

import sys
from pathlib import Path

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common import aegis_performance_showcase_v1 as showcase


def _artifact(logical_name: str, path: str, payload: object, sha: str = "a" * 64) -> showcase.ArtifactInputV1:
    return showcase.ArtifactInputV1(logical_name=logical_name, path=path, sha256=sha, payload=payload)


def _nav(day: str, value: int, status: str = "ACTIVE") -> dict[str, object]:
    return {
        "day_utc": day,
        "status": status,
        "nav": {"nav_total": value},
        "reason_codes": [],
    }


def _sample_report() -> dict[str, object]:
    nav_artifacts = [
        _artifact("accounting_nav_v2", "nav/2026-04-02/nav.v2.json", _nav("2026-04-02", 100000), "a" * 64),
        _artifact("accounting_nav_v2", "nav/2026-04-06/nav.v2.json", _nav("2026-04-06", 0, "BOOTSTRAP"), "b" * 64),
        _artifact("accounting_nav_v2", "nav/2026-04-08/nav.v2.json", _nav("2026-04-08", 101000), "c" * 64),
    ]
    benchmark = _artifact(
        "spy_market_data_snapshot_v1",
        "market/SPY/2026.jsonl",
        [
            {"timestamp_utc": "2026-04-02T00:00:00Z", "symbol": "SPY", "adjusted_close": "100", "source_name": "local", "source_hash": "h1"},
            {"timestamp_utc": "2026-04-08T00:00:00Z", "symbol": "SPY", "adjusted_close": "110", "source_name": "local", "source_hash": "h2"},
        ],
        "d" * 64,
    )
    capital = _artifact(
        "capital_authority_allocation_v1",
        "allocation/capital_authority_allocation.v1.json",
        {
            "per_sleeve": [
                {
                    "sleeve_id": "C2_TREND_EQ_PRIMARY",
                    "engine_ids": ["C2_TREND_EQ_PRIMARY_V1"],
                    "allowed_capital_at_risk_cents": 10000,
                    "used_capital_at_risk_cents": 5000,
                    "headroom_cents": 5000,
                }
            ]
        },
        "e" * 64,
    )
    fill = _artifact(
        "fill_ledger_v1",
        "fill/fill_ledger.v1.json",
        {
            "day_utc": "2026-04-08",
            "engine_id": "C2_TREND_EQ_PRIMARY_V1",
            "source_intent_id": "intent-1",
            "intent_sha256": "intent-sha",
            "submission_id": "sub-1",
            "order_qty": 1,
            "filled_qty": 0,
            "remaining_qty": 1,
            "avg_fill_price_weighted": "0",
            "lifecycle_status": "OPEN",
        },
        "f" * 64,
    )
    order = _artifact(
        "equity_order_plan_v1",
        "orders/equity_order_plan.v1.json",
        {"intent_sha256": "intent-sha", "symbol": "SPY", "action": "BUY", "qty_shares": 1},
        "1" * 64,
    )
    broker = _artifact(
        "broker_submission_record_v2",
        "orders/broker_submission_record.v2.json",
        {"submission_id": "sub-1", "status": "SUBMITTED", "broker": {"environment": "PAPER"}},
        "2" * 64,
    )
    return showcase.build_aegis_performance_showcase_v1(
        report_date="2026-04-08",
        data_mode="PAPER_TRADING",
        nav_artifacts=nav_artifacts,
        benchmark_artifact=benchmark,
        capital_allocation_artifact=capital,
        fill_ledger_artifacts=[fill],
        order_plan_artifacts=[order],
        broker_submission_artifacts=[broker],
        generated_at_utc="2026-04-08T00:00:00Z",
    )


def test_report_refuses_to_fabricate_missing_performance_data() -> None:
    report = _sample_report()
    assert report["metrics"]["annualized_return_pct"] is None
    assert report["metrics"]["sharpe_ratio"] is None
    assert report["metrics"]["win_rate_pct"] is None
    assert report["sleeve_curves"] == []
    assert report["monthly_returns"] == []
    gap_fields = {gap["field"] for gap in report["data_gaps"]}
    assert {"sleeve_curves", "monthly_returns", "win_rate_pct"}.issubset(gap_fields)


def test_json_includes_source_artifact_paths_and_hashes() -> None:
    report = _sample_report()
    assert report["source_artifacts"]
    assert all(row["path"] for row in report["source_artifacts"])
    assert all(len(row["sha256"]) == 64 for row in report["source_artifacts"])


def test_data_mode_is_required() -> None:
    with pytest.raises(ValueError, match="data_mode"):
        showcase.build_aegis_performance_showcase_v1(
            report_date="2026-04-08",
            data_mode="",
            nav_artifacts=[],
            generated_at_utc="2026-04-08T00:00:00Z",
        )


def test_paper_trading_label_is_present() -> None:
    report = _sample_report()
    assert report["data_mode"] == "PAPER_TRADING"
    assert report["data_label"] == "PAPER TRADING"
    assert report["advisory_only"] is True
    assert report["controls_runtime_behavior"] is False
    assert report["controls_broker_execution"] is False
    assert report["controls_phasec_materialization"] is False


def test_bootstrap_nav_is_excluded_from_performance_calculations() -> None:
    report = _sample_report()
    assert report["metrics"]["valid_nav_points"] == 2
    assert report["metrics"]["excluded_nav_points"] == 1
    assert [point["date"] for point in report["equity_curve"]] == ["2026-04-02", "2026-04-08"]
    assert report["metrics"]["total_return_pct"] == "1.000000"


def test_benchmark_uses_real_local_rows_only_for_nav_dates() -> None:
    report = _sample_report()
    assert [point["date"] for point in report["benchmark_curve"]] == ["2026-04-02", "2026-04-08"]
    assert report["benchmark_curve"][1]["cumulative_return_pct"] == "10.000000"
    assert report["benchmark_curve"][0]["source_path"] == "market/SPY/2026.jsonl"


def test_paper_orders_and_capital_allocation_are_surfaced() -> None:
    report = _sample_report()
    assert report["metrics"]["total_orders"] == 1
    assert report["metrics"]["completed_trades"] == 0
    assert report["paper_orders"][0]["broker_environment"] == "PAPER"
    assert report["capital_risk_allocation"][0]["utilization_pct"] == "50.000000"


def test_metrics_and_ids_are_deterministic() -> None:
    first = _sample_report()
    second = _sample_report()
    assert first["report_id"] == second["report_id"]
    assert first["metrics"] == second["metrics"]
    assert first["equity_curve"] == second["equity_curve"]


def test_html_generation_succeeds_from_fixture_data(tmp_path: Path) -> None:
    report = _sample_report()
    paths = showcase.write_aegis_performance_showcase_artifacts_v1(report=report, output_dir=tmp_path)
    html_path = Path(paths["html"])
    json_path = Path(paths["json"])
    assert html_path.exists()
    assert json_path.exists()
    html = html_path.read_text(encoding="utf-8")
    assert "AEGIS Performance Cockpit" in html
    assert "PAPER TRADING" in html
    assert "DATA GAP" in html
