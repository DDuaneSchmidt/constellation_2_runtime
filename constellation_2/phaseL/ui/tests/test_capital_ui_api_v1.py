from __future__ import annotations

from pathlib import Path

from constellation_2.common.capital.constants_v1 import (
    EXPECTED_BUCKET_TOTALS_V1,
    EXPECTED_CONTROL_TOTALS_V1,
    EXPECTED_INVESTABLE_TOTAL_V1,
)
from constellation_2.common.capital.service_v1 import CapitalDomainServiceV1
from constellation_2.common.capital.seed_v1 import seed_capital_reference_dataset_v1
from constellation_2.phaseL.ui_api.capital_read_model import (
    build_capital_accounts_view,
    build_capital_allocation_view,
    build_capital_cashflow_view,
    build_capital_flows_view,
    build_capital_history_view,
    build_capital_overview_view,
    build_capital_validation_view,
)


def _capital_db(tmp_path: Path) -> Path:
    return tmp_path / "runtime" / "capital_v1" / "capital_domain.v1.sqlite3"


def test_capital_views_fail_closed_when_db_is_missing(monkeypatch, tmp_path: Path) -> None:
    db_path = _capital_db(tmp_path)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.capital_read_model.resolve_capital_db_path_v1", lambda: db_path)
    payload = build_capital_overview_view()
    assert payload["truth_state"] == "degraded"
    assert "CAPITAL_DB_MISSING" in payload["degradation_codes"]
    assert payload["status"] == "FAIL_CLOSED"


def test_capital_overview_and_allocation_views_match_expected_seed_totals(monkeypatch, tmp_path: Path) -> None:
    db_path = _capital_db(tmp_path)
    service = CapitalDomainServiceV1(db_path=db_path, actor="test_capital_ui_api_v1")
    seed_capital_reference_dataset_v1(service=service)
    monkeypatch.setattr("constellation_2.phaseL.ui_api.capital_read_model.resolve_capital_db_path_v1", lambda: db_path)

    overview = build_capital_overview_view()
    allocation = build_capital_allocation_view()
    accounts = build_capital_accounts_view()
    history = build_capital_history_view()
    flows = build_capital_flows_view()
    cashflow = build_capital_cashflow_view(scenario="florida")
    validation = build_capital_validation_view()

    assert overview["overview"]["investable_total"] == EXPECTED_INVESTABLE_TOTAL_V1
    assert overview["overview"]["included_account_count"] == 7
    assert overview["overview"]["excluded_account_count"] == 5
    assert overview["report_basis_metadata"]["report_basis"] == "latest_per_account"
    assert overview["report_basis_metadata"]["stale_account_count"] == 0
    assert overview["overview"]["investable_explainability"]["contributors"]
    assert overview["_metadata_validation"]["ok"] is True

    control_rows = {row["control_type"]: row["balance_total"] for row in allocation["allocation_by_control"]["rows"]}
    assert control_rows == EXPECTED_CONTROL_TOTALS_V1
    bucket_rows = {row["bucket_type"]: row["balance_total"] for row in allocation["allocation_by_bucket"]["rows"]}
    assert bucket_rows == EXPECTED_BUCKET_TOTALS_V1
    assert allocation["basis"]["numerator"] == "included_balance_by_dimension"
    assert allocation["basis"]["denominator"] == "total_included_balance"
    assert allocation["report_basis_metadata"]["report_basis"] == "latest_per_account"
    assert allocation["allocation_by_control"]["rows"][0]["contributors"]

    account_ids = {row["account_id"] for row in accounts["rows"]}
    assert "aegis_paper" in account_ids
    included_map = {row["account_id"]: row["include_in_allocation"] for row in accounts["rows"]}
    assert included_map["aegis_paper"] is False
    confidence_band_map = {row["account_id"]: row["confidence_band"] for row in accounts["rows"]}
    assert confidence_band_map["aegis_paper"] == "high"
    assert accounts["recent_audit_entries"]

    assert history["investable_time_series"]["points"][-1]["investable_total"] == EXPECTED_INVESTABLE_TOTAL_V1
    assert flows["summary_by_period"]["rows"] == []
    assert flows["summary_by_period"]["flow_truth_owner"]["truth_owner"] == "capital_cash_flows_v1"
    assert cashflow["scenario"] == "florida"
    assert cashflow["monthly_projection"][0]["net"] == 6000.0
    assert cashflow["basis"]["deterministic_only"] is True
    assert validation["validation"]["status"] == "HEALTHY"


def test_capital_routes_and_clients_are_shell_wired() -> None:
    root = Path(__file__).resolve().parents[4]
    domain_client = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "domain_client" / "index.js"
    ).read_text(encoding="utf-8")
    pages = (
        root / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
    ).read_text(encoding="utf-8")
    server = (
        root / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py"
    ).read_text(encoding="utf-8")

    for endpoint in [
        'query("/api/capital/overview")',
        'query("/api/capital/accounts")',
        'query("/api/capital/allocation")',
        'query("/api/capital/history")',
        'query("/api/capital/flows")',
        '"/api/capital/cashflow"',
        'query("/api/capital/validation")',
    ]:
        assert endpoint in domain_client

    for route_path in [
        'path: "/capital"',
        'path: "/capital/accounts"',
        'path: "/capital/allocation"',
        'path: "/capital/history"',
        'path: "/capital/flows"',
        'path: "/capital/cashflow"',
        'path: "/capital/validation"',
    ]:
        assert route_path in pages

    for shell_route in [
        '"/capital"',
        '"/capital/accounts"',
        '"/capital/allocation"',
        '"/capital/history"',
        '"/capital/flows"',
        '"/capital/cashflow"',
        '"/capital/validation"',
    ]:
        assert shell_route in server

    for render_fn in [
        "renderCapitalOverviewPage",
        "renderCapitalAccountsPage",
        "renderCapitalAllocationPage",
        "renderCapitalHistoryPage",
        "renderCapitalFlowsPage",
        "renderCapitalCashflowPage",
        "renderCapitalValidationPage",
    ]:
        assert render_fn in pages

    for basis_keyword in [
        "report_basis_metadata",
        "freshness_status",
        "contributors",
        "Confidence Band",
    ]:
        assert basis_keyword in pages

    for api_route in [
        '"/api/capital/overview"',
        '"/api/capital/accounts"',
        '"/api/capital/allocation"',
        '"/api/capital/history"',
        '"/api/capital/flows"',
        '"/api/capital/cashflow"',
        '"/api/capital/validation"',
    ]:
        assert api_route in server
