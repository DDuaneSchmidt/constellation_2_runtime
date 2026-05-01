from __future__ import annotations

from pathlib import Path

from constellation_2.common.capital.service_v1 import CapitalDomainServiceV1
from constellation_2.common.capital.seed_v1 import seed_capital_reference_dataset_v1


def _service(tmp_path: Path) -> CapitalDomainServiceV1:
    return CapitalDomainServiceV1(
        db_path=tmp_path / "capital_v1" / "capital_domain.v1.sqlite3",
        actor="test_capital_cashflow_timeline_v1",
    )


def test_cashflow_projection_florida_and_chile_monthly_net(tmp_path: Path) -> None:
    service = _service(tmp_path)
    seed_capital_reference_dataset_v1(service=service)

    florida = service.cashflow_projection(
        scenario="florida",
        start_month="2026-05",
        horizon_months=3,
    )
    chile = service.cashflow_projection(
        scenario="chile",
        start_month="2026-05",
        horizon_months=3,
    )

    assert florida["status"] == "HEALTHY"
    assert florida["monthly_projection"][0]["income"] == 18000.0
    assert florida["monthly_projection"][0]["expenses"] == 12000.0
    assert florida["monthly_projection"][0]["net"] == 6000.0
    assert florida["operator_console"]["safety"]["answer"] == "SAFE_WITHIN_HORIZON"
    assert florida["operator_console"]["weakest_month"]["month"] == "2026-05"
    assert florida["operator_console"]["weakest_month"]["net"] == 6000.0
    assert florida["operator_console"]["failure"]["status"] == "NO_FAILURE_WITHIN_HORIZON"
    assert florida["operator_console"]["trust"]["calculation_authority"] == "CapitalDomainServiceV1.cashflow_projection"

    assert chile["status"] == "HEALTHY"
    assert chile["monthly_projection"][0]["income"] == 18000.0
    assert chile["monthly_projection"][0]["expenses"] == 8000.0
    assert chile["monthly_projection"][0]["net"] == 10000.0


def test_cashflow_projection_excludes_inheritance_by_default(tmp_path: Path) -> None:
    service = _service(tmp_path)
    seed_capital_reference_dataset_v1(service=service)

    deterministic = service.cashflow_projection(
        scenario="florida",
        start_month="2036-01",
        horizon_months=1,
        include_nondeterministic=False,
    )
    with_inheritance = service.cashflow_projection(
        scenario="florida",
        start_month="2036-01",
        horizon_months=1,
        include_nondeterministic=True,
    )

    deterministic_net = deterministic["monthly_projection"][0]["net"]
    with_inheritance_net = with_inheritance["monthly_projection"][0]["net"]
    assert deterministic["basis"]["inheritance_excluded"] is True
    assert with_inheritance["basis"]["inheritance_excluded"] is False
    assert with_inheritance_net - deterministic_net == 500000.0


def test_cashflow_projection_negative_streak_sets_at_risk(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.append_cashflow_event(
        event_name="income_only",
        event_type="invest_income",
        scenario="base",
        start_date="2026-05-01",
        end_date=None,
        frequency="monthly",
        amount=1000.0,
        confidence=1.0,
        is_deterministic=True,
        reason="cashflow_test_setup",
    )
    service.append_cashflow_event(
        event_name="expense_only",
        event_type="expense",
        scenario="florida",
        start_date="2026-05-01",
        end_date=None,
        frequency="monthly",
        amount=3000.0,
        confidence=1.0,
        is_deterministic=True,
        reason="cashflow_test_setup",
    )

    projection = service.cashflow_projection(
        scenario="florida",
        start_month="2026-05",
        horizon_months=6,
    )
    finding_codes = {row["code"] for row in projection["validation"]["findings"]}
    assert projection["status"] == "AT_RISK"
    assert projection["operator_status"] == "AT_RISK"
    assert "CAPITAL_CASHFLOW_NEGATIVE_STREAK_AT_RISK" in finding_codes
    assert projection["operator_console"]["safety"]["answer"] == "NOT_SAFE"
    assert projection["operator_console"]["weakest_month"]["net"] == -2000.0
    assert projection["operator_console"]["failure"]["status"] == "FAILS_WITHIN_HORIZON"
    assert projection["operator_console"]["failure"]["month"] == "2026-07"
    driver_names = [row["event_name"] for row in projection["operator_console"]["drivers"]["contributors"]]
    assert driver_names == ["expense_only", "income_only"]


def test_cashflow_projection_missing_expense_scenario_is_degraded(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.append_cashflow_event(
        event_name="income_only",
        event_type="invest_income",
        scenario="base",
        start_date="2026-05-01",
        end_date=None,
        frequency="monthly",
        amount=5000.0,
        confidence=1.0,
        is_deterministic=True,
        reason="cashflow_test_setup",
    )
    service.append_cashflow_event(
        event_name="florida_expense_only",
        event_type="expense",
        scenario="florida",
        start_date="2026-05-01",
        end_date=None,
        frequency="monthly",
        amount=2000.0,
        confidence=1.0,
        is_deterministic=True,
        reason="cashflow_test_setup",
    )

    projection = service.cashflow_projection(
        scenario="chile",
        start_month="2026-05",
        horizon_months=3,
    )
    finding_codes = {row["code"] for row in projection["validation"]["findings"]}
    assert projection["status"] == "DEGRADED"
    assert projection["operator_status"] == "TIGHT"
    assert "CAPITAL_CASHFLOW_SCENARIO_EXPENSE_MISSING" in finding_codes
    assert projection["operator_console"]["safety"]["answer"] == "SAFE_BUT_DEGRADED"


def test_cashflow_projection_is_deterministic_for_same_inputs(tmp_path: Path) -> None:
    service = _service(tmp_path)
    seed_capital_reference_dataset_v1(service=service)
    first = service.cashflow_projection(
        scenario="florida",
        start_month="2026-05",
        horizon_months=6,
    )
    second = service.cashflow_projection(
        scenario="florida",
        start_month="2026-05",
        horizon_months=6,
    )
    assert first["monthly_projection"] == second["monthly_projection"]
    assert first["validation"] == second["validation"]
