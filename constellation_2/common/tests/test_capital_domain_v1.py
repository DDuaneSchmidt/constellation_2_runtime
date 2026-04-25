from __future__ import annotations

from pathlib import Path

import pytest

from constellation_2.common.capital.constants_v1 import (
    EXPECTED_BUCKET_TOTALS_V1,
    EXPECTED_CONTROL_TOTALS_V1,
    EXPECTED_INVESTABLE_TOTAL_V1,
)
from constellation_2.common.capital.service_v1 import CapitalDomainServiceV1
from constellation_2.common.capital.seed_v1 import seed_capital_reference_dataset_v1


def _service(tmp_path: Path) -> CapitalDomainServiceV1:
    return CapitalDomainServiceV1(
        db_path=tmp_path / "capital_v1" / "capital_domain.v1.sqlite3",
        actor="test_capital_domain_v1",
    )


def test_seed_reference_dataset_matches_expected_investable_totals(tmp_path: Path) -> None:
    service = _service(tmp_path)
    seed = seed_capital_reference_dataset_v1(service=service)
    assert seed["investable_total"] == EXPECTED_INVESTABLE_TOTAL_V1

    overview = service.overview_surface(as_of_date="2026-04-22")
    assert overview["investable_total"] == EXPECTED_INVESTABLE_TOTAL_V1
    assert overview["included_account_count"] == 7
    assert overview["excluded_account_count"] == 5

    control_map = {row["control_type"]: row["balance_total"] for row in overview["allocation_by_control"]["rows"]}
    assert control_map == EXPECTED_CONTROL_TOTALS_V1

    bucket_map = {row["bucket_type"]: row["balance_total"] for row in overview["allocation_by_bucket"]["rows"]}
    assert bucket_map == EXPECTED_BUCKET_TOTALS_V1


def test_seed_keeps_aegis_paper_excluded_from_investable_allocation(tmp_path: Path) -> None:
    service = _service(tmp_path)
    seed_capital_reference_dataset_v1(service=service)
    overview = service.overview_surface(as_of_date="2026-04-22")
    contributing = {row["account_id"] for row in overview["contributing_accounts"]}
    assert "aegis_paper" not in contributing
    assert "aegis_paul" in contributing


def test_snapshot_append_is_append_only_and_duplicate_source_is_blocked(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.create_account(account_id="test_account", account_name="Test Account")
    service.append_classification(
        account_id="test_account",
        effective_from="2026-04-22",
        effective_to=None,
        capital_type="owned",
        control_type="self",
        bucket_type="growth",
        include_in_allocation=True,
        confidence_level=1.0,
        reason="test_initial_classification",
    )
    service.append_balance_snapshot(
        account_id="test_account",
        as_of_date="2026-04-22",
        balance=1000.0,
        input_source="manual_entry",
    )
    with pytest.raises(ValueError, match="CAPITAL_SNAPSHOT_DUPLICATE"):
        service.append_balance_snapshot(
            account_id="test_account",
            as_of_date="2026-04-22",
            balance=1005.0,
            input_source="manual_entry",
        )
    service.append_balance_snapshot(
        account_id="test_account",
        as_of_date="2026-04-22",
        balance=1005.0,
        input_source="operator_adjustment",
    )
    validation = service.validation_errors(as_of_date="2026-04-22")
    codes = {row["code"] for row in validation["findings"]}
    assert "CAPITAL_DUPLICATE_SNAPSHOT_ACCOUNT_DAY" in codes


def test_validation_flags_disallowed_included_capital_type(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.create_account(account_id="future_account", account_name="Future Account")
    service.append_classification(
        account_id="future_account",
        effective_from="2026-04-22",
        effective_to=None,
        capital_type="future",
        control_type="passive",
        bucket_type="growth",
        include_in_allocation=True,
        confidence_level=0.5,
        reason="test_future_included_violation",
    )
    service.append_balance_snapshot(
        account_id="future_account",
        as_of_date="2026-04-22",
        balance=42.0,
        input_source="manual_entry",
    )
    validation = service.validation_errors(as_of_date="2026-04-22")
    codes = {row["code"] for row in validation["findings"]}
    assert "CAPITAL_INCLUDED_FUTURE_DISALLOWED" in codes
    assert "CAPITAL_INCLUDED_ACCOUNT_DISALLOWED_TYPE" in codes


def test_history_series_and_aegis_pct_series_are_present_after_seed(tmp_path: Path) -> None:
    service = _service(tmp_path)
    seed_capital_reference_dataset_v1(service=service)
    investable = service.history_investable_total()
    aegis_pct = service.history_aegis_allocation_pct()
    assert investable["points"]
    assert aegis_pct["points"]
    assert investable["points"][-1]["day"] == "2026-04-22"
    assert investable["points"][-1]["investable_total"] == EXPECTED_INVESTABLE_TOTAL_V1
    assert aegis_pct["points"][-1]["aegis_balance"] == EXPECTED_CONTROL_TOTALS_V1["aegis"]


def test_cash_flows_are_append_only_and_summarized(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.create_account(account_id="flow_account", account_name="Flow Account")
    service.append_classification(
        account_id="flow_account",
        effective_from="2026-04-22",
        effective_to=None,
        capital_type="owned",
        control_type="self",
        bucket_type="income",
        include_in_allocation=True,
        confidence_level=1.0,
        reason="test_flow_account_initial_classification",
    )
    service.append_balance_snapshot(
        account_id="flow_account",
        as_of_date="2026-04-22",
        balance=100.0,
        input_source="manual_entry",
    )
    service.append_cash_flow(
        account_id="flow_account",
        flow_date="2026-04-22",
        flow_amount=25.0,
        flow_type="contribution",
    )
    service.append_cash_flow(
        account_id="flow_account",
        flow_date="2026-04-23",
        flow_amount=-10.0,
        flow_type="withdrawal",
    )
    rows = service.list_cash_flows()
    assert len(rows) == 2
    summary = service.flow_summary_by_period()
    assert summary["flow_truth_owner"]["truth_owner"] == "capital_cash_flows_v1"
    assert summary["rows"][0]["period"] == "2026-04"
    assert summary["rows"][0]["net_flow_amount"] == 15.0


def test_classification_change_requires_reason(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.create_account(account_id="reason_required", account_name="Reason Required")
    with pytest.raises(ValueError, match="CAPITAL_REASON_REQUIRED"):
        service.append_classification(
            account_id="reason_required",
            effective_from="2026-04-22",
            effective_to=None,
            capital_type="owned",
            control_type="self",
            bucket_type="growth",
            include_in_allocation=True,
            confidence_level=1.0,
            reason="",
        )


def test_classification_change_records_before_after_and_preserves_history(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.create_account(account_id="history_account", account_name="History Account")
    first = service.append_classification(
        account_id="history_account",
        effective_from="2026-04-22",
        effective_to="2026-04-23",
        capital_type="owned",
        control_type="passive",
        bucket_type="safety",
        include_in_allocation=True,
        confidence_level=0.9,
        reason="initial_semantic_state",
    )
    second = service.append_classification(
        account_id="history_account",
        effective_from="2026-04-24",
        effective_to=None,
        capital_type="owned",
        control_type="advisor",
        bucket_type="growth",
        include_in_allocation=True,
        confidence_level=0.9,
        reason="advisor_took_control",
    )
    rows = service.list_classifications(account_id="history_account")
    assert len(rows) == 2
    day_22 = service.current_classification_per_account(as_of_date="2026-04-22")["rows"][0]
    day_24 = service.current_classification_per_account(as_of_date="2026-04-24")["rows"][0]
    assert day_22["control_type"] == "passive"
    assert day_24["control_type"] == "advisor"
    audits = service.list_audit_entries(limit=25)
    target = next(
        row
        for row in audits
        if row["entity_type"] == "classification" and row["entity_id"] == second["classification_id"]
    )
    assert target["before_json"] is not None
    assert target["after_json"] is not None


def test_report_basis_and_explainability_are_present(tmp_path: Path) -> None:
    service = _service(tmp_path)
    seed_capital_reference_dataset_v1(service=service)
    overview = service.overview_surface(as_of_date="2026-04-22")
    assert overview["report_basis_metadata"]["report_basis"] == "latest_per_account"
    assert overview["report_basis_metadata"]["stale_account_count"] == 0
    assert overview["investable_explainability"]["contributors"]
    assert overview["investable_explainability"]["denominator_value"] == EXPECTED_INVESTABLE_TOTAL_V1
    control = service.allocation_by_control(as_of_date="2026-04-22")
    assert control["report_basis_metadata"]["report_basis"] == "latest_per_account"
    assert control["rows"][0]["contributors"]
    assert control["rows"][0]["denominator_value"] == EXPECTED_INVESTABLE_TOTAL_V1


def test_conflicting_snapshot_notes_do_not_override_cash_flow_truth(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.create_account(account_id="flow_truth", account_name="Flow Truth")
    service.append_classification(
        account_id="flow_truth",
        effective_from="2026-04-22",
        effective_to=None,
        capital_type="owned",
        control_type="self",
        bucket_type="income",
        include_in_allocation=True,
        confidence_level=1.0,
        reason="flow_truth_account_setup",
    )
    service.append_balance_snapshot(
        account_id="flow_truth",
        as_of_date="2026-04-22",
        balance=1000.0,
        input_source="manual_entry",
        notes="net_flow_override_attempt=9999999",
    )
    service.append_cash_flow(
        account_id="flow_truth",
        flow_date="2026-04-22",
        flow_amount=50.0,
        flow_type="contribution",
    )
    summary = service.flow_summary_by_period()
    assert summary["rows"][0]["net_flow_amount"] == 50.0


def test_validation_flags_residence_and_simulated_included_rules(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.create_account(account_id="sim_inc", account_name="Sim Inc")
    service.append_classification(
        account_id="sim_inc",
        effective_from="2026-04-22",
        effective_to=None,
        capital_type="simulated",
        control_type="self",
        bucket_type="growth",
        include_in_allocation=True,
        confidence_level=0.7,
        reason="rule_test_simulated",
    )
    service.append_balance_snapshot(
        account_id="sim_inc",
        as_of_date="2026-04-22",
        balance=100.0,
        input_source="manual_entry",
    )
    service.create_account(account_id="res_inc", account_name="Residence Inc")
    service.append_classification(
        account_id="res_inc",
        effective_from="2026-04-22",
        effective_to=None,
        capital_type="owned",
        control_type="self",
        bucket_type="residence",
        include_in_allocation=True,
        confidence_level=0.7,
        reason="rule_test_residence",
    )
    service.append_balance_snapshot(
        account_id="res_inc",
        as_of_date="2026-04-22",
        balance=200.0,
        input_source="manual_entry",
    )
    validation = service.validation_errors(as_of_date="2026-04-22")
    codes = {row["code"] for row in validation["findings"]}
    assert "CAPITAL_INCLUDED_SIMULATED_DISALLOWED" in codes
    assert "CAPITAL_INCLUDED_RESIDENCE_BUCKET_DISALLOWED" in codes


def test_stale_included_account_detection_and_status(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.create_account(account_id="fresh", account_name="Fresh")
    service.create_account(account_id="stale", account_name="Stale")
    for account_id in ["fresh", "stale"]:
        service.append_classification(
            account_id=account_id,
            effective_from="2026-04-22",
            effective_to=None,
            capital_type="owned",
            control_type="self",
            bucket_type="growth",
            include_in_allocation=True,
            confidence_level=1.0,
            reason="stale_detection_setup",
        )
    service.append_balance_snapshot(
        account_id="stale",
        as_of_date="2026-04-22",
        balance=100.0,
        input_source="manual_entry",
    )
    service.append_balance_snapshot(
        account_id="fresh",
        as_of_date="2026-04-23",
        balance=100.0,
        input_source="manual_entry",
    )
    validation = service.validation_errors(as_of_date="2026-04-23")
    codes = {row["code"] for row in validation["findings"]}
    assert validation["status"] == "DEGRADED"
    assert "CAPITAL_INCLUDED_ACCOUNT_SNAPSHOT_STALE" in codes


def test_confidence_band_is_present_after_seed(tmp_path: Path) -> None:
    service = _service(tmp_path)
    seed_capital_reference_dataset_v1(service=service)
    classes = service.current_classification_per_account(as_of_date="2026-04-22")
    by_account = {row["account_id"]: row for row in classes["rows"]}
    assert by_account["inheritance"]["confidence_band"] == "low"
    assert by_account["schwab_brokerage"]["confidence_band"] == "high"
