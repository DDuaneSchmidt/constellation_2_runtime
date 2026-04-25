from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.tax.corporate_actions_v1 import (
    accept_tax_corporate_action_candidate_v1,
    build_tax_corporate_action_candidate_v1,
    build_tax_corporate_action_state_v1,
)
from constellation_2.common.tax.decision_v1 import preview_sell_tax_decision_v1
from constellation_2.common.tax.policy_v1 import build_tax_policy_registry_v1, resolve_tax_policy_set_v1
from constellation_2.common.tax.reconciliation_v1 import build_broker_tax_reconciliation_report_v1, build_lot_reconciliation_report_v1
from constellation_2.common.tax.state_v1 import build_tax_snapshot_v1
from constellation_2.common.tax.truth_v1 import accept_tax_fact_candidate_v1, build_tax_fact_candidate_v1, build_tax_observed_event_v1


def _accepted_fact(event_family: str, payload: dict[str, str], recorded_at: str) -> dict[str, object]:
    observed = build_tax_observed_event_v1(
        event_family=event_family,
        scope_ids=("filing:joint",),
        payload=payload,
        source_event_ref=f"{event_family}:{recorded_at}",
        observed_at=recorded_at,
        recorded_at=recorded_at,
    )
    candidate = build_tax_fact_candidate_v1(observed)
    _, accepted = accept_tax_fact_candidate_v1(candidate)
    assert accepted is not None
    return accepted


def test_provisional_corporate_action_restricts_sell_optimization_until_finalized() -> None:
    lot_fact = _accepted_fact(
        "lot_opened",
        {
            "lot_id": "LOT-A",
            "account_id": "ACC-1",
            "security_id": "AAPL",
            "quantity": "10",
            "basis_total": "1000.00",
            "holding_period_start_at": "2025-01-01T00:00:00Z",
        },
        "2026-04-16T10:01:00Z",
    )
    account_fact = _accepted_fact(
        "account_classification_tax_regime",
        {"account_id": "ACC-1", "tax_regime": "taxable_brokerage"},
        "2026-04-16T10:00:00Z",
    )
    observed = build_tax_observed_event_v1(
        event_family="corporate_action_observed",
        scope_ids=("filing:joint",),
        payload={
            "security_id": "AAPL",
            "event_type": "stock_split",
            "effective_at": "2026-04-16T12:00:00Z",
            "ratio_numerator": "2",
            "ratio_denominator": "1",
        },
        source_event_ref="corp:AAPL:split",
        observed_at="2026-04-16T11:00:00Z",
        recorded_at="2026-04-16T11:00:00Z",
    )
    candidate = build_tax_corporate_action_candidate_v1(observed)
    acceptance_decision, provisional_fact = accept_tax_corporate_action_candidate_v1(candidate, finalize=False)
    facts = (account_fact, lot_fact, provisional_fact)
    snapshot, _ = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at=str(provisional_fact["recorded_at"]),
        accepted_facts=facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    policy_registry = build_tax_policy_registry_v1(scope_id="filing:joint")
    resolved = resolve_tax_policy_set_v1(
        policy_registry=policy_registry,
        scope_id="filing:joint",
        account_tax_regime="taxable_brokerage",
    )
    sell_decision, _ = preview_sell_tax_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={"account_id": "ACC-1", "security_id": "AAPL", "quantity": "1", "assumed_sale_price": "120.00"},
    )

    assert acceptance_decision["decision_status"] == "provisional"
    assert "TAX_CORP_ACTION_UNRESOLVED" in snapshot["reason_codes"]
    assert sell_decision["decision_mode"] == "PREVIEW_ONLY"
    assert "TAX_CORP_ACTION_UNRESOLVED" in sell_decision["reason_codes"]


def test_finalized_split_flows_additively_and_updates_snapshot_quantity() -> None:
    account_fact = _accepted_fact(
        "account_classification_tax_regime",
        {"account_id": "ACC-1", "tax_regime": "taxable_brokerage"},
        "2026-04-16T10:00:00Z",
    )
    lot_fact = _accepted_fact(
        "lot_opened",
        {
            "lot_id": "LOT-A",
            "account_id": "ACC-1",
            "security_id": "AAPL",
            "quantity": "10",
            "basis_total": "1000.00",
            "holding_period_start_at": "2025-01-01T00:00:00Z",
        },
        "2026-04-16T10:01:00Z",
    )
    observed = build_tax_observed_event_v1(
        event_family="corporate_action_observed",
        scope_ids=("filing:joint",),
        payload={
            "security_id": "AAPL",
            "event_type": "stock_split",
            "effective_at": "2026-04-16T12:00:00Z",
            "ratio_numerator": "2",
            "ratio_denominator": "1",
        },
        source_event_ref="corp:AAPL:split",
        observed_at="2026-04-16T11:00:00Z",
        recorded_at="2026-04-16T11:00:00Z",
    )
    candidate = build_tax_corporate_action_candidate_v1(observed)
    _, provisional_fact = accept_tax_corporate_action_candidate_v1(candidate, finalize=False)
    final_decision, finalized_fact = accept_tax_corporate_action_candidate_v1(candidate, finalize=True)
    snapshot, _ = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at=finalized_fact["recorded_at"],
        accepted_facts=(account_fact, lot_fact, provisional_fact, finalized_fact),
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    corp_state = build_tax_corporate_action_state_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        accepted_facts=(provisional_fact, finalized_fact),
    )

    assert final_decision["decision_status"] == "finalized"
    assert provisional_fact["accepted_fact_id"] != finalized_fact["accepted_fact_id"]
    assert snapshot["lot_states"][0]["remaining_quantity"] == "20.00000000"
    assert snapshot["lot_states"][0]["basis_total"] == "1000.00"
    assert corp_state["actions"][0]["action_state"] == "finalized"


def test_unsupported_corporate_action_yields_deferred_reason_codes() -> None:
    observed = build_tax_observed_event_v1(
        event_family="corporate_action_observed",
        scope_ids=("filing:joint",),
        payload={"security_id": "AAPL", "event_type": "spinoff", "effective_at": "2026-04-16T12:00:00Z"},
        source_event_ref="corp:AAPL:spinoff",
        observed_at="2026-04-16T11:00:00Z",
        recorded_at="2026-04-16T11:00:00Z",
    )
    candidate = build_tax_corporate_action_candidate_v1(observed)
    decision, accepted_fact = accept_tax_corporate_action_candidate_v1(candidate, finalize=True)

    assert candidate["support_status"] == "deferred"
    assert decision["decision_status"] == "deferred"
    assert "TAX_CORP_ACTION_UNSUPPORTED" in accepted_fact["reason_codes"]


def test_reconciliation_reports_detect_basis_realized_and_missing_mismatches_without_mutating_truth() -> None:
    facts = (
        _accepted_fact(
            "account_classification_tax_regime",
            {"account_id": "ACC-1", "tax_regime": "taxable_brokerage"},
            "2026-04-16T10:00:00Z",
        ),
        _accepted_fact(
            "lot_opened",
            {
                "lot_id": "LOT-A",
                "account_id": "ACC-1",
                "security_id": "AAPL",
                "quantity": "10",
                "basis_total": "1000.00",
                "holding_period_start_at": "2025-01-01T00:00:00Z",
            },
            "2026-04-16T10:01:00Z",
        ),
        _accepted_fact(
            "realization_recorded",
            {
                "lot_id": "LOT-A",
                "account_id": "ACC-1",
                "security_id": "AAPL",
                "realized_gain_loss": "-50.00",
                "realized_proceeds": "950.00",
                "realized_at": "2026-04-15T10:00:00Z",
            },
            "2026-04-16T10:02:00Z",
        ),
    )
    before_ids = tuple(fact["accepted_fact_id"] for fact in facts)
    snapshot, _ = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at="2026-04-16T10:02:00Z",
        accepted_facts=facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    lot_report = build_lot_reconciliation_report_v1(
        snapshot=snapshot,
        accepted_facts=facts,
        broker_lot_view=(
            {
                "lot_id": "LOT-A",
                "remaining_quantity": "10",
                "basis_total": "900.00",
                "acquisition_date": "2025-01-01T00:00:00Z",
                "holding_period_state": "long_term",
                "corporate_action_refs": [],
            },
            {
                "lot_id": "LOT-BROKER-ONLY",
                "remaining_quantity": "1",
                "basis_total": "10.00",
                "acquisition_date": "2026-01-01T00:00:00Z",
                "holding_period_state": "short_term",
                "corporate_action_refs": [],
            },
        ),
    )
    broker_report = build_broker_tax_reconciliation_report_v1(
        snapshot=snapshot,
        accepted_facts=facts,
        broker_lot_view=(
            {
                "lot_id": "LOT-A",
                "remaining_quantity": "10",
                "basis_total": "900.00",
                "acquisition_date": "2025-01-01T00:00:00Z",
                "holding_period_state": "long_term",
                "corporate_action_refs": [],
            },
        ),
        broker_realized_view=(
            {
                "lot_id": "LOT-A",
                "account_id": "ACC-1",
                "security_id": "AAPL",
                "realized_at": "2026-04-15T10:00:00Z",
                "realized_proceeds": "940.00",
                "realized_gain_loss": "-60.00",
            },
        ),
    )

    assert lot_report["mismatch_summary"]["basis_mismatch"] == 1
    assert lot_report["mismatch_summary"]["missing_in_constellation"] == 1
    assert broker_report["mismatch_summary"]["realized_gain_loss_mismatch"] == 1
    assert broker_report["mismatch_summary"]["realized_proceeds_mismatch"] == 1
    assert tuple(fact["accepted_fact_id"] for fact in facts) == before_ids
