from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.tax.corporate_actions_v1 import (
    accept_tax_corporate_action_candidate_v1,
    build_tax_corporate_action_candidate_v1,
)
from constellation_2.common.tax.harvest_v1 import preview_harvest_candidate_decision_v1
from constellation_2.common.tax.policy_v1 import build_tax_policy_registry_v1, resolve_tax_policy_set_v1
from constellation_2.common.tax.routing_v1 import preview_account_routing_tax_decision_v1
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


def test_account_routing_prefers_taxable_when_harvesting_is_required() -> None:
    facts = (
        _accepted_fact(
            "account_classification_tax_regime",
            {"account_id": "ACC-TAX", "tax_regime": "taxable_brokerage"},
            "2026-04-16T10:00:00Z",
        ),
        _accepted_fact(
            "account_classification_tax_regime",
            {"account_id": "ACC-ROTH", "tax_regime": "roth"},
            "2026-04-16T10:00:01Z",
        ),
    )
    snapshot, _ = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at="2026-04-16T10:00:01Z",
        accepted_facts=facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    policy_registry = build_tax_policy_registry_v1(scope_id="filing:joint")
    resolved = resolve_tax_policy_set_v1(
        policy_registry=policy_registry,
        scope_id="filing:joint",
        account_tax_regime="taxable_brokerage",
    )
    decision, _ = preview_account_routing_tax_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={
            "candidate_account_ids": ["ACC-ROTH", "ACC-TAX"],
            "security_id": "AAPL",
            "asset_tax_classification": "capital_gain_sensitive",
            "requires_loss_harvesting": True,
        },
    )

    assert decision["decision_status"] == "approved"
    assert decision["chosen_account"]["account_id"] == "ACC-TAX"


def test_account_routing_prefers_sheltered_for_turnover_and_requires_review_when_asset_classification_missing() -> None:
    facts = (
        _accepted_fact(
            "account_classification_tax_regime",
            {"account_id": "ACC-TAX", "tax_regime": "taxable_brokerage"},
            "2026-04-16T10:00:00Z",
        ),
        _accepted_fact(
            "account_classification_tax_regime",
            {"account_id": "ACC-DEF", "tax_regime": "tax_deferred"},
            "2026-04-16T10:00:01Z",
        ),
    )
    snapshot, _ = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at="2026-04-16T10:00:01Z",
        accepted_facts=facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    policy_registry = build_tax_policy_registry_v1(scope_id="filing:joint")
    resolved = resolve_tax_policy_set_v1(
        policy_registry=policy_registry,
        scope_id="filing:joint",
        account_tax_regime="taxable_brokerage",
    )
    decision, _ = preview_account_routing_tax_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={
            "candidate_account_ids": ["ACC-TAX", "ACC-DEF"],
            "security_id": "VNQ",
            "turnover_profile": "high",
        },
    )

    assert decision["decision_status"] == "requires_operator_review"
    assert decision["chosen_account"]["account_id"] == "ACC-DEF"
    assert "TAX_ASSET_TAX_CLASSIFICATION_MISSING" in decision["reason_codes"]


def test_account_routing_blocks_when_candidate_accounts_have_no_known_regime() -> None:
    snapshot, _ = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at="2026-04-16T10:00:00Z",
        accepted_facts=(),
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    policy_registry = build_tax_policy_registry_v1(scope_id="filing:joint")
    resolved = resolve_tax_policy_set_v1(
        policy_registry=policy_registry,
        scope_id="filing:joint",
        account_tax_regime="UNKNOWN",
    )
    decision, _ = preview_account_routing_tax_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={"candidate_account_ids": ["ACC-MISSING"], "security_id": "AAPL", "asset_tax_classification": "capital_gain_sensitive"},
    )

    assert decision["decision_status"] == "hard_blocked"
    assert decision["reason_codes"] == ["TAX_ACCOUNT_REGIME_MISSING"]


def test_harvest_preview_excludes_unknown_basis_and_corporate_action_and_keeps_ordering_stable() -> None:
    corporate_action_observed = build_tax_observed_event_v1(
        event_family="corporate_action_observed",
        scope_ids=("filing:joint",),
        payload={
            "security_id": "MSFT",
            "event_type": "stock_split",
            "effective_at": "2026-04-16T12:00:00Z",
            "ratio_numerator": "2",
            "ratio_denominator": "1",
        },
        source_event_ref="corp:MSFT:split",
        observed_at="2026-04-16T10:04:00Z",
        recorded_at="2026-04-16T10:04:00Z",
    )
    corporate_action_candidate = build_tax_corporate_action_candidate_v1(corporate_action_observed)
    _, provisional_fact = accept_tax_corporate_action_candidate_v1(corporate_action_candidate, finalize=False)
    facts = (
        _accepted_fact(
            "account_classification_tax_regime",
            {"account_id": "ACC-1", "tax_regime": "taxable_brokerage"},
            "2026-04-16T10:00:00Z",
        ),
        _accepted_fact(
            "lot_opened",
            {
                "lot_id": "LOSS-BIG",
                "account_id": "ACC-1",
                "security_id": "AAPL",
                "quantity": "10",
                "basis_total": "1500.00",
                "holding_period_start_at": "2025-01-01T00:00:00Z",
            },
            "2026-04-16T10:01:00Z",
        ),
        _accepted_fact(
            "lot_opened",
            {
                "lot_id": "LOSS-SMALL",
                "account_id": "ACC-1",
                "security_id": "MSFT",
                "quantity": "10",
                "basis_total": "1200.00",
                "holding_period_start_at": "2025-01-01T00:00:00Z",
            },
            "2026-04-16T10:02:00Z",
        ),
        _accepted_fact(
            "lot_imported",
            {
                "lot_id": "IMPORT-UNKNOWN",
                "account_id": "ACC-1",
                "security_id": "NVDA",
                "quantity": "5",
                "confidence_state": "unknown",
                "maturity_state": "restricted",
            },
            "2026-04-16T10:03:00Z",
        ),
        provisional_fact,
    )
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
    decision, _ = preview_harvest_candidate_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={
            "market_prices_by_security": {
                "AAPL": "100.00",
                "MSFT": "110.00",
                "NVDA": "50.00",
            }
        },
    )

    assert [row["lot_id"] for row in decision["candidate_lots"]] == ["LOSS-BIG"]
    excluded = {row["lot_id"]: row["reason_codes"] for row in decision["excluded_lots"]}
    assert "TAX_BASIS_UNKNOWN" in excluded["IMPORT-UNKNOWN"]
    assert "TAX_CORP_ACTION_UNRESOLVED" in excluded["LOSS-SMALL"]


def test_harvest_preview_blocks_wash_risk_when_policy_requires_it() -> None:
    facts = (
        _accepted_fact(
            "account_classification_tax_regime",
            {"account_id": "ACC-1", "tax_regime": "taxable_brokerage"},
            "2026-04-16T10:00:00Z",
        ),
        _accepted_fact(
            "lot_opened",
            {
                "lot_id": "LOSS-LOT",
                "account_id": "ACC-1",
                "security_id": "NVDA",
                "quantity": "10",
                "basis_total": "1000.00",
                "holding_period_start_at": "2025-01-01T00:00:00Z",
            },
            "2026-04-16T10:01:00Z",
        ),
        _accepted_fact(
            "wash_sale_detected",
            {
                "security_id": "NVDA",
                "loss_sale_at": "2026-04-15T10:00:00Z",
                "window_end_at": "2026-05-15T10:00:00Z",
            },
            "2026-04-16T10:02:00Z",
        ),
    )
    snapshot, _ = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at="2026-04-16T10:02:00Z",
        accepted_facts=facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    policy_registry = build_tax_policy_registry_v1(scope_id="filing:joint")
    resolved = resolve_tax_policy_set_v1(
        policy_registry=policy_registry,
        scope_id="filing:joint",
        account_tax_regime="taxable_brokerage",
    )
    decision, _ = preview_harvest_candidate_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={"market_prices_by_security": {"NVDA": "80.00"}},
    )

    assert decision["candidate_lots"] == []
    assert decision["excluded_lots"][0]["reason_codes"] == ["TAX_WASH_RISK_BLOCK"]
