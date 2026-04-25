from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.tax.decision_v1 import (
    preview_sell_tax_decision_v1,
    validate_tax_decision_dependency_fingerprint_v1,
)
from constellation_2.common.tax.policy_v1 import build_tax_policy_registry_v1, resolve_tax_policy_set_v1
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


def test_snapshot_reproducibility_and_sell_decision_determinism() -> None:
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
            "lot_opened",
            {
                "lot_id": "LOT-B",
                "account_id": "ACC-1",
                "security_id": "AAPL",
                "quantity": "10",
                "basis_total": "1250.00",
                "holding_period_start_at": "2026-04-01T00:00:00Z",
            },
            "2026-04-16T10:02:00Z",
        ),
    )

    snapshot_a, manifest_a = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at="2026-04-16T10:02:00Z",
        accepted_facts=facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    snapshot_b, manifest_b = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at="2026-04-16T10:02:00Z",
        accepted_facts=facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    assert snapshot_a == snapshot_b
    assert manifest_a == manifest_b

    policy_registry = build_tax_policy_registry_v1(scope_id="filing:joint")
    resolved = resolve_tax_policy_set_v1(
        policy_registry=policy_registry,
        scope_id="filing:joint",
        account_tax_regime="taxable_brokerage",
    )
    request = {"account_id": "ACC-1", "security_id": "AAPL", "quantity": "10", "assumed_sale_price": "150.00"}
    decision_a, fingerprint_a = preview_sell_tax_decision_v1(
        snapshot=snapshot_a,
        resolved_policy_set=resolved,
        request=request,
    )
    decision_b, fingerprint_b = preview_sell_tax_decision_v1(
        snapshot=snapshot_a,
        resolved_policy_set=resolved,
        request=request,
    )

    assert decision_a == decision_b
    assert fingerprint_a == fingerprint_b
    assert decision_a["decision_status"] == "approved"
    assert decision_a["chosen_route"]["route"]["route_lots"][0]["lot_id"] == "LOT-B"


def test_unknown_basis_and_unknown_holding_period_disable_optimization() -> None:
    facts = (
        _accepted_fact(
            "account_classification_tax_regime",
            {"account_id": "ACC-1", "tax_regime": "taxable_brokerage"},
            "2026-04-16T10:00:00Z",
        ),
        _accepted_fact(
            "lot_imported",
            {
                "lot_id": "LOT-IMPORT",
                "account_id": "ACC-1",
                "security_id": "MSFT",
                "quantity": "5",
                "confidence_state": "unknown",
                "maturity_state": "restricted",
            },
            "2026-04-16T10:01:00Z",
        ),
    )
    snapshot, _ = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at="2026-04-16T10:01:00Z",
        accepted_facts=facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    policy_registry = build_tax_policy_registry_v1(scope_id="filing:joint")
    resolved = resolve_tax_policy_set_v1(
        policy_registry=policy_registry,
        scope_id="filing:joint",
        account_tax_regime="taxable_brokerage",
    )
    decision, _ = preview_sell_tax_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={"account_id": "ACC-1", "security_id": "MSFT", "quantity": "5"},
    )

    assert decision["decision_mode"] in {"PREVIEW_ONLY", "REQUIRES_OPERATOR_REVIEW"}
    assert "TAX_BASIS_UNKNOWN" in decision["reason_codes"]
    assert "TAX_HOLDING_PERIOD_UNKNOWN" in decision["reason_codes"]


def test_no_silent_fallback_lot_selection_and_dependency_invalidation() -> None:
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
    )
    snapshot, _ = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at="2026-04-16T10:01:00Z",
        accepted_facts=facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    policy_registry = build_tax_policy_registry_v1(
        scope_id="filing:joint",
        lot_selection_policy={"allowed_lot_selection_methods": [], "allow_partial_lot_selection": True},
    )
    resolved = resolve_tax_policy_set_v1(
        policy_registry=policy_registry,
        scope_id="filing:joint",
        account_tax_regime="taxable_brokerage",
    )
    blocked_decision, fingerprint = preview_sell_tax_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={"account_id": "ACC-1", "security_id": "AAPL", "quantity": "1"},
    )
    assert blocked_decision["decision_status"] == "hard_blocked"
    assert blocked_decision["reason_codes"] == ["TAX_POLICY_BLOCK"]

    changed_policy_registry = build_tax_policy_registry_v1(scope_id="filing:joint")
    changed_resolved = resolve_tax_policy_set_v1(
        policy_registry=changed_policy_registry,
        scope_id="filing:joint",
        account_tax_regime="taxable_brokerage",
    )
    matches, reason_codes = validate_tax_decision_dependency_fingerprint_v1(
        fingerprint=fingerprint,
        snapshot=snapshot,
        resolved_policy_set=changed_resolved,
        account_tax_regime="taxable_brokerage",
        algorithm_version=blocked_decision["algorithm_version"],
    )
    assert not matches
    assert "TAX_DEPENDENCY_FINGERPRINT_MISMATCH" in reason_codes
