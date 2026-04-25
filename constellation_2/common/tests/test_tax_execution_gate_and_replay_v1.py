from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.tax.decision_v1 import preview_buy_tax_decision_v1, preview_sell_tax_decision_v1
from constellation_2.common.tax.execution_gate_v1 import validate_tax_decision_for_execution_v1
from constellation_2.common.tax.policy_v1 import build_tax_policy_registry_v1, resolve_tax_policy_set_v1
from constellation_2.common.tax.replay_v1 import (
    build_tax_correction_impact_index_v1,
    build_tax_decision_time_truth_view_v1,
    replay_tax_decision_v1,
)
from constellation_2.common.tax.state_v1 import build_tax_snapshot_v1
from constellation_2.common.tax.storage_v1 import append_validated_jsonl_v1, tax_jsonl_path_v1
from constellation_2.common.tax.truth_v1 import (
    accept_tax_fact_candidate_v1,
    build_tax_fact_candidate_v1,
    build_tax_fact_correction_v1,
    build_tax_observed_event_v1,
)


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


def test_execution_gate_accepts_current_sell_decision_and_is_journalable(tmp_path) -> None:
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
                "basis_total": "1400.00",
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
    policy_registry = build_tax_policy_registry_v1(scope_id="filing:joint")
    resolved = resolve_tax_policy_set_v1(
        policy_registry=policy_registry,
        scope_id="filing:joint",
        account_tax_regime="taxable_brokerage",
    )
    decision, fingerprint = preview_sell_tax_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={"account_id": "ACC-1", "security_id": "AAPL", "quantity": "2", "assumed_sale_price": "120.00"},
    )
    gate_result = validate_tax_decision_for_execution_v1(
        decision=decision,
        dependency_fingerprint=fingerprint,
        snapshot=snapshot,
        resolved_policy_set=resolved,
        account_tax_regime="taxable_brokerage",
        execution_subject_ref="mock_execution_intent:INTENT-1",
    )

    assert gate_result["gate_status"] == "EXECUTION_ELIGIBLE"
    journal_path = tax_jsonl_path_v1(
        output_root=tmp_path,
        family="tax_execution_gate_result",
        scope_id="filing:joint",
        filename="tax_execution_gate_result.v1.jsonl",
    )
    result = append_validated_jsonl_v1(
        path=journal_path,
        payloads=(gate_result,),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/TAX/tax_execution_gate_result.v1.schema.json",
    )
    assert result["appended_count"] == 1
    rows = [json.loads(line) for line in journal_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rows[0]["gate_status"] == "EXECUTION_ELIGIBLE"


def test_execution_gate_blocks_truth_and_policy_mismatch_deterministically() -> None:
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
                "security_id": "MSFT",
                "quantity": "5",
                "basis_total": "500.00",
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
    policy_registry = build_tax_policy_registry_v1(scope_id="filing:joint")
    resolved = resolve_tax_policy_set_v1(
        policy_registry=policy_registry,
        scope_id="filing:joint",
        account_tax_regime="taxable_brokerage",
    )
    decision, fingerprint = preview_sell_tax_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={"account_id": "ACC-1", "security_id": "MSFT", "quantity": "1", "assumed_sale_price": "90.00"},
    )

    changed_truth_facts = facts + (
        _accepted_fact(
            "buy_execution_posted",
            {
                "lot_id": "LOT-A",
                "account_id": "ACC-1",
                "security_id": "MSFT",
                "quantity": "1",
                "executed_at": "2026-04-16T11:00:00Z",
                "price": "90.00",
            },
            "2026-04-16T11:00:00Z",
        ),
    )
    truth_snapshot, _ = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at="2026-04-16T11:00:00Z",
        accepted_facts=changed_truth_facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    truth_gate = validate_tax_decision_for_execution_v1(
        decision=decision,
        dependency_fingerprint=fingerprint,
        snapshot=truth_snapshot,
        resolved_policy_set=resolved,
        account_tax_regime="taxable_brokerage",
        execution_subject_ref="mock_execution_intent:INTENT-2",
    )
    assert truth_gate["gate_status"] == "EXECUTION_BLOCKED_TRUTH_MISMATCH"
    assert "accepted_fact_set_hash" in truth_gate["mismatch_fields"]

    changed_policy_registry = build_tax_policy_registry_v1(
        scope_id="filing:joint",
        lot_selection_policy={"allowed_lot_selection_methods": ["deterministic_ranked_specific_lot"], "allow_partial_lot_selection": False},
    )
    changed_resolved = resolve_tax_policy_set_v1(
        policy_registry=changed_policy_registry,
        scope_id="filing:joint",
        account_tax_regime="taxable_brokerage",
    )
    stale_gate = validate_tax_decision_for_execution_v1(
        decision=decision,
        dependency_fingerprint=fingerprint,
        snapshot=snapshot,
        resolved_policy_set=changed_resolved,
        account_tax_regime="taxable_brokerage",
        execution_subject_ref="mock_execution_intent:INTENT-3",
    )
    assert stale_gate["gate_status"] == "EXECUTION_BLOCKED_STALE"
    assert "resolved_policy_set_id" in stale_gate["mismatch_fields"]


def test_execution_gate_review_required_for_safe_mode() -> None:
    facts = (
        _accepted_fact(
            "account_classification_tax_regime",
            {"account_id": "ACC-1", "tax_regime": "taxable_brokerage"},
            "2026-04-16T10:00:00Z",
        ),
        _accepted_fact(
            "tax_data_gap_detected",
            {"gap_code": "MISSING_HOLDING_PERIOD_HISTORY"},
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
    decision, fingerprint = preview_buy_tax_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={
            "account_id": "ACC-1",
            "security_id": "NVDA",
            "quantity": "1",
            "effective_at": "2026-04-16T16:00:00Z",
        },
    )
    gate_result = validate_tax_decision_for_execution_v1(
        decision=decision,
        dependency_fingerprint=fingerprint,
        snapshot=snapshot,
        resolved_policy_set=resolved,
        account_tax_regime="taxable_brokerage",
        execution_subject_ref="mock_execution_intent:INTENT-4",
    )

    assert decision["decision_mode"] == "SAFE_EXECUTION_ALLOWED"
    assert gate_result["gate_status"] == "EXECUTION_REVIEW_REQUIRED"
    assert "TAX_SAFE_DEGRADED_MODE" in gate_result["review_reasons"]


def test_correction_impact_index_and_replay_distinguish_decision_time_from_current_truth() -> None:
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
    decision_time_facts = (account_fact, lot_fact)
    snapshot, _ = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at="2026-04-16T10:01:00Z",
        accepted_facts=decision_time_facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    policy_registry = build_tax_policy_registry_v1(scope_id="filing:joint")
    resolved = resolve_tax_policy_set_v1(
        policy_registry=policy_registry,
        scope_id="filing:joint",
        account_tax_regime="taxable_brokerage",
    )
    decision, fingerprint = preview_sell_tax_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={"account_id": "ACC-1", "security_id": "AAPL", "quantity": "5", "assumed_sale_price": "150.00"},
    )

    correction, corrected_fact = build_tax_fact_correction_v1(
        superseded_fact=lot_fact,
        corrected_payload={
            "lot_id": "LOT-A",
            "account_id": "ACC-1",
            "security_id": "AAPL",
            "quantity": "10",
            "basis_total": "1400.00",
            "holding_period_start_at": "2025-01-01T00:00:00Z",
        },
        correction_reason="broker corrected historical basis",
        recorded_at="2026-04-16T11:00:00Z",
        affected_snapshot_ids=(snapshot["snapshot_id"],),
        affected_decision_ids=(decision["decision_id"],),
    )
    decision_time_view = build_tax_decision_time_truth_view_v1(
        decision=decision,
        accepted_facts=decision_time_facts,
    )
    impact_index = build_tax_correction_impact_index_v1(
        correction=correction,
        corrected_fact=corrected_fact,
        snapshot_memberships=(
            {"snapshot_id": snapshot["snapshot_id"], "accepted_fact_ids": decision_time_view["accepted_fact_ids"]},
        ),
        decision_truth_views=(decision_time_view,),
    )
    current_facts = decision_time_facts + (corrected_fact,)
    current_snapshot, _ = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at="2026-04-16T11:00:00Z",
        accepted_facts=current_facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    replay_report = replay_tax_decision_v1(
        decision=decision,
        original_dependency_fingerprint=fingerprint,
        decision_time_accepted_facts=decision_time_facts,
        current_accepted_facts=current_facts,
        current_snapshot=current_snapshot,
        current_resolved_policy_set=resolved,
    )

    assert impact_index["affected_snapshot_ids"] == [snapshot["snapshot_id"]]
    assert impact_index["affected_decision_ids"] == [decision["decision_id"]]
    assert impact_index["replay_required"] is True
    assert impact_index["impact_class"] == "decision_affecting"
    assert replay_report["original_truth_view"]["accepted_fact_set_hash"] != replay_report["current_truth_view"]["accepted_fact_set_hash"]
    assert replay_report["original_decision_valid_under_current_truth"] is False
    assert "TAX_REPLAY_DIVERGENCE" in replay_report["divergence_reason_codes"]
