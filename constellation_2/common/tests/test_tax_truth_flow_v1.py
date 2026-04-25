from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.tax.truth_v1 import (
    accept_tax_fact_candidate_v1,
    build_accepted_tax_fact_journal_entry_v1,
    build_tax_fact_candidate_v1,
    build_tax_fact_correction_v1,
    build_tax_observed_event_v1,
    effective_accepted_facts_v1,
)
from constellation_2.common.tax.storage_v1 import append_validated_jsonl_v1, tax_jsonl_path_v1


def test_observed_to_candidate_to_accepted_truth_flow_is_explicit(tmp_path) -> None:
    observed = build_tax_observed_event_v1(
        event_family="account_classification_tax_regime",
        scope_ids=("filing:joint",),
        payload={"account_id": "ACC-1", "tax_regime": "taxable_brokerage"},
        source_event_ref="broker:acct-regime:1",
        observed_at="2026-04-16T13:00:00Z",
        recorded_at="2026-04-16T13:01:00Z",
    )
    candidate = build_tax_fact_candidate_v1(observed)
    decision, accepted = accept_tax_fact_candidate_v1(candidate)

    assert observed["schema_id"] == "tax_observed_event"
    assert candidate["truth_state"] == "candidate"
    assert decision["decision_status"] == "accept"
    assert accepted is not None
    assert accepted["truth_state"] == "accepted"
    assert accepted["payload"]["tax_regime"] == "taxable_brokerage"

    journal_entry = build_accepted_tax_fact_journal_entry_v1(accepted)
    result = append_validated_jsonl_v1(
        path=tax_jsonl_path_v1(
            output_root=tmp_path,
            family="accepted_tax_fact_v1",
            scope_id="filing:joint",
            filename="accepted_tax_fact_journal.v1.jsonl",
        ),
        payloads=(journal_entry,),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/TAX/accepted_tax_fact_journal.v1.schema.json",
    )
    assert result["appended_count"] == 1


def test_imported_fact_becomes_restricted_use_when_basis_or_holding_period_is_unknown() -> None:
    observed = build_tax_observed_event_v1(
        event_family="lot_imported",
        scope_ids=("filing:joint",),
        payload={
            "lot_id": "LOT-IMPORT-1",
            "account_id": "ACC-1",
            "security_id": "AAPL",
            "quantity": "10",
            "confidence_state": "unknown",
            "maturity_state": "restricted",
        },
        source_event_ref="import:lot:1",
        observed_at="2026-04-16T14:00:00Z",
        recorded_at="2026-04-16T14:01:00Z",
    )
    candidate = build_tax_fact_candidate_v1(observed)
    decision, accepted = accept_tax_fact_candidate_v1(candidate)

    assert decision["decision_status"] == "restrict"
    assert accepted is not None
    assert accepted["truth_state"] == "restricted_use"
    assert "TAX_BASIS_UNKNOWN" in accepted["reason_codes"]
    assert "TAX_HOLDING_PERIOD_UNKNOWN" in accepted["reason_codes"]


def test_correction_lineage_is_additive_and_preserves_original_fact() -> None:
    observed = build_tax_observed_event_v1(
        event_family="account_classification_tax_regime",
        scope_ids=("filing:joint",),
        payload={"account_id": "ACC-1", "tax_regime": "taxable_brokerage"},
        source_event_ref="broker:acct-regime:1",
        observed_at="2026-04-16T13:00:00Z",
        recorded_at="2026-04-16T13:01:00Z",
    )
    candidate = build_tax_fact_candidate_v1(observed)
    _, accepted = accept_tax_fact_candidate_v1(candidate)
    assert accepted is not None

    correction, corrected_fact = build_tax_fact_correction_v1(
        superseded_fact=accepted,
        corrected_payload={"account_id": "ACC-1", "tax_regime": "tax_deferred"},
        correction_reason="broker corrected account tax classification",
        recorded_at="2026-04-16T15:00:00Z",
    )

    effective_before = effective_accepted_facts_v1(
        accepted_facts=(accepted, corrected_fact),
        recorded_at_cutoff="2026-04-16T14:59:59Z",
    )
    effective_after = effective_accepted_facts_v1(
        accepted_facts=(accepted, corrected_fact),
        recorded_at_cutoff="2026-04-16T15:00:00Z",
    )

    assert accepted["payload"]["tax_regime"] == "taxable_brokerage"
    assert correction["superseded_fact_id"] == accepted["accepted_fact_id"]
    assert corrected_fact["supersedes_fact_id"] == accepted["accepted_fact_id"]
    assert effective_before[0]["accepted_fact_id"] == accepted["accepted_fact_id"]
    assert effective_after[0]["accepted_fact_id"] == corrected_fact["accepted_fact_id"]
