from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.tax.decision_v1 import (
    build_tax_decision_journal_entry_v1,
    preview_buy_tax_decision_v1,
    preview_sell_tax_decision_v1,
)
from constellation_2.common.tax.policy_v1 import build_tax_policy_registry_v1, resolve_tax_policy_set_v1
from constellation_2.common.tax.reporting_v1 import (
    build_broker_tax_reconciliation_report_v1,
    build_decision_replay_report_v1,
    build_realized_tax_report_v1,
    build_tax_advisory_explanation_v1,
)
from constellation_2.common.tax.state_v1 import build_tax_snapshot_v1
from constellation_2.common.tax.storage_v1 import append_validated_jsonl_v1, tax_jsonl_path_v1
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


def test_buy_preview_blocks_for_wash_sale_conflict_and_replay_is_journalable(tmp_path) -> None:
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
                "quantity": "5",
                "basis_total": "600.00",
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
        _accepted_fact(
            "realization_recorded",
            {
                "lot_id": "LOSS-LOT",
                "account_id": "ACC-1",
                "security_id": "NVDA",
                "realized_gain_loss": "-50.00",
                "realized_at": "2026-04-15T10:00:00Z",
            },
            "2026-04-16T10:03:00Z",
        ),
    )
    snapshot, _ = build_tax_snapshot_v1(
        scope_id="filing:joint",
        as_of_effective_at="2026-04-16T16:00:00Z",
        facts_included_through_recorded_at="2026-04-16T10:03:00Z",
        accepted_facts=facts,
        policy_dependency_version_set={"policy_registry": "v1"},
    )
    policy_registry = build_tax_policy_registry_v1(scope_id="filing:joint")
    resolved = resolve_tax_policy_set_v1(
        policy_registry=policy_registry,
        scope_id="filing:joint",
        account_tax_regime="taxable_brokerage",
    )
    buy_decision, _ = preview_buy_tax_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={
            "account_id": "ACC-1",
            "security_id": "NVDA",
            "quantity": "1",
            "effective_at": "2026-04-16T16:00:00Z",
        },
    )

    assert buy_decision["decision_status"] == "hard_blocked"
    assert buy_decision["reason_codes"] == ["TAX_WASH_RISK_BLOCK"]

    sell_decision, _ = preview_sell_tax_decision_v1(
        snapshot=snapshot,
        resolved_policy_set=resolved,
        request={"account_id": "ACC-1", "security_id": "NVDA", "quantity": "1"},
    )
    replay_report = build_decision_replay_report_v1(decision=sell_decision, replayed_decision=sell_decision)
    explanation = build_tax_advisory_explanation_v1(decision=sell_decision)
    realized_report = build_realized_tax_report_v1(snapshot=snapshot, accepted_facts=facts)
    broker_report = build_broker_tax_reconciliation_report_v1(scope_id="filing:joint")

    assert replay_report["replayed_equal"] is True
    assert explanation["decision_id"] == sell_decision["decision_id"]
    assert len(realized_report["realizations"]) == 1
    assert broker_report["status"] == "scaffold"

    journal_entry = build_tax_decision_journal_entry_v1(sell_decision)
    journal_path = tax_jsonl_path_v1(
        output_root=tmp_path,
        family="tax_decision_v1",
        scope_id="filing:joint",
        filename="tax_decision_journal.v1.jsonl",
    )
    result = append_validated_jsonl_v1(
        path=journal_path,
        payloads=(journal_entry,),
        schema_relpath="governance/04_DATA/SCHEMAS/C2/TAX/tax_decision_journal.v1.schema.json",
    )
    assert result["appended_count"] == 1
    rows = [json.loads(line) for line in journal_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rows[0]["decision_id"] == sell_decision["decision_id"]
