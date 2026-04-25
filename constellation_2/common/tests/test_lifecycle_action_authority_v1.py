from __future__ import annotations

import inspect
from pathlib import Path

from constellation_2.common.lifecycle_action_authority_v1 import (
    load_core2_trade_bundle_v1,
    materialize_lifecycle_action_authority_set_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_validated_json_v1,
    read_json_object_v1,
)


DAY_UTC = "2026-04-11"
MATERIALIZATION_SET_ID = "b" * 64
TRADE_ID = "a" * 64


def _write_core2_trade(
    root: Path,
    *,
    trade_id: str = TRADE_ID,
    ownership_classification: str = "CONSTELLATION_OWNED",
    ambiguity_state: str = "NONE",
    blocker_state: str = "CLEAR",
    reconciliation_state: str = "TRUSTED",
    freshness_status: str = "FRESH",
    ambiguity_status: str = "CLEAR",
    insufficient_status: str = "SUFFICIENT",
    downstream_action_posture: str = "SAFE",
    lifecycle_status: str = "OPEN_LONG",
    protection_status: str = "PROTECTION_NOT_OBSERVED",
    description_downstream_posture: str = "SAFE",
    description_reconciliation_status: str = "TRUSTED",
    current_quantity: str = "10",
    side: str = "LONG",
    orphan_count: int = 0,
    current_working_orders: list[dict] | None = None,
) -> Path:
    trade_dir = root / "reconciled_trade_state_v1" / "materializations" / DAY_UTC / MATERIALIZATION_SET_ID / "trades" / trade_id
    trade_dir.mkdir(parents=True, exist_ok=True)
    evaluation_utc = f"{DAY_UTC}T14:30:00Z"

    identity_ref = atomic_write_validated_json_v1(
        path=trade_dir / "trade_identity.v1.json",
        payload={
            "schema_id": "trade_identity",
            "schema_version": "v1",
            "authority_owner": "trade_identity_resolution_v1",
            "materialization_set_id": MATERIALIZATION_SET_ID,
            "day_utc": DAY_UTC,
            "evaluation_utc": evaluation_utc,
            "trade_identity_id": trade_id,
            "environment": "PAPER",
            "sleeve_id": "PRIMARY",
            "account_id": "DUO847203",
            "instrument_identity": {
                "symbol": "SPY",
                "sec_type": "STK",
                "exchange": "SMART",
                "currency": "USD",
                "raw_summary": "SPY-STK-SMART-USD",
            },
            "ownership_classification": ownership_classification,
            "ownership_reason_codes": ["TEST_CASE"],
            "lineage_attachment_refs": {
                "order_ids": ["101"],
                "perm_ids": ["555001"],
                "execution_ids": ["E-PRIMARY-0001"],
                "fact_record_ids": ["1" * 64],
            },
            "open_close_continuity": {
                "continuity_key": "SPY:PRIMARY:PAPER",
                "segment_index": 1,
                "continuity_status": "OPEN",
                "opened_by_fact_record_id": "1" * 64,
                "closed_by_fact_record_id": "",
            },
            "ambiguity_state": ambiguity_state,
            "blocker_state": blocker_state,
            "blocker_codes": [],
            "derived_only": False,
        },
        schema_relpath="governance/04_DATA/SCHEMAS/C2/TRADE_STATE/trade_identity.v1.schema.json",
    )
    incorporated_ref = atomic_write_validated_json_v1(
        path=trade_dir / "incorporated_broker_trade_state.v1.json",
        payload={
            "schema_id": "incorporated_broker_trade_state",
            "schema_version": "v1",
            "authority_owner": "incorporated_broker_trade_state_v1",
            "truth_owner_status": "CANONICAL_CURRENT_TRUTH_OWNER",
            "materialization_set_id": MATERIALIZATION_SET_ID,
            "day_utc": DAY_UTC,
            "evaluation_utc": evaluation_utc,
            "trade_identity_ref": {
                "trade_identity_id": trade_id,
                "artifact_path": str(identity_ref.path),
                "artifact_sha256": str(identity_ref.sha256),
            },
            "environment": "PAPER",
            "sleeve_id": "PRIMARY",
            "account_id": "DUO847203",
            "instrument_identity": {
                "symbol": "SPY",
                "sec_type": "STK",
                "exchange": "SMART",
                "currency": "USD",
                "raw_summary": "SPY-STK-SMART-USD",
            },
            "ownership_classification": ownership_classification,
            "current_quantity": current_quantity,
            "side": side,
            "average_cost": "500.30",
            "incorporated_fills": [
                {
                    "execution_id": "E-PRIMARY-0001",
                    "order_id": "101",
                    "perm_id": "555001",
                    "fill_quantity": "10",
                    "fill_price": "500.30",
                    "side": "BOT",
                    "commission": "1.25",
                    "currency": "USD",
                    "observed_utc": evaluation_utc,
                    "fact_record_ids": ["2" * 64],
                }
            ],
            "current_working_orders": current_working_orders or [],
            "terminal_order_lineage": [],
            "orphan_order_facts": [
                {
                    "order_key": f"ORPHAN-{index}",
                    "fact_record_id": f"{index + 3}" * 64,
                    "reason_code": "ORPHAN_ORDER_FACT",
                }
                for index in range(orphan_count)
            ],
            "last_reconciled_fact_boundary": {
                "max_journal_sequence_number": 10,
                "latest_observed_utc": evaluation_utc,
                "raw_journal_path": "/tmp/nonexistent_raw_journal.jsonl",
                "raw_journal_sha256": "3" * 64,
            },
            "drift_basis": {
                "position_quantity": current_quantity,
                "net_fill_quantity": current_quantity,
                "position_vs_fill_consistent": True,
                "mismatch_quantity": "0",
                "reason_codes": [],
            },
            "freshness_basis": {
                "core1_downstream_trust_verdict": "TRUSTED",
                "core1_freshness_status": freshness_status,
                "evaluation_utc": evaluation_utc,
                "latest_observed_utc": evaluation_utc,
                "age_seconds": 30,
            },
            "first_blocker": "NONE",
            "ambiguity_state": ambiguity_state,
            "blocker_codes": [],
            "degraded_codes": [],
            "last_successful_reconciliation_utc": evaluation_utc,
            "upstream_core1_evidence_refs": {
                "raw_journal_ref": {"artifact_path": "/tmp/does-not-exist.jsonl", "artifact_sha256": "4" * 64},
                "fact_ledger_refs": {
                    "observed_order_fact": {"artifact_path": "/tmp/order.jsonl", "artifact_sha256": "5" * 64},
                    "observed_order_status_fact": {"artifact_path": "/tmp/order_status.jsonl", "artifact_sha256": "6" * 64},
                    "observed_fill_fact": {"artifact_path": "/tmp/fill.jsonl", "artifact_sha256": "7" * 64},
                    "observed_position_fact": {"artifact_path": "/tmp/position.jsonl", "artifact_sha256": "8" * 64},
                },
                "health_ref": {"artifact_path": "/tmp/health.json", "artifact_sha256": "9" * 64},
            },
            "derived_only": False,
        },
        schema_relpath="governance/04_DATA/SCHEMAS/C2/TRADE_STATE/incorporated_broker_trade_state.v1.schema.json",
    )
    atomic_write_validated_json_v1(
        path=trade_dir / "reconciled_trade_description.v1.json",
        payload={
            "schema_id": "reconciled_trade_description",
            "schema_version": "v1",
            "authority_owner": "reconciled_trade_description_v1",
            "materialization_set_id": MATERIALIZATION_SET_ID,
            "day_utc": DAY_UTC,
            "evaluation_utc": evaluation_utc,
            "trade_identity_id": trade_id,
            "incorporated_state_ref": {
                "artifact_path": str(incorporated_ref.path),
                "artifact_sha256": str(incorporated_ref.sha256),
            },
            "lifecycle_status": lifecycle_status,
            "protection_status": protection_status,
            "reconciliation_descriptive_status": description_reconciliation_status,
            "downstream_posture": description_downstream_posture,
            "derived_from_incorporated_state": True,
            "derived_only": True,
        },
        schema_relpath="governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciled_trade_description.v1.schema.json",
    )
    atomic_write_validated_json_v1(
        path=trade_dir / "reconciliation_health.v1.json",
        payload={
            "schema_id": "reconciliation_health",
            "schema_version": "v1",
            "authority_owner": "reconciliation_health_v1",
            "materialization_set_id": MATERIALIZATION_SET_ID,
            "day_utc": DAY_UTC,
            "evaluation_utc": evaluation_utc,
            "trade_identity_id": trade_id,
            "incorporated_state_ref": {
                "artifact_path": str(incorporated_ref.path),
                "artifact_sha256": str(incorporated_ref.sha256),
            },
            "current_state": reconciliation_state,
            "blocker_reasons": [],
            "degraded_reasons": [],
            "freshness_status": freshness_status,
            "ambiguity_status": ambiguity_status,
            "drift_status": "CLEAR",
            "insufficient_evidence_status": insufficient_status,
            "downstream_action_posture": downstream_action_posture,
            "derived_only": True,
        },
        schema_relpath="governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciliation_health.v1.schema.json",
    )
    atomic_write_validated_json_v1(
        path=trade_dir / "reconciliation_provenance.v1.json",
        payload={
            "schema_id": "reconciliation_provenance",
            "schema_version": "v1",
            "authority_owner": "reconciliation_provenance_v1",
            "materialization_set_id": MATERIALIZATION_SET_ID,
            "day_utc": DAY_UTC,
            "evaluation_utc": evaluation_utc,
            "trade_identity_id": trade_id,
            "incorporated_state_ref": {
                "artifact_path": str(incorporated_ref.path),
                "artifact_sha256": str(incorporated_ref.sha256),
            },
            "rule_versions": {
                "core2_rule_pack": "C2_CORE2_RULE_PACK_V1",
                "identity_rules": "C2_TRADE_IDENTITY_RULES_V1",
                "description_rules": "C2_RECONCILED_DESCRIPTION_RULES_V1",
                "health_rules": "C2_RECONCILIATION_HEALTH_RULES_V1",
            },
            "upstream_core1_evidence_refs": {
                "raw_journal_ref": {"artifact_path": "/tmp/does-not-exist.jsonl", "artifact_sha256": "4" * 64},
                "fact_ledger_refs": {
                    "observed_order_fact": {"artifact_path": "/tmp/order.jsonl", "artifact_sha256": "5" * 64},
                    "observed_order_status_fact": {"artifact_path": "/tmp/order_status.jsonl", "artifact_sha256": "6" * 64},
                    "observed_fill_fact": {"artifact_path": "/tmp/fill.jsonl", "artifact_sha256": "7" * 64},
                    "observed_position_fact": {"artifact_path": "/tmp/position.jsonl", "artifact_sha256": "8" * 64},
                },
                "health_ref": {"artifact_path": "/tmp/health.json", "artifact_sha256": "9" * 64},
            },
            "incorporated_fact_refs": [
                {
                    "schema_id": "observed_fill_fact",
                    "fact_record_id": "2" * 64,
                    "canonical_event_identity": "c" * 64,
                    "reason_code": "INCORPORATED",
                }
            ],
            "ignored_fact_refs": [],
            "blocked_fact_refs": [],
            "prior_state_change_summary": {
                "change_status": "INITIAL_MATERIALIZATION",
                "prior_state_ref": {"artifact_path": "", "artifact_sha256": ""},
                "changed_fields": ["current_quantity"],
                "summary": "Initial test bundle.",
            },
            "derived_only": True,
        },
        schema_relpath="governance/04_DATA/SCHEMAS/C2/TRADE_STATE/reconciliation_provenance.v1.schema.json",
    )
    return trade_dir.parent.parent


def test_lifecycle_action_authority_actionable_and_protection_required(tmp_path: Path) -> None:
    materialization_dir = _write_core2_trade(tmp_path, orphan_count=1)
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    result = materialize_lifecycle_action_authority_set_v1(
        core2_materialization_dir=materialization_dir,
        execution_root=execution_root,
        evaluated_at_utc=f"{DAY_UTC}T14:31:00Z",
    )
    trade = result["trade_results"][0]
    gate = read_json_object_v1(trade["gate"].path)
    candidates = read_json_object_v1(trade["candidates"].path)
    authority = read_json_object_v1(trade["authority"].path)
    provenance = read_json_object_v1(trade["provenance"].path)

    assert gate["gate_verdict"] == "ACTIONABLE"
    assert {row["action_code"] for row in candidates["candidate_rows"]} >= {"HOLD", "ADD_INITIAL_PROTECTION", "CLOSE_POSITION", "CANCEL_ORPHAN_CHILD"}
    assert authority["final_action_posture"] == "ACTION_REQUIRED"
    assert authority["required_actions"] == ["ADD_INITIAL_PROTECTION", "CANCEL_ORPHAN_CHILD"]
    assert authority["allowed_actions"] == ["HOLD"]
    assert authority["action_safety_posture"] == "ACTIONABLE"
    assert provenance["candidate_actions_generated"]
    assert provenance["core2_fields_read"]


def test_lifecycle_action_authority_degraded_requires_review(tmp_path: Path) -> None:
    materialization_dir = _write_core2_trade(
        tmp_path,
        reconciliation_state="DEGRADED",
        freshness_status="STALE",
        downstream_action_posture="DEGRADED",
        description_downstream_posture="DEGRADED",
    )
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    result = materialize_lifecycle_action_authority_set_v1(
        core2_materialization_dir=materialization_dir,
        execution_root=execution_root,
        evaluated_at_utc=f"{DAY_UTC}T14:31:00Z",
    )
    authority = read_json_object_v1(result["trade_results"][0]["authority"].path)
    gate = read_json_object_v1(result["trade_results"][0]["gate"].path)
    assert gate["gate_verdict"] == "DEGRADED_REVIEW_REQUIRED"
    assert authority["final_action_posture"] == "REVIEW_REQUIRED"
    assert authority["required_actions"] == ["OPERATOR_REVIEW_REQUIRED"]
    assert authority["allowed_actions"] == ["HOLD"]


def test_lifecycle_action_authority_blocked_for_ambiguous_ownership(tmp_path: Path) -> None:
    materialization_dir = _write_core2_trade(
        tmp_path,
        ownership_classification="AMBIGUOUS_OWNERSHIP",
        ambiguity_state="OWNERSHIP_AMBIGUOUS",
        ambiguity_status="AMBIGUOUS",
    )
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    result = materialize_lifecycle_action_authority_set_v1(
        core2_materialization_dir=materialization_dir,
        execution_root=execution_root,
        evaluated_at_utc=f"{DAY_UTC}T14:31:00Z",
    )
    authority = read_json_object_v1(result["trade_results"][0]["authority"].path)
    gate = read_json_object_v1(result["trade_results"][0]["gate"].path)
    assert gate["gate_verdict"] == "BLOCKED"
    assert authority["final_action_posture"] == "BLOCKED"
    assert "CLOSE_POSITION" in authority["blocked_actions"]


def test_close_beats_reduce_and_provenance_records_rejection(tmp_path: Path) -> None:
    materialization_dir = _write_core2_trade(
        tmp_path,
        protection_status="PROTECTION_WORKING_PRESENT",
        lifecycle_status="OPEN_LONG_WITH_WORKING_EXIT",
        current_working_orders=[
            {
                "order_key": "REDUCE-1",
                "order_id": "101",
                "perm_id": "555001",
                "status": "Submitted",
                "action": "SELL",
                "total_quantity": "3",
                "filled_quantity": "0",
                "remaining_quantity": "3",
                "observed_utc": f"{DAY_UTC}T14:30:00Z",
                "fact_record_ids": ["d" * 64],
            }
        ],
    )
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    result = materialize_lifecycle_action_authority_set_v1(
        core2_materialization_dir=materialization_dir,
        execution_root=execution_root,
        evaluated_at_utc=f"{DAY_UTC}T14:31:00Z",
    )
    authority = read_json_object_v1(result["trade_results"][0]["authority"].path)
    provenance = read_json_object_v1(result["trade_results"][0]["provenance"].path)
    assert authority["final_action_posture"] == "ACTION_ALLOWED"
    assert authority["allowed_actions"] == ["HOLD", "CLOSE_POSITION"]
    assert "REDUCE_POSITION" in authority["blocked_actions"]
    assert any(row["action_code"] == "REDUCE_POSITION" for row in provenance["rejected_candidates"])


def test_operator_surface_and_no_raw_core1_dependency(tmp_path: Path) -> None:
    materialization_dir = _write_core2_trade(tmp_path)
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    result = materialize_lifecycle_action_authority_set_v1(
        core2_materialization_dir=materialization_dir,
        execution_root=execution_root,
        evaluated_at_utc=f"{DAY_UTC}T14:31:00Z",
    )
    surface = read_json_object_v1(result["operator_surface"].path)
    assert surface["health_summary"]["trades_total"] == 1
    assert surface["trade_action_dossiers"][0]["trade_identity_id"] == TRADE_ID


def test_boundary_no_transmission_logic_imported() -> None:
    import constellation_2.common.lifecycle_action_authority_v1 as module

    source = inspect.getsource(module)
    assert "submit_boundary" not in source
    assert "ibapi" not in source


def test_loader_uses_core2_bundle_only(tmp_path: Path) -> None:
    materialization_dir = _write_core2_trade(tmp_path)
    trade_dir = materialization_dir / "trades" / TRADE_ID
    bundle = load_core2_trade_bundle_v1(trade_dir)
    assert bundle.trade_identity.payload["schema_id"] == "trade_identity"
    assert bundle.reconciliation_provenance.payload["schema_id"] == "reconciliation_provenance"
