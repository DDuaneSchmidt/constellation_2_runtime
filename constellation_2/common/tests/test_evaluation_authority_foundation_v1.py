from __future__ import annotations

import hashlib
import json
from pathlib import Path

import constellation_2.common.evaluation_authority_foundation_v1 as foundation
import constellation_2.common.sleeve_edge_measurement_v1 as sleeve_edge


DAY = "2026-04-16"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _bootstrap_truth_roots(tmp_path: Path) -> tuple[Path, Path]:
    truth_root = tmp_path / "truth"
    execution_root = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    truth_root.mkdir(parents=True, exist_ok=True)
    execution_root.mkdir(parents=True, exist_ok=True)
    return truth_root, execution_root


def _write_day_open_attempt(truth_root: Path, *, include_runtime_lifecycle_ref: bool) -> None:
    payload = {
        "schema_id": "C2_DAY_OPEN_ATTEMPT_V1",
        "schema_version": "v1",
        "day_utc": DAY,
    }
    if include_runtime_lifecycle_ref:
        payload["runtime_lifecycle_ref"] = {
            "run_id": "20260418T150000Z__c2_paper_day_orchestrator_service__pid5150",
            "runtime_identity_contract_path": "/tmp/active_runtime_contract.v1.json",
            "runtime_identity_contract_sha256": "a" * 64,
            "startup_identity_receipt_path": "/tmp/runtime_startup_identity.v1.json",
            "lifecycle_start_receipt_path": "/tmp/runtime_lifecycle_receipt.v1.json",
        }
    _write_json(
        truth_root / "reports" / "day_open_attempt_v1" / DAY / "day_open_attempt.v1.json",
        payload,
    )


def _write_execution_reconciliation(truth_root: Path) -> None:
    _write_json(
        truth_root / "reports" / "execution_reconciliation_v1" / DAY / "execution_reconciliation.v1.json",
        {
            "schema_id": "C2_EXECUTION_RECONCILIATION_V1",
            "schema_version": "v1",
            "day_utc": DAY,
        },
    )


def _write_reconciliation_report(execution_root: Path) -> None:
    _write_json(
        execution_root / "reports" / "reconciliation_report_v3" / DAY / "reconciliation_report.v3.json",
        {
            "schema_id": "C2_RECONCILIATION_REPORT_V3",
            "schema_version": "v3",
            "day_utc": DAY,
        },
    )


def _write_capital_authority_allocation(execution_root: Path, *, sleeve_id: str, action_state: str = "continue") -> None:
    _write_json(
        execution_root / "allocation_v1" / "capital_authority_allocation_v1" / DAY / "capital_authority_allocation.v1.json",
        {
            "schema_id": "capital_authority_allocation",
            "schema_version": "v1",
            "day_utc": DAY,
            "governed_evaluation_control_state": {
                "sleeve_controls": [
                    {
                        "scope_id": sleeve_id,
                        "adoption_state": "ADOPTED",
                        "action_state": action_state,
                        "control_state": "allow" if action_state == "continue" else "throttle",
                        "headroom_multiplier_bp": 10000 if action_state == "continue" else 5000,
                        "diagnostic": "test_control_row",
                    }
                ]
            },
        },
    )


def _sleeve_fact_ledger_payload(*, sleeve_id: str) -> dict:
    trade_ids = ["1" * 64, "2" * 64, "3" * 64, "4" * 64]
    return {
        "schema_id": "C2_SLEEVE_EDGE_FACT_LEDGER_V1",
        "schema_version": "v1",
        "produced_utc": f"{DAY}T20:00:00Z",
        "day_utc": DAY,
        "as_of_ts": f"{DAY}T20:00:00Z",
        "sleeve_id": sleeve_id,
        "strategy_family": sleeve_id,
        "source_execution_sleeve_id": "PRIMARY",
        "source_execution_root_path": "/tmp/truth_sleeves/PRIMARY/PAPER",
        "calculation_version": "sleeve_edge_measurement_v1",
        "core2_materialization_set_id": "a" * 64,
        "engine_ids": ["engine-1"],
        "fact_input_hash": "b" * 64,
        "input_manifest": [
            {
                "type": "reconciled_trade_state_summary",
                "path": "/tmp/reconciled_trade_state_summary.v1.json",
                "sha256": "e" * 64,
                "day_utc": DAY,
                "producer": "test",
            },
            {
                "type": "reconciled_trade_state_summary",
                "path": "/tmp/reconciled_trade_state_summary_2.v1.json",
                "sha256": "f" * 64,
                "day_utc": DAY,
                "producer": "test",
            },
        ],
        "trade_facts": [],
        "included_trade_ids": trade_ids,
        "excluded_trade_ids": [],
        "exclusion_details": [],
        "invalidity_reasons": [],
        "producer": {"repo": "constellation", "git_sha": "a" * 40, "module": "test"},
    }


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sleeve_snapshot_payload(
    *,
    sleeve_id: str,
    fact_ledger_path: Path,
    fact_ledger_sha256: str,
    fact_input_hash: str,
    sample_count: int = 4,
) -> dict:
    trade_ids = ["1" * 64, "2" * 64, "3" * 64, "4" * 64]
    return {
        "schema_id": "C2_SLEEVE_EDGE_SNAPSHOT_V1",
        "schema_version": "v1",
        "snapshot_id": "c" * 64,
        "produced_utc": f"{DAY}T20:00:00Z",
        "day_utc": DAY,
        "as_of_ts": f"{DAY}T20:00:00Z",
        "sleeve_id": sleeve_id,
        "strategy_family": sleeve_id,
        "metric_window": {
            "sample_basis": "CLOSED_TRADES_ONLY",
            "recent_trade_count": sample_count,
            "baseline_trade_count": sample_count,
        },
        "included_trade_ids": trade_ids,
        "excluded_trade_ids": [],
        "exclusion_details": [],
        "fact_input_hash": fact_input_hash,
        "calculation_version": "sleeve_edge_measurement_v1",
        "policy_version": "v1",
        "fact_ledger_ref": {
            "artifact_path": str(fact_ledger_path.resolve()),
            "artifact_sha256": fact_ledger_sha256,
        },
        "factual_metrics": {
            "native_trade_count": sample_count,
            "native_net_pnl": "120.00",
            "native_gross_pnl": "123.00",
            "native_net_expectancy": "30.00",
            "native_expectancy_per_unit_risk": {"status": "UNAVAILABLE", "value": None, "reason_codes": ["X"]},
            "native_realized_drawdown": "-0.010000",
            "native_capital_efficiency": {"status": "UNAVAILABLE", "value": None, "reason_codes": ["X"]},
            "native_budget_utilization": {"status": "UNAVAILABLE", "value": None, "reason_codes": ["X"]},
            "native_recent_vs_baseline_drift": {"status": "UNAVAILABLE", "value": None, "reason_codes": ["X"]},
            "adopted_trade_count": 0,
            "adopted_net_pnl": "0",
            "adopted_management_expectancy": "0",
            "adopted_drawdown": "0",
            "adopted_capital_efficiency": {"status": "UNAVAILABLE", "value": None, "reason_codes": ["X"]},
            "fee_drag": "1.50",
            "measured_slippage_drag": {"status": "AVAILABLE", "value": "0.50", "reason_codes": []},
            "execution_data_completeness": {
                "fill_price_complete": True,
                "fee_complete": True,
                "decision_price_complete": True,
                "state": "COMPLETE",
            },
            "unknown_attribution_count": 0,
            "sample_count": sample_count,
            "window_coverage": {
                "recent_required_trade_count": sample_count,
                "recent_available_trade_count": sample_count,
                "baseline_required_trade_count": sample_count,
                "baseline_available_trade_count": sample_count,
            },
            "required_inputs_complete": True,
            "invalidity_reasons": [],
        },
        "unavailable_metrics": [],
        "qualification": {
            "edge_band": "QUALIFIED_POSITIVE",
            "execution_health_band": "ACCEPTABLE",
            "sample_sufficiency_band": "SUFFICIENT",
            "drift_band": "STABLE",
            "qualification_state": "QUALIFIED",
            "reason_codes": [],
        },
        "reason_codes": [],
        "snapshot_lineage": {
            "core2_materialization_set_id": "a" * 64,
            "source_execution_sleeve_id": "PRIMARY",
            "previous_snapshot_id": None,
            "revision_type": "INITIAL_PUBLISH",
            "revision_reason": "",
            "prior_snapshot_ref": {"artifact_path": "", "snapshot_id": "", "qualification_state": ""},
            "state_transition_reason_codes": [],
        },
        "input_manifest": [
            {
                "type": "reconciled_trade_state_summary",
                "path": str((fact_ledger_path.parent / "reconciled_trade_state_summary.v1.json").resolve()),
                "sha256": "e" * 64,
                "day_utc": DAY,
                "producer": "test",
            },
            {
                "type": "qualification_policy_manifest",
                "path": str((fact_ledger_path.parent / "qualification_policy_manifest.v1.json").resolve()),
                "sha256": "f" * 64,
                "day_utc": DAY,
                "producer": "test",
            },
        ],
        "producer": {"repo": "constellation", "git_sha": "a" * 40, "module": "test"},
    }


def _write_sleeve_measurement_inputs(execution_root: Path, *, sleeve_id: str) -> None:
    fact_path = execution_root / "reports" / "sleeve_edge_fact_ledger_v1" / DAY / sleeve_id / ("f" * 64) / "sleeve_edge_fact_ledger.v1.json"
    snapshot_path = execution_root / "reports" / "sleeve_edge_snapshot_v1" / DAY / sleeve_id / ("f" * 64) / "sleeve_edge_snapshot.v1.json"
    fact_payload = _sleeve_fact_ledger_payload(sleeve_id=sleeve_id)
    fact_payload["fact_input_hash"] = sleeve_edge._fact_input_hash_from_fact_ledger(fact_payload)
    _write_json(fact_path, fact_payload)
    _write_json(
        snapshot_path,
        _sleeve_snapshot_payload(
            sleeve_id=sleeve_id,
            fact_ledger_path=fact_path,
            fact_ledger_sha256=_sha256_file(fact_path),
            fact_input_hash=str(fact_payload["fact_input_hash"]),
        ),
    )


def _patch_snapshot_reader(monkeypatch, execution_root: Path, *, sleeve_id: str) -> None:
    snapshot_path = execution_root / "reports" / "sleeve_edge_snapshot_v1" / DAY / sleeve_id / ("f" * 64) / "sleeve_edge_snapshot.v1.json"

    def _reader(*, truth_root: Path, sleeve_id: str, day_utc: str, expected_policy_version: str) -> dict:
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        payload["artifact_path"] = str(snapshot_path.resolve())
        payload["artifact_sha256"] = _sha256_file(snapshot_path)
        return payload

    monkeypatch.setattr(foundation, "read_sleeve_edge_snapshot_for_day_v1", _reader)


def test_materialize_evaluation_authority_slice_emits_linked_artifacts(monkeypatch, tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    sleeve_id = "C2_TREND_EQ_PRIMARY"
    _write_day_open_attempt(truth_root, include_runtime_lifecycle_ref=True)
    _write_execution_reconciliation(truth_root)
    _write_reconciliation_report(execution_root)
    _write_capital_authority_allocation(execution_root, sleeve_id=sleeve_id)
    _write_sleeve_measurement_inputs(execution_root, sleeve_id=sleeve_id)
    _patch_snapshot_reader(monkeypatch, execution_root, sleeve_id=sleeve_id)

    result = foundation.materialize_evaluation_authority_slice_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_id=sleeve_id,
    )

    manifest = json.loads(Path(result["evaluation_input_manifest_ref"]["path"]).read_text(encoding="utf-8"))
    attribution = json.loads(Path(result["outcome_attribution_ref"]["path"]).read_text(encoding="utf-8"))
    measurement = json.loads(Path(result["sleeve_edge_measurement_ref"]["path"]).read_text(encoding="utf-8"))
    allocation = json.loads(Path(result["allocation_governance_ref"]["path"]).read_text(encoding="utf-8"))
    operative = json.loads(Path(result["operative_control_ref"]["path"]).read_text(encoding="utf-8"))

    assert manifest["runtime_lifecycle_ref"]["run_id"] == "20260418T150000Z__c2_paper_day_orchestrator_service__pid5150"
    assert [row["artifact_id"] for row in manifest["source_evidence_refs"]][:2] == [
        "day_open_attempt_v1",
        "execution_reconciliation_v1",
    ]
    assert [
        row["artifact_id"] for row in manifest["constitutional_dependency_declaration"]["dependency_refs"]
    ] == [
        "reconciliation_report_v3",
        "sleeve_edge_fact_ledger_v1",
        "sleeve_edge_snapshot_v1",
        "capital_authority_allocation_v1",
        "evaluation_policy_snapshot_v1",
    ]
    assert attribution["evaluation_input_manifest_ref"]["path"] == result["evaluation_input_manifest_ref"]["path"]
    assert measurement["outcome_attribution_ref"]["path"] == result["outcome_attribution_ref"]["path"]
    assert allocation["measurement_ref"]["path"] == result["sleeve_edge_measurement_ref"]["path"]
    assert operative["allocation_governance_ref"]["path"] == result["allocation_governance_ref"]["path"]
    assert operative["state_class"] == "ACTIVE"
    assert operative["binding_state"] == "NOT_BOUND_LIVE"


def test_operator_override_promotes_operative_control_state_to_override(monkeypatch, tmp_path: Path) -> None:
    truth_root, execution_root = _bootstrap_truth_roots(tmp_path)
    sleeve_id = "C2_TREND_EQ_PRIMARY"
    _write_day_open_attempt(truth_root, include_runtime_lifecycle_ref=False)
    _write_execution_reconciliation(truth_root)
    _write_reconciliation_report(execution_root)
    _write_capital_authority_allocation(execution_root, sleeve_id=sleeve_id, action_state="reduce")
    _write_sleeve_measurement_inputs(execution_root, sleeve_id=sleeve_id)
    _patch_snapshot_reader(monkeypatch, execution_root, sleeve_id=sleeve_id)

    override_path = execution_root / "reports" / "operator_intervention_state_v1" / DAY / "override" / "operator_intervention_state.v1.json"
    _write_json(
        override_path,
        {
            "override_status": "ACTIVE",
            "human_decision_class": "REQUEST_REEVALUATION",
        },
    )
    monkeypatch.setattr(foundation, "validate_against_repo_schema_v1", lambda *_args, **_kwargs: None)

    result = foundation.materialize_evaluation_authority_slice_v1(
        truth_root=truth_root,
        day_utc=DAY,
        execution_sleeve_id="PRIMARY",
        mode="PAPER",
        sleeve_id=sleeve_id,
        operator_intervention_state_path=override_path,
    )

    operative = json.loads(Path(result["operative_control_ref"]["path"]).read_text(encoding="utf-8"))
    manifest = json.loads(Path(result["evaluation_input_manifest_ref"]["path"]).read_text(encoding="utf-8"))

    assert "runtime_lifecycle_ref" not in manifest
    assert operative["state_class"] == "OVERRIDE"
    assert operative["operator_override_ref"]["path"] == str(override_path.resolve())
