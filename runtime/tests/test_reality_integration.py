from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPO_ROOT = Path("/home/node/constellation")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from runtime.governance_verification import verification_api as gv_api
from runtime.meta_governance import api as mg_api
from runtime.meta_governance.audit_log import read_canonical_audit_stream
from runtime.meta_governance.interpreter_version import get_interpreter_version
from runtime.meta_governance.predicate_gates import REQUIRED_PREDICATES
from runtime.meta_governance.schemas import utc_now
from runtime.meta_governance.store import ArtifactStore
from runtime.meta_governance.types import ActivationSnapshot, ApprovalDecision, PredicateEvaluation
from runtime.reality_integration import reconciliation_api as ri_api


def _policy() -> dict:
    return {
        "version": "v1",
        "weights": {"risk": 0.7, "tax": 0.1, "allocation": 0.2},
        "limits": {"max_actions_per_cycle": 10, "min_decision_score": 0.5},
        "cooldown": {"enabled": False, "block_same_action": False, "min_seconds_between_same_action": 60},
        "autonomy": {"allowed_actions": ["EXIT"]},
        "execution": {"allowed_scopes": ["position"]},
    }


def _append_runtime_event(store: ArtifactStore, *, timestamp: str, event: dict) -> None:
    path = store.stream_path("governed_audit")
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"stream_type": "runtime_event", "timestamp": timestamp, "ts_epoch": None, "event_id": f"runtime-{timestamp}", "event": event}, sort_keys=True) + "\n")


def _write_approval(store: ArtifactStore, proposal_id: str = "seed-proposal") -> str:
    approval = ApprovalDecision(
        proposal_id=proposal_id,
        required_approval_level="human_critical",
        approved_by="human.reviewer",
        approved_at="2026-01-01T00:00:00Z",
        reviewed_artifact_hashes=("artifact",),
        notes="seed approval",
    )
    store.write_immutable("approvals", proposal_id, approval, artifact_type="ApprovalDecision", created_at="2026-01-01T00:00:00Z")
    return proposal_id


def _write_predicates(store: ArtifactStore, proposal_id: str = "seed-proposal", artifact_id: str = "seed-eval") -> str:
    evaluations = [
        PredicateEvaluation(
            proposal_id=proposal_id,
            predicate_id=predicate_id,
            result=True,
            evidence_refs=(proposal_id,),
        )
        for predicate_id in REQUIRED_PREDICATES
    ]
    store.write_immutable(
        "evaluation_artifacts",
        artifact_id,
        {"proposal_id": proposal_id, "evaluations": evaluations},
        artifact_type="PredicateEvaluations",
        created_at="2026-01-01T00:00:00Z",
    )
    return artifact_id


def _write_root_snapshot(store: ArtifactStore, graph_id: str, graph_hash: str, approval_ref: str, evaluation_ref: str) -> str:
    snapshot = ActivationSnapshot(
        snapshot_id="root-snapshot",
        graph_id=graph_id,
        graph_hash=graph_hash,
        interpreter_version=get_interpreter_version(),
        active_at="2026-01-01T00:00:00Z",
        activated_by="system",
        activation_scope="root",
        approval_refs=(approval_ref,),
        evaluation_refs=(evaluation_ref,),
        rollback_snapshot_ref="root-snapshot",
        prior_snapshot_id=None,
        verification_refs=(),
        lifecycle_state="approved",
    )
    store.write_immutable("activation_snapshots", snapshot.snapshot_id, snapshot, artifact_type="ActivationSnapshot", created_at="2026-01-01T00:00:00Z")
    return snapshot.snapshot_id


def _build_active_runtime(tmp_path: Path) -> tuple[Path, dict]:
    store_root = tmp_path / "store"
    store = ArtifactStore(store_root)
    mg_api.register_module(
        module_id="runtime_policy",
        tier="tier_2",
        semantic_domain="risk",
        version="v1",
        module_content=_policy(),
        store_root=store_root,
    )
    mg_api.create_proposal(
        proposal_id="seed-proposal",
        target_type="module",
        target_id="runtime_policy",
        target_tier="tier_2",
        from_version="v0",
        to_version="v1",
        authored_by="tester",
        justification="seed governed runtime fixture",
        hypothesis="establish active runtime authority",
        expected_benefit="deterministic test baseline",
        worst_case_downside="invalid fixture lineage",
        reversal_criteria="rollback to root snapshot",
        review_deadline="2026-12-31T00:00:00Z",
        dependency_impact={"modules": ["runtime_policy"]},
        proposed_scope={"domains": ["risk"]},
        store_root=store_root,
    )
    graph_id = mg_api.compile_canonical_graph(active_modules=[("runtime_policy", "v1")], store_root=store_root)
    graph = store.read("canonical_graphs", graph_id)
    approval_ref = _write_approval(store)
    evaluation_ref = _write_predicates(store)
    root_snapshot_id = _write_root_snapshot(store, graph_id, graph["record"]["graph_hash"], approval_ref, evaluation_ref)
    snapshot_id = mg_api.create_activation_snapshot(
        graph_id=graph_id,
        activated_by="human.reviewer",
        activation_scope="runtime",
        approval_refs=(approval_ref,),
        evaluation_refs=(evaluation_ref,),
        rollback_snapshot_ref=root_snapshot_id,
        prior_snapshot_id=root_snapshot_id,
        store_root=store_root,
    )
    mg_api.activate_snapshot(snapshot_id, actor="human.reviewer", store_root=store_root)
    return store_root, {
        "graph_id": graph_id,
        "snapshot_id": snapshot_id,
        "approval_ref": approval_ref,
        "evaluation_ref": evaluation_ref,
    }


def _seed_positions(store: ArtifactStore) -> None:
    _append_runtime_event(
        store,
        timestamp="2026-01-01T00:00:01Z",
        event={
            "type": "POSITION_SNAPSHOT",
            "snapshot_id": "pos-1",
            "positions": [
                {"account_id": "ACC1", "position_id": "POS1", "symbol": "AAPL", "quantity": 100},
            ],
        },
    )


def _seed_execution(store: ArtifactStore, **overrides: object) -> None:
    event = {
        "type": "EXECUTION_RESULT",
        "execution_key": "exec-1",
        "account_id": "ACC1",
        "position_id": "POS1",
        "symbol": "AAPL",
        "action": "EXIT",
        "side": "SELL",
        "status": "FILLED",
        "filled_qty": 100,
        "price": 101.5,
        "fee": 1.25,
        "timestamp": "2026-01-01T00:00:02Z",
    }
    event.update(overrides)
    _append_runtime_event(store, timestamp="2026-01-01T00:00:02Z", event=event)


def _external_snapshot(
    store_root: Path,
    *,
    position_records: list[dict] | None = None,
    taxlot_records: list[dict] | None = None,
    execution_records: list[dict] | None = None,
    valuation_records: list[dict] | None = None,
    pnl_records: list[dict] | None = None,
) -> str:
    return ri_api.ingest_external_snapshot(
        source_type="broker_api_snapshot",
        source_name="paper-broker",
        captured_at="2026-01-01T00:00:05Z",
        account_refs=[{"account_id": "ACC1"}],
        position_records=position_records or [],
        taxlot_records=taxlot_records or [],
        execution_records=execution_records or [],
        valuation_records=valuation_records or [],
        pnl_records=pnl_records or [],
        store_root=store_root,
    )


def test_same_internal_state_produces_deterministic_hash(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    store = ArtifactStore(store_root)
    _seed_positions(store)
    first = ri_api.capture_internal_snapshot(store_root=store_root)
    second = ri_api.capture_internal_snapshot(store_root=store_root)
    first_record = store.read("internal_reality_snapshots", first)
    second_record = store.read("internal_reality_snapshots", second)
    assert first_record["record"]["content_hash"] == second_record["record"]["content_hash"]


def test_missing_unsupported_required_state_surface_fails_closed(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    with pytest.raises(ValueError, match="UNSUPPORTED_INTERNAL_SURFACE:taxlots"):
        ri_api.capture_internal_snapshot(store_root=store_root, required_surfaces=("positions", "executions", "taxlots"))


def test_same_external_structured_input_produces_deterministic_hash(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    first = _external_snapshot(store_root, position_records=[{"account_id": "ACC1", "symbol": "AAPL", "quantity": 100}])
    second = _external_snapshot(store_root, position_records=[{"account_id": "ACC1", "symbol": "AAPL", "quantity": 100}])
    store = ArtifactStore(store_root)
    assert store.read("external_reality_snapshots", first)["record"]["content_hash"] == store.read("external_reality_snapshots", second)["record"]["content_hash"]


def test_schema_validation_rejects_malformed_external_snapshot(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    with pytest.raises(ValueError, match="UNSUPPORTED_EXTERNAL_SOURCE_TYPE"):
        ri_api.ingest_external_snapshot(
            source_type="unknown_source",
            source_name="broken",
            captured_at="2026-01-01T00:00:05Z",
            store_root=store_root,
        )


def test_matching_positions_produce_no_material_mismatch(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    store = ArtifactStore(store_root)
    _seed_positions(store)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    external_ref = _external_snapshot(store_root, position_records=[{"account_id": "ACC1", "symbol": "AAPL", "quantity": 100}])
    result = ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("positions",), store_root=store_root)
    record = store.read("reconciliation_results", result["result_ref"])
    assert record["record"]["position_mismatches"] == []
    assert record["record"]["overall_severity"] == "informational"


def test_quantity_mismatch_classified_correctly(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    store = ArtifactStore(store_root)
    _seed_positions(store)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    external_ref = _external_snapshot(store_root, position_records=[{"account_id": "ACC1", "symbol": "AAPL", "quantity": 80}])
    result = ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("positions",), store_root=store_root)
    mismatch = store.read("reconciliation_results", result["result_ref"])["record"]["position_mismatches"][0]
    assert mismatch["mismatch_type"] == "quantity_mismatch"
    assert mismatch["severity"] == "material"


def test_missing_internal_position_classified_correctly(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    external_ref = _external_snapshot(store_root, position_records=[{"account_id": "ACC1", "symbol": "AAPL", "quantity": 50}])
    mismatch = ArtifactStore(store_root).read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("positions",), store_root=store_root)["result_ref"],
    )["record"]["position_mismatches"][0]
    assert mismatch["mismatch_type"] == "missing_internal"


def test_missing_external_position_classified_correctly(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    store = ArtifactStore(store_root)
    _seed_positions(store)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    external_ref = _external_snapshot(store_root)
    mismatch = store.read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("positions",), store_root=store_root)["result_ref"],
    )["record"]["position_mismatches"][0]
    assert mismatch["mismatch_type"] == "missing_external"


def test_matching_tax_lots_reconcile_cleanly(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "taxlots"),
        supplemental_state={"taxlots": [{"account_id": "ACC1", "symbol": "AAPL", "lot_key": "lot-1", "basis": 1000.0, "quantity": 10.0}]},
    )
    external_ref = _external_snapshot(store_root, taxlot_records=[{"account_id": "ACC1", "symbol": "AAPL", "lot_key": "lot-1", "basis": 1000.0, "quantity": 10.0}])
    result = ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("taxlots",), store_root=store_root)
    assert ArtifactStore(store_root).read("reconciliation_results", result["result_ref"])["record"]["taxlot_mismatches"] == []


def test_basis_mismatch_classified_correctly(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "taxlots"),
        supplemental_state={"taxlots": [{"account_id": "ACC1", "symbol": "AAPL", "lot_key": "lot-1", "basis": 1000.0, "quantity": 10.0}]},
    )
    external_ref = _external_snapshot(store_root, taxlot_records=[{"account_id": "ACC1", "symbol": "AAPL", "lot_key": "lot-1", "basis": 900.0, "quantity": 10.0}])
    mismatch = ArtifactStore(store_root).read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("taxlots",), thresholds={"taxlot_basis_tolerance": 1.0}, store_root=store_root)["result_ref"],
    )["record"]["taxlot_mismatches"][0]
    assert mismatch["mismatch_type"] == "basis_mismatch"


def test_quantity_mismatch_taxlots_classified_correctly(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "taxlots"),
        supplemental_state={"taxlots": [{"account_id": "ACC1", "symbol": "AAPL", "lot_key": "lot-1", "basis": 1000.0, "quantity": 10.0}]},
    )
    external_ref = _external_snapshot(store_root, taxlot_records=[{"account_id": "ACC1", "symbol": "AAPL", "lot_key": "lot-1", "basis": 1000.0, "quantity": 8.0}])
    mismatch = ArtifactStore(store_root).read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("taxlots",), store_root=store_root)["result_ref"],
    )["record"]["taxlot_mismatches"][0]
    assert mismatch["mismatch_type"] == "quantity_mismatch"


def test_ambiguous_lot_identity_is_explicit(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "taxlots"),
        supplemental_state={"taxlots": [{"account_id": "ACC1", "symbol": "AAPL", "basis": 1000.0, "quantity": 10.0}]},
    )
    external_ref = _external_snapshot(store_root, taxlot_records=[{"account_id": "ACC1", "symbol": "AAPL", "basis": 1000.0, "quantity": 10.0}])
    mismatch = ArtifactStore(store_root).read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("taxlots",), store_root=store_root)["result_ref"],
    )["record"]["taxlot_mismatches"][0]
    assert mismatch["mismatch_type"] == "lot_identity_ambiguous"


def test_matching_fills_reconcile_cleanly(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    store = ArtifactStore(store_root)
    _seed_positions(store)
    _seed_execution(store)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    external_ref = _external_snapshot(store_root, execution_records=[{"execution_key": "exec-1", "account_id": "ACC1", "symbol": "AAPL", "action": "EXIT", "side": "SELL", "filled_qty": 100, "price": 101.5, "fee": 1.25, "timestamp": "2026-01-01T00:00:02Z"}])
    result = ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("executions",), thresholds={"execution_timestamp_tolerance_seconds": 0.0}, store_root=store_root)
    assert ArtifactStore(store_root).read("reconciliation_results", result["result_ref"])["record"]["execution_mismatches"] == []


def test_missing_fill_classified_critical_when_it_changes_live_position_truth(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    store = ArtifactStore(store_root)
    _seed_positions(store)
    _seed_execution(store)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    external_ref = _external_snapshot(store_root)
    mismatch = ArtifactStore(store_root).read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("executions",), store_root=store_root)["result_ref"],
    )["record"]["execution_mismatches"][0]
    assert mismatch["mismatch_type"] == "extra_fill"
    assert mismatch["severity"] == "critical"


def test_fee_mismatch_classified_correctly(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    store = ArtifactStore(store_root)
    _seed_positions(store)
    _seed_execution(store)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    external_ref = _external_snapshot(store_root, execution_records=[{"execution_key": "exec-1", "account_id": "ACC1", "symbol": "AAPL", "action": "EXIT", "side": "SELL", "filled_qty": 100, "price": 101.5, "fee": 2.25, "timestamp": "2026-01-01T00:00:02Z"}])
    mismatch = ArtifactStore(store_root).read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("executions",), thresholds={"execution_fee_tolerance": 0.1}, store_root=store_root)["result_ref"],
    )["record"]["execution_mismatches"][0]
    assert mismatch["mismatch_type"] == "fee_mismatch"


def test_side_mismatch_classified_correctly(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    store = ArtifactStore(store_root)
    _seed_positions(store)
    _seed_execution(store)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    external_ref = _external_snapshot(store_root, execution_records=[{"execution_key": "exec-1", "account_id": "ACC1", "symbol": "AAPL", "action": "EXIT", "side": "BUY", "filled_qty": 100, "price": 101.5, "fee": 1.25, "timestamp": "2026-01-01T00:00:02Z"}])
    mismatch = ArtifactStore(store_root).read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("executions",), store_root=store_root)["result_ref"],
    )["record"]["execution_mismatches"][0]
    assert mismatch["mismatch_type"] == "side_mismatch"
    assert mismatch["severity"] == "critical"


def test_stale_valuation_detected(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "valuations"),
        supplemental_state={"valuations": [{"account_id": "ACC1", "symbol": "AAPL", "value": 1000.0, "captured_at": "2026-01-01T00:00:00Z"}]},
    )
    external_ref = _external_snapshot(store_root, valuation_records=[{"account_id": "ACC1", "symbol": "AAPL", "value": 1000.0, "captured_at": "2026-01-01T01:00:00Z"}])
    mismatch = ArtifactStore(store_root).read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(
            internal_snapshot_id=internal_ref,
            external_snapshot_id=external_ref,
            requested_surfaces=("valuations",),
            thresholds={"valuation_material_delta": 50.0, "valuation_staleness_seconds": 60.0, "valuation_timestamp_tolerance_seconds": 10.0},
            store_root=store_root,
        )["result_ref"],
    )["record"]["valuation_mismatches"][0]
    assert mismatch["mismatch_type"] == "stale_valuation"


def test_materially_different_valuation_detected(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "valuations"),
        supplemental_state={"valuations": [{"account_id": "ACC1", "symbol": "AAPL", "value": 1000.0, "captured_at": "2026-01-01T00:00:00Z"}]},
    )
    external_ref = _external_snapshot(store_root, valuation_records=[{"account_id": "ACC1", "symbol": "AAPL", "value": 1200.0, "captured_at": "2026-01-01T00:00:30Z"}])
    mismatch = ArtifactStore(store_root).read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(
            internal_snapshot_id=internal_ref,
            external_snapshot_id=external_ref,
            requested_surfaces=("valuations",),
            thresholds={"valuation_material_delta": 50.0, "valuation_staleness_seconds": 3600.0, "valuation_timestamp_tolerance_seconds": 10.0},
            store_root=store_root,
        )["result_ref"],
    )["record"]["valuation_mismatches"][0]
    assert mismatch["mismatch_type"] == "value_mismatch"


def test_timestamp_only_minor_discrepancy_classified_correctly(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "valuations"),
        supplemental_state={"valuations": [{"account_id": "ACC1", "symbol": "AAPL", "value": 1000.0, "captured_at": "2026-01-01T00:00:00Z"}]},
    )
    external_ref = _external_snapshot(store_root, valuation_records=[{"account_id": "ACC1", "symbol": "AAPL", "value": 1000.0, "captured_at": "2026-01-01T00:00:20Z"}])
    mismatch = ArtifactStore(store_root).read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(
            internal_snapshot_id=internal_ref,
            external_snapshot_id=external_ref,
            requested_surfaces=("valuations",),
            thresholds={"valuation_material_delta": 50.0, "valuation_staleness_seconds": 3600.0, "valuation_timestamp_tolerance_seconds": 5.0},
            store_root=store_root,
        )["result_ref"],
    )["record"]["valuation_mismatches"][0]
    assert mismatch["mismatch_type"] == "timestamp_mismatch"
    assert mismatch["severity"] == "minor"


def test_realized_pnl_mismatch_detected(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "pnl"),
        supplemental_state={"pnl": [{"account_id": "ACC1", "symbol_or_scope": "AAPL", "pnl_type": "realized", "pnl": 10.0}]},
    )
    external_ref = _external_snapshot(store_root, pnl_records=[{"account_id": "ACC1", "symbol_or_scope": "AAPL", "pnl_type": "realized", "pnl": 20.0}])
    mismatch = ArtifactStore(store_root).read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("pnl",), thresholds={"pnl_material_delta": 5.0}, store_root=store_root)["result_ref"],
    )["record"]["pnl_mismatches"][0]
    assert mismatch["mismatch_type"] == "realized_pnl_mismatch"


def test_unrealized_pnl_mismatch_detected(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "pnl"),
        supplemental_state={"pnl": [{"account_id": "ACC1", "symbol_or_scope": "AAPL", "pnl_type": "unrealized", "pnl": 10.0}]},
    )
    external_ref = _external_snapshot(store_root, pnl_records=[{"account_id": "ACC1", "symbol_or_scope": "AAPL", "pnl_type": "unrealized", "pnl": 25.0}])
    mismatch = ArtifactStore(store_root).read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("pnl",), thresholds={"pnl_material_delta": 5.0}, store_root=store_root)["result_ref"],
    )["record"]["pnl_mismatches"][0]
    assert mismatch["mismatch_type"] == "unrealized_pnl_mismatch"


def test_fee_driven_pnl_mismatch_detected(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "pnl"),
        supplemental_state={"pnl": [{"account_id": "ACC1", "symbol_or_scope": "fees", "pnl_type": "fees", "pnl": -1.0}]},
    )
    external_ref = _external_snapshot(store_root, pnl_records=[{"account_id": "ACC1", "symbol_or_scope": "fees", "pnl_type": "fees", "pnl": -10.0}])
    mismatch = ArtifactStore(store_root).read(
        "reconciliation_results",
        ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("pnl",), thresholds={"pnl_material_delta": 5.0}, store_root=store_root)["result_ref"],
    )["record"]["pnl_mismatches"][0]
    assert mismatch["mismatch_type"] == "fee_pnl_mismatch"


def test_critical_execution_discrepancy_yields_freeze_review_recommendation(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    store = ArtifactStore(store_root)
    _seed_positions(store)
    _seed_execution(store)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    external_ref = _external_snapshot(store_root)
    result = ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("executions",), store_root=store_root)
    actions = {record["record"]["action_type"] for record in ri_api.recommend_corrections(result["reconciliation_id"], store_root=store_root)}
    assert "freeze_governed_execution" in actions
    assert "import_missing_execution" in actions


def test_material_tax_lot_discrepancy_yields_review_recommendation(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "taxlots"),
        supplemental_state={"taxlots": [{"account_id": "ACC1", "symbol": "AAPL", "lot_key": "lot-1", "basis": 1000.0, "quantity": 10.0}]},
    )
    external_ref = _external_snapshot(store_root, taxlot_records=[{"account_id": "ACC1", "symbol": "AAPL", "lot_key": "lot-1", "basis": 900.0, "quantity": 10.0}])
    result = ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("taxlots",), thresholds={"taxlot_basis_tolerance": 1.0}, store_root=store_root)
    actions = {record["record"]["action_type"] for record in ri_api.recommend_corrections(result["reconciliation_id"], store_root=store_root)}
    assert "refresh_tax_lots" in actions


def test_informational_discrepancy_yields_visible_only_or_no_action(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "valuations"),
        supplemental_state={"valuations": [{"account_id": "ACC1", "symbol": "AAPL", "value": 1000.0, "captured_at": "2026-01-01T00:00:00Z"}]},
    )
    external_ref = _external_snapshot(store_root, valuation_records=[{"account_id": "ACC1", "symbol": "AAPL", "value": 1000.0, "captured_at": "2026-01-01T00:00:20Z"}])
    result = ri_api.reconcile_snapshots(
        internal_snapshot_id=internal_ref,
        external_snapshot_id=external_ref,
        requested_surfaces=("valuations",),
        thresholds={"valuation_material_delta": 50.0, "valuation_staleness_seconds": 3600.0, "valuation_timestamp_tolerance_seconds": 5.0},
        store_root=store_root,
    )
    bundle = ArtifactStore(store_root).read("reconciliation_bundles", result["bundle_ref"])
    classification = ArtifactStore(store_root).read("discrepancy_classifications", bundle["record"]["discrepancy_refs"][0])
    actions = {record["record"]["action_type"] for record in ri_api.recommend_corrections(result["reconciliation_id"], store_root=store_root)}
    assert classification["record"]["operator_visibility"] == "visible_only"
    assert "no_action" in actions


def test_reconciliation_bundle_is_stored_and_recoverable(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    store = ArtifactStore(store_root)
    _seed_positions(store)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    external_ref = _external_snapshot(store_root, position_records=[{"account_id": "ACC1", "symbol": "AAPL", "quantity": 80}])
    result = ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("positions",), store_root=store_root)
    bundle = ArtifactStore(store_root).read("reconciliation_bundles", result["bundle_ref"])
    assert bundle["record"]["result_ref"] == result["result_ref"]


def test_reconciliation_results_link_to_lineage_and_audit_surfaces(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    store = ArtifactStore(store_root)
    _seed_positions(store)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    external_ref = _external_snapshot(store_root, position_records=[{"account_id": "ACC1", "symbol": "AAPL", "quantity": 80}])
    result = ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("positions",), store_root=store_root)
    mg_api.record_decision_lineage("decision-reality", chain["snapshot_id"], store_root=store_root)
    lineage = mg_api.lineage_for_decision("decision-reality", store_root=store_root)
    assert lineage["reconciliation_bundles"][0]["record"]["reconciliation_id"] == result["reconciliation_id"]
    stream = read_canonical_audit_stream(store)
    assert any(row.get("event_type") == "reconciliation_completed" for row in stream if row.get("stream_type") == "governed_audit_event")


def test_expectation_realized_evidence_can_reference_reconciliation_artifacts_where_integrated(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    store = ArtifactStore(store_root)
    _seed_positions(store)
    dataset_ref = gv_api.register_dataset(
        dataset_id="verification-dataset",
        dataset_type="historical_decision_inputs",
        source_description="reality integration proof",
        time_range={"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T00:01:00Z"},
        dataset={"events": [{"timestamp": "2026-01-01T00:00:01Z", "ts_epoch": 1, "event": {"type": "POSITION_SNAPSHOT", "positions": [{"position_id": "POS1", "symbol": "AAPL", "quantity": 100, "risk_flag": "CRITICAL"}]}}]},
        store_root=store_root,
    )
    verification = gv_api.run_verification(
        active_snapshot_id=chain["snapshot_id"],
        candidate_snapshot_id=chain["snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        proposal_id="seed-proposal",
        review_window_start="2026-02-01T00:00:00Z",
        review_window_end="2026-02-28T00:00:00Z",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 5.0, "max_allowed_turnover_increase": 5.0, "max_allowed_tax_degradation": 5.0, "max_allowed_decision_churn": 5}},
        store_root=store_root,
    )
    verified_snapshot_id = mg_api.create_activation_snapshot(
        graph_id=chain["graph_id"],
        activated_by="human.reviewer",
        activation_scope="runtime",
        approval_refs=(chain["approval_ref"],),
        evaluation_refs=(chain["evaluation_ref"],),
        verification_refs=(verification["bundle_ref"],),
        rollback_snapshot_ref=chain["snapshot_id"],
        prior_snapshot_id=chain["snapshot_id"],
        store_root=store_root,
    )
    mg_api.activate_snapshot(verified_snapshot_id, actor="human.reviewer", store_root=store_root)
    mg_api.record_decision_lineage("decision-reality-verified", verified_snapshot_id, store_root=store_root)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    external_ref = _external_snapshot(store_root, position_records=[{"account_id": "ACC1", "symbol": "AAPL", "quantity": 100}])
    ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=external_ref, requested_surfaces=("positions",), store_root=store_root)
    lineage = mg_api.lineage_for_decision("decision-reality-verified", store_root=store_root)
    assert lineage["expectation_records"]
    assert lineage["reconciliation_bundles"]


def test_reconciliation_artifacts_are_append_only(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    external_ref = _external_snapshot(store_root, position_records=[{"account_id": "ACC1", "symbol": "AAPL", "quantity": 100}])
    store = ArtifactStore(store_root)
    with pytest.raises(ValueError, match="IMMUTABLE_CONFLICT"):
        store.write_immutable(
            "external_reality_snapshots",
            external_ref,
            {"changed": True},
            artifact_type="ExternalRealitySnapshot",
            created_at="2026-01-01T00:00:05Z",
        )
