from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPO_ROOT = Path("/home/node/constellation")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from runtime.adaptive_intelligence import adaptation_api as ai_api
from runtime.governance_verification import verification_api as gv_api
from runtime.meta_governance import api as mg_api
from runtime.meta_governance.audit_log import read_canonical_audit_stream
from runtime.meta_governance.interpreter_version import get_interpreter_version
from runtime.meta_governance.predicate_gates import REQUIRED_PREDICATES
from runtime.meta_governance.store import ArtifactStore
from runtime.meta_governance.types import ActivationSnapshot, ApprovalDecision, PredicateEvaluation
from runtime.reality_integration import reconciliation_api as ri_api


DRIFT_THRESHOLDS = {
    "realized_persistence_min_count": 2,
    "reconciliation_persistence_min_count": 2,
}

REGIME_THRESHOLDS = {
    "execution_quality_min_evidence": 2,
    "execution_quality_critical_mismatch_threshold": 1,
    "execution_quality_material_mismatch_threshold": 1,
}

IMPACT_THRESHOLDS = {
    "capital": {"moderate": 1.0, "high": 5.0, "critical": 10.0},
    "risk": {"moderate": 1.0, "high": 5.0, "critical": 10.0},
    "tax": {"moderate": 1.0, "high": 5.0, "critical": 10.0},
    "execution": {"moderate": 1.0, "high": 3.0, "critical": 5.0},
    "trust": {"moderate": 1.0, "high": 2.0, "critical": 3.0},
}


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


def _register_dataset(store_root: Path, dataset_id: str = "adaptive-dataset") -> str:
    return gv_api.register_dataset(
        dataset_id=dataset_id,
        dataset_type="historical_decision_inputs",
        source_description="adaptive intelligence fixture",
        time_range={"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T00:01:00Z"},
        dataset={"events": [{"timestamp": "2026-01-01T00:00:01Z", "ts_epoch": 1, "event": {"type": "POSITION_SNAPSHOT", "positions": [{"position_id": "POS1", "symbol": "AAPL", "quantity": 100, "risk_flag": "CRITICAL"}]}}]},
        store_root=store_root,
    )


def _build_verified_snapshot(store_root: Path, chain: dict) -> tuple[str, dict]:
    dataset_ref = _register_dataset(store_root)
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
    return verified_snapshot_id, verification


def _expectation_ref(store_root: Path, verification: dict, metric_name: str) -> str:
    store = ArtifactStore(store_root)
    for ref in verification["expectation_refs"]:
        if store.read("expectation_records", ref)["record"]["metric_name"] == metric_name:
            return ref
    raise AssertionError(metric_name)


def _seed_positions_and_execution(store: ArtifactStore) -> None:
    _append_runtime_event(
        store,
        timestamp="2026-01-01T00:00:01Z",
        event={
            "type": "POSITION_SNAPSHOT",
            "snapshot_id": "pos-1",
            "positions": [{"account_id": "ACC1", "position_id": "POS1", "symbol": "AAPL", "quantity": 100}],
        },
    )
    _append_runtime_event(
        store,
        timestamp="2026-01-01T00:00:02Z",
        event={
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
        },
    )


def _external_snapshot(
    store_root: Path,
    *,
    captured_at: str,
    position_records: list[dict] | None = None,
    taxlot_records: list[dict] | None = None,
    execution_records: list[dict] | None = None,
) -> str:
    return ri_api.ingest_external_snapshot(
        source_type="broker_api_snapshot",
        source_name="paper-broker",
        captured_at=captured_at,
        account_refs=[{"account_id": "ACC1"}],
        position_records=position_records or [],
        taxlot_records=taxlot_records or [],
        execution_records=execution_records or [],
        store_root=store_root,
    )


def _execution_reconciliation_refs(store_root: Path) -> tuple[str, str]:
    store = ArtifactStore(store_root)
    _seed_positions_and_execution(store)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    first_external = _external_snapshot(store_root, captured_at="2026-01-01T00:00:05Z")
    second_external = _external_snapshot(store_root, captured_at="2026-01-01T00:05:05Z")
    first = ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=first_external, requested_surfaces=("executions",), store_root=store_root)
    second = ri_api.reconcile_snapshots(internal_snapshot_id=internal_ref, external_snapshot_id=second_external, requested_surfaces=("executions",), store_root=store_root)
    return first["result_ref"], second["result_ref"]


def _tax_reconciliation_ref(store_root: Path) -> str:
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "taxlots"),
        supplemental_state={"taxlots": [{"account_id": "ACC1", "symbol": "AAPL", "lot_key": "lot-1", "basis": 1000.0, "quantity": 10.0}]},
    )
    external_ref = _external_snapshot(
        store_root,
        captured_at="2026-01-01T00:06:05Z",
        taxlot_records=[{"account_id": "ACC1", "symbol": "AAPL", "lot_key": "lot-1", "basis": 900.0, "quantity": 10.0}],
    )
    result = ri_api.reconcile_snapshots(
        internal_snapshot_id=internal_ref,
        external_snapshot_id=external_ref,
        requested_surfaces=("taxlots",),
        thresholds={"taxlot_basis_tolerance": 1.0},
        store_root=store_root,
    )
    return result["result_ref"]


def test_repeated_expectation_miss_becomes_persistent_drift(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    refs = ai_api.detect_drift_signals(realized_validation_refs=(first, second), thresholds=DRIFT_THRESHOLDS, store_root=store_root)
    signal = ArtifactStore(store_root).read("drift_signals", refs[0])["record"]
    assert signal["signal_family"] == "execution_drift"
    assert signal["drift_classification"] == "persistent_systematic"


def test_one_off_miss_does_not_become_persistent_systematic_drift(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    validation_ref = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    refs = ai_api.detect_drift_signals(realized_validation_refs=(validation_ref,), thresholds=DRIFT_THRESHOLDS, store_root=store_root)
    signal = ArtifactStore(store_root).read("drift_signals", refs[0])["record"]
    assert signal["drift_classification"] != "persistent_systematic"


def test_repeated_reconciliation_break_becomes_drift_signal(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    first, second = _execution_reconciliation_refs(store_root)
    refs = ai_api.detect_drift_signals(reconciliation_result_refs=(first, second), thresholds=DRIFT_THRESHOLDS, store_root=store_root)
    signal = ArtifactStore(store_root).read("drift_signals", refs[0])["record"]
    assert signal["signal_family"] == "execution_drift"
    assert signal["drift_classification"] == "persistent_systematic"


def test_drift_signal_hashing_is_deterministic(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    left = ai_api.detect_drift_signals(realized_validation_refs=(first, second), thresholds=DRIFT_THRESHOLDS, store_root=store_root)
    right = ai_api.detect_drift_signals(realized_validation_refs=(first, second), thresholds=DRIFT_THRESHOLDS, store_root=store_root)
    assert left == right


def test_insufficient_data_yields_insufficient_evidence_regime_signal(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    validation_ref = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    drift_ref = ai_api.detect_drift_signals(realized_validation_refs=(validation_ref,), thresholds=DRIFT_THRESHOLDS, store_root=store_root)[0]
    regime_ref = ai_api.detect_regime_signals(
        drift_signal_refs=(drift_ref,),
        requested_regime_families=("execution_quality_regime",),
        thresholds=REGIME_THRESHOLDS,
        store_root=store_root,
    )[0]
    regime = ArtifactStore(store_root).read("regime_signals", regime_ref)["record"]
    assert regime["confidence_class"] == "insufficient_evidence"


def test_supported_input_pattern_yields_deterministic_regime_classification(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first_validation = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second_validation = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    drift_ref = ai_api.detect_drift_signals(realized_validation_refs=(first_validation, second_validation), thresholds=DRIFT_THRESHOLDS, store_root=store_root)[0]
    first_reconciliation, second_reconciliation = _execution_reconciliation_refs(store_root)
    left = ai_api.detect_regime_signals(
        drift_signal_refs=(drift_ref,),
        reconciliation_result_refs=(first_reconciliation, second_reconciliation),
        requested_regime_families=("execution_quality_regime",),
        thresholds=REGIME_THRESHOLDS,
        store_root=store_root,
    )
    right = ai_api.detect_regime_signals(
        drift_signal_refs=(drift_ref,),
        reconciliation_result_refs=(first_reconciliation, second_reconciliation),
        requested_regime_families=("execution_quality_regime",),
        thresholds=REGIME_THRESHOLDS,
        store_root=store_root,
    )
    regime = ArtifactStore(store_root).read("regime_signals", left[0])["record"]
    assert left == right
    assert regime["candidate_regime"] == "degraded_execution_quality"


def test_higher_capital_and_risk_exposure_increases_impact_class(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_risk_delta")
    validation_ref = gv_api.validate_realized_expectation(expectation_ref, actual_value=10.0, store_root=store_root)
    drift_ref = ai_api.detect_drift_signals(realized_validation_refs=(validation_ref,), thresholds=DRIFT_THRESHOLDS, store_root=store_root)[0]
    low = ai_api.assess_impacts(
        source_refs=(drift_ref,),
        thresholds=IMPACT_THRESHOLDS,
        exposure_overrides={drift_ref: {"capital_exposure_estimate": 0.5, "risk_exposure_estimate": 0.5, "operator_trust_impact": 0.5}},
        store_root=store_root,
    )[0]
    high = ai_api.assess_impacts(
        source_refs=(drift_ref,),
        thresholds=IMPACT_THRESHOLDS,
        exposure_overrides={drift_ref: {"capital_exposure_estimate": 12.0, "risk_exposure_estimate": 12.0, "operator_trust_impact": 3.0}},
        store_root=store_root,
    )[0]
    store = ArtifactStore(store_root)
    assert store.read("impact_assessments", low)["record"]["impact_class"] == "low"
    assert store.read("impact_assessments", high)["record"]["impact_class"] == "critical"


def test_missing_required_exposure_input_fails_closed(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_risk_delta")
    validation_ref = gv_api.validate_realized_expectation(expectation_ref, actual_value=10.0, store_root=store_root)
    drift_ref = ai_api.detect_drift_signals(realized_validation_refs=(validation_ref,), thresholds=DRIFT_THRESHOLDS, store_root=store_root)[0]
    with pytest.raises(ValueError, match="EXPOSURE_INPUT_REQUIRED"):
        ai_api.assess_impacts(source_refs=(drift_ref,), thresholds=IMPACT_THRESHOLDS, require_explicit_exposure=True, store_root=store_root)


def test_critical_persistent_issue_outranks_minor_one_off_issue(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    execution_expectation = _expectation_ref(store_root, verification, "expected_execution_delta")
    tax_expectation = _expectation_ref(store_root, verification, "expected_tax_delta")
    severe_left = gv_api.validate_realized_expectation(execution_expectation, actual_value=5.0, store_root=store_root)
    severe_right = gv_api.validate_realized_expectation(execution_expectation, actual_value=6.0, store_root=store_root)
    mild = gv_api.validate_realized_expectation(tax_expectation, actual_value=0.06, store_root=store_root)
    drift_refs = ai_api.detect_drift_signals(realized_validation_refs=(severe_left, severe_right, mild), thresholds=DRIFT_THRESHOLDS, store_root=store_root)
    impact_refs = ai_api.assess_impacts(source_refs=drift_refs, thresholds=IMPACT_THRESHOLDS, store_root=store_root)
    ranked_refs = ai_api.rank_adaptive_issues(impact_assessment_refs=impact_refs, store_root=store_root)
    store = ArtifactStore(store_root)
    top_issue = store.read("ranked_issues", ranked_refs[0])["record"]
    assert top_issue["source_refs"][0] in drift_refs
    assert top_issue["severity"] == "critical"


def test_deterministic_rank_ordering_holds_across_identical_inputs(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    drift_refs = ai_api.detect_drift_signals(realized_validation_refs=(first, second), thresholds=DRIFT_THRESHOLDS, store_root=store_root)
    impact_refs = ai_api.assess_impacts(source_refs=drift_refs, thresholds=IMPACT_THRESHOLDS, store_root=store_root)
    left = ai_api.rank_adaptive_issues(impact_assessment_refs=impact_refs, store_root=store_root)
    right = ai_api.rank_adaptive_issues(impact_assessment_refs=impact_refs, store_root=store_root)
    record = ArtifactStore(store_root).read("ranked_issues", left[0])["record"]
    assert left == right
    assert "severity_score" in record["rank_score_components"]


def test_persistent_execution_drift_yields_tighten_execution_guardrails_recommendation(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    drift_refs = ai_api.detect_drift_signals(realized_validation_refs=(first, second), thresholds=DRIFT_THRESHOLDS, store_root=store_root)
    impact_refs = ai_api.assess_impacts(source_refs=drift_refs, thresholds=IMPACT_THRESHOLDS, store_root=store_root)
    ranked_refs = ai_api.rank_adaptive_issues(impact_assessment_refs=impact_refs, store_root=store_root)
    recommendation_refs = ai_api.generate_adaptation_recommendations(ranked_issue_refs=ranked_refs, store_root=store_root)
    recommendations = [ArtifactStore(store_root).read("adaptation_recommendations", ref)["record"]["recommendation_type"] for ref in recommendation_refs]
    assert "tighten_execution_guardrails" in recommendations


def test_repeated_truth_mismatch_yields_investigate_broker_truth_or_rebuild_state_surface(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    reconciliation_ref = _tax_reconciliation_ref(store_root)
    impact_refs = ai_api.assess_impacts(source_refs=(reconciliation_ref,), thresholds=IMPACT_THRESHOLDS, store_root=store_root)
    ranked_refs = ai_api.rank_adaptive_issues(impact_assessment_refs=impact_refs, store_root=store_root)
    recommendation_refs = ai_api.generate_adaptation_recommendations(ranked_issue_refs=ranked_refs, store_root=store_root)
    recommendations = {ArtifactStore(store_root).read("adaptation_recommendations", ref)["record"]["recommendation_type"] for ref in recommendation_refs}
    assert recommendations & {"investigate_broker_truth", "rebuild_state_surface"}


def test_low_severity_isolated_issue_can_yield_no_action_or_collect_more_evidence(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    validation_ref = gv_api.validate_realized_expectation(expectation_ref, actual_value=0.01, store_root=store_root)
    drift_refs = ai_api.detect_drift_signals(realized_validation_refs=(validation_ref,), thresholds=DRIFT_THRESHOLDS, store_root=store_root)
    regime_refs = ai_api.detect_regime_signals(
        drift_signal_refs=drift_refs,
        requested_regime_families=("execution_quality_regime",),
        thresholds=REGIME_THRESHOLDS,
        store_root=store_root,
    )
    impact_refs = ai_api.assess_impacts(source_refs=regime_refs, thresholds=IMPACT_THRESHOLDS, store_root=store_root)
    ranked_refs = ai_api.rank_adaptive_issues(impact_assessment_refs=impact_refs, store_root=store_root)
    recommendation_refs = ai_api.generate_adaptation_recommendations(ranked_issue_refs=ranked_refs, store_root=store_root)
    recommendation_types = {ArtifactStore(store_root).read("adaptation_recommendations", ref)["record"]["recommendation_type"] for ref in recommendation_refs}
    assert recommendation_types & {"no_action", "collect_more_evidence"}


def test_evidence_backed_candidate_is_created_with_lineage_refs(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    verified_snapshot_id, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    result = ai_api.run_adaptive_cycle(
        realized_validation_refs=(first, second),
        requested_regime_families=("execution_quality_regime",),
        drift_thresholds=DRIFT_THRESHOLDS,
        regime_thresholds=REGIME_THRESHOLDS,
        impact_thresholds=IMPACT_THRESHOLDS,
        store_root=store_root,
    )
    candidate = ArtifactStore(store_root).read("proposal_candidates", result["proposal_candidate_refs"][0])["record"]
    assert verified_snapshot_id
    assert candidate["source_refs"]
    assert candidate["evidence_bundle_refs"]


def test_candidate_remains_artifact_only_and_does_not_mutate_governance_state(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    proposal_ids_before = ArtifactStore(store_root).list_ids("proposals")
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    ai_api.run_adaptive_cycle(
        realized_validation_refs=(first, second),
        requested_regime_families=("execution_quality_regime",),
        drift_thresholds=DRIFT_THRESHOLDS,
        regime_thresholds=REGIME_THRESHOLDS,
        impact_thresholds=IMPACT_THRESHOLDS,
        store_root=store_root,
    )
    assert ArtifactStore(store_root).list_ids("proposals") == proposal_ids_before


def test_operator_summary_includes_top_ranked_issues_and_recommendations(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    result = ai_api.run_adaptive_cycle(
        realized_validation_refs=(first, second),
        requested_regime_families=("execution_quality_regime",),
        drift_thresholds=DRIFT_THRESHOLDS,
        regime_thresholds=REGIME_THRESHOLDS,
        impact_thresholds=IMPACT_THRESHOLDS,
        store_root=store_root,
    )
    summary = ArtifactStore(store_root).read("operator_summaries", result["operator_summary_ref"])["record"]
    assert summary["top_ranked_issues"]
    assert summary["recommended_actions"]


def test_operator_summary_is_deterministic_and_hashable(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    drift_refs = ai_api.detect_drift_signals(realized_validation_refs=(first, second), thresholds=DRIFT_THRESHOLDS, store_root=store_root)
    regime_refs = ai_api.detect_regime_signals(
        drift_signal_refs=drift_refs,
        requested_regime_families=("execution_quality_regime",),
        thresholds=REGIME_THRESHOLDS,
        store_root=store_root,
    )
    impact_refs = ai_api.assess_impacts(source_refs=tuple(sorted(drift_refs + regime_refs)), thresholds=IMPACT_THRESHOLDS, store_root=store_root)
    ranked_refs = ai_api.rank_adaptive_issues(impact_assessment_refs=impact_refs, store_root=store_root)
    recommendation_refs = ai_api.generate_adaptation_recommendations(ranked_issue_refs=ranked_refs, store_root=store_root)
    left = ai_api.build_operator_summary(
        ranked_issue_refs=ranked_refs,
        drift_signal_refs=drift_refs,
        regime_signal_refs=regime_refs,
        recommendation_refs=recommendation_refs,
        proposal_candidate_refs=(),
        as_of="2026-03-01T00:00:00Z",
        store_root=store_root,
    )
    right = ai_api.build_operator_summary(
        ranked_issue_refs=ranked_refs,
        drift_signal_refs=drift_refs,
        regime_signal_refs=regime_refs,
        recommendation_refs=recommendation_refs,
        proposal_candidate_refs=(),
        as_of="2026-03-01T00:00:00Z",
        store_root=store_root,
    )
    assert left == right


def test_operator_summary_includes_insufficient_evidence_blockers_when_relevant(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    validation_ref = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    result = ai_api.run_adaptive_cycle(
        realized_validation_refs=(validation_ref,),
        requested_regime_families=("execution_quality_regime",),
        drift_thresholds=DRIFT_THRESHOLDS,
        regime_thresholds=REGIME_THRESHOLDS,
        impact_thresholds=IMPACT_THRESHOLDS,
        store_root=store_root,
    )
    summary = ArtifactStore(store_root).read("operator_summaries", result["operator_summary_ref"])["record"]
    regime = ArtifactStore(store_root).read("regime_signals", summary["major_regime_signals"][0])["record"]
    assert regime["confidence_class"] == "insufficient_evidence"


def test_adaptation_bundle_is_stored_and_recoverable(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    result = ai_api.run_adaptive_cycle(
        realized_validation_refs=(first, second),
        requested_regime_families=("execution_quality_regime",),
        drift_thresholds=DRIFT_THRESHOLDS,
        regime_thresholds=REGIME_THRESHOLDS,
        impact_thresholds=IMPACT_THRESHOLDS,
        store_root=store_root,
    )
    bundle = ArtifactStore(store_root).read("adaptation_bundles", result["bundle_ref"])
    assert bundle["record"]["operator_summary_ref"] == result["operator_summary_ref"]


def test_ranked_issues_and_recommendations_link_back_to_evidence_artifacts(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    result = ai_api.run_adaptive_cycle(
        realized_validation_refs=(first, second),
        requested_regime_families=("execution_quality_regime",),
        drift_thresholds=DRIFT_THRESHOLDS,
        regime_thresholds=REGIME_THRESHOLDS,
        impact_thresholds=IMPACT_THRESHOLDS,
        store_root=store_root,
    )
    issue = ArtifactStore(store_root).read("ranked_issues", result["ranked_issue_refs"][0])["record"]
    recommendation = ArtifactStore(store_root).read("adaptation_recommendations", result["recommendation_refs"][0])["record"]
    assert issue["source_refs"]
    assert recommendation["source_refs"]


def test_proposal_candidate_links_to_supporting_adaptation_bundle(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    result = ai_api.run_adaptive_cycle(
        realized_validation_refs=(first, second),
        requested_regime_families=("execution_quality_regime",),
        drift_thresholds=DRIFT_THRESHOLDS,
        regime_thresholds=REGIME_THRESHOLDS,
        impact_thresholds=IMPACT_THRESHOLDS,
        store_root=store_root,
    )
    candidate = ArtifactStore(store_root).read("proposal_candidates", result["proposal_candidate_refs"][0])["record"]
    assert result["bundle_ref"] in candidate["evidence_bundle_refs"]


def test_adaptation_bundle_and_artifacts_link_into_lineage_and_audit(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    verified_snapshot_id, verification = _build_verified_snapshot(store_root, chain)
    mg_api.record_decision_lineage("decision-adaptive", verified_snapshot_id, store_root=store_root)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    reconciliation_ref = _tax_reconciliation_ref(store_root)
    result = ai_api.run_adaptive_cycle(
        realized_validation_refs=(first, second),
        reconciliation_result_refs=(reconciliation_ref,),
        requested_regime_families=("execution_quality_regime",),
        drift_thresholds=DRIFT_THRESHOLDS,
        regime_thresholds=REGIME_THRESHOLDS,
        impact_thresholds=IMPACT_THRESHOLDS,
        store_root=store_root,
    )
    lineage = mg_api.lineage_for_decision("decision-adaptive", store_root=store_root)
    stream = read_canonical_audit_stream(ArtifactStore(store_root))
    assert lineage["adaptation_bundles"][0]["record"]["adaptation_bundle_id"] == result["bundle_ref"]
    assert lineage["proposal_candidates"]
    assert any(row.get("event_type") == "adaptation_bundle_created" for row in stream if row.get("stream_type") == "governed_audit_event")


def test_adaptation_artifacts_are_append_only(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification = _build_verified_snapshot(store_root, chain)
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    result = ai_api.run_adaptive_cycle(
        realized_validation_refs=(first, second),
        requested_regime_families=("execution_quality_regime",),
        drift_thresholds=DRIFT_THRESHOLDS,
        regime_thresholds=REGIME_THRESHOLDS,
        impact_thresholds=IMPACT_THRESHOLDS,
        store_root=store_root,
    )
    store = ArtifactStore(store_root)
    with pytest.raises(ValueError, match="IMMUTABLE_CONFLICT"):
        store.write_immutable(
            "adaptation_bundles",
            result["bundle_ref"],
            {"changed": True},
            artifact_type="AdaptationBundle",
            created_at="2026-03-01T00:00:00Z",
        )
