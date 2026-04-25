from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPO_ROOT = Path("/home/node/constellation")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from runtime.adaptive_intelligence import adaptation_api as ai_api
from runtime.autonomous_decision import autonomous_api as ad_api
from runtime.autonomous_decision.schemas import content_hash
from runtime.autonomous_decision.types import CandidateAction
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
        handle.write(
            json.dumps(
                {
                    "stream_type": "runtime_event",
                    "timestamp": timestamp,
                    "ts_epoch": None,
                    "event_id": f"runtime-{timestamp}",
                    "event": event,
                },
                sort_keys=True,
            )
            + "\n"
        )


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
        expected_benefit="deterministic autonomous baseline",
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
        "graph_hash": graph["record"]["graph_hash"],
        "snapshot_id": snapshot_id,
        "approval_ref": approval_ref,
        "evaluation_ref": evaluation_ref,
    }


def _register_dataset(store_root: Path, dataset_id: str = "autonomous-dataset") -> str:
    return gv_api.register_dataset(
        dataset_id=dataset_id,
        dataset_type="historical_decision_inputs",
        source_description="autonomous decision fixture",
        time_range={"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T00:01:00Z"},
        dataset={
            "events": [
                {
                    "timestamp": "2026-01-01T00:00:01Z",
                    "ts_epoch": 1,
                    "event": {
                        "type": "POSITION_SNAPSHOT",
                        "positions": [{"position_id": "POS1", "symbol": "AAPL", "quantity": 100, "risk_flag": "CRITICAL"}],
                    },
                }
            ]
        },
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
        scope={
            "behavioral_budgets": {
                "max_allowed_risk_increase": 5.0,
                "max_allowed_turnover_increase": 5.0,
                "max_allowed_tax_degradation": 5.0,
                "max_allowed_decision_churn": 5,
            }
        },
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
    captured_at: str = "2026-01-01T00:00:05Z",
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


def _persistent_execution_validations(store_root: Path, verification: dict) -> tuple[str, str]:
    expectation_ref = _expectation_ref(store_root, verification, "expected_execution_delta")
    first = gv_api.validate_realized_expectation(expectation_ref, actual_value=5.0, store_root=store_root)
    second = gv_api.validate_realized_expectation(expectation_ref, actual_value=6.0, store_root=store_root)
    return first, second


def _adaptive_execution_cycle(store_root: Path, chain: dict) -> tuple[str, dict, dict]:
    verified_snapshot_id, verification = _build_verified_snapshot(store_root, chain)
    first, second = _persistent_execution_validations(store_root, verification)
    result = ai_api.run_adaptive_cycle(
        realized_validation_refs=(first, second),
        requested_regime_families=("execution_quality_regime",),
        drift_thresholds=DRIFT_THRESHOLDS,
        regime_thresholds=REGIME_THRESHOLDS,
        impact_thresholds=IMPACT_THRESHOLDS,
        store_root=store_root,
    )
    return verified_snapshot_id, verification, result


def _recommendation_ref_by_type(store: ArtifactStore, refs: tuple[str, ...], kind: str, field: str, expected: str) -> str:
    for ref in refs:
        if store.read(kind, ref)["record"][field] == expected:
            return ref
    raise AssertionError(expected)


def _critical_execution_reconciliation(store_root: Path) -> dict:
    store = ArtifactStore(store_root)
    _seed_positions_and_execution(store)
    internal_ref = ri_api.capture_internal_snapshot(store_root=store_root)
    external_ref = _external_snapshot(store_root, execution_records=[])
    return ri_api.reconcile_snapshots(
        internal_snapshot_id=internal_ref,
        external_snapshot_id=external_ref,
        requested_surfaces=("executions",),
        store_root=store_root,
    )


def _material_tax_reconciliation(store_root: Path) -> dict:
    store = ArtifactStore(store_root)
    _seed_positions_and_execution(store)
    internal_ref = ri_api.capture_internal_snapshot(
        store_root=store_root,
        required_surfaces=("positions", "executions", "taxlots"),
        supplemental_state={
            "taxlots": [{"account_id": "ACC1", "symbol": "AAPL", "lot_key": "lot-1", "basis": 1000.0, "quantity": 10.0}],
        },
    )
    external_ref = _external_snapshot(
        store_root,
        captured_at="2026-01-01T00:06:05Z",
        taxlot_records=[{"account_id": "ACC1", "symbol": "AAPL", "lot_key": "lot-1", "basis": 900.0, "quantity": 10.0}],
    )
    return ri_api.reconcile_snapshots(
        internal_snapshot_id=internal_ref,
        external_snapshot_id=external_ref,
        requested_surfaces=("taxlots",),
        thresholds={"taxlot_basis_tolerance": 1.0},
        store_root=store_root,
    )


def _write_manual_candidate_action(
    store_root: Path,
    *,
    action_family: str,
    target_surface: str,
    source_refs: tuple[str, ...] = ("manual-source",),
    supporting_evidence_refs: tuple[str, ...] = (),
    proposed_effect: str = "manual autonomous test effect",
    rationale: str = "manual autonomous test rationale",
    estimated_blast_radius: str = "safe",
    estimated_scope: dict | None = None,
    created_at: str = "2026-03-01T00:00:00Z",
) -> str:
    estimated_scope = estimated_scope or {"reversible": action_family in {"freeze_execution_family", "defer_change_family", "no_immediate_action"}}
    payload = {
        "action_family": action_family,
        "target_surface": target_surface,
        "source_refs": source_refs,
        "proposed_effect": proposed_effect,
        "rationale": rationale,
        "supporting_evidence_refs": supporting_evidence_refs,
        "estimated_blast_radius": estimated_blast_radius,
        "estimated_scope": estimated_scope,
    }
    artifact_hash = content_hash(payload)
    action = CandidateAction(
        candidate_action_id=f"candidate-action-{artifact_hash[:12]}",
        action_family=action_family,
        target_surface=target_surface,
        source_refs=source_refs,
        proposed_effect=proposed_effect,
        rationale=rationale,
        supporting_evidence_refs=supporting_evidence_refs,
        estimated_blast_radius=estimated_blast_radius,
        estimated_scope=estimated_scope,
        artifact_hash=artifact_hash,
    )
    ArtifactStore(store_root).write_immutable(
        "candidate_actions",
        action.candidate_action_id,
        action,
        artifact_type="CandidateAction",
        created_at=created_at,
    )
    return action.candidate_action_id


def _seed_insufficient_regime_signal(store_root: Path, regime_id: str = "regime-insufficient") -> str:
    ArtifactStore(store_root).write_immutable(
        "regime_signals",
        regime_id,
        {
            "regime_signal_id": regime_id,
            "regime_family": "execution_quality_regime",
            "source_refs": ("manual-evidence",),
            "supporting_metrics": {"count": 1},
            "prior_regime": "stable",
            "candidate_regime": "unknown",
            "confidence_class": "insufficient_evidence",
            "severity": "informational",
            "explanation": "insufficient evidence fixture",
            "artifact_hash": content_hash({"regime_signal_id": regime_id, "confidence_class": "insufficient_evidence"}),
        },
        artifact_type="RegimeSignal",
        created_at="2026-03-01T00:00:00Z",
    )
    return regime_id


def _seed_broker_conflict_recommendation(store_root: Path, recommendation_id: str = "broker-conflict-rec") -> str:
    ArtifactStore(store_root).write_immutable(
        "correction_recommendations",
        recommendation_id,
        {
            "reconciliation_id": "reconciliation-broker-conflict",
            "recommendation_id": recommendation_id,
            "target_surface": "taxlots",
            "action_type": "broker_review_required",
            "action_priority": "p1",
            "requires_human_review": True,
            "rationale": "broker truth conflict fixture",
        },
        artifact_type="CorrectionRecommendation",
        created_at="2026-03-01T00:00:00Z",
    )
    return recommendation_id


def test_adaptive_recommendation_yields_expected_candidate_action(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, _, adaptive = _adaptive_execution_cycle(store_root, chain)
    store = ArtifactStore(store_root)
    recommendation_ref = _recommendation_ref_by_type(
        store,
        adaptive["recommendation_refs"],
        "adaptation_recommendations",
        "recommendation_type",
        "tighten_execution_guardrails",
    )
    candidate_ref = ad_api.generate_candidate_actions(
        adaptation_recommendation_refs=(recommendation_ref,),
        store_root=store_root,
    )[0]
    candidate = store.read("candidate_actions", candidate_ref)["record"]
    assert candidate["action_family"] == "tighten_guardrail"
    assert candidate["target_surface"] == "execution"


def test_reconciliation_recommendation_yields_expected_candidate_action(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    reconciliation = _critical_execution_reconciliation(store_root)
    store = ArtifactStore(store_root)
    recommendation_ref = _recommendation_ref_by_type(
        store,
        reconciliation["recommendation_refs"],
        "correction_recommendations",
        "action_type",
        "freeze_governed_execution",
    )
    candidate_ref = ad_api.generate_candidate_actions(
        correction_recommendation_refs=(recommendation_ref,),
        store_root=store_root,
    )[0]
    candidate = store.read("candidate_actions", candidate_ref)["record"]
    assert candidate["action_family"] == "freeze_execution_family"
    assert candidate["target_surface"] == "execution"


def test_candidate_action_hashing_is_deterministic(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    reconciliation = _critical_execution_reconciliation(store_root)
    store = ArtifactStore(store_root)
    recommendation_ref = _recommendation_ref_by_type(
        store,
        reconciliation["recommendation_refs"],
        "correction_recommendations",
        "action_type",
        "freeze_governed_execution",
    )
    left = ad_api.generate_candidate_actions(correction_recommendation_refs=(recommendation_ref,), store_root=store_root)
    right = ad_api.generate_candidate_actions(correction_recommendation_refs=(recommendation_ref,), store_root=store_root)
    assert left == right
    assert store.read("candidate_actions", left[0])["record"]["artifact_hash"] == store.read("candidate_actions", right[0])["record"]["artifact_hash"]


def test_insufficient_evidence_becomes_advisory_only(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    regime_ref = _seed_insufficient_regime_signal(store_root)
    candidate_ref = _write_manual_candidate_action(
        store_root,
        action_family="freeze_execution_family",
        target_surface="execution",
        supporting_evidence_refs=(regime_ref,),
    )
    classification_ref = ad_api.classify_candidate_actions(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    classification = ArtifactStore(store_root).read("autonomy_classifications", classification_ref)["record"]
    assert classification["autonomy_class"] == "advisory_only"
    assert classification["confidence_class"] == "insufficient_evidence"


def test_live_capital_or_risk_action_requires_approval(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, _, adaptive = _adaptive_execution_cycle(store_root, chain)
    store = ArtifactStore(store_root)
    recommendation_ref = _recommendation_ref_by_type(
        store,
        adaptive["recommendation_refs"],
        "adaptation_recommendations",
        "recommendation_type",
        "tighten_execution_guardrails",
    )
    candidate_ref = ad_api.generate_candidate_actions(
        adaptation_recommendation_refs=(recommendation_ref,),
        store_root=store_root,
    )[0]
    classification_ref = ad_api.classify_candidate_actions(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    classification = store.read("autonomy_classifications", classification_ref)["record"]
    assert classification["autonomy_class"] == "approval_required"
    assert "live_capital_or_risk_surface" in classification["classification_reasons"]


def test_protective_freeze_can_become_auto_executable_when_predicates_pass(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    candidate_ref = _write_manual_candidate_action(
        store_root,
        action_family="freeze_execution_family",
        target_surface="execution",
        source_refs=("manual-protective",),
    )
    classification_ref = ad_api.classify_candidate_actions(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    authority_ref = ad_api.assess_authority(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    predicate_refs = ad_api.evaluate_action_predicates(
        candidate_action_refs=(candidate_ref,),
        authority_evaluation_refs=(authority_ref,),
        classification_refs=(classification_ref,),
        store_root=store_root,
    )
    eligibility_ref = ad_api.determine_execution_eligibility(
        candidate_action_refs=(candidate_ref,),
        classification_refs=(classification_ref,),
        predicate_result_refs=predicate_refs,
        authority_evaluation_refs=(authority_ref,),
        store_root=store_root,
    )[0]
    store = ArtifactStore(store_root)
    assert chain["snapshot_id"]
    assert store.read("autonomy_classifications", classification_ref)["record"]["autonomy_class"] == "auto_executable"
    assert all(store.read("autonomy_predicate_results", ref)["record"]["result"] for ref in predicate_refs)
    assert store.read("execution_eligibilities", eligibility_ref)["record"]["eligible_for_execution"] is True


def test_unresolved_broker_truth_conflict_blocks_auto_executable(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    recommendation_ref = _seed_broker_conflict_recommendation(store_root)
    candidate_ref = _write_manual_candidate_action(
        store_root,
        action_family="freeze_execution_family",
        target_surface="execution",
        supporting_evidence_refs=(recommendation_ref,),
    )
    classification_ref = ad_api.classify_candidate_actions(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    classification = ArtifactStore(store_root).read("autonomy_classifications", classification_ref)["record"]
    assert classification["autonomy_class"] == "approval_required"
    assert "broker_truth_conflict" in classification["classification_reasons"]


def test_invalid_active_snapshot_blocks_execution_eligibility(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    candidate_ref = _write_manual_candidate_action(
        store_root,
        action_family="freeze_execution_family",
        target_surface="execution",
    )
    classification_ref = ad_api.classify_candidate_actions(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    authority_ref = ad_api.assess_authority(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    predicate_refs = ad_api.evaluate_action_predicates(
        candidate_action_refs=(candidate_ref,),
        authority_evaluation_refs=(authority_ref,),
        classification_refs=(classification_ref,),
        store_root=store_root,
    )
    eligibility_ref = ad_api.determine_execution_eligibility(
        candidate_action_refs=(candidate_ref,),
        classification_refs=(classification_ref,),
        predicate_result_refs=predicate_refs,
        authority_evaluation_refs=(authority_ref,),
        store_root=store_root,
    )[0]
    store = ArtifactStore(store_root)
    authority = store.read("authority_evaluations", authority_ref)["record"]
    eligibility = store.read("execution_eligibilities", eligibility_ref)["record"]
    assert "ACTIVE_SNAPSHOT_REQUIRED" in authority["blocking_conditions"]
    assert eligibility["eligible_for_execution"] is False


def test_missing_approval_blocks_approval_required_execution(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, _, adaptive = _adaptive_execution_cycle(store_root, chain)
    store = ArtifactStore(store_root)
    recommendation_ref = _recommendation_ref_by_type(
        store,
        adaptive["recommendation_refs"],
        "adaptation_recommendations",
        "recommendation_type",
        "tighten_execution_guardrails",
    )
    candidate_ref = ad_api.generate_candidate_actions(
        adaptation_recommendation_refs=(recommendation_ref,),
        store_root=store_root,
    )[0]
    classification_ref = ad_api.classify_candidate_actions(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    authority_ref = ad_api.assess_authority(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    predicate_refs = ad_api.evaluate_action_predicates(
        candidate_action_refs=(candidate_ref,),
        authority_evaluation_refs=(authority_ref,),
        classification_refs=(classification_ref,),
        store_root=store_root,
    )
    eligibility_ref = ad_api.determine_execution_eligibility(
        candidate_action_refs=(candidate_ref,),
        classification_refs=(classification_ref,),
        predicate_result_refs=predicate_refs,
        authority_evaluation_refs=(authority_ref,),
        store_root=store_root,
    )[0]
    eligibility = store.read("execution_eligibilities", eligibility_ref)["record"]
    assert eligibility["eligible_for_execution"] is False
    assert "HUMAN_APPROVAL_REQUIRED" in eligibility["blocking_reasons"]
    assert "APPROVAL_REQUIRED_ACTION" in eligibility["blocking_reasons"]


def test_critical_reconciliation_break_blocks_auto_execution(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    reconciliation = _critical_execution_reconciliation(store_root)
    store = ArtifactStore(store_root)
    recommendation_ref = _recommendation_ref_by_type(
        store,
        reconciliation["recommendation_refs"],
        "correction_recommendations",
        "action_type",
        "freeze_governed_execution",
    )
    candidate_ref = ad_api.generate_candidate_actions(
        correction_recommendation_refs=(recommendation_ref,),
        store_root=store_root,
    )[0]
    classification_ref = ad_api.classify_candidate_actions(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    authority_ref = ad_api.assess_authority(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    predicate_refs = ad_api.evaluate_action_predicates(
        candidate_action_refs=(candidate_ref,),
        authority_evaluation_refs=(authority_ref,),
        classification_refs=(classification_ref,),
        store_root=store_root,
    )
    eligibility_ref = ad_api.determine_execution_eligibility(
        candidate_action_refs=(candidate_ref,),
        classification_refs=(classification_ref,),
        predicate_result_refs=predicate_refs,
        authority_evaluation_refs=(authority_ref,),
        store_root=store_root,
    )[0]
    predicate_map = {
        ArtifactStore(store_root).read("autonomy_predicate_results", ref)["record"]["predicate_id"]: ArtifactStore(store_root).read("autonomy_predicate_results", ref)["record"]
        for ref in predicate_refs
    }
    eligibility = store.read("execution_eligibilities", eligibility_ref)["record"]
    assert store.read("autonomy_classifications", classification_ref)["record"]["autonomy_class"] == "auto_executable"
    assert predicate_map["no_critical_reconciliation_break"]["result"] is False
    assert "CRITICAL_RECONCILIATION_BREAK" in eligibility["blocking_reasons"]
    assert eligibility["eligible_for_execution"] is False


def test_authority_scope_mismatch_blocks_execution(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    reconciliation = _material_tax_reconciliation(store_root)
    store = ArtifactStore(store_root)
    recommendation_ref = _recommendation_ref_by_type(
        store,
        reconciliation["recommendation_refs"],
        "correction_recommendations",
        "action_type",
        "broker_review_required",
    )
    candidate_ref = ad_api.generate_candidate_actions(
        correction_recommendation_refs=(recommendation_ref,),
        store_root=store_root,
    )[0]
    classification_ref = ad_api.classify_candidate_actions(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    authority_ref = ad_api.assess_authority(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    predicate_refs = ad_api.evaluate_action_predicates(
        candidate_action_refs=(candidate_ref,),
        authority_evaluation_refs=(authority_ref,),
        classification_refs=(classification_ref,),
        store_root=store_root,
    )
    eligibility_ref = ad_api.determine_execution_eligibility(
        candidate_action_refs=(candidate_ref,),
        classification_refs=(classification_ref,),
        predicate_result_refs=predicate_refs,
        authority_evaluation_refs=(authority_ref,),
        store_root=store_root,
    )[0]
    predicate_map = {
        store.read("autonomy_predicate_results", ref)["record"]["predicate_id"]: store.read("autonomy_predicate_results", ref)["record"]
        for ref in predicate_refs
    }
    assert predicate_map["authority_scope_allows_action"]["result"] is False
    assert "AUTHORITY_SCOPE_MISMATCH" in store.read("execution_eligibilities", eligibility_ref)["record"]["blocking_reasons"]


def test_active_valid_authority_permits_scoped_action_evaluation(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    candidate_ref = _write_manual_candidate_action(
        store_root,
        action_family="freeze_execution_family",
        target_surface="execution",
    )
    authority_ref = ad_api.assess_authority(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    authority = ArtifactStore(store_root).read("authority_evaluations", authority_ref)["record"]
    assert authority["allowed"] is True
    assert authority["active_snapshot_id"] == chain["snapshot_id"]
    assert authority["authority_scope"]["allowed_scopes"] == ["position"]


def test_mismatched_interpreter_or_graph_blocks_allowed_true(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    candidate_ref = _write_manual_candidate_action(
        store_root,
        action_family="freeze_execution_family",
        target_surface="execution",
    )
    authority_ref = ad_api.assess_authority(
        candidate_action_refs=(candidate_ref,),
        expected_graph_hash="wrong-graph",
        expected_interpreter_version="constellation.meta_governance.interpreter.v999",
        store_root=store_root,
    )[0]
    authority = ArtifactStore(store_root).read("authority_evaluations", authority_ref)["record"]
    assert chain["graph_hash"]
    assert authority["allowed"] is False
    assert "GRAPH_HASH_MISMATCH" in authority["blocking_conditions"]
    assert "INTERPRETER_VERSION_MISMATCH" in authority["blocking_conditions"]


def test_predicate_serialization_is_stable(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    candidate_ref = _write_manual_candidate_action(
        store_root,
        action_family="freeze_execution_family",
        target_surface="execution",
    )
    classification_ref = ad_api.classify_candidate_actions(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    authority_ref = ad_api.assess_authority(candidate_action_refs=(candidate_ref,), store_root=store_root)[0]
    left = ad_api.evaluate_action_predicates(
        candidate_action_refs=(candidate_ref,),
        authority_evaluation_refs=(authority_ref,),
        classification_refs=(classification_ref,),
        store_root=store_root,
    )
    right = ad_api.evaluate_action_predicates(
        candidate_action_refs=(candidate_ref,),
        authority_evaluation_refs=(authority_ref,),
        classification_refs=(classification_ref,),
        store_root=store_root,
    )
    assert left == right


def test_higher_impact_urgent_action_outranks_low_impact_advisory_action(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    urgent_ref = _write_manual_candidate_action(
        store_root,
        action_family="freeze_execution_family",
        target_surface="execution",
        source_refs=("urgent",),
    )
    advisory_ref = _write_manual_candidate_action(
        store_root,
        action_family="no_immediate_action",
        target_surface="global",
        source_refs=("advisory",),
    )
    classification_refs = ad_api.classify_candidate_actions(candidate_action_refs=(urgent_ref, advisory_ref), store_root=store_root)
    prioritized_refs = ad_api.prioritize_actions(
        candidate_action_refs=(urgent_ref, advisory_ref),
        classification_refs=classification_refs,
        exposure_overrides={
            urgent_ref: {"impact_class": "critical"},
            advisory_ref: {"impact_class": "low"},
        },
        store_root=store_root,
    )
    store = ArtifactStore(store_root)
    first = store.read("prioritized_actions", prioritized_refs[0])["record"]
    second = store.read("prioritized_actions", prioritized_refs[1])["record"]
    assert first["candidate_action_ref"] == urgent_ref
    assert first["final_rank"] < second["final_rank"]


def test_missing_required_exposure_inputs_fail_closed_in_strict_mode(tmp_path: Path) -> None:
    store_root, _ = _build_active_runtime(tmp_path)
    candidate_ref = _write_manual_candidate_action(
        store_root,
        action_family="freeze_execution_family",
        target_surface="execution",
    )
    classification_refs = ad_api.classify_candidate_actions(candidate_action_refs=(candidate_ref,), store_root=store_root)
    with pytest.raises(ValueError, match=f"PRIORITIZATION_EXPOSURE_REQUIRED:{candidate_ref}"):
        ad_api.prioritize_actions(
            candidate_action_refs=(candidate_ref,),
            classification_refs=classification_refs,
            require_explicit_exposure=True,
            store_root=store_root,
        )


def test_action_plan_separates_classes_and_operator_view_is_deterministic(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    store = ArtifactStore(store_root)
    _, _, adaptive = _adaptive_execution_cycle(store_root, chain)
    adaptive_rec = _recommendation_ref_by_type(
        store,
        adaptive["recommendation_refs"],
        "adaptation_recommendations",
        "recommendation_type",
        "tighten_execution_guardrails",
    )
    blocked_reconciliation = _critical_execution_reconciliation(store_root)
    blocked_rec = _recommendation_ref_by_type(
        store,
        blocked_reconciliation["recommendation_refs"],
        "correction_recommendations",
        "action_type",
        "freeze_governed_execution",
    )
    advisory_regime_ref = _seed_insufficient_regime_signal(store_root, regime_id="plan-insufficient")
    advisory_candidate = _write_manual_candidate_action(
        store_root,
        action_family="freeze_execution_family",
        target_surface="execution",
        supporting_evidence_refs=(advisory_regime_ref,),
        source_refs=("advisory-plan",),
    )
    auto_candidate = _write_manual_candidate_action(
        store_root,
        action_family="freeze_execution_family",
        target_surface="execution",
        source_refs=("auto-plan",),
    )
    generated = ad_api.generate_candidate_actions(
        adaptation_recommendation_refs=(adaptive_rec,),
        correction_recommendation_refs=(blocked_rec,),
        store_root=store_root,
    )
    all_candidates = tuple(sorted(generated + (advisory_candidate, auto_candidate)))
    classification_refs = ad_api.classify_candidate_actions(candidate_action_refs=all_candidates, store_root=store_root)
    authority_refs = ad_api.assess_authority(candidate_action_refs=all_candidates, store_root=store_root)
    predicate_refs = ad_api.evaluate_action_predicates(
        candidate_action_refs=all_candidates,
        authority_evaluation_refs=authority_refs,
        classification_refs=classification_refs,
        store_root=store_root,
    )
    prioritized_refs = ad_api.prioritize_actions(
        candidate_action_refs=all_candidates,
        classification_refs=classification_refs,
        exposure_overrides={
            auto_candidate: {"impact_class": "high"},
            advisory_candidate: {"impact_class": "low"},
        },
        store_root=store_root,
    )
    eligibility_refs = ad_api.determine_execution_eligibility(
        candidate_action_refs=all_candidates,
        classification_refs=classification_refs,
        predicate_result_refs=predicate_refs,
        authority_evaluation_refs=authority_refs,
        store_root=store_root,
    )
    left_view = ad_api.build_operator_action_view(
        prioritized_action_refs=prioritized_refs,
        execution_eligibility_refs=eligibility_refs,
        as_of="2026-04-01T00:00:00Z",
        store_root=store_root,
    )
    right_view = ad_api.build_operator_action_view(
        prioritized_action_refs=prioritized_refs,
        execution_eligibility_refs=eligibility_refs,
        as_of="2026-04-01T00:00:00Z",
        store_root=store_root,
    )
    plan_ref = ad_api.build_action_plan(
        prioritized_action_refs=prioritized_refs,
        execution_eligibility_refs=eligibility_refs,
        generated_at="2026-04-01T00:00:00Z",
        store_root=store_root,
    )
    plan = store.read("autonomous_action_plans", plan_ref)["record"]
    view = store.read("operator_action_views", left_view)["record"]
    assert left_view == right_view
    assert advisory_candidate in plan["advisory_refs"]
    assert auto_candidate in plan["auto_executable_refs"]
    assert any(store.read("candidate_actions", ref)["record"]["action_family"] == "tighten_guardrail" for ref in plan["approval_required_refs"])
    assert any(store.read("candidate_actions", ref)["record"]["action_family"] == "freeze_execution_family" for ref in plan["blocked_refs"])
    assert view["top_priorities"]
    blocked_refs = set(view["blocked_actions"])
    assert blocked_refs.issuperset(set(plan["blocked_refs"]))


def test_runtime_lineage_resolves_autonomous_artifacts_and_audit_events(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    verified_snapshot_id, verification, adaptive = _adaptive_execution_cycle(store_root, chain)
    reconciliation = _critical_execution_reconciliation(store_root)
    store = ArtifactStore(store_root)
    result = ad_api.run_autonomous_cycle(
        adaptation_recommendation_refs=adaptive["recommendation_refs"],
        correction_recommendation_refs=reconciliation["recommendation_refs"],
        realized_validation_refs=_persistent_execution_validations(store_root, verification),
        exposure_overrides={},
        store_root=store_root,
    )
    mg_api.record_decision_lineage("decision-auto", verified_snapshot_id, store_root=store_root)
    lineage = mg_api.lineage_for_decision("decision-auto", store_root=store_root)
    assert result["bundle_ref"] in {bundle["record"]["bundle_id"] for bundle in lineage["autonomous_bundles"]}
    assert lineage["candidate_actions"]
    assert lineage["autonomy_classifications"]
    assert lineage["autonomous_action_plans"]
    assert lineage["operator_action_views"]
    event_types = {
        entry["event_type"]
        for entry in read_canonical_audit_stream(store)
        if entry.get("stream_type") == "governed_audit_event"
    }
    assert {
        "candidate_action_created",
        "candidate_action_classified",
        "authority_evaluated",
        "autonomy_predicate_evaluated",
        "candidate_action_prioritized",
        "execution_eligibility_determined",
        "operator_action_view_created",
        "autonomous_action_plan_created",
        "autonomous_decision_bundle_created",
    }.issubset(event_types)


def test_autonomous_artifacts_are_append_only(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _, verification, adaptive = _adaptive_execution_cycle(store_root, chain)
    reconciliation = _critical_execution_reconciliation(store_root)
    result = ad_api.run_autonomous_cycle(
        adaptation_recommendation_refs=adaptive["recommendation_refs"],
        correction_recommendation_refs=reconciliation["recommendation_refs"],
        realized_validation_refs=_persistent_execution_validations(store_root, verification),
        store_root=store_root,
    )
    store = ArtifactStore(store_root)
    bundle = store.read("autonomous_decision_bundles", result["bundle_ref"])
    modified = dict(bundle["record"])
    modified["action_plan_ref"] = "different-plan"
    with pytest.raises(ValueError, match="IMMUTABLE_CONFLICT"):
        store.write_immutable(
            "autonomous_decision_bundles",
            result["bundle_ref"],
            modified,
            artifact_type="AutonomousDecisionBundle",
            created_at=bundle["created_at"],
        )
