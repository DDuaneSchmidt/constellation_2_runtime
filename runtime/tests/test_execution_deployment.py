from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPO_ROOT = Path("/home/node/constellation")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from runtime.autonomous_decision.schemas import content_hash as autonomous_hash
from runtime.autonomous_decision.types import (
    AuthorityEvaluation,
    AutonomousActionPlan,
    AutonomousDecisionBundle,
    AutonomyClassification,
    ExecutionEligibility,
    OperatorActionView,
    PrioritizedAction,
)
from runtime.execution_deployment import execution_api as ex_api
from runtime.execution_deployment.execution_state_machine import current_state
from runtime.execution_deployment.schemas import content_hash as execution_hash
from runtime.execution_deployment.types import ExecutionIntent
from runtime.meta_governance import api as mg_api
from runtime.meta_governance.audit_log import read_canonical_audit_stream
from runtime.meta_governance.interpreter_version import get_interpreter_version
from runtime.meta_governance.predicate_gates import REQUIRED_PREDICATES
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
        justification="seed execution fixture",
        hypothesis="establish active runtime authority",
        expected_benefit="deterministic execution baseline",
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


def _seed_account_state(store_root: Path) -> str:
    store = ArtifactStore(store_root)
    _append_runtime_event(
        store,
        timestamp="2026-01-01T00:00:01Z",
        event={
            "type": "POSITION_SNAPSHOT",
            "snapshot_id": "pos-1",
            "positions": [{"account_id": "ACC1", "position_id": "POS1", "symbol": "AAPL", "quantity": 100}],
        },
    )
    return ri_api.capture_internal_snapshot(store_root=store_root)


def _write_candidate_action(
    store_root: Path,
    *,
    action_family: str,
    target_surface: str,
    rationale: str = "execution candidate rationale",
    supporting_evidence_refs: tuple[str, ...] = (),
    source_refs: tuple[str, ...] = ("execution-source",),
    estimated_scope: dict | None = None,
) -> str:
    store = ArtifactStore(store_root)
    estimated_scope = estimated_scope or {"reversible": False}
    payload = {
        "action_family": action_family,
        "target_surface": target_surface,
        "source_refs": source_refs,
        "proposed_effect": "execute governed portfolio action",
        "rationale": rationale,
        "supporting_evidence_refs": supporting_evidence_refs,
        "estimated_blast_radius": "moderate",
        "estimated_scope": estimated_scope,
    }
    artifact_hash = autonomous_hash(payload)
    record = {
        "candidate_action_id": f"candidate-action-{artifact_hash[:12]}",
        "action_family": action_family,
        "target_surface": target_surface,
        "source_refs": source_refs,
        "proposed_effect": payload["proposed_effect"],
        "rationale": rationale,
        "supporting_evidence_refs": supporting_evidence_refs,
        "estimated_blast_radius": "moderate",
        "estimated_scope": estimated_scope,
        "artifact_hash": artifact_hash,
    }
    store.write_immutable("candidate_actions", record["candidate_action_id"], record, artifact_type="CandidateAction", created_at="2026-03-01T00:00:00Z")
    return record["candidate_action_id"]


def _write_autonomous_fixture(
    store_root: Path,
    chain: dict,
    *,
    action_family: str = "close_position_candidate",
    target_surface: str = "execution",
    eligible: bool = True,
    supporting_evidence_refs: tuple[str, ...] = (),
) -> dict:
    store = ArtifactStore(store_root)
    candidate_ref = _write_candidate_action(
        store_root,
        action_family=action_family,
        target_surface=target_surface,
        supporting_evidence_refs=supporting_evidence_refs,
        estimated_scope={"reversible": False},
    )
    classification_payload = {
        "candidate_action_id": candidate_ref,
        "autonomy_class": "auto_executable" if eligible else "advisory_only",
        "classification_reasons": ("manual_fixture",),
        "confidence_class": "high" if eligible else "low",
        "evidence_completeness": "strong" if eligible else "partial",
        "operator_visibility": "review_required",
    }
    classification = AutonomyClassification(
        candidate_action_id=candidate_ref,
        autonomy_class=classification_payload["autonomy_class"],
        classification_reasons=classification_payload["classification_reasons"],
        confidence_class=classification_payload["confidence_class"],
        evidence_completeness=classification_payload["evidence_completeness"],
        operator_visibility=classification_payload["operator_visibility"],
        artifact_hash=autonomous_hash(classification_payload),
    )
    store.write_immutable("autonomy_classifications", candidate_ref, classification, artifact_type="AutonomyClassification", created_at="2026-03-01T00:00:00Z")
    authority_payload = {
        "candidate_action_id": candidate_ref,
        "active_snapshot_id": chain["snapshot_id"],
        "graph_hash": chain["graph_hash"],
        "interpreter_version": get_interpreter_version(),
        "authority_scope": {"allowed_actions": ("EXIT",), "allowed_scopes": ("position",)},
        "blocking_conditions": (),
        "allowed": True,
    }
    authority = AuthorityEvaluation(
        candidate_action_id=candidate_ref,
        active_snapshot_id=chain["snapshot_id"],
        graph_hash=chain["graph_hash"],
        interpreter_version=get_interpreter_version(),
        authority_scope={"allowed_actions": ("EXIT",), "allowed_scopes": ("position",)},
        blocking_conditions=(),
        allowed=True,
        artifact_hash=autonomous_hash(authority_payload),
    )
    store.write_immutable("authority_evaluations", candidate_ref, authority, artifact_type="AuthorityEvaluation", created_at="2026-03-01T00:00:00Z")
    eligibility_payload = {
        "candidate_action_id": candidate_ref,
        "autonomy_class": classification.autonomy_class,
        "eligible_for_execution": eligible,
        "blocking_reasons": () if eligible else ("ADVISORY_ONLY",),
        "required_approval_refs": (),
    }
    eligibility = ExecutionEligibility(
        candidate_action_id=candidate_ref,
        autonomy_class=classification.autonomy_class,
        eligible_for_execution=eligible,
        blocking_reasons=() if eligible else ("ADVISORY_ONLY",),
        required_approval_refs=(),
        artifact_hash=autonomous_hash(eligibility_payload),
    )
    store.write_immutable("execution_eligibilities", candidate_ref, eligibility, artifact_type="ExecutionEligibility", created_at="2026-03-01T00:00:00Z")
    prioritized = PrioritizedAction(
        prioritized_action_id=f"prioritized-action-{candidate_ref}",
        candidate_action_ref=candidate_ref,
        autonomy_class=classification.autonomy_class,
        urgency_class="urgent" if eligible else "informational",
        impact_class="high" if eligible else "low",
        rank_components={"impact_score": 4, "urgency_score": 3 if eligible else 0, "autonomy_score": 2 if eligible else 0, "reversibility_score": 0, "evidence_quality_score": 3 if eligible else 1, "score_value": 12 if eligible else 1},
        final_rank=1,
        explanation="manual autonomous execution fixture",
    )
    store.write_immutable("prioritized_actions", prioritized.prioritized_action_id, prioritized, artifact_type="PrioritizedAction", created_at="2026-03-01T00:00:00Z")
    operator_view_payload = {
        "as_of": "2026-03-01T00:00:00Z",
        "advisory_actions": () if eligible else (candidate_ref,),
        "approval_required_actions": (),
        "auto_executable_actions": (candidate_ref,) if eligible else (),
        "blocked_actions": (),
        "top_priorities": (prioritized.prioritized_action_id,),
    }
    operator_view = OperatorActionView(
        operator_action_view_id=f"operator-action-view-{autonomous_hash(operator_view_payload)[:12]}",
        as_of="2026-03-01T00:00:00Z",
        advisory_actions=operator_view_payload["advisory_actions"],
        approval_required_actions=(),
        auto_executable_actions=operator_view_payload["auto_executable_actions"],
        blocked_actions=(),
        top_priorities=operator_view_payload["top_priorities"],
        view_hash=autonomous_hash(operator_view_payload),
    )
    store.write_immutable("operator_action_views", operator_view.operator_action_view_id, operator_view, artifact_type="OperatorActionView", created_at=operator_view.as_of)
    plan_payload = {
        "active_snapshot_id": chain["snapshot_id"],
        "action_refs": (candidate_ref,),
        "advisory_refs": operator_view.advisory_actions,
        "approval_required_refs": (),
        "auto_executable_refs": operator_view.auto_executable_actions,
        "blocked_refs": (),
        "generated_at": "2026-03-01T00:00:00Z",
    }
    action_plan = AutonomousActionPlan(
        action_plan_id=f"autonomous-plan-{autonomous_hash(plan_payload)[:12]}",
        active_snapshot_id=chain["snapshot_id"],
        action_refs=(candidate_ref,),
        advisory_refs=operator_view.advisory_actions,
        approval_required_refs=(),
        auto_executable_refs=operator_view.auto_executable_actions,
        blocked_refs=(),
        generated_at="2026-03-01T00:00:00Z",
        artifact_hash=autonomous_hash(plan_payload),
    )
    store.write_immutable("autonomous_action_plans", action_plan.action_plan_id, action_plan, artifact_type="AutonomousActionPlan", created_at=action_plan.generated_at)
    bundle_payload = {
        "candidate_action_refs": (candidate_ref,),
        "autonomy_classification_refs": (candidate_ref,),
        "predicate_result_refs": (),
        "authority_evaluation_refs": (candidate_ref,),
        "prioritized_action_refs": (prioritized.prioritized_action_id,),
        "execution_eligibility_refs": (candidate_ref,),
        "operator_action_view_ref": operator_view.operator_action_view_id,
        "action_plan_ref": action_plan.action_plan_id,
    }
    bundle = AutonomousDecisionBundle(
        bundle_id=f"autonomous-bundle-{autonomous_hash(bundle_payload)[:12]}",
        candidate_action_refs=(candidate_ref,),
        autonomy_classification_refs=(candidate_ref,),
        predicate_result_refs=(),
        authority_evaluation_refs=(candidate_ref,),
        prioritized_action_refs=(prioritized.prioritized_action_id,),
        execution_eligibility_refs=(candidate_ref,),
        operator_action_view_ref=operator_view.operator_action_view_id,
        action_plan_ref=action_plan.action_plan_id,
        artifact_hash=autonomous_hash(bundle_payload),
    )
    store.write_immutable("autonomous_decision_bundles", bundle.bundle_id, bundle, artifact_type="AutonomousDecisionBundle", created_at="2026-03-01T00:00:00Z")
    return {
        "candidate_action_ref": candidate_ref,
        "execution_eligibility_ref": candidate_ref,
        "action_plan_ref": action_plan.action_plan_id,
        "bundle_ref": bundle.bundle_id,
    }


def _intent_inputs(missing: tuple[str, ...] = ()) -> dict[str, dict]:
    payload = {
        "target_account": "ACC1",
        "target_symbol": "AAPL",
        "target_side": "SELL",
        "target_quantity": 100.0,
        "target_order_type": "MKT",
        "target_time_in_force": "DAY",
    }
    for field in missing:
        payload[field] = None
    return {"payload": payload}


def _correction_recommendation(store_root: Path, *, recommendation_id: str, action_type: str) -> str:
    ArtifactStore(store_root).write_immutable(
        "correction_recommendations",
        recommendation_id,
        {
            "reconciliation_id": f"reconciliation-{recommendation_id}",
            "recommendation_id": recommendation_id,
            "target_surface": "executions",
            "action_type": action_type,
            "action_priority": "p1",
            "requires_human_review": True,
            "rationale": "manual correction fixture",
        },
        artifact_type="CorrectionRecommendation",
        created_at="2026-03-01T00:00:00Z",
    )
    return recommendation_id


def _manual_execution_intent(store_root: Path, *, active_snapshot_id: str | None = None) -> str:
    store = ArtifactStore(store_root)
    candidate_ref = _write_candidate_action(
        store_root,
        action_family="close_position_candidate",
        target_surface="execution",
        source_refs=("manual-intent-source",),
    )
    eligibility_payload = {
        "candidate_action_id": candidate_ref,
        "autonomy_class": "auto_executable",
        "eligible_for_execution": True,
        "blocking_reasons": (),
        "required_approval_refs": (),
    }
    store.write_immutable(
        "execution_eligibilities",
        candidate_ref,
        ExecutionEligibility(
            candidate_action_id=candidate_ref,
            autonomy_class="auto_executable",
            eligible_for_execution=True,
            blocking_reasons=(),
            required_approval_refs=(),
            artifact_hash=autonomous_hash(eligibility_payload),
        ),
        artifact_type="ExecutionEligibility",
        created_at="2026-03-01T00:00:00Z",
    )
    payload = {
        "candidate_action_ref": candidate_ref,
        "action_plan_ref": "manual-plan",
        "active_snapshot_id": active_snapshot_id or "",
        "graph_hash": "manual-graph",
        "interpreter_version": get_interpreter_version(),
        "target_account": "ACC1",
        "target_symbol": "AAPL",
        "target_side": "SELL",
        "target_quantity": 10.0,
        "target_order_type": "MKT",
        "target_limit_price": None,
        "target_time_in_force": "DAY",
        "rationale": "manual intent",
        "supporting_evidence_refs": (),
    }
    artifact_hash = execution_hash(payload)
    intent = ExecutionIntent(
        execution_intent_id=f"execution-intent-{artifact_hash[:12]}",
        candidate_action_ref=candidate_ref,
        action_plan_ref="manual-plan",
        active_snapshot_id=active_snapshot_id or "",
        graph_hash="manual-graph",
        interpreter_version=get_interpreter_version(),
        target_account="ACC1",
        target_symbol="AAPL",
        target_side="SELL",
        target_quantity=10.0,
        target_order_type="MKT",
        target_limit_price=None,
        target_time_in_force="DAY",
        rationale="manual intent",
        supporting_evidence_refs=(),
        artifact_hash=artifact_hash,
    )
    ArtifactStore(store_root).write_immutable("execution_intents", intent.execution_intent_id, intent, artifact_type="ExecutionIntent", created_at="2026-03-01T00:00:00Z")
    return intent.execution_intent_id


def test_eligible_autonomous_action_yields_execution_intent(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    intent = ArtifactStore(store_root).read("execution_intents", intent_ref)["record"]
    assert intent["candidate_action_ref"] == autonomous["candidate_action_ref"]
    assert intent["target_symbol"] == "AAPL"


def test_non_executable_action_does_not_yield_intent(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    autonomous = _write_autonomous_fixture(store_root, chain, action_family="no_immediate_action", target_surface="global")
    refs = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )
    assert refs == ()


def test_intent_hashing_is_deterministic(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    left = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )
    right = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )
    assert left == right


def test_invalid_snapshot_blocks_submission(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    intent_ref = _manual_execution_intent(store_root)
    result = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    gate_map = {ArtifactStore(store_root).read("pre_execution_gate_results", ref)["record"]["gate_id"]: ArtifactStore(store_root).read("pre_execution_gate_results", ref)["record"] for ref in result["gate_result_refs"]}
    submission = ArtifactStore(store_root).read("execution_submissions", result["submission_ref"])["record"]
    assert gate_map["active_snapshot_valid"]["result"] is False
    assert submission["status"] == "rejected_local"


def test_unresolved_broker_truth_conflict_blocks_submission(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    broker_ref = _correction_recommendation(store_root, recommendation_id="broker-conflict", action_type="broker_review_required")
    autonomous = _write_autonomous_fixture(store_root, chain, supporting_evidence_refs=(broker_ref,))
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    result = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    failures = [ArtifactStore(store_root).read("pre_execution_gate_results", ref)["record"]["failure_reason"] for ref in result["gate_result_refs"]]
    assert "BROKER_TRUTH_CONFLICT" in failures


def test_duplicate_submission_detected_and_blocked(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    first = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    second = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    failures = [ArtifactStore(store_root).read("pre_execution_gate_results", ref)["record"]["failure_reason"] for ref in second["gate_result_refs"]]
    assert first["submission_ref"] != second["submission_ref"]
    assert "DUPLICATE_SUBMISSION_PRESENT" in failures


def test_missing_account_symbol_or_quantity_blocks_submission(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs(missing=("target_symbol", "target_quantity"))["payload"]},
        store_root=store_root,
    )[0]
    result = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    failures = [ArtifactStore(store_root).read("pre_execution_gate_results", ref)["record"]["failure_reason"] for ref in result["gate_result_refs"]]
    assert "ORDER_COORDINATES_INCOMPLETE" in failures


def test_unhealthy_deployment_state_blocks_submission_when_required(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    deployment = ex_api.build_deployment_runtime(runtime_mode="live_disabled", store_root=store_root)
    result = ex_api.submit_execution_intent(
        execution_intent_ref=intent_ref,
        runtime_mode="paper",
        deployment_state_ref=deployment["deployment_state_ref"],
        store_root=store_root,
    )
    failures = [ArtifactStore(store_root).read("pre_execution_gate_results", ref)["record"]["failure_reason"] for ref in result["gate_result_refs"]]
    assert "DEPLOYMENT_STATE_BLOCKS_SUBMISSION" in failures


def test_validated_paper_mode_submission_records_submission_artifact(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    result = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    store = ArtifactStore(store_root)
    submission = store.read("execution_submissions", result["submission_ref"])["record"]
    assert submission["status"] == "submitted"
    assert result["receipt_refs"]


def test_blocked_submission_records_rejected_local_path(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    broker_ref = _correction_recommendation(store_root, recommendation_id="freeze-1", action_type="freeze_governed_execution")
    autonomous = _write_autonomous_fixture(store_root, chain, supporting_evidence_refs=(broker_ref,))
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    result = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    assert ArtifactStore(store_root).read("execution_submissions", result["submission_ref"])["record"]["status"] == "rejected_local"
    assert current_state(ArtifactStore(store_root), result["submission_ref"]) == "locally_blocked"


def test_live_disabled_mode_fails_closed(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    result = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="live_disabled", store_root=store_root)
    failures = [ArtifactStore(store_root).read("pre_execution_gate_results", ref)["record"]["failure_reason"] for ref in result["gate_result_refs"]]
    assert "LIVE_EXECUTION_DISABLED" in failures or "PAPER_ADAPTER_RUNTIME_MODE_MISMATCH:live_disabled" in failures


def test_valid_transitions_succeed_and_are_recorded(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    result = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    receipt_ref, transition_ref = ex_api.ingest_execution_receipt(
        submission_ref=result["submission_ref"],
        receipt_type="partial_fill",
        received_at="2026-04-01T00:00:05Z",
        external_execution_ref="paper-fill-1",
        order_status="PARTIAL",
        filled_quantity=40.0,
        remaining_quantity=60.0,
        average_fill_price=101.25,
        fee_amount=1.0,
        message="partial fill",
        store_root=store_root,
    )
    assert receipt_ref
    assert current_state(ArtifactStore(store_root), result["submission_ref"]) == "partially_filled"
    assert ArtifactStore(store_root).read("execution_state_transitions", transition_ref)["record"]["new_state"] == "partially_filled"


def test_invalid_transition_fails_closed(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    result = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    ex_api.ingest_execution_receipt(
        submission_ref=result["submission_ref"],
        receipt_type="full_fill",
        received_at="2026-04-01T00:00:05Z",
        external_execution_ref="paper-fill-2",
        order_status="FILLED",
        filled_quantity=100.0,
        remaining_quantity=0.0,
        average_fill_price=101.5,
        fee_amount=1.2,
        message="full fill",
        store_root=store_root,
    )
    with pytest.raises(ValueError, match="INVALID_EXECUTION_TRANSITION"):
        ex_api.ingest_execution_receipt(
            submission_ref=result["submission_ref"],
            receipt_type="cancel_ack",
            received_at="2026-04-01T00:00:06Z",
            external_execution_ref="paper-fill-2",
            order_status="CANCELED",
            filled_quantity=100.0,
            remaining_quantity=0.0,
            average_fill_price=101.5,
            fee_amount=1.2,
            message="invalid cancel after fill",
            store_root=store_root,
        )


def test_partial_fill_then_full_fill_transitions_correctly(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    result = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    ex_api.ingest_execution_receipt(
        submission_ref=result["submission_ref"],
        receipt_type="partial_fill",
        received_at="2026-04-01T00:00:05Z",
        external_execution_ref="paper-fill-3",
        order_status="PARTIAL",
        filled_quantity=40.0,
        remaining_quantity=60.0,
        average_fill_price=101.0,
        fee_amount=1.0,
        message="partial fill",
        store_root=store_root,
    )
    ex_api.ingest_execution_receipt(
        submission_ref=result["submission_ref"],
        receipt_type="full_fill",
        received_at="2026-04-01T00:00:06Z",
        external_execution_ref="paper-fill-4",
        order_status="FILLED",
        filled_quantity=60.0,
        remaining_quantity=0.0,
        average_fill_price=101.4,
        fee_amount=0.5,
        message="full fill",
        store_root=store_root,
    )
    assert current_state(ArtifactStore(store_root), result["submission_ref"]) == "fully_filled"


def test_reject_transition_recorded_deterministically(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    result = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    left = ex_api.ingest_execution_receipt(
        submission_ref=result["submission_ref"],
        receipt_type="reject",
        received_at="2026-04-01T00:00:05Z",
        external_execution_ref="paper-reject-1",
        order_status="REJECTED",
        filled_quantity=0.0,
        remaining_quantity=100.0,
        average_fill_price=None,
        fee_amount=None,
        message="reject",
        store_root=store_root,
    )
    right = ex_api.ingest_execution_receipt(
        submission_ref=result["submission_ref"],
        receipt_type="reject",
        received_at="2026-04-01T00:00:05Z",
        external_execution_ref="paper-reject-1",
        order_status="REJECTED",
        filled_quantity=0.0,
        remaining_quantity=100.0,
        average_fill_price=None,
        fee_amount=None,
        message="reject",
        store_root=store_root,
    )
    assert left == right


def test_open_submissions_are_recovered_and_duplicate_resubmission_blocked(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    first = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    recovery_ref = ex_api.recover_execution_state(as_of="2026-04-01T00:01:00Z", store_root=store_root)
    recovery = ArtifactStore(store_root).read("execution_recovery_records", recovery_ref)["record"]
    second = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    failures = [ArtifactStore(store_root).read("pre_execution_gate_results", ref)["record"]["failure_reason"] for ref in second["gate_result_refs"]]
    assert first["submission_ref"] in recovery["open_submission_refs"]
    assert recovery["unresolved_refs"]
    assert "DUPLICATE_SUBMISSION_PRESENT" in failures


def test_degraded_component_yields_blocked_or_recovering_deployment_state(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    recovery_ref = ex_api.recover_execution_state(as_of="2026-04-01T00:01:00Z", store_root=store_root)
    deployment = ex_api.build_deployment_runtime(runtime_mode="paper", recovery_ref=recovery_ref, store_root=store_root)
    state = ArtifactStore(store_root).read("deployment_states", deployment["deployment_state_ref"])["record"]
    assert state["service_state"] in {"recovering", "degraded", "blocked"}


def test_startup_gate_failure_blocks_live_active_submission(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    intent_ref = _manual_execution_intent(store_root)
    deployment = ex_api.build_deployment_runtime(runtime_mode="live_active", store_root=store_root)
    result = ex_api.submit_execution_intent(
        execution_intent_ref=intent_ref,
        runtime_mode="live_active",
        deployment_state_ref=deployment["deployment_state_ref"],
        store_root=store_root,
    )
    failures = [ArtifactStore(store_root).read("pre_execution_gate_results", ref)["record"]["failure_reason"] for ref in result["gate_result_refs"]]
    assert "DEPLOYMENT_STATE_BLOCKS_SUBMISSION" in failures or "ACTIVE_SNAPSHOT_REQUIRED" in failures


def test_operator_execution_view_includes_blocked_reasons_and_pending_items_and_is_deterministic(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    good = _write_autonomous_fixture(store_root, chain)
    bad = _write_autonomous_fixture(store_root, chain, supporting_evidence_refs=(_correction_recommendation(store_root, recommendation_id="freeze-op", action_type="freeze_governed_execution"),))
    good_intent = ex_api.create_execution_intents(
        candidate_action_refs=(good["candidate_action_ref"],),
        action_plan_ref=good["action_plan_ref"],
        execution_eligibility_refs=(good["execution_eligibility_ref"],),
        intent_inputs_by_action={good["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    bad_intent = ex_api.create_execution_intents(
        candidate_action_refs=(bad["candidate_action_ref"],),
        action_plan_ref=bad["action_plan_ref"],
        execution_eligibility_refs=(bad["execution_eligibility_ref"],),
        intent_inputs_by_action={bad["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    ex_api.submit_execution_intent(execution_intent_ref=good_intent, runtime_mode="paper", store_root=store_root)
    ex_api.submit_execution_intent(execution_intent_ref=bad_intent, runtime_mode="paper", store_root=store_root)
    recovery_ref = ex_api.recover_execution_state(as_of="2026-04-01T00:01:00Z", store_root=store_root)
    deployment = ex_api.build_deployment_runtime(runtime_mode="paper", recovery_ref=recovery_ref, as_of="2026-04-01T00:02:00Z", store_root=store_root)
    left = ex_api.build_execution_operator_view(
        deployment_state_ref=deployment["deployment_state_ref"],
        recovery_refs=(recovery_ref,),
        as_of="2026-04-01T00:02:00Z",
        store_root=store_root,
    )
    right = ex_api.build_execution_operator_view(
        deployment_state_ref=deployment["deployment_state_ref"],
        recovery_refs=(recovery_ref,),
        as_of="2026-04-01T00:02:00Z",
        store_root=store_root,
    )
    view = ArtifactStore(store_root).read("execution_operator_views", left)["record"]
    assert left == right
    assert view["pending_submissions"]
    assert view["blocked_intents"]
    assert view["top_execution_risks"]


def test_execution_bundle_stored_and_recoverable_via_lineage_and_audit(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    result = ex_api.run_execution_cycle(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        runtime_mode="paper",
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        submit=True,
        store_root=store_root,
    )
    mg_api.record_decision_lineage("decision-exec", chain["snapshot_id"], store_root=store_root)
    lineage = mg_api.lineage_for_decision("decision-exec", store_root=store_root)
    store = ArtifactStore(store_root)
    assert result["bundle_ref"] in {bundle["record"]["bundle_id"] for bundle in lineage["execution_bundles"]}
    assert lineage["execution_intents"]
    assert lineage["execution_submissions"]
    assert lineage["execution_receipts"]
    assert ex_api.find_execution_bundles(snapshot_id=chain["snapshot_id"], store_root=store_root)
    event_types = {
        entry["event_type"]
        for entry in read_canonical_audit_stream(store)
        if entry.get("stream_type") == "governed_audit_event"
    }
    assert {
        "execution_intent_created",
        "pre_execution_gate_evaluated",
        "execution_submission_recorded",
        "deployment_state_recorded",
        "execution_deployment_bundle_created",
    }.issubset(event_types)


def test_execution_artifacts_are_append_only(tmp_path: Path) -> None:
    store_root, chain = _build_active_runtime(tmp_path)
    _seed_account_state(store_root)
    autonomous = _write_autonomous_fixture(store_root, chain)
    intent_ref = ex_api.create_execution_intents(
        candidate_action_refs=(autonomous["candidate_action_ref"],),
        action_plan_ref=autonomous["action_plan_ref"],
        execution_eligibility_refs=(autonomous["execution_eligibility_ref"],),
        intent_inputs_by_action={autonomous["candidate_action_ref"]: _intent_inputs()["payload"]},
        store_root=store_root,
    )[0]
    result = ex_api.submit_execution_intent(execution_intent_ref=intent_ref, runtime_mode="paper", store_root=store_root)
    store = ArtifactStore(store_root)
    submission = store.read("execution_submissions", result["submission_ref"])
    mutated = dict(submission["record"])
    mutated["status"] = "rejected_local"
    with pytest.raises(ValueError, match="IMMUTABLE_CONFLICT"):
        store.write_immutable(
            "execution_submissions",
            result["submission_ref"],
            mutated,
            artifact_type="ExecutionSubmission",
            created_at=submission["created_at"],
        )
