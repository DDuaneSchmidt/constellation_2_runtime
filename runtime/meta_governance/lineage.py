from __future__ import annotations

from .proposal_store import get_latest_proposal
from .store import ArtifactStore
from .types import RuntimeDecisionLineage


def _related_adaptation_bundle(store: ArtifactStore, bundle: dict, known_refs: set[str]) -> bool:
    bundle_record = bundle["record"]
    for drift_ref in bundle_record["drift_signal_refs"]:
        signal = store.read("drift_signals", drift_ref)["record"]
        if set(signal["source_refs"]) & known_refs:
            return True
    for regime_ref in bundle_record["regime_signal_refs"]:
        signal = store.read("regime_signals", regime_ref)["record"]
        if set(signal["source_refs"]) & known_refs:
            return True
    for impact_ref in bundle_record["impact_assessment_refs"]:
        assessment = store.read("impact_assessments", impact_ref)["record"]
        if set(assessment["source_refs"]) & known_refs:
            return True
    for issue_ref in bundle_record["ranked_issue_refs"]:
        issue = store.read("ranked_issues", issue_ref)["record"]
        if set(issue["source_refs"]) & known_refs:
            return True
    for recommendation_ref in bundle_record["recommendation_refs"]:
        recommendation = store.read("adaptation_recommendations", recommendation_ref)["record"]
        if set(recommendation["source_refs"]) & known_refs:
            return True
    for candidate_ref in bundle_record["proposal_candidate_refs"]:
        candidate = store.read("proposal_candidates", candidate_ref)["record"]
        if set(candidate["source_refs"]) & known_refs or set(candidate["evidence_bundle_refs"]) & known_refs:
            return True
    return False


def _related_autonomous_bundle(store: ArtifactStore, bundle: dict, known_refs: set[str]) -> bool:
    bundle_record = bundle["record"]
    for action_ref in bundle_record["candidate_action_refs"]:
        action = store.read("candidate_actions", action_ref)["record"]
        if set(action["source_refs"]) & known_refs or set(action["supporting_evidence_refs"]) & known_refs:
            return True
    for classification_ref in bundle_record["autonomy_classification_refs"]:
        classification = store.read("autonomy_classifications", classification_ref)["record"]
        if classification["candidate_action_id"] in known_refs:
            return True
    for authority_ref in bundle_record["authority_evaluation_refs"]:
        authority = store.read("authority_evaluations", authority_ref)["record"]
        if authority["active_snapshot_id"] in known_refs or authority["graph_hash"] in known_refs:
            return True
    for prioritized_ref in bundle_record["prioritized_action_refs"]:
        prioritized = store.read("prioritized_actions", prioritized_ref)["record"]
        if prioritized["candidate_action_ref"] in known_refs:
            return True
    for eligibility_ref in bundle_record["execution_eligibility_refs"]:
        eligibility = store.read("execution_eligibilities", eligibility_ref)["record"]
        if eligibility["candidate_action_id"] in known_refs:
            return True
    return False


def _related_execution_bundle(store: ArtifactStore, bundle: dict, known_refs: set[str]) -> bool:
    bundle_record = bundle["record"]
    for intent_ref in bundle_record["intent_refs"]:
        intent = store.read("execution_intents", intent_ref)["record"]
        if (
            intent["candidate_action_ref"] in known_refs
            or intent["action_plan_ref"] in known_refs
            or intent["active_snapshot_id"] in known_refs
            or intent["graph_hash"] in known_refs
            or set(intent["supporting_evidence_refs"]) & known_refs
        ):
            return True
    for submission_ref in bundle_record["submission_refs"]:
        submission = store.read("execution_submissions", submission_ref)["record"]
        if submission["execution_intent_ref"] in known_refs:
            return True
    for receipt_ref in bundle_record["receipt_refs"]:
        receipt = store.read("execution_receipts", receipt_ref)["record"]
        if receipt["submission_ref"] in known_refs or (receipt["external_execution_ref"] and receipt["external_execution_ref"] in known_refs):
            return True
    for transition_ref in bundle_record["state_transition_refs"]:
        transition = store.read("execution_state_transitions", transition_ref)["record"]
        if transition["submission_ref"] in known_refs or (transition["receipt_ref"] and transition["receipt_ref"] in known_refs):
            return True
    for recovery_ref in bundle_record["recovery_refs"]:
        recovery = store.read("execution_recovery_records", recovery_ref)["record"]
        if set(recovery["open_submission_refs"]) & known_refs or set(recovery["unresolved_refs"]) & known_refs:
            return True
    deployment = store.read("deployment_states", bundle_record["deployment_state_ref"])["record"]
    if deployment["active_snapshot_id"] in known_refs:
        return True
    return False


def record_runtime_decision_lineage(store: ArtifactStore, decision_id: str, snapshot_id: str) -> str:
    snapshot = store.read("activation_snapshots", snapshot_id)
    graph = store.read("canonical_graphs", snapshot["record"]["graph_id"])
    proposal_refs: set[str] = set()
    for approval_ref in snapshot["record"]["approval_refs"]:
        approval = store.read("approvals", approval_ref)
        proposal_refs.add(approval["record"]["proposal_id"])
    for evaluation_ref in snapshot["record"]["evaluation_refs"]:
        evaluation = store.read("evaluation_artifacts", evaluation_ref)
        proposal_id = evaluation["record"].get("proposal_id")
        if proposal_id:
            proposal_refs.add(proposal_id)
    lineage = RuntimeDecisionLineage(
        decision_id=decision_id,
        snapshot_id=snapshot_id,
        graph_hash=graph["record"]["graph_hash"],
        module_refs=tuple(sorted(graph["record"]["active_module_versions"].keys())),
        parameter_refs=tuple(sorted(graph["record"]["active_parameter_versions"].keys())),
        proposal_refs=tuple(sorted(proposal_refs)),
        approval_refs=tuple(sorted(snapshot["record"]["approval_refs"])),
    )
    store.write_immutable(
        "lineage_records",
        decision_id,
        lineage,
        artifact_type="RuntimeDecisionLineage",
    )
    return decision_id


def lineage_for_decision(store: ArtifactStore, decision_id: str) -> dict:
    lineage = store.read("lineage_records", decision_id)
    snapshot = store.read("activation_snapshots", lineage["record"]["snapshot_id"])
    graph = store.read("canonical_graphs", snapshot["record"]["graph_id"])
    approvals = [store.read("approvals", approval_ref) for approval_ref in lineage["record"]["approval_refs"]]
    evaluations = [store.read("evaluation_artifacts", ref) for ref in snapshot["record"]["evaluation_refs"]]
    verification_bundles = [store.read("verification_bundles", ref) for ref in snapshot["record"].get("verification_refs", ())]
    proposals = [get_latest_proposal(store, proposal_id) for proposal_id in lineage["record"]["proposal_refs"]]
    modules = [
        store.read("governance_modules", f"{module_id}__{graph['record']['active_module_versions'][module_id]}")
        for module_id in lineage["record"]["module_refs"]
    ]
    parameters = [
        store.read("parameter_groups", f"{group_id}__{graph['record']['active_parameter_versions'][group_id]}")
        for group_id in lineage["record"]["parameter_refs"]
    ]
    verification_contexts = [store.read("verification_contexts", bundle["record"]["context_ref"]) for bundle in verification_bundles]
    decision_diffs = [store.read("decision_diff_artifacts", bundle["record"]["decision_diff_ref"]) for bundle in verification_bundles]
    impact_summaries = [store.read("impact_summaries", bundle["record"]["impact_summary_ref"]) for bundle in verification_bundles]
    interaction_results = [store.read("interaction_analysis_results", bundle["record"]["interaction_analysis_ref"]) for bundle in verification_bundles]
    expectation_records = [
        store.read("expectation_records", expectation_ref)
        for bundle in verification_bundles
        for expectation_ref in bundle["record"]["expectation_refs"]
    ]
    realized_validation_results = [
        store.read("realized_validation_results", validation_ref)
        for validation_ref in store.list_ids("realized_validation_results")
        if store.read("realized_validation_results", validation_ref)["record"]["expectation_id"] in {record["record"]["expectation_id"] for record in expectation_records}
    ]
    internal_reality_snapshots = [
        store.read("internal_reality_snapshots", snapshot_ref)
        for snapshot_ref in store.list_ids("internal_reality_snapshots")
        if store.read("internal_reality_snapshots", snapshot_ref)["record"]["runtime_snapshot_id"] == snapshot["record"]["snapshot_id"]
    ]
    reconciliation_bundles = [
        store.read("reconciliation_bundles", bundle_ref)
        for bundle_ref in store.list_ids("reconciliation_bundles")
        if any(
            internal_snapshot["record"]["internal_snapshot_id"] == store.read("reconciliation_bundles", bundle_ref)["record"]["internal_snapshot_ref"]
            for internal_snapshot in internal_reality_snapshots
        )
    ]
    reconciliation_results = [
        store.read("reconciliation_results", bundle["record"]["result_ref"])
        for bundle in reconciliation_bundles
    ]
    discrepancy_classifications = [
        store.read("discrepancy_classifications", classification_ref)
        for bundle in reconciliation_bundles
        for classification_ref in bundle["record"]["discrepancy_refs"]
    ]
    correction_recommendations = [
        store.read("correction_recommendations", recommendation_ref)
        for bundle in reconciliation_bundles
        for recommendation_ref in bundle["record"]["recommendation_refs"]
    ]
    known_refs = {
        lineage["record"]["decision_id"],
        lineage["record"]["snapshot_id"],
        graph["record"]["graph_id"],
        graph["record"]["graph_hash"],
        *lineage["record"]["proposal_refs"],
        *lineage["record"]["approval_refs"],
        *snapshot["record"]["evaluation_refs"],
        *snapshot["record"].get("verification_refs", ()),
        *(bundle["record"]["context_ref"] for bundle in verification_bundles),
        *(bundle["record"]["decision_diff_ref"] for bundle in verification_bundles),
        *(bundle["record"]["impact_summary_ref"] for bundle in verification_bundles),
        *(bundle["record"]["interaction_analysis_ref"] for bundle in verification_bundles),
        *(record["record"]["expectation_id"] for record in expectation_records),
        *(record["record"]["expectation_id"] for record in realized_validation_results),
        *(snapshot_doc["record"]["internal_snapshot_id"] for snapshot_doc in internal_reality_snapshots),
        *(bundle["record"]["reconciliation_id"] for bundle in reconciliation_bundles),
        *(bundle["record"]["result_ref"] for bundle in reconciliation_bundles),
        *(ref["record"]["reconciliation_id"] for ref in discrepancy_classifications),
    }
    adaptation_bundles = [
        store.read("adaptation_bundles", bundle_ref)
        for bundle_ref in store.list_ids("adaptation_bundles")
        if _related_adaptation_bundle(store, store.read("adaptation_bundles", bundle_ref), known_refs)
    ]
    drift_signals = [
        store.read("drift_signals", drift_ref)
        for bundle in adaptation_bundles
        for drift_ref in bundle["record"]["drift_signal_refs"]
    ]
    regime_signals = [
        store.read("regime_signals", regime_ref)
        for bundle in adaptation_bundles
        for regime_ref in bundle["record"]["regime_signal_refs"]
    ]
    impact_assessments = [
        store.read("impact_assessments", impact_ref)
        for bundle in adaptation_bundles
        for impact_ref in bundle["record"]["impact_assessment_refs"]
    ]
    ranked_issues = [
        store.read("ranked_issues", issue_ref)
        for bundle in adaptation_bundles
        for issue_ref in bundle["record"]["ranked_issue_refs"]
    ]
    adaptation_recommendations = [
        store.read("adaptation_recommendations", recommendation_ref)
        for bundle in adaptation_bundles
        for recommendation_ref in bundle["record"]["recommendation_refs"]
    ]
    proposal_candidates = [
        store.read("proposal_candidates", candidate_ref)
        for bundle in adaptation_bundles
        for candidate_ref in bundle["record"]["proposal_candidate_refs"]
    ]
    operator_summaries = [
        store.read("operator_summaries", bundle["record"]["operator_summary_ref"])
        for bundle in adaptation_bundles
    ]
    known_refs.update(
        {
            *(bundle["record"]["adaptation_bundle_id"] for bundle in adaptation_bundles),
            *(record["record"]["drift_signal_id"] for record in drift_signals),
            *(record["record"]["regime_signal_id"] for record in regime_signals),
            *(record["record"]["impact_assessment_id"] for record in impact_assessments),
            *(record["record"]["ranked_issue_id"] for record in ranked_issues),
            *(record["record"]["recommendation_id"] for record in adaptation_recommendations),
            *(record["record"]["proposal_candidate_id"] for record in proposal_candidates),
            *(record["record"]["summary_id"] for record in operator_summaries),
        }
    )
    autonomous_bundles = [
        store.read("autonomous_decision_bundles", bundle_ref)
        for bundle_ref in store.list_ids("autonomous_decision_bundles")
        if _related_autonomous_bundle(store, store.read("autonomous_decision_bundles", bundle_ref), known_refs)
    ]
    candidate_actions = [
        store.read("candidate_actions", action_ref)
        for bundle in autonomous_bundles
        for action_ref in bundle["record"]["candidate_action_refs"]
    ]
    autonomy_classifications = [
        store.read("autonomy_classifications", classification_ref)
        for bundle in autonomous_bundles
        for classification_ref in bundle["record"]["autonomy_classification_refs"]
    ]
    autonomy_predicate_results = [
        store.read("autonomy_predicate_results", predicate_ref)
        for bundle in autonomous_bundles
        for predicate_ref in bundle["record"]["predicate_result_refs"]
    ]
    authority_evaluations = [
        store.read("authority_evaluations", authority_ref)
        for bundle in autonomous_bundles
        for authority_ref in bundle["record"]["authority_evaluation_refs"]
    ]
    prioritized_actions = [
        store.read("prioritized_actions", prioritized_ref)
        for bundle in autonomous_bundles
        for prioritized_ref in bundle["record"]["prioritized_action_refs"]
    ]
    execution_eligibilities = [
        store.read("execution_eligibilities", eligibility_ref)
        for bundle in autonomous_bundles
        for eligibility_ref in bundle["record"]["execution_eligibility_refs"]
    ]
    operator_action_views = [
        store.read("operator_action_views", bundle["record"]["operator_action_view_ref"])
        for bundle in autonomous_bundles
    ]
    autonomous_action_plans = [
        store.read("autonomous_action_plans", bundle["record"]["action_plan_ref"])
        for bundle in autonomous_bundles
    ]
    known_refs.update(
        {
            *(bundle["record"]["bundle_id"] for bundle in autonomous_bundles),
            *(record["record"]["candidate_action_id"] for record in candidate_actions),
            *(record["record"]["candidate_action_id"] for record in autonomy_classifications),
            *(record["record"]["candidate_action_id"] for record in authority_evaluations),
            *(record["record"]["prioritized_action_id"] for record in prioritized_actions),
            *(record["record"]["candidate_action_id"] for record in execution_eligibilities),
            *(record["record"]["operator_action_view_id"] for record in operator_action_views),
            *(record["record"]["action_plan_id"] for record in autonomous_action_plans),
        }
    )
    execution_bundles = [
        store.read("execution_deployment_bundles", bundle_ref)
        for bundle_ref in store.list_ids("execution_deployment_bundles")
        if _related_execution_bundle(store, store.read("execution_deployment_bundles", bundle_ref), known_refs)
    ]
    execution_intents = [
        store.read("execution_intents", intent_ref)
        for bundle in execution_bundles
        for intent_ref in bundle["record"]["intent_refs"]
    ]
    pre_execution_gate_results = [
        store.read("pre_execution_gate_results", gate_ref)
        for bundle in execution_bundles
        for gate_ref in bundle["record"]["gate_result_refs"]
    ]
    execution_submissions = [
        store.read("execution_submissions", submission_ref)
        for bundle in execution_bundles
        for submission_ref in bundle["record"]["submission_refs"]
    ]
    execution_receipts = [
        store.read("execution_receipts", receipt_ref)
        for bundle in execution_bundles
        for receipt_ref in bundle["record"]["receipt_refs"]
    ]
    execution_state_transitions = [
        store.read("execution_state_transitions", transition_ref)
        for bundle in execution_bundles
        for transition_ref in bundle["record"]["state_transition_refs"]
    ]
    execution_recovery_records = [
        store.read("execution_recovery_records", recovery_ref)
        for bundle in execution_bundles
        for recovery_ref in bundle["record"]["recovery_refs"]
    ]
    deployment_states = [
        store.read("deployment_states", bundle["record"]["deployment_state_ref"])
        for bundle in execution_bundles
    ]
    health_check_results = [
        store.read("health_check_results", health_ref)
        for bundle in execution_bundles
        for health_ref in bundle["record"]["health_check_refs"]
    ]
    execution_operator_views = [
        store.read("execution_operator_views", bundle["record"]["operator_view_ref"])
        for bundle in execution_bundles
    ]
    return {
        "lineage": lineage,
        "snapshot": snapshot,
        "graph": graph,
        "modules": modules,
        "parameter_groups": parameters,
        "proposals": proposals,
        "approvals": approvals,
        "evaluations": evaluations,
        "verification_bundles": verification_bundles,
        "verification_contexts": verification_contexts,
        "decision_diff_artifacts": decision_diffs,
        "impact_summaries": impact_summaries,
        "interaction_results": interaction_results,
        "expectation_records": expectation_records,
        "realized_validation_results": realized_validation_results,
        "internal_reality_snapshots": internal_reality_snapshots,
        "reconciliation_bundles": reconciliation_bundles,
        "reconciliation_results": reconciliation_results,
        "discrepancy_classifications": discrepancy_classifications,
        "correction_recommendations": correction_recommendations,
        "adaptation_bundles": adaptation_bundles,
        "drift_signals": drift_signals,
        "regime_signals": regime_signals,
        "impact_assessments": impact_assessments,
        "ranked_issues": ranked_issues,
        "adaptation_recommendations": adaptation_recommendations,
        "proposal_candidates": proposal_candidates,
        "operator_summaries": operator_summaries,
        "autonomous_bundles": autonomous_bundles,
        "candidate_actions": candidate_actions,
        "autonomy_classifications": autonomy_classifications,
        "autonomy_predicate_results": autonomy_predicate_results,
        "authority_evaluations": authority_evaluations,
        "prioritized_actions": prioritized_actions,
        "execution_eligibilities": execution_eligibilities,
        "operator_action_views": operator_action_views,
        "autonomous_action_plans": autonomous_action_plans,
        "execution_bundles": execution_bundles,
        "execution_intents": execution_intents,
        "pre_execution_gate_results": pre_execution_gate_results,
        "execution_submissions": execution_submissions,
        "execution_receipts": execution_receipts,
        "execution_state_transitions": execution_state_transitions,
        "execution_recovery_records": execution_recovery_records,
        "deployment_states": deployment_states,
        "health_check_results": health_check_results,
        "execution_operator_views": execution_operator_views,
    }
