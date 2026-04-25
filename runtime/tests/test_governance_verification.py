from __future__ import annotations

from pathlib import Path
import sys

import pytest

REPO_ROOT = Path("/home/node/constellation")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from runtime.governance_verification import verification_api as gv_api
from runtime.meta_governance import api as mg_api
from runtime.meta_governance.predicate_gates import REQUIRED_PREDICATES
from runtime.meta_governance.schemas import utc_now
from runtime.meta_governance.store import ArtifactStore
from runtime.meta_governance.types import ActivationSnapshot, ApprovalDecision, PredicateEvaluation


def _policy(
    version: str,
    *,
    risk_weight: float = 0.7,
    tax_weight: float = 0.1,
    allocation_weight: float = 0.2,
    min_score: float = 0.5,
    max_actions: int = 10,
    cooldown_enabled: bool = False,
    cooldown_seconds: int = 60,
    block_same_action: bool = False,
    autonomy_actions: tuple[str, ...] = ("EXIT",),
    execution_scopes: tuple[str, ...] = ("position",),
) -> dict:
    return {
        "version": version,
        "weights": {
            "risk": risk_weight,
            "tax": tax_weight,
            "allocation": allocation_weight,
        },
        "limits": {
            "max_actions_per_cycle": max_actions,
            "min_decision_score": min_score,
        },
        "cooldown": {
            "enabled": cooldown_enabled,
            "block_same_action": block_same_action,
            "min_seconds_between_same_action": cooldown_seconds,
        },
        "autonomy": {
            "allowed_actions": list(autonomy_actions),
        },
        "execution": {
            "allowed_scopes": list(execution_scopes),
        },
    }


def _events(
    *,
    with_recent_exit: bool = False,
    second_position: bool = False,
) -> list[dict]:
    positions = [
        {"position_id": "POS1", "symbol": "AAPL", "quantity": 100, "risk_flag": "CRITICAL"},
    ]
    if second_position:
        positions.append({"position_id": "POS2", "symbol": "MSFT", "quantity": 50, "risk_flag": "CRITICAL"})
    records = [
        {"timestamp": "2026-01-01T00:00:00Z", "ts_epoch": 0, "event": {"type": "POSITION_SNAPSHOT", "positions": positions}},
        {"timestamp": "2026-01-01T00:00:01Z", "ts_epoch": 1, "event": {"type": "INTENT_SNAPSHOT", "intent": {"target": "NORMAL_OPERATION"}}},
    ]
    if with_recent_exit:
        records.extend(
            [
                {
                    "timestamp": "2026-01-01T00:00:02Z",
                    "ts_epoch": 2,
                    "event": {"type": "POSITION_ACTION_DECIDED", "position_id": "POS1", "action": "EXIT"},
                },
                {
                    "timestamp": "2026-01-01T00:00:03Z",
                    "ts_epoch": 3,
                    "event": {"type": "EXECUTION_RESULT", "position_id": "POS1", "action": "EXIT", "status": "FILLED", "filled_qty": 0},
                },
            ]
        )
    return records


def _create_seed_proposal(store_root: Path, proposal_id: str = "proposal-verification") -> str:
    return mg_api.create_proposal(
        proposal_id=proposal_id,
        target_type="module",
        target_id="runtime_policy",
        target_tier="tier_2",
        from_version="v1",
        to_version="v2",
        authored_by="tester",
        justification="verification coverage",
        hypothesis="candidate policy changes governed behavior",
        expected_benefit="safe policy evolution",
        worst_case_downside="bad governed decisions",
        reversal_criteria="rollback",
        review_deadline="2026-12-31T00:00:00Z",
        dependency_impact={"modules": ["runtime_policy"]},
        proposed_scope={"domains": ["risk", "tax", "allocation"]},
        store_root=store_root,
    )


def _write_passed_evaluations(store: ArtifactStore, proposal_id: str, artifact_id: str = "ok-eval") -> str:
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
    )
    return artifact_id


def _write_approval(store: ArtifactStore, proposal_id: str) -> str:
    store.write_immutable(
        "approvals",
        proposal_id,
        ApprovalDecision(
            proposal_id=proposal_id,
            required_approval_level="human_critical",
            approved_by="human.reviewer",
            approved_at=utc_now(),
            reviewed_artifact_hashes=("artifact-hash",),
            notes="approved for verification fixtures",
        ),
        artifact_type="ApprovalDecision",
    )
    return proposal_id


def _write_root_snapshot(store: ArtifactStore, graph_id: str, graph_hash: str, approval_ref: str, evaluation_ref: str) -> str:
    snapshot = ActivationSnapshot(
        snapshot_id="root-snapshot",
        graph_id=graph_id,
        graph_hash=graph_hash,
        interpreter_version=mg_api.get_interpreter_version(),
        active_at=utc_now(),
        activated_by="system",
        activation_scope="root",
        approval_refs=(approval_ref,),
        evaluation_refs=(evaluation_ref,),
        verification_refs=(),
        rollback_snapshot_ref="root-snapshot",
        prior_snapshot_id=None,
        lifecycle_state="approved",
    )
    store.write_immutable("activation_snapshots", snapshot.snapshot_id, snapshot, artifact_type="ActivationSnapshot")
    return snapshot.snapshot_id


def _build_snapshot_pair(
    tmp_path: Path,
    *,
    active_policy: dict,
    candidate_policy: dict,
    candidate_version: str = "v2",
    proposal_id: str = "proposal-verification",
) -> tuple[Path, dict]:
    store_root = tmp_path / "store"
    store = ArtifactStore(store_root)
    mg_api.register_module(
        module_id="runtime_policy",
        tier="tier_2",
        semantic_domain="risk",
        version="v1",
        module_content=active_policy,
        store_root=store_root,
    )
    mg_api.register_module(
        module_id="runtime_policy",
        tier="tier_2",
        semantic_domain="risk",
        version=candidate_version,
        module_content=candidate_policy,
        store_root=store_root,
    )
    _create_seed_proposal(store_root, proposal_id=proposal_id)
    approval_ref = _write_approval(store, proposal_id)
    evaluation_ref = _write_passed_evaluations(store, proposal_id)
    active_graph_id = mg_api.compile_canonical_graph(active_modules=[("runtime_policy", "v1")], store_root=store_root)
    candidate_graph_id = mg_api.compile_canonical_graph(active_modules=[("runtime_policy", candidate_version)], store_root=store_root)
    active_graph = store.read("canonical_graphs", active_graph_id)
    root_snapshot_id = _write_root_snapshot(store, active_graph_id, active_graph["record"]["graph_hash"], approval_ref, evaluation_ref)
    active_snapshot_id = mg_api.create_activation_snapshot(
        graph_id=active_graph_id,
        activated_by="human.reviewer",
        activation_scope="runtime",
        approval_refs=(approval_ref,),
        evaluation_refs=(evaluation_ref,),
        rollback_snapshot_ref=root_snapshot_id,
        prior_snapshot_id=root_snapshot_id,
        store_root=store_root,
    )
    mg_api.activate_snapshot(active_snapshot_id, actor="human.reviewer", store_root=store_root)
    candidate_snapshot_id = mg_api.create_activation_snapshot(
        graph_id=candidate_graph_id,
        activated_by="human.reviewer",
        activation_scope="runtime",
        approval_refs=(approval_ref,),
        evaluation_refs=(evaluation_ref,),
        rollback_snapshot_ref=active_snapshot_id,
        prior_snapshot_id=active_snapshot_id,
        store_root=store_root,
    )
    return store_root, {
        "proposal_id": proposal_id,
        "approval_ref": approval_ref,
        "evaluation_ref": evaluation_ref,
        "active_graph_id": active_graph_id,
        "candidate_graph_id": candidate_graph_id,
        "root_snapshot_id": root_snapshot_id,
        "active_snapshot_id": active_snapshot_id,
        "candidate_snapshot_id": candidate_snapshot_id,
    }


def _register_decision_dataset(store_root: Path, dataset_id: str, events: list[dict]) -> str:
    return gv_api.register_dataset(
        dataset_id=dataset_id,
        dataset_type="historical_decision_inputs",
        source_description="deterministic governed replay inputs",
        time_range={"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T00:01:00Z"},
        dataset={"events": events},
        store_root=store_root,
    )


def test_same_snapshot_vs_same_snapshot_produces_zero_deltas(tmp_path: Path) -> None:
    policy = _policy("v1")
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=policy, candidate_policy=policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-same", _events())
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["active_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 0.0, "max_allowed_turnover_increase": 0.0, "max_allowed_tax_degradation": 0.0, "max_allowed_decision_churn": 0.0}},
        store_root=store_root,
    )
    diff = ArtifactStore(store_root).read("decision_diff_artifacts", result["decision_diff_ref"])
    assert diff["record"]["decision_count_changed"] == 0
    assert diff["record"]["decision_deltas"] == []


def test_candidate_snapshot_with_known_rule_change_produces_expected_decision_deltas(tmp_path: Path) -> None:
    active_policy = _policy("v1", cooldown_enabled=False, block_same_action=False)
    candidate_policy = _policy("v2", cooldown_enabled=True, block_same_action=True, cooldown_seconds=9999999999)
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=active_policy, candidate_policy=candidate_policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-cooldown", _events(with_recent_exit=True))
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["candidate_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 1.0, "max_allowed_turnover_increase": 1.0, "max_allowed_tax_degradation": 1.0, "max_allowed_decision_churn": 10}},
        store_root=store_root,
    )
    diff = ArtifactStore(store_root).read("decision_diff_artifacts", result["decision_diff_ref"])
    assert diff["record"]["decision_count_changed"] == 1
    assert diff["record"]["decision_deltas"][0]["delta_type"] == "removed"
    assert diff["record"]["decision_deltas"][0]["decision_key"] == "POS1::EXIT"


def test_decision_delta_categories_are_stable_and_correct(tmp_path: Path) -> None:
    active_policy = _policy("v1", min_score=0.9)
    candidate_policy = _policy("v2", min_score=0.5)
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=active_policy, candidate_policy=candidate_policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-allowed", _events())
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["candidate_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 1.0, "max_allowed_turnover_increase": 1.0, "max_allowed_tax_degradation": 1.0, "max_allowed_decision_churn": 10}},
        store_root=store_root,
    )
    diff = ArtifactStore(store_root).read("decision_diff_artifacts", result["decision_diff_ref"])
    assert diff["record"]["decision_deltas"][0]["delta_type"] == "newly_allowed"
    assert diff["record"]["decision_deltas"][0]["materiality_class"] == "critical"


def test_risk_threshold_breach_fails_invariant(tmp_path: Path) -> None:
    active_policy = _policy("v1", risk_weight=0.7)
    candidate_policy = _policy("v2", risk_weight=1.0)
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=active_policy, candidate_policy=candidate_policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-risk", _events())
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["candidate_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 0.1, "max_allowed_turnover_increase": 5.0, "max_allowed_tax_degradation": 5.0, "max_allowed_decision_churn": 5}},
        store_root=store_root,
    )
    store = ArtifactStore(store_root)
    risk_result = store.read("behavioral_invariant_results", f"{result['verification_id']}__max_allowed_risk_increase")
    assert risk_result["record"]["result"] is False
    assert risk_result["record"]["failure_reason"] == "risk increase exceeds budget"


def test_turnover_threshold_breach_fails_invariant(tmp_path: Path) -> None:
    active_policy = _policy("v1", cooldown_enabled=True, block_same_action=True, cooldown_seconds=9999999999)
    candidate_policy = _policy("v2", cooldown_enabled=False, block_same_action=False)
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=active_policy, candidate_policy=candidate_policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-turnover", _events(with_recent_exit=True))
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["candidate_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 5.0, "max_allowed_turnover_increase": 0.0, "max_allowed_tax_degradation": 5.0, "max_allowed_decision_churn": 5}},
        store_root=store_root,
    )
    turnover_result = ArtifactStore(store_root).read("behavioral_invariant_results", f"{result['verification_id']}__max_allowed_turnover_increase")
    assert turnover_result["record"]["result"] is False


def test_forbidden_autonomy_broadening_fails_invariant(tmp_path: Path) -> None:
    active_policy = _policy("v1", autonomy_actions=("EXIT",))
    candidate_policy = _policy("v2", autonomy_actions=("EXIT", "ENTER"))
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=active_policy, candidate_policy=candidate_policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-autonomy", _events())
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["candidate_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 5.0, "max_allowed_turnover_increase": 5.0, "max_allowed_tax_degradation": 5.0, "max_allowed_decision_churn": 5, "allow_new_autonomy_classes": False}},
        store_root=store_root,
    )
    autonomy_result = ArtifactStore(store_root).read("behavioral_invariant_results", f"{result['verification_id']}__no_forbidden_autonomy_broadening")
    assert autonomy_result["record"]["result"] is False


def test_allowed_small_behavioral_change_passes_invariant(tmp_path: Path) -> None:
    active_policy = _policy("v1", risk_weight=0.7)
    candidate_policy = _policy("v2", risk_weight=0.72)
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=active_policy, candidate_policy=candidate_policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-small", _events())
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["candidate_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 0.05, "max_allowed_turnover_increase": 1.0, "max_allowed_tax_degradation": 1.0, "max_allowed_decision_churn": 1}},
        store_root=store_root,
    )
    risk_result = ArtifactStore(store_root).read("behavioral_invariant_results", f"{result['verification_id']}__max_allowed_risk_increase")
    assert risk_result["record"]["result"] is True


def test_summary_reflects_expected_changed_domains_and_decision_counts(tmp_path: Path) -> None:
    active_policy = _policy("v1", cooldown_enabled=False, block_same_action=False)
    candidate_policy = _policy("v2", cooldown_enabled=True, block_same_action=True, cooldown_seconds=9999999999)
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=active_policy, candidate_policy=candidate_policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-summary", _events(with_recent_exit=True))
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["candidate_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 5.0, "max_allowed_turnover_increase": 5.0, "max_allowed_tax_degradation": 5.0, "max_allowed_decision_churn": 10}},
        store_root=store_root,
    )
    summary = ArtifactStore(store_root).read("impact_summaries", result["impact_summary_ref"])
    assert summary["record"]["decision_surface_delta_count"] == 1
    assert "risk" in summary["record"]["major_changed_domains"]
    assert "execution" in summary["record"]["major_changed_domains"]


def test_summary_hashing_is_deterministic(tmp_path: Path) -> None:
    active_policy = _policy("v1")
    candidate_policy = _policy("v2", risk_weight=0.8)
    left_root, left_chain = _build_snapshot_pair(tmp_path / "left", active_policy=active_policy, candidate_policy=candidate_policy)
    right_root, right_chain = _build_snapshot_pair(tmp_path / "right", active_policy=active_policy, candidate_policy=candidate_policy)
    left_dataset = _register_decision_dataset(left_root, "dataset-left", _events())
    right_dataset = _register_decision_dataset(right_root, "dataset-right", _events())
    left = gv_api.run_verification(
        active_snapshot_id=left_chain["active_snapshot_id"],
        candidate_snapshot_id=left_chain["candidate_snapshot_id"],
        dataset_refs=(left_dataset,),
        requested_by="tester",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 5.0, "max_allowed_turnover_increase": 5.0, "max_allowed_tax_degradation": 5.0, "max_allowed_decision_churn": 10}},
        store_root=left_root,
    )
    right = gv_api.run_verification(
        active_snapshot_id=right_chain["active_snapshot_id"],
        candidate_snapshot_id=right_chain["candidate_snapshot_id"],
        dataset_refs=(right_dataset,),
        requested_by="tester",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 5.0, "max_allowed_turnover_increase": 5.0, "max_allowed_tax_degradation": 5.0, "max_allowed_decision_churn": 10}},
        store_root=right_root,
    )
    left_summary = ArtifactStore(left_root).read("impact_summaries", left["impact_summary_ref"])
    right_summary = ArtifactStore(right_root).read("impact_summaries", right["impact_summary_ref"])
    assert left_summary["record"]["artifact_hash"] == right_summary["record"]["artifact_hash"]


def test_two_harmless_isolated_changes_can_be_detected_as_harmful_in_combination(tmp_path: Path) -> None:
    active_policy = _policy("v1", autonomy_actions=("EXIT",), execution_scopes=("position",))
    component_a = _policy("v2", autonomy_actions=("EXIT", "ENTER"), execution_scopes=("position",))
    component_b = _policy("v3", autonomy_actions=("EXIT",), execution_scopes=("position", "portfolio"))
    combined = _policy("v4", autonomy_actions=("EXIT", "ENTER"), execution_scopes=("position", "portfolio"))
    store_root = tmp_path / "store"
    store = ArtifactStore(store_root)
    for version, policy in (("v1", active_policy), ("v2", component_a), ("v3", component_b), ("v4", combined)):
        mg_api.register_module(
            module_id="runtime_policy",
            tier="tier_2",
            semantic_domain="risk",
            version=version,
            module_content=policy,
            store_root=store_root,
        )
    _create_seed_proposal(store_root)
    approval_ref = _write_approval(store, "proposal-verification")
    evaluation_ref = _write_passed_evaluations(store, "proposal-verification")
    active_graph = mg_api.compile_canonical_graph(active_modules=[("runtime_policy", "v1")], store_root=store_root)
    graph_v2 = mg_api.compile_canonical_graph(active_modules=[("runtime_policy", "v2")], store_root=store_root)
    graph_v3 = mg_api.compile_canonical_graph(active_modules=[("runtime_policy", "v3")], store_root=store_root)
    graph_v4 = mg_api.compile_canonical_graph(active_modules=[("runtime_policy", "v4")], store_root=store_root)
    active_graph_doc = store.read("canonical_graphs", active_graph)
    root_snapshot_id = _write_root_snapshot(store, active_graph, active_graph_doc["record"]["graph_hash"], approval_ref, evaluation_ref)
    active_snapshot_id = mg_api.create_activation_snapshot(
        graph_id=active_graph,
        activated_by="human.reviewer",
        activation_scope="runtime",
        approval_refs=(approval_ref,),
        evaluation_refs=(evaluation_ref,),
        rollback_snapshot_ref=root_snapshot_id,
        prior_snapshot_id=root_snapshot_id,
        store_root=store_root,
    )
    mg_api.activate_snapshot(active_snapshot_id, actor="human.reviewer", store_root=store_root)
    snapshot_a = mg_api.create_activation_snapshot(
        graph_id=graph_v2,
        activated_by="human.reviewer",
        activation_scope="runtime",
        approval_refs=(approval_ref,),
        evaluation_refs=(evaluation_ref,),
        rollback_snapshot_ref=active_snapshot_id,
        prior_snapshot_id=active_snapshot_id,
        store_root=store_root,
    )
    snapshot_b = mg_api.create_activation_snapshot(
        graph_id=graph_v3,
        activated_by="human.reviewer",
        activation_scope="runtime",
        approval_refs=(approval_ref,),
        evaluation_refs=(evaluation_ref,),
        rollback_snapshot_ref=active_snapshot_id,
        prior_snapshot_id=active_snapshot_id,
        store_root=store_root,
    )
    combined_snapshot = mg_api.create_activation_snapshot(
        graph_id=graph_v4,
        activated_by="human.reviewer",
        activation_scope="runtime",
        approval_refs=(approval_ref,),
        evaluation_refs=(evaluation_ref,),
        rollback_snapshot_ref=active_snapshot_id,
        prior_snapshot_id=active_snapshot_id,
        store_root=store_root,
    )
    dataset_ref = _register_decision_dataset(store_root, "dataset-interaction", _events())
    result = gv_api.run_verification(
        active_snapshot_id=active_snapshot_id,
        candidate_snapshot_id=combined_snapshot,
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        scope={
            "behavioral_budgets": {
                "max_allowed_risk_increase": 5.0,
                "max_allowed_turnover_increase": 5.0,
                "max_allowed_tax_degradation": 5.0,
                "max_allowed_decision_churn": 5,
                "allow_new_autonomy_classes": True,
                "allow_execution_scope_increase": True,
            },
            "component_snapshot_ids": {"proposal-a": snapshot_a, "proposal-b": snapshot_b},
        },
        store_root=store_root,
    )
    interaction = store.read("interaction_analysis_results", result["interaction_analysis_ref"])
    assert interaction["record"]["pairwise_results"]
    assert interaction["record"]["combined_result"]["clean_verification"] is False
    assert interaction["record"]["nonlinear_effects_detected"] is True


def test_pairwise_interaction_result_is_persisted(tmp_path: Path) -> None:
    test_two_harmless_isolated_changes_can_be_detected_as_harmful_in_combination(tmp_path)


def test_combined_harmful_result_blocks_clean_verification_classification(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    test_two_harmless_isolated_changes_can_be_detected_as_harmful_in_combination(tmp_path)
    interactions = ArtifactStore(store_root).list_ids("interaction_analysis_results")
    interaction = ArtifactStore(store_root).read("interaction_analysis_results", interactions[0])
    assert interaction["record"]["blocked_reason"]


def test_verified_candidate_writes_expectation_records(tmp_path: Path) -> None:
    active_policy = _policy("v1")
    candidate_policy = _policy("v2", risk_weight=0.75)
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=active_policy, candidate_policy=candidate_policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-expect", _events())
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["candidate_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        proposal_id=chain["proposal_id"],
        review_window_start="2026-02-01T00:00:00Z",
        review_window_end="2026-02-28T00:00:00Z",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 5.0, "max_allowed_turnover_increase": 5.0, "max_allowed_tax_degradation": 5.0, "max_allowed_decision_churn": 5}},
        store_root=store_root,
    )
    assert result["expectation_refs"]
    first = ArtifactStore(store_root).read("expectation_records", result["expectation_refs"][0])
    assert first["record"]["snapshot_id"] == chain["candidate_snapshot_id"]
    assert first["record"]["proposal_id"] == chain["proposal_id"]


def test_actual_within_tolerance_returns_within_tolerance(tmp_path: Path) -> None:
    active_policy = _policy("v1")
    candidate_policy = _policy("v2", risk_weight=0.75)
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=active_policy, candidate_policy=candidate_policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-realized", _events())
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["candidate_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        proposal_id=chain["proposal_id"],
        review_window_start="2026-02-01T00:00:00Z",
        review_window_end="2026-02-28T00:00:00Z",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 5.0, "max_allowed_turnover_increase": 5.0, "max_allowed_tax_degradation": 5.0, "max_allowed_decision_churn": 5}},
        store_root=store_root,
    )
    validation_ref = gv_api.validate_realized_expectation(result["expectation_refs"][0], actual_value=0.03, store_root=store_root)
    validation = ArtifactStore(store_root).read("realized_validation_results", validation_ref)
    assert validation["record"]["drift_classification"] == "within_tolerance"
    assert validation["record"]["recommended_action"] == "no_action"


def test_actual_outside_tolerance_returns_correct_drift_classification(tmp_path: Path) -> None:
    active_policy = _policy("v1")
    candidate_policy = _policy("v2", risk_weight=0.75)
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=active_policy, candidate_policy=candidate_policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-realized-bad", _events())
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["candidate_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        proposal_id=chain["proposal_id"],
        review_window_start="2026-02-01T00:00:00Z",
        review_window_end="2026-02-28T00:00:00Z",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 5.0, "max_allowed_turnover_increase": 5.0, "max_allowed_tax_degradation": 5.0, "max_allowed_decision_churn": 5}},
        store_root=store_root,
    )
    validation_ref = gv_api.validate_realized_expectation(result["expectation_refs"][0], actual_value=0.4, store_root=store_root)
    validation = ArtifactStore(store_root).read("realized_validation_results", validation_ref)
    assert validation["record"]["drift_classification"] in {"material_drift", "severe_drift"}
    assert validation["record"]["recommended_action"] in {"rollback_review", "freeze_similar_changes"}


def test_review_recommendation_is_deterministic(tmp_path: Path) -> None:
    active_policy = _policy("v1")
    candidate_policy = _policy("v2", risk_weight=0.75)
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=active_policy, candidate_policy=candidate_policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-realized-deterministic", _events())
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["candidate_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        proposal_id=chain["proposal_id"],
        review_window_start="2026-02-01T00:00:00Z",
        review_window_end="2026-02-28T00:00:00Z",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 5.0, "max_allowed_turnover_increase": 5.0, "max_allowed_tax_degradation": 5.0, "max_allowed_decision_churn": 5}},
        store_root=store_root,
    )
    first = gv_api.validate_realized_expectation(result["expectation_refs"][0], actual_value=0.2, store_root=store_root)
    second = gv_api.validate_realized_expectation(result["expectation_refs"][0], actual_value=0.2, store_root=store_root)
    first_record = ArtifactStore(store_root).read("realized_validation_results", first)
    second_record = ArtifactStore(store_root).read("realized_validation_results", second)
    assert first_record["record"]["recommended_action"] == second_record["record"]["recommended_action"]


def test_verification_bundle_can_be_traced_from_proposal_to_activation_to_snapshot(tmp_path: Path) -> None:
    active_policy = _policy("v1")
    candidate_policy = _policy("v2", risk_weight=0.75)
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=active_policy, candidate_policy=candidate_policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-lineage", _events())
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["candidate_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        proposal_id=chain["proposal_id"],
        review_window_start="2026-02-01T00:00:00Z",
        review_window_end="2026-02-28T00:00:00Z",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 5.0, "max_allowed_turnover_increase": 5.0, "max_allowed_tax_degradation": 5.0, "max_allowed_decision_churn": 5}},
        store_root=store_root,
    )
    verified_snapshot_id = mg_api.create_activation_snapshot(
        graph_id=chain["candidate_graph_id"],
        activated_by="human.reviewer",
        activation_scope="runtime",
        approval_refs=(chain["approval_ref"],),
        evaluation_refs=(chain["evaluation_ref"],),
        verification_refs=(result["bundle_ref"],),
        rollback_snapshot_ref=chain["active_snapshot_id"],
        prior_snapshot_id=chain["active_snapshot_id"],
        store_root=store_root,
    )
    mg_api.activate_snapshot(verified_snapshot_id, actor="human.reviewer", store_root=store_root)
    mg_api.record_decision_lineage("decision-verification", verified_snapshot_id, store_root=store_root)
    lineage = mg_api.lineage_for_decision("decision-verification", store_root=store_root)
    assert lineage["verification_bundles"][0]["record"]["verification_id"] == result["verification_id"]
    assert lineage["verification_contexts"][0]["record"]["candidate_snapshot_id"] == chain["candidate_snapshot_id"]
    assert lineage["proposals"][0]["record"]["proposal_id"] == chain["proposal_id"]


def test_decision_diff_artifact_is_recoverable_from_lineage(tmp_path: Path) -> None:
    active_policy = _policy("v1")
    candidate_policy = _policy("v2", risk_weight=0.75)
    store_root, chain = _build_snapshot_pair(tmp_path, active_policy=active_policy, candidate_policy=candidate_policy)
    dataset_ref = _register_decision_dataset(store_root, "dataset-lineage-diff", _events())
    result = gv_api.run_verification(
        active_snapshot_id=chain["active_snapshot_id"],
        candidate_snapshot_id=chain["candidate_snapshot_id"],
        dataset_refs=(dataset_ref,),
        requested_by="tester",
        proposal_id=chain["proposal_id"],
        review_window_start="2026-02-01T00:00:00Z",
        review_window_end="2026-02-28T00:00:00Z",
        scope={"behavioral_budgets": {"max_allowed_risk_increase": 5.0, "max_allowed_turnover_increase": 5.0, "max_allowed_tax_degradation": 5.0, "max_allowed_decision_churn": 5}},
        store_root=store_root,
    )
    verified_snapshot_id = mg_api.create_activation_snapshot(
        graph_id=chain["candidate_graph_id"],
        activated_by="human.reviewer",
        activation_scope="runtime",
        approval_refs=(chain["approval_ref"],),
        evaluation_refs=(chain["evaluation_ref"],),
        verification_refs=(result["bundle_ref"],),
        rollback_snapshot_ref=chain["active_snapshot_id"],
        prior_snapshot_id=chain["active_snapshot_id"],
        store_root=store_root,
    )
    mg_api.activate_snapshot(verified_snapshot_id, actor="human.reviewer", store_root=store_root)
    mg_api.record_decision_lineage("decision-diff-lineage", verified_snapshot_id, store_root=store_root)
    lineage = mg_api.lineage_for_decision("decision-diff-lineage", store_root=store_root)
    assert lineage["decision_diff_artifacts"][0]["record"]["verification_id"] == result["verification_id"]


def test_verification_artifacts_are_append_only(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    gv_api.register_dataset(
        dataset_id="dataset-immutable",
        dataset_type="historical_decision_inputs",
        source_description="immutable test",
        time_range={"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T00:01:00Z"},
        dataset={"events": _events()},
        store_root=store_root,
    )
    with pytest.raises(ValueError, match="IMMUTABLE_CONFLICT"):
        gv_api.register_dataset(
            dataset_id="dataset-immutable",
            dataset_type="historical_decision_inputs",
            source_description="immutable test changed",
            time_range={"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T00:01:00Z"},
            dataset={"events": _events(with_recent_exit=True)},
            store_root=store_root,
        )
