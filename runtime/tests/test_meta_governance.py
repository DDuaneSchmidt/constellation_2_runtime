from __future__ import annotations

import importlib.util
import inspect
from pathlib import Path
import sys

import pytest

REPO_ROOT = Path("/home/node/constellation")
RUNTIME_ROOT = REPO_ROOT / "runtime"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from runtime.meta_governance import api
from runtime.meta_governance.audit_log import read_canonical_audit_stream
from runtime.meta_governance.interpreter_version import get_interpreter_version
from runtime.meta_governance.predicate_gates import REQUIRED_PREDICATES
from runtime.meta_governance.proposal_store import get_latest_proposal, transition_proposal
from runtime.meta_governance.schemas import content_hash, utc_now
from runtime.meta_governance.store import ArtifactStore
from runtime.meta_governance.mutation_protocols import fast_path_allowed
from runtime.meta_governance.types import ActivationSnapshot, ApprovalDecision, CanonicalGovernanceGraph


def _load_run_decision_cycle_module():
    module_path = Path("/home/node/constellation/runtime/run_decision_cycle.py")
    if str(RUNTIME_ROOT) not in sys.path:
        sys.path.insert(0, str(RUNTIME_ROOT))
    spec = importlib.util.spec_from_file_location("runtime_run_decision_cycle_test", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _policy_v1() -> dict:
    return {
        "cooldown": {
            "block_same_action": True,
            "enabled": True,
            "min_seconds_between_same_action": 60,
        },
        "limits": {
            "max_actions_per_cycle": 5,
            "min_decision_score": 0.1,
        },
        "version": 1,
        "weights": {
            "allocation": 0.2,
            "risk": 0.7,
            "tax": 0.1,
        },
    }


def _policy_v2() -> dict:
    updated = _policy_v1()
    updated["limits"]["max_actions_per_cycle"] = 6
    return updated


def _write_predicate_artifact(
    store: ArtifactStore,
    artifact_id: str,
    proposal_id: str,
    *,
    failing_predicate: str | None = None,
) -> str:
    evaluations = []
    for predicate_id in REQUIRED_PREDICATES:
        result = predicate_id != failing_predicate
        evaluations.append(
            {
                "proposal_id": proposal_id,
                "predicate_id": predicate_id,
                "result": result,
                "evidence_refs": ["evidence"],
                "failure_reason": "" if result else "forced_failure",
            }
        )
    store.write_immutable(
        "evaluation_artifacts",
        artifact_id,
        {"proposal_id": proposal_id, "evaluations": evaluations},
        artifact_type="PredicateEvaluations",
    )
    return artifact_id


def _write_root_snapshot(
    store: ArtifactStore,
    graph_id: str,
    graph_hash: str,
    approval_ref: str,
    evaluation_ref: str,
) -> str:
    root_snapshot = ActivationSnapshot(
        snapshot_id="seed-root",
        graph_id=graph_id,
        graph_hash=graph_hash,
        interpreter_version=get_interpreter_version(),
        active_at=utc_now(),
        activated_by="seed",
        activation_scope="seed",
        approval_refs=(approval_ref,),
        evaluation_refs=(evaluation_ref,),
        rollback_snapshot_ref="seed-root",
        prior_snapshot_id=None,
    )
    store.write_immutable(
        "activation_snapshots",
        root_snapshot.snapshot_id,
        root_snapshot,
        artifact_type="ActivationSnapshot",
    )
    return root_snapshot.snapshot_id


def _build_active_chain(tmp_path: Path) -> tuple[Path, dict]:
    store_root = tmp_path / "store"
    store = ArtifactStore(store_root)
    api.register_module(
        module_id="runtime_policy",
        tier="tier_2",
        semantic_domain="risk",
        version="v1",
        module_content=_policy_v1(),
        store_root=store_root,
    )
    graph_id = api.compile_canonical_graph(store_root=store_root)
    graph = store.read("canonical_graphs", graph_id)
    api.create_proposal(
        proposal_id="proposal-runtime",
        target_type="policy_module",
        target_id="runtime_policy",
        target_tier="tier_2",
        from_version="v1",
        to_version="v2",
        authored_by="operator",
        justification="bounded policy update",
        hypothesis="more capacity",
        expected_benefit="higher throughput",
        worst_case_downside="mis-sizing",
        reversal_criteria="rollback to prior snapshot",
        review_deadline="2026-04-30T00:00:00Z",
        dependency_impact={"capital_exposure_class": "standard"},
        proposed_scope={"domains": ["risk"]},
        store_root=store_root,
    )
    api.compute_proposal_diff("proposal-runtime", _policy_v1(), _policy_v2(), store_root=store_root)
    api.assess_blast_radius("proposal-runtime", semantic_domain="risk", store_root=store_root)
    transition_proposal(store, "proposal-runtime", "validated")
    approval_ref = api.approve_proposal(
        "proposal-runtime",
        approved_by="human.reviewer",
        reviewed_artifact_hashes=(graph["content_hash"],),
        store_root=store_root,
    )
    evaluation_ref = _write_predicate_artifact(store, "predicate_evaluations__proposal-runtime", "proposal-runtime")
    root_snapshot_id = _write_root_snapshot(store, graph_id, graph["record"]["graph_hash"], approval_ref, evaluation_ref)
    snapshot_id = api.create_activation_snapshot(
        graph_id=graph_id,
        activated_by="human.reviewer",
        activation_scope="runtime",
        approval_refs=(approval_ref,),
        evaluation_refs=(evaluation_ref,),
        rollback_snapshot_ref=root_snapshot_id,
        prior_snapshot_id=root_snapshot_id,
        store_root=store_root,
    )
    api.activate_snapshot(snapshot_id, actor="human.reviewer", store_root=store_root)
    return store_root, {
        "graph_id": graph_id,
        "graph": graph,
        "approval_ref": approval_ref,
        "evaluation_ref": evaluation_ref,
        "root_snapshot_id": root_snapshot_id,
        "snapshot_id": snapshot_id,
    }


def _set_store_root(monkeypatch: pytest.MonkeyPatch, store_root: Path) -> None:
    monkeypatch.setenv(api.STORE_ROOT_ENV, str(store_root))


def test_tier_0_mutation_requires_human_approval(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    api.create_proposal(
        proposal_id="proposal-tier0",
        target_type="invariant",
        target_id="constitution",
        target_tier="tier_0",
        from_version="v1",
        to_version="v2",
        authored_by="operator",
        justification="change invariant",
        hypothesis="none",
        expected_benefit="none",
        worst_case_downside="critical",
        reversal_criteria="rollback",
        review_deadline="2026-04-30T00:00:00Z",
        dependency_impact={"capital_exposure_class": "standard"},
        store_root=store_root,
    )
    api.compute_proposal_diff("proposal-tier0", {"invariant": {"enforced": True}}, {"invariant": {"enforced": False}}, store_root=store_root)
    api.assess_blast_radius("proposal-tier0", semantic_domain="governance", store_root=store_root)
    assert api.determine_required_approval("proposal-tier0", store_root=store_root) == "human_critical"


def test_tier_1_mutation_requires_human_approval(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    api.create_proposal(
        proposal_id="proposal-tier1",
        target_type="protocol",
        target_id="approval_policy",
        target_tier="tier_1",
        from_version="v1",
        to_version="v2",
        authored_by="operator",
        justification="protocol update",
        hypothesis="none",
        expected_benefit="none",
        worst_case_downside="critical",
        reversal_criteria="rollback",
        review_deadline="2026-04-30T00:00:00Z",
        dependency_impact={"capital_exposure_class": "standard"},
        store_root=store_root,
    )
    api.compute_proposal_diff("proposal-tier1", {"approval": {"level": "human_review"}}, {"approval": {"level": "maintainer_review"}}, store_root=store_root)
    api.assess_blast_radius("proposal-tier1", semantic_domain="governance", store_root=store_root)
    assert api.determine_required_approval("proposal-tier1", store_root=store_root) == "human_critical"


def test_tier_3_fast_path_requires_all_fast_path_conditions(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    api.create_proposal(
        proposal_id="proposal-tier3-safe",
        target_type="parameter_group",
        target_id="risk_knobs",
        target_tier="tier_3",
        from_version="v1",
        to_version="v2",
        authored_by="operator",
        justification="bounded threshold change",
        hypothesis="narrow tuning",
        expected_benefit="lower noise",
        worst_case_downside="more churn",
        reversal_criteria="rollback",
        review_deadline="2026-04-30T00:00:00Z",
        dependency_impact={"capital_exposure_class": "standard"},
        proposed_scope={"domains": ["risk"]},
        store_root=store_root,
    )
    api.compute_proposal_diff("proposal-tier3-safe", {"rebalance_step": 3}, {"rebalance_step": 4}, store_root=store_root)
    blast_ref = api.assess_blast_radius("proposal-tier3-safe", semantic_domain="risk", store_root=store_root)
    store = ArtifactStore(store_root)
    assert fast_path_allowed(get_latest_proposal(store, "proposal-tier3-safe"), store.read("evaluation_artifacts", blast_ref))

    api.create_proposal(
        proposal_id="proposal-tier3-blocked",
        target_type="parameter_group",
        target_id="risk_knobs",
        target_tier="tier_3",
        from_version="v1",
        to_version="v2",
        authored_by="operator",
        justification="unsafe audit change",
        hypothesis="none",
        expected_benefit="none",
        worst_case_downside="audit loss",
        reversal_criteria="rollback",
        review_deadline="2026-04-30T00:00:00Z",
        dependency_impact={"capital_exposure_class": "standard"},
        proposed_scope={"domains": ["risk"]},
        store_root=store_root,
    )
    api.compute_proposal_diff("proposal-tier3-blocked", {"audit": {"lineage_required": True}}, {"audit": {"lineage_required": False}}, store_root=store_root)
    blocked_ref = api.assess_blast_radius("proposal-tier3-blocked", semantic_domain="risk", store_root=store_root)
    assert not fast_path_allowed(get_latest_proposal(store, "proposal-tier3-blocked"), store.read("evaluation_artifacts", blocked_ref))


def test_activation_fails_if_required_predicate_fails(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    store = ArtifactStore(store_root)
    api.register_module(
        module_id="runtime_policy",
        tier="tier_2",
        semantic_domain="risk",
        version="v1",
        module_content=_policy_v1(),
        store_root=store_root,
    )
    graph_id = api.compile_canonical_graph(store_root=store_root)
    graph = store.read("canonical_graphs", graph_id)
    store.write_immutable(
        "approvals",
        "seed-approval",
        ApprovalDecision(
            proposal_id="seed-proposal",
            required_approval_level="human_critical",
            approved_by="human",
            approved_at=utc_now(),
            reviewed_artifact_hashes=(graph["content_hash"],),
        ),
        artifact_type="ApprovalDecision",
    )
    _write_predicate_artifact(store, "failed-eval", "seed-proposal", failing_predicate="approval_sufficiency")
    _write_root_snapshot(store, graph_id, graph["record"]["graph_hash"], "seed-approval", "failed-eval")
    snapshot_id = api.create_activation_snapshot(
        graph_id=graph_id,
        activated_by="human",
        activation_scope="runtime",
        approval_refs=("seed-approval",),
        evaluation_refs=("failed-eval",),
        rollback_snapshot_ref="seed-root",
        prior_snapshot_id="seed-root",
        store_root=store_root,
    )
    with pytest.raises(ValueError, match="PREDICATE_FAILURE"):
        api.activate_snapshot(snapshot_id, actor="human", store_root=store_root)


def test_activation_fails_if_audit_artifact_missing(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    store = ArtifactStore(store_root)
    api.register_module(
        module_id="runtime_policy",
        tier="tier_2",
        semantic_domain="risk",
        version="v1",
        module_content=_policy_v1(),
        store_root=store_root,
    )
    graph_id = api.compile_canonical_graph(store_root=store_root)
    graph = store.read("canonical_graphs", graph_id)
    _write_predicate_artifact(store, "ok-eval", "seed-proposal")
    _write_root_snapshot(store, graph_id, graph["record"]["graph_hash"], "missing-approval", "ok-eval")
    snapshot_id = api.create_activation_snapshot(
        graph_id=graph_id,
        activated_by="human",
        activation_scope="runtime",
        approval_refs=(),
        evaluation_refs=("ok-eval",),
        rollback_snapshot_ref="seed-root",
        prior_snapshot_id="seed-root",
        store_root=store_root,
    )
    with pytest.raises(ValueError, match="AUDIT_ARTIFACT_MISSING"):
        api.activate_snapshot(snapshot_id, actor="human", store_root=store_root)


def test_activation_fails_if_rollback_target_missing(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    store = ArtifactStore(store_root)
    api.register_module(
        module_id="runtime_policy",
        tier="tier_2",
        semantic_domain="risk",
        version="v1",
        module_content=_policy_v1(),
        store_root=store_root,
    )
    graph_id = api.compile_canonical_graph(store_root=store_root)
    graph = store.read("canonical_graphs", graph_id)
    store.write_immutable(
        "approvals",
        "seed-approval",
        ApprovalDecision(
            proposal_id="seed-proposal",
            required_approval_level="human_critical",
            approved_by="human",
            approved_at=utc_now(),
            reviewed_artifact_hashes=(graph["content_hash"],),
        ),
        artifact_type="ApprovalDecision",
    )
    _write_predicate_artifact(store, "ok-eval", "seed-proposal")
    snapshot_id = api.create_activation_snapshot(
        graph_id=graph_id,
        activated_by="human",
        activation_scope="runtime",
        approval_refs=("seed-approval",),
        evaluation_refs=("ok-eval",),
        rollback_snapshot_ref="missing-snapshot",
        prior_snapshot_id=None,
        store_root=store_root,
    )
    with pytest.raises(ValueError, match="ROLLBACK_TARGET_MISSING"):
        api.activate_snapshot(snapshot_id, actor="human", store_root=store_root)


def test_activation_fails_if_interpreter_version_mismatch_exists(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    store = ArtifactStore(store_root)
    bad_graph = CanonicalGovernanceGraph(
        graph_id="graph-bad",
        graph_hash=content_hash({"graph": "bad"}),
        interpreter_version="constellation.meta_governance.interpreter.v999",
        active_module_versions={"runtime_policy": "v1"},
        active_parameter_versions={},
        dependency_edges=(),
        invariants=(),
        compiled_at=utc_now(),
        resolved_modules={},
        resolved_parameter_values={},
        compiled_policy=_policy_v1(),
    )
    store.write_immutable("canonical_graphs", bad_graph.graph_id, bad_graph, artifact_type="CanonicalGovernanceGraph")
    store.write_immutable(
        "approvals",
        "seed-approval",
        ApprovalDecision(
            proposal_id="seed-proposal",
            required_approval_level="human_critical",
            approved_by="human",
            approved_at=utc_now(),
            reviewed_artifact_hashes=("bad-hash",),
        ),
        artifact_type="ApprovalDecision",
    )
    _write_predicate_artifact(store, "ok-eval", "seed-proposal")
    _write_root_snapshot(store, bad_graph.graph_id, bad_graph.graph_hash, "seed-approval", "ok-eval")
    bad_snapshot = ActivationSnapshot(
        snapshot_id="bad-snapshot",
        graph_id=bad_graph.graph_id,
        graph_hash=bad_graph.graph_hash,
        interpreter_version=bad_graph.interpreter_version,
        active_at=utc_now(),
        activated_by="human",
        activation_scope="runtime",
        approval_refs=("seed-approval",),
        evaluation_refs=("ok-eval",),
        rollback_snapshot_ref="seed-root",
        prior_snapshot_id="seed-root",
    )
    store.write_immutable("activation_snapshots", bad_snapshot.snapshot_id, bad_snapshot, artifact_type="ActivationSnapshot")
    with pytest.raises(ValueError, match="INTERPRETER_VERSION_MISMATCH"):
        api.activate_snapshot("bad-snapshot", actor="human", store_root=store_root)


def test_governed_runtime_refuses_to_start_with_no_active_snapshot(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="ACTIVE_SNAPSHOT_REQUIRED"):
        api.resolve_active_runtime_authority(store_root=tmp_path / "store")


def test_governed_runtime_refuses_invalid_snapshot(tmp_path: Path) -> None:
    store_root, chain = _build_active_chain(tmp_path)
    store = ArtifactStore(store_root)
    invalid_snapshot = ActivationSnapshot(
        snapshot_id="invalid-snapshot",
        graph_id=chain["graph_id"],
        graph_hash=chain["graph"]["record"]["graph_hash"],
        interpreter_version=get_interpreter_version(),
        active_at=utc_now(),
        activated_by="human",
        activation_scope="runtime",
        approval_refs=(chain["approval_ref"],),
        evaluation_refs=(chain["evaluation_ref"],),
        rollback_snapshot_ref=chain["root_snapshot_id"],
        prior_snapshot_id=chain["root_snapshot_id"],
        lifecycle_state="deprecated",
    )
    store.write_immutable("activation_snapshots", invalid_snapshot.snapshot_id, invalid_snapshot, artifact_type="ActivationSnapshot")
    store.write_pointer("active_snapshot_ref", {"snapshot_id": invalid_snapshot.snapshot_id})
    with pytest.raises(ValueError, match="SNAPSHOT_LIFECYCLE_INVALID_FOR_RUNTIME"):
        api.resolve_active_runtime_authority(store_root=store_root)


def test_startup_gate_refuses_graph_hash_mismatch(tmp_path: Path) -> None:
    store_root, chain = _build_active_chain(tmp_path)
    store = ArtifactStore(store_root)
    mismatch_snapshot = ActivationSnapshot(
        snapshot_id="mismatch-snapshot",
        graph_id=chain["graph_id"],
        graph_hash="bad-hash",
        interpreter_version=get_interpreter_version(),
        active_at=utc_now(),
        activated_by="human",
        activation_scope="runtime",
        approval_refs=(chain["approval_ref"],),
        evaluation_refs=(chain["evaluation_ref"],),
        rollback_snapshot_ref=chain["root_snapshot_id"],
        prior_snapshot_id=chain["root_snapshot_id"],
    )
    store.write_immutable("activation_snapshots", mismatch_snapshot.snapshot_id, mismatch_snapshot, artifact_type="ActivationSnapshot")
    store.write_pointer("active_snapshot_ref", {"snapshot_id": mismatch_snapshot.snapshot_id})
    with pytest.raises(ValueError, match="GRAPH_HASH_MISMATCH"):
        api.resolve_active_runtime_authority(store_root=store_root)


def test_startup_gate_refuses_unresolved_dependency(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    store = ArtifactStore(store_root)
    broken_graph = CanonicalGovernanceGraph(
        graph_id="broken-graph",
        graph_hash=content_hash({"graph": "broken"}),
        interpreter_version=get_interpreter_version(),
        active_module_versions={"runtime_policy": "v1"},
        active_parameter_versions={},
        dependency_edges=(("runtime_policy", "missing"),),
        invariants=(),
        compiled_at=utc_now(),
        resolved_modules={},
        resolved_parameter_values={},
        compiled_policy=_policy_v1(),
    )
    store.write_immutable("canonical_graphs", broken_graph.graph_id, broken_graph, artifact_type="CanonicalGovernanceGraph")
    store.write_immutable(
        "approvals",
        "seed-approval",
        ApprovalDecision(
            proposal_id="seed-proposal",
            required_approval_level="human_critical",
            approved_by="human",
            approved_at=utc_now(),
            reviewed_artifact_hashes=("broken-graph",),
        ),
        artifact_type="ApprovalDecision",
    )
    _write_predicate_artifact(store, "ok-eval", "seed-proposal")
    _write_root_snapshot(store, broken_graph.graph_id, broken_graph.graph_hash, "seed-approval", "ok-eval")
    active_snapshot = ActivationSnapshot(
        snapshot_id="broken-active",
        graph_id=broken_graph.graph_id,
        graph_hash=broken_graph.graph_hash,
        interpreter_version=get_interpreter_version(),
        active_at=utc_now(),
        activated_by="human",
        activation_scope="runtime",
        approval_refs=("seed-approval",),
        evaluation_refs=("ok-eval",),
        rollback_snapshot_ref="seed-root",
        prior_snapshot_id="seed-root",
    )
    store.write_immutable("activation_snapshots", active_snapshot.snapshot_id, active_snapshot, artifact_type="ActivationSnapshot")
    store.write_pointer("active_snapshot_ref", {"snapshot_id": active_snapshot.snapshot_id})
    with pytest.raises(ValueError, match="UNRESOLVED_GRAPH_DEPENDENCY"):
        api.resolve_active_runtime_authority(store_root=store_root)


def test_autonomy_broadening_becomes_critical(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    api.create_proposal(
        proposal_id="proposal-autonomy",
        target_type="policy_module",
        target_id="runtime_policy",
        target_tier="tier_2",
        from_version="v1",
        to_version="v2",
        authored_by="operator",
        justification="permission change",
        hypothesis="more automation",
        expected_benefit="less latency",
        worst_case_downside="unsafe autonomy",
        reversal_criteria="rollback",
        review_deadline="2026-04-30T00:00:00Z",
        dependency_impact={"capital_exposure_class": "standard"},
        store_root=store_root,
    )
    api.compute_proposal_diff("proposal-autonomy", {"permissions": []}, {"permissions": ["auto_submit"]}, store_root=store_root)
    blast_ref = api.assess_blast_radius("proposal-autonomy", semantic_domain="risk", store_root=store_root)
    assert ArtifactStore(store_root).read("evaluation_artifacts", blast_ref)["record"]["classification_result"] == "critical"


def test_audit_weakening_becomes_critical(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    api.create_proposal(
        proposal_id="proposal-audit",
        target_type="policy_module",
        target_id="runtime_policy",
        target_tier="tier_2",
        from_version="v1",
        to_version="v2",
        authored_by="operator",
        justification="audit change",
        hypothesis="less storage",
        expected_benefit="none",
        worst_case_downside="audit loss",
        reversal_criteria="rollback",
        review_deadline="2026-04-30T00:00:00Z",
        dependency_impact={"capital_exposure_class": "standard"},
        store_root=store_root,
    )
    api.compute_proposal_diff("proposal-audit", {"audit": {"lineage_required": True}}, {"audit": {"lineage_required": False}}, store_root=store_root)
    blast_ref = api.assess_blast_radius("proposal-audit", semantic_domain="risk", store_root=store_root)
    assert ArtifactStore(store_root).read("evaluation_artifacts", blast_ref)["record"]["classification_result"] == "critical"


def test_approval_rule_weakening_becomes_critical(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    api.create_proposal(
        proposal_id="proposal-approval",
        target_type="protocol",
        target_id="approval_policy",
        target_tier="tier_2",
        from_version="v1",
        to_version="v2",
        authored_by="operator",
        justification="approval change",
        hypothesis="less overhead",
        expected_benefit="speed",
        worst_case_downside="governance weakening",
        reversal_criteria="rollback",
        review_deadline="2026-04-30T00:00:00Z",
        dependency_impact={"capital_exposure_class": "standard"},
        store_root=store_root,
    )
    api.compute_proposal_diff("proposal-approval", {"approval": {"level": "human_review"}}, {"approval": {"level": "maintainer_review"}}, store_root=store_root)
    blast_ref = api.assess_blast_radius("proposal-approval", semantic_domain="governance", store_root=store_root)
    assert ArtifactStore(store_root).read("evaluation_artifacts", blast_ref)["record"]["classification_result"] == "critical"


def test_transitive_dependency_impact_changes_classification(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    api.register_module(
        module_id="tax_guard",
        tier="tier_2",
        semantic_domain="tax",
        version="v1",
        module_content={"tax_mode": "strict"},
        store_root=store_root,
    )
    api.register_module(
        module_id="execution_guard",
        tier="tier_2",
        semantic_domain="execution",
        version="v1",
        module_content={"execution_mode": "gated"},
        dependency_ids=("tax_guard",),
        store_root=store_root,
    )
    api.register_module(
        module_id="runtime_policy",
        tier="tier_2",
        semantic_domain="risk",
        version="v1",
        module_content=_policy_v1(),
        dependency_ids=("execution_guard",),
        store_root=store_root,
    )
    api.create_proposal(
        proposal_id="proposal-transitive",
        target_type="policy_module",
        target_id="runtime_policy",
        target_tier="tier_2",
        from_version="v1",
        to_version="v2",
        authored_by="operator",
        justification="small change with transitive impact",
        hypothesis="bounded",
        expected_benefit="bounded",
        worst_case_downside="transitive execution/tax effect",
        reversal_criteria="rollback",
        review_deadline="2026-04-30T00:00:00Z",
        dependency_impact={"capital_exposure_class": "standard"},
        store_root=store_root,
    )
    api.compute_proposal_diff("proposal-transitive", {"rebalance_step": 1}, {"rebalance_step": 2}, store_root=store_root)
    blast_ref = api.assess_blast_radius("proposal-transitive", semantic_domain="risk", store_root=store_root)
    blast = ArtifactStore(store_root).read("evaluation_artifacts", blast_ref)["record"]
    assert set(blast["domains_touched"]) >= {"risk", "execution", "tax"}
    assert blast["classification_result"] == "moderate"


def test_same_inputs_produce_identical_graph_hash(tmp_path: Path) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    api.register_module(module_id="runtime_policy", tier="tier_2", semantic_domain="risk", version="v1", module_content=_policy_v1(), store_root=left)
    api.register_module(module_id="runtime_policy", tier="tier_2", semantic_domain="risk", version="v1", module_content=_policy_v1(), store_root=right)
    left_graph = ArtifactStore(left).read("canonical_graphs", api.compile_canonical_graph(store_root=left))
    right_graph = ArtifactStore(right).read("canonical_graphs", api.compile_canonical_graph(store_root=right))
    assert left_graph["record"]["graph_hash"] == right_graph["record"]["graph_hash"]


def test_reordering_source_fields_does_not_change_graph_hash(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    reordered = {
        "weights": {"tax": 0.1, "allocation": 0.2, "risk": 0.7},
        "version": 1,
        "limits": {"min_decision_score": 0.1, "max_actions_per_cycle": 5},
        "cooldown": {"min_seconds_between_same_action": 60, "enabled": True, "block_same_action": True},
    }
    api.register_module(module_id="runtime_policy", tier="tier_2", semantic_domain="risk", version="v1", module_content=_policy_v1(), store_root=first)
    api.register_module(module_id="runtime_policy", tier="tier_2", semantic_domain="risk", version="v1", module_content=reordered, store_root=second)
    first_graph = ArtifactStore(first).read("canonical_graphs", api.compile_canonical_graph(store_root=first))
    second_graph = ArtifactStore(second).read("canonical_graphs", api.compile_canonical_graph(store_root=second))
    assert first_graph["record"]["graph_hash"] == second_graph["record"]["graph_hash"]


def test_missing_dependency_fails_compilation(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    api.register_module(
        module_id="runtime_policy",
        tier="tier_2",
        semantic_domain="risk",
        version="v1",
        module_content=_policy_v1(),
        dependency_ids=("missing_dependency",),
        store_root=store_root,
    )
    with pytest.raises(ValueError, match="MISSING_DEPENDENCY"):
        api.compile_canonical_graph(store_root=store_root)


def test_runtime_consumer_can_resolve_active_snapshot(tmp_path: Path) -> None:
    store_root, chain = _build_active_chain(tmp_path)
    authority = api.resolve_active_runtime_authority(store_root=store_root)
    assert authority["snapshot"]["snapshot_id"] == chain["snapshot_id"]
    assert authority["graph"]["graph_id"] == chain["graph_id"]
    assert authority["compiled_policy"]["version"] == 1


def test_all_governed_entry_points_resolve_same_active_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store_root, chain = _build_active_chain(tmp_path)
    _set_store_root(monkeypatch, store_root)
    run_cycle_module = _load_run_decision_cycle_module()
    authority = api.resolve_active_runtime_authority(store_root=store_root)
    runtime_authority = run_cycle_module.resolve_active_runtime_authority()
    assert authority["snapshot"]["snapshot_id"] == chain["snapshot_id"]
    assert runtime_authority["snapshot"]["snapshot_id"] == chain["snapshot_id"]


def test_decisions_attach_snapshot_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store_root, chain = _build_active_chain(tmp_path)
    _set_store_root(monkeypatch, store_root)
    run_cycle_module = _load_run_decision_cycle_module()
    snapshot = {
        "snapshot_id": chain["snapshot_id"],
        "graph_hash": chain["graph"]["record"]["graph_hash"],
    }
    monkeypatch.setattr(run_cycle_module, "record_decision_lineage", lambda *args, **kwargs: None)
    governed = run_cycle_module.attach_governance_context(
        [{"position_id": "POS-1", "action": "EXIT", "scores": {}, "total_score": 1.0}],
        snapshot,
        "input-hash",
    )
    assert governed[0]["snapshot_id"] == chain["snapshot_id"]
    assert governed[0]["graph_hash"] == chain["graph"]["record"]["graph_hash"]
    assert governed[0]["decision_id"]


def test_action_contains_snapshot_id_and_graph_hash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module_path = Path("/home/node/constellation/runtime/action_plan.py")
    spec = importlib.util.spec_from_file_location("runtime_action_plan_test", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    if str(RUNTIME_ROOT) not in sys.path:
        sys.path.insert(0, str(RUNTIME_ROOT))
    _set_store_root(monkeypatch, tmp_path / "store")
    spec.loader.exec_module(module)
    plan = module.compile_plan(
        [
            {
                "position_id": "POS-1",
                "action": "EXIT",
                "decision_id": "decision-1",
                "snapshot_id": "snapshot-1",
                "graph_hash": "graph-hash-1",
            }
        ]
    )
    assert plan[0]["snapshot_id"] == "snapshot-1"
    assert plan[0]["graph_hash"] == "graph-hash-1"


def test_raw_policy_runtime_read_path_is_blocked() -> None:
    module_path = Path("/home/node/constellation/runtime/policy_loader.py")
    spec = importlib.util.spec_from_file_location("runtime_policy_loader_test", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with pytest.raises(RuntimeError, match="RAW_POLICY_RUNTIME_AUTHORITY_DISABLED"):
        module.load_policy("/home/node/constellation/runtime/policy_bundle.yaml")


def test_snapshot_is_immutable_after_creation(tmp_path: Path) -> None:
    store_root, chain = _build_active_chain(tmp_path)
    store = ArtifactStore(store_root)
    with pytest.raises(ValueError, match="IMMUTABLE_CONFLICT"):
        store.write_immutable(
            "activation_snapshots",
            chain["snapshot_id"],
            {"different": True},
            artifact_type="ActivationSnapshot",
        )


def test_deprecated_or_superseded_snapshot_cannot_become_active(tmp_path: Path) -> None:
    store_root, chain = _build_active_chain(tmp_path)
    store = ArtifactStore(store_root)
    deprecated_snapshot = ActivationSnapshot(
        snapshot_id="deprecated-snapshot",
        graph_id=chain["graph_id"],
        graph_hash=chain["graph"]["record"]["graph_hash"],
        interpreter_version=get_interpreter_version(),
        active_at=utc_now(),
        activated_by="human",
        activation_scope="runtime",
        approval_refs=(chain["approval_ref"],),
        evaluation_refs=(chain["evaluation_ref"],),
        rollback_snapshot_ref=chain["root_snapshot_id"],
        prior_snapshot_id=chain["root_snapshot_id"],
        lifecycle_state="deprecated",
    )
    store.write_immutable("activation_snapshots", deprecated_snapshot.snapshot_id, deprecated_snapshot, artifact_type="ActivationSnapshot")
    with pytest.raises(ValueError, match="SNAPSHOT_LIFECYCLE_INVALID_FOR_RUNTIME"):
        api.activate_snapshot(deprecated_snapshot.snapshot_id, actor="human", store_root=store_root)


def test_rollback_activates_prior_snapshot_without_mutating_history(tmp_path: Path) -> None:
    store_root, chain = _build_active_chain(tmp_path)
    store = ArtifactStore(store_root)
    rollback_id = api.rollback_to_snapshot(
        chain["root_snapshot_id"],
        rolled_back_by="human.reviewer",
        reason="revert",
        store_root=store_root,
    )
    active = api.get_active_snapshot(store_root=store_root)
    assert active is not None
    assert active["record"]["snapshot_id"] == chain["root_snapshot_id"]
    assert store.exists("activation_snapshots", chain["snapshot_id"])
    assert store.exists("rollback_records", rollback_id)
    assert any("snapshot_rolled_back" in event_id for event_id in store.list_ids("audit_events"))


def test_unified_audit_stream_records_proposal_activation_rollback_and_lineage(tmp_path: Path) -> None:
    store_root, chain = _build_active_chain(tmp_path)
    api.record_decision_lineage("decision-audit", chain["snapshot_id"], store_root=store_root)
    api.rollback_to_snapshot(
        chain["root_snapshot_id"],
        rolled_back_by="human.reviewer",
        reason="audit proof",
        store_root=store_root,
    )
    stream = read_canonical_audit_stream(ArtifactStore(store_root))
    event_types = {row.get("event_type") for row in stream if row.get("stream_type") == "governed_audit_event"}
    assert "proposal_created" in event_types
    assert "snapshot_activated" in event_types
    assert "snapshot_rolled_back" in event_types
    assert "decision_lineage_recorded" in event_types


def test_blocked_activation_emits_canonical_audit_event(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    store = ArtifactStore(store_root)
    api.register_module(
        module_id="runtime_policy",
        tier="tier_2",
        semantic_domain="risk",
        version="v1",
        module_content=_policy_v1(),
        store_root=store_root,
    )
    graph_id = api.compile_canonical_graph(store_root=store_root)
    graph = store.read("canonical_graphs", graph_id)
    store.write_immutable(
        "approvals",
        "seed-approval",
        ApprovalDecision(
            proposal_id="seed-proposal",
            required_approval_level="human_critical",
            approved_by="human",
            approved_at=utc_now(),
            reviewed_artifact_hashes=(graph["content_hash"],),
        ),
        artifact_type="ApprovalDecision",
    )
    _write_predicate_artifact(store, "failed-eval", "seed-proposal", failing_predicate="approval_sufficiency")
    _write_root_snapshot(store, graph_id, graph["record"]["graph_hash"], "seed-approval", "failed-eval")
    snapshot_id = api.create_activation_snapshot(
        graph_id=graph_id,
        activated_by="human",
        activation_scope="runtime",
        approval_refs=("seed-approval",),
        evaluation_refs=("failed-eval",),
        rollback_snapshot_ref="seed-root",
        prior_snapshot_id="seed-root",
        store_root=store_root,
    )
    with pytest.raises(ValueError):
        api.activate_snapshot(snapshot_id, actor="human", store_root=store_root)
    stream = read_canonical_audit_stream(store)
    assert any(
        row.get("event_type") == "blocked_activation"
        for row in stream
        if row.get("stream_type") == "governed_audit_event"
    )


def test_live_decision_can_be_traced_back_to_proposal_and_approval_chain(tmp_path: Path) -> None:
    store_root, chain = _build_active_chain(tmp_path)
    api.record_decision_lineage("decision-1", chain["snapshot_id"], store_root=store_root)
    lineage = api.lineage_for_decision("decision-1", store_root=store_root)
    assert lineage["lineage"]["record"]["snapshot_id"] == chain["snapshot_id"]
    assert lineage["proposals"][0]["record"]["proposal_id"] == "proposal-runtime"
    assert lineage["approvals"][0]["record"]["proposal_id"] == "proposal-runtime"


def test_illegal_transition_blocked(tmp_path: Path) -> None:
    store_root = tmp_path / "store"
    api.create_proposal(
        proposal_id="proposal-illegal",
        target_type="policy_module",
        target_id="runtime_policy",
        target_tier="tier_2",
        from_version="v1",
        to_version="v2",
        authored_by="operator",
        justification="illegal jump",
        hypothesis="none",
        expected_benefit="none",
        worst_case_downside="invalid lifecycle",
        reversal_criteria="rollback",
        review_deadline="2026-04-30T00:00:00Z",
        dependency_impact={"capital_exposure_class": "standard"},
        store_root=store_root,
    )
    with pytest.raises(ValueError, match="ILLEGAL_LIFECYCLE_TRANSITION"):
        transition_proposal(ArtifactStore(store_root), "proposal-illegal", "active")


def test_no_override_path_exists() -> None:
    assert "force" not in inspect.signature(api.activate_snapshot).parameters


def test_runtime_governed_execution_blocked_on_interpreter_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store_root, chain = _build_active_chain(tmp_path)
    _set_store_root(monkeypatch, store_root)
    store = ArtifactStore(store_root)
    mismatch_snapshot = ActivationSnapshot(
        snapshot_id="runtime-mismatch",
        graph_id=chain["graph_id"],
        graph_hash=chain["graph"]["record"]["graph_hash"],
        interpreter_version="constellation.meta_governance.interpreter.v999",
        active_at=utc_now(),
        activated_by="human",
        activation_scope="runtime",
        approval_refs=(chain["approval_ref"],),
        evaluation_refs=(chain["evaluation_ref"],),
        rollback_snapshot_ref=chain["root_snapshot_id"],
        prior_snapshot_id=chain["root_snapshot_id"],
    )
    store.write_immutable("activation_snapshots", mismatch_snapshot.snapshot_id, mismatch_snapshot, artifact_type="ActivationSnapshot")
    store.write_pointer("active_snapshot_ref", {"snapshot_id": mismatch_snapshot.snapshot_id})
    run_cycle_module = _load_run_decision_cycle_module()
    with pytest.raises(ValueError, match="INTERPRETER_VERSION_MISMATCH"):
        run_cycle_module.run_cycle()


def test_rollback_blocked_on_unknown_or_mismatched_interpreter(tmp_path: Path) -> None:
    store_root, chain = _build_active_chain(tmp_path)
    store = ArtifactStore(store_root)
    bad_snapshot = ActivationSnapshot(
        snapshot_id="bad-rollback-snapshot",
        graph_id=chain["graph_id"],
        graph_hash=chain["graph"]["record"]["graph_hash"],
        interpreter_version="",
        active_at=utc_now(),
        activated_by="human",
        activation_scope="runtime",
        approval_refs=(chain["approval_ref"],),
        evaluation_refs=(chain["evaluation_ref"],),
        rollback_snapshot_ref=chain["root_snapshot_id"],
        prior_snapshot_id=chain["root_snapshot_id"],
    )
    store.write_immutable("activation_snapshots", bad_snapshot.snapshot_id, bad_snapshot, artifact_type="ActivationSnapshot")
    with pytest.raises(ValueError, match="INTERPRETER_VERSION_REQUIRED"):
        api.rollback_to_snapshot(
            bad_snapshot.snapshot_id,
            rolled_back_by="human",
            reason="invalid interpreter",
            store_root=store_root,
        )
