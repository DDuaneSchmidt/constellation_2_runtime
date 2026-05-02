from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_action_validity_v1 as action_validity  # noqa: E402
import ops.tools.run_ai_recommendation_queue_v1 as queue  # noqa: E402
import ops.tools.run_post_promotion_monitor_v1 as monitor  # noqa: E402
import ops.tools.run_shadow_evaluation_v1 as shadow  # noqa: E402
import ops.tools.run_strategy_change_proposal_v1 as proposal  # noqa: E402
import ops.tools.run_strategy_promotion_gate_v1 as promotion  # noqa: E402
import ops.tools.run_unified_truth_kernel_v1 as kernel  # noqa: E402
import ops.tools.run_aegis_live_intelligence_v1 as live  # noqa: E402
from constellation_2.phaseB.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402
from ops.tools import run_aegis_bod_prepare_v1 as bod  # noqa: E402
from ops.tools.aegis_producer_contract_v1 import producer_contract_v1  # noqa: E402

DAY = "2026-04-29"


def _ctx(tmp_path: Path) -> bod.BodContext:
    truth = tmp_path / "truth"
    execution = tmp_path / "truth_sleeves" / "PRIMARY" / "PAPER"
    runtime = tmp_path / "runtime"
    operator = tmp_path / "operator"
    for path in (truth, execution, runtime, operator):
        path.mkdir(parents=True, exist_ok=True)
    return bod.BodContext(DAY, "PAPER", truth, execution, runtime, operator, "DU123456")


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _report(ctx: bod.BodContext, family: str, filename: str) -> Path:
    return ctx.truth_root / "reports" / family / ctx.day_utc / filename


def _base(ctx: bod.BodContext, *, ready: bool = False) -> None:
    _write(_report(ctx, "aegis_day_run_v1", "day_run.v1.json"), {"day_utc": DAY, "final_status": "PAPER_READY" if ready else "NOT_READY", "canonical_phase": "" if ready else "BOD_INPUTS", "canonical_blocker": "" if ready else "C2_KILL_SWITCH_ACTIVE", "source_repo_status": {"git_dirty_status": "CLEAN", "source_reproducibility_status": "REPRODUCIBLE"}})
    _write(_report(ctx, "aegis_requirement_graph_v1", "requirement_graph.v1.json"), {"day_utc": DAY, "status": "PASS", "requirements": []})
    _write(_report(ctx, "evidence_lineage_index_v1", "evidence_lineage_index.v1.json"), {"day_utc": DAY, "status": "PASS", "lineage_nodes": []})
    _write(_report(ctx, "state_consistency_v1", "state_consistency.v1.json"), {"day_utc": DAY, "status": "PASS", "invariant_results": []})
    _write(_report(ctx, "truth_freshness_v1", "truth_freshness.v1.json"), {"day_utc": DAY, "status": "PASS", "freshness_records": []})
    _write(_report(ctx, "market_data_supply_v1", "market_data_supply.v1.json"), {"day_utc": DAY, "status": "PASS"})
    _write(_report(ctx, "authorization_supply_v1", "authorization_supply.v1.json"), {"day_utc": DAY, "status": "PASS", "authorization_export": {"authorized_intents": [{"intent_id": "i"}]}})
    _write(_report(ctx, "strategy_decision_authority_v1", "strategy_decision_authority.v1.json"), {"day_utc": DAY, "status": "PASS", "strategy_decision_state": "READY", "selected_intent_id": "base"})
    _write(action_validity.action_validity_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), action_validity.build_action_validity_v1(ctx))


def _validate(payload: dict, schema_name: str) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, f"governance/04_DATA/SCHEMAS/C2/REPORTS/{schema_name}.v1.schema.json")


def _with_contract(payload: dict, *, producer_name: str, output_path: Path) -> dict:
    payload = dict(payload)
    payload["producer_contract_v1"] = producer_contract_v1(
        producer_name=producer_name,
        producer_command=f"python3 {producer_name}",
        input_artifacts=[],
        output_artifacts=[output_path],
        schema_versions={"test": "v1"},
    )
    return payload


def _materialize_queue(ctx: bod.BodContext) -> dict:
    path = queue.ai_recommendation_queue_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    payload = _with_contract(queue.build_ai_recommendation_queue_v1(ctx), producer_name="ops/tools/run_ai_recommendation_queue_v1.py", output_path=path)
    _write(path, payload)
    return payload


def _materialize_proposal(ctx: bod.BodContext) -> dict:
    path = proposal.strategy_change_proposal_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    payload = _with_contract(proposal.build_strategy_change_proposal_v1(ctx), producer_name="ops/tools/run_strategy_change_proposal_v1.py", output_path=path)
    _write(path, payload)
    return payload


def _materialize_shadow(ctx: bod.BodContext) -> dict:
    path = shadow.shadow_evaluation_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    payload = _with_contract(shadow.build_shadow_evaluation_v1(ctx), producer_name="ops/tools/run_shadow_evaluation_v1.py", output_path=path)
    _write(path, payload)
    return payload


def _materialize_promotion(ctx: bod.BodContext) -> dict:
    path = promotion.strategy_promotion_gate_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    payload = _with_contract(promotion.build_strategy_promotion_gate_v1(ctx), producer_name="ops/tools/run_strategy_promotion_gate_v1.py", output_path=path)
    _write(path, payload)
    return payload


def _materialize_monitor(ctx: bod.BodContext) -> dict:
    path = monitor.post_promotion_monitor_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    payload = _with_contract(monitor.build_post_promotion_monitor_v1(ctx), producer_name="ops/tools/run_post_promotion_monitor_v1.py", output_path=path)
    _write(path, payload)
    return payload


def test_recommendation_queue_is_advisory_and_low_confidence_for_unknown_evidence(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base(ctx)
    _write(_report(ctx, "edge_attribution_v1", "edge_attribution.v1.json"), {"day_utc": DAY, "status": "UNPROVEN"})
    payload = _materialize_queue(ctx)

    _validate(payload, "ai_recommendation_queue")
    assert payload["advisory_only"] is True
    assert payload["readiness_effect"] == "NONE"
    assert payload["submit_boundary_effect"] == "NONE"
    assert any(row["source_type"] == "EDGE" and row["confidence"] == "LOW" for row in payload["recommendations"])


def test_strategy_proposals_are_pending_inert_and_do_not_mutate_config(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base(ctx)
    _materialize_queue(ctx)
    payload = _materialize_proposal(ctx)

    _validate(payload, "strategy_change_proposal")
    assert payload["active_strategy_mutation"] is False
    assert payload["readiness_effect"] == "NONE"
    assert all(row["activation_status"] == "NOT_ACTIVE" for row in payload["proposals"])
    assert all(row["human_approval_status"] == "PENDING" for row in payload["proposals"])


def test_shadow_evaluation_is_observe_only_and_preserves_baseline(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base(ctx)
    _materialize_queue(ctx)
    _materialize_proposal(ctx)
    before = json.loads(_report(ctx, "strategy_decision_authority_v1", "strategy_decision_authority.v1.json").read_text())
    payload = _materialize_shadow(ctx)
    after = json.loads(_report(ctx, "strategy_decision_authority_v1", "strategy_decision_authority.v1.json").read_text())

    _validate(payload, "shadow_evaluation")
    assert payload["submit_allowed"] is False
    assert payload["readiness_effect"] == "NONE"
    assert payload["active_strategy_mutation"] is False
    assert before == after
    assert payload["decision_delta"] == "NO_ACTIVE_BEHAVIOR_CHANGE"


def test_promotion_gate_blocks_without_human_approval_shadow_or_clean_kernel(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base(ctx, ready=True)
    _materialize_queue(ctx)
    _materialize_proposal(ctx)
    _materialize_shadow(ctx)
    _write(kernel.unified_truth_kernel_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), {"day_utc": DAY, "final_status": "NOT_READY", "canonical_blocker": "C2_KILL_SWITCH_ACTIVE"})
    payload = _materialize_promotion(ctx)

    _validate(payload, "strategy_promotion_gate")
    assert payload["promotion_status"] == "BLOCKED"
    assert "HUMAN_APPROVAL_MISSING" in payload["blockers"]
    assert "UNIFIED_TRUTH_KERNEL_BLOCKED" in payload["blockers"]
    assert payload["submit_allowed"] is False
    assert payload["readiness_effect"] == "NONE"


def test_promotion_gate_can_be_eligible_only_after_approval_and_shadow_pass(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base(ctx, ready=True)
    _materialize_queue(ctx)
    _materialize_proposal(ctx)
    proposal_path = proposal.strategy_change_proposal_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    proposal_payload = json.loads(proposal_path.read_text())
    proposal_payload["proposals"][0]["human_approval_status"] = "APPROVED"
    _write(proposal_path, proposal_payload)
    _materialize_shadow(ctx)
    _write(kernel.unified_truth_kernel_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), {"day_utc": DAY, "final_status": "PAPER_READY", "canonical_blocker": ""})
    payload = _materialize_promotion(ctx)

    assert payload["promotion_status"] == "APPROVED_FOR_PROMOTION"
    assert payload["submit_allowed"] is False
    assert payload["active_strategy_mutation"] is False


def test_post_promotion_monitor_success_failure_inconclusive_and_rollback(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base(ctx, ready=True)
    _materialize_queue(ctx)
    _materialize_proposal(ctx)
    proposal_path = proposal.strategy_change_proposal_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    proposal_payload = json.loads(proposal_path.read_text())
    proposal_payload["proposals"][0]["human_approval_status"] = "APPROVED"
    _write(proposal_path, proposal_payload)
    _materialize_shadow(ctx)
    _write(kernel.unified_truth_kernel_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), {"day_utc": DAY, "final_status": "PAPER_READY", "canonical_blocker": ""})
    _materialize_promotion(ctx)

    _write(_report(ctx, "trade_outcome_v1", "trade_outcome.v1.json"), {"day_utc": DAY, "outcome_status": "PASS"})
    success = _materialize_monitor(ctx)
    assert success["status"] == "PASS"
    assert success["rollback_recommended"] is False

    _write(_report(ctx, "trade_outcome_v1", "trade_outcome.v1.json"), {"day_utc": DAY, "outcome_status": "FAIL"})
    failed = _materialize_monitor(ctx)
    assert failed["status"] == "ROLLBACK_RECOMMENDED"
    assert failed["rollback_recommended"] is True
    assert failed["submit_allowed"] is False

    gate_path = promotion.strategy_promotion_gate_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    _write(gate_path, {"day_utc": DAY, "promotion_status": "BLOCKED"})
    inconclusive = _materialize_monitor(ctx)
    assert inconclusive["status"] == "NOT_APPLICABLE"
    assert inconclusive["readiness_effect"] == "NONE"


def test_unified_kernel_and_live_surface_learning_loop_without_authority(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base(ctx)
    _materialize_queue(ctx)
    _materialize_proposal(ctx)
    _materialize_shadow(ctx)
    _materialize_promotion(ctx)
    _materialize_monitor(ctx)
    payload = kernel.build_unified_truth_kernel_v1(ctx)
    _write(kernel.unified_truth_kernel_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), payload)
    live_payload = live.build_live_intelligence_v1(ctx)

    assert payload["open_recommendation_count"] > 0
    assert payload["pending_proposal_count"] > 0
    assert payload["automatic_deployment_allowed"] is False
    assert live_payload["direct_proposal_write_allowed"] is False
    assert live_payload["active_strategy_mutation_allowed"] is False
