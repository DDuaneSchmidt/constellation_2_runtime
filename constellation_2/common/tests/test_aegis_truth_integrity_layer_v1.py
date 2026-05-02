from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_action_validity_v1 as action_validity  # noqa: E402
import ops.tools.run_evidence_lineage_index_v1 as lineage  # noqa: E402
import ops.tools.run_outcome_attribution_v1 as outcome  # noqa: E402
import ops.tools.run_state_consistency_v1 as consistency  # noqa: E402
import ops.tools.run_truth_freshness_v1 as freshness  # noqa: E402
from ops.tools import run_aegis_bod_prepare_v1 as bod  # noqa: E402
from ops.tools.aegis_producer_contract_v1 import producer_contract_v1  # noqa: E402
from ops.tools.aegis_truth_integrity_common_v1 import artifact_specs_v1  # noqa: E402
import ops.tools.run_aegis_operator_projection_v1 as projection  # noqa: E402
import ops.tools.run_aegis_live_intelligence_v1 as live  # noqa: E402
import ops.tools.run_unified_truth_kernel_v1 as kernel  # noqa: E402
from constellation_2.phaseB.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402
from constellation_2.phaseL.ui_api.readiness_kernel_v1 import build_readiness_kernel_v1  # noqa: E402


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


def _with_contract(path: Path, payload: dict, producer: str = "ops/tools/run_aegis_day_v1.py", inputs: list[Path] | None = None) -> None:
    payload = dict(payload)
    payload.setdefault("day_utc", DAY)
    payload.setdefault("generated_at_utc", "2026-04-29T14:00:00Z")
    payload["producer_contract_v1"] = producer_contract_v1(
        producer_name=producer,
        producer_command=f"python3 {producer}",
        input_artifacts=inputs or [],
        output_artifacts=[path],
        schema_versions={"test": "v1"},
    )
    _write(path, payload)


def _base_artifacts(ctx: bod.BodContext) -> None:
    _with_contract(
        _report(ctx, "aegis_day_run_v1", "day_run.v1.json"),
        {
            "schema_id": "aegis_day_run",
            "schema_version": "aegis_day_run.v1",
            "final_status": "NOT_READY",
            "canonical_blocker": "MARKET_CLOSED",
            "source_repo_status": {"git_dirty_status": "CLEAN", "source_reproducibility_status": "REPRODUCIBLE"},
        },
    )
    _with_contract(_report(ctx, "aegis_requirement_graph_v1", "requirement_graph.v1.json"), {"schema_id": "aegis_requirement_graph", "schema_version": "aegis_requirement_graph.v1", "status": "PASS", "canonical_blocker": "", "requirements": []})
    _with_contract(_report(ctx, "market_open_data_gate_v1", "market_open_data_gate.v1.json"), {"schema_id": "market_open_data_gate", "schema_version": "market_open_data_gate.v1", "status": "BLOCKED", "canonical_blocker": "MARKET_CLOSED"})
    _with_contract(_report(ctx, "market_data_supply_v1", "market_data_supply.v1.json"), {"schema_id": "market_data_supply", "schema_version": "market_data_supply.v1", "status": "BLOCKED", "canonical_blocker": "OPTIONS_SNAPSHOT_STALE"})
    _with_contract(_report(ctx, "broker_supply_v1", "broker_supply.v1.json"), {"schema_id": "broker_supply", "schema_version": "broker_supply.v1", "status": "PASS", "canonical_blocker": ""})
    _with_contract(_report(ctx, "capital_supply_v1", "capital_supply.v1.json"), {"schema_id": "capital_supply", "schema_version": "capital_supply.v1", "status": "PASS", "canonical_blocker": ""})
    _with_contract(_report(ctx, "risk_budget_supply_v1", "risk_budget_supply.v1.json"), {"schema_id": "risk_budget_supply", "schema_version": "risk_budget_supply.v1", "status": "PASS", "canonical_blocker": ""})
    _with_contract(_report(ctx, "authorization_supply_v1", "authorization_supply.v1.json"), {"schema_id": "authorization_supply", "schema_version": "authorization_supply.v1", "status": "BLOCKED", "canonical_blocker": "NO_ELIGIBLE_OPTION_STRUCTURE", "authorization_export": {"authorized_intents": []}})
    _with_contract(_report(ctx, "submit_boundary_status_v1", "submit_boundary_status.v1.json"), {"schema_id": "submit_boundary_status", "schema_version": "v1", "status": "NOT_READY", "canonical_blocker": "MARKET_CLOSED", "submit_allowed": False, "submission_authorized": False})
    _with_contract(_report(ctx, "trading_day_control_plane_v1", "trading_day_control_plane.v1.json"), {"schema_id": "trading_day_control_plane", "schema_version": "v1", "status": "BLOCKED"})
    _with_contract(_report(ctx, "aegis_operator_projection_v1", "operator_projection.v1.json"), {"schema_id": "aegis_operator_projection", "schema_version": "aegis_operator_projection.v1", "final_status": "NOT_READY", "canonical_blocker": "MARKET_CLOSED"})
    _with_contract(_report(ctx, "aegis_live_intelligence_v1", "live_intelligence.v1.json"), {"schema_id": "aegis_live_intelligence", "schema_version": "aegis_live_intelligence.v1", "status": "NOT_READY", "final_status_observed": "NOT_READY", "canonical_blocker": "MARKET_CLOSED", "advisory_only": True, "opportunity_radar": {"actionable_recommendations": []}})
    _write(_report(ctx, "market_data_authority_v1", "market_data_authority.v1.json"), {"day_utc": DAY, "market_data_state": "MISSING_REQUIRED_DATA"})
    _write(_report(ctx, "strategy_decision_authority_v1", "strategy_decision_authority.v1.json"), {"day_utc": DAY, "status": "BLOCKED", "strategy_decision_state": "MARKET_DATA_BLOCKED"})


def test_lineage_indexes_required_artifacts_and_flags_missing_contract(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base_artifacts(ctx)
    payload = lineage.build_evidence_lineage_index_v1(ctx)

    indexed = {row["artifact_type"] for row in payload["lineage_nodes"]}
    assert {"aegis_day_run_v1", "aegis_requirement_graph_v1", "market_open_data_gate_v1", "submit_boundary_status_v1", "aegis_operator_projection_v1", "aegis_live_intelligence_v1"} <= indexed
    assert {row["artifact_type"] for row in payload["lineage_nodes"]} >= {row["artifact_type"] for row in artifact_specs_v1()}

    gate = next(row for row in payload["lineage_nodes"] if row["artifact_type"] == "market_open_data_gate_v1")
    assert gate["producer_contract_present"] is True
    assert gate["lineage_status"] in {"PASS", "UNKNOWN"}

    no_contract = _report(ctx, "market_open_data_gate_v1", "market_open_data_gate.v1.json")
    _write(no_contract, {"day_utc": DAY, "status": "BLOCKED", "canonical_blocker": "MARKET_CLOSED"})
    payload = lineage.build_evidence_lineage_index_v1(ctx)
    gate = next(row for row in payload["lineage_nodes"] if row["artifact_type"] == "market_open_data_gate_v1")
    assert gate["lineage_status"] == "FAIL"
    assert gate["canonical_blocker"] == "PRODUCER_CONTRACT_MISSING"


def test_lineage_does_not_warn_for_required_authority_with_only_optional_missing_inputs(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base_artifacts(ctx)
    day_run_path = _report(ctx, "aegis_day_run_v1", "day_run.v1.json")
    optional_submission_dir = ctx.execution_root / "execution_evidence_v1" / "submissions" / DAY
    _with_contract(
        day_run_path,
        {
            "schema_id": "aegis_day_run",
            "schema_version": "aegis_day_run.v1",
            "final_status": "NOT_READY",
            "canonical_phase": "BOD_INPUTS",
            "canonical_blocker": "TARGET_DAY_DATE_MISMATCH",
            "source_repo_status": {"git_dirty_status": "CLEAN", "source_reproducibility_status": "REPRODUCIBLE"},
        },
        inputs=[optional_submission_dir],
    )

    payload = lineage.build_evidence_lineage_index_v1(ctx)
    day_run = next(row for row in payload["lineage_nodes"] if row["artifact_type"] == "aegis_day_run_v1")

    assert day_run["required_for_current_day"] is True
    assert day_run["lineage_status"] == "PASS"


def _invariant(payload: dict, invariant_id: str) -> dict:
    return next(row for row in payload["invariant_results"] if row["invariant_id"] == invariant_id)


def test_state_consistency_invariant_pass_and_fail_cases(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base_artifacts(ctx)
    payload = consistency.build_state_consistency_v1(ctx)
    for invariant_id in [
        "SOURCE_INTEGRITY_CLEAN_IMPLIES_NO_SOURCE_BLOCKER",
        "DAY_RUN_IS_FINAL_READINESS_AUTHORITY",
        "BLOCKED_LEDGER_CAPS_LIVE_INTELLIGENCE",
        "AUTHORIZATION_PASS_REQUIRES_AUTHORIZED_INTENT",
        "MARKET_DATA_BLOCKED_PROPAGATES_TO_STRATEGY",
        "SUBMIT_FORBIDDEN_WHILE_LEDGER_BLOCKED",
        "REQUIREMENT_GRAPH_PASS_REQUIRES_REQUIRED_ARTIFACTS_PRESENT",
    ]:
        assert _invariant(payload, invariant_id)["status"] == "PASS"

    _write(_report(ctx, "aegis_operator_projection_v1", "operator_projection.v1.json"), {"day_utc": DAY, "final_status": "PAPER_READY", "canonical_blocker": ""})
    assert _invariant(consistency.build_state_consistency_v1(ctx), "DAY_RUN_IS_FINAL_READINESS_AUTHORITY")["status"] == "FAIL"
    _base_artifacts(ctx)
    _write(_report(ctx, "aegis_live_intelligence_v1", "live_intelligence.v1.json"), {"day_utc": DAY, "status": "PASS", "final_status_observed": "NOT_READY", "advisory_only": False, "opportunity_radar": {"actionable_recommendations": ["trade"]}})
    assert _invariant(consistency.build_state_consistency_v1(ctx), "BLOCKED_LEDGER_CAPS_LIVE_INTELLIGENCE")["status"] == "FAIL"
    _base_artifacts(ctx)
    _write(_report(ctx, "authorization_supply_v1", "authorization_supply.v1.json"), {"day_utc": DAY, "status": "PASS", "canonical_blocker": "", "authorization_export": {"authorized_intents": []}})
    assert _invariant(consistency.build_state_consistency_v1(ctx), "AUTHORIZATION_PASS_REQUIRES_AUTHORIZED_INTENT")["status"] == "FAIL"
    _base_artifacts(ctx)
    _write(_report(ctx, "strategy_decision_authority_v1", "strategy_decision_authority.v1.json"), {"day_utc": DAY, "status": "PASS", "strategy_decision_state": "READY"})
    assert _invariant(consistency.build_state_consistency_v1(ctx), "MARKET_DATA_BLOCKED_PROPAGATES_TO_STRATEGY")["status"] == "FAIL"
    _base_artifacts(ctx)
    _write(_report(ctx, "submit_boundary_status_v1", "submit_boundary_status.v1.json"), {"day_utc": DAY, "submit_allowed": True, "submission_authorized": True})
    assert _invariant(consistency.build_state_consistency_v1(ctx), "SUBMIT_FORBIDDEN_WHILE_LEDGER_BLOCKED")["status"] == "FAIL"
    _base_artifacts(ctx)
    _write(_report(ctx, "aegis_day_run_v1", "day_run.v1.json"), {"day_utc": DAY, "final_status": "NOT_READY", "canonical_blocker": "SOURCE_REPRODUCIBILITY_BLOCKED", "source_repo_status": {"git_dirty_status": "CLEAN", "source_reproducibility_status": "REPRODUCIBLE"}})
    assert _invariant(consistency.build_state_consistency_v1(ctx), "SOURCE_INTEGRITY_CLEAN_IMPLIES_NO_SOURCE_BLOCKER")["status"] == "FAIL"
    _base_artifacts(ctx)
    missing_path = ctx.truth_root / "reports" / "missing" / DAY / "missing.json"
    _write(_report(ctx, "aegis_requirement_graph_v1", "requirement_graph.v1.json"), {"day_utc": DAY, "status": "PASS", "requirements": [{"blocking_class": "HARD_BLOCKER", "status": "SATISFIED", "expected_path": str(missing_path)}]})
    assert _invariant(consistency.build_state_consistency_v1(ctx), "REQUIREMENT_GRAPH_PASS_REQUIRES_REQUIRED_ARTIFACTS_PRESENT")["status"] == "FAIL"


def test_truth_freshness_statuses(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base_artifacts(ctx)
    observed = "2026-04-29T14:05:00Z"
    payload = freshness.build_truth_freshness_v1(ctx, observed_at_utc=observed)
    assert any(row["freshness_status"] == "FRESH" for row in payload["freshness_records"])

    _write(_report(ctx, "broker_supply_v1", "broker_supply.v1.json"), {"day_utc": DAY, "status": "PASS"})
    payload = freshness.build_truth_freshness_v1(ctx, observed_at_utc=observed)
    assert next(row for row in payload["freshness_records"] if row["artifact_type"] == "broker_supply_v1")["freshness_status"] == "UNKNOWN"

    _with_contract(_report(ctx, "broker_supply_v1", "broker_supply.v1.json"), {"day_utc": DAY, "generated_at_utc": "2026-04-27T14:00:00Z", "status": "PASS"})
    payload = freshness.build_truth_freshness_v1(ctx, observed_at_utc=observed)
    assert next(row for row in payload["freshness_records"] if row["artifact_type"] == "broker_supply_v1")["freshness_status"] == "EXPIRED"


def test_action_validity_state_transitions(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base_artifacts(ctx)
    payload = action_validity.build_action_validity_v1(ctx)
    rules = {row["action_id"]: row for row in payload["action_rules"]}
    assert rules["submit_paper_order"]["status"] == "FORBIDDEN"
    assert rules["enable_broker_transmit"]["risk_level"] == "PROHIBITED"
    assert rules["inspect_market_data_artifacts"]["status"] == "ALLOWED"
    assert rules["rerun_market_open_data_gate"]["status"] == "BLOCKED"

    _write(_report(ctx, "aegis_day_run_v1", "day_run.v1.json"), {"day_utc": DAY, "final_status": "NOT_READY", "canonical_blocker": "SOURCE_REPRODUCIBILITY_BLOCKED"})
    rules = {row["action_id"]: row for row in action_validity.build_action_validity_v1(ctx)["action_rules"]}
    assert rules["clean_and_protect_repo"]["status"] == "ALLOWED"


def test_outcome_attribution_advisory_only_cases(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    payload = outcome.build_outcome_attribution_v1(ctx)
    assert payload["advisory_only"] is True
    assert payload["status"] == "NOT_APPLICABLE"
    assert payload["readiness_effect"] == "NONE"

    sub = ctx.execution_root / "execution_evidence_v1" / "submissions" / DAY / "abc" / "broker_submission_record.v2.json"
    _write(sub, {"submission_id": "abc", "status": "FILLED", "intent_id": "intent1"})
    payload = outcome.build_outcome_attribution_v1(ctx)
    assert payload["attribution_records"][0]["attribution_status"] == "INSUFFICIENT_EVIDENCE"
    assert payload["attribution_records"][0]["human_review_required"] is True


def test_projection_and_live_integrity_integration(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base_artifacts(ctx)
    av_path = action_validity.action_validity_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    _write(av_path, action_validity.build_action_validity_v1(ctx))
    _write(freshness.truth_freshness_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), freshness.build_truth_freshness_v1(ctx, observed_at_utc="2026-04-29T14:05:00Z"))
    _write(consistency.state_consistency_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), consistency.build_state_consistency_v1(ctx))
    _write(lineage.evidence_lineage_index_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), lineage.build_evidence_lineage_index_v1(ctx))
    _write(outcome.outcome_attribution_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), outcome.build_outcome_attribution_v1(ctx))

    proj = projection._projection_for(
        ctx,
        json.loads(_report(ctx, "aegis_day_run_v1", "day_run.v1.json").read_text()),
        json.loads(_report(ctx, "aegis_requirement_graph_v1", "requirement_graph.v1.json").read_text()),
        json.loads(lineage.evidence_lineage_index_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc).read_text()),
        json.loads(consistency.state_consistency_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc).read_text()),
        json.loads(freshness.truth_freshness_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc).read_text()),
        json.loads(av_path.read_text()),
    )
    assert proj["action_validity_status"] == "PASS"
    assert "Submit paper order" not in proj["next_valid_actions"]

    _write_truth_integrity_reports(ctx)
    live_payload = live.build_live_intelligence_v1(ctx)
    assert live_payload["advisory_only"] is True
    assert live_payload["opportunity_radar"]["actionable_recommendations"] == []
    assert live_payload["action_validity_status"] == "PASS"


def _write_truth_integrity_reports(ctx: bod.BodContext) -> dict:
    _write(action_validity.action_validity_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), action_validity.build_action_validity_v1(ctx))
    _write(freshness.truth_freshness_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), freshness.build_truth_freshness_v1(ctx, observed_at_utc="2026-04-29T14:05:00Z"))
    _write(consistency.state_consistency_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), consistency.build_state_consistency_v1(ctx))
    _write(lineage.evidence_lineage_index_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), lineage.build_evidence_lineage_index_v1(ctx))
    _write(outcome.outcome_attribution_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), outcome.build_outcome_attribution_v1(ctx))
    payload = kernel.build_unified_truth_kernel_v1(ctx)
    payload["producer_contract_v1"] = producer_contract_v1(
        producer_name="ops/tools/run_unified_truth_kernel_v1.py",
        producer_command="python3 ops/tools/run_unified_truth_kernel_v1.py",
        input_artifacts=[Path(path) for path in payload["input_artifact_paths"]],
        output_artifacts=[kernel.unified_truth_kernel_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)],
        schema_versions={"unified_truth_kernel": "unified_truth_kernel.v1"},
    )
    payload["producer_contract"] = dict(payload["producer_contract_v1"])
    _write(kernel.unified_truth_kernel_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), payload)
    return payload


def test_unified_truth_kernel_enforces_day_run_final_status_and_schema(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base_artifacts(ctx)
    _write(_report(ctx, "aegis_operator_projection_v1", "operator_projection.v1.json"), {"day_utc": DAY, "final_status": "PAPER_READY", "canonical_blocker": ""})
    _write(_report(ctx, "aegis_live_intelligence_v1", "live_intelligence.v1.json"), {"day_utc": DAY, "status": "PASS", "final_status_observed": "PAPER_READY", "advisory_only": False, "opportunity_radar": {"actionable_recommendations": ["trade"]}})
    payload = _write_truth_integrity_reports(ctx)

    assert payload["final_status"] == "NOT_READY"
    assert payload["final_status_source"] == "aegis_day_run_ledger_v1"
    assert payload["first_blocker"] == "MARKET_CLOSED"
    assert payload["advisory_intelligence_status"] in {"PASS", "NOT_READY"}
    validate_against_repo_schema_v1(payload, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/REPORTS/unified_truth_kernel.v1.schema.json")


def test_unified_truth_kernel_missing_advisory_does_not_block_but_stale_authority_lowers_confidence(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base_artifacts(ctx)
    payload = _write_truth_integrity_reports(ctx)
    assert payload["trade_health_status"] == "UNKNOWN"
    assert payload["canonical_blocker"] == "MARKET_CLOSED"

    _with_contract(_report(ctx, "broker_supply_v1", "broker_supply.v1.json"), {"day_utc": DAY, "generated_at_utc": "2026-04-27T14:00:00Z", "status": "PASS"})
    _write(freshness.truth_freshness_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc), freshness.build_truth_freshness_v1(ctx, observed_at_utc="2026-04-29T14:05:00Z"))
    payload = kernel.build_unified_truth_kernel_v1(ctx)
    assert payload["truth_confidence"] == "LOW"
    assert any(row["artifact_type"] == "broker_supply_v1" for row in payload["unknown_or_untrusted_artifacts"])


def test_projection_and_live_consume_unified_truth_kernel(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base_artifacts(ctx)
    payload = _write_truth_integrity_reports(ctx)
    proj = projection._projection_from_kernel(ctx, payload)
    assert proj["final_status"] == payload["final_status"]
    assert proj["next_valid_actions"] == [row["label"] for row in payload["allowed_operator_actions"]]

    live_payload = live.build_live_intelligence_v1(ctx)
    assert live_payload["status"] == "NOT_READY"
    assert live_payload["advisory_only"] is True
    assert live_payload["opportunity_radar"]["actionable_recommendations"] == []
    assert live_payload["kernel_final_status_source"] == "aegis_day_run_ledger_v1"


def test_projection_uses_concrete_blocker_evidence_for_known_hard_blockers(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    base_kernel = {
        "final_status": "NOT_READY",
        "final_status_source": "aegis_day_run_ledger_v1",
        "first_blocker_phase": "BOD_INPUTS",
        "first_blocker_owner": "risk_control",
        "allowed_operator_actions": [{"label": "Rerun day"}],
        "forbidden_operator_actions": [{"label": "Submit paper order"}],
        "unsafe_actions": ["Submit paper order"],
        "downstream_consequences": ["Submit remains blocked."],
        "truth_confidence": "HIGH",
        "trade_health": {},
        "human_review_required": False,
    }

    kill = projection._projection_from_kernel(ctx, {**base_kernel, "first_blocker": "C2_KILL_SWITCH_ACTIVE", "canonical_blocker": "C2_KILL_SWITCH_ACTIVE"})
    assert kill["evidence_paths"]
    assert "global_kill_switch_state.v1.json" in kill["evidence_paths"][0]
    assert "kill switch" in kill["operator_next_action"].lower()

    session = projection._projection_from_kernel(ctx, {**base_kernel, "first_blocker": "SESSION_AUTHORITY_MISSING", "canonical_blocker": "SESSION_AUTHORITY_MISSING"})
    assert session["evidence_paths"]
    assert "paper_session_authority.v1.json" in session["evidence_paths"][0]
    assert "run_paper_session_bootstrap_v1.py" in session["operator_next_action"]

    mismatch = projection._projection_from_kernel(ctx, {**base_kernel, "first_blocker": "TARGET_DAY_DATE_MISMATCH", "canonical_blocker": "TARGET_DAY_DATE_MISMATCH"})
    assert "paper capital seed" in mismatch["operator_next_action"]
    assert "operator statement" in mismatch["operator_next_action"]
    assert "pre-open bundle" in mismatch["operator_next_action"]
    assert any("paper_capital_seed.v1.json" in path for path in mismatch["evidence_paths"])
    assert any("operator_statement.v1.json" in path for path in mismatch["evidence_paths"])
    assert any("pre_open_bundle.v1.json" in path for path in mismatch["evidence_paths"])


def test_ui_readiness_kernel_uses_unified_truth_kernel_when_present(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base_artifacts(ctx)
    payload = _write_truth_integrity_reports(ctx)
    _write(_report(ctx, "aegis_day_run_v1", "day_run.v1.json"), {"day_utc": DAY, "final_status": "PAPER_READY", "canonical_blocker": ""})

    ui = build_readiness_kernel_v1(day=DAY, truth_root=ctx.truth_root, sleeve_truth_root=ctx.execution_root)
    assert ui["overall_status"] == "BLOCKED"
    assert ui["canonical_blocker"] == payload["canonical_blocker"]
    assert ui["truth_resolution_source_path"].endswith("unified_truth_kernel.v1.json")


def test_unified_truth_kernel_trade_health_is_advisory_when_present(tmp_path: Path) -> None:
    ctx = _ctx(tmp_path)
    _base_artifacts(ctx)
    _write(_report(ctx, "edge_attribution_v1", "edge_attribution.v1.json"), {"day_utc": DAY, "status": "UNPROVEN"})
    _write(_report(ctx, "regime_confidence_v1", "regime_confidence.v1.json"), {"day_utc": DAY, "status": "UNKNOWN"})
    _write(_report(ctx, "trade_outcome_v1", "trade_outcome.v1.json"), {"day_utc": DAY, "status": "UNKNOWN", "outcome_status": "UNKNOWN"})
    _write(_report(ctx, "decision_consistency_v1", "decision_consistency.v1.json"), {"day_utc": DAY, "status": "PASS"})
    _write(_report(ctx, "missed_opportunity_v1", "missed_opportunity.v1.json"), {"day_utc": DAY, "status": "PASS", "alternatives": [{"intent_id": "alt"}]})
    _write(_report(ctx, "insight_engine_v1", "insight_engine.v1.json"), {"day_utc": DAY, "status": "PASS", "drift_alerts": [{"code": "DRIFT"}]})
    _write(_report(ctx, "ai_advisory_review_v1", "ai_advisory_review.v1.json"), {"day_utc": DAY, "status": "PASS", "requires_human_review": True})
    _write(_report(ctx, "strategy_change_governance_v1", "strategy_change_governance.v1.json"), {"day_utc": DAY, "status": "PENDING", "human_approval_required": True, "automatic_deployment_allowed": False})

    payload = _write_truth_integrity_reports(ctx)

    assert payload["final_status"] == "NOT_READY"
    assert payload["trade_health_status"] == "ADVISORY_ONLY"
    assert payload["trade_health"]["edge_status"] == "UNPROVEN"
    assert payload["trade_health"]["outcome_status"] == "UNKNOWN"
    assert payload["trade_health"]["regime_status"] == "UNKNOWN"
    assert payload["trade_health"]["missed_opportunity_count"] == 1
    assert payload["trade_health"]["drift_alert_count"] == 1
    assert payload["trade_health"]["human_review_required"] is True
    assert payload["trade_health"]["automatic_deployment_allowed"] is False
