from __future__ import annotations

import sys
from pathlib import Path


SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


MIGRATED_SAFE_STAMPING_LOOKUP_SET = {
    "ops/tools/run_bod_execution_environment_proof_v1.py": "ops/tools/run_bod_execution_environment_proof_v1.py",
    "ops/tools/run_day_failure_causality_v1.py": "ops/tools/run_day_failure_causality_v1.py",
    "ops/tools/run_next_day_readiness_probe_v1.py": "ops/tools/run_next_day_readiness_probe_v1.py",
    "ops/tools/run_operator_day_authority_summary_v1.py": "ops/tools/run_operator_day_authority_summary_v1.py",
    "ops/tools/run_policy_diff_v1.py": "ops/tools/run_policy_diff_v1.py",
    "ops/tools/run_session_authority_status_v1.py": "ops/tools/run_session_authority_status_v1.py",
    "ops/tools/run_startup_proof_validation_v1.py": "ops/tools/run_startup_proof_validation_v1.py",
    "ops/tools/run_submit_boundary_status_v1.py": "ops/tools/run_submit_boundary_status_v1.py",
}


MIGRATED_RUNTIME_CRITICAL_DECISION_SET = {
    "ops/tools/run_fresh_day_admission_v1.py": "ops/tools/run_fresh_day_admission_v1.py",
    "ops/tools/run_paper_session_admission_v1.py": "ops/tools/run_paper_session_admission_v1.py",
    "ops/tools/run_paper_session_bootstrap_v1.py": "ops/tools/run_paper_session_bootstrap_v1.py",
    "ops/tools/run_paper_trading_integration_v1.py": "ops/tools/run_paper_trading_integration_v1.py",
}


RUNTIME_CRITICAL_DO_NOT_TOUCH_SET = {
    "constellation_2/common/fresh_day_admission_v1.py",
    "constellation_2/common/runtime_identity_v1.py",
    "constellation_2/common/trade_submit_readiness_authority_v1.py",
    "ops/run/c2_supervisor_paper_v2.py",
    "ops/tools/run_c2_capital_risk_envelope_gate_v2.py",
    "ops/tools/run_c2_daily_operator_gate_v1.py",
    "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py",
    "ops/tools/run_c2_paper_day_orchestrator_v2.py",
    "ops/tools/run_correlation_preconditions_gate_v1.py",
    "ops/tools/run_correlation_preconditions_gate_v2.py",
    "ops/tools/run_engine_model_registry_gate_v1.py",
    "ops/tools/run_execution_readiness_gate_v1.py",
    "ops/tools/run_feed_attestation_gate_v1.py",
    "ops/tools/run_gate_authority_plane_v1.py",
    "ops/tools/run_gate_stack_verdict_v1.py",
    "ops/tools/run_gate_completeness_gate_v1.py",
    "ops/tools/run_global_kill_switch_v1.py",
    "ops/tools/run_heartbeat_gate_v1.py",
    "ops/tools/run_liquidity_slippage_gate_v1.py",
    "ops/tools/run_operator_daily_gate_v1.py",
    "ops/tools/run_operator_daily_gate_v2.py",
    "ops/tools/run_operator_gate_verdict_v2.py",
    "ops/tools/run_operator_gate_verdict_v3.py",
    "ops/tools/run_recurrence_kill_gate_v1.py",
    "ops/tools/run_systemic_risk_gate_v1.py",
    "ops/tools/run_systemic_risk_gate_v2.py",
    "ops/tools/run_systemic_risk_gate_v3.py",
    "ops/tools/run_trade_submit_readiness_c2_v1.py",
    "ops/tools/run_truth_surface_authority_gate_v1.py",
}


REMAINING_BLOCKED_BY_DECISION_ROOT_SET = {
    "constellation_2/common/capability_state_v1.py",
    "constellation_2/common/next_day_readiness_probe_v1.py",
    "ops/tools/run_c2_global_monitoring_refresh_v1.py",
    "ops/tools/run_canonical_lifecycle_closure_day_v1.py",
    "ops/tools/run_capability_state_v1.py",
    "ops/tools/run_day_open_attempt_v1.py",
    "ops/tools/run_day_open_trigger_v1.py",
    "ops/tools/run_deployment_state_machine_v1.py",
    "ops/tools/run_market_calendar_coverage_authority_v1.py",
    "ops/tools/run_market_calendar_source_coverage_check_v1.py",
    "ops/tools/run_paper_policy_verdict_v1.py",
    "ops/tools/run_paper_session_ledger_v1.py",
    "ops/tools/run_paper_startup_authorization_convergence_v1.py",
    "ops/tools/run_paper_startup_intent_input_convergence_v1.py",
    "ops/tools/run_paper_trading_posture_v1.py",
    "ops/tools/run_production_policy_verdict_v1.py",
    "ops/tools/run_session_authority_alert_v1.py",
    "ops/tools/run_session_authority_v1.py",
    "ops/tools/run_startup_materialization_input_convergence_v1.py",
    "ops/tools/run_startup_materialization_v1.py",
    "ops/tools/run_subsystem_authority_v1.py",
    "ops/tools/run_tomorrow_paper_startup_prep_v1.py",
    "ops/tools/run_trading_day_state_machine_v1.py",
}


def _assert_cutover_to_decision_bridge(relpath: str, caller_label: str) -> None:
    path = (SOURCE_ROOT / relpath).resolve()
    text = path.read_text(encoding="utf-8")
    assert "decision_authority_bridge_v1" in text, relpath
    assert "resolve_decision_truth_root_bridge_v1" in text, relpath
    assert f'caller="{caller_label}"' in text, relpath
    assert "resolve_decision_truth_root_v1(" not in text, relpath


def _assert_not_importing_decision_bridge(relpath: str) -> None:
    path = (SOURCE_ROOT / relpath).resolve()
    text = path.read_text(encoding="utf-8")
    assert "decision_authority_bridge_v1" not in text, relpath
    assert "resolve_decision_truth_root_bridge_v1" not in text, relpath


def test_migrated_safe_stamping_lookup_set_uses_decision_bridge() -> None:
    for relpath, caller_label in MIGRATED_SAFE_STAMPING_LOOKUP_SET.items():
        _assert_cutover_to_decision_bridge(relpath, caller_label)


def test_migrated_runtime_critical_decision_set_uses_decision_bridge() -> None:
    for relpath, caller_label in MIGRATED_RUNTIME_CRITICAL_DECISION_SET.items():
        _assert_cutover_to_decision_bridge(relpath, caller_label)


def test_runtime_critical_set_still_not_importing_decision_bridge() -> None:
    for relpath in RUNTIME_CRITICAL_DO_NOT_TOUCH_SET:
        _assert_not_importing_decision_bridge(relpath)


def test_remaining_blocked_decision_root_set_still_not_importing_decision_bridge() -> None:
    for relpath in REMAINING_BLOCKED_BY_DECISION_ROOT_SET:
        _assert_not_importing_decision_bridge(relpath)
