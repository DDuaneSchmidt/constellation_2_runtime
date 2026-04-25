from __future__ import annotations

import sys
from pathlib import Path


SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))


MIGRATED_FIRST_SET = {
    "ops/tools/run_reconciliation_report_v3.py": "ops/tools/run_reconciliation_report_v3.py",
    "ops/tools/run_lifecycle_monitor_v1.py": "ops/tools/run_lifecycle_monitor_v1.py",
    "ops/tools/run_regime_snapshot_v2.py": "ops/tools/run_regime_snapshot_v2.py",
    "ops/tools/run_engine_daily_returns_day_v1.py": "ops/tools/run_engine_daily_returns_day_v1.py",
    "constellation_2/phaseJ/monitoring/run/run_engine_correlation_matrix_day_v1.py": (
        "constellation_2/phaseJ/monitoring/run/run_engine_correlation_matrix_day_v1.py"
    ),
}


MIGRATED_SECOND_SET = {
    "constellation_2/phaseJ/monitoring/run/run_capital_efficiency_day_v1.py": (
        "constellation_2/phaseJ/monitoring/run/run_capital_efficiency_day_v1.py"
    ),
    "constellation_2/phaseJ/monitoring/run/run_degradation_sentinel_day_v1.py": (
        "constellation_2/phaseJ/monitoring/run/run_degradation_sentinel_day_v1.py"
    ),
    "constellation_2/phaseJ/monitoring/run/run_engine_daily_returns_day_v1.py": (
        "constellation_2/phaseJ/monitoring/run/run_engine_daily_returns_day_v1.py"
    ),
    "constellation_2/phaseJ/monitoring/run/run_engine_metrics_day_v1.py": (
        "constellation_2/phaseJ/monitoring/run/run_engine_metrics_day_v1.py"
    ),
    "constellation_2/phaseJ/monitoring/run/run_portfolio_nav_series_day_v1.py": (
        "constellation_2/phaseJ/monitoring/run/run_portfolio_nav_series_day_v1.py"
    ),
    "constellation_2/phaseJ/monitoring/run/run_stress_replay_report_day_v1.py": (
        "constellation_2/phaseJ/monitoring/run/run_stress_replay_report_day_v1.py"
    ),
    "constellation_2/phaseJ/reporting/daily_snapshot_v1.py": "constellation_2/phaseJ/reporting/daily_snapshot_v1.py",
    "constellation_2/phaseJ/reporting/daily_summary_v1.py": "constellation_2/phaseJ/reporting/daily_summary_v1.py",
    "ops/tools/run_active_engine_set_snapshot_v1.py": "ops/tools/run_active_engine_set_snapshot_v1.py",
    "ops/tools/run_intents_summary_day_v1.py": "ops/tools/run_intents_summary_day_v1.py",
    "ops/tools/run_regime_snapshot_v1.py": "ops/tools/run_regime_snapshot_v1.py",
    "ops/tools/run_regime_snapshot_v3.py": "ops/tools/run_regime_snapshot_v3.py",
    "ops/tools/run_weekly_engine_diagnostic_report_v1.py": "ops/tools/run_weekly_engine_diagnostic_report_v1.py",
}


MIGRATED_RUNTIME_CRITICAL_ROOT_ONLY_SET = {
    "ops/run/c2_supervisor_paper_v2.py": "ops/run/c2_supervisor_paper_v2.py",
    "ops/tools/run_c2_capital_risk_envelope_gate_v2.py": "ops/tools/run_c2_capital_risk_envelope_gate_v2.py",
    "ops/tools/run_c2_daily_operator_gate_v1.py": "ops/tools/run_c2_daily_operator_gate_v1.py",
    "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py": "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py",
    "ops/tools/run_c2_paper_day_orchestrator_v2.py": "ops/tools/run_c2_paper_day_orchestrator_v2.py",
    "ops/tools/run_correlation_preconditions_gate_v1.py": "ops/tools/run_correlation_preconditions_gate_v1.py",
    "ops/tools/run_correlation_preconditions_gate_v2.py": "ops/tools/run_correlation_preconditions_gate_v2.py",
    "ops/tools/run_engine_model_registry_gate_v1.py": "ops/tools/run_engine_model_registry_gate_v1.py",
    "ops/tools/run_execution_readiness_gate_v1.py": "ops/tools/run_execution_readiness_gate_v1.py",
    "ops/tools/run_feed_attestation_gate_v1.py": "ops/tools/run_feed_attestation_gate_v1.py",
    "ops/tools/run_gate_authority_plane_v1.py": "ops/tools/run_gate_authority_plane_v1.py",
    "ops/tools/run_gate_completeness_gate_v1.py": "ops/tools/run_gate_completeness_gate_v1.py",
    "ops/tools/run_heartbeat_gate_v1.py": "ops/tools/run_heartbeat_gate_v1.py",
    "ops/tools/run_liquidity_slippage_gate_v1.py": "ops/tools/run_liquidity_slippage_gate_v1.py",
    "ops/tools/run_operator_daily_gate_v1.py": "ops/tools/run_operator_daily_gate_v1.py",
    "ops/tools/run_operator_daily_gate_v2.py": "ops/tools/run_operator_daily_gate_v2.py",
    "ops/tools/run_operator_gate_verdict_v2.py": "ops/tools/run_operator_gate_verdict_v2.py",
    "ops/tools/run_operator_gate_verdict_v3.py": "ops/tools/run_operator_gate_verdict_v3.py",
    "ops/tools/run_recurrence_kill_gate_v1.py": "ops/tools/run_recurrence_kill_gate_v1.py",
    "ops/tools/run_systemic_risk_gate_v1.py": "ops/tools/run_systemic_risk_gate_v1.py",
    "ops/tools/run_systemic_risk_gate_v2.py": "ops/tools/run_systemic_risk_gate_v2.py",
    "ops/tools/run_systemic_risk_gate_v3.py": "ops/tools/run_systemic_risk_gate_v3.py",
    "ops/tools/run_trade_submit_readiness_c2_v1.py": "ops/tools/run_trade_submit_readiness_c2_v1.py",
    "ops/tools/run_truth_surface_authority_gate_v1.py": "ops/tools/run_truth_surface_authority_gate_v1.py",
}


DO_NOT_TOUCH_SET = {
    "constellation_2/common/runtime_identity_v1.py",
    "constellation_2/common/fresh_day_admission_v1.py",
    "ops/tools/run_paper_session_bootstrap_v1.py",
    "ops/tools/run_paper_session_admission_v1.py",
    "ops/tools/run_global_kill_switch_v1.py",
    "ops/tools/run_gate_stack_verdict_v1.py",
}


RUNTIME_CRITICAL_BLOCKED_SET = {
    "constellation_2/common/fresh_day_admission_v1.py",
    "constellation_2/common/runtime_identity_v1.py",
    "constellation_2/common/trade_submit_readiness_authority_v1.py",
    "ops/tools/run_fresh_day_admission_v1.py",
    "ops/tools/run_global_kill_switch_v1.py",
    "ops/tools/run_paper_session_admission_v1.py",
    "ops/tools/run_paper_trading_integration_v1.py",
}


BLOCKED_BY_DECISION_ROOT_SET = {
    "constellation_2/common/capability_state_v1.py",
    "constellation_2/common/next_day_readiness_probe_v1.py",
    "ops/tools/run_bod_execution_environment_proof_v1.py",
    "ops/tools/run_c2_global_monitoring_refresh_v1.py",
    "ops/tools/run_canonical_lifecycle_closure_day_v1.py",
    "ops/tools/run_capability_state_v1.py",
    "ops/tools/run_day_failure_causality_v1.py",
    "ops/tools/run_day_open_attempt_v1.py",
    "ops/tools/run_day_open_trigger_v1.py",
    "ops/tools/run_deployment_state_machine_v1.py",
    "ops/tools/run_market_calendar_coverage_authority_v1.py",
    "ops/tools/run_market_calendar_source_coverage_check_v1.py",
    "ops/tools/run_next_day_readiness_probe_v1.py",
    "ops/tools/run_operator_day_authority_summary_v1.py",
    "ops/tools/run_paper_policy_verdict_v1.py",
    "ops/tools/run_paper_session_bootstrap_v1.py",
    "ops/tools/run_paper_session_ledger_v1.py",
    "ops/tools/run_paper_startup_authorization_convergence_v1.py",
    "ops/tools/run_paper_startup_intent_input_convergence_v1.py",
    "ops/tools/run_paper_trading_posture_v1.py",
    "ops/tools/run_policy_diff_v1.py",
    "ops/tools/run_production_policy_verdict_v1.py",
    "ops/tools/run_session_authority_alert_v1.py",
    "ops/tools/run_session_authority_status_v1.py",
    "ops/tools/run_session_authority_v1.py",
    "ops/tools/run_startup_materialization_input_convergence_v1.py",
    "ops/tools/run_startup_materialization_v1.py",
    "ops/tools/run_startup_proof_validation_v1.py",
    "ops/tools/run_submit_boundary_status_v1.py",
    "ops/tools/run_subsystem_authority_v1.py",
    "ops/tools/run_tomorrow_paper_startup_prep_v1.py",
    "ops/tools/run_trading_day_state_machine_v1.py",
}


BLOCKED_BY_RUNTIME_IDENTITY_SET = {
    "constellation_2/common/runtime_contract_v1.py",
}


def _read(relpath: str) -> str:
    return (SOURCE_ROOT / relpath).resolve().read_text(encoding="utf-8")


def _assert_bridge_cutover(relpath: str, caller_label: str) -> None:
    text = _read(relpath)
    assert "runtime_authority_bridge_v1" in text, relpath
    assert f'caller="{caller_label}"' in text, relpath
    assert (
        "resolve_canonical_truth_root_bridge_v1" in text
        or "resolve_truth_root_bridge_v1" in text
    ), relpath
    assert "from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root" not in text, relpath
    assert "from constellation_2.common.truth_root_v1 import resolve_truth_root" not in text, relpath


def test_first_bridge_cutover_callers_use_runtime_authority_bridge() -> None:
    for relpath, caller_label in MIGRATED_FIRST_SET.items():
        _assert_bridge_cutover(relpath, caller_label)


def test_second_bridge_cutover_callers_use_runtime_authority_bridge() -> None:
    for relpath, caller_label in MIGRATED_SECOND_SET.items():
        _assert_bridge_cutover(relpath, caller_label)


def test_runtime_critical_root_only_cutover_callers_use_runtime_authority_bridge() -> None:
    for relpath, caller_label in MIGRATED_RUNTIME_CRITICAL_ROOT_ONLY_SET.items():
        _assert_bridge_cutover(relpath, caller_label)


def _assert_not_importing_bridge(relpath: str) -> None:
    text = _read(relpath)
    assert "runtime_authority_bridge_v1" not in text, relpath
    assert "resolve_canonical_truth_root_bridge_v1" not in text, relpath
    assert "resolve_truth_root_bridge_v1" not in text, relpath


def test_runtime_critical_do_not_touch_set_still_not_importing_bridge() -> None:
    for relpath in DO_NOT_TOUCH_SET:
        _assert_not_importing_bridge(relpath)


def test_blocked_runtime_critical_set_still_not_importing_bridge() -> None:
    for relpath in RUNTIME_CRITICAL_BLOCKED_SET:
        _assert_not_importing_bridge(relpath)


def test_blocked_by_decision_root_set_still_not_importing_bridge() -> None:
    for relpath in BLOCKED_BY_DECISION_ROOT_SET:
        _assert_not_importing_bridge(relpath)


def test_blocked_by_runtime_identity_set_still_not_importing_bridge() -> None:
    for relpath in BLOCKED_BY_RUNTIME_IDENTITY_SET:
        _assert_not_importing_bridge(relpath)


def test_phase3af_runtime_bootstrap_targets_no_longer_use_primary_legacy_reads() -> None:
    runtime_path_text = _read("constellation_2/common/runtime_path_authority_v1.py")
    assert "load_release_current_runtime_authority_v1" in runtime_path_text
    assert "resolve_release_provenance(" not in runtime_path_text

    systemd_text = _read("ops/run/c2_paper_day_orchestrator_systemd_entry_v1.sh")
    assert "load_release_current_runtime_authority_v1" in systemd_text
    assert "--surface release_manifest_active" not in systemd_text

    recurrence_text = _read("ops/tools/run_recurrence_kill_gate_v1.py")
    assert "load_active_runtime_contract_or_fail" in recurrence_text
    assert "read_json_object_v1(ACTIVE_RUNTIME_CONTRACT_PATH)" not in recurrence_text

    orchestrator_text = _read("ops/tools/run_c2_paper_day_orchestrator_v2.py")
    assert "def _git_sha(" not in orchestrator_text
    assert "resolve_release_provenance(" not in orchestrator_text
