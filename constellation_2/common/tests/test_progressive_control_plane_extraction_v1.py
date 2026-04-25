from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path("/home/node/constellation").resolve()


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


def test_run_session_authority_control_plane_module_is_gateway_only() -> None:
    source = _read("ops/tools/run_session_authority_control_plane_v1.py")
    assert "read_control_plane_surface_v1(" in source
    for forbidden in (".open(", ".read_text(", ".read_bytes(", ".iterdir(", ".glob(", ".rglob("):
        assert forbidden not in source


def test_status_collector_control_plane_module_is_gateway_only() -> None:
    source = _read("constellation_2/phaseL/ui/server/c2_ops_cockpit_status_v2_collector_control_plane_v1.py")
    assert "read_control_plane_surface_v1(" in source
    for forbidden in (".open(", ".read_text(", ".read_bytes(", ".iterdir(", ".glob(", ".rglob("):
        assert forbidden not in source


def test_kernel_operator_shell_control_plane_module_is_gateway_only() -> None:
    source = _read("constellation_2/phaseL/ui_api/kernel_operator_shell_control_plane_v1.py")
    assert "read_control_plane_surface_v1(" in source
    for forbidden in (".open(", ".read_text(", ".read_bytes(", ".iterdir(", ".glob(", ".rglob("):
        assert forbidden not in source


def test_target_functions_call_extracted_control_plane_modules() -> None:
    session_source = _read("ops/tools/run_session_authority_v1.py")
    collector_source = _read("constellation_2/phaseL/ui/server/c2_ops_cockpit_status_v2_collector_v1.py")
    assert "collect_target_day_build_source_refs_v1(" in session_source
    assert "finalize_target_day_build_v1(" in session_source
    assert "load_status_collector_control_plane_bundle_v1(" in collector_source


def test_control_plane_modules_do_not_import_diagnostic_helpers() -> None:
    session_cp = _read("ops/tools/run_session_authority_control_plane_v1.py")
    collector_cp = _read("constellation_2/phaseL/ui/server/c2_ops_cockpit_status_v2_collector_control_plane_v1.py")
    kernel_cp = _read("constellation_2/phaseL/ui_api/kernel_operator_shell_control_plane_v1.py")
    assert "diagnostic_v1" not in session_cp
    assert "diagnostic_v1" not in collector_cp
    assert "diagnostic_v1" not in kernel_cp


def test_main_files_use_extracted_diagnostic_helpers() -> None:
    session_source = _read("ops/tools/run_session_authority_v1.py")
    collector_source = _read("constellation_2/phaseL/ui/server/c2_ops_cockpit_status_v2_collector_v1.py")
    kernel_source = _read("constellation_2/phaseL/ui_api/kernel_operator_shell_v1.py")
    assert "collect_target_day_build_source_refs_v1(" in session_source
    assert "finalize_target_day_build_v1(" in session_source
    assert "run_session_authority_phase_flow_v1(" in session_source
    assert "finalize_session_authority_cli_result_v1(" in session_source
    assert "discover_attempts_v1(" in collector_source
    assert "select_preferred_attempt_v1(" in collector_source
    assert "load_portfolio_positions_bundle_v1(" in collector_source
    assert "load_fixed_gate_tiles_v1(" in collector_source
    assert "load_broker_connection_observer_tile_v1(" in collector_source
    assert "load_flow_diagnostics_bundle_v1(" in collector_source
    assert "load_run_scope_diagnostics_bundle_v1(" in collector_source
    assert "load_platform_readiness_policy_view_v1(" in collector_source
    assert "load_platform_readiness_history_v1(" in collector_source
    assert "load_runtime_control_workspace_bundle_v1(" in kernel_source
    assert "discover_runtime_scope_v1(" in kernel_source
    assert "load_state_workspace_snapshot_bundle_v1(" in kernel_source
    assert "load_advisory_workspace_bundle_v1(" in kernel_source
    assert "load_submission_workspace_bundle_v1(" in kernel_source
    assert "load_lifecycle_workspace_bundle_v1(" in kernel_source


def test_session_legacy_row_collectors_removed_from_main_file() -> None:
    session_source = _read("ops/tools/run_session_authority_v1.py")
    session_orch = _read("ops/tools/run_session_authority_orchestration_v1.py")
    for legacy_def in (
        "def _collect_market_calendar_row(",
        "def _collect_day_authority_row(",
        "def _collect_pre_open_bundle_rows(",
        "def _collect_handshake_rows(",
        "def _collect_kill_switch_row(",
        "def _collect_primary_scoped_authority_rows(",
        "def _collect_previous_day_economic_rows_from_readiness(",
    ):
        assert legacy_def not in session_source
    assert "read_pre_open_bundle_ref_v1(" not in session_source
    assert "def _write_promoted_active_session(" not in session_source
    assert "def _refresh_active_session_for_final_convergence(" not in session_source
    assert "def _tool_cmd(" not in session_source
    assert "def _parse_tool_stdout_json(" not in session_source
    assert "def _session_readiness_refresh_startup_ready(" not in session_source
    assert "def _annotate_source_ref_for_closure(" not in session_source
    assert "def _source_ref_blocks_build(" not in session_source
    assert "def _run_tool(" not in session_source
    assert "def _compute_hidden_dependency_check_result(" not in session_source
    assert "write_promoted_active_session_v1(" in session_orch
    assert "refresh_active_session_for_final_convergence_v1(" in session_orch
    assert "build_tool_command_v1(" in session_orch
    assert "run_tool_v1(" in session_orch
    assert "collect_target_day_build_source_refs_v1(" in session_orch
    assert "finalize_target_day_build_v1(" in session_orch


def test_collector_dead_oms_helpers_removed_from_main_file() -> None:
    collector_source = _read("constellation_2/phaseL/ui/server/c2_ops_cockpit_status_v2_collector_v1.py")
    collector_diag = _read("constellation_2/phaseL/ui/server/c2_ops_cockpit_status_v2_collector_diagnostic_v1.py")
    assert "def _load_oms_terminal_dispositions(" not in collector_source
    assert "def _summarize_oms_terminal_dispositions(" not in collector_source
    assert "def _load_activity_flow_diagnostics(" not in collector_source
    assert "def _load_selected_run_verdict_doc(" not in collector_source
    assert "def _load_scope_health_summary(" not in collector_source
    assert "def _candidate_activity_rollup_path(" not in collector_source
    assert "def _load_activity_rollup(" not in collector_source
    assert "def _extract_flow_from_activity_rollup(" not in collector_source
    assert "def discover_attempts(" not in collector_source
    assert "def select_latest_attempt(" not in collector_source
    assert "def _load_attempt_verdict(" not in collector_source
    assert "def select_preferred_attempt(" not in collector_source
    assert "def _load_nav(" not in collector_source
    assert "def _extract_portfolio_metrics(" not in collector_source
    assert "def _load_positions_snapshot(" not in collector_source
    assert "def _load_exposure_net(" not in collector_source
    assert "def _extract_positions_exposure(" not in collector_source
    assert "def _load_day_start_blocked(" not in collector_source
    assert "def _load_trading_day_state(" not in collector_source
    assert "def _load_sleeve_live_readiness(" not in collector_source
    assert "def _load_platform_bug_metrics(" not in collector_source
    assert "def _load_signal_activity(" not in collector_source
    assert "def _load_platform_readiness_policy_view(" not in collector_source
    assert "def _load_platform_readiness(" not in collector_source
    assert "def _load_platform_readiness_history(" not in collector_source
    assert "def discover_attempts_v1(" in collector_diag
    assert "def load_attempt_verdict_v1(" in collector_diag
    assert "def select_preferred_attempt_v1(" in collector_diag
    assert "def load_portfolio_positions_bundle_v1(" in collector_diag
    assert "def _load_day_start_blocked_v1(" in collector_diag
    assert "def _load_trading_day_state_v1(" in collector_diag
    assert "def load_sleeve_live_readiness_v1(" in collector_diag
    assert "def load_platform_bug_metrics_v1(" in collector_diag
    assert "def load_signal_activity_v1(" in collector_diag


def test_kernel_workspace_scanner_helpers_removed_from_main_file() -> None:
    kernel_source = _read("constellation_2/phaseL/ui_api/kernel_operator_shell_v1.py")
    for legacy_def in (
        "def _latest_submission_record(",
        "def _latest_execution_state_record(",
        "def _latest_execution_submission_decision_for_intent(",
        "def _latest_execution_run_envelope_for_submission(",
        "def _latest_lifecycle_decision_for_submission(",
        "def _latest_lifecycle_envelope_for_submission(",
    ):
        assert legacy_def not in kernel_source
