from __future__ import annotations

from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

from constellation_2.phaseL.ui.server import run_ops_dashboard_v1 as server
from ops.aegis.operator_state.canonical_operator_state_builder_v1 import load_or_build_operator_state_snapshot_response_v1
from ops.aegis.operator_state.current_operator_truth_resolver_v1 import resolve_current_operator_truth_v1


def test_operator_state_routes_are_registered_in_server_source() -> None:
    source = Path(server.__file__).read_text(encoding="utf-8")
    assert '"/api/aegis/operator/current-truth"' in source
    assert '"/api/aegis/operator/current-truth/latest"' in source
    assert '"/api/aegis/operator/state-snapshot/latest"' in source
    assert '"/api/aegis/operator/manual-capture/latest"' in source
    assert '"/api/aegis/operator/suppressed-watchlist/latest"' in source
    assert "load_or_build_operator_state_snapshot_response_v1" in source
    assert "resolve_current_operator_truth_v1" in source




def test_retry_market_data_refresh_enqueues_idempotent_background_job(tmp_path: Path) -> None:
    root = tmp_path / "truth"
    market_path = root / "reports" / "aegis_market_data_v1" / "2026-05-21" / "market_data.v1.json"
    market_path.parent.mkdir(parents=True, exist_ok=True)
    market_path.write_text('{"status":"FAILED","failure_reason":"MARKET_DATA_FETCH_FAILED","generated_at_utc":"2026-05-21T18:55:00Z","missing_symbols":["SPY"]}', encoding="utf-8")

    first = server._enqueue_data_remediation_job_v1(truth_root=root, day_utc="2026-05-21", playbook_id="refresh_required_symbol_data", request_payload={"symbols": ["SPY"]})
    second = server._enqueue_data_remediation_job_v1(truth_root=root, day_utc="2026-05-21", playbook_id="refresh_required_symbol_data", request_payload={"symbols": ["SPY"]})

    assert first["status"] == "QUEUED"
    assert first["validation_status"] == "PENDING_VENDOR_DATA"
    assert second["job_id"] == first["job_id"]
    assert second["job_already_existed"] is True
    assert Path(second["job_path"]).exists()
    assert first["broker_execution_allowed"] is False
    assert first["autonomous_execution_allowed"] is False

def test_missing_runtime_artifacts_return_degraded_payload_not_exception(tmp_path: Path) -> None:
    response = load_or_build_operator_state_snapshot_response_v1(truth_root=tmp_path / "truth", day_utc="2026-05-20")
    assert response["ok"] is True
    assert response["degraded"] is True
    assert response["data"]["schema_id"] == "operator_state_snapshot"
    assert response["errors"]
    assert response["next_action"]


def test_frontend_renders_packet32_panels_and_uses_stable_route() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)
    domain_client = (root / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js").read_text(encoding="utf-8")
    contract = (root / "constellation_2/phaseL/ui/docs/ui_sturdiness_contract_v1.md").read_text(encoding="utf-8")

    assert "renderManualCaptureProjectionPanel" in pages
    assert "renderSuppressedWatchlistProjectionPanel" in pages
    assert "renderOperatorReadinessProjectionPanel" in pages
    assert "IB capture tickets" in pages
    assert "Latest historical capture" in pages
    assert "Current intraday candidates" in pages
    assert "Market data state" in pages
    assert "Candidate certification" in pages
    assert "Execution eligibility" in pages
    assert "Final EOD certification pending." in pages
    assert "Today’s intraday sleeve run is current. Final EOD certification is pending." in pages
    assert "payload.data.schema_id === \"operator_state_snapshot\"" in pages
    assert "Final EOD certified candidates" in pages
    assert "Historical candidates" in pages
    assert "manual_capture_ticket_count" in pages
    assert "renderAegisCandidatesWorkflow" in pages
    assert "CANDIDATE_BLOTTER" in pages
    assert "current_day_candidate_rows" in pages
    assert "manual_capture_candidate_v1" in pages
    assert "suppressed_candidate_watchlist_v1" in pages
    assert "fetchAegisOperatorStateSnapshotLatest" in pages
    assert 'query("/api/aegis/operator/manual-capture/latest"' in domain_client
    assert 'query("/api/aegis/operator/suppressed-watchlist/latest"' in domain_client
    assert "Degraded data is a valid UI state" in contract


def test_frontend_renders_candidate_pipeline_observability_panels() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)
    dashboard_block = pages[pages.index("function renderAegisTodayWorkflow"):pages.index("function renderDashboardSystemStatus")]

    for function_name in [
        "renderDashboardCandidateFunnel",
        "renderDashboardSleeveHealth",
        "renderDashboardRegimeActivity",
        "renderDashboardCaptureReadyTrend",
    ]:
        assert function_name in pages
        assert function_name in dashboard_block
    for label in [
        "Candidate funnel",
        "Sleeve health",
        "Regime activity level",
        "IB ticket trend",
        "Generated",
        "Qualified",
        "Suppressed",
        "Blocked",
        "Selected",
        "Certified",
        "IB ticket candidates",
        "No candidate pipeline alerts.",
    ]:
        assert label in pages



def test_operator_platform_primary_navigation_is_workflow_first() -> None:
    root = Path(__file__).resolve().parents[4]
    navigation = (root / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js").read_text(encoding="utf-8")
    pages = pages_source_v1(root)

    for label in ["Dashboard", "Candidates", "Hypotheses", "Captured Trades", "System Health"]:
        assert f'label: "{label}"' in navigation
    assert 'id: "evidence"' not in navigation.split("export const LEGACY_NAVIGATION_REFERENCE", 1)[0]
    assert 'route: "/aegis-candidates"' in navigation
    assert 'id: "aegis_candidates"' in pages
    assert 'return renderAegisWorkflowPage("candidates")' in pages
    assert 'title: "Dashboard"' in pages
    assert 'title: "Captured Trades"' in pages

def test_manual_capture_panel_is_compact_and_blocked_label_is_explicit() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)
    manual_block = pages.split("function renderManualCaptureProjectionPanel", 1)[1].split("function renderSuppressedWatchlistProjectionPanel", 1)[0]
    watchlist_block = pages.split("function renderSuppressedWatchlistProjectionPanel", 1)[1].split("function renderOperatorReadinessProjectionPanel", 1)[0]

    assert "Review only - blocked" in manual_block
    assert "Manual paper capture candidate unavailable" not in manual_block
    assert "metric-grid" in manual_block
    assert "Selected exposure" in manual_block
    assert "source_day" in manual_block
    assert "stale_source" in manual_block
    assert "renderDefinitionRows([" not in manual_block
    assert "rows.slice(0, 10)" in watchlist_block
    assert "Show all" in watchlist_block


def test_frontend_surfaces_current_day_failure_as_dedicated_incident_page() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)
    shell = (root / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    css = (root / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")
    shell = (root / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")

    assert "function renderDashboardIncidentPage" in pages
    assert "function dashboardIncidentState" in pages
    assert '"CURRENT_DAY_FAILED", "MARKET_DATA_FETCH_FAILED", "INTRADAY_OPERATIONAL_FAILED", "FINAL_EOD_BLOCKED"' in pages
    assert "Today’s run failed" in pages
    assert "Market data refresh failed for" in pages
    assert "Current-day candidates are unavailable." in pages
    assert "Do not use stale prior-day prices." in pages
    assert "Retry market data refresh." in pages
    assert "is available as read-only historical fallback." in pages
    assert "data-dashboard-incident-details" in pages
    assert "Raw blocker code" in pages
    assert "dashboardIncidentMode: true" in pages
    incident_branch = pages[pages.index("if (incidentState)"):pages.index("const runtimeDay", pages.index("if (incidentState)"))]
    assert "renderDashboardSystemStatus" not in incident_branch
    assert "renderDashboardTodaySummary" not in incident_branch
    assert 'document.body.classList.toggle("dashboard-incident-mode"' in shell
    assert "contextHost.hidden = view.dashboardIncidentMode === true" in shell
    assert ".dashboard-incident-mode .shell-context" in css
    assert "display: none" in css[css.index(".dashboard-incident-mode .shell-context"):css.index(".dashboard-incident-page")]
    assert "retry-market-data-refresh" in pages




def test_vendor_lag_states_render_degraded_read_only_not_incident() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)
    css = (root / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")
    shell = (root / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")

    assert "DASHBOARD_DEGRADED_READ_ONLY_STATES" in pages
    assert "PENDING_VENDOR_DATA" in pages
    assert "READ_ONLY_PRIOR_DAY_FALLBACK" in pages
    assert "PARTIAL_DATA_AVAILABLE" in pages
    assert "function renderDashboardDegradedReadOnlyBanner" in pages
    assert "Market data pending" in pages
    assert "Using validated prior-day data in read-only mode until vendor certification completes." in pages
    assert "Operational Mode: DEGRADED_READ_ONLY" in pages
    assert "dashboardDegradedReadOnlyState(payload)) return """ in pages
    assert "Current-day candidate generation unlocks after validated EOD data." in pages
    assert "Generate current-day candidates" in pages
    assert "Queue refresh retry" in pages
    assert "Background refresh queued" in shell
    assert "NON_CERTIFIED" in pages
    assert "Lane" in pages
    assert "Read-only" in pages
    degraded_css = css[css.index(".current-day-status-banner.degraded-read-only"):css.index(".current-day-status-banner.warning")]
    assert "#111827" in degraded_css
    assert "#f8fafc" in degraded_css
    assert "#0ea5e9" in degraded_css
    assert "#fef2f2" not in degraded_css
    assert "#dc2626" not in degraded_css


def test_degraded_read_only_preserves_dashboard_navigation_cards() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)
    workflow_block = pages[pages.index("function renderAegisTodayWorkflow"):pages.index("function renderDashboardSystemStatus")]

    assert "if (incidentState)" in workflow_block
    assert "dashboardDegradedReadOnlyState" not in workflow_block.split("if (incidentState)", 1)[1].split("const runtimeDay", 1)[0]
    assert "renderDashboardSystemStatus(payload" in workflow_block
    assert "renderDashboardTodaySummary(payload)" in workflow_block
    assert "renderDashboardAttentionRequired(payload)" in workflow_block
    assert "renderDashboardRecentEvents(payload)" in workflow_block

def test_frontend_healthy_dashboard_path_still_renders_normal_cards() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)
    workflow_block = pages[pages.index("function renderAegisTodayWorkflow"):pages.index("function renderDashboardSystemStatus")]

    assert "if (incidentState)" in workflow_block
    assert "renderDashboardIncidentPage(payload)" in workflow_block
    assert "renderCurrentDayStatusBanner(payload)" in workflow_block
    assert "renderDashboardSystemStatus(payload" in workflow_block
    assert "renderDashboardTodaySummary(payload)" in workflow_block
    assert "renderDashboardAttentionRequired(payload)" in workflow_block
    assert "renderDashboardRecentEvents(payload)" in workflow_block


def test_server_latest_operator_truth_uses_current_day_not_latest_historical_report() -> None:
    source = Path(server.__file__).read_text(encoding="utf-8")

    assert "def _operator_truth_day" in source
    assert "date.today().isoformat()" in source
    assert "day_utc=_operator_truth_day(requested_day)" in source


def test_current_operator_truth_marks_today_failed_without_silent_fallback(tmp_path: Path) -> None:
    day = "2026-05-21"
    prior = "2026-05-20"
    market_path = tmp_path / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json"
    inputs_path = tmp_path / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json"
    readiness_path = tmp_path / "reports" / "market_data_readiness_v1" / day / "market_data_readiness.v1.json"
    historical_gate = tmp_path / "reports" / "portfolio_gate_candidate_report_v1" / prior / "portfolio_gate_candidate_report.v1.json"
    scheduler_path = tmp_path / "reports" / "aegis_day_run_v1" / day / "run.json"
    for path in [market_path, inputs_path, readiness_path, historical_gate, scheduler_path]:
        path.parent.mkdir(parents=True, exist_ok=True)
    market_path.write_text('{"status":"STALE","generated_at_utc":"2026-05-21T19:10:59Z"}', encoding="utf-8")
    inputs_path.write_text('{"validation_status":"BLOCKED","stale_symbols":["DBC","SPY","VIX"],"stale_input_ids":["market.price.DBC","market.price.SPY","market.volatility.VIX"]}', encoding="utf-8")
    readiness_path.write_text('{"blocked_input_ids":["market.price.DBC","market.price.SPY","market.volatility.VIX"]}', encoding="utf-8")
    historical_gate.write_text('{"day_utc":"2026-05-20","candidate_count":1,"selected_candidate_id":"prior-dow","candidate_rows":[{"candidate_id":"prior-dow","symbol":"DOW","sleeve_id":"C2_MEAN_REVERSION_EQ_V1"}]}', encoding="utf-8")
    scheduler_path.write_text('{"stderr":"FAIL: REFUSE_OVERWRITE_EXISTING_ARTIFACT:/tmp/x"}', encoding="utf-8")

    payload = resolve_current_operator_truth_v1(truth_root=tmp_path, day_utc=day, generated_at_utc="2026-05-21T20:00:00Z")

    assert payload["current_truth_status"] == "PENDING_VENDOR_DATA"
    assert payload["market_data_state"] == "MARKET_DATA_PENDING"
    assert payload["candidate_certification_state"] == "CANDIDATES_PROVISIONAL"
    assert payload["execution_eligibility_state"] == "EXECUTION_LOCKED_NON_CERTIFIED"
    assert payload["source_day"] == day
    assert payload["displayed_artifact_day"] == prior
    assert payload["current_day_status"]["failed_step"] == "market_data_vendor_pending"
    assert payload["current_day_status"]["critical_missing_symbols"] == ["DBC", "SPY", "VIX"]
    assert payload["current_day_status"]["scheduler_failure"]["blocker"] == "REFUSE_OVERWRITE_EXISTING_ARTIFACT"
    assert payload["historical_fallback"]["source_day"] == prior

    response = load_or_build_operator_state_snapshot_response_v1(truth_root=tmp_path, day_utc=day)
    today = response["data"]["operator_today_projection"]
    assert today["operational_mode"] == "DEGRADED_READ_ONLY"
    assert today["market_data_state"] == "MARKET_DATA_PENDING"
    assert today["candidate_certification_state"] == "CANDIDATES_PROVISIONAL"
    assert today["execution_eligibility_state"] == "EXECUTION_LOCKED_NON_CERTIFIED"
    assert today["readiness_status"] == "DEGRADED_READ_ONLY"
    assert today["blocked_sleeves"] == 0


def test_frontend_current_day_banner_uses_operator_semantics() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)

    assert "critical_missing_symbols" in pages
    assert "Current-day completed captures" in pages
    assert "Historical captures" in pages
    assert "Historical fallback" in pages
    assert "Scheduler:" in pages
    assert 'retry_action_available === true' in pages
    assert 'name="day_utc"' in pages


def test_frontend_distinguishes_market_finalization_and_provider_failures() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)

    assert "MARKET_NOT_FINALIZED_YET" in pages
    assert "PROVIDER_TIMEOUT" in pages
    assert "PROVIDER_SOURCE_UNAVAILABLE" in pages
    assert "CURRENT_DAY_DATA_STALE" in pages
    assert "PARTIAL_PROVIDER_SUCCESS" in pages
    assert "FINAL_EOD_READY" in pages
    assert "Market data is not finalized yet. Aegis will retry at 16:30, 17:00, and 18:00." in pages
    assert "Provider timeout. Some symbols were not fetched." in pages
    assert "Partial market data available" in pages



def test_current_operator_truth_intraday_ready_is_not_current_day_failed(tmp_path: Path) -> None:
    day = "2026-05-21"
    market_path = tmp_path / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json"
    inputs_path = tmp_path / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json"
    readiness_path = tmp_path / "reports" / "market_data_readiness_v1" / day / "market_data_readiness.v1.json"
    for path in [market_path, inputs_path, readiness_path]:
        path.parent.mkdir(parents=True, exist_ok=True)
    market_path.write_text('{"status":"CURRENT","operator_market_data_state":"INTRADAY_OPERATIONAL_READY","market_data_mode":"INTRADAY_OPERATIONAL","intraday_operational_ready":true,"final_eod_certification_status":"PENDING","final_eod_certification_pending":true,"generated_at_utc":"2026-05-21T14:50:00Z"}', encoding="utf-8")
    inputs_path.write_text('{"validation_status":"VALID","status":"READY","market_data_mode":"INTRADAY_OPERATIONAL","final_eod_certification_status":"PENDING","input_records":[]}', encoding="utf-8")
    readiness_path.write_text('{"status":"READY","blocked_input_ids":[]}', encoding="utf-8")

    payload = resolve_current_operator_truth_v1(truth_root=tmp_path, day_utc=day, generated_at_utc="2026-05-21T15:00:00Z")

    assert payload["current_truth_status"] == "INTRADAY_OPERATIONAL_READY"
    assert payload["current_day_status"]["intraday_operational_ready"] is True
    assert payload["current_day_status"]["final_eod_certification_status"] == "PENDING"
    assert payload["current_day_status"]["market_data_mode"] == "INTRADAY_OPERATIONAL"


def test_operator_snapshot_intraday_runtime_day_not_historical_fallback(tmp_path: Path) -> None:
    day = "2026-05-21"
    prior = "2026-05-20"
    market_path = tmp_path / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json"
    inputs_path = tmp_path / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json"
    readiness_path = tmp_path / "reports" / "market_data_readiness_v1" / day / "market_data_readiness.v1.json"
    manifest_path = tmp_path / "reports" / "candidate_generation_manifest_v1" / day / f"sleeve_evaluation_kernel_v1:{day}" / "candidate_generation_manifest.v1.json"
    historical_gate = tmp_path / "reports" / "portfolio_gate_candidate_report_v1" / prior / "portfolio_gate_candidate_report.v1.json"
    for artifact_path in [market_path, inputs_path, readiness_path, manifest_path, historical_gate]:
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
    market_path.write_text('{"status":"CURRENT","operator_market_data_state":"INTRADAY_OPERATIONAL_READY","market_data_mode":"INTRADAY_OPERATIONAL","intraday_operational_ready":true,"final_eod_certification_status":"PENDING","final_eod_certification_pending":true,"generated_at_utc":"2026-05-21T14:50:00Z"}', encoding="utf-8")
    inputs_path.write_text('{"validation_status":"VALID","status":"READY","market_data_mode":"INTRADAY_OPERATIONAL","final_eod_certification_status":"PENDING","final_eod_certification_pending":true,"input_records":[]}', encoding="utf-8")
    readiness_path.write_text('{"status":"READY","blocked_input_ids":[]}', encoding="utf-8")
    manifest_path.write_text('{"summary":{"candidate_count":3,"status_counts":{"CANDIDATE_CREATED":1,"BLOCKED":1,"NO_SIGNAL":1}},"candidate_rows":[{"status":"CANDIDATE_CREATED","candidate_id":"candidate-spy","symbol_or_pair":"SPY","candidate_data_status":"PROVISIONAL_CANDIDATE","source_data_mode":"INTRADAY_OPERATIONAL","final_eod_certification_status":"PENDING","final_eod_certification_pending":true,"input_market_data_snapshot_ids":["market-data-snapshot:2026-05-21:CERTIFICATION_PENDING:test"]},{"status":"BLOCKED"},{"status":"NO_SIGNAL"}]}', encoding="utf-8")
    historical_gate.write_text('{"day_utc":"2026-05-20","candidate_count":1,"selected_candidate_id":"prior-dow","candidate_rows":[{"candidate_id":"prior-dow","symbol":"DOW","sleeve_id":"C2_MEAN_REVERSION_EQ_V1"}]}', encoding="utf-8")

    response = load_or_build_operator_state_snapshot_response_v1(truth_root=tmp_path, day_utc=day)
    payload = response["data"]
    today = payload["operator_today_projection"]

    assert payload["current_truth_status"] == "INTRADAY_OPERATIONAL_READY"
    assert payload["runtime_mode"] == "OPEN_INTRADAY"
    assert payload["market_data_state"] == "MARKET_DATA_VALIDATED"
    assert payload["candidate_certification_state"] == "CANDIDATES_CERTIFICATION_PENDING"
    assert payload["execution_eligibility_state"] == "EXECUTION_LOCKED_NON_CERTIFIED"
    assert payload["runtime_mode"] != "CERTIFIED_READY"
    assert payload["displayed_artifact_day"] == day
    assert today["current_runtime_day"] == day
    assert today["displayed_artifact_day"] == day
    assert today["runtime_mode"] == "OPEN_INTRADAY"
    assert today["market_data_state"] == "MARKET_DATA_VALIDATED"
    assert today["candidate_certification_state"] == "CANDIDATES_CERTIFICATION_PENDING"
    assert today["execution_eligibility_state"] == "EXECUTION_LOCKED_NON_CERTIFIED"
    assert today["current_intraday_candidate_count"] == 1
    assert today["current_day_generated_candidate_count"] == 3
    assert today["final_eod_certified_candidate_count"] == 0
    assert today["final_eod_certification_status"] == "PENDING"
    assert today["historical_latest_capture_day"] == prior
    candidate = payload["current_day_status"]["candidate_rows"][0]
    assert candidate["candidate_lane"] == "PROVISIONAL"
    assert candidate["certification_state"] == "CERTIFICATION_PENDING"
    assert candidate["certification_label"] == "NON_CERTIFIED"
    assert candidate["analytical_state"] == "QUALIFIED"
    assert candidate["operator_task_state"] == "NO_USER_ACTION"
    assert candidate["execution_state"] == "EXECUTION_LOCKED_NON_CERTIFIED"
    assert "Actionable" not in candidate["operator_state"]
    assert candidate["execution_eligible"] is False
    assert candidate["read_only"] is True
    assert candidate["manual_capture_eligible"] is False
    assert candidate["input_market_data_snapshot_ids"] == ["market-data-snapshot:2026-05-21:CERTIFICATION_PENDING:test"]
    assert payload["current_day_status"]["operator_task_state_counts"]["NO_USER_ACTION"] >= 1
    assert today["operator_task_state_counts"]["NO_USER_ACTION"] >= 1
    assert today["execution_locked_non_certified_count"] >= 1


def test_dashboard_operator_semantics_are_consolidated_in_system_status() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)

    workflow_block = pages[pages.index("function renderAegisTodayWorkflow"):pages.index("function renderDashboardSystemStatus")]
    system_status_block = pages[pages.index("function renderDashboardSystemStatus"):pages.index("function renderDashboardTodaySummary")]

    assert 'contextHtml: ""' in workflow_block
    assert "hideContextRail: true" in workflow_block
    assert "workflowContextHtml(payload)" not in workflow_block
    assert "Dashboard System Status is the single authoritative operational status area" in system_status_block
    assert "Market data state" in system_status_block
    assert "Candidate certification state" in system_status_block
    assert "Execution eligibility" in system_status_block
    assert "Fallback / read-only state" in system_status_block
    assert "Validation explanation" in system_status_block
    assert "Retry state" in system_status_block
    assert "Next scheduled action" in system_status_block
    assert "resolvedMarketDataState" in system_status_block
    assert "resolvedCandidateCertificationState" in system_status_block
    assert "resolvedExecutionEligibilityState" in system_status_block
    assert "Execution-facing workflows remain locked until candidate certification completes." in system_status_block
    assert "renderDashboardEvidenceDrawer(payload)" in system_status_block
    assert "Current Truth" not in workflow_block
    assert "Current Truth" not in system_status_block


def test_operator_cockpit_keeps_snapshot_as_dashboard_authority() -> None:
    source = Path(server.__file__).read_text(encoding="utf-8")

    assert "Dashboard day/mode/count fields are governed by operator_state_snapshot_v1" in source
    assert '"operator_today_projection"' in source
    assert 'payload["runtime_mode"] = str(operator_snapshot.get("runtime_mode")' in source
    assert 'payload["displayed_artifact_day"] = str(operator_snapshot.get("displayed_artifact_day")' in source
    assert 'payload["current_day_status"] = operator_snapshot.get("current_day_status")' in source



def test_candidates_ui_surfaces_selected_promotion_lifecycle_and_blocker() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)

    candidates_block = pages[pages.index("function renderAegisCandidatesWorkflow"):pages.index("function renderAegisReviewWorkflow")]
    assert "Intent state" in candidates_block
    assert "Confidence" in candidates_block
    assert "Stability" in candidates_block
    assert "Convergence" in candidates_block
    assert "Capture guidance" in candidates_block
    assert "Guidance reason" in candidates_block
    assert "Your tasks" in candidates_block
    assert "Actionable candidate" not in candidates_block
    assert "intent_state" in pages
    assert "capture_guidance_reason" in pages
    assert "Blocked selected" in pages
    assert "No manual IB capture recommendation" in pages
    assert "IB recommendations" in pages


def test_selected_candidate_not_capture_ready_has_exact_promotion_blocker(tmp_path: Path) -> None:
    day = "2026-05-21"
    market_path = tmp_path / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json"
    inputs_path = tmp_path / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json"
    readiness_path = tmp_path / "reports" / "market_data_readiness_v1" / day / "market_data_readiness.v1.json"
    manifest_path = tmp_path / "reports" / "candidate_generation_manifest_v1" / day / f"sleeve_evaluation_kernel_v1:{day}" / "candidate_generation_manifest.v1.json"
    scoring_path = tmp_path / "reports" / "portfolio_scoring_v1" / day / "portfolio_scoring.v1.json"
    arbitration_path = tmp_path / "reports" / "intent_arbitration_v1" / day / "intent_arbitration.v1.json"
    promotion_map_path = tmp_path / "reports" / "candidate_promotion_map_v1" / day / "candidate_promotion_map.v1.json"
    pointer_path = tmp_path / "pointers" / "selected_intent_pointer.v1.json"
    for artifact_path in [market_path, inputs_path, readiness_path, manifest_path, scoring_path, arbitration_path, promotion_map_path, pointer_path]:
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
    market_path.write_text('{"status":"CURRENT","operator_market_data_state":"INTRADAY_OPERATIONAL_READY","market_data_mode":"INTRADAY_OPERATIONAL","intraday_operational_ready":true,"final_eod_certification_status":"PENDING","final_eod_certification_pending":true}', encoding="utf-8")
    inputs_path.write_text('{"validation_status":"VALID","status":"READY","market_data_mode":"INTRADAY_OPERATIONAL","final_eod_certification_status":"PENDING","final_eod_certification_pending":true}', encoding="utf-8")
    readiness_path.write_text('{"status":"READY","blocked_input_ids":[]}', encoding="utf-8")
    manifest_path.write_text('{"summary":{"candidate_count":1,"status_counts":{"CANDIDATE_CREATED":1}},"candidate_rows":[{"candidate_id":"cand-aapl","raw_intent_id":"intent-aapl","symbol":"AAPL","sleeve_id":"C2_TREND_EQ_PRIMARY_V1","status":"CANDIDATE_CREATED","source_data_mode":"INTRADAY_OPERATIONAL","final_eod_certification_pending":true,"final_eod_certification_status":"PENDING"}]}', encoding="utf-8")
    scoring_path.write_text('{"rankings":[{"intent_id":"intent-aapl","score_total":39.3,"rank":1}]}', encoding="utf-8")
    arbitration_path.write_text('{"status":"SELECTED","selected_intent":{"intent_id":"intent-aapl","symbol":"AAPL","sleeve_id":"C2_TREND_EQ_PRIMARY_V1"}}', encoding="utf-8")
    promotion_map_path.write_text('{"selected_candidate_count":1,"capture_ready_count":0,"blocked_selected_count":1,"candidate_rows":[{"candidate_id":"cand-aapl","raw_intent_id":"intent-aapl","symbol":"AAPL","sleeve_id":"C2_TREND_EQ_PRIMARY_V1","selected_status":"SELECTED","manual_capture_status":"NOT_CAPTURE_READY","final_operator_status":"SELECTED_NOT_CAPTURE_READY","promotion_status":"PROMOTED_TO_OPERATOR_REVIEW","construction_status":"PENDING","conversion_status":"MISSING","submit_boundary_status":"NOT_STARTED","ticket_lineage_status":"NOT_STARTED","exact_blocker":"CONVERSION_MISSING","next_safe_action":"Build conversion and submit-boundary evidence before capture."}]}', encoding="utf-8")
    pointer_path.write_text('{"day_utc":"2026-05-21","selected_intent":{"intent_id":"intent-aapl","symbol":"AAPL","sleeve_id":"C2_TREND_EQ_PRIMARY_V1"},"source_arbitration_path":"' + str(arbitration_path) + '"}', encoding="utf-8")

    payload = resolve_current_operator_truth_v1(truth_root=tmp_path, day_utc=day, generated_at_utc="2026-05-21T15:00:00Z")
    row = payload["current_day_status"]["candidate_rows"][0]

    assert payload["current_day_status"]["selected_candidate_count"] == 1
    assert payload["current_day_status"]["capture_ready_ticket_count"] == 0
    assert payload["current_day_status"]["blocked_selected_candidate_count"] == 1
    assert row["selection_status"] == "SELECTED"
    assert row["capture_eligibility_status"] == "READ_ONLY_NON_CERTIFIED"
    assert row["promotion_lifecycle_stage"] == "SELECTED_READ_ONLY_NON_CERTIFIED"
    assert row["certification_label"] == "NON_CERTIFIED"
    assert row["analytical_state"] == "QUALIFIED"
    assert row["operator_task_state"] == "NO_USER_ACTION"
    assert row["execution_state"] == "EXECUTION_LOCKED_NON_CERTIFIED"
    assert "Actionable" not in row["operator_state"]
    assert row["execution_eligible"] is False
    assert row["read_only"] is True
    assert row["promotion_blocker"] == "NON_CERTIFIED_CANDIDATE_SNAPSHOT"
    assert row["capture_guidance"] == "AWAIT_CERTIFICATION"
    assert row["next_action"] == "Aegis needs another snapshot before stability can be trusted."


def test_certified_capture_ready_row_has_action_affordance_and_execution_eligibility(tmp_path: Path) -> None:
    day = "2026-05-21"
    market_path = tmp_path / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json"
    inputs_path = tmp_path / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json"
    readiness_path = tmp_path / "reports" / "market_data_readiness_v1" / day / "market_data_readiness.v1.json"
    manifest_path = tmp_path / "reports" / "candidate_generation_manifest_v1" / day / f"sleeve_evaluation_kernel_v1:{day}" / "candidate_generation_manifest.v1.json"
    arbitration_path = tmp_path / "reports" / "intent_arbitration_v1" / day / "intent_arbitration.v1.json"
    promotion_map_path = tmp_path / "reports" / "candidate_promotion_map_v1" / day / "candidate_promotion_map.v1.json"
    for artifact_path in [market_path, inputs_path, readiness_path, manifest_path, arbitration_path, promotion_map_path]:
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
    market_path.write_text('{"status":"CURRENT","operator_market_data_state":"FINAL_EOD_READY","market_data_mode":"FINAL_EOD_CERTIFIED","final_eod_certification_status":"VALID","final_eod_certification_pending":false}', encoding="utf-8")
    inputs_path.write_text('{"validation_status":"VALID","status":"READY","market_data_mode":"FINAL_EOD_CERTIFIED","final_eod_certification_status":"VALID","final_eod_certification_pending":false}', encoding="utf-8")
    readiness_path.write_text('{"status":"READY","blocked_input_ids":[]}', encoding="utf-8")
    manifest_path.write_text('{"summary":{"candidate_count":1,"status_counts":{"CAPTURE_READY":1}},"candidate_lane":"CERTIFIED","certification_state":"CERTIFIED","candidate_rows":[{"candidate_id":"cand-msft","raw_intent_id":"intent-msft","symbol":"MSFT","sleeve_id":"C2_TREND_EQ_PRIMARY_V1","status":"CAPTURE_READY","candidate_lane":"CERTIFIED","certification_state":"CERTIFIED","execution_eligible":true,"manual_capture_eligible":true,"source_data_mode":"FINAL_EOD_CERTIFIED","final_eod_certification_status":"VALID","final_eod_certification_pending":false}]}', encoding="utf-8")
    arbitration_path.write_text('{"status":"SELECTED","selected_intent":{"intent_id":"intent-msft","symbol":"MSFT","sleeve_id":"C2_TREND_EQ_PRIMARY_V1"}}', encoding="utf-8")
    promotion_map_path.write_text('{"selected_candidate_count":1,"capture_ready_count":1,"blocked_selected_count":0,"candidate_rows":[{"candidate_id":"cand-msft","raw_intent_id":"intent-msft","candidate_lane":"CERTIFIED","certification_state":"CERTIFIED","execution_eligible":true,"manual_capture_status":"CAPTURE_READY"}]}', encoding="utf-8")

    payload = resolve_current_operator_truth_v1(truth_root=tmp_path, day_utc=day, generated_at_utc="2026-05-21T22:00:00Z")
    row = payload["current_day_status"]["candidate_rows"][0]

    assert row["analytical_state"] == "QUALIFIED"
    assert row["operator_task_state"] == "MANUAL_IB_CAPTURE_READY"
    assert row["execution_state"] == "EXECUTION_ELIGIBLE"
    assert row["operator_state"] == "Manual IB capture ready"
    assert row["execution_eligible"] is True
    assert row["manual_capture_eligible"] is True
    assert payload["current_day_status"]["operator_action_available_count"] == 1
    assert payload["current_day_status"]["execution_eligible_row_count"] == 1


def test_blocked_and_suppressed_candidate_semantics_are_distinct(tmp_path: Path) -> None:
    day = "2026-05-21"
    market_path = tmp_path / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json"
    readiness_path = tmp_path / "reports" / "market_data_readiness_v1" / day / "market_data_readiness.v1.json"
    manifest_path = tmp_path / "reports" / "candidate_generation_manifest_v1" / day / f"sleeve_evaluation_kernel_v1:{day}" / "candidate_generation_manifest.v1.json"
    for artifact_path in [market_path, readiness_path, manifest_path]:
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
    market_path.write_text('{"status":"CURRENT","operator_market_data_state":"INTRADAY_OPERATIONAL_READY","market_data_mode":"INTRADAY_OPERATIONAL"}', encoding="utf-8")
    readiness_path.write_text('{"status":"READY","blocked_input_ids":[]}', encoding="utf-8")
    manifest_path.write_text('{"summary":{"candidate_count":2,"status_counts":{"BLOCKED":1,"SUPPRESSED":1}},"candidate_rows":[{"candidate_id":"blocked","status":"BLOCKED","reason_codes":["MISSING_RISK"]},{"candidate_id":"suppressed","status":"SUPPRESSED"}]}', encoding="utf-8")

    payload = resolve_current_operator_truth_v1(truth_root=tmp_path, day_utc=day, generated_at_utc="2026-05-21T15:00:00Z")
    rows = payload["current_day_status"]["candidate_rows"]
    by_id = {row["candidate_id"]: row for row in rows}

    assert by_id["blocked"]["analytical_state"] == "BLOCKED"
    assert by_id["blocked"]["operator_task_state"] == "MONITOR_ONLY"
    assert by_id["blocked"]["execution_state"] == "NON_EXECUTABLE"
    assert by_id["suppressed"]["analytical_state"] == "SUPPRESSED"
    assert by_id["suppressed"]["operator_task_state"] == "NO_USER_ACTION"
    assert by_id["suppressed"]["execution_state"] == "NON_EXECUTABLE"



def test_primary_candidate_workflow_uses_user_task_semantics() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)
    dashboard_block = pages[pages.index("function renderDashboardAttentionRequired"):pages.index("function renderDashboardRecentEvents")]
    candidates_block = pages[pages.index("function renderAegisCandidatesWorkflow"):pages.index("function renderAegisReviewWorkflow")]
    action_block = pages[pages.index("function renderCandidateActionButtons"):pages.index("function renderCandidateReviewDialog")]

    assert "Your tasks" in dashboard_block
    assert "IB capture tickets: 0" in dashboard_block
    assert "No action required" in dashboard_block
    assert "Your task" in candidates_block
    assert "MANUAL_IB_CAPTURE_RECOMMENDED" in candidates_block
    assert "SYSTEM_REPAIR_REQUIRED" in candidates_block
    assert "NO_USER_ACTION" in candidates_block
    assert "MONITOR_ONLY" in pages
    assert "Record manual IB capture" in action_block
    assert "MANUAL_IB_CAPTURE_RECOMMENDED" in action_block
    for forbidden in ["REVIEW_AVAILABLE", "Review blocker", "human review", "operator decision", "Review Candidate", "Record Review"]:
        assert forbidden not in dashboard_block
        assert forbidden not in candidates_block
        assert forbidden not in action_block



def test_system_repair_required_is_reserved_for_integrity_or_required_feed_failures(tmp_path: Path) -> None:
    day = "2026-05-21"
    market_path = tmp_path / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json"
    readiness_path = tmp_path / "reports" / "market_data_readiness_v1" / day / "market_data_readiness.v1.json"
    manifest_path = tmp_path / "reports" / "candidate_generation_manifest_v1" / day / f"sleeve_evaluation_kernel_v1:{day}" / "candidate_generation_manifest.v1.json"
    for artifact_path in [market_path, readiness_path, manifest_path]:
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
    market_path.write_text('{"status":"CURRENT","operator_market_data_state":"INTRADAY_OPERATIONAL_READY","market_data_mode":"INTRADAY_OPERATIONAL"}', encoding="utf-8")
    readiness_path.write_text('{"status":"READY","blocked_input_ids":[]}', encoding="utf-8")
    manifest_path.write_text('{"summary":{"candidate_count":1,"status_counts":{"BLOCKED":1}},"candidate_rows":[{"candidate_id":"repair","status":"BLOCKED","reason_codes":["MISSING_REQUIRED_FEED"]}]}', encoding="utf-8")

    payload = resolve_current_operator_truth_v1(truth_root=tmp_path, day_utc=day, generated_at_utc="2026-05-21T15:00:00Z")
    row = payload["current_day_status"]["candidate_rows"][0]

    assert row["analytical_state"] == "BLOCKED"
    assert row["operator_task_state"] == "SYSTEM_REPAIR_REQUIRED"
    assert payload["current_day_status"]["system_repair_required_count"] == 1



def test_manual_capture_record_preserves_candidate_snapshot_fields(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)
    manual_record = (root / "ops/aegis/operator_state/manual_capture_record_v1.py").read_text(encoding="utf-8")

    assert 'name="candidate_snapshot_id"' in pages
    assert 'name="candidate_certification_state"' in pages
    assert 'name="input_market_data_snapshot_ids"' in pages
    assert 'candidate_snapshot_id' in manual_record
    assert 'candidate_certification_state' in manual_record
    assert 'input_market_data_snapshot_ids' in manual_record
    assert 'manual_capture_notes' in manual_record


def test_operator_workflow_timestamps_are_live_projection_fields_not_static_shell_copy(tmp_path: Path) -> None:
    day = "2026-05-22"
    market_path = tmp_path / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json"
    inputs_path = tmp_path / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json"
    manifest_path = tmp_path / "reports" / "candidate_generation_manifest_v1" / day / f"sleeve_evaluation_kernel_v1:{day}" / "candidate_generation_manifest.v1.json"
    for artifact in [market_path, inputs_path, manifest_path]:
        artifact.parent.mkdir(parents=True, exist_ok=True)
    market_path.write_text(
        '{"status":"CURRENT","operator_market_data_state":"INTRADAY_OPERATIONAL_READY","generated_at_utc":"2026-05-22T20:05:00Z","final_eod_certification_status":"PENDING","final_eod_certification_pending":true,"finalization_window":{"market_close_at_utc":"2026-05-22T20:00:00Z","vendor_lag_buffer_minutes":30},"next_retry_utc":"2026-05-22T20:35:00Z","last_certification_attempt_at_utc":"2026-05-22T20:10:00Z"}',
        encoding="utf-8",
    )
    inputs_path.write_text('{"validation_status":"VALID","status":"READY","generated_at_utc":"2026-05-22T20:06:00Z","final_eod_certification_status":"PENDING","final_eod_certification_pending":true}', encoding="utf-8")
    manifest_path.write_text('{"schema_id":"candidate_generation_manifest","day_utc":"2026-05-22","produced_at_utc":"2026-05-22T20:12:00Z","summary":{"candidate_count":1,"status_counts":{"CANDIDATE_CREATED":1}},"candidate_rows":[{"candidate_id":"cand-spy","raw_intent_id":"intent-spy","symbol_or_pair":"SPY","engine_id":"C2_TREND_EQ_PRIMARY_V1","status":"CANDIDATE_CREATED","input_market_data_snapshot_ids":["md-2026-05-22"]}]}', encoding="utf-8")

    response = load_or_build_operator_state_snapshot_response_v1(truth_root=tmp_path, day_utc=day)
    current_day = response["data"]["current_day_status"]
    today = response["data"]["operator_today_projection"]

    assert current_day["market_data_last_updated_at"] == "2026-05-22T20:05:00Z"
    assert current_day["candidate_snapshot_generated_at"] == "2026-05-22T20:12:00Z"
    assert current_day["last_certification_attempt_at"] == "2026-05-22T20:10:00Z"
    assert current_day["eod_certification_timing"]["market_close_at"] == "2026-05-22T20:00:00Z"
    assert current_day["eod_certification_timing"]["vendor_lag_window"] == "30"
    assert current_day["eod_certification_timing"]["estimated_next_certification_attempt_at"] == "2026-05-22T20:35:00Z"
    assert today["market_data_last_updated_at"] == "2026-05-22T20:05:00Z"
    assert today["candidate_snapshot_generated_at"] == "2026-05-22T20:12:00Z"
    assert today["eod_certification_timing"]["final_certification_status"] == "PENDING"


def test_operator_shell_does_not_render_stale_data_as_of_timestamp() -> None:
    root = Path(__file__).resolve().parents[4]
    html = (root / "constellation_2/phaseL/ui/static/index.html").read_text(encoding="utf-8")
    main = (root / "constellation_2/phaseL/ui/static/operator_shell/main.js").read_text(encoding="utf-8")
    pages = pages_source_v1(root)
    workflow_block = pages[pages.index("async function renderAegisWorkflowPage"):pages.index("function renderMissingCanonicalTodayWorkflow")]

    assert "Apr 27, 2026" not in html
    assert "Apr 27, 2026" not in main
    assert "Data as of: Apr 27" not in html + main + pages
    assert "Operational timestamps loading" in html
    assert "renderHeaderOperationalTimestamps" in main
    assert "view.operationalTimestamps" in main
    assert "Data as of" not in workflow_block


def test_dashboard_shows_explicit_operational_and_eod_timing_sections() -> None:
    root = Path(__file__).resolve().parents[4]
    pages = pages_source_v1(root)
    dashboard_block = pages[pages.index("function renderAegisTodayWorkflow"):pages.index("function renderDashboardSystemStatus")]

    assert "renderDashboardOperationalTimestamps" in dashboard_block
    assert "renderDashboardEodPipelineTiming" in dashboard_block
    for label in [
        "Runtime Timeline",
        "Operational day",
        "Market data last updated",
        "Candidate snapshot timestamp",
        "Last certification attempt",
        "Final EOD certification completed at",
        "Certification Progress",
        "Market close",
        "Vendor lag window",
        "Certification pending",
        "Estimated next certification attempt",
        "Final certification status",
    ]:
        assert label in pages
