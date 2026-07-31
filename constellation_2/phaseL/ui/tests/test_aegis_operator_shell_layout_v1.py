from __future__ import annotations

from pathlib import Path

from constellation_2.phaseL.ui.server.run_ops_dashboard_v1 import OpsHandler


ROOT = Path(__file__).resolve().parents[4]
STATIC = ROOT / "constellation_2/phaseL/ui/static"
CSS = STATIC / "aegis.css"
HTML = STATIC / "index.html"
MAIN = STATIC / "operator_shell/main.js"
NAV = STATIC / "operator_shell/navigation_schema.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_desktop_sidebar_is_pinned_visible_and_scrollable() -> None:
    css = _text(CSS)

    assert "--operator-sidebar-width" in css
    assert ".shell-frame" in css
    assert "grid-template-columns: var(--operator-sidebar-width) minmax(0, 1fr)" in css
    assert ".shell-leftnav" in css
    assert "position: sticky" in css
    assert "max-height: calc(100dvh - 24px)" in css
    assert "overflow-y: auto" in css


def test_main_content_cannot_overlap_sidebar_or_right_rail() -> None:
    css = _text(CSS)

    assert "grid-template-columns: var(--operator-sidebar-width) minmax(0, 1fr) minmax(240px, var(--operator-right-rail-width))" in css
    assert ".shell-main" in css
    assert ".shell-context" in css
    assert "min-width: 0" in css
    assert "grid-column: 2" in css


def test_workflow_layout_removes_operational_right_rail() -> None:
    css = _text(CSS)
    main = _text(MAIN)
    routes = _text(STATIC / "operator_shell/pages/route_metadata.js")
    server = _text(SERVER)

    assert 'const OPERATIONAL_LAYOUT = "OPERATIONAL_LAYOUT"' in main
    assert 'const WORKFLOW_LAYOUT = "WORKFLOW_LAYOUT"' in main
    assert 'layoutMode === WORKFLOW_LAYOUT' in main
    assert 'contextHost.hidden = rightRailHidden' in main
    assert '.workflow-layout .shell-frame' in css
    assert 'grid-template-columns: var(--operator-sidebar-width) minmax(0, 1fr);' in css
    assert '.workflow-layout .shell-context' in css
    assert 'display: none;' in css[css.index('.workflow-layout .shell-context'):css.index('.workflow-layout .shell-main')]

    for route_id in [
        '"aegis_candidates"',
        '"aegis_candidate_funnel"',
        '"aegis_exit_review"',
        '"aegis_performance"',
        '"aegis_journal"',
        '"aegis_captured_trades"',
        '"research_lab"',
        '"research_start"',
    ]:
        assert route_id in main

    for path in [
        '"/aegis-exit-review"',
        '"/aegis-performance"',
        '"/aegis-captured-trades"',
        '"/aegis-candidate-funnel"',
        '"/research-lab"',
    ]:
        assert path in routes or path in server

    assert '"aegis_runtime_timeline"' in main
    assert '"aegis_repair_center"' in main


def test_collapsed_sidebar_remains_accessible_icon_rail() -> None:
    css = _text(CSS)
    main = _text(MAIN)

    assert "--operator-sidebar-rail-width" in css
    assert ".sidebar-collapsed .shell-leftnav" in css
    assert "width: var(--operator-sidebar-rail-width)" in css
    assert ".sidebar-collapsed .sidebar-collapse-button" in css
    assert 'state.sidebarMode === "expanded" ? "collapsed" : "expanded"' in main
    render_block = main.split("function renderNav", 1)[1].split("document.getElementById(\"workspaceNav\")", 1)[0]
    assert 'data-engineering-drawer' in render_block
    assert 'state.engineeringDrawerOpen ? (engineeringGroups.length ? engineeringGroups.map' in render_block


def test_mobile_nav_has_visible_open_close_and_scrim_controls() -> None:
    html = _text(HTML)
    css = _text(CSS)
    main = _text(MAIN)

    assert 'id="navOpenButton"' in html
    assert "data-sidebar-open" in html
    assert "data-sidebar-close" in html
    assert ".mobile-nav-button" in css
    assert "@media (max-width: 900px)" in css
    assert ".sidebar-expanded .shell-leftnav" in css
    assert "window.matchMedia(\"(max-width: 900px)\")" in main
    assert 'event.key === "Escape"' in main


def test_primary_navigation_is_ai_cio_workflow_only() -> None:
    nav = _text(NAV)
    primary_nav = nav.split("export const ENGINEERING_NAVIGATION_SCHEMA", 1)[0]

    for label in ['label: "CIO Briefing"', 'label: "Capital Map"', 'label: "Portfolios"', 'label: "Research Lab"', 'label: "Opportunities"', 'label: "Retirement Simulator"', 'label: "Advisor Oversight"', 'label: "Documents"', 'label: "Carolyn"']:
        assert label in primary_nav
    for hidden in ['label: "Engineering Dashboard"', 'label: "Dashboard"', 'label: "Candidates"', 'label: "Hypotheses"', 'label: "Captured Trades"', 'label: "Repair Center"', 'label: "Command Center"']:
        assert hidden not in primary_nav
    assert primary_nav.count("children: []") == 9


def test_history_deep_links_use_central_route_metadata_navigation() -> None:
    nav = _text(NAV)
    routes = _text(STATIC / "operator_shell/pages/route_metadata.js")

    assert "OPERATOR_ROUTE_WORKSPACE_ALIASES" not in nav
    assert 'import { activeNavSelectionForPath } from "./pages/route_metadata.js"' in nav
    for route in ["/aegis-history", "/aegis-journal", "/aegis-captured-trades", "/aegis-review", "/aegis-performance"]:
        assert route in routes
    active_block = nav.split("export function activeNavigationForPath", 1)[1].split("export const LEGACY_CORE_NAVIGATION_REFERENCE", 1)[0]
    assert "activeNavSelectionForPath(pathname)" in active_block


def test_closed_trades_operator_wording_replaces_captured_trades() -> None:
    pages = _text(STATIC / "operator_shell/pages/index.js")
    history_block = pages.split("async function renderHistoryWorkspace", 1)[1].split("function renderResearchWorkspace", 1)[0]
    journal_block = pages.split("function renderAegisHistoryWorkflow", 1)[1].split("function renderCapturedTradeTable", 1)[0]

    assert 'title: "Closed Trades"' in history_block
    assert "Closed Governed Trades" in history_block
    assert "Legacy / Partial Historical Trades" in history_block
    assert "This page contains closed governed paper trades and legacy historical records. Open positions appear under Positions." in history_block
    assert "Captured Trades" not in history_block
    assert "Legacy Captures" not in history_block
    assert "Historical Ledger" not in history_block
    assert "commandCenterOpenPositions" not in history_block
    assert 'title: "Closed Trades"' in journal_block
    assert "Closed governed trades" in journal_block
    assert "Legacy / partial historical trades" in journal_block


def test_command_center_missing_canonical_state_shows_repair_guidance() -> None:
    pages = _text(STATIC / "operator_shell/pages/index.js")
    command_block = pages.split("async function renderCommandCenterWorkspace", 1)[1].split("async function renderPositionsWorkspace", 1)[0]

    assert "canonicalMissing" in command_block
    assert "Current-day operator state missing" in command_block
    assert "Portal fallback is active" in command_block
    assert "Generate Operator State" in pages
    assert "canonical_operator_state.v1.json is missing for today" in pages


def test_direct_operator_routes_are_shell_routes() -> None:
    server = _text(SERVER)
    for route in ["/aegis-command-center", "/aegis-positions", "/aegis-history", "/aegis-research-workspace"]:
        assert f'"{route}"' in server.split("SHELL_ROUTES = {", 1)[1].split("def translate_path", 1)[0]
    assert "elif normalized in self.SHELL_ROUTES" in server



def test_intraday_pipeline_publishes_daily_operator_workspace() -> None:
    pipeline = _text(ROOT / "ops/tools/run_aegis_intraday_sleeves_now_v1.py")
    package = _text(ROOT / "package.json")
    workspace_tool = _text(ROOT / "ops/tools/build_aegis_daily_operator_workspace_v1.py")

    canonical_index = pipeline.index("build_aegis_canonical_operator_state_v1.py")
    daily_index = pipeline.index("write_aegis_daily_operator_v1.py")
    workspace_index = pipeline.index("build_aegis_daily_operator_workspace_v1.py")
    projection_index = pipeline.index("run_aegis_projection_refresh_v1.py")

    assert canonical_index < daily_index < workspace_index < projection_index
    assert '"aegis:daily-operator-workspace"' in package
    assert '"schema_id": "aegis_daily_operator_workspace"' in workspace_tool
    assert '"broker_submit_transmit_allowed": False' in workspace_tool
    assert '"autonomous_execution_allowed": False' in workspace_tool
    assert '"trade_advice_allowed": False' in workspace_tool

def test_workflow_routes_are_registered_and_no_execution_posts_added() -> None:
    server = _text(SERVER)

    for route in ["/aegis-opportunities", "/aegis-edge-lab", "/aegis-paper-performance", "/aegis-performance", "/aegis-journal", "/aegis-today", "/aegis-review", "/aegis-research", "/aegis-history"]:
        assert route in OpsHandler.SHELL_ROUTES
    post_block = server.split("def do_POST", 1)[1].split("def do_PATCH", 1)[0]
    patch_block = server.split("def do_PATCH", 1)[1]
    for route in ["/aegis-opportunities", "/aegis-edge-lab", "/aegis-paper-performance", "/aegis-performance", "/aegis-journal", "/aegis-today", "/aegis-review", "/aegis-research", "/aegis-history", "/api/aegis/operator-cockpit"]:
        assert route not in post_block
        assert route not in patch_block


def test_left_app_header_contains_canonical_aegis_icon_and_no_operator_avatar_control() -> None:
    html = _text(HTML)
    css = _text(CSS)
    main = _text(MAIN)
    handler = OpsHandler.__new__(OpsHandler)

    brand_block = html.split('class="brand-identity-line"', 1)[1].split('<h1', 1)[0]

    assert 'id="brandMark"' in brand_block
    assert 'data-static-brand="true"' in brand_block
    assert 'class="brand-aegis-icon"' in brand_block
    assert 'src="assets/brand/aegis-logo-official-transparent.png"' in brand_block
    assert 'alt=""' in brand_block
    assert '<div class="section-eyebrow">AEGIS</div>' in brand_block
    assert '.brand-aegis-icon-shell' in css
    assert '.brand-aegis-icon' in css
    assert 'width: 190px' in css
    assert 'flex: 0 0 190px' in css
    assert 'width: 178px' in css
    assert 'brandMark.dataset.staticBrand === "true"' in main

    resolved = Path(handler.translate_path('/aegis-lite/assets/brand/aegis-logo-official-transparent.png'))
    assert resolved == STATIC / 'assets/brand/aegis-logo-official-transparent.png'
    assert resolved.exists()

    assert 'class="avatar-button"' not in html
    assert 'aria-label="User menu"' not in html
    assert 'aria-label="Help"' not in html
    assert 'aria-label="Notifications"' not in html


def test_aegis_boot_renders_final_route_once_and_preserves_content_on_refresh() -> None:
    main = _text(MAIN)
    css = _text(CSS)

    boot_block = main.split("export async function bootOperatorShell", 1)[1].split('document.getElementById("commandPaletteButton")', 1)[0]
    assert 'await Promise.all([sharedStateLoad, routeRender])' in boot_block
    assert 'await loadSharedShellState({ summaryOnly: true })' not in boot_block
    assert 'await renderRoute();' not in boot_block
    assert 'renderRoute({ source: "boot" })' in boot_block

    render_block = main.split("async function renderRoute", 1)[1].split("async function navigateTo", 1)[0]
    assert 'const shouldPreserveContent = backgroundRefresh || sameRenderedRoute || Boolean(cachedView)' in render_block
    assert 'setWorkspaceUpdating({ active: true })' in render_block
    assert 'setWorkspaceUpdating({ warning: error?.message || "Refresh failed; showing last good content." })' in render_block
    assert 'mainHost.innerHTML = `<div class="page-loading">Loading ${escapeHtml(route.label)}...</div>`' in render_block
    assert '.workspace-refresh-indicator' in css


def test_aegis_boot_diagnostics_and_duplicate_fetch_dedupe_are_engineering_only() -> None:
    main = _text(MAIN)
    api_client = _text(STATIC / "operator_shell/api_client/index.js")

    assert 'markBootEvent("BOOT_START"' in main
    assert 'markBootEvent("ROUTE_RESOLVED"' in main
    assert 'markBootEvent("STATE_FETCH_START"' in main
    assert 'markBootEvent("STATE_FETCH_END"' in main
    assert 'markBootEvent("RENDER_START"' in main
    assert 'markBootEvent("RENDER_END"' in main
    assert 'markBootEvent("REFRESH_START"' in main
    assert 'markBootEvent("REFRESH_END"' in main
    assert 'markBootEvent("ROUTE_REPLACED"' in main
    assert 'markBootEvent("CONTENT_CLEARED"' in main
    assert 'bootDiagnosticsPanel = state.engineeringDrawerOpen ?' in main
    assert 'boot_time_ms' in main
    assert 'first_paint_ms' in main
    assert 'data_ready_ms' in main
    assert 'duplicate_fetch_count' in main
    assert 'route_rerender_count' in main

    assert 'const inFlightJsonRequests = new Map()' in api_client
    assert 'inFlightJsonRequests.has(requestKey)' in api_client
    assert 'window.__AEGIS_DUPLICATE_FETCH_COUNT' in api_client


def test_aegis_startup_wires_click_handler_once_and_refresh_uses_background_render() -> None:
    main = _text(MAIN)
    boot_block = main.split("export async function bootOperatorShell", 1)[1]

    assert 'dataset.aegisEarlyClickWired' not in boot_block
    assert boot_block.count('document.body.addEventListener("click"') == 1
    assert 'document.body.dataset.aegisClickWired !== "true"' in boot_block
    assert 'await renderRoute({ backgroundRefresh: true, source: "refresh" });' in main
    assert 'markBootEvent("REFRESH_TIMERS_ARMED"' in main
