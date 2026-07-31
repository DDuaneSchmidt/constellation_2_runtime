from __future__ import annotations

import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import pytest
from ops.tools.research_hypotheses_clickthrough_qa_v1 import CdpClient, wait_for_target

ROOT = Path(__file__).resolve().parents[4]
NAV = ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js"
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
ROUTES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js"
MAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_http_ok(url: str, timeout: float = 15.0) -> None:
    import urllib.request

    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except Exception as exc:
            last_error = exc
        time.sleep(0.25)
    raise RuntimeError(f"Server did not become ready: {last_error}")


def test_operator_mode_shows_ai_cio_nav_items() -> None:
    nav = _text(NAV)
    operator_nav = nav.split("export const ENGINEERING_NAVIGATION_SCHEMA", 1)[0]
    assert 'section: "AI CIO"' in operator_nav
    for label in [
        'label: "CIO Briefing"',
        'label: "Capital Map"',
        'label: "Portfolios"',
        'label: "Research Lab"',
        'label: "Opportunities"',
        'label: "Retirement Simulator"',
        'label: "Advisor Oversight"',
        'label: "Documents"',
        'label: "Carolyn"',
    ]:
        assert label in operator_nav
    for removed in [
        'label: "Command Center"',
        'label: "Positions"',
        'label: "History"',
        'label: "Change Control"',
        'label: "Dashboard"',
        'label: "Candidate Funnel"',
        'label: "Captured Trades"',
    ]:
        assert removed not in operator_nav


def test_ai_cio_home_page_answers_investment_questions() -> None:
    pages = _text(PAGES)
    block = pages
    for text in [
        "AI CIO Briefing",
        "Capital allocation, portfolio oversight, and investment research for David & Carolyn",
        "Portfolio decisions, not paperwork.",
        "Total Investable Assets",
        "Month-to-Date Change",
        "YTD Change",
        "Cash Available",
        "Advisor Fee Drag",
        "Research / Paper Trading Status",
        "What changed?",
        "What is my capital doing?",
        "What deserves attention?",
        "What investment opportunities or risks exist?",
        "What should I do next?",
        "Companion 50/50 Paper \" + \"Trade",
        "Ultra-Safe Portfolio",
        "Portfolio123 Screens",
        "Aegis / Paul Trade Logic",
        "Oak Harvest fee drag",
    ]:
        assert text in block
    for forbidden in [
        "assets are not the constraint",
        "umbrella insurance",
        "healthcare bridge",
        "generic protection",
    ]:
        assert forbidden not in block


def test_engineering_mode_exposes_old_routes_and_low_level_surfaces() -> None:
    nav = _text(NAV)
    engineering_nav = nav.split("export const ENGINEERING_NAVIGATION_SCHEMA", 1)[1]
    for label in ["Runtime Truth", "Verified Graph", "Repair Center", "Hash Lineage", "Candidate Lineage", "Evidence Ledger", "Input Checks", "Provider Health", "Candidate Funnel", "Closed Trades"]:
        assert label in engineering_nav
    assert "hidden_by_default: true" in engineering_nav
    assert "available_in_engineering_mode: true" in engineering_nav
    for route in ["/aegis-runtime-timeline", "/aegis-verified-runtime", "/aegis-repair-center", "/aegis-candidate-lineage", "/aegis-trading-desk", "/aegis-operations", "/aegis-audit-evidence"]:
        assert route in _text(ROUTES) or route in _text(NAV)
        assert route in _text(SERVER)


def test_operational_command_center_remains_available_but_not_home() -> None:
    pages = _text(PAGES)
    routes = _text(ROUTES)
    server = _text(SERVER)

    assert 'path: "/aegis-command-center"' in routes
    assert 'case "aegis_command_center"' in pages
    assert 'renderCommandCenterWorkspace()' in pages
    assert '"/aegis-command-center"' in server
    assert 'id: "ai_cio_briefing"' in routes



def test_operator_plain_language_codes_and_progressive_disclosure_exist() -> None:
    pages = _text(PAGES)
    for pair in [
        'MARKET_DATA_SHA_MISMATCH: "Data refresh mismatch"',
        'MISSING_REQUIRED_INPUTS: "Missing required data"',
        'PARTIAL_CONTEXT: "Some non-paper systems unavailable"',
        'GRAPH_READY_RUNTIME_BLOCKED: "Paper mode ready; full platform incomplete"',
    ]:
        assert pair in pages
    assert "View Evidence" in pages
    assert "Engineering Details" in pages


def test_positions_history_and_routes_are_registered() -> None:
    pages = _text(PAGES)
    server = _text(SERVER)
    routes = _text(ROUTES)
    for route in ["/aegis-command-center", "/aegis-positions", "/aegis-positions-diagnostics", "/aegis-history", "/aegis-research-workspace"]:
        assert route in routes
        assert route in server
    assert "async function renderPositionsWorkspace" in pages
    assert "async function renderPositionsDiagnosticsWorkspace" in pages
    assert "async function renderHistoryWorkspace" in pages
    assert "Legacy / partial historical record, not an open paper position" in pages


def test_engineering_drawer_is_explicit_and_default_sidebar_stays_operator_only() -> None:
    main = _text(MAIN)
    render_block = main.split("function renderNav", 1)[1].split("function renderKernelRail", 1)[0]
    assert "const navigationSchema = NAVIGATION_SCHEMA" in render_block
    assert "navigationSchemaForMode" not in render_block
    assert "data-engineering-drawer-toggle" in render_block
    assert "data-engineering-drawer" in render_block
    assert "state.engineeringDrawerOpen ? (engineeringGroups.length ? engineeringGroups.map" in render_block
    assert "data-operator-mode-toggle" not in main
    assert "aegis.operator.mode" not in main


def test_ai_cio_is_default_landing_page_and_system_health_stays_engineering() -> None:
    routes = _text(ROUTES)
    nav = _text(NAV)
    pages = _text(PAGES)
    assert 'path: "/",' in routes
    assert 'id: "ai_cio_briefing"' in routes
    assert 'nav_item_id: "cio_briefing"' in routes
    assert 'label: "CIO Briefing"' in nav
    assert 'label: "System Health"' in nav
    assert 'label: "Dashboard"' not in nav
    assert 'title: "AI CIO Briefing"' in pages
    assert 'title: "System Health"' in pages
    assert 'title: "Dashboard"' not in pages


def test_cio_policy_guardrails_present() -> None:
    source = _text(PAGES)
    cio_block = source
    assert "No broker execution is authorized by this page" in cio_block
    assert "trade advice" in cio_block
    assert "read-only oversight" in cio_block.lower()
    assert "Paper \" + \"Trade" in cio_block


def test_operator_header_is_status_only_and_refresh_is_contextual() -> None:
    html = (ROOT / "constellation_2/phaseL/ui/static/index.html").read_text(encoding="utf-8")
    main = _text(MAIN)
    pages = _text(PAGES)
    header = html.split('<header class="shell-topbar">', 1)[1].split('</header>', 1)[0]

    assert 'id="headerEnvironment"' in header
    assert 'id="headerDataTimestamp"' in header
    assert 'id="topRuntimeMode"' in header
    assert 'id="topReadiness"' in header
    for forbidden in ["refreshButton", "commandPaletteButton", "Quick Actions", "notification-button", "notificationBadge", "aria-label=\"Help\"", "avatar-button", "headerBackButton"]:
        assert forbidden not in header
    assert 'document.getElementById("refreshButton")?.addEventListener' in main
    assert 'document.getElementById("commandPaletteButton")?.addEventListener' in main
    assert "Refresh Positions" in pages
    assert 'data-refresh-route="positions"' in pages


def test_operator_sidebar_has_no_kernel_status_stack_and_runtime_moves_to_engineering_drawer() -> None:
    html = (ROOT / "constellation_2/phaseL/ui/static/index.html").read_text(encoding="utf-8")
    main = _text(MAIN)
    sidebar = html.split('<aside class="shell-leftnav">', 1)[1].split('</aside>', 1)[0]
    render_block = main.split("function renderNav", 1)[1].split("function renderKernelRail", 1)[0]

    assert "kernelStatusRail" not in sidebar
    assert "Kernel Status" not in sidebar
    assert "kernel-status-rail" not in sidebar
    assert "sidebarStatusChip" in sidebar
    assert "Paper Mode Ready" in sidebar
    assert "Runtime Status" in render_block
    assert "engineering-runtime-status-panel" in render_block
    assert "status-rail-item" in render_block
    assert 'state.engineeringDrawerOpen ? `' in render_block


def test_sidebar_structure_is_navigation_only_plus_engineering_toggle() -> None:
    html = (ROOT / "constellation_2/phaseL/ui/static/index.html").read_text(encoding="utf-8")
    main = _text(MAIN)
    sidebar = html.split('<aside class="shell-leftnav">', 1)[1].split('</aside>', 1)[0]
    render_block = main.split("function renderNav", 1)[1].split("function renderKernelRail", 1)[0]

    assert "sidebarSearch" in sidebar
    assert "workspaceNav" in sidebar
    assert "data-engineering-drawer-toggle" in render_block
    assert "CIO Briefing" in _text(NAV)
    assert "Capital Map" in _text(NAV)
    assert "Portfolios" in _text(NAV)
    assert "Research Lab" in _text(NAV)


def test_operator_shell_navigation_contract_is_central_source_of_truth() -> None:
    routes = _text(ROUTES)
    nav = _text(NAV)
    main = _text(MAIN)

    assert Path(ROOT / "AEGIS_OPERATOR_SHELL_NAVIGATION_REQUIREMENTS.md").exists()
    for token in [
        "OPERATOR_SHELL_ROUTE_REGISTRY",
        "OPERATOR_SHELL_ROUTE_NAV_OVERRIDES",
        "operatorShellRouteContractForPath",
        "activeNavSelectionForPath",
        "normalizeRoutePath",
        "routeShouldPreserveDayQuery",
        "NOT_FOUND_ROUTE",
    ]:
        assert token in routes
    assert 'import { activeNavSelectionForPath } from "./pages/route_metadata.js"' in nav
    assert "OPERATOR_ROUTE_WORKSPACE_ALIASES" not in nav
    assert "operatorShellRouteContractForPath(window.location.pathname)" in main
    assert "routeShouldPreserveDayQuery(finalPath)" in main


def test_route_contract_declares_required_identity_fields() -> None:
    routes = _text(ROUTES)
    for field in ["route_id", "path", "nav_item_id", "title", "workspace_id", "parent_nav_item_id", "canonical_path"]:
        assert field in routes
    for route, nav_id, workspace in [
        ('"/aegis-command-center"', 'nav_item_id: "workspace_command_center"', 'workspace_id: "command_center"'),
        ('"/aegis-positions"', 'nav_item_id: "workspace_positions"', 'workspace_id: "positions"'),
        ('"/aegis-paper-performance"', 'nav_item_id: "aegis_paper_performance"', 'workspace_id: "performance"'),
        ('"/aegis-opportunities"', 'nav_item_id: "aegis_dashboard"', 'workspace_id: "engineering"'),
        ('"/research-lab/review"', 'nav_item_id: "research_review"', 'workspace_id: "research"'),
    ]:
        assert route in routes
        assert nav_id in routes
        assert workspace in routes


def test_engineering_route_selection_uses_contract_and_not_command_center_fallback() -> None:
    main = _text(MAIN)
    render_block = main.split("function renderNav", 1)[1].split("function renderKernelRail", 1)[0]

    assert "const activeSelection = activeNavSelectionForPath(window.location.pathname)" in render_block
    assert 'activeSelection.workspace_id === "engineering"' in render_block
    assert 'data-engineering-drawer-toggle="true" data-route="/aegis-opportunities"' in render_block
    assert 'const alreadyInEngineering = activeSelection.workspace_id === "engineering"' in main
    assert "await navigateTo(targetRoute)" in main
    assert "activeNavigationForPath(window.location.pathname, navigationSchema)" in render_block
    assert 'label: "System Health"' in _text(NAV)
    assert 'title: "System Health"' in _text(PAGES)


def test_unknown_route_renders_safe_not_found_page() -> None:
    routes = _text(ROUTES)
    pages = _text(PAGES)
    main = _text(MAIN)

    assert 'id: "operator_shell_not_found"' in routes
    assert 'workspace_id: "not_found"' in routes
    assert 'case "operator_shell_not_found"' in pages
    assert "Page Not Found" in pages
    assert "ROUTES[0]" not in main.split("function currentRoute", 1)[1].split("function renderBrand", 1)[0]


def test_day_query_preservation_is_centralized_for_shell_navigation() -> None:
    main = _text(MAIN)
    navigate_block = main.split("async function navigateTo", 1)[1].split("function revealHypothesisSummaryTarget", 1)[0]

    assert 'new URLSearchParams(window.location.search || "")' in navigate_block
    assert '"day", "operational_day"' in navigate_block
    assert "routeShouldPreserveDayQuery(finalPath)" in navigate_block
    assert "targetQuery.set(key, currentQuery.get(key))" in navigate_block
    assert 'activeNavSelectionForPath(finalPath).workspace_id !== "engineering"' in navigate_block
    assert "state.engineeringDrawerOpen = false" in navigate_block


def test_top_level_cio_nav_items_have_contract_routes() -> None:
    nav = _text(NAV)
    routes = _text(ROUTES)

    for nav_id in [
        "cio_briefing",
        "cio_capital_map",
        "cio_portfolios",
        "cio_research_lab",
        "cio_opportunities",
        "cio_retirement_simulator",
        "cio_advisor_oversight",
        "cio_documents",
        "cio_carolyn",
    ]:
        assert nav_id in nav
        assert f'nav_item_id: "{nav_id}"' in routes

    for route in [
        "/",
        "/capital-map",
        "/portfolios",
        "/research-lab",
        "/cio-opportunities",
        "/retirement-simulator",
        "/advisor-oversight",
        "/documents",
        "/carolyn",
    ]:
        assert route in nav or route in routes
        assert route in routes


def test_engineering_drawer_is_not_noop_and_groups_internal_tools() -> None:
    main = _text(MAIN)
    render_block = main.split("function renderNav", 1)[1].split("function renderKernelRail", 1)[0]
    click_block = main.split("async function handleClick", 1)[1].split("const routeLink = event.target.closest", 1)[0]

    assert "data-engineering-drawer-toggle" in render_block
    assert "state.engineeringDrawerOpen = !state.engineeringDrawerOpen" in main
    assert "const engineeringDrawerToggle = event.target.closest" in click_block
    assert "const engineeringDrawerClose = event.target.closest" in click_block
    assert "data-engineering-drawer-close" in render_block
    assert "engineering-toggle-button ${state.engineeringDrawerOpen || activeEngineeringRoute ?" in render_block
    assert "active open" in render_block
    assert "engineeringNavStatus" in render_block
    assert "Drawer open" not in render_block

    for group in ["System", "Diagnostics", "Audit", "Repairs"]:
        assert f'label: "{group}"' in render_block
    combined = render_block + _text(NAV)
    for label in [
        "System Health",
        "System Evidence",
        "Run History",
        "Input Checks",
        "Market Data Coverage",
        "Data Lineage",
        "Provider Health",
        "Candidate Details",
        "Candidate Lineage",
        "Evidence Ledger",
        "Legacy / Partial Historical Trades",
        "Fix Issues",
        "Repair Commands",
    ]:
        assert label in combined
    assert "No engineering tools available" in render_block


def test_engineering_button_click_navigates_and_cio_briefing_click_returns() -> None:
    chromium = shutil.which("chromium") or shutil.which("chromium-browser")
    if not chromium:
        pytest.skip("Chromium is not installed")
    server_port = _free_port()
    cdp_port = _free_port()
    server = subprocess.Popen(
        [
            "python3",
            "-B",
            "-m",
            "constellation_2.phaseL.ui.server.run_ops_dashboard_v1",
            "--host",
            "127.0.0.1",
            "--port",
            str(server_port),
        ],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    profile = tempfile.TemporaryDirectory()
    browser = None
    try:
        _wait_http_ok(f"http://127.0.0.1:{server_port}/healthz")
        browser = subprocess.Popen(
            [
                chromium,
                "--headless=new",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-application-cache",
                "--disk-cache-size=0",
                "--media-cache-size=0",
                f"--remote-debugging-port={cdp_port}",
                f"--user-data-dir={profile.name}",
                "--window-size=1600,900",
                "about:blank",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cdp = CdpClient(wait_for_target(cdp_port, timeout=10))
        cdp.command("Runtime.enable")
        cdp.command("Page.enable")
        cdp.command("Network.enable")
        cdp.command("Network.setCacheDisabled", {"cacheDisabled": True})
        cdp.command("Emulation.setDeviceMetricsOverride", {"width": 1600, "height": 900, "deviceScaleFactor": 1, "mobile": False})
        cdp.command("Page.navigate", {"url": f"http://127.0.0.1:{server_port}/?day=2026-05-29"})
        time.sleep(4)
        expression = r'''
(async () => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const waitFor = async (predicate) => {
    for (let i = 0; i < 50; i += 1) {
      if (predicate()) return true;
      await sleep(100);
    }
    return false;
  };
  const summary = () => ({
    url: window.location.href,
    path: window.location.pathname,
    title: document.title,
    renderedTitle: document.querySelector('#workspaceTitle')?.textContent || '',
    commandActive: !!document.querySelector('.sidebar-group.active-parent [data-route="/"]'),
    engineeringActive: !!document.querySelector('[data-engineering-drawer-toggle].active'),
    commandText: document.querySelector('#workspaceContent')?.innerText?.slice(0, 200) || '',
  });
  const before = summary();
  const engineering = document.querySelector('[data-engineering-drawer-toggle]');
  const clickedEngineering = {
    tag: engineering?.tagName || null,
    href: engineering?.getAttribute('href') || null,
    dataRoute: engineering?.getAttribute('data-route') || null,
    disabled: !!engineering?.disabled,
  };
  engineering?.click();
  await waitFor(() => window.location.pathname === '/aegis-opportunities' && /System Health/.test(document.title));
  const afterEngineering = summary();
  const command = document.querySelector('[data-route="/"]');
  const clickedCommand = {
    tag: command?.tagName || null,
    href: command?.getAttribute('href') || null,
    dataRoute: command?.getAttribute('data-route') || null,
    disabled: !!command?.disabled,
  };
  command?.click();
  await waitFor(() => window.location.pathname === '/' && /AI CIO Briefing/.test(document.title));
  return {before, clickedEngineering, afterEngineering, clickedCommand, afterCommand: summary()};
})()
'''
        result = cdp.command("Runtime.evaluate", {"expression": expression, "awaitPromise": True, "returnByValue": True}, timeout=30).get("result", {}).get("value")
    finally:
        if browser:
            browser.terminate()
            try:
                browser.wait(timeout=5)
            except subprocess.TimeoutExpired:
                browser.kill()
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
        profile.cleanup()

    assert result["before"]["path"] == "/"
    assert result["clickedEngineering"]["tag"] == "BUTTON"
    assert result["clickedEngineering"]["dataRoute"] == "/aegis-opportunities"
    assert result["afterEngineering"]["path"] == "/aegis-opportunities"
    assert result["afterEngineering"]["renderedTitle"] == "System Health"
    assert result["afterEngineering"]["engineeringActive"] is True
    assert result["afterEngineering"]["commandActive"] is False
    assert "day=2026-05-29" in result["afterEngineering"]["url"]
    assert result["clickedCommand"]["dataRoute"] == "/"
    assert result["afterCommand"]["path"] == "/"
    assert result["afterCommand"]["renderedTitle"] == "AI CIO Briefing"
    assert result["afterCommand"]["commandActive"] is True
    assert result["afterCommand"]["engineeringActive"] is False
    assert "day=2026-05-29" in result["afterCommand"]["url"]


def test_engineering_internal_routes_remain_deeplinkable_with_operator_sidebar() -> None:
    main = _text(MAIN)
    nav = _text(NAV)
    server = _text(SERVER)

    for route in ["/aegis-opportunities", "/aegis-verified-runtime", "/aegis-runtime-timeline", "/aegis-repair-center", "/aegis-candidate-lineage"]:
        assert route in nav or route in main
        assert route in server
    assert "const navigationSchema = NAVIGATION_SCHEMA" in main
    assert "activeEngineeringRoute" in main
    assert "engineering-route-link ${isRouteActive ?" in main
