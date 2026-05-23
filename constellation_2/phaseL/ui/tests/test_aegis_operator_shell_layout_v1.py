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


def test_collapsed_sidebar_remains_accessible_icon_rail() -> None:
    css = _text(CSS)
    main = _text(MAIN)

    assert "--operator-sidebar-rail-width" in css
    assert ".sidebar-collapsed .shell-leftnav" in css
    assert "width: var(--operator-sidebar-rail-width)" in css
    assert ".sidebar-collapsed .sidebar-collapse-button" in css
    assert 'state.sidebarMode === "expanded" ? "collapsed" : "expanded"' in main
    assert '"hidden"' not in main.split("function renderNav", 1)[1].split("document.getElementById(\"workspaceNav\")", 1)[0]


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


def test_primary_navigation_is_operator_workflow_only() -> None:
    nav = _text(NAV)
    primary_nav = nav.split("export const LEGACY_NAVIGATION_REFERENCE", 1)[0]

    for label in ['label: "Dashboard"', 'label: "Candidates"', 'label: "Hypotheses"', 'label: "Captured Trades"', 'label: "System Health"']:
        assert label in primary_nav
    for legacy_primary in ['label: "Opportunities"', 'label: "Edge Lab"', 'label: "Performance"', 'label: "Journal"']:
        assert legacy_primary not in primary_nav
    for clutter in [
        'label: "Receipts / Outcomes Drilldown"',
        'label: "Sleeve Performance Drilldown"',
        'label: "Event Trigger Drilldown"',
        'label: "Adaptive Intelligence Drilldown"',
        'label: "Research Lab Drilldown"',
    ]:
        assert clutter not in primary_nav


def test_workflow_routes_are_registered_and_no_execution_posts_added() -> None:
    server = _text(SERVER)

    for route in ["/aegis-opportunities", "/aegis-edge-lab", "/aegis-performance", "/aegis-journal", "/aegis-today", "/aegis-review", "/aegis-research", "/aegis-history"]:
        assert route in OpsHandler.SHELL_ROUTES
    post_block = server.split("def do_POST", 1)[1].split("def do_PATCH", 1)[0]
    patch_block = server.split("def do_PATCH", 1)[1]
    for route in ["/aegis-opportunities", "/aegis-edge-lab", "/aegis-performance", "/aegis-journal", "/aegis-today", "/aegis-review", "/aegis-research", "/aegis-history", "/api/aegis/operator-cockpit"]:
        assert route not in post_block
        assert route not in patch_block
