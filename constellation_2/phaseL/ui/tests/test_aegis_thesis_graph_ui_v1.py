from __future__ import annotations

from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

from constellation_2.phaseL.ui.server.run_ops_dashboard_v1 import OpsHandler

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
NAV = ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js"
DOMAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_legacy_market_theses_route_opens_primary_hypotheses_workspace() -> None:
    assert "/aegis-theses" in OpsHandler.SHELL_ROUTES
    nav = _text(NAV)
    assert 'label: "Hypotheses"' in nav
    assert 'label: "Market Theses"' not in nav.split("export function flattenNavigation", 1)[0]
    pages = pages_source_v1(ROOT)
    assert 'id: "aegis_theses"' in pages
    assert 'return renderAegisWorkflowPage("theses")' in pages
    assert 'title: "Hypotheses"' in pages
    assert 'html: renderHypothesesWorkspace(consolePayload, {})' in pages
    assert 'hideContextRail: true' in pages


def test_hypotheses_workspace_replaces_thesis_graph_primary_surface() -> None:
    pages = pages_source_v1(ROOT)
    block = pages.split("async function renderAegisThesesWorkflow", 1)[1].split("function renderMissingCanonicalTodayWorkflow", 1)[0]
    assert "fetchResearchConsole" in block
    assert "renderHypothesesWorkspace" in block
    assert "fetchAegisThesisGraph" not in block
    for old_label in [
        "Persistent, append-only thesis graph",
        "Active investigations",
        "Impacted intents/candidates",
        "Open thesis detail",
        "Capture History",
    ]:
        assert old_label not in block


def test_thesis_graph_api_remains_available_but_not_primary_nav() -> None:
    domain = _text(DOMAIN)
    server = _text(SERVER)
    pages = pages_source_v1(ROOT)
    nav_primary = _text(NAV).split("export function flattenNavigation", 1)[0]
    assert 'query("/api/aegis/thesis-graph", params)' in domain
    assert 'load_or_build_thesis_graph_response_v1' in server
    assert '"/api/aegis/thesis-graph"' in server
    assert "renderDashboardThesisSignals" in pages
    assert 'label: "Market Theses"' not in nav_primary


def test_primary_hypotheses_surface_does_not_expose_execution_controls() -> None:
    pages = pages_source_v1(ROOT)
    block = pages.split("function renderHypothesesWorkspace", 1)[1].split("function researchFilterTokens", 1)[0]
    for forbidden in ["Submit order", "Transmit", "Autonomous trade", "Execute trade", "Broker submit/transmit"]:
        assert forbidden not in block
