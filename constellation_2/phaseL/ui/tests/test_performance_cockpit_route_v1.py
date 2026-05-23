from __future__ import annotations

from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1


ROOT = Path(__file__).resolve().parents[4]


def test_performance_cockpit_route_is_wired_read_only() -> None:
    pages = pages_source_v1(ROOT)
    navigation = (ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js").read_text(encoding="utf-8")
    server = (ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py").read_text(encoding="utf-8")

    assert 'path: "/performance"' in pages
    assert 'id: "performance_cockpit"' in pages
    assert 'case "performance_cockpit":' in pages
    assert 'renderPerformanceCockpitPage' in pages
    assert 'src="${escapeHtml(cockpitUrl)}"' in pages
    assert 'loading="lazy"' in pages
    assert 'route: "/performance"' in navigation
    assert 'truthOwner: "aegis_performance_showcase_v1"' in navigation
    assert '"/performance"' in server
    assert '"/performance/cockpit.html"' in server
    assert "PERFORMANCE_SHOWCASE_FAMILY" in server
    assert "text/html; charset=utf-8" in server

