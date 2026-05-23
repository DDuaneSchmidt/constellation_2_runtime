from __future__ import annotations

from pathlib import Path

PAGES = Path(__file__).resolve().parents[2] / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
SERVER = Path(__file__).resolve().parents[2] / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py"


def test_ui_shows_projection_health_and_no_generic_false_empty() -> None:
    source = PAGES.read_text(encoding="utf-8")
    assert "Projection-backed operator queue" in source
    assert "Source proposals" in source
    assert "Projected items" in source
    assert "trusted_empty" in source
    assert "Projection not built or not trusted empty" in source


def test_projection_health_endpoint_exists() -> None:
    server = SERVER.read_text(encoding="utf-8")
    assert "/api/research-lab/projection-health" in server
    assert "research_lab_projection_health_v1" in server
