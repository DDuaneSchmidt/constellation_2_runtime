from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
PAGES = REPO / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
CLIENT = REPO / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "domain_client" / "index.js"
SERVER = REPO / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py"
NAV = REPO / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "navigation_schema.js"


def test_position_review_route_and_endpoint_exist() -> None:
    server = SERVER.read_text(encoding="utf-8")
    client = CLIENT.read_text(encoding="utf-8")
    pages = PAGES.read_text(encoding="utf-8")
    assert '"/aegis-position-review"' in server
    assert '"/api/aegis/position-review/latest"' in server
    assert 'fetchAegisPositionReviewBrief' in client
    assert 'query("/api/aegis/position-review/latest", params)' in client
    assert 'case "aegis_position_review"' in pages


def test_open_positions_link_to_position_review() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    assert 'View Position Review' in pages
    assert '/aegis-position-review' in pages


def test_position_review_ui_reads_brief_artifact_not_raw_ai() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    nav = NAV.read_text(encoding="utf-8")
    assert 'aegis_position_review_brief_v1' in pages
    assert 'aegis_position_review_score_v1' in nav
    assert 'aegis_position_review_brief_v1' in nav
    assert 'fetchAegisPositionReviewBrief(routeParams)' in pages
    assert 'OpenAI(' not in pages
    assert 'chat.completions' not in pages
    assert 'responses.create' not in pages


def test_position_review_shows_required_sections() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    for label in ["Key Insight", "Position Health", "Thesis Status", "Most Important Supporting Evidence", "Most Important Contradicting Evidence", "Most Important Risk", "Most Important Confirmation", "Monitoring Points", "Data Quality", "Context Hash"]:
        assert label in pages
    assert "position-review-source-diagnostics" in pages
    assert '<details class="operator-disclosure position-review-source-diagnostics">' in pages
