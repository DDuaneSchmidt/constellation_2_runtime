from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
ROUTES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"


def test_candidates_nav_and_view_use_canonical_paper_projection() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    routes = ROUTES.read_text(encoding="utf-8")
    block = pages.split("function renderAegisCandidatesWorkflow", 1)[1].split("function renderRefreshCandidateProjectionButton", 1)[0]

    assert 'id: "aegis_candidates"' in routes
    assert "Today's Candidates / Paper Sessions" in routes + pages
    assert "dashboardPaperOperatorProjection(payload)" in block
    assert "candidate_ui_projection" not in block
    assert "paper_session_id" in block
    assert "Official session" in block
    assert "Canonicalized" in block
    assert "Reconstructions" in block
    for label in ["Today's Candidates", "Carry-forward", "Open Paper Positions", "Blocked / Skipped", "Symbol", "Sleeve / Strategy", "Direction", "Score / Conviction", "Status", "Paper Construction", "Market Data", "Session", "Created", "Reason"]:
        assert label in block


def test_dashboard_links_and_mode_split_are_operator_visible() -> None:
    pages = PAGES.read_text(encoding="utf-8")

    assert "renderRuntimeModeStatusStrip" in pages
    assert "Paper Mode" in pages
    assert "Advisory Mode" in pages
    assert "Live Mode" in pages
    assert "/aegis-candidates?session=current" in pages
    assert "Candidate Contracts" in pages
    assert "Current-day candidate diagnostics available" in pages
    assert "Paper Mode Ready" in pages


def test_state_snapshot_api_attaches_paper_operator_projection() -> None:
    server = SERVER.read_text(encoding="utf-8")

    assert "build_and_write_paper_operator_projection_v1" in server
    assert 'data["paper_operator_projection_v1"]' in server
    assert 'payload["paper_operator_projection_v1"]' in server
    assert 'source_paths"]["paper_operator_projection"]' in server
