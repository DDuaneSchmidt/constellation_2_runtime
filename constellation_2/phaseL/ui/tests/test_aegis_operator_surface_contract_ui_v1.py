from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
DOMAIN = REPO / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "domain_client" / "index.js"
PAGES = REPO / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
SERVER = REPO / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py"


def test_operator_surface_contract_api_and_client_are_registered() -> None:
    domain = DOMAIN.read_text(encoding="utf-8")
    server = SERVER.read_text(encoding="utf-8")
    assert "fetchAegisOperatorSurfaceContract" in domain
    assert '"/api/aegis/operator-surface-contract/latest"' in domain
    assert "operator_surface_contract_path_v1" in server
    assert '"/api/aegis/operator-surface-contract"' in server


def test_operator_shell_has_central_contract_helper() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    assert "function surfaceContractPayload" in pages
    assert "function surfaceContractRow" in pages
    assert "surfaceContractRow({ operator_surface_contract: readinessPayload }" in pages
    assert "fetchAegisOperatorSurfaceContract(routeParams)" in pages


def test_shared_contract_renderer_exists_and_exposes_required_dom_hooks() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    for helper in [
        "function renderOperatorSurfaceContractState",
        "function renderContractSummaryCards",
        "function renderContractEmptyState",
        "function renderContractAskAegis",
    ]:
        assert helper in pages
    for hook in [
        "contract-primary-message",
        "contract-reason",
        "contract-impact",
        "contract-next-step",
        "contract-ask-aegis",
    ]:
        assert hook in pages


def test_requested_operator_pages_gate_legacy_primary_content_on_contract() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    for surface in [
        '"command_center"',
        '"positions"',
        '"performance"',
        '"position_review"',
        '"research"',
        '"engineering"',
    ]:
        assert surface in pages
    assert pages.count("contractPrimaryRenderAllowed") >= 8
    assert pages.count("renderContractGatedPage") >= 7


def test_contract_gated_surfaces_do_not_render_local_primary_actions_when_actions_disabled() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    assert 'surfaceActionsAllowed(payload, "positions") ? renderOpenPaperPositionExitButton(row)' in pages
    assert 'surfaceActionsAllowed(payload, "positions") ? renderPositionsTodayCandidateActions(row, payload)' in pages
    assert 'No operator action required' in pages


def test_contract_renderer_keeps_raw_evidence_collapsed_and_primary_copy_operator_readable() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    assert '<details class="operator-disclosure contract-diagnostics"' in pages
    assert "function contractOperatorText" in pages
    assert "Runtime truth is blocked" in pages
    assert "Use Ask Aegis or open diagnostics for the recovery plan." in pages


def test_ready_surfaces_render_contract_template_before_legacy_workflow() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    assert "function renderOperatorSurfaceContractReadyBanner" in pages
    assert "function renderSharedOperatorTemplate" in pages
    assert "data-operator-surface-contract-first" in pages
    assert 'OperatorInboxTemplate({ surfaceId: "command_center", contractRow: commandSurface, bodyHtml:' in pages
    assert 'EntityListTemplate({ surfaceId: "positions", contractRow: positionsSurface, bodyHtml:' in pages
    assert 'EntityListTemplate({ surfaceId: "history", contractRow: historySurface, bodyHtml:' in pages
    assert 'ReviewTemplate({ surfaceId: "position_review", contractRow: reviewSurface, bodyHtml:' in pages
    assert 'EntityListTemplate({ surfaceId: "research", contractRow: researchSurface, bodyHtml:' in pages
    assert 'TroubleshootingTemplate({ surfaceId: "engineering", contractRow: engineeringSurface, bodyHtml:' in pages
