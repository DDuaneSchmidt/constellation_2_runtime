from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
PAGES = REPO / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
INVENTORY = REPO / "AEGIS_OPERATOR_LEGACY_RENDERER_INVENTORY.md"


def _pages() -> str:
    return PAGES.read_text(encoding="utf-8")


def _block(source: str, start: str, end: str) -> str:
    return source.split(start, 1)[1].split(end, 1)[0]


def test_legacy_renderer_inventory_exists() -> None:
    text = INVENTORY.read_text(encoding="utf-8")
    for page in ["Command Center", "Positions", "Performance", "Position Review", "Research", "Engineering"]:
        assert page in text
    assert "delete / bypass plan" in text.lower()


def test_forbidden_operator_strings_removed_from_current_route_renderers() -> None:
    pages = _pages()
    for forbidden in [
        "PAPER-2026-05-26",
        "AEGIS_POSITION_REVIEW_BRIEF_V1",
        "No position review briefs are available. Run npm run",
        "Run npm run aegis:position-review",
        "Action Queue",
        "Unavailable: aegis_sleeve_analytics_v1",
        "Unavailable: no position attribution",
        "Performance unavailable",
    ]:
        assert forbidden not in pages


def test_current_operator_pages_return_single_shared_template_path() -> None:
    pages = _pages()
    expectations = [
        ("async function renderCommandCenterWorkspace", "function positionsCaptureSummary", "html: OperatorInboxTemplate({ surfaceId: \"command_center\", contractRow: commandSurface, bodyHtml:"),
        ("async function renderPositionsWorkspace", "function positionNumber", "html: EntityListTemplate({ surfaceId: \"positions\", contractRow: positionsSurface, bodyHtml:"),
        ("function renderAegisPaperPerformancePage", "function renderAegisPositionReviewPage", "html: AnalyticsTemplate({ surfaceId: \"performance\", contractRow: performanceSurface, bodyHtml:"),
        ("function renderAegisPositionReviewPage", "function renderPositionReviewBriefCard", "html: ReviewTemplate({ surfaceId: \"position_review\", contractRow: reviewSurface, bodyHtml:"),
        ("async function renderResearchLabPage", "function _configFieldValue", "html: EntityListTemplate({ surfaceId: \"research\", contractRow: researchSurface, bodyHtml:"),
        ("async function renderEngineeringDashboardWorkspace", "function renderAegisTodayWorkflow", "html: TroubleshootingTemplate({ surfaceId: \"engineering\", contractRow: engineeringSurface, bodyHtml:"),
    ]
    for start, end, expected in expectations:
        block = _block(pages, start, end)
        assert expected in block
        assert "contextHtml: renderTrustPanel(" not in block


def test_current_operator_pages_do_not_prepend_template_as_array_item() -> None:
    pages = _pages()
    for start, end in [
        ("async function renderCommandCenterWorkspace", "function positionsCaptureSummary"),
        ("async function renderPositionsWorkspace", "function positionNumber"),
        ("function renderAegisPaperPerformancePage", "function renderAegisPositionReviewPage"),
        ("function renderAegisPositionReviewPage", "function renderPositionReviewBriefCard"),
        ("async function renderResearchLabPage", "function _configFieldValue"),
        ("async function renderEngineeringDashboardWorkspace", "function renderAegisTodayWorkflow"),
    ]:
        block = _block(pages, start, end)
        assert "html: [" not in block
        assert ".join(\"\"),\n    contextHtml" not in block


def test_runtime_operator_codes_are_translated_before_visible_rendering() -> None:
    pages = _pages()
    label_block = _block(pages, "function operatorPlainLabel", "function commandCenterDaily")
    for code in [
        "UNKNOWN_REQUIRES_DIAGNOSTICS",
        "STALE_READ_MODEL",
        "OUTPUT_CANDIDATES_CAPTURED",
    ]:
        assert code in label_block
    assert "Signal evidence is unavailable" in label_block
    assert "SIGNAL_EVIDENCE_MISSING" not in label_block
    capture_block = _block(pages, "function renderCandidateCaptureConfirmationPanel", "async function renderPositionsWorkspace")
    assert "operatorPlainLabel(summary.classification)" in capture_block
    assert "operatorPlainLabel(summary.status)" in capture_block
