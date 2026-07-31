from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
PAGES = REPO / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "pages" / "index.js"
MODULE = REPO / "aegis" / "modules" / "operator_portal" / "aegis.module.yaml"


def _pages() -> str:
    return PAGES.read_text(encoding="utf-8")


def test_operator_ui_architecture_authorities_are_manifested() -> None:
    data = json.loads(MODULE.read_text(encoding="utf-8"))
    inputs = set(data.get("inputs", []))
    assert "AEGIS_OPERATOR_UI_ARCHITECTURE_REQUIREMENTS.md" in inputs
    assert "AEGIS_OPERATOR_EMPTY_STATE_REQUIREMENTS.md" in inputs
    assert "reports/aegis_operator_route_inventory_v1/{day}/operator_route_inventory.v1.json" in inputs
    assert "reports/aegis_operator_route_inventory_v1/{day}/operator_route_inventory.v1.json" in set(data.get("outputs", []))
    commands = {row.get("command") for row in data.get("commands", []) if isinstance(row, dict)}
    assert "npm run aegis:operator-route-inventory" in commands
    assert "npm run aegis:operator-route-inventory-self-check" in commands


def test_shared_operator_templates_exist_for_required_page_types() -> None:
    pages = _pages()
    for name in [
        "function OperatorInboxTemplate",
        "function EntityListTemplate",
        "function AnalyticsTemplate",
        "function ReviewTemplate",
        "function TroubleshootingTemplate",
        "function renderSharedOperatorTemplate",
    ]:
        assert name in pages
    assert "renderOperatorSurfaceContractState(surfaceId, contractRow)" in pages


def test_page_renderers_use_contract_owned_templates() -> None:
    pages = _pages()
    expected = {
        "renderCommandCenterWorkspace": "OperatorInboxTemplate({ surfaceId: \"command_center\"",
        "renderPositionsWorkspace": "renderPositionsOwnershipPage({ payload, openPositions, closedPositions, summary, state })",
        "renderHistoryWorkspace": "EntityListTemplate({ surfaceId: \"history\"",
        "renderAegisPaperPerformancePage": "AnalyticsTemplate({ surfaceId: \"performance\"",
        "renderAegisPositionReviewPage": "ReviewTemplate({ surfaceId: \"position_review\"",
        "renderResearchLabPage": "EntityListTemplate({ surfaceId: \"research\"",
        "renderEngineeringDashboardWorkspace": "TroubleshootingTemplate({ surfaceId: \"engineering\"",
    }
    for function_name, template_call in expected.items():
        assert function_name in pages
        assert template_call in pages


def test_history_surface_is_contract_gated_before_legacy_content() -> None:
    pages = _pages()
    block = pages.split("async function renderHistoryWorkspace", 1)[1].split("async function renderResearchWorkspace", 1)[0]
    assert "fetchAegisOperatorSurfaceContract(routeParams)" in block
    assert "surfaceContractRow(payload, \"history\")" in block
    assert "renderContractGatedPage({ title: \"History\"" in block
    assert block.index("EntityListTemplate({ surfaceId: \"history\"") < block.index("renderSectionHeader({ eyebrow: \"Trade History\"")


def test_non_ready_contract_visible_text_hooks_are_required() -> None:
    pages = _pages()
    for visible_hook in [
        "contract-primary-message",
        "contract-reason",
        "contract-impact",
        "contract-next-step",
        "contract-ask-aegis",
        "View Diagnostics",
    ]:
        assert visible_hook in pages
