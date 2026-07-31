from __future__ import annotations

from pathlib import Path
from constellation_2.phaseL.ui.tests.operator_shell_test_sources import pages_source_v1

from ops.aegis.operator_action_command_contracts_v1 import command_registry_v1

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
NAV = ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js"
CSS = ROOT / "constellation_2/phaseL/ui/static/aegis.css"
MAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
CLICKTHROUGH_QA = ROOT / "ops/tools/research_hypotheses_clickthrough_qa_v1.py"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _block(source: str, start: str, end: str) -> str:
    return source.split(start, 1)[1].split(end, 1)[0]


def test_primary_nav_uses_hypotheses_as_research_concept() -> None:
    nav = _text(NAV)
    primary = nav.split("export function flattenNavigation", 1)[0]
    assert 'label: "Hypotheses"' in primary
    assert 'route: "/research-lab"' in primary
    assert 'label: "Research"' in primary
    assert 'label: "Market Theses"' not in primary


def test_hypotheses_cards_answer_primary_workflow_questions() -> None:
    source = pages_source_v1(ROOT)
    card = _block(source, "function renderHypothesisCard", "function emptyHypothesisViewSection")
    primary = _block(source, "function hypothesisPrimaryAction", "function hypothesisSecondaryActions")
    action_bar = _block(source, "function renderHypothesisActionBar", "function renderHypothesisCard")
    command_button = _block(source, "function renderCommandButton", "function renderHypothesisActionBar")
    workspace = _block(source, "function renderHypothesesWorkspace", "function researchFilterTokens")
    summary = _block(source, "function renderHypothesesSummaryCounts", "function renderHypothesesHeroCta")
    for label in ["title", "Status", "Current stage", "Symbols", "Last run", "Trigger source", "Recommendation"]:
        assert label in card
    commands = command_registry_v1()["commands_by_id"]
    for command_id in ["START_RESEARCH", "VIEW_QUEUE", "VIEW_PROGRESS", "VIEW_WAITING_REASON", "VIEW_FINDINGS", "VIEW_BLOCKER", "VIEW_RECOMMENDATION"]:
        assert commands[command_id]["label"]
    assert "row.primary_command" in primary
    assert "hypothesis-primary-action" in command_button
    assert "hypothesis-more-menu" in action_bar
    assert "data-aegis-command-id" in command_button
    assert "hypothesis-start-form" not in action_bar
    assert 'type="submit"' not in action_bar
    for section in ["recommendations_ready", "collecting_evidence", "ready_to_start", "researching", "waiting", "blocked", "completed"]:
        assert section in source
    assert "top_item" in summary
    assert "visible_items" in source



def test_hypotheses_one_primary_action_and_more_menu_rules() -> None:
    source = pages_source_v1(ROOT)
    action_bar = _block(source, "function renderHypothesisActionBar", "function renderHypothesisCard")
    primary = _block(source, "function hypothesisPrimaryAction", "function hypothesisSecondaryActions")
    secondary = _block(source, "function hypothesisSecondaryActions", "function renderHypothesisActionBar")
    summary = _block(source, "function renderHypothesesSummaryCounts", "function renderHypothesesHeroCta")
    section = _block(source, "function renderHypothesisSection", "function renderHypothesesDiagnosticsPanel")

    assert "row.primary_command" in primary
    assert "No action required" not in primary
    assert "Action unavailable" not in primary
    assert "safeList(row.secondary_commands)" in secondary
    command_button = _block(source, "function renderCommandButton", "function renderHypothesisActionBar")
    assert "data-aegis-command-id" in command_button
    assert "data-aegis-command-action-type" in command_button
    assert "data-aegis-command-payload" in command_button
    assert 'data-hypothesis-detail-target' in command_button
    assert 'data-hypothesis-detail-action' in command_button
    assert 'href="${escapeHtml(primary.href)}"' not in action_bar
    assert "hypothesis-start-form" not in action_bar
    assert "Pause Research" not in secondary
    assert "hypotheses-summary-counts" in summary
    assert "safeList(viewModel.summary_cards)" in summary
    assert "card.top_item" in summary
    assert "card.top_item?.card_anchor_id" in summary
    assert "data-hypothesis-summary-section-target" in summary
    assert "data-hypothesis-summary-card-target" in summary
    assert "section.visible_items" in section
    assert "section.collapsed_by_default || count === 0" in section
    assert "hypothesis-section-collapsed" in section

def test_hypothesis_statuses_are_simplified_for_primary_ux() -> None:
    source = pages_source_v1(ROOT)
    status_block = _block(source, "const HYPOTHESIS_USER_STATUSES", "function hypothesisRunState")
    status_function = _block(source, "function hypothesisUserStatus", "function hypothesisCurrentStage")
    for status in ["Ready to Start", "Queued", "Scheduled", "Researching", "Collecting Evidence", "Waiting", "Complete", "Blocked", "Recommendation Ready"]:
        assert status in status_block
    for internal in ["IDEA", "RESEARCHING", "VALIDATING", "PAPER_TRIAL", "READY", "ARCHIVED"]:
        assert internal not in status_block
    assert "research_run_state" in source
    assert "row.user_facing_status || row.status || runState.user_facing_status" in status_function
    assert "text.includes" not in status_function
    assert "lifecycle_state" not in status_function


def test_hypotheses_workspace_renders_only_server_view_model() -> None:
    source = pages_source_v1(ROOT)
    workspace = _block(source, "function renderHypothesesWorkspace", "function researchFilterTokens")
    assert "hypothesis_view_model_v1" in source
    assert "renderHypothesesSummaryCounts(viewModel)" in workspace
    assert "renderHypothesisSection(viewModel.sections[sectionId]" in workspace
    assert "rendered_hypothesis_count" in source
    assert "unmapped_hypothesis_ids" in source
    assert "filter((row)" not in workspace
    assert ".sort(" not in workspace
    assert "hypothesisSectionName" not in source



def test_start_page_focuses_selected_ready_hypothesis() -> None:
    source = pages_source_v1(ROOT)
    panel = _block(source, "function renderResearchConsolePanel", "function renderHypothesisQueueConsolePanel")
    workflow = _block(source, "export async function executeResearchConsoleWorkflow", "export async function executeResearchDataAcquisitionWorkflow") if "export async function executeResearchDataAcquisitionWorkflow" in source.split("export async function executeResearchConsoleWorkflow", 1)[1] else source[source.index("export async function executeResearchConsoleWorkflow"):]
    assert 'currentSearchParams().get("hypothesis_id")' in panel
    assert "data-start-focused-hypothesis" in panel
    assert 'name="hypothesis_id"' in panel
    assert "buttonLabel" in panel
    assert 'hypothesis_id: String(formData?.get("hypothesis_id")' in workflow


def test_start_research_card_uses_command_contract_not_broken_navigation() -> None:
    source = pages_source_v1(ROOT)
    action_bar = _block(source, "function renderHypothesisActionBar", "function renderHypothesisCard")
    main = _text(MAIN)
    client = _text(ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js")
    command_button = _block(source, "function renderCommandButton", "function renderHypothesisActionBar")
    assert "START_RESEARCH" not in action_bar  # command id comes from server projection, not a frontend status map
    assert "data-aegis-command-id" in command_button
    assert "data-aegis-command-action-type" in command_button
    assert "actionType" in command_button
    assert "executeAegisCommand" in main
    assert 'postJson("/api/aegis/commands/execute"' in client
    assert "hypothesis-start-form" not in action_bar
    assert 'href="/research-lab/start"' not in action_bar


def test_start_research_stays_in_app_and_route_guard_exists() -> None:
    main = _text(MAIN)
    server = _text(SERVER)
    handler = _block(main, "async function runAegisCommandElement", "async function handleClick")
    assert "executeAegisCommand" in handler
    assert "event.preventDefault()" in _block(main, 'const aegisCommandAction = event.target.closest("[data-aegis-command-id]");', 'const candidateModalOpen = event.target.closest("[data-candidate-open-modal]");')
    assert "START_RESEARCH" in handler
    assert "card.dataset.hypothesisStatus" in handler
    assert '"/api/aegis/commands/execute"' in server
    assert '"/research-lab/start"' in server
    assert '"/research-lab/hypotheses"' in server
    assert 'normalized.startswith("/research-lab/")' in server
    assert 'normalized.startsWith("/research-lab/")' in main
    assert 'if path == "/api/research-lab/start-research"' in server


def test_summary_deep_links_to_exact_cards_and_no_global_start_button() -> None:
    source = pages_source_v1(ROOT)
    summary = _block(source, "function renderHypothesesSummaryCounts", "function renderHypothesesHeroCta")
    card = _block(source, "function renderHypothesisCard", "function emptyHypothesisViewSection")
    workspace = _block(source, "function renderHypothesesWorkspace", "function researchFilterTokens")
    assert "card.top_item?.title" in summary
    assert "card.top_item?.card_anchor_id" in summary
    assert "hypotheses-summary-section-button" in summary
    assert "hypotheses-summary-card-button" in summary
    assert '<a class="hypotheses-summary-count"' not in summary
    assert 'id="${escapeHtml(anchor)}"' in card
    assert 'tabindex="-1"' in card
    assert "renderHypothesisDetailPanel(row, detailPanelId)" in card
    assert "data-hypothesis-card-message-output" in card
    assert "renderHypothesesHeroCta(viewModel)" not in workspace
    assert "data-hypotheses-total-count" in workspace
    assert "renderHypothesesSummaryCounts(viewModel)" in workspace
    assert "renderResearchConsoleActionFeedback(state.researchConsoleWorkflow" in workspace
    assert "Start Research</a>" not in workspace


def test_hypotheses_clickthrough_qa_script_targets_visible_interactions() -> None:
    source = _text(CLICKTHROUGH_QA)
    assert "chromium" in source.lower()
    assert "--window-size=1600,900" in source
    assert "data-hypotheses-workspace" in source
    assert "data-hypothesis-detail-target" in source
    assert "data-hypothesis-summary-card-target" in source
    assert "hypothesis-more-menu" in source
    assert "Start Research" in source
    assert "produced browser/404 state" in source
    assert "no visible UI change" in source


def test_hypothesis_card_actions_open_inline_details_not_missing_routes() -> None:
    source = pages_source_v1(ROOT)
    main = _text(MAIN)
    css = _text(CSS)
    action_bar = _block(source, "function renderHypothesisActionBar", "function renderHypothesisCard")
    detail_panel = _block(source, "function renderHypothesisDetailPanel", "function renderHypothesisActionBar")
    command_button = _block(source, "function renderCommandButton", "function renderHypothesisActionBar")
    handler = _block(main, "function showCommandDetailPanel", "async function runAegisCommandElement")

    commands = command_registry_v1()["commands_by_id"]
    assert commands["VIEW_WAITING_REASON"]["label"] == "View Waiting Reason"
    assert commands["VIEW_FINDINGS"]["label"] == "View Findings"
    assert commands["VIEW_BLOCKER"]["label"] == "View Blocker"
    assert commands["VIEW_QUEUE"]["label"] == "View Queue"
    assert "data-hypothesis-detail-target" in command_button
    assert '<a class="primary-button research-action-button hypothesis-primary-action"' not in action_bar
    assert "hypothesis-inline-detail" in detail_panel
    assert "panel.hidden = false" in handler
    assert "panel.focus?.({ preventScroll: true })" in handler
    assert "hypothesis-card-highlight" in handler
    assert ".hypothesis-inline-detail" in css
    assert ".hypothesis-card-message" in css


def test_hypotheses_summary_controls_expand_scroll_focus_and_highlight() -> None:
    source = pages_source_v1(ROOT)
    main = _text(MAIN)
    css = _text(CSS)
    summary = _block(source, "function renderHypothesesSummaryCounts", "function renderHypothesesHeroCta")
    handler = _block(main, "function revealHypothesisSummaryTarget", "async function openArtifact") + _block(main, "async function handleClick", "const expandableRow = event.target.closest")

    assert "data-hypothesis-summary-section-target" in summary
    assert "data-hypothesis-summary-card-target" in summary
    assert "hypothesis-section-${card.section_id}" in summary
    assert "card.top_item?.card_anchor_id" in summary
    assert '<a class="hypotheses-summary-count"' not in summary
    assert "section.open = true" in handler
    assert 'scrollIntoView({ behavior: "smooth"' in handler
    assert "target.focus?.({ preventScroll: true })" in handler
    assert "hypothesis-card-highlight" in handler
    assert "hypothesis-section-highlight" in handler
    assert ".hypotheses-summary-count.is-empty" in css
    assert ".hypothesis-card-highlight" in css
    assert "@keyframes hypothesis-card-focus-pulse" in css


def test_no_backend_internal_states_in_primary_hypotheses_workspace() -> None:
    source = pages_source_v1(ROOT)
    workspace = _block(source, "function renderHypothesesWorkspace", "function researchFilterTokens")
    card = _block(source, "function renderHypothesisCard", "function emptyHypothesisViewSection")
    primary = workspace + card
    for forbidden in ["lifecycle_state", "current_gate", "proposal_status", "current_status", "gate_status", "operator_lifecycle_state"]:
        assert forbidden not in primary


def test_infrastructure_jargon_is_hidden_in_diagnostics_by_default() -> None:
    source = pages_source_v1(ROOT)
    workspace = _block(source, "function renderHypothesesWorkspace", "function researchFilterTokens")
    diagnostics = _block(source, "function renderHypothesesDiagnosticsPanel", "function renderHypothesesWorkspace")
    for forbidden in ["thesis graph", "projection semantics", "runtime metadata", "certification internals", "governance enums", "replay metadata", "scheduler internals"]:
        assert forbidden not in workspace.lower()
    assert '<details class="hypotheses-diagnostics"' in diagnostics
    assert "renderResearchInventoryPanel(payload)" in diagnostics
    assert "renderResearchPipelineSummaryPanel(payload)" in diagnostics
    assert "renderHypothesesDiagnosticsPanel(payload, state)" not in workspace
    assert "renderHypothesesDiagnosticsPanel(payload, state)" not in workspace


def test_recommendation_workflow_is_visible_without_execution_language() -> None:
    source = pages_source_v1(ROOT)
    card = _block(source, "function renderHypothesisCard", "function emptyHypothesisViewSection")
    detail = _block(source, "function renderResearchDossierPanel", "function renderResearchPlansConsolePanel")
    assert "Recommendation Ready" in card
    assert "Review Brief Available" in card
    assert "Collecting Evidence" in card
    assert "Manual IB capture guidance" not in card
    assert "confidence" in detail.lower()
    assert "Related symbols" in detail
    for forbidden in ["broker submit", "execution eligible", "order routing"]:
        assert forbidden not in (card + detail).lower()


def test_primary_workspaces_remove_right_side_rails() -> None:
    source = pages_source_v1(ROOT)
    for start, end in [
        ("function renderAegisCandidatesWorkflow", "function renderAegisReviewWorkflow"),
        ("function renderAegisResearchWorkflow", "function renderEdgeLabCommandHeader"),
        ("async function renderAegisThesesWorkflow", "function renderMissingCanonicalTodayWorkflow"),
        ("async function renderResearchLabPage", "function _configFieldValue"),
    ]:
        block = _block(source, start, end)
        assert 'contextHtml: ""' in block
        assert "hideContextRail: true" in block


def test_hypotheses_layout_is_responsive_for_1600_by_900() -> None:
    css = _text(CSS)
    assert ".workflow-layout .page-content" in css
    assert ".hypothesis-card-grid" in css
    assert "grid-template-columns: repeat(2, minmax(0, 1fr))" in css
    assert "@media (max-width: 1180px)" in css
    assert ".hypotheses-mission-control" in css
    assert ".hypotheses-summary-counts" in css
    assert ".hypothesis-more-menu" in css
    assert ".hypothesis-section-collapsed" in css


def test_hypotheses_render_as_single_column_full_width_list() -> None:
    source = pages_source_v1(ROOT)
    css = _text(CSS)
    section = _block(source, "function renderHypothesisSection", "function renderHypothesesDiagnosticsPanel")
    card = _block(source, "function renderHypothesisCard", "function emptyHypothesisViewSection")

    assert 'class="hypothesis-card-grid"' in section
    assert "grid-template-columns: minmax(0, 1fr);" in css
    assert ".hypothesis-card {\n  width: 100%;" in css
    assert "hypothesis-card-description" in card
    assert "hypothesis-compact-fact-rows" in card
    assert "hypothesis-fact-row" in card


def test_hypothesis_cards_use_compact_rows_not_six_box_grid() -> None:
    source = pages_source_v1(ROOT)
    css = _text(CSS)
    card = _block(source, "function renderHypothesisCard", "function emptyHypothesisViewSection")

    for label in ["Status", "Symbols", "Last run", "Trigger source", "Recommendation"]:
        assert label in card
    assert "hypothesis-card-facts.hypothesis-compact-fact-rows" in css
    assert "border: 0;" in css
    assert "background: transparent;" in css
    assert "grid-template-columns: 140px minmax(0, 1fr);" in css


def test_long_hypothesis_titles_wrap_and_action_stays_visible() -> None:
    source = pages_source_v1(ROOT)
    css = _text(CSS)
    action_bar = _block(source, "function renderHypothesisActionBar", "function renderHypothesisCard")

    assert "overflow-wrap: anywhere;" in css
    assert "hyphens: auto;" in css
    assert "hypothesis-primary-action" in action_bar or "hypothesis-primary-action" in source
    assert "justify-content: flex-start;" in css
    assert "@media (max-width: 760px)" in css
