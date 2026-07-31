from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
CSS = ROOT / "constellation_2/phaseL/ui/static/aegis.css"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _input_checks_block() -> str:
    pages = _text(PAGES)
    return pages.split("function renderInputContractReconciliationRows", 1)[1].split("function renderDashboardLatestRunSummary", 1)[0]


def _summary_block() -> str:
    pages = _text(PAGES)
    return pages.split("function renderEngineeringRunSummaryCards", 1)[1].split("function renderMarketDataCoveragePanel", 1)[0]


def test_input_checks_summary_appears_once_in_default_hierarchy() -> None:
    pages = _text(PAGES)
    latest_block = pages.split("function renderDashboardLatestRunSummary", 1)[1].split("function renderDashboardCurrentOperationalState", 1)[0]
    input_block = _input_checks_block()

    assert "renderEngineeringRunSummaryCards" in latest_block
    assert "Input Checks" in _summary_block()
    assert "Input Contract Issues" in input_block
    assert "engineering-summary-grid compact" not in input_block
    assert "Shared repair action is shown once" in input_block


def test_warnings_and_non_blocking_rows_are_hidden_by_default() -> None:
    pages = _text(PAGES)
    block = _input_checks_block()

    assert 'return engineeringIssueSeverity(row) === "BLOCKING"' in pages
    assert 'summary>Warnings (' in block
    assert 'summary>Informational rows (' in block
    assert "warningRows.map" in block
    assert "informationalRows.map" in block
    assert "engineeringIssueVisibleByDefault" not in block


def test_non_blocking_rows_are_informational_not_action_required() -> None:
    pages = _text(PAGES)
    actionability_block = pages.split("function engineeringActionability", 1)[1].split("function engineeringIssueVisibleByDefault", 1)[0]

    assert 'if (severity === "BLOCKING") return "ACTION_REQUIRED"' in actionability_block
    assert 'if (severity === "WARNING") return "MONITOR"' in actionability_block
    assert 'return "INFORMATIONAL"' in actionability_block
    assert 'engineeringIssueHasAction(row)) return "ACTION_REQUIRED"' not in actionability_block


def test_one_grouped_repair_action_is_shown_for_input_contract_issues() -> None:
    pages = _text(PAGES)
    block = _input_checks_block()

    assert "Fix Next" in pages
    assert "Repair Input Contracts" in block
    assert "data-copy-text" in block
    assert "Grouped repair action" in block
    assert "Run repair-input-contracts" in pages
    assert 'label: "Repair"' not in block
    assert "Required action" not in pages.split("function renderEngineeringIssueCard", 1)[1].split("function renderEngineeringFixNext", 1)[0]


def test_view_details_reveals_full_rows_without_information_loss() -> None:
    block = _input_checks_block()

    assert "View Details" in block
    assert "Producer contract mismatch table" in block
    assert "renderSimpleTable" in block
    assert "Allowed symbols" in block
    assert "Market hash" in block
    assert "Stale intent" in block
    assert "Cause" in block
    assert "Evidence:" in block


def test_top_summary_order_and_low_density_styles_exist() -> None:
    summary = _summary_block()
    css = _text(CSS)

    assert summary.index("Market Data") < summary.index("Candidate Contracts") < summary.index("Input Checks")
    assert "engineering-summary-grid" in css
    assert "engineering-fix-next-grid" in css
    assert "engineering-fix-actions" in css
    assert "engineering-group-action" in css



def _diagnostic_block() -> str:
    pages = _text(PAGES)
    return pages.split("function renderDiagnosticRejectionReasons", 1)[1].split("function engineeringRowText", 1)[0]


def test_diagnostic_rejections_render_grouped_action_summary_by_default() -> None:
    block = _diagnostic_block()
    before_details = block.split('<summary>View Details</summary>', 1)[0]

    assert "Candidate Rejections" in block
    assert "groupDiagnosticRejectionRows" in block
    assert "issueType" in block
    assert "affectedSleeve" in block
    assert "requiredAction" in block
    assert "Action required" in block
    assert "Monitor" in block
    assert "Informational" in block
    assert "Top Issue" in block
    assert "Impact" in block
    assert "Next Action" in block
    assert "Affected Count" in block
    assert "${fullTable}" in block.split('<summary>View Details</summary>', 1)[1]


def test_diagnostic_rejections_hide_paths_and_raw_rows_until_details() -> None:
    block = _diagnostic_block()
    before_details = block.split('<summary>View Details</summary>', 1)[0]
    after_details = block.split('<summary>View Details</summary>', 1)[1]

    assert "diagnostic-detail-drawer" in block
    assert "Full diagnostic rejection table" in after_details
    assert "renderSimpleTable" in block
    assert "Copy Path" in after_details
    assert "${fullTable}" in after_details
    assert "Diagnostics evidence" in after_details


def test_diagnostic_rejections_dedupe_repeated_commands_at_group_level() -> None:
    block = _diagnostic_block()

    assert "renderDiagnosticGroupedCommands" in block
    assert "new Set" in block
    assert "Grouped repair action" in block
    assert "data-copy-text" in block
    assert "Copy Command" in block


def test_diagnostic_rejections_hide_non_actionable_rows_by_default() -> None:
    block = _diagnostic_block()
    actionability = block.split("function diagnosticRejectionActionability", 1)[1].split("function diagnosticRejectionRequiredAction", 1)[0]

    assert 'return "INFORMATIONAL"' in actionability
    assert 'return "MONITOR"' in actionability
    assert 'return "ACTION_REQUIRED"' in actionability
    assert "informational row" in block
    assert "monitor row" in block
    assert "actionRequired.map" in block
    assert "informational.map" in block
