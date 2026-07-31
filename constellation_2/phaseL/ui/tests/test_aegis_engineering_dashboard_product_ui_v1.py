from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
DOMAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/domain_client/index.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
NAV = ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js"
ROUTES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js"
MANIFEST = ROOT / "aegis/modules/operator_portal/aegis.module.yaml"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_engineering_priority_queue_api_and_command_are_wired() -> None:
    assert "aegis:engineering-priority-queue" in _text(ROOT / "package.json")
    assert "aegis_engineering_priority_queue_v1" in _text(MANIFEST)
    assert "fetchAegisEngineeringPriorityQueue" in _text(DOMAIN)
    assert "/api/aegis/engineering-priority-queue/latest" in _text(DOMAIN)
    assert "/api/aegis/engineering-priority-queue" in _text(SERVER)
    assert "build_engineering_priority_queue_v1" in _text(SERVER)


def test_engineering_dashboard_renders_system_health_sections() -> None:
    pages = _text(PAGES)
    block = pages.split("async function renderEngineeringDashboardWorkspace", 1)[1].split("function renderAegisTodayWorkflow", 1)[0]

    for expected in [
        "System Health",
        "Can Aegis operate, what is healthy, what is degraded, and what is blocked?",
        "Aegis is monitoring only because runtime evidence is incomplete.",
        "Can Aegis operate?",
        "David action",
        "What is healthy?",
        "Evidence graph healthy.",
        "Runtime readiness",
        "Data availability",
        "Safety mode",
        "Trade advice, broker execution, live trading, and autonomous trading remain disabled.",
        "Recovery",
        "Top Health Issue",
        "What should happen next?",
        "Active Blockers",
        "What is blocked?",
        "Degraded Dependencies",
        "What is degraded?",
        "Data Availability",
        "Is required data available?",
        "Run and Dependency Health",
        "What dependencies are unhealthy?",
        "David Action",
        "View health evidence",
        "Ask Aegis",
        "Recovery plan required",
        "Repair Command",
        "Copy Repair Command",
        "Copy Verify Command",
        "Repair command unavailable.",
        "Runtime recovery plan",
        "Action Type",
        "human_evidence_summary",
        "raw_evidence",
        "Impact",
        "Repair",
        "Verify",
        "User Action Required",
    ]:
        assert expected in block or expected in pages
    assert "data-testid=\"system-health-operating-status\"" in pages
    assert "data-testid=\"system-health-fix-first\"" in pages
    assert "data-testid=\"system-health-blockers\"" in pages
    assert "data-testid=\"system-health-degraded\"" in pages
    assert "data-testid=\"system-health-data-availability\"" in pages
    assert "data-testid=\"system-health-dependencies\"" in pages
    assert "data-testid=\"system-health-action-state\"" in pages
    assert "renderAskAegisPanel(healthAskPayload, verifiedRuntime)" in block
    assert "renderEngineeringOperatorActionTable" in pages
    assert "renderEngineeringAskIssueButtons(row)" in pages
    assert block.index("renderSystemHealthStatus(healthQueue, verifiedRuntime)") < block.index("renderSystemHealthFixFirst(healthQueue)")
    assert block.index("renderSystemHealthFixFirst(healthQueue)") < block.index("renderAskAegisPanel(healthAskPayload, verifiedRuntime)")
    assert block.index("renderSystemHealthBlockers(healthQueue)") < block.index("renderSystemHealthDegraded(healthQueue)")
    assert block.index("renderSystemHealthDataAvailability(healthQueue)") < block.index("renderSystemHealthDependencies(healthQueue)")
    action_block = pages.split("function renderEngineeringIssueActionButtons", 1)[1].split("function renderEngineeringRepairRows", 1)[0]
    assert action_block.index("renderEngineeringAskIssueButtons(row)") < action_block.index("Copy Repair Command")
    assert "Copy Command" not in action_block
    assert "engineeringHasRepairCommand(row)" in action_block


def test_engineering_diagnostics_are_collapsed_by_default() -> None:
    pages = _text(PAGES)
    diagnostics = pages.split("function renderEngineeringDiagnostics", 1)[1].split("async function renderEngineeringDashboardWorkspace", 1)[0]

    assert "<details" in diagnostics
    assert "open" not in diagnostics.split("<details", 1)[1].split(">", 1)[0]
    assert "Source artifacts" in diagnostics
    assert "Raw issue count" in diagnostics


def test_engineering_route_uses_central_route_and_active_nav_identity() -> None:
    pages = _text(PAGES)
    nav = _text(NAV)
    routes = _text(ROUTES)

    assert 'case "aegis_opportunities"' in pages
    assert "return renderEngineeringDashboardWorkspace();" in pages
    assert 'id: "aegis_opportunities"' in routes
    assert 'nav_item_id: "aegis_dashboard"' in routes
    assert 'label: "System Health"' in nav
    assert 'route: "/aegis-opportunities"' in nav


def test_engineering_dashboard_distinguishes_repair_and_verify_commands() -> None:
    pages = _text(PAGES)
    repair_block = pages.split("function renderEngineeringRepairRows", 1)[1].split("function renderEngineeringIssueTable", 1)[0]
    top_block = pages.split("function renderEngineeringTopIssueHero", 1)[1].split("function renderEngineeringFixFirst", 1)[0]

    assert "Repair command unavailable. Use Ask Aegis or inspect recovery plan." in pages
    assert "Copy Repair Command" in pages
    assert "Copy Verify Command" in pages
    assert "Copy Command" not in repair_block
    assert "Copy Command" not in top_block
    assert repair_block.index("Human evidence summary") < repair_block.index("Raw engineering label")
    assert "<details" in repair_block
    assert "raw_evidence" in repair_block
