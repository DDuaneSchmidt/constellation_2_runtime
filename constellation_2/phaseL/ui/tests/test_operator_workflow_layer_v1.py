from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]


def test_server_exposes_kernel_aligned_shell_routes_and_commands() -> None:
    server_source = (ROOT / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py").read_text(encoding="utf-8")

    for route in [
        '"/api/shell/status-rail"',
        '"/api/work-queue"',
        '"/api/workspace/"',
        '"/api/commands/"',
        '"/api/command/overview"',
    ]:
        assert route in server_source

    assert "dispatch_kernel_command" in server_source

    for shell_route in ['"/"', '"/control"', '"/state"', '"/advisory"', '"/submission"', '"/lifecycle"']:
        assert shell_route in server_source


def test_shell_frontend_uses_path_routes_and_shared_kernel_primitives() -> None:
    shell_main = (ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "main.js").read_text(encoding="utf-8")
    primitives = (ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell" / "kernel_primitives" / "index.js").read_text(encoding="utf-8")

    for route in ['{ path: "/", id: "work-queue"', '{ path: "/control"', '{ path: "/state"', '{ path: "/advisory"', '{ path: "/submission"', '{ path: "/lifecycle"']:
        assert route in shell_main

    for endpoint in [
        'fetchJson("/api/shell/status-rail")',
        'fetchJson("/api/work-queue")',
        'fetchJson(`/api/workspace/${route.id}`)',
    ]:
        assert endpoint in shell_main

    for primitive_name in [
        "renderAuthorityArtifactCard",
        "renderCommandPanel",
        "renderRunEnvelopePanel",
        "renderLineageChain",
        "renderSupersessionBanner",
        "renderDerivedSummaryCard",
    ]:
        assert f"export function {primitive_name}" in primitives


def test_root_html_frames_single_operator_shell_not_dashboard() -> None:
    html = (ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "index.html").read_text(encoding="utf-8")
    assert "Kernel-Aligned Operator Console" in html
    assert "Kernel Workspaces" in html
    assert "Single-entry Operations + Advisory Surface" not in html


def test_advisory_dashboard_uses_governed_schema_navigation_and_command_overview() -> None:
    static_root = ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "operator_shell"
    navigation = (static_root / "navigation_schema.js").read_text(encoding="utf-8")
    command_overview = (static_root / "components" / "command_overview.js").read_text(encoding="utf-8")
    pages = (static_root / "pages" / "index.js").read_text(encoding="utf-8")
    api_client = (static_root / "api_client" / "index.js").read_text(encoding="utf-8")
    domain_client = (static_root / "domain_client" / "index.js").read_text(encoding="utf-8")
    shell_main = (static_root / "main.js").read_text(encoding="utf-8")

    for required in [
        'section: "CORE"',
        'section: "INTELLIGENCE"',
        'section: "SYSTEM"',
        'label: "Command"',
        'label: "Advisory"',
        'label: "Taxes"',
        'label: "Bug Log"',
        "badgeCount: 3",
        "badgeCount: 5",
        "truthOwner",
    ]:
        assert required in navigation

    for component_name in [
        "StatusStrip",
        "ExceptionCard",
        "ReadinessTile",
        "DecisionList",
        "EvidenceSummary",
        "DataLineage",
        "ContextRail",
    ]:
        assert f"export function {component_name}" in command_overview

    for status_class in [
        "status-ready",
        "status-warning",
        "status-error",
        "status-info",
        "status-muted",
        "readiness-progress",
    ]:
        assert status_class in command_overview

    assert "fetchCommandOverview" in pages
    assert 'query("/api/command/overview"' in domain_client
    assert "__AEGIS_API_BASE_URL" in api_client
    assert "LOCAL_OPERATOR_API_ORIGIN" not in api_client
    assert 'build_command_overview_view' in (
        ROOT / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py"
    ).read_text(encoding="utf-8")
    assert 'data_source_state: "MOCK / UNAVAILABLE"' in pages
    assert "COMMAND_OVERVIEW_MOCK" in pages

    assert "NAVIGATION_SCHEMA" in shell_main
    assert "data-nav-group-toggle" in shell_main
    assert "aria-controls" in shell_main
    assert "Boolean(query)" in shell_main
    assert "|| isActiveParent" not in shell_main
    assert "data-sidebar-collapse" in shell_main
    assert "formatDisplayLabel" in shell_main
    assert 'summary.environment || summary.runtime_mode || runtimeStatus.runtime_mode || "UNKNOWN")' in shell_main
    assert 'summary.kernel_version || summary.summary_id || "governed")' in shell_main
    assert "runExceptionAction" in shell_main
    assert "data-exception-card" in command_overview
    assert "data-exception-action" in command_overview
    assert "exception-action-state" in command_overview
    assert "policy_runtime" in command_overview
    assert "policy-runtime-banner" in command_overview

    css = (ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "aegis.css").read_text(encoding="utf-8")
    for visual_token in [
        "--aegis-green",
        "--aegis-teal",
        "--aegis-amber",
        "--aegis-red",
        ".readiness-progress span",
        ".exception-card.status-error",
        ".readiness-tile.status-ready",
        ".exception-action-button",
        ".exception-action-state",
        ".policy-runtime-banner",
        ":focus-visible",
    ]:
        assert visual_token in css
