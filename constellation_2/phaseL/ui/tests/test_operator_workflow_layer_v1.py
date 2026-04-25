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
