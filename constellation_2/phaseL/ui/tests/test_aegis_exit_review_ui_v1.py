from __future__ import annotations

from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time

import pytest

from ops.tools.research_hypotheses_clickthrough_qa_v1 import CdpClient, wait_for_target

from ops.aegis.operator_action_command_contracts_v1 import command_registry_v1


ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
NAVIGATION = ROOT / "constellation_2/phaseL/ui/static/operator_shell/navigation_schema.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
ROUTE_METADATA = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/route_metadata.js"
MAIN = ROOT / "constellation_2/phaseL/ui/static/operator_shell/main.js"
CSS = ROOT / "constellation_2/phaseL/ui/static/aegis.css"


def test_exit_review_route_and_workspace_are_registered() -> None:
    pages = PAGES.read_text(encoding="utf-8")
    navigation = NAVIGATION.read_text(encoding="utf-8")
    server = SERVER.read_text(encoding="utf-8")
    route_metadata = ROUTE_METADATA.read_text(encoding="utf-8")

    assert 'path: "/aegis-exit-review"' in route_metadata
    assert 'id: "aegis_exit_review"' in route_metadata
    assert 'route: "/aegis-exit-review"' in navigation
    assert 'id: "aegis_exit_review"' in navigation
    assert '"/aegis-exit-review"' in server
    assert '"/api/aegis/exit-review/latest"' in server
    assert 'case "aegis_exit_review":' in pages
    assert 'renderAegisWorkflowPage("exit_review")' in pages
    assert "renderExitReviewWorkspace" in pages


def test_exit_review_ui_shows_decision_sections_and_reasons() -> None:
    source = PAGES.read_text(encoding="utf-8")
    for text in [
        "Exit Summary",
        "Open Positions",
        "Needs Review",
        "Stop / Target Status",
        "Thesis / Time Stop Status",
        "Closed Outcomes",
        "exit_decision",
        "decision_reason",
        "next_operator_action",
        "Thesis state",
        "Exit bias",
        "Manual next action",
        "Review suggested",
    ]:
        assert text in source


def test_exit_review_commands_are_contract_only_and_manual_safe() -> None:
    source = PAGES.read_text(encoding="utf-8")
    renderer = source[source.index("function exitReviewCommandButton"):source.index("function renderExitReviewActions")]
    assert 'data-aegis-command-action-type="IN_PAGE_DETAIL"' in renderer
    assert "href=" not in renderer

    commands = command_registry_v1()["commands_by_id"]
    for command_id in [
        "VIEW_EXIT_DECISION",
        "UPDATE_STOP_PLAN",
        "RECORD_PARTIAL_EXIT",
        "RECORD_FULL_EXIT",
        "RECORD_TRADE_OUTCOME",
        "VIEW_POSITION_DETAIL",
    ]:
        assert command_id in commands
        assert commands[command_id]["action_type"] == "IN_PAGE_DETAIL"
        assert commands[command_id]["safety_classification"]["broker_submit_transmit_allowed"] is False
        assert commands[command_id]["safety_classification"]["autonomous_execution_allowed"] is False
        assert commands[command_id]["safety_classification"]["order_routing_allowed"] is False


def test_exit_review_detail_dialogs_are_visible_modal_surfaces() -> None:
    css = CSS.read_text(encoding="utf-8")
    main = MAIN.read_text(encoding="utf-8")
    pages = PAGES.read_text(encoding="utf-8")

    assert 'class="command-detail-dialog"' in pages
    assert "data-command-detail-target" in pages
    assert ".command-detail-dialog" in css
    assert ".command-detail-dialog[open]" in css
    assert "width: min(820px, calc(100vw - 32px));" in css
    assert "max-height: calc(100dvh - 32px);" in css
    assert ".command-detail-dialog-inner" in css
    assert "overflow: auto;" in css
    assert "if (panel.showModal && !panel.open)" in main
    assert "panel.showModal();" in main
    assert "openCommandDetail: null" in main
    assert "rememberOpenCommandDetail(commandElement, panelId);" in main
    assert "restoreOpenCommandDetailAfterRender();" in main
    assert "wireCommandDetailClose(panel);" in main
    assert "document.body.contains(panel)" in main
    assert "routeRenderInProgress" in main


def test_exit_review_detail_dialog_contains_required_operator_fields() -> None:
    source = PAGES.read_text(encoding="utf-8")
    detail = source[source.index("function renderExitReviewDetailDrawer"):source.index("function renderExitReviewPositionTable")]

    for text in [
        "Exit decision",
        "Reason",
        "Stop",
        "Target",
        "P&L",
        "Next action",
        "Thesis state",
        "Why",
        "Evidence count",
        "Supporting evidence",
        "Contradicting evidence",
        "Exit bias",
        "Manual next action",
        "View Evidence",
        "Manual review only",
        "Artifact",
        "artifact_path",
        "thesis_state_projection_v1",
    ]:
        assert text in detail


def test_exit_review_static_bundle_is_not_cached_by_managed_server() -> None:
    server = SERVER.read_text(encoding="utf-8")
    assert 'self.send_header("Cache-Control", "no-store")' in server



def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_http_ok(url: str, timeout: float = 15.0) -> None:
    import urllib.request

    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except Exception as exc:
            last_error = exc
        time.sleep(0.25)
    raise RuntimeError(f"server did not become ready: {last_error}")


def test_exit_review_dow_command_dialogs_open_visible_in_browser() -> None:
    chromium = shutil.which("chromium") or shutil.which("chromium-browser")
    if not chromium:
        pytest.skip("Chromium is not installed")
    server_port = _free_port()
    cdp_port = _free_port()
    profile = tempfile.TemporaryDirectory()
    server = subprocess.Popen(
        [
            "python3",
            "-B",
            "-m",
            "constellation_2.phaseL.ui.server.run_ops_dashboard_v1",
            "--host",
            "127.0.0.1",
            "--port",
            str(server_port),
        ],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    browser = None
    cdp = None
    try:
        _wait_http_ok(f"http://127.0.0.1:{server_port}/healthz")
        browser = subprocess.Popen(
            [
                chromium,
                "--headless=new",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-application-cache",
                "--disk-cache-size=0",
                "--media-cache-size=0",
                f"--remote-debugging-port={cdp_port}",
                f"--user-data-dir={profile.name}",
                "about:blank",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        cdp = CdpClient(wait_for_target(cdp_port, timeout=10))
        cdp.command("Runtime.enable")
        cdp.command("Page.enable")
        cdp.command("Network.enable")
        cdp.command("Network.setCacheDisabled", {"cacheDisabled": True})
        cdp.command("Emulation.setDeviceMetricsOverride", {"width": 1600, "height": 900, "deviceScaleFactor": 1, "mobile": False})
        cdp.command("Page.navigate", {"url": f"http://127.0.0.1:{server_port}/aegis-exit-review?cache_bust=exit_dialog"})
        time.sleep(5)
        expression = r"""
(async () => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  window.__exitReviewErrors = [];
  window.addEventListener('error', (event) => window.__exitReviewErrors.push(String(event.message || event.error || 'error')), true);
  window.addEventListener('unhandledrejection', (event) => window.__exitReviewErrors.push(String(event.reason?.message || event.reason || 'unhandled rejection')), true);
  const text = (node) => (node?.innerText || node?.textContent || '').trim();
  const visible = (node) => {
    if (!node || !node.open || node.hidden) return false;
    const style = getComputedStyle(node);
    const rect = node.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 300 && rect.height > 300;
  };
  for (let i = 0; i < 50; i += 1) {
    if (/\bDOW\b/.test(document.body.innerText || '') && document.querySelector('[data-aegis-command-id="VIEW_EXIT_DECISION"]')) break;
    await sleep(200);
  }
  const findDowButton = (commandId) => Array.from(document.querySelectorAll(`[data-aegis-command-id="${commandId}"]`)).find((button) => {
    const area = button.closest('tr') || button.closest('[data-command-surface]') || button.parentElement;
    return /\bDOW\b/.test(text(area));
  });
  const checks = [];
  for (const commandId of ['VIEW_EXIT_DECISION', 'VIEW_POSITION_DETAIL']) {
    document.querySelectorAll('dialog[open]').forEach((dialog) => dialog.close?.());
    await sleep(100);
    const button = findDowButton(commandId);
    if (button) {
      button.scrollIntoView({ behavior: 'instant', block: 'center' });
      await sleep(50);
      button.click();
      await sleep(150);
      if (!document.getElementById(button.getAttribute('data-command-detail-target'))?.open) {
        button.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window }));
      }
    }
    await sleep(900);
    const panel = button ? document.getElementById(button.getAttribute('data-command-detail-target')) : null;
    const body = text(panel);
    checks.push({
      commandId,
      found: Boolean(button),
      open: Boolean(panel?.open),
      visible: visible(panel),
      openedBy: panel?.dataset?.openedBy || '',
      openCommand: panel?.dataset?.commandDetailOpenCommand || '',
      hasDow: /\bDOW\b/.test(body),
      hasDecision: /Exit decision/i.test(body),
      hasReason: /Reason/i.test(body),
      hasStop: /Stop/i.test(body),
      hasTarget: /Target/i.test(body),
      hasPnl: /P&L/i.test(body),
      hasEvidence: /Evidence|Artifact|artifact_path/i.test(body),
      hasNextAction: /Next action/i.test(body),
      hasManual: /Manual review only/i.test(body),
    });
  }
  document.querySelectorAll('dialog[open]').forEach((dialog) => dialog.close?.());
  return {
    path: window.location.pathname,
    checks,
    errors: window.__exitReviewErrors,
    no404: !/Error response\s+Error code:\s*404|ENDPOINT_NOT_FOUND|File not found/i.test(document.body.innerText || ''),
  };
})()
"""
        result = cdp.command("Runtime.evaluate", {"expression": expression, "awaitPromise": True, "returnByValue": True}, timeout=30).get("result", {}).get("value")
        assert result["path"] == "/aegis-exit-review"
        assert result["no404"] is True
        assert result["errors"] == []
        assert {row["commandId"] for row in result["checks"]} == {"VIEW_EXIT_DECISION", "VIEW_POSITION_DETAIL"}
        for row in result["checks"]:
            assert row["found"] is True
            assert row["open"] is True
            assert row["visible"] is True
            assert row["openCommand"] == row["commandId"]
            assert row["hasDow"] is True
            assert row["hasDecision"] is True
            assert row["hasReason"] is True
            assert row["hasStop"] is True
            assert row["hasTarget"] is True
            assert row["hasPnl"] is True
            assert row["hasEvidence"] is True
            assert row["hasNextAction"] is True
            assert row["hasManual"] is True
    finally:
        if cdp is not None:
            cdp.close()
        if browser is not None:
            browser.terminate()
            try:
                browser.wait(timeout=5)
            except subprocess.TimeoutExpired:
                browser.kill()
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
        profile.cleanup()
