from __future__ import annotations

import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

from ops.tools.research_hypotheses_clickthrough_qa_v1 import CdpClient, wait_for_target

ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"


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


def test_evidence_drawer_applies_to_target_workspaces() -> None:
    source = PAGES.read_text(encoding="utf-8")
    for start, end in [
        ("function renderNarrativeStatementList", "function renderNarrativeChart"),
        ("function renderTradeDetailDialog", "function renderTradeEvaluationTable"),
        ("function renderExitReviewDetailDrawer", "function renderExitReviewPositionTable"),
        ("function renderCapturedTradeTable", "function renderJournalHeader"),
    ]:
        block = source[source.index(start):source.index(end)]
        assert "renderEvidenceTrigger" in block
    candidate_drilldown = source[
        source.index("function renderCandidateFunnelDrilldownTable"):
        source.index("function renderCandidateFunnelWorkspace")
    ]
    assert "Evidence available" in candidate_drilldown
    assert "renderConfidenceBadge" in candidate_drilldown
    assert "renderEvidenceStatusBadge" in candidate_drilldown
    candidate_workspace = source[
        source.index("function renderCandidateFunnelWorkspace"):
        source.index("if (typeof window !== \"undefined\")", source.index("function renderCandidateFunnelWorkspace"))
    ]
    assert "renderEvidenceTrigger" in candidate_workspace
    assert "Universe evidence" in source
    assert "Source artifacts" in source


def test_evidence_drawer_opens_from_narrative_item_and_copy_buttons_work() -> None:
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
        cdp.command("Page.navigate", {"url": f"http://127.0.0.1:{server_port}/aegis-performance?cache_bust=evidence_drawer"})
        time.sleep(1.5)
        expression = r"""
(async () => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  for (let i = 0; i < 90; i += 1) {
    if (document.querySelector('[data-narrative-operational-analytics] [data-evidence-payload]')) break;
    await sleep(200);
  }
  const mainTextBefore = document.body.innerText || '';
  const button = document.querySelector('[data-narrative-operational-analytics] [data-evidence-payload]');
  if (button) button.click();
  await sleep(500);
  const drawer = document.getElementById('evidenceDrawer');
  const body = document.getElementById('evidenceDrawerBody');
  const buttons = Array.from(body?.querySelectorAll('button[data-copy-text]:not([disabled])') || []);
  const pathButton = buttons[0] || null;
  const hashButton = buttons[1] || null;
  if (pathButton) pathButton.click();
  await sleep(120);
  return {
    path: window.location.pathname,
    opened: Boolean(drawer?.open),
    mainHasLongPathBefore: /\/home\/node\/constellation_runtime_data\/truth\//.test(mainTextBefore),
    drawerText: body?.innerText || '',
    hasArtifactPath: /Artifact path/i.test(body?.innerText || '') && /\/home\/node\/constellation_runtime_data\/truth\//.test(body?.innerText || ''),
    hasHash: /Artifact hash|Replay \/ evaluation hash/i.test(body?.innerText || ''),
    hasMetric: /Supporting metric/i.test(body?.innerText || ''),
    hasConfidence: /Confidence/i.test(body?.innerText || ''),
    hasStatus: /Evidence status/i.test(body?.innerText || ''),
    copyPathText: pathButton?.textContent || '',
    hasCopyHash: Boolean(hashButton),
    no404: !/Error response\s+Error code:\s*404|ENDPOINT_NOT_FOUND|File not found/i.test(document.body.innerText || ''),
  };
})()
"""
        result = cdp.command("Runtime.evaluate", {"expression": expression, "awaitPromise": True, "returnByValue": True}, timeout=30).get("result", {}).get("value")
        assert result["path"] == "/aegis-performance"
        assert result["opened"] is True
        assert result["mainHasLongPathBefore"] is False
        assert result["hasArtifactPath"] is True
        assert result["hasHash"] is True
        assert result["hasMetric"] is True
        assert result["hasConfidence"] is True
        assert result["hasStatus"] is True
        assert result["copyPathText"] == "Copied"
        assert result["hasCopyHash"] is True
        assert result["no404"] is True
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
