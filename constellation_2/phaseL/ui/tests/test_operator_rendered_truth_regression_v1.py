from __future__ import annotations

import re
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import pytest
from ops.tools.research_hypotheses_clickthrough_qa_v1 import CdpClient, wait_for_target

ROOT = Path(__file__).resolve().parents[4]
DAY = "2026-06-01"


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
    raise RuntimeError(f"Server did not become ready: {last_error}")


def _normalize_visible_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


@pytest.fixture(scope="module")
def rendered_portal() -> str:
    chromium = shutil.which("chromium") or shutil.which("chromium-browser")
    if not chromium:
        raise AssertionError("Chromium is required for mandatory rendered-browser truth regression")
    server_port = _free_port()
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
    try:
        _wait_http_ok(f"http://127.0.0.1:{server_port}/healthz")
        yield f"http://127.0.0.1:{server_port}"
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()


def _render_visible_text(base_url: str, route: str, required_fragments: list[str]) -> str:
    cdp_port = _free_port()
    chromium = shutil.which("chromium") or shutil.which("chromium-browser")
    assert chromium
    profile = tempfile.TemporaryDirectory()
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
            "--window-size=1600,1200",
            "about:blank",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        cdp = CdpClient(wait_for_target(cdp_port, timeout=10))
        cdp.command("Runtime.enable")
        cdp.command("Page.enable")
        cdp.command("Network.enable")
        cdp.command("Network.setCacheDisabled", {"cacheDisabled": True})
        cdp.command("Page.navigate", {"url": f"{base_url}{route}"})
        required = [_normalize_visible_text(fragment) for fragment in required_fragments]
        expression = r'''
(async (requiredFragments) => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const normalize = (value) => String(value || "").replace(/\s+/g, " " ).trim().toLowerCase();
  for (let i = 0; i < 200; i += 1) {
    const text = document.body?.innerText || "";
    const normalized = normalize(text);
    const compact = normalized.replace(/ /g, "");
    const hasRequired = requiredFragments.every((fragment) => normalized.includes(fragment) || compact.includes(fragment.replace(/ /g, "")));
    const hasForbidden = normalized.includes("open positions 0")
      || normalized.includes("no open paper positions are recorded")
      || normalized.includes("open positions unavailable")
      || normalized.includes("performance open positions unavailable")
      || normalized.includes("routequeryparams is not defined");
    if (hasRequired || hasForbidden) {
      return {url: window.location.href, title: document.title, text};
    }
    await sleep(100);
  }
  return {url: window.location.href, title: document.title, text: document.body?.innerText || ""};
})
'''
        result = cdp.command(
            "Runtime.evaluate",
            {"expression": f"({expression})({required!r})", "awaitPromise": True, "returnByValue": True},
            timeout=30,
        ).get("result", {}).get("value")
        assert isinstance(result, dict), result
        return _normalize_visible_text(str(result.get("text") or ""))
    finally:
        browser.terminate()
        try:
            browser.wait(timeout=5)
        except subprocess.TimeoutExpired:
            browser.kill()
        profile.cleanup()



ROUTE_CASES = [
    (
        "today_command_center_rendered_current_truth",
        f"/aegis-command-center?day={DAY}",
        ["PAPER MODE", "TODAY'S RESEARCH RESULT", "DAVID ACTIONS", "GENERATED HYPOTHESIS PROGRESS", "VALIDATION PROGRESS", "CURRENT BOTTLENECK", "Action Required", "Macro calendar event dislocation watch", "David Actions 1", "New Candidates +21", "New Observations +21", "New Closed Outcomes +2", "New Validation Samples +2", "Generated Advanced 0", "Oil shock reversals across energy ETFs", "MISSING_DATA", "Connect Source", "Upload Dataset", "Defer", "Research observations only", "Closed today 2", "Included Samples 2", "Manual Review Queue 2"],
    ),
    (
        "today_command_center_explicit_day_rendered_current_truth",
        f"/aegis-command-center?day={DAY}",
        ["PAPER MODE", "Research observations only", "Raw signals 21", "Valid candidates 19", "Auto-promoted 19", "Open observations 53", "Closed today 2", "Included samples 2", "Manual review queue 2", "Recommendations 10", "HOLD 6", "PAUSE 4"],
    ),
    (
        "positions_rendered_current_truth",
        "/aegis-positions",
        ["Paper Research Mode", "Open paper observations", "Closed paper outcomes today", "Manual review queue", "No broker execution occurred", "Not trade advice", "Manual review only"],
    ),
    (
        "positions_explicit_day_rendered_manual_review_exits",
        f"/aegis-positions?day={DAY}",
        ["Paper Research Mode", "Open paper observations", "Closed paper outcomes today", "Manual review queue", "No broker execution occurred", "Not trade advice", "Manual review only"],
    ),
    (
        "research_workspace_rendered_canonical_workflow_state",
        f"/aegis-research-workspace?day={DAY}",
        ["Paper Research Mode", "David Action Queue", "Hypothesis Status Summary", "Hypothesis List", "Macro calendar event dislocation watch", "NEEDS_DATA", "PROVIDE_DATA_SOURCE", "Connect Source", "Upload Dataset", "Mark Not Available", "Defer", "macro event calendar", "event_name", "release_datetime", "affected_assets", "Oil shock reversals across energy ETFs", "PAPER_TRACKING_READY", "Awaiting qualifying paper candidates", "Paper research only. Not trade advice. No broker execution. No live trading."],
    ),
]



@pytest.mark.parametrize(("case_name", "route", "required_fragments"), ROUTE_CASES, ids=[case[0] for case in ROUTE_CASES])
def test_rendered_browser_open_position_truth_does_not_diverge_from_api_payload(
    rendered_portal: str,
    case_name: str,
    route: str,
    required_fragments: list[str],
) -> None:
    text = _render_visible_text(rendered_portal, route, required_fragments)
    compact_text = text.replace(" ", "")
    for fragment in required_fragments:
        normalized = _normalize_visible_text(fragment)
        assert normalized in text or normalized.replace(" ", "") in compact_text, f"{case_name} missing visible fragment {fragment!r}"
    forbidden_fragments = [
        "open positions 0",
        "no open paper positions are recorded",
        "open positions unavailable",
        "performance open positions unavailable",
        "routequeryparams is not defined",
        "outcome closure / validation throughput",
    ]
    if route.startswith("/aegis-positions"):
        forbidden_fragments.extend([
            "position values incomplete",
        ])
    if route.startswith("/aegis-research-workspace"):
        forbidden_fragments.extend([
            "1 paper promotion approval needed",
            "ready for human review",
            "paper promotion recommendations",
            "trade recommendation",
            "manual capture",
            "unknown",
        ])
    if route.startswith("/aegis-command-center"):
        forbidden_fragments.extend([
            "raw signals 0",
            "valid candidates 0",
            "auto-promoted 0",
            "55 open paper observations",
            "closed today 0",
            "included samples 0",
            "manual review queue 0",
            "research observations only research observations only",
            "no live trading no live trading",
            "no broker execution no broker execution",
            "monitoring only",
            "last successful run last successful run",
        ])
    for fragment in forbidden_fragments:
        normalized = _normalize_visible_text(fragment)
        assert normalized not in text and normalized.replace(" ", "") not in compact_text, f"{case_name} rendered forbidden fragment {fragment!r}"
