from __future__ import annotations

from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request

from ops.aegis.attention_queue_projection_v1 import build_attention_queue_projection_v1
from constellation_2.phaseL.ui.server.run_ops_dashboard_v1 import OpsHandler
from ops.tools.research_hypotheses_clickthrough_qa_v1 import CdpClient, wait_for_target


ROOT = Path(__file__).resolve().parents[4]
PAGES = ROOT / "constellation_2/phaseL/ui/static/operator_shell/pages/index.js"
SERVER = ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"



def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_http_ok(url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if 200 <= response.status < 300:
                    return
        except Exception as exc:
            last_error = exc
        time.sleep(0.2)
    raise AssertionError(f"Timed out waiting for {url}: {last_error}")

def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _sample_payload() -> dict:
    return {
        "day_utc": "2026-05-22",
        "displayed_artifact_day": "2026-05-22",
        "generated_at_utc": "2026-05-22T22:00:00Z",
        "operator_today_projection": {
            "platform_capture_capability": "READY",
            "capture_ticket_count": 1,
            "capture_ticket_status": "TICKET_READY",
            "candidate_funnel_projection": {
                "trend_5d": [
                    {"day_utc": "2026-05-21", "excluded_uncovered_symbol_count": 6},
                    {"day_utc": "2026-05-22", "excluded_uncovered_symbol_count": 3},
                ],
                "dynamic_certification_queue": {
                    "artifact_path": "/truth/reports/dynamic_certification_queue_v1/2026-05-22/dynamic_certification_queue.v1.json",
                    "artifact_content_hash": "abc123",
                    "certification_status": "CERTIFIED",
                    "requested_symbols": ["CRWD"],
                    "certification_results": [{"symbol": "CRWD", "status": "CERTIFIED"}],
                },
            },
        },
        "paper_trade_evaluation_projection_v1": {
            "open_trade_count": 1,
            "unrealized_pnl": -8.30,
        },
        "exit_review_projection_v1": {
            "content_hash": "exit-hash",
            "open_positions": [
                {
                    "symbol": "DOW",
                    "position_id": "ticket_02200087bcdf9839aacb8958",
                    "exit_decision": "HOLD",
                    "unrealized_pnl": -8.30,
                    "decision_reason": "No exit trigger is active.",
                    "next_operator_guidance": "No manual exit action needed; continue monitoring.",
                    "required_evidence": [],
                }
            ],
        },
        "repair_center_projection_v1": {
            "content_hash": "repair-hash",
            "summary": {"blocked_external_requirements": 1, "total_open": 1},
            "summary_banner": {"message": "Core system operational. 4 domains certified. 3 external source requirements remain. 1 sleeve blocked by macro-calendar source."},
            "repair_items": [
                {
                    "repair_id": "repair:macro",
                    "domain_id": "MACRO_CALENDAR",
                    "status": "BLOCKED_WITH_EXACT_EXTERNAL_REQUIREMENT",
                    "repair_mode": "SOURCE_SETUP_REQUIRED",
                    "plain_english_problem": "Macro calendar source missing.",
                    "affected_sleeves": ["C2_EVENT_DISLOCATION_V1"],
                    "next_required_step": "Configure AEGIS_MACRO_CALENDAR_SOURCE_FILE.",
                }
            ],
        },
    }


def test_attention_queue_projection_orders_operator_items() -> None:
    projection = build_attention_queue_projection_v1(_sample_payload(), day_utc="2026-05-22")
    titles = [item["title"] for item in projection["items"]]

    assert projection["schema_id"] == "attention_queue_projection"
    assert titles[0] == "1 IB capture ticket awaiting review"
    assert any("DOW exit review: HOLD, P&L -$8.30" == title for title in titles)
    assert any("Macro Calendar source missing" == title for title in titles)
    assert any("Dynamic certification added CRWD" == title for title in titles)
    assert [item["priority"] for item in projection["items"]] == ["ACTION_REQUIRED", "REVIEW", "WARNING", "INFO"]
    assert projection["key_numbers"]["capture_tickets"] == 1
    assert projection["key_numbers"]["open_trades"] == 1
    assert projection["key_numbers"]["unrealized_pnl"] == -8.3
    assert projection["broker_submit_transmit_allowed"] is False
    assert projection["autonomous_execution_allowed"] is False
    assert projection["trade_advice_allowed"] is False


def test_attention_queue_empty_state_is_clean() -> None:
    payload = {
        "operator_today_projection": {"capture_ticket_count": 0, "capture_ticket_status": "NONE_AVAILABLE"},
        "paper_trade_evaluation_projection_v1": {"open_trade_count": 0, "unrealized_pnl": 0},
        "exit_review_projection_v1": {"open_positions": []},
        "repair_center_projection_v1": {"summary": {}, "summary_banner": {"message": "Core system operational."}, "repair_items": []},
    }
    projection = build_attention_queue_projection_v1(payload, day_utc="2026-05-22")

    assert projection["items"] == []
    assert projection["empty_state_message"] == "No operator action required."
    assert projection["key_numbers"]["capture_tickets"] == 0


def test_dashboard_uses_attention_projection_without_raw_artifact_paths_inline() -> None:
    pages = _text(PAGES)
    dashboard_block = pages.split("function renderAegisTodayWorkflow", 1)[1].split("function dashboardCaptureProjection", 1)[0]

    assert "renderDashboardAttentionQueueV1" in dashboard_block
    assert "renderDashboardKeyNumbersV1" in dashboard_block
    assert "renderDashboardRecentImportantEventsV1" in dashboard_block
    assert "renderDashboardSystemStatus" not in dashboard_block
    assert "renderDashboardEvidenceDrawer" not in dashboard_block
    assert "Source artifact path" not in dashboard_block
    assert "/home/node/constellation_runtime_data" not in dashboard_block



def test_dashboard_attention_queue_is_top_three_grouped_and_shell_routed() -> None:
    pages = _text(PAGES)
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")
    block = pages[pages.index("function renderDashboardAttentionQueueV1"):pages.index("function renderDashboardKeyNumbersV1")]

    assert "dashboardGroupedAttentionItems" in pages
    assert "external sources missing" in pages and "repairSources.length" in pages
    assert "visible = items.slice(0, 3)" in block
    assert "View all attention items" in block
    assert 'data-aegis-command-id="OPEN_VALID_ROUTE"' in pages
    assert 'data-route="${escapeHtml(route)}"' in pages
    assert '<a class="primary-button" href="${escapeHtml(item.target_route' not in pages
    assert "grid-template-columns: repeat(3, minmax(0, 1fr));" in css


def test_dashboard_status_block_is_compact_with_runtime_details() -> None:
    pages = _text(PAGES)
    css = (ROOT / "constellation_2/phaseL/ui/static/aegis.css").read_text(encoding="utf-8")
    workflow_block = pages[pages.index("function renderAegisTodayWorkflow"):pages.index("function dashboardCaptureProjection")]

    assert "Latest Run Summary" in pages
    assert "Latest session:" not in pages
    assert "View runtime details" in pages
    assert "dashboard-runtime-details" in pages
    assert "renderDashboardLatestRunSummary(payload)" in workflow_block
    assert workflow_block.index("renderDashboardLatestRunSummary(payload)") < workflow_block.index("renderDashboardAttentionQueueV1(payload)")
    assert ".current-day-status-banner.compact" in css


def test_direct_aegis_routes_are_shell_fallbacks() -> None:
    server = _text(SERVER)
    required_routes = [
        "/aegis-exit-review",
        "/aegis-performance",
        "/aegis-repair-center",
        "/aegis-candidate-funnel",
        "/aegis-captured-trades",
        "/aegis-runtime-timeline",
    ]
    for route in required_routes:
        assert route in OpsHandler.SHELL_ROUTES
    assert "elif normalized in self.SHELL_ROUTES:" in server
    assert 'rel = "index.html"' in server


def test_attention_queue_routes_are_registered() -> None:
    pages = _text(PAGES)
    server = _text(SERVER)

    assert "/api/aegis/attention-queue/latest" in server
    assert "/api/aegis/attention-queue" in server
    for route in ["/aegis-captured-trades", "/aegis-exit-review", "/aegis-repair-center", "/aegis-candidate-funnel", "/aegis-runtime-timeline"]:
        assert route in pages or route in OpsHandler.SHELL_ROUTES



def test_dashboard_suppresses_legacy_dow_exit_review_from_primary_queue() -> None:
    pages = _text(PAGES)
    assert "dashboardLegacyAttentionSuppressed" in pages
    assert "/\\bDOW\\b/i" in pages
    assert "exit review" in pages


def test_dashboard_open_workspace_uses_shell_navigation_without_404() -> None:
    chromium = shutil.which("chromium") or shutil.which("chromium-browser")
    if not chromium:
        return
    server_port = _free_port()
    cdp_port = _free_port()
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
    profile = tempfile.TemporaryDirectory()
    browser = None
    cdp = None
    try:
        _wait_http_ok(f"http://127.0.0.1:{server_port}/healthz")
        for route in ["/aegis-exit-review", "/aegis-performance", "/aegis-repair-center", "/aegis-candidate-funnel", "/aegis-captured-trades", "/aegis-runtime-timeline"]:
            with urllib.request.urlopen(f"http://127.0.0.1:{server_port}{route}", timeout=5) as response:
                body = response.read().decode("utf-8", errors="replace")
                assert response.status == 200
                assert "/app.js" in body or "operator_shell/main.js" in body
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
        cdp.command("Page.navigate", {"url": f"http://127.0.0.1:{server_port}/aegis-opportunities?cache_bust=attention_nav"}, timeout=20)
        ready_expr = "Boolean(document.querySelector('[data-attention-id] [data-aegis-command-id=\"OPEN_VALID_ROUTE\"]'))"
        for _ in range(80):
            time.sleep(0.25)
            ready = cdp.command("Runtime.evaluate", {"expression": ready_expr, "returnByValue": True}, timeout=10).get("result", {}).get("value")
            if ready:
                break
        assert ready is True
        click_expr = r'''
(() => {
  const items = [...document.querySelectorAll('[data-attention-id]')];
  const dow = items.find((node) => (node.innerText || '').includes('DOW exit review'));
  const item = dow || items[0];
  const button = item?.querySelector('[data-aegis-command-id="OPEN_VALID_ROUTE"]');
  if (!button) return { clicked: false, itemText: item?.innerText || '' };
  const before = window.location.pathname;
  button.click();
  return { clicked: true, before, route: button.getAttribute('data-route'), itemText: item.innerText };
})()
'''
        clicked = cdp.command("Runtime.evaluate", {"expression": click_expr, "returnByValue": True}, timeout=20).get("result", {}).get("value")
        assert clicked["clicked"] is True
        result = {}
        check_expr = r'''
(() => {
  const text = document.body.innerText || '';
  return {
    path: window.location.pathname,
    no404: !/Error response\s+Error code:\s*404|ENDPOINT_NOT_FOUND|File not found/i.test(text),
    hasWorkspace: text.includes('Exit Review') || text.includes('Performance') || text.includes('Repair Center') || text.includes('Candidate Funnel') || text.includes('Closed Trades') || text.includes('Runtime Timeline'),
  };
})()
'''
        for _ in range(60):
            time.sleep(0.25)
            result = cdp.command("Runtime.evaluate", {"expression": check_expr, "returnByValue": True}, timeout=10).get("result", {}).get("value") or {}
            if result.get("path") == clicked.get("route") and result.get("hasWorkspace"):
                break
        assert result["path"] == clicked["route"]
        assert result["no404"] is True
        assert result["hasWorkspace"] is True
    finally:
        if cdp is not None:
            cdp.close()
        if browser is not None:
            browser.terminate()
            try:
                browser.wait(timeout=5)
            except subprocess.TimeoutExpired:
                browser.kill()
        profile.cleanup()
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
