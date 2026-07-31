#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.operational_soak_report_v1 import (
    SOAK_ROUTES,
    build_aegis_operational_soak_report_v1,
    write_aegis_operational_soak_report_v1,
)

DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")


def _free_debug_port_v1() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


SAFE_UI_SOAK_SCRIPT = r"""
(async () => {
  const failures = [];
  const clicked = [];
  const skippedApiCommands = [];
  try { history.pushState({aegisSoakGuard: true}, "", window.location.href); } catch (error) {}
  document.addEventListener('submit', (event) => { event.preventDefault(); event.stopImmediatePropagation(); }, true);
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const visible = (el) => {
    if (!el || el.disabled || el.hidden) return false;
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
  };
  const bad = () => /Error response\s+Error code:\s*404|ENDPOINT_NOT_FOUND|File not found|This site can.t be reached/i.test(document.body.innerText || '') || window.location.pathname.includes('404');
  const snapshot = () => ({
    path: window.location.pathname,
    text: document.body.innerText || '',
    panels: document.querySelectorAll('[data-command-result-panel], [data-hypothesis-inline-detail]:not([hidden]), dialog[open], details[open], .is-highlighted, .hypothesis-card-highlight, .hypothesis-section-highlight').length,
    messages: document.querySelectorAll('[data-command-result-panel], [data-hypothesis-card-message-output]:not([hidden]), .callout, [role="alert"]').length,
    active: document.activeElement?.outerHTML?.slice(0, 120) || '',
    scrollY: Math.round(window.scrollY || 0)
  });
  const workspaceRoot = document.getElementById('workspaceContent') || document;
  const getControls = () => Array.from(workspaceRoot.querySelectorAll('button, a[href], summary, [data-aegis-command-id]')).filter(visible);
  for (let wait = 0; wait < 30; wait += 1) {
    const text = workspaceRoot.innerText || '';
    if (getControls().length > 0 || (!/Loading/.test(text) && text.trim().length > 0)) break;
    await sleep(250);
  }
  const initialCount = getControls().length;
  for (let i = 0; i < initialCount; i += 1) {
    const el = getControls()[i];
    if (!el) continue;
    const label = (el.innerText || el.getAttribute('aria-label') || el.getAttribute('data-aegis-command-id') || el.href || `control-${i}`).trim().slice(0, 120);
    const commandId = el.getAttribute('data-aegis-command-id') || '';
    const actionType = el.getAttribute('data-aegis-command-action-type') || '';
    if (actionType === 'API_COMMAND') {
      skippedApiCommands.push({label, command_id: commandId, action_type: actionType});
      continue;
    }
    const before = snapshot();
    el.scrollIntoView({behavior: 'instant', block: 'center'});
    await sleep(50);
    const route = el.getAttribute('data-route') || el.getAttribute('href') || '';
    if ((el.tagName === 'A' && el.href) || (commandId && actionType === 'EXPAND_SECTION' && route)) {
      const href = new URL(el.href || route, window.location.href);
      if (href.origin === window.location.origin) {
        const response = await fetch(href.pathname + href.search, {method: 'GET'});
        clicked.push({label, command_id: commandId, action_type: actionType, before: before.path, after: href.pathname, link_status: response.status});
        if (response.status === 404) failures.push(`${label}: link route returned 404 ${href.pathname}`);
        continue;
      }
    }
    el.click();
    await sleep(750);
    const after = snapshot();
    clicked.push({label, command_id: commandId, action_type: actionType, before: before.path, after: after.path});
    if (bad()) failures.push(`${label}: produced browser/404 state`);
    if (!after.path.startsWith('/')) failures.push(`${label}: invalid route ${after.path}`);
    const changed = before.path !== after.path || before.text !== after.text || before.panels !== after.panels || before.messages !== after.messages || before.active !== after.active || before.scrollY !== after.scrollY;
    if (commandId && !changed && after.panels === 0 && after.messages === 0) failures.push(`${label}: command click produced no visible response`);
  }
  return {ok: failures.length === 0, failures, clicked, skipped_api_commands: skippedApiCommands, skipped_api_command_count: skippedApiCommands.length, clicked_count: clicked.length, path: window.location.pathname};
})()
"""


def _run_ui_clickthrough(base_url: str, chromium: str) -> dict[str, Any]:
    from ops.tools.research_hypotheses_clickthrough_qa_v1 import CdpClient, wait_for_target

    profile = Path(f"/tmp/aegis-operational-soak-clickthrough-{os.getpid()}")
    profile.mkdir(parents=True, exist_ok=True)
    debug_port = _free_debug_port_v1()
    browser = subprocess.Popen(
        [
            chromium,
            "--headless=new",
            f"--remote-debugging-port={debug_port}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-gpu",
            "--window-size=1600,900",
            f"--user-data-dir={profile}",
            "about:blank",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        cdp = CdpClient(wait_for_target(debug_port, timeout=15))
        cdp.command("Runtime.enable")
        cdp.command("Page.enable")
        results = []
        for route in SOAK_ROUTES:
            url = base_url.rstrip("/") + route
            cdp.command("Page.navigate", {"url": url})
            time.sleep(1.5)
            cdp.command("Emulation.setDeviceMetricsOverride", {"width": 1600, "height": 900, "deviceScaleFactor": 1, "mobile": False})
            value = cdp.command("Runtime.evaluate", {"expression": SAFE_UI_SOAK_SCRIPT, "awaitPromise": True, "returnByValue": True}, timeout=60).get("result", {}).get("value")
            results.append({"route": route, **(value or {"ok": False, "failures": ["no script result"]})})
        failures = [f"{row['route']}: {failure}" for row in results for failure in row.get("failures", [])]
        return {"ok": not failures, "routes": list(SOAK_ROUTES), "results": results, "failures": failures}
    finally:
        browser.terminate()
        try:
            browser.wait(timeout=5)
        except subprocess.TimeoutExpired:
            browser.kill()


def _latest_day(truth_root: Path) -> str:
    reports_root = truth_root / "reports"
    days: set[str] = set()
    today = datetime.now(UTC).date().isoformat()
    ignored_families = {"aegis_operational_soak_report_v1"}
    if reports_root.exists():
        for family in reports_root.iterdir():
            if family.is_dir() and family.name not in ignored_families:
                days.update(
                    path.name
                    for path in family.iterdir()
                    if path.is_dir() and len(path.name) == 10 and path.name <= today
                )
    return sorted(days)[-1] if days else datetime.now(UTC).date().isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the Aegis operational soak acceptance report from existing artifacts.")
    parser.add_argument("--truth-root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default="", help="Operational day YYYY-MM-DD. Defaults to latest report day.")
    parser.add_argument("--lookback-days", type=int, default=5)
    parser.add_argument("--run-ui-clickthrough", action="store_true")
    parser.add_argument("--ui-base-url", default="http://127.0.0.1:8787")
    parser.add_argument("--chromium", default=os.environ.get("CHROMIUM_BIN") or shutil.which("chromium") or shutil.which("chromium-browser") or "/usr/bin/chromium-browser")
    parser.add_argument("--json", action="store_true", help="Print the full report JSON instead of a short summary.")
    args = parser.parse_args()

    truth_root = Path(args.truth_root).expanduser().resolve()
    operational_day = args.day or _latest_day(truth_root)
    ui_results = _run_ui_clickthrough(args.ui_base_url, args.chromium) if args.run_ui_clickthrough else None

    report = build_aegis_operational_soak_report_v1(
        truth_root=truth_root,
        operational_day=operational_day,
        lookback_days=args.lookback_days,
        ui_results=ui_results,
    )
    paths = write_aegis_operational_soak_report_v1(truth_root=truth_root, operational_day=operational_day, payload=report)

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(json.dumps({
            "ok": report.get("overall_operational_health") != "FAIL",
            "operational_day": operational_day,
            "overall_operational_health": report.get("overall_operational_health"),
            "replay_status": report.get("replay_status"),
            "provider_status": report.get("provider_status"),
            "timeout_events": report.get("timeout_events"),
            "ui_status": report.get("ui_workflow_soak", {}).get("status"),
            "content_hash": report.get("content_hash"),
            "path": paths.get("aegis_operational_soak_report"),
        }, sort_keys=True))
    return 2 if report.get("overall_operational_health") == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())
