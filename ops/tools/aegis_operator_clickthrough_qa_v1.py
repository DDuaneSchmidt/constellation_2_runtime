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
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.research_hypotheses_clickthrough_qa_v1 import CdpClient, wait_for_target

DEFAULT_ROUTES = [
    "/aegis-opportunities",
    "/research-lab",
    "/aegis-runtime-timeline",
    "/aegis-repair-center",
    "/aegis-candidates",
    "/aegis-candidate-funnel",
    "/aegis-exit-review",
    "/aegis-performance",
    "/aegis-journal",
    "/aegis-captured-trades",
]


def _free_debug_port_v1() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


SCRIPT = r"""
(async () => {
  const failures = [];
  const clicked = [];
  try { history.pushState({aegisQaGuard: true}, "", window.location.href); } catch (error) {}
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
    const before = snapshot();
    el.scrollIntoView({behavior: 'instant', block: 'center'});
    await sleep(50);
    const commandId = el.getAttribute('data-aegis-command-id');
    const actionType = el.getAttribute('data-aegis-command-action-type');
    const route = el.getAttribute('data-route') || el.getAttribute('href') || '';
    if ((el.tagName === 'A' && el.href) || (commandId && actionType === 'EXPAND_SECTION' && route)) {
      const href = new URL(el.href || route, window.location.href);
      if (href.origin === window.location.origin) {
        const response = await fetch(href.pathname + href.search, {method: 'GET'});
        clicked.push({label, before: before.path, after: href.pathname, link_status: response.status});
        if (response.status === 404) failures.push(`${label}: link route returned 404 ${href.pathname}`);
        continue;
      }
    }
    el.click();
    await sleep(1200);
    const after = snapshot();
    clicked.push({label, before: before.path, after: after.path});
    if (bad()) failures.push(`${label}: produced browser/404 state`);
    if (!after.path.startsWith('/')) failures.push(`${label}: invalid route ${after.path}`);
    const changed = before.path !== after.path || before.text !== after.text || before.panels !== after.panels || before.messages !== after.messages || before.active !== after.active || before.scrollY !== after.scrollY;
    if (commandId && !changed && after.panels === 0 && after.messages === 0) failures.push(`${label}: command click produced no visible response`);
  }
  return {ok: failures.length === 0, failures, clicked, clicked_count: clicked.length, path: window.location.pathname};
})()
"""


def run(routes: list[str], base_url: str, chromium: str) -> dict:
    profile = Path(f"/tmp/aegis-operator-clickthrough-{os.getpid()}")
    profile.mkdir(parents=True, exist_ok=True)
    debug_port = _free_debug_port_v1()
    browser = subprocess.Popen([chromium, "--headless=new", f"--remote-debugging-port={debug_port}", "--no-first-run", "--no-default-browser-check", "--disable-gpu", "--window-size=1600,900", f"--user-data-dir={profile}", "about:blank"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        cdp = CdpClient(wait_for_target(debug_port, timeout=15))
        cdp.command("Runtime.enable")
        cdp.command("Page.enable")
        results = []
        for route in routes:
            url = base_url.rstrip("/") + route
            cdp.command("Page.navigate", {"url": url})
            time.sleep(1.5)
            cdp.command("Emulation.setDeviceMetricsOverride", {"width": 1600, "height": 900, "deviceScaleFactor": 1, "mobile": False})
            value = cdp.command("Runtime.evaluate", {"expression": SCRIPT, "awaitPromise": True, "returnByValue": True}, timeout=60).get("result", {}).get("value")
            results.append({"route": route, **(value or {"ok": False, "failures": ["no script result"]})})
        failures = [f"{row['route']}: {failure}" for row in results for failure in row.get("failures", [])]
        return {"ok": not failures, "routes": routes, "results": results, "failures": failures}
    finally:
        browser.terminate()
        try:
            browser.wait(timeout=5)
        except subprocess.TimeoutExpired:
            browser.kill()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8790")
    parser.add_argument("--route", action="append", default=[])
    parser.add_argument("--chromium", default=os.environ.get("CHROMIUM_BIN") or shutil.which("chromium") or shutil.which("chromium-browser") or "/usr/bin/chromium-browser")
    args = parser.parse_args()
    result = run(args.route or DEFAULT_ROUTES, args.base_url, args.chromium)
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
