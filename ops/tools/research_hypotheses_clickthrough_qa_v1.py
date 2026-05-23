#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import struct
import subprocess
import time
import urllib.request
from pathlib import Path
from urllib.parse import urlparse


class CdpClient:
    def __init__(self, ws_url: str):
        parsed = urlparse(ws_url)
        path = parsed.path + (("?" + parsed.query) if parsed.query else "")
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock = socket.create_connection((parsed.hostname, parsed.port), timeout=10)
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {parsed.hostname}:{parsed.port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.sock.sendall(request.encode())
        response = self.sock.recv(4096)
        if b" 101 " not in response.split(b"\r\n", 1)[0]:
            raise RuntimeError(f"WebSocket upgrade failed: {response!r}")
        self.buffer = b""
        self.counter = 0

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass

    def _send_frame(self, payload: bytes) -> None:
        mask = os.urandom(4)
        header = bytearray([0x81])
        n = len(payload)
        if n < 126:
            header.append(0x80 | n)
        elif n < 65536:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", n))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", n))
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(bytes(header) + mask + masked)

    def _recv_frame(self, timeout: int = 20) -> dict:
        self.sock.settimeout(timeout)
        while True:
            if len(self.buffer) >= 2:
                b1, b2 = self.buffer[0], self.buffer[1]
                masked = bool(b2 & 0x80)
                length = b2 & 0x7F
                offset = 2
                if length == 126 and len(self.buffer) >= offset + 2:
                    length = struct.unpack("!H", self.buffer[offset:offset + 2])[0]
                    offset += 2
                elif length == 127 and len(self.buffer) >= offset + 8:
                    length = struct.unpack("!Q", self.buffer[offset:offset + 8])[0]
                    offset += 8
                elif length in {126, 127}:
                    length = -1
                mask = None
                if length >= 0 and masked and len(self.buffer) >= offset + 4:
                    mask = self.buffer[offset:offset + 4]
                    offset += 4
                if length >= 0 and (not masked or mask is not None) and len(self.buffer) >= offset + length:
                    payload = self.buffer[offset:offset + length]
                    self.buffer = self.buffer[offset + length:]
                    if mask:
                        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
                    opcode = b1 & 0x0F
                    if opcode == 0x8:
                        raise RuntimeError("WebSocket closed")
                    if opcode == 0x1:
                        return json.loads(payload.decode())
            chunk = self.sock.recv(65536)
            if not chunk:
                raise RuntimeError("Socket closed")
            self.buffer += chunk

    def command(self, method: str, params: dict | None = None, timeout: int = 20) -> dict:
        self.counter += 1
        msg = {"id": self.counter, "method": method}
        if params is not None:
            msg["params"] = params
        self._send_frame(json.dumps(msg).encode())
        while True:
            event = self._recv_frame(timeout=timeout)
            if event.get("id") == self.counter:
                if "error" in event:
                    raise RuntimeError(f"CDP error for {method}: {event['error']}")
                return event.get("result", {})


def wait_for_target(port: int, timeout: float = 10.0) -> str:
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            targets = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/json", timeout=1))
            for target in targets:
                if target.get("type") == "page":
                    return str(target["webSocketDebuggerUrl"])
        except Exception as exc:
            last_error = exc
        time.sleep(0.2)
    raise RuntimeError(f"No Chromium page target found: {last_error}")


CLICKTHROUGH_SCRIPT = r"""
(async () => {
  const clicked = [];
  const failures = [];
  const requests = [];
  const originalFetch = window.fetch;
  window.fetch = async (...args) => {
    const response = await originalFetch(...args);
    requests.push({ url: String(args[0]), status: response.status });
    return response;
  };
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const visible = (el) => {
    if (!el || el.disabled || el.hidden) return false;
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
  };
  const state = () => ({
    url: window.location.pathname + window.location.search,
    text: document.body.innerText || '',
    detailCount: document.querySelectorAll('[data-hypothesis-inline-detail]:not([hidden])').length,
    highlighted: document.querySelectorAll('.hypothesis-card-highlight, .hypothesis-section-highlight').length,
    openMenus: document.querySelectorAll('.hypothesis-more-menu[open]').length,
    openSections: document.querySelectorAll('.hypothesis-section-collapsed[open]').length,
    messages: document.querySelectorAll('[data-hypothesis-card-message-output]:not([hidden])').length,
    focusToken: document.body.dataset.hypothesisLastFocusToken || '',
  });
  const badState = (after) => after.url.includes('404') || after.text.includes('404') || after.text.includes('File not found') || after.text.includes('This site can’t be reached');
  async function clickAndAssert(label, el, expectChange = true) {
    if (!visible(el)) return;
    const before = state();
    el.scrollIntoView({ behavior: 'instant', block: 'center' });
    await sleep(80);
    el.click();
    await sleep(850);
    const after = state();
    clicked.push({ label, before: before.url, after: after.url });
    if (!after.url.startsWith('/research-lab')) failures.push(`${label}: left research-lab: ${after.url}`);
    if (badState(after)) failures.push(`${label}: produced browser/404 state`);
    const changed = before.url !== after.url || before.detailCount !== after.detailCount || before.highlighted !== after.highlighted || before.openMenus !== after.openMenus || before.openSections !== after.openSections || before.messages !== after.messages || before.focusToken !== after.focusToken || before.text !== after.text;
    if (expectChange && !changed) failures.push(`${label}: no visible UI change`);
  }

  for (let i = 0; i < 40 && !document.querySelector('[data-hypotheses-workspace]'); i += 1) {
    await sleep(250);
  }
  const workspace = document.querySelector('[data-hypotheses-workspace]');
  if (!workspace) failures.push('Hypotheses workspace missing');
  await clickAndAssert('summary waiting section', document.querySelector('[data-hypotheses-summary-card="waiting"] [data-hypothesis-summary-section-target]'));
  await clickAndAssert('summary NVIDIA card', Array.from(document.querySelectorAll('[data-hypothesis-summary-card-target]')).find((el) => el.innerText.includes('NVIDIA')));
  const nvidia = document.querySelector('[data-hypothesis-id="rh-nvidia-earnings-event-dislocation-v1"]');
  await clickAndAssert('NVIDIA primary action', nvidia?.querySelector('[data-hypothesis-detail-target], .hypothesis-primary-action'));
  await clickAndAssert('NVIDIA more menu', nvidia?.querySelector('.hypothesis-more-menu summary'));
  await clickAndAssert('NVIDIA more item', nvidia?.querySelector('.hypothesis-more-menu button, .hypothesis-more-menu a'));
  const firstMore = Array.from(document.querySelectorAll('.hypothesis-more-menu summary')).find(visible);
  if (!firstMore) failures.push('More menu missing from visible hypotheses');
  await clickAndAssert('first visible More menu', firstMore);
  await clickAndAssert('first visible More item', firstMore?.parentElement?.querySelector('button, a'));

  const visibleControls = Array.from(document.querySelectorAll('[data-hypotheses-workspace] button, [data-hypotheses-workspace] a, [data-hypotheses-workspace] summary'))
    .filter(visible)
    .slice(0, 36);
  let startClicked = false;
  for (const [index, el] of visibleControls.entries()) {
    const label = (el.innerText || el.getAttribute('aria-label') || el.href || `control-${index}`).trim();
    if (label === 'Start Research') {
      if (startClicked) continue;
      startClicked = true;
    }
    await clickAndAssert(`visible ${index}: ${label}`, el, true);
  }
  return { ok: failures.length === 0, failures, clicked, requests, finalUrl: window.location.pathname + window.location.search };
})()
"""


def run_clickthrough(url: str, chromium: str, port: int, keep_browser: bool = False) -> dict:
    profile = Path(f"/tmp/aegis-hypotheses-clickthrough-{os.getpid()}")
    profile.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen([
        chromium,
        "--headless=new",
        "--disable-gpu",
        "--no-sandbox",
        "--window-size=1600,900",
        "--remote-debugging-address=127.0.0.1",
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile}",
        url,
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    client: CdpClient | None = None
    try:
        client = CdpClient(wait_for_target(port))
        client.command("Runtime.enable")
        client.command("Page.enable")
        time.sleep(2.5)
        result = client.command("Runtime.evaluate", {"expression": CLICKTHROUGH_SCRIPT, "awaitPromise": True, "returnByValue": True}, timeout=90)
        return result.get("result", {}).get("value") or {}
    finally:
        if client:
            client.close()
        if not keep_browser:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


def main() -> int:
    ap = argparse.ArgumentParser(description="Click-through QA for /research-lab Hypotheses interactions using Chromium CDP.")
    ap.add_argument("--url", default="http://127.0.0.1:8790/research-lab")
    ap.add_argument("--chromium", default=os.environ.get("CHROMIUM_BIN", "/usr/bin/chromium-browser"))
    ap.add_argument("--port", type=int, default=9224)
    ap.add_argument("--keep-browser", action="store_true")
    args = ap.parse_args()
    result = run_clickthrough(args.url, args.chromium, args.port, args.keep_browser)
    print(json.dumps(result, indent=2))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
