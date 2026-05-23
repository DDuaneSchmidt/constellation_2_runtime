#!/usr/bin/env python3
"""Deterministic live UI validation for the Aegis Research inventory.

This is intentionally browser- and OCR-based. It treats the screenshot as the
operator-facing truth and fails closed when the expected text is not visible to
Tesseract after the real search input is typed.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_URL = "http://127.0.0.1:3917/research-lab"
DEFAULT_SEARCH_TERM = "NVDA"
DEFAULT_EXPECTED_TEXT = "NVIDIA Earnings Event Dislocation"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "artifacts" / "live_ui_validation"
DEFAULT_SERVER = REPO_ROOT / "constellation_2" / "phaseL" / "ui" / "server" / "run_ops_dashboard_v1.py"


class WebSocketClient:
    def __init__(self, url: str) -> None:
        if not url.startswith("ws://"):
            raise ValueError(f"unsupported websocket url: {url}")
        rest = url[5:]
        hostport, path = rest.split("/", 1)
        host, port = hostport.split(":", 1)
        self.sock = socket.create_connection((host, int(port)), timeout=15)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            f"GET /{path} HTTP/1.1\r\n"
            f"Host: {hostport}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.sock.sendall(request.encode("ascii"))
        response = b""
        while b"\r\n\r\n" not in response:
            response += self.sock.recv(4096)
        if b" 101 " not in response.split(b"\r\n", 1)[0]:
            raise RuntimeError(f"websocket upgrade failed: {response[:200]!r}")
        self.next_id = 1

    def _send_frame(self, payload: bytes) -> None:
        header = bytearray([0x81])
        size = len(payload)
        if size < 126:
            header.append(0x80 | size)
        elif size < 65536:
            header.append(0x80 | 126)
            header += struct.pack("!H", size)
        else:
            header.append(0x80 | 127)
            header += struct.pack("!Q", size)
        mask = os.urandom(4)
        header += mask
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self.sock.sendall(header + masked)

    def send(self, message: dict[str, Any]) -> None:
        self._send_frame(json.dumps(message, separators=(",", ":")).encode("utf-8"))

    def _read_exact(self, size: int) -> bytes:
        chunks = b""
        while len(chunks) < size:
            chunk = self.sock.recv(size - len(chunks))
            if not chunk:
                raise EOFError("websocket closed")
            chunks += chunk
        return chunks

    def recv(self) -> dict[str, Any]:
        head = self._read_exact(2)
        opcode = head[0] & 0x0F
        size = head[1] & 0x7F
        masked = bool(head[1] & 0x80)
        if size == 126:
            size = struct.unpack("!H", self._read_exact(2))[0]
        elif size == 127:
            size = struct.unpack("!Q", self._read_exact(8))[0]
        mask = self._read_exact(4) if masked else b""
        payload = self._read_exact(size)
        if masked:
            payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        if opcode == 8:
            raise EOFError("websocket closed")
        if opcode not in {1, 2}:
            return self.recv()
        return json.loads(payload.decode("utf-8"))

    def call(self, method: str, params: dict[str, Any] | None = None, timeout: float = 30.0) -> dict[str, Any]:
        message_id = self.next_id
        self.next_id += 1
        self.send({"id": message_id, "method": method, "params": params or {}})
        deadline = time.time() + timeout
        while time.time() < deadline:
            message = self.recv()
            if message.get("id") == message_id:
                if "error" in message:
                    raise RuntimeError(f"{method}: {message['error']}")
                return dict(message.get("result") or {})
        raise TimeoutError(method)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _http_ok(url: str, timeout: float = 2.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return 200 <= int(response.status) < 500
    except Exception:
        return False


def _wait_for_url(url: str, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _http_ok(url, timeout=1.5):
            return True
        time.sleep(0.25)
    return False


def _start_server_if_needed(url: str, *, no_start_server: bool) -> subprocess.Popen[str] | None:
    if _http_ok(url):
        return None
    if no_start_server:
        raise RuntimeError(f"live route is unavailable and --no-start-server was set: {url}")
    parsed = urllib.parse.urlparse(url)
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise RuntimeError(f"refusing to start local server for non-local URL: {url}")
    port = parsed.port or 80
    proc = subprocess.Popen(
        [sys.executable, str(DEFAULT_SERVER), "--host", "127.0.0.1", "--port", str(port)],
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if not _wait_for_url(url, timeout=25):
        output = ""
        if proc.stdout:
            try:
                output = proc.stdout.read(2000)
            except Exception:
                output = ""
        proc.terminate()
        raise RuntimeError(f"server did not become ready for {url}; output={output}")
    return proc


def _chrome_target(debug_port: int) -> dict[str, Any]:
    deadline = time.time() + 15
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{debug_port}/json/list", timeout=1) as response:
                targets = json.loads(response.read().decode("utf-8"))
            for target in targets:
                if target.get("type") == "page":
                    return dict(target)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(0.2)
    raise RuntimeError(f"chrome target not available: {last_error}")


def _runtime_value(ws: WebSocketClient, expression: str, *, timeout: float = 30.0) -> Any:
    result = ws.call(
        "Runtime.evaluate",
        {"expression": expression, "awaitPromise": True, "returnByValue": True},
        timeout=timeout,
    )
    value = (result.get("result") or {}).get("value")
    return value


def _wait_for_stable_inventory(ws: WebSocketClient, *, timeout: float = 30.0) -> dict[str, Any]:
    deadline = time.time() + timeout
    previous: tuple[int, int, str] | None = None
    stable_samples = 0
    latest: dict[str, Any] = {}
    expression = """
(() => {
  const rows = Array.from(document.querySelectorAll('[data-research-inventory-row]'));
  const input = document.querySelector('[data-research-hypothesis-search]');
  const count = document.querySelector('[data-research-hypothesis-search-count]');
  return {
    rowCount: rows.length,
    visibleCount: rows.filter((row) => !row.hidden).length,
    inputPresent: Boolean(input),
    countText: count ? count.innerText : '',
    ready: Boolean(input) && rows.length > 0,
  };
})()
"""
    while time.time() < deadline:
        latest = dict(_runtime_value(ws, expression) or {})
        sample = (int(latest.get("rowCount") or 0), int(latest.get("visibleCount") or 0), str(latest.get("countText") or ""))
        if latest.get("ready") and sample == previous:
            stable_samples += 1
            if stable_samples >= 3:
                return latest
        else:
            stable_samples = 0
            previous = sample
        time.sleep(0.25)
    raise RuntimeError(f"inventory did not stabilize: {latest}")


def _ocr(path: Path) -> str:
    tesseract = shutil.which("tesseract")
    if not tesseract:
        raise RuntimeError("tesseract executable is required for live UI validation")
    completed = subprocess.run(
        [tesseract, str(path), "stdout", "--psm", "6"],
        check=False,
        capture_output=True,
        text=True,
    )
    output = (completed.stdout or "") + ("\n" + completed.stderr if completed.stderr else "")
    if completed.returncode != 0:
        raise RuntimeError(f"tesseract failed rc={completed.returncode}: {output}")
    return output


def _normalize_ocr(text: str) -> str:
    return " ".join(text.split()).casefold()


def run_validation(args: argparse.Namespace) -> dict[str, Any]:
    chromium = shutil.which(args.browser)
    if not chromium:
        raise RuntimeError(f"browser executable not found: {args.browser}")
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    server_proc = _start_server_if_needed(args.url, no_start_server=args.no_start_server)
    user_data = Path(tempfile.mkdtemp(prefix="aegis-live-ui-profile-"))
    chrome_proc: subprocess.Popen[str] | None = None
    try:
        chrome_proc = subprocess.Popen(
            [
                chromium,
                "--headless",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-application-cache",
                f"--remote-debugging-port={args.debug_port}",
                f"--user-data-dir={user_data}",
                f"--window-size={args.viewport}",
                "about:blank",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        target = _chrome_target(args.debug_port)
        ws = WebSocketClient(str(target["webSocketDebuggerUrl"]))
        for method in ["Page.enable", "Runtime.enable", "Network.enable"]:
            ws.call(method)
        ws.call("Network.setCacheDisabled", {"cacheDisabled": True})
        ws.call("Page.navigate", {"url": "about:blank"})
        time.sleep(0.5)
        ws.call("Page.navigate", {"url": args.url})
        _wait_for_stable_inventory(ws, timeout=args.timeout)
        baseline = _runtime_value(
            ws,
            """
(() => {
  const rows = Array.from(document.querySelectorAll('[data-research-inventory-row]'));
  return {
    rowCountBeforeFilter: rows.length,
    visibleCountBeforeFilter: rows.filter((row) => !row.hidden).length,
    countTextBeforeFilter: document.querySelector('[data-research-hypothesis-search-count]')?.innerText || '',
  };
})()
""",
        )
        if args.preselect_filter:
            _runtime_value(
                ws,
                f"""
(() => {{
  const button = document.querySelector('[data-research-quick-filter="{args.preselect_filter}"]');
  if (button) button.click();
  return Boolean(button);
}})()
""",
            )
            time.sleep(0.3)
        preselected = _runtime_value(
            ws,
            """
(() => {
  const rows = Array.from(document.querySelectorAll('[data-research-inventory-row]'));
  return {
    activeFilter: document.querySelector('[data-research-hypothesis-search-region]')?.dataset?.activeFilter || '',
    visibleCountAfterPreselect: rows.filter((row) => !row.hidden).length,
  };
})()
""",
        )
        _runtime_value(
            ws,
            """
(() => {
  const input = document.querySelector('[data-research-hypothesis-search]');
  if (!input) return false;
  input.focus();
  input.value = '';
  input.dispatchEvent(new Event('input', { bubbles: true }));
  return true;
})()
""",
        )
        ws.call("Input.insertText", {"text": args.search_term})
        time.sleep(0.5)
        after = _wait_for_stable_inventory(ws, timeout=args.timeout)
        dom_result = _runtime_value(
            ws,
            f"""
(() => {{
  const rows = Array.from(document.querySelectorAll('[data-research-inventory-row]'));
  const visible = rows.filter((row) => !row.hidden);
  const expected = {json.dumps(args.expected_text)};
  const expectedRow = rows.find((row) => (row.innerText || '').includes(expected));
  return {{
    exactDomQuery: '[data-research-inventory-row]',
    inputValue: document.querySelector('[data-research-hypothesis-search]')?.value || '',
    rowCountAfterSearch: rows.length,
    visibleCountAfterSearch: visible.length,
    countTextAfterSearch: document.querySelector('[data-research-hypothesis-search-count]')?.innerText || '',
    noResultsHidden: document.querySelector('[data-research-no-results]')?.hidden ?? null,
    activeFilterAfterSearch: document.querySelector('[data-research-hypothesis-search-region]')?.dataset?.activeFilter || '',
    expectedRowExists: Boolean(expectedRow),
    expectedRowHidden: expectedRow ? expectedRow.hidden : null,
    expectedRowSearchText: expectedRow ? String(expectedRow.getAttribute('data-research-search') || '') : '',
    visibleTitles: visible.map((row) => (row.querySelector('.research-inventory-title')?.innerText || '').trim()),
  }};
}})()
""",
        )
        rect = _runtime_value(
            ws,
            """
(() => {
  const panel = document.querySelector('.research-inventory-panel');
  const rect = panel ? panel.getBoundingClientRect() : document.body.getBoundingClientRect();
  return {x: Math.max(0, rect.x), y: Math.max(0, rect.y), width: Math.min(rect.width, window.innerWidth), height: Math.min(520, window.innerHeight - Math.max(0, rect.y))};
})()
""",
        )
        full_screenshot_path = output_dir / args.full_screenshot_name
        screenshot_path = output_dir / args.screenshot_name
        full_data = ws.call("Page.captureScreenshot", {"format": "png", "captureBeyondViewport": False}, timeout=60)["data"]
        full_screenshot_path.write_bytes(base64.b64decode(full_data))
        clip_data = ws.call(
            "Page.captureScreenshot",
            {
                "format": "png",
                "captureBeyondViewport": False,
                "clip": {
                    "x": float(rect.get("x") or 0),
                    "y": float(rect.get("y") or 0),
                    "width": max(1.0, float(rect.get("width") or 1)),
                    "height": max(1.0, float(rect.get("height") or 1)),
                    "scale": float(args.screenshot_scale),
                },
            },
            timeout=60,
        )["data"]
        screenshot_path.write_bytes(base64.b64decode(clip_data))
        ocr_output = _ocr(screenshot_path)
        ocr_path = output_dir / args.ocr_name
        ocr_path.write_text(ocr_output, encoding="utf-8")
        normalized_ocr = _normalize_ocr(ocr_output)
        expected_texts = [args.expected_text, *args.expected_ocr]
        missing = [text for text in expected_texts if text.casefold() not in normalized_ocr]
        ok = not missing
        result = {
            "ok": ok,
            "route": args.url,
            "search_term": args.search_term,
            "expected_text": args.expected_text,
            "expected_ocr": expected_texts,
            "missing_ocr_text": missing,
            "screenshot_path": str(screenshot_path),
            "screenshot_sha256": _sha256(screenshot_path),
            "full_screenshot_path": str(full_screenshot_path),
            "full_screenshot_sha256": _sha256(full_screenshot_path),
            "ocr_output_path": str(ocr_path),
            "ocr_output": ocr_output,
            "baseline": baseline,
            "preselected_filter": args.preselect_filter,
            "preselected": preselected,
            "after_stabilization": after,
            "dom_result": dom_result,
            "browser_profile_dir": str(user_data),
            "cache_disabled": True,
            "fresh_browser_profile": True,
            "server_started_by_harness": server_proc is not None,
        }
        result_path = output_dir / args.result_name
        result_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
        result["result_path"] = str(result_path)
        return result
    finally:
        if chrome_proc is not None:
            chrome_proc.terminate()
            try:
                chrome_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                chrome_proc.kill()
        shutil.rmtree(user_data, ignore_errors=True)
        if server_proc is not None:
            server_proc.terminate()
            try:
                server_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server_proc.kill()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate live Research UI by screenshot OCR.")
    parser.add_argument("--url", default=os.environ.get("AEGIS_LIVE_UI_ROUTE", DEFAULT_URL))
    parser.add_argument("--search-term", default=os.environ.get("AEGIS_LIVE_UI_SEARCH_TERM", DEFAULT_SEARCH_TERM))
    parser.add_argument("--expected-text", default=os.environ.get("AEGIS_LIVE_UI_EXPECTED_TEXT", DEFAULT_EXPECTED_TEXT))
    parser.add_argument("--expected-ocr", action="append", default=[])
    parser.add_argument("--preselect-filter", default="blocked", help="Quick filter to select before typing, proving search overrides stale filter state. Set empty string to disable.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--screenshot-name", default="research_ui_live_validation_v1.png")
    parser.add_argument("--full-screenshot-name", default="research_ui_live_validation_full_v1.png")
    parser.add_argument("--ocr-name", default="research_ui_live_validation_v1.ocr.txt")
    parser.add_argument("--result-name", default="research_ui_live_validation_v1.json")
    parser.add_argument("--browser", default=os.environ.get("CHROMIUM", "chromium"))
    parser.add_argument("--debug-port", type=int, default=int(os.environ.get("AEGIS_LIVE_UI_DEBUG_PORT", "9235")))
    parser.add_argument("--viewport", default="1920,1080")
    parser.add_argument("--screenshot-scale", type=float, default=2.0)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--no-start-server", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if not args.preselect_filter:
        args.preselect_filter = ""
    result = run_validation(args)
    print(json.dumps({
        "ok": result["ok"],
        "route": result["route"],
        "search_term": result["search_term"],
        "screenshot_path": result["screenshot_path"],
        "ocr_output_path": result["ocr_output_path"],
        "result_path": result["result_path"],
        "dom_result": result["dom_result"],
        "missing_ocr_text": result["missing_ocr_text"],
    }, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
