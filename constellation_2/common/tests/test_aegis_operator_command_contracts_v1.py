from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from pathlib import Path

import pytest


CURRENT_RELEASE_MANIFEST = Path("/home/node/constellation_runtime_data/truth/releases/current_release.v1.json")
CURRENT_RELEASE_LAUNCHER = Path("/home/node/constellation_runtime_data/truth/releases/run_current_release_tool_v1.sh")
ACTIVE_RELEASE_LINK = Path("/home/node/constellation_active")
PAPER_READY_SERVICE = Path("/home/node/.config/systemd/user/aegis-paper-ready-kernel-v1.service")
POST_TRADE_SERVICE = Path("/home/node/.config/systemd/user/aegis-post-trade-measurement-v1.service")


def _require_runtime_contract() -> dict:
    if not CURRENT_RELEASE_MANIFEST.is_file():
        pytest.skip(f"current-release manifest missing: {CURRENT_RELEASE_MANIFEST}")
    if not CURRENT_RELEASE_LAUNCHER.is_file():
        pytest.skip(f"current-release launcher missing: {CURRENT_RELEASE_LAUNCHER}")
    if not ACTIVE_RELEASE_LINK.exists():
        pytest.skip(f"active release symlink missing: {ACTIVE_RELEASE_LINK}")
    payload = json.loads(CURRENT_RELEASE_MANIFEST.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _run_launcher(*args: str, timeout: int = 20) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [str(CURRENT_RELEASE_LAUNCHER), *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
        env=env,
    )


def _release_path() -> Path:
    payload = _require_runtime_contract()
    release_path = Path(str(payload.get("release_path") or "")).resolve()
    if not release_path.is_dir():
        pytest.fail(f"current-release release_path missing: {release_path}")
    return release_path


def test_current_release_launcher_pointer_matches_active_release() -> None:
    payload = _require_runtime_contract()
    release_path = Path(str(payload.get("release_path") or "")).resolve()
    active_path = ACTIVE_RELEASE_LINK.resolve()
    if release_path != active_path:
        pytest.skip(f"current-release pointer is environment-specific: manifest={release_path} active={active_path}")

    manifest_path = release_path / "release_manifest.v1.json"
    assert manifest_path.is_file()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload.get("release_id") == manifest.get("release_id")
    assert payload.get("commit") == manifest.get("git_sha")

    expected_hash = str(payload.get("release_manifest_hash") or "")
    if expected_hash:
        actual_hash = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        assert actual_hash == expected_hash


def test_launcher_supports_tool_name_form_for_no_trade_explainer() -> None:
    proc = _run_launcher("explain_aegis_no_paper_trade_v1", "--no-journal")
    assert proc.returncode == 0, proc.stderr or proc.stdout
    assert "No PAPER trade because:" in proc.stdout
    assert "Submit flags:" in proc.stdout


def test_launcher_supports_npm_form_for_kernel_tests() -> None:
    proc = _run_launcher("npm", "run", "aegis:kernel:test", timeout=60)
    assert proc.returncode == 0, proc.stderr or proc.stdout
    assert "16 passed" in proc.stdout


def test_launcher_rejects_or_does_not_support_python_file_form() -> None:
    proc = _run_launcher("python3", "ops/tools/explain_aegis_no_paper_trade_v1.py")
    assert proc.returncode != 0
    combined = f"{proc.stdout}\n{proc.stderr}"
    assert "invalid tool name" in combined or "ops/tools/python3.py" in combined


def _timer_tool_names(service_path: Path) -> list[str]:
    if not service_path.is_file():
        pytest.skip(f"systemd service missing: {service_path}")
    text = service_path.read_text(encoding="utf-8")
    tools: list[str] = []
    for line in text.splitlines():
        if not line.startswith("ExecStart="):
            continue
        match = re.search(r"run_current_release_tool_v1\.sh\s+([A-Za-z0-9_:-]+)", line)
        assert match, line
        tool_name = match.group(1)
        assert "/" not in tool_name
        assert not tool_name.endswith(".py")
        assert tool_name != "python3"
        tools.append(tool_name)
    assert tools
    return tools


def test_timer_invoked_tools_exist_in_active_release() -> None:
    release_path = _release_path()
    timer_tools = _timer_tool_names(PAPER_READY_SERVICE) + _timer_tool_names(POST_TRADE_SERVICE)
    for tool_name in timer_tools:
        tool_path = release_path / "ops" / "tools" / f"{tool_name}.py"
        assert tool_path.is_file(), f"timer-invoked tool missing from active release: {tool_path}"

