from __future__ import annotations

import os
import subprocess
from pathlib import Path


WRAPPER = Path("/home/node/constellation/ops/run/c2_paper_auto_repair_controller_run_v1.sh")
RUNTIME_ROOT = "/home/node/constellation_2_runtime"


def _fake_python(tmp_path: Path) -> Path:
    fake = tmp_path / "fake_python.sh"
    fake.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
{
  echo "PWD=$(pwd)"
  echo "PYTHONPATH=${PYTHONPATH:-}"
  i=0
  for arg in "$@"; do
    echo "ARG_${i}=${arg}"
    i=$((i + 1))
  done
} > "${C2_AUTO_REPAIR_CAPTURE:?}"
""",
        encoding="utf-8",
    )
    fake.chmod(0o755)
    return fake


def _run_wrapper(tmp_path: Path, extra_env: dict[str, str] | None = None) -> tuple[subprocess.CompletedProcess[str], dict[str, str]]:
    capture = tmp_path / "capture.txt"
    fake_tool = tmp_path / "fake_controller.py"
    fake_tool.write_text("# fake controller path; never executed by this test\n", encoding="utf-8")

    env = {
        "HOME": "/home/node",
        "USER": "node",
        "PATH": "/usr/bin:/bin",
        "C2_AUTO_REPAIR_PY": str(_fake_python(tmp_path)),
        "C2_AUTO_REPAIR_TOOL": str(fake_tool),
        "C2_AUTO_REPAIR_DAY_UTC": "2026-05-11",
        "C2_AUTO_REPAIR_CAPTURE": str(capture),
    }
    if extra_env:
        env.update(extra_env)

    result = subprocess.run(
        ["bash", str(WRAPPER)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    parsed: dict[str, str] = {}
    if capture.exists():
        for line in capture.read_text(encoding="utf-8").splitlines():
            key, _, value = line.partition("=")
            parsed[key] = value
    return result, parsed


def test_wrapper_defaults_to_dry_run_and_sets_pythonpath(tmp_path: Path) -> None:
    result, captured = _run_wrapper(tmp_path)

    assert result.returncode == 0, result.stderr
    assert captured["PWD"] == RUNTIME_ROOT
    assert captured["PYTHONPATH"].split(":")[0] == RUNTIME_ROOT
    assert captured["ARG_0"].endswith("fake_controller.py")
    assert captured["ARG_1"] == "--day_utc"
    assert captured["ARG_2"] == "2026-05-11"
    assert captured["ARG_3"] == "--launch_mode"
    assert captured["ARG_4"] == "DRY_RUN"
    assert "effective_launch_mode=DRY_RUN" in result.stderr


def test_wrapper_forces_dry_run_when_live_requested_without_allow_flag(tmp_path: Path) -> None:
    result, captured = _run_wrapper(tmp_path, {"C2_AUTO_REPAIR_LAUNCH_MODE": "LIVE"})

    assert result.returncode == 0, result.stderr
    assert captured["ARG_4"] == "DRY_RUN"
    assert "requested_launch_mode=LIVE" in result.stderr
    assert "effective_launch_mode=DRY_RUN" in result.stderr
    assert "live_allowed=0" in result.stderr


def test_wrapper_allows_live_only_with_explicit_allow_flag(tmp_path: Path) -> None:
    result, captured = _run_wrapper(
        tmp_path,
        {
            "C2_AUTO_REPAIR_LAUNCH_MODE": "LIVE",
            "C2_AUTO_REPAIR_ALLOW_LIVE": "1",
        },
    )

    assert result.returncode == 0, result.stderr
    assert captured["ARG_4"] == "LIVE"
    assert "requested_launch_mode=LIVE" in result.stderr
    assert "effective_launch_mode=LIVE" in result.stderr
    assert "live_allowed=1" in result.stderr


def test_wrapper_does_not_call_real_repair_controller_in_validation(tmp_path: Path) -> None:
    result, captured = _run_wrapper(tmp_path)

    assert result.returncode == 0, result.stderr
    assert captured["ARG_0"].startswith(str(tmp_path))
    assert "run_c2_auto_repair_controller_v1.py" not in captured["ARG_0"]


def test_wrapper_source_does_not_hardcode_live_launch_mode() -> None:
    text = WRAPPER.read_text(encoding="utf-8")

    assert "--launch_mode LIVE" not in text
    assert 'EFFECTIVE_LAUNCH_MODE="DRY_RUN"' in text
    assert "C2_AUTO_REPAIR_ALLOW_LIVE" in text
    assert "export PYTHONPATH=" in text
