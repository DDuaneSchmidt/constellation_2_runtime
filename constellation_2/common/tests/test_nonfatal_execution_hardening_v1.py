from __future__ import annotations

import subprocess
import sys
from pathlib import Path

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.bridge_accounting_nav_v2_to_compat_v1 as compat_module
import ops.tools.run_correlation_envelope_gate_v1 as correlation_module
import ops.tools.run_replay_certification_bundle_v1 as replay_bundle_module
from constellation_2.common import canonical_fact_store_v1 as canonical_fact_store_module


def test_bridge_git_sha_suppresses_git_stderr_on_fallback(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def _fake_check_output(cmd: list[str], **kwargs: object) -> bytes:
        calls.append({"cmd": list(cmd), "kwargs": dict(kwargs)})
        raise subprocess.CalledProcessError(returncode=128, cmd=cmd)

    monkeypatch.setattr(compat_module.subprocess, "check_output", _fake_check_output)

    assert compat_module._git_sha() == "0" * 40
    assert calls and calls[0]["kwargs"]["stderr"] is subprocess.DEVNULL


def test_replay_bundle_git_sha_suppresses_git_stderr_on_fallback(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def _fake_check_output(cmd: list[str], **kwargs: object) -> bytes:
        calls.append({"cmd": list(cmd), "kwargs": dict(kwargs)})
        raise subprocess.CalledProcessError(returncode=128, cmd=cmd)

    monkeypatch.setattr(replay_bundle_module.subprocess, "check_output", _fake_check_output)

    assert replay_bundle_module._git_sha() == "UNKNOWN"
    assert calls and calls[0]["kwargs"]["stderr"] is subprocess.DEVNULL


def test_correlation_gate_git_sha_suppresses_git_stderr_on_fallback(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def _fake_check_output(cmd: list[str], **kwargs: object) -> bytes:
        calls.append({"cmd": list(cmd), "kwargs": dict(kwargs)})
        raise subprocess.CalledProcessError(returncode=128, cmd=cmd)

    monkeypatch.setattr(correlation_module.subprocess, "check_output", _fake_check_output)

    assert correlation_module._git_sha() == "UNKNOWN"
    assert calls and calls[0]["kwargs"]["stderr"] is subprocess.DEVNULL


def test_capture_executed_code_identity_suppresses_release_branch_git_stderr(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def _fake_check_output(cmd: list[str], **kwargs: object) -> str:
        calls.append({"cmd": list(cmd), "kwargs": dict(kwargs)})
        raise subprocess.CalledProcessError(returncode=128, cmd=cmd)

    monkeypatch.setattr(
        canonical_fact_store_module,
        "_load_release_manifest_if_present",
        lambda **kwargs: {"release_id": "20260410T000000Z__abc", "git_sha": "a" * 40},
    )
    monkeypatch.setattr(canonical_fact_store_module.subprocess, "check_output", _fake_check_output)

    payload = canonical_fact_store_module.capture_executed_code_identity(repo_root=SOURCE_ROOT)

    assert payload["git_sha"] == "a" * 40
    assert payload["branch"] == "RELEASE"
    assert calls and calls[0]["kwargs"]["stderr"] is subprocess.DEVNULL
