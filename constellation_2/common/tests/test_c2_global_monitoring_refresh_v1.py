from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root
import ops.tools.run_c2_global_monitoring_refresh_v1 as monitoring_module


def test_rc_2_is_reported_as_degraded_not_ok(capsys: pytest.CaptureFixture[str]) -> None:
    results = iter(
        [
            {
                "name": "session_authority_reentry",
                "cmd": [],
                "returncode": 0,
                "status": "OK",
                "stdout": "",
                "stderr": "",
            },
            {"name": "day_open_trigger", "cmd": [], "returncode": 0, "status": "OK", "stdout": "", "stderr": ""},
            {"name": "day_open_attempt", "cmd": [], "returncode": 0, "status": "OK", "stdout": "", "stderr": ""},
            {"name": "position_lifecycle_v2", "cmd": [], "returncode": 0, "status": "OK", "stdout": "", "stderr": ""},
            {"name": "exit_obligations_v1", "cmd": [], "returncode": 2, "status": "DEGRADED", "stdout": "", "stderr": ""},
            {"name": "exposure_reconciliation_v2", "cmd": [], "returncode": 0, "status": "OK", "stdout": "", "stderr": ""},
            {"name": "lifecycle_monitor", "cmd": [], "returncode": 0, "status": "OK", "stdout": "", "stderr": ""},
            {"name": "paper_readiness", "cmd": [], "returncode": 0, "status": "OK", "stdout": "", "stderr": ""},
            {
                "name": "capital_authority_monitoring_attestation",
                "cmd": [],
                "returncode": 0,
                "status": "OK",
                "stdout": "",
                "stderr": "",
            },
        ]
    )
    with patch.object(monitoring_module, "_run_step", side_effect=lambda name, cmd: next(results)):
        rc = monitoring_module.main(["--day_utc", "2026-04-14"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 2
    assert payload["status"] == "DEGRADED"
    assert payload["degraded_steps"] == ["exit_obligations_v1"]
    assert payload["hard_failures"] == []


def test_default_truth_root_is_canonical_and_out_of_contract_override_is_rejected() -> None:
    captured: list[list[str]] = []

    def fake_run_step(name: str, cmd: list[str]) -> dict[str, object]:
        captured.append(list(cmd))
        return {"name": name, "cmd": cmd, "returncode": 0, "status": "OK", "stdout": "", "stderr": ""}

    with patch.object(monitoring_module, "_run_step", side_effect=fake_run_step):
        rc = monitoring_module.main(["--day_utc", "2026-04-14"])
    assert rc == 0
    default_truth_root = str(resolve_canonical_truth_root())
    assert all("--truth_root" in cmd and default_truth_root in cmd for cmd in captured[:6])

    captured.clear()
    with patch.object(monitoring_module, "_run_step", side_effect=fake_run_step):
        with pytest.raises(ValueError, match="RUNTIME_PATH_AUTHORITY_POLICY_TRUTH_ROOT_FORBIDDEN"):
            monitoring_module.main(["--day_utc", "2026-04-14", "--truth_root", "/tmp/explicit-truth-root"])


def test_bootstrap_env_skips_session_authority_reentry(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: list[tuple[str, list[str]]] = []

    def fake_run_step(name: str, cmd: list[str]) -> dict[str, object]:
        captured.append((name, list(cmd)))
        return {"name": name, "cmd": cmd, "returncode": 0, "status": "OK", "stdout": "", "stderr": ""}

    monkeypatch.setenv(monitoring_module.SKIP_SESSION_AUTHORITY_REENTRY_ENV, "YES")
    with patch.object(monitoring_module, "_run_step", side_effect=fake_run_step):
        rc = monitoring_module.main(["--day_utc", "2026-04-14"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["session_authority_reentry_skipped"] is True
    assert all(name != "session_authority_reentry" for name, _ in captured)
