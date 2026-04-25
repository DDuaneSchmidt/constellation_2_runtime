from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

SOURCE_ROOT = Path("/home/node/constellation")
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import ops.tools.run_session_readiness_refresh_v1 as session_refresh_module


def test_monitoring_refresh_extra_env_skips_session_authority_reentry() -> None:
    env = session_refresh_module._monitoring_refresh_extra_env()

    assert env["C2_SKIP_AUTHORITATIVE_ORCHESTRATOR_BACKFILL"] == "YES"
    assert env["C2_SKIP_SESSION_AUTHORITY_REENTRY"] == "YES"


def test_main_uses_build_fast_path_and_skips_recursive_session_authority(monkeypatch, capsys) -> None:
    def fake_fast_path(*, day_utc: str, truth_root: Path) -> int:
        print(
            json.dumps(
                {
                    "day_utc": day_utc,
                    "truth_root": str(truth_root),
                    "status": "SESSION_READINESS_BUILD_FAST_PATH",
                    "results": {
                        "authority_kernel_validation": {"validation_summary": {"validation_state": "PASS"}},
                        "scope_summary": {"primary_ready": True},
                        "day_authority_decision": {"decision_state": "OPEN"},
                    },
                },
                sort_keys=True,
            )
        )
        return 0

    monkeypatch.setattr(session_refresh_module, "_invoked_by_session_authority_build", lambda: True)
    monkeypatch.setattr(session_refresh_module, "_run_build_context_fast_path", fake_fast_path)

    with patch("sys.argv", ["run_session_readiness_refresh_v1.py", "--day_utc", "2026-04-24"]):
        rc = session_refresh_module.main()

    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["status"] == "SESSION_READINESS_BUILD_FAST_PATH"
    assert payload["results"]["day_authority_decision"]["decision_state"] == "OPEN"
    assert payload["results"]["scope_summary"]["primary_ready"] is True
