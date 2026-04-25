from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path("/home/node/constellation")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import ops.tools.run_session_readiness_refresh_v1 as session_refresh_module


DAY = "2026-03-16"


def test_session_refresh_runs_global_kill_switch_before_submit_boundary_status() -> None:
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        target = str(cmd[1]) if len(cmd) > 1 else ""
        stdout = ""
        if target == str(session_refresh_module.PAPER_SESSION_LEDGER_TOOL):
            stdout = json.dumps(
                {
                    "path": "/tmp/paper_session_ledger.v1.json",
                    "ledger_id": "paper-session-ledger::2026-03-16",
                    "authority_status": "GRANTED",
                },
                sort_keys=True,
            )
        return {"cmd": cmd, "returncode": 0, "stdout": stdout, "stderr": ""}

    with tempfile.TemporaryDirectory(dir=str(REPO_ROOT / "tmp")) as td:
        global_truth = Path(td) / "truth"
        with patch.object(
            session_refresh_module, "GLOBAL_TRUTH_ROOT", global_truth
        ), patch.object(
            session_refresh_module, "resolve_single_paper_ib_account_from_sleeve_registry", return_value="DUO847203"
        ), patch.object(
            session_refresh_module, "_load_accounts", return_value={"PAPER": ["DUO847203"], "LIVE": []}
        ), patch.object(
            session_refresh_module, "_run", side_effect=fake_run
        ), patch.object(
            session_refresh_module, "_resolve_paper_sleeve_truth_bindings", return_value=[]
        ), patch.object(
            session_refresh_module, "resolve_governed_paper_execution_profile", return_value=SimpleNamespace(host="127.0.0.1", port=4002, client_id_observer=179)
        ), patch.object(
            session_refresh_module, "validate_against_repo_schema_v1", lambda *args, **kwargs: None
        ), patch(
            "sys.argv", ["run_session_readiness_refresh_v1.py", "--day_utc", DAY]
        ):
            rc = session_refresh_module.main()

    assert rc == 0
    targets = [str(cmd[1]) for cmd in calls if len(cmd) > 1]
    kill_idx = targets.index(str(session_refresh_module.GLOBAL_KILL_SWITCH_TOOL))
    boundary_idx = targets.index(str(session_refresh_module.SUBMIT_BOUNDARY_STATUS_TOOL))
    assert kill_idx < boundary_idx
