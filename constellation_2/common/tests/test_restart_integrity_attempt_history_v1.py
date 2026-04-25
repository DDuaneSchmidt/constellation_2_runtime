from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import ops.tools.run_testing_evidence_plane_v1 as evidence_plane_module


REPO_ROOT = Path("/home/node/constellation")


def test_workflow_restart_evidence_references_existing_repo_tests() -> None:
    _, suite_configs = evidence_plane_module.RESULT_GROUPS["workflow_restart_result_v1"]
    configured_paths = {path for suite in suite_configs for path in suite.test_paths}
    assert configured_paths == {
        "constellation_2/common/tests/test_paper_day_control_plane_v1.py::test_paper_day_control_plane_attempt_history_is_immutable_and_day_file_tracks_latest",
        "constellation_2/common/tests/test_runtime_replay_day_v1.py",
        "constellation_2/common/tests/test_session_authority_v1.py::test_build_and_admission_attempt_history_is_immutable_and_day_file_tracks_latest",
    }
    for relpath in configured_paths:
        assert (REPO_ROOT / relpath.split("::", 1)[0]).is_file(), relpath


def test_workflow_restart_evidence_suites_run_from_repo() -> None:
    _, suite_configs = evidence_plane_module.RESULT_GROUPS["workflow_restart_result_v1"]
    for suite in suite_configs:
        completed = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", *suite.test_paths],
            cwd=str(REPO_ROOT),
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, completed.stdout + completed.stderr
