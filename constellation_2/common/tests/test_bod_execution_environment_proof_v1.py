from __future__ import annotations

import builtins
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from constellation_2.common.bod_execution_environment_proof_v1 import (
    derive_bod_execution_environment_proof_payload,
)


REPO_ROOT = Path("/home/node/constellation")


def test_bod_execution_environment_proof_passes_with_bridge_probe(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True)

    payload = derive_bod_execution_environment_proof_payload(
        repo_root=REPO_ROOT,
        truth_root=truth_root,
        day_utc="2026-04-14",
    )

    assert payload["status"] == "PASS"
    assert payload["blocking_codes"] == []
    assert payload["bridge_import_probe"]["returncode"] == 0
    assert payload["bridge_import_probe"]["proof_payload"]["proof_mode"] == "import_only"


def test_bod_execution_environment_proof_fails_closed_on_import_probe_failure(tmp_path: Path) -> None:
    truth_root = tmp_path / "truth"
    truth_root.mkdir(parents=True)
    original_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "constellation_2":
            raise ModuleNotFoundError("synthetic import failure")
        return original_import(name, globals, locals, fromlist, level)

    with patch("builtins.__import__", side_effect=fake_import), patch(
        "constellation_2.common.bod_execution_environment_proof_v1.subprocess.run",
        return_value=SimpleNamespace(returncode=1, stdout="", stderr="bridge import failed"),
    ):
        payload = derive_bod_execution_environment_proof_payload(
            repo_root=REPO_ROOT,
            truth_root=truth_root,
            day_utc="2026-04-14",
        )

    assert payload["status"] == "BLOCKED_BY_DEFECT"
    assert "BOD_EXECUTION_SUBSTRATE_PROOF_FAIL:CONSTELLATION_2_IMPORT_FAIL" in payload["blocking_codes"]
    assert "BOD_EXECUTION_SUBSTRATE_PROOF_FAIL:BRIDGE_IMPORT_PROBE_NONZERO" in payload["blocking_codes"]
