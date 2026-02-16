"""
test_paper_day_orchestrator_smoke_v1.py

Bundle 2 orchestrator smoke tests (audit-grade).

Threat model / hostile review focus:
- Orchestrator must be runnable deterministically via file path.
- Orchestrator must refuse LIVE mode.
- Orchestrator must refuse running from non-repo-root cwd.
- Orchestrator must fail-closed on bootstrap dataset when upstream engine invariants are unmet.

No network assumptions. These tests do not require IB connectivity.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
ORCH = (REPO_ROOT / "ops" / "tools" / "run_c2_paper_day_orchestrator_v1.py").resolve()


class TestPaperDayOrchestratorSmokeV1(unittest.TestCase):
    def test_orchestrator_file_exists(self) -> None:
        self.assertTrue(ORCH.exists(), f"missing orchestrator: {ORCH}")

    def test_refuses_non_repo_root_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = subprocess.run(
                ["python3", str(ORCH), "--day_utc", "2017-01-17", "--mode", "PAPER", "--symbol", "SPY"],
                cwd=td,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertNotEqual(p.returncode, 0, "expected non-zero rc when cwd != repo root")
            self.assertIn("must run from repo root", p.stderr, f"unexpected stderr:\n{p.stderr}")

    def test_refuses_live_mode(self) -> None:
        p = subprocess.run(
            ["python3", str(ORCH), "--day_utc", "2017-01-17", "--mode", "LIVE", "--symbol", "SPY"],
            cwd=str(REPO_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertNotEqual(p.returncode, 0, "expected non-zero rc for LIVE mode")
        self.assertIn("supports PAPER mode only", p.stderr, f"unexpected stderr:\n{p.stderr}")

    def test_fail_closed_on_bootstrap_dataset_at_mr(self) -> None:
        """
        Repo bootstrap market data is proven to be 10 rows for SPY 2017-01-03..2017-01-17.
        MR engine default requires 20 session bars; therefore orchestrator must fail-closed at stage 1.
        """
        p = subprocess.run(
            ["python3", str(ORCH), "--day_utc", "2017-01-17", "--mode", "PAPER", "--symbol", "SPY"],
            cwd=str(REPO_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertNotEqual(p.returncode, 0, "expected fail-closed rc != 0 on bootstrap dataset")
        self.assertIn("STAGE_START ENGINE_MEAN_REVERSION", p.stdout, f"unexpected stdout:\n{p.stdout}")
        self.assertIn("STAGE_FAIL ENGINE_MEAN_REVERSION", p.stderr, f"unexpected stderr:\n{p.stderr}")
        self.assertIn("INSUFFICIENT_SESSION_BARS", p.stdout + p.stderr, "expected MR invariant failure evidence")


if __name__ == "__main__":
    unittest.main()
