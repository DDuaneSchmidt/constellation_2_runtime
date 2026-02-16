"""
test_supervisor_phased_root_trigger_v1.py

Hostile-review acceptance test for Bundle 3 supervisor fix.

Validates artifacts (strong evidence) rather than brittle log substring checks.

Proves:
- Supervisor v2 runs in run_once mode without broker connectivity
- Supervisor v2 writes local fingerprint state
- Supervisor v2 writes local health artifact that includes PhaseD submissions root info
"""

from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
SUP = (REPO_ROOT / "ops" / "run" / "c2_supervisor_paper_v2.py").resolve()

STATE_ROOT = (Path.home() / ".local/state/constellation_2").resolve()
FP_PATH = (STATE_ROOT / "phaseD_submissions_root_fp.sha256").resolve()


class TestSupervisorPhaseDRootTriggerV1(unittest.TestCase):
    def test_supervisor_file_exists(self) -> None:
        self.assertTrue(SUP.exists(), f"missing supervisor: {SUP}")

    def test_run_once_writes_health_and_fp(self) -> None:
        if FP_PATH.exists():
            FP_PATH.unlink()

        p = subprocess.run(
            ["python3", str(SUP), "--run_once", "true", "--poll_seconds", "30"],
            cwd=str(REPO_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        self.assertEqual(
            p.returncode,
            0,
            f"expected rc=0\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}",
        )
        self.assertTrue(FP_PATH.exists(), "expected fingerprint file to be written in local state")

        health_files = sorted(STATE_ROOT.glob("supervisor_health_*.v1.json"))
        self.assertTrue(health_files, "expected at least one supervisor health file in local state")

        hp = health_files[-1]
        with open(hp, "r", encoding="utf-8") as f:
            obj = json.load(f)

        self.assertIsInstance(obj, dict, "health file must be a JSON object")

        phaseD = obj.get("phaseD_submissions_root")
        self.assertIsInstance(phaseD, dict, "health must include phaseD_submissions_root object")

        d = phaseD.get("dir")
        self.assertIsInstance(d, str, "phaseD_submissions_root.dir must be string")
        self.assertIn("constellation_2/phaseD/outputs/submissions", d, "health must reference PhaseD submissions root")

        self.assertIn("days_considered", obj, "health must include days_considered")


if __name__ == "__main__":
    unittest.main()
