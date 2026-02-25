"""
test_defensive_tail_determinism_v1.py

Audit-grade determinism tests for Engine 5 (Defensive Tail) ExposureIntent v2 emitter.

Tests:
- runner exists
- deterministic output bytes: two runs on same day produce identical bytes
- NO_INTENT smoke test on a known NORMAL day (CHOSEN_NO_INTENT_DAY from discovery)
- FORCE_ENTER test-only flag emits intent deterministically
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
import json


def _list_own_v2_intents(day_dir: Path, day: str) -> list[Path]:
    """
    Return only intents produced by THIS runner version:
      producer == "run_defensive_tail_intents_day_v1"
      produced_utc == f"{day}T00:00:00Z"
    This filters out legacy v2 artifacts already present in truth.
    """
    if not day_dir.exists():
        return []
    out: list[Path] = []
    produced = f"{day}T00:00:00Z"
    for p in sorted([x for x in day_dir.iterdir() if x.is_file() and x.name.endswith(".exposure_intent.v2.json")]):
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        if str(obj.get("producer") or "") != "run_defensive_tail_intents_day_v1":
            continue
        if str(obj.get("produced_utc") or "") != produced:
            continue
        out.append(p)
    return sorted(out)



REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
RUNNER = (REPO_ROOT / "constellation_2" / "phaseI" / "defensive_tail" / "run" / "run_defensive_tail_intents_day_v1.py").resolve()
INTENTS_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth" / "intents_v1" / "snapshots").resolve()

NO_INTENT_DAY_DEFAULT = "2001-01-01"
ENTER_DAY_DEFAULT = "2001-03-05"


class TestDefensiveTailDeterminismV1(unittest.TestCase):
    def test_runner_exists(self) -> None:
        self.assertTrue(RUNNER.exists(), f"missing runner: {RUNNER}")

    def _run(self, day: str, extra: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", "-m", "constellation_2.phaseI.defensive_tail.run.run_defensive_tail_intents_day_v1",
             "--day_utc", day, "--mode", "PAPER", "--symbol", "SPY"] + extra,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )

    def test_no_intent_smoke_default_day(self) -> None:
        day = os.environ.get("C2_DEF_TAIL_NO_INTENT_DAY_UTC", NO_INTENT_DAY_DEFAULT).strip()
        day_dir = (INTENTS_ROOT / day).resolve()

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            backup_dir = td_path / "backup_day_dir"
            existed = day_dir.exists()
            if existed:
                shutil.copytree(day_dir, backup_dir)

            try:
                p = self._run(day, [])
                self.assertEqual(p.returncode, 0, f"runner failed rc={p.returncode}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}")

                # Should emit no v2 intent files on NO_INTENT day
                if day_dir.exists():
                    files = _list_own_v2_intents(day_dir, day)
                else:
                    files = []
                self.assertEqual(files, [], f"expected NO_INTENT but found intent files: {[f.name for f in files]}")
            finally:
                if day_dir.exists():
                    shutil.rmtree(day_dir)
                if existed:
                    shutil.copytree(backup_dir, day_dir)

    def test_force_enter_deterministic_bytes_v2(self) -> None:
        day = os.environ.get("C2_DEF_TAIL_ENTER_DAY_UTC", ENTER_DAY_DEFAULT).strip()

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)

            day_dir = (INTENTS_ROOT / day).resolve()
            backup_dir = td_path / "backup_day_dir"
            existed = day_dir.exists()
            if existed:
                shutil.copytree(day_dir, backup_dir)

            try:
                p1 = self._run(day, ["--force_enter_test_only"])
                self.assertEqual(p1.returncode, 0, f"run1 failed rc={p1.returncode}\nSTDOUT:\n{p1.stdout}\nSTDERR:\n{p1.stderr}")

                self.assertTrue(day_dir.exists(), "expected intents day dir to exist after run")
                files1 = _list_own_v2_intents(day_dir, day)
                bytes1 = [p.read_bytes() for p in files1]

                # Clear and rerun
                for p in files1:
                    p.unlink()

                p2 = self._run(day, ["--force_enter_test_only"])
                self.assertEqual(p2.returncode, 0, f"run2 failed rc={p2.returncode}\nSTDOUT:\n{p2.stdout}\nSTDERR:\n{p2.stderr}")

                files2 = _list_own_v2_intents(day_dir, day)
                bytes2 = [p.read_bytes() for p in files2]

                self.assertEqual(bytes1, bytes2, "non-deterministic output bytes detected (force-enter)")
            finally:
                if day_dir.exists():
                    shutil.rmtree(day_dir)
                if existed:
                    shutil.copytree(backup_dir, day_dir)

    def test_entry_rule_deterministic_bytes_v2(self) -> None:
        # Uses default enter day where regime != NORMAL (per truth), so engine should emit.
        day = os.environ.get("C2_DEF_TAIL_ENTER_DAY_UTC", ENTER_DAY_DEFAULT).strip()

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)

            day_dir = (INTENTS_ROOT / day).resolve()
            backup_dir = td_path / "backup_day_dir"
            existed = day_dir.exists()
            if existed:
                shutil.copytree(day_dir, backup_dir)

            try:
                p1 = self._run(day, [])
                self.assertEqual(p1.returncode, 0, f"run1 failed rc={p1.returncode}\nSTDOUT:\n{p1.stdout}\nSTDERR:\n{p1.stderr}")

                files1 = _list_own_v2_intents(day_dir, day)
                bytes1 = [p.read_bytes() for p in files1]

                for p in files1:
                    p.unlink()

                p2 = self._run(day, [])
                self.assertEqual(p2.returncode, 0, f"run2 failed rc={p2.returncode}\nSTDOUT:\n{p2.stdout}\nSTDERR:\n{p2.stderr}")

                files2 = _list_own_v2_intents(day_dir, day)
                bytes2 = [p.read_bytes() for p in files2]

                self.assertEqual(bytes1, bytes2, "non-deterministic output bytes detected (entry rule)")
            finally:
                if day_dir.exists():
                    shutil.rmtree(day_dir)
                if existed:
                    shutil.copytree(backup_dir, day_dir)


if __name__ == "__main__":
    unittest.main()
