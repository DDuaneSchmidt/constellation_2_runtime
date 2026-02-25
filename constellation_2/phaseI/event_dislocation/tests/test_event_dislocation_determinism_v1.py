"""
test_event_dislocation_determinism_v1.py

Audit-grade determinism test for Event/Dislocation ExposureIntent emitter.

CRITICAL TEST ISOLATION CONTRACT:
- Multiple engines write into the same intents day directory.
- This test MUST isolate files produced by this engine only, by filtering on engine.engine_id.
- It MUST NOT delete or compare intents from other engines.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import List

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
RUNNER = (
    REPO_ROOT
    / "constellation_2"
    / "phaseI"
    / "event_dislocation"
    / "run"
    / "run_event_dislocation_intents_day_v1.py"
).resolve()
INTENTS_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth" / "intents_v1" / "snapshots").resolve()

ENGINE_ID = "C2_EVENT_DISLOCATION_V1"


def _engine_intent_files(day_dir: Path) -> List[Path]:
    out: List[Path] = []
    if not day_dir.exists():
        return out
    for p in sorted(day_dir.iterdir()):
        if not (p.is_file() and p.name.endswith(".exposure_intent.v1.json")):
            continue
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        eng = obj.get("engine") if isinstance(obj, dict) else None
        eid = eng.get("engine_id") if isinstance(eng, dict) else None
        if eid == ENGINE_ID:
            out.append(p)
    return out


class TestEventDislocationDeterminismV1(unittest.TestCase):
    def test_runner_exists(self) -> None:
        self.assertTrue(RUNNER.exists(), f"missing runner: {RUNNER}")

    def test_deterministic_output_bytes_v1(self) -> None:
        day = os.environ.get("C2_EVENT_DISLOCATION_TEST_DAY_UTC", "2017-01-17").strip()
        symbol = os.environ.get("C2_EVENT_DISLOCATION_TEST_SYMBOL", "SPY").strip().upper()

        # Use explicit thresholds from env; defaults are acceptable but tests should be controllable.
        gap_abs_enter = os.environ.get("C2_EVENT_DISLOCATION_TEST_GAP_ABS_ENTER", "0.02").strip()
        range_enter = os.environ.get("C2_EVENT_DISLOCATION_TEST_RANGE_ENTER", "0.03").strip()

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)

            day_dir = (INTENTS_ROOT / day).resolve()
            backup_dir = td_path / "backup_day_dir"
            existed = day_dir.exists()

            if existed:
                shutil.copytree(day_dir, backup_dir)

            try:
                p1 = subprocess.run(
                    [
                        "python3",
                        "-m",
                        "constellation_2.phaseI.event_dislocation.run.run_event_dislocation_intents_day_v1",
                        "--day_utc",
                        day,
                        "--mode",
                        "PAPER",
                        "--symbol",
                        symbol,
                        "--gap_abs_enter",
                        gap_abs_enter,
                        "--range_enter",
                        range_enter,
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=False,
                )
                self.assertEqual(
                    p1.returncode,
                    0,
                    f"run1 failed rc={p1.returncode}\nSTDOUT:\n{p1.stdout}\nSTDERR:\n{p1.stderr}",
                )

                files1 = _engine_intent_files(day_dir)
                bytes1 = [p.read_bytes() for p in files1]

                # Delete only this engine's intents, not other engines.
                for p in files1:
                    p.unlink()

                p2 = subprocess.run(
                    [
                        "python3",
                        "-m",
                        "constellation_2.phaseI.event_dislocation.run.run_event_dislocation_intents_day_v1",
                        "--day_utc",
                        day,
                        "--mode",
                        "PAPER",
                        "--symbol",
                        symbol,
                        "--gap_abs_enter",
                        gap_abs_enter,
                        "--range_enter",
                        range_enter,
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=False,
                )
                self.assertEqual(
                    p2.returncode,
                    0,
                    f"run2 failed rc={p2.returncode}\nSTDOUT:\n{p2.stdout}\nSTDERR:\n{p2.stderr}",
                )

                files2 = _engine_intent_files(day_dir)
                bytes2 = [p.read_bytes() for p in files2]

                self.assertEqual(bytes1, bytes2, "non-deterministic output bytes detected (engine-isolated)")
            finally:
                if day_dir.exists():
                    shutil.rmtree(day_dir)
                if existed:
                    shutil.copytree(backup_dir, day_dir)


if __name__ == "__main__":
    unittest.main()
