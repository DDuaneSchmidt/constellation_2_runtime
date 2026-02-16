"""
test_vol_income_defined_risk_determinism_v1.py

Audit-grade determinism test for Vol Income (Defined Risk) ExposureIntent emitter.

Calendar-independent. Uses market_data_snapshot_v1 truth spine.
Mirrors MR determinism test methodology.

CRITICAL TEST ISOLATION CONTRACT:
- Multiple engines write into the same intents day directory.
- This test MUST isolate files produced by this engine only, by filtering on engine.engine_id.
- It MUST NOT delete or compare intents from other engines.

NOTE:
- Repo bootstrap market data sample is 10 rows ending 2017-01-17 (proven).
- Default test day uses that last available day.
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
    / "vol_income_defined_risk"
    / "run"
    / "run_vol_income_defined_risk_intents_day_v1.py"
).resolve()
INTENTS_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth" / "intents_v1" / "snapshots").resolve()

ENGINE_ID = "C2_VOL_INCOME_DEFINED_RISK_V1"


def _engine_intent_files(day_dir: Path) -> List[Path]:
    out: List[Path] = []
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


class TestVolIncomeDefinedRiskDeterminismV1(unittest.TestCase):
    def test_runner_exists(self) -> None:
        self.assertTrue(RUNNER.exists(), f"missing runner: {RUNNER}")

    def test_deterministic_output_bytes_v1(self) -> None:
        day = os.environ.get("C2_VOL_TEST_DAY_UTC", "2017-01-17").strip()
        symbol = os.environ.get("C2_VOL_TEST_SYMBOL", "SPY").strip().upper()

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)

            day_dir = (INTENTS_ROOT / day).resolve()
            backup_dir = td_path / "backup_day_dir"
            existed = day_dir.exists()

            if existed:
                shutil.copytree(day_dir, backup_dir)

            try:
                p1 = subprocess.run(
                    ["python3", "-m", "constellation_2.phaseI.vol_income_defined_risk.run.run_vol_income_defined_risk_intents_day_v1",
                     "--day_utc", day, "--mode", "PAPER", "--symbol", symbol],
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

                self.assertTrue(day_dir.exists(), "expected intents day dir to exist after run")
                files1 = _engine_intent_files(day_dir)
                bytes1 = [p.read_bytes() for p in files1]

                for p in files1:
                    p.unlink()

                p2 = subprocess.run(
                    ["python3", "-m", "constellation_2.phaseI.vol_income_defined_risk.run.run_vol_income_defined_risk_intents_day_v1",
                     "--day_utc", day, "--mode", "PAPER", "--symbol", symbol],
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
