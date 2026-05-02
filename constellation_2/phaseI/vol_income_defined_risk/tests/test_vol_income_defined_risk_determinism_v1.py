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
- Repo bootstrap market data sample is current-repo local. The test copies it
  into a temporary truth root so it never mutates runtime evidence.
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

REPO_ROOT = Path(__file__).resolve().parents[4]
RUNNER = (
    REPO_ROOT
    / "constellation_2"
    / "phaseI"
    / "vol_income_defined_risk"
    / "run"
    / "run_vol_income_defined_risk_intents_day_v1.py"
).resolve()
MARKET_DATA_FIXTURE_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth" / "market_data_snapshot_v1").resolve()

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
        day = os.environ.get("C2_VOL_TEST_DAY_UTC", "2026-04-16").strip()
        symbol = os.environ.get("C2_VOL_TEST_SYMBOL", "SPY").strip().upper()

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            truth_root = (td_path / "truth").resolve()
            shutil.copytree(MARKET_DATA_FIXTURE_ROOT, truth_root / "market_data_snapshot_v1")
            day_dir = (truth_root / "intents_v1" / "snapshots" / day).resolve()

            try:
                p1 = subprocess.run(
                    ["python3", "-m", "constellation_2.phaseI.vol_income_defined_risk.run.run_vol_income_defined_risk_intents_day_v1",
                     "--day_utc", day, "--mode", "PAPER", "--symbol", symbol,
                     "--truth_root", str(truth_root)],
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
                     "--day_utc", day, "--mode", "PAPER", "--symbol", symbol,
                     "--truth_root", str(truth_root)],
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


if __name__ == "__main__":
    unittest.main()
