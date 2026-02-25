"""
test_event_dislocation_smoke_no_intent_v1.py

Smoke test: runner must return 0 and produce deterministic NO_INTENT when thresholds are set
so high they cannot trigger on normal data.

This test does NOT assume the intents day directory exists.
"""

from __future__ import annotations

import os
import subprocess
import unittest


class TestEventDislocationSmokeNoIntentV1(unittest.TestCase):
    def test_smoke_no_intent(self) -> None:
        day = os.environ.get("C2_EVENT_DISLOCATION_SMOKE_DAY_UTC", "2017-01-17").strip()
        symbol = os.environ.get("C2_EVENT_DISLOCATION_SMOKE_SYMBOL", "SPY").strip().upper()

        # Force NO_INTENT deterministically by making thresholds nearly impossible.
        gap_abs_enter = "0.99"
        range_enter = "0.99"

        p = subprocess.run(
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
        self.assertEqual(p.returncode, 0, f"runner failed rc={p.returncode}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}")

        # Must print an OK line with ED_NO_INTENT.
        self.assertIn("OK: ED_NO_INTENT", p.stdout, f"expected ED_NO_INTENT in stdout\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}")


if __name__ == "__main__":
    unittest.main()
