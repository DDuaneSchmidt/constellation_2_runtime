"""
test_phaseD_risk_budget_gate_v1.py

Acceptance (Phase D Test A):
- If WhatIf margin exceeds RiskBudget, gate returns allow=False with reason_code C2_RISK_BUDGET_EXCEEDED.

Execution:
  constellation_2/.venv/bin/python -m constellation_2.phaseD.tests.test_phaseD_risk_budget_gate_v1
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from constellation_2.phaseD.lib.risk_budget_gate_v1 import enforce_risk_budget_against_whatif_v1

REPO_ROOT = Path(__file__).resolve().parents[3]


class TestPhaseDRiskBudgetGateV1(unittest.TestCase):
    def test_exceed_margin_vetos(self) -> None:
        rb = json.load(
            open(REPO_ROOT / "constellation_2/phaseD/inputs/sample_risk_budget.v1.json", "r", encoding="utf-8")
        )

        dec = enforce_risk_budget_against_whatif_v1(
            repo_root=REPO_ROOT,
            risk_budget=rb,
            whatif_margin_change_usd="999999",
            whatif_notional_usd="1",
            engine_id=None,
        )

        self.assertFalse(dec.allow)
        self.assertEqual(dec.reason_code, "C2_RISK_BUDGET_EXCEEDED")


if __name__ == "__main__":
    unittest.main()
