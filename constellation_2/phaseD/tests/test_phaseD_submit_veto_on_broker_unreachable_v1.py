"""
test_phaseD_submit_veto_on_broker_unreachable_v1.py

Acceptance (Phase D Test C/D baseline):
- If broker is unreachable, Phase D submit boundary emits VetoRecord with reason_code C2_BROKER_ADAPTER_NOT_AVAILABLE.
- No BrokerSubmissionRecord is written.

Execution:
  constellation_2/.venv/bin/python -m constellation_2.phaseD.tests.test_phaseD_submit_veto_on_broker_unreachable_v1
"""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from constellation_2.phaseC.tools.c2_submit_preflight_offline_v1 import main as phasec_main

REPO_ROOT = Path(__file__).resolve().parents[3]
SAMPLES = REPO_ROOT / "constellation_2" / "acceptance" / "samples"


class TestPhaseDSubmitVetoOnBrokerUnreachableV1(unittest.TestCase):
    def test_veto_written_when_broker_unreachable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            phasec_out = td_path / "phasec_out"
            phased_out = td_path / "phased_out"

            rc = phasec_main(
                [
                    "--intent",
                    str(SAMPLES / "sample_options_intent.v2.json"),
                    "--chain_snapshot",
                    str(SAMPLES / "sample_chain_snapshot.v1.json"),
                    "--freshness_cert",
                    str(SAMPLES / "sample_freshness_certificate.v1.json"),
                    "--eval_time_utc",
                    "2026-02-13T21:52:00Z",
                    "--tick_size",
                    "0.01",
                    "--out_dir",
                    str(phasec_out),
                ]
            )
            self.assertEqual(rc, 0, "Phase C should succeed with acceptance samples")

            cmd = [
                str(REPO_ROOT / "constellation_2/.venv/bin/python"),
                str(REPO_ROOT / "constellation_2/phaseD/tools/c2_submit_paper_v1.py"),
                "--phasec_out_dir",
                str(phasec_out),
                "--risk_budget",
                str(REPO_ROOT / "constellation_2/phaseD/inputs/sample_risk_budget.v1.json"),
                "--eval_time_utc",
                "2026-02-14T00:00:00Z",
                "--out_dir",
                str(phased_out),
                "--ib_host",
                "127.0.0.1",
                "--ib_port",
                "7497",
                "--ib_client_id",
                "1",
            ]

            p = subprocess.run(cmd, capture_output=True, text=True)
            self.assertEqual(
                p.returncode,
                2,
                f"expected veto return code 2, got {p.returncode}\nSTDOUT={p.stdout}\nSTDERR={p.stderr}",
            )

            veto_path = phased_out / "veto_record.v1.json"
            self.assertTrue(veto_path.exists(), "veto_record must be written")

            veto = json.load(open(veto_path, "r", encoding="utf-8"))
            self.assertEqual(veto.get("reason_code"), "C2_BROKER_ADAPTER_NOT_AVAILABLE")

            self.assertFalse((phased_out / "broker_submission_record.v2.json").exists())


if __name__ == "__main__":
    unittest.main()
