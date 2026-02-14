"""
test_phaseD_submission_id_determinism_v1.py

Acceptance (Phase D Test E):
- submission_id is derived deterministically from binding_hash and is stable across identical inputs.

Execution:
  constellation_2/.venv/bin/python -m constellation_2.phaseD.tests.test_phaseD_submission_id_determinism_v1
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from constellation_2.phaseC.tools.c2_submit_preflight_offline_v1 import main as phasec_main
from constellation_2.phaseC.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1
from constellation_2.phaseD.lib.idempotency_guard_v1 import derive_submission_id_from_binding_hash_v1

REPO_ROOT = Path(__file__).resolve().parents[3]
SAMPLES = REPO_ROOT / "constellation_2" / "acceptance" / "samples"


class TestPhaseDSubmissionIdDeterminismV1(unittest.TestCase):
    def test_submission_id_equals_binding_hash_stable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            out1 = td_path / "out1"
            out2 = td_path / "out2"

            base_args = [
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
            ]

            rc1 = phasec_main(base_args + ["--out_dir", str(out1)])
            rc2 = phasec_main(base_args + ["--out_dir", str(out2)])
            self.assertEqual(rc1, 0)
            self.assertEqual(rc2, 0)

            b1 = json.load(open(out1 / "binding_record.v1.json", "r", encoding="utf-8"))
            b2 = json.load(open(out2 / "binding_record.v1.json", "r", encoding="utf-8"))

            h1 = canonical_hash_for_c2_artifact_v1(b1)
            h2 = canonical_hash_for_c2_artifact_v1(b2)
            self.assertEqual(h1, h2)

            sid1 = derive_submission_id_from_binding_hash_v1(h1)
            sid2 = derive_submission_id_from_binding_hash_v1(h2)
            self.assertEqual(sid1, sid2)
            self.assertEqual(sid1, h1)


if __name__ == "__main__":
    unittest.main()
