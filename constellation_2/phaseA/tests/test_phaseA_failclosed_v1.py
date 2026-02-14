"""
test_phaseA_failclosed_v1.py

Acceptance:
- If freshness cert is expired relative to injected “now”, mapping emits VetoRecord
  with correct reason code and no partial outputs.

Execution:
  python3 -m constellation_2.phaseA.tests.test_phaseA_failclosed_v1
"""

from __future__ import annotations

import unittest
from pathlib import Path

from constellation_2.phaseA.lib.canon_json_v1 import load_json_file
from constellation_2.phaseA.lib.map_vertical_spread_v1 import map_vertical_spread_offline


REPO_ROOT = Path(__file__).resolve().parents[3]
SAMPLES = REPO_ROOT / "constellation_2" / "acceptance" / "samples"


class TestPhaseAFailClosedV1(unittest.TestCase):
    def test_expired_freshness_vetos_and_no_partial_outputs(self) -> None:
        intent = load_json_file(SAMPLES / "sample_options_intent.v2.json")
        chain = load_json_file(SAMPLES / "sample_chain_snapshot.v1.json")
        cert = load_json_file(SAMPLES / "sample_freshness_certificate.v1.json")

        # Outside certificate valid_until_utc (which is 21:55Z)
        now_utc = "2026-02-13T22:00:00Z"
        tick_size = "0.01"
        pointers = [
            str(SAMPLES / "sample_options_intent.v2.json"),
            str(SAMPLES / "sample_chain_snapshot.v1.json"),
            str(SAMPLES / "sample_freshness_certificate.v1.json"),
        ]

        r = map_vertical_spread_offline(
            intent=intent,
            chain=chain,
            cert=cert,
            now_utc=now_utc,
            tick_size=tick_size,
            pointers=pointers,
        )

        self.assertFalse(r.ok, "mapping must veto on expired freshness cert")
        self.assertIsNotNone(r.veto_record, "veto_record must be present")
        self.assertIsNone(r.order_plan, "no partial order_plan allowed")
        self.assertIsNone(r.mapping_ledger_record, "no partial mapping_ledger_record allowed")
        self.assertIsNone(r.binding_record, "no partial binding_record allowed")

        assert r.veto_record is not None
        self.assertEqual(r.veto_record["reason_code"], "C2_FRESHNESS_CERT_INVALID_OR_EXPIRED")


if __name__ == "__main__":
    unittest.main()
