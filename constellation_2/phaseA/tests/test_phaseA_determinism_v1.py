"""
test_phaseA_determinism_v1.py

Acceptance:
- Run mapping twice on identical inputs and assert identical canonical hashes for outputs.

Execution:
  python3 -m constellation_2.phaseA.tests.test_phaseA_determinism_v1
"""

from __future__ import annotations

import unittest
from pathlib import Path

from constellation_2.phaseA.lib.canon_json_v1 import load_json_file
from constellation_2.phaseA.lib.map_vertical_spread_v1 import map_vertical_spread_offline


REPO_ROOT = Path(__file__).resolve().parents[3]
SAMPLES = REPO_ROOT / "constellation_2" / "acceptance" / "samples"


class TestPhaseADeterminismV1(unittest.TestCase):
    def test_mapping_is_deterministic(self) -> None:
        intent = load_json_file(SAMPLES / "sample_options_intent.v2.json")
        chain = load_json_file(SAMPLES / "sample_chain_snapshot.v1.json")
        cert = load_json_file(SAMPLES / "sample_freshness_certificate.v1.json")

        now_utc = "2026-02-13T21:52:00Z"
        tick_size = "0.01"
        pointers = [
            str(SAMPLES / "sample_options_intent.v2.json"),
            str(SAMPLES / "sample_chain_snapshot.v1.json"),
            str(SAMPLES / "sample_freshness_certificate.v1.json"),
        ]

        r1 = map_vertical_spread_offline(
            intent=intent,
            chain=chain,
            cert=cert,
            now_utc=now_utc,
            tick_size=tick_size,
            pointers=pointers,
        )
        r2 = map_vertical_spread_offline(
            intent=intent,
            chain=chain,
            cert=cert,
            now_utc=now_utc,
            tick_size=tick_size,
            pointers=pointers,
        )

        self.assertTrue(r1.ok, "first mapping should succeed")
        self.assertTrue(r2.ok, "second mapping should succeed")

        assert r1.order_plan and r1.mapping_ledger_record and r1.binding_record
        assert r2.order_plan and r2.mapping_ledger_record and r2.binding_record

        self.assertEqual(r1.order_plan["canonical_json_hash"], r2.order_plan["canonical_json_hash"])
        self.assertEqual(r1.mapping_ledger_record["canonical_json_hash"], r2.mapping_ledger_record["canonical_json_hash"])
        self.assertEqual(r1.binding_record["canonical_json_hash"], r2.binding_record["canonical_json_hash"])


if __name__ == "__main__":
    unittest.main()
