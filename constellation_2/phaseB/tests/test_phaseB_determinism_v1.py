"""
test_phaseB_determinism_v1.py

Acceptance:
- Build snapshot+cert twice from identical raw input and assert:
  - snapshot canonical hash matches across runs
  - certificate snapshot_hash equals snapshot hash
  - cert canonical hash matches across runs
  - produced dicts are byte-identical under canonical JSON

Execution:
  python3 -m constellation_2.phaseB.tests.test_phaseB_determinism_v1
"""

from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any, Dict

from constellation_2.phaseB.lib.build_freshness_certificate_v1 import build_freshness_certificate_v1
from constellation_2.phaseB.lib.build_options_chain_snapshot_v1 import build_options_chain_snapshot_v1
from constellation_2.phaseB.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseB.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
PHASEB_INPUTS = REPO_ROOT / "constellation_2" / "phaseB" / "inputs"


def _load_json(path: Path) -> Any:
    import json

    return json.loads(path.read_text(encoding="utf-8"))


def _inject_hash(obj: Dict[str, Any]) -> Dict[str, Any]:
    h = canonical_hash_for_c2_artifact_v1(obj)
    out = dict(obj)
    out["canonical_json_hash"] = h
    return out


class TestPhaseBDeterminismV1(unittest.TestCase):
    def test_snapshot_and_cert_are_deterministic(self) -> None:
        raw = _load_json(PHASEB_INPUTS / "sample_raw_chain_input.v1.json")
        assert isinstance(raw, dict)

        # Run 1
        snap0_1 = build_options_chain_snapshot_v1(raw=raw, repo_root=REPO_ROOT)
        snap1 = _inject_hash(snap0_1)
        validate_against_repo_schema_v1(
            snap1,
            repo_root=REPO_ROOT,
            schema_relpath="constellation_2/schemas/options_chain_snapshot.v1.schema.json",
        )

        cert0_1 = build_freshness_certificate_v1(
            snapshot=snap1,
            repo_root=REPO_ROOT,
            max_age_seconds=300,
            clock_skew_tolerance_seconds=5,
        )
        cert1 = _inject_hash(cert0_1)
        validate_against_repo_schema_v1(
            cert1,
            repo_root=REPO_ROOT,
            schema_relpath="constellation_2/schemas/freshness_certificate.v1.schema.json",
        )

        # Run 2
        snap0_2 = build_options_chain_snapshot_v1(raw=raw, repo_root=REPO_ROOT)
        snap2 = _inject_hash(snap0_2)
        cert0_2 = build_freshness_certificate_v1(
            snapshot=snap2,
            repo_root=REPO_ROOT,
            max_age_seconds=300,
            clock_skew_tolerance_seconds=5,
        )
        cert2 = _inject_hash(cert0_2)

        # Hash equality
        self.assertEqual(snap1["canonical_json_hash"], snap2["canonical_json_hash"])
        self.assertEqual(cert1["canonical_json_hash"], cert2["canonical_json_hash"])

        # Binding: cert snapshot_hash equals snapshot hash (hash excludes self-hash by convention)
        self.assertEqual(cert1["snapshot_hash"], snap1["canonical_json_hash"])
        self.assertEqual(cert2["snapshot_hash"], snap2["canonical_json_hash"])

        # Canonical bytes identical
        self.assertEqual(canonical_json_bytes_v1(snap1), canonical_json_bytes_v1(snap2))
        self.assertEqual(canonical_json_bytes_v1(cert1), canonical_json_bytes_v1(cert2))


if __name__ == "__main__":
    unittest.main()
