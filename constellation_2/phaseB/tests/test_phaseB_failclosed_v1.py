"""
test_phaseB_failclosed_v1.py

Acceptance:
- Invalid raw input => fail-closed (exception) and no partial outputs.

Execution:
  python3 -m constellation_2.phaseB.tests.test_phaseB_failclosed_v1
"""

from __future__ import annotations

import copy
import unittest
from pathlib import Path
from typing import Any

from constellation_2.phaseB.lib.build_options_chain_snapshot_v1 import RawInputError, build_options_chain_snapshot_v1


REPO_ROOT = Path(__file__).resolve().parents[3]
PHASEB_INPUTS = REPO_ROOT / "constellation_2" / "phaseB" / "inputs"


def _load_json(path: Path) -> Any:
    import json

    return json.loads(path.read_text(encoding="utf-8"))


class TestPhaseBFailClosedV1(unittest.TestCase):
    def test_missing_required_field_raises(self) -> None:
        raw = _load_json(PHASEB_INPUTS / "sample_raw_chain_input.v1.json")
        assert isinstance(raw, dict)
        bad = copy.deepcopy(raw)

        # Remove required field: underlying.symbol
        assert "underlying" in bad and isinstance(bad["underlying"], dict)
        bad["underlying"].pop("symbol", None)

        with self.assertRaises(RawInputError):
            _ = build_options_chain_snapshot_v1(raw=bad, repo_root=REPO_ROOT)

    def test_invalid_multiplier_raises(self) -> None:
        raw = _load_json(PHASEB_INPUTS / "sample_raw_chain_input.v1.json")
        assert isinstance(raw, dict)
        bad = copy.deepcopy(raw)

        # Force invalid multiplier
        assert isinstance(bad.get("contracts"), list) and len(bad["contracts"]) > 0
        assert isinstance(bad["contracts"][0], dict)
        assert isinstance(bad["contracts"][0].get("ib"), dict)
        bad["contracts"][0]["ib"]["multiplier"] = 10

        with self.assertRaises(RawInputError):
            _ = build_options_chain_snapshot_v1(raw=bad, repo_root=REPO_ROOT)


if __name__ == "__main__":
    unittest.main()
