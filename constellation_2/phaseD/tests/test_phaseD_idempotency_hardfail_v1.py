"""
test_phaseD_idempotency_hardfail_v1.py

Acceptance (Phase D Test B):
- Duplicate submission attempt for the same submission_id HARD FAILS.

Execution:
  constellation_2/.venv/bin/python -m constellation_2.phaseD.tests.test_phaseD_idempotency_hardfail_v1
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from constellation_2.phaseD.lib.idempotency_guard_v1 import IdempotencyError, assert_idempotent_or_raise_v1


class TestPhaseDIdempotencyHardFailV1(unittest.TestCase):
    def test_duplicate_hard_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            sid = "a" * 64
            (root / sid).mkdir(parents=True, exist_ok=False)

            with self.assertRaises(IdempotencyError):
                assert_idempotent_or_raise_v1(submissions_root=root, submission_id=sid)


if __name__ == "__main__":
    unittest.main()
