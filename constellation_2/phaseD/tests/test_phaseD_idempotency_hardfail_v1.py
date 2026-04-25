"""
test_phaseD_idempotency_hardfail_v1.py

Acceptance (Phase D Test B):
- Duplicate submission attempt for the same submission_id HARD FAILS.

Execution:
  constellation_2/.venv/bin/python -m constellation_2.phaseD.tests.test_phaseD_idempotency_hardfail_v1
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from constellation_2.phaseD.lib.idempotency_guard_v1 import (
    IdempotencyError,
    assert_idempotent_or_raise_v1,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


class TestPhaseDIdempotencyHardFailV1(unittest.TestCase):
    def test_duplicate_hard_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            sid = "a" * 64
            (root / sid).mkdir(parents=True, exist_ok=False)

            with self.assertRaises(IdempotencyError):
                assert_idempotent_or_raise_v1(submissions_root=root, submission_id=sid)

    def test_brokered_submission_blocks_retry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            sid = "b" * 64
            subdir = root / sid
            _write_json(
                subdir / "broker_submission_record.v2.json",
                {
                    "schema_id": "broker_submission_record",
                    "schema_version": "v2",
                    "status": "SUBMITTED",
                    "broker_transmitted": True,
                    "broker_ids": {"order_id": 101, "perm_id": 202},
                },
            )

            with self.assertRaises(IdempotencyError):
                assert_idempotent_or_raise_v1(submissions_root=root, submission_id=sid)

    def test_dry_run_only_submission_allows_retry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            sid = "c" * 64
            subdir = root / sid
            _write_json(
                subdir / "broker_submission_record.v2.json",
                {
                    "schema_id": "broker_submission_record",
                    "schema_version": "v2",
                    "status": "PENDINGSUBMIT",
                    "dry_run": True,
                    "broker_transmitted": False,
                    "broker_ids": {"order_id": None, "perm_id": None},
                    "error": {"code": "DRY_RUN_NO_BROKER_ID"},
                },
            )

            check = assert_idempotent_or_raise_v1(submissions_root=root, submission_id=sid)
            self.assertTrue(check.retry_allowed)
            self.assertEqual(check.classification, "B")

    def test_veto_only_submission_allows_retry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            sid = "d" * 64
            subdir = root / sid
            _write_json(
                subdir / "veto_record.v1.json",
                {
                    "schema_id": "veto_record",
                    "schema_version": "v1",
                    "reason_code": "C2_SUBMIT_AUTHZ_NOT_AUTHORIZED",
                },
            )

            check = assert_idempotent_or_raise_v1(submissions_root=root, submission_id=sid)
            self.assertTrue(check.retry_allowed)
            self.assertEqual(check.classification, "C")

    def test_failed_before_broker_submission_allows_retry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            sid = "e" * 64
            subdir = root / sid
            _write_json(
                subdir / "broker_submission_record.v2.json",
                {
                    "schema_id": "broker_submission_record",
                    "schema_version": "v2",
                    "status": "REJECTED",
                    "dry_run": False,
                    "broker_transmitted": False,
                    "broker_ids": {"order_id": None, "perm_id": None},
                    "error": {"code": "IB_REJECTED"},
                },
            )
            _write_json(
                subdir / "veto_record.v1.json",
                {
                    "schema_id": "veto_record",
                    "schema_version": "v1",
                    "reason_code": "SUBMISSION_REJECTED",
                },
            )

            check = assert_idempotent_or_raise_v1(submissions_root=root, submission_id=sid)
            self.assertTrue(check.retry_allowed)
            self.assertEqual(check.classification, "D")

    def test_dry_run_marker_without_dry_run_flag_allows_retry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            sid = "g" * 64
            subdir = root / sid
            _write_json(
                subdir / "broker_submission_record.v2.json",
                {
                    "schema_id": "broker_submission_record",
                    "schema_version": "v2",
                    "status": "PENDINGSUBMIT",
                    "broker_ids": {"order_id": None, "perm_id": None},
                    "error": {"code": "DRY_RUN_NO_BROKER_ID"},
                },
            )

            check = assert_idempotent_or_raise_v1(submissions_root=root, submission_id=sid)
            self.assertTrue(check.retry_allowed)
            self.assertEqual(check.classification, "B")

    def test_ambiguous_broker_record_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            sid = "f" * 64
            subdir = root / sid
            _write_json(
                subdir / "broker_submission_record.v2.json",
                {
                    "schema_id": "broker_submission_record",
                    "schema_version": "v2",
                    "status": "SUBMITTED",
                    "broker_transmitted": "UNKNOWN",
                    "broker_ids": {"order_id": None, "perm_id": None},
                },
            )

            with self.assertRaises(IdempotencyError):
                assert_idempotent_or_raise_v1(submissions_root=root, submission_id=sid)

    def test_retry_does_not_overwrite_prior_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            sid = "0" * 64
            subdir = root / sid
            veto_path = subdir / "veto_record.v1.json"
            original_payload = {
                "schema_id": "veto_record",
                "schema_version": "v1",
                "reason_code": "UPSTREAM_BLOCKED",
            }
            _write_json(veto_path, original_payload)
            before = veto_path.read_text(encoding="utf-8")

            check = assert_idempotent_or_raise_v1(submissions_root=root, submission_id=sid)
            after = veto_path.read_text(encoding="utf-8")

            self.assertTrue(check.retry_allowed)
            self.assertEqual(check.classification, "C")
            self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
