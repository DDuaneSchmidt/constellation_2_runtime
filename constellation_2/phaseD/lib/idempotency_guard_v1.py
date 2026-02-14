"""
idempotency_guard_v1.py

Constellation 2.0 Phase D
Idempotent submission control (HARD FAIL on duplicates).

Authority:
- constellation_2/governance/C2_INVARIANTS_AND_REASON_CODES.md
  - C2_IDEMPOTENCY_DUPLICATE_SUBMISSION (HARD FAIL)
- constellation_2/governance/C2_DETERMINISM_STANDARD.md

Rule:
- submission_id MUST be derived from binding_hash (deterministic)
- If any prior submission evidence exists for the same submission_id => HARD FAIL
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


class IdempotencyError(Exception):
    pass


def derive_submission_id_from_binding_hash_v1(binding_hash: str) -> str:
    """
    Deterministic: submission_id is the binding_hash itself (64 hex chars).
    This is stable, reproducible, and meets schema constraints (minLength 16).
    """
    if not isinstance(binding_hash, str) or len(binding_hash) != 64:
        raise IdempotencyError("BINDING_HASH_INVALID_FOR_SUBMISSION_ID")
    return binding_hash


@dataclass(frozen=True)
class IdempotencyCheckV1:
    submission_id: str
    already_exists: bool
    existing_path: Optional[str]


def assert_idempotent_or_raise_v1(*, submissions_root: Path, submission_id: str) -> None:
    """
    HARD FAIL if a submission with this id already exists under submissions_root.
    """
    if not submissions_root.exists():
        # allow root to be created by caller; absence is not a duplicate.
        return
    if not submissions_root.is_dir():
        raise IdempotencyError(f"SUBMISSIONS_ROOT_NOT_DIRECTORY: {str(submissions_root)}")

    p = submissions_root / submission_id
    if p.exists():
        raise IdempotencyError(f"C2_IDEMPOTENCY_DUPLICATE_SUBMISSION: {submission_id} already exists at {str(p)}")
