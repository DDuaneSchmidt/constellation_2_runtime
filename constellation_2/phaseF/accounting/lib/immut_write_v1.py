from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


class ImmutableWriteError(Exception):
    pass


@dataclass(frozen=True)
class WriteResultV1:
    path: str
    sha256: str
    bytes_written: int
    action: str  # "WROTE" | "SKIP_IDENTICAL"


def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def write_file_immutable_v1(*, path: Path, data: bytes, create_dirs: bool = True) -> WriteResultV1:
    """
    Immutable write rule:
    - If file absent: write bytes, return WROTE.
    - If file present:
        - if existing bytes sha256 == candidate sha256 => SKIP_IDENTICAL
        - else raise ImmutableWriteError (ATTEMPTED_REWRITE)
    """
    if create_dirs:
        path.parent.mkdir(parents=True, exist_ok=True)

    cand_sha = _sha256(data)

    if path.exists():
        if not path.is_file():
            raise ImmutableWriteError(f"TARGET_NOT_FILE: {str(path)}")
        existing = path.read_bytes()
        ex_sha = _sha256(existing)
        if ex_sha == cand_sha:
            return WriteResultV1(path=str(path), sha256=cand_sha, bytes_written=0, action="SKIP_IDENTICAL")
        raise ImmutableWriteError(f"ATTEMPTED_REWRITE: {str(path)} existing_sha={ex_sha} candidate_sha={cand_sha}")

    path.write_bytes(data)
    return WriteResultV1(path=str(path), sha256=cand_sha, bytes_written=len(data), action="WROTE")
