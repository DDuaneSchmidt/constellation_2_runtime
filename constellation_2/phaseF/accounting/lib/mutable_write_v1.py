from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WriteResultV1:
    path: str
    sha256: str
    bytes_written: int
    action: str  # "WROTE" | "SKIP_IDENTICAL"


def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def write_file_atomic_mutable_v1(*, path: Path, data: bytes, create_dirs: bool = True) -> WriteResultV1:
    """
    Atomic mutable write rule (for LATEST POINTERS ONLY):

    - If file absent: atomically create via temp file + os.replace, return WROTE.
    - If file present:
        - If existing sha256 == candidate sha256 => SKIP_IDENTICAL (no write)
        - Else atomically replace file contents (temp file + os.replace), return WROTE.

    Non-goals:
    - Do NOT use for day-keyed immutable truth artifacts.
    """
    if create_dirs:
        path.parent.mkdir(parents=True, exist_ok=True)

    cand_sha = _sha256(data)

    if path.exists():
        if not path.is_file():
            raise RuntimeError(f"TARGET_NOT_FILE: {str(path)}")
        existing = path.read_bytes()
        ex_sha = _sha256(existing)
        if ex_sha == cand_sha:
            return WriteResultV1(path=str(path), sha256=cand_sha, bytes_written=0, action="SKIP_IDENTICAL")

    # Atomic write: temp file in same directory, then replace
    tmp_fd = None
    tmp_path = None
    try:
        fd, p = tempfile.mkstemp(prefix=".tmp_write_", dir=str(path.parent))
        tmp_fd = fd
        tmp_path = Path(p)
        os.write(tmp_fd, data)
        os.fsync(tmp_fd)
        os.close(tmp_fd)
        tmp_fd = None
        os.replace(str(tmp_path), str(path))
    finally:
        if tmp_fd is not None:
            try:
                os.close(tmp_fd)
            except Exception:
                pass
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass

    return WriteResultV1(path=str(path), sha256=cand_sha, bytes_written=len(data), action="WROTE")
