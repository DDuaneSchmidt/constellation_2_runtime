from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path


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


def _fsync_dir(path: Path) -> None:
    d = path.parent
    dir_fd = os.open(str(d), os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def write_file_immutable_v1(*, path: Path, data: bytes, create_dirs: bool = True) -> WriteResultV1:
    """
    Immutable write rule (audit-grade atomic publish):

    - If file absent: atomically create (temp file in same dir + fsync(file) + os.replace + fsync(dir)), return WROTE.
    - If file present:
        - if existing bytes sha256 == candidate sha256 => SKIP_IDENTICAL
        - else raise ImmutableWriteError (ATTEMPTED_REWRITE)

    Crash safety:
    - No partial write can appear at the final path.
    - Final directory entry is fsync'd for durability.
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
        raise ImmutableWriteError(
            f"ATTEMPTED_REWRITE: {str(path)} existing_sha={ex_sha} candidate_sha={cand_sha}"
        )

    tmp_fd: int | None = None
    tmp_path: str | None = None
    try:
        tmp_fd, tmp_path = tempfile.mkstemp(prefix=f".{path.name}.tmp.", dir=str(path.parent))
        os.write(tmp_fd, data)
        os.fsync(tmp_fd)
        os.close(tmp_fd)
        tmp_fd = None

        os.replace(tmp_path, str(path))
        _fsync_dir(path)
    except Exception as e:
        raise ImmutableWriteError(f"ATOMIC_IMMUTABLE_WRITE_FAILED: {str(path)} err={e!r}") from e
    finally:
        if tmp_fd is not None:
            try:
                os.close(tmp_fd)
            except Exception:
                pass
        if tmp_path is not None and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

return WriteResultV1(path=str(path), sha256=cand_sha, bytes_written=len(data), action="WROTE")
