from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Union


class AtomicWriteError(RuntimeError):
    pass


def atomic_write_text(path: Union[str, Path], text: str, *, mode: int = 0o644) -> None:
    """
    Audit-grade atomic write:
    - write to temp file in same directory
    - fsync file
    - chmod
    - atomic replace to final path
    - fsync directory
    """
    p = Path(path)
    d = p.parent
    d.mkdir(parents=True, exist_ok=True)

    tmp_fd = None
    tmp_path = None
    try:
        tmp_fd, tmp_path = tempfile.mkstemp(prefix=f".{p.name}.tmp.", dir=str(d))
        with os.fdopen(tmp_fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        tmp_fd = None

        os.chmod(tmp_path, mode)
        os.replace(tmp_path, str(p))

        # fsync directory for durability
        dir_fd = os.open(str(d), os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    except Exception as e:
        raise AtomicWriteError(f"ATOMIC_WRITE_FAILED: path={str(p)!r} err={e!r}") from e
    finally:
        if tmp_fd is not None:
            try:
                os.close(tmp_fd)
            except Exception:
                pass
        if tmp_path is not None:
            try:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except Exception:
                pass
