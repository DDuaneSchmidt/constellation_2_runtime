from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

from .artifact_store import DEFAULT_STORE_ROOT

DEFAULT_STALE_AFTER_SECONDS = 60 * 60


@dataclass(frozen=True)
class SchedulerLockResult:
    acquired: bool
    lock_id: str
    lock_path: str
    reason: str = ""
    stale_lock_cleaned: bool = False
    existing_lock: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def scheduler_lock_path(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = Path(root) / "scheduler" / "scheduler.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


@contextmanager
def scheduler_lock(root: str | Path = DEFAULT_STORE_ROOT, *, owner_id: str, trigger_id: str, stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS) -> Iterator[SchedulerLockResult]:
    lock_path = scheduler_lock_path(root)
    cleaned = cleanup_stale_scheduler_lock(root, stale_after_seconds=stale_after_seconds)
    lock_id = f"scheduler-lock-{owner_id}"
    payload = {
        "lock_id": lock_id,
        "owner_id": owner_id,
        "trigger_id": trigger_id,
        "created_at": now_utc(),
        "pid": os.getpid(),
    }
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        existing = read_scheduler_lock(root)
        yield SchedulerLockResult(False, "", str(lock_path), reason="LOCK_CONFLICT", stale_lock_cleaned=cleaned, existing_lock=existing)
        return
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        yield SchedulerLockResult(True, lock_id, str(lock_path), stale_lock_cleaned=cleaned)
    finally:
        try:
            current = read_scheduler_lock(root)
            if current.get("lock_id") == lock_id:
                lock_path.unlink()
        except FileNotFoundError:
            pass


def cleanup_stale_scheduler_lock(root: str | Path = DEFAULT_STORE_ROOT, *, stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS) -> bool:
    path = scheduler_lock_path(root)
    if not path.exists():
        return False
    payload = read_scheduler_lock(root)
    created_at = payload.get("created_at")
    if not created_at:
        path.unlink()
        return True
    age = (datetime.now(UTC) - parse_time(str(created_at))).total_seconds()
    if age > stale_after_seconds:
        path.unlink()
        return True
    return False


def read_scheduler_lock(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    path = scheduler_lock_path(root)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def parse_time(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
