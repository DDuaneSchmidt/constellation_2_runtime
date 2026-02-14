from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

LIFECYCLE_ROOT = (TRUTH_ROOT / "position_lifecycle_v1").resolve()


@dataclass(frozen=True)
class LifecycleDayPathsV1:
    day_utc: str
    snapshot_dir: Path
    snapshot_path: Path
    latest_path: Path
    failure_dir: Path
    failure_path: Path


def day_paths_v1(day_utc: str) -> LifecycleDayPathsV1:
    day = (day_utc or "").strip()
    if not day:
        raise ValueError("DAY_UTC_REQUIRED")

    snap_dir = (LIFECYCLE_ROOT / "snapshots" / day).resolve()
    fail_dir = (LIFECYCLE_ROOT / "failures" / day).resolve()

    return LifecycleDayPathsV1(
        day_utc=day,
        snapshot_dir=snap_dir,
        snapshot_path=(snap_dir / "position_lifecycle_snapshot.v1.json").resolve(),
        latest_path=(LIFECYCLE_ROOT / "latest.json").resolve(),
        failure_dir=fail_dir,
        failure_path=(fail_dir / "failure.json").resolve(),
    )
