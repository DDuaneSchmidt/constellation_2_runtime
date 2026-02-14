from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

POSITIONS_ROOT = (TRUTH_ROOT / "positions_v1").resolve()


@dataclass(frozen=True)
class PositionsDayPathsV3:
    day_utc: str
    snapshot_dir: Path
    snapshot_path: Path
    latest_v3_path: Path
    failure_dir: Path
    failure_path: Path


def day_paths_v3(day_utc: str) -> PositionsDayPathsV3:
    day = (day_utc or "").strip()
    if not day:
        raise ValueError("DAY_UTC_REQUIRED")

    snap_dir = POSITIONS_ROOT / "snapshots" / day
    failure_dir = POSITIONS_ROOT / "failures" / day

    return PositionsDayPathsV3(
        day_utc=day,
        snapshot_dir=snap_dir,
        snapshot_path=snap_dir / "positions_snapshot.v3.json",
        latest_v3_path=POSITIONS_ROOT / "latest_v3.json",
        failure_dir=failure_dir,
        failure_path=failure_dir / "failure_v3.json",
    )
