from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()
POSITIONS_ROOT = (TRUTH_ROOT / "positions_v1").resolve()


@dataclass(frozen=True)
class DayPathsV4:
    snapshot_path: Path
    failure_path: Path


def day_paths_v4(day_utc: str) -> DayPathsV4:
    day = (day_utc or "").strip()
    snap = (POSITIONS_ROOT / "snapshots" / day / "positions_snapshot.v4.json").resolve()
    fail = (POSITIONS_ROOT / "failures" / day / "failure_v4.json").resolve()
    return DayPathsV4(snapshot_path=snap, failure_path=fail)
