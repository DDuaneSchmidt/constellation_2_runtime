from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

POSITIONS_ROOT = (TRUTH_ROOT / "positions_v1").resolve()
EFFECTIVE_ROOT = (POSITIONS_ROOT / "effective_v1").resolve()


@dataclass(frozen=True)
class PositionsEffectiveDayPathsV1:
    day_utc: str
    out_dir: Path
    pointer_path: Path
    latest_effective_path: Path
    failure_dir: Path
    failure_path: Path


def day_paths_effective_v1(day_utc: str) -> PositionsEffectiveDayPathsV1:
    day = (day_utc or "").strip()
    if not day:
        raise ValueError("DAY_UTC_REQUIRED")

    out_dir = EFFECTIVE_ROOT / "days" / day
    failure_dir = EFFECTIVE_ROOT / "failures" / day

    return PositionsEffectiveDayPathsV1(
        day_utc=day,
        out_dir=out_dir,
        pointer_path=out_dir / "positions_effective_pointer.v1.json",
        latest_effective_path=EFFECTIVE_ROOT / "latest_effective.json",
        failure_dir=failure_dir,
        failure_path=failure_dir / "failure.json",
    )
