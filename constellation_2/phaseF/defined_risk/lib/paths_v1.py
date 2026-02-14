from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

RISK_ROOT = (TRUTH_ROOT / "defined_risk_v1").resolve()


@dataclass(frozen=True)
class DefinedRiskDayPathsV1:
    day_utc: str
    snapshot_dir: Path
    snapshot_path: Path
    latest_path: Path
    failure_dir: Path
    failure_path: Path


def day_paths_v1(day_utc: str) -> DefinedRiskDayPathsV1:
    day = (day_utc or "").strip()
    if not day:
        raise ValueError("DAY_UTC_REQUIRED")

    snap_dir = (RISK_ROOT / "snapshots" / day).resolve()
    fail_dir = (RISK_ROOT / "failures" / day).resolve()

    return DefinedRiskDayPathsV1(
        day_utc=day,
        snapshot_dir=snap_dir,
        snapshot_path=(snap_dir / "defined_risk_snapshot.v1.json").resolve(),
        latest_path=(RISK_ROOT / "latest.json").resolve(),
        failure_dir=fail_dir,
        failure_path=(fail_dir / "failure.json").resolve(),
    )
