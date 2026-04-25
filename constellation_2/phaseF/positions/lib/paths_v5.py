from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = resolve_canonical_truth_root().resolve()


@dataclass(frozen=True)
class DayPathsV5:
    snapshot_path: Path
    failure_path: Path


def day_paths_v5(day_utc: str) -> DayPathsV5:
    d = (TRUTH / "positions_v1/snapshots" / day_utc).resolve()
    return DayPathsV5(
        snapshot_path=(d / "positions_snapshot.v5.json").resolve(),
        failure_path=(TRUTH / "positions_v1/failures" / day_utc / "positions_snapshot.v5.failure.json").resolve(),
    )
