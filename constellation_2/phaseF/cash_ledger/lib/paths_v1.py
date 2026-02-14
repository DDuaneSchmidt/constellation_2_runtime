from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

CASH_LEDGER_ROOT = (TRUTH_ROOT / "cash_ledger_v1").resolve()


@dataclass(frozen=True)
class CashLedgerDayPathsV1:
    day_utc: str
    snapshot_dir: Path
    snapshot_path: Path
    latest_path: Path
    failure_dir: Path
    failure_path: Path


def day_paths_v1(day_utc: str) -> CashLedgerDayPathsV1:
    day = (day_utc or "").strip()
    if not day:
        raise ValueError("DAY_UTC_REQUIRED")

    snap_dir = CASH_LEDGER_ROOT / "snapshots" / day
    failure_dir = CASH_LEDGER_ROOT / "failures" / day

    return CashLedgerDayPathsV1(
        day_utc=day,
        snapshot_dir=snap_dir,
        snapshot_path=snap_dir / "cash_ledger_snapshot.v1.json",
        latest_path=CASH_LEDGER_ROOT / "latest.json",
        failure_dir=failure_dir,
        failure_path=failure_dir / "failure.json",
    )
