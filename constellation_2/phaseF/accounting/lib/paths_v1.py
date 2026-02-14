from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()

TRUTH_ROOT = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

ACCOUNTING_ROOT = (TRUTH_ROOT / "accounting_v1").resolve()


@dataclass(frozen=True)
class AccountingDayPathsV1:
    day_utc: str

    nav_path: Path
    exposure_path: Path
    attribution_path: Path
    failure_path: Path

    nav_dir: Path
    exposure_dir: Path
    attribution_dir: Path
    failure_dir: Path

    latest_path: Path


def day_paths_v1(day_utc: str) -> AccountingDayPathsV1:
    day = (day_utc or "").strip()
    if not day:
        raise ValueError("DAY_UTC_REQUIRED")

    nav_dir = ACCOUNTING_ROOT / "nav" / day
    exposure_dir = ACCOUNTING_ROOT / "exposure" / day
    attribution_dir = ACCOUNTING_ROOT / "attribution" / day
    failure_dir = ACCOUNTING_ROOT / "failures" / day

    return AccountingDayPathsV1(
        day_utc=day,
        nav_dir=nav_dir,
        exposure_dir=exposure_dir,
        attribution_dir=attribution_dir,
        failure_dir=failure_dir,
        nav_path=nav_dir / "nav.json",
        exposure_path=exposure_dir / "exposure.json",
        attribution_path=attribution_dir / "engine_attribution.json",
        failure_path=failure_dir / "failure.json",
        latest_path=ACCOUNTING_ROOT / "latest.json",
    )
