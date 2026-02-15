#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Set


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
WRITER = (REPO_ROOT / "constellation_2/phaseJ/reporting/daily_snapshot_v1.py").resolve()

OUT_DIR = (REPO_ROOT / "constellation_2/runtime/truth/reports").resolve()
NAV_ROOT = (REPO_ROOT / "constellation_2/runtime/truth/accounting_v1/nav").resolve()
NAV_SERIES_ROOT = (REPO_ROOT / "constellation_2/runtime/truth/monitoring_v1/nav_series").resolve()

DEFAULT_SEED = "DAILY_SNAPSHOT_V1_SEED_FIXED"


def _die(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    raise SystemExit(2)


def _parse_bool(s: str, what: str) -> bool:
    v = (s or "").strip().lower()
    if v in ("1", "true", "yes", "y"):
        return True
    if v in ("0", "false", "no", "n"):
        return False
    _die(f"{what}_INVALID_BOOL")


def _produced_utc_for_day(day_utc: str) -> str:
    # Deterministic: fixed at start-of-day UTC.
    return f"{day_utc}T00:00:00Z"


def _out_path_for_day(day_utc: str) -> Path:
    ymd = day_utc.replace("-", "")
    return (OUT_DIR / f"daily_portfolio_snapshot_{ymd}.json").resolve()


def _list_day_dirs_or_fail(root: Path, name: str) -> List[str]:
    if not root.exists() or not root.is_dir():
        _die(f"{name}_ROOT_MISSING: {root}")
    days = sorted([p.name for p in root.iterdir() if p.is_dir()])
    if not days:
        _die(f"NO_{name}_DAYS_PRESENT")
    return days


def _latest_overlap_day_or_fail() -> str:
    nav_days = set(_list_day_dirs_or_fail(NAV_ROOT, "ACCOUNTING_NAV"))
    ns_days = set(_list_day_dirs_or_fail(NAV_SERIES_ROOT, "NAV_SERIES"))
    overlap = sorted(nav_days.intersection(ns_days))
    if not overlap:
        _die("NO_OVERLAP_DAYS_BETWEEN_ACCOUNTING_NAV_AND_NAV_SERIES")
    return overlap[-1]


def _run_writer(day_utc: str, seed: str, allow_degraded: bool) -> None:
    if not WRITER.exists():
        _die(f"WRITER_MISSING: {WRITER}")

    produced_utc = _produced_utc_for_day(day_utc)
    out_path = _out_path_for_day(day_utc)

    cmd = [
        "python3",
        str(WRITER),
        "--day_utc",
        day_utc,
        "--produced_utc",
        produced_utc,
        "--seed",
        seed,
        "--allow_degraded_report",
        "true" if allow_degraded else "false",
    ]

    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        _die(f"WRITER_FAILED day={day_utc}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}")

    if not out_path.exists():
        _die(f"OUTPUT_MISSING_AFTER_WRITE: {out_path}")

    print(f"OK: DAILY_SNAPSHOT_AUTO day={day_utc} out={out_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Auto-generate daily consolidated portfolio snapshot (deterministic; dependency-safe).")
    ap.add_argument("--day_utc", default="", help="YYYY-MM-DD. If omitted: uses latest overlap of accounting NAV and nav_series.")
    ap.add_argument("--seed", default=DEFAULT_SEED, help="Deterministic seed (default fixed).")
    ap.add_argument("--allow_degraded", default="true", help="true/false. Default true.")
    ap.add_argument("--backfill_last_n_days", default="0", help="0 or positive int. Uses overlap days; writes only missing days.")
    args = ap.parse_args()

    allow_degraded = _parse_bool(args.allow_degraded, "ALLOW_DEGRADED")

    try:
        n = int((args.backfill_last_n_days or "0").strip())
    except Exception:
        _die("BACKFILL_N_NOT_INT")
    if n < 0:
        _die("BACKFILL_N_NEGATIVE")

    day = (args.day_utc or "").strip()
    if not day:
        day = _latest_overlap_day_or_fail()

    if n > 0:
        nav_days = set(_list_day_dirs_or_fail(NAV_ROOT, "ACCOUNTING_NAV"))
        ns_days = set(_list_day_dirs_or_fail(NAV_SERIES_ROOT, "NAV_SERIES"))
        overlap = sorted(nav_days.intersection(ns_days))
        if day not in overlap:
            _die(f"BACKFILL_END_DAY_NOT_IN_OVERLAP: {day}")
        end_idx = overlap.index(day)
        start_idx = max(0, end_idx - (n - 1))
        sel = overlap[start_idx : end_idx + 1]

        for d in sel:
            outp = _out_path_for_day(d)
            if outp.exists():
                print(f"OK: SKIP_EXISTS day={d} out={outp}")
                continue
            _run_writer(d, seed=(args.seed or DEFAULT_SEED), allow_degraded=allow_degraded)
        return

    _run_writer(day, seed=(args.seed or DEFAULT_SEED), allow_degraded=allow_degraded)


if __name__ == "__main__":
    main()
