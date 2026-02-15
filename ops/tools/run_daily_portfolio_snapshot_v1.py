#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
WRITER = (REPO_ROOT / "constellation_2/phaseJ/reporting/daily_snapshot_v1.py").resolve()

OUT_DIR = (REPO_ROOT / "constellation_2/runtime/truth/reports").resolve()

DEFAULT_SEED = "DAILY_SNAPSHOT_V1_SEED_FIXED"


def _die(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    raise SystemExit(2)


def _utc_today_ymd() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _produced_utc_for_day(day_utc: str) -> str:
    # Deterministic: generated timestamp is fixed at start-of-day UTC.
    return f"{day_utc}T00:00:00Z"


def _out_path_for_day(day_utc: str) -> Path:
    ymd = day_utc.replace("-", "")
    return (OUT_DIR / f"daily_portfolio_snapshot_{ymd}.json").resolve()


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

    # Proof output exists
    if not out_path.exists():
        _die(f"OUTPUT_MISSING_AFTER_WRITE: {out_path}")

    print(f"OK: DAILY_SNAPSHOT_AUTO day={day_utc} out={out_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Repo-native runner: generate daily consolidated portfolio snapshot deterministically (no broker calls).")
    ap.add_argument("--day_utc", default="", help="YYYY-MM-DD. Default: today UTC.")
    ap.add_argument("--seed", default=DEFAULT_SEED, help="Deterministic seed (default fixed).")
    ap.add_argument("--allow_degraded", default="true", help="true/false. Default true.")
    ap.add_argument("--backfill_last_n_days", default="0", help="0 or positive int. Writes only missing days, fails if writer fails.")
    args = ap.parse_args()

    day = (args.day_utc or "").strip()
    if not day:
        day = _utc_today_ymd()

    allow_degraded_s = (args.allow_degraded or "").strip().lower()
    if allow_degraded_s not in ("true", "false"):
        _die("ALLOW_DEGRADED_INVALID_BOOL")
    allow_degraded = (allow_degraded_s == "true")

    try:
        n = int((args.backfill_last_n_days or "0").strip())
    except Exception:
        _die("BACKFILL_N_NOT_INT")
    if n < 0:
        _die("BACKFILL_N_NEGATIVE")

    # Backfill mode: generate for last N days inclusive ending at day_utc, but only if file missing.
    if n > 0:
        # We intentionally avoid calendar assumptions beyond simple day stepping.
        from datetime import date, timedelta

        y, m, d = [int(x) for x in day.split("-")]
        end = date(y, m, d)

        for i in range(n):
            cur = end - timedelta(days=(n - 1 - i))
            cur_s = cur.isoformat()
            outp = _out_path_for_day(cur_s)
            if outp.exists():
                print(f"OK: SKIP_EXISTS day={cur_s} out={outp}")
                continue
            _run_writer(cur_s, seed=(args.seed or DEFAULT_SEED), allow_degraded=allow_degraded)
        return

    # Single day mode
    _run_writer(day, seed=(args.seed or DEFAULT_SEED), allow_degraded=allow_degraded)


if __name__ == "__main__":
    main()
