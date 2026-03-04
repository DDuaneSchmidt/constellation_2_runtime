#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

def main() -> int:
    ap = argparse.ArgumentParser(prog="c2_check_baseline_ready_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--ib_account", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    acct = str(args.ib_account).strip()

    p = (TRUTH_ROOT / "readiness_v1" / "baseline_ready" / day / "baseline_ready.v1.json").resolve()
    if not p.exists():
        raise SystemExit(f"FAIL: BASELINE_READY_SENTINEL_MISSING: {p}")

    o = json.load(open(p, "r", encoding="utf-8"))
    if str(o.get("day_utc") or "").strip() != day:
        raise SystemExit(f"FAIL: BASELINE_READY_DAY_MISMATCH: expected={day} got={o.get('day_utc')!r} path={p}")
    if str(o.get("ib_account") or "").strip() != acct:
        raise SystemExit(f"FAIL: BASELINE_READY_ACCOUNT_MISMATCH: expected={acct} got={o.get('ib_account')!r} path={p}")

    nav = o.get("nav") or {}
    nt = nav.get("nav_total")
    if not isinstance(nt, int) or nt <= 0:
        raise SystemExit(f"FAIL: BASELINE_READY_NAV_NOT_POSITIVE: nav_total={nt!r} path={p}")

    print(f"OK: BASELINE_READY_VALID day_utc={day} ib_account={acct} nav_total={nt} path={p}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
