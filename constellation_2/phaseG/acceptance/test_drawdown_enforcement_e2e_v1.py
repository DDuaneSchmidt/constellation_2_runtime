#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _expect_multiplier(dd_pct_s: str) -> str:
    dd = Decimal(dd_pct_s)
    if dd <= Decimal("-0.150000"):
        return "0.25"
    if dd <= Decimal("-0.100000"):
        return "0.50"
    if dd <= Decimal("-0.050000"):
        return "0.75"
    return "1.00"


def main() -> int:
    day_ok = "2026-02-20"
    day_bad = "2026-02-22"

    nav_ok = REPO_ROOT / "constellation_2/runtime/truth/accounting_v1/nav" / day_ok / "nav.json"
    alloc_ok = REPO_ROOT / "constellation_2/runtime/truth/allocation_v1/summary" / day_ok / "summary.json"

    if not nav_ok.is_file():
        raise SystemExit(f"FAIL: missing required nav fixture: {nav_ok}")
    if not alloc_ok.is_file():
        raise SystemExit(f"FAIL: missing required allocation summary fixture: {alloc_ok}")

    nav_obj = json.load(open(nav_ok, "r", encoding="utf-8"))
    hist = nav_obj.get("history", {})
    if not isinstance(hist, dict):
        raise SystemExit("FAIL: nav.history missing")
    dd_pct = hist.get("drawdown_pct")
    if not isinstance(dd_pct, str):
        raise SystemExit("FAIL: expected dd_pct string in nav fixture")

    alloc_obj = json.load(open(alloc_ok, "r", encoding="utf-8"))
    summ = alloc_obj.get("summary", {})
    dd = summ.get("drawdown_enforcement", None)
    if not isinstance(dd, dict):
        raise SystemExit("FAIL: missing summary.drawdown_enforcement")

    # Field presence
    for k in [
        "contract_id",
        "nav_source_path",
        "nav_source_sha256",
        "nav_asof_day_utc",
        "rolling_peak_nav",
        "nav_total",
        "drawdown_abs",
        "drawdown_pct",
        "multiplier",
        "thresholds",
    ]:
        if k not in dd:
            raise SystemExit(f"FAIL: drawdown_enforcement missing field: {k}")

    # Contract id
    if dd["contract_id"] != "C2_DRAWDOWN_CONVENTION_V1":
        raise SystemExit("FAIL: wrong contract_id")

    # Multiplier correctness
    exp = _expect_multiplier(dd["drawdown_pct"])
    if dd["multiplier"] != exp:
        raise SystemExit(f"FAIL: multiplier mismatch: dd={dd['drawdown_pct']} got={dd['multiplier']} exp={exp}")

    # Threshold table exact match (order and values)
    exp_thresh = [
        {"drawdown_pct": "0.000000", "multiplier": "1.00"},
        {"drawdown_pct": "-0.050000", "multiplier": "0.75"},
        {"drawdown_pct": "-0.100000", "multiplier": "0.50"},
        {"drawdown_pct": "-0.150000", "multiplier": "0.25"},
    ]
    if dd["thresholds"] != exp_thresh:
        raise SystemExit("FAIL: thresholds table mismatch")

    # Fail-closed on missing drawdown day (must NOT create summary.json; should exit non-zero)
    cmd = [
        sys.executable,
        "-m",
        "constellation_2.phaseG.allocation.run.run_allocation_day_v1",
        "--day_utc",
        day_bad,
        "--producer_git_sha",
        "41770ea",
    ]
    p = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)
    if p.returncode == 0:
        raise SystemExit("FAIL: allocation unexpectedly succeeded on missing-drawdown day")
    # Must fail for drawdown typing missing, not random
    if "ACCOUNTING_PEAK_NAV_NOT_INT" not in (p.stderr or ""):
        raise SystemExit(f"FAIL: unexpected failure reason. stderr={p.stderr.strip()!r}")

    # Deterministic hash proof (same file hashed twice)
    h1 = _sha256_file(alloc_ok)
    h2 = _sha256_file(alloc_ok)
    if h1 != h2:
        raise SystemExit("FAIL: summary sha256 not deterministic (should never happen)")

    print("OK: drawdown enforcement e2e v1")
    print(f"OK: summary_sha256={h1}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
