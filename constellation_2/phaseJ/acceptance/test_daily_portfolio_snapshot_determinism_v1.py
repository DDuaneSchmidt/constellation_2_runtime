#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
WRITER = (REPO_ROOT / "constellation_2/phaseJ/reporting/daily_snapshot_v1.py").resolve()
OUT_DIR = (REPO_ROOT / "constellation_2/runtime/truth/reports").resolve()
OUT_FILE = (OUT_DIR / "daily_portfolio_snapshot_20260220.json").resolve()


def _sha256_bytes(b: bytes) -> str:
    import hashlib
    return hashlib.sha256(b).hexdigest()


def _run(allow_degraded: str) -> subprocess.CompletedProcess:
    cmd = [
        "python3",
        str(WRITER),
        "--day_utc",
        "2026-02-20",
        "--produced_utc",
        "2026-02-15T06:00:00Z",
        "--seed",
        "DAILY_SNAPSHOT_V1_SEED_FIXED",
        "--allow_degraded_report",
        allow_degraded,
    ]
    return subprocess.run(cmd, capture_output=True, text=True)


def main() -> None:
    if not WRITER.exists():
        raise SystemExit(f"FAIL: missing writer: {WRITER}")

    # 1) STRICT mode must FAIL closed for this day (engine_metrics reconciliation ok=false)
    p = _run("false")
    if p.returncode == 0:
        raise SystemExit("FAIL: strict mode unexpectedly succeeded (should fail-closed on engine_metrics reconciliation)")

    if "RECON_FAIL_ENGINE_METRICS_REPORTED_NOT_OK" not in (p.stderr or ""):
        raise SystemExit(f"FAIL: strict mode failed, but wrong reason:\nSTDERR:\n{p.stderr}")

    # 2) Degraded-allowed mode must SUCCEED and be deterministic/immutable
    p2 = _run("true")
    if p2.returncode != 0:
        raise SystemExit(f"FAIL: degraded mode failed:\nSTDOUT:\n{p2.stdout}\nSTDERR:\n{p2.stderr}")

    if not OUT_FILE.exists():
        raise SystemExit(f"FAIL: missing output: {OUT_FILE}")

    b1 = OUT_FILE.read_bytes()
    h1 = _sha256_bytes(b1)

    # Run again with same args: must not change bytes
    p3 = _run("true")
    if p3.returncode != 0:
        raise SystemExit(f"FAIL: degraded mode failed second run:\nSTDOUT:\n{p3.stdout}\nSTDERR:\n{p3.stderr}")

    b2 = OUT_FILE.read_bytes()
    h2 = _sha256_bytes(b2)

    if h1 != h2:
        raise SystemExit(f"FAIL: nondeterminism/immutability breach: h1={h1} h2={h2}")

    # Top-level keys exact
    obj = json.loads(b2.decode("utf-8"))
    got = sorted(list(obj.keys()))
    exp = sorted(["meta", "portfolio", "sleeves", "risk", "statistics", "attribution", "compliance"])
    if got != exp:
        raise SystemExit(f"FAIL: top-level keys mismatch got={got} exp={exp}")

    # Degraded mode must be explicit in compliance/risk
    comp = obj.get("compliance", {})
    risk = obj.get("risk", {})
    if comp.get("within_10_percent_mandate_envelope") is not False:
        raise SystemExit("FAIL: degraded mode must set within_10_percent_mandate_envelope=false")
    if comp.get("risk_identity_compliant") is not False:
        raise SystemExit("FAIL: degraded mode must set risk_identity_compliant=false")
    if risk.get("risk_violations_today") is not True:
        raise SystemExit("FAIL: degraded mode must set risk_violations_today=true")

    print("OK: daily_portfolio_snapshot strict fail-closed + degraded deterministic immutable output")


if __name__ == "__main__":
    main()
