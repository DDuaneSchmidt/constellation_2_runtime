#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from pathlib import Path


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
RUNNER = (REPO_ROOT / "constellation_2/phaseK_struct/run/run_phaseK_struct_day_v1.py").resolve()
OUT_DIR = (REPO_ROOT / "constellation_2/runtime/truth/certification_v1/phaseK_struct/2026-02-20").resolve()
HASH_PATH = (OUT_DIR / "sha256_manifest.v1.json").resolve()


def _run() -> None:
    if not RUNNER.exists():
        raise SystemExit(f"FAIL: missing runner: {RUNNER}")

    # Deterministic produced_utc and seed.
    cmd = [
        "python3",
        str(RUNNER),
        "--asof_day_utc",
        "2026-02-20",
        "--produced_utc",
        "2026-02-15T06:00:00Z",
        "--seed",
        "PHASEK_STRUCT_V1_SEED_FIXED",
        "--paths",
        "1000",
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise SystemExit(f"FAIL: runner failed:\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}")

    if not HASH_PATH.exists():
        raise SystemExit(f"FAIL: missing hash manifest: {HASH_PATH}")

    h1 = json.loads(HASH_PATH.read_text(encoding="utf-8"))
    # Run again, same args; must be identical hashes
    p2 = subprocess.run(cmd, capture_output=True, text=True)
    if p2.returncode != 0:
        raise SystemExit(f"FAIL: runner failed second run:\nSTDOUT:\n{p2.stdout}\nSTDERR:\n{p2.stderr}")

    h2 = json.loads(HASH_PATH.read_text(encoding="utf-8"))

    if h1.get("sha256") != h2.get("sha256"):
        raise SystemExit(f"FAIL: nondeterminism detected: h1={h1.get('sha256')} h2={h2.get('sha256')}")

    print("OK: phaseK_struct determinism (sha256 stable)")


if __name__ == "__main__":
    _run()
