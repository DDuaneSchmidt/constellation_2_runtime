#!/usr/bin/env python3
"""
verify_engine_attribution_reconciliation_v1.py

Phase J hostile-review readiness gate:
- Engine attribution reconciliation must be proven (sum engine contributions == portfolio return).

This script FAILS CLOSED if:
- monitoring engine_metrics artifact missing
- JSON invalid
- reconciliation.ok is not True

This is a certification gate, not a daily monitoring writer.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()


class GateError(Exception):
    pass


def _must_file(p: Path) -> Path:
    if not p.exists() or not p.is_file():
        raise GateError(f"MISSING_FILE: {p}")
    return p


def _load_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise GateError(f"JSON_READ_FAILED: {p}: {e}") from e


def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = (args.day_utc or "").strip()
    if not day:
        raise GateError("MISSING_DAY_UTC")

    p = _must_file(TRUTH / "monitoring_v1" / "engine_metrics" / day / "engine_metrics.v1.json")
    obj = _load_json(p)
    if not isinstance(obj, dict):
        raise GateError("OBJ_NOT_DICT")

    rec = obj.get("reconciliation")
    if not isinstance(rec, dict):
        raise GateError("RECONCILIATION_MISSING")

    ok = rec.get("ok")
    if ok is not True:
        delta = rec.get("delta")
        pr = rec.get("portfolio_return_window")
        sr = rec.get("sum_engine_contributions")
        raise GateError(f"RECONCILIATION_NOT_OK: ok={ok} delta={delta} portfolio_return={pr} sum_engine={sr}")

    print(f"OK: ENGINE_ATTRIBUTION_RECONCILIATION_OK day={day} file={p}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except GateError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        raise SystemExit(2)
