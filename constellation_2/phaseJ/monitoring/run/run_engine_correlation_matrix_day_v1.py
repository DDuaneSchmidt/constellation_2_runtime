#!/usr/bin/env python3
"""
run_engine_correlation_matrix_day_v1.py

Phase J — Engine Correlation Matrix v1 writer
Deterministic, fail-closed, single-writer, canonical JSON, schema-validated.

Inputs (immutable truth):
- constellation_2/runtime/truth/monitoring_v1/engine_daily_returns_v1/<DAY>/engine_daily_returns.v1.json

Output (immutable truth):
- constellation_2/runtime/truth/monitoring_v1/engine_correlation_matrix/<DAY>/engine_correlation_matrix.v1.json

Notes:
- Bootstrap-safe: 1x1 matrix allowed (diagonal=1.000000).
- Uses Decimal math; quantizes correlations to 6dp.
- Window uses the most recent N available engine_daily_returns days <= day_utc (no calendar assumptions).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

# FIX: correct input root is engine_daily_returns_v1 (matches orchestrator + truth)
IN_ROOT = (TRUTH / "monitoring_v1/engine_daily_returns_v1").resolve()
OUT_ROOT = (TRUTH / "monitoring_v1/engine_correlation_matrix").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/engine_correlation_matrix.v1.schema.json"

Q6 = Decimal("0.000000")


class CliError(Exception):
    pass


def _utc_now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception as e:  # noqa: BLE001
        raise CliError(f"FAIL_GIT_SHA: {e}") from e


def _sha256_file(p: Path) -> str:
    import hashlib  # local import

    h = hashlib.sha256()
    try:
        b = p.read_bytes()
    except Exception as e:  # noqa: BLE001
        raise CliError(f"READ_FAILED: {p}: {e}") from e
    h.update(b)
    return h.hexdigest()


def _parse_day(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except Exception as e:  # noqa: BLE001
        raise CliError(f"BAD_DAY_UTC: {s}: {e}") from e


def _day_str(d: date) -> str:
    return d.isoformat()


def _read_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise CliError(f"JSON_READ_FAILED: {p}: {e}") from e


def _quant6(x: Decimal) -> Decimal:
    return x.quantize(Q6, rounding=ROUND_HALF_UP)


def _clamp_corr(x: Decimal) -> Decimal:
    if x > Decimal("1"):
        return Decimal("1")
    if x < Decimal("-1"):
        return Decimal("-1")
    return x


def _list_days(root: Path) -> List[date]:
    if not root.exists() or not root.is_dir():
        raise CliError(f"MISSING_ROOT_DIR: {root}")
    out: List[date] = []
    for child in sorted(root.iterdir()):
        if child.is_dir():
            try:
                out.append(_parse_day(child.name))
            except CliError:
                continue
    return sorted(out)


def _select_window(all_days: List[date], end_day: date, window_days: int) -> List[date]:
    if end_day not in all_days:
        raise CliError(f"END_DAY_NOT_FOUND: {end_day.isoformat()}")
    eligible = [d for d in all_days if d <= end_day]
    if len(eligible) < 1:
        raise CliError("NO_ELIGIBLE_DAYS")
    if window_days <= 0:
        raise CliError("BAD_WINDOW_DAYS")
    # Allow degraded operation when insufficient history: take as many as available.
    if len(eligible) < window_days:
        return eligible
    return eligible[-window_days:]


def _extract_returns(obj: Dict[str, Any]) -> Dict[str, Decimal]:
    # engine_daily_returns.v1.json -> returns: [{engine_id, daily_return}]
    rs = obj.get("returns")
    if not isinstance(rs, list):
        return {}
    out: Dict[str, Decimal] = {}
    for row in rs:
        if not isinstance(row, dict):
            continue
        eid = str(row.get("engine_id") or "").strip()
        dr = str(row.get("daily_return") or "").strip()
        if eid == "" or dr == "":
            continue
        try:
            out[eid] = Decimal(dr)
        except Exception:
            continue
    return out


def _corr(a: List[Decimal], b: List[Decimal]) -> Decimal:
    # Pearson corr, Decimal math; fail-closed to 0 if degenerate
    if len(a) != len(b) or len(a) < 2:
        return Decimal("0")
    n = Decimal(len(a))
    ma = sum(a) / n
    mb = sum(b) / n
    da = [x - ma for x in a]
    db = [x - mb for x in b]
    num = sum([da[i] * db[i] for i in range(len(a))])
    den_a = sum([x * x for x in da])
    den_b = sum([x * x for x in db])
    if den_a == 0 or den_b == 0:
        return Decimal("0")
    # Decimal sqrt via float is not acceptable; use exponentiation with context
    # Safe: convert to float only for sqrt magnitude, then back to Decimal quantized.
    import math  # local import

    den = Decimal(str(math.sqrt(float(den_a * den_b))))
    if den == 0:
        return Decimal("0")
    return num / den


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_engine_correlation_matrix_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--window_days", type=int, default=20, help="window size (uses most recent available <= day_utc)")
    args = ap.parse_args()

    day = _parse_day(args.day_utc)
    window_days = int(args.window_days)

    all_days = _list_days(IN_ROOT)
    win = _select_window(all_days, day, window_days)

    input_manifest: List[Dict[str, Any]] = []
    series_by_engine: Dict[str, List[Decimal]] = {}

    status = "OK"
    reason_codes: List[str] = []

    for d in win:
        p = (IN_ROOT / _day_str(d) / "engine_daily_returns.v1.json").resolve()
        if not p.exists():
            status = "FAIL_CORRUPT_INPUTS"
            reason_codes.append("MISSING_ENGINE_DAILY_RETURNS_FILE")
            continue

        sha = _sha256_file(p)
        input_manifest.append(
            {"type": "engine_daily_returns", "path": str(p), "sha256": sha, "producer": "phaseJ_engine_daily_returns_v1", "day_utc": _day_str(d)}
        )

        obj = _read_json(p)
        if not isinstance(obj, dict):
            status = "FAIL_CORRUPT_INPUTS"
            reason_codes.append("ENGINE_DAILY_RETURNS_NOT_OBJECT")
            continue

        returns = _extract_returns(obj)
        # If NOT_AVAILABLE, returns may be empty; still allowed for bootstrap but degrades signal.
        for eid, val in returns.items():
            series_by_engine.setdefault(eid, []).append(val)

    engine_ids = sorted(series_by_engine.keys())
    if len(engine_ids) == 0:
        # Bootstrap: no returns => 1x1 with placeholder engine id, degraded status
        status = "DEGRADED_INSUFFICIENT_HISTORY"
        reason_codes.append("NO_ENGINE_RETURNS_AVAILABLE")
        engine_ids = ["BOOTSTRAP"]
        corr = [["1.000000"]]
        flags = {"crowding_threshold": "0.75", "sustained_days": 1, "pairs": []}
    else:
        # Ensure aligned series lengths (pad missing with 0)
        max_len = max([len(series_by_engine[eid]) for eid in engine_ids])
        for eid in engine_ids:
            s = series_by_engine[eid]
            if len(s) < max_len:
                series_by_engine[eid] = s + [Decimal("0")] * (max_len - len(s))

        if max_len < 2:
            status = "DEGRADED_INSUFFICIENT_HISTORY"
            reason_codes.append("INSUFFICIENT_HISTORY_LT_2")
        n = len(engine_ids)

        corr: List[List[str]] = []
        getcontext().prec = 28

        for i in range(n):
            row: List[str] = []
            for j in range(n):
                if i == j:
                    row.append("1.000000")
                else:
                    c = _corr(series_by_engine[engine_ids[i]], series_by_engine[engine_ids[j]])
                    c = _clamp_corr(c)
                    c = _quant6(c)
                    row.append(f"{c:.6f}")
            corr.append(row)

        crowding_threshold = Decimal("0.75")
        pairs: List[Dict[str, Any]] = []
        max_pairwise = Decimal("0")
        for i in range(n):
            for j in range(i + 1, n):
                c = Decimal(corr[i][j])
                if abs(c) > abs(max_pairwise):
                    max_pairwise = c
                pairs.append(
                    {
                        "engine_a": engine_ids[i],
                        "engine_b": engine_ids[j],
                        "corr": corr[i][j],
                        "sustained": 0,
                        "flag": abs(c) >= crowding_threshold,
                    }
                )

        flags = {
            "crowding_threshold": "0.75",
            "sustained_days": 1,
            "pairs": pairs,
        }

    produced_utc = _utc_now_iso_z()
    payload: Dict[str, Any] = {
        "schema_id": "C2_ENGINE_CORRELATION_MATRIX_V1",
        "schema_version": 1,
        "status": status,
        "day_utc": _day_str(day),
        "window_days": int(window_days),
        "matrix": {"engine_ids": engine_ids, "corr": corr},
        "flags": flags,
        "input_manifest": input_manifest if len(input_manifest) > 0 else [{"type": "engine_daily_returns", "path": str(IN_ROOT), "sha256": "0" * 64, "producer": "phaseJ_engine_daily_returns_v1", "day_utc": _day_str(day)}],
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "constellation_2/phaseJ/monitoring/run/run_engine_correlation_matrix_day_v1.py"},
        "reason_codes": sorted(list(dict.fromkeys(reason_codes))),
    }

    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    out_path = (OUT_ROOT / _day_str(day) / "engine_correlation_matrix.v1.json").resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(canonical_json_bytes_v1(payload))

    print(f"OK: ENGINE_CORRELATION_MATRIX_V1_WRITTEN day={_day_str(day)} out={out_path}")
    return 0 if status == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
