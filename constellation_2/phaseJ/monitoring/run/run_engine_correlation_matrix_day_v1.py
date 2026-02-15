#!/usr/bin/env python3
"""
run_engine_correlation_matrix_day_v1.py

Phase J — Engine Correlation Matrix v1 writer
Deterministic, fail-closed, single-writer, canonical JSON, schema-validated.

Inputs (immutable truth):
- constellation_2/runtime/truth/monitoring_v1/engine_daily_returns/<DAY>/engine_daily_returns.v1.json

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

IN_ROOT = (TRUTH / "monitoring_v1/engine_daily_returns").resolve()
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
    if len(eligible) < window_days:
        return eligible
    return eligible[-window_days:]


def _detect_gaps(days: List[date]) -> bool:
    for i in range(1, len(days)):
        if days[i] != days[i - 1] + timedelta(days=1):
            return True
    return False


def _load_daily_returns(day: date) -> Tuple[str, Dict[str, Decimal], Path]:
    p = (IN_ROOT / _day_str(day) / "engine_daily_returns.v1.json").resolve()
    if not p.exists() or not p.is_file():
        raise CliError(f"MISSING_ENGINE_DAILY_RETURNS: {p}")
    obj = _read_json(p)
    if not isinstance(obj, dict):
        raise CliError("OBJ_NOT_DICT")
    r = obj.get("returns")
    if not isinstance(r, dict):
        raise CliError("RETURNS_FIELD_MISSING")
    currency = r.get("currency")
    if not isinstance(currency, str) or not currency:
        raise CliError("CURRENCY_INVALID")
    be = r.get("by_engine")
    if not isinstance(be, list):
        raise CliError("BY_ENGINE_INVALID")
    out: Dict[str, Decimal] = {}
    for it in be:
        if not isinstance(it, dict):
            continue
        eid = it.get("engine_id")
        dr = it.get("daily_return")
        if not isinstance(eid, str) or not eid:
            continue
        if not isinstance(dr, str) or not dr:
            raise CliError("DAILY_RETURN_MISSING")
        try:
            out[eid] = Decimal(dr)
        except Exception as e:  # noqa: BLE001
            raise CliError(f"DAILY_RETURN_DECIMAL_PARSE_FAILED: {eid}: {e}") from e
    if not out:
        raise CliError("NO_ENGINE_RETURNS_FOUND")
    return currency, out, p


def _pearson_corr(xs: List[Decimal], ys: List[Decimal]) -> Decimal:
    n = len(xs)
    if n == 0:
        return Decimal("0")
    mx = sum(xs) / Decimal(n)
    my = sum(ys) / Decimal(n)

    cov = Decimal("0")
    vx = Decimal("0")
    vy = Decimal("0")
    for i in range(n):
        dx = xs[i] - mx
        dy = ys[i] - my
        cov += dx * dy
        vx += dx * dx
        vy += dy * dy

    cov = cov / Decimal(n)
    vx = vx / Decimal(n)
    vy = vy / Decimal(n)

    if vx == 0 or vy == 0:
        return Decimal("0")

    denom = vx.sqrt() * vy.sqrt()
    if denom == 0:
        return Decimal("0")

    return cov / denom


def _write_failclosed_new(path: Path, obj: Dict[str, Any]) -> None:
    if path.exists():
        raise CliError(f"REFUSE_OVERWRITE: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(obj))


def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--window_days", required=True, type=int)
    ap.add_argument("--crowding_threshold", default="0.70")
    ap.add_argument("--sustained_days", default=30, type=int)
    args = ap.parse_args()

    getcontext().prec = 50

    day = _parse_day((args.day_utc or "").strip())
    window_days = int(args.window_days)
    if window_days < 1:
        raise CliError("WINDOW_DAYS_TOO_SMALL")

    # Gather window days
    all_days = _list_days(IN_ROOT)
    sel = _select_window(all_days, day, window_days)
    has_gap = _detect_gaps(sel)

    # Load daily returns for each selected day
    currency: Optional[str] = None
    day_maps: List[Tuple[date, Dict[str, Decimal], Path]] = []
    for d in sel:
        ccy, m, p = _load_daily_returns(d)
        if currency is None:
            currency = ccy
        if currency != ccy:
            raise CliError("CURRENCY_MISMATCH_ACROSS_DAYS")
        day_maps.append((d, m, p))

    assert currency is not None

    # Union of engines observed across window
    engines = sorted({eid for (_, m, _) in day_maps for eid in m.keys()})
    if not engines:
        raise CliError("NO_ENGINES_FOUND")

    # Build aligned vectors per engine (missing => 0, but mark degraded via reason code)
    missing_any = False
    series: Dict[str, List[Decimal]] = {eid: [] for eid in engines}
    for (_, m, _) in day_maps:
        for eid in engines:
            if eid in m:
                series[eid].append(m[eid])
            else:
                series[eid].append(Decimal("0"))
                missing_any = True

    # Compute correlation matrix
    corr_mat: List[List[str]] = []
    for i, a in enumerate(engines):
        row: List[str] = []
        for j, b in enumerate(engines):
            if i == j:
                row.append(f"{_quant6(Decimal('1')):.6f}")
            else:
                c = _pearson_corr(series[a], series[b])
                c = _clamp_corr(c)
                row.append(f"{_quant6(c):.6f}")
        corr_mat.append(row)

    # Flags: pair list (only computed if >=2 engines and >=2 days)
    pairs: List[Dict[str, Any]] = []
    thr = Decimal(str(args.crowding_threshold)).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)
    sustained_days = int(args.sustained_days)

    if len(engines) >= 2 and len(sel) >= 2:
        # sustained logic requires a time series of correlations; we do not have that yet.
        # So we report current corr only with sustained=0.
        for i in range(len(engines)):
            for j in range(i + 1, len(engines)):
                c = Decimal(corr_mat[i][j])
                flag = (c >= thr)
                pairs.append(
                    {
                        "engine_a": engines[i],
                        "engine_b": engines[j],
                        "corr": f"{_quant6(c):.6f}",
                        "sustained": 0,
                        "flag": bool(flag),
                    }
                )

    status = "OK"
    reason_codes: List[str] = []
    if len(sel) < window_days:
        status = "DEGRADED_INSUFFICIENT_HISTORY"
        reason_codes.append("J_CORR_INSUFFICIENT_HISTORY")
    if has_gap:
        status = "DEGRADED_INSUFFICIENT_HISTORY" if status == "OK" else status
        reason_codes.append("J_CORR_GAPS_DETECTED")
    if len(engines) < 2:
        status = "DEGRADED_INSUFFICIENT_HISTORY" if status == "OK" else status
        reason_codes.append("J_CORR_NEED_AT_LEAST_2_ENGINES")
    if missing_any:
        status = "DEGRADED_INSUFFICIENT_HISTORY" if status == "OK" else status
        reason_codes.append("J_CORR_MISSING_ENGINE_DAY_TREATED_AS_ZERO")

    # input_manifest: include each day file sha256
    input_manifest: List[Dict[str, Any]] = []
    for (d, _, p) in day_maps:
        input_manifest.append(
            {
                "type": "engine_daily_returns",
                "path": str(p),
                "sha256": _sha256_file(p),
                "producer": "phaseJ_engine_daily_returns_v1",
                "day_utc": _day_str(d),
            }
        )

    out_obj: Dict[str, Any] = {
        "schema_id": "C2_ENGINE_CORRELATION_MATRIX_V1",
        "schema_version": 1,
        "status": status,
        "day_utc": _day_str(day),
        "window_days": int(len(sel)),
        "matrix": {"engine_ids": engines, "corr": corr_mat},
        "flags": {"crowding_threshold": f"{thr:.2f}", "sustained_days": sustained_days, "pairs": pairs},
        "input_manifest": input_manifest,
        "produced_utc": _utc_now_iso_z(),
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "constellation_2/phaseJ/monitoring/run/run_engine_correlation_matrix_day_v1.py"},
        "reason_codes": reason_codes,
    }

    validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_RELPATH)

    out_path = (OUT_ROOT / _day_str(day) / "engine_correlation_matrix.v1.json").resolve()
    _write_failclosed_new(out_path, out_obj)

    print(f"OK: ENGINE_CORRELATION_MATRIX_V1_WRITTEN day={_day_str(day)} out={out_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CliError as e:
        print(f"FAIL: {e}", file=os.sys.stderr)
        raise SystemExit(2)
