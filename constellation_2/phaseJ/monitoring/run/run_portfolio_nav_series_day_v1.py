#!/usr/bin/env python3
"""
run_portfolio_nav_series_day_v1.py

Constellation 2.0 — Phase J (Monitoring)
Portfolio NAV Series v1 writer (single-writer, deterministic, fail-closed)

Inputs (immutable truth):
- constellation_2/runtime/truth/accounting_v1/nav/<DAY_UTC>/nav.json

Outputs (immutable truth):
- constellation_2/runtime/truth/monitoring_v1/nav_series/<DAY_UTC>/portfolio_nav_series.v1.json

Determinism + fail-closed:
- Refuses overwrite of output file.
- Uses Decimal math.
- Canonical JSON serialization via canon_json_v1.
- Validates output against governed schema:
  governance/04_DATA/SCHEMAS/C2/MONITORING/portfolio_nav_series.v1.schema.json
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
ACCOUNTING_NAV_ROOT = (TRUTH_ROOT / "accounting_v1/nav").resolve()
OUT_ROOT = (TRUTH_ROOT / "monitoring_v1/nav_series").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/portfolio_nav_series.v1.schema.json"

# Quantization rules (deterministic)
RET_Q = Decimal("0.00000001")     # 8dp for returns/sharpe/vol
DD_Q = Decimal("0.000001")        # 6dp for drawdown_pct per contract
VOL_SQRT_252 = None  # initialized after context


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


def _day_to_str(d: date) -> str:
    return d.isoformat()


def _read_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise CliError(f"JSON_READ_FAILED: {p}: {e}") from e


def _dec(s: str, ctx: str) -> Decimal:
    if not isinstance(s, str) or not s.strip():
        raise CliError(f"DECIMAL_STRING_REQUIRED: {ctx}")
    try:
        return Decimal(s.strip())
    except Exception as e:  # noqa: BLE001
        raise CliError(f"DECIMAL_PARSE_FAILED: {ctx}: {e}") from e


def _quant_ret(x: Decimal) -> Decimal:
    return x.quantize(RET_Q, rounding=ROUND_HALF_UP)


def _quant_dd(x: Decimal) -> Decimal:
    return x.quantize(DD_Q, rounding=ROUND_HALF_UP)


def _sqrt(x: Decimal) -> Decimal:
    # Decimal.sqrt is deterministic under a fixed context precision
    if x < 0:
        raise CliError("SQRT_NEGATIVE")
    return x.sqrt()


def _mean(xs: List[Decimal]) -> Decimal:
    if not xs:
        raise CliError("MEAN_EMPTY")
    s = Decimal("0")
    for v in xs:
        s += v
    return s / Decimal(len(xs))


def _stddev_population(xs: List[Decimal]) -> Decimal:
    # population stddev (divide by N) — deterministic
    if not xs:
        raise CliError("STD_EMPTY")
    m = _mean(xs)
    acc = Decimal("0")
    for v in xs:
        dv = v - m
        acc += dv * dv
    var = acc / Decimal(len(xs))
    return _sqrt(var)


def _rolling_vol_annualized(ret_series: List[Decimal]) -> Optional[Decimal]:
    # ret_series: daily_return for last N points (N must be 30)
    if len(ret_series) < 30:
        return None
    last = ret_series[-30:]
    sd = _stddev_population(last)
    if VOL_SQRT_252 is None:
        raise CliError("SQRT252_UNINIT")
    out = sd * VOL_SQRT_252
    return _quant_ret(out)


def _rolling_sharpe_annualized(ret_series: List[Decimal]) -> Optional[Decimal]:
    # ret_series: daily_return for last N points (N must be 90)
    if len(ret_series) < 90:
        return None
    last = ret_series[-90:]
    sd = _stddev_population(last)
    if sd == 0:
        return None
    mu = _mean(last)
    if VOL_SQRT_252 is None:
        raise CliError("SQRT252_UNINIT")
    out = (mu / sd) * VOL_SQRT_252
    return _quant_ret(out)


@dataclass(frozen=True)
class NavPoint:
    day: date
    nav_total: int


def _list_nav_days() -> List[date]:
    if not ACCOUNTING_NAV_ROOT.exists() or not ACCOUNTING_NAV_ROOT.is_dir():
        raise CliError(f"MISSING_ACCOUNTING_NAV_ROOT: {ACCOUNTING_NAV_ROOT}")
    out: List[date] = []
    for child in sorted(ACCOUNTING_NAV_ROOT.iterdir()):
        if child.is_dir():
            try:
                out.append(_parse_day(child.name))
            except CliError:
                continue
    return sorted(out)


def _load_nav_point(d: date) -> NavPoint:
    p = (ACCOUNTING_NAV_ROOT / _day_to_str(d) / "nav.json").resolve()
    if not p.exists() or not p.is_file():
        raise CliError(f"NAV_FILE_MISSING: {p}")
    obj = _read_json(p)
    if not isinstance(obj, dict):
        raise CliError("NAV_OBJ_NOT_DICT")
    nav = obj.get("nav")
    if not isinstance(nav, dict):
        raise CliError("NAV_FIELD_MISSING")
    nav_total = nav.get("nav_total")
    if not isinstance(nav_total, int) or nav_total < 0:
        raise CliError("NAV_TOTAL_INVALID")
    return NavPoint(day=d, nav_total=nav_total)


def _select_window(all_days: List[date], end_day: date, window_days: int) -> List[date]:
    # choose the last `window_days` available <= end_day, but require end_day present
    if end_day not in all_days:
        raise CliError(f"END_DAY_NOT_FOUND_IN_NAV_DIRS: {end_day.isoformat()}")
    eligible = [d for d in all_days if d <= end_day]
    if len(eligible) < window_days:
        # We still compute with what we have, but mark degraded
        return eligible
    return eligible[-window_days:]


def _detect_gaps(days: List[date]) -> bool:
    # gap means missing a calendar day between adjacent points
    for i in range(1, len(days)):
        if days[i] != days[i - 1] + timedelta(days=1):
            return True
    return False


def _write_failclosed_new(path: Path, obj: Dict[str, Any]) -> None:
    # single-writer: refuse overwrite
    if path.exists():
        raise CliError(f"REFUSE_OVERWRITE: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    b = canonical_json_bytes_v1(obj)
    try:
        path.write_bytes(b)
    except Exception as e:  # noqa: BLE001
        raise CliError(f"WRITE_FAILED: {path}: {e}") from e


def build_nav_series(day_utc: str, window_days: int) -> Dict[str, Any]:
    if window_days < 2:
        raise CliError("WINDOW_DAYS_TOO_SMALL")

    end_day = _parse_day(day_utc)
    all_days = _list_nav_days()
    sel_days = _select_window(all_days, end_day, window_days)
    degraded_missing_days = len(sel_days) < window_days
    has_gap = _detect_gaps(sel_days)

    points: List[NavPoint] = [_load_nav_point(d) for d in sel_days]

    # Determine base NAV for cumulative return
    nav0 = points[0].nav_total
    if nav0 == 0:
        if any(p.nav_total != 0 for p in points):
            raise CliError("DIV0_BASE_NAV_NONZERO_LATER")
        base_nav_ok = False
    else:
        base_nav_ok = True

    rolling_peak = 0
    daily_returns: List[Decimal] = []

    out_points: List[Dict[str, Any]] = []
    for i, pt in enumerate(points):
        nav = pt.nav_total
        if nav > rolling_peak:
            rolling_peak = nav

        # daily_return
        if i == 0:
            dr = Decimal("0")
        else:
            prev = points[i - 1].nav_total
            if prev == 0:
                if nav != 0:
                    raise CliError("DIV0_PREV_NAV_NONZERO")
                dr = Decimal("0")
            else:
                dr = (Decimal(nav) / Decimal(prev)) - Decimal("1")
        drq = _quant_ret(dr)
        daily_returns.append(drq)

        # cumulative_return
        if not base_nav_ok:
            crq = Decimal("0")
        else:
            cr = (Decimal(nav) / Decimal(nav0)) - Decimal("1")
            crq = _quant_ret(cr)

        # drawdown_pct per contract: (NAV - peak) / peak, underwater negative
        if rolling_peak <= 0:
            ddq = Decimal("0").quantize(DD_Q, rounding=ROUND_HALF_UP)
        else:
            dd = (Decimal(nav) - Decimal(rolling_peak)) / Decimal(rolling_peak)
            ddq = _quant_dd(dd)

        vol30 = _rolling_vol_annualized(daily_returns)
        shr90 = _rolling_sharpe_annualized(daily_returns)

        out_points.append(
            {
                "day_utc": _day_to_str(pt.day),
                "nav_total": int(nav),
                "daily_return": f"{drq:.8f}",
                "cumulative_return": f"{crq:.8f}",
                "rolling_peak_nav": int(rolling_peak),
                "drawdown_pct": f"{ddq:.6f}",
                "rolling_vol_30d": None if vol30 is None else f"{vol30:.8f}",
                "rolling_sharpe_90d": None if shr90 is None else f"{shr90:.8f}",
            }
        )

    # Input manifest: include each day nav.json path + sha
    input_manifest: List[Dict[str, Any]] = []
    for d in sel_days:
        p = (ACCOUNTING_NAV_ROOT / _day_to_str(d) / "nav.json").resolve()
        input_manifest.append(
            {
                "type": "accounting_nav",
                "path": str(p),
                "sha256": _sha256_file(p),
                "producer": "bundle_f_accounting_v1",
                "day_utc": _day_to_str(d),
            }
        )

    status = "OK"
    reason_codes: List[str] = []
    if degraded_missing_days:
        status = "DEGRADED_INCOMPLETE_HISTORY"
        reason_codes.append("J_NAV_SERIES_INSUFFICIENT_HISTORY")
    if has_gap:
        status = "DEGRADED_MISSING_DAYS" if status == "OK" else status
        reason_codes.append("J_NAV_SERIES_GAPS_DETECTED")

    obj: Dict[str, Any] = {
        "schema_id": "C2_PORTFOLIO_NAV_SERIES_V1",
        "schema_version": 1,
        "status": status,
        "day_utc": day_utc,
        "series": {
            "contract_id": "C2_DRAWDOWN_CONVENTION_V1",
            "window_days": int(len(out_points)),
            "points": out_points,
            "invariants": {
                "no_gaps": (not has_gap),
                "no_lookahead": True,
                "drawdown_convention": {"contract_id": "C2_DRAWDOWN_CONVENTION_V1", "underwater_negative": True},
            },
        },
        "input_manifest": input_manifest,
        "produced_utc": _utc_now_iso_z(),
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "constellation_2/phaseJ/monitoring/run/run_portfolio_nav_series_day_v1.py"},
        "reason_codes": reason_codes,
    }

    # Schema validate (fail closed)
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
    return obj


def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--day_utc", required=True, help="Target day_utc (YYYY-MM-DD) to write series artifact for.")
    ap.add_argument("--window_days", required=True, type=int, help="Number of days to include (ending at day_utc).")
    args = ap.parse_args()

    # Fixed precision for deterministic Decimal sqrt/variance work
    getcontext().prec = 50
    global VOL_SQRT_252  # noqa: PLW0603
    VOL_SQRT_252 = Decimal("252").sqrt()

    day_utc = (args.day_utc or "").strip()
    if not day_utc:
        raise CliError("MISSING_DAY_UTC")

    obj = build_nav_series(day_utc=day_utc, window_days=int(args.window_days))

    out_path = (OUT_ROOT / day_utc / "portfolio_nav_series.v1.json").resolve()
    _write_failclosed_new(out_path, obj)

    print(f"OK: PORTFOLIO_NAV_SERIES_V1_WRITTEN day={day_utc} out={out_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CliError as e:
        print(f"FAIL: {e}", file=os.sys.stderr)
        raise SystemExit(2)
