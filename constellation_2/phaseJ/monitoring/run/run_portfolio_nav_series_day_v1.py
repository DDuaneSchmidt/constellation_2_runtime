#!/usr/bin/env python3
"""
PhaseJ monitoring: portfolio NAV series v1 (deterministic; fail-closed).

Writes:
- constellation_2/runtime/truth/monitoring_v1/nav_series/<DAY_UTC>/portfolio_nav_series.v1.json

Schema:
- governance/04_DATA/SCHEMAS/C2/MONITORING/portfolio_nav_series.v1.schema.json

Key properties:
- Deterministic computations (Decimal; fixed quantization)
- Fail-closed immutability on write
- Degraded when insufficient history and/or gaps detected

IMPORTANT (schema compliance):
- schema requires series.points minItems=2 and series.window_days >= 2
- when only a single accounting NAV day exists, we deterministically GENESIS-PAD a T-1 point:
  - day_utc = (asof_day_utc - 1 calendar day)
  - nav_total = same as asof day
  - daily_return = 0
  - cumulative_return = 0
  - rolling_peak_nav = nav_total
  - drawdown_pct = 0
  - rolling metrics = None
  - input_manifest remains only actual accounting NAV inputs (no synthetic input)
  - reason_codes includes J_NAV_SERIES_INSUFFICIENT_HISTORY and J_NAV_SERIES_GENESIS_PADDED

ACCOUNTING NAV INPUT FORMAT (proven):
- accounting_v1/nav/<DAY>/nav.json has nav_total at: obj["nav"]["nav_total"] (int)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path(__file__).resolve().parents[4]
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

ACCOUNTING_NAV_ROOT = (TRUTH_ROOT / "accounting_v1/nav").resolve()
OUT_ROOT = (TRUTH_ROOT / "monitoring_v1/nav_series").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/portfolio_nav_series.v1.schema.json"

RET_Q = Decimal("0.00000001")     # 8 dp
DD_Q = Decimal("0.000001")        # 6 dp

VOL_SQRT_252: Decimal = Decimal("0")  # initialized in main()


class CliError(RuntimeError):
    pass


@dataclass(frozen=True)
class NavPoint:
    day: date
    nav_total: int


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _utc_now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _canonical_json_bytes(obj: Any) -> bytes:
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return s.encode("utf-8")


def _write_failclosed_new(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _canonical_json_bytes(obj)
    if path.exists():
        existing = path.read_bytes()
        if existing == payload:
            print(f"OK: IMMUTABLE_ALREADY_MATCHES path={path}")
            return
        raise CliError(f"IMMUTABILITY_VIOLATION_EXISTING_REPORT_DIFFERS:{path}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(payload)
    os.replace(tmp, path)


def _day_to_str(d: date) -> str:
    return d.isoformat()


def _str_to_day(s: str) -> date:
    return date.fromisoformat(s)


def _list_all_nav_days() -> List[date]:
    if not ACCOUNTING_NAV_ROOT.exists():
        return []
    days: List[date] = []
    for child in sorted(ACCOUNTING_NAV_ROOT.iterdir()):
        if not child.is_dir():
            continue
        try:
            days.append(_str_to_day(child.name))
        except Exception:
            continue
    return sorted(days)


def _must_nav_path(d: date) -> Path:
    p = (ACCOUNTING_NAV_ROOT / _day_to_str(d) / "nav.json").resolve()
    if not p.exists():
        raise CliError(f"MISSING:accounting_nav:{p}")
    return p


def _load_nav_point(d: date) -> NavPoint:
    p = _must_nav_path(d)
    o = json.load(open(p, "r", encoding="utf-8"))
    nav_obj = o.get("nav")
    if not isinstance(nav_obj, dict):
        raise CliError(f"NAV_OBJECT_MISSING_OR_NOT_OBJECT:{p}")
    nav_total = nav_obj.get("nav_total")
    if nav_total is None:
        raise CliError(f"NAV_TOTAL_MISSING:{p}")
    if not isinstance(nav_total, int):
        raise CliError(f"NAV_TOTAL_NOT_INT:{p}")
    return NavPoint(day=d, nav_total=int(nav_total))


def _select_window(all_days: List[date], end_day: date, window_days: int) -> List[date]:
    eligible = [d for d in all_days if d <= end_day]
    if end_day not in eligible:
        raise CliError("DAY_UTC_NOT_PRESENT_IN_ACCOUNTING_NAV")
    if len(eligible) < window_days:
        return eligible
    return eligible[-window_days:]


def _has_calendar_gap(days: List[date]) -> bool:
    if len(days) <= 1:
        return False
    for a, b in zip(days, days[1:]):
        if (b - a).days != 1:
            return True
    return False


def _quant_ret(x: Decimal) -> Decimal:
    return x.quantize(RET_Q, rounding=ROUND_HALF_UP)


def _quant_dd(x: Decimal) -> Decimal:
    return x.quantize(DD_Q, rounding=ROUND_HALF_UP)


def _rolling_vol_annualized(daily_returns: List[Decimal]) -> Optional[Decimal]:
    if len(daily_returns) < 30:
        return None
    r = daily_returns[-30:]
    m = sum(r) / Decimal(len(r))
    var = sum((x - m) * (x - m) for x in r) / Decimal(len(r) - 1)
    if var < 0:
        return None
    vol = var.sqrt() * VOL_SQRT_252
    return _quant_ret(vol)


def _rolling_sharpe_annualized(daily_returns: List[Decimal]) -> Optional[Decimal]:
    if len(daily_returns) < 90:
        return None
    r = daily_returns[-90:]
    m = sum(r) / Decimal(len(r))
    var = sum((x - m) * (x - m) for x in r) / Decimal(len(r) - 1)
    if var <= 0:
        return None
    vol = var.sqrt()
    shr = (m / vol) * VOL_SQRT_252
    return _quant_ret(shr)


def _genesis_pad_point(asof: Dict[str, Any]) -> Dict[str, Any]:
    d0 = _str_to_day(asof["day_utc"])
    d_pad = d0 - timedelta(days=1)
    nav = int(asof["nav_total"])
    return {
        "day_utc": _day_to_str(d_pad),
        "nav_total": int(nav),
        "daily_return": f"{Decimal('0'):.8f}",
        "cumulative_return": f"{Decimal('0'):.8f}",
        "rolling_peak_nav": int(nav),
        "drawdown_pct": f"{Decimal('0').quantize(DD_Q, rounding=ROUND_HALF_UP):.6f}",
        "rolling_vol_30d": None,
        "rolling_sharpe_90d": None,
    }


def build_nav_series(day_utc: str, window_days: int) -> Dict[str, Any]:
    if window_days < 2:
        raise CliError("WINDOW_DAYS_MIN_2")

    end_day = _str_to_day(day_utc)
    all_days = _list_all_nav_days()
    sel_days = _select_window(all_days, end_day, window_days)

    degraded_missing_days = len(sel_days) < window_days
    has_gap = _has_calendar_gap(sel_days)

    points: List[NavPoint] = [_load_nav_point(d) for d in sel_days]
    if len(points) < 1:
        raise CliError("NO_NAV_POINTS_AVAILABLE")

    nav0 = points[0].nav_total
    base_nav_ok = nav0 > 0
    if not base_nav_ok:
        if any(p.nav_total != 0 for p in points):
            raise CliError("NAV_BASE_ZERO_BUT_NONZERO_LATER")

    out_points: List[Dict[str, Any]] = []
    daily_returns: List[Decimal] = []
    rolling_peak: int = points[0].nav_total

    for i, pt in enumerate(points):
        nav = pt.nav_total
        rolling_peak = max(rolling_peak, nav)

        if i == 0 or (not base_nav_ok):
            drq = Decimal("0")
        else:
            prev = points[i - 1].nav_total
            if prev <= 0:
                drq = Decimal("0")
            else:
                dr = (Decimal(nav) / Decimal(prev)) - Decimal("1")
                drq = _quant_ret(dr)
        daily_returns.append(Decimal(drq))

        if not base_nav_ok:
            crq = Decimal("0")
        else:
            cr = (Decimal(nav) / Decimal(nav0)) - Decimal("1")
            crq = _quant_ret(cr)

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

    genesis_padded = False
    if len(out_points) == 1:
        genesis_padded = True
        out_points = [_genesis_pad_point(out_points[0])] + out_points

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
    if genesis_padded:
        if "J_NAV_SERIES_INSUFFICIENT_HISTORY" not in reason_codes:
            reason_codes.append("J_NAV_SERIES_INSUFFICIENT_HISTORY")
        reason_codes.append("J_NAV_SERIES_GENESIS_PADDED")

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

    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
    return obj


def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--day_utc", required=True, help="Target day_utc (YYYY-MM-DD) to write series artifact for.")
    ap.add_argument("--window_days", required=True, type=int, help="Number of days to include (ending at day_utc).")
    args = ap.parse_args()

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
