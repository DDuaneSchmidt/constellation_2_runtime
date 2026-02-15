#!/usr/bin/env python3
"""
run_engine_metrics_day_v1.py

Constellation 2.0 — Phase J (Monitoring)
Engine Metrics v1 writer (single-writer, deterministic, fail-closed)

Inputs (immutable truth):
- constellation_2/runtime/truth/accounting_v1/attribution/<DAY_UTC>/engine_attribution.json
- constellation_2/runtime/truth/accounting_v1/nav/<DAY_UTC>/nav.json

Output (immutable truth):
- constellation_2/runtime/truth/monitoring_v1/engine_metrics/<DAY_UTC>/engine_metrics.v1.json

Notes:
- We do NOT have trade-level win/loss truth in C2 yet, so per-trade metrics
  (expectancy/win_rate/avg_gain/avg_loss) are emitted as null with explicit reason codes.
- Engine daily return is computed from delta(engine_pnl_to_date) / prev_day_nav_total.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

ATTR_ROOT = (TRUTH_ROOT / "accounting_v1/attribution").resolve()
NAV_ROOT = (TRUTH_ROOT / "accounting_v1/nav").resolve()

OUT_ROOT = (TRUTH_ROOT / "monitoring_v1/engine_metrics").resolve()

SCHEMA_RELPATH_OUT = "governance/04_DATA/SCHEMAS/C2/MONITORING/engine_metrics.v1.schema.json"

RET_Q = Decimal("0.00000001")   # 8dp
WIN_Q = Decimal("0.000001")     # 6dp
SQRT252 = None  # set in main()


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


def _quant_ret(x: Decimal) -> Decimal:
    return x.quantize(RET_Q, rounding=ROUND_HALF_UP)


def _sqrt(x: Decimal) -> Decimal:
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
    if not xs:
        raise CliError("STD_EMPTY")
    m = _mean(xs)
    acc = Decimal("0")
    for v in xs:
        dv = v - m
        acc += dv * dv
    var = acc / Decimal(len(xs))
    return _sqrt(var)


def _rolling_sharpe(ret_series: List[Decimal], n: int) -> Optional[Decimal]:
    if len(ret_series) < n:
        return None
    last = ret_series[-n:]
    sd = _stddev_population(last)
    if sd == 0:
        return None
    mu = _mean(last)
    if SQRT252 is None:
        raise CliError("SQRT252_UNINIT")
    out = (mu / sd) * SQRT252
    return _quant_ret(out)


def _compound_return(ret_series: List[Decimal], n: int) -> Optional[Decimal]:
    if len(ret_series) < n:
        return None
    last = ret_series[-n:]
    x = Decimal("1")
    for r in last:
        x = x * (Decimal("1") + r)
    return _quant_ret(x - Decimal("1"))


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


def _load_nav_total(day: date) -> int:
    p = (NAV_ROOT / _day_to_str(day) / "nav.json").resolve()
    if not p.exists() or not p.is_file():
        raise CliError(f"NAV_MISSING: {p}")
    obj = _read_json(p)
    if not isinstance(obj, dict):
        raise CliError("NAV_OBJ_NOT_DICT")
    nav = obj.get("nav")
    if not isinstance(nav, dict):
        raise CliError("NAV_FIELD_MISSING")
    nt = nav.get("nav_total")
    if not isinstance(nt, int) or nt < 0:
        raise CliError("NAV_TOTAL_INVALID")
    return nt


def _load_attribution(day: date) -> Dict[str, Any]:
    p = (ATTR_ROOT / _day_to_str(day) / "engine_attribution.json").resolve()
    if not p.exists() or not p.is_file():
        raise CliError(f"ATTR_MISSING: {p}")
    obj = _read_json(p)
    if not isinstance(obj, dict):
        raise CliError("ATTR_OBJ_NOT_DICT")
    return obj


def _index_engine_pnl_to_date(attr_obj: Dict[str, Any]) -> Tuple[str, Dict[str, int]]:
    a = attr_obj.get("attribution")
    if not isinstance(a, dict):
        raise CliError("ATTRIBUTION_FIELD_MISSING")
    currency = a.get("currency")
    if not isinstance(currency, str) or not currency:
        raise CliError("ATTR_CURRENCY_INVALID")

    by_engine = a.get("by_engine")
    if not isinstance(by_engine, list):
        raise CliError("ATTR_BY_ENGINE_INVALID")

    out: Dict[str, int] = {}
    for it in by_engine:
        if not isinstance(it, dict):
            continue
        eid = it.get("engine_id")
        if not isinstance(eid, str) or not eid:
            continue
        rp = it.get("realized_pnl_to_date")
        up = it.get("unrealized_pnl")
        if not isinstance(rp, int) or not isinstance(up, int):
            raise CliError("ATTR_PNL_FIELDS_NOT_INT")
        out[eid] = int(rp + up)
    return currency, out


def _write_failclosed_new(path: Path, obj: Dict[str, Any]) -> None:
    if path.exists():
        raise CliError(f"REFUSE_OVERWRITE: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    b = canonical_json_bytes_v1(obj)
    try:
        path.write_bytes(b)
    except Exception as e:  # noqa: BLE001
        raise CliError(f"WRITE_FAILED: {path}: {e}") from e


def build_engine_metrics(day_utc: str, window_days: int) -> Dict[str, Any]:
    if window_days < 2:
        raise CliError("WINDOW_DAYS_TOO_SMALL")

    end_day = _parse_day(day_utc)

    all_attr_days = _list_days(ATTR_ROOT)
    sel_days = _select_window(all_attr_days, end_day, window_days)

    degraded_hist = len(sel_days) < window_days
    has_gap = _detect_gaps(sel_days)

    # Load attribution + nav for each selected day
    attrs: List[Dict[str, Any]] = []
    navs: List[int] = []
    for d in sel_days:
        attrs.append(_load_attribution(d))
        navs.append(_load_nav_total(d))

    # Build per-day engine pnl_to_date index
    currency0, pnl0 = _index_engine_pnl_to_date(attrs[0])

    # Verify all days have same currency
    pnl_by_day: List[Dict[str, int]] = []
    currencies: List[str] = []
    for aobj in attrs:
        ccy, pnl_map = _index_engine_pnl_to_date(aobj)
        currencies.append(ccy)
        pnl_by_day.append(pnl_map)
    if any(c != currencies[0] for c in currencies):
        raise CliError("CURRENCY_MISMATCH_ACROSS_DAYS")
    currency = currencies[0]

    # Engine set: union of engines seen over selected window
    engines: List[str] = sorted({eid for m in pnl_by_day for eid in m.keys()})
    if not engines:
        raise CliError("NO_ENGINES_FOUND")

    # Compute engine daily returns series (length = len(sel_days); first day return 0)
    engine_ret_series: Dict[str, List[Decimal]] = {eid: [] for eid in engines}

    for i, d in enumerate(sel_days):
        if i == 0:
            for eid in engines:
                engine_ret_series[eid].append(Decimal("0").quantize(RET_Q, rounding=ROUND_HALF_UP))
            continue

        prev_nav = navs[i - 1]
        if prev_nav < 0:
            raise CliError("NEG_PREV_NAV_IMPOSSIBLE")

        prev_map = pnl_by_day[i - 1]
        cur_map = pnl_by_day[i]

        for eid in engines:
            prev_pnl = prev_map.get(eid, 0)
            cur_pnl = cur_map.get(eid, 0)
            delta = int(cur_pnl - prev_pnl)

            if prev_nav == 0:
                if delta != 0:
                    raise CliError("DIV0_PREV_NAV_WITH_NONZERO_DELTA_PNL")
                r = Decimal("0")
            else:
                r = Decimal(delta) / Decimal(prev_nav)

            engine_ret_series[eid].append(_quant_ret(r))

    # Compute portfolio return over window from NAV endpoints
    nav0 = navs[0]
    navN = navs[-1]
    if nav0 == 0:
        if navN != 0:
            raise CliError("DIV0_PORTFOLIO_START_NAV")
        portfolio_ret = Decimal("0")
    else:
        portfolio_ret = (Decimal(navN) / Decimal(nav0)) - Decimal("1")
    portfolio_ret_q = _quant_ret(portfolio_ret)

    # Build metrics per engine
    by_engine_out: List[Dict[str, Any]] = []
    sum_contrib = Decimal("0")

    for eid in engines:
        rs = engine_ret_series[eid]

        r30 = _compound_return(rs, 30)
        r90 = _compound_return(rs, 90)
        r180 = _compound_return(rs, 180)

        s30 = _rolling_sharpe(rs, 30)
        s90 = _rolling_sharpe(rs, 90)
        s180 = _rolling_sharpe(rs, 180)

        # Contribution: approximate as compounded return over full window (same length as rs),
        # but only meaningful if window has >=2 points (always true here).
        contrib = _compound_return(rs, len(rs))
        if contrib is None:
            contrib_s = None
        else:
            contrib_s = f"{contrib:.8f}"
            sum_contrib += contrib

        by_engine_out.append(
            {
                "engine_id": eid,
                "currency": currency,
                "rolling_return_30d": None if r30 is None else f"{r30:.8f}",
                "rolling_return_90d": None if r90 is None else f"{r90:.8f}",
                "rolling_return_180d": None if r180 is None else f"{r180:.8f}",
                "rolling_sharpe_30d": None if s30 is None else f"{s30:.8f}",
                "rolling_sharpe_90d": None if s90 is None else f"{s90:.8f}",
                "rolling_sharpe_180d": None if s180 is None else f"{s180:.8f}",
                "expectancy": None,
                "win_rate": None,
                "avg_gain": None,
                "avg_loss": None,
                "contribution_to_portfolio_return": contrib_s,
                "notes": [
                    "per-trade metrics unavailable: no trade-level win/loss truth spine present",
                    "daily return computed as delta(engine_pnl_to_date) / prev_day_nav_total"
                ],
            }
        )

    sum_contrib_q = _quant_ret(sum_contrib)
    delta_q = _quant_ret(sum_contrib_q - portfolio_ret_q)

    # Reconciliation ok iff delta is exactly 0 at 8dp (strict)
    ok = (delta_q == Decimal("0").quantize(RET_Q, rounding=ROUND_HALF_UP))

    # Build input manifest: attribution + nav paths with sha
    input_manifest: List[Dict[str, Any]] = []
    for d in sel_days:
        ap = (ATTR_ROOT / _day_to_str(d) / "engine_attribution.json").resolve()
        np = (NAV_ROOT / _day_to_str(d) / "nav.json").resolve()
        input_manifest.append({"type": "accounting_attr", "path": str(ap), "sha256": _sha256_file(ap), "producer": "bundle_f_accounting_v1", "day_utc": _day_to_str(d)})
        input_manifest.append({"type": "accounting_nav", "path": str(np), "sha256": _sha256_file(np), "producer": "bundle_f_accounting_v1", "day_utc": _day_to_str(d)})

    status = "OK"
    reason_codes: List[str] = []

    if degraded_hist:
        status = "DEGRADED_INSUFFICIENT_HISTORY"
        reason_codes.append("J_ENGINE_METRICS_INSUFFICIENT_HISTORY")
    if has_gap:
        status = "DEGRADED_INSUFFICIENT_HISTORY" if status == "OK" else status
        reason_codes.append("J_ENGINE_METRICS_GAPS_DETECTED")
    # We always mark trade-metrics as unavailable until there is a dedicated truth spine.
    reason_codes.append("J_ENGINE_METRICS_NO_TRADE_LEVEL_TRUTH")

    obj: Dict[str, Any] = {
        "schema_id": "C2_ENGINE_METRICS_V1",
        "schema_version": 1,
        "status": status,
        "day_utc": day_utc,
        "window_days": int(len(sel_days)),
        "metrics": {"by_engine": by_engine_out},
        "reconciliation": {
            "portfolio_return_window": f"{portfolio_ret_q:.8f}",
            "sum_engine_contributions": f"{sum_contrib_q:.8f}",
            "delta": f"{delta_q:.8f}",
            "ok": bool(ok),
        },
        "input_manifest": input_manifest,
        "produced_utc": _utc_now_iso_z(),
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "constellation_2/phaseJ/monitoring/run/run_engine_metrics_day_v1.py"},
        "reason_codes": reason_codes,
    }

    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH_OUT)
    return obj


def main() -> int:
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--window_days", required=True, type=int)
    args = ap.parse_args()

    getcontext().prec = 50
    global SQRT252  # noqa: PLW0603
    SQRT252 = Decimal("252").sqrt()

    day_utc = (args.day_utc or "").strip()
    if not day_utc:
        raise CliError("MISSING_DAY_UTC")

    obj = build_engine_metrics(day_utc=day_utc, window_days=int(args.window_days))

    out_path = (OUT_ROOT / day_utc / "engine_metrics.v1.json").resolve()
    _write_failclosed_new(out_path, obj)

    print(f"OK: ENGINE_METRICS_V1_WRITTEN day={day_utc} out={out_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CliError as e:
        print(f"FAIL: {e}", file=os.sys.stderr)
        raise SystemExit(2)
