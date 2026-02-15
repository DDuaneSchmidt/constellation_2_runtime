#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# --- IMPORT PATH HARDENING ----------------------------------------------------
_THIS = Path(__file__).resolve()
_REPO_ROOT = _THIS.parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
# -----------------------------------------------------------------------------

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

PATH_ACCOUNTING_NAV = (TRUTH / "accounting_v1/nav").resolve()
PATH_ACCOUNTING_ATTR = (TRUTH / "accounting_v1/attribution").resolve()
PATH_ALLOCATION_SUMMARY = (TRUTH / "allocation_v1/summary").resolve()
PATH_MON_NAV_SERIES = (TRUTH / "monitoring_v1/nav_series").resolve()
PATH_MON_ENGINE_METRICS = (TRUTH / "monitoring_v1/engine_metrics").resolve()
PATH_MON_ENGINE_CORR = (TRUTH / "monitoring_v1/engine_correlation_matrix").resolve()
PATH_MON_CAP_EFF = (TRUTH / "monitoring_v1/capital_efficiency").resolve()

RISK_TRANSFORMER_PATH = (REPO_ROOT / "constellation_2/phaseH/tools/c2_risk_transformer_offline_v1.py").resolve()

OUT_DIR = (TRUTH / "reports").resolve()

SCHEMA_VERSION = 1
RET_Q = Decimal("0.00000001")
DD_Q = Decimal("0.000001")
TRADING_DAYS = Decimal("252")

ENV_CAGR_MIN = Decimal("0.08")
ENV_VOL_MAX = Decimal("0.12")
ENV_MAX_DD_MAX = Decimal("0.15")
ENV_SHARPE_MIN = Decimal("0.8")

TOL = Decimal("0")


class ReportError(Exception):
    pass


def _die(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    raise SystemExit(2)


def _read_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise ReportError(f"JSON_READ_ERROR: {p}: {e}") from e


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _dec(x: Any, what: str) -> Decimal:
    try:
        if isinstance(x, Decimal):
            return x
        if isinstance(x, int):
            return Decimal(x)
        if isinstance(x, float):
            return Decimal(str(x))
        if isinstance(x, str):
            return Decimal(x)
    except (InvalidOperation, ValueError) as e:
        raise ReportError(f"DEC_PARSE_ERROR({what}): {x}") from e
    raise ReportError(f"DEC_TYPE_ERROR({what}): {type(x).__name__}")


def _mean(xs: List[Decimal]) -> Optional[Decimal]:
    if not xs:
        return None
    return sum(xs) / Decimal(len(xs))


def _std_sample(xs: List[Decimal]) -> Optional[Decimal]:
    n = len(xs)
    if n < 2:
        return None
    m = _mean(xs)
    if m is None:
        return None
    var = sum((x - m) * (x - m) for x in xs) / Decimal(n - 1)
    if var < 0:
        raise ReportError("NEG_VARIANCE_IMPOSSIBLE")
    return var.sqrt()


def _annualized_vol(daily: List[Decimal]) -> Optional[Decimal]:
    sd = _std_sample(daily)
    if sd is None:
        return None
    return (sd * TRADING_DAYS.sqrt()).quantize(RET_Q, rounding=ROUND_HALF_UP)


def _sharpe(daily: List[Decimal]) -> Optional[Decimal]:
    sd = _std_sample(daily)
    m = _mean(daily)
    if sd is None or m is None or sd == 0:
        return None
    return ((m / sd) * TRADING_DAYS.sqrt()).quantize(RET_Q, rounding=ROUND_HALF_UP)


def _quantile(xs: List[Decimal], q: Decimal) -> Optional[Decimal]:
    if not xs:
        return None
    if q < 0 or q > 1:
        raise ReportError("QUANTILE_Q_OUT_OF_RANGE")
    s = sorted(xs)
    n = len(s)
    idx = int((q * Decimal(n - 1)).to_integral_value(rounding=ROUND_HALF_UP))
    idx = max(0, min(n - 1, idx))
    return s[idx].quantize(RET_Q, rounding=ROUND_HALF_UP)


def _skew_kurtosis(xs: List[Decimal]) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    n = len(xs)
    if n < 3:
        return (None, None)
    m = _mean(xs)
    if m is None:
        return (None, None)
    m2 = sum((x - m) ** 2 for x in xs) / Decimal(n)
    if m2 == 0:
        return (Decimal("0").quantize(RET_Q), Decimal("0").quantize(RET_Q))
    m3 = sum((x - m) ** 3 for x in xs) / Decimal(n)
    m4 = sum((x - m) ** 4 for x in xs) / Decimal(n)
    skew = (m3 / (m2.sqrt() ** 3)).quantize(RET_Q, rounding=ROUND_HALF_UP)
    kurt = (m4 / (m2 ** 2) - Decimal("3")).quantize(RET_Q, rounding=ROUND_HALF_UP)
    return (skew, kurt)


def _find_prev_day_dir(root: Path, day_utc: str) -> Optional[str]:
    if not root.exists():
        return None
    ds = [d.name for d in sorted(root.iterdir()) if d.is_dir() and d.name < day_utc]
    return ds[-1] if ds else None


def _load_nav(day_utc: str) -> Tuple[Path, Dict[str, Any]]:
    p = (PATH_ACCOUNTING_NAV / day_utc / "nav.json").resolve()
    if not p.exists():
        raise ReportError(f"NAV_MISSING: {p}")
    return (p, _read_json(p))


def _load_attr(day_utc: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    p = (PATH_ACCOUNTING_ATTR / day_utc / "engine_attribution.json").resolve()
    if not p.exists():
        return (None, None)
    return (p, _read_json(p))


def _load_alloc_summary(day_utc: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    p = (PATH_ALLOCATION_SUMMARY / day_utc / "summary.json").resolve()
    if not p.exists():
        return (None, None)
    return (p, _read_json(p))


def _load_nav_series(day_utc: str) -> Tuple[Path, Dict[str, Any]]:
    p = (PATH_MON_NAV_SERIES / day_utc / "portfolio_nav_series.v1.json").resolve()
    if not p.exists():
        raise ReportError(f"NAV_SERIES_MISSING: {p}")
    return (p, _read_json(p))


def _load_engine_metrics(day_utc: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    p = (PATH_MON_ENGINE_METRICS / day_utc / "engine_metrics.v1.json").resolve()
    if not p.exists():
        return (None, None)
    return (p, _read_json(p))


def _load_engine_corr(day_utc: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    p = (PATH_MON_ENGINE_CORR / day_utc / "engine_correlation_matrix.v1.json").resolve()
    if not p.exists():
        return (None, None)
    return (p, _read_json(p))


def _load_cap_eff(day_utc: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    p = (PATH_MON_CAP_EFF / day_utc / "capital_efficiency.v1.json").resolve()
    if not p.exists():
        return (None, None)
    return (p, _read_json(p))


def _validate_corr_matrix_or_fail(corr_obj: Dict[str, Any]) -> None:
    m = corr_obj.get("matrix", {})
    ids = m.get("engine_ids", None)
    corr = m.get("corr", None)
    if not isinstance(ids, list) or not isinstance(corr, list):
        raise ReportError("CORR_MATRIX_SHAPE_INVALID")
    n = len(ids)
    if n != len(corr):
        raise ReportError("CORR_MATRIX_DIM_MISMATCH")
    for i in range(n):
        row = corr[i]
        if not isinstance(row, list) or len(row) != n:
            raise ReportError("CORR_MATRIX_ROW_DIM_MISMATCH")
        for j in range(n):
            v = _dec(row[j], f"corr[{i}][{j}]")
            if v < Decimal("-1.0") - Decimal("0.0000001") or v > Decimal("1.0") + Decimal("0.0000001"):
                raise ReportError("CORR_MATRIX_OUT_OF_BOUNDS")
            v2 = _dec(corr[j][i], f"corr[{j}][{i}]")
            if (v - v2).copy_abs() > Decimal("0.000001"):
                raise ReportError("CORR_MATRIX_NOT_SYMMETRIC")


def _effective_div_ratio_equal_weight(corr_obj: Dict[str, Any]) -> Optional[Decimal]:
    m = corr_obj.get("matrix", {})
    ids = m.get("engine_ids", None)
    corr = m.get("corr", None)
    if not isinstance(ids, list) or not isinstance(corr, list) or not ids:
        return None
    n = len(ids)
    w = Decimal("1") / Decimal(n)
    s = Decimal("0")
    for i in range(n):
        for j in range(n):
            s += w * _dec(corr[i][j], "corr") * w
    if s <= 0:
        return None
    return (Decimal("1") / s.sqrt()).quantize(RET_Q, rounding=ROUND_HALF_UP)


def _git_sha(repo_root: Path) -> str:
    head = repo_root / ".git/HEAD"
    if not head.exists():
        return "UNKNOWN"
    s = head.read_text(encoding="utf-8").strip()
    if s.startswith("ref: "):
        ref = s.split("ref: ", 1)[1].strip()
        refp = repo_root / ".git" / ref
        if refp.exists():
            return refp.read_text(encoding="utf-8").strip()
        return "UNKNOWN"
    return s[:40] if s else "UNKNOWN"


def _build_output_path(day_utc: str) -> Path:
    ymd = day_utc.replace("-", "")
    return (OUT_DIR / f"daily_portfolio_snapshot_{ymd}.json").resolve()


def build_daily_snapshot(day_utc: str, produced_utc: str, deterministic_seed: str, allow_degraded_report: bool) -> Dict[str, Any]:
    if not RISK_TRANSFORMER_PATH.exists():
        raise ReportError(f"RISK_TRANSFORMER_MISSING: {RISK_TRANSFORMER_PATH}")

    nav_path, nav_obj = _load_nav(day_utc)
    ns_path, ns_obj = _load_nav_series(day_utc)
    attr_path, attr_obj = _load_attr(day_utc)
    alloc_path, alloc_obj = _load_alloc_summary(day_utc)
    em_path, em_obj = _load_engine_metrics(day_utc)
    corr_path, corr_obj = _load_engine_corr(day_utc)
    cap_path, cap_obj = _load_cap_eff(day_utc)

    manifest: List[Dict[str, Any]] = []
    manifest.append({"type": "accounting_nav", "path": str(nav_path), "sha256": _sha256_file(nav_path)})
    manifest.append({"type": "portfolio_nav_series", "path": str(ns_path), "sha256": _sha256_file(ns_path)})
    if attr_path is not None:
        manifest.append({"type": "accounting_attribution", "path": str(attr_path), "sha256": _sha256_file(attr_path)})
    if alloc_path is not None:
        manifest.append({"type": "allocation_summary", "path": str(alloc_path), "sha256": _sha256_file(alloc_path)})
    if em_path is not None:
        manifest.append({"type": "engine_metrics", "path": str(em_path), "sha256": _sha256_file(em_path)})
    if corr_path is not None:
        manifest.append({"type": "engine_correlation_matrix", "path": str(corr_path), "sha256": _sha256_file(corr_path)})
    if cap_path is not None:
        manifest.append({"type": "capital_efficiency", "path": str(cap_path), "sha256": _sha256_file(cap_path)})
    manifest.append({"type": "risk_transformer_module", "path": str(RISK_TRANSFORMER_PATH), "sha256": _sha256_file(RISK_TRANSFORMER_PATH)})

    # nav_series lookup
    series = ns_obj.get("series", {})
    pts = series.get("points", None)
    if not isinstance(pts, list) or not pts:
        raise ReportError("NAV_SERIES_POINTS_INVALID")
    idx = None
    for i, p in enumerate(pts):
        if isinstance(p, dict) and p.get("day_utc") == day_utc:
            idx = i
            break
    if idx is None:
        raise ReportError("NAV_SERIES_DAY_NOT_FOUND")
    pt = pts[idx]

    nav_eod = _dec(pt.get("nav_total"), "nav_total").to_integral_value(rounding=ROUND_HALF_UP)
    if idx > 0:
        nav_sod = _dec(pts[idx - 1].get("nav_total"), "nav_total_prev").to_integral_value(rounding=ROUND_HALF_UP)
    else:
        nav_sod = nav_eod

    daily_ret = _dec(pt.get("daily_return"), "daily_return").quantize(RET_Q, rounding=ROUND_HALF_UP)
    cum_ret = _dec(pt.get("cumulative_return"), "cumulative_return").quantize(RET_Q, rounding=ROUND_HALF_UP)
    rolling_peak_nav = int(_dec(pt.get("rolling_peak_nav"), "rolling_peak_nav"))
    dd_pct = _dec(pt.get("drawdown_pct"), "drawdown_pct").quantize(DD_Q, rounding=ROUND_HALF_UP)

    # NAV return reconciliation
    if nav_sod > 0:
        recon = ((Decimal(nav_eod) - Decimal(nav_sod)) / Decimal(nav_sod)).quantize(RET_Q, rounding=ROUND_HALF_UP)
        if (recon - daily_ret).copy_abs() > TOL:
            raise ReportError(f"RECON_FAIL_NAV_RETURN: recon={str(recon)} daily_return={str(daily_ret)}")
    else:
        if nav_eod != 0 and daily_ret != 0:
            raise ReportError("RECON_FAIL_DIV0_NAV_START_WITH_NONZERO_RETURN")

    daily_series: List[Decimal] = []
    for p in pts[: idx + 1]:
        if not isinstance(p, dict) or "daily_return" not in p:
            raise ReportError("NAV_SERIES_POINT_MISSING_DAILY_RETURN")
        daily_series.append(_dec(p["daily_return"], "series.daily_return").quantize(RET_Q, rounding=ROUND_HALF_UP))

    tail95 = _quantile(daily_series, Decimal("0.05"))
    tail99 = _quantile(daily_series, Decimal("0.01"))
    skew, kurt = _skew_kurtosis(daily_series)

    def _rolling_vol(n: int) -> Optional[Decimal]:
        xs = daily_series[-n:] if len(daily_series) >= 2 else []
        if len(xs) < 2:
            return None
        return _annualized_vol(xs)

    def _rolling_sharpe(n: int) -> Optional[Decimal]:
        xs = daily_series[-n:] if len(daily_series) >= 2 else []
        if len(xs) < 2:
            return None
        return _sharpe(xs)

    vol_30 = _rolling_vol(30)
    vol_90 = _rolling_vol(90)
    vol_180 = _rolling_vol(180)
    sh_30 = _rolling_sharpe(30)
    sh_90 = _rolling_sharpe(90)
    sh_180 = _rolling_sharpe(180)
    sh_252 = _rolling_sharpe(252)

    # --- Attribution (includes CASH sleeve from cash_total delta) ---
    daily_pnl_by: Dict[str, int] = {}
    cumulative_pnl_by: Dict[str, int] = {}
    contribution_pct_by: Dict[str, str] = {}

    # previous NAV day for cash delta attribution
    prev_nav_day = _find_prev_day_dir(PATH_ACCOUNTING_NAV, day_utc)
    prev_cash_total = None
    if prev_nav_day is not None:
        prev_nav_path = (PATH_ACCOUNTING_NAV / prev_nav_day / "nav.json").resolve()
        if prev_nav_path.exists():
            prev_nav_obj = _read_json(prev_nav_path)
            prev_cash_total = prev_nav_obj.get("nav", {}).get("cash_total", None)

    cur_cash_total = nav_obj.get("nav", {}).get("cash_total", None)
    if cur_cash_total is None:
        raise ReportError("NAV_CASH_TOTAL_MISSING")
    if prev_cash_total is None:
        # If we have a nonzero portfolio pnl but no prev cash baseline, fail closed.
        if int(nav_eod - nav_sod) != 0:
            raise ReportError("CASH_ATTR_BASELINE_MISSING_FOR_NONZERO_PNL")
        prev_cash_total = cur_cash_total

    cash_daily_pnl = int(int(cur_cash_total) - int(prev_cash_total))
    daily_pnl_by["CASH"] = cash_daily_pnl
    cumulative_pnl_by["CASH"] = int(cur_cash_total)

    # engine attribution (to-date)
    prev_attr_day = _find_prev_day_dir(PATH_ACCOUNTING_ATTR, day_utc)
    prev_attr_obj: Optional[Dict[str, Any]] = None
    if prev_attr_day is not None:
        prev_p = (PATH_ACCOUNTING_ATTR / prev_attr_day / "engine_attribution.json").resolve()
        if prev_p.exists():
            prev_attr_obj = _read_json(prev_p)

    if attr_obj is not None:
        cur_rows = attr_obj.get("attribution", {}).get("by_engine", [])
        if not isinstance(cur_rows, list):
            raise ReportError("ATTR_BY_ENGINE_INVALID")

        prev_map: Dict[str, int] = {}
        if prev_attr_obj is not None:
            prow = prev_attr_obj.get("attribution", {}).get("by_engine", [])
            if isinstance(prow, list):
                for r in prow:
                    if isinstance(r, dict) and isinstance(r.get("engine_id"), str):
                        eid = r["engine_id"]
                        rp = int(r.get("realized_pnl_to_date", 0))
                        up = int(r.get("unrealized_pnl", 0))
                        prev_map[eid] = int(rp + up)

        for r in cur_rows:
            if not isinstance(r, dict) or not isinstance(r.get("engine_id"), str):
                raise ReportError("ATTR_ROW_INVALID")
            eid = r["engine_id"]
            rp = int(r.get("realized_pnl_to_date", 0))
            up = int(r.get("unrealized_pnl", 0))
            pnl_to_date = int(rp + up)
            cumulative_pnl_by[eid] = pnl_to_date
            daily_pnl_by[eid] = int(pnl_to_date - prev_map.get(eid, pnl_to_date))

    # contribution pct
    for eid, dpnl in daily_pnl_by.items():
        if nav_sod > 0:
            contribution = (Decimal(int(dpnl)) / Decimal(nav_sod)).quantize(RET_Q, rounding=ROUND_HALF_UP)
            contribution_pct_by[eid] = f"{contribution:.8f}"
        else:
            if int(dpnl) != 0:
                raise ReportError("DIV0_NAV_SOD_WITH_NONZERO_DAILY_PNL")
            contribution_pct_by[eid] = "0.00000000"

    # Attribution sum must equal portfolio P&L exactly (STRICT always)
    port_daily_pnl = int(nav_eod - nav_sod)
    s_pnl = sum(int(v) for v in daily_pnl_by.values())
    if s_pnl != port_daily_pnl:
        raise ReportError(f"RECON_FAIL_ATTR_PNL_SUM: sum={s_pnl} port_daily_pnl={port_daily_pnl}")

    # --- Engine metrics reconciliation gate (strict unless allow_degraded_report) ---
    degraded_reasons: List[str] = []
    em_recon_ok = None
    em_portfolio_return_window = None

    if em_obj is not None:
        recon = em_obj.get("reconciliation", {})
        if isinstance(recon, dict):
            em_recon_ok = recon.get("ok", None)
            em_portfolio_return_window = recon.get("portfolio_return_window", None)

    if em_recon_ok is False:
        if not allow_degraded_report:
            raise ReportError("RECON_FAIL_ENGINE_METRICS_REPORTED_NOT_OK")
        degraded_reasons.append("ENGINE_METRICS_RECONCILIATION_NOT_OK")

    sleeves: Dict[str, Any] = {}
    if allow_degraded_report and degraded_reasons:
        # forensic sleeves from attribution only (including CASH)
        for eid in sorted(daily_pnl_by.keys()):
            dr = None
            if nav_sod > 0:
                drd = (Decimal(daily_pnl_by.get(eid, 0)) / Decimal(nav_sod)).quantize(RET_Q, rounding=ROUND_HALF_UP)
                dr = f"{drd:.8f}"
            sleeves[eid] = {
                "sleeve_id": eid,
                "allocation_pct": None,
                "exposure_pct": None,
                "daily_return": dr,
                "cumulative_return": None,
                "rolling_90d_sharpe": None,
                "rolling_180d_sharpe": None,
                "rolling_expectancy": None,
                "win_rate": None,
                "avg_gain": None,
                "avg_loss": None,
                "max_dd_trailing_180d": None,
                "capital_at_risk_pct": None,
            }
    else:
        # STRICT sleeves from engine_metrics (if present) and enforce prw==daily_ret and contrib sum==prw
        contrib_sum = Decimal("0")
        if em_obj is not None:
            by_engine = em_obj.get("metrics", {}).get("by_engine", [])
            if isinstance(by_engine, list):
                for r in by_engine:
                    if not isinstance(r, dict) or not isinstance(r.get("engine_id"), str):
                        raise ReportError("ENGINE_METRICS_ROW_INVALID")
                    eid = r["engine_id"]
                    sleeves[eid] = {
                        "sleeve_id": eid,
                        "allocation_pct": None,
                        "exposure_pct": None,
                        "daily_return": r.get("contribution_to_portfolio_return", None),
                        "cumulative_return": None,
                        "rolling_90d_sharpe": r.get("rolling_sharpe_90d", None),
                        "rolling_180d_sharpe": r.get("rolling_sharpe_180d", None),
                        "rolling_expectancy": r.get("expectancy", None),
                        "win_rate": r.get("win_rate", None),
                        "avg_gain": r.get("avg_gain", None),
                        "avg_loss": r.get("avg_loss", None),
                        "max_dd_trailing_180d": None,
                        "capital_at_risk_pct": None,
                    }
                    c = r.get("contribution_to_portfolio_return", None)
                    if c is not None:
                        contrib_sum += _dec(c, "contrib").quantize(RET_Q, rounding=ROUND_HALF_UP)

        if em_portfolio_return_window is not None:
            prw = _dec(em_portfolio_return_window, "portfolio_return_window").quantize(RET_Q, rounding=ROUND_HALF_UP)
            if (prw - daily_ret).copy_abs() > TOL:
                raise ReportError(f"RECON_FAIL_PORTFOLIO_RETURN_WINDOW: prw={str(prw)} daily_return={str(daily_ret)}")
            if contrib_sum != prw:
                raise ReportError(f"RECON_FAIL_SLEEVE_CONTRIB_SUM: sum={str(contrib_sum)} prw={str(prw)}")

    # risk (drawdown scaling from allocation summary)
    drawdown_multiplier = None
    drawdown_scaling_state = None
    risk_near_flags: List[str] = []
    risk_violations_today = bool(degraded_reasons)

    if alloc_obj is not None:
        dd = alloc_obj.get("summary", {}).get("drawdown_enforcement", None)
        if isinstance(dd, dict):
            drawdown_multiplier = dd.get("multiplier", None)
            drawdown_scaling_state = {
                "contract_id": dd.get("contract_id", None),
                "drawdown_pct": dd.get("drawdown_pct", None),
                "thresholds": dd.get("thresholds", None),
            }

    corr_matrix = None
    eff_div = None
    if corr_obj is not None:
        _validate_corr_matrix_or_fail(corr_obj)
        corr_matrix = corr_obj.get("matrix", None)
        ed = _effective_div_ratio_equal_weight(corr_obj)
        eff_div = None if ed is None else f"{ed:.8f}"

    capital_utilization_pct = None
    if cap_obj is not None:
        res = cap_obj.get("results", {})
        if isinstance(res, dict):
            cu = res.get("capital_utilization_peak", None)
            if cu is not None:
                capital_utilization_pct = cu

    dd_within = (dd_pct.copy_abs() <= ENV_MAX_DD_MAX)
    vol_use = vol_30
    vol_within = (vol_use <= ENV_VOL_MAX) if vol_use is not None else False
    sharpe_use = sh_252 if sh_252 is not None else sh_90
    sharpe_ok = (sharpe_use >= ENV_SHARPE_MIN) if sharpe_use is not None else False

    within_env = False if degraded_reasons else bool(vol_within and dd_within and sharpe_ok)
    envelope_reason = "ENGINE_METRICS_RECONCILIATION_NOT_OK" if degraded_reasons else "CAGR_INSUFFICIENT_HISTORY"

    out: Dict[str, Any] = {}
    out["meta"] = {
        "report_date_utc": day_utc,
        "generation_timestamp_utc": produced_utc,
        "git_commit_hash": _git_sha(REPO_ROOT),
        "accounting_snapshot_hash": _sha256_file(nav_path),
        "allocation_snapshot_hash": None if alloc_path is None else _sha256_file(alloc_path),
        "risk_transformer_version": _sha256_file(RISK_TRANSFORMER_PATH),
        "engine_versions": {},
        "data_sources_used": manifest,
        "deterministic_seed": deterministic_seed,
        "schema_version": SCHEMA_VERSION,
    }

    out["portfolio"] = {
        "nav_start_of_day": int(nav_sod),
        "nav_end_of_day": int(nav_eod),
        "daily_return": f"{daily_ret:.8f}",
        "cumulative_return": f"{cum_ret:.8f}",
        "rolling_peak_nav": int(rolling_peak_nav),
        "current_drawdown_pct": f"{dd_pct:.6f}",
        "rolling_30d_vol": None if vol_30 is None else f"{vol_30:.8f}",
        "rolling_90d_vol": None if vol_90 is None else f"{vol_90:.8f}",
        "rolling_180d_vol": None if vol_180 is None else f"{vol_180:.8f}",
        "rolling_30d_sharpe": None if sh_30 is None else f"{sh_30:.8f}",
        "rolling_90d_sharpe": None if sh_90 is None else f"{sh_90:.8f}",
        "rolling_180d_sharpe": None if sh_180 is None else f"{sh_180:.8f}",
        "rolling_252d_sharpe": None if sh_252 is None else f"{sh_252:.8f}",
        "gross_exposure_pct": None,
        "net_delta_pct": None,
        "open_risk_pct": None,
        "capital_utilization_pct": capital_utilization_pct,
    }

    out["sleeves"] = sleeves

    out["risk"] = {
        "max_trade_risk_cap": None,
        "max_engine_risk_cap": None,
        "max_portfolio_open_risk": None,
        "delta_cap": None,
        "drawdown_multiplier": drawdown_multiplier,
        "drawdown_scaling_state": drawdown_scaling_state,
        "risk_violations_today": bool(risk_violations_today),
        "risk_near_boundary_flags": risk_near_flags,
    }

    out["statistics"] = {
        "95_percentile_daily_loss": None if tail95 is None else f"{tail95:.8f}",
        "99_percentile_daily_loss": None if tail99 is None else f"{tail99:.8f}",
        "skew": None if skew is None else f"{skew:.8f}",
        "kurtosis": None if kurt is None else f"{kurt:.8f}",
        "rolling_correlation_matrix": corr_matrix,
        "effective_diversification_ratio": eff_div,
        "beta_to_benchmark": None,
    }

    out["attribution"] = {
        "daily_pnl_by_sleeve": daily_pnl_by,
        "cumulative_pnl_by_sleeve": cumulative_pnl_by,
        "contribution_pct_by_sleeve": contribution_pct_by,
        "trailing_90d_contribution_pct": None,
    }

    out["compliance"] = {
        "within_10_percent_mandate_envelope": bool(within_env),
        "envelope_violation_reason": envelope_reason,
        "drawdown_within_limit": bool(dd_within),
        "volatility_within_limit": bool(vol_within),
        "sharpe_above_min_threshold": bool(sharpe_ok),
        "risk_identity_compliant": False if degraded_reasons else True,
    }

    keys = sorted(out.keys())
    exp = sorted(["meta", "portfolio", "sleeves", "risk", "statistics", "attribution", "compliance"])
    if keys != exp:
        raise ReportError(f"TOP_LEVEL_KEYS_INVALID: got={keys} exp={exp}")

    return out


def _atomic_write_once_immutable(out_path: Path, obj: Dict[str, Any]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    new_bytes = (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
    new_hash = _sha256_bytes(new_bytes)

    if out_path.exists():
        old = out_path.read_bytes()
        old_hash = _sha256_bytes(old)
        if old_hash != new_hash:
            raise ReportError(f"IMMUTABILITY_VIOLATION_EXISTING_REPORT_DIFFERS: {out_path}")
        return

    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    if tmp.exists():
        raise ReportError(f"TEMP_EXISTS: {tmp}")
    tmp.write_bytes(new_bytes)
    tmp.replace(out_path)


def _parse_bool(s: str) -> bool:
    v = (s or "").strip().lower()
    if v in ("1", "true", "yes", "y"):
        return True
    if v in ("0", "false", "no", "n"):
        return False
    raise ReportError("ALLOW_DEGRADED_REPORT_INVALID_BOOL")


def main() -> None:
    ap = argparse.ArgumentParser(description="PhaseJ reporting: daily consolidated portfolio snapshot (deterministic; fail-closed).")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--produced_utc", required=True, help="Deterministic produced timestamp (e.g., 2026-02-15T06:00:00Z)")
    ap.add_argument("--seed", required=True, help="Deterministic seed material string")
    ap.add_argument("--allow_degraded_report", required=True, help="true/false")
    args = ap.parse_args()

    day_utc = (args.day_utc or "").strip()
    produced_utc = (args.produced_utc or "").strip()
    seed = (args.seed or "").strip()
    allow_degraded = _parse_bool(args.allow_degraded_report)

    if not day_utc:
        _die("MISSING_DAY_UTC")
    if not produced_utc:
        _die("MISSING_PRODUCED_UTC")
    if not seed:
        _die("MISSING_SEED")

    out_path = _build_output_path(day_utc)
    obj = build_daily_snapshot(day_utc=day_utc, produced_utc=produced_utc, deterministic_seed=seed, allow_degraded_report=allow_degraded)
    _atomic_write_once_immutable(out_path, obj)
    print(f"OK: DAILY_SNAPSHOT_WRITTEN path={out_path}")


if __name__ == "__main__":
    try:
        main()
    except ReportError as e:
        _die(str(e))
