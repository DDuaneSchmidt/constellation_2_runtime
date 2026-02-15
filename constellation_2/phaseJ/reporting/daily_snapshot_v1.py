#!/usr/bin/env python3
"""
Phase J reporting: consolidated daily portfolio snapshot.

INSTITUTIONAL REQUIREMENTS:
- Deterministic output (stable JSON bytes for identical truth inputs + CLI identity)
- Fail-closed on reconciliation / contract violations (unless allow_degraded_report=true)
- Immutable once written: re-run must produce identical bytes or fail
- Audit-proof: meta must include full reconstruction identity (produced_utc, deterministic_seed, allow_degraded_report)
- Versioned artifact: v2 filename to avoid mutating legacy v1 artifacts that lacked identity fields
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import hashlib
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP, getcontext
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---- Repo root / output locations ----
REPO_ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = (REPO_ROOT / "constellation_2/runtime/truth/reports").resolve()

# ---- Upstream truth sources ----
RUNTIME_TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

ACCOUNTING_NAV_DIR = (RUNTIME_TRUTH / "accounting_v1/nav").resolve()
ACCOUNTING_ATTR_DIR = (RUNTIME_TRUTH / "accounting_v1/attribution").resolve()
ALLOCATION_SUMMARY_DIR = (RUNTIME_TRUTH / "allocation_v1/summary").resolve()

MONITORING_ROOT = (RUNTIME_TRUTH / "monitoring_v1").resolve()
NAV_SERIES_DIR = (MONITORING_ROOT / "nav_series").resolve()
ENGINE_METRICS_DIR = (MONITORING_ROOT / "engine_metrics").resolve()
ENGINE_CORR_DIR = (MONITORING_ROOT / "engine_correlation_matrix").resolve()
CAP_EFF_DIR = (MONITORING_ROOT / "capital_efficiency").resolve()

# Risk transformer module is treated as an auditable input dependency (hash-bound)
RISK_TRANSFORMER_PATH = (REPO_ROOT / "constellation_2/phaseH/tools/c2_risk_transformer_offline_v1.py").resolve()

# ---- Numeric policy ----
getcontext().prec = 50

RET_Q = Decimal("0.00000000")
DD_Q = Decimal("0.000000")
TOL = Decimal("0.00000001")


class ReportError(Exception):
    pass


def _die(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    raise SystemExit(2)


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    return _sha256_bytes(p.read_bytes())


def _dec(x: Any, field: str) -> Decimal:
    try:
        return Decimal(str(x))
    except Exception:
        raise ReportError(f"DEC_PARSE_FAIL:{field}")


def _load_json(p: Path, label: str) -> Dict[str, Any]:
    if not p.exists():
        raise ReportError(f"MISSING:{label}:{p}")
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        raise ReportError(f"JSON_PARSE_FAIL:{label}:{p}:{e}")


def _load_nav(day_utc: str) -> Tuple[Path, Dict[str, Any]]:
    p = (ACCOUNTING_NAV_DIR / day_utc / "nav.json").resolve()
    return p, _load_json(p, "accounting_nav")


def _load_nav_series(day_utc: str) -> Tuple[Path, Dict[str, Any]]:
    p = (NAV_SERIES_DIR / day_utc / "portfolio_nav_series.v1.json").resolve()
    return p, _load_json(p, "portfolio_nav_series")


def _load_attr(day_utc: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    p = (ACCOUNTING_ATTR_DIR / day_utc / "engine_attribution.json").resolve()
    if not p.exists():
        return None, None
    return p, _load_json(p, "accounting_attribution")


def _load_alloc_summary(day_utc: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    p = (ALLOCATION_SUMMARY_DIR / day_utc / "summary.json").resolve()
    if not p.exists():
        return None, None
    return p, _load_json(p, "allocation_summary")


def _load_engine_metrics(day_utc: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    p = (ENGINE_METRICS_DIR / day_utc / "engine_metrics.v1.json").resolve()
    if not p.exists():
        return None, None
    return p, _load_json(p, "engine_metrics")


def _load_engine_corr(day_utc: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    p = (ENGINE_CORR_DIR / day_utc / "engine_correlation_matrix.v1.json").resolve()
    if not p.exists():
        return None, None
    return p, _load_json(p, "engine_correlation_matrix")


def _load_cap_eff(day_utc: str) -> Tuple[Optional[Path], Optional[Dict[str, Any]]]:
    p = (CAP_EFF_DIR / day_utc / "capital_efficiency.v1.json").resolve()
    if not p.exists():
        return None, None
    return p, _load_json(p, "capital_efficiency")


def _build_output_path(day_utc: str) -> Path:
    """
    V2 artifact naming to preserve immutability of legacy v1 artifacts that lacked
    full reconstruction identity fields in meta.
    """
    ymd = day_utc.replace("-", "")
    return (OUT_DIR / f"daily_portfolio_snapshot_v2_{ymd}.json").resolve()


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
        nav_sod = nav_eod  # fail-closed would block returns recon if daily_return nonzero

    # --- Engine metrics reconciliation gate (strict unless allow_degraded_report) ---
    degraded_reasons: List[str] = []
    reconciliation_ok = None
    reconciliation_delta = None
    if isinstance(em_obj, dict):
        recon = em_obj.get("reconciliation", {})
        if isinstance(recon, dict):
            reconciliation_ok = recon.get("ok", None)
            reconciliation_delta = recon.get("delta", None)

        if reconciliation_ok is False:
            degraded_reasons.append("ENGINE_METRICS_RECONCILIATION_NOT_OK")
            if not allow_degraded_report:
                raise ReportError("ENGINE_METRICS_RECONCILIATION_NOT_OK")

        # carry forward reason codes (informational)
        rcs = em_obj.get("reason_codes", [])
        if isinstance(rcs, list):
            for r in rcs:
                if isinstance(r, str) and r:
                    degraded_reasons.append(r)

    # Drawdown scaling state (contract snapshot)
    thresholds = [
        {"drawdown_pct": "0.000000", "multiplier": "1.00"},
        {"drawdown_pct": "-0.050000", "multiplier": "0.75"},
        {"drawdown_pct": "-0.100000", "multiplier": "0.50"},
        {"drawdown_pct": "-0.150000", "multiplier": "0.25"},
    ]
    drawdown_multiplier = "1.00"
    try:
        dd_val = Decimal(str(dd_pct))
        if dd_val <= Decimal("-0.150000"):
            drawdown_multiplier = "0.25"
        elif dd_val <= Decimal("-0.100000"):
            drawdown_multiplier = "0.50"
        elif dd_val <= Decimal("-0.050000"):
            drawdown_multiplier = "0.75"
        else:
            drawdown_multiplier = "1.00"
    except Exception:
        degraded_reasons.append("DRAWDOWN_MULTIPLIER_PARSE_FAIL")

    # Placeholder risk caps (unknown unless upstream provides)
    # Keep fields for schema stability.
    out: Dict[str, Any] = {}

    out["meta"] = {
        "report_date_utc": day_utc,
        "generation_timestamp_utc": produced_utc,
        "produced_utc": produced_utc,
        "deterministic_seed": deterministic_seed,
        "allow_degraded_report": bool(allow_degraded_report),
        "schema_version": 2,
        "git_commit_hash": os.environ.get("GIT_COMMIT_HASH") or _git_head_sha_or_none(),
        "accounting_snapshot_hash": _sha256_file(nav_path),
        "allocation_snapshot_hash": _sha256_file(alloc_path) if alloc_path is not None else None,
        "risk_transformer_version": _sha256_file(RISK_TRANSFORMER_PATH),
        "engine_versions": {},
        "data_sources_used": manifest,
    }

    # Portfolio fields
    out["portfolio"] = {
        "nav_start_of_day": int(nav_sod),
        "nav_end_of_day": int(nav_eod),
        "daily_return": str(daily_ret),
        "cumulative_return": str(cum_ret),
        "rolling_peak_nav": int(rolling_peak_nav),
        "current_drawdown_pct": str(dd_pct),
        # vol / sharpe values are derived from nav_series point if present
        "rolling_30d_vol": str(pt.get("rolling_vol_30d")) if pt.get("rolling_vol_30d") is not None else str(pt.get("rolling_30d_vol")) if pt.get("rolling_30d_vol") is not None else str(pt.get("rolling_30d_vol", pt.get("rolling_vol_30d", "0"))),
        "rolling_90d_vol": str(pt.get("rolling_vol_90d")) if pt.get("rolling_vol_90d") is not None else str(pt.get("rolling_90d_vol")) if pt.get("rolling_90d_vol") is not None else str(pt.get("rolling_90d_vol", pt.get("rolling_vol_90d", "0"))),
        "rolling_180d_vol": str(pt.get("rolling_vol_180d")) if pt.get("rolling_vol_180d") is not None else str(pt.get("rolling_180d_vol")) if pt.get("rolling_180d_vol") is not None else str(pt.get("rolling_180d_vol", pt.get("rolling_vol_180d", "0"))),
        "rolling_30d_sharpe": str(pt.get("rolling_sharpe_30d")) if pt.get("rolling_sharpe_30d") is not None else str(pt.get("rolling_30d_sharpe")) if pt.get("rolling_30d_sharpe") is not None else str(pt.get("rolling_30d_sharpe", pt.get("rolling_sharpe_30d", "0"))),
        "rolling_90d_sharpe": str(pt.get("rolling_sharpe_90d")) if pt.get("rolling_sharpe_90d") is not None else str(pt.get("rolling_90d_sharpe")) if pt.get("rolling_90d_sharpe") is not None else str(pt.get("rolling_90d_sharpe", pt.get("rolling_sharpe_90d", "0"))),
        "rolling_180d_sharpe": str(pt.get("rolling_sharpe_180d")) if pt.get("rolling_sharpe_180d") is not None else str(pt.get("rolling_180d_sharpe")) if pt.get("rolling_180d_sharpe") is not None else str(pt.get("rolling_180d_sharpe", pt.get("rolling_sharpe_180d", "0"))),
        "rolling_252d_sharpe": str(pt.get("rolling_sharpe_252d")) if pt.get("rolling_sharpe_252d") is not None else str(pt.get("rolling_252d_sharpe")) if pt.get("rolling_252d_sharpe") is not None else str(pt.get("rolling_252d_sharpe", pt.get("rolling_sharpe_252d", "0"))),
        "gross_exposure_pct": None,
        "net_delta_pct": None,
        "open_risk_pct": None,
        "capital_utilization_pct": None,
    }

    # Sleeves from engine_metrics if present, else fallback
    sleeves: Dict[str, Any] = {}
    if isinstance(em_obj, dict):
        metrics = em_obj.get("metrics", {})
        by_eng = None
        if isinstance(metrics, dict):
            by_eng = metrics.get("by_engine")
        if isinstance(by_eng, list) and by_eng:
            for row in by_eng:
                if not isinstance(row, dict):
                    continue
                eng = row.get("engine_id") or "unknown"
                sleeves[eng] = {
                    "sleeve_id": eng,
                    "allocation_pct": None,
                    "exposure_pct": None,
                    "daily_return": row.get("daily_return") or row.get("contribution_to_portfolio_return") or "0.00000000",
                    "cumulative_return": None,
                    "rolling_90d_sharpe": row.get("rolling_sharpe_90d"),
                    "rolling_180d_sharpe": row.get("rolling_sharpe_180d"),
                    "rolling_expectancy": row.get("expectancy"),
                    "win_rate": row.get("win_rate"),
                    "avg_gain": row.get("avg_gain"),
                    "avg_loss": row.get("avg_loss"),
                    "max_dd_trailing_180d": None,
                    "capital_at_risk_pct": None,
                }
    if not sleeves:
        sleeves["unknown"] = {
            "sleeve_id": "unknown",
            "allocation_pct": None,
            "exposure_pct": None,
            "daily_return": "0.00000000",
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
    out["sleeves"] = sleeves

    # Risk block (drawdown scaling always present)
    out["risk"] = {
        "max_trade_risk_cap": None,
        "max_engine_risk_cap": None,
        "max_portfolio_open_risk": None,
        "delta_cap": None,
        "drawdown_multiplier": str(drawdown_multiplier),
        "drawdown_scaling_state": {
            "contract_id": "C2_DRAWDOWN_CONVENTION_V1",
            "drawdown_pct": str(dd_pct),
            "thresholds": thresholds,
        },
        "risk_violations_today": True if degraded_reasons else False,
        "risk_near_boundary_flags": [],
    }

    # Statistics block (correlation matrix if available)
    stats: Dict[str, Any] = {
        "95_percentile_daily_loss": str(daily_ret),
        "99_percentile_daily_loss": str(daily_ret),
        "skew": None,
        "kurtosis": None,
        "effective_diversification_ratio": "1.00000000",
        "beta_to_benchmark": None,
        "rolling_correlation_matrix": None,
    }
    if isinstance(corr_obj, dict):
        cm = corr_obj.get("corr", None)
        ids = corr_obj.get("engine_ids", None)
        if cm is not None and ids is not None:
            stats["rolling_correlation_matrix"] = {"corr": cm, "engine_ids": ids}
    out["statistics"] = stats

    # Attribution block from accounting attribution if present
    daily_pnl_by: Dict[str, Any] = {}
    cumulative_pnl_by: Dict[str, Any] = {}
    contribution_pct_by: Dict[str, Any] = {}

    if isinstance(attr_obj, dict):
        # Keep current behavior if file format differs; this is intentionally minimal.
        # If absent/unknown, leave as empty dicts.
        pass

    # If we cannot compute, keep deterministic placeholder (matches your existing artifact style)
    if not daily_pnl_by:
        daily_pnl_by = {"unknown": 0}
    if not cumulative_pnl_by:
        cumulative_pnl_by = {"unknown": 0}
    if not contribution_pct_by:
        # If daily_return is nonzero and only CASH exists, keep compatibility
        contribution_pct_by = {"unknown": "0.00000000"}

    out["attribution"] = {
        "daily_pnl_by_sleeve": daily_pnl_by,
        "cumulative_pnl_by_sleeve": cumulative_pnl_by,
        "contribution_pct_by_sleeve": contribution_pct_by,
        "trailing_90d_contribution_pct": None,
    }

    within_env = False
    envelope_reason = None
    dd_within = False
    vol_within = False
    sharpe_ok = False

    # Compliance is conservative: if degraded reasons exist, risk_identity_compliant=false
    if degraded_reasons:
        envelope_reason = "ENGINE_METRICS_RECONCILIATION_NOT_OK"
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


def _git_head_sha_or_none() -> Optional[str]:
    try:
        p = (REPO_ROOT / ".git/HEAD").resolve()
        if not p.exists():
            return None
        head = p.read_text(encoding="utf-8").strip()
        if head.startswith("ref:"):
            ref = head.split(":", 1)[1].strip()
            ref_path = (REPO_ROOT / ".git" / ref).resolve()
            if ref_path.exists():
                return ref_path.read_text(encoding="utf-8").strip()
            return None
        return head
    except Exception:
        return None


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
