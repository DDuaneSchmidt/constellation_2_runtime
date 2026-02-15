#!/usr/bin/env python3
"""
Constellation 2.0 — Human Daily Summary Artifact (Derived / Non-Authoritative)

MODE: deterministic + read-only + derived-from-canonical + no-logic-duplication + fail-closed

Rules enforced:
- Reads ONLY canonical daily snapshot JSON v2 from runtime/truth/reports/.
- Does NOT compute financial logic; renders precomputed fields only.
- Does NOT query broker/accounting spines.
- Fails closed if snapshot missing or malformed.
- Deterministic formatting and stable ordering (sleeves sorted by name).
- Atomic write (temp + replace).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


REPO_ROOT = Path("/home/node/constellation_2_runtime")
CANON_DIR = REPO_ROOT / "constellation_2/runtime/truth/reports"

SNAPSHOT_BASENAME_RE = re.compile(r"^daily_portfolio_snapshot_v2_(\d{8})\.json$")
SUMMARY_BASENAME_RE = re.compile(r"^daily_portfolio_summary_(\d{8})\.txt$")


@dataclass(frozen=True)
class RequiredPath:
    label: str
    path: Tuple[str, ...]


def _die(msg: str, code: int = 2) -> None:
    sys.stderr.write(f"ERROR: {msg}\n")
    raise SystemExit(code)


def _proof_path_exists(p: Path, desc: str) -> None:
    if not p.exists():
        _die(f"required path missing ({desc}): {p}")


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
    except FileNotFoundError:
        _die(f"canonical snapshot missing: {path}")
    except json.JSONDecodeError as e:
        _die(f"canonical snapshot not valid JSON: {path} ({e})")
    if not isinstance(obj, dict):
        _die(f"canonical snapshot root must be object/dict: {path}")
    return obj


def _get(obj: Dict[str, Any], rp: RequiredPath) -> Any:
    cur: Any = obj
    for k in rp.path:
        if not isinstance(cur, dict) or k not in cur:
            _die(f"missing required field: {rp.label} (path={'.'.join(rp.path)})")
        cur = cur[k]
    return cur


def _optional_get(obj: Dict[str, Any], path: Tuple[str, ...]) -> Any:
    cur: Any = obj
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


def _as_str(v: Any) -> str:
    # Deterministic rendering: no locale, no extra formatting. Preserve canonical representation.
    if v is None:
        return "None"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        # Avoid scientific formatting changes; use Python's stable repr for numbers.
        return str(v)
    return str(v)


def _atomic_write_text(dst: Path, text: str) -> None:
    _proof_path_exists(dst.parent, "output directory")
    # Write temp in same directory for atomic replace.
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_daily_portfolio_summary_", dir=str(dst.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
            if not text.endswith("\n"):
                f.write("\n")
        os.replace(tmp_path, dst)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except Exception:
            pass


def _find_latest_snapshot() -> Path:
    _proof_path_exists(CANON_DIR, "canonical reports directory")
    candidates: List[Tuple[str, Path]] = []
    for p in CANON_DIR.iterdir():
        if p.is_file():
            m = SNAPSHOT_BASENAME_RE.match(p.name)
            if m:
                candidates.append((m.group(1), p))
    if not candidates:
        _die(f"no canonical snapshot v2 files found in: {CANON_DIR}")
    # Sort by YYYYMMDD descending (lexicographic works for fixed-width).
    candidates.sort(key=lambda t: t[0], reverse=True)
    return candidates[0][1]


def _snapshot_path_for_day_yyyymmdd(day_yyyymmdd: str) -> Path:
    if not re.fullmatch(r"\d{8}", day_yyyymmdd):
        _die(f"--day_yyyymmdd must be YYYYMMDD digits: got {day_yyyymmdd!r}")
    return CANON_DIR / f"daily_portfolio_snapshot_v2_{day_yyyymmdd}.json"


def _extract_reason_notes(snapshot: Dict[str, Any]) -> List[str]:
    # Only extract explicit fields if present; do not infer.
    notes: List[str] = []

    # Prefer explicit reason_codes arrays if they exist.
    for path in [
        ("reason_codes",),
        ("meta", "reason_codes"),
        ("compliance", "reason_codes"),
        ("risk", "reason_codes"),
    ]:
        v = _optional_get(snapshot, path)
        if isinstance(v, list) and all(isinstance(x, (str, int, float, bool)) or x is None for x in v):
            for x in v:
                notes.append(_as_str(x))

    # Include envelope_violation_reason if non-null (it is explicit in canonical snapshot).
    env_reason = _optional_get(snapshot, ("compliance", "envelope_violation_reason"))
    if env_reason is not None:
        notes.append(f"envelope_violation_reason={_as_str(env_reason)}")

    # Include degraded reasons if explicitly present.
    for path in [
        ("meta", "degraded_reasons"),
        ("meta", "degraded_reason_codes"),
        ("meta", "degraded_reason"),
        ("meta", "degraded"),
    ]:
        v = _optional_get(snapshot, path)
        if v is None:
            continue
        if isinstance(v, list):
            for x in v:
                notes.append(f"degraded={_as_str(x)}")
        else:
            notes.append(f"degraded={_as_str(v)}")

    # Deterministic ordering: preserve discovered order, but de-dup stably.
    seen = set()
    out: List[str] = []
    for n in notes:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def _extract_degraded_reasons_line(snapshot: Dict[str, Any]) -> str:
    # Only display explicit degraded reasons if present; otherwise show "None".
    # This does not infer degradation; it only reports what canonical snapshot says.
    for path in [
        ("meta", "degraded_reasons"),
        ("meta", "degraded_reason_codes"),
        ("meta", "degraded_reason"),
    ]:
        v = _optional_get(snapshot, path)
        if v is None:
            continue
        if isinstance(v, list):
            if not v:
                return "None"
            return "; ".join(_as_str(x) for x in v)
        return _as_str(v)
    return "None"


def _render(snapshot: Dict[str, Any]) -> str:
    # Validate required top-level keys exist (fail closed).
    required_top = [
        "meta",
        "portfolio",
        "sleeves",
        "risk",
        "compliance",
    ]
    for k in required_top:
        if k not in snapshot:
            _die(f"missing required top-level key: {k}")

    # Validate required leaf paths for the strict template.
    required = [
        RequiredPath("meta.generation_timestamp_utc", ("meta", "generation_timestamp_utc")),
        RequiredPath("meta.report_date_utc", ("meta", "report_date_utc")),
        RequiredPath("portfolio.nav_start_of_day", ("portfolio", "nav_start_of_day")),
        RequiredPath("portfolio.nav_end_of_day", ("portfolio", "nav_end_of_day")),
        RequiredPath("portfolio.daily_return", ("portfolio", "daily_return")),
        RequiredPath("portfolio.cumulative_return", ("portfolio", "cumulative_return")),
        RequiredPath("portfolio.current_drawdown_pct", ("portfolio", "current_drawdown_pct")),
        RequiredPath("portfolio.rolling_90d_sharpe", ("portfolio", "rolling_90d_sharpe")),
        RequiredPath("portfolio.rolling_90d_vol", ("portfolio", "rolling_90d_vol")),
        RequiredPath("risk.drawdown_multiplier", ("risk", "drawdown_multiplier")),
        RequiredPath("risk.risk_violations_today", ("risk", "risk_violations_today")),
        RequiredPath("risk.risk_near_boundary_flags", ("risk", "risk_near_boundary_flags")),
        RequiredPath("compliance.risk_identity_compliant", ("compliance", "risk_identity_compliant")),
        RequiredPath("compliance.within_10_percent_mandate_envelope", ("compliance", "within_10_percent_mandate_envelope")),
        RequiredPath("compliance.sharpe_above_min_threshold", ("compliance", "sharpe_above_min_threshold")),
        RequiredPath("compliance.volatility_within_limit", ("compliance", "volatility_within_limit")),
    ]
    for rp in required:
        _get(snapshot, rp)

    day = _as_str(_get(snapshot, RequiredPath("meta.report_date_utc", ("meta", "report_date_utc"))))
    generated = _as_str(_get(snapshot, RequiredPath("meta.generation_timestamp_utc", ("meta", "generation_timestamp_utc"))))

    nav_start = _as_str(_get(snapshot, RequiredPath("portfolio.nav_start_of_day", ("portfolio", "nav_start_of_day"))))
    nav_end = _as_str(_get(snapshot, RequiredPath("portfolio.nav_end_of_day", ("portfolio", "nav_end_of_day"))))
    daily_return = _as_str(_get(snapshot, RequiredPath("portfolio.daily_return", ("portfolio", "daily_return"))))
    cumulative_return = _as_str(_get(snapshot, RequiredPath("portfolio.cumulative_return", ("portfolio", "cumulative_return"))))
    drawdown = _as_str(_get(snapshot, RequiredPath("portfolio.current_drawdown_pct", ("portfolio", "current_drawdown_pct"))))
    sharpe_90 = _as_str(_get(snapshot, RequiredPath("portfolio.rolling_90d_sharpe", ("portfolio", "rolling_90d_sharpe"))))
    vol_90 = _as_str(_get(snapshot, RequiredPath("portfolio.rolling_90d_vol", ("portfolio", "rolling_90d_vol"))))

    dd_mult = _as_str(_get(snapshot, RequiredPath("risk.drawdown_multiplier", ("risk", "drawdown_multiplier"))))
    risk_id_ok = _as_str(_get(snapshot, RequiredPath("compliance.risk_identity_compliant", ("compliance", "risk_identity_compliant"))))
    risk_viol_today = _as_str(_get(snapshot, RequiredPath("risk.risk_violations_today", ("risk", "risk_violations_today"))))

    near_flags_v = _get(snapshot, RequiredPath("risk.risk_near_boundary_flags", ("risk", "risk_near_boundary_flags")))
    if not isinstance(near_flags_v, list):
        _die("risk.risk_near_boundary_flags must be a list")
    near_flags = "None" if len(near_flags_v) == 0 else "; ".join(_as_str(x) for x in near_flags_v)

    within_env = _as_str(_get(snapshot, RequiredPath("compliance.within_10_percent_mandate_envelope", ("compliance", "within_10_percent_mandate_envelope"))))
    sharpe_ok = _as_str(_get(snapshot, RequiredPath("compliance.sharpe_above_min_threshold", ("compliance", "sharpe_above_min_threshold"))))
    vol_ok = _as_str(_get(snapshot, RequiredPath("compliance.volatility_within_limit", ("compliance", "volatility_within_limit"))))

    sleeves_v = snapshot.get("sleeves")
    if not isinstance(sleeves_v, dict):
        _die("sleeves must be an object/dict")

    sleeve_lines: List[str] = []
    for sleeve_name in sorted(sleeves_v.keys()):
        sleeve_obj = sleeves_v.get(sleeve_name)
        if not isinstance(sleeve_obj, dict):
            _die(f"sleeves.{sleeve_name} must be an object/dict")

        # Approved sleeve fields for this summary (no computation).
        # These may be null in canonical snapshot; we display deterministically.
        s_daily = _as_str(_optional_get(sleeve_obj, ("daily_return",)))
        s_sharpe_90 = _as_str(_optional_get(sleeve_obj, ("rolling_90d_sharpe",)))
        s_cap_risk = _as_str(_optional_get(sleeve_obj, ("capital_at_risk_pct",)))

        sleeve_lines.append(f"{sleeve_name}:")
        sleeve_lines.append(f"  Daily Return: {s_daily}")
        sleeve_lines.append(f"  Rolling 90d Sharpe: {s_sharpe_90}")
        sleeve_lines.append(f"  Capital at Risk: {s_cap_risk}")
        sleeve_lines.append("")

    if sleeve_lines and sleeve_lines[-1] == "":
        sleeve_lines.pop()

    degraded_reasons = _extract_degraded_reasons_line(snapshot)
    notes = _extract_reason_notes(snapshot)

    # Strict stable format (fixed labels, stable spacing).
    out_lines: List[str] = []
    out_lines.append("CONSTELLATION 2.0 — DAILY SUMMARY")
    out_lines.append(f"Day: {day}")
    out_lines.append(f"Generated: {generated}")
    out_lines.append("")
    out_lines.append("--- PORTFOLIO ---")
    out_lines.append(f"NAV Start: {nav_start}")
    out_lines.append(f"NAV End: {nav_end}")
    out_lines.append(f"Daily Return: {daily_return}")
    out_lines.append(f"Cumulative Return: {cumulative_return}")
    out_lines.append(f"Drawdown: {drawdown}")
    out_lines.append(f"Rolling 90d Sharpe: {sharpe_90}")
    out_lines.append(f"Rolling 90d Volatility: {vol_90}")
    out_lines.append("")
    out_lines.append("--- SLEEVES ---")
    out_lines.extend(sleeve_lines if sleeve_lines else ["None"])
    out_lines.append("")
    out_lines.append("--- RISK ---")
    out_lines.append(f"Drawdown Multiplier: {dd_mult}")
    out_lines.append(f"Risk Identity Compliant: {risk_id_ok}")
    out_lines.append(f"Risk Violations Today: {risk_viol_today}")
    out_lines.append(f"Near Boundary Flags: {near_flags}")
    out_lines.append("")
    out_lines.append("--- MONITORING STATUS ---")
    out_lines.append(f"Within 10% Mandate Envelope: {within_env}")
    out_lines.append(f"Sharpe Above Threshold: {sharpe_ok}")
    out_lines.append(f"Volatility Within Limit: {vol_ok}")
    out_lines.append(f"Degraded Reasons (if any): {degraded_reasons}")
    out_lines.append("")
    out_lines.append("--- NOTES ---")
    if notes:
        for n in notes:
            out_lines.append(n)
    else:
        out_lines.append("None")

    return "\n".join(out_lines) + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(add_help=True)
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--day_yyyymmdd", help="Day in UTC as YYYYMMDD (selects canonical snapshot v2 file).")
    g.add_argument("--latest", action="store_true", help="Use latest available canonical snapshot v2.")
    args = p.parse_args(argv)

    _proof_path_exists(REPO_ROOT, "repo root")
    _proof_path_exists(CANON_DIR, "canonical reports dir")

    if args.latest or (args.day_yyyymmdd is None):
        snap_path = _find_latest_snapshot()
    else:
        snap_path = _snapshot_path_for_day_yyyymmdd(args.day_yyyymmdd)

    snapshot = _read_json(snap_path)

    # Ensure schema version is 2 (fail closed).
    schema_v = _optional_get(snapshot, ("meta", "schema_version"))
    if schema_v != 2:
        _die(f"canonical snapshot schema_version must be 2; got {schema_v!r} in {snap_path}")

    rendered = _render(snapshot)

    # Output filename is based on meta.report_date_utc (canonical), not on CLI input.
    day = _optional_get(snapshot, ("meta", "report_date_utc"))
    if not isinstance(day, str):
        _die("meta.report_date_utc must be string")
    day_yyyymmdd = day.replace("-", "")
    if not re.fullmatch(r"\d{8}", day_yyyymmdd):
        _die(f"meta.report_date_utc must be YYYY-MM-DD; got {day!r}")

    out_path = CANON_DIR / f"daily_portfolio_summary_{day_yyyymmdd}.txt"
    _atomic_write_text(out_path, rendered)

    sys.stdout.write(f"OK: DAILY_SUMMARY_V1 day_utc={day} out={out_path}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
