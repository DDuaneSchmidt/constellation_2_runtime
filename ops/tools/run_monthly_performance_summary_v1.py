#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_authority_bridge_v1 import resolve_canonical_truth_root_bridge_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/monthly_performance_summary.v1.schema.json"
DEFAULT_TRUTH_ROOT = resolve_canonical_truth_root_bridge_v1(
    caller="ops/tools/run_monthly_performance_summary_v1.py"
).resolve()

RET_Q = Decimal("0.00000001")
DD_Q = Decimal("0.000000")


@dataclass(frozen=True)
class NavDay:
    day: date
    day_utc: str
    nav_total: Decimal
    realized: Decimal
    unrealized: Decimal
    source_path: Path

    @property
    def pnl_total(self) -> Decimal:
        return self.realized + self.unrealized

    @property
    def month_utc(self) -> str:
        return self.day_utc[:7]


class CliError(RuntimeError):
    pass


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _immut_write(path: Path, content: bytes) -> None:
    if path.exists():
        existing = path.read_bytes()
        if hashlib.sha256(existing).hexdigest() != hashlib.sha256(content).hexdigest():
            raise CliError(f"IMMUTABLE_REWRITE_DIFFERENT_CONTENT: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _parse_month_utc(value: str) -> Tuple[int, int]:
    text = str(value or "").strip()
    if len(text) != 7 or text[4] != "-":
        raise CliError(f"MONTH_UTC_INVALID: {text!r}")
    try:
        year = int(text[:4])
        month = int(text[5:7])
    except ValueError as exc:
        raise CliError(f"MONTH_UTC_INVALID: {text!r}") from exc
    if year < 2000 or month < 1 or month > 12:
        raise CliError(f"MONTH_UTC_INVALID: {text!r}")
    return year, month


def _resolve_truth_root(raw: str) -> Path:
    text = str(raw or "").strip()
    if not text:
        return DEFAULT_TRUTH_ROOT
    p = Path(text).expanduser().resolve()
    if not p.is_absolute() or (not p.exists()) or (not p.is_dir()):
        raise CliError(f"TRUTH_ROOT_INVALID: {p}")
    return p


def _parse_decimal(value: Any, field: str, day_utc: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise CliError(f"NAV_FIELD_INVALID:{field}:{day_utc}") from exc


def _parse_day_utc(day_utc: str) -> date:
    try:
        return date.fromisoformat(day_utc)
    except ValueError as exc:
        raise CliError(f"DAY_UTC_INVALID_IN_NAV: {day_utc!r}") from exc


def _load_nav_rows(truth_root: Path) -> List[NavDay]:
    nav_root = (truth_root / "accounting_v2" / "nav").resolve()
    if not nav_root.exists() or not nav_root.is_dir():
        return []

    rows: List[NavDay] = []
    for nav_file in sorted(nav_root.glob("*/nav.v2.json")):
        obj = json.loads(nav_file.read_text(encoding="utf-8"))
        day_utc = str(obj.get("day_utc") or nav_file.parent.name).strip()
        nav = obj.get("nav")
        if not isinstance(nav, dict):
            raise CliError(f"NAV_OBJECT_MISSING:{day_utc}")
        row = NavDay(
            day=_parse_day_utc(day_utc),
            day_utc=day_utc,
            nav_total=_parse_decimal(nav.get("nav_total"), "nav_total", day_utc),
            realized=_parse_decimal(nav.get("realized_pnl_to_date"), "realized_pnl_to_date", day_utc),
            unrealized=_parse_decimal(nav.get("unrealized_pnl"), "unrealized_pnl", day_utc),
            source_path=nav_file.resolve(),
        )
        rows.append(row)
    rows.sort(key=lambda x: x.day)
    return rows


def _fmt_ret(value: Decimal) -> str:
    return format(value.quantize(RET_Q, rounding=ROUND_HALF_UP), "f")


def _fmt_dd(value: Decimal) -> str:
    return format(value.quantize(DD_Q, rounding=ROUND_HALF_UP), "f")


def _month_key(d: date) -> str:
    return d.isoformat()[:7]


def _iter_months(start_month_utc: str, count: int) -> Iterable[str]:
    year = int(start_month_utc[:4])
    month = int(start_month_utc[5:7])
    for _ in range(count):
        yield f"{year:04d}-{month:02d}"
        month += 1
        if month > 12:
            month = 1
            year += 1


def _month_offset(month_utc: str, delta: int) -> str:
    year = int(month_utc[:4])
    month = int(month_utc[5:7])
    idx = (year * 12 + (month - 1)) + delta
    out_year = idx // 12
    out_month = (idx % 12) + 1
    return f"{out_year:04d}-{out_month:02d}"


def _expected_window(end_month_utc: str, months: int) -> List[str]:
    start = _month_offset(end_month_utc, -(months - 1))
    return list(_iter_months(start, months))


def _load_fill_ledger_files_for_days(truth_root: Path, day_utcs: Iterable[str]) -> List[Path]:
    fill_root = (truth_root / "fill_ledger_v1").resolve()
    files: List[Path] = []
    for day_utc in day_utcs:
        day_dir = (fill_root / day_utc).resolve()
        if not day_dir.exists() or not day_dir.is_dir():
            continue
        files.extend(sorted(day_dir.glob("*.fill_ledger.v1.json")))
    return files


def _count_trades_from_fill_ledgers(paths: List[Path]) -> int:
    count = 0
    for p in paths:
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        qty = obj.get("filled_qty")
        try:
            if Decimal(str(qty)) > 0:
                count += 1
        except Exception:
            continue
    return count


@dataclass(frozen=True)
class MonthCore:
    month_utc: str
    starting_nav: Optional[Decimal]
    ending_nav: Optional[Decimal]
    net_pnl: Optional[Decimal]
    monthly_return_pct: Optional[Decimal]
    realized_pnl: Optional[Decimal]
    unrealized_pnl: Optional[Decimal]
    deposits_additions: Optional[Decimal]
    withdrawals: Optional[Decimal]
    net_external_flow: Optional[Decimal]
    return_excluding_external_flows: Optional[Decimal]
    max_drawdown_for_month: Optional[Decimal]
    trading_days_count: int
    trades_count: int
    winning_days_count: Optional[int]
    losing_days_count: Optional[int]
    blocker_codes: Tuple[str, ...]
    reason_codes: Tuple[str, ...]
    nav_paths: Tuple[Path, ...]
    fill_paths: Tuple[Path, ...]
    baseline_path: Optional[Path]


def _compute_month_core(month_utc: str, rows: List[NavDay], truth_root: Path) -> MonthCore:
    month_rows = [r for r in rows if r.month_utc == month_utc]
    reasons: List[str] = []
    blockers: List[str] = []

    if not month_rows:
        blockers.append("MISSING_NAV_DAYS_FOR_MONTH")
        return MonthCore(
            month_utc=month_utc,
            starting_nav=None,
            ending_nav=None,
            net_pnl=None,
            monthly_return_pct=None,
            realized_pnl=None,
            unrealized_pnl=None,
            deposits_additions=None,
            withdrawals=None,
            net_external_flow=None,
            return_excluding_external_flows=None,
            max_drawdown_for_month=None,
            trading_days_count=0,
            trades_count=0,
            winning_days_count=None,
            losing_days_count=None,
            blocker_codes=tuple(blockers),
            reason_codes=tuple(reasons),
            nav_paths=tuple(),
            fill_paths=tuple(),
            baseline_path=None,
        )

    idx_by_day = {r.day: i for i, r in enumerate(rows)}
    first = month_rows[0]
    last = month_rows[-1]
    first_idx = idx_by_day[first.day]
    baseline = rows[first_idx - 1] if first_idx > 0 else None
    if baseline is None:
        reasons.append("BASELINE_PREV_DAY_MISSING_USING_FIRST_DAY_BASELINE")

    starting_row = baseline or first
    starting_nav = starting_row.nav_total
    ending_nav = last.nav_total

    daily_pnls: List[Decimal] = []
    daily_flows: List[Decimal] = []
    wins = 0
    losses = 0

    for r in month_rows:
        idx = idx_by_day[r.day]
        if idx == 0:
            continue
        prev = rows[idx - 1]
        daily_pnl = r.pnl_total - prev.pnl_total
        daily_flow = (r.nav_total - prev.nav_total) - daily_pnl
        daily_pnls.append(daily_pnl)
        daily_flows.append(daily_flow)
        if daily_pnl > 0:
            wins += 1
        elif daily_pnl < 0:
            losses += 1

    net_pnl = sum(daily_pnls, Decimal("0"))
    deposits = sum((x for x in daily_flows if x > 0), Decimal("0"))
    withdrawals = sum(((-x) for x in daily_flows if x < 0), Decimal("0"))
    net_external = deposits - withdrawals

    if starting_nav <= 0:
        blockers.append("STARTING_NAV_NON_POSITIVE")
        month_ret = None
        ret_ex_flow = None
    else:
        month_ret = (ending_nav - starting_nav) / starting_nav
        ret_ex_flow = net_pnl / starting_nav

    realized_pnl = last.realized - starting_row.realized
    unrealized_pnl = last.unrealized - starting_row.unrealized

    peak = starting_nav
    min_dd = Decimal("0")
    if peak <= 0:
        max_drawdown = None
        blockers.append("DRAWDOWN_BASELINE_NON_POSITIVE")
    else:
        for r in month_rows:
            if r.nav_total > peak:
                peak = r.nav_total
            if peak > 0:
                dd = (r.nav_total - peak) / peak
                if dd < min_dd:
                    min_dd = dd
        max_drawdown = min_dd

    fill_paths = _load_fill_ledger_files_for_days(truth_root, [r.day_utc for r in month_rows])
    trades_count = _count_trades_from_fill_ledgers(fill_paths)

    return MonthCore(
        month_utc=month_utc,
        starting_nav=starting_nav,
        ending_nav=ending_nav,
        net_pnl=net_pnl,
        monthly_return_pct=month_ret,
        realized_pnl=realized_pnl,
        unrealized_pnl=unrealized_pnl,
        deposits_additions=deposits,
        withdrawals=withdrawals,
        net_external_flow=net_external,
        return_excluding_external_flows=ret_ex_flow,
        max_drawdown_for_month=max_drawdown,
        trading_days_count=len(month_rows),
        trades_count=trades_count,
        winning_days_count=wins,
        losing_days_count=losses,
        blocker_codes=tuple(sorted(set(blockers))),
        reason_codes=tuple(sorted(set(reasons))),
        nav_paths=tuple(r.source_path for r in month_rows),
        fill_paths=tuple(fill_paths),
        baseline_path=starting_row.source_path if baseline is not None else None,
    )


def _rolling_return(
    month_utc: str,
    months: int,
    core_by_month: Dict[str, MonthCore],
) -> Tuple[Optional[Decimal], List[str]]:
    reasons: List[str] = []
    expected = _expected_window(month_utc, months)
    cores: List[MonthCore] = []
    for m in expected:
        c = core_by_month.get(m)
        if c is None:
            reasons.append(f"MISSING_MONTH_{m}_FOR_ROLLING_{months}")
            return None, reasons
        if c.return_excluding_external_flows is None:
            reasons.append(f"RETURN_EX_FLOW_UNAVAILABLE_{m}_FOR_ROLLING_{months}")
            return None, reasons
        cores.append(c)
    comp = Decimal("1")
    for c in cores:
        comp *= (Decimal("1") + c.return_excluding_external_flows)  # type: ignore[arg-type]
    return comp - Decimal("1"), reasons


def _best_worst_last_12(month_utc: str, core_by_month: Dict[str, MonthCore]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], List[str]]:
    reasons: List[str] = []
    expected = _expected_window(month_utc, 12)
    candidates: List[MonthCore] = []
    for m in expected:
        c = core_by_month.get(m)
        if c is None or c.return_excluding_external_flows is None:
            continue
        candidates.append(c)
    if len(candidates) < 12:
        reasons.append("INSUFFICIENT_MONTH_HISTORY_FOR_LAST_12_WINDOW")
    if not candidates:
        return None, None, reasons
    best = max(candidates, key=lambda c: c.return_excluding_external_flows or Decimal("-999"))
    worst = min(candidates, key=lambda c: c.return_excluding_external_flows or Decimal("999"))
    best_obj = {
        "month": best.month_utc,
        "return_pct": _fmt_ret(best.return_excluding_external_flows or Decimal("0")),
        "net_pnl": float(best.net_pnl) if best.net_pnl is not None else None,
    }
    worst_obj = {
        "month": worst.month_utc,
        "return_pct": _fmt_ret(worst.return_excluding_external_flows or Decimal("0")),
        "net_pnl": float(worst.net_pnl) if worst.net_pnl is not None else None,
    }
    return best_obj, worst_obj, reasons


def _status_from_codes(blockers: List[str], reasons: List[str]) -> str:
    if blockers:
        return "BLOCKED"
    if reasons:
        return "DEGRADED"
    return "ACTIVE"


def _build_report(month_utc: str, truth_root: Path) -> Dict[str, Any]:
    rows = _load_nav_rows(truth_root)
    months = sorted({_month_key(r.day) for r in rows})
    core_by_month: Dict[str, MonthCore] = {}
    for m in months:
        core_by_month[m] = _compute_month_core(m, rows, truth_root)

    target_core = core_by_month.get(month_utc)
    if target_core is None:
        target_core = _compute_month_core(month_utc, rows, truth_root)
        core_by_month[month_utc] = target_core

    reasons = list(target_core.reason_codes)
    blockers = list(target_core.blocker_codes)

    rolling_3, r3_reasons = _rolling_return(month_utc, 3, core_by_month)
    rolling_6, r6_reasons = _rolling_return(month_utc, 6, core_by_month)
    rolling_12, r12_reasons = _rolling_return(month_utc, 12, core_by_month)
    reasons.extend(r3_reasons + r6_reasons + r12_reasons)

    avg_12 = None
    expected_12 = _expected_window(month_utc, 12)
    month_values: List[Decimal] = []
    for m in expected_12:
        c = core_by_month.get(m)
        if c is None or c.net_pnl is None:
            month_values = []
            reasons.append("INSUFFICIENT_MONTH_HISTORY_FOR_ROLLING_12_DOLLAR_AVG")
            break
        month_values.append(c.net_pnl)
    if month_values:
        avg_12 = sum(month_values, Decimal("0")) / Decimal(len(month_values))

    best_12, worst_12, bw_reasons = _best_worst_last_12(month_utc, core_by_month)
    reasons.extend(bw_reasons)

    status = _status_from_codes(blockers, reasons)
    quality_status = "COMPLETE" if status == "ACTIVE" else ("BLOCKED" if status == "BLOCKED" else "DEGRADED")

    input_manifest: List[Dict[str, Any]] = []
    for p in target_core.nav_paths:
        input_manifest.append(
            {
                "type": "accounting_nav_v2",
                "path": str(p),
                "sha256": _sha256_file(p),
            }
        )
    if target_core.baseline_path is not None:
        input_manifest.append(
            {
                "type": "accounting_nav_v2_baseline_prev_day",
                "path": str(target_core.baseline_path),
                "sha256": _sha256_file(target_core.baseline_path),
            }
        )
    for p in target_core.fill_paths:
        input_manifest.append(
            {
                "type": "fill_ledger_v1",
                "path": str(p),
                "sha256": _sha256_file(p),
            }
        )

    report = {
        "month": month_utc,
        "starting_nav": float(target_core.starting_nav) if target_core.starting_nav is not None else None,
        "ending_nav": float(target_core.ending_nav) if target_core.ending_nav is not None else None,
        "net_pnl": float(target_core.net_pnl) if target_core.net_pnl is not None else None,
        "monthly_return_pct": _fmt_ret(target_core.monthly_return_pct) if target_core.monthly_return_pct is not None else None,
        "realized_pnl": float(target_core.realized_pnl) if target_core.realized_pnl is not None else None,
        "unrealized_pnl": float(target_core.unrealized_pnl) if target_core.unrealized_pnl is not None else None,
        "deposits_additions": float(target_core.deposits_additions) if target_core.deposits_additions is not None else None,
        "withdrawals": float(target_core.withdrawals) if target_core.withdrawals is not None else None,
        "net_external_flow": float(target_core.net_external_flow) if target_core.net_external_flow is not None else None,
        "return_excluding_external_flows": _fmt_ret(target_core.return_excluding_external_flows) if target_core.return_excluding_external_flows is not None else None,
        "max_drawdown_for_month": _fmt_dd(target_core.max_drawdown_for_month) if target_core.max_drawdown_for_month is not None else None,
        "trading_days_count": target_core.trading_days_count,
        "trades_count": target_core.trades_count,
        "winning_days_count": target_core.winning_days_count,
        "losing_days_count": target_core.losing_days_count,
        "rolling_3_month_return_pct": _fmt_ret(rolling_3) if rolling_3 is not None else None,
        "rolling_6_month_return_pct": _fmt_ret(rolling_6) if rolling_6 is not None else None,
        "rolling_12_month_return_pct": _fmt_ret(rolling_12) if rolling_12 is not None else None,
        "rolling_12_month_avg_return_dollars": float(avg_12) if avg_12 is not None else None,
        "best_month_last_12": best_12,
        "worst_month_last_12": worst_12,
        "data_quality_status": quality_status,
        "blocker_codes": sorted(set(blockers)),
    }

    return {
        "schema_id": "C2_MONTHLY_PERFORMANCE_SUMMARY_V1",
        "schema_version": 1,
        "produced_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "month_utc": month_utc,
        "status": status,
        "data_quality_status": quality_status,
        "blocker_codes": sorted(set(blockers)),
        "reason_codes": sorted(set(reasons)),
        "input_manifest": input_manifest,
        "report": report,
    }


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="run_monthly_performance_summary_v1")
    ap.add_argument("--month_utc", required=True, help="YYYY-MM")
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    _parse_month_utc(args.month_utc)
    month_utc = str(args.month_utc).strip()
    truth_root = _resolve_truth_root(args.truth_root)

    obj = _build_report(month_utc, truth_root)
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)

    out_path = (truth_root / "reports" / "monthly_performance_summary_v1" / month_utc / "monthly_performance_summary.v1.json").resolve()
    _immut_write(out_path, _json_bytes(obj))
    print(f"OK: wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
