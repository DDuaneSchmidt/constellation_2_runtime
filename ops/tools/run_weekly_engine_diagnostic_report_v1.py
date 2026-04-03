#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any


DEFAULT_TRUTH_ROOT = Path("constellation_2/runtime/truth")
DEC6 = Decimal("0.000001")
DEC2 = Decimal("0.01")


@dataclass
class EngineStats:
    intents_emitted: int = 0
    orders_submitted: int = 0
    fills_completed: int = 0
    filled_qty: int = 0
    open_or_unfilled_orders: int = 0
    allocation_block_count: int = 0
    allocation_active_count: int = 0
    heartbeat_ok_days: int = 0
    heartbeat_warn_days: int = 0
    heartbeat_fail_days: int = 0
    return_samples: list[Decimal] = field(default_factory=list)
    block_reasons: Counter = field(default_factory=Counter)
    heartbeat_reason_codes: Counter = field(default_factory=Counter)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deterministic weekly Constellation engine diagnostic markdown generator."
    )
    parser.add_argument(
        "--week-end",
        required=True,
        help="Inclusive week-end day in YYYY-MM-DD.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Inclusive lookback window in calendar days. Default: 7",
    )
    parser.add_argument(
        "--truth-root",
        default=str(DEFAULT_TRUTH_ROOT),
        help="Truth root path. Default: constellation_2/runtime/truth",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional markdown output file path.",
    )
    return parser.parse_args()


def fail(msg: str) -> None:
    print(f"FAIL_CLOSED: {msg}", file=sys.stderr)
    raise SystemExit(1)


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        fail(f"cannot parse json: {path} :: {exc}")
    raise AssertionError("unreachable")


def parse_day(day_text: str) -> date:
    try:
        return datetime.strptime(day_text, "%Y-%m-%d").date()
    except ValueError as exc:
        fail(f"invalid day format '{day_text}': {exc}")
    raise AssertionError("unreachable")


def iso_day(d: date) -> str:
    return d.isoformat()


def daterange_inclusive(end_day: date, days: int) -> list[date]:
    if days < 1:
        fail("--days must be >= 1")
    start_day = end_day - timedelta(days=days - 1)
    out: list[date] = []
    current = start_day
    while current <= end_day:
        out.append(current)
        current += timedelta(days=1)
    return out


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return None
        try:
            return Decimal(text)
        except InvalidOperation:
            return None
    return None


def fmt_dec(value: Decimal | None, places: int = 6) -> str:
    if value is None:
        return "NOT_AVAILABLE_FROM_PROVEN_INPUTS"
    q = Decimal("1").scaleb(-places)
    return str(value.quantize(q, rounding=ROUND_HALF_UP))


def fmt_pct(value: Decimal | None, places: int = 6) -> str:
    if value is None:
        return "NOT_AVAILABLE_FROM_PROVEN_INPUTS"
    q = Decimal("1").scaleb(-places)
    return str(value.quantize(q, rounding=ROUND_HALF_UP)) + "%"


def pct_change(start_value: Decimal | None, end_value: Decimal | None) -> Decimal | None:
    if start_value is None or end_value is None:
        return None
    if start_value == 0:
        return None
    return ((end_value - start_value) / start_value) * Decimal("100")


def min_drawdown_pct(nav_values: list[Decimal]) -> Decimal | None:
    if not nav_values:
        return None

    if all(v == 0 for v in nav_values):
        return None

    peak = None
    worst = Decimal("0")

    for nav in nav_values:
        if nav <= 0:
            continue

        if peak is None or nav > peak:
            peak = nav

        if peak is None or peak == 0:
            continue

        dd = ((nav - peak) / peak) * Decimal("100")
        if dd < worst:
            worst = dd

    return worst if peak is not None else None

def series_drawdown_from_returns(samples: list[Decimal]) -> Decimal | None:
    if not samples:
        return None
    equity = Decimal("1")
    peak = Decimal("1")
    worst = Decimal("0")
    for r in samples:
        equity = equity * (Decimal("1") + r)
        if equity > peak:
            peak = equity
        if peak == 0:
            continue
        dd = ((equity - peak) / peak) * Decimal("100")
        if dd < worst:
            worst = dd
    return worst


def average(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values) / Decimal(len(values))


def scan_invalid_counts(truth_root: Path, days: list[date]) -> tuple[int, dict[str, int]]:
    patterns = [
        "accounting_v2/nav/{day}/*.INVALID_*.json",
        "accounting_v2/attribution/{day}/*.INVALID_*.json",
        "intents_v1/day_rollup/{day}/*.INVALID_*.json",
        "fill_ledger_v1/{day}/*.INVALID_*.json",
        "monitoring_v1/engine_daily_returns_v1/{day}/*.INVALID_*.json",
        "monitoring_v1/engine_correlation_matrix/{day}/*.INVALID_*.json",
        "allocation_v1/decisions/{day}/*.INVALID_*.json",
    ]
    totals: dict[str, int] = {}
    total = 0
    for d in days:
        ds = iso_day(d)
        for pattern in patterns:
            rel = pattern.format(day=ds)
            count = len(list(truth_root.glob(rel)))
            if count > 0:
                totals[rel] = count
                total += count
    return total, totals


def load_nav_day(truth_root: Path, d: date) -> dict[str, Any] | None:
    path = truth_root / "accounting_v2" / "nav" / iso_day(d) / "nav.v2.json"
    if not path.exists():
        return None
    data = read_json(path)
    nav_block = data.get("nav")
    if not isinstance(nav_block, dict):
        nav_block = {}
    return {
        "path": str(path),
        "status": data.get("status", "UNKNOWN"),
        "reason_codes": data.get("reason_codes", []),
        "nav_total": to_decimal(nav_block.get("nav_total")),
        "realized_pnl_to_date": to_decimal(nav_block.get("realized_pnl_to_date")),
        "unrealized_pnl": to_decimal(nav_block.get("unrealized_pnl")),
    }


def load_attribution_day(truth_root: Path, d: date) -> dict[str, Any] | None:
    path = truth_root / "accounting_v2" / "attribution" / iso_day(d) / "engine_attribution.v2.json"
    if not path.exists():
        return None
    data = read_json(path)
    return {
        "path": str(path),
        "status": data.get("status", "UNKNOWN"),
        "reason_codes": data.get("reason_codes", []),
    }


def load_intents_day_rollup(truth_root: Path, d: date) -> dict[str, Any] | None:
    path = truth_root / "intents_v1" / "day_rollup" / iso_day(d) / "intents_day_rollup.v1.json"
    if not path.exists():
        return None
    return read_json(path)


def load_fill_ledgers(truth_root: Path, d: date) -> list[dict[str, Any]]:
    day_dir = truth_root / "fill_ledger_v1" / iso_day(d)
    if not day_dir.exists():
        return []
    out: list[dict[str, Any]] = []
    for path in sorted(day_dir.glob("*.fill_ledger.v1.json")):
        out.append(read_json(path))
    return out


def load_allocation_decisions(truth_root: Path, d: date) -> list[dict[str, Any]]:
    day_dir = truth_root / "allocation_v1" / "decisions" / iso_day(d)
    if not day_dir.exists():
        return []
    out: list[dict[str, Any]] = []
    for path in sorted(day_dir.glob("*.allocation_decision.v1.json")):
        out.append(read_json(path))
    return out


def load_engine_daily_returns(truth_root: Path, d: date) -> dict[str, Any] | None:
    path = truth_root / "monitoring_v1" / "engine_daily_returns_v1" / iso_day(d) / "engine_daily_returns.v1.json"
    if not path.exists():
        return None
    return read_json(path)


def load_engine_correlation(truth_root: Path, d: date) -> dict[str, Any] | None:
    path = truth_root / "monitoring_v1" / "engine_correlation_matrix" / iso_day(d) / "engine_correlation_matrix.v1.json"
    if not path.exists():
        return None
    return read_json(path)


def load_engine_heartbeats(truth_root: Path, d: date) -> list[dict[str, Any]]:
    day_dir = truth_root / "monitoring_v1" / "engine_heartbeat_v1" / iso_day(d)
    if not day_dir.exists():
        return []
    out: list[dict[str, Any]] = []
    for path in sorted(day_dir.glob("*/engine_heartbeat.v1.json")):
        out.append(read_json(path))
    return out


def collect_all_nav_days(truth_root: Path) -> list[date]:
    base = truth_root / "accounting_v2" / "nav"
    if not base.exists():
        return []
    out: list[date] = []
    for path in sorted(base.glob("*/nav.v2.json")):
        try:
            out.append(parse_day(path.parent.name))
        except SystemExit:
            raise
        except Exception:
            continue
    return sorted(set(out))


def latest_nav_on_or_before(nav_map: dict[date, dict[str, Any]], target: date) -> dict[str, Any] | None:
    candidates = [d for d in nav_map if d <= target]
    if not candidates:
        return None
    return nav_map[max(candidates)]


def confidence_label(trades: int) -> str:
    if trades < 5:
        return "low sample size"
    if trades <= 20:
        return "moderate evidence"
    return "strong evidence"


def grade_engine(stats: EngineStats) -> tuple[str, str]:
    avg_return = average(stats.return_samples)
    confidence = confidence_label(stats.fills_completed)

    if stats.heartbeat_fail_days > 0 and stats.intents_emitted == 0 and stats.fills_completed == 0:
        return "F", confidence

    if avg_return is None:
        if stats.intents_emitted == 0 and stats.heartbeat_ok_days > 0:
            return "C", confidence
        if stats.fills_completed > 0:
            return "B", confidence
        return "C", confidence

    if avg_return > 0:
        if stats.fills_completed > 20:
            return "A", confidence
        return "B", confidence

    if avg_return < 0:
        if stats.fills_completed > 20:
            return "F", confidence
        return "D", confidence

    return "C", confidence


def build_report(args: argparse.Namespace) -> str:
    truth_root = Path(args.truth_root)
    if not truth_root.exists():
        fail(f"truth root does not exist: {truth_root}")

    week_end = parse_day(args.week_end)
    days = daterange_inclusive(week_end, args.days)
    week_start = days[0]

    invalid_total, invalid_details = scan_invalid_counts(truth_root, days)

    nav_by_day: dict[date, dict[str, Any]] = {}
    attribution_by_day: dict[date, dict[str, Any]] = {}
    engine_stats: dict[str, EngineStats] = defaultdict(EngineStats)

    total_intents = 0
    total_orders = 0
    total_fills = 0
    total_filled_qty = 0
    total_open_or_unfilled = 0

    missing_artifacts: list[str] = []
    allocation_block_reasons: Counter = Counter()

    for d in days:
        nav = load_nav_day(truth_root, d)
        if nav is None:
            missing_artifacts.append(f"missing nav.v2 for {iso_day(d)}")
        else:
            nav_by_day[d] = nav

        attribution = load_attribution_day(truth_root, d)
        if attribution is None:
            missing_artifacts.append(f"missing engine_attribution.v2 for {iso_day(d)}")
        else:
            attribution_by_day[d] = attribution

        intents_rollup = load_intents_day_rollup(truth_root, d)
        if intents_rollup is None:
            missing_artifacts.append(f"missing intents_day_rollup.v1 for {iso_day(d)}")
        else:
            engines = intents_rollup.get("engines", [])
            if isinstance(engines, list):
                for item in engines:
                    if not isinstance(item, dict):
                        continue
                    engine_id = str(item.get("engine_id", "UNKNOWN_ENGINE"))
                    intent_count = int(item.get("intent_count", 0))
                    engine_stats[engine_id].intents_emitted += intent_count
                    total_intents += intent_count

        for item in load_fill_ledgers(truth_root, d):
            engine_id = str(item.get("engine_id", "UNKNOWN_ENGINE"))
            filled_qty = int(item.get("filled_qty", 0))
            remaining_qty = int(item.get("remaining_qty", 0))
            status = str(item.get("status", "UNKNOWN"))
            lifecycle_status = str(item.get("lifecycle_status", "UNKNOWN"))

            if status == "OK" and filled_qty > 0:
                engine_stats[engine_id].fills_completed += 1
                total_fills += 1

            engine_stats[engine_id].filled_qty += filled_qty
            total_filled_qty += filled_qty

            if filled_qty == 0 or remaining_qty > 0 or lifecycle_status in {"OPEN", "PARTIALLY_FILLED"}:
                engine_stats[engine_id].open_or_unfilled_orders += 1
                total_open_or_unfilled += 1

        for item in load_allocation_decisions(truth_root, d):
            decision = item.get("decision")
            if not isinstance(decision, dict):
                continue

            engine_id = str(decision.get("engine_id", "UNKNOWN_ENGINE"))
            engine_stats[engine_id].orders_submitted += 1
            total_orders += 1

            status = str(item.get("status", "UNKNOWN"))
            if status == "BLOCK":
                engine_stats[engine_id].allocation_block_count += 1
                constraints = decision.get("binding_constraints", [])
                if isinstance(constraints, list):
                    for reason in constraints:
                        reason_text = str(reason)
                        engine_stats[engine_id].block_reasons[reason_text] += 1
                        allocation_block_reasons[reason_text] += 1
            else:
                engine_stats[engine_id].allocation_active_count += 1

        for item in load_engine_heartbeats(truth_root, d):
            engine_id = str(item.get("engine_id", "UNKNOWN_ENGINE"))
            status = str(item.get("status", "UNKNOWN"))
            reasons = item.get("reason_codes", [])
            if status == "OK":
                engine_stats[engine_id].heartbeat_ok_days += 1
            elif status == "WARN":
                engine_stats[engine_id].heartbeat_warn_days += 1
            elif status == "FAIL":
                engine_stats[engine_id].heartbeat_fail_days += 1
            if isinstance(reasons, list):
                for reason in reasons:
                    engine_stats[engine_id].heartbeat_reason_codes[str(reason)] += 1

        returns_doc = load_engine_daily_returns(truth_root, d)
        if returns_doc is not None:
            returns = returns_doc.get("returns", [])
            if isinstance(returns, list):
                for item in returns:
                    if not isinstance(item, dict):
                        continue
                    engine_id = str(item.get("engine_id", "UNKNOWN_ENGINE"))
                    value = to_decimal(item.get("daily_return"))
                    if value is not None:
                        engine_stats[engine_id].return_samples.append(value)

    all_nav_days = collect_all_nav_days(truth_root)
    if not all_nav_days:
        fail("no accounting_v2 nav.v2.json files found anywhere under truth root")

    earliest_nav_day = min(all_nav_days)
    earliest_nav = load_nav_day(truth_root, earliest_nav_day)
    if earliest_nav is None:
        fail("earliest nav day was discovered but nav file could not be loaded")

    baseline_before_week = latest_nav_on_or_before(nav_by_day, week_start - timedelta(days=1))
    if baseline_before_week is None:
        baseline_before_week = earliest_nav

    end_nav = nav_by_day.get(week_end)
    start_nav = nav_by_day.get(week_start)

    week_nav_values = [
        nav_by_day[d]["nav_total"]
        for d in days
        if d in nav_by_day and isinstance(nav_by_day[d].get("nav_total"), Decimal)
    ]
    week_nav_values = [v for v in week_nav_values if isinstance(v, Decimal)]

    weekly_nav_change = None
    weekly_return = None
    realized_pnl = None
    unrealized_pnl = None
    cumulative_return = None
    max_drawdown_week = None
    if week_nav_values:
        non_zero_nav_values = [v for v in week_nav_values if v != 0]
        if non_zero_nav_values:
            nav_statuses_in_window = {
                str(nav_by_day[d].get("status", "UNKNOWN"))
                for d in days
                if d in nav_by_day
            }
            if "BOOTSTRAP" not in nav_statuses_in_window:
                max_drawdown_week = min_drawdown_pct(week_nav_values)
    if start_nav is not None and end_nav is not None:
        weekly_nav_change = (
            end_nav.get("nav_total") - start_nav.get("nav_total")
            if start_nav.get("nav_total") is not None and end_nav.get("nav_total") is not None
            else None
        )
        weekly_return = pct_change(start_nav.get("nav_total"), end_nav.get("nav_total"))

    if baseline_before_week is not None and end_nav is not None:
        start_realized = baseline_before_week.get("realized_pnl_to_date")
        end_realized = end_nav.get("realized_pnl_to_date")
        if start_realized is not None and end_realized is not None:
            realized_pnl = end_realized - start_realized

    if end_nav is not None:
        unrealized_pnl = end_nav.get("unrealized_pnl")

    if earliest_nav is not None and end_nav is not None:
        cumulative_return = pct_change(earliest_nav.get("nav_total"), end_nav.get("nav_total"))

    intent_to_fill_ratio = None
    if total_intents > 0:
        intent_to_fill_ratio = (Decimal(total_fills) / Decimal(total_intents)) * Decimal("100")

    correlation_doc = load_engine_correlation(truth_root, week_end)
    correlation_status = "NOT_AVAILABLE_FROM_PROVEN_INPUTS"
    correlation_reason_codes = "NOT_AVAILABLE_FROM_PROVEN_INPUTS"
    if correlation_doc is not None:
        correlation_status = str(correlation_doc.get("status", "UNKNOWN"))
        correlation_reason_codes = ", ".join(correlation_doc.get("reason_codes", [])) or "NONE"

    end_nav_status = end_nav.get("status") if end_nav is not None else "MISSING"
    end_nav_reasons = ", ".join(end_nav.get("reason_codes", [])) if end_nav is not None else "MISSING"
    degraded_attr_days = [
        f"{iso_day(d)}:{attribution_by_day[d]['status']}"
        for d in sorted(attribution_by_day)
        if attribution_by_day[d]["status"] != "OK"
    ]

    heartbeat_fail_total = sum(stats.heartbeat_fail_days for stats in engine_stats.values())
    live_zero_intent_engines = [
        engine_id
        for engine_id, stats in sorted(engine_stats.items())
        if stats.heartbeat_ok_days > 0 and stats.intents_emitted == 0
    ]

    score = 0
    if invalid_total == 0:
        score += 1
    if total_intents > 0:
        score += 1
    if total_fills > 0:
        score += 1
    if heartbeat_fail_total == 0:
        score += 1
    if end_nav_status not in {"MISSING", "BOOTSTRAP"}:
        score += 1

    if score >= 5:
        system_grade = "A"
    elif score == 4:
        system_grade = "B"
    elif score == 3:
        system_grade = "C"
    elif score == 2:
        system_grade = "D"
    else:
        system_grade = "F"

    lines: list[str] = []
    lines.append("# Constellation Weekly Engine Diagnostic Review")
    lines.append("")
    lines.append(f"Window: {iso_day(week_start)} through {iso_day(week_end)}")
    lines.append("Mode: Deterministic Research Auditor")
    lines.append("")

    lines.append("## 1. System Overview")
    lines.append("")
    lines.append(f"- total trades executed: {total_fills}")
    lines.append(f"- total intents emitted: {total_intents}")
    lines.append(f"- intent-to-fill ratio: {fmt_pct(intent_to_fill_ratio)}")
    lines.append(f"- realized P&L: {fmt_dec(realized_pnl)}")
    lines.append(f"- unrealized P&L: {fmt_dec(unrealized_pnl)}")
    lines.append(f"- weekly return: {fmt_pct(weekly_return)}")
    lines.append(f"- cumulative return: {fmt_pct(cumulative_return)}")
    lines.append(f"- portfolio NAV change: {fmt_dec(weekly_nav_change)}")
    lines.append(f"- max drawdown this week: {fmt_pct(max_drawdown_week)}")
    lines.append(f"- end NAV status: {end_nav_status}")
    lines.append(f"- end NAV reason codes: {end_nav_reasons}")
    lines.append(f"- Constellation weekly grade: {system_grade}")
    lines.append("")
    lines.append("Dataset Integrity")
    if invalid_total == 0 and not missing_artifacts:
        lines.append("- DATA_INTEGRITY_WARNING: none")
    else:
        lines.append(f"- DATA_INTEGRITY_WARNING count: {invalid_total + len(missing_artifacts)}")
        for item in missing_artifacts[:20]:
            lines.append(f"  - {item}")
        for pattern, count in sorted(invalid_details.items()):
            lines.append(f"  - invalid artifacts: {pattern} :: {count}")
    lines.append("")
    lines.append("Abnormal Behavior")
    if degraded_attr_days:
        lines.append(f"- degraded attribution days: {', '.join(degraded_attr_days)}")
    else:
        lines.append("- degraded attribution days: none")
    lines.append(f"- correlation matrix status: {correlation_status}")
    lines.append(f"- correlation matrix reason codes: {correlation_reason_codes}")
    if live_zero_intent_engines:
        lines.append(f"- live engines with zero intents: {', '.join(live_zero_intent_engines)}")
    else:
        lines.append("- live engines with zero intents: none")
    lines.append("")

    lines.append("## 2. Engine Activity Analysis")
    lines.append("")
    if not engine_stats:
        lines.append("No engine evidence found in the selected window.")
    else:
        for engine_id in sorted(engine_stats):
            stats = engine_stats[engine_id]
            grade, confidence = grade_engine(stats)
            avg_return = average(stats.return_samples)
            dd_proxy = series_drawdown_from_returns(stats.return_samples)
            lines.append(f"### {engine_id}")
            lines.append(f"- candidate opportunities: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
            lines.append(f"- intents emitted: {stats.intents_emitted}")
            lines.append(f"- allocation decisions observed: {stats.orders_submitted}")
            lines.append(f"- order submissions observed from proven inputs: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
            lines.append(f"- fills completed: {stats.fills_completed}")
            lines.append(f"- win rate: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
            lines.append(f"- average win: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
            lines.append(f"- average loss: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
            lines.append(f"- expectancy: EXPECTANCY_NOT_PROVEN_V1")
            lines.append(f"- daily return expectancy proxy: {fmt_dec(avg_return)}")
            lines.append(f"- total P&L: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
            lines.append(f"- max drawdown: {fmt_pct(dd_proxy)}")
            lines.append(f"- average hold time: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
            lines.append(f"- symbol concentration: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
            lines.append(f"- time-of-day distribution: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
            lines.append(f"- heartbeat OK/WARN/FAIL days: {stats.heartbeat_ok_days}/{stats.heartbeat_warn_days}/{stats.heartbeat_fail_days}")
            lines.append(f"- grade: {grade}")
            lines.append(f"- confidence level: {confidence}")
            lines.append("")
    lines.append("")

    lines.append("## 3. Intent Pipeline Diagnostics")
    lines.append("")
    lines.append(f"- candidate opportunities: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append(f"- filtered signals: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append(f"- intents: {total_intents}")
    lines.append(f"- allocation decisions observed: {total_orders}")
    lines.append(f"- order submissions observed from proven inputs: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append(f"- fills: {total_fills}")
    lines.append(f"- intent suppression rate: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append(f"- order rejection rate: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append(f"- open or unfilled orders observed: {total_open_or_unfilled}")
    lines.append("")
    lines.append("Top Three Gating Bottlenecks")
    if allocation_block_reasons:
        for reason, count in allocation_block_reasons.most_common(3):
            lines.append(f"- {reason}: {count}")
    else:
        lines.append("- NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append("")
    lines.append("Pipeline Diagnosis")
    if live_zero_intent_engines:
        lines.append(f"- over-filtering or no-signal risk: {', '.join(live_zero_intent_engines)}")
    else:
        lines.append("- over-filtering or no-signal risk: none proven")
    if total_orders > 0 and total_fills == 0:
        lines.append("- allocation decisions were observed but no completed fills were proven")
    elif total_orders == 0 and total_intents > 0:
        lines.append("- emissions reached intents but not allocation decisions")
    else:
        lines.append("- no single collapse point dominates the full observed window")
    lines.append("")

    lines.append("## 4. Parameter Efficiency Review")
    lines.append("")
    lines.append("NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append("")
    lines.append("Reason")
    lines.append("- parameter files and parameter schemas were not proven to this generator version")
    lines.append("- this v1 generator therefore does not infer or guess parameter values")
    lines.append("")

    lines.append("## 5. Risk and Capital Efficiency")
    lines.append("")
    lines.append(f"- NAV utilization: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append(f"- capital allocation per sleeve: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append(f"- unused capital: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append(f"- exposure concentration: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append(f"- drawdown containment: {fmt_pct(max_drawdown_week)}")
    lines.append(f"- trade sizing behavior: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append(f"- return on deployed capital: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append(f"- engine correlation status: {correlation_status}")
    lines.append("")

    lines.append("## 6. Trade Outcome Diagnostics")
    lines.append("")
    lines.append("- R-multiple distribution: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append("- hold time distribution: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append("- win/loss clustering: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append("- symbol-specific results: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append("- time-of-day patterns: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append("- stop-loss / target diagnostics: NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append("")

    lines.append("## 7. Regime Analysis")
    lines.append("")
    lines.append("NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append("")
    lines.append("Reason")
    lines.append("- regime snapshot surfaces were not proven to this generator version")
    lines.append("")

    lines.append("## 8. Engine Health Table")
    lines.append("")
    lines.append("| engine | grade | trades | expectancy | drawdown | confidence level |")
    lines.append("|---|---:|---:|---|---:|---|")
    if not engine_stats:
        lines.append("| NONE | C | 0 | EXPECTANCY_NOT_PROVEN_V1 | NOT_AVAILABLE_FROM_PROVEN_INPUTS | low sample size |")
    else:
        for engine_id in sorted(engine_stats):
            stats = engine_stats[engine_id]
            grade, confidence = grade_engine(stats)
            dd_proxy = series_drawdown_from_returns(stats.return_samples)
            lines.append(
                f"| {engine_id} | {grade} | {stats.fills_completed} | EXPECTANCY_NOT_PROVEN_V1 | {fmt_pct(dd_proxy)} | {confidence} |"
            )
    lines.append("")

    lines.append("## 9. Parameter Adjustment Recommendations")
    lines.append("")
    lines.append("No exact deterministic parameter adjustments are emitted in v1.")
    lines.append("")
    lines.append("Reason")
    lines.append("- exact parameter surfaces were not proven")
    lines.append("- this generator will not invent parameter names or values")
    lines.append("")

    lines.append("## 10. Sandbox Experiment Proposals")
    lines.append("")
    lines.append("No exact sandbox experiments are emitted in v1.")
    lines.append("")
    lines.append("Reason")
    lines.append("- exact rule-change surfaces were not proven")
    lines.append("- this generator will not fabricate deterministic rule deltas")
    lines.append("")

    lines.append("## 11. Critical Risks")
    lines.append("")
    if invalid_total > 0:
        lines.append(f"- invalid truth artifacts detected in window: {invalid_total}")
    else:
        lines.append("- invalid truth artifacts detected in window: none")
    if end_nav_status == "BOOTSTRAP":
        lines.append("- NAV remains in BOOTSTRAP status at week end")
    if degraded_attr_days:
        lines.append("- engine attribution degraded on one or more days")
    if live_zero_intent_engines:
        lines.append("- live engines produced zero intents across the observed window")
    if total_orders > 0 and total_fills == 0:
        lines.append("- allocation activity observed without completed fills")
    if not degraded_attr_days and not live_zero_intent_engines and invalid_total == 0:
        lines.append("- no critical structural risk dominated the proven v1 surfaces")
    lines.append("")

    lines.append("## 12. Weekly Summary")
    lines.append("")
    lines.append("Top three engine improvement opportunities")
    opportunities = []
    for engine_id in sorted(engine_stats):
        stats = engine_stats[engine_id]
        if stats.heartbeat_ok_days > 0 and stats.intents_emitted == 0:
            opportunities.append(f"{engine_id}: engine alive but emitted zero intents")
        if stats.allocation_block_count > 0:
            opportunities.append(f"{engine_id}: allocation blocks observed ({stats.allocation_block_count})")
        if stats.open_or_unfilled_orders > 0:
            opportunities.append(f"{engine_id}: open or unfilled orders observed ({stats.open_or_unfilled_orders})")
    if opportunities:
        for item in opportunities[:3]:
            lines.append(f"- {item}")
    else:
        lines.append("- none proven from current v1 surfaces")
    lines.append("")
    lines.append("Top three parameter adjustments")
    lines.append("- NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append("")
    lines.append("Top three sandbox experiments")
    lines.append("- NOT_AVAILABLE_FROM_PROVEN_INPUTS")
    lines.append("")
    lines.append("Key risks to monitor next week")
    risk_items = []
    if invalid_total > 0:
        risk_items.append(f"invalid artifact recurrence ({invalid_total})")
    if degraded_attr_days:
        risk_items.append("degraded attribution persistence")
    if live_zero_intent_engines:
        risk_items.append("continued zero-intent live engines")
    if total_orders > 0 and total_fills == 0:
        risk_items.append("allocation-to-fill collapse")
    if risk_items:
        for item in risk_items[:4]:
            lines.append(f"- {item}")
    else:
        lines.append("- no dominant risk proved by current v1 surfaces")

    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    report = build_report(args)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report)
    sys.stdout.write(report)


if __name__ == "__main__":
    main()
