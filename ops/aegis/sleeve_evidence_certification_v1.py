from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.paper_pnl_report_v1 import paper_pnl_report_path_v1
from ops.aegis.paper_position_ledger_v1 import paper_position_ledger_path_v1
from ops.aegis.sleeve_performance_truth_v1 import (
    build_sleeve_performance_truth_v1,
    sleeve_performance_truth_path_v1,
)


REPORT_FAMILY = "aegis_sleeve_evidence_certification_v1"
JSON_FILENAME = "sleeve_evidence_certification.v1.json"
SUMMARY_FILENAME = "sleeve_evidence_certification_summary.md"

MINIMUM_SAMPLE_THRESHOLD = 5
SUFFICIENT_SAMPLE_THRESHOLD = 20
DEFAULT_BENCHMARK_SYMBOL = "SPY"

SAFETY = {
    "review_only": True,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_policy_changed": False,
    "runtime_policy_changed": False,
    "no_trade_recommendations": True,
    "no_investable_edge_claims": True,
}

VALID_FACTORY_CLASSIFICATIONS = {
    "ALPHA_DISCOVERY",
    "TECHNICAL_STRATEGY",
    "INVESTMENT_PROCESS",
    "ALLOCATION",
    "UNCLASSIFIED",
}


def sleeve_evidence_certification_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / JSON_FILENAME


def sleeve_evidence_certification_summary_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / SUMMARY_FILENAME


def build_sleeve_evidence_certification_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    source_paths = _source_paths(root, day)
    payloads = {name: _read_json(path) for name, path in source_paths.items()}

    sleeve_truth = payloads["sleeve_performance_truth"] or build_sleeve_performance_truth_v1(truth_root=root, day_utc=day)
    ledger = payloads["paper_position_ledger"]
    paper_pnl = payloads["paper_pnl_report"]
    market_data = payloads["market_data"]
    market_data_inputs = payloads["market_data_inputs"]
    paper_outcomes = payloads["paper_trade_outcomes"]

    missing_required = [name for name in ("sleeve_performance_truth", "paper_position_ledger", "paper_pnl_report") if not source_paths[name].exists()]
    closed_by_sleeve = _closed_positions_by_sleeve(ledger, paper_outcomes)
    benchmark_index = _benchmark_index(market_data, market_data_inputs)

    rows = []
    for sleeve_row in _safe_rows(sleeve_truth, "sleeves"):
        rows.append(
            _certify_sleeve(
                sleeve_row=sleeve_row,
                closed_positions=closed_by_sleeve.get(str(sleeve_row.get("sleeve_id") or ""), []),
                benchmark_index=benchmark_index,
                missing_required=missing_required,
                paper_pnl=paper_pnl,
            )
        )
    rows.sort(key=lambda row: str(row.get("sleeve_id") or ""))

    payload = {
        "schema_id": "aegis_sleeve_evidence_certification",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at_utc": _now(),
        "truth_model": "CANONICAL_DERIVED_READ_MODEL_FROM_EXISTING_AUTHORITIES",
        "purpose": "Convert sleeve mark-to-market observations into conservative evidence classifications without changing sleeve logic or making trade recommendations.",
        "sample_thresholds": {
            "minimum_sample_threshold": MINIMUM_SAMPLE_THRESHOLD,
            "sufficient_sample_threshold": SUFFICIENT_SAMPLE_THRESHOLD,
        },
        "rules": {
            "zero_closed_positions_never_working": True,
            "positive_unrealized_pnl_alone_never_positive_evidence": True,
            "benchmark_excess_return_explicit_or_not_evaluable": True,
            "closed_realized_evidence_required_for_certification": True,
            "read_model_only_no_sleeve_logic_change": True,
        },
        "sleeves": rows,
        "sleeve_count": len(rows),
        "summary": _summary(rows),
        "source_artifact_paths": {name: str(path) for name, path in source_paths.items() if path.exists()},
        "source_hashes": {name: _sha256(path) for name, path in source_paths.items() if path.exists()},
        "missing_required_inputs": missing_required,
        "data_quality_status": "DATA_INCOMPLETE" if missing_required else "PASS",
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def write_sleeve_evidence_certification_v1(
    *, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None
) -> dict[str, Path]:
    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or build_sleeve_evidence_certification_v1(truth_root=root, day_utc=day_utc))
    json_path = sleeve_evidence_certification_path_v1(truth_root=root, day_utc=day_utc)
    summary_path = sleeve_evidence_certification_summary_path_v1(truth_root=root, day_utc=day_utc)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(json_path, json.dumps(body, indent=2, sort_keys=True) + "\n")
    _atomic_write(summary_path, render_sleeve_evidence_certification_summary_v1(body))
    return {"json": json_path, "markdown": summary_path}


def render_sleeve_evidence_certification_summary_v1(payload: Mapping[str, Any]) -> str:
    rows = _safe_rows(payload, "sleeves")
    lines = [
        "# AEGIS Sleeve Evidence Certification V1",
        "",
        f"Day UTC: {payload.get('day_utc', '')}",
        "",
        "Paper research positions are research observations, not investment recommendations. This artifact makes no trade recommendations and no investable-edge claims.",
        "",
        "## Classification Counts",
    ]
    counts = payload.get("summary", {}).get("evidence_status_counts", {}) if isinstance(payload.get("summary"), Mapping) else {}
    for status in sorted(counts):
        lines.append(f"- {status}: {counts[status]}")
    lines.extend(["", "## Sleeves", ""])
    for row in rows:
        lines.append(
            f"- {row.get('sleeve_id')}: sample={row.get('sample_status')}, evidence={row.get('evidence_status')}, "
            f"net_pnl={row.get('net_pnl')}, realized={row.get('realized_pnl')}, unrealized={row.get('unrealized_pnl')}, "
            f"benchmark_excess={row.get('benchmark_excess_return')}"
        )
        reasons = row.get("blocking_reasons") if isinstance(row.get("blocking_reasons"), list) else []
        if reasons:
            lines.append(f"  Blocking reasons: {', '.join(str(reason) for reason in reasons)}")
    lines.append("")
    return "\n".join(lines)


def _certify_sleeve(
    *,
    sleeve_row: Mapping[str, Any],
    closed_positions: list[Mapping[str, Any]],
    benchmark_index: Mapping[str, Mapping[str, Any]],
    missing_required: list[str],
    paper_pnl: Mapping[str, Any],
) -> dict[str, Any]:
    sleeve_id = str(sleeve_row.get("sleeve_id") or "UNKNOWN")
    active = _int(sleeve_row.get("open_paper_position_count"))
    closed = _int(sleeve_row.get("closed_paper_position_count"))
    if closed_positions:
        closed = len(closed_positions)
    realized = _number(sleeve_row.get("realized_pnl")) or Decimal("0")
    unrealized = _number(sleeve_row.get("unrealized_pnl")) or _number(sleeve_row.get("certified_unrealized_pnl")) or Decimal("0")
    net_pnl = realized + unrealized
    exposure = _number(sleeve_row.get("certified_entry_notional")) or _number(sleeve_row.get("open_exposure")) or Decimal("0")
    sample_status = _sample_status(closed)
    pnl_values = [_number(row.get("realized_pnl")) or Decimal("0") for row in closed_positions]
    average_gain = _average([value for value in pnl_values if value > 0])
    average_loss = _average([value for value in pnl_values if value < 0])
    win_count = len([value for value in pnl_values if value > 0])
    loss_count = len([value for value in pnl_values if value < 0])
    outcome_count = win_count + loss_count
    win_rate = Decimal(win_count) / Decimal(outcome_count) if outcome_count else _number(sleeve_row.get("win_rate"))
    expected_value = _expected_value(win_rate, average_gain, average_loss)
    profit_factor = _profit_factor(pnl_values)
    max_drawdown = _max_drawdown(pnl_values)
    average_holding_period = _average_holding_period(closed_positions, sleeve_row)

    factory = _factory_classification(sleeve_row)
    benchmark_symbol = str(sleeve_row.get("benchmark_symbol") or DEFAULT_BENCHMARK_SYMBOL)
    benchmark_return = _number(sleeve_row.get("benchmark_return"))
    benchmark_status = ""
    if benchmark_return is None:
        benchmark = benchmark_index.get(benchmark_symbol, {})
        benchmark_return = _number(benchmark.get("return"))
        benchmark_status = str(benchmark.get("status") or "")
    sleeve_return = _safe_ratio(net_pnl, exposure)
    benchmark_excess = _number(sleeve_row.get("benchmark_excess_return"))
    if benchmark_excess is None and benchmark_return is not None and sleeve_return is not None:
        benchmark_excess = sleeve_return - benchmark_return

    blocking_reasons: list[str] = []
    if missing_required:
        blocking_reasons.extend([f"MISSING_REQUIRED_INPUT:{name}" for name in missing_required])
    if factory == "UNCLASSIFIED":
        blocking_reasons.append("FACTORY_CLASSIFICATION_MISSING")
    if closed == 0:
        blocking_reasons.append("ZERO_CLOSED_POSITIONS")
    elif sample_status == "UNDERPOWERED":
        blocking_reasons.append("CLOSED_SAMPLE_BELOW_MINIMUM_THRESHOLD")
    elif sample_status == "BUILDING_SAMPLE":
        blocking_reasons.append("CLOSED_SAMPLE_BELOW_SUFFICIENT_THRESHOLD")
    if unrealized > 0 and closed < SUFFICIENT_SAMPLE_THRESHOLD:
        blocking_reasons.append("POSITIVE_UNREALIZED_PNL_IS_MARK_TO_MARKET_ONLY")
    if benchmark_return is None:
        blocking_reasons.append("BENCHMARK_RETURN_NOT_EVALUABLE")
    elif benchmark_status == "STALE":
        blocking_reasons.append("BENCHMARK_STALE")
    if benchmark_excess is not None and benchmark_excess < 0:
        blocking_reasons.append("BENCHMARK_UNDERPERFORMANCE")
    if expected_value is None:
        blocking_reasons.append("EXPECTED_VALUE_NOT_EVALUABLE")

    evidence_status = _evidence_status(
        sample_status=sample_status,
        missing_required=missing_required,
        net_pnl=net_pnl,
        benchmark_return=benchmark_return,
        benchmark_excess=benchmark_excess,
        benchmark_status=benchmark_status,
    )
    recommended_action = _recommended_action(evidence_status, sample_status, net_pnl, expected_value)

    return {
        "sleeve_id": sleeve_id,
        "factory_classification": factory,
        "active_position_count": active,
        "closed_position_count": closed,
        "realized_pnl": _decimal_text(realized),
        "unrealized_pnl": _decimal_text(unrealized),
        "net_pnl": _decimal_text(net_pnl),
        "mark_to_market_observation": {
            "unrealized_pnl": _decimal_text(unrealized),
            "unrealized_pnl_status": str(sleeve_row.get("unrealized_pnl_status") or ""),
            "mark_coverage_by_position_pct": sleeve_row.get("mark_coverage_by_position_pct"),
        },
        "realized_evidence": {
            "closed_position_count": closed,
            "realized_pnl": _decimal_text(realized),
            "win_rate": _optional_decimal_text(win_rate),
        },
        "benchmark_relative_evidence": {
            "benchmark_symbol": benchmark_symbol,
            "benchmark_return": _optional_decimal_text(benchmark_return),
            "benchmark_excess_return": _optional_decimal_text(benchmark_excess),
            "status": "EVALUABLE" if benchmark_excess is not None else "NOT_EVALUABLE",
        },
        "average_holding_period": _optional_decimal_text(average_holding_period),
        "win_rate": _optional_decimal_text(win_rate),
        "average_gain": _optional_decimal_text(average_gain),
        "average_loss": _optional_decimal_text(average_loss),
        "expected_value": _optional_decimal_text(expected_value),
        "profit_factor": _optional_decimal_text(profit_factor),
        "max_drawdown": _optional_decimal_text(max_drawdown),
        "benchmark_symbol": benchmark_symbol,
        "benchmark_return": _optional_decimal_text(benchmark_return),
        "benchmark_excess_return": _optional_decimal_text(benchmark_excess),
        "sample_status": sample_status,
        "evidence_status": evidence_status,
        "recommended_action": recommended_action,
        "blocking_reasons": sorted(set(blocking_reasons)) or ["NONE"],
        "source_notes": {
            "paper_pnl_report_day": paper_pnl.get("day_utc"),
            "no_trade_recommendations": True,
            "no_investable_edge_claims": True,
        },
    }


def _evidence_status(
    *,
    sample_status: str,
    missing_required: list[str],
    net_pnl: Decimal,
    benchmark_return: Decimal | None,
    benchmark_excess: Decimal | None,
    benchmark_status: str,
) -> str:
    if missing_required:
        return "DATA_INCOMPLETE"
    if sample_status in {"ZERO_SAMPLE", "UNDERPOWERED", "BUILDING_SAMPLE"}:
        return "UNDERPOWERED"
    if benchmark_status == "STALE":
        return "BENCHMARK_STALE"
    if benchmark_return is None or benchmark_excess is None:
        return "NOT_EVALUABLE"
    if net_pnl > 0 and benchmark_excess > 0:
        return "POSITIVE_EVIDENCE"
    if net_pnl > 0 and benchmark_excess <= 0:
        return "BASELINE_FAIL"
    if net_pnl < 0 and benchmark_excess < 0:
        return "BASELINE_FAIL"
    if net_pnl < 0:
        return "NEGATIVE_EVIDENCE"
    return "NOT_EVALUABLE"


def _recommended_action(evidence_status: str, sample_status: str, net_pnl: Decimal, expected_value: Decimal | None) -> str:
    if evidence_status in {"DATA_INCOMPLETE", "BENCHMARK_STALE", "NOT_EVALUABLE"}:
        return "DO_NOT_USE_FOR_CAPITAL"
    if evidence_status == "BASELINE_FAIL":
        return "HOSTILE_REVIEW_REQUIRED"
    if evidence_status == "NEGATIVE_EVIDENCE":
        if net_pnl < 0 and (expected_value is None or expected_value < 0):
            return "HOSTILE_REVIEW_REQUIRED"
        return "INCREASE_RESEARCH_ATTENTION"
    if sample_status in {"ZERO_SAMPLE", "UNDERPOWERED", "BUILDING_SAMPLE"}:
        return "CONTINUE_OBSERVATION"
    if evidence_status == "POSITIVE_EVIDENCE":
        return "REDUCE_RESEARCH_ATTENTION"
    return "CONTINUE_OBSERVATION"


def _sample_status(closed: int) -> str:
    if closed <= 0:
        return "ZERO_SAMPLE"
    if closed < MINIMUM_SAMPLE_THRESHOLD:
        return "UNDERPOWERED"
    if closed < SUFFICIENT_SAMPLE_THRESHOLD:
        return "BUILDING_SAMPLE"
    return "SUFFICIENT_SAMPLE"


def _source_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "sleeve_performance_truth": sleeve_performance_truth_path_v1(truth_root=root, day_utc=day),
        "paper_position_ledger": paper_position_ledger_path_v1(truth_root=root, day_utc=day),
        "paper_pnl_report": paper_pnl_report_path_v1(truth_root=root, day_utc=day),
        "market_data": root / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json",
        "market_data_inputs": root / "reports" / "market_data_inputs_v1" / day / "market_data_inputs.v1.json",
        "paper_trade_outcomes": root / "reports" / "aegis_paper_trade_outcomes_v1" / day / "paper_trade_outcomes.v1.json",
        "paper_outcome_auto_closure": root / "reports" / "aegis_paper_outcome_auto_closure_v1" / day / "paper_outcome_auto_closure.v1.json",
    }


def _closed_positions_by_sleeve(ledger: Mapping[str, Any], paper_outcomes: Mapping[str, Any]) -> dict[str, list[Mapping[str, Any]]]:
    out: dict[str, list[Mapping[str, Any]]] = {}
    for row in _safe_rows(ledger, "closed_positions"):
        sleeve = str(row.get("sleeve_id") or "UNKNOWN")
        out.setdefault(sleeve, []).append(row)
    if out:
        return out
    for row in _safe_rows(paper_outcomes, "closed_trades", "outcomes", "rows"):
        sleeve = str(row.get("sleeve_id") or "UNKNOWN")
        out.setdefault(sleeve, []).append(row)
    return out


def _benchmark_index(market_data: Mapping[str, Any], market_data_inputs: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in _safe_rows(market_data, "normalized_records", "records", "rows"):
        symbol = str(row.get("canonical_symbol") or row.get("symbol") or "").upper()
        if not symbol:
            continue
        ret = _row_return(row)
        stale = str(row.get("freshness_status") or "").upper() == "STALE"
        out[symbol] = {"return": ret, "status": "STALE" if stale else "CURRENT"}
    for row in _safe_rows(market_data_inputs, "input_records", "records", "rows"):
        symbol = str(row.get("symbol") or row.get("canonical_symbol") or "").upper()
        if symbol and symbol not in out:
            status = "STALE" if str(row.get("validation_status") or "").upper() in {"STALE", "INVALID_STALE"} else "CURRENT"
            out[symbol] = {"return": row.get("return") or row.get("benchmark_return"), "status": status}
    return out


def _row_return(row: Mapping[str, Any]) -> str | None:
    direct = _number(row.get("return") or row.get("daily_return") or row.get("benchmark_return"))
    if direct is not None:
        return _decimal_text(direct)
    open_price = _number(row.get("open"))
    close_price = _number(row.get("close") or row.get("last") or row.get("value"))
    if open_price is None or close_price is None or open_price == 0:
        return None
    return _decimal_text((close_price - open_price) / open_price)


def _factory_classification(row: Mapping[str, Any]) -> str:
    raw = str(row.get("factory_classification") or row.get("factory") or row.get("sleeve_factory") or "").strip().upper()
    return raw if raw in VALID_FACTORY_CLASSIFICATIONS and raw != "UNCLASSIFIED" else "UNCLASSIFIED"


def _average(values: list[Decimal]) -> Decimal | None:
    return sum(values, Decimal("0")) / Decimal(len(values)) if values else None


def _expected_value(win_rate: Decimal | None, average_gain: Decimal | None, average_loss: Decimal | None) -> Decimal | None:
    if win_rate is None or average_gain is None or average_loss is None:
        return None
    return (win_rate * average_gain) + ((Decimal("1") - win_rate) * average_loss)


def _profit_factor(values: list[Decimal]) -> Decimal | None:
    gains = sum((value for value in values if value > 0), Decimal("0"))
    losses = abs(sum((value for value in values if value < 0), Decimal("0")))
    if losses == 0:
        return None
    return gains / losses


def _max_drawdown(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    equity = Decimal("0")
    peak = Decimal("0")
    drawdown = Decimal("0")
    for value in values:
        equity += value
        if equity > peak:
            peak = equity
        current = peak - equity
        if current > drawdown:
            drawdown = current
    return drawdown


def _average_holding_period(closed_positions: list[Mapping[str, Any]], sleeve_row: Mapping[str, Any]) -> Decimal | None:
    values = []
    for row in closed_positions:
        direct = _number(row.get("holding_period_days") or row.get("hold_time_days") or row.get("average_holding_period"))
        if direct is not None:
            values.append(direct)
            continue
        entry = _parse_dt(row.get("entry_time"))
        exit_time = _parse_dt(row.get("exit_time"))
        if entry and exit_time and exit_time >= entry:
            values.append(Decimal(str((exit_time - entry).total_seconds() / 86400.0)))
    return _average(values) or _number(sleeve_row.get("average_hold_time"))


def _safe_ratio(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    evidence_counts: dict[str, int] = {}
    sample_counts: dict[str, int] = {}
    for row in rows:
        evidence_counts[str(row.get("evidence_status") or "UNKNOWN")] = evidence_counts.get(str(row.get("evidence_status") or "UNKNOWN"), 0) + 1
        sample_counts[str(row.get("sample_status") or "UNKNOWN")] = sample_counts.get(str(row.get("sample_status") or "UNKNOWN"), 0) + 1
    return {
        "evidence_status_counts": evidence_counts,
        "sample_status_counts": sample_counts,
        "positive_evidence_count": evidence_counts.get("POSITIVE_EVIDENCE", 0),
        "underpowered_count": evidence_counts.get("UNDERPOWERED", 0),
        "baseline_fail_count": evidence_counts.get("BASELINE_FAIL", 0),
        "not_evaluable_count": evidence_counts.get("NOT_EVALUABLE", 0),
    }


def _safe_rows(payload: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        rows = payload.get(key)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _number(value: Any) -> Decimal | None:
    try:
        if value in (None, "") or isinstance(value, bool):
            return None
        if isinstance(value, str) and value.upper() == "NOT_EVALUABLE":
            return None
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _decimal_text(value: Decimal | int | float | str) -> str:
    if not isinstance(value, Decimal):
        value = Decimal(str(value or "0"))
    normalized = value.quantize(Decimal("0.000001"))
    return format(normalized.normalize(), "f")


def _optional_decimal_text(value: Decimal | int | float | str | None) -> str:
    if value is None:
        return "NOT_EVALUABLE"
    return _decimal_text(value)


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def _atomic_write(path: Path, body: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(body, encoding="utf-8")
    os.replace(tmp, path)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
