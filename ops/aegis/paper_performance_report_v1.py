from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.advisor_benchmark_v1 import (
    advisor_benchmark_is_stale_v1,
    latest_advisor_benchmark_snapshot_v1,
)
from ops.aegis.candidate_lifecycle_projection_v1 import candidate_lifecycle_projection_path_v1
from ops.aegis.daily_paper_performance_v1 import daily_paper_performance_path_v1
from ops.aegis.paper_pnl_report_v1 import paper_pnl_report_path_v1
from ops.aegis.paper_position_ledger_v1 import paper_position_ledger_path_v1
from ops.aegis.operator_portfolio_valuation_estimate_v1 import (
    build_operator_portfolio_valuation_estimate_v1,
    operator_portfolio_valuation_estimate_path_v1,
    write_operator_portfolio_valuation_estimate_v1,
)
from ops.aegis.sleeve_performance_truth_v1 import sleeve_performance_truth_path_v1


def build_paper_performance_report_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    source_paths = {
        "paper_pnl_report_v1": paper_pnl_report_path_v1(truth_root=root, day_utc=day),
        "daily_paper_performance_v1": daily_paper_performance_path_v1(truth_root=root, day_utc=day),
        "sleeve_performance_truth_v1": sleeve_performance_truth_path_v1(truth_root=root, day_utc=day),
        "candidate_lifecycle_projection_v1": candidate_lifecycle_projection_path_v1(truth_root=root, day_utc=day),
        "paper_position_ledger_v1": paper_position_ledger_path_v1(truth_root=root, day_utc=day),
        "runtime_truth_kernel_v1": root / "reports" / "aegis_runtime_truth_kernel_v1" / day / "runtime_truth_kernel.v1.json",
        "verified_runtime_graph_v1": root / "reports" / "aegis_verified_runtime_graph_v1" / day / "verified_runtime_graph.v1.json",
        "operator_portfolio_valuation_estimate_v1": operator_portfolio_valuation_estimate_path_v1(truth_root=root, day_utc=day),
    }
    estimate_path = source_paths["operator_portfolio_valuation_estimate_v1"]
    estimate_payload = _read_json(estimate_path)
    if not isinstance(estimate_payload, dict) or not estimate_payload:
        estimate_payload = build_operator_portfolio_valuation_estimate_v1(truth_root=root, day_utc=day)
        estimate_path = write_operator_portfolio_valuation_estimate_v1(truth_root=root, day_utc=day, payload=estimate_payload)
        source_paths["operator_portfolio_valuation_estimate_v1"] = estimate_path
    payloads = {name: _read_json(path) for name, path in source_paths.items()}
    payloads["operator_portfolio_valuation_estimate_v1"] = estimate_payload
    paper_pnl = payloads["paper_pnl_report_v1"]
    daily = payloads["daily_paper_performance_v1"]
    sleeve_truth = payloads["sleeve_performance_truth_v1"]
    lifecycle = payloads["candidate_lifecycle_projection_v1"]
    ledger = payloads["paper_position_ledger_v1"]
    runtime_truth = payloads["runtime_truth_kernel_v1"]
    graph = payloads["verified_runtime_graph_v1"]
    valuation_estimate = payloads["operator_portfolio_valuation_estimate_v1"]

    performance_diagnostics = _artifact_diagnostics(source_paths, payloads)
    performance_diagnostics.extend(_data_quality_diagnostics(paper_pnl, daily, sleeve_truth))
    performance_diagnostics.extend(_position_diagnostics(paper_pnl, ledger))
    engineering_diagnostics = _runtime_diagnostics(runtime_truth, graph)
    configuration_diagnostics: list[dict[str, str]] = []

    spy_benchmark = _latest_spy_benchmark(root, day)
    advisor_benchmark = _latest_advisor_benchmark(root, day)
    if advisor_benchmark.get("stale"):
        performance_diagnostics.append(_diag("warning", "Advisor benchmark stale", f"Latest advisor benchmark as_of_date={advisor_benchmark.get('as_of_date') or 'UNKNOWN'} is older than the expected quarter-end benchmark.", classification="PERFORMANCE_WARNING", diagnostic_type="stale_benchmark"))
    if not advisor_benchmark["available"]:
        configuration_diagnostics.append(_diag("info", "Advisor benchmark not configured", "No existing advisor_benchmark_v1 artifact is available.", classification="CONFIGURATION", diagnostic_type="advisor_benchmark_not_configured"))

    diagnostics = performance_diagnostics
    status = _overall_status(payloads, diagnostics)
    aegis_paper_benchmark = _aegis_paper_benchmark(
        paper_pnl=paper_pnl,
        daily=daily,
        sleeve_truth=sleeve_truth,
        source=str(source_paths["sleeve_performance_truth_v1"] if sleeve_truth else source_paths["paper_pnl_report_v1"]),
    )
    return {
        "schema_id": "aegis_paper_performance_report",
        "schema_version": "v1",
        "status": status,
        "day_utc": day,
        "rebuilt_on_request": False,
        "source_read_models": [
            "paper_pnl_report_v1",
            "daily_paper_performance_v1",
            "sleeve_performance_truth_v1",
            "candidate_lifecycle_projection_v1",
            "paper_position_ledger_v1",
        ],
        "as_of": _first_text(
            daily.get("generated_at_utc"),
            paper_pnl.get("generated_at_utc"),
            sleeve_truth.get("generated_at_utc"),
            lifecycle.get("generated_at_utc"),
            lifecycle.get("generated_at"),
        ),
        "generated_at_utc": _now(),
        "overview": _overview(paper_pnl=paper_pnl, daily=daily, sleeve_truth=sleeve_truth),
        "operator_portfolio_valuation_estimate_v1": valuation_estimate,
        "operator_portfolio_valuation_estimate": valuation_estimate,
        "sleeves": _sleeves(sleeve_truth=sleeve_truth, daily=daily, paper_pnl=paper_pnl),
        "position_attribution": _position_attribution(paper_pnl=paper_pnl),
        "candidate_outcomes": _candidate_outcomes(lifecycle),
        "benchmarks": {
            "aegis_paper": aegis_paper_benchmark,
            "spy": spy_benchmark,
            "advisor": advisor_benchmark,
        },
        "diagnostic_summary": _diagnostic_summary(paper_pnl=paper_pnl),
        "diagnostics": diagnostics,
        "performance_diagnostics": performance_diagnostics,
        "configuration_diagnostics": configuration_diagnostics,
        "runtime_health": {
            "diagnostics": engineering_diagnostics,
            "diagnostic_count": len(engineering_diagnostics),
            "source": "runtime_truth_kernel_v1+verified_runtime_graph_v1",
        },
        "engineering_diagnostics": engineering_diagnostics,
        "source_artifact_paths": {name: str(path) for name, path in source_paths.items()},
        "safety": {
            "read_only": True,
            "no_accounting_engine_created": True,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
            "live_trading_allowed": False,
        },
    }


def _overview(*, paper_pnl: Mapping[str, Any], daily: Mapping[str, Any], sleeve_truth: Mapping[str, Any]) -> dict[str, Any]:
    totals = sleeve_truth.get("totals") if isinstance(sleeve_truth.get("totals"), Mapping) else {}
    best = _first_row(daily.get("best_paper_positions"))
    worst = _first_row(daily.get("worst_paper_positions"))
    return {
        "total_pnl": _num(paper_pnl.get("total_paper_pnl")) if paper_pnl.get("total_paper_pnl") is not None else _num(_first_text(daily.get("total_paper_pnl"), totals.get("total_paper_pnl"))),
        "realized_pnl": _num(_first_text(paper_pnl.get("realized_pnl"), daily.get("realized_pnl"), totals.get("realized_pnl"))),
        "unrealized_pnl": _num(paper_pnl.get("unrealized_pnl")) if paper_pnl.get("unrealized_pnl") is not None else _num(_first_text(daily.get("unrealized_pnl"), totals.get("unrealized_pnl"))),
        "full_portfolio_pnl_status": str(paper_pnl.get("full_portfolio_pnl_status") or daily.get("full_portfolio_pnl_status") or "NOT_CANONICAL"),
        "certified_unrealized_pnl": _num(_first_text(daily.get("certified_unrealized_pnl"), paper_pnl.get("certified_unrealized_pnl"), totals.get("certified_unrealized_pnl"))),
        "certified_total_paper_pnl": _num(_first_text(daily.get("certified_total_paper_pnl"), paper_pnl.get("certified_total_paper_pnl"), totals.get("certified_total_paper_pnl"))),
        "mark_coverage": _mark_coverage(paper_pnl=paper_pnl, daily=daily, totals=totals),
        "missing_mark_count": _num(_first_any(paper_pnl.get("missing_mark_count"), (paper_pnl.get("mark_coverage") or {}).get("missing_mark_position_count") if isinstance(paper_pnl.get("mark_coverage"), Mapping) else None, totals.get("missing_mark_position_count"))),
        "missing_mark_symbols": list(paper_pnl.get("missing_mark_symbols") or ((paper_pnl.get("mark_coverage") or {}).get("missing_symbols") if isinstance(paper_pnl.get("mark_coverage"), Mapping) else []) or []),
        "data_quality_explanation": _first_text(daily.get("data_quality_explanation"), paper_pnl.get("data_quality_explanation")),
        "open_positions": _num(_first_text(daily.get("total_open_positions"), paper_pnl.get("open_position_count"), totals.get("open_paper_position_count"))),
        "closed_positions": _num(_first_any(daily.get("total_closed_positions"), paper_pnl.get("closed_position_count"), totals.get("closed_paper_position_count"))),
        "win_rate": _num(totals.get("win_rate")),
        "best_position": best,
        "worst_position": worst,
        "data_quality": _display_data_quality(paper_pnl=paper_pnl, sleeve_truth=sleeve_truth),
    }


def _display_data_quality(*, paper_pnl: Mapping[str, Any], sleeve_truth: Mapping[str, Any]) -> str:
    coverage = paper_pnl.get("mark_coverage") if isinstance(paper_pnl.get("mark_coverage"), Mapping) else {}
    if int(coverage.get("missing_mark_position_count") or len(paper_pnl.get("missing_mark_position_ids") or [])) > 0:
        return "PARTIAL_MARK_COVERAGE"
    unknown_count = sum(1 for row in _rows(paper_pnl, "open_positions") if str(row.get("sleeve_id") or "UNKNOWN") == "UNKNOWN")
    if unknown_count:
        return "DEGRADED_SLEEVE_ATTRIBUTION"
    status = _first_text(paper_pnl.get("data_quality_status"), sleeve_truth.get("data_quality_status"))
    return status or "Unavailable"


def _mark_coverage(*, paper_pnl: Mapping[str, Any], daily: Mapping[str, Any], totals: Mapping[str, Any]) -> dict[str, Any]:
    coverage = paper_pnl.get("mark_coverage") if isinstance(paper_pnl.get("mark_coverage"), Mapping) else {}
    if not coverage and isinstance(daily.get("mark_coverage"), Mapping):
        coverage = daily.get("mark_coverage")
    return {
        "open_position_count": _num(_first_any(coverage.get("open_position_count"), paper_pnl.get("open_position_count"), totals.get("open_paper_position_count"))),
        "marked_position_count": _num(_first_any(coverage.get("marked_position_count"), totals.get("marked_open_position_count"))),
        "missing_mark_position_count": _num(_first_any(coverage.get("missing_mark_position_count"), totals.get("missing_mark_position_count"))),
        "mark_coverage_by_position_pct": _num(_first_any(coverage.get("mark_coverage_by_position_pct"), paper_pnl.get("mark_coverage_by_position_pct"), totals.get("mark_coverage_by_position_pct"))),
        "mark_coverage_by_entry_notional_pct": _num(_first_any(coverage.get("mark_coverage_by_entry_notional_pct"), paper_pnl.get("mark_coverage_by_entry_notional_pct"), totals.get("mark_coverage_by_entry_notional_pct"))),
        "missing_symbols": list(coverage.get("missing_symbols") or paper_pnl.get("missing_mark_symbols") or []),
    }


def _sleeves(*, sleeve_truth: Mapping[str, Any], daily: Mapping[str, Any], paper_pnl: Mapping[str, Any]) -> list[dict[str, Any]]:
    daily_by_sleeve = {str(row.get("sleeve_id") or ""): row for row in _rows(daily, "sleeve_comparison")}
    position_metrics = _sleeve_position_metrics(paper_pnl)
    rows = []
    for row in _rows(sleeve_truth, "sleeves"):
        sleeve_id = str(row.get("sleeve_id") or "UNKNOWN")
        daily_row = daily_by_sleeve.get(sleeve_id, {})
        metrics = position_metrics.get(sleeve_id, {})
        rows.append({
            "name": sleeve_id,
            "market_value": _num(metrics.get("market_value")),
            "realized_pnl": _num(row.get("realized_pnl")),
            "unrealized_pnl": _num(row.get("unrealized_pnl")),
            "certified_unrealized_pnl": _num(_first_text(row.get("certified_unrealized_pnl"), daily_row.get("certified_unrealized_pnl"))),
            "total_pnl": _num(_first_text(row.get("total_paper_pnl"), daily_row.get("total_pnl"))),
            "return_pct": _num(metrics.get("return_pct")),
            "open_positions": _num(row.get("open_paper_position_count")),
            "closed_positions": _num(row.get("closed_paper_position_count")),
            "status": _readiness_status(row.get("data_quality_status")),
            "source_status": str(row.get("data_quality_status") or "UNKNOWN"),
            "mark_coverage_by_position_pct": _num(_first_text(row.get("mark_coverage_by_position_pct"), daily_row.get("mark_coverage_by_position_pct"))),
            "mark_coverage_by_entry_notional_pct": _num(_first_text(row.get("mark_coverage_by_entry_notional_pct"), daily_row.get("mark_coverage_by_entry_notional_pct"))),
            "missing_mark_position_count": _num(_first_any(row.get("missing_mark_position_count"), daily_row.get("missing_mark_position_count"))),
            "win_rate": _num(row.get("win_rate")),
            "average_hold_time": _num(row.get("average_hold_time")),
            "source": "sleeve_performance_truth_v1",
        })
    if rows:
        return rows
    for row in _rows(daily, "sleeve_comparison"):
        rows.append({
            "name": str(row.get("sleeve_id") or "UNKNOWN"),
            "market_value": None,
            "realized_pnl": _num(row.get("realized_pnl")),
            "unrealized_pnl": _num(row.get("open_pnl")),
            "certified_unrealized_pnl": _num(row.get("certified_unrealized_pnl")),
            "total_pnl": _num(row.get("total_pnl")),
            "return_pct": None,
            "open_positions": None,
            "closed_positions": None,
            "status": _readiness_status(row.get("data_quality_status")),
            "source_status": str(row.get("data_quality_status") or "UNKNOWN"),
            "mark_coverage_by_position_pct": _num(row.get("mark_coverage_by_position_pct")),
            "mark_coverage_by_entry_notional_pct": _num(row.get("mark_coverage_by_entry_notional_pct")),
            "missing_mark_position_count": _num(row.get("missing_mark_position_count")),
            "win_rate": None,
            "average_hold_time": _num(row.get("average_hold_time")),
            "source": "daily_paper_performance_v1",
        })
    return rows


def _position_attribution(*, paper_pnl: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in _rows(paper_pnl, "open_positions"):
        qty = _num(row.get("quantity")) or 0.0
        entry = _num(row.get("entry_price"))
        current = _num(_first_text(row.get("current_certified_mark"), row.get("mark_price")))
        unrealized = _num(row.get("unrealized_pnl")) if str(row.get("unrealized_pnl_status") or "").upper() == "AVAILABLE" else None
        entry_notional = abs(qty * entry) if entry is not None else None
        total_pnl = unrealized
        return_pct = (total_pnl / entry_notional * 100) if total_pnl is not None and entry_notional else None
        rows.append({
            "symbol": str(row.get("symbol") or ""),
            "status": "open",
            "sleeve": str(row.get("sleeve_id") or "UNKNOWN"),
            "entry_price": _num(row.get("entry_price")),
            "quantity": qty,
            "current_price": current,
            "current_or_exit_price": current,
            "realized_pnl": None,
            "unrealized_pnl": unrealized,
            "certified_unrealized_pnl": unrealized,
            "total_pnl": total_pnl,
            "return_pct": return_pct,
            "mark_coverage_by_position_pct": 100.0 if unrealized is not None and current is not None else 0.0,
            "duration_days": _duration_days(row.get("entry_time"), paper_pnl.get("day_utc")),
            "position_id": str(row.get("position_id") or ""),
            "data_quality_status": str(row.get("unrealized_pnl_status") or row.get("mark_certification_status") or "UNKNOWN"),
        })
    for row in _rows(paper_pnl, "closed_positions"):
        rows.append({
            "symbol": str(row.get("symbol") or ""),
            "status": "closed",
            "sleeve": str(row.get("sleeve_id") or "UNKNOWN"),
            "entry_price": _num(row.get("entry_price")),
            "quantity": _num(row.get("quantity")),
            "current_price": _num(row.get("exit_price")),
            "current_or_exit_price": _num(row.get("exit_price")),
            "realized_pnl": _num(row.get("realized_pnl")),
            "unrealized_pnl": None,
            "certified_unrealized_pnl": None,
            "total_pnl": _num(row.get("realized_pnl")),
            "return_pct": _closed_return_pct(row),
            "mark_coverage_by_position_pct": 100.0,
            "duration_days": _duration_days(row.get("entry_time"), row.get("exit_time")),
            "position_id": str(row.get("position_id") or ""),
            "data_quality_status": "REALIZED",
        })
    return rows


def _sleeve_position_metrics(paper_pnl: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    metrics: dict[str, dict[str, float]] = {}
    for row in _position_attribution(paper_pnl=paper_pnl):
        if row.get("status") != "open":
            continue
        sleeve = str(row.get("sleeve") or "UNKNOWN")
        bucket = metrics.setdefault(sleeve, {"market_value": 0.0, "entry_notional": 0.0, "total_pnl": 0.0})
        qty = _num(row.get("quantity")) or 0.0
        current = _num(row.get("current_or_exit_price")) or 0.0
        entry = _num(row.get("entry_price")) or 0.0
        bucket["market_value"] += abs(qty * current)
        bucket["entry_notional"] += abs(qty * entry)
        bucket["total_pnl"] += _num(row.get("total_pnl")) or 0.0
    out: dict[str, dict[str, Any]] = {}
    for sleeve, row in metrics.items():
        entry = row.get("entry_notional") or 0.0
        out[sleeve] = {
            "market_value": row.get("market_value") or 0.0,
            "return_pct": (row.get("total_pnl", 0.0) / entry * 100) if entry else None,
        }
    return out


def _closed_return_pct(row: Mapping[str, Any]) -> float | None:
    realized = _num(row.get("realized_pnl"))
    qty = _num(row.get("quantity")) or 0.0
    entry = _num(row.get("entry_price"))
    notional = abs(qty * entry) if entry is not None else None
    return (realized / notional * 100) if realized is not None and notional else None


def _duration_days(start: Any, end: Any) -> float | None:
    try:
        start_text = str(start or "").replace("Z", "+00:00")
        if not start_text:
            return None
        start_dt = datetime.fromisoformat(start_text)
        end_text = str(end or "").replace("Z", "+00:00")
        if len(end_text) == 10:
            end_text = end_text + "T23:59:59+00:00"
        end_dt = datetime.fromisoformat(end_text) if end_text else datetime.now(UTC)
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=UTC)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=UTC)
        return round(max((end_dt - start_dt).total_seconds(), 0) / 86400, 4)
    except Exception:
        return None


def _candidate_outcomes(lifecycle: Mapping[str, Any]) -> dict[str, Any]:
    rows = _rows(lifecycle, "current_session_candidates") + _rows(lifecycle, "carry_forward_context") + _rows(lifecycle, "legacy_context")
    states = [str(row.get("candidate_lifecycle_state") or row.get("decision") or "").upper() for row in rows]
    return {
        "available": bool(lifecycle),
        "approved": {"count": _count_states(states, {"APPROVED_FOR_PAPER", "POSITION_OPEN", "POSITION_CLOSED", "ENTRY_RECORDED"})},
        "rejected": {"count": _count_states(states, {"REJECTED", "CANDIDATE_REJECTED"})},
        "deferred": {"count": _count_states(states, {"DEFERRED", "CANDIDATE_DEFERRED"})},
        "approval_performance": None,
        "avoided_loss": None,
        "source": "candidate_lifecycle_projection_v1" if lifecycle else None,
    }


def _latest_spy_benchmark(root: Path, day: str) -> dict[str, Any]:
    base = root / "reports" / "aegis_performance_showcase_v1"
    path = _latest_file(base, "aegis_performance_showcase.v1.json", day)
    payload = _read_json(path) if path else {}
    curve = payload.get("benchmark_curve") if isinstance(payload.get("benchmark_curve"), list) else []
    last = curve[-1] if curve and isinstance(curve[-1], Mapping) else {}
    available = bool(last)
    stale = bool(path and path.parent.name != day)
    return {
        "available": available,
        "return_pct": _num(last.get("cumulative_return_pct")),
        "source": str(path) if path else None,
        "stale": stale,
        "status": "ready" if available and not stale else "unavailable" if available else "not_connected",
    }


def _aegis_paper_benchmark(*, paper_pnl: Mapping[str, Any], daily: Mapping[str, Any], sleeve_truth: Mapping[str, Any], source: str) -> dict[str, Any]:
    totals = sleeve_truth.get("totals") if isinstance(sleeve_truth.get("totals"), Mapping) else {}
    coverage = paper_pnl.get("mark_coverage") if isinstance(paper_pnl.get("mark_coverage"), Mapping) else {}
    capital_basis = _num(_first_text(
        totals.get("open_exposure"),
        totals.get("certified_entry_notional"),
        coverage.get("total_entry_notional"),
        coverage.get("marked_entry_notional"),
        _entry_notional_from_positions(paper_pnl),
    ))
    realized_pnl = _num(_first_text(totals.get("realized_pnl"), daily.get("realized_pnl"), paper_pnl.get("realized_pnl")))
    unrealized_pnl = _num(_first_text(totals.get("unrealized_pnl"), daily.get("unrealized_pnl"), paper_pnl.get("unrealized_pnl")))
    total_pnl = _num(_first_text(totals.get("total_paper_pnl"), daily.get("total_paper_pnl"), paper_pnl.get("total_paper_pnl")))
    if total_pnl is None:
        total_pnl = _num(_first_text(totals.get("certified_total_paper_pnl"), daily.get("certified_total_paper_pnl"), paper_pnl.get("certified_total_paper_pnl")))
    if unrealized_pnl is None:
        unrealized_pnl = _num(_first_text(totals.get("certified_unrealized_pnl"), daily.get("certified_unrealized_pnl"), paper_pnl.get("certified_unrealized_pnl")))
    null_reason = None if capital_basis else "CAPITAL_BASIS_UNAVAILABLE"
    return {
        "available": bool(paper_pnl or daily or sleeve_truth),
        "capital_basis": capital_basis,
        "capital_basis_source": "entry_notional",
        "realized_return_pct": _return_pct(realized_pnl, capital_basis),
        "unrealized_return_pct": _return_pct(unrealized_pnl, capital_basis),
        "total_return_pct": _return_pct(total_pnl, capital_basis),
        "return_pct": _return_pct(total_pnl, capital_basis),
        "formula": {
            "realized_return_pct": "realized_pnl / capital_basis * 100",
            "unrealized_return_pct": "unrealized_pnl / capital_basis * 100",
            "total_return_pct": "total_paper_pnl / capital_basis * 100",
            "capital_basis": "canonical open entry notional from sleeve_performance_truth_v1 totals.open_exposure, falling back to paper_pnl_report_v1 mark_coverage.total_entry_notional",
        },
        "null_reason": null_reason,
        "source": source if (paper_pnl or daily or sleeve_truth) else None,
        "stale": False,
        "status": "ready" if capital_basis else "unavailable",
    }


def _entry_notional_from_positions(paper_pnl: Mapping[str, Any]) -> float | None:
    total = 0.0
    found = False
    for row in _rows(paper_pnl, "open_positions") + _rows(paper_pnl, "closed_positions"):
        explicit = _num(row.get("entry_notional"))
        if explicit is not None:
            total += abs(explicit)
            found = True
            continue
        qty = _num(row.get("quantity"))
        entry = _num(row.get("entry_price"))
        if qty is not None and entry is not None:
            total += abs(qty * entry)
            found = True
    return total if found else None


def _return_pct(pnl: float | None, capital_basis: float | None) -> float | None:
    if pnl is None or not capital_basis:
        return None
    return pnl / capital_basis * 100


def _latest_advisor_benchmark(root: Path, day: str) -> dict[str, Any]:
    snapshot = latest_advisor_benchmark_snapshot_v1(truth_root=root, day_utc=day)
    if not snapshot:
        return {
            "available": False,
            "return_pct": None,
            "period_type": None,
            "as_of_date": None,
            "source": None,
            "notes": None,
            "stale": False,
            "status": "not_connected",
        }
    stale = advisor_benchmark_is_stale_v1(snapshot, day_utc=day)
    return {
        "available": True,
        "return_pct": _num(snapshot.get("return_pct")),
        "period_type": snapshot.get("period_type"),
        "as_of_date": snapshot.get("as_of_date"),
        "source": snapshot.get("source") or snapshot.get("artifact_path"),
        "notes": snapshot.get("notes"),
        "stale": stale,
        "status": "unavailable" if stale else "ready",
        "artifact_path": snapshot.get("artifact_path"),
    }


def _diagnostic_summary(*, paper_pnl: Mapping[str, Any]) -> dict[str, Any]:
    coverage = paper_pnl.get("mark_coverage") if isinstance(paper_pnl.get("mark_coverage"), Mapping) else {}
    rows = _rows(paper_pnl, "open_positions")
    missing_sleeve = sum(1 for row in rows if str(row.get("sleeve_id") or "UNKNOWN") == "UNKNOWN")
    missing_fields = sum(1 for row in rows if not str(row.get("quantity") or "").strip() or not str(row.get("entry_price") or "").strip())
    return {
        "missing_sleeve_assignment_count": missing_sleeve,
        "missing_canonical_field_count": missing_fields,
        "missing_mark_count": int(coverage.get("missing_mark_position_count") or len(paper_pnl.get("missing_mark_position_ids") or [])),
    }


def _artifact_diagnostics(paths: Mapping[str, Path], payloads: Mapping[str, Mapping[str, Any]]) -> list[dict[str, str]]:
    rows = []
    for name, path in paths.items():
        if name in {"runtime_truth_kernel_v1", "verified_runtime_graph_v1"}:
            continue
        if not payloads.get(name):
            rows.append(_diag("error", f"{name} unavailable", f"Missing or unreadable artifact: {path}", classification="PERFORMANCE_CRITICAL", diagnostic_type="missing_performance_read_model"))
    return rows


def _data_quality_diagnostics(*payloads: Mapping[str, Any]) -> list[dict[str, str]]:
    rows = []
    for payload in payloads:
        status = str(payload.get("data_quality_status") or "").strip()
        if status and status not in {"PASS", "OK"}:
            rows.append(_diag("warning", "Performance data quality", status, classification="PERFORMANCE_WARNING", diagnostic_type="partial_attribution" if "ATTRIBUTION" in status.upper() else "performance_data_quality"))
    return rows


def _position_diagnostics(paper_pnl: Mapping[str, Any], ledger: Mapping[str, Any]) -> list[dict[str, str]]:
    rows = []
    coverage = paper_pnl.get("mark_coverage") if isinstance(paper_pnl.get("mark_coverage"), Mapping) else {}
    missing_count = int(coverage.get("missing_mark_position_count") or len(paper_pnl.get("missing_mark_position_ids") or []))
    if missing_count:
        rows.append(_diag("error", "Missing marks", f"Certified partial P&L is available for {coverage.get('marked_position_count', 0)}/{coverage.get('open_position_count', 0)} open positions; full portfolio P&L remains NOT_CANONICAL.", classification="PERFORMANCE_CRITICAL", diagnostic_type="missing_marks"))
    for position_id in paper_pnl.get("missing_mark_position_ids") or []:
        rows.append(_diag("error", "Missing mark", f"Position {position_id} does not have canonical unrealized P&L.", classification="PERFORMANCE_CRITICAL", diagnostic_type="missing_marks"))
    missing_quantity = [row for row in _rows(paper_pnl, "open_positions") if not str(row.get("quantity") or "").strip()]
    missing_sleeve = [row for row in _rows(paper_pnl, "open_positions") if str(row.get("sleeve_id") or "UNKNOWN") == "UNKNOWN"]
    if missing_quantity:
        rows.append(_diag("error", "Missing position attribution", f"{len(missing_quantity)} open positions are missing quantity.", classification="PERFORMANCE_CRITICAL", diagnostic_type="missing_position_attribution"))
    if missing_sleeve:
        rows.append(_diag("error", "Missing sleeve assignments", f"{len(missing_sleeve)} open positions remain assigned to UNKNOWN after lineage recovery.", classification="PERFORMANCE_CRITICAL", diagnostic_type="missing_sleeve_assignments"))
    attributed_rows = _rows(paper_pnl, "open_positions") + _rows(paper_pnl, "closed_positions")
    missing_position_attribution = [row for row in attributed_rows if not str(row.get("position_id") or "").strip() or str(row.get("sleeve_id") or "UNKNOWN") == "UNKNOWN"]
    if missing_position_attribution and not missing_sleeve:
        rows.append(_diag("error", "Missing position attribution", f"{len(missing_position_attribution)} positions lack required attribution identity.", classification="PERFORMANCE_CRITICAL", diagnostic_type="missing_position_attribution"))
    partial_attribution = [row for row in attributed_rows if str(row.get("unknown_reason") or "").strip()]
    if partial_attribution:
        rows.append(_diag("warning", "Partial attribution", f"{len(partial_attribution)} positions have explicit attribution uncertainty.", classification="PERFORMANCE_WARNING", diagnostic_type="partial_attribution"))
    for row in ledger.get("paper_position_ledger_mismatches") or []:
        if isinstance(row, Mapping):
            rows.append(_diag("error", "Missing position attribution", str(row.get("reason") or row.get("status") or "Mismatch reported."), classification="PERFORMANCE_CRITICAL", diagnostic_type="missing_position_attribution"))
    return rows


def _runtime_diagnostics(runtime_truth: Mapping[str, Any], graph: Mapping[str, Any]) -> list[dict[str, str]]:
    rows = []
    classification = str(runtime_truth.get("runtime_truth_classification") or "")
    readiness = str(runtime_truth.get("highest_readiness_layer") or "")
    if classification and classification != "READY":
        rows.append(_diag("warning", "Runtime truth partial", f"runtime_truth_classification={classification}", classification="ENGINEERING", diagnostic_type="runtime_truth_partial"))
    if readiness and readiness != "READY":
        rows.append(_diag("warning", "Readiness layer blocked", f"highest_readiness_layer={readiness}", classification="ENGINEERING", diagnostic_type="readiness_layer_blocked"))
    graph_status = str(graph.get("graph_status") or "")
    if graph_status and graph_status != "READY":
        rows.append(_diag("warning", "Verified runtime graph", f"graph_status={graph_status}", classification="ENGINEERING", diagnostic_type="verified_runtime_graph_not_ready"))
    return rows



def _readiness_status(value: Any) -> str:
    text = str(value or "").upper()
    if text in {"PASS", "OK", "READY", "AVAILABLE"}:
        return "ready"
    if "PARTIAL" in text or "NOT_CANONICAL" in text or "DEGRADED" in text:
        return "partial"
    if text in {"", "UNKNOWN"}:
        return "unavailable"
    if "MISSING" in text or "BLOCKED" in text or "ERROR" in text:
        return "unavailable"
    return "partial"

def _overall_status(payloads: Mapping[str, Mapping[str, Any]], diagnostics: list[Mapping[str, str]]) -> str:
    required = ["paper_pnl_report_v1", "daily_paper_performance_v1", "sleeve_performance_truth_v1", "candidate_lifecycle_projection_v1"]
    present = sum(1 for key in required if payloads.get(key))
    if present == len(required) and not any(row.get("severity") == "error" for row in diagnostics):
        return "ready"
    if present:
        return "partial"
    return "unavailable"


def _read_json(path: Path | None) -> dict[str, Any]:
    if not path:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _latest_file(base: Path, filename: str, day: str) -> Path | None:
    if not base.exists():
        return None
    candidates = [path for path in base.glob(f"**/{filename}") if path.is_file()]
    eligible = [path for path in candidates if _path_day(path) <= day]
    selected = eligible or candidates
    return sorted(selected, key=lambda path: str(path))[-1] if selected else None


def _path_day(path: Path) -> str:
    for part in reversed(path.parts):
        if len(part) == 10 and part[4] == "-" and part[7] == "-":
            return part
    return ""


def _advisor_day(path: Path) -> str:
    return _path_day(path)


def _rows(payload: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        rows = payload.get(key)
        if isinstance(rows, list):
            return [dict(row) for row in rows if isinstance(row, Mapping)]
    return []


def _first_row(value: Any) -> dict[str, Any] | None:
    if isinstance(value, list) and value and isinstance(value[0], Mapping):
        return dict(value[0])
    return None


def _count_states(states: list[str], expected: set[str]) -> int:
    return sum(1 for state in states if state in expected)


def _num(value: Any) -> float | None:
    if value in (None, "", "NOT_CANONICAL", "n/a"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text and text != "NOT_CANONICAL":
            return text
    return ""


def _first_any(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return ""


def _diag(severity: str, label: str, detail: str, *, classification: str, diagnostic_type: str) -> dict[str, str]:
    return {"severity": severity, "classification": classification, "diagnostic_type": diagnostic_type, "label": label, "detail": detail}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
