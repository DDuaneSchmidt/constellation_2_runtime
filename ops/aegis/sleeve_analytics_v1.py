from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.daily_paper_performance_v1 import daily_paper_performance_path_v1
from ops.aegis.paper_pnl_report_v1 import paper_pnl_report_path_v1
from ops.aegis.paper_position_ledger_v1 import paper_position_ledger_path_v1
from ops.aegis.sleeve_performance_truth_v1 import sleeve_performance_truth_path_v1
from ops.aegis.sleeve_evaluation_v1 import sleeve_evaluation_path_v1


REPORT_FAMILY = "aegis_sleeve_analytics_v1"
REPORT_FILENAME = "sleeve_analytics.v1.json"

SAFETY = {
    "read_only": True,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "live_trading_allowed": False,
    "allocation_instructions_allowed": False,
}


def sleeve_analytics_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_sleeve_analytics_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    source_paths = _source_paths(root, day)
    payloads = {name: _read_json(path) for name, path in source_paths.items()}
    silent_sleeve_evaluation = _silent_sleeve_evaluation(payloads.get("sleeve_evaluation_v1", {}))
    paper_pnl = payloads["paper_pnl_report_v1"]
    sleeve_truth = payloads["sleeve_performance_truth_v1"]
    ledger = payloads["paper_position_ledger_v1"]

    open_positions = _rows(paper_pnl, "open_positions") or _rows(ledger, "open_positions")
    closed_positions = _rows(paper_pnl, "closed_positions") or _rows(ledger, "closed_positions")
    positions_by_sleeve = _position_metrics(open_positions, closed_positions)
    sleeve_truth_rows = _rows(sleeve_truth, "sleeves")
    sleeve_ids = sorted({str(row.get("sleeve_id") or "UNKNOWN") for row in sleeve_truth_rows} | set(positions_by_sleeve))

    sleeves = []
    for sleeve_id in sleeve_ids:
        truth_row = next((row for row in sleeve_truth_rows if str(row.get("sleeve_id") or "UNKNOWN") == sleeve_id), {})
        metrics = positions_by_sleeve.get(sleeve_id, {})
        realized = _first_number(truth_row.get("realized_pnl"), metrics.get("realized_pnl"), 0.0) or 0.0
        unrealized = _first_number(truth_row.get("unrealized_pnl"), metrics.get("unrealized_pnl"), 0.0) or 0.0
        total = realized + unrealized
        open_count = int(_first_number(truth_row.get("open_paper_position_count"), metrics.get("open_positions"), 0) or 0)
        closed_count = int(_first_number(truth_row.get("closed_paper_position_count"), metrics.get("closed_positions"), 0) or 0)
        entry_notional = _first_number(metrics.get("entry_notional"), truth_row.get("open_exposure"), 0.0) or 0.0
        mark_coverage = _first_number(truth_row.get("mark_coverage_by_position_pct"), metrics.get("mark_coverage_pct"))
        row_diagnostics = _sleeve_diagnostics(sleeve_id, truth_row, metrics)
        sleeves.append({
            "sleeve_id": sleeve_id,
            "sleeve_name": str(truth_row.get("sleeve_name") or sleeve_id),
            "status": _sleeve_status(truth_row, row_diagnostics),
            "open_positions": open_count,
            "closed_positions": closed_count,
            "market_value": _round_or_none(metrics.get("market_value")),
            "realized_pnl": _round_or_none(realized),
            "unrealized_pnl": _round_or_none(unrealized),
            "total_pnl": _round_or_none(total),
            "return_pct": _round_or_none((total / entry_notional * 100) if entry_notional else None),
            "win_rate": _first_number(truth_row.get("win_rate")),
            "average_winner": None,
            "average_loser": None,
            "profit_factor": None,
            "expectancy": None,
            "average_hold_days": _first_number(truth_row.get("average_hold_time")),
            "mark_coverage_pct": _round_or_none(mark_coverage),
            "attribution_coverage_pct": 0.0 if sleeve_id == "UNKNOWN" else 100.0,
            "data_quality_status": _sleeve_data_quality(truth_row, row_diagnostics),
            "diagnostics_count": len(row_diagnostics),
            "null_reasons": {
                "win_rate": "NO_CLOSED_TRADES" if closed_count == 0 else None,
                "average_winner": "PHASE_2_CLOSED_TRADE_ANALYTICS_NOT_IMPLEMENTED",
                "average_loser": "PHASE_2_CLOSED_TRADE_ANALYTICS_NOT_IMPLEMENTED",
                "profit_factor": "PHASE_2_CLOSED_TRADE_ANALYTICS_NOT_IMPLEMENTED",
                "expectancy": "PHASE_2_CLOSED_TRADE_ANALYTICS_NOT_IMPLEMENTED",
                "return_pct": None if entry_notional else "CAPITAL_BASIS_UNAVAILABLE",
            },
            "formulas": {
                "total_pnl": "realized_pnl + unrealized_pnl",
                "return_pct": "total_pnl / capital_basis",
            },
            "source_artifact": "aegis_sleeve_analytics_v1",
        })

    sleeve_assignment_reconciliation = _sleeve_assignment_reconciliation(open_positions)
    summary = _summary(sleeves, paper_pnl, sleeve_truth)
    attribution_reconciliation = _rows(paper_pnl, "sleeve_attribution_reconciliation")
    data_quality = _data_quality(source_paths, payloads, open_positions, sleeves)
    diagnostics = _diagnostics(source_paths, payloads, sleeves, data_quality)
    status = _status(data_quality, diagnostics)
    evidence_coverage_panel = _evidence_coverage_panel(summary, data_quality, payloads, open_positions, status)
    summary["data_quality_status"] = status if status != "CANONICAL" else str(data_quality.get("data_quality_status") or "PASS")
    if summary.get("data_quality_status") == "PASS" and (float(summary.get("mark_coverage_pct") or 0) < 100 or float(summary.get("sleeve_attribution_coverage_pct") or 0) < 100):
        summary["data_quality_status"] = "PARTIAL"
    payload = {
        "schema_id": "aegis_sleeve_analytics",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "as_of": _first_text(
            payloads["daily_paper_performance_v1"].get("generated_at_utc"),
            paper_pnl.get("generated_at_utc"),
            sleeve_truth.get("generated_at_utc"),
        ),
        "generated_at": _now(),
        "status": status,
        "data_quality": data_quality,
        "summary": summary,
        "evidence_coverage_panel": evidence_coverage_panel,
        "sleeves": sleeves,
        "sleeve_assignment_reconciliation": sleeve_assignment_reconciliation,
        "silent_sleeve_evaluation": silent_sleeve_evaluation,
        "diagnostics": diagnostics,
        "sleeve_attribution_reconciliation": attribution_reconciliation,
        "input_sources": [_source_summary(name, path, payloads.get(name, {}), day) for name, path in source_paths.items()],
        "source_artifact_paths": {name: str(path) for name, path in source_paths.items() if path.exists()},
        "source_hashes": {name: _sha256(path) for name, path in source_paths.items() if path.exists()},
        "auditability": {
            "metrics_include_source_artifact": True,
            "metrics_include_formula": True,
            "metrics_include_null_reason_when_unavailable": True,
            "excluded_row_count": int(data_quality.get("legacy_or_unattributed_excluded_count") or 0),
            "sleeve_attribution_reconciliation_count": len(attribution_reconciliation),
        },
        "phase": "PHASE_1",
        "composite_sleeve_score_included": False,
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_sleeve_analytics_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or build_sleeve_analytics_v1(truth_root=root, day_utc=day_utc))
    path = sleeve_analytics_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return path


def _source_paths(root: Path, day: str) -> dict[str, Path]:
    return {
        "paper_position_ledger_v1": paper_position_ledger_path_v1(truth_root=root, day_utc=day),
        "paper_entry_receipts_v1": root / "reports" / "aegis_paper_entry_receipts_v1" / day / "paper_entry_receipts.v1.json",
        "paper_exit_receipts_v1": root / "reports" / "aegis_paper_exit_receipts_v1" / day / "paper_exit_receipts.v1.json",
        "paper_pnl_report_v1": paper_pnl_report_path_v1(truth_root=root, day_utc=day),
        "daily_paper_performance_v1": daily_paper_performance_path_v1(truth_root=root, day_utc=day),
        "sleeve_performance_truth_v1": sleeve_performance_truth_path_v1(truth_root=root, day_utc=day),
        "sleeve_evaluation_v1": sleeve_evaluation_path_v1(truth_root=root, day_utc=day),
        "market_data_v1": root / "reports" / "aegis_market_data_v1" / day / "market_data.v1.json",
        "evidence_lineage_integrity_v1": root / "reports" / "aegis_evidence_lineage_integrity_v1" / day / "evidence_lineage_integrity.v1.json",
        "mark_coverage_report_v1": root / "reports" / "aegis_mark_coverage_report_v1" / day / "mark_coverage_report.v1.json",
        "validation_integrity_report_v1": root / "reports" / "aegis_validation_integrity_report_v1" / day / "validation_integrity_report.v1.json",
    }


def _sleeve_assignment_reconciliation(open_positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in open_positions:
        sleeve = str(row.get("sleeve_id") or "UNKNOWN") or "UNKNOWN"
        rows.append({
            "position_id": str(row.get("position_id") or ""),
            "symbol": str(row.get("symbol") or ""),
            "sleeve_assignment_source": str(row.get("sleeve_assignment_source") or "position.sleeve_id" if sleeve != "UNKNOWN" else "unresolved"),
            "recovered_sleeve": row.get("recovered_sleeve") if row.get("recovered_sleeve") else (sleeve if sleeve != "UNKNOWN" else None),
            "unknown_reason": row.get("unknown_reason") if sleeve == "UNKNOWN" else None,
        })
    return rows


def _silent_sleeve_evaluation(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload:
        return {
            "available": False,
            "status": "UNAVAILABLE",
            "summary": {"silent_sleeves": 0, "correctly_silent": 0, "blocked_sleeves": 0, "needs_investigation": 0},
            "sleeves": [],
            "diagnostic": "aegis_sleeve_evaluation_v1 artifact is missing. Run npm run aegis:sleeve-evaluation.",
        }
    rows = [dict(row) for row in payload.get("sleeves") or [] if isinstance(row, Mapping)]
    silent_rows = [row for row in rows if not row.get("produced_candidates") and not row.get("produced_output_intents")]
    correctly_silent = sum(1 for row in silent_rows if str(row.get("classification") or "") in {"SILENT_CORRECT_NO_SIGNAL", "SILENT_MARKET_CONDITION_NOT_MET", "SILENT_CONFIG_DISABLED"})
    data_blocked = sum(1 for row in silent_rows if str(row.get("classification") or "") == "SILENT_DATA_BLOCKED")
    runtime_failures = sum(1 for row in silent_rows if str(row.get("classification") or "") == "SILENT_RUNTIME_FAILURE")
    blocked = sum(1 for row in silent_rows if str(row.get("classification") or "") in {"SILENT_DATA_BLOCKED", "SILENT_RUNTIME_FAILURE", "SILENT_IMPLEMENTATION_GAP"})
    needs = sum(1 for row in silent_rows if str(row.get("classification") or "") in {"SILENT_THRESHOLD_TOO_STRICT", "SILENT_UNKNOWN_REQUIRES_INVESTIGATION"})
    summary = dict(payload.get("summary") or {})
    summary.update({
        "correctly_silent": correctly_silent,
        "blocked_sleeves": blocked,
        "data_blocked_sleeves": data_blocked,
        "runtime_failures": runtime_failures,
        "needs_investigation": needs,
    })
    return {
        "available": True,
        "status": payload.get("status") or "READY",
        "summary": summary,
        "sleeves": silent_rows,
        "artifact_id": payload.get("artifact_id"),
        "day_utc": payload.get("day_utc"),
        "generated_at": payload.get("generated_at"),
    }

def _position_metrics(open_positions: list[dict[str, Any]], closed_positions: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for row in open_positions:
        sleeve = str(row.get("sleeve_id") or "UNKNOWN")
        bucket = out.setdefault(sleeve, {"open_positions": 0, "closed_positions": 0, "market_value": 0.0, "entry_notional": 0.0, "unrealized_pnl": 0.0, "realized_pnl": 0.0, "marked_positions": 0})
        qty = abs(_first_number(row.get("quantity"), 0.0) or 0.0)
        current = _first_number(row.get("current_certified_mark"), row.get("mark_price"), row.get("current_price"))
        entry = _first_number(row.get("entry_price"))
        unrealized = _first_number(row.get("unrealized_pnl"), 0.0) or 0.0
        bucket["open_positions"] += 1
        if current is not None:
            bucket["market_value"] += qty * current
            bucket["marked_positions"] += 1
        if entry is not None:
            bucket["entry_notional"] += qty * entry
        bucket["unrealized_pnl"] += unrealized
    for row in closed_positions:
        sleeve = str(row.get("sleeve_id") or "UNKNOWN")
        bucket = out.setdefault(sleeve, {"open_positions": 0, "closed_positions": 0, "market_value": 0.0, "entry_notional": 0.0, "unrealized_pnl": 0.0, "realized_pnl": 0.0, "marked_positions": 0})
        qty = abs(_first_number(row.get("quantity"), 0.0) or 0.0)
        entry = _first_number(row.get("entry_price"))
        bucket["closed_positions"] += 1
        bucket["realized_pnl"] += _first_number(row.get("realized_pnl"), 0.0) or 0.0
        if entry is not None:
            bucket["entry_notional"] += qty * entry
    for row in out.values():
        open_count = row.get("open_positions") or 0
        row["mark_coverage_pct"] = (row.get("marked_positions", 0) / open_count * 100) if open_count else 100.0
    return out


def _summary(sleeves: list[dict[str, Any]], paper_pnl: Mapping[str, Any], sleeve_truth: Mapping[str, Any]) -> dict[str, Any]:
    active = [row for row in sleeves if int(row.get("open_positions") or 0) or int(row.get("closed_positions") or 0)]
    realized = sum(_first_number(row.get("realized_pnl"), 0.0) or 0.0 for row in sleeves)
    unrealized = sum(_first_number(row.get("unrealized_pnl"), 0.0) or 0.0 for row in sleeves)
    best = max(sleeves, key=lambda row: _first_number(row.get("total_pnl"), float("-inf")) or float("-inf"), default=None)
    worst = min(sleeves, key=lambda row: _first_number(row.get("total_pnl"), float("inf")) or float("inf"), default=None)
    coverage = paper_pnl.get("mark_coverage") if isinstance(paper_pnl.get("mark_coverage"), Mapping) else {}
    total_positions = sum(int(row.get("open_positions") or 0) for row in sleeves)
    unknown_positions = sum(int(row.get("open_positions") or 0) + int(row.get("closed_positions") or 0) for row in sleeves if row.get("sleeve_id") == "UNKNOWN")
    attribution_denominator = sum(int(row.get("open_positions") or 0) + int(row.get("closed_positions") or 0) for row in sleeves)
    return {
        "total_sleeves": len(sleeves),
        "active_sleeves": len(active),
        "sleeves_with_open_positions": sum(1 for row in sleeves if int(row.get("open_positions") or 0) > 0),
        "sleeves_with_closed_positions": sum(1 for row in sleeves if int(row.get("closed_positions") or 0) > 0),
        "total_open_positions": sum(int(row.get("open_positions") or 0) for row in sleeves),
        "total_closed_positions": sum(int(row.get("closed_positions") or 0) for row in sleeves),
        "total_realized_pnl": _round_or_none(realized),
        "total_unrealized_pnl": _round_or_none(unrealized),
        "total_pnl": _round_or_none(realized + unrealized),
        "mark_coverage_pct": _round_or_none(_first_number(coverage.get("mark_coverage_by_position_pct"), (sum(_first_number(row.get("mark_coverage_pct"), 0.0) or 0.0 for row in sleeves) / len(sleeves) if sleeves else 100.0))),
        "sleeve_attribution_coverage_pct": _round_or_none(((attribution_denominator - unknown_positions) / attribution_denominator * 100) if attribution_denominator else 100.0),
        "data_quality_status": str(sleeve_truth.get("data_quality_status") or "PASS"),
        "best_sleeve": _summary_sleeve(best),
        "worst_sleeve": _summary_sleeve(worst),
        "position_count_basis": total_positions,
    }


def _summary_sleeve(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {"sleeve_id": row.get("sleeve_id"), "sleeve_name": row.get("sleeve_name"), "total_pnl": row.get("total_pnl")}


def _data_quality(source_paths: Mapping[str, Path], payloads: Mapping[str, Mapping[str, Any]], open_positions: list[dict[str, Any]], sleeves: list[dict[str, Any]]) -> dict[str, Any]:
    missing_sources = [name for name, path in source_paths.items() if name in {"paper_position_ledger_v1", "paper_pnl_report_v1", "sleeve_performance_truth_v1"} and not path.exists()]
    stale_inputs = [name for name, payload in payloads.items() if payload and str(payload.get("day_utc") or "") and str(payload.get("day_utc")) not in {"", str(source_paths[name].parent.name)}]
    missing_marks = sum(1 for row in open_positions if _first_number(row.get("current_certified_mark"), row.get("mark_price"), row.get("current_price")) is None)
    missing_sleeve = sum(1 for row in open_positions if str(row.get("sleeve_id") or "UNKNOWN") == "UNKNOWN")
    unknown_sleeve_positions = sum(
        int(row.get("open_positions") or 0) + int(row.get("closed_positions") or 0)
        for row in sleeves
        if str(row.get("sleeve_id") or "UNKNOWN") == "UNKNOWN"
    )
    missing_sleeve = max(missing_sleeve, unknown_sleeve_positions)
    return {
        "missing_mark_count": missing_marks,
        "missing_sleeve_assignment_count": missing_sleeve,
        "missing_entry_receipt_count": 0,
        "missing_exit_receipt_count": 0,
        "missing_realized_pnl_count": sum(1 for row in sleeves if int(row.get("closed_positions") or 0) and row.get("realized_pnl") is None),
        "stale_mark_count": 0,
        "stale_input_count": len(stale_inputs),
        "stale_inputs": stale_inputs,
        "partial_history_count": 0,
        "missing_required_sources": missing_sources,
        "legacy_or_unattributed_excluded_count": 0,
        "data_quality_status": "NOT_CANONICAL" if missing_sources else "PARTIAL" if missing_marks or missing_sleeve or stale_inputs else "PASS",
    }


def _evidence_coverage_panel(summary: Mapping[str, Any], data_quality: Mapping[str, Any], payloads: Mapping[str, Mapping[str, Any]], open_positions: list[dict[str, Any]], status: str) -> dict[str, Any]:
    integrity = payloads.get("evidence_lineage_integrity_v1") or {}
    if isinstance(integrity.get("evidence_coverage_panel"), Mapping):
        return dict(integrity.get("evidence_coverage_panel") or {})
    validation_report = payloads.get("validation_integrity_report_v1") or {}
    mark_report = payloads.get("mark_coverage_report_v1") or {}
    total_positions = len(open_positions)
    missing_marks = int(data_quality.get("missing_mark_count") or 0)
    unknown_positions = int(data_quality.get("missing_sleeve_assignment_count") or 0)
    marked_positions = total_positions - missing_marks
    sample_binding_errors = int(validation_report.get("mismatched_samples") or 0)
    validation_coverage = _first_number(validation_report.get("coverage_pct"), 100.0)
    mark_coverage = _first_number(mark_report.get("coverage_pct"), summary.get("mark_coverage_pct"), _pct_number(marked_positions, total_positions))
    sleeve_coverage = _first_number(summary.get("sleeve_attribution_coverage_pct"), _pct_number(total_positions - unknown_positions, total_positions))
    broken_chain_count = int(missing_marks + unknown_positions + sample_binding_errors)
    current_status = "GREEN" if broken_chain_count == 0 and (mark_coverage or 0) >= 100.0 and (validation_coverage or 0) >= 100.0 and status in {"CANONICAL", "PASS"} else "RED" if missing_marks or unknown_positions or sample_binding_errors else "YELLOW"
    return {
        "candidate_coverage_pct": _first_number((integrity.get("summary") or {}).get("candidate_coverage_pct") if isinstance(integrity.get("summary"), Mapping) else None, 100.0),
        "sleeve_attribution_pct": _round_or_none(sleeve_coverage),
        "mark_coverage_pct": _round_or_none(mark_coverage),
        "validation_coverage_pct": _round_or_none(validation_coverage),
        "broken_chain_count": broken_chain_count,
        "unknown_position_count": unknown_positions,
        "sample_binding_errors": sample_binding_errors,
        "current_integrity_status": current_status,
    }


def _pct_number(linked: int, total: int) -> float:
    if total == 0:
        return 100.0
    return round((linked / total) * 100.0, 6)

def _diagnostics(source_paths: Mapping[str, Path], payloads: Mapping[str, Mapping[str, Any]], sleeves: list[dict[str, Any]], data_quality: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name in data_quality.get("missing_required_sources") or []:
        rows.append({"severity": "error", "label": "Missing required input", "detail": f"{name} is missing at {source_paths[name]}", "collapsed_by_default": True})
    if data_quality.get("missing_mark_count"):
        rows.append({"severity": "warning", "label": "Missing marks", "detail": f"{data_quality.get('missing_mark_count')} open positions lack current marks.", "collapsed_by_default": True})
    if data_quality.get("missing_sleeve_assignment_count"):
        unknown = [row for row in sleeves if row.get("sleeve_id") == "UNKNOWN"]
        rows.append({"severity": "warning", "label": "Missing sleeve attribution", "detail": f"{data_quality.get('missing_sleeve_assignment_count')} positions remain in UNKNOWN after recovery.", "affected_sleeves": [row.get("sleeve_id") for row in unknown], "collapsed_by_default": True})
    for name, payload in payloads.items():
        if payload and str(payload.get("data_quality_status") or "").upper() not in {"", "PASS", "OK"}:
            rows.append({"severity": "info", "label": "Input data quality", "detail": f"{name}: {payload.get('data_quality_status')}", "collapsed_by_default": True})
    return rows


def _sleeve_diagnostics(sleeve_id: str, truth_row: Mapping[str, Any], metrics: Mapping[str, Any]) -> list[str]:
    rows = []
    if sleeve_id == "UNKNOWN":
        rows.append("SLEEVE_ATTRIBUTION_UNRECOVERED")
    if int(truth_row.get("missing_mark_position_count") or 0):
        rows.append("MISSING_MARKS")
    if not truth_row and not metrics:
        rows.append("MISSING_SLEEVE_TRUTH_ROW")
    return rows


def _sleeve_status(truth_row: Mapping[str, Any], diagnostics: list[str]) -> str:
    if any(item in diagnostics for item in {"SLEEVE_ATTRIBUTION_UNRECOVERED", "MISSING_MARKS"}):
        return "PARTIAL"
    status = str(truth_row.get("data_quality_status") or "PASS").upper()
    if status in {"PASS", "OK", "READY"}:
        return "CANONICAL"
    if status in {"BLOCKED", "NOT_CANONICAL"}:
        return "NOT_CANONICAL"
    return "PARTIAL" if diagnostics else "CANONICAL"


def _sleeve_data_quality(truth_row: Mapping[str, Any], diagnostics: list[str]) -> str:
    if diagnostics:
        return "PARTIAL"
    return str(truth_row.get("data_quality_status") or "PASS")


def _status(data_quality: Mapping[str, Any], diagnostics: list[Mapping[str, Any]]) -> str:
    if any(str(row.get("severity")) == "error" for row in diagnostics):
        return "NOT_CANONICAL"
    if str(data_quality.get("data_quality_status") or "").upper() in {"PARTIAL", "NOT_CANONICAL"}:
        return str(data_quality.get("data_quality_status")).upper()
    return "CANONICAL"


def _source_summary(name: str, path: Path, payload: Mapping[str, Any], day: str) -> dict[str, Any]:
    return {
        "name": name,
        "path": str(path),
        "generated_at": payload.get("generated_at") or payload.get("generated_at_utc"),
        "freshness_status": "MISSING" if not path.exists() else "CURRENT" if path.parent.name == day else "STALE",
        "input_row_count": _input_row_count(payload),
        "content_hash": _sha256(path) if path.exists() else None,
    }


def _input_row_count(payload: Mapping[str, Any]) -> int:
    for key in ("sleeves", "open_positions", "rows", "records", "events"):
        value = payload.get(key)
        if isinstance(value, list):
            return len(value)
    return 1 if payload else 0


def _rows(payload: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        rows = payload.get(key)
        if isinstance(rows, list):
            return [dict(row) for row in rows if isinstance(row, Mapping)]
    return []


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _first_number(*values: Any) -> float | None:
    for value in values:
        if value in (None, "", "NOT_CANONICAL", "n/a") or isinstance(value, bool):
            continue
        try:
            return float(str(value).replace(",", ""))
        except Exception:
            continue
    return None


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _round_or_none(value: Any) -> float | None:
    number = _first_number(value)
    return round(number, 6) if number is not None else None


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
