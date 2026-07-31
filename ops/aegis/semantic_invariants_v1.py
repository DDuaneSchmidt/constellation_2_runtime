from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Callable, Mapping

from ops.aegis.intelligence_common_v1 import now_utc_v1, read_json_v1, write_json_v1

REPORT_FAMILY = "aegis_semantic_invariants_v1"
REPORT_FILENAME = "semantic_invariants.v1.json"
STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_WARNING = "WARNING"

SAFETY = {
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "live_trading_allowed": False,
    "autonomous_live_trading_allowed": False,
}


def semantic_invariants_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_semantic_invariants_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    generated_at = now_utc_v1()
    sources = _source_payloads(root, day)
    results: list[dict[str, Any]] = []
    results.extend(_performance_invariants(day=day, generated_at=generated_at, sources=sources))
    results.extend(_sleeve_analytics_invariants(day=day, generated_at=generated_at, sources=sources))
    fail_count = sum(1 for row in results if row.get("status") == STATUS_FAIL)
    warning_count = sum(1 for row in results if row.get("status") == STATUS_WARNING)
    blocking_failure_count = sum(1 for row in results if row.get("status") == STATUS_FAIL and row.get("blocking") is True)
    by_surface: dict[str, dict[str, Any]] = {}
    for surface_id in sorted({str(row.get("surface_id") or "") for row in results if row.get("surface_id")}):
        rows = [row for row in results if row.get("surface_id") == surface_id]
        failures = [row for row in rows if row.get("status") == STATUS_FAIL]
        blocking = [row for row in failures if row.get("blocking") is True]
        warnings = [row for row in rows if row.get("status") == STATUS_WARNING]
        by_surface[surface_id] = {
            "surface_id": surface_id,
            "status": STATUS_FAIL if blocking else STATUS_WARNING if warnings or failures else STATUS_PASS,
            "invariant_count": len(rows),
            "fail_count": len(failures),
            "warning_count": len(warnings),
            "blocking_failure_count": len(blocking),
            "blocking_invariant_ids": [str(row.get("invariant_id")) for row in blocking],
            "failed_invariants": failures,
        }
    return {
        "schema_id": REPORT_FAMILY,
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at": generated_at,
        "status": STATUS_FAIL if blocking_failure_count else STATUS_WARNING if warning_count or fail_count else STATUS_PASS,
        "summary": {
            "invariant_count": len(results),
            "pass_count": sum(1 for row in results if row.get("status") == STATUS_PASS),
            "fail_count": fail_count,
            "warning_count": warning_count,
            "blocking_failure_count": blocking_failure_count,
            "surfaces_checked": sorted(by_surface),
        },
        "surfaces": by_surface,
        "invariants": results,
        "source_artifacts": {name: str(row["path"]) for name, row in sources.items()},
        "source_artifact_hashes": {name: row["hash"] for name, row in sources.items() if row.get("hash")},
        "safety": dict(SAFETY),
        **SAFETY,
    }


def write_semantic_invariants_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    body = dict(payload or build_semantic_invariants_v1(truth_root=truth_root, day_utc=day_utc))
    return write_json_v1(semantic_invariants_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def run_semantic_invariants_self_check_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    payload = read_json_v1(semantic_invariants_path_v1(truth_root=root, day_utc=day))
    if not payload:
        payload = build_semantic_invariants_v1(truth_root=root, day_utc=day)
        write_semantic_invariants_v1(truth_root=root, day_utc=day, payload=payload)
    failures: list[dict[str, Any]] = []
    rows = payload.get("invariants") if isinstance(payload.get("invariants"), list) else []
    required_fields = {"surface_id", "invariant_id", "status", "blocking", "reason", "field_values", "source_artifacts", "source_artifact_hashes", "generated_at"}
    for idx, row in enumerate(rows):
        if not isinstance(row, Mapping):
            failures.append({"check": "invariant_row_shape", "index": idx, "reason": "row is not an object"})
            continue
        missing = sorted(required_fields - set(row))
        if missing:
            failures.append({"check": "invariant_required_fields", "invariant_id": row.get("invariant_id"), "reason": f"missing fields: {', '.join(missing)}"})
        if row.get("status") not in {STATUS_PASS, STATUS_FAIL, STATUS_WARNING}:
            failures.append({"check": "invariant_status", "invariant_id": row.get("invariant_id"), "reason": f"invalid status {row.get('status')}"})
        if row.get("status") == STATUS_FAIL and row.get("blocking") is True:
            failures.append({"check": "blocking_semantic_invariant", "invariant_id": row.get("invariant_id"), "surface_id": row.get("surface_id"), "reason": row.get("reason")})
    for surface_id in ("performance", "sleeve_analytics"):
        if not any(isinstance(row, Mapping) and row.get("surface_id") == surface_id for row in rows):
            failures.append({"check": "surface_covered", "surface_id": surface_id, "reason": "no invariants emitted for required surface"})
    return {
        "ok": not failures,
        "status": "PASS" if not failures else "FAIL",
        "day_utc": day,
        "path": str(semantic_invariants_path_v1(truth_root=root, day_utc=day)),
        "summary": payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {},
        "failures": failures,
        "safety": dict(SAFETY),
    }


def blocking_failures_for_surface_v1(*, truth_root: Path | str, day_utc: str, surface_id: str) -> list[dict[str, Any]]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    payload = read_json_v1(semantic_invariants_path_v1(truth_root=root, day_utc=day))
    if not payload:
        payload = build_semantic_invariants_v1(truth_root=root, day_utc=day)
        write_semantic_invariants_v1(truth_root=root, day_utc=day, payload=payload)
    surface = payload.get("surfaces", {}).get(surface_id) if isinstance(payload.get("surfaces"), Mapping) else {}
    failures = surface.get("failed_invariants") if isinstance(surface, Mapping) and isinstance(surface.get("failed_invariants"), list) else []
    return [dict(row) for row in failures if isinstance(row, Mapping) and row.get("blocking") is True]


def _source_payloads(root: Path, day: str) -> dict[str, dict[str, Any]]:
    reports = root / "reports"
    paths = {
        "paper_pnl_report": reports / "aegis_paper_pnl_report_v1" / day / "paper_pnl_report.v1.json",
        "daily_paper_performance": reports / "aegis_daily_paper_performance_v1" / day / "daily_paper_performance.v1.json",
        "sleeve_analytics": reports / "aegis_sleeve_analytics_v1" / day / "sleeve_analytics.v1.json",
        "paper_position_ledger": reports / "aegis_paper_position_ledger_v1" / day / "paper_position_ledger.v1.json",
    }
    return {name: {"path": path, "payload": read_json_v1(path), "hash": _sha256(path) if path.exists() else ""} for name, path in paths.items()}


def _performance_invariants(*, day: str, generated_at: str, sources: Mapping[str, dict[str, Any]]) -> list[dict[str, Any]]:
    pnl = _payload(sources, "paper_pnl_report")
    daily = _payload(sources, "daily_paper_performance")
    overview = pnl.get("overview") if isinstance(pnl.get("overview"), Mapping) else {}
    mark_coverage = overview.get("mark_coverage") if isinstance(overview.get("mark_coverage"), Mapping) else pnl.get("mark_coverage") if isinstance(pnl.get("mark_coverage"), Mapping) else {}
    full_status = _upper(overview.get("full_portfolio_pnl_status") or pnl.get("full_portfolio_pnl_status") or daily.get("full_portfolio_pnl_status"))
    data_quality = _upper(overview.get("data_quality") or pnl.get("data_quality_status") or daily.get("data_quality_status"))
    total_pnl = overview.get("total_pnl", pnl.get("total_paper_pnl", daily.get("total_paper_pnl")))
    open_positions = overview.get("open_positions", pnl.get("open_position_count", daily.get("total_open_positions")))
    missing_marks = overview.get("missing_mark_count", mark_coverage.get("missing_mark_position_count"))
    mark_pct = mark_coverage.get("mark_coverage_by_position_pct", overview.get("mark_coverage_pct"))
    if mark_pct is None and mark_coverage.get("marked_position_count") is not None and mark_coverage.get("open_position_count"):
        mark_pct = _pct(mark_coverage.get("marked_position_count"), mark_coverage.get("open_position_count"))
    source_keys = ["paper_pnl_report", "daily_paper_performance", "paper_position_ledger"]
    return [
        _result(
            surface_id="performance",
            invariant_id="performance_open_positions_unavailable_mark_coverage_complete",
            failed=_is_unavailable(open_positions) and _is_complete_pct(mark_pct),
            reason="Open positions are unavailable but mark coverage is reported as complete.",
            fields={"open_positions": open_positions, "mark_coverage": mark_pct},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
        _result(
            surface_id="performance",
            invariant_id="performance_not_canonical_with_complete_analytics",
            failed=data_quality == "NOT_CANONICAL" and (full_status == "CANONICAL" or _is_complete_pct(mark_pct) or _is_number(total_pnl)),
            reason="Performance data quality is NOT_CANONICAL but complete analytics are present.",
            fields={"data_quality_status": data_quality, "full_portfolio_pnl_status": full_status, "mark_coverage": mark_pct, "total_paper_pnl": total_pnl},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
        _result(
            surface_id="performance",
            invariant_id="performance_status_not_canonical_total_pnl_canonical",
            failed=full_status == "NOT_CANONICAL" and _is_number(total_pnl),
            reason="Full portfolio P&L is NOT_CANONICAL but total P&L appears canonical.",
            fields={"full_portfolio_pnl_status": full_status, "total_paper_pnl": total_pnl},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
        _result(
            surface_id="performance",
            invariant_id="performance_missing_marks_with_complete_coverage",
            failed=(_number(missing_marks) or 0) > 0 and _is_complete_pct(mark_pct),
            reason="Missing marks are present but mark coverage is reported as 100%.",
            fields={"missing_marks": missing_marks, "mark_coverage": mark_pct},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
        _result(
            surface_id="performance",
            invariant_id="performance_missing_marks_unavailable_with_complete_coverage",
            failed=_is_unavailable(missing_marks) and _is_complete_pct(mark_pct),
            reason="Missing mark count is unavailable but mark coverage is reported as 100%.",
            fields={"missing_marks": missing_marks, "mark_coverage": mark_pct},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
        _result(
            surface_id="performance",
            invariant_id="performance_total_pnl_unavailable_but_canonical",
            failed=_is_unavailable(total_pnl) and full_status == "CANONICAL",
            reason="Total paper P&L is unavailable but full portfolio P&L status is CANONICAL.",
            fields={"total_paper_pnl": total_pnl, "full_portfolio_pnl_status": full_status},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
    ]


def _sleeve_analytics_invariants(*, day: str, generated_at: str, sources: Mapping[str, dict[str, Any]]) -> list[dict[str, Any]]:
    sleeve = _payload(sources, "sleeve_analytics")
    summary = sleeve.get("summary") if isinstance(sleeve.get("summary"), Mapping) else {}
    dq = sleeve.get("data_quality") if isinstance(sleeve.get("data_quality"), Mapping) else {}
    status = _upper(sleeve.get("status"))
    data_quality_status = _upper(summary.get("data_quality_status") or dq.get("data_quality_status"))
    total_sleeves = summary.get("total_sleeves")
    open_positions = summary.get("total_open_positions")
    mark_pct = summary.get("mark_coverage_pct")
    attribution_pct = summary.get("sleeve_attribution_coverage_pct")
    missing_marks = dq.get("missing_mark_count")
    total_pnl = summary.get("total_pnl")
    source_keys = ["sleeve_analytics", "paper_position_ledger"]
    return [
        _result(
            surface_id="sleeve_analytics",
            invariant_id="sleeve_total_sleeves_zero_attribution_complete",
            failed=(_number(total_sleeves) == 0) and _is_complete_pct(attribution_pct),
            reason="Total sleeves is zero but attribution coverage is 100%.",
            fields={"total_sleeves": total_sleeves, "attribution_coverage": attribution_pct},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
        _result(
            surface_id="sleeve_analytics",
            invariant_id="sleeve_total_sleeves_zero_mark_coverage_complete",
            failed=(_number(total_sleeves) == 0) and _is_complete_pct(mark_pct),
            reason="Total sleeves is zero but mark coverage is 100%.",
            fields={"total_sleeves": total_sleeves, "mark_coverage": mark_pct},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
        _result(
            surface_id="sleeve_analytics",
            invariant_id="sleeve_open_positions_unavailable_mark_coverage_complete",
            failed=_is_unavailable(open_positions) and _is_complete_pct(mark_pct),
            reason="Open positions are unavailable but mark coverage is reported as complete.",
            fields={"open_positions": open_positions, "mark_coverage": mark_pct},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
        _result(
            surface_id="sleeve_analytics",
            invariant_id="sleeve_open_positions_unavailable_attribution_complete",
            failed=_is_unavailable(open_positions) and _is_complete_pct(attribution_pct),
            reason="Open positions are unavailable but attribution coverage is reported as complete.",
            fields={"open_positions": open_positions, "attribution_coverage": attribution_pct},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
        _result(
            surface_id="sleeve_analytics",
            invariant_id="sleeve_not_canonical_with_complete_analytics",
            failed=(status == "NOT_CANONICAL" or data_quality_status == "NOT_CANONICAL") and (_is_complete_pct(mark_pct) or _is_complete_pct(attribution_pct) or _is_number(total_pnl)),
            reason="Sleeve Analytics is NOT_CANONICAL but complete analytics are present.",
            fields={"status": status, "data_quality_status": data_quality_status, "mark_coverage": mark_pct, "attribution_coverage": attribution_pct, "total_pnl": total_pnl},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
        _result(
            surface_id="sleeve_analytics",
            invariant_id="sleeve_status_not_canonical_total_pnl_canonical",
            failed=status == "NOT_CANONICAL" and _is_number(total_pnl),
            reason="Sleeve Analytics status is NOT_CANONICAL but total P&L appears canonical.",
            fields={"status": status, "total_pnl": total_pnl},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
        _result(
            surface_id="sleeve_analytics",
            invariant_id="sleeve_missing_marks_with_complete_coverage",
            failed=(_number(missing_marks) or 0) > 0 and _is_complete_pct(mark_pct),
            reason="Missing marks are present but mark coverage is reported as 100%.",
            fields={"missing_marks": missing_marks, "mark_coverage": mark_pct},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
        _result(
            surface_id="sleeve_analytics",
            invariant_id="sleeve_missing_marks_unavailable_with_complete_coverage",
            failed=_is_unavailable(missing_marks) and _is_complete_pct(mark_pct),
            reason="Missing mark count is unavailable but mark coverage is reported as 100%.",
            fields={"missing_marks": missing_marks, "mark_coverage": mark_pct},
            sources=sources,
            source_keys=source_keys,
            generated_at=generated_at,
        ),
    ]


def _result(*, surface_id: str, invariant_id: str, failed: bool, reason: str, fields: Mapping[str, Any], sources: Mapping[str, dict[str, Any]], source_keys: list[str], generated_at: str, blocking: bool = True) -> dict[str, Any]:
    source_artifacts = {key: str(sources[key]["path"]) for key in source_keys if key in sources}
    hashes = {key: str(sources[key].get("hash") or "") for key in source_keys if key in sources and sources[key].get("hash")}
    return {
        "surface_id": surface_id,
        "invariant_id": invariant_id,
        "status": STATUS_FAIL if failed else STATUS_PASS,
        "blocking": bool(blocking),
        "reason": reason if failed else "Invariant passed.",
        "field_values": dict(fields),
        "source_artifacts": source_artifacts,
        "source_artifact_hashes": hashes,
        "generated_at": generated_at,
    }


def _payload(sources: Mapping[str, dict[str, Any]], key: str) -> dict[str, Any]:
    payload = sources.get(key, {}).get("payload", {})
    return payload if isinstance(payload, dict) else {}


def _upper(value: Any) -> str:
    return str(value or "").upper()


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip().replace("%", "")
        if not stripped or stripped.upper() in {"UNAVAILABLE", "NOT_CANONICAL", "N/A", "NONE", "NULL", "BLOCKED"}:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _is_number(value: Any) -> bool:
    return _number(value) is not None


def _is_unavailable(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().upper() in {"", "UNAVAILABLE", "NOT_CANONICAL", "N/A", "NONE", "NULL", "BLOCKED"}
    return False


def _is_complete_pct(value: Any) -> bool:
    number = _number(value)
    if number is None:
        return False
    return number >= 99.999


def _pct(numerator: Any, denominator: Any) -> float | None:
    n = _number(numerator)
    d = _number(denominator)
    if n is None or d in {None, 0}:
        return None
    return round((n / d) * 100, 6)


def _sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""
