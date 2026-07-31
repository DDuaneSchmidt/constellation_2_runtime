from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.exit_recommendations_v1 import exit_recommendations_path_v1
from ops.aegis.paper_position_ledger_v1 import build_paper_position_ledger_v1, paper_position_ledger_path_v1
from ops.aegis.sleeve_attribution_recovery_v1 import SleeveAttributionRecoveryIndex

REPORT_FAMILY = "aegis_paper_pnl_report_v1"
REPORT_FILENAME = "paper_pnl_report.v1.json"

SAFETY = {
    "paper_only": True,
    "human_review_required": True,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "autonomous_execution_allowed": False,
    "trade_advice_allowed": False,
    "automatic_exit_allowed": False,
    "live_trading_allowed": False,
}


def paper_pnl_report_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_paper_pnl_report_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    ledger_path = paper_position_ledger_path_v1(truth_root=root, day_utc=day)
    ledger = _read_json(ledger_path) or build_paper_position_ledger_v1(truth_root=root, day_utc=day)
    exit_path = exit_recommendations_path_v1(truth_root=root, day_utc=day)
    exit_payload = _read_json(exit_path)
    sleeve_index = _candidate_sleeve_index(root=root, day_utc=day, exit_payload=exit_payload)
    recovery_index = SleeveAttributionRecoveryIndex(truth_root=root, day_utc=day)

    open_positions = []
    closed_positions = []
    by_sleeve: dict[str, dict[str, Any]] = {}
    by_symbol: dict[str, dict[str, Any]] = {}
    missing_mark_positions: list[str] = []
    sleeve_attribution_reconciliation: list[dict[str, Any]] = []

    for row in _rows(ledger, "open_positions"):
        out = _open_position_row(row, sleeve_index, recovery_index)
        reconciliation = out.pop("_sleeve_attribution_reconciliation", None)
        if isinstance(reconciliation, dict):
            sleeve_attribution_reconciliation.append(reconciliation)
        open_positions.append(out)
        sleeve = str(out.get("sleeve_id") or "UNKNOWN")
        symbol = str(out.get("symbol") or "UNKNOWN")
        _accumulate(by_sleeve, sleeve, out, is_open=True, id_field="sleeve_id")
        _accumulate(by_symbol, symbol, out, is_open=True, id_field="symbol")
        if out.get("unrealized_pnl_status") != "AVAILABLE":
            missing_mark_positions.append(str(out.get("position_id") or ""))

    for row in _rows(ledger, "closed_positions"):
        out = _closed_position_row(row, sleeve_index, recovery_index)
        reconciliation = out.pop("_sleeve_attribution_reconciliation", None)
        if isinstance(reconciliation, dict):
            sleeve_attribution_reconciliation.append(reconciliation)
        closed_positions.append(out)
        sleeve = str(out.get("sleeve_id") or "UNKNOWN")
        symbol = str(out.get("symbol") or "UNKNOWN")
        _accumulate(by_sleeve, sleeve, out, is_open=False, id_field="sleeve_id")
        _accumulate(by_symbol, symbol, out, is_open=False, id_field="symbol")

    total_unrealized = sum((_decimal(row.get("unrealized_pnl")) or Decimal("0")) for row in open_positions if row.get("unrealized_pnl_status") == "AVAILABLE")
    total_realized = sum((_decimal(row.get("realized_pnl")) or Decimal("0")) for row in closed_positions)
    mark_coverage = _mark_coverage(open_positions)
    full_mark_coverage = not missing_mark_positions
    source_paths = {
        "paper_position_ledger": str(ledger_path),
        "exit_recommendations": str(exit_path) if exit_path.exists() else "",
        "market_data": str(ledger.get("market_data_path") or ""),
    }
    source_hashes = {key: _file_hash(Path(path)) for key, path in source_paths.items() if path}
    payload = {
        "schema_id": "aegis_paper_pnl_report",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at_utc": _now(),
        "operating_mode": "HUMAN_REVIEWED_PAPER_MODE",
        "truth_model": "CERTIFIED_MARK_PAPER_PNL_READ_MODEL",
        "open_position_count": len(open_positions),
        "closed_position_count": len(closed_positions),
        "open_positions": open_positions,
        "closed_positions": closed_positions,
        "unrealized_pnl": _decimal_text(total_unrealized) if full_mark_coverage else "NOT_CANONICAL",
        "realized_pnl": _decimal_text(total_realized),
        "total_paper_pnl": _decimal_text(total_realized + total_unrealized) if full_mark_coverage else "NOT_CANONICAL",
        "full_portfolio_pnl_status": "CANONICAL" if full_mark_coverage else "NOT_CANONICAL",
        "certified_unrealized_pnl": _decimal_text(total_unrealized),
        "certified_total_paper_pnl": _decimal_text(total_realized + total_unrealized),
        "mark_coverage": mark_coverage,
        "mark_coverage_by_position_pct": mark_coverage.get("mark_coverage_by_position_pct"),
        "mark_coverage_by_entry_notional_pct": mark_coverage.get("mark_coverage_by_entry_notional_pct"),
        "missing_mark_count": mark_coverage.get("missing_mark_position_count"),
        "missing_mark_symbols": mark_coverage.get("missing_symbols"),
        "pnl_by_sleeve": [_final_bucket(row) for row in sorted(by_sleeve.values(), key=lambda item: str(item.get("sleeve_id") or ""))],
        "pnl_by_symbol": [_final_bucket(row) for row in sorted(by_symbol.values(), key=lambda item: str(item.get("symbol") or ""))],
        "sleeve_attribution_reconciliation": sleeve_attribution_reconciliation,
        "source_artifact_paths": source_paths,
        "source_hashes": source_hashes,
        "missing_mark_position_ids": [row for row in missing_mark_positions if row],
        "data_quality_status": "PASS" if full_mark_coverage else "PARTIAL_CERTIFIED_UNREALIZED_PNL",
        "data_quality_explanation": "Full portfolio P&L is canonical because every open position has a certified current mark." if full_mark_coverage else "Full portfolio P&L is not canonical because one or more open positions lack certified current marks. Certified partial P&L is shown for covered positions only.",
        "rules": {
            "unrealized_pnl_requires_certified_current_mark": True,
            "missing_or_stale_mark_keeps_unrealized_pnl_not_canonical": True,
            "certified_partial_unrealized_pnl_allowed_for_covered_positions": True,
            "closed_realized_pnl_comes_from_position_ledger": True,
        },
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = _stable_hash({**payload, "generated_at_utc": "", "content_hash": ""})
    return payload


def write_paper_pnl_report_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    root = Path(truth_root).expanduser().resolve()
    body = dict(payload or build_paper_pnl_report_v1(truth_root=root, day_utc=day_utc))
    path = paper_pnl_report_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return path


def _open_position_row(row: Mapping[str, Any], sleeve_index: Mapping[str, Mapping[str, str]], recovery_index: SleeveAttributionRecoveryIndex) -> dict[str, Any]:
    sleeve, source, unknown_reason, reconciliation = _sleeve_assignment(row, sleeve_index, recovery_index)
    return {
        "position_id": str(row.get("position_id") or ""),
        "candidate_id": str(row.get("candidate_id") or ""),
        "symbol": str(row.get("symbol") or "").upper(),
        "sleeve_id": sleeve,
        "sleeve_assignment_source": source,
        "recovered_sleeve": sleeve if unknown_reason is None else None,
        "unknown_reason": unknown_reason,
        "side": str(row.get("side") or ""),
        "quantity": str(row.get("quantity") or ""),
        "entry_price": str(row.get("entry_price") or ""),
        "entry_notional": _decimal_text((_decimal(row.get("quantity")) or Decimal("0")) * (_decimal(row.get("entry_price")) or Decimal("0"))),
        "entry_time": str(row.get("entry_time") or ""),
        "current_certified_mark": str(row.get("current_certified_mark") or ""),
        "mark_price": str(row.get("mark_price") or ""),
        "mark_timestamp_utc": str(row.get("mark_timestamp_utc") or ""),
        "mark_source_path": str(row.get("mark_source_path") or ""),
        "mark_source_hash": str(row.get("mark_source_hash") or ""),
        "mark_freshness_status": str(row.get("mark_freshness_status") or ""),
        "mark_certification_status": str(row.get("mark_certification_status") or "MISSING_MARK"),
        "unrealized_pnl_status": str(row.get("unrealized_pnl_status") or "NOT_CANONICAL"),
        "unrealized_pnl": str(row.get("unrealized_pnl") or "") if str(row.get("unrealized_pnl_status") or "") == "AVAILABLE" else "NOT_CANONICAL",
        "_sleeve_attribution_reconciliation": reconciliation,
    }


def _closed_position_row(row: Mapping[str, Any], sleeve_index: Mapping[str, Mapping[str, str]], recovery_index: SleeveAttributionRecoveryIndex) -> dict[str, Any]:
    sleeve, source, unknown_reason, reconciliation = _sleeve_assignment(row, sleeve_index, recovery_index)
    return {
        "position_id": str(row.get("position_id") or ""),
        "candidate_id": str(row.get("candidate_id") or ""),
        "symbol": str(row.get("symbol") or "").upper(),
        "sleeve_id": sleeve,
        "sleeve_assignment_source": source,
        "recovered_sleeve": sleeve if unknown_reason is None else None,
        "unknown_reason": unknown_reason,
        "side": str(row.get("side") or ""),
        "quantity": str(row.get("quantity") or ""),
        "entry_price": str(row.get("entry_price") or ""),
        "entry_notional": _decimal_text((_decimal(row.get("quantity")) or Decimal("0")) * (_decimal(row.get("entry_price")) or Decimal("0"))),
        "entry_time": str(row.get("entry_time") or ""),
        "exit_price": str(row.get("exit_price") or ""),
        "exit_time": str(row.get("exit_time") or ""),
        "realized_pnl": str(row.get("realized_pnl") or "0"),
        "_sleeve_attribution_reconciliation": reconciliation,
    }


def _accumulate(buckets: dict[str, dict[str, Any]], key: str, row: Mapping[str, Any], *, is_open: bool, id_field: str) -> None:
    bucket = buckets.setdefault(key, {
        id_field: key,
        "open_position_count": 0,
        "closed_position_count": 0,
        "unrealized_pnl": Decimal("0"),
        "realized_pnl": Decimal("0"),
        "unrealized_pnl_status": "AVAILABLE",
        "certified_unrealized_pnl": Decimal("0"),
        "open_entry_notional": Decimal("0"),
        "certified_entry_notional": Decimal("0"),
        "marked_open_position_count": 0,
        "missing_mark_position_count": 0,
        "missing_mark_symbols": [],
        "position_ids": [],
    })
    bucket["position_ids"].append(str(row.get("position_id") or ""))
    if is_open:
        bucket["open_position_count"] += 1
        entry_notional = _decimal(row.get("entry_notional")) or Decimal("0")
        bucket["open_entry_notional"] += entry_notional
        if row.get("unrealized_pnl_status") == "AVAILABLE":
            pnl = _decimal(row.get("unrealized_pnl")) or Decimal("0")
            bucket["unrealized_pnl"] += pnl
            bucket["certified_unrealized_pnl"] += pnl
            bucket["certified_entry_notional"] += entry_notional
            bucket["marked_open_position_count"] += 1
        else:
            bucket["unrealized_pnl_status"] = "NOT_CANONICAL"
            bucket["missing_mark_position_count"] += 1
            symbol = str(row.get("symbol") or "")
            if symbol and symbol not in bucket["missing_mark_symbols"]:
                bucket["missing_mark_symbols"].append(symbol)
    else:
        bucket["closed_position_count"] += 1
        bucket["realized_pnl"] += _decimal(row.get("realized_pnl")) or Decimal("0")


def _final_bucket(row: dict[str, Any]) -> dict[str, Any]:
    unrealized_status = str(row.get("unrealized_pnl_status") or "AVAILABLE")
    realized_value = row.get("realized_pnl") or Decimal("0")
    certified_unrealized = row.get("certified_unrealized_pnl") or row.get("unrealized_pnl") or Decimal("0")
    unrealized = _decimal_text(row.get("unrealized_pnl") or Decimal("0")) if unrealized_status == "AVAILABLE" else "NOT_CANONICAL"
    realized = _decimal_text(realized_value)
    total = _decimal_text(realized_value + (row.get("unrealized_pnl") or Decimal("0"))) if unrealized_status == "AVAILABLE" else "NOT_CANONICAL"
    open_count = int(row.get("open_position_count") or 0)
    marked_count = int(row.get("marked_open_position_count") or 0)
    total_notional = row.get("open_entry_notional") or Decimal("0")
    marked_notional = row.get("certified_entry_notional") or Decimal("0")
    return {
        **row,
        "unrealized_pnl": unrealized,
        "realized_pnl": realized,
        "total_paper_pnl": total,
        "certified_unrealized_pnl": _decimal_text(certified_unrealized),
        "certified_total_paper_pnl": _decimal_text(realized_value + certified_unrealized),
        "open_entry_notional": _decimal_text(total_notional),
        "certified_entry_notional": _decimal_text(marked_notional),
        "mark_coverage_by_position_pct": round((marked_count / open_count) * 100, 4) if open_count else 100.0,
        "mark_coverage_by_entry_notional_pct": round((float(marked_notional) / float(total_notional)) * 100, 4) if total_notional else 100.0,
        "missing_mark_symbols": sorted(row.get("missing_mark_symbols") or []),
    }


def _mark_coverage(open_positions: list[dict[str, Any]]) -> dict[str, Any]:
    open_count = len(open_positions)
    marked = [row for row in open_positions if row.get("unrealized_pnl_status") == "AVAILABLE"]
    missing = [row for row in open_positions if row.get("unrealized_pnl_status") != "AVAILABLE"]
    total_notional = sum((_decimal(row.get("entry_notional")) or Decimal("0")) for row in open_positions)
    marked_notional = sum((_decimal(row.get("entry_notional")) or Decimal("0")) for row in marked)
    return {
        "open_position_count": open_count,
        "marked_position_count": len(marked),
        "missing_mark_position_count": len(missing),
        "unique_missing_symbol_count": len({str(row.get("symbol") or "") for row in missing if row.get("symbol")}),
        "mark_coverage_by_position_pct": round((len(marked) / open_count) * 100, 4) if open_count else 100.0,
        "mark_coverage_by_entry_notional_pct": round((float(marked_notional) / float(total_notional)) * 100, 4) if total_notional else 100.0,
        "total_entry_notional": _decimal_text(total_notional),
        "marked_entry_notional": _decimal_text(marked_notional),
        "missing_symbols": sorted({str(row.get("symbol") or "") for row in missing if row.get("symbol")}),
        "missing_position_ids": [str(row.get("position_id") or "") for row in missing if row.get("position_id")],
    }


def _sleeve_id(row: Mapping[str, Any], sleeve_index: Mapping[str, Mapping[str, str]], recovery_index: SleeveAttributionRecoveryIndex) -> str:
    return _sleeve_assignment(row, sleeve_index, recovery_index)[0]


def _sleeve_assignment(row: Mapping[str, Any], sleeve_index: Mapping[str, Mapping[str, str]], recovery_index: SleeveAttributionRecoveryIndex) -> tuple[str, str, str | None, dict[str, Any] | None]:
    lineage = row.get("candidate_lineage") if isinstance(row.get("candidate_lineage"), Mapping) else {}
    receipt = row.get("source_receipt") if isinstance(row.get("source_receipt"), Mapping) else {}
    candidate_id = str(row.get("candidate_id") or receipt.get("candidate_id") or "")
    by_candidate = sleeve_index.get("by_candidate", {}) if isinstance(sleeve_index.get("by_candidate"), Mapping) else {}
    candidates = [
        ("position.sleeve_id", row.get("sleeve_id")),
        ("candidate_lineage.sleeve_id", lineage.get("sleeve_id")),
        ("source_receipt.sleeve_id", receipt.get("sleeve_id")),
        ("candidate_index.by_candidate", by_candidate.get(candidate_id)),
    ]
    for source, value in candidates:
        sleeve = str(value or "").strip()
        if sleeve and sleeve.upper() != "UNKNOWN":
            return sleeve, source, None, None
    reconciliation = recovery_index.recover(row)
    recovered = str(reconciliation.get("recovered_sleeve_id") or "UNKNOWN")
    if recovered != "UNKNOWN":
        return recovered, str(reconciliation.get("attempted_recovery_source") or "lineage_recovery"), None, reconciliation
    return "UNKNOWN", "unresolved", str(reconciliation.get("blocker_reason") or "NO_NON_UNKNOWN_SLEEVE_ASSIGNMENT_SOURCE"), reconciliation


def _candidate_sleeve_index(*, root: Path, day_utc: str, exit_payload: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    by_candidate: dict[str, str] = {}
    by_symbol: dict[str, str] = {}

    def add(row: Mapping[str, Any]) -> None:
        sleeve = str(row.get("sleeve_id") or row.get("sleeve") or "").strip()
        context = row.get("candidate_context") if isinstance(row.get("candidate_context"), Mapping) else {}
        sleeve = sleeve or str(context.get("sleeve_id") or "").strip()
        if not sleeve:
            graph = str(row.get("graph_linkage") or "")
            if "." in graph:
                sleeve = graph.rsplit(".", 1)[-1]
        if not sleeve:
            return
        candidate_id = str(row.get("candidate_id") or row.get("candidate_contract_id") or "").strip()
        symbol = str(row.get("symbol") or "").strip().upper()
        if candidate_id:
            by_candidate.setdefault(candidate_id, sleeve)
        if symbol:
            by_symbol.setdefault(symbol, sleeve)

    for row in _rows(exit_payload, "recommendations", "rows"):
        add(row)
    for family, filename in (
        ("aegis_candidate_contracts_v1", "candidate_contracts.v1.json"),
        ("aegis_candidate_lifecycle_projection_v1", "candidate_lifecycle_projection.v1.json"),
        ("aegis_signal_evidence_boundary_v1", "signal_evidence_boundary.v1.json"),
    ):
        for payload in _latest_payloads(root=root, family=family, filename=filename, day_utc=day_utc):
            for key in ("candidate_contracts", "contracts", "rows", "current_session_candidates", "signals", "boundary_rows"):
                for row in _rows(payload, key):
                    add(row)
    return {"by_candidate": by_candidate, "by_symbol": by_symbol}


def _latest_payloads(*, root: Path, family: str, filename: str, day_utc: str) -> list[dict[str, Any]]:
    base = root / "reports" / family
    if not base.exists():
        return []
    paths = sorted(path for path in base.glob(f"*/{filename}") if path.parent.name <= day_utc)
    return [_read_json(path) for path in paths if _read_json(path)]


def _rows(payload: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
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


def _decimal(value: Any) -> Decimal | None:
    try:
        if value in (None, "") or isinstance(value, bool) or str(value) == "NOT_CANONICAL":
            return None
        return Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _decimal_text(value: Decimal | Any) -> str:
    number = value if isinstance(value, Decimal) else _decimal(value) or Decimal("0")
    normalized = number.quantize(Decimal("0.000001"))
    text = format(normalized.normalize(), "f")
    return "0" if text in {"-0", "-0.0"} else text


def _file_hash(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
