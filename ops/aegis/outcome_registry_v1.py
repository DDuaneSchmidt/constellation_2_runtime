from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.paper_outcome_auto_closure_v1 import AUTO_CLOSED_PAPER_OUTCOME, paper_outcome_auto_closure_path_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1, text_v1

REPORT_FAMILY = "aegis_outcome_registry_v1"
REPORT_FILENAME = "outcome_registry.v1.json"
SAFETY = {"read_only": True, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "live_trading_allowed": False}


def outcome_registry_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def build_outcome_registry_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    ledger_path = report_path_v1(root, "aegis_paper_position_ledger_v1", day_utc, "paper_position_ledger.v1.json")
    ledger = read_json_v1(ledger_path)
    auto_closure_path = paper_outcome_auto_closure_path_v1(truth_root=root, day_utc=str(day_utc))
    auto_closure = read_json_v1(auto_closure_path)
    auto_closures_by_position = _auto_closures_by_position(auto_closure)
    positions = _positions(ledger)
    outcomes = [_outcome_from_position(row, day_utc=str(day_utc), ledger_path=ledger_path, auto_closure=auto_closures_by_position.get(_position_id(row), {}), auto_closure_path=auto_closure_path) for row in positions]
    summary = {
        "paper_position_count": len(positions),
        "open_outcomes": sum(1 for row in outcomes if row["outcome_state"] == "OPEN"),
        "closed_outcomes": sum(1 for row in outcomes if row["outcome_state"].startswith("CLOSED_")),
        "unknown_blocked": sum(1 for row in outcomes if row["outcome_state"] == "UNKNOWN_BLOCKED"),
        "resolved_outcomes": sum(1 for row in outcomes if row["outcome_state"] in {"CLOSED_WIN", "CLOSED_LOSS", "CLOSED_FLAT", "EXPIRED", "INVALIDATED"}),
    }
    payload = {"schema_id": "aegis_outcome_registry", "schema_version": "v1", "artifact_id": REPORT_FAMILY, "day_utc": str(day_utc), "generated_at": _now(), "outcomes": outcomes, "summary": summary, "source_artifact_paths": {"paper_position_ledger": str(ledger_path), "paper_outcome_auto_closure": str(auto_closure_path) if auto_closure else ""}, "safety": dict(SAFETY), **SAFETY}
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_outcome_registry_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(outcome_registry_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_outcome_registry_v1(truth_root=truth_root, day_utc=day_utc))


def _positions(ledger: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for key in ("positions", "open_positions", "closed_positions"):
        for row in ledger.get(key) or []:
            if isinstance(row, Mapping):
                rows.append(dict(row))
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        position_id = text_v1(row.get("position_id") or row.get("candidate_id"))
        if position_id:
            out[position_id] = {**out.get(position_id, {}), **row}
    return list(out.values())


def _position_id(row: Mapping[str, Any]) -> str:
    return text_v1(row.get("position_id") or row.get("candidate_id"))


def _auto_closures_by_position(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in payload.get("rows") or []:
        if not isinstance(row, Mapping) or row.get("auto_closure_state") != AUTO_CLOSED_PAPER_OUTCOME:
            continue
        position_id = text_v1(row.get("position_id"))
        if position_id:
            out[position_id] = dict(row)
    return out


def _outcome_from_position(row: Mapping[str, Any], *, day_utc: str, ledger_path: Path, auto_closure: Mapping[str, Any] | None = None, auto_closure_path: Path | None = None) -> dict[str, Any]:
    position_id = text_v1(row.get("position_id") or row.get("candidate_id"))
    entry = _num(row.get("entry_price"))
    latest = _num(row.get("current_certified_mark"), row.get("mark_price"))
    auto_closure = auto_closure if isinstance(auto_closure, Mapping) else {}
    auto_closed = bool(auto_closure.get("auto_closure_state") == AUTO_CLOSED_PAPER_OUTCOME)
    exit_mark = _num(auto_closure.get("exit_mark") if auto_closed else None, row.get("exit_price"))
    realized_pnl = _num(row.get("realized_pnl"))
    qty = abs(_num(row.get("quantity")) or 0.0)
    close_time = text_v1(auto_closure.get("outcome_timestamp") if auto_closed else "") or text_v1(row.get("exit_time"))
    open_day = _day(row.get("entry_time") or row.get("originating_day")) or text_v1(row.get("originating_day")) or day_utc
    close_day = _day(close_time) if close_time else ""
    blockers = []
    closed = bool(auto_closed or close_time or exit_mark is not None or realized_pnl not in (None, 0.0) and text_v1(row.get("realized_pnl")))
    if not text_v1(row.get("hypothesis_id")):
        blockers.append("MISSING_HYPOTHESIS_ID")
    if not text_v1(row.get("sleeve_id")) or text_v1(row.get("sleeve_id")) == "UNKNOWN":
        blockers.append("MISSING_SLEEVE_ID")
    if not text_v1(row.get("candidate_id")):
        blockers.append("MISSING_CANDIDATE_ID")
    if entry is None:
        blockers.append("MISSING_ENTRY_MARK")
    if latest is None and not closed:
        blockers.append("MISSING_LATEST_MARK")
    if closed and exit_mark is None and realized_pnl is None:
        blockers.append("MISSING_EXIT_OR_REALIZED_RETURN")
    if blockers:
        state = "UNKNOWN_BLOCKED"
    elif closed:
        realized_return = _realized_return(entry, exit_mark, realized_pnl, qty)
        if realized_return is None:
            state = "UNKNOWN_BLOCKED"; blockers.append("MISSING_REALIZED_RETURN")
        elif realized_return > 0:
            state = "CLOSED_WIN"
        elif realized_return < 0:
            state = "CLOSED_LOSS"
        else:
            state = "CLOSED_FLAT"
    else:
        state = "OPEN"
    realized_return = _num(auto_closure.get("realized_return")) if auto_closed and not blockers else (_realized_return(entry, exit_mark, realized_pnl, qty) if closed and not blockers else None)
    unrealized_return = ((latest - entry) / entry) if (not closed and latest is not None and entry not in (None, 0.0)) else None
    source_artifacts = [str(ledger_path), text_v1(row.get("mark_source_path")), *[text_v1(p) for p in (((row.get("candidate_lineage") or {}) if isinstance(row.get("candidate_lineage"), Mapping) else {}).get("evidence_paths") or [])]]
    if auto_closed and auto_closure_path:
        source_artifacts.extend([str(auto_closure_path), *[text_v1(p) for p in (auto_closure.get("source_artifacts") or [])]])
    source_artifacts = [x for x in dict.fromkeys(source_artifacts) if x]
    generated_at = _now()
    outcome_id = "outcome_" + stable_hash_v1({"position_id": position_id, "candidate_id": row.get("candidate_id"), "day_utc": day_utc})[:24]
    return {
        "outcome_id": outcome_id,
        "thesis_id": text_v1(row.get("thesis_id") or ((row.get("candidate_lineage") or {}) if isinstance(row.get("candidate_lineage"), Mapping) else {}).get("thesis_id")),
        "hypothesis_id": text_v1(row.get("hypothesis_id") or ((row.get("candidate_lineage") or {}) if isinstance(row.get("candidate_lineage"), Mapping) else {}).get("hypothesis_id")),
        "sleeve_id": text_v1(row.get("sleeve_id")),
        "candidate_id": text_v1(row.get("candidate_id")),
        "position_id": position_id,
        "open_day_utc": open_day,
        "close_day_utc": close_day,
        "outcome_state": state,
        "entry_mark": entry,
        "latest_mark": latest,
        "exit_mark": exit_mark,
        "exit_trigger": text_v1(auto_closure.get("exit_trigger")) if auto_closed else "",
        "trigger_timestamp": text_v1(auto_closure.get("trigger_timestamp")) if auto_closed else "",
        "outcome_timestamp": text_v1(auto_closure.get("outcome_timestamp")) if auto_closed else close_time,
        "auto_closure_state": text_v1(auto_closure.get("auto_closure_state")) if auto_closed else "",
        "closure_source": "aegis_paper_outcome_auto_closure_v1" if auto_closed else ("paper_position_ledger" if closed else ""),
        "deterministic_return_formula_version": text_v1(auto_closure.get("deterministic_return_formula_version")) if auto_closed else "",
        "realized_return": _round(realized_return),
        "unrealized_return": _round(unrealized_return),
        "holding_period_days": _holding_days(open_day, close_day or day_utc),
        "source_artifacts": source_artifacts,
        "source_hashes": {path: _file_hash(path) for path in source_artifacts},
        "blocker_reasons": blockers,
        "generated_at": generated_at,
    }


def _realized_return(entry: float | None, exit_mark: float | None, realized_pnl: float | None, qty: float) -> float | None:
    if entry in (None, 0.0):
        return None
    if exit_mark is not None:
        return (exit_mark - entry) / entry
    if realized_pnl is not None and qty:
        return realized_pnl / (entry * qty)
    return None


def _num(*values: Any) -> float | None:
    for value in values:
        if value in (None, "") or isinstance(value, bool):
            continue
        try:
            return float(str(value).replace(",", ""))
        except Exception:
            continue
    return None


def _day(value: Any) -> str:
    raw = text_v1(value)
    return raw[:10] if len(raw) >= 10 else ""


def _holding_days(open_day: str, close_day: str) -> int | None:
    try:
        return max(0, (datetime.fromisoformat(close_day[:10]) - datetime.fromisoformat(open_day[:10])).days)
    except Exception:
        return None


def _file_hash(path: str) -> str:
    try:
        return hashlib.sha256(Path(path).expanduser().read_bytes()).hexdigest()
    except OSError:
        return ""


def _round(value: float | None) -> float | None:
    return round(value, 8) if value is not None else None


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
