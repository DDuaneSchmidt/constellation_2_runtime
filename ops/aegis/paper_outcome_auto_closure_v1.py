from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from ops.aegis.exit_recommendations_v1 import build_exit_recommendations_v1, exit_recommendations_path_v1
from ops.aegis.future_target_day_audit_guard_v1 import future_target_day_audit_guard_path_v1, read_future_target_day_audit_guard_v1
from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.paper_position_ledger_v1 import build_paper_position_ledger_v1, paper_position_ledger_path_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1, text_v1

REPORT_FAMILY = "aegis_paper_outcome_auto_closure_v1"
REPORT_FILENAME = "paper_outcome_auto_closure.v1.json"

AUTO_CLOSED_PAPER_OUTCOME = "AUTO_CLOSED_PAPER_OUTCOME"
AUTO_CLOSURE_BLOCKED = "AUTO_CLOSURE_BLOCKED"
AUTO_CLOSURE_NOT_ELIGIBLE = "AUTO_CLOSURE_NOT_ELIGIBLE"
RETURN_FORMULA_VERSION = "LONG_EQUITY_RETURN_V1"

SAFETY = {
    "paper_only": True,
    "research_only": True,
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "live_trading_allowed": False,
    "autonomous_execution_allowed": False,
    "automatic_real_world_position_management_allowed": False,
}

EXIT_RECOMMENDATIONS = {
    "EXIT_STOP_LOSS",
    "EXIT_TAKE_PROFIT",
    "EXIT_TRAILING_STOP",
    "EXIT_TIME_STOP",
    "EXIT_SIGNAL_INVALIDATED",
    "EXIT_REGIME_INVALIDATED",
}


def paper_outcome_auto_closure_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def build_paper_outcome_auto_closure_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    ledger_path = paper_position_ledger_path_v1(truth_root=root, day_utc=str(day_utc))
    ledger = read_json_v1(ledger_path)
    if not ledger:
        ledger = build_paper_position_ledger_v1(truth_root=root, day_utc=str(day_utc))
    rec_path = exit_recommendations_path_v1(truth_root=root, day_utc=str(day_utc))
    rec_payload = read_json_v1(rec_path)
    if not rec_payload:
        rec_payload = build_exit_recommendations_v1(truth_root=root, day_utc=str(day_utc))
    runtime_path = root / "reports" / "aegis_runtime_truth_kernel_v1" / str(day_utc) / "runtime_truth_kernel.v1.json"
    runtime = read_json_v1(runtime_path)
    guard_path = future_target_day_audit_guard_path_v1(truth_root=root, day_utc=str(day_utc))
    guard = read_future_target_day_audit_guard_v1(truth_root=root, day_utc=str(day_utc))
    future_guarded = guard.get("guard_status") == "TARGET_DAY_IN_FUTURE_GUARDED" and guard.get("is_future_target_day") is True

    recs = _recommendations_by_position(rec_payload)
    rows = []
    seen_position_ids: set[str] = set()
    for position in _open_positions(ledger):
        rec = recs.get(_position_key(position)) or recs.get(_candidate_key(position)) or {}
        row = _closure_row(position, rec, day_utc=str(day_utc), ledger_path=ledger_path, rec_path=rec_path, runtime_path=runtime_path, runtime=runtime, future_guarded=future_guarded)
        rows.append(row)
        if text_v1(row.get("position_id")):
            seen_position_ids.add(text_v1(row.get("position_id")))
    for row in _persisted_auto_closed_rows(ledger, day_utc=str(day_utc), ledger_path=ledger_path):
        position_id = text_v1(row.get("position_id"))
        if position_id and position_id not in seen_position_ids:
            rows.append(row)
            seen_position_ids.add(position_id)
    rows.sort(key=lambda row: (text_v1(row.get("sleeve_id")), text_v1(row.get("symbol")), text_v1(row.get("position_id"))))
    manual_review_queue = [_manual_review_row(row) for row in rows if row.get("auto_closure_state") == AUTO_CLOSED_PAPER_OUTCOME]
    source_artifacts = {
        "paper_position_ledger": str(ledger_path),
        "exit_recommendations": str(rec_path),
        "runtime_truth_kernel": str(runtime_path) if runtime_path.exists() else "",
        "future_target_day_guard": str(guard_path) if guard_path.exists() else "",
    }
    summary = {
        "positions_evaluated": len(rows),
        "auto_closed_count": sum(1 for row in rows if row.get("auto_closure_state") == AUTO_CLOSED_PAPER_OUTCOME),
        "blocked_count": sum(1 for row in rows if row.get("auto_closure_state") == AUTO_CLOSURE_BLOCKED),
        "not_eligible_count": sum(1 for row in rows if row.get("auto_closure_state") == AUTO_CLOSURE_NOT_ELIGIBLE),
        "hold_not_eligible_count": sum(1 for row in rows if row.get("exit_recommendation") == "HOLD"),
        "manual_review_queue_count": len(manual_review_queue),
        "included_validation_sample_candidate_count": sum(1 for row in rows if row.get("auto_closure_state") == AUTO_CLOSED_PAPER_OUTCOME),
        "automatic_real_world_position_management_allowed": False,
        "future_target_day_guarded": future_guarded,
        "future_target_day_guard_status": text_v1(guard.get("guard_status")),
    }
    payload = {
        "schema_id": "aegis_paper_outcome_auto_closure",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": str(day_utc),
        "generated_at": _now(),
        "rows": rows,
        "manual_review_queue": manual_review_queue,
        "summary": summary,
        "source_artifacts": source_artifacts,
        "source_hashes": {path: _file_hash(path) for path in source_artifacts.values() if path},
        "safety": dict(SAFETY),
        **SAFETY,
    }
    payload["content_hash"] = stable_hash_v1(normalized_paper_outcome_auto_closure_v1(payload))
    return payload


def write_paper_outcome_auto_closure_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(paper_outcome_auto_closure_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_paper_outcome_auto_closure_v1(truth_root=truth_root, day_utc=day_utc))


def normalized_paper_outcome_auto_closure_v1(payload: Mapping[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(dict(payload))
    out.pop("generated_at", None)
    out.pop("content_hash", None)
    for row in out.get("rows") if isinstance(out.get("rows"), list) else []:
        if isinstance(row, dict):
            row.pop("outcome_timestamp", None)
    return out


def _open_positions(ledger: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in ledger.get("open_positions") or [] if isinstance(row, Mapping)]


def _persisted_auto_closed_rows(ledger: Mapping[str, Any], *, day_utc: str, ledger_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for position in ledger.get("closed_positions") or []:
        if not isinstance(position, Mapping):
            continue
        receipt = position.get("exit_source_receipt") if isinstance(position.get("exit_source_receipt"), Mapping) else {}
        lineage = position.get("closure_lineage") if isinstance(position.get("closure_lineage"), Mapping) else {}
        if not _is_persisted_auto_closure(position, receipt, lineage):
            continue
        source_row = receipt.get("source_row") if isinstance(receipt.get("source_row"), Mapping) else {}
        row = dict(source_row) if source_row else {}
        row.update({
            "position_id": text_v1(row.get("position_id") or position.get("position_id")),
            "candidate_id": text_v1(row.get("candidate_id") or position.get("candidate_id")),
            "symbol": text_v1(row.get("symbol") or position.get("symbol")).upper(),
            "sleeve_id": text_v1(row.get("sleeve_id") or position.get("sleeve_id")),
            "hypothesis_id": text_v1(row.get("hypothesis_id") or position.get("hypothesis_id") or _lineage(position).get("hypothesis_id")),
            "thesis_id": text_v1(row.get("thesis_id") or position.get("thesis_id") or _lineage(position).get("thesis_id")),
            "auto_closure_state": AUTO_CLOSED_PAPER_OUTCOME,
            "lifecycle_state": AUTO_CLOSED_PAPER_OUTCOME,
            "outcome_timestamp": text_v1(row.get("outcome_timestamp") or position.get("exit_time") or lineage.get("outcome_timestamp")),
            "trigger_timestamp": text_v1(row.get("trigger_timestamp") or position.get("exit_time") or lineage.get("trigger_timestamp")),
            "exit_mark": _num(row.get("exit_mark"), position.get("exit_price")),
            "entry_mark": _num(row.get("entry_mark"), position.get("entry_price")),
            "exit_trigger": text_v1(row.get("exit_trigger") or lineage.get("exit_trigger") or receipt.get("exit_trigger") or "AUTO_CLOSED_PAPER_OUTCOME"),
            "exit_recommendation": text_v1(row.get("exit_recommendation") or receipt.get("exit_reason_selected_by_operator") or "AUTO_CLOSED_PAPER_OUTCOME"),
            "deterministic_return_formula_version": text_v1(row.get("deterministic_return_formula_version") or lineage.get("deterministic_return_formula_version") or RETURN_FORMULA_VERSION),
            "closure_id": text_v1(row.get("closure_id") or lineage.get("closure_id") or receipt.get("closure_id")),
            "persisted_from_paper_position_ledger": True,
            "persisted_position_ledger_path": str(ledger_path),
            "persisted_position_ledger_hash": _file_hash(ledger_path),
            "safety": dict(SAFETY),
            **SAFETY,
        })
        if not row["closure_id"]:
            row["closure_id"] = "paper-auto-closure:" + stable_hash_v1({"position_id": row["position_id"], "candidate_id": row["candidate_id"], "day_utc": day_utc, "trigger_timestamp": row["trigger_timestamp"]})[:24]
        reason_codes = [text_v1(code) for code in row.get("auto_closure_reason_codes") or [] if text_v1(code)]
        if not reason_codes:
            reason_codes = ["AUTO_CLOSURE_ALLOWED", row["exit_trigger"]]
        row["auto_closure_reason_codes"] = sorted(dict.fromkeys(reason_codes))
        source_artifacts = [text_v1(item) for item in row.get("source_artifacts") or [] if text_v1(item)]
        for item in [lineage.get("source_path"), *[text_v1(v) for v in position.get("closure_source_artifacts") or []], str(ledger_path)]:
            if text_v1(item):
                source_artifacts.append(text_v1(item))
        row["source_artifacts"] = [item for item in dict.fromkeys(source_artifacts) if item]
        source_hashes = dict(row.get("source_hashes")) if isinstance(row.get("source_hashes"), Mapping) else {}
        if text_v1(lineage.get("source_path")) and text_v1(lineage.get("source_hash")):
            source_hashes[text_v1(lineage.get("source_path"))] = text_v1(lineage.get("source_hash"))
        if isinstance(position.get("closure_source_hashes"), Mapping):
            for key, value in position.get("closure_source_hashes", {}).items():
                if text_v1(key) and text_v1(value):
                    source_hashes[text_v1(key)] = text_v1(value)
        source_hashes[str(ledger_path)] = _file_hash(ledger_path)
        row["source_hashes"] = source_hashes
        row["realized_return"] = row.get("realized_return") if row.get("realized_return") not in (None, "") else _round(_realized_return(row["entry_mark"], row["exit_mark"], position.get("side")))
        row["content_hash"] = stable_hash_v1({**row, "content_hash": ""})
        rows.append(row)
    return rows


def _is_persisted_auto_closure(position: Mapping[str, Any], receipt: Mapping[str, Any], lineage: Mapping[str, Any]) -> bool:
    if text_v1(lineage.get("source_family")) == REPORT_FAMILY:
        return text_v1(lineage.get("auto_closure_state")) == AUTO_CLOSED_PAPER_OUTCOME or text_v1(receipt.get("auto_closure_state")) == AUTO_CLOSED_PAPER_OUTCOME
    if text_v1(receipt.get("receipt_type")) == "DERIVED_FROM_AEGIS_PAPER_OUTCOME_AUTO_CLOSURE_V1":
        return text_v1(receipt.get("auto_closure_state")) == AUTO_CLOSED_PAPER_OUTCOME
    return False


def _recommendations_by_position(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in payload.get("recommendations") or payload.get("rows") or []:
        if not isinstance(row, Mapping):
            continue
        item = dict(row)
        position_id = text_v1(item.get("position_id"))
        candidate_id = text_v1(item.get("candidate_id"))
        if position_id:
            out[f"position:{position_id}"] = item
        if candidate_id:
            out[f"candidate:{candidate_id}"] = item
    return out


def _position_key(row: Mapping[str, Any]) -> str:
    return f"position:{text_v1(row.get('position_id'))}"


def _candidate_key(row: Mapping[str, Any]) -> str:
    return f"candidate:{text_v1(row.get('candidate_id'))}"


def _closure_row(position: Mapping[str, Any], rec: Mapping[str, Any], *, day_utc: str, ledger_path: Path, rec_path: Path, runtime_path: Path, runtime: Mapping[str, Any], future_guarded: bool = False) -> dict[str, Any]:
    entry = _num(position.get("entry_price") or rec.get("entry_price"))
    exit_mark = _num(position.get("current_certified_mark"), position.get("mark_price"), rec.get("current_mark"))
    recommendation = text_v1(rec.get("exit_recommendation") or "") or "MISSING_EXIT_EVALUATION"
    reason_codes = [text_v1(code) for code in rec.get("reason_codes") or [] if text_v1(code)]
    blockers: list[str] = []
    not_eligible: list[str] = []

    if future_guarded:
        not_eligible.append("TARGET_DAY_IN_FUTURE")
    if text_v1(position.get("paper_tracking_mode")).upper() == "AUTO_PROMOTED_RESEARCH_OBSERVATION" and text_v1(position.get("originating_day")) == day_utc:
        not_eligible.append("SAME_DAY_AUTO_PROMOTED_RESEARCH_OBSERVATION")
    if not rec:
        not_eligible.append("EXIT_EVALUATION_MISSING")
    elif recommendation == "HOLD":
        not_eligible.append("HOLD_RECOMMENDATION")
    elif recommendation not in EXIT_RECOMMENDATIONS:
        blockers.append("UNSUPPORTED_EXIT_RECOMMENDATION")
    if entry in (None, 0.0):
        blockers.append("MISSING_ENTRY_MARK")
    if exit_mark in (None, 0.0):
        blockers.append("MISSING_EXIT_MARK")
    if text_v1(position.get("mark_certification_status")).upper() != "CERTIFIED":
        blockers.append("EXIT_MARK_NOT_CERTIFIED")
    if text_v1(position.get("mark_freshness_status")).upper() not in {"CURRENT", "CERTIFIED", ""}:
        blockers.append("EXIT_MARK_NOT_CURRENT")
    if not text_v1(position.get("mark_timestamp_utc")):
        blockers.append("EXIT_MARK_TIMESTAMP_MISSING")
    if not text_v1(position.get("mark_source_path")):
        blockers.append("EXIT_SOURCE_ARTIFACT_MISSING")
    if not text_v1(position.get("mark_source_hash")):
        blockers.append("EXIT_SOURCE_HASH_MISSING")
    policy = rec.get("policy") if isinstance(rec.get("policy"), Mapping) else {}
    sleeve_id = text_v1(position.get("sleeve_id") or rec.get("sleeve_id"))
    policy_sleeve = text_v1(policy.get("sleeve_id"))
    if rec and (not policy_sleeve or (policy_sleeve == "DEFAULT" and sleeve_id != "DEFAULT")):
        blockers.append("POLICY_AUTHORITY_NOT_EXPLICIT")
    if rec and policy_sleeve and policy_sleeve not in {sleeve_id, "DEFAULT"}:
        blockers.append("POLICY_AUTHORITY_MISMATCH")
    if _runtime_safety_enabled(runtime):
        blockers.append("RUNTIME_SAFETY_FLAG_UNEXPECTEDLY_ENABLED")

    realized_return = _realized_return(entry, exit_mark, position.get("side")) if not blockers and not not_eligible else None
    if not_eligible:
        state = AUTO_CLOSURE_NOT_ELIGIBLE
        all_codes = not_eligible
    elif blockers:
        state = AUTO_CLOSURE_BLOCKED
        all_codes = blockers
    else:
        state = AUTO_CLOSED_PAPER_OUTCOME
        all_codes = ["AUTO_CLOSURE_ALLOWED", recommendation, *reason_codes]

    trigger_timestamp = text_v1(position.get("mark_timestamp_utc") or rec.get("generated_at_utc")) or f"{day_utc}T00:00:00Z"
    outcome_timestamp = trigger_timestamp if state == AUTO_CLOSED_PAPER_OUTCOME else ""
    source_artifacts = _source_artifacts(position, rec, ledger_path, rec_path, runtime_path)
    source_hashes = {path: _source_hash(path, position, rec, ledger_path, rec_path, runtime_path) for path in source_artifacts}
    row = {
        "position_id": text_v1(position.get("position_id")),
        "candidate_id": text_v1(position.get("candidate_id") or rec.get("candidate_id")),
        "symbol": text_v1(position.get("symbol") or rec.get("symbol")).upper(),
        "sleeve_id": sleeve_id,
        "hypothesis_id": text_v1(position.get("hypothesis_id") or _lineage(position).get("hypothesis_id")),
        "thesis_id": text_v1(position.get("thesis_id") or _lineage(position).get("thesis_id")),
        "auto_closure_state": state,
        "lifecycle_state": state,
        "auto_closure_reason_codes": sorted(dict.fromkeys(all_codes)),
        "future_target_day_guarded": future_guarded,
        "exit_recommendation": recommendation,
        "exit_trigger": ",".join(reason_codes) if reason_codes else recommendation,
        "trigger_timestamp": trigger_timestamp,
        "outcome_timestamp": outcome_timestamp,
        "entry_mark": entry,
        "entry_price_source_artifact": _entry_source_artifact(position, ledger_path),
        "entry_price_source_hash": _entry_source_hash(position, ledger_path),
        "exit_mark": exit_mark if state == AUTO_CLOSED_PAPER_OUTCOME else None,
        "exit_price_source_artifact": text_v1(position.get("mark_source_path")),
        "exit_price_source_hash": text_v1(position.get("mark_source_hash")),
        "exit_price_timestamp": text_v1(position.get("mark_timestamp_utc")),
        "exit_mark_certification_status": text_v1(position.get("mark_certification_status")),
        "realized_return": _round(realized_return),
        "deterministic_return_formula_version": RETURN_FORMULA_VERSION,
        "rerun_stability_key": stable_hash_v1({"position_id": position.get("position_id"), "entry": entry, "exit": exit_mark, "formula": RETURN_FORMULA_VERSION, "trigger_timestamp": trigger_timestamp}),
        "plain_english_reason": _plain_reason(position, recommendation, reason_codes),
        "source_artifacts": source_artifacts,
        "source_hashes": source_hashes,
        "safety": dict(SAFETY),
        **SAFETY,
    }
    row["closure_id"] = "paper-auto-closure:" + stable_hash_v1({"position_id": row["position_id"], "candidate_id": row["candidate_id"], "day_utc": day_utc, "recommendation": recommendation})[:24]
    row["content_hash"] = stable_hash_v1({**row, "content_hash": ""})
    return row


def _manual_review_row(row: Mapping[str, Any]) -> dict[str, Any]:
    symbol = text_v1(row.get("symbol"))
    trigger = text_v1(row.get("exit_trigger") or row.get("exit_recommendation"))
    return {
        "symbol": symbol,
        "sleeve": text_v1(row.get("sleeve_id")),
        "hypothesis": text_v1(row.get("hypothesis_id")),
        "paper_entry_price": row.get("entry_mark"),
        "paper_exit_price": row.get("exit_mark"),
        "paper_return": row.get("realized_return"),
        "exit_trigger": trigger,
        "exit_timestamp": text_v1(row.get("outcome_timestamp") or row.get("trigger_timestamp")),
        "plain_english_reason": f"{symbol} paper observation auto-closed by {trigger.replace('_', ' ').lower()} rule. Review manually only if this corresponds to a real-world position.",
        "not_trade_advice_statement": "This is not trade advice, not broker execution, and not an instruction to trade.",
        "position_id": text_v1(row.get("position_id")),
        "candidate_id": text_v1(row.get("candidate_id")),
        "paper_only": True,
        "trade_advice_allowed": False,
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "autonomous_execution_allowed": False,
        "live_trading_allowed": False,
    }


def _source_artifacts(position: Mapping[str, Any], rec: Mapping[str, Any], ledger_path: Path, rec_path: Path, runtime_path: Path) -> list[str]:
    values = [str(ledger_path), str(rec_path)]
    if runtime_path.exists():
        values.append(str(runtime_path))
    for value in [position.get("mark_source_path"), *_lineage(position).get("evidence_paths", []), *(rec.get("evidence_paths") or [])]:
        text = text_v1(value)
        if text:
            values.append(text)
    return [item for item in dict.fromkeys(values) if item]


def _source_hash(path: str, position: Mapping[str, Any], rec: Mapping[str, Any], ledger_path: Path, rec_path: Path, runtime_path: Path) -> str:
    if path == str(ledger_path):
        return _file_hash(path)
    if path == str(rec_path):
        return _file_hash(path)
    if path == str(runtime_path):
        return _file_hash(path)
    if path == text_v1(position.get("mark_source_path")):
        return text_v1(position.get("mark_source_hash")) or _file_hash(path)
    hashes = _lineage(position).get("evidence_hashes") if isinstance(_lineage(position).get("evidence_hashes"), Mapping) else {}
    if text_v1(hashes.get(path)):
        return text_v1(hashes.get(path))
    rec_hashes = rec.get("source_hashes") if isinstance(rec.get("source_hashes"), Mapping) else {}
    if text_v1(rec_hashes.get(path)):
        return text_v1(rec_hashes.get(path))
    return _file_hash(path)


def _entry_source_artifact(position: Mapping[str, Any], ledger_path: Path) -> str:
    lineage = _lineage(position)
    for path in lineage.get("evidence_paths") or []:
        if "aegis_candidate_contracts_v1" in text_v1(path) or "entry_reference_price_certification" in text_v1(path):
            return text_v1(path)
    return str(ledger_path)


def _entry_source_hash(position: Mapping[str, Any], ledger_path: Path) -> str:
    artifact = _entry_source_artifact(position, ledger_path)
    hashes = _lineage(position).get("evidence_hashes") if isinstance(_lineage(position).get("evidence_hashes"), Mapping) else {}
    return text_v1(hashes.get(artifact)) or _file_hash(artifact)


def _lineage(position: Mapping[str, Any]) -> Mapping[str, Any]:
    return position.get("candidate_lineage") if isinstance(position.get("candidate_lineage"), Mapping) else {}


def _runtime_safety_enabled(runtime: Mapping[str, Any]) -> bool:
    for key in ("trade_advice_allowed", "broker_execution_allowed", "broker_submit_transmit_allowed", "autonomous_execution_allowed", "live_trading_allowed"):
        if runtime.get(key) is True:
            return True
    return False


def _realized_return(entry: float | None, exit_mark: float | None, side: Any) -> float | None:
    if entry in (None, 0.0) or exit_mark is None:
        return None
    sign = -1 if text_v1(side).upper() in {"SELL", "SHORT"} else 1
    return ((exit_mark - entry) / entry) * sign


def _plain_reason(position: Mapping[str, Any], recommendation: str, reason_codes: list[str]) -> str:
    symbol = text_v1(position.get("symbol")).upper()
    reason = ", ".join(reason_codes) if reason_codes else recommendation
    return f"{symbol} paper observation auto-closed by {reason.replace('_', ' ').lower()} rule."


def _num(*values: Any) -> float | None:
    for value in values:
        if value in (None, "") or isinstance(value, bool):
            continue
        try:
            return float(str(value).replace(",", ""))
        except Exception:
            continue
    return None


def _round(value: float | None) -> float | None:
    return round(value, 8) if value is not None else None


def _file_hash(path: str | Path) -> str:
    try:
        return hashlib.sha256(Path(path).expanduser().read_bytes()).hexdigest()
    except OSError:
        return ""


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
