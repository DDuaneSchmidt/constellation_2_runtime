from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import now_utc_v1, write_json_v1

REPORT_FAMILY = "aegis_paper_lifecycle_reconciliation_v1"


def paper_lifecycle_reconciliation_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / REPORT_FAMILY / day_utc / "paper_lifecycle_reconciliation.v1.json"


def build_paper_lifecycle_reconciliation_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    days = _inspection_days(day_utc)
    queues = {day: _read_json(_path(root, "aegis_paper_review_queue_v1", day, "paper_review_queue.v1.json")) for day in days}
    decisions = {day: _read_jsonl(_path(root, "aegis_paper_review_queue_v1", day, "paper_review_decisions.v1.jsonl")) for day in days}
    receipts = {day: _safe_list(_read_json(_path(root, "aegis_paper_trade_receipts_v1", day, "paper_trade_receipts.v1.json")).get("receipts")) for day in days}
    events = {day: _read_jsonl(_path(root, "aegis_paper_position_events_v1", day, "paper_position_events.v1.jsonl")) for day in days}
    audits = {day: _read_jsonl(_path(root, "aegis_operator_command_audit_v1", day, "operator_command_audit.v1.jsonl")) for day in days}
    outcomes = {day: _read_json(_path(root, "aegis_paper_trade_outcomes_v1", day, "paper_trade_outcomes.v1.json")) for day in days}
    human_mode = {day: _read_json(_path(root, "aegis_mode_readiness_v1", day, "mode_readiness.v1.json")) for day in days}
    ledger = _read_json(_path(root, "aegis_paper_position_ledger_v1", day_utc, "paper_position_ledger.v1.json"))
    candidate_state = _read_json(_path(root, "aegis_candidate_state_v1", day_utc, "candidate_state.v1.json"))
    canonical = _read_json(_path(root, "aegis_canonical_operator_state_v1", day_utc, "canonical_operator_state.v1.json"))

    queue_rows_by_day = {day: _safe_list(queues[day].get("rows")) for day in days}
    all_queue_rows = [dict(row, source_day=day) for day in days for row in queue_rows_by_day[day] if isinstance(row, dict)]
    latest_queue_by_id: dict[str, dict[str, Any]] = {}
    for row in all_queue_rows:
        cid = str(row.get("candidate_id") or "")
        if cid:
            latest_queue_by_id[cid] = row

    state_by_id = {str(row.get("candidate_id") or ""): row for row in _safe_list(candidate_state.get("candidates")) if isinstance(row, dict)}
    receipt_rows = [_receipt_with_source(row, day, root) for day, rows in receipts.items() for row in rows if isinstance(row, dict)]
    entry_receipts = [row for row in receipt_rows if str(row.get("action") or "").upper() != "EXIT" and str(row.get("receipt_type") or "").upper() == "SIMULATED_PAPER"]
    receipt_by_id = _group_by_candidate(entry_receipts)
    event_rows = [_event_with_source(row, day, root) for day, rows in events.items() for row in rows if isinstance(row, dict)]
    open_events = [row for row in event_rows if str(row.get("event_type") or "").upper() == "PAPER_POSITION_OPENED"]
    event_by_id = _group_by_candidate(open_events)
    audit_rows = [_audit_with_source(row, day, root) for day, rows in audits.items() for row in rows if isinstance(row, dict) and str(row.get("command_id") or "") == "PAPER_TRADE_CANDIDATE"]
    attempts_by_id = _group_by_target(audit_rows)
    failed_attempts = [row for row in audit_rows if not _audit_succeeded(row)]
    succeeded_attempts = [row for row in audit_rows if _audit_succeeded(row)]
    failed_by_id = _group_by_target(failed_attempts)
    decisions_all = [_decision_with_source(row, day, root) for day, rows in decisions.items() for row in rows if isinstance(row, dict)]
    approvals = [row for row in decisions_all if str(row.get("decision") or "").upper() == "APPROVE"]
    rejections = [row for row in decisions_all if str(row.get("decision") or "").upper() == "REJECT"]
    approval_by_id = _group_by_candidate(approvals)
    open_positions = _safe_list(ledger.get("open_positions"))
    closed_positions = _safe_list(ledger.get("closed_positions"))
    open_by_id = {str(row.get("candidate_id") or ""): row for row in open_positions if isinstance(row, dict)}
    closed_by_id = {str(row.get("candidate_id") or ""): row for row in closed_positions if isinstance(row, dict)}

    candidate_rows = []
    for candidate_id in sorted(set(latest_queue_by_id) | set(state_by_id) | set(attempts_by_id) | set(receipt_by_id) | set(event_by_id)):
        queue_row = latest_queue_by_id.get(candidate_id, {})
        state_row = state_by_id.get(candidate_id, {})
        attempts = attempts_by_id.get(candidate_id, [])
        failures = failed_by_id.get(candidate_id, [])
        receipt_list = receipt_by_id.get(candidate_id, [])
        event_list = event_by_id.get(candidate_id, [])
        current_state = str(state_row.get("current_state") or queue_row.get("current_state") or queue_row.get("status") or "UNKNOWN")
        last_event = _last_event(candidate_id, queue_row, state_row, attempts, receipt_list, event_list, approval_by_id.get(candidate_id, []))
        visible = candidate_id in open_by_id
        missing_artifact, reason, repair_action = _visibility_reason(
            candidate_id=candidate_id,
            current_state=current_state,
            attempts=attempts,
            failures=failures,
            receipts=receipt_list,
            events=event_list,
            open_by_id=open_by_id,
            closed_by_id=closed_by_id,
            day_utc=day_utc,
        )
        candidate_rows.append({
            "symbol": str(queue_row.get("symbol") or state_row.get("symbol") or (receipt_list[-1].get("symbol") if receipt_list else "") or ""),
            "candidate_id": candidate_id,
            "source_day": str(queue_row.get("source_day") or state_row.get("source_queue_day") or ""),
            "originating_day": str(queue_row.get("originating_day") or state_row.get("originating_day") or ""),
            "current_lifecycle_state": current_state,
            "last_event": last_event,
            "paper_trade_attempt_count": len(attempts),
            "paper_trade_failed_count": len(failures),
            "receipt_count": len(receipt_list),
            "paper_position_opened_event_count": len(event_list),
            "open_ledger_position": visible,
            "missing_artifact_or_event": missing_artifact,
            "reason_not_visible_in_positions": reason,
            "repair_action": repair_action,
        })

    not_open = [row for row in candidate_rows if not row["open_ledger_position"]]
    mismatches = _ledger_mismatches(entry_receipts, open_by_id, closed_by_id, day_utc)
    failed_action_rows = [_failed_action_row(row, latest_queue_by_id, state_by_id, day_utc) for row in failed_attempts]
    awaiting_count = sum(1 for row in candidate_rows if row["current_lifecycle_state"] == "AWAITING_REVIEW")
    open_count = len(open_positions)
    operator_messages = []
    if awaiting_count > open_count:
        operator_messages.append(f"{awaiting_count} candidates still awaiting Paper Trade")
    if mismatches:
        operator_messages.append("PAPER_LEDGER_EVENT_MISSING")
    if failed_action_rows:
        operator_messages.append("PAPER_TRADE_ACTION_FAILED")

    per_day = {day: _day_counts(day, queue_rows_by_day[day], decisions[day], receipts[day], events[day], audits[day], outcomes[day]) for day in days}
    totals = _total_counts(candidate_rows, approvals, audit_rows, succeeded_attempts, failed_attempts, entry_receipts, open_events, open_positions, rejections)
    payload = {
        "schema_id": "aegis_paper_lifecycle_reconciliation",
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day_utc,
        "inspection_days": days,
        "generated_at_utc": now_utc_v1(),
        "operator_summary_messages": operator_messages,
        "counts": totals,
        "per_day_counts": per_day,
        "candidate_reconciliation_rows": candidate_rows,
        "expected_open_but_missing": not_open,
        "receipt_without_ledger_event": mismatches,
        "failed_actions": failed_action_rows,
        "source_artifact_paths": _source_paths(root, days, day_utc),
        "human_reviewed_paper_mode_artifacts": {day: {"available": bool(human_mode[day]), "path": str(_path(root, "aegis_mode_readiness_v1", day, "mode_readiness.v1.json")) if _path(root, "aegis_mode_readiness_v1", day, "mode_readiness.v1.json").exists() else ""} for day in days},
        "canonical_operator_state_summary": {
            "path": str(_path(root, "aegis_canonical_operator_state_v1", day_utc, "canonical_operator_state.v1.json")),
            "awaiting_review_count": (canonical.get("candidate_ui_projection") or {}).get("awaiting_review_count"),
            "paper_position_open_count": (canonical.get("candidate_ui_projection") or {}).get("paper_position_open_count"),
        },
        "safety": {
            "paper_only": True,
            "broker_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
            "autonomous_execution_allowed": False,
            "trade_advice_allowed": False,
            "live_trading_allowed": False,
        },
    }
    return payload


def write_paper_lifecycle_reconciliation_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(paper_lifecycle_reconciliation_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_paper_lifecycle_reconciliation_v1(truth_root=truth_root, day_utc=day_utc))


def _inspection_days(day_utc: str) -> list[str]:
    try:
        day = datetime.strptime(day_utc, "%Y-%m-%d").replace(tzinfo=UTC)
        prev = (day - timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        prev = day_utc
    return sorted({prev, day_utc})


def _path(root: Path, family: str, day: str, filename: str) -> Path:
    return root / "reports" / family / day / filename


def _source_paths(root: Path, days: list[str], day_utc: str) -> dict[str, Any]:
    families = {
        "paper_review_queue": ("aegis_paper_review_queue_v1", "paper_review_queue.v1.json"),
        "paper_review_decisions": ("aegis_paper_review_queue_v1", "paper_review_decisions.v1.jsonl"),
        "paper_position_events": ("aegis_paper_position_events_v1", "paper_position_events.v1.jsonl"),
        "paper_trade_receipts": ("aegis_paper_trade_receipts_v1", "paper_trade_receipts.v1.json"),
        "paper_trade_outcomes": ("aegis_paper_trade_outcomes_v1", "paper_trade_outcomes.v1.json"),
        "candidate_state": ("aegis_candidate_state_v1", "candidate_state.v1.json"),
        "canonical_operator_state": ("aegis_canonical_operator_state_v1", "canonical_operator_state.v1.json"),
        "operator_command_audit": ("aegis_operator_command_audit_v1", "operator_command_audit.v1.jsonl"),
    }
    out: dict[str, Any] = {}
    for key, (family, filename) in families.items():
        selected_days = [day_utc] if key in {"candidate_state", "canonical_operator_state"} else days
        out[key] = {day: str(_path(root, family, day, filename)) for day in selected_days if _path(root, family, day, filename).exists()}
    return out


def _day_counts(day: str, queue_rows: list[Any], decisions: list[dict[str, Any]], receipts: list[Any], events: list[dict[str, Any]], audits: list[dict[str, Any]], outcomes: dict[str, Any]) -> dict[str, Any]:
    queue = [row for row in queue_rows if isinstance(row, dict)]
    status = Counter(str(row.get("status") or row.get("current_state") or "UNKNOWN") for row in queue)
    paper_audits = [row for row in audits if isinstance(row, dict) and str(row.get("command_id") or "") == "PAPER_TRADE_CANDIDATE"]
    return {
        "total_candidates": len(queue),
        "awaiting_review": status.get("AWAITING_REVIEW", 0),
        "approved_for_paper": status.get("APPROVED_FOR_PAPER", 0),
        "paper_trade_attempted": len(paper_audits),
        "paper_trade_succeeded": sum(1 for row in paper_audits if _audit_succeeded(row)),
        "receipt_created": sum(1 for row in receipts if isinstance(row, dict) and str(row.get("receipt_type") or "").upper() == "SIMULATED_PAPER" and str(row.get("action") or "").upper() != "EXIT"),
        "paper_position_opened_events": sum(1 for row in events if str(row.get("event_type") or "").upper() == "PAPER_POSITION_OPENED"),
        "open_ledger_positions": len(_safe_list(outcomes.get("open_trades"))),
        "failed_actions": sum(1 for row in paper_audits if not _audit_succeeded(row)),
        "rejected_candidates": status.get("REJECTED_BY_OPERATOR", 0) + sum(1 for row in decisions if str(row.get("decision") or "").upper() == "REJECT"),
        "expired_candidates": status.get("EXPIRED", 0),
    }


def _total_counts(candidate_rows: list[dict[str, Any]], approvals: list[dict[str, Any]], audits: list[dict[str, Any]], succeeded: list[dict[str, Any]], failed: list[dict[str, Any]], receipts: list[dict[str, Any]], open_events: list[dict[str, Any]], open_positions: list[Any], rejections: list[dict[str, Any]]) -> dict[str, int]:
    states = Counter(row["current_lifecycle_state"] for row in candidate_rows)
    return {
        "total_candidates": len(candidate_rows),
        "awaiting_review": states.get("AWAITING_REVIEW", 0),
        "approved_for_paper": len({str(row.get("candidate_id") or "") for row in approvals}),
        "paper_trade_attempted": len(audits),
        "paper_trade_succeeded": len(succeeded),
        "receipt_created": len(receipts),
        "paper_position_opened_events": len(open_events),
        "open_ledger_positions": len(open_positions),
        "failed_actions": len(failed),
        "rejected_candidates": len(rejections) + states.get("REJECTED_BY_OPERATOR", 0),
        "expired_candidates": states.get("EXPIRED", 0),
    }


def _visibility_reason(*, candidate_id: str, current_state: str, attempts: list[dict[str, Any]], failures: list[dict[str, Any]], receipts: list[dict[str, Any]], events: list[dict[str, Any]], open_by_id: dict[str, Any], closed_by_id: dict[str, Any], day_utc: str) -> tuple[str, str, str]:
    if candidate_id in open_by_id:
        return "", "Visible in Positions from paper_position_ledger.open_positions.", ""
    if candidate_id in closed_by_id:
        return "", "Closed paper position; visible in History, not Open Positions.", ""
    if receipts and not events:
        return "PAPER_POSITION_OPENED", "SIMULATED_PAPER receipt exists but no PAPER_POSITION_OPENED event was found.", f"TARGET_DAY={day_utc} npm run aegis:paper-position-ledger"
    if failures:
        return "SIMULATED_PAPER_RECEIPT", str(failures[-1].get("error") or "PAPER_TRADE_ACTION_FAILED"), "Approve the candidate if still desired, then rerun Paper Trade."
    if attempts and not receipts:
        return "SIMULATED_PAPER_RECEIPT", "Paper Trade was attempted but no SIMULATED_PAPER receipt exists.", "Review failed action details and retry Paper Trade."
    if current_state == "AWAITING_REVIEW":
        return "SIMULATED_PAPER_RECEIPT", "Candidate is still awaiting Paper Trade; no receipt or ledger event should exist yet.", "Use Paper Trade if the operator still wants to open a simulated paper position."
    if current_state == "APPROVED_FOR_PAPER":
        return "SIMULATED_PAPER_RECEIPT", "Candidate is approved but no Paper Trade receipt has been recorded.", "Use Paper Trade to create the simulated receipt and ledger event."
    return "", f"Candidate state is {current_state}; it is not an open ledger position.", ""


def _ledger_mismatches(receipts: list[dict[str, Any]], open_by_id: dict[str, Any], closed_by_id: dict[str, Any], day_utc: str) -> list[dict[str, Any]]:
    rows = []
    for receipt in receipts:
        cid = str(receipt.get("candidate_id") or "")
        if cid and cid not in open_by_id and cid not in closed_by_id:
            rows.append({
                "status": "PAPER_LEDGER_EVENT_MISSING",
                "candidate_id": cid,
                "symbol": str(receipt.get("symbol") or ""),
                "receipt_path": str(receipt.get("receipt_path") or ""),
                "missing_event": "PAPER_POSITION_OPENED",
                "repair_action": f"TARGET_DAY={day_utc} npm run aegis:paper-position-ledger",
            })
    return rows


def _failed_action_row(row: dict[str, Any], queue: dict[str, dict[str, Any]], state: dict[str, dict[str, Any]], day_utc: str) -> dict[str, Any]:
    cid = str(row.get("target_id") or (row.get("request_payload") or {}).get("target_id") or "")
    q = queue.get(cid, {})
    s = state.get(cid, {})
    return {
        "status": "PAPER_TRADE_ACTION_FAILED",
        "candidate_id": cid,
        "symbol": str(q.get("symbol") or s.get("symbol") or ""),
        "current_lifecycle_state": str(s.get("current_state") or q.get("status") or "UNKNOWN"),
        "error": str(row.get("error") or ((row.get("response_summary") or {}).get("user_message")) or "FAILED"),
        "audit_path": str(row.get("audit_path") or ""),
        "requested_at": str(row.get("requested_at") or ""),
        "repair_action": "Refresh the queue projection, confirm the candidate reaches APPROVED_FOR_PAPER, then retry Paper Trade.",
    }


def _last_event(candidate_id: str, queue_row: dict[str, Any], state_row: dict[str, Any], attempts: list[dict[str, Any]], receipts: list[dict[str, Any]], events: list[dict[str, Any]], approvals: list[dict[str, Any]]) -> dict[str, str]:
    candidates = []
    for kind, rows, ts_key in [
        ("PAPER_TRADE_ATTEMPT", attempts, "requested_at"),
        ("SIMULATED_PAPER_RECEIPT", receipts, "timestamp_utc"),
        ("PAPER_POSITION_OPENED", events, "event_time_utc"),
        ("PAPER_REVIEW_DECISION_RECORDED", approvals, "timestamp_utc"),
    ]:
        for row in rows:
            candidates.append((str(row.get(ts_key) or row.get("timestamp") or ""), kind, row))
    if queue_row:
        candidates.append((str(queue_row.get("timestamp") or ""), str(queue_row.get("status") or "QUEUE_ROW"), queue_row))
    if not candidates:
        return {"event_type": "UNKNOWN", "timestamp_utc": "", "source_path": ""}
    ts, kind, row = sorted(candidates, key=lambda item: item[0])[-1]
    return {"event_type": kind, "timestamp_utc": ts, "source_path": str(row.get("audit_path") or row.get("receipt_path") or row.get("event_path") or row.get("decision_path") or "")}


def _audit_succeeded(row: dict[str, Any]) -> bool:
    summary = row.get("response_summary") if isinstance(row.get("response_summary"), dict) else {}
    return bool(summary.get("ok") is True or str(row.get("result") or "").upper() == "SIMULATED_PAPER_RECEIPT_RECORDED")


def _group_by_candidate(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        cid = str(row.get("candidate_id") or row.get("linked_candidate_id") or "")
        if cid:
            out[cid].append(row)
    return dict(out)


def _group_by_target(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        cid = str(row.get("target_id") or (row.get("request_payload") or {}).get("target_id") or "")
        if cid:
            out[cid].append(row)
    return dict(out)


def _receipt_with_source(row: dict[str, Any], day: str, root: Path) -> dict[str, Any]:
    return {**row, "receipt_day": day, "receipt_path": str(_path(root, "aegis_paper_trade_receipts_v1", day, "paper_trade_receipts.v1.json"))}


def _event_with_source(row: dict[str, Any], day: str, root: Path) -> dict[str, Any]:
    return {**row, "event_day": day, "event_path": str(_path(root, "aegis_paper_position_events_v1", day, "paper_position_events.v1.jsonl"))}


def _audit_with_source(row: dict[str, Any], day: str, root: Path) -> dict[str, Any]:
    return {**row, "audit_day": day, "audit_path": str(_path(root, "aegis_operator_command_audit_v1", day, "operator_command_audit.v1.jsonl"))}


def _decision_with_source(row: dict[str, Any], day: str, root: Path) -> dict[str, Any]:
    return {**row, "decision_day": day, "decision_path": str(_path(root, "aegis_paper_review_queue_v1", day, "paper_review_decisions.v1.jsonl"))}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except Exception:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _safe_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []
