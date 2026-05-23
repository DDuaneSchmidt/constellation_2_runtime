from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.candidate_lifecycle_v1 import build_candidate_lifecycle_v1
from ops.aegis.intelligence_common_v1 import write_json_v1


REPORT_FAMILY = "aegis_candidate_review_ledger_v1"
FILTERS = {"active", "watchlisted", "dismissed", "needs_more_evidence", "expired", "all"}
ACTIVE_STATES = {"REVIEW_REQUIRED", "WATCHLISTED", "NEEDS_MORE_EVIDENCE"}
HISTORICAL_STATES = {"DISMISSED", "EXPIRED"}


def build_candidate_review_ledger_v1(*, truth_root: Path, day_utc: str, filter_name: str = "all") -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    normalized_filter = normalize_filter_v1(filter_name)
    lifecycle = build_candidate_lifecycle_v1(truth_root=root, day_utc=day_utc)
    rows = [_ledger_row(row) for row in lifecycle.get("candidates", []) if _is_review_candidate(row)]
    rows = sorted(rows, key=lambda row: (str(row.get("generated_at") or ""), str(row.get("candidate_id") or "")))
    filtered = filter_candidate_review_rows_v1(rows, normalized_filter)
    return {
        "schema_id": "aegis_candidate_review_ledger",
        "schema_version": "v1",
        "artifact_id": "aegis_candidate_review_ledger_v1",
        "day_utc": day_utc,
        "generated_at_utc": _now(),
        "filter": normalized_filter,
        "filter_options": sorted(FILTERS),
        "candidate_count": len(rows),
        "filtered_candidate_count": len(filtered),
        "active_count": len(filter_candidate_review_rows_v1(rows, "active")),
        "watchlisted_count": len(filter_candidate_review_rows_v1(rows, "watchlisted")),
        "dismissed_count": len(filter_candidate_review_rows_v1(rows, "dismissed")),
        "needs_more_evidence_count": len(filter_candidate_review_rows_v1(rows, "needs_more_evidence")),
        "expired_count": len(filter_candidate_review_rows_v1(rows, "expired")),
        "expired_watchlisted_count": sum(1 for row in rows if str(row.get("current_review_state") or "").upper() == "EXPIRED" and str(row.get("expired_from_review_state") or "").upper() == "WATCHLISTED"),
        "candidates": rows,
        "filtered_candidates": filtered,
        "source_artifacts": [str(lifecycle.get("candidate_reviews_path") or ""), str(lifecycle.get("candidate_decisions_path") or "")],
        "read_model": "candidate_lifecycle.v1.json + candidate_reviews.v1.jsonl",
        "allowed_review_actions": ["watchlist", "dismiss", "needs-more-evidence", "add-note"],
        "forbidden_actions": ["approve", "approve-to-trade", "execute", "route", "order", "allocate", "broker", "autonomous"],
        "safety": _safety(),
    }


def write_candidate_review_ledger_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "candidate_review_ledger.v1.json", payload)
    summary_path = out_dir / "candidate_review_ledger.summary.txt"
    matrix_path = out_dir / "candidate_review_ledger.matrix.csv"
    summary_path.write_text(render_candidate_review_ledger_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_candidate_review_ledger_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def filter_candidate_review_ledger_v1(payload: dict[str, Any], filter_name: str) -> dict[str, Any]:
    normalized = normalize_filter_v1(filter_name)
    rows = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    filtered = filter_candidate_review_rows_v1([row for row in rows if isinstance(row, dict)], normalized)
    return {
        **payload,
        "filter": normalized,
        "filtered_candidate_count": len(filtered),
        "filtered_candidates": filtered,
    }


def filter_candidate_review_rows_v1(rows: list[dict[str, Any]], filter_name: str) -> list[dict[str, Any]]:
    normalized = normalize_filter_v1(filter_name)
    if normalized == "all":
        return list(rows)
    if normalized == "active":
        return [row for row in rows if bool(row.get("active"))]
    wanted = {
        "watchlisted": "WATCHLISTED",
        "dismissed": "DISMISSED",
        "needs_more_evidence": "NEEDS_MORE_EVIDENCE",
        "expired": "EXPIRED",
    }[normalized]
    return [row for row in rows if str(row.get("current_review_state") or "").upper() == wanted]


def normalize_filter_v1(filter_name: str) -> str:
    normalized = str(filter_name or "all").strip().lower().replace("-", "_")
    if normalized not in FILTERS:
        return "all"
    return normalized


def render_candidate_review_ledger_summary_v1(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "AEGIS CANDIDATE REVIEW LEDGER v1",
            f"day_utc: {payload.get('day_utc')}",
            f"candidate_count: {payload.get('candidate_count')}",
            f"active_count: {payload.get('active_count')}",
            f"watchlisted_count: {payload.get('watchlisted_count')}",
            f"dismissed_count: {payload.get('dismissed_count')}",
            f"needs_more_evidence_count: {payload.get('needs_more_evidence_count')}",
            f"expired_count: {payload.get('expired_count')}",
            f"expired_watchlisted_count: {payload.get('expired_watchlisted_count')}",
            "allowed_review_actions: watchlist, dismiss, needs-more-evidence, add-note",
            "broker_execution_allowed: false",
            "autonomous_execution_allowed: false",
            "automatic_approval_allowed: false",
            "",
        ]
    )


def render_candidate_review_ledger_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    writer = csv.DictWriter(
        out,
        fieldnames=[
            "candidate_id",
            "symbol",
            "sleeve_id",
            "direction",
            "generated_at",
            "current_review_state",
            "active",
            "expiry",
            "expired_from_review_state",
            "latest_operator_note",
            "audit_action_count",
            "executable_status",
        ],
    )
    writer.writeheader()
    for row in payload.get("candidates") or []:
        if not isinstance(row, dict):
            continue
        writer.writerow(
            {
                "candidate_id": row.get("candidate_id", ""),
                "symbol": row.get("symbol", ""),
                "sleeve_id": row.get("sleeve_id", ""),
                "direction": row.get("direction", ""),
                "generated_at": row.get("generated_at", ""),
                "current_review_state": row.get("current_review_state", ""),
                "active": row.get("active", False),
                "expiry": row.get("expiry", ""),
                "expired_from_review_state": row.get("expired_from_review_state", ""),
                "latest_operator_note": row.get("latest_operator_note", ""),
                "audit_action_count": row.get("audit_action_count", 0),
                "executable_status": row.get("no_execution_status", {}).get("executable_status", "NON_EXECUTABLE"),
            }
        )
    return out.getvalue()


def _ledger_row(row: dict[str, Any]) -> dict[str, Any]:
    state = str(row.get("review_state") or row.get("operator_review_status") or "REVIEW_REQUIRED").upper()
    active = state in ACTIVE_STATES and not bool(row.get("review_expired", False))
    if state in HISTORICAL_STATES:
        active = False
    history = row.get("review_action_history") if isinstance(row.get("review_action_history"), list) else []
    return {
        "candidate_id": str(row.get("candidate_id") or ""),
        "symbol": str(row.get("symbol") or ""),
        "sleeve_id": str(row.get("sleeve_id") or ""),
        "direction": str(row.get("direction") or ""),
        "generated_at": str(row.get("generated_at") or ""),
        "current_review_state": state,
        "expiry": str(row.get("review_expires_at") or ""),
        "active": bool(active),
        "expired": state == "EXPIRED" or bool(row.get("review_expired", False)),
        "expired_from_review_state": str(row.get("expired_from_review_state") or ""),
        "latest_operator_note": str(row.get("latest_operator_note") or row.get("current_operator_note") or ""),
        "latest_reviewed_at": str(row.get("latest_reviewed_at") or ""),
        "latest_review_source": str(row.get("latest_review_source") or ""),
        "audit_action_count": int(row.get("review_action_history_count") or len(history)),
        "audit_action_history": [_audit_event(event) for event in history if isinstance(event, dict)],
        "promotion_status": str(row.get("promotion_status") or ""),
        "review_only": bool(row.get("review_only", True)),
        "human_review_required": bool(row.get("human_review_required", True)),
        "no_execution_status": {
            "executable_status": str(row.get("executable_status") or "NON_EXECUTABLE"),
            "review_only": bool(row.get("review_only", True)),
            "human_review_required": bool(row.get("human_review_required", True)),
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_approval_allowed": False,
            "broker_submit_transmit_allowed": False,
            "order_routing_allowed": False,
            "allocation_action_allowed": False,
        },
        "source_artifact": str((row.get("evidence_artifacts") or [""])[0] if isinstance(row.get("evidence_artifacts"), list) else ""),
    }


def _audit_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "review_event_id": str(event.get("review_event_id") or ""),
        "candidate_id": str(event.get("candidate_id") or ""),
        "action": str(event.get("action") or ""),
        "prior_state": str(event.get("prior_state") or ""),
        "new_state": str(event.get("new_state") or ""),
        "operator_note": str(event.get("operator_note") or ""),
        "operator": str(event.get("operator") or ""),
        "timestamp_utc": str(event.get("timestamp_utc") or event.get("timestamp") or ""),
        "source": str(event.get("source") or ""),
        "review_only": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_approval_allowed": False,
    }


def _is_review_candidate(row: dict[str, Any]) -> bool:
    return bool(row.get("review_only")) or bool(row.get("promotion_status")) or bool(row.get("promotion_contract"))


def _safety() -> dict[str, bool]:
    return {
        "review_only": True,
        "human_review_required": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "live_trading_allowed": False,
        "automatic_approval_allowed": False,
        "broker_submit_transmit_allowed": False,
        "order_routing_allowed": False,
        "allocation_action_allowed": False,
        "fake_data_created": False,
        "fake_candidates_created": False,
        "sleeve_mutation_allowed": False,
    }


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
