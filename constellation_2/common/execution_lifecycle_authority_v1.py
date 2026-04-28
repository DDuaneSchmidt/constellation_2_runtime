from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SCHEMA_ID = "C2_EXECUTION_LIFECYCLE_AUTHORITY_V1"
SCHEMA_VERSION = 1

STATES = {
    "NO_SUBMISSION",
    "DRY_RUN_COMPLETE",
    "SUBMITTED_PENDING_ACK",
    "ACKNOWLEDGED_OPEN",
    "PARTIALLY_FILLED",
    "FILLED",
    "CANCELED",
    "REJECTED",
    "FAILED",
    "RECONCILED",
    "LINEAGE_GAP",
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _sha256_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _coerce_positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _coerce_nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value >= 0:
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _submission_dirs(submissions_day_dir: Path) -> list[Path]:
    if not submissions_day_dir.exists() or not submissions_day_dir.is_dir():
        return []
    return sorted(path.resolve() for path in submissions_day_dir.iterdir() if path.is_dir() and not path.name.startswith("_"))


def _find_order_plan(submission_dir: Path) -> tuple[dict[str, Any] | None, Path | None]:
    for pattern in ("equity_order_plan.v2.json", "equity_order_plan.v1.json", "order_plan.v1.json", "*order_plan*.json"):
        for path in sorted(submission_dir.glob(pattern)):
            payload = _read_json(path.resolve())
            if isinstance(payload, dict):
                return payload, path.resolve()
    return None, None


def _broker_ids(broker_record: dict[str, Any] | None) -> tuple[int | None, int | None]:
    ids = broker_record.get("broker_ids") if isinstance(broker_record, dict) and isinstance(broker_record.get("broker_ids"), dict) else {}
    return _coerce_positive_int(ids.get("order_id")), _coerce_positive_int(ids.get("perm_id"))


def _dry_run_evidence(
    *,
    broker_record: dict[str, Any] | None,
    submit_attempt: dict[str, Any] | None,
) -> bool:
    if isinstance(submit_attempt, dict) and submit_attempt.get("dry_run") is True:
        return True
    error = broker_record.get("error") if isinstance(broker_record, dict) and isinstance(broker_record.get("error"), dict) else {}
    return str(error.get("code") or "").strip().upper() == "DRY_RUN_NO_BROKER_ID"


def _broker_transmit_expected(
    *,
    broker_record: dict[str, Any] | None,
    submit_attempt: dict[str, Any] | None,
    dry_run: bool,
    order_id: int | None,
    perm_id: int | None,
) -> bool:
    if dry_run:
        return False
    if isinstance(submit_attempt, dict) and submit_attempt.get("dry_run") is False:
        return True
    if isinstance(broker_record, dict) and broker_record.get("broker_transmitted") is False:
        return False
    if isinstance(broker_record, dict) and broker_record.get("broker_transmitted") is True:
        return True
    return bool(order_id is not None or perm_id is not None or broker_record is not None)


def _fill_status(fill_ledger: dict[str, Any] | None, *, simulated: bool, broker_transmit_expected: bool) -> tuple[str, int | None, int | None]:
    if simulated or not broker_transmit_expected:
        return "NOT_EXPECTED", None, None
    if not isinstance(fill_ledger, dict):
        return "MISSING", None, None

    order_qty = _coerce_nonnegative_int(fill_ledger.get("order_qty"))
    filled_qty = _coerce_nonnegative_int(fill_ledger.get("filled_qty"))
    lifecycle = str(fill_ledger.get("lifecycle_status") or "").strip().upper()
    if lifecycle == "FILLED":
        return "COMPLETE", filled_qty, order_qty
    if lifecycle == "PARTIALLY_FILLED":
        return "PARTIAL", filled_qty, order_qty
    if filled_qty is not None and order_qty is not None and order_qty > 0:
        if filled_qty >= order_qty:
            return "COMPLETE", filled_qty, order_qty
        if filled_qty > 0:
            return "PARTIAL", filled_qty, order_qty
    return "MISSING", filled_qty, order_qty


def _reconciliation_complete(reconciliation: dict[str, Any] | None) -> bool:
    if not isinstance(reconciliation, dict):
        return False
    status = str(reconciliation.get("status") or "").strip().upper()
    semantic = str(reconciliation.get("semantic_status") or "").strip().upper()
    return status in {"PASS", "OK"} and semantic not in {"MATERIALIZED_FAILURE", "BLOCKED_BY_UPSTREAM_PREREQUISITE"}


def _input_status(path: Path, artifact_type: str) -> dict[str, Any]:
    return {
        "artifact_type": artifact_type,
        "path": str(path.resolve()),
        "exists": bool(path.exists()),
    }


def execution_lifecycle_authority_output_path(*, execution_root: Path, day_utc: str) -> Path:
    return (
        Path(execution_root).resolve()
        / "reports"
        / "execution_lifecycle_authority_v1"
        / day_utc
        / "execution_lifecycle_authority.v1.json"
    ).resolve()


def _projection_consistency(
    *,
    submission_rows: list[dict[str, Any]],
    submission_index: dict[str, Any] | None,
    current_head: dict[str, Any] | None,
    submission_index_path: Path,
    current_head_path: Path,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    submission_ids = {str(row.get("submission_id") or "") for row in submission_rows}
    authority_order_ids = {
        str(row.get("submission_id") or ""): row.get("broker_order_id")
        for row in submission_rows
    }

    if isinstance(current_head, dict):
        selected = str(current_head.get("selected_attempt_id") or "").strip()
        ok = (not selected) or selected in submission_ids
        rows.append(
            {
                "projection": "execution_evidence_v1/current_head",
                "path": str(current_head_path),
                "status": "AGREE" if ok else "CONFLICT",
                "reason_code": "" if ok else "CURRENT_HEAD_SELECTED_ATTEMPT_NOT_IN_AUTHORITY",
            }
        )

    if isinstance(submission_index, dict):
        conflicts: list[str] = []
        attempts = submission_index.get("attempts") if isinstance(submission_index.get("attempts"), list) else []
        for attempt in attempts:
            if not isinstance(attempt, dict):
                continue
            sid = str(attempt.get("attempt_id") or "").strip()
            indexed_order_id = attempt.get("broker_order_id")
            if sid in authority_order_ids and indexed_order_id not in {None, authority_order_ids[sid]}:
                conflicts.append(sid)
        rows.append(
            {
                "projection": "submission_index_v1",
                "path": str(submission_index_path),
                "status": "AGREE" if not conflicts else "CONFLICT",
                "reason_code": "" if not conflicts else "SUBMISSION_INDEX_BROKER_ID_CONFLICT",
            }
        )
    return rows


def _state_for_submission(
    *,
    broker_record: dict[str, Any] | None,
    submit_attempt: dict[str, Any] | None,
    fill_ledger: dict[str, Any] | None,
    reconciliation_complete: bool,
    broker_record_path: Path,
) -> tuple[str, dict[str, Any]]:
    order_id, perm_id = _broker_ids(broker_record)
    dry_run = _dry_run_evidence(broker_record=broker_record, submit_attempt=submit_attempt)
    transmit_expected = _broker_transmit_expected(
        broker_record=broker_record,
        submit_attempt=submit_attempt,
        dry_run=dry_run,
        order_id=order_id,
        perm_id=perm_id,
    )
    broker_status = str((broker_record or {}).get("status") or "").strip().upper()
    fill_status, filled_qty, order_qty = _fill_status(
        fill_ledger,
        simulated=dry_run,
        broker_transmit_expected=transmit_expected,
    )

    first_blocker = ""
    missing_ids_expected_diagnostic = False
    if dry_run:
        state = "DRY_RUN_COMPLETE"
        missing_ids_expected_diagnostic = bool(order_id is None or perm_id is None)
    elif not isinstance(broker_record, dict):
        state = "LINEAGE_GAP"
        first_blocker = "BROKER_SUBMISSION_RECORD_MISSING"
    elif broker_status in {"REJECTED", "INACTIVE"}:
        state = "REJECTED" if broker_status == "REJECTED" else "FAILED"
        first_blocker = str(((broker_record.get("error") or {}) if isinstance(broker_record.get("error"), dict) else {}).get("code") or "")
    elif broker_status in {"CANCELLED", "CANCELED"}:
        state = "CANCELED"
    elif transmit_expected and (order_id is None or perm_id is None):
        state = "LINEAGE_GAP"
        first_blocker = "BROKER_ORDER_ID_MISSING" if order_id is None else "BROKER_PERM_ID_MISSING"
    elif fill_status == "COMPLETE":
        state = "RECONCILED" if reconciliation_complete else "FILLED"
    elif fill_status == "PARTIAL":
        state = "PARTIALLY_FILLED"
    elif order_id is not None and perm_id is not None:
        state = "ACKNOWLEDGED_OPEN"
    elif transmit_expected:
        state = "SUBMITTED_PENDING_ACK"
    else:
        state = "FAILED"
        first_blocker = "BROKER_TRANSMIT_EXPECTATION_UNKNOWN"

    if reconciliation_complete and state in {"ACKNOWLEDGED_OPEN", "PARTIALLY_FILLED", "FILLED"}:
        state = "RECONCILED"

    diagnostics = {
        "was_simulated": dry_run,
        "was_transmitted": bool(not dry_run and (order_id is not None or perm_id is not None)),
        "broker_transmit_expected": transmit_expected,
        "broker_order_id_assigned": order_id is not None,
        "broker_perm_id_assigned": perm_id is not None,
        "broker_order_id": order_id,
        "broker_perm_id": perm_id,
        "missing_broker_ids_expected_diagnostic": missing_ids_expected_diagnostic,
        "missing_broker_ids_failure": bool(transmit_expected and (order_id is None or perm_id is None)),
        "fills_status": fill_status,
        "filled_qty": filled_qty,
        "order_qty": order_qty,
        "reconciliation_complete": reconciliation_complete,
        "first_blocker_or_gap": first_blocker,
        "broker_submission_record_path": str(broker_record_path),
    }
    return state, diagnostics


def _rollup_state(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "NO_SUBMISSION"
    states = [str(row.get("current_lifecycle_state") or "") for row in rows]
    for state in ("LINEAGE_GAP", "FAILED", "REJECTED", "CANCELED"):
        if state in states:
            return state
    if all(state == "RECONCILED" for state in states):
        return "RECONCILED"
    if "PARTIALLY_FILLED" in states:
        return "PARTIALLY_FILLED"
    if all(state in {"FILLED", "RECONCILED"} for state in states):
        return "FILLED"
    if all(state == "DRY_RUN_COMPLETE" for state in states):
        return "DRY_RUN_COMPLETE"
    if "ACKNOWLEDGED_OPEN" in states:
        return "ACKNOWLEDGED_OPEN"
    if "SUBMITTED_PENDING_ACK" in states:
        return "SUBMITTED_PENDING_ACK"
    return states[0] if states[0] in STATES else "FAILED"


def evaluate_execution_lifecycle_authority_v1(
    *,
    day_utc: str,
    execution_root: Path,
    canonical_truth_root: Path | None = None,
    sleeve: str = "PRIMARY",
    environment: str = "PAPER",
    produced_utc: str | None = None,
) -> dict[str, Any]:
    execution_root = Path(execution_root).resolve()
    canonical_root = Path(canonical_truth_root).resolve() if canonical_truth_root is not None else execution_root
    submissions_day = (execution_root / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    fill_day = (execution_root / "fill_ledger_v1" / day_utc).resolve()
    submission_index_path = (execution_root / "submission_index_v1" / day_utc / "submission_index.v1.json").resolve()
    current_head_path = (execution_root / "execution_evidence_v1" / "current_head" / day_utc / "current_head.v1.json").resolve()
    reconciliation_path = (execution_root / "reports" / "execution_reconciliation_v1" / day_utc / "execution_reconciliation.v1.json").resolve()
    canonical_reconciliation_path = (canonical_root / "reports" / "execution_reconciliation_v1" / day_utc / "execution_reconciliation.v1.json").resolve()
    if not reconciliation_path.exists() and canonical_reconciliation_path.exists():
        reconciliation_path = canonical_reconciliation_path
    submit_decision_root = (canonical_root / "reports" / "submit_decision_trace_v1" / day_utc).resolve()
    attribution_path = (
        canonical_root
        / "reports"
        / "sleeve_intent_trade_attribution_v1"
        / day_utc
        / "sleeve_intent_trade_attribution.v1.json"
    ).resolve()
    paper_day_authority_path = (
        canonical_root
        / "reports"
        / "paper_trading_day_authority_v1"
        / day_utc
        / "paper_trading_day_authority.v1.json"
    ).resolve()
    submit_boundary_path = (
        canonical_root
        / "reports"
        / "submit_boundary_status_v1"
        / day_utc
        / "submit_boundary_status.v1.json"
    ).resolve()
    trade_lineage_graph_path = (
        canonical_root
        / "reports"
        / "trade_lineage_graph_v1"
        / day_utc
        / "trade_lineage_graph.v1.json"
    ).resolve()

    reconciliation = _read_json(reconciliation_path)
    reconciliation_ok = _reconciliation_complete(reconciliation)
    current_head = _read_json(current_head_path)
    submission_index = _read_json(submission_index_path)

    rows: list[dict[str, Any]] = []
    for submission_dir in _submission_dirs(submissions_day):
        submission_id = submission_dir.name
        broker_record_path = (submission_dir / "broker_submission_record.v2.json").resolve()
        submit_attempt_path = (submission_dir / "broker_submit_attempt_v1.json").resolve()
        fill_path = (fill_day / f"{submission_id}.fill_ledger.v1.json").resolve()
        order_plan, order_plan_path = _find_order_plan(submission_dir)
        broker_record = _read_json(broker_record_path)
        submit_attempt = _read_json(submit_attempt_path)
        fill_ledger = _read_json(fill_path)
        state, diagnostics = _state_for_submission(
            broker_record=broker_record,
            submit_attempt=submit_attempt,
            fill_ledger=fill_ledger,
            reconciliation_complete=reconciliation_ok,
            broker_record_path=broker_record_path,
        )
        observed_submission_id = str((broker_record or {}).get("submission_id") or submission_id).strip() or submission_id
        evidence_paths = {
            "submission_dir": str(submission_dir),
            "broker_submission_record_v2": str(broker_record_path),
            "broker_submit_attempt_v1": str(submit_attempt_path),
            "fill_ledger_v1": str(fill_path),
            "order_plan": str(order_plan_path) if order_plan_path else "",
            "execution_reconciliation_v1": str(reconciliation_path),
            "submission_index_v1": str(submission_index_path),
            "execution_evidence_current_head_v1": str(current_head_path),
            "submit_decision_trace_v1": str(submit_decision_root),
            "sleeve_intent_trade_attribution_v1": str(attribution_path),
            "trade_lineage_graph_v1": str(trade_lineage_graph_path),
            "paper_trading_day_authority_v1": str(paper_day_authority_path),
            "submit_boundary_status_v1": str(submit_boundary_path),
        }
        rows.append(
            {
                "submission_id": observed_submission_id,
                "current_lifecycle_state": state,
                "symbol": (order_plan or {}).get("symbol"),
                "side": (order_plan or {}).get("action"),
                "quantity": (order_plan or {}).get("qty_shares"),
                **diagnostics,
                "evidence_paths": evidence_paths,
            }
        )

    projection_rows = _projection_consistency(
        submission_rows=rows,
        submission_index=submission_index,
        current_head=current_head,
        submission_index_path=submission_index_path,
        current_head_path=current_head_path,
    )
    projection_conflicts = [row for row in projection_rows if row.get("status") == "CONFLICT"]
    state = _rollup_state(rows)
    first_blocker = ""
    for row in rows:
        first_blocker = str(row.get("first_blocker_or_gap") or "").strip()
        if first_blocker:
            break
    if not first_blocker and not rows:
        first_blocker = "NO_SUBMISSION_EVIDENCE"

    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "sleeve": sleeve,
        "environment": environment,
        "produced_utc": produced_utc or _utc_now_iso(),
        "authority_scope": "POST_SUBMIT_EXECUTION_LIFECYCLE",
        "status": "PASS" if state not in {"LINEAGE_GAP", "FAILED"} and not projection_conflicts else "FAIL",
        "current_lifecycle_state": state,
        "first_blocker_or_gap": first_blocker,
        "submission_count": len(rows),
        "submissions": rows,
        "projection_consistency": projection_rows,
        "input_evidence": [
            _input_status(submission_index_path, "submission_index_v1"),
            _input_status(current_head_path, "execution_evidence_v1/current_head"),
            _input_status(submissions_day, "execution_evidence_v1/submissions"),
            _input_status(fill_day, "fill_ledger_v1"),
            _input_status(reconciliation_path, "execution_reconciliation_v1"),
            _input_status(submit_decision_root, "submit_decision_trace_v1"),
            _input_status(attribution_path, "sleeve_intent_trade_attribution_v1"),
            _input_status(trade_lineage_graph_path, "trade_lineage_graph_v1"),
            _input_status(paper_day_authority_path, "paper_trading_day_authority_v1"),
            _input_status(submit_boundary_path, "submit_boundary_status_v1"),
        ],
        "canonical_json_hash": "",
    }
    payload["canonical_json_hash"] = _sha256_payload(payload)
    return payload


def write_execution_lifecycle_authority_v1(*, execution_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    output_path = execution_lifecycle_authority_output_path(execution_root=execution_root, day_utc=day_utc)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(_canonical_bytes(payload) + b"\n")
    return output_path
