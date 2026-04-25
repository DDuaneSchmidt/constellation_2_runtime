from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    now_utc_iso_v1,
    producer_block_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.trade_readiness_reducer_v1 import (
    build_trade_readiness_decision_payload_v1,
    write_trade_readiness_decision_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_day_open_attempt_path,
    resolve_day_open_trigger_path,
    resolve_operator_day_authority_summary_path,
    resolve_paper_session_ledger_path,
    resolve_report_artifact_path,
    resolve_trade_readiness_decision_path,
    resolve_trading_day_state_machine_path,
)
from constellation_2.common.session_authority_monitor_v1 import resolve_session_authority_status_path
from constellation_2.common.session_authority_v1 import (
    resolve_active_session_path,
    resolve_target_day_admission_path,
)


SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_day_authority_summary.v1.schema.json"


def _classification(
    *,
    day_utc: str,
    active_session: Dict[str, Any],
    admission: Dict[str, Any],
    session_status: Dict[str, Any],
    ledger: Dict[str, Any],
    state_machine: Dict[str, Any],
    execution_reconciliation: Dict[str, Any] | None,
) -> tuple[str, str]:
    admission_status = str(admission.get("admission_status") or "").strip().upper()
    rollover_status = str(active_session.get("rollover_status") or "").strip().upper()
    active_day = str(active_session.get("active_day") or "").strip()
    final_start_decision = str(state_machine.get("final_start_decision") or "").strip().upper()
    summary = state_machine.get("derived_daily_summary") if isinstance(state_machine.get("derived_daily_summary"), dict) else {}
    summary_state = str(summary.get("summary_state") or final_start_decision or "").strip().upper()
    submit_lifecycle = ledger.get("submit_lifecycle") if isinstance(ledger.get("submit_lifecycle"), dict) else {}
    post_submit_lifecycle = ledger.get("post_submit_lifecycle") if isinstance(ledger.get("post_submit_lifecycle"), dict) else {}
    control_state = ledger.get("control_state") if isinstance(ledger.get("control_state"), dict) else {}
    submit_attempted = bool(
        submit_lifecycle.get("submit_attempted") is True or ledger.get("submit_attempted") is True
    )
    submission_authorized = bool(
        control_state.get("submission_authorized") is True
        or ledger.get("submission_authorized") is True
    )
    authority_status = str(
        control_state.get("authority_status")
        or ledger.get("authority_status")
        or ""
    ).strip().upper()
    reconciliation_codes = list((execution_reconciliation or {}).get("reason_codes") or [])
    open_lifecycle_state = str(state_machine.get("open_lifecycle_state") or "").strip().upper()
    open_policy = state_machine.get("open_policy") if isinstance(state_machine.get("open_policy"), dict) else {}
    is_paper = str(open_policy.get("environment") or "PAPER").strip().upper() == "PAPER"
    gap_codes = [str(code).strip().upper() for code in (post_submit_lifecycle.get("gap_codes") or []) if str(code).strip()]
    runtime_projection = (
        execution_reconciliation.get("runtime_ledger_projection")
        if isinstance(execution_reconciliation, dict)
        else {}
    )
    event_types = {
        str(item).strip().upper()
        for item in (runtime_projection.get("event_types") or [])
        if str(item).strip()
    }
    fill_observed = any("FILL" in item for item in event_types)
    submission_observed = any("SUBMIT" in item or "ORDER" in item for item in event_types)
    if not submission_observed:
        submission_observed = bool(
            submit_attempted
            and not any("SUBMISSION" in code and "MISSING" in code for code in gap_codes)
        )
    if final_start_decision == "READY_NOW":
        if open_lifecycle_state == "OPEN_SUCCEEDED":
            if fill_observed:
                return "OPEN_WITH_FILLS", "Paper open succeeded and downstream fill evidence is present."
            if submission_observed:
                return "OPEN_WITH_SUBMISSIONS", "Paper open succeeded and downstream submission evidence is present."
            if submission_authorized:
                return "OPEN_SUBMIT_CAPABLE", "Paper open succeeded and submission authority is currently available."
            return "OPEN_NO_TRADES", "Paper open succeeded while submission remains fail-closed; no submissions or fills are present."
        if submission_authorized:
            return "OPEN_SUBMIT_CAPABLE", "Paper startup authority is READY_NOW and submission authority is currently available."
        return "OPEN_NO_TRADES", "Paper startup authority is READY_NOW while submission remains fail-closed."

    if active_day != day_utc and admission_status == "ADMIT":
        return "PRE_OPEN_READY", "Binding admission is green for the target day before active-session rollover."
    if admission_status != "ADMIT" or "BLOCKED" in final_start_decision or "BLOCKED" in summary_state:
        return "BLOCKED", "Binding admission or day-state authority is blocking the target day."
    if "DEFECT" in final_start_decision or str(session_status.get("status_severity") or "").strip().upper() == "CRITICAL":
        return "FAILED", "Binding control-plane authority reports a defect-level failure."
    if open_lifecycle_state in {
        "PAPER_OPEN_AVAILABLE",
        "OPEN_TRIGGER_EMITTED",
        "OPEN_ATTEMPTED",
        "OPEN_SUCCEEDED",
        "OPEN_MISSED",
        "OPEN_FAILED",
        "OPEN_WAITING_FOR_AUTHORITY",
        "LATE_OPEN_AVAILABLE",
        "LATE_OPEN_EXHAUSTED",
    }:
        message_map = {
            "OPEN_WAITING_FOR_AUTHORITY": "Binding admission is green, but the governed open trigger has not been emitted yet.",
            "PAPER_OPEN_AVAILABLE": "Paper readiness and authority are currently granted. Governed paper trading is allowed now.",
            "OPEN_TRIGGER_EMITTED": "A governed day-open trigger exists and is waiting for orchestrator consumption.",
            "OPEN_ATTEMPTED": "A governed day-open attempt is in flight or has been recorded without final completion yet.",
            "OPEN_SUCCEEDED": (
                "The latest governed paper-open attempt executed successfully, and paper trading remains allowed while readiness stays granted."
                if is_paper
                else (
                    "A successful open already occurred today; additional opens are forbidden."
                    if bool(open_policy.get("successful_open_already_recorded") is True)
                    else "The governed day-open attempt executed successfully."
                )
            ),
            "LATE_OPEN_AVAILABLE": "Initial open missed; one governed late-open remains available.",
            "LATE_OPEN_EXHAUSTED": "Late-open has been consumed; no further open attempts are allowed today.",
            "OPEN_MISSED": "The governed day-open window expired without a successful open attempt.",
            "OPEN_FAILED": (
                "The latest governed paper-open attempt failed, but paper trading remains allowed while readiness stays granted."
                if is_paper
                else "The governed day-open attempt executed but failed."
            ),
        }
        return open_lifecycle_state, message_map[open_lifecycle_state]
    if submit_attempted:
        if any(str(code).strip() == "NO_SUBMISSIONS_FOUND" for code in reconciliation_codes):
            return "STARTED", "Startup authority granted and submit attempt observed without downstream progress evidence yet."
        return "STARTED_AND_PROGRESSED", "Startup authority granted and downstream progress evidence is present."
    if authority_status == "GRANTED" and rollover_status in {"ROLLOVER_COMPLETED", "ACTIVE_SESSION_CONFIRMED"}:
        return "STARTING", "Binding startup authority is granted and the active session has rolled forward."
    return "READY_NOT_STARTED", "Binding admission is green but no governed start evidence is present yet."


def build_operator_day_authority_summary_payload(
    *,
    truth_root: Path,
    day_utc: str,
) -> Dict[str, Any]:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    admission = read_json_object_v1(resolve_target_day_admission_path(truth_root=root, target_day=day_utc))
    active_session = read_json_object_v1(resolve_active_session_path(truth_root=root))
    session_status = read_json_object_v1(resolve_session_authority_status_path(truth_root=root))
    state_machine = read_json_object_v1(resolve_trading_day_state_machine_path(truth_root=root, day_utc=day_utc))
    ledger = read_json_object_v1(resolve_paper_session_ledger_path(truth_root=root, day_utc=day_utc))
    execution_reconciliation_path = resolve_report_artifact_path(
        truth_root=root,
        artifact_family="execution_reconciliation_v1",
        day_utc=day_utc,
        filename="execution_reconciliation.v1.json",
    )
    execution_reconciliation = (
        read_json_object_v1(execution_reconciliation_path)
        if execution_reconciliation_path.exists() and execution_reconciliation_path.is_file()
        else None
    )
    trade_readiness_path = resolve_trade_readiness_decision_path(truth_root=root, day_utc=day_utc)
    if not trade_readiness_path.exists() or not trade_readiness_path.is_file():
        try:
            paper_account = resolve_single_paper_ib_account_from_sleeve_registry(Path(__file__).resolve().parents[2])
            decision_payload = build_trade_readiness_decision_payload_v1(
                repo_root=Path(__file__).resolve().parents[2],
                truth_root=root,
                day_utc=day_utc,
                environment="PAPER",
                ib_account=paper_account,
            )
            write_trade_readiness_decision_v1(truth_root=root, payload=decision_payload)
        except Exception:
            pass
    trade_readiness_yes_no = "NO"
    trade_readiness_gate = "Submit Permission"
    trade_readiness_blocker = "TRADE_READINESS_DECISION_MISSING"
    trade_readiness_reason = "trade_readiness_decision_v1 is missing."
    if trade_readiness_path.exists() and trade_readiness_path.is_file():
        try:
            trade_readiness = read_json_object_v1(trade_readiness_path)
            trade_readiness_yes_no = str(trade_readiness.get("decision") or "NO").strip().upper() or "NO"
            trade_readiness_gate = str(trade_readiness.get("canonical_gate") or "Submit Permission").strip()
            canonical_blocker = trade_readiness.get("canonical_blocker")
            trade_readiness_blocker = "" if canonical_blocker is None else str(canonical_blocker).strip()
            trade_readiness_reason = str(trade_readiness.get("canonical_reason") or "").strip()
            if not trade_readiness_reason:
                trade_readiness_reason = "trade_readiness_decision_v1 canonical reason is unavailable."
        except Exception:
            trade_readiness_yes_no = "NO"
            trade_readiness_gate = "Submit Permission"
            trade_readiness_blocker = "TRADE_READINESS_DECISION_INVALID"
            trade_readiness_reason = "trade_readiness_decision_v1 is malformed."
    final_classification, operator_message = _classification(
        day_utc=day_utc,
        active_session=active_session,
        admission=admission,
        session_status=session_status,
        ledger=ledger,
        state_machine=state_machine,
        execution_reconciliation=execution_reconciliation,
    )
    open_policy = state_machine.get("open_policy") if isinstance(state_machine.get("open_policy"), dict) else {}
    first_true_blocker = state_machine.get("first_true_blocker") if isinstance(state_machine.get("first_true_blocker"), dict) else {}
    return {
        "schema_id": "operator_day_authority_summary",
        "schema_version": "v1",
        "binding_classification": "BINDING_AUTHORITY_SUMMARY",
        "day_utc": str(day_utc).strip(),
        "produced_at_utc": now_utc_iso_v1(),
        "producer": producer_block_v1(module="constellation_2/common/operator_day_authority_summary_v1.py"),
        "active_day": str(active_session.get("active_day") or "").strip(),
        "admission_status": str(admission.get("admission_status") or "").strip().upper(),
        "session_authority_rollover_status": str(active_session.get("rollover_status") or "").strip().upper(),
        "session_authority_status_severity": str(session_status.get("status_severity") or "").strip().upper(),
        "final_start_decision": str(state_machine.get("final_start_decision") or "").strip().upper(),
        "startup_open_status": final_classification,
        "open_policy": open_policy,
        "first_blocker_code": str(first_true_blocker.get("first_true_blocker_code") or "").strip(),
        "first_blocker_artifact_path": str(first_true_blocker.get("first_true_blocker_artifact_path") or "").strip(),
        "operator_message": operator_message,
        "trade_readiness_yes_no": trade_readiness_yes_no,
        "trade_readiness_canonical_gate": trade_readiness_gate,
        "trade_readiness_canonical_blocker": trade_readiness_blocker,
        "trade_readiness_reason": trade_readiness_reason,
        "input_refs": {
            "active_session_v1": str(resolve_active_session_path(truth_root=root)),
            "target_day_admission_v1": str(resolve_target_day_admission_path(truth_root=root, target_day=day_utc)),
            "session_authority_status_v1": str(resolve_session_authority_status_path(truth_root=root)),
            "paper_session_ledger_v1": str(resolve_paper_session_ledger_path(truth_root=root, day_utc=day_utc)),
            "trading_day_state_machine_v1": str(resolve_trading_day_state_machine_path(truth_root=root, day_utc=day_utc)),
            "day_open_trigger_v1": str(resolve_day_open_trigger_path(truth_root=root, day_utc=day_utc)),
            "day_open_attempt_v1": str(resolve_day_open_attempt_path(truth_root=root, day_utc=day_utc)),
            "execution_reconciliation_v1": str(execution_reconciliation_path),
            "trade_readiness_decision_v1": str(trade_readiness_path),
        },
    }


def write_operator_day_authority_summary_v1(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_operator_day_authority_summary_path(
            truth_root=root,
            day_utc=str(payload.get("day_utc") or "").strip(),
        ),
        payload=payload,
        schema_relpath=SCHEMA_RELPATH,
        volatile_field_names=("produced_at_utc",),
    )
