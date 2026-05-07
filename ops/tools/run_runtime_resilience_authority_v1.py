#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from constellation_2.common.execution_evidence_current_head_v1 import current_head_output_path
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1
from constellation_2.common.submission_index_v1 import submission_index_output_path
from constellation_2.common.trading_day_readiness_authority_v1 import read_or_evaluate_trading_day_readiness_authority_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools import run_ib_broker_event_probe_v1 as probe
from ops.tools.run_broker_supply_v1 import broker_supply_path
from ops.tools.run_capital_supply_v1 import capital_supply_path
from ops.tools.run_intent_lifecycle_state_v1 import intent_lifecycle_state_path
from ops.tools.run_position_lifecycle_state_v1 import position_lifecycle_state_path

PAPER_MODE = "PAPER"
SCHEMA_VERSION = "runtime_resilience_authority.v1"
FRESHNESS_SECONDS = 300.0


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")


def _git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT), text=True).strip()
    except Exception:
        return "UNKNOWN"


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "runtime_resilience_authority_v1" / day_utc / "runtime_resilience_authority.v1.json"


def runtime_resilience_authority_path(*, truth_root: Path, day_utc: str) -> Path:
    return _write_path(truth_root=truth_root, day_utc=day_utc)


def _event_log_path(execution_root: Path, day_utc: str) -> Path:
    return execution_root / "execution_evidence_v1" / "broker_events" / day_utc / "broker_event_log.v1.jsonl"


def _event_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.is_file():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _event_types(rows: list[dict[str, Any]]) -> set[str]:
    return {str(row.get("event_type") or "").strip() for row in rows if str(row.get("event_type") or "").strip()}


def _latest_time(rows: list[dict[str, Any]], *keys: str) -> str:
    timestamps: list[str] = []
    for row in rows:
        for key in keys:
            value = str(row.get(key) or "").strip()
            if value:
                timestamps.append(value)
                break
    return sorted(timestamps)[-1] if timestamps else ""


def _status(payload: dict[str, Any]) -> str:
    return str(payload.get("status") or payload.get("state") or "").strip().upper()


def _is_fresh_file(path: Path, freshness_seconds: float = FRESHNESS_SECONDS) -> bool:
    if not path.exists() or not path.is_file():
        return False
    return max(0.0, time.time() - path.stat().st_mtime) <= float(freshness_seconds)


def _timestamp_day(value: str) -> str:
    text = str(value or "").strip()
    return text[:10] if len(text) >= 10 else ""


def _state_from_artifact(payload: dict[str, Any], path: Path, *, pass_statuses: set[str] | None = None) -> str:
    if not path.exists() or not path.is_file():
        return "MISSING"
    if not payload:
        return "DEGRADED"
    status = _status(payload)
    if pass_statuses is None:
        pass_statuses = {"PASS", "READY", "OK", "MANUAL_MODE_READY"}
    if status in pass_statuses:
        return "PRESENT"
    if status in {"BLOCKED", "FAIL", "ERROR"}:
        return "DEGRADED"
    return "PRESENT" if status else "DEGRADED"


def _first_existing_path(*paths: Path) -> Path:
    for path in paths:
        if path.exists():
            return path
    return paths[0]


def _account_summary_state(*, broker_supply: dict[str, Any], broker_path: Path, event_types: set[str], day_utc: str) -> tuple[str, str]:
    if not broker_path.exists():
        return "MISSING", ""
    if _status(broker_supply) not in {"PASS", "READY", "OK"}:
        return "MISSING", ""
    values = broker_supply.get("account_values") if isinstance(broker_supply.get("account_values"), dict) else {}
    has_values = values.get("net_liquidation_cents") is not None and values.get("total_cash_value_cents") is not None
    latest = str(broker_supply.get("generated_at_utc") or broker_supply.get("produced_at_utc") or "").strip()
    if latest and _timestamp_day(latest) != day_utc:
        return "MISSING", latest
    if has_values and ("accountSummary" in event_types or "updateAccountValue" in event_types or broker_supply):
        return "PRESENT", latest
    if "accountSummary" in event_types or "updateAccountValue" in event_types:
        return "DEGRADED", latest
    return "MISSING", latest


def _ib_connection_state(*, probe_payload: dict[str, Any], account_summary_state: str, broker_event_log_state: str) -> str:
    conn = probe_payload.get("connection") if isinstance(probe_payload.get("connection"), dict) else {}
    connected = bool(conn.get("connected") is True or probe_payload.get("status") == "PASS")
    if not probe_payload:
        if account_summary_state == "PRESENT" and broker_event_log_state == "PRESENT":
            return "CONNECTED"
        return "UNKNOWN"
    if not connected:
        return "DISCONNECTED"
    if account_summary_state != "PRESENT":
        return "STALE"
    return "CONNECTED"


def _broker_event_state(*, path: Path, rows: list[dict[str, Any]], day_utc: str, readiness: dict[str, Any]) -> tuple[str, str]:
    latest = _latest_time(rows, "received_utc", "timestamp_utc", "observed_at_utc", "event_time_utc")
    if not path.is_file():
        return "MISSING", latest
    if bool(readiness.get("requires_same_day_broker_event_log") is True) and _timestamp_day(latest) != day_utc:
        return "STALE", latest
    if not _is_fresh_file(path) and bool(readiness.get("requires_same_day_broker_event_log") is True):
        return "STALE", latest
    return "PRESENT", latest


def _market_data_state(*, truth_root: Path, day_utc: str) -> tuple[str, str, Path]:
    candidates = [
        truth_root / "reports" / "market_data_authority_v1" / day_utc / "market_data_authority.v1.json",
        truth_root / "reports" / "market_open_data_gate_v1" / day_utc / "market_open_data_gate.v1.json",
        truth_root / "reports" / "market_data_supply_v1" / day_utc / "market_data_supply.v1.json",
    ]
    for path in candidates:
        payload = _read_json(path)
        if not payload:
            continue
        status = _status(payload)
        last = str(
            payload.get("last_market_data_at_utc")
            or payload.get("latest_snapshot_at_utc")
            or payload.get("generated_at_utc")
            or payload.get("produced_at_utc")
            or ""
        ).strip()
        if status in {"PASS", "READY", "PRESENT"}:
            return "PRESENT", last, path
        if status in {"PENDING", "SKIPPED"}:
            return "DEGRADED", last, path
        return "DEGRADED", last, path
    return "MISSING", "", candidates[0]


def _restart_detected(runtime_root: Path, runtime_service: dict[str, Any]) -> bool:
    process_root = Path(runtime_root).resolve() / "runtime" / "process_state"
    for path in (
        process_root / "supervisor_state.json",
        process_root / "service_status.json",
        Path(runtime_root).resolve() / "process_state" / "supervisor_state.json",
        Path(runtime_root).resolve() / "process_state" / "service_status.json",
    ):
        payload = _read_json(path)
        if not payload:
            continue
        if payload.get("restart_detected") is True:
            return True
        try:
            if int(payload.get("restart_count") or payload.get("restarts") or 0) > 0:
                return True
        except Exception:
            pass
        services = payload.get("services") if isinstance(payload.get("services"), list) else []
        for service in services:
            if isinstance(service, dict) and (service.get("restart_detected") is True or str(service.get("state") or "").upper() == "RECONNECTING"):
                return True
    return bool(runtime_service.get("restart_detected") is True)


def _rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def _pending_and_position_reconciliation(position_lifecycle: dict[str, Any], submission_index: dict[str, Any], current_head: dict[str, Any]) -> tuple[bool, bool, list[str]]:
    reason_codes: list[str] = []
    pos_status = _status(position_lifecycle)
    counts = position_lifecycle.get("counts") if isinstance(position_lifecycle.get("counts"), dict) else {}
    rows = _rows(position_lifecycle)
    if pos_status in {"FAIL", "BLOCKED"}:
        reason_codes.append("POSITION_LIFECYCLE_NOT_RECONCILED")
        reason_codes.append("POSITION_OR_ORDER_TRUTH_UNKNOWN")
        return False, False, reason_codes
    if any(str(row.get("lifecycle_state") or "").upper() == "UNKNOWN" for row in rows):
        reason_codes.append("POSITION_OR_ORDER_TRUTH_UNKNOWN")
        return False, False, reason_codes
    if any(code in {reason for row in rows for reason in row.get("reason_codes", []) if isinstance(row.get("reason_codes"), list)} for code in ("POSITION_STATE_STALE", "POSITION_MATCH_UNCERTAIN")):
        reason_codes.append("POSITION_OR_ORDER_TRUTH_UNKNOWN")
        return False, False, reason_codes
    submission_status = _status(submission_index)
    head_status = _status(current_head)
    pending_count = int(counts.get("pending_order_count") or 0)
    open_count = int(counts.get("open_position_count") or 0)
    pending_reconciled = bool(submission_status in {"PASS", "OK", ""} and head_status in {"PASS", "OK", ""})
    open_reconciled = bool(pos_status in {"PASS", "DEGRADED", ""})
    if pending_count and not pending_reconciled:
        reason_codes.append("PENDING_ORDERS_NOT_RECONCILED")
    if open_count and not open_reconciled:
        reason_codes.append("OPEN_POSITIONS_NOT_RECONCILED")
    if not pending_count and submission_status in {"FAIL", "BLOCKED"}:
        pending_reconciled = False
        reason_codes.append("PENDING_ORDERS_NOT_RECONCILED")
    if not open_count and pos_status in {"FAIL", "BLOCKED"}:
        open_reconciled = False
        reason_codes.append("OPEN_POSITIONS_NOT_RECONCILED")
    return pending_reconciled, open_reconciled, sorted(set(reason_codes))


def _operator_action(blocker: str, restart_detected: bool) -> str:
    if blocker == "IB_DISCONNECTED":
        return "Restore IBKR Gateway/TWS API connection and confirm accountSummary is readable before submit."
    if blocker == "IB_RECONNECTING":
        return "Wait for reconnect to complete, then regenerate broker, capital, lifecycle, safety, and submit-boundary artifacts."
    if blocker == "BROKER_EVENT_LOG_MISSING":
        return "Start the authoritative IB observer and capture same-day broker event truth before intraday submit."
    if blocker == "BROKER_EVENT_LOG_STALE":
        return "Refresh same-day broker event truth through the IB observer before intraday submit."
    if blocker == "ACCOUNT_SUMMARY_MISSING":
        return "Refresh IB account summary/account values before submit; socket-only connectivity is insufficient."
    if blocker in {"PENDING_ORDERS_NOT_RECONCILED", "OPEN_POSITIONS_NOT_RECONCILED", "POSITION_OR_ORDER_TRUTH_UNKNOWN"}:
        return "Regenerate position_lifecycle_state_v1, intent_lifecycle_state_v1, submission_index_v1, and current_head, then confirm re-entry suppression."
    if blocker == "RESTART_RECOVERY_REQUIRED" or restart_detected:
        return "After restart, regenerate broker_supply_v1, capital_supply_v1, position/intent lifecycle, safety_state_authority_v1, and submit_boundary_status_v1 before submit."
    return "Runtime resilience evidence is current; no recovery action required."


def build_runtime_resilience_authority_v1(
    *,
    day_utc: str,
    truth_root: Path,
    execution_root: Path,
    runtime_root: Path,
    environment: str = PAPER_MODE,
    broker_account: str = "",
    freshness_seconds: float = FRESHNESS_SECONDS,
) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    execution_root = Path(execution_root).resolve()
    runtime_root = Path(runtime_root).resolve()
    readiness_path, readiness = read_or_evaluate_trading_day_readiness_authority_v1(
        target_day=day_utc,
        truth_root=truth_root,
        execution_root=execution_root,
        environment=environment,
    )
    readiness_mode = str(readiness.get("readiness_mode") or "").strip().upper()
    broker_path = _first_existing_path(
        broker_supply_path(truth_root=truth_root, day_utc=day_utc),
        broker_supply_path(truth_root=execution_root, day_utc=day_utc),
    )
    capital_path = _first_existing_path(
        capital_supply_path(truth_root=truth_root, day_utc=day_utc),
        capital_supply_path(truth_root=execution_root, day_utc=day_utc),
    )
    runtime_service_path = truth_root / "reports" / "runtime_service_authority_v1" / day_utc / "runtime_service_authority.v1.json"
    probe_path = probe.probe_artifact_path_v1(truth_root=truth_root, day_utc=day_utc)
    position_path = position_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc)
    intent_path = intent_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc)
    submission_path = submission_index_output_path(execution_root=execution_root, day_utc=day_utc)
    head_path = current_head_output_path(execution_root=execution_root, day_utc=day_utc)
    event_path = _event_log_path(execution_root, day_utc)
    broker = _read_json(broker_path)
    capital = _read_json(capital_path)
    runtime_service = _read_json(runtime_service_path)
    probe_payload = _read_json(probe_path)
    position = _read_json(position_path)
    intent = _read_json(intent_path)
    submission = _read_json(submission_path)
    current_head = _read_json(head_path)
    events = _event_rows(event_path)
    event_types = _event_types(events)
    account_summary_state, last_account_summary = _account_summary_state(broker_supply=broker, broker_path=broker_path, event_types=event_types, day_utc=day_utc)
    broker_event_log_state, last_broker_event = _broker_event_state(path=event_path, rows=events, day_utc=day_utc, readiness=readiness)
    market_data_heartbeat_state, last_market_data, market_path = _market_data_state(truth_root=truth_root, day_utc=day_utc)
    if market_data_heartbeat_state == "MISSING" and execution_root != truth_root:
        market_data_heartbeat_state, last_market_data, market_path = _market_data_state(truth_root=execution_root, day_utc=day_utc)
    ib_connection_state = _ib_connection_state(probe_payload=probe_payload, account_summary_state=account_summary_state, broker_event_log_state=broker_event_log_state)
    if _status(runtime_service) == "RECONNECTING" or str(runtime_service.get("service_state") or "").upper() == "RECONNECTING":
        ib_connection_state = "RECONNECTING"
    restart_detected = _restart_detected(runtime_root, runtime_service)
    pending_reconciled, open_reconciled, reconciliation_codes = _pending_and_position_reconciliation(position, submission, current_head)
    reason_codes: list[str] = []
    if ib_connection_state in {"DISCONNECTED", "UNKNOWN"}:
        reason_codes.append("IB_DISCONNECTED")
    elif ib_connection_state in {"STALE"}:
        reason_codes.append("ACCOUNT_SUMMARY_MISSING")
    elif ib_connection_state == "RECONNECTING":
        reason_codes.append("IB_RECONNECTING")
    if bool(readiness.get("requires_same_day_broker_event_log") is True) and broker_event_log_state == "MISSING":
        reason_codes.append("BROKER_EVENT_LOG_MISSING")
    if bool(readiness.get("requires_same_day_broker_event_log") is True) and broker_event_log_state == "STALE":
        reason_codes.append("BROKER_EVENT_LOG_STALE")
    if restart_detected:
        reason_codes.append("RESTART_RECOVERY_REQUIRED")
    reason_codes.extend(reconciliation_codes)
    if account_summary_state == "MISSING":
        reason_codes.append("ACCOUNT_SUMMARY_MISSING")
    recovery_status = "NOT_REQUIRED"
    if restart_detected:
        if reconciliation_codes or ib_connection_state != "CONNECTED" or account_summary_state != "PRESENT":
            recovery_status = "BLOCKED"
        elif pending_reconciled and open_reconciled:
            recovery_status = "PASS"
        else:
            recovery_status = "IN_PROGRESS"
    canonical_priority = [
        "IB_RECONNECTING",
        "IB_DISCONNECTED",
        "ACCOUNT_SUMMARY_MISSING",
        "BROKER_EVENT_LOG_MISSING",
        "BROKER_EVENT_LOG_STALE",
        "POSITION_OR_ORDER_TRUTH_UNKNOWN",
        "PENDING_ORDERS_NOT_RECONCILED",
        "OPEN_POSITIONS_NOT_RECONCILED",
        "RESTART_RECOVERY_REQUIRED",
    ]
    normalized = sorted(set(reason_codes))
    canonical_blocker = next((code for code in canonical_priority if code in normalized), "")
    submit_blocked_during_recovery = bool(recovery_status in {"IN_PROGRESS", "BLOCKED", "DEGRADED"} or ib_connection_state in {"DISCONNECTED", "STALE", "RECONNECTING", "UNKNOWN"} or not pending_reconciled or not open_reconciled)
    if canonical_blocker:
        status = "BLOCKED" if canonical_blocker not in {"ACCOUNT_SUMMARY_MISSING"} else "DEGRADED"
    elif market_data_heartbeat_state in {"MISSING", "STALE", "DEGRADED"}:
        status = "DEGRADED"
    else:
        status = "PASS"
    config, _ = probe._resolve_config(day_utc, environment, 12.0, freshness_seconds, False)
    evidence_paths = [
        str(path)
        for path in (readiness_path, broker_path, capital_path, runtime_service_path, probe_path, position_path, intent_path, submission_path, head_path, event_path, market_path)
        if path.exists()
    ]
    out_path = runtime_resilience_authority_path(truth_root=truth_root, day_utc=day_utc)
    payload = {
        "schema_id": "runtime_resilience_authority",
        "schema_version": SCHEMA_VERSION,
        "day_utc": day_utc,
        "environment": environment,
        "truth_root": str(truth_root),
        "producer": {
            "repo": REPO_ROOT.name,
            "module": "ops/tools/run_runtime_resilience_authority_v1.py",
            "git_sha": _git_sha(),
        },
        "status": status,
        "ib_connection_state": ib_connection_state,
        "account_summary_state": account_summary_state,
        "market_data_heartbeat_state": market_data_heartbeat_state,
        "broker_event_log_state": broker_event_log_state,
        "restart_detected": restart_detected,
        "recovery_status": recovery_status,
        "pending_orders_reconciled": pending_reconciled,
        "open_positions_reconciled": open_reconciled,
        "broker_account": broker_account or str(broker.get("account") or config.expected_account),
        "ib_host": str(config.host),
        "ib_port": int(config.port),
        "ib_client_id": int(config.observer_client_id),
        "last_heartbeat_at_utc": str(runtime_service.get("produced_utc") or runtime_service.get("generated_at_utc") or ""),
        "last_account_summary_at_utc": last_account_summary,
        "last_broker_event_at_utc": last_broker_event,
        "last_market_data_at_utc": last_market_data,
        "submit_blocked_during_recovery": submit_blocked_during_recovery,
        "canonical_blocker": canonical_blocker,
        "reason_codes": normalized,
        "evidence_paths": sorted(set(evidence_paths)),
        "operator_next_action": _operator_action(canonical_blocker, restart_detected),
        "generated_at_utc": _now_iso(),
        "produced_at_utc": _now_iso(),
        "readiness_authority_path": str(readiness_path),
        "readiness_mode": readiness_mode,
        "capital_supply_status": _status(capital),
        "position_lifecycle_status": _status(position),
        "intent_lifecycle_status": _status(intent),
        "submission_index_status": _status(submission),
        "current_head_status": _status(current_head),
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    parser = argparse.ArgumentParser(prog="run_runtime_resilience_authority_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--freshness_seconds", type=float, default=FRESHNESS_SECONDS)
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment or PAPER_MODE).strip().upper()
    truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root or "",
        repo_root=REPO_ROOT,
        caller="ops/tools/run_runtime_resilience_authority_v1.py",
    )
    ctx = bod._resolve_context(day_utc, environment, str(truth_root))
    payload = build_runtime_resilience_authority_v1(
        day_utc=day_utc,
        truth_root=ctx.truth_root,
        execution_root=ctx.execution_root,
        runtime_root=ctx.runtime_root,
        environment=ctx.environment,
        broker_account=ctx.ib_account,
        freshness_seconds=float(args.freshness_seconds),
    )
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "path": payload["artifact_path"]}, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "DEGRADED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
