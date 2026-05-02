#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.trading_day_readiness_authority_v1 import read_or_evaluate_trading_day_readiness_authority_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools import run_ib_broker_event_probe_v1 as probe

SCHEMA_VERSION = "broker_supply.v1"
REQUIRED_EVENTS = {"nextValidId", "managedAccounts", "accountSummary", "accountSummaryEnd", "positionEnd"}
VALUE_TAGS = {"NetLiquidation", "TotalCashValue", "AvailableFunds", "BuyingPower"}


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _runtime_resilience_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "runtime_resilience_authority_v1" / day_utc / "runtime_resilience_authority.v1.json").resolve()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def _money_to_cents(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int((Decimal(text) * Decimal("100")).quantize(Decimal("1")))
    except (InvalidOperation, ValueError):
        return None


def broker_supply_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "broker_supply_v1" / day_utc / "broker_supply.v1.json").resolve()


def _probe_path(*, truth_root: Path, day_utc: str) -> Path:
    return probe.probe_artifact_path_v1(truth_root=truth_root, day_utc=day_utc)


def _event_log_path(ctx: bod.BodContext) -> Path:
    return (
        ctx.execution_root
        / "execution_evidence_v1"
        / "broker_events"
        / ctx.day_utc
        / "broker_event_log.v1.jsonl"
    ).resolve()


def _prior_day(day_utc: str) -> str:
    from datetime import date, timedelta

    return (date.fromisoformat(day_utc) - timedelta(days=1)).isoformat()


def _arg_values(row: dict[str, Any]) -> list[str]:
    args = ((row.get("ib_fields") or {}).get("args") or []) if isinstance(row.get("ib_fields"), dict) else []
    return [str(item.get("value") or "") for item in args if isinstance(item, dict)]


def _extract_prefixed(values: list[str], prefix: str) -> str:
    for value in values:
        for token in str(value or "").replace(",", " ").split():
            if token.startswith(prefix):
                return token.split("=", 1)[1].strip()
        if str(value).startswith(prefix):
            return str(value).split("=", 1)[1].strip()
    return ""


def _normalize_event(row: dict[str, Any]) -> dict[str, Any]:
    event_type = str(row.get("event_type") or "").strip()
    if "ib_fields" not in row:
        return dict(row)
    values = _arg_values(row)
    event = {"event_type": event_type, "received_utc": str(row.get("received_utc") or "")}
    if event_type == "managedAccounts":
        raw = _extract_prefixed(values, "accounts=")
        event["accounts"] = [item.strip() for item in raw.split(",") if item.strip()]
    elif event_type == "accountSummary":
        event["account"] = _extract_prefixed(values, "account=")
        event["tag"] = _extract_prefixed(values, "tag=")
        event["value"] = _extract_prefixed(values, "value=")
        event["currency"] = _extract_prefixed(values, "currency=")
    elif event_type in {"position", "updateAccountValue", "updatePortfolio", "accountDownloadEnd"}:
        event["account"] = _extract_prefixed(values, "account=")
        if event_type == "updateAccountValue":
            event["key"] = _extract_prefixed(values, "key=")
            event["value"] = _extract_prefixed(values, "value=")
            event["currency"] = _extract_prefixed(values, "currency=")
    return event


def _read_event_log(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not path.exists() or not path.is_file():
        return events
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict):
            events.append(_normalize_event(row))
    return events


def _event_types(events: list[dict[str, Any]]) -> set[str]:
    return {str(row.get("event_type") or "").strip() for row in events if str(row.get("event_type") or "").strip()}


def _latest_event_at(events: list[dict[str, Any]]) -> str:
    timestamps = sorted(str(row.get("received_utc") or "") for row in events if str(row.get("received_utc") or "").strip())
    return timestamps[-1] if timestamps else ""


def _managed_accounts(events: list[dict[str, Any]]) -> list[str]:
    accounts: set[str] = set()
    for row in events:
        if row.get("event_type") == "managedAccounts":
            for account in row.get("accounts") or []:
                text = str(account or "").strip()
                if text:
                    accounts.add(text)
    return sorted(accounts)


def _account_summary_values(events: list[dict[str, Any]], expected_account: str) -> tuple[dict[str, Any], str]:
    raw_fields: dict[str, Any] = {}
    currency = ""
    for row in events:
        event_type = str(row.get("event_type") or "")
        account = str(row.get("account") or "").strip()
        if account and account != expected_account:
            continue
        if event_type == "accountSummary":
            tag = str(row.get("tag") or "").strip()
            if tag in VALUE_TAGS:
                raw_fields[tag] = row.get("value")
                currency = str(row.get("currency") or currency or "").strip()
        elif event_type == "updateAccountValue":
            tag = str(row.get("key") or "").strip()
            if tag in VALUE_TAGS:
                raw_fields[tag] = row.get("value")
                currency = str(row.get("currency") or currency or "").strip()
    return raw_fields, currency or "USD"


def _position_rows(events: list[dict[str, Any]], expected_account: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in events:
        if row.get("event_type") != "position":
            continue
        account = str(row.get("account") or "").strip()
        if account and account != expected_account:
            continue
        out.append(dict(row))
    return out


def _operator_action(blocker: str) -> str:
    mapping = {
        "IB_CONNECTION_FAILED": "Restore IBKR Gateway/TWS API connection and rerun run_broker_supply_v1.py.",
        "BROKER_EVENT_LOG_MISSING": "Start the authoritative IB execution observer so it writes the current-day broker event log, then rerun run_broker_supply_v1.py.",
        "BROKER_EVENT_LOG_STALE": "Restart or unblock the authoritative IB execution observer so broker events are fresh, then rerun run_broker_supply_v1.py.",
        "BROKER_ACCOUNT_MISMATCH": "Log in to the governed paper account DUO847203 or update governed account config only through the approved account registry.",
        "BROKER_ACCOUNT_SUMMARY_MISSING": "Ensure the observer/probe requests accountSummary or account updates for DUO847203.",
        "BROKER_NAV_EVIDENCE_MISSING": "Capture current-day IB account summary NetLiquidation for DUO847203.",
        "BROKER_CASH_EVIDENCE_MISSING": "Capture current-day IB account summary TotalCashValue for DUO847203.",
        "BROKER_POSITION_SNAPSHOT_MISSING": "Ensure the observer/probe requests positions and receives positionEnd for DUO847203.",
        "BROKER_VALUES_INVALID": "Fix non-numeric or non-positive broker account values before using broker capital evidence.",
    }
    return mapping.get(blocker, f"Resolve {blocker}, then rerun run_broker_supply_v1.py." if blocker else "")


def _connection_from_probe(probe_payload: dict[str, Any], config: probe.ProbeConfig, probe_path: Path) -> dict[str, Any]:
    conn = probe_payload.get("connection") if isinstance(probe_payload.get("connection"), dict) else {}
    connected = bool(conn.get("connected") is True or probe_payload.get("status") == "PASS")
    return {
        "status": "CONNECTED" if connected else ("DISCONNECTED" if probe_payload else "UNKNOWN"),
        "host": str(probe_payload.get("expected_host") or config.host),
        "port": int(probe_payload.get("expected_port") or config.port),
        "observer_client_id": int(probe_payload.get("execution_observer_client_id") or config.observer_client_id),
        "probe_path": str(probe_path),
    }


def build_broker_supply(ctx: bod.BodContext, *, freshness_seconds: float = 300.0) -> dict[str, Any]:
    readiness_path, readiness = read_or_evaluate_trading_day_readiness_authority_v1(
        target_day=ctx.day_utc,
        truth_root=ctx.truth_root,
        execution_root=ctx.execution_root,
        environment=ctx.environment,
    )
    readiness_mode = str(readiness.get("readiness_mode") or "").strip().upper()
    evidence_policy = readiness.get("evidence_policy") if isinstance(readiness.get("evidence_policy"), dict) else {}
    carry_forward_allowed = (
        "broker_event_log" in {str(item).strip() for item in readiness.get("allowed_carry_forward_sources") or []}
        and not bool(readiness.get("requires_same_day_broker_event_log") is True)
    )
    config, truth_root = probe._resolve_config(ctx.day_utc, ctx.environment, 12.0, freshness_seconds, False)
    probe_path = _probe_path(truth_root=truth_root, day_utc=ctx.day_utc)
    probe_payload = _read_json(probe_path)
    log_path = _event_log_path(ctx)
    carry_forward_day = _prior_day(ctx.day_utc)
    carry_forward_path = (
        ctx.execution_root
        / "execution_evidence_v1"
        / "broker_events"
        / carry_forward_day
        / "broker_event_log.v1.jsonl"
    ).resolve()
    same_day_log_exists = log_path.exists() and log_path.is_file()
    carry_forward_log_exists = carry_forward_path.exists() and carry_forward_path.is_file()
    target_is_future = str(readiness.get("session_state") or "").strip().upper() == "FUTURE_TARGET_DAY"
    if target_is_future and carry_forward_allowed and carry_forward_log_exists:
        broker_event_source = "CARRY_FORWARD"
    else:
        broker_event_source = "SAME_DAY" if same_day_log_exists else ("CARRY_FORWARD" if carry_forward_allowed and carry_forward_log_exists else "MISSING")
    event_log_day_used = ctx.day_utc if broker_event_source == "SAME_DAY" else (carry_forward_day if broker_event_source == "CARRY_FORWARD" else "")
    event_log_path_used = log_path if broker_event_source in {"SAME_DAY", "MISSING"} else carry_forward_path
    events = _read_event_log(event_log_path_used) if broker_event_source != "MISSING" else []
    event_types = _event_types(events)
    latest_event_at = _latest_event_at(events)
    managed = _managed_accounts(events)
    raw_fields, currency = _account_summary_values(events, ctx.ib_account)
    positions = _position_rows(events, ctx.ib_account)
    required_seen = sorted(REQUIRED_EVENTS & event_types)
    log_status = "MISSING"
    broker_event_freshness_status = "MISSING"
    if broker_event_source == "CARRY_FORWARD":
        log_status = "PRESENT"
        broker_event_freshness_status = "CARRY_FORWARD_T_MINUS_1"
    elif log_path.exists() and log_path.is_file():
        log_status = "PRESENT"
        if max(0.0, time.time() - log_path.stat().st_mtime) > float(freshness_seconds):
            log_status = "STALE"
            broker_event_freshness_status = "STALE"
        else:
            broker_event_freshness_status = "FRESH"
    account_values = {
        "net_liquidation_cents": _money_to_cents(raw_fields.get("NetLiquidation")),
        "total_cash_value_cents": _money_to_cents(raw_fields.get("TotalCashValue")),
        "available_funds_cents": _money_to_cents(raw_fields.get("AvailableFunds")),
        "buying_power_cents": _money_to_cents(raw_fields.get("BuyingPower")),
        "currency": currency,
        "raw_fields": raw_fields,
    }
    blocker = ""
    if bool(readiness.get("requires_live_account_truth") is True) and (
        (probe_payload.get("status") == "BLOCKED" and str(probe_payload.get("canonical_blocker") or "").startswith("IB_"))
        or (probe_payload and (probe_payload.get("connection") or {}).get("connected") is False)
    ):
        blocker = "IB_CONNECTION_FAILED"
    elif log_status == "MISSING":
        blocker = "BROKER_EVENT_LOG_MISSING"
    elif log_status == "STALE" and broker_event_source != "CARRY_FORWARD":
        blocker = "BROKER_EVENT_LOG_STALE"
    elif ctx.ib_account not in managed:
        blocker = "BROKER_ACCOUNT_MISMATCH" if managed else "BROKER_ACCOUNT_SUMMARY_MISSING"
    elif "accountSummary" not in event_types and "updateAccountValue" not in event_types:
        blocker = "BROKER_ACCOUNT_SUMMARY_MISSING"
    elif "NetLiquidation" in raw_fields and account_values["net_liquidation_cents"] is None:
        blocker = "BROKER_VALUES_INVALID"
    elif "TotalCashValue" in raw_fields and account_values["total_cash_value_cents"] is None:
        blocker = "BROKER_VALUES_INVALID"
    elif account_values["net_liquidation_cents"] is None:
        blocker = "BROKER_NAV_EVIDENCE_MISSING"
    elif account_values["total_cash_value_cents"] is None:
        blocker = "BROKER_CASH_EVIDENCE_MISSING"
    elif int(account_values["net_liquidation_cents"]) <= 0 or int(account_values["total_cash_value_cents"]) < 0:
        blocker = "BROKER_VALUES_INVALID"
    elif "positionEnd" not in event_types:
        blocker = "BROKER_POSITION_SNAPSHOT_MISSING"

    status = "PASS" if not blocker else "BLOCKED"
    capital_usable = status == "PASS"
    execution_usable = status == "PASS"
    runtime_path = _runtime_resilience_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    runtime_resilience = _read_json(runtime_path)
    return {
        "schema_id": "broker_supply",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "truth_root": str(ctx.truth_root.resolve()),
        "generated_at_utc": _now_iso(),
        "status": status,
        "canonical_blocker": blocker,
        "provider": "IBKR",
        "account": ctx.ib_account,
        "connection": _connection_from_probe(probe_payload, config, probe_path),
        "event_log": {
            "path": str(event_log_path_used),
            "status": log_status,
            "source": broker_event_source,
            "day_used": event_log_day_used,
            "latest_event_at_utc": latest_event_at,
            "required_events_seen": required_seen,
        },
        "readiness_authority_path": str(readiness_path),
        "readiness_mode": readiness_mode,
        "evidence_policy_used": evidence_policy,
        "carry_forward_source_used": str(carry_forward_path) if broker_event_source == "CARRY_FORWARD" else "",
        "mode_specific_blocker": bool(blocker and readiness_mode in {"PREOPEN_BUILD", "PREOPEN_ADMISSION", "AFTER_HOURS_CLOSURE", "HISTORICAL_REPLAY"}),
        "broker_event_source": broker_event_source,
        "broker_event_day_used": event_log_day_used,
        "broker_event_freshness_status": broker_event_freshness_status,
        "carry_forward_allowed": carry_forward_allowed,
        "carry_forward_reason": "PREOPEN_T_MINUS_1_BROKER_EVENT_LOG_ALLOWED" if broker_event_source == "CARRY_FORWARD" else "",
        "account_identity": {
            "expected_account": ctx.ib_account,
            "observed_accounts": managed,
            "match": ctx.ib_account in managed,
        },
        "account_values": account_values,
        "positions": positions,
        "freshness": {
            "freshness_seconds": float(freshness_seconds),
            "broker_events_fresh": log_status == "PRESENT",
            "current_day_events": bool(latest_event_at.startswith(ctx.day_utc)),
        },
        "capital_supply_export": {
            "usable_for_capital_supply": capital_usable,
            "cash_total_cents": account_values["total_cash_value_cents"] if capital_usable else None,
            "net_liquidation_cents": account_values["net_liquidation_cents"] if capital_usable else None,
            "trust_level": "HIGH",
        },
        "execution_readiness_export": {
            "usable_for_execution_readiness": execution_usable,
            "account_visible": ctx.ib_account in managed,
            "positions_complete": "positionEnd" in event_types,
            "broker_events_fresh": log_status == "PRESENT",
        },
        "operator_next_action": _operator_action(blocker),
        "runtime_resilience_authority_path": str(runtime_path),
        "runtime_resilience_status": str(runtime_resilience.get("status") or "UNKNOWN").strip().upper(),
        "runtime_resilience_blocker": str(runtime_resilience.get("canonical_blocker") or "").strip().upper(),
    }


def run_broker_supply_v1(day_utc: str, environment: str, truth_root: str = "", freshness_seconds: float = 300.0) -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_broker_supply(ctx, freshness_seconds=freshness_seconds)
    path = broker_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    previous = _read_json(path)
    if previous and str(previous.get("day_utc") or "") != ctx.day_utc:
        raise SystemExit(f"FAIL: WRONG_DAY_BROKER_SUPPLY_COLLISION: {path}")
    input_paths = [
        _probe_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc),
        payload.get("event_log", {}).get("path") if isinstance(payload.get("event_log"), dict) else "",
        payload.get("readiness_authority_path", ""),
        payload.get("runtime_resilience_authority_path", ""),
    ]
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_broker_supply_v1.py",
        producer_command=f"python3 ops/tools/run_broker_supply_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}",
        input_artifacts=input_paths,
        output_artifacts=[path],
        schema_versions={"broker_supply": SCHEMA_VERSION},
    )
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_broker_supply_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--freshness_seconds", type=float, default=300.0)
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment or "PAPER").strip().upper()
    path, payload = run_broker_supply_v1(day_utc, environment, str(args.truth_root or ""), float(args.freshness_seconds))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "broker_supply_path": str(path)}, sort_keys=True))
    return 0 if payload.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
