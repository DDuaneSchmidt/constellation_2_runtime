#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.run_market_data_supply_v1 import (
    _latest_snapshot_for_symbol,
    _parse_iso,
    market_data_supply_path,
)
from ops.tools.run_intent_arbitration_v1 import selected_intent_pointer_path
from constellation_2.common.trading_day_readiness_authority_v1 import read_or_evaluate_trading_day_readiness_authority_v1

SCHEMA_VERSION = "market_open_data_gate.v1"
REFRESHABLE_BLOCKERS = {
    "OPTIONS_SNAPSHOT_STALE",
    "OPTIONS_SNAPSHOT_CAPTURE_FAILED",
    "OPTIONS_QUOTES_MISSING",
    "OPTIONS_QUOTES_MISSING_BID_ASK",
    "OPTIONS_DELAYED_QUOTES_NOT_RETURNED_BY_IB",
    "OPTIONS_FRESHNESS_CERTIFICATE_MISSING",
}
ALLOWED_BLOCKERS = {
    "MARKET_NOT_OPEN",
    "MARKET_CLOSED",
    "OPTIONS_SNAPSHOT_STALE",
    "OPTIONS_SNAPSHOT_CAPTURE_FAILED",
    "OPTIONS_QUOTES_MISSING_BID_ASK",
    "OPTIONS_DELAYED_QUOTES_NOT_RETURNED_BY_IB",
    "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
    "OPTIONS_FRESHNESS_CERTIFICATE_MISSING",
    "OPTIONS_SNAPSHOT_SYMBOL_MISMATCH",
}


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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def market_open_data_gate_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "market_open_data_gate_v1" / day_utc / "market_open_data_gate.v1.json").resolve()


def _market_session_state(now_utc: datetime | None = None) -> str:
    now = now_utc or datetime.now(UTC)
    local = now.astimezone(ZoneInfo("America/New_York"))
    if local.weekday() >= 5:
        return "NON_TRADING_DAY"
    minutes = local.hour * 60 + local.minute
    if minutes < (9 * 60 + 30):
        return "PRE_MARKET"
    if minutes < (16 * 60):
        return "REGULAR"
    return "AFTER_HOURS"


def _run_market_data_supply(ctx: bod.BodContext) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "ops/tools/run_market_data_supply_v1.py",
        "--day_utc",
        ctx.day_utc,
        "--environment",
        ctx.environment,
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False, timeout=300)
    return {
        "command": " ".join(cmd),
        "exit_code": int(proc.returncode),
        "stdout_summary": str(proc.stdout or "").strip()[-1200:],
        "stderr_summary": str(proc.stderr or "").strip()[-1200:],
    }


def _run_capture(ctx: bod.BodContext, instrument: str) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "ops/tools/run_options_chain_snapshot_required_day_v1.py",
        "--day_utc",
        ctx.day_utc,
        "--truth_root",
        str(ctx.execution_root),
        "--symbol",
        instrument,
        "--symbols_from_intents",
        "NO",
    ]
    env = dict(os.environ)
    env["C2_TRUTH_ROOT"] = str(ctx.execution_root)
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False, env=env, timeout=120)
    blocker = ""
    snapshot_path = ""
    cert_path = ""
    try:
        payload = json.loads(str(proc.stdout or "").splitlines()[-1])
    except Exception:
        payload = {}
    if isinstance(payload, dict):
        results = payload.get("results")
        if isinstance(results, list) and results:
            first = results[0] if isinstance(results[0], dict) else {}
            blocker = str(first.get("reason_code") or "").strip()
            snapshot_path = str(first.get("path") or "")
            cert_path = str(first.get("freshness_certificate_path") or "")
        else:
            blocker = str(payload.get("canonical_blocker") or payload.get("reason_code") or "").strip()
    if proc.returncode != 0 and not blocker:
        blocker = "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
    if blocker == "OPTIONS_QUOTES_MISSING":
        blocker = "OPTIONS_QUOTES_MISSING_BID_ASK"
    if blocker not in ALLOWED_BLOCKERS and blocker:
        blocker = "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
    return {
        "instrument": instrument,
        "command": " ".join(cmd),
        "status": "PASS" if proc.returncode == 0 else "BLOCKED",
        "blocker": blocker,
        "snapshot_path": snapshot_path,
        "freshness_certificate_path": cert_path,
        "exit_code": int(proc.returncode),
        "stdout_summary": str(proc.stdout or "").strip()[-1200:],
        "stderr_summary": str(proc.stderr or "").strip()[-1200:],
    }


def _root_instrument(supply: dict[str, Any]) -> str:
    for row in supply.get("requirements") or []:
        if not isinstance(row, dict):
            continue
        instrument = str(row.get("instrument") or "").strip().upper()
        if instrument:
            return instrument
    return ""


def _selected_intent_state(ctx: bod.BodContext) -> tuple[str, str]:
    pointer = _read_json(selected_intent_pointer_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc))
    status = str(pointer.get("status") or "").strip().upper()
    selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    symbol = str(selected.get("symbol") or "").strip().upper()
    intent_path = str(selected.get("intent_path") or "").strip()
    if intent_path:
        payload = _read_json(Path(intent_path).expanduser().resolve())
        underlying = payload.get("underlying") if isinstance(payload.get("underlying"), dict) else {}
        symbol = str(underlying.get("symbol") or payload.get("symbol") or symbol).strip().upper()
    return status, symbol


def _has_bid_ask(contract: dict[str, Any]) -> bool:
    if contract.get("bid") not in (None, "") and contract.get("ask") not in (None, ""):
        return True
    if contract.get("delayed_bid") not in (None, "") and contract.get("delayed_ask") not in (None, ""):
        return True
    quote = contract.get("quote")
    if isinstance(quote, dict):
        return quote.get("bid") not in (None, "") and quote.get("ask") not in (None, "")
    return False


def _snapshot_age_seconds(snapshot: dict[str, Any], now_utc: datetime) -> int | None:
    as_of = _parse_iso(snapshot.get("as_of_utc"))
    if as_of is None:
        underlying = snapshot.get("underlying") if isinstance(snapshot.get("underlying"), dict) else {}
        as_of = _parse_iso(underlying.get("spot_as_of_utc"))
    if as_of is None:
        return None
    return max(0, int((now_utc - as_of).total_seconds()))


def _validate_current_snapshot(ctx: bod.BodContext, instrument: str, now_utc: datetime) -> dict[str, Any]:
    snapshot_path, cert_path, snapshot, cert = _latest_snapshot_for_symbol(
        execution_root=ctx.execution_root,
        day_utc=ctx.day_utc,
        instrument=instrument,
    )
    result: dict[str, Any] = {
        "instrument": instrument,
        "snapshot_path": str(snapshot_path or ""),
        "freshness_certificate_path": str(cert_path or ""),
        "snapshot_age_seconds": _snapshot_age_seconds(snapshot, now_utc) if snapshot else None,
        "quote_count": 0,
        "blocker": "",
        "options_snapshot_symbol": "",
    }
    if snapshot_path is None or not snapshot:
        result["blocker"] = "OPTIONS_SNAPSHOT_STALE"
        return result
    expected_root = (ctx.execution_root / "options_chain_snapshot_v1" / ctx.day_utc).resolve()
    if not str(snapshot_path).startswith(str(expected_root)):
        result["blocker"] = "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
        return result
    underlying = snapshot.get("underlying") if isinstance(snapshot.get("underlying"), dict) else {}
    observed_symbol = str(underlying.get("symbol") or snapshot.get("symbol") or "").strip().upper()
    result["options_snapshot_symbol"] = observed_symbol
    if observed_symbol != instrument.upper():
        result["blocker"] = "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
        return result
    if underlying.get("spot_price") in (None, "") and underlying.get("spot") in (None, "") and snapshot.get("spot_price") in (None, ""):
        result["blocker"] = "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
        return result
    if not str(snapshot.get("as_of_utc") or "").strip() or not str(underlying.get("spot_as_of_utc") or "").strip():
        result["blocker"] = "OPTIONS_SNAPSHOT_STALE"
        return result
    contracts = snapshot.get("contracts")
    if not isinstance(contracts, list) or not contracts:
        result["blocker"] = "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
        return result
    quote_count = sum(1 for row in contracts if isinstance(row, dict) and _has_bid_ask(row))
    result["quote_count"] = quote_count
    if quote_count <= 0:
        result["blocker"] = "OPTIONS_QUOTES_MISSING_BID_ASK"
        return result
    if cert_path is None or not cert_path.exists() or not cert:
        result["blocker"] = "OPTIONS_FRESHNESS_CERTIFICATE_MISSING"
        return result
    valid_until = _parse_iso(cert.get("valid_until_utc"))
    if valid_until is None or valid_until < now_utc:
        result["blocker"] = "OPTIONS_SNAPSHOT_STALE"
        return result
    return result


def _blocker_from_supply(supply: dict[str, Any]) -> str:
    blocker = str(supply.get("canonical_blocker") or "").strip()
    if blocker in {"OPTIONS_QUOTES_MISSING", "OPTIONS_QUOTES_UNAVAILABLE_OUTSIDE_MARKET_HOURS"}:
        return "OPTIONS_QUOTES_MISSING_BID_ASK"
    return blocker


def build_market_open_data_gate(ctx: bod.BodContext) -> dict[str, Any]:
    generated_at = _now_iso()
    now_utc = datetime.now(UTC)
    readiness_path, readiness = read_or_evaluate_trading_day_readiness_authority_v1(
        target_day=ctx.day_utc,
        truth_root=ctx.truth_root,
        execution_root=ctx.execution_root,
        environment=ctx.environment,
    )
    readiness_mode = str(readiness.get("readiness_mode") or "").strip().upper()
    selected_intent_status, selected_instrument = _selected_intent_state(ctx)
    symbol_diagnostics: dict[str, Any] = {
        "selected_intent_status": selected_intent_status,
        "selected_intent_symbol": selected_instrument,
        "required_options_symbol": selected_instrument if bool(readiness.get("requires_same_day_options_snapshot") is True) else "",
        "options_snapshot_symbol": "",
        "symbol_source": "SELECTED_INTENT" if selected_instrument else "NONE",
        "stale_default_symbol_detected": False,
    }
    if not bool(readiness.get("requires_same_day_options_snapshot") is True):
        status = "PASS" if readiness_mode in {"AFTER_HOURS_CLOSURE", "HISTORICAL_REPLAY"} else "PENDING"
        blocker = "" if status == "PASS" else "MARKET_NOT_OPEN"
        return {
            "schema_id": "market_open_data_gate",
            "schema_version": SCHEMA_VERSION,
            "day_utc": ctx.day_utc,
            "environment": ctx.environment,
            "generated_at_utc": generated_at,
            "market_session_state": str(readiness.get("session_state") or ""),
            "status": status,
            "canonical_blocker": blocker,
            "readiness_authority_path": str(readiness_path),
            "readiness_mode": readiness_mode,
            "evidence_policy_used": readiness.get("evidence_policy") if isinstance(readiness.get("evidence_policy"), dict) else {},
            "carry_forward_source_used": "",
            "mode_specific_blocker": bool(blocker),
            "market_data_supply_path": str(market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)),
            "command_result": {},
            "snapshot_path": "",
            "freshness_certificate_path": "",
            "snapshot_age_seconds": None,
            "capture_attempted_by_gate": False,
            "capture_result": {},
            **symbol_diagnostics,
            "operator_next_action": str(readiness.get("operator_next_action") or ""),
        }
    session_state = _market_session_state()
    supply_path = market_data_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    command_result: dict[str, Any] = {}
    capture_result: dict[str, Any] = {}
    capture_attempted = False
    instrument = selected_instrument
    snapshot_validation: dict[str, Any] = {}
    if selected_intent_status != "SELECTED":
        status = "PASS"
        blocker = ""
        action = ""
        snapshot_validation = {
            "blocker": "",
            "snapshot_path": "",
            "freshness_certificate_path": "",
            "snapshot_age_seconds": None,
        }
    elif session_state in {"PRE_MARKET", "NON_TRADING_DAY"}:
        status = "PENDING"
        blocker = "MARKET_NOT_OPEN"
        action = "Rerun market-open data gate after 09:30 ET during regular US options market hours."
    elif session_state == "AFTER_HOURS":
        status = "BLOCKED"
        blocker = "MARKET_CLOSED"
        action = "Rerun market-open data gate during regular US options market hours; submit-time quote freshness is not evaluated after market close."
    else:
        command_result = _run_market_data_supply(ctx)
        supply = _read_json(supply_path)
        supply_instrument = _root_instrument(supply)
        if selected_instrument:
            instrument = selected_instrument
            if supply_instrument and supply_instrument != selected_instrument:
                symbol_diagnostics["stale_default_symbol_detected"] = True
        else:
            instrument = supply_instrument
            symbol_diagnostics["symbol_source"] = "MARKET_DATA_SUPPLY" if supply_instrument else "NONE"
        symbol_diagnostics["required_options_symbol"] = instrument
        if not instrument:
            status = "BLOCKED"
            blocker = "SELECTED_INTENT_SYMBOL_MISSING"
            action = "Resolve selected intent symbol before evaluating market-open option data."
            snapshot_validation = {
                "blocker": blocker,
                "snapshot_path": "",
                "freshness_certificate_path": "",
                "snapshot_age_seconds": None,
            }
            return {
                "schema_id": "market_open_data_gate",
                "schema_version": SCHEMA_VERSION,
                "day_utc": ctx.day_utc,
                "environment": ctx.environment,
                "generated_at_utc": generated_at,
                "market_session_state": session_state,
                "status": status,
                "canonical_blocker": blocker,
                "market_data_supply_path": str(supply_path),
                "command_result": command_result,
                "snapshot_path": "",
                "freshness_certificate_path": "",
                "snapshot_age_seconds": None,
                "capture_attempted_by_gate": False,
                "capture_result": {},
                **symbol_diagnostics,
                "operator_next_action": action,
            }
        snapshot_validation = _validate_current_snapshot(ctx, instrument, now_utc)
        symbol_diagnostics["options_snapshot_symbol"] = str(snapshot_validation.get("options_snapshot_symbol") or "")
        supply_status = str(supply.get("status") or "").strip().upper()
        blocker = _blocker_from_supply(supply) or str(snapshot_validation.get("blocker") or "").strip()
        if symbol_diagnostics["stale_default_symbol_detected"] and blocker in {"", "OPTIONS_SNAPSHOT_STALE"}:
            blocker = "OPTIONS_SNAPSHOT_SYMBOL_MISMATCH"
        if blocker in REFRESHABLE_BLOCKERS:
            capture_attempted = True
            capture_result = _run_capture(ctx, instrument)
            command_result = {
                "initial_market_data_supply": command_result,
                "capture": capture_result,
                "post_capture_market_data_supply": _run_market_data_supply(ctx),
            }
            supply = _read_json(supply_path)
            supply_status = str(supply.get("status") or "").strip().upper()
            snapshot_validation = _validate_current_snapshot(ctx, instrument, datetime.now(UTC))
            symbol_diagnostics["options_snapshot_symbol"] = str(snapshot_validation.get("options_snapshot_symbol") or "")
            blocker = _blocker_from_supply(supply) or str(snapshot_validation.get("blocker") or "").strip()
            if symbol_diagnostics["stale_default_symbol_detected"] and blocker in {"", "OPTIONS_SNAPSHOT_STALE"}:
                blocker = "OPTIONS_SNAPSHOT_SYMBOL_MISMATCH"
            if capture_result.get("status") != "PASS" and blocker in {"", "OPTIONS_SNAPSHOT_STALE", "OPTIONS_SNAPSHOT_CAPTURE_FAILED"}:
                blocker = str(capture_result.get("blocker") or "") or "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
        if blocker == "OPTIONS_QUOTES_MISSING":
            blocker = "OPTIONS_QUOTES_MISSING_BID_ASK"
        if supply_status == "PASS" and not snapshot_validation.get("blocker") and not blocker:
            status = "PASS"
        else:
            status = "BLOCKED"
            blocker = blocker or str(snapshot_validation.get("blocker") or "") or "OPTIONS_SNAPSHOT_CAPTURE_FAILED"
        action = "" if status == "PASS" else f"Capture current {instrument} option bid/ask quotes and freshness certificate during regular market hours, then rerun this gate."
    return {
        "schema_id": "market_open_data_gate",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": generated_at,
        "market_session_state": session_state,
        "status": status,
        "canonical_blocker": blocker,
        "readiness_authority_path": str(readiness_path),
        "readiness_mode": readiness_mode,
        "evidence_policy_used": readiness.get("evidence_policy") if isinstance(readiness.get("evidence_policy"), dict) else {},
        "carry_forward_source_used": "",
        "mode_specific_blocker": bool(blocker and readiness_mode in {"PREOPEN_BUILD", "PREOPEN_ADMISSION", "AFTER_HOURS_CLOSURE", "HISTORICAL_REPLAY"}),
        "market_data_supply_path": str(supply_path),
        "command_result": command_result,
        "snapshot_path": str(snapshot_validation.get("snapshot_path") or ""),
        "freshness_certificate_path": str(snapshot_validation.get("freshness_certificate_path") or ""),
        "snapshot_age_seconds": snapshot_validation.get("snapshot_age_seconds"),
        "capture_attempted_by_gate": capture_attempted,
        "capture_result": capture_result,
        **symbol_diagnostics,
        "operator_next_action": action,
    }


def run_market_open_data_gate_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_market_open_data_gate(ctx)
    path = market_open_data_gate_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    previous = _read_json(path)
    if previous and str(previous.get("day_utc") or "") != ctx.day_utc:
        raise SystemExit(f"FAIL: WRONG_DAY_MARKET_OPEN_DATA_GATE_COLLISION: {path}")
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_market_open_data_gate_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment or "PAPER").strip().upper()
    path, payload = run_market_open_data_gate_v1(day_utc, environment, str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "market_open_data_gate_path": str(path)}, sort_keys=True))
    return 0 if payload.get("status") in {"PASS", "PENDING"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
