#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_operator_statement_path,
    resolve_paper_capital_seed_path,
)
from constellation_2.common.trading_day_readiness_authority_v1 import read_or_evaluate_trading_day_readiness_authority_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod

SCHEMA_VERSION = "capital_supply.v1"
ALLOWED_BLOCKERS = {
    "CAPITAL_SOURCE_MISSING",
    "BROKER_NAV_EVIDENCE_MISSING",
    "NAV_TOTAL_MISSING_OR_INVALID",
    "CAPITAL_EVIDENCE_STALE",
    "OPERATOR_STATEMENT_REQUIRED",
    "BOOTSTRAP_CAPITAL_ONLY",
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


def _runtime_resilience_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "runtime_resilience_authority_v1" / day_utc / "runtime_resilience_authority.v1.json").resolve()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def _money_to_cents(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value * 100))
    text = str(value).strip()
    if not text:
        return None
    try:
        if "." in text:
            return int((Decimal(text) * Decimal("100")).quantize(Decimal("1")))
        return int(text)
    except (InvalidOperation, ValueError):
        return None


def _dollars_to_cents(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int((Decimal(str(value).strip()) * Decimal("100")).quantize(Decimal("1")))
    except (InvalidOperation, ValueError):
        return None


def _iter_dicts(value: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(value, dict):
        out.append(value)
        for item in value.values():
            out.extend(_iter_dicts(item))
    elif isinstance(value, list):
        for item in value:
            out.extend(_iter_dicts(item))
    return out


def capital_supply_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "capital_supply_v1" / day_utc / "capital_supply.v1.json").resolve()


def _broker_supply_path(ctx: bod.BodContext) -> Path:
    return (ctx.truth_root / "reports" / "broker_supply_v1" / ctx.day_utc / "broker_supply.v1.json").resolve()


def _source_row(
    *,
    source_type: str,
    source_path: Path | None,
    status: str,
    account: str,
    cash_total_cents: int | None,
    net_liquidation_cents: int | None,
    currency: str,
    freshness_utc: str,
    trust_level: str,
    blocker: str,
) -> dict[str, Any]:
    return {
        "source_type": source_type,
        "source_path": str(source_path or ""),
        "status": status,
        "account": account,
        "cash_total_cents": cash_total_cents,
        "net_liquidation_cents": net_liquidation_cents,
        "currency": currency,
        "freshness_utc": freshness_utc,
        "trust_level": trust_level,
        "blocker": blocker,
    }


def _extract_account_values(payload: dict[str, Any]) -> dict[str, Any]:
    nav = payload.get("nav") if isinstance(payload.get("nav"), dict) else {}
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    account = payload.get("account") if isinstance(payload.get("account"), dict) else {}
    values = payload.get("values") if isinstance(payload.get("values"), dict) else {}
    return {
        "account": str(payload.get("account_id") or snapshot.get("account_id") or account.get("account_id") or payload.get("account") or "").strip(),
        "currency": str(payload.get("currency") or snapshot.get("currency") or nav.get("currency") or values.get("currency") or "USD").strip(),
        "cash_total_cents": _money_to_cents(
            payload.get("cash_total_cents")
            or snapshot.get("cash_total_cents")
            or values.get("cash_total_cents")
            or payload.get("TotalCashValue")
            or payload.get("cash_total")
            or nav.get("cash_total")
        ),
        "net_liquidation_cents": _money_to_cents(
            payload.get("net_liquidation_cents")
            or payload.get("nlv_total_cents")
            or snapshot.get("net_liquidation_cents")
            or snapshot.get("nlv_total_cents")
            or values.get("net_liquidation_cents")
            or payload.get("NetLiquidation")
            or payload.get("net_liquidation")
            or payload.get("nlv_total")
            or nav.get("nav_total")
        ),
        "freshness_utc": str(payload.get("observed_at_utc") or snapshot.get("observed_at_utc") or payload.get("produced_utc") or payload.get("generated_at_utc") or "").strip(),
    }


def _broker_snapshot_paths(ctx: bod.BodContext) -> list[Path]:
    roots = [ctx.execution_root, ctx.truth_root] if ctx.execution_root != ctx.truth_root else [ctx.truth_root]
    out: list[Path] = []
    for root in roots:
        out.append((root / "broker_account_snapshot_v1" / ctx.day_utc / "broker_account_snapshot.v1.json").resolve())
        out.append((root / "account_snapshot_v1" / ctx.day_utc / "account_snapshot.v1.json").resolve())
    return out


def _load_broker_source(ctx: bod.BodContext) -> dict[str, Any]:
    broker_supply_path = _broker_supply_path(ctx)
    broker_supply = _read_json(broker_supply_path)
    if broker_supply:
        export = broker_supply.get("capital_supply_export") if isinstance(broker_supply.get("capital_supply_export"), dict) else {}
        values = broker_supply.get("account_values") if isinstance(broker_supply.get("account_values"), dict) else {}
        usable = bool(export.get("usable_for_capital_supply") is True)
        blocker = str(broker_supply.get("canonical_blocker") or "").strip()
        cash = export.get("cash_total_cents") if usable else values.get("total_cash_value_cents")
        nav = export.get("net_liquidation_cents") if usable else values.get("net_liquidation_cents")
        if usable and isinstance(cash, int) and isinstance(nav, int) and nav > 0:
            status, row_blocker = "VALID", ""
        elif blocker in {"BROKER_NAV_EVIDENCE_MISSING", "BROKER_CASH_EVIDENCE_MISSING", "BROKER_VALUES_INVALID"}:
            status, row_blocker = "NULL_VALUES", blocker
        elif blocker:
            status, row_blocker = "MISSING", "BROKER_NAV_EVIDENCE_MISSING"
        else:
            status, row_blocker = "MISSING", "BROKER_NAV_EVIDENCE_MISSING"
        return _source_row(
            source_type="BROKER_ACCOUNT",
            source_path=broker_supply_path,
            status=status,
            account=str(broker_supply.get("account") or ctx.ib_account),
            cash_total_cents=cash if isinstance(cash, int) else None,
            net_liquidation_cents=nav if isinstance(nav, int) else None,
            currency=str(values.get("currency") or "USD"),
            freshness_utc=str(broker_supply.get("generated_at_utc") or ""),
            trust_level=str(export.get("trust_level") or "HIGH"),
            blocker=row_blocker,
        )
    for path in _broker_snapshot_paths(ctx):
        payload = _read_json(path)
        if not payload:
            continue
        values = _extract_account_values(payload)
        account = str(values.get("account") or ctx.ib_account)
        nav = values.get("net_liquidation_cents")
        cash = values.get("cash_total_cents")
        if account and account != ctx.ib_account:
            status, blocker = "NULL_VALUES", "BROKER_NAV_EVIDENCE_MISSING"
        elif not str(values.get("freshness_utc") or "").startswith(ctx.day_utc):
            status, blocker = "STALE", "CAPITAL_EVIDENCE_STALE"
        elif not isinstance(nav, int) or nav <= 0:
            status, blocker = "NULL_VALUES", "NAV_TOTAL_MISSING_OR_INVALID"
        elif not isinstance(cash, int):
            status, blocker = "NULL_VALUES", "BROKER_NAV_EVIDENCE_MISSING"
        else:
            status, blocker = "VALID", ""
        return _source_row(
            source_type="BROKER_ACCOUNT",
            source_path=path,
            status=status,
            account=account,
            cash_total_cents=cash if isinstance(cash, int) else None,
            net_liquidation_cents=nav if isinstance(nav, int) else None,
            currency=str(values.get("currency") or "USD"),
            freshness_utc=str(values.get("freshness_utc") or ""),
            trust_level="HIGH",
            blocker=blocker,
        )
    return _source_row(
        source_type="BROKER_ACCOUNT",
        source_path=None,
        status="MISSING",
        account=ctx.ib_account,
        cash_total_cents=None,
        net_liquidation_cents=None,
        currency="USD",
        freshness_utc="",
        trust_level="HIGH",
        blocker="BROKER_NAV_EVIDENCE_MISSING",
    )


def _load_operator_source(ctx: bod.BodContext) -> dict[str, Any]:
    path = resolve_operator_statement_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
    payload = _read_json(path)
    if not payload:
        return _source_row(source_type="OPERATOR_STATEMENT", source_path=path, status="MISSING", account=ctx.ib_account, cash_total_cents=None, net_liquidation_cents=None, currency="USD", freshness_utc="", trust_level="MEDIUM", blocker="OPERATOR_STATEMENT_REQUIRED")
    values = _extract_account_values(payload)
    nav = values.get("net_liquidation_cents")
    cash = values.get("cash_total_cents")
    if not str(values.get("freshness_utc") or "").startswith(ctx.day_utc):
        status, blocker = "STALE", "CAPITAL_EVIDENCE_STALE"
    elif not isinstance(nav, int) or nav <= 0:
        status, blocker = "NULL_VALUES", "NAV_TOTAL_MISSING_OR_INVALID"
    elif not isinstance(cash, int):
        status, blocker = "NULL_VALUES", "NAV_TOTAL_MISSING_OR_INVALID"
    else:
        status, blocker = "DEGRADED", ""
    return _source_row(source_type="OPERATOR_STATEMENT", source_path=path, status=status, account=str(values.get("account") or ctx.ib_account), cash_total_cents=cash if isinstance(cash, int) else None, net_liquidation_cents=nav if isinstance(nav, int) else None, currency=str(values.get("currency") or "USD"), freshness_utc=str(values.get("freshness_utc") or ""), trust_level="MEDIUM", blocker=blocker)


def _load_seed_source(ctx: bod.BodContext) -> dict[str, Any]:
    path = resolve_paper_capital_seed_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
    payload = _read_json(path)
    if not payload:
        return _source_row(source_type="BOOTSTRAP_SEED", source_path=path, status="MISSING", account=ctx.ib_account, cash_total_cents=None, net_liquidation_cents=None, currency="USD", freshness_utc="", trust_level="LOW", blocker="CAPITAL_SOURCE_MISSING")
    cash = _dollars_to_cents(payload.get("cash_total"))
    nav = _dollars_to_cents(payload.get("nlv_total"))
    if str(payload.get("day_utc") or "") != ctx.day_utc:
        status, blocker = "STALE", "CAPITAL_EVIDENCE_STALE"
    elif not isinstance(nav, int) or nav <= 0:
        status, blocker = "NULL_VALUES", "NAV_TOTAL_MISSING_OR_INVALID"
    elif not isinstance(cash, int):
        status, blocker = "NULL_VALUES", "NAV_TOTAL_MISSING_OR_INVALID"
    else:
        status, blocker = "DEGRADED", "BOOTSTRAP_CAPITAL_ONLY"
    return _source_row(source_type="BOOTSTRAP_SEED", source_path=path, status=status, account=str(payload.get("ib_account") or ctx.ib_account), cash_total_cents=cash, net_liquidation_cents=nav, currency=str(payload.get("currency") or "USD"), freshness_utc=str(payload.get("produced_utc") or f"{ctx.day_utc}T00:00:00Z"), trust_level="LOW", blocker=blocker)


def _select_source(sources: list[dict[str, Any]]) -> tuple[dict[str, Any], str, str]:
    for source_type in ("BROKER_ACCOUNT", "OPERATOR_STATEMENT", "BOOTSTRAP_SEED"):
        row = next((item for item in sources if item.get("source_type") == source_type), {})
        if row.get("status") == "VALID":
            return row, "PASS", ""
        if row.get("status") == "DEGRADED":
            return row, "DEGRADED", "BOOTSTRAP_CAPITAL_ONLY" if source_type == "BOOTSTRAP_SEED" else ""
    blocker = next((str(item.get("blocker") or "") for item in sources if item.get("blocker")), "CAPITAL_SOURCE_MISSING")
    return {}, "BLOCKED", blocker if blocker in ALLOWED_BLOCKERS else "CAPITAL_SOURCE_MISSING"


def _nav_evidence(ctx: bod.BodContext, selected: dict[str, Any]) -> tuple[dict[str, Any], str]:
    selected_nav = selected.get("net_liquidation_cents")
    if (
        selected.get("source_type") == "BROKER_ACCOUNT"
        and "broker_supply_v1" in str(selected.get("source_path") or "")
        and isinstance(selected_nav, int)
        and selected_nav > 0
    ):
        return (
            {
                "path": str(selected.get("source_path") or ""),
                "exists": True,
                "status": "BROKER_SUPPLY_VALID",
                "nav_total_cents": selected_nav,
                "cash_total_cents": selected.get("cash_total_cents"),
                "source_type": selected.get("source_type", ""),
            },
            "",
        )
    path = (ctx.execution_root / "accounting_v2" / "nav" / ctx.day_utc / "nav.v2.json").resolve()
    payload = _read_json(path)
    nav = payload.get("nav") if isinstance(payload.get("nav"), dict) else {}
    result = {"path": str(path), "exists": bool(payload), "status": "MISSING", "nav_total_cents": None, "cash_total_cents": None, "source_type": selected.get("source_type", "")}
    if payload and str(payload.get("day_utc") or "") != ctx.day_utc:
        result["status"] = "STALE"
        return result, "CAPITAL_EVIDENCE_STALE"
    if not payload:
        return result, "BROKER_NAV_EVIDENCE_MISSING"
    nav_total_cents = _dollars_to_cents(nav.get("nav_total"))
    result.update({"nav_total_cents": nav_total_cents, "cash_total_cents": _dollars_to_cents(nav.get("cash_total"))})
    if not isinstance(nav_total_cents, int) or nav_total_cents <= 0:
        result["status"] = "INVALID"
        return result, "NAV_TOTAL_MISSING_OR_INVALID"
    result["status"] = "VALID"
    return result, ""


def _exposure_budget(ctx: bod.BodContext) -> tuple[dict[str, Any], str]:
    path = (ctx.execution_root / "allocation_v1" / "summary" / ctx.day_utc / "summary.json").resolve()
    payload = _read_json(path)
    result = {"path": str(path), "exists": bool(payload), "status": "MISSING", "nav_total_cents": None, "reason_codes": []}
    if not payload:
        return result, "EXPOSURE_BUDGET_MISSING"
    result["reason_codes"] = payload.get("reason_codes") if isinstance(payload.get("reason_codes"), list) else []
    nav_value = None
    for row in _iter_dicts(payload):
        for key in ("nav_total_cents", "exposure_budget_nav_total_cents", "account_net_liquidation_cents"):
            if isinstance(row.get(key), int):
                nav_value = row.get(key)
                break
        if nav_value is not None:
            break
    result["nav_total_cents"] = nav_value
    if not isinstance(nav_value, int) or nav_value <= 0:
        result["status"] = "NAV_MISSING"
        return result, "EXPOSURE_BUDGET_NAV_MISSING"
    result["status"] = "VALID"
    return result, ""


def _run_capital_risk_envelope(ctx: bod.BodContext) -> tuple[dict[str, Any], str]:
    cmd = [sys.executable, "ops/tools/run_c2_capital_risk_envelope_gate_v2.py", "--out_day_utc", ctx.day_utc, "--input_day_utc", ctx.day_utc, "--produced_utc", f"{ctx.day_utc}T00:00:00Z", "--truth_root", str(ctx.execution_root)]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False, timeout=60)
    path = (ctx.execution_root / "reports" / "capital_risk_envelope_v2" / ctx.day_utc / "capital_risk_envelope.v2.json").resolve()
    payload = _read_json(path)
    status = str(payload.get("status") or "").strip().upper()
    result = {"command": " ".join(cmd), "path": str(path), "exists": path.exists(), "status": status or "MISSING", "exit_code": int(proc.returncode), "reason_codes": payload.get("reason_codes") if isinstance(payload.get("reason_codes"), list) else [], "stdout_summary": str(proc.stdout or "").strip()[-1200:], "stderr_summary": str(proc.stderr or "").strip()[-1200:]}
    return result, "" if proc.returncode == 0 and status == "PASS" else "CAPITAL_RISK_ENVELOPE_BLOCKED"


def _operator_action(blocker: str, ctx: bod.BodContext) -> str:
    mapping = {
        "BROKER_NAV_EVIDENCE_MISSING": "Capture current-day IB account summary evidence with NetLiquidation and TotalCashValue for DUO847203, then rerun run_capital_supply_v1.py.",
        "NAV_TOTAL_MISSING_OR_INVALID": "Provide valid positive current-day NAV/accounting evidence before risk sizing.",
        "OPERATOR_STATEMENT_REQUIRED": "Provide current-day operator statement or broker NAV evidence before risk sizing.",
        "BOOTSTRAP_CAPITAL_ONLY": "Replace bootstrap-only capital with broker NAV evidence or explicit operator statement before live-like paper submit.",
    }
    return mapping.get(blocker, f"Resolve {blocker}, then rerun python3 ops/tools/run_capital_supply_v1.py --day_utc {ctx.day_utc} --environment PAPER." if blocker else "")


def build_capital_supply(ctx: bod.BodContext) -> dict[str, Any]:
    readiness_path, readiness = read_or_evaluate_trading_day_readiness_authority_v1(
        target_day=ctx.day_utc,
        truth_root=ctx.truth_root,
        execution_root=ctx.execution_root,
        environment=ctx.environment,
    )
    sources = [_load_broker_source(ctx), _load_operator_source(ctx), _load_seed_source(ctx)]
    selected, source_status, blocker = _select_source(sources)
    degraded_blocker = blocker if source_status == "DEGRADED" else ""
    if source_status == "DEGRADED":
        blocker = ""
    nav_result: dict[str, Any] = {}
    if not blocker or source_status == "DEGRADED":
        nav_result, nav_blocker = _nav_evidence(ctx, selected)
        if nav_blocker:
            blocker = nav_blocker
            source_status = "BLOCKED"
    status = "PASS" if source_status == "PASS" and not blocker else ("DEGRADED" if source_status == "DEGRADED" and not blocker else "BLOCKED")
    canonical_blocker = degraded_blocker if status == "DEGRADED" else blocker
    broker_supply = _read_json(_broker_supply_path(ctx))
    broker_event_source = str(broker_supply.get("broker_event_source") or "").strip().upper()
    runtime_path = _runtime_resilience_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    runtime_resilience = _read_json(runtime_path)
    return {
        "schema_id": "capital_supply",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": _now_iso(),
        "status": status,
        "canonical_blocker": canonical_blocker,
        "readiness_authority_path": str(readiness_path),
        "readiness_mode": str(readiness.get("readiness_mode") or "").strip().upper(),
        "evidence_policy_used": readiness.get("evidence_policy") if isinstance(readiness.get("evidence_policy"), dict) else {},
        "carry_forward_source_used": str(broker_supply.get("carry_forward_source_used") or "") if broker_event_source == "CARRY_FORWARD" else "",
        "mode_specific_blocker": bool(canonical_blocker and str(readiness.get("readiness_mode") or "").strip().upper() in {"PREOPEN_BUILD", "PREOPEN_ADMISSION", "AFTER_HOURS_CLOSURE", "HISTORICAL_REPLAY"}),
        "capital_sources": sources,
        "selected_source": selected,
        "nav_evidence": nav_result,
        "exposure_budget": {},
        "capital_risk_envelope": {},
        "operator_next_action": _operator_action(canonical_blocker, ctx),
        "runtime_resilience_authority_path": str(runtime_path),
        "runtime_resilience_status": str(runtime_resilience.get("status") or "UNKNOWN").strip().upper(),
        "runtime_resilience_blocker": str(runtime_resilience.get("canonical_blocker") or "").strip().upper(),
    }


def run_capital_supply_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_capital_supply(ctx)
    path = capital_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    previous = _read_json(path)
    if previous and str(previous.get("day_utc") or "") != ctx.day_utc:
        raise SystemExit(f"FAIL: WRONG_DAY_CAPITAL_SUPPLY_COLLISION: {path}")
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_capital_supply_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment or "PAPER").strip().upper()
    path, payload = run_capital_supply_v1(day_utc, environment, str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "capital_supply_path": str(path)}, sort_keys=True))
    return 0 if payload.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
