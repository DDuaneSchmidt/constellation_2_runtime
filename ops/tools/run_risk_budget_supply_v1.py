#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod

SCHEMA_VERSION = "risk_budget_supply.v1"
POLICY_ID = "C2_CAPITAL_RISK_ENVELOPE_CONTRACT_V2"
POLICY_SOURCE = (REPO_ROOT / "governance/05_CONTRACTS/C2/capital_risk_envelope_v2.contract.md").resolve()
MAX_ACCOUNT_RISK_PCT = Decimal("0.020000")

ALLOWED_BLOCKERS = {
    "CAPITAL_SUPPLY_MISSING",
    "CAPITAL_SUPPLY_BLOCKED",
    "NAV_BASIS_MISSING",
    "NAV_BASIS_INVALID",
    "RISK_BUDGET_POLICY_MISSING",
    "INTENT_BUDGET_MISSING",
    "INTENT_BUDGET_COMPUTE_FAILED",
    "CAPITAL_RISK_ENVELOPE_BLOCKED",
    "RISK_SIZING_EXPORT_MISSING",
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


def _int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def risk_budget_supply_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "risk_budget_supply_v1" / day_utc / "risk_budget_supply.v1.json").resolve()


def _capital_supply_path(ctx: bod.BodContext) -> Path:
    return (ctx.truth_root / "reports" / "capital_supply_v1" / ctx.day_utc / "capital_supply.v1.json").resolve()


def _source_to_nav_basis_source(source_type: str) -> str:
    if source_type == "BROKER_ACCOUNT":
        return "BROKER_SUPPLY"
    if source_type == "OPERATOR_STATEMENT":
        return "OPERATOR_STATEMENT"
    if source_type == "BOOTSTRAP_SEED":
        return "BOOTSTRAP_SEED"
    return ""


def _load_capital_supply(ctx: bod.BodContext) -> tuple[dict[str, Any], str]:
    path = _capital_supply_path(ctx)
    payload = _read_json(path)
    if not payload:
        return {}, "CAPITAL_SUPPLY_MISSING"
    if str(payload.get("day_utc") or "").strip() != ctx.day_utc:
        return payload, "CAPITAL_SUPPLY_MISSING"
    if str(payload.get("status") or "").strip().upper() == "BLOCKED":
        return payload, "CAPITAL_SUPPLY_BLOCKED"
    return payload, ""


def _nav_basis(ctx: bod.BodContext, capital_supply: dict[str, Any]) -> tuple[dict[str, Any], str]:
    selected = capital_supply.get("selected_source") if isinstance(capital_supply.get("selected_source"), dict) else {}
    if not selected:
        return {"valid": False}, "NAV_BASIS_MISSING"
    source_type = str(selected.get("source_type") or "").strip()
    nav = _int(selected.get("net_liquidation_cents"))
    cash = _int(selected.get("cash_total_cents"))
    freshness = str(selected.get("freshness_utc") or capital_supply.get("generated_at_utc") or "").strip()
    trust = str(selected.get("trust_level") or "").strip()
    basis = {
        "source": _source_to_nav_basis_source(source_type),
        "net_liquidation_cents": nav,
        "cash_total_cents": cash,
        "trust_level": trust,
        "freshness_utc": freshness,
        "valid": False,
    }
    if basis["source"] == "":
        return basis, "NAV_BASIS_MISSING"
    if not isinstance(nav, int) or nav <= 0 or not isinstance(cash, int) or cash < 0 or not trust:
        return basis, "NAV_BASIS_INVALID"
    if freshness and not freshness.startswith(ctx.day_utc):
        return basis, "NAV_BASIS_INVALID"
    basis["valid"] = True
    return basis, ""


def _budget_policy() -> tuple[dict[str, Any], str]:
    if not POLICY_SOURCE.exists() or not POLICY_SOURCE.is_file():
        return {}, "RISK_BUDGET_POLICY_MISSING"
    return (
        {
            "policy_id": POLICY_ID,
            "max_account_risk_pct": f"{MAX_ACCOUNT_RISK_PCT:.6f}",
            "per_intent_target_pct": "ACTIVE_INTENT_TARGET",
            "source": str(POLICY_SOURCE),
        },
        "",
    )


def _json_files(root: Path) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return sorted(path.resolve() for path in root.glob("*.json") if path.is_file())


def _intent_hash(path: Path, payload: dict[str, Any]) -> str:
    return str(payload.get("intent_hash") or payload.get("canonical_json_hash") or path.name.split(".", 1)[0]).strip()


def _active_intents(ctx: bod.BodContext) -> list[tuple[Path, dict[str, Any]]]:
    roots = [ctx.execution_root, ctx.truth_root] if ctx.execution_root != ctx.truth_root else [ctx.truth_root]
    seen: set[str] = set()
    rows: list[tuple[Path, dict[str, Any]]] = []
    for root in roots:
        for path in _json_files(root / "intents_v1" / "snapshots" / ctx.day_utc):
            payload = _read_json(path)
            if not payload or str(payload.get("day_utc") or ctx.day_utc) != ctx.day_utc:
                continue
            key = _intent_hash(path, payload)
            if key in seen:
                continue
            seen.add(key)
            rows.append((path, payload))
    return rows


def _instrument(payload: dict[str, Any]) -> str:
    underlying = payload.get("underlying") if isinstance(payload.get("underlying"), dict) else {}
    return str(payload.get("instrument") or payload.get("symbol") or underlying.get("symbol") or "").strip()


def _target_pct(payload: dict[str, Any]) -> Decimal | None:
    constraints = payload.get("constraints") if isinstance(payload.get("constraints"), dict) else {}
    return _decimal(payload.get("target_notional_pct") or constraints.get("max_risk_pct"))


def _intent_budgets(ctx: bod.BodContext, nav_total_cents: int) -> tuple[list[dict[str, Any]], str]:
    budgets: list[dict[str, Any]] = []
    blocker = ""
    for path, intent in _active_intents(ctx):
        target = _target_pct(intent)
        intent_id = str(intent.get("intent_id") or _intent_hash(path, intent)).strip()
        instrument = _instrument(intent)
        row = {
            "intent_id": intent_id,
            "instrument": instrument,
            "target_pct": str(target) if target is not None else "",
            "allowed_risk_cents": None,
            "nav_total_cents": nav_total_cents,
            "status": "PASS",
            "blocker": "",
            "source_path": str(path),
        }
        if target is None:
            row["status"] = "BLOCKED"
            row["blocker"] = "INTENT_BUDGET_MISSING"
            blocker = blocker or "INTENT_BUDGET_MISSING"
        elif target < 0 or target > MAX_ACCOUNT_RISK_PCT:
            row["status"] = "BLOCKED"
            row["blocker"] = "INTENT_BUDGET_COMPUTE_FAILED"
            blocker = blocker or "INTENT_BUDGET_COMPUTE_FAILED"
        else:
            row["allowed_risk_cents"] = int((Decimal(nav_total_cents) * target).to_integral_value(rounding=ROUND_FLOOR))
        budgets.append(row)
    return budgets, blocker


def _run_capital_risk_envelope(ctx: bod.BodContext) -> tuple[dict[str, Any], str]:
    cmd = [
        sys.executable,
        "ops/tools/run_c2_capital_risk_envelope_gate_v2.py",
        "--out_day_utc",
        ctx.day_utc,
        "--input_day_utc",
        ctx.day_utc,
        "--produced_utc",
        f"{ctx.day_utc}T00:00:00Z",
        "--truth_root",
        str(ctx.execution_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False, timeout=60)
    path = (ctx.execution_root / "reports" / "capital_risk_envelope_v2" / ctx.day_utc / "capital_risk_envelope.v2.json").resolve()
    payload = _read_json(path)
    status = str(payload.get("status") or "").strip().upper()
    result = {
        "command": " ".join(cmd),
        "path": str(path),
        "exists": path.exists(),
        "status": status or "MISSING",
        "exit_code": int(proc.returncode),
        "reason_codes": payload.get("reason_codes") if isinstance(payload.get("reason_codes"), list) else [],
        "stdout_summary": str(proc.stdout or "").strip()[-1200:],
        "stderr_summary": str(proc.stderr or "").strip()[-1200:],
    }
    return result, "" if proc.returncode == 0 and status == "PASS" else "CAPITAL_RISK_ENVELOPE_BLOCKED"


def _operator_action(blocker: str, ctx: bod.BodContext, capital_supply: dict[str, Any] | None = None) -> str:
    capital_blocker = ""
    if isinstance(capital_supply, dict):
        capital_blocker = str(capital_supply.get("canonical_blocker") or "").strip()
    mapping = {
        "CAPITAL_SUPPLY_MISSING": "Run python3 ops/tools/run_capital_supply_v1.py for the current day before risk budget supply.",
        "CAPITAL_SUPPLY_BLOCKED": f"Resolve Capital Supply blocker {capital_blocker or '<unknown>'}, then rerun run_risk_budget_supply_v1.py.",
        "NAV_BASIS_MISSING": "Regenerate Capital Supply with selected current-day NAV/cash evidence before risk budgeting.",
        "NAV_BASIS_INVALID": "Provide valid positive current-day NAV/cash evidence before risk budgeting.",
        "RISK_BUDGET_POLICY_MISSING": "Restore governed capital_risk_envelope_v2 policy evidence before risk budgeting.",
        "INTENT_BUDGET_MISSING": "Add target_notional_pct or constraints.max_risk_pct to each active current-day intent.",
        "INTENT_BUDGET_COMPUTE_FAILED": "Correct active intent budget percentage so it is non-negative and within governed policy.",
        "CAPITAL_RISK_ENVELOPE_BLOCKED": "Resolve capital risk envelope reason codes, then rerun run_risk_budget_supply_v1.py.",
        "RISK_SIZING_EXPORT_MISSING": "Regenerate risk budget supply after NAV basis, policy, intent budgets, and capital envelope pass.",
    }
    return mapping.get(blocker, f"Resolve {blocker}, then rerun python3 ops/tools/run_risk_budget_supply_v1.py --day_utc {ctx.day_utc} --environment PAPER." if blocker else "")


def build_risk_budget_supply(ctx: bod.BodContext) -> dict[str, Any]:
    capital_supply, blocker = _load_capital_supply(ctx)
    capital_path = _capital_supply_path(ctx)
    nav_basis: dict[str, Any] = {}
    policy: dict[str, Any] = {}
    budgets: list[dict[str, Any]] = []
    envelope: dict[str, Any] = {}
    export: dict[str, Any] = {
        "usable_for_risk_sizing": False,
        "nav_total_cents": None,
        "intent_budgets_available": False,
        "budget_source_path": str(risk_budget_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)),
    }
    status = "BLOCKED" if blocker else "PASS"

    if not blocker:
        nav_basis, blocker = _nav_basis(ctx, capital_supply)
        if blocker:
            status = "BLOCKED"
    if not blocker:
        policy, blocker = _budget_policy()
        if blocker:
            status = "BLOCKED"
    if not blocker:
        nav_total = _int(nav_basis.get("net_liquidation_cents"))
        if not isinstance(nav_total, int):
            blocker = "NAV_BASIS_INVALID"
            status = "BLOCKED"
        else:
            budgets, blocker = _intent_budgets(ctx, nav_total)
            if blocker:
                status = "BLOCKED"
    if not blocker:
        envelope, blocker = _run_capital_risk_envelope(ctx)
        if blocker:
            status = "BLOCKED"
    if not blocker:
        nav_total = _int(nav_basis.get("net_liquidation_cents"))
        export = {
            "usable_for_risk_sizing": True,
            "nav_total_cents": nav_total,
            "intent_budgets_available": all(row.get("status") == "PASS" for row in budgets),
            "budget_source_path": str(risk_budget_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)),
        }

    canonical_blocker = blocker if blocker in ALLOWED_BLOCKERS else ("RISK_SIZING_EXPORT_MISSING" if blocker else "")
    return {
        "schema_id": "risk_budget_supply",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": _now_iso(),
        "status": status,
        "canonical_blocker": canonical_blocker,
        "capital_supply_path": str(capital_path),
        "capital_supply_result": {
            "status": str(capital_supply.get("status") or ("MISSING" if not capital_supply else "")).strip().upper(),
            "canonical_blocker": str(capital_supply.get("canonical_blocker") or "").strip(),
        },
        "nav_basis": nav_basis,
        "budget_policy": policy,
        "intent_budgets": budgets,
        "capital_risk_envelope": envelope,
        "risk_sizing_export": export,
        "operator_next_action": _operator_action(canonical_blocker, ctx, capital_supply),
    }


def run_risk_budget_supply_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_risk_budget_supply(ctx)
    path = risk_budget_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    previous = _read_json(path)
    if previous and str(previous.get("day_utc") or "") != ctx.day_utc:
        raise SystemExit(f"FAIL: WRONG_DAY_RISK_BUDGET_SUPPLY_COLLISION: {path}")
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_risk_budget_supply_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment or "PAPER").strip().upper()
    path, payload = run_risk_budget_supply_v1(day_utc, environment, str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "risk_budget_supply_path": str(path)}, sort_keys=True))
    return 0 if payload.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
