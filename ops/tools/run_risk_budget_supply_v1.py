#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.trading_day_readiness_authority_v1 import read_or_evaluate_trading_day_readiness_authority_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1

SCHEMA_VERSION = "risk_budget_supply.v1"
POLICY_ID = "C2_CAPITAL_RISK_ENVELOPE_CONTRACT_V2"
POLICY_SOURCE = (REPO_ROOT / "governance/05_CONTRACTS/C2/capital_risk_envelope_v2.contract.md").resolve()
MAX_ACCOUNT_RISK_PCT = Decimal("0.020000")
CAPITAL_RISK_ENVELOPE_PASS_RECHECK_SECONDS = 15.0
CAPITAL_RISK_ENVELOPE_PASS_RECHECK_INTERVAL_SECONDS = 1.0

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


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return "0" * 64
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _file_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


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


def capital_risk_envelope_adapter_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "risk_budget_supply_v1" / day_utc / "capital_risk_envelope_adapter.v1.json").resolve()


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


def _nav_basis(ctx: bod.BodContext, capital_supply: dict[str, Any], readiness: dict[str, Any]) -> tuple[dict[str, Any], str]:
    selected = capital_supply.get("selected_source") if isinstance(capital_supply.get("selected_source"), dict) else {}
    if not selected:
        return {"valid": False}, "NAV_BASIS_MISSING"
    source_type = str(selected.get("source_type") or "").strip()
    nav = _int(selected.get("net_liquidation_cents"))
    cash = _int(selected.get("cash_total_cents"))
    freshness = str(selected.get("freshness_utc") or capital_supply.get("generated_at_utc") or "").strip()
    trust = str(selected.get("trust_level") or "").strip()
    readiness_mode = str(readiness.get("readiness_mode") or "").strip().upper()
    policy = readiness.get("evidence_policy") if isinstance(readiness.get("evidence_policy"), dict) else {}
    carry_forward_source = str(capital_supply.get("carry_forward_source_used") or "").strip()
    t_minus_1_allowed = (
        readiness_mode in {"PREOPEN_BUILD", "PREOPEN_ADMISSION"}
        and str(policy.get("broker_account_truth") or "").strip().upper() == "T_MINUS_1_ALLOWED"
        and bool(carry_forward_source)
        and carry_forward_source.upper() not in {"NONE", "MISSING", "SAME_DAY", "LIVE"}
    )
    freshness_day = freshness[:10] if len(freshness) >= 10 else ""
    basis = {
        "source": _source_to_nav_basis_source(source_type),
        "net_liquidation_cents": nav,
        "cash_total_cents": cash,
        "trust_level": trust,
        "freshness_utc": freshness,
        "freshness_day": freshness_day,
        "freshness_status": "CURRENT" if freshness_day == ctx.day_utc else ("CARRY_FORWARD_T_MINUS_1" if t_minus_1_allowed else "STALE"),
        "carry_forward_allowed": bool(t_minus_1_allowed),
        "carry_forward_reason": "PREOPEN_T_MINUS_1_ACCOUNT_TRUTH_ALLOWED" if t_minus_1_allowed else "",
        "valid": False,
    }
    if basis["source"] == "":
        return basis, "NAV_BASIS_MISSING"
    if not isinstance(nav, int) or nav <= 0 or not isinstance(cash, int) or cash < 0 or not trust:
        return basis, "NAV_BASIS_INVALID"
    if freshness and freshness_day != ctx.day_utc and not t_minus_1_allowed:
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


def _selected_intent_pointer(ctx: bod.BodContext) -> dict[str, Any]:
    roots = [ctx.truth_root]
    if ctx.execution_root != ctx.truth_root:
        roots.append(ctx.execution_root)
    for root in roots:
        pointer_path = (root / "pointers" / "selected_intent_pointer.v1.json").resolve()
        payload = _read_json(pointer_path)
        if not payload or str(payload.get("day_utc") or "").strip() != ctx.day_utc:
            continue
        return payload
    return {}


def _selected_intent_identity(pointer: dict[str, Any]) -> dict[str, str]:
    selected = pointer.get("selected_intent") if isinstance(pointer.get("selected_intent"), dict) else {}
    return {
        "intent_id": str(selected.get("intent_id") or pointer.get("selected_intent_id") or "").strip(),
        "intent_hash": str(selected.get("intent_hash") or pointer.get("selected_intent_hash") or "").strip(),
        "intent_path": str(selected.get("intent_path") or pointer.get("selected_intent_path") or "").strip(),
    }


def _matches_selected_intent(path: Path, payload: dict[str, Any], identity: dict[str, str]) -> bool:
    selected_path = str(identity.get("intent_path") or "").strip()
    if selected_path:
        try:
            if Path(selected_path).resolve() == path.resolve():
                return True
        except Exception:
            pass
    selected_hash = str(identity.get("intent_hash") or "").strip()
    if selected_hash and selected_hash == _intent_hash(path, payload):
        return True
    selected_id = str(identity.get("intent_id") or "").strip()
    return bool(selected_id and selected_id == str(payload.get("intent_id") or "").strip())


def _active_intents(ctx: bod.BodContext) -> list[tuple[Path, dict[str, Any]]]:
    pointer = _selected_intent_pointer(ctx)
    pointer_status = str(pointer.get("status") or "").strip().upper()
    pointer_blocker = str(pointer.get("canonical_blocker") or "").strip().upper()
    if pointer_status == "NO_EXECUTABLE_INTENT" or pointer_blocker == "NO_EXECUTABLE_INTENT":
        return []
    selected_identity = _selected_intent_identity(pointer)
    selected_present = any(str(value or "").strip() for value in selected_identity.values())
    if pointer and not selected_present:
        return []

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
            if selected_present and not _matches_selected_intent(path, payload, selected_identity):
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


def _non_selected_intent_diagnostics(ctx: bod.BodContext) -> list[dict[str, Any]]:
    pointer = _selected_intent_pointer(ctx)
    selected_identity = _selected_intent_identity(pointer)
    selected_present = any(str(value or "").strip() for value in selected_identity.values())
    if not selected_present:
        return []

    roots = [ctx.execution_root, ctx.truth_root] if ctx.execution_root != ctx.truth_root else [ctx.truth_root]
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for root in roots:
        for path in _json_files(root / "intents_v1" / "snapshots" / ctx.day_utc):
            payload = _read_json(path)
            if not payload or str(payload.get("day_utc") or ctx.day_utc) != ctx.day_utc:
                continue
            key = _intent_hash(path, payload)
            if key in seen:
                continue
            seen.add(key)
            if _matches_selected_intent(path, payload, selected_identity):
                continue
            target = _target_pct(payload)
            blocker = ""
            if target is None:
                blocker = "INTENT_BUDGET_MISSING"
            elif target < 0 or target > MAX_ACCOUNT_RISK_PCT:
                blocker = "INTENT_BUDGET_COMPUTE_FAILED"
            rows.append(
                {
                    "intent_id": str(payload.get("intent_id") or key).strip(),
                    "instrument": _instrument(payload),
                    "target_pct": str(target) if target is not None else "",
                    "status": "DIAGNOSTIC_ONLY",
                    "blocker": blocker,
                    "selection_status": "NON_SELECTED",
                    "source_path": str(path),
                }
            )
    return rows


def _cents_to_floor_dollars(cents: int) -> int:
    return int((Decimal(cents) / Decimal("100")).to_integral_value(rounding=ROUND_FLOOR))


def _git_sha() -> str:
    try:
        proc = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT), capture_output=True, text=True, check=False, timeout=10)
    except Exception:
        return "0" * 40
    sha = str(proc.stdout or "").strip()
    return sha if len(sha) == 40 else "0" * 40


def _source_ref(path: Path, *, day_utc: str, producer: str) -> dict[str, Any]:
    return {
        "type": "other",
        "path": str(path),
        "sha256": _sha256_file(path),
        "day_utc": day_utc,
        "producer": producer,
    }


def _materialize_capital_risk_envelope_adapter(
    ctx: bod.BodContext,
    *,
    nav_basis: dict[str, Any],
    budget_policy: dict[str, Any],
    intent_budgets: list[dict[str, Any]],
) -> dict[str, Any]:
    nav_total_cents = _int(nav_basis.get("net_liquidation_cents"))
    cash_total_cents = _int(nav_basis.get("cash_total_cents"))
    if not isinstance(nav_total_cents, int) or nav_total_cents <= 0:
        raise ValueError("NAV_BASIS_INVALID")
    if not isinstance(cash_total_cents, int) or cash_total_cents < 0:
        raise ValueError("NAV_BASIS_INVALID")

    adapter_path = capital_risk_envelope_adapter_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    risk_budget_path = risk_budget_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    nav_path = (ctx.execution_root / "accounting_v2" / "nav" / ctx.day_utc / "nav.v2.json").resolve()
    summary_path = (ctx.execution_root / "allocation_v1" / "summary" / ctx.day_utc / "summary.json").resolve()
    produced = f"{ctx.day_utc}T00:00:00Z"
    git_sha = _git_sha()

    adapter_payload = {
        "schema_id": "capital_risk_envelope_adapter",
        "schema_version": "capital_risk_envelope_adapter.v1",
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "produced_utc": produced,
        "source_path": str(risk_budget_path),
        "source_trust_level": str(nav_basis.get("trust_level") or ""),
        "account": ctx.ib_account,
        "nav_total_cents": nav_total_cents,
        "cash_total_cents": cash_total_cents,
        "budget_policy": budget_policy,
        "intent_budgets": intent_budgets,
        "legacy_input_paths": {
            "accounting_nav_v2": str(nav_path),
            "allocation_summary": str(summary_path),
            "positions_snapshot_v2": str((ctx.execution_root / "positions_v1" / "snapshots" / ctx.day_utc / "positions_snapshot.v2.json").resolve()),
            "capital_risk_envelope_v2": str((ctx.execution_root / "reports" / "capital_risk_envelope_v2" / ctx.day_utc / "capital_risk_envelope.v2.json").resolve()),
        },
    }
    _write_json(adapter_path, adapter_payload)

    nav_total = _cents_to_floor_dollars(nav_total_cents)
    cash_total = _cents_to_floor_dollars(cash_total_cents)
    nav_payload = {
        "schema_id": "C2_ACCOUNTING_NAV_V2",
        "schema_version": 2,
        "day_utc": ctx.day_utc,
        "produced_utc": produced,
        "producer": {"repo": "constellation", "git_sha": git_sha, "module": "ops/tools/run_risk_budget_supply_v1.py"},
        "status": "ACTIVE",
        "reason_codes": ["RISK_BUDGET_SUPPLY_CAPITAL_RISK_ENVELOPE_ADAPTER"],
        "input_manifest": [_source_ref(adapter_path, day_utc=ctx.day_utc, producer="risk_budget_supply_v1")],
        "history": {
            "peak_nav": nav_total,
            "drawdown_abs": 0,
            "drawdown_pct": "0.000000",
        },
        "nav": {
            "currency": "USD",
            "cash_total": cash_total,
            "cash_total_cents": cash_total_cents,
            "gross_positions_value": 0,
            "nav_total": nav_total,
            "nav_total_cents": nav_total_cents,
            "realized_pnl_to_date": 0,
            "unrealized_pnl": 0,
            "components": [
                {
                    "kind": "CASH",
                    "symbol": "USD",
                    "qty": "0",
                    "mv": cash_total,
                    "mark": {
                        "source": "RISK_BUDGET_SUPPLY",
                        "asof_utc": produced,
                        "bid": None,
                        "ask": None,
                        "last": None,
                    },
                }
            ],
            "notes": ["capital risk envelope adapter from verified Risk Budget Supply NAV basis"],
        },
    }
    _write_json(nav_path, nav_payload)

    decisions = []
    for row in intent_budgets:
        if not isinstance(row, dict):
            continue
        source_path = Path(str(row.get("source_path") or adapter_path)).resolve()
        decisions.append(
            {
                "intent_id": str(row.get("intent_id") or ""),
                "status": "ALLOW" if row.get("status") == "PASS" else "BLOCK",
                "path": str(source_path),
                "sha256": _sha256_file(source_path),
            }
        )
    summary_payload = {
        "schema_id": "C2_ALLOCATION_SUMMARY_V1",
        "schema_version": 1,
        "produced_utc": produced,
        "day_utc": ctx.day_utc,
        "producer": {"repo": "constellation", "git_sha": git_sha, "module": "ops/tools/run_risk_budget_supply_v1.py"},
        "authority_classification": "NON_CANONICAL_ADVISORY_ONLY",
        "control_decision_warning": "DO_NOT_USE_FOR_CONTROL_DECISIONS; adapter input for capital_risk_envelope_v2 from Risk Budget Supply",
        "status": "OK",
        "reason_codes": ["RISK_BUDGET_SUPPLY_CAPITAL_RISK_ENVELOPE_ADAPTER"],
        "input_manifest": [_source_ref(adapter_path, day_utc=ctx.day_utc, producer="risk_budget_supply_v1")],
        "summary": {
            "decisions": decisions,
            "counts": {
                "allow": sum(1 for row in decisions if row.get("status") == "ALLOW"),
                "block": sum(1 for row in decisions if row.get("status") == "BLOCK"),
            },
            "notes": ["exposure budget adapter generated from Risk Budget Supply intent budgets"],
            "drawdown_enforcement": {
                "contract_id": "C2_DRAWDOWN_CONVENTION_V1",
                "nav_source_path": str(nav_path),
                "nav_source_sha256": _sha256_file(nav_path),
                "nav_asof_day_utc": ctx.day_utc,
                "rolling_peak_nav": nav_total,
                "nav_total": nav_total,
                "drawdown_abs": 0,
                "drawdown_pct": "0.000000",
                "multiplier": "1.00",
                "thresholds": [
                    {"drawdown_pct": "0.000000", "multiplier": "1.00"},
                    {"drawdown_pct": "-0.050000", "multiplier": "0.75"},
                    {"drawdown_pct": "-0.100000", "multiplier": "0.50"},
                    {"drawdown_pct": "-0.150000", "multiplier": "0.25"},
                ],
            },
            "sleeves": [
                {
                    "sleeve_id": "PRIMARY",
                    "truth_root": str(ctx.execution_root),
                    "decision_count": len(decisions),
                    "allow": sum(1 for row in decisions if row.get("status") == "ALLOW"),
                    "block": sum(1 for row in decisions if row.get("status") == "BLOCK"),
                }
            ],
        },
    }
    _write_json(summary_path, summary_payload)

    return {
        "path": str(adapter_path),
        "status": "PASS",
        "nav_path": str(nav_path),
        "allocation_summary_path": str(summary_path),
        "nav_total_cents": nav_total_cents,
        "cash_total_cents": cash_total_cents,
        "intent_budget_count": len(intent_budgets),
    }


def _read_current_capital_risk_pass(
    *,
    path: Path,
    ctx: bod.BodContext,
    started_at_epoch: float,
    before_sha: str,
) -> tuple[dict[str, Any], str, bool]:
    payload = _read_json(path)
    status = str(payload.get("status") or "").strip().upper()
    schema_id = str(payload.get("schema_id") or "").strip()
    schema_version = str(payload.get("schema_version") or "").strip()
    day_utc = str(payload.get("day_utc") or "").strip()
    reason_codes = payload.get("reason_codes") if isinstance(payload.get("reason_codes"), list) else []
    after_sha = _sha256_file(path)
    fresh_after_start = _file_mtime(path) >= started_at_epoch - 0.5 and after_sha != before_sha
    valid_current_pass = (
        schema_id == "capital_risk_envelope"
        and schema_version == "v2"
        and day_utc == ctx.day_utc
        and status == "PASS"
        and not reason_codes
    )
    return payload, status, bool(valid_current_pass and fresh_after_start)


def _wait_for_current_capital_risk_pass(
    *,
    path: Path,
    ctx: bod.BodContext,
    started_at_epoch: float,
    before_sha: str,
) -> tuple[dict[str, Any], str, bool]:
    payload, status, fresh_pass = _read_current_capital_risk_pass(
        path=path,
        ctx=ctx,
        started_at_epoch=started_at_epoch,
        before_sha=before_sha,
    )
    if fresh_pass:
        return payload, status, True
    deadline = time.monotonic() + max(0.0, CAPITAL_RISK_ENVELOPE_PASS_RECHECK_SECONDS)
    interval = max(0.05, CAPITAL_RISK_ENVELOPE_PASS_RECHECK_INTERVAL_SECONDS)
    while time.monotonic() < deadline:
        time.sleep(interval)
        payload, status, fresh_pass = _read_current_capital_risk_pass(
            path=path,
            ctx=ctx,
            started_at_epoch=started_at_epoch,
            before_sha=before_sha,
        )
        if fresh_pass:
            return payload, status, True
    return payload, status, False


def _run_capital_risk_envelope(ctx: bod.BodContext) -> tuple[dict[str, Any], str]:
    path = (ctx.execution_root / "reports" / "capital_risk_envelope_v2" / ctx.day_utc / "capital_risk_envelope.v2.json").resolve()
    before_sha = _sha256_file(path)
    started_at_epoch = time.time()
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
    payload, status, fresh_pass_after_start = _read_current_capital_risk_pass(
        path=path,
        ctx=ctx,
        started_at_epoch=started_at_epoch,
        before_sha=before_sha,
    )
    pass_recheck_attempted = False
    if not fresh_pass_after_start and (proc.returncode != 0 or status != "PASS"):
        pass_recheck_attempted = True
        payload, status, fresh_pass_after_start = _wait_for_current_capital_risk_pass(
            path=path,
            ctx=ctx,
            started_at_epoch=started_at_epoch,
            before_sha=before_sha,
        )
    result = {
        "command": " ".join(cmd),
        "path": str(path),
        "exists": path.exists(),
        "status": status or "MISSING",
        "exit_code": int(proc.returncode),
        "pass_recheck_attempted": pass_recheck_attempted,
        "fresh_pass_observed_after_invocation_start": fresh_pass_after_start,
        "artifact_sha256_before": before_sha,
        "artifact_sha256_after": _sha256_file(path),
        "reason_codes": payload.get("reason_codes") if isinstance(payload.get("reason_codes"), list) else [],
        "stdout_summary": str(proc.stdout or "").strip()[-1200:],
        "stderr_summary": str(proc.stderr or "").strip()[-1200:],
    }
    return result, "" if (proc.returncode == 0 and status == "PASS") or fresh_pass_after_start else "CAPITAL_RISK_ENVELOPE_BLOCKED"


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
    readiness_path, readiness = read_or_evaluate_trading_day_readiness_authority_v1(
        target_day=ctx.day_utc,
        truth_root=ctx.truth_root,
        execution_root=ctx.execution_root,
        environment=ctx.environment,
    )
    capital_supply, blocker = _load_capital_supply(ctx)
    capital_path = _capital_supply_path(ctx)
    nav_basis: dict[str, Any] = {}
    policy: dict[str, Any] = {}
    budgets: list[dict[str, Any]] = []
    non_selected_diagnostics: list[dict[str, Any]] = []
    adapter: dict[str, Any] = {}
    envelope: dict[str, Any] = {}
    export: dict[str, Any] = {
        "usable_for_risk_sizing": False,
        "nav_total_cents": None,
        "intent_budgets_available": False,
        "budget_source_path": str(risk_budget_supply_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)),
    }
    status = "BLOCKED" if blocker else "PASS"

    if not blocker:
        nav_basis, blocker = _nav_basis(ctx, capital_supply, readiness)
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
            non_selected_diagnostics = _non_selected_intent_diagnostics(ctx)
            if blocker:
                status = "BLOCKED"
    if not blocker:
        try:
            adapter = _materialize_capital_risk_envelope_adapter(ctx, nav_basis=nav_basis, budget_policy=policy, intent_budgets=budgets)
        except Exception:
            blocker = "RISK_SIZING_EXPORT_MISSING"
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
        "readiness_authority_path": str(readiness_path),
        "readiness_mode": str(readiness.get("readiness_mode") or "").strip(),
        "evidence_policy_used": readiness.get("evidence_policy") if isinstance(readiness.get("evidence_policy"), dict) else {},
        "carry_forward_source_used": str(capital_supply.get("carry_forward_source_used") or "").strip(),
        "mode_specific_blocker": str(readiness.get("canonical_blocker") or "").strip() == "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE",
        "nav_basis": nav_basis,
        "budget_policy": policy,
        "intent_budgets": budgets,
        "non_selected_intent_diagnostics": non_selected_diagnostics,
        "capital_risk_envelope_adapter": adapter,
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
    input_paths: list[Any] = [
        payload.get("capital_supply_path", ""),
        payload.get("readiness_authority_path", ""),
    ]
    for row in payload.get("intent_budgets", []) if isinstance(payload.get("intent_budgets"), list) else []:
        if isinstance(row, dict):
            input_paths.append(str(row.get("source_path") or ""))
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_risk_budget_supply_v1.py",
        producer_command=f"python3 ops/tools/run_risk_budget_supply_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}",
        input_artifacts=input_paths,
        output_artifacts=[path],
        schema_versions={"risk_budget_supply": SCHEMA_VERSION},
    )
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
