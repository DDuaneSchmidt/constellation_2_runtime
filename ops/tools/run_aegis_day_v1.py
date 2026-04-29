#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.runtime_contract_v1 import resolve_runtime_data_root
from ops.tools import run_aegis_bod_prepare_v1 as bod

SCHEMA_VERSION = "aegis_day_run.v1"
PHASE_ORDER = [
    "SOURCE_INTEGRITY",
    "BROKER_HEALTH",
    "BOD_INPUTS",
    "SESSION_AUTHORITY",
    "MARKET_DATA_BOD_PREP",
    "STRATEGY_AND_RISK",
    "AUTHORIZATION_PREP",
    "MARKET_OPEN_DATA_GATE",
    "AUTHORIZATION_FINAL",
    "SUBMIT_BOUNDARY",
    "PAPER_READY",
    "EXECUTION",
    "EOD_RECONCILIATION",
]
PRE_READY_PHASES = set(PHASE_ORDER[:11])

MARKET_DATA_BLOCKERS = {
    "MARKET_DATA_AUTHORITY_BLOCKED",
    "MARKET_DATA_CAPTURE_UNAVAILABLE",
    "OPTIONS_CHAIN_SNAPSHOT_MISSING",
    "OPTIONS_SNAPSHOT_CAPTURE_FAILED",
    "OPTIONS_SNAPSHOT_ROOT_MISSING",
    "UNDERLYING_SPOT_MISSING",
    "OPTIONS_UNDERLYING_SPOT_MISSING",
    "OPTIONS_QUOTES_MISSING",
    "OPTIONS_QUOTES_MISSING_BID_ASK",
    "OPTIONS_DELAYED_QUOTES_NOT_RETURNED_BY_IB",
    "OPTIONS_QUOTES_UNAVAILABLE_OUTSIDE_MARKET_HOURS",
    "OPTIONS_SNAPSHOT_STALE",
    "OPTIONS_MARKET_DATA_PERMISSION_DENIED",
    "OPTIONS_MARKET_CLOSED_OR_UNAVAILABLE",
    "OPTIONS_CONTRACT_QUALIFICATION_FAILED",
    "OPTIONS_CAPTURE_TIMEOUT",
    "OPTIONS_CAPTURE_IMPLEMENTATION_ERROR",
}
GENERIC_MARKET_DATA_BLOCKERS = {
    "OPTIONS_CHAIN_SNAPSHOT_MISSING",
    "OPTIONS_SNAPSHOT_CAPTURE_FAILED",
    "OPTIONS_SNAPSHOT_ROOT_MISSING",
    "OPTIONS_SNAPSHOT_SYMBOL_MISSING",
}
SPECIFIC_OPTIONS_CAPTURE_BLOCKERS = MARKET_DATA_BLOCKERS - GENERIC_MARKET_DATA_BLOCKERS - {
    "MARKET_DATA_AUTHORITY_BLOCKED",
    "MARKET_DATA_CAPTURE_UNAVAILABLE",
    "UNDERLYING_SPOT_MISSING",
}

SESSION_SUPPORTING_READINESS_BLOCKERS = {
    "TARGET_DAY_ADMISSION_NOT_READY",
    "TARGET_DAY_BUILD_NOT_READY",
    "SESSION_PROMOTION_DECISION_NOT_READY",
    "OPTIONS_CHAIN_SNAPSHOT_MISSING",
    "OPTIONS_SNAPSHOT_CAPTURE_FAILED",
    "OPTIONS_SNAPSHOT_ROOT_MISSING",
    "MARKET_DATA_AUTHORITY_BLOCKED",
    "AUTHZ_MISSING_DEFINED_RISK_EVIDENCE",
    "CAPITAL_RISK_ENVELOPE_NOT_PASS",
    "SUBMIT_BOUNDARY_NOT_AUTHORIZED",
    "SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS",
}

MARKET_OPEN_DATA_GATE_BLOCKERS = {
    "MARKET_OPEN_DATA_PENDING",
    "OPTIONS_QUOTES_MISSING",
    "OPTIONS_QUOTES_MISSING_BID_ASK",
    "OPTIONS_DELAYED_QUOTES_NOT_RETURNED_BY_IB",
    "OPTIONS_QUOTES_UNAVAILABLE_OUTSIDE_MARKET_HOURS",
    "OPTIONS_SNAPSHOT_STALE",
    "OPTIONS_SNAPSHOT_CAPTURE_FAILED",
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


def _ledger_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "aegis_day_run_v1" / day_utc / "day_run.v1.json").resolve()


def _fallback_ledger_path(day_utc: str) -> Path:
    return (
        resolve_runtime_data_root().resolve()
        / "truth"
        / "reports"
        / "aegis_day_run_v1"
        / day_utc
        / "day_run.v1.json"
    ).resolve()


def _source_repo_status() -> dict[str, Any]:
    proc = subprocess.run(
        ["git", "status", "--short"],
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    lines = [line for line in str(proc.stdout or "").splitlines() if line.strip()]
    protection = bod.read_protection_status_v1()
    protection_status = str(protection.get("status") or "UNKNOWN").strip().upper()
    return {
        "git_dirty_status": "DIRTY" if lines else "CLEAN",
        "dirty_path_count": len(lines),
        "source_reproducibility_status": "NOT_REPRODUCIBLE_DIRTY_WORKTREE" if lines else "REPRODUCIBLE",
        "canonical_repo_protection_status": protection_status,
        "canonical_repo_protection_status_path": str(protection.get("status_path") or ""),
    }


@dataclass(frozen=True)
class PhaseContext:
    day_utc: str
    environment: str
    truth_root: Path
    execution_root: Path
    runtime_root: Path
    operator_input_root: Path
    ib_account: str


def _ctx_from_bod(ctx: bod.BodContext) -> PhaseContext:
    return PhaseContext(
        day_utc=ctx.day_utc,
        environment=ctx.environment,
        truth_root=ctx.truth_root,
        execution_root=ctx.execution_root,
        runtime_root=ctx.runtime_root,
        operator_input_root=ctx.operator_input_root,
        ib_account=ctx.ib_account,
    )


def _empty_phase(
    phase: str,
    *,
    status: str,
    canonical_blocker: str = "",
    blocker_detail: str = "",
    inputs: list[str] | None = None,
    outputs: list[str] | None = None,
    downstream_consequences: list[str] | None = None,
    producer_command: str = "",
    started_at_utc: str = "",
    completed_at_utc: str = "",
    duration_ms: int = 0,
    exit_code: int = 0,
    child_steps: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    now = _now_iso()
    return {
        "phase": phase,
        "status": status,
        "canonical_blocker": canonical_blocker,
        "blocker_detail": blocker_detail,
        "inputs": inputs or [],
        "outputs": outputs or [],
        "downstream_consequences": downstream_consequences or [],
        "producer_command": producer_command,
        "started_at_utc": started_at_utc or now,
        "completed_at_utc": completed_at_utc or now,
        "duration_ms": int(duration_ms),
        "exit_code": int(exit_code),
        "schema_version": SCHEMA_VERSION,
        "child_steps": child_steps or [],
    }


def _blocker_from_step(row: dict[str, Any], fallback: str) -> str:
    blocker = str(row.get("blocker") or "").strip()
    return blocker or fallback


def _specific_market_data_blocker_from_steps(steps: list[dict[str, Any]]) -> str:
    for row in steps:
        blocker = str(row.get("blocker") or "").strip()
        if blocker in SPECIFIC_OPTIONS_CAPTURE_BLOCKERS:
            return blocker
        for key in ("stdout_summary", "stderr_summary"):
            text = str(row.get(key) or "").strip()
            if not text:
                continue
            try:
                payload = json.loads(text.splitlines()[-1])
            except Exception:
                payload = {}
            results = payload.get("results") if isinstance(payload, dict) else None
            if not isinstance(results, list):
                continue
            for item in results:
                if not isinstance(item, dict):
                    continue
                reason = str(item.get("reason_code") or "").strip()
                if reason in SPECIFIC_OPTIONS_CAPTURE_BLOCKERS:
                    return reason
    return ""


def _run_steps(
    phase: str,
    commands: list[tuple[str, list[str], int]],
    *,
    env: dict[str, str],
    run_child: Callable[..., dict[str, Any]] = bod._run_child_with_retries,
) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    steps: list[dict[str, Any]] = []
    outputs: list[str] = []
    blockers: list[str] = []
    for step_name, cmd, attempts in commands:
        row = run_child(step_name, cmd, env=env, max_attempts=attempts)
        steps.append(row)
        artifact_path = str(row.get("artifact_path") or "").strip()
        if artifact_path:
            outputs.append(artifact_path)
        if row.get("status") in {"BLOCKED", "TIMEOUT"}:
            blockers.append(_blocker_from_step(row, f"{phase}_BLOCKED"))
            break
    return steps, outputs, blockers


def _artifact(ctx: PhaseContext, family: str, filename: str) -> Path:
    return ctx.truth_root / "reports" / family / ctx.day_utc / filename


def _requirement_graph_path(ctx: PhaseContext) -> Path:
    return _artifact(ctx, "aegis_requirement_graph_v1", "requirement_graph.v1.json")


def _market_data_supply_path(ctx: PhaseContext) -> Path:
    return _artifact(ctx, "market_data_supply_v1", "market_data_supply.v1.json")


def _capital_supply_path(ctx: PhaseContext) -> Path:
    return _artifact(ctx, "capital_supply_v1", "capital_supply.v1.json")


def _risk_budget_supply_path(ctx: PhaseContext) -> Path:
    return _artifact(ctx, "risk_budget_supply_v1", "risk_budget_supply.v1.json")


def _authorization_supply_path(ctx: PhaseContext) -> Path:
    return _artifact(ctx, "authorization_supply_v1", "authorization_supply.v1.json")


def _broker_supply_path(ctx: PhaseContext) -> Path:
    return _artifact(ctx, "broker_supply_v1", "broker_supply.v1.json")


def _market_open_data_gate_path(ctx: PhaseContext) -> Path:
    return _artifact(ctx, "market_open_data_gate_v1", "market_open_data_gate.v1.json")


def _market_data_delayed_used(ctx: PhaseContext) -> bool:
    supply = _read_json(_market_data_supply_path(ctx))
    return supply.get("delayed_data_used") is True and str(supply.get("market_data_mode") or "").strip().upper() == "DELAYED"


def _load_requirement_graph_root(ctx: PhaseContext) -> dict[str, Any]:
    graph = _read_json(_requirement_graph_path(ctx))
    if str(graph.get("day_utc") or "").strip() != ctx.day_utc:
        return {}
    root = graph.get("root_requirement")
    return root if isinstance(root, dict) else {}


def _execution_authorization_root(ctx: PhaseContext) -> Path:
    return ctx.execution_root / "engine_activity_v1" / "authorization_v1" / ctx.day_utc


def _phase_source_integrity(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    del env
    started = _now_iso()
    status = _source_repo_status()
    blocker = ""
    owner_detail = ""
    if status["git_dirty_status"] != "CLEAN" or status["source_reproducibility_status"] != "REPRODUCIBLE":
        blocker = "SOURCE_REPRODUCIBILITY_BLOCKED"
        owner_detail = "git tree is dirty or source is not reproducible"
    elif status["canonical_repo_protection_status"] != "PROTECTED":
        blocker = "CANONICAL_REPO_PROTECTION_BLOCKED"
        owner_detail = "canonical repo protection is not PROTECTED"
    completed = _now_iso()
    return _empty_phase(
        "SOURCE_INTEGRITY",
        status="BLOCKED" if blocker else "PASS",
        canonical_blocker=blocker,
        blocker_detail=owner_detail,
        outputs=[str(status.get("canonical_repo_protection_status_path") or "")],
        producer_command="git status --short ; read_protection_status_v1",
        started_at_utc=started,
        completed_at_utc=completed,
        exit_code=2 if blocker else 0,
    )


def _phase_broker_health(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    py = sys.executable
    started = _now_iso()
    steps, outputs, blockers = _run_steps(
        "BROKER_HEALTH",
        [("broker_supply", [py, "ops/tools/run_broker_supply_v1.py", "--day_utc", ctx.day_utc, "--environment", ctx.environment], 1)],
        env=env,
    )
    path = _broker_supply_path(ctx)
    payload = _read_json(path)
    status = str(payload.get("status") or "").strip().upper()
    blocker = str(payload.get("canonical_blocker") or "").strip() or (blockers[0] if blockers else "")
    if status != "PASS" and not blocker:
        blocker = "BROKER_ACCOUNT_SUMMARY_MISSING"
    completed = _now_iso()
    return _empty_phase(
        "BROKER_HEALTH",
        status="PASS" if status == "PASS" and not blockers else "BLOCKED",
        canonical_blocker="" if status == "PASS" and not blockers else blocker,
        blocker_detail=f"broker_supply_status={status or 'MISSING'}",
        outputs=outputs or [str(path)],
        producer_command="python3 ops/tools/run_broker_supply_v1.py",
        started_at_utc=started,
        completed_at_utc=completed,
        duration_ms=sum(int(s.get("duration_ms") or 0) for s in steps),
        exit_code=0 if status == "PASS" and not blockers else 2,
        child_steps=steps,
    )


def _phase_bod_inputs(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    py = sys.executable
    started = _now_iso()
    seed_path = bod.resolve_paper_capital_seed_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
    statement_path = bod.resolve_operator_statement_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
    commands = [
        ("paper_capital_seed", [py, "ops/tools/ensure_paper_capital_seed_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.operator_input_root), "--ib_account", ctx.ib_account, "--seed_usd", "0.00", "--allow_create", "YES"], 1),
        ("operator_statement", [py, "ops/tools/ensure_cash_ledger_operator_statement_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.operator_input_root), "--ib_account", ctx.ib_account, "--mode", "GOVERNED_SEED", "--allow_create", "YES"], 1),
        ("pre_open_bundle", [py, "ops/tools/run_pre_open_materializer_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--environment", ctx.environment, "--ib_account", ctx.ib_account], 1),
    ]
    steps, outputs, blockers = _run_steps("BOD_INPUTS", commands, env=env)
    completed = _now_iso()
    return _empty_phase(
        "BOD_INPUTS",
        status="BLOCKED" if blockers else "PASS",
        canonical_blocker=blockers[0] if blockers else "",
        blocker_detail="BOD input materialization failed" if blockers else "",
        inputs=[str(seed_path), str(statement_path)],
        outputs=outputs,
        producer_command="ensure paper capital seed; ensure operator statement; run pre-open materializer",
        started_at_utc=started,
        completed_at_utc=completed,
        duration_ms=sum(int(s.get("duration_ms") or 0) for s in steps),
        exit_code=2 if blockers else 0,
        child_steps=steps,
    )


def _phase_session_authority(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    py = sys.executable
    started = _now_iso()
    commands = [
        ("paper_session_bootstrap", [py, "ops/tools/run_paper_session_bootstrap_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--operator_input_root", str(ctx.operator_input_root), "--environment", ctx.environment, "--ib_account", ctx.ib_account, "--seed_usd", "0.00", "--materialize", "YES", "--emit_report", "YES"], 1),
        ("paper_trading_day_authority", [py, "ops/tools/run_paper_trading_day_authority_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root)], 1),
    ]
    steps, outputs, blockers = _run_steps("SESSION_AUTHORITY", commands, env=env)
    session_path = _artifact(ctx, "paper_session_authority_v1", "paper_session_authority.v1.json")
    bootstrap_path = _artifact(ctx, "paper_session_bootstrap_v1", "paper_session_bootstrap.v1.json")
    session = _read_json(session_path)
    bootstrap = _read_json(bootstrap_path)
    kill_path = ctx.truth_root / "risk_v1" / "kill_switch_v1" / ctx.day_utc / "global_kill_switch_state.v1.json"
    kill = _read_json(kill_path)
    authority_status = str(session.get("authority_status") or "").strip().upper()
    bootstrap_semantic_status = str(bootstrap.get("bootstrap_semantic_status") or "").strip().upper()
    bootstrap_status = str(bootstrap.get("bootstrap_status") or "").strip().upper()
    kill_state = str(kill.get("state") or kill.get("kill_switch_state") or "").strip().upper()
    blocker = ""
    downstream: list[str] = []
    for item in blockers:
        if item in MARKET_DATA_BLOCKERS:
            downstream.append(item)
        elif (
            item in SESSION_SUPPORTING_READINESS_BLOCKERS
            and authority_status in {"GRANTED", "AUTHORIZED"}
            and kill_state in {"", "INACTIVE"}
            and (bootstrap_semantic_status in {"READY_PAPER_ONLY", "READY"} or bootstrap_status == "READY")
        ):
            downstream.append(item)
        else:
            blocker = item
            break
    if not blocker and authority_status not in {"GRANTED", "AUTHORIZED"}:
        blocker = "SESSION_AUTHORITY_DENIED" if authority_status == "DENIED" else "SESSION_AUTHORITY_MISSING"
    if not blocker and kill_state and kill_state != "INACTIVE":
        blocker = "C2_KILL_SWITCH_ACTIVE"
    completed = _now_iso()
    return _empty_phase(
        "SESSION_AUTHORITY",
        status="BLOCKED" if blocker else "PASS",
        canonical_blocker=blocker,
        blocker_detail=(
            f"paper_session_authority={authority_status or 'MISSING'} "
            f"kill_switch={kill_state or 'MISSING'} "
            f"bootstrap_semantic_status={bootstrap_semantic_status or 'MISSING'}"
        ),
        inputs=[str(session_path), str(kill_path)],
        outputs=outputs or [str(session_path)],
        downstream_consequences=downstream,
        producer_command="run paper session bootstrap; run paper trading day authority",
        started_at_utc=started,
        completed_at_utc=completed,
        duration_ms=sum(int(s.get("duration_ms") or 0) for s in steps),
        exit_code=2 if blocker else 0,
        child_steps=steps,
    )


def _phase_market_data_bod_prep(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    py = sys.executable
    started = _now_iso()
    commands = [("market_data_supply", [py, "ops/tools/run_market_data_supply_v1.py", "--day_utc", ctx.day_utc, "--environment", ctx.environment], 1)]
    steps, outputs, blockers = _run_steps("MARKET_DATA_BOD_PREP", commands, env=env)
    supply_path = _market_data_supply_path(ctx)
    supply = _read_json(supply_path)
    supply_status = str(supply.get("status") or "").strip().upper()
    blocker = str(supply.get("canonical_blocker") or "").strip() or (blockers[0] if blockers else "")
    first_requirement = next((row for row in supply.get("requirements", []) if isinstance(row, dict)), {})
    first_provider = next((row for row in supply.get("provider_checks", []) if isinstance(row, dict) and row.get("blocker")), {})
    detail_parts = [f"market_data_supply_status={supply_status or 'MISSING'}"]
    if first_requirement.get("requirement_id"):
        detail_parts.append(f"requirement_id={first_requirement.get('requirement_id')}")
    if first_provider.get("capability"):
        detail_parts.append(f"provider_capability={first_provider.get('capability')}")
    blocker_detail = " ".join(detail_parts)
    completed = _now_iso()
    phase_status = (
        "PASS"
        if (
            supply_status in {"PASS", "SKIPPED", "PRE_MARKET_PENDING"}
            and blocker in {"", "MARKET_OPEN_DATA_PENDING"}
        )
        or blocker in MARKET_OPEN_DATA_GATE_BLOCKERS
        else "BLOCKED"
    )
    return _empty_phase(
        "MARKET_DATA_BOD_PREP",
        status=phase_status,
        canonical_blocker="" if phase_status == "PASS" else (blocker or "MARKET_DATA_AUTHORITY_BLOCKED"),
        blocker_detail=blocker_detail,
        inputs=[str(_requirement_graph_path(ctx))],
        outputs=outputs or [str(supply_path)],
        downstream_consequences=[blocker] if phase_status == "PASS" and blocker in MARKET_OPEN_DATA_GATE_BLOCKERS else [],
        producer_command="run market data supply",
        started_at_utc=started,
        completed_at_utc=completed,
        duration_ms=sum(int(s.get("duration_ms") or 0) for s in steps),
        exit_code=0 if phase_status == "PASS" else 2,
        child_steps=steps,
    )


def _phase_market_data(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    return _phase_market_data_bod_prep(ctx, env)


def _phase_strategy_and_risk(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    py = sys.executable
    started = _now_iso()
    pre_capital_commands = [
        ("trading_day_intent_generation", [py, "ops/tools/run_trading_day_intent_generation_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root)], 1),
        ("portfolio_account_authority", [py, "ops/tools/run_portfolio_account_authority_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--execution_root", str(ctx.execution_root)], 1),
        ("phasec_risk_inputs_prep", [py, "ops/tools/run_phasec_risk_inputs_prep_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root)], 1),
    ]
    steps, outputs, blockers = _run_steps("STRATEGY_AND_RISK", pre_capital_commands, env=env)
    capital_path = _capital_supply_path(ctx)
    risk_budget_path = _risk_budget_supply_path(ctx)
    authorization_path = _authorization_supply_path(ctx)
    if not blockers:
        capital_steps, capital_outputs, capital_blockers = _run_steps(
            "STRATEGY_AND_RISK",
            [("capital_supply", [py, "ops/tools/run_capital_supply_v1.py", "--day_utc", ctx.day_utc, "--environment", ctx.environment], 1)],
            env=env,
        )
        steps.extend(capital_steps)
        outputs.extend(capital_outputs or [str(capital_path)])
        blockers.extend(capital_blockers)
        capital_payload = _read_json(capital_path)
        capital_status = str(capital_payload.get("status") or "").strip().upper()
        capital_blocker = str(capital_payload.get("canonical_blocker") or "").strip()
        if capital_status == "BLOCKED":
            blockers = [capital_blocker or "CAPITAL_SOURCE_MISSING"]
        elif capital_status == "DEGRADED":
            blockers = [capital_blocker or "BOOTSTRAP_CAPITAL_ONLY"]
        elif capital_status == "PASS":
            risk_budget_steps, risk_budget_outputs, risk_budget_blockers = _run_steps(
                "STRATEGY_AND_RISK",
                [("risk_budget_supply", [py, "ops/tools/run_risk_budget_supply_v1.py", "--day_utc", ctx.day_utc, "--environment", ctx.environment], 1)],
                env=env,
            )
            steps.extend(risk_budget_steps)
            outputs.extend(risk_budget_outputs or [str(risk_budget_path)])
            blockers.extend(risk_budget_blockers)
            risk_budget_payload = _read_json(risk_budget_path)
            risk_budget_status = str(risk_budget_payload.get("status") or "").strip().upper()
            risk_budget_blocker = str(risk_budget_payload.get("canonical_blocker") or "").strip()
            if risk_budget_status == "BLOCKED":
                blockers = [risk_budget_blocker or "RISK_SIZING_EXPORT_MISSING"]
            elif risk_budget_status == "DEGRADED":
                blockers = [risk_budget_blocker or "RISK_SIZING_EXPORT_MISSING"]
            elif risk_budget_status != "PASS":
                blockers = ["RISK_SIZING_EXPORT_MISSING"]
        else:
            blockers = ["CAPITAL_SOURCE_MISSING"]
    completed = _now_iso()
    capital_payload = _read_json(capital_path)
    risk_budget_payload = _read_json(risk_budget_path)
    authorization_payload = _read_json(authorization_path)
    blocker_detail = "strategy, portfolio, capital supply, or risk sizing failed" if blockers else ""
    if blockers and authorization_payload and str(authorization_payload.get("status") or "").strip().upper() == "BLOCKED":
        phasec = authorization_payload.get("phasec_defined_risk") if isinstance(authorization_payload.get("phasec_defined_risk"), dict) else {}
        blocker_detail = (
            f"authorization_supply_status={authorization_payload.get('status', 'MISSING')} "
            f"market_data_input={(authorization_payload.get('market_data_input') or {}).get('status', '') if isinstance(authorization_payload.get('market_data_input'), dict) else ''} "
            f"risk_budget_input={(authorization_payload.get('risk_budget_input') or {}).get('status', '') if isinstance(authorization_payload.get('risk_budget_input'), dict) else ''} "
            f"phasec_status={phasec.get('status', '')}"
        )
    elif blockers and risk_budget_payload and str(risk_budget_payload.get("status") or "").strip().upper() == "BLOCKED":
        nav_basis = risk_budget_payload.get("nav_basis") if isinstance(risk_budget_payload.get("nav_basis"), dict) else {}
        blocker_detail = (
            f"risk_budget_supply_status={risk_budget_payload.get('status', 'MISSING')} "
            f"nav_basis_source={nav_basis.get('source', '')} "
            f"nav_total_cents={nav_basis.get('net_liquidation_cents', '')}"
        )
    elif blockers and capital_payload:
        selected = capital_payload.get("selected_source") if isinstance(capital_payload.get("selected_source"), dict) else {}
        blocker_detail = (
            f"capital_supply_status={capital_payload.get('status', 'MISSING')} "
            f"selected_source={selected.get('source_type', '')} "
            f"trust_level={selected.get('trust_level', '')}"
        )
    return _empty_phase(
        "STRATEGY_AND_RISK",
        status="BLOCKED" if blockers else "PASS",
        canonical_blocker=blockers[0] if blockers else "",
        blocker_detail=blocker_detail,
        outputs=outputs,
        producer_command="run intent generation; portfolio; PhaseC prep; capital supply; risk budget supply",
        started_at_utc=started,
        completed_at_utc=completed,
        duration_ms=sum(int(s.get("duration_ms") or 0) for s in steps),
        exit_code=2 if blockers else 0,
        child_steps=steps,
    )


def _phase_authorization_prep(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    del env
    return _empty_phase(
        "AUTHORIZATION_PREP",
        status="PASS",
        blocker_detail="PhaseC and final authorization are deferred until MARKET_OPEN_DATA_GATE passes",
        producer_command="authorization prep defers quote-dependent PhaseC",
    )


def _phase_market_open_data_gate(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    py = sys.executable
    started = _now_iso()
    steps, outputs, blockers = _run_steps(
        "MARKET_OPEN_DATA_GATE",
        [("market_open_data_gate", [py, "ops/tools/run_market_open_data_gate_v1.py", "--day_utc", ctx.day_utc, "--environment", ctx.environment], 1)],
        env=env,
    )
    path = _market_open_data_gate_path(ctx)
    payload = _read_json(path)
    status = str(payload.get("status") or "").strip().upper()
    blocker = str(payload.get("canonical_blocker") or "").strip() or (blockers[0] if blockers else "")
    completed = _now_iso()
    phase_status = "PASS" if status == "PASS" and not blockers else ("PENDING" if status == "PENDING" and blocker == "MARKET_NOT_OPEN" else "BLOCKED")
    return _empty_phase(
        "MARKET_OPEN_DATA_GATE",
        status=phase_status,
        canonical_blocker="" if phase_status == "PASS" else blocker,
        blocker_detail=f"market_open_data_gate_status={status or 'MISSING'} market_session_state={payload.get('market_session_state', '')}",
        outputs=outputs or [str(path)],
        producer_command="run market-open data gate",
        started_at_utc=started,
        completed_at_utc=completed,
        duration_ms=sum(int(s.get("duration_ms") or 0) for s in steps),
        exit_code=0 if phase_status in {"PASS", "PENDING"} else 2,
        child_steps=steps,
    )


def _phase_authorization_final(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    py = sys.executable
    started = _now_iso()
    authorization_path = _authorization_supply_path(ctx)
    steps, outputs, blockers = _run_steps(
        "AUTHORIZATION_FINAL",
        [("authorization_supply", [py, "ops/tools/run_authorization_supply_v1.py", "--day_utc", ctx.day_utc, "--environment", ctx.environment], 1)],
        env=env,
    )
    payload = _read_json(authorization_path)
    status = str(payload.get("status") or "").strip().upper()
    blocker = str(payload.get("canonical_blocker") or "").strip() or (blockers[0] if blockers else "")
    if status == "PASS" and not blockers:
        risk_steps, risk_outputs, risk_blockers = _run_steps(
            "AUTHORIZATION_FINAL",
            [("risk_sizing_authority", [py, "ops/tools/run_risk_sizing_authority_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--execution_root", str(ctx.execution_root)], 1)],
            env=env,
        )
        steps.extend(risk_steps)
        outputs.extend(risk_outputs)
        blockers.extend(risk_blockers)
        blocker = blockers[0] if blockers else ""
    elif not blocker:
        blocker = "AUTHORIZATION_EVIDENCE_MISSING"
    completed = _now_iso()
    return _empty_phase(
        "AUTHORIZATION_FINAL",
        status="PASS" if status == "PASS" and not blockers else "BLOCKED",
        canonical_blocker="" if status == "PASS" and not blockers else blocker,
        blocker_detail=f"authorization_supply_status={status or 'MISSING'}",
        outputs=outputs or [str(authorization_path)],
        producer_command="run authorization supply; run risk sizing",
        started_at_utc=started,
        completed_at_utc=completed,
        duration_ms=sum(int(s.get("duration_ms") or 0) for s in steps),
        exit_code=0 if status == "PASS" and not blockers else 2,
        child_steps=steps,
    )


def _phase_authorization(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    del env
    started = _now_iso()
    path = _authorization_supply_path(ctx)
    payload = _read_json(path)
    status = str(payload.get("status") or "").strip().upper()
    blocker = str(payload.get("canonical_blocker") or "").strip()
    authorized = payload.get("authorization_export") if isinstance(payload.get("authorization_export"), dict) else {}
    authorized_count = len(authorized.get("authorized_intents") or []) if isinstance(authorized.get("authorized_intents"), list) else 0
    if status != "PASS" and not blocker:
        blocker = "AUTHORIZATION_EVIDENCE_MISSING"
    completed = _now_iso()
    return _empty_phase(
        "AUTHORIZATION",
        status="BLOCKED" if blocker else "PASS",
        canonical_blocker=blocker,
        blocker_detail=f"authorization_supply_status={status or 'MISSING'} authorized_intent_count={authorized_count}",
        outputs=[str(path)],
        producer_command="verify authorization supply",
        started_at_utc=started,
        completed_at_utc=completed,
        exit_code=2 if blocker else 0,
    )


def _phase_submit_boundary(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    py = sys.executable
    started = _now_iso()
    commands = [
        ("trade_submit_readiness", [py, "ops/tools/run_trade_submit_readiness_c2_v1.py", "--day_utc", ctx.day_utc, "--ib_account", ctx.ib_account, "--environment", ctx.environment], 1),
        ("submit_boundary_status", [py, "ops/tools/run_submit_boundary_status_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root)], 1),
        ("submit_decision_trace", [py, "ops/tools/run_submit_decision_trace_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--environment", ctx.environment], 1),
    ]
    steps, outputs, blockers = _run_steps("SUBMIT_BOUNDARY", commands, env=env)
    boundary_path = _artifact(ctx, "submit_boundary_status_v1", "submit_boundary_status.v1.json")
    boundary = _read_json(boundary_path)
    boundary_status = str(boundary.get("boundary_status") or boundary.get("readiness_status") or "").strip().upper()
    authorized = boundary.get("submission_authorized") is True or boundary.get("submit_allowed") is True
    blocker = str(boundary.get("first_blocker_code") or boundary.get("canonical_blocker") or "").strip() or (blockers[0] if blockers else "")
    if not authorized and not blocker:
        blocker = "SUBMIT_BOUNDARY_NOT_AUTHORIZED"
    completed = _now_iso()
    return _empty_phase(
        "SUBMIT_BOUNDARY",
        status="PASS" if authorized and not blockers else "BLOCKED",
        canonical_blocker="" if authorized and not blockers else blocker,
        blocker_detail=f"boundary_status={boundary_status or 'MISSING'} submission_authorized={authorized}",
        outputs=outputs or [str(boundary_path)],
        producer_command="run trade submit readiness; submit boundary; submit decision trace",
        started_at_utc=started,
        completed_at_utc=completed,
        duration_ms=sum(int(s.get("duration_ms") or 0) for s in steps),
        exit_code=0 if authorized and not blockers else 2,
        child_steps=steps,
    )


def _phase_paper_ready(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    py = sys.executable
    started = _now_iso()
    steps, outputs, blockers = _run_steps(
        "PAPER_READY",
        [("aegis_paper_ready", [py, "ops/tools/run_aegis_paper_ready_v1.py", "--day_utc", ctx.day_utc, "--environment", ctx.environment], 1)],
        env=env,
    )
    path = _artifact(ctx, "aegis_paper_ready_v1", "aegis_paper_ready.v1.json")
    payload = _read_json(path)
    state = str(payload.get("status") or "").strip().upper()
    blocker = str(payload.get("canonical_blocker") or "").strip() or (blockers[0] if blockers else "")
    completed = _now_iso()
    return _empty_phase(
        "PAPER_READY",
        status="PASS" if state == "PAPER_READY" and not blockers else "BLOCKED",
        canonical_blocker="" if state == "PAPER_READY" and not blockers else (blocker or "PAPER_READY_NOT_PROVEN"),
        blocker_detail=f"paper_ready_status={state or 'MISSING'}",
        outputs=outputs or [str(path)],
        producer_command="run Aegis paper-ready authority",
        started_at_utc=started,
        completed_at_utc=completed,
        duration_ms=sum(int(s.get("duration_ms") or 0) for s in steps),
        exit_code=0 if state == "PAPER_READY" and not blockers else 2,
        child_steps=steps,
    )


def _submission_started(ctx: PhaseContext) -> bool:
    root = ctx.execution_root / "execution_evidence_v1" / "submissions" / ctx.day_utc
    return root.exists() and any(root.rglob("broker_submission_record.v2.json"))


def _phase_execution(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    del env
    if not _submission_started(ctx):
        return _empty_phase(
            "EXECUTION",
            status="SKIPPED",
            blocker_detail="execution phase is not required until actual paper submit begins",
            producer_command="execution evidence inspection",
            outputs=[str(ctx.execution_root / "execution_evidence_v1" / "submissions" / ctx.day_utc)],
        )
    return _empty_phase(
        "EXECUTION",
        status="BLOCKED",
        canonical_blocker="EXECUTION_LEDGER_REVIEW_REQUIRED",
        blocker_detail="paper submission evidence exists; execution ledger review is required",
        producer_command="execution evidence inspection",
        exit_code=2,
    )


def _phase_eod_reconciliation(ctx: PhaseContext, env: dict[str, str]) -> dict[str, Any]:
    del env
    return _empty_phase(
        "EOD_RECONCILIATION",
        status="SKIPPED",
        blocker_detail="EOD reconciliation is not required before trading day close",
        producer_command="EOD phase deferred until close",
        outputs=[str(_artifact(ctx, "trading_day_closure_authority_v1", "trading_day_closure_authority.v1.json"))],
    )


PHASE_RUNNERS: dict[str, Callable[[PhaseContext, dict[str, str]], dict[str, Any]]] = {
    "SOURCE_INTEGRITY": _phase_source_integrity,
    "BROKER_HEALTH": _phase_broker_health,
    "BOD_INPUTS": _phase_bod_inputs,
    "SESSION_AUTHORITY": _phase_session_authority,
    "MARKET_DATA_BOD_PREP": _phase_market_data_bod_prep,
    "STRATEGY_AND_RISK": _phase_strategy_and_risk,
    "AUTHORIZATION_PREP": _phase_authorization_prep,
    "MARKET_OPEN_DATA_GATE": _phase_market_open_data_gate,
    "AUTHORIZATION_FINAL": _phase_authorization_final,
    "SUBMIT_BOUNDARY": _phase_submit_boundary,
    "PAPER_READY": _phase_paper_ready,
    "EXECUTION": _phase_execution,
    "EOD_RECONCILIATION": _phase_eod_reconciliation,
}


def _first_blocked_phase(phase_results: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    for phase in PHASE_ORDER:
        row = phase_results.get(phase) or {}
        if row.get("status") == "BLOCKED":
            return row
    return None


def _operator_next_action(canonical_phase: str, canonical_blocker: str) -> str:
    if not canonical_blocker:
        return "No readiness blocker. Continue with governed paper-ready workflow."
    mapping = {
        "SOURCE_INTEGRITY": "Clean and protect the canonical repo, then rerun run_aegis_day_v1.py.",
        "BROKER_HEALTH": "Restore authoritative IB observer/probe broker event evidence, then rerun run_aegis_day_v1.py.",
        "BOD_INPUTS": "Provide or regenerate paper capital seed, operator statement, and pre-open bundle, then rerun run_aegis_day_v1.py.",
        "SESSION_AUTHORITY": "Resolve paper session authority, kill switch, or session admission blocker, then rerun run_aegis_day_v1.py.",
        "MARKET_DATA_BOD_PREP": "Resolve BOD market-data ownership, entitlement, or policy blocker, then rerun run_aegis_day_v1.py.",
        "STRATEGY_AND_RISK": "Resolve strategy, NAV, capital envelope, and risk-sizing inputs, then rerun run_aegis_day_v1.py.",
        "MARKET_OPEN_DATA_GATE": "Rerun market-open data gate after 09:30 ET and capture quote-complete options evidence.",
        "AUTHORIZATION_FINAL": "Generate valid PhaseC defined-risk and engine authorization evidence, then rerun run_aegis_day_v1.py.",
        "SUBMIT_BOUNDARY": "Resolve submit readiness, submit boundary, and decision trace blockers, then rerun run_aegis_day_v1.py.",
        "PAPER_READY": "Resolve final paper-ready aggregation blocker, then rerun run_aegis_day_v1.py.",
    }
    return mapping.get(canonical_phase, "Resolve the canonical phase blocker, then rerun run_aegis_day_v1.py.")


def build_day_run_payload(ctx: PhaseContext) -> dict[str, Any]:
    created_at = _now_iso()
    env = dict(os.environ)
    env.setdefault("C2_GOVERNED_SUBMIT_DRY_RUN", "YES")
    env.setdefault("AEGIS_BOD_STEP_TIMEOUT_SECONDS", "120")
    env.setdefault("AEGIS_BOD_STEP_TIMEOUT_OPTIONS_CHAIN_SNAPSHOT_SECONDS", "75")

    phase_results: dict[str, dict[str, Any]] = {}
    root_cause_chain: list[dict[str, str]] = []
    downstream_consequences: list[dict[str, str]] = []
    blocked_phase = ""

    for phase in PHASE_ORDER:
        if blocked_phase and phase in PRE_READY_PHASES:
            consequence = f"Skipped because {blocked_phase} blocked first."
            row = _empty_phase(
                phase,
                status="SKIPPED",
                blocker_detail=consequence,
                downstream_consequences=[consequence],
                producer_command="not run after upstream canonical blocker",
            )
            phase_results[phase] = row
            downstream_consequences.append({"phase": phase, "reason": consequence})
            continue
        row = PHASE_RUNNERS[phase](ctx, env)
        phase_results[phase] = row
        for consequence in row.get("downstream_consequences") or []:
            downstream_consequences.append({"phase": phase, "reason": str(consequence)})
        if row.get("status") in {"BLOCKED", "PENDING"} and not blocked_phase and phase in PRE_READY_PHASES:
            blocked_phase = phase
            if row.get("status") == "BLOCKED":
                root_cause_chain.append(
                    {
                        "phase": phase,
                        "canonical_blocker": str(row.get("canonical_blocker") or ""),
                        "blocker_detail": str(row.get("blocker_detail") or ""),
                    }
                )

    blocked = _first_blocked_phase({p: phase_results[p] for p in PRE_READY_PHASES if p in phase_results})
    pending_gate = phase_results.get("MARKET_OPEN_DATA_GATE", {}) if not blocked else {}
    canonical_phase = str(blocked.get("phase") or "") if blocked else ""
    canonical_blocker = str(blocked.get("canonical_blocker") or "") if blocked else ""
    if blocked:
        final_status = "NOT_READY"
    elif pending_gate.get("status") == "PENDING":
        final_status = "PRE_MARKET_READY"
        canonical_phase = "MARKET_OPEN_DATA_GATE"
        canonical_blocker = str(pending_gate.get("canonical_blocker") or "MARKET_NOT_OPEN")
    else:
        execution = phase_results.get("EXECUTION", {})
        eod = phase_results.get("EOD_RECONCILIATION", {})
        if eod.get("status") == "PASS":
            final_status = "EOD_COMPLETE"
        elif execution.get("status") == "PASS":
            final_status = "TRADING_ACTIVE"
        elif _market_data_delayed_used(ctx):
            final_status = "PAPER_READY_WITH_DELAYED_DATA"
        else:
            final_status = "PAPER_READY"

    payload = {
        "schema_id": "aegis_day_run",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "final_status": final_status,
        "canonical_phase": canonical_phase,
        "canonical_blocker": canonical_blocker,
        "root_cause_chain": root_cause_chain,
        "downstream_consequences": downstream_consequences,
        "phase_order": PHASE_ORDER,
        "phase_results": phase_results,
        "created_at_utc": created_at,
        "updated_at_utc": _now_iso(),
        "source_repo_status": _source_repo_status(),
        "operator_next_action": _operator_next_action(canonical_phase, canonical_blocker),
        "truth_root": str(ctx.truth_root),
        "execution_root": str(ctx.execution_root),
        "operator_input_root": str(ctx.operator_input_root),
        "ib_account": ctx.ib_account,
    }
    payload["ledger_path"] = str(_ledger_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc))
    return payload


def run_aegis_day_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    bod_ctx = bod._resolve_context(day_utc, environment, truth_root)
    ctx = _ctx_from_bod(bod_ctx)
    payload = build_day_run_payload(ctx)
    path = _ledger_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    previous = _read_json(path)
    if previous and str(previous.get("day_utc") or "") != ctx.day_utc:
        raise SystemExit(f"FAIL: WRONG_DAY_LEDGER_COLLISION: {path}")
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_day_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment).strip().upper()
    try:
        path, payload = run_aegis_day_v1(day_utc, environment, str(args.truth_root or ""))
    except Exception as exc:  # noqa: BLE001
        path = _fallback_ledger_path(day_utc)
        payload = {
            "schema_id": "aegis_day_run",
            "schema_version": SCHEMA_VERSION,
            "day_utc": day_utc,
            "environment": environment,
            "final_status": "NOT_READY",
            "canonical_phase": "SOURCE_INTEGRITY",
            "canonical_blocker": "DAY_RUN_LEDGER_FAILED",
            "root_cause_chain": [{"phase": "SOURCE_INTEGRITY", "canonical_blocker": "DAY_RUN_LEDGER_FAILED", "blocker_detail": f"{type(exc).__name__}:{exc}"}],
            "downstream_consequences": [],
            "phase_order": PHASE_ORDER,
            "phase_results": {},
            "created_at_utc": _now_iso(),
            "updated_at_utc": _now_iso(),
            "source_repo_status": _source_repo_status(),
            "operator_next_action": "Resolve day-run ledger failure, then rerun run_aegis_day_v1.py.",
            "ledger_path": str(path),
        }
        _write_json(path, payload)
    print(
        json.dumps(
            {
                "status": payload["final_status"],
                "canonical_phase": payload["canonical_phase"],
                "canonical_blocker": payload["canonical_blocker"],
                "ledger_path": str(path),
            },
            sort_keys=True,
        )
    )
    return 0 if payload.get("final_status") in {"PRE_MARKET_READY", "PAPER_READY", "PAPER_READY_WITH_DELAYED_DATA", "TRADING_ACTIVE", "EOD_COMPLETE"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
