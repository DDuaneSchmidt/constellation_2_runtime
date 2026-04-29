#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1, resolve_market_calendar_record_v1
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_operator_statement_path,
    resolve_paper_capital_seed_path,
)
from constellation_2.common.runtime_contract_v1 import resolve_runtime_data_root
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from ops.tools.repo_protection_common_v1 import read_protection_status_v1


DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_OPTIONS_TIMEOUT_SECONDS = 75


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


def _child_pids(parent_pid: int) -> list[int]:
    out: list[int] = []
    proc_root = Path("/proc")
    if not proc_root.exists():
        return out
    for path in proc_root.iterdir():
        if not path.name.isdigit():
            continue
        try:
            stat_text = (path / "stat").read_text(encoding="utf-8", errors="ignore")
            ppid = int(stat_text.rsplit(")", 1)[1].split()[1])
        except Exception:
            continue
        if ppid == parent_pid:
            pid = int(path.name)
            out.append(pid)
            out.extend(_child_pids(pid))
    return out


def _terminate_process_tree(root_pid: int, sig: int) -> None:
    for pid in reversed(_child_pids(root_pid)):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            continue
    try:
        os.killpg(root_pid, sig)
    except ProcessLookupError:
        return


def _timeout_seconds(step_name: str) -> int:
    specific = os.environ.get(f"AEGIS_BOD_STEP_TIMEOUT_{step_name.upper().replace('.', '_')}_SECONDS")
    raw = str(specific or os.environ.get("AEGIS_BOD_STEP_TIMEOUT_SECONDS") or "").strip()
    if not raw:
        return DEFAULT_OPTIONS_TIMEOUT_SECONDS if "options" in step_name else DEFAULT_TIMEOUT_SECONDS
    try:
        parsed = int(raw)
    except ValueError:
        return DEFAULT_TIMEOUT_SECONDS
    return parsed if parsed > 0 else DEFAULT_TIMEOUT_SECONDS


def _stderr_summary(stderr: str) -> str:
    text = " ".join(str(stderr or "").split())
    return text[-800:]


def _extract_path(stdout: str) -> str:
    text = str(stdout or "").strip()
    if not text:
        return ""
    try:
        payload = json.loads(text.splitlines()[-1])
    except Exception:
        payload = {}
    if isinstance(payload, dict):
        for key in ("path", "authority_path", "latest_packet_path", "operator_summary_path", "manifest_path"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
    marker = " path="
    if marker in f" {text}":
        return text.rsplit(marker, 1)[-1].split()[0]
    return ""


def _extract_blocker(stdout: str, stderr: str) -> str:
    for text in (str(stdout or "").strip(), str(stderr or "").strip()):
        if not text:
            continue
        try:
            payload = json.loads(text.splitlines()[-1])
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            for key in ("canonical_blocker", "first_blocker", "first_blocker_code", "reason_code"):
                value = payload.get(key)
                if isinstance(value, str) and value:
                    return value
                if isinstance(value, dict):
                    code = str(value.get("code") or "").strip()
                    if code:
                        return code
            results = payload.get("results")
            if isinstance(results, list):
                for row in results:
                    if isinstance(row, dict) and str(row.get("reason_code") or "").strip():
                        return str(row.get("reason_code")).strip()
            blockers = payload.get("blocking_reason_codes") or payload.get("blocker_chain")
            if isinstance(blockers, list) and blockers:
                return str(blockers[0])
    if stderr:
        return _stderr_summary(stderr).split(":", 1)[0]
    return ""


def _run_child(step_name: str, cmd: list[str], *, env: dict[str, str] | None = None) -> dict[str, Any]:
    started = _now_iso()
    start_monotonic = time.monotonic()
    timeout_s = _timeout_seconds(step_name)
    proc = subprocess.Popen(
        cmd,
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
        env=env,
    )
    timed_out = False
    try:
        stdout, stderr = proc.communicate(timeout=timeout_s)
        exit_code = int(proc.returncode)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            _terminate_process_tree(proc.pid, signal.SIGTERM)
            stdout, stderr = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            _terminate_process_tree(proc.pid, signal.SIGKILL)
            stdout, stderr = proc.communicate()
        exit_code = 124
        stderr = "\n".join(
            item for item in (str(stderr or "").strip(), f"BOD_STEP_TIMEOUT:{step_name}:{timeout_s}s") if item
        )
    completed = _now_iso()
    blocker = "BOD_STEP_TIMEOUT" if timed_out else _extract_blocker(stdout, stderr)
    status = "PASS" if exit_code == 0 else ("TIMEOUT" if timed_out else "BLOCKED")
    return {
        "step_name": step_name,
        "command": cmd,
        "status": status,
        "artifact_path": _extract_path(stdout),
        "blocker": "" if status == "PASS" else blocker,
        "started_at_utc": started,
        "completed_at_utc": completed,
        "duration_ms": int((time.monotonic() - start_monotonic) * 1000),
        "exit_code": exit_code,
        "stderr_summary": _stderr_summary(stderr),
        "stdout_summary": str(stdout or "").strip()[-1200:],
        "timeout_seconds": timeout_s,
    }


def _internal_step(step_name: str, *, status: str, artifact_path: str = "", blocker: str = "", command: list[str] | None = None) -> dict[str, Any]:
    now = _now_iso()
    return {
        "step_name": step_name,
        "command": command or ["internal", step_name],
        "status": status,
        "artifact_path": artifact_path,
        "blocker": blocker,
        "started_at_utc": now,
        "completed_at_utc": now,
        "duration_ms": 0,
        "exit_code": 0 if status in {"PASS", "SKIPPED"} else 2,
        "stderr_summary": "",
        "stdout_summary": "",
        "timeout_seconds": 0,
    }


@dataclass(frozen=True)
class BodContext:
    day_utc: str
    environment: str
    truth_root: Path
    execution_root: Path
    runtime_root: Path
    operator_input_root: Path
    ib_account: str


@dataclass(frozen=True)
class ArtifactSpec:
    name: str
    authority: str
    canonical_relpath: str
    execution_relpath: str = ""
    operator_relpath: str = ""
    date_scoped: bool = True
    carry_forward_allowed: bool = False
    bod_should_generate: bool = True
    command: str = ""

    def path_for(self, ctx: BodContext, day_utc: str) -> Path:
        values = {"day_utc": day_utc, "environment": ctx.environment, "ib_account": ctx.ib_account}
        if self.operator_relpath:
            return (ctx.operator_input_root / self.operator_relpath.format(**values)).resolve()
        if self.execution_relpath:
            return (ctx.execution_root / self.execution_relpath.format(**values)).resolve()
        return (ctx.truth_root / self.canonical_relpath.format(**values)).resolve()


def _artifact_specs() -> list[ArtifactSpec]:
    py = "python3"
    return [
        ArtifactSpec("paper_session_authority", "paper_session_authority_v1", "reports/paper_session_authority_v1/{day_utc}/paper_session_authority.v1.json", command=f"{py} ops/tools/run_paper_session_bootstrap_v1.py --day_utc {{day_utc}} --environment PAPER"),
        ArtifactSpec("paper_trading_day_authority", "paper_trading_day_authority_v1", "reports/paper_trading_day_authority_v1/{day_utc}/paper_trading_day_authority.v1.json", command=f"{py} ops/tools/run_paper_trading_day_authority_v1.py --day_utc {{day_utc}}"),
        ArtifactSpec("paper_session_bootstrap", "paper_session_bootstrap_v1", "reports/paper_session_bootstrap_v1/{day_utc}/paper_session_bootstrap.v1.json", command=f"{py} ops/tools/run_paper_session_bootstrap_v1.py --day_utc {{day_utc}} --environment PAPER"),
        ArtifactSpec("paper_capital_seed", "paper_capital_seed_v1", "", operator_relpath="operator_inputs/paper_capital_seed_v1/{day_utc}/paper_capital_seed.v1.json", command=f"{py} ops/tools/ensure_paper_capital_seed_v1.py --day_utc {{day_utc}} --truth_root <runtime_root> --seed_usd 0.00 --allow_create YES"),
        ArtifactSpec("operator_statement", "cash_ledger_operator_statement_v1", "", operator_relpath="operator_inputs/cash_ledger_operator_statements/{day_utc}/operator_statement.v1.json", command=f"{py} ops/tools/ensure_cash_ledger_operator_statement_v1.py --day_utc {{day_utc}} --truth_root <runtime_root> --mode GOVERNED_SEED --allow_create YES"),
        ArtifactSpec("pre_open_bundle", "pre_open_bundle_v1", "reports/pre_open_bundle_v1/{day_utc}/pre_open_bundle.v1.json", command=f"{py} ops/tools/run_pre_open_materializer_v1.py --day_utc {{day_utc}} --environment PAPER"),
        ArtifactSpec("options_chain_snapshot", "market_data_authority_v1", "", execution_relpath="options_chain_snapshot_v1/{day_utc}", command=f"{py} ops/tools/run_options_chain_snapshot_required_day_v1.py --day_utc {{day_utc}}"),
        ArtifactSpec("options_snapshot_freshness_certificate", "market_data_authority_v1", "", execution_relpath="options_chain_snapshot_v1/{day_utc}", command="produced by options snapshot capture"),
        ArtifactSpec("market_data_authority", "market_data_authority_v1", "reports/market_data_authority_v1/{day_utc}/market_data_authority.v1.json", command=f"{py} ops/tools/run_market_data_authority_v1.py --day_utc {{day_utc}}"),
        ArtifactSpec("strategy_decision_authority", "strategy_decision_authority_v1", "reports/strategy_decision_authority_v1/{day_utc}/strategy_decision_authority.v1.json", command=f"{py} ops/tools/run_strategy_decision_authority_v1.py --day_utc {{day_utc}}"),
        ArtifactSpec("portfolio_account_authority", "portfolio_account_authority_v1", "reports/portfolio_account_authority_v1/{day_utc}/portfolio_account_authority.v1.json", command=f"{py} ops/tools/run_portfolio_account_authority_v1.py --day_utc {{day_utc}}"),
        ArtifactSpec("risk_sizing_authority", "risk_sizing_authority_v1", "reports/risk_sizing_authority_v1/{day_utc}/risk_sizing_authority.v1.json", command=f"{py} ops/tools/run_risk_sizing_authority_v1.py --day_utc {{day_utc}}"),
        ArtifactSpec("defined_risk_phasec_evidence", "risk_sizing_authority_v1", "", execution_relpath="phaseC_preflight_v1/{day_utc}", command=f"{py} ops/tools/run_phasec_identity_materializer_day_v1.py --day_utc {{day_utc}} --execution_truth_root <execution_root>"),
        ArtifactSpec("trade_submit_readiness", "trade_submit_readiness_c2_v1", "", execution_relpath="trade_submit_readiness_c2_v1/{environment}/{ib_account}/status.json", command=f"{py} ops/tools/run_trade_submit_readiness_c2_v1.py --day_utc {{day_utc}} --environment PAPER --ib_account <account>"),
        ArtifactSpec("submit_boundary_status", "submit_boundary_status_v1", "reports/submit_boundary_status_v1/{day_utc}/submit_boundary_status.v1.json", command=f"{py} ops/tools/run_submit_boundary_status_v1.py --day_utc {{day_utc}}"),
        ArtifactSpec("execution_mode_authority", "execution_mode_authority_v1", "reports/execution_mode_authority_v1/{day_utc}/execution_mode_authority.v1.json", command=f"{py} ops/tools/run_execution_mode_authority_v1.py --day_utc {{day_utc}} --environment PAPER"),
        ArtifactSpec("runtime_service_authority", "runtime_service_authority_v1", "reports/runtime_service_authority_v1/{day_utc}/runtime_service_authority.v1.json", command=f"{py} ops/tools/run_runtime_service_authority_v1.py --day_utc {{day_utc}}"),
        ArtifactSpec("trading_day_control_plane", "trading_day_control_plane_v1", "reports/trading_day_control_plane_v1/{day_utc}/trading_day_control_plane.v1.json", command=f"{py} ops/tools/run_trading_day_control_plane_v1.py --day_utc {{day_utc}}"),
        ArtifactSpec("aegis_operating_contract", "aegis_operating_contract_v1", "reports/aegis_operating_contract_v1/{day_utc}/aegis_operating_contract.v1.json", command=f"{py} ops/tools/run_aegis_operating_contract_v1.py --day_utc {{day_utc}} --mode DRY_RUN --run_style MANUAL"),
        ArtifactSpec("aegis_authority_graph", "aegis_authority_graph_v1", "reports/aegis_authority_graph_v1/{day_utc}/aegis_authority_graph.v1.json", command=f"{py} ops/tools/run_aegis_authority_graph_v1.py --day_utc {{day_utc}}"),
        ArtifactSpec("aegis_daily_operator_summary", "aegis_daily_operator_summary_v1", "reports/aegis_daily_operator_summary_v1/{day_utc}/aegis_daily_operator_summary.v1.json", command=f"{py} ops/tools/run_aegis_paper_daily_v1.py --day_utc {{day_utc}}"),
    ]


def _artifact_status(path: Path, name: str) -> str:
    if name == "options_snapshot_freshness_certificate":
        if not path.exists() or not path.is_dir():
            return "MISSING"
        return "EXISTS" if any(path.rglob("freshness_certificate.v1.json")) else "MISSING"
    if path.exists():
        return "EXISTS"
    return "MISSING"


def _build_artifact_comparison(ctx: BodContext, prior_day: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for spec in _artifact_specs():
        prior_path = spec.path_for(ctx, prior_day)
        current_path = spec.path_for(ctx, ctx.day_utc)
        rows.append(
            {
                "artifact_name": spec.name,
                "authority": spec.authority,
                "prior_day_utc": prior_day,
                "prior_path": str(prior_path),
                "prior_status": _artifact_status(prior_path, spec.name),
                "current_day_utc": ctx.day_utc,
                "current_expected_path": str(current_path),
                "current_status": _artifact_status(current_path, spec.name),
                "date_scoped": spec.date_scoped,
                "carry_forward_allowed": spec.carry_forward_allowed,
                "bod_should_generate": spec.bod_should_generate,
                "generation_or_fetch_command": spec.command.format(day_utc=ctx.day_utc),
            }
        )
    return rows


def _source_integrity_step() -> dict[str, Any]:
    proc = subprocess.run(["git", "status", "--short"], cwd=str(REPO_ROOT), text=True, capture_output=True, check=False)
    dirty = bool(str(proc.stdout or "").strip())
    protection = read_protection_status_v1()
    protection_status = str(protection.get("status") or "").strip().upper()
    if dirty:
        return _internal_step("source_integrity_gate", status="BLOCKED", blocker="SOURCE_REPRODUCIBILITY_BLOCKED", command=["git", "status", "--short"])
    if protection_status != "PROTECTED":
        return _internal_step("source_integrity_gate", status="BLOCKED", blocker="CANONICAL_REPO_PROTECTION_BLOCKED", command=["read_protection_status_v1"])
    return _internal_step("source_integrity_gate", status="PASS", artifact_path=str(protection.get("status_path") or ""))


def _resolve_context(day_utc: str, environment: str, truth_root_arg: str = "") -> BodContext:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    truth_root = resolve_decision_truth_root_bridge_v1(
        truth_root_arg or "",
        repo_root=REPO_ROOT,
        caller="ops/tools/run_aegis_bod_prepare_v1.py",
    )
    runtime_root = resolve_runtime_data_root().resolve()
    ib_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    execution_root = resolve_sleeve_execution_root_v1(
        repo_root=REPO_ROOT,
        environment=environment,
        ib_account=ib_account,
        sleeve_id="PRIMARY",
    ).execution_root_path.resolve()
    return BodContext(
        day_utc=day_utc,
        environment=environment,
        truth_root=truth_root,
        execution_root=execution_root,
        runtime_root=runtime_root,
        operator_input_root=runtime_root,
        ib_account=ib_account,
    )


def _manifest_path(ctx: BodContext) -> Path:
    return (
        ctx.truth_root
        / "reports"
        / "aegis_bod_prepare_v1"
        / ctx.day_utc
        / "aegis_bod_prepare.v1.json"
    ).resolve()


def _session_denied(ctx: BodContext) -> bool:
    path = ctx.truth_root / "reports" / "paper_session_authority_v1" / ctx.day_utc / "paper_session_authority.v1.json"
    payload = _read_json(path)
    return str(payload.get("authority_status") or "").strip().upper() == "DENIED"


def _defined_risk_expected(ctx: BodContext) -> list[dict[str, str]]:
    day_root = ctx.execution_root / "intents_v1" / "snapshots" / ctx.day_utc
    rows: list[dict[str, str]] = []
    if not day_root.exists() or not day_root.is_dir():
        return rows
    for path in sorted(day_root.glob("*.json")):
        payload = _read_json(path)
        if str(payload.get("exposure_type") or "").strip().upper() != "SHORT_VOL_DEFINED":
            continue
        intent_hash = path.name.split(".", 1)[0]
        rows.append(
            {
                "intent_id": str(payload.get("intent_id") or ""),
                "intent_hash": intent_hash,
                "expected_path": str((ctx.execution_root / "phaseC_preflight_v1" / ctx.day_utc / "attempt_*" / intent_hash / "execution_identity_record.v1.json").resolve()),
                "creation_command": f"python3 ops/tools/run_phasec_identity_materializer_day_v1.py --day_utc {ctx.day_utc} --eval_time_utc {ctx.day_utc}T00:00:00Z --truth_root {ctx.truth_root} --execution_truth_root {ctx.execution_root}",
            }
        )
    return rows


def _run_sequence(ctx: BodContext) -> list[dict[str, Any]]:
    py = sys.executable
    env = dict(os.environ)
    env.setdefault("C2_GOVERNED_SUBMIT_DRY_RUN", "YES")
    env.setdefault("C2_OPTIONS_SNAPSHOT_STEP_TIMEOUT_SECONDS", str(DEFAULT_OPTIONS_TIMEOUT_SECONDS))
    steps: list[dict[str, Any]] = []

    seed_path = resolve_paper_capital_seed_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
    statement_path = resolve_operator_statement_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)
    steps.append(
        _run_child(
            "paper_capital_seed",
            [
                py,
                "ops/tools/ensure_paper_capital_seed_v1.py",
                "--day_utc",
                ctx.day_utc,
                "--truth_root",
                str(ctx.operator_input_root),
                "--ib_account",
                ctx.ib_account,
                "--seed_usd",
                "0.00",
                "--allow_create",
                "YES",
            ],
            env=env,
        )
    )
    steps[-1]["artifact_path"] = steps[-1]["artifact_path"] or str(seed_path)

    steps.append(
        _run_child(
            "operator_statement",
            [
                py,
                "ops/tools/ensure_cash_ledger_operator_statement_v1.py",
                "--day_utc",
                ctx.day_utc,
                "--truth_root",
                str(ctx.operator_input_root),
                "--ib_account",
                ctx.ib_account,
                "--mode",
                "GOVERNED_SEED",
                "--allow_create",
                "YES",
            ],
            env=env,
        )
    )
    steps[-1]["artifact_path"] = steps[-1]["artifact_path"] or str(statement_path)

    commands: list[tuple[str, list[str]]] = [
        ("pre_open_bundle", [py, "ops/tools/run_pre_open_materializer_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--environment", ctx.environment, "--ib_account", ctx.ib_account]),
        ("paper_session_bootstrap", [py, "ops/tools/run_paper_session_bootstrap_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--operator_input_root", str(ctx.operator_input_root), "--environment", ctx.environment, "--ib_account", ctx.ib_account, "--seed_usd", "0.00", "--materialize", "YES", "--emit_report", "YES"]),
        ("paper_trading_day_authority", [py, "ops/tools/run_paper_trading_day_authority_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root)]),
        ("trading_day_intent_generation", [py, "ops/tools/run_trading_day_intent_generation_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root)]),
        ("portfolio_account_authority", [py, "ops/tools/run_portfolio_account_authority_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--execution_root", str(ctx.execution_root)]),
    ]
    for name, cmd in commands:
        steps.append(_run_child(name, cmd, env=env))

    if _session_denied(ctx):
        steps.append(
            _internal_step(
                "options_chain_snapshot",
                status="SKIPPED",
                artifact_path=str(ctx.execution_root / "options_chain_snapshot_v1" / ctx.day_utc),
                blocker="SESSION_DENIED_OPTIONS_DIAGNOSTIC_ONLY",
                command=["internal", "skip_options_snapshot_when_session_denied"],
            )
        )
    else:
        steps.append(
            _run_child(
                "options_chain_snapshot",
                [
                    py,
                    "ops/tools/run_options_chain_snapshot_required_day_v1.py",
                    "--day_utc",
                    ctx.day_utc,
                    "--truth_root",
                    str(ctx.execution_root),
                    "--symbols_from_intents",
                    "YES",
                ],
                env=env,
            )
        )

    for name, cmd in [
        ("market_data_authority", [py, "ops/tools/run_market_data_authority_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--execution_root", str(ctx.execution_root)]),
        ("strategy_decision_authority", [py, "ops/tools/run_strategy_decision_authority_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--execution_root", str(ctx.execution_root)]),
        ("defined_risk_phasec_evidence", [py, "ops/tools/run_phasec_identity_materializer_day_v1.py", "--day_utc", ctx.day_utc, "--eval_time_utc", f"{ctx.day_utc}T00:00:00Z", "--truth_root", str(ctx.truth_root), "--execution_truth_root", str(ctx.execution_root)]),
        ("risk_sizing_authority", [py, "ops/tools/run_risk_sizing_authority_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--execution_root", str(ctx.execution_root)]),
        ("execution_mode_authority", [py, "ops/tools/run_execution_mode_authority_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--execution_root", str(ctx.execution_root), "--environment", ctx.environment]),
        ("runtime_service_authority", [py, "ops/tools/run_runtime_service_authority_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--expected_run_mode", "MANUAL"]),
        ("trade_submit_readiness", [py, "ops/tools/run_trade_submit_readiness_c2_v1.py", "--day_utc", ctx.day_utc, "--ib_account", ctx.ib_account, "--environment", ctx.environment]),
        ("submit_boundary_status", [py, "ops/tools/run_submit_boundary_status_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root)]),
        ("trading_day_control_plane", [py, "ops/tools/run_trading_day_control_plane_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root)]),
        ("aegis_operating_contract", [py, "ops/tools/run_aegis_operating_contract_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--mode", "DRY_RUN", "--run_style", "MANUAL"]),
        ("aegis_authority_graph", [py, "ops/tools/run_aegis_authority_graph_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--execution_root", str(ctx.execution_root)]),
        ("aegis_daily_operator_summary", [py, "ops/tools/run_aegis_paper_daily_v1.py", "--day_utc", ctx.day_utc, "--truth_root", str(ctx.truth_root), "--mode", "DRY_RUN", "--run_style", "MANUAL"]),
        ("aegis_chatgpt_packet", [py, "ops/tools/aegis_chatgpt_packet.py"]),
    ]:
        steps.append(_run_child(name, cmd, env=env))
    return steps


def _canonical_blocker(steps: list[dict[str, Any]], ctx: BodContext) -> str:
    source = next((s for s in steps if s["step_name"] == "source_integrity_gate"), {})
    if source.get("status") == "BLOCKED":
        return str(source.get("blocker") or "SOURCE_INTEGRITY_BLOCKED")
    runtime = next((s for s in steps if s["step_name"] == "runtime_contract_resolution"), {})
    if runtime.get("status") == "BLOCKED":
        return "TRUTH_ROOT_RUNTIME_CONTRACT_BLOCKED"
    calendar = next((s for s in steps if s["step_name"] == "calendar_session_eligibility"), {})
    if calendar.get("status") == "BLOCKED":
        return str(calendar.get("blocker") or "SESSION_NOT_ALLOWED")
    day_authority = _read_json(ctx.truth_root / "reports" / "paper_trading_day_authority_v1" / ctx.day_utc / "paper_trading_day_authority.v1.json")
    day_blocker = str(day_authority.get("canonical_blocker") or "").strip()
    if day_blocker in {"SESSION_AUTHORITY_DENIED", "SESSION_NOT_ALLOWED", "MARKET_CLOSED"}:
        return day_blocker
    for step_name in ("paper_capital_seed", "operator_statement", "pre_open_bundle", "paper_session_bootstrap"):
        row = next((s for s in steps if s["step_name"] == step_name), {})
        if row.get("status") in {"BLOCKED", "TIMEOUT"}:
            return str(row.get("blocker") or f"{step_name.upper()}_BLOCKED")
    if day_blocker:
        return day_blocker
    for step_name in ("options_chain_snapshot", "market_data_authority", "defined_risk_phasec_evidence", "risk_sizing_authority", "execution_mode_authority", "runtime_service_authority", "submit_boundary_status"):
        row = next((s for s in steps if s["step_name"] == step_name), {})
        if row.get("status") in {"BLOCKED", "TIMEOUT"}:
            if step_name == "options_chain_snapshot" and _session_denied(ctx):
                continue
            return str(row.get("blocker") or f"{step_name.upper()}_BLOCKED")
    return ""


def _operator_actions(ctx: BodContext, steps: list[dict[str, Any]]) -> list[dict[str, str]]:
    actions: list[dict[str, str]] = []
    for row in steps:
        if row.get("status") not in {"BLOCKED", "TIMEOUT"}:
            continue
        name = str(row.get("step_name") or "")
        blocker = str(row.get("blocker") or "")
        if name == "paper_capital_seed":
            actions.append({"blocker": blocker or "PAPER_CAPITAL_SEED_MISSING", "action": f"Provide governed paper capital seed at {resolve_paper_capital_seed_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)} or rerun BOD with governed seed policy available."})
        elif name == "operator_statement":
            actions.append({"blocker": blocker or "OPERATOR_STATEMENT_MISSING", "action": f"Provide operator statement at {resolve_operator_statement_path(operator_input_root=ctx.operator_input_root, day_utc=ctx.day_utc)} with observed_at_utc, currency, cash_total, nlv_total, account_id."})
        elif name == "options_chain_snapshot":
            actions.append({"blocker": blocker or "OPTIONS_SNAPSHOT_CAPTURE_FAILED", "action": "Restore IB/market-data access and rerun BOD; do not fabricate options snapshot evidence."})
        elif name == "defined_risk_phasec_evidence":
            for item in _defined_risk_expected(ctx):
                actions.append({"blocker": "AUTHZ_MISSING_DEFINED_RISK_EVIDENCE", "action": f"Create defined-risk PhaseC evidence matching {item['expected_path']} via: {item['creation_command']}"})
        elif name == "pre_open_bundle":
            actions.append({"blocker": blocker or "PRE_OPEN_BUNDLE_INCOMPLETE", "action": "Resolve missing pre-open bundle components shown in the BOD step stderr/stdout and rerun BOD."})
    return actions


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_bod_prepare_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    environment = str(args.environment).strip().upper()
    started = _now_iso()
    steps: list[dict[str, Any]] = []
    ctx: BodContext | None = None
    comparison: list[dict[str, Any]] = []

    source_step = _source_integrity_step()
    steps.append(source_step)
    if source_step["status"] == "BLOCKED":
        runtime_root = resolve_runtime_data_root().resolve()
        fallback_truth = runtime_root / "truth"
        path = fallback_truth / "reports" / "aegis_bod_prepare_v1" / day_utc / "aegis_bod_prepare.v1.json"
        payload = {
            "schema_id": "aegis_bod_prepare",
            "schema_version": "v1",
            "day_utc": day_utc,
            "environment": environment,
            "status": "BLOCKED",
            "canonical_blocker": source_step["blocker"],
            "started_at_utc": started,
            "completed_at_utc": _now_iso(),
            "steps": steps,
        }
        _write_json(path, payload)
        print(json.dumps({"status": "BLOCKED", "canonical_blocker": source_step["blocker"], "manifest_path": str(path)}, sort_keys=True))
        return 2

    try:
        ctx = _resolve_context(day_utc, environment, args.truth_root)
        steps.append(_internal_step("runtime_contract_resolution", status="PASS", artifact_path=str(ctx.truth_root)))
    except Exception as exc:  # noqa: BLE001
        runtime_root = resolve_runtime_data_root().resolve()
        path = runtime_root / "truth" / "reports" / "aegis_bod_prepare_v1" / day_utc / "aegis_bod_prepare.v1.json"
        steps.append(_internal_step("runtime_contract_resolution", status="BLOCKED", blocker=f"{type(exc).__name__}:{exc}"))
        payload = {
            "schema_id": "aegis_bod_prepare",
            "schema_version": "v1",
            "day_utc": day_utc,
            "environment": environment,
            "status": "BLOCKED",
            "canonical_blocker": "TRUTH_ROOT_RUNTIME_CONTRACT_BLOCKED",
            "started_at_utc": started,
            "completed_at_utc": _now_iso(),
            "steps": steps,
        }
        _write_json(path, payload)
        print(json.dumps({"status": "BLOCKED", "canonical_blocker": "TRUTH_ROOT_RUNTIME_CONTRACT_BLOCKED", "manifest_path": str(path)}, sort_keys=True))
        return 2

    calendar = resolve_market_calendar_record_v1(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    record = calendar.get("record") if isinstance(calendar.get("record"), dict) else {}
    is_session = bool(record.get("is_trading_session") is True or record.get("is_open") is True)
    calendar_status = "PASS" if is_session else "BLOCKED"
    calendar_blocker = "" if is_session else str(calendar.get("reason_code") or "SESSION_NOT_ALLOWED")
    steps.append(_internal_step("calendar_session_eligibility", status=calendar_status, artifact_path=str(calendar.get("path") or ""), blocker=calendar_blocker))
    comparison = _build_artifact_comparison(ctx, "2026-04-28")

    if calendar_status == "PASS":
        steps.extend(_run_sequence(ctx))

    blocker = _canonical_blocker(steps, ctx)
    status = "OK" if not blocker else "BLOCKED"
    manifest = {
        "schema_id": "aegis_bod_prepare",
        "schema_version": "v1",
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "status": status,
        "canonical_blocker": blocker,
        "started_at_utc": started,
        "completed_at_utc": _now_iso(),
        "truth_root": str(ctx.truth_root),
        "execution_root": str(ctx.execution_root),
        "operator_input_root": str(ctx.operator_input_root),
        "ib_account": ctx.ib_account,
        "artifact_comparison": comparison,
        "steps": steps,
        "operator_actions": _operator_actions(ctx, steps),
        "defined_risk_expected_evidence": _defined_risk_expected(ctx),
    }
    path = _manifest_path(ctx)
    _write_json(path, manifest)
    print(json.dumps({"status": status, "canonical_blocker": blocker, "manifest_path": str(path)}, sort_keys=True))
    return 0 if status == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
