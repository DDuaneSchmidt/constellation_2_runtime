#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1

CANONICAL_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
PAPER_SLEEVE_ROOT = Path("/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER")
CURRENT_RELEASE_MANIFEST = CANONICAL_TRUTH_ROOT / "releases" / "current_release.v1.json"
REPORT_REL = Path("reports/aegis_paper_ready_kernel_v1")

PAPER_READY = "PAPER_READY"
BLOCKED = "BLOCKED"
MARKET_NOT_OPEN = "MARKET_NOT_OPEN"
ERROR = "ERROR"


@dataclass(frozen=True)
class KernelStage:
    stage_id: str
    owner: str
    command: list[str]
    truth_role: str
    artifact_rel: Path | None
    pass_statuses: tuple[str, ...]
    repair_command: str
    operator_next_action: str


def report_path(truth_root: Path, target_day: str) -> Path:
    return truth_root / REPORT_REL / target_day / "paper_ready_kernel.v1.json"


def run_paper_ready_kernel_v1(
    *,
    target_day: str,
    canonical_truth_root: str | Path = CANONICAL_TRUTH_ROOT,
    paper_sleeve_root: str | Path = PAPER_SLEEVE_ROOT,
    environment: str = "PAPER",
    ib_account: str = "DUO847203",
    cwd: str | Path | None = None,
    current_release_manifest: str | Path = CURRENT_RELEASE_MANIFEST,
    require_current_release: bool = True,
    scheduled_run: bool = False,
    command_runner: Callable[[list[str], Path], subprocess.CompletedProcess[str]] | None = None,
    now: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    if environment != "PAPER":
        raise RuntimeError("aegis paper ready kernel only supports PAPER")

    workdir = Path(cwd or REPO_ROOT).resolve()
    canonical_root = Path(canonical_truth_root).resolve()
    sleeve_root = Path(paper_sleeve_root).resolve()
    manifest_path = Path(current_release_manifest).resolve()
    if require_current_release:
        _require_current_release_runtime(workdir, manifest_path)

    now_fn = now or (lambda: datetime.now(UTC))
    generated_at = _iso(now_fn())
    release_commit = _release_commit(manifest_path)
    runner = command_runner or _run_command

    report = _base_report(
        target_day=target_day,
        environment=environment,
        generated_at_utc=generated_at,
        canonical_truth_root=canonical_root,
        paper_sleeve_root=sleeve_root,
        scheduled_run=scheduled_run,
    )

    stages = _stages(
        target_day=target_day,
        canonical_truth_root=canonical_root,
        paper_sleeve_root=sleeve_root,
        environment=environment,
        ib_account=ib_account,
        release_commit=release_commit,
    )
    refresh_stage, refresh_result = _refresh_trading_day_readiness_authority(
        report=report,
        runner=runner,
        workdir=workdir,
        canonical_root=canonical_root,
        sleeve_root=sleeve_root,
        target_day=target_day,
        environment=environment,
    )
    if refresh_result["status"] != "PASS":
        return _finish_blocked_report(
            report=report,
            stage=refresh_stage,
            result=refresh_result,
            canonical_truth_root=canonical_root,
            target_day=target_day,
        )
    for stage in stages:
        _assert_safe_command(stage.command)
        truth_root = sleeve_root if stage.truth_role == "PAPER_SLEEVE" else canonical_root
        artifact_path = truth_root / stage.artifact_rel if stage.artifact_rel is not None else None
        command_text = _command_text(stage.command)
        result: dict[str, Any] = {
            "stage_id": stage.stage_id,
            "owner": stage.owner,
            "truth_role": stage.truth_role,
            "producer_command": command_text,
            "repair_command": stage.repair_command,
            "artifact_path": str(artifact_path) if artifact_path else "",
            "status": "RUNNING",
        }

        completed = runner(stage.command, workdir)
        result["returncode"] = completed.returncode
        result["stdout_tail"] = (completed.stdout or "")[-1000:]
        result["stderr_tail"] = (completed.stderr or "")[-1000:]
        validation = _validate_stage_artifact(
            stage=stage,
            artifact_path=artifact_path,
            target_day=target_day,
        )
        result.update(validation)
        report["stage_results"].append(result)
        if artifact_path is not None:
            report["artifact_paths"][stage.stage_id] = str(artifact_path)

        _refresh_summary_fields(report, canonical_root=canonical_root, sleeve_root=sleeve_root, target_day=target_day)

        if result["status"] == MARKET_NOT_OPEN:
            _refresh_submit_boundary_after_market_block(
                report=report,
                stages=stages,
                runner=runner,
                workdir=workdir,
                canonical_root=canonical_root,
                sleeve_root=sleeve_root,
                target_day=target_day,
            )
            return _finish_blocked_report(
                report=report,
                stage=stage,
                result=result,
                canonical_truth_root=canonical_root,
                target_day=target_day,
            )

        if result["status"] != "PASS":
            return _finish_blocked_report(
                report=report,
                stage=stage,
                result=result,
                canonical_truth_root=canonical_root,
                target_day=target_day,
            )

        if completed.returncode != 0:
            result["producer_exit_ignored_after_valid_artifact"] = True
            result["producer_exit_code"] = completed.returncode

    if report.get("submit_allowed") is True and report.get("submission_authorized") is True and isinstance(report.get("broker_transmit_enabled"), bool):
        report["final_status"] = PAPER_READY
        report["first_blocker"] = ""
        report["blocker_owner"] = ""
        report["operator_next_action"] = "No readiness blocker remains; submit boundary is authoritative. Do not submit orders from this validation command."
    else:
        report["final_status"] = BLOCKED
        report["first_blocker"] = "SUBMIT_BOUNDARY_NOT_AUTHORIZED"
        report["blocker_owner"] = "submit_boundary"
        report["operator_next_action"] = "Rerun governed submit boundary after upstream authorization and trading-day readiness are current."
    _write_report(report, canonical_root, target_day)
    return report


def _refresh_submit_boundary_after_market_block(
    *,
    report: dict[str, Any],
    stages: list[KernelStage],
    runner: Callable[[list[str], Path], subprocess.CompletedProcess[str]],
    workdir: Path,
    canonical_root: Path,
    sleeve_root: Path,
    target_day: str,
) -> None:
    submit_stage = next((stage for stage in stages if stage.stage_id == "submit_boundary_status"), None)
    if submit_stage is None:
        return
    artifact_path = canonical_root / submit_stage.artifact_rel if submit_stage.artifact_rel is not None else None
    completed = runner(submit_stage.command, workdir)
    result: dict[str, Any] = {
        "stage_id": submit_stage.stage_id,
        "owner": submit_stage.owner,
        "truth_role": submit_stage.truth_role,
        "producer_command": _command_text(submit_stage.command),
        "repair_command": submit_stage.repair_command,
        "artifact_path": str(artifact_path) if artifact_path else "",
        "status": "RUNNING",
        "returncode": completed.returncode,
        "stdout_tail": (completed.stdout or "")[-1000:],
        "stderr_tail": (completed.stderr or "")[-1000:],
        "freshness_role": "MARKET_NOT_OPEN_REFRESH",
    }
    result.update(
        _validate_stage_artifact(
            stage=submit_stage,
            artifact_path=artifact_path,
            target_day=target_day,
        )
    )
    report["stage_results"].append(result)
    if artifact_path is not None:
        report["artifact_paths"][submit_stage.stage_id] = str(artifact_path)
    _refresh_summary_fields(report, canonical_root=canonical_root, sleeve_root=sleeve_root, target_day=target_day)


def _stages(
    *,
    target_day: str,
    canonical_truth_root: Path,
    paper_sleeve_root: Path,
    environment: str,
    ib_account: str,
    release_commit: str,
) -> list[KernelStage]:
    return [
        _stage("broker_supply", "broker_supply", ["python3", "ops/tools/run_broker_supply_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/broker_supply_v1") / target_day / "broker_supply.v1.json", ("PASS", "OK", "READY"), "python3 ops/tools/run_broker_supply_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Refresh broker account supply from broker observer evidence."),
        _stage("cash_ledger_from_broker", "accounting", ["python3", "ops/tools/run_cash_ledger_from_broker_v1.py", "--day_utc", target_day, "--truth_root", str(paper_sleeve_root), "--account", ib_account, "--environment", environment, "--producer_git_sha", release_commit], "PAPER_SLEEVE", Path("cash_ledger_v1/snapshots") / target_day / "cash_ledger_snapshot.v1.json", ("PASS", "OK"), "python3 ops/tools/run_cash_ledger_from_broker_v1.py --day_utc {day} --truth_root {sleeve} --account DUO847203 --environment PAPER --producer_git_sha <current_release_commit>", "Materialize broker-backed capital into the PAPER sleeve cash ledger."),
        _stage("positions_snapshot", "positions", ["python3", "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v5.py", "--day_utc", target_day, "--truth_root", str(paper_sleeve_root), "--producer_git_sha", release_commit, "--ib_account", ib_account], "PAPER_SLEEVE", Path("positions_v1/snapshots") / target_day / "positions_snapshot.v5.json", ("OK", "PASS", "READY"), "python3 constellation_2/phaseF/positions/run/run_positions_snapshot_day_v5.py --day_utc {day} --truth_root {sleeve} --producer_git_sha <current_release_commit> --ib_account DUO847203", "Materialize current PAPER sleeve positions before NAV."),
        _stage("accounting_nav", "accounting", ["python3", "ops/tools/run_accounting_nav_v2_day_v1.py", "--day_utc", target_day, "--truth_root", str(paper_sleeve_root), "--producer_git_sha", release_commit], "PAPER_SLEEVE", Path("accounting_v2/nav") / target_day / "nav.v2.json", ("ACTIVE", "PASS", "OK"), "python3 ops/tools/run_accounting_nav_v2_day_v1.py --day_utc {day} --truth_root {sleeve} --producer_git_sha <current_release_commit>", "Recompute NAV from broker-backed PAPER sleeve cash ledger."),
        _stage("accounting_nav_compat_bridge", "accounting", ["python3", "ops/tools/bridge_accounting_nav_v2_to_compat_v1.py", "--day_utc", target_day, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("accounting_compat_v1/nav") / target_day / "nav_snapshot.v1.json", ("OK", "PASS", "ACTIVE"), "python3 ops/tools/bridge_accounting_nav_v2_to_compat_v1.py --day_utc {day} --truth_root {sleeve}", "Bridge NAV v2 into the compatibility NAV surface."),
        _stage("capital_supply", "capital_supply", ["python3", "ops/tools/run_capital_supply_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/capital_supply_v1") / target_day / "capital_supply.v1.json", ("PASS", "OK", "READY"), "python3 ops/tools/run_capital_supply_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Produce PAPER sleeve capital supply from broker supply."),
        _stage("trading_day_intent_generation", "intent", ["python3", "ops/tools/run_trading_day_intent_generation_v1.py", "--day_utc", target_day, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/trading_day_intent_generation_v1") / target_day / "trading_day_intent_generation.v1.json", ("INTENTS_PRESENT", "VALID_ZERO", "PASS", "OK"), "python3 ops/tools/run_trading_day_intent_generation_v1.py --day_utc {day} --truth_root {sleeve}", "Generate current-day PAPER intents before portfolio scoring."),
        _stage("portfolio_activation_gate", "portfolio", ["python3", "ops/tools/run_portfolio_activation_gate_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/portfolio_activation_gate_v1") / target_day / "portfolio_activation_gate.v1.json", ("PASS", "ALLOW", "OK", "READY", "DEGRADED"), "python3 ops/tools/run_portfolio_activation_gate_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Refresh portfolio activation gate."),
        _stage("portfolio_scoring", "portfolio", ["python3", "ops/tools/run_portfolio_scoring_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/portfolio_scoring_v1") / target_day / "portfolio_scoring.v1.json", ("PASS", "SCORED", "BOOTSTRAP_ACCEPTED_FOR_PAPER", "OK", "DEGRADED"), "python3 ops/tools/run_portfolio_scoring_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Score current-day PAPER sleeve intents."),
        _stage("intent_arbitration", "intent", ["python3", "ops/tools/run_intent_arbitration_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/intent_arbitration_v1") / target_day / "intent_arbitration.v1.json", ("SELECTED", "PASS", "OK"), "python3 ops/tools/run_intent_arbitration_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Select the executable PAPER intent."),
        _stage("risk_budget_supply", "risk_budget", ["python3", "ops/tools/run_risk_budget_supply_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/risk_budget_supply_v1") / target_day / "risk_budget_supply.v1.json", ("PASS", "OK", "READY"), "python3 ops/tools/run_risk_budget_supply_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Compute risk budget from NAV, selected intent, and capital risk envelope."),
        _stage("market_open_data_gate", "market_data", ["python3", "ops/tools/run_market_open_data_gate_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/market_open_data_gate_v1") / target_day / "market_open_data_gate.v1.json", ("PASS", "OK", "READY"), "python3 ops/tools/run_market_open_data_gate_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Wait for market open, then produce current quote-complete options data."),
        _stage("structure_decision_supply", "structure", ["python3", "ops/tools/run_structure_decision_supply_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/structure_decision_supply_v1") / target_day / "structure_decision_supply.v1.json", ("PASS", "OK", "READY"), "python3 ops/tools/run_structure_decision_supply_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Build option structure decision from market-open data."),
        _stage("capital_authority_allocation", "authorization", ["python3", "ops/tools/run_capital_authority_allocation_day_v1.py", "--day_utc", target_day, "--truth_root", str(paper_sleeve_root), "--canonical_sequence_owner", "ops/tools/run_c2_paper_day_orchestrator_v2.py"], "PAPER_SLEEVE", Path("allocation_v1/capital_authority_allocation_v1") / target_day / "capital_authority_allocation.v1.json", ("OK", "PASS", "READY"), "python3 ops/tools/run_capital_authority_allocation_day_v1.py --day_utc {day} --truth_root {sleeve} --canonical_sequence_owner ops/tools/run_c2_paper_day_orchestrator_v2.py", "Allocate capital authority for the selected intent."),
        _stage("phasec_identity_materializer", "authorization", ["python3", "ops/tools/run_phasec_identity_materializer_day_v1.py", "--day_utc", target_day, "--eval_time_utc", _iso(datetime.now(UTC)), "--truth_root", str(canonical_truth_root), "--execution_truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("phaseC_preflight_v1") / target_day, ("OK", "PASS", "READY", "SUCCESS"), "python3 ops/tools/run_phasec_identity_materializer_day_v1.py --day_utc {day} --eval_time_utc $(date -u +%FT%TZ) --truth_root {canonical} --execution_truth_root {sleeve}", "Materialize Phase C identity and defined-risk proof into the PAPER sleeve."),
        _stage("authorization_artifacts", "authorization", ["python3", "ops/tools/run_authorization_artifacts_day_v1.py", "--day_utc", target_day, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("engine_activity_v1/authorization_v1") / target_day, ("OK", "PASS", "READY", "AUTHORIZED"), "python3 ops/tools/run_authorization_artifacts_day_v1.py --day_utc {day} --truth_root {sleeve}", "Write governed authorization artifacts for approved intents."),
        _stage("authorization_supply", "authorization", ["python3", "ops/tools/run_authorization_supply_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(canonical_truth_root)], "CANONICAL", Path("reports/authorization_supply_v1") / target_day / "authorization_supply.v1.json", ("PASS", "OK", "READY", "AUTHORIZED"), "python3 ops/tools/run_authorization_supply_v1.py --day_utc {day} --environment PAPER --truth_root {canonical}", "Refresh canonical authorization supply from PAPER sleeve authorization evidence."),
        _stage("global_kill_switch", "risk", ["python3", "ops/tools/run_global_kill_switch_v1.py", "--day_utc", target_day], "CANONICAL", Path("risk_v1/kill_switch_v1") / target_day / "global_kill_switch_state.v1.json", ("INACTIVE", "PASS", "OK"), "python3 ops/tools/run_global_kill_switch_v1.py --day_utc {day}", "Refresh governed global kill switch from current authorization evidence."),
        _stage("trading_day_readiness_authority", "trading_day", ["python3", "ops/tools/run_trading_day_readiness_authority_v1.py", "--target_day", target_day, "--truth_root", str(canonical_truth_root), "--execution_root", str(paper_sleeve_root), "--environment", environment], "CANONICAL", Path("reports/trading_day_readiness_authority_v1") / target_day / "trading_day_readiness_authority.v1.json", ("PASS", "OK", "READY", "SUBMIT_ALLOWED"), "python3 ops/tools/run_trading_day_readiness_authority_v1.py --target_day {day} --truth_root {canonical} --execution_root {sleeve} --environment PAPER", "Refresh trading-day readiness authority at the current scheduled mode."),
        _stage("submit_boundary_status", "submit_boundary", ["python3", "ops/tools/run_submit_boundary_status_v1.py", "--day_utc", target_day, "--truth_root", str(canonical_truth_root)], "CANONICAL", Path("reports/submit_boundary_status_v1") / target_day / "submit_boundary_status.v1.json", ("AUTHORIZED", "PASS", "OK", "READY", "SUBMIT_ALLOWED"), "python3 ops/tools/run_submit_boundary_status_v1.py --day_utc {day} --truth_root {canonical}", "Refresh submit boundary status after all upstream authorities are current."),
    ]


def _stage(stage_id: str, owner: str, command: list[str], truth_role: str, artifact_rel: Path | None, pass_statuses: tuple[str, ...], repair_command: str, next_action: str) -> KernelStage:
    return KernelStage(stage_id, owner, command, truth_role, artifact_rel, pass_statuses, repair_command, next_action)


def _validate_stage_artifact(*, stage: KernelStage, artifact_path: Path | None, target_day: str) -> dict[str, Any]:
    if artifact_path is None:
        return {"status": "PASS", "artifact_status": "OK"}
    if artifact_path.is_dir():
        if not any(artifact_path.iterdir()):
            return _blocked("MISSING_ARTIFACT", "MISSING", f"{artifact_path} exists but is empty", stage)
        return {"status": "PASS", "artifact_status": "OK"}
    if not artifact_path.exists():
        return _blocked("MISSING_ARTIFACT", "MISSING", str(artifact_path), stage)
    try:
        data = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return _blocked("ARTIFACT_JSON_INVALID", "INVALID", f"{type(exc).__name__}: {exc}", stage)
    if _wrong_day(data, target_day):
        return _blocked("TARGET_DAY_DATE_MISMATCH", _status_of(data), "artifact day does not match target day", stage)
    blocker = _blocker_of(data)
    artifact_status = _status_of(data)
    if blocker == MARKET_NOT_OPEN or _contains_text(data, MARKET_NOT_OPEN):
        return {"status": MARKET_NOT_OPEN, "artifact_status": artifact_status, "first_blocker": MARKET_NOT_OPEN, "failed_field": "market_open_data_gate.canonical_blocker", "actual_value": MARKET_NOT_OPEN, "expected_value": "market_open_data_gate.status PASS", "operator_next_action": stage.operator_next_action}
    if _contains_text(data, "STALE"):
        return _blocked("STALE_ARTIFACT", artifact_status, "artifact reports stale evidence", stage)
    if _contains_text(data, "KILL_SWITCH_ACTIVE"):
        return _blocked("KILL_SWITCH_ACTIVE", artifact_status, "kill switch blocks entries", stage)
    if stage.stage_id == "accounting_nav":
        nav = data.get("nav") if isinstance(data.get("nav"), dict) else {}
        nav_total = _int_value(nav.get("nav_total_cents"), nav.get("nav_total"))
        if nav_total <= 0:
            return _blocked("NAV_TOTAL_NONPOSITIVE", artifact_status, "nav.nav_total must be positive", stage, failed_field="nav.nav_total", expected_value="> 0", actual_value=nav_total)
    if stage.stage_id == "cash_ledger_from_broker":
        cash = _int_value(data.get("cash_total_cents"), _nested(data, "snapshot", "cash_total_cents"))
        nlv = _int_value(data.get("nlv_total_cents"), _nested(data, "snapshot", "nlv_total_cents"))
        if cash <= 0:
            return _blocked("CASH_TOTAL_NONPOSITIVE", artifact_status, "cash_total_cents must be positive", stage, failed_field="cash_total_cents", expected_value="> 0", actual_value=cash)
        if nlv <= 0:
            return _blocked("NLV_TOTAL_NONPOSITIVE", artifact_status, "nlv_total_cents must be positive", stage, failed_field="nlv_total_cents", expected_value="> 0", actual_value=nlv)
    if stage.stage_id == "intent_arbitration" and artifact_status not in stage.pass_statuses:
        return _blocked(blocker or "NO_SELECTED_INTENT", artifact_status, "intent arbitration did not select an executable intent", stage)
    if stage.stage_id == "global_kill_switch":
        if str(data.get("state", "")).upper() != "INACTIVE" or data.get("allow_entries") is not True:
            return _blocked(blocker or "GLOBAL_KILL_SWITCH_NOT_INACTIVE", artifact_status, "kill switch must be inactive and allow entries", stage)
    if stage.stage_id == "submit_boundary_status":
        if data.get("submit_allowed") is not True or data.get("submission_authorized") is not True:
            return _blocked(blocker or "SUBMIT_BOUNDARY_NOT_AUTHORIZED", artifact_status, "submit_allowed and submission_authorized must both be true", stage, failed_field="submit_allowed/submission_authorized", expected_value="true/true", actual_value=f"{data.get('submit_allowed')}/{data.get('submission_authorized')}")
        if not isinstance(data.get("broker_transmit_enabled"), bool):
            return _blocked("BROKER_TRANSMIT_ENABLED_UNSPECIFIED", artifact_status, "broker_transmit_enabled must be explicit true or false", stage, failed_field="broker_transmit_enabled", expected_value="true or false", actual_value=data.get("broker_transmit_enabled"))
    if artifact_status not in stage.pass_statuses:
        return _blocked(blocker or f"ARTIFACT_STATUS_{artifact_status}", artifact_status, "artifact status does not satisfy stage contract", stage)
    return {"status": "PASS", "artifact_status": artifact_status}


def _blocked(blocker: str, artifact_status: str, detail: str, stage: KernelStage, *, failed_field: str = "status", expected_value: Any = "PASS", actual_value: Any = None) -> dict[str, Any]:
    return {
        "status": "BLOCKED",
        "artifact_status": artifact_status,
        "first_blocker": blocker,
        "detail": detail,
        "failed_field": failed_field,
        "expected_value": expected_value,
        "actual_value": artifact_status if actual_value is None else actual_value,
        "operator_next_action": stage.operator_next_action,
    }


def _finish_blocked_report(*, report: dict[str, Any], stage: KernelStage, result: dict[str, Any], canonical_truth_root: Path, target_day: str) -> dict[str, Any]:
    final_status = MARKET_NOT_OPEN if result.get("status") == MARKET_NOT_OPEN or result.get("first_blocker") == MARKET_NOT_OPEN else BLOCKED
    report["final_status"] = final_status
    report["first_blocker"] = result.get("first_blocker") or "UNKNOWN_BLOCKER"
    report["blocker_owner"] = stage.owner
    report["operator_next_action"] = result.get("operator_next_action") or stage.operator_next_action
    report["failed_stage_id"] = stage.stage_id
    report["failed_field"] = result.get("failed_field", "")
    report["expected_value"] = result.get("expected_value")
    report["actual_value"] = result.get("actual_value")
    _write_report(report, canonical_truth_root, target_day)
    return report


def _base_report(*, target_day: str, environment: str, generated_at_utc: str, canonical_truth_root: Path, paper_sleeve_root: Path, scheduled_run: bool) -> dict[str, Any]:
    return {
        "schema_version": "aegis_paper_ready_kernel.v1",
        "target_day": target_day,
        "environment": environment,
        "scheduled_run": bool(scheduled_run),
        "final_status": ERROR,
        "first_blocker": "",
        "blocker_owner": "",
        "operator_next_action": "Run the governed Aegis paper ready kernel.",
        "canonical_truth_root": str(canonical_truth_root),
        "paper_sleeve_root": str(paper_sleeve_root),
        "stage_results": [],
        "artifact_paths": {},
        "submit_allowed": False,
        "submission_authorized": False,
        "broker_transmit_enabled": None,
        "selected_intent": None,
        "nav_total": None,
        "cash_total": None,
        "risk_budget_status": None,
        "authorization_status": None,
        "submit_boundary_status": None,
        "generated_at_utc": generated_at_utc,
    }


def _refresh_trading_day_readiness_authority(
    *,
    report: dict[str, Any],
    runner: Callable[[list[str], Path], subprocess.CompletedProcess[str]],
    workdir: Path,
    canonical_root: Path,
    sleeve_root: Path,
    target_day: str,
    environment: str,
) -> tuple[KernelStage, dict[str, Any]]:
    stage = _stage(
        "trading_day_readiness_authority_refresh",
        "trading_day",
        [
            "python3",
            "ops/tools/run_trading_day_readiness_authority_v1.py",
            "--target_day",
            target_day,
            "--truth_root",
            str(canonical_root),
            "--execution_root",
            str(sleeve_root),
            "--environment",
            environment,
        ],
        "CANONICAL",
        Path("reports/trading_day_readiness_authority_v1") / target_day / "trading_day_readiness_authority.v1.json",
        ("PASS",),
        "python3 ops/tools/run_trading_day_readiness_authority_v1.py --target_day {day} --truth_root {canonical} --execution_root {sleeve} --environment PAPER",
        "Refresh trading-day readiness authority at the current scheduled mode.",
    )
    _assert_safe_command(stage.command)
    artifact_path = canonical_root / stage.artifact_rel
    completed = runner(stage.command, workdir)
    result: dict[str, Any] = {
        "stage_id": stage.stage_id,
        "owner": stage.owner,
        "truth_role": stage.truth_role,
        "producer_command": _command_text(stage.command),
        "repair_command": stage.repair_command,
        "artifact_path": str(artifact_path),
        "status": "RUNNING",
        "returncode": completed.returncode,
        "stdout_tail": (completed.stdout or "")[-1000:],
        "stderr_tail": (completed.stderr or "")[-1000:],
        "freshness_role": "CURRENT_SESSION_REFRESH",
    }
    result.update(_validate_operational_refresh_artifact(artifact_path=artifact_path, target_day=target_day, stage=stage))
    report["stage_results"].append(result)
    report["artifact_paths"][stage.stage_id] = str(artifact_path)
    _refresh_summary_fields(report, canonical_root=canonical_root, sleeve_root=sleeve_root, target_day=target_day)
    if completed.returncode != 0 and result["status"] == "PASS":
        result.update(_blocked("TRADING_DAY_READINESS_AUTHORITY_REFRESH_FAILED", result.get("artifact_status", "UNKNOWN"), "producer exited nonzero during operational authority refresh", stage, failed_field="returncode", expected_value=0, actual_value=completed.returncode))
    return stage, result


def _validate_operational_refresh_artifact(*, artifact_path: Path, target_day: str, stage: KernelStage) -> dict[str, Any]:
    if not artifact_path.exists():
        return _blocked("MISSING_ARTIFACT", "MISSING", str(artifact_path), stage)
    try:
        data = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return _blocked("ARTIFACT_JSON_INVALID", "INVALID", f"{type(exc).__name__}: {exc}", stage)
    if _wrong_day(data, target_day):
        return _blocked("TARGET_DAY_DATE_MISMATCH", _status_of(data), "artifact day does not match target day", stage)
    return {"status": "PASS", "artifact_status": str(data.get("readiness_mode") or _status_of(data)).upper()}


def _refresh_summary_fields(report: dict[str, Any], *, canonical_root: Path, sleeve_root: Path, target_day: str) -> None:
    nav = _read_json(sleeve_root / "accounting_v2/nav" / target_day / "nav.v2.json")
    if nav:
        nav_body = nav.get("nav") if isinstance(nav.get("nav"), dict) else {}
        report["nav_total"] = _int_value(nav_body.get("nav_total_cents"), nav_body.get("nav_total"))
        report["cash_total"] = _int_value(nav_body.get("cash_total_cents"), nav_body.get("cash_total"))
    intent = _read_json(sleeve_root / "reports/intent_arbitration_v1" / target_day / "intent_arbitration.v1.json")
    if intent:
        report["selected_intent"] = intent.get("selected_intent") or intent.get("selected_intent_pointer") or intent.get("selected_intent_id")
    risk = _read_json(sleeve_root / "reports/risk_budget_supply_v1" / target_day / "risk_budget_supply.v1.json")
    if risk:
        report["risk_budget_status"] = _status_of(risk)
    auth = _read_json(canonical_root / "reports/authorization_supply_v1" / target_day / "authorization_supply.v1.json")
    if auth:
        report["authorization_status"] = _status_of(auth)
    submit = _read_json(canonical_root / "reports/submit_boundary_status_v1" / target_day / "submit_boundary_status.v1.json")
    if submit:
        report["submit_boundary_status"] = _status_of(submit)
        report["submit_allowed"] = submit.get("submit_allowed") is True
        report["submission_authorized"] = submit.get("submission_authorized") is True
        report["broker_transmit_enabled"] = submit.get("broker_transmit_enabled")


def _write_report(report: dict[str, Any], truth_root: Path, target_day: str) -> Path:
    path = report_path(truth_root, target_day)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return path


def _require_current_release_runtime(cwd: Path, manifest_path: Path) -> None:
    require_authoritative_repo_runtime_v1(cwd)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != "aegis_current_release.v1":
        raise RuntimeError("current_release.v1.json schema_version mismatch")
    release_path = Path(str(manifest.get("release_path", ""))).resolve()
    if cwd.resolve() != release_path:
        raise RuntimeError(f"aegis paper ready kernel must run from current release: cwd={cwd.resolve()} current_release={release_path}")


def _release_commit(manifest_path: Path) -> str:
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        value = str(manifest.get("commit") or manifest.get("release_id") or "").strip()
        if value:
            return value
    except Exception:
        pass
    return "0" * 40


def _run_command(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, text=True, capture_output=True)


def _assert_safe_command(command: list[str]) -> None:
    text = _command_text(command)
    forbidden = ("place" + "Order", "submit" + "_order", "broker_transmit_enabled" + "=true", "--" + "transmit")
    if any(item in text for item in forbidden):
        raise RuntimeError(f"unsafe paper ready kernel command refused: {text}")


def _command_text(command: list[str]) -> str:
    return " ".join(str(part) for part in command)


def _status_of(data: dict[str, Any]) -> str:
    for key in ("status", "final_status", "boundary_status", "state", "decision"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value.upper()
    if data.get("submit_allowed") is True:
        return "SUBMIT_ALLOWED"
    return "UNKNOWN"


def _blocker_of(data: dict[str, Any]) -> str:
    for key in ("first_blocker", "primary_blocker", "canonical_blocker", "blocker"):
        value = data.get(key)
        if isinstance(value, str) and value:
            return value.upper()
    codes = data.get("blocking_codes") or data.get("blocking_reason_codes") or data.get("reason_codes")
    if isinstance(codes, list) and codes:
        return str(codes[0]).upper()
    return ""


def _wrong_day(data: dict[str, Any], target_day: str) -> bool:
    for key in ("target_day", "day_utc", "trading_day", "session_day"):
        value = data.get(key)
        if isinstance(value, str) and len(value) >= 10 and value[:10] != target_day:
            return True
    return _contains_text(data, "TARGET_DAY_DATE_MISMATCH")


def _contains_text(value: Any, needle: str) -> bool:
    if isinstance(value, str):
        return needle in value
    if isinstance(value, dict):
        return any(_contains_text(item, needle) for item in value.values())
    if isinstance(value, list):
        return any(_contains_text(item, needle) for item in value)
    return False


def _nested(data: dict[str, Any], *keys: str) -> Any:
    cur: Any = data
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _int_value(*values: Any) -> int:
    for value in values:
        if isinstance(value, bool) or value is None:
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str) and value.strip():
            try:
                return int(float(value))
            except ValueError:
                continue
    return 0


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        if path.exists() and path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else None
    except Exception:
        return None
    return None


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the governed Aegis PAPER readiness DAG.")
    parser.add_argument("--target-day", "--day_utc", dest="target_day", default=datetime.now(UTC).date().isoformat())
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--ib-account", default="DUO847203")
    parser.add_argument("--truth-root", default=str(CANONICAL_TRUTH_ROOT), help="Canonical truth root; operators normally omit this.")
    parser.add_argument("--paper-sleeve-root", default=str(PAPER_SLEEVE_ROOT), help="PAPER sleeve truth root; operators normally omit this.")
    parser.add_argument("--current-release-manifest", default=str(CURRENT_RELEASE_MANIFEST))
    parser.add_argument("--scheduled-run", default="false", choices=["true", "false", "YES", "NO"], help="Mark report as generated by a systemd scheduled PAPER readiness run.")
    args = parser.parse_args()
    try:
        require_current_release = REPO_ROOT != Path("/home/node/constellation").resolve()
        report = run_paper_ready_kernel_v1(
            target_day=args.target_day,
            canonical_truth_root=Path(args.truth_root),
            paper_sleeve_root=Path(args.paper_sleeve_root),
            environment=args.environment,
            ib_account=args.ib_account,
            cwd=REPO_ROOT,
            current_release_manifest=Path(args.current_release_manifest),
            require_current_release=require_current_release,
            scheduled_run=str(args.scheduled_run).lower() in {"true", "yes"},
        )
    except Exception as exc:
        print(f"BLOCKED target_day={args.target_day} first_blocker={type(exc).__name__} next_action=run_from_current_release broker_transmit_enabled=false", file=sys.stderr)
        return 2

    print(
        " ".join(
            [
                str(report.get("final_status") or BLOCKED),
                f"target_day={report.get('target_day')}",
                f"first_blocker={report.get('first_blocker') or ''}",
                f"next_action={json.dumps(report.get('operator_next_action') or '')}",
                f"selected_intent={json.dumps(report.get('selected_intent'))}",
                f"submit_allowed={str(report.get('submit_allowed')).lower()}",
                f"submission_authorized={str(report.get('submission_authorized')).lower()}",
                f"broker_transmit_enabled={json.dumps(report.get('broker_transmit_enabled'))}",
            ]
        )
    )
    return _process_exit_code_for_report(report)


def _process_exit_code_for_report(report: dict[str, Any]) -> int:
    final_status = str(report.get("final_status") or "").strip().upper()
    if final_status in {PAPER_READY, MARKET_NOT_OPEN, BLOCKED}:
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
