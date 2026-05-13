#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
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
GATE_HIERARCHY_POLICY_REL = Path("governance/02_REGISTRIES/GATE_HIERARCHY_V1.json")
PAPER_DAY_ORCHESTRATOR_SERVICE_REL = Path("ops/systemd/user/c2-paper-day-orchestrator.service")

PAPER_READY = "PAPER_READY"
BLOCKED = "BLOCKED"
MARKET_NOT_OPEN = "MARKET_NOT_OPEN"
ERROR = "ERROR"
BOOTSTRAP_ACCEPTED_FOR_PAPER = "BOOTSTRAP_ACCEPTED_FOR_PAPER"
PORTFOLIO_BOOTSTRAP_REASON = "PORTFOLIO_STATE_BOOTSTRAP_ACCEPTED_FOR_PAPER_NO_SUPPRESSION_APPLIED"


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

        if stage.stage_id == "paper_authority_pointer_refresh":
            result.update(
                _run_paper_authority_pointer_refresh_stage(
                    stage=stage,
                    artifact_path=artifact_path,
                    target_day=target_day,
                    sleeve_root=sleeve_root,
                    runner=runner,
                    workdir=workdir,
                    release_commit=release_commit,
                )
            )
            report["stage_results"].append(result)
            if artifact_path is not None:
                report["artifact_paths"][stage.stage_id] = str(artifact_path)
            _refresh_summary_fields(report, canonical_root=canonical_root, sleeve_root=sleeve_root, target_day=target_day)
            if result["status"] != "PASS":
                return _finish_blocked_report(
                    report=report,
                    stage=stage,
                    result=result,
                    canonical_truth_root=canonical_root,
                    target_day=target_day,
                )
            continue

        stage_started_at = now_fn()
        completed = runner(stage.command, workdir)
        result["returncode"] = completed.returncode
        result["stdout_tail"] = (completed.stdout or "")[-1000:]
        result["stderr_tail"] = (completed.stderr or "")[-1000:]
        validation = _validate_stage_artifact(
            stage=stage,
            artifact_path=artifact_path,
            target_day=target_day,
            min_generated_at_utc=_iso(stage_started_at) if stage.stage_id == "paper_startup_intent_input_convergence" else "",
        )
        result.update(validation)
        if stage.stage_id == "paper_startup_intent_input_convergence" and completed.returncode != 0 and result["status"] == "PASS":
            result.update(
                _blocked(
                    "PAPER_STARTUP_INTENT_INPUT_CONVERGENCE_FAILED",
                    result.get("artifact_status", "UNKNOWN"),
                    "startup convergence producer exited nonzero; stale prior artifact cannot satisfy the scheduled run",
                    stage,
                    failed_field="returncode",
                    expected_value=0,
                    actual_value=completed.returncode,
                )
            )
        if stage.stage_id == "exposure_net" and completed.returncode != 0 and result["status"] == "PASS":
            result.update(
                _blocked(
                    "EXPOSURE_NET_PRODUCER_FAILED",
                    result.get("artifact_status", "UNKNOWN"),
                    "exposure-net producer exited nonzero; capital allocation cannot rely on this stage",
                    stage,
                    failed_field="returncode",
                    expected_value=0,
                    actual_value=completed.returncode,
                )
            )
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
        _stage("paper_startup_intent_input_convergence", "intent", ["python3", "ops/tools/run_paper_startup_intent_input_convergence_v1.py", "--day_utc", target_day, "--truth_root", str(canonical_truth_root), "--environment", environment, "--ib_account", ib_account], "CANONICAL", Path("reports/paper_startup_intent_input_convergence_v1") / target_day / "paper_startup_intent_input_convergence.v1.json", ("SUCCESS", "PASS", "OK"), "python3 ops/tools/run_paper_startup_intent_input_convergence_v1.py --day_utc {day} --truth_root {canonical} --environment PAPER --ib_account DUO847203", "Refresh registry-derived PAPER startup input convergence before intent generation."),
        _stage("trading_day_intent_generation", "intent", ["python3", "ops/tools/run_trading_day_intent_generation_v1.py", "--day_utc", target_day, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/trading_day_intent_generation_v1") / target_day / "trading_day_intent_generation.v1.json", ("INTENTS_PRESENT", "VALID_ZERO", "PASS", "OK"), "python3 ops/tools/run_trading_day_intent_generation_v1.py --day_utc {day} --truth_root {sleeve}", "Generate current-day PAPER intents before portfolio scoring."),
        _stage("portfolio_activation_gate", "portfolio", ["python3", "ops/tools/run_portfolio_activation_gate_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/portfolio_activation_gate_v1") / target_day / "portfolio_activation_gate.v1.json", ("PASS", "ALLOW", "OK", "READY", "DEGRADED"), "python3 ops/tools/run_portfolio_activation_gate_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Refresh portfolio activation gate."),
        _stage("portfolio_scoring", "portfolio", ["python3", "ops/tools/run_portfolio_scoring_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/portfolio_scoring_v1") / target_day / "portfolio_scoring.v1.json", ("PASS", "SCORED", "BOOTSTRAP_ACCEPTED_FOR_PAPER", "OK", "DEGRADED"), "python3 ops/tools/run_portfolio_scoring_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Score current-day PAPER sleeve intents."),
        _stage("intent_arbitration", "intent", ["python3", "ops/tools/run_intent_arbitration_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/intent_arbitration_v1") / target_day / "intent_arbitration.v1.json", ("SELECTED", "PASS", "OK"), "python3 ops/tools/run_intent_arbitration_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Select the executable PAPER intent."),
        _stage("risk_budget_supply", "risk_budget", ["python3", "ops/tools/run_risk_budget_supply_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/risk_budget_supply_v1") / target_day / "risk_budget_supply.v1.json", ("PASS", "OK", "READY"), "python3 ops/tools/run_risk_budget_supply_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Compute risk budget from NAV, selected intent, and capital risk envelope."),
        _stage("market_open_data_gate", "market_data", ["python3", "ops/tools/run_market_open_data_gate_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/market_open_data_gate_v1") / target_day / "market_open_data_gate.v1.json", ("PASS", "OK", "READY"), "python3 ops/tools/run_market_open_data_gate_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Wait for market open, then produce current quote-complete options data."),
        _stage("structure_decision_supply", "structure", ["python3", "ops/tools/run_structure_decision_supply_v1.py", "--day_utc", target_day, "--environment", environment, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("reports/structure_decision_supply_v1") / target_day / "structure_decision_supply.v1.json", ("PASS", "OK", "READY"), "python3 ops/tools/run_structure_decision_supply_v1.py --day_utc {day} --environment PAPER --truth_root {sleeve}", "Build option structure decision from market-open data."),
        _stage("paper_authority_pointer_refresh", "authorization", ["python3", "ops/tools/run_pointer_append_v1.py", "--guarded-by", "authorization_gate_verdict_v1"], "PAPER_SLEEVE", Path("run_pointer_v2/canonical_authority_head.v1.json"), ("PASS", "BOOTSTRAP_PASS"), "Run governed pointer append/head materialization only after same-day PAPER authorization verdict PASS.", "Refresh the same-day PAPER canonical authority head from the governed PASS authorization verdict before capital allocation."),
        _stage("paper_authority_head_freshness", "authorization", ["python3", "-c", "pass"], "PAPER_SLEEVE", Path("run_pointer_v2/canonical_authority_head.v1.json"), ("PASS",), "Inspect PAPER authorization_gate_verdict_v1 and canonical authority head; do not synthesize authority.", "Resolve same-day PAPER authorization gate verdict and canonical authority head before capital allocation."),
        _stage("exposure_net", "risk", ["python3", "ops/tools/run_exposure_net_day_v1.py", "--day_utc", target_day, "--truth_root", str(paper_sleeve_root)], "PAPER_SLEEVE", Path("risk_v1/exposure_net_v1") / target_day / "exposure_net.v1.json", ("OK", "PASS", "READY"), "python3 ops/tools/run_exposure_net_day_v1.py --day_utc {day} --truth_root {sleeve}", "Produce same-day exposure_net_v1 before capital authority allocation."),
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


def _validate_stage_artifact(*, stage: KernelStage, artifact_path: Path | None, target_day: str, min_generated_at_utc: str = "") -> dict[str, Any]:
    if artifact_path is None:
        return {"status": "PASS", "artifact_status": "OK"}
    if artifact_path.is_dir():
        if not any(artifact_path.iterdir()):
            return _blocked("MISSING_ARTIFACT", "MISSING", f"{artifact_path} exists but is empty", stage)
        return {"status": "PASS", "artifact_status": "OK"}
    if not artifact_path.exists():
        if stage.stage_id == "paper_authority_head_freshness":
            return _authority_head_blocked(
                "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
                "MISSING",
                "PAPER canonical authority head is missing before capital allocation",
                stage,
                target_day=target_day,
                artifact_path=artifact_path,
                failed_field="run_pointer_v2/canonical_authority_head.v1.json",
                expected_value="same-day PASS or BOOTSTRAP_PASS authoritative head",
                actual_value="missing",
            )
        return _blocked("MISSING_ARTIFACT", "MISSING", str(artifact_path), stage)
    try:
        data = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return _blocked("ARTIFACT_JSON_INVALID", "INVALID", f"{type(exc).__name__}: {exc}", stage)
    if stage.stage_id == "paper_authority_head_freshness":
        return _validate_paper_authority_head_freshness(
            data=data,
            artifact_path=artifact_path,
            target_day=target_day,
            stage=stage,
        )
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
    if stage.stage_id == "paper_startup_intent_input_convergence":
        generated_utc = str(data.get("generated_utc") or data.get("produced_at_utc") or "").strip()
        if not generated_utc:
            return _blocked(
                "PAPER_STARTUP_INTENT_INPUT_CONVERGENCE_FRESHNESS_MISSING",
                artifact_status,
                "startup convergence artifact must carry generated_utc for scheduled kernel validation",
                stage,
                failed_field="generated_utc",
                expected_value=f">= {min_generated_at_utc}" if min_generated_at_utc else "present",
                actual_value=generated_utc,
            )
        if min_generated_at_utc and _iso_before(generated_utc, min_generated_at_utc):
            return _blocked(
                "PAPER_STARTUP_INTENT_INPUT_CONVERGENCE_STALE",
                artifact_status,
                "startup convergence artifact predates this stage invocation",
                stage,
                failed_field="generated_utc",
                expected_value=f">= {min_generated_at_utc}",
                actual_value=generated_utc,
            )
        symbol_diagnostics = data.get("symbol_diagnostics") if isinstance(data.get("symbol_diagnostics"), dict) else {}
        if symbol_diagnostics.get("bridge_symbol_used_for_market_snapshot") is True:
            return _blocked(
                "PAPER_STARTUP_INTENT_INPUT_CONVERGENCE_STALE_BRIDGE_SYMBOL",
                artifact_status,
                "startup convergence artifact still reports deprecated bridge-symbol market snapshot materialization",
                stage,
                failed_field="symbol_diagnostics.bridge_symbol_used_for_market_snapshot",
                expected_value=False,
                actual_value=True,
            )
    if stage.stage_id == "paper_startup_intent_input_convergence" and artifact_status not in stage.pass_statuses:
        symbol_diagnostics = data.get("symbol_diagnostics") if isinstance(data.get("symbol_diagnostics"), dict) else {}
        missing_symbols = symbol_diagnostics.get("missing_snapshot_symbols")
        if isinstance(missing_symbols, list) and missing_symbols:
            normalized_missing = [str(symbol).strip().upper() for symbol in missing_symbols if str(symbol).strip()]
            return _blocked(
                "MARKET_DATA_SNAPSHOT_V1_MISSING_SYMBOLS",
                artifact_status,
                "startup convergence missing required registry-derived market snapshots",
                stage,
                failed_field="symbol_diagnostics.missing_snapshot_symbols",
                expected_value=[],
                actual_value=normalized_missing,
            )
    if stage.stage_id == "portfolio_activation_gate" and artifact_status == BOOTSTRAP_ACCEPTED_FOR_PAPER:
        return _validate_portfolio_bootstrap_acceptance(data=data, artifact_status=artifact_status, stage=stage)
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


def _validate_portfolio_bootstrap_acceptance(*, data: dict[str, Any], artifact_status: str, stage: KernelStage) -> dict[str, Any]:
    environment = str(data.get("environment") or "").strip().upper()
    if environment != "PAPER":
        return _blocked(
            "PORTFOLIO_ACTIVATION_GATE_BOOTSTRAP_NOT_PAPER",
            artifact_status,
            "portfolio bootstrap acceptance is valid only for PAPER",
            stage,
            failed_field="environment",
            expected_value="PAPER",
            actual_value=environment or None,
        )
    canonical_blocker = str(data.get("canonical_blocker") or data.get("first_blocker") or data.get("primary_blocker") or "").strip()
    if canonical_blocker:
        return _blocked(
            "PORTFOLIO_ACTIVATION_GATE_BOOTSTRAP_HAS_BLOCKER",
            artifact_status,
            "portfolio bootstrap acceptance cannot carry a canonical blocker",
            stage,
            failed_field="canonical_blocker",
            expected_value="",
            actual_value=canonical_blocker,
        )
    approved = data.get("approved_executable_intents")
    if not isinstance(approved, list) or not approved:
        return _blocked(
            "PORTFOLIO_ACTIVATION_GATE_BOOTSTRAP_EVIDENCE_MISSING",
            artifact_status,
            "portfolio bootstrap acceptance requires approved executable intent evidence",
            stage,
            failed_field="approved_executable_intents",
            expected_value="non-empty list",
            actual_value=type(approved).__name__,
        )
    for index, row in enumerate(approved):
        if not isinstance(row, dict):
            return _blocked(
                "PORTFOLIO_ACTIVATION_GATE_BOOTSTRAP_EVIDENCE_INVALID",
                artifact_status,
                "portfolio bootstrap approved intent evidence must be object rows",
                stage,
                failed_field=f"approved_executable_intents[{index}]",
                expected_value="object",
                actual_value=type(row).__name__,
            )
        if row.get("allowed_by_portfolio_gate") is not True:
            return _blocked(
                "PORTFOLIO_ACTIVATION_GATE_BOOTSTRAP_EVIDENCE_INVALID",
                artifact_status,
                "portfolio bootstrap approved intents must be explicitly allowed by portfolio gate",
                stage,
                failed_field=f"approved_executable_intents[{index}].allowed_by_portfolio_gate",
                expected_value=True,
                actual_value=row.get("allowed_by_portfolio_gate"),
            )
        if str(row.get("portfolio_gate_decision") or "").strip().upper() != "ALLOW":
            return _blocked(
                "PORTFOLIO_ACTIVATION_GATE_BOOTSTRAP_EVIDENCE_INVALID",
                artifact_status,
                "portfolio bootstrap approved intents must carry an ALLOW gate decision",
                stage,
                failed_field=f"approved_executable_intents[{index}].portfolio_gate_decision",
                expected_value="ALLOW",
                actual_value=row.get("portfolio_gate_decision"),
            )
        reasons = row.get("reason_codes")
        normalized_reasons = [str(reason).strip().upper() for reason in reasons] if isinstance(reasons, list) else []
        if PORTFOLIO_BOOTSTRAP_REASON not in normalized_reasons:
            return _blocked(
                "PORTFOLIO_ACTIVATION_GATE_BOOTSTRAP_EVIDENCE_INVALID",
                artifact_status,
                "portfolio bootstrap approved intents must carry the governed PAPER bootstrap reason code",
                stage,
                failed_field=f"approved_executable_intents[{index}].reason_codes",
                expected_value=PORTFOLIO_BOOTSTRAP_REASON,
                actual_value=normalized_reasons,
            )
    return {
        "status": "PASS",
        "artifact_status": artifact_status,
        "paper_bootstrap_accepted": True,
        "portfolio_bootstrap_reason": PORTFOLIO_BOOTSTRAP_REASON,
        "detail": "portfolio activation gate bootstrap status accepted for PAPER only",
    }


def _run_paper_authority_pointer_refresh_stage(
    *,
    stage: KernelStage,
    artifact_path: Path | None,
    target_day: str,
    sleeve_root: Path,
    runner: Callable[[list[str], Path], subprocess.CompletedProcess[str]],
    workdir: Path,
    release_commit: str,
) -> dict[str, Any]:
    if artifact_path is None:
        return _blocked(
            "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
            "MISSING",
            "authority pointer refresh stage has no authority-head artifact path",
            stage,
            failed_field="artifact_path",
            expected_value="run_pointer_v2/canonical_authority_head.v1.json",
            actual_value="",
        )

    verdict_path = _same_day_authorization_gate_verdict_path(authority_head_path=artifact_path, target_day=target_day)
    verdict = _read_json(verdict_path)
    verdict_validation = _validate_same_day_authorization_verdict_for_pointer_refresh(
        verdict=verdict,
        verdict_path=verdict_path,
        target_day=target_day,
        stage=stage,
    )
    if verdict_validation["status"] != "PASS":
        return verdict_validation

    current_head = _read_json(artifact_path)
    if isinstance(current_head, dict):
        current_validation = _validate_paper_authority_head_freshness(
            data=current_head,
            artifact_path=artifact_path,
            target_day=target_day,
            stage=stage,
        )
        if current_validation["status"] == "PASS":
            current_validation["authority_pointer_refresh"] = {
                "action": "SKIPPED_HEAD_ALREADY_FRESH",
                "authorization_verdict_path": str(verdict_path),
                "append_attempted": False,
                "head_materialization_attempted": False,
            }
            current_validation["detail"] = "same-day PAPER authority head was already fresh"
            return current_validation

    cfg_hash = _sha256_file((workdir / PAPER_DAY_ORCHESTRATOR_SERVICE_REL).resolve())
    policy_hash = _sha256_file((workdir / GATE_HIERARCHY_POLICY_REL).resolve())

    attempt_cmd = [
        "python3",
        "ops/tools/run_pointer_attempt_alloc_v1.py",
        "--day_utc",
        target_day,
        "--mode",
        "PAPER",
        "--orchestrator_config_hash",
        cfg_hash,
        "--git_sha",
        release_commit,
        "--truth_root",
        str(sleeve_root),
    ]
    attempt_completed = runner(attempt_cmd, workdir)
    subcommands: list[dict[str, Any]] = [_command_result("run_pointer_attempt_alloc_v1", attempt_cmd, attempt_completed)]
    if attempt_completed.returncode != 0:
        return _authority_pointer_refresh_blocked(
            stage=stage,
            blocker="AUTHORITY_POINTER_REFRESH_FAILED",
            detail="pointer attempt allocation failed",
            failed_field="run_pointer_attempt_alloc_v1.returncode",
            expected_value=0,
            actual_value=attempt_completed.returncode,
            verdict_path=verdict_path,
            subcommands=subcommands,
        )

    attempt_payload = _json_stdout(attempt_completed.stdout)
    attempt_id = str(attempt_payload.get("attempt_id") or "").strip()
    attempt_seq = _int_value(attempt_payload.get("attempt_seq"))
    if not attempt_id or attempt_seq <= 0:
        return _authority_pointer_refresh_blocked(
            stage=stage,
            blocker="AUTHORITY_POINTER_REFRESH_FAILED",
            detail="pointer attempt allocation did not return a valid attempt id",
            failed_field="run_pointer_attempt_alloc_v1.stdout",
            expected_value="attempt_id and positive attempt_seq",
            actual_value=(attempt_completed.stdout or "")[-500:],
            verdict_path=verdict_path,
            subcommands=subcommands,
        )

    append_cmd = [
        "python3",
        "ops/tools/run_pointer_append_v1.py",
        "--day_utc",
        target_day,
        "--attempt_id",
        attempt_id,
        "--attempt_seq",
        str(attempt_seq),
        "--mode",
        "PAPER",
        "--status",
        "PASS",
        "--authoritative",
        "YES",
        "--policy_hash",
        policy_hash,
        "--orchestrator_config_hash",
        cfg_hash,
        "--produced_utc",
        f"{target_day}T00:00:00Z",
        "--points_to",
        str(verdict_path),
        "--git_sha",
        release_commit,
        "--truth_root",
        str(sleeve_root),
    ]
    append_completed = runner(append_cmd, workdir)
    subcommands.append(_command_result("run_pointer_append_v1", append_cmd, append_completed))
    if append_completed.returncode != 0:
        return _authority_pointer_refresh_blocked(
            stage=stage,
            blocker="AUTHORITY_POINTER_REFRESH_FAILED",
            detail="pointer append failed",
            failed_field="run_pointer_append_v1.returncode",
            expected_value=0,
            actual_value=append_completed.returncode,
            verdict_path=verdict_path,
            subcommands=subcommands,
        )

    heads_cmd = [
        "python3",
        "ops/tools/run_pointer_heads_materialize_v1.py",
        "--fail_if_no_authority_head",
        "YES",
        "--expected_day_utc",
        target_day,
        "--truth_root",
        str(sleeve_root),
    ]
    heads_completed = runner(heads_cmd, workdir)
    subcommands.append(_command_result("run_pointer_heads_materialize_v1", heads_cmd, heads_completed))
    if heads_completed.returncode != 0:
        return _authority_pointer_refresh_blocked(
            stage=stage,
            blocker="AUTHORITY_POINTER_REFRESH_FAILED",
            detail="pointer head materialization failed",
            failed_field="run_pointer_heads_materialize_v1.returncode",
            expected_value=0,
            actual_value=heads_completed.returncode,
            verdict_path=verdict_path,
            subcommands=subcommands,
        )

    refreshed_head = _read_json(artifact_path)
    if not isinstance(refreshed_head, dict):
        return _authority_pointer_refresh_blocked(
            stage=stage,
            blocker="AUTHORITY_HEAD_NOT_READY_FOR_DAY",
            detail="pointer refresh completed but same-day authority head is missing or invalid",
            failed_field="run_pointer_v2/canonical_authority_head.v1.json",
            expected_value="fresh same-day authority head",
            actual_value="missing_or_invalid",
            verdict_path=verdict_path,
            subcommands=subcommands,
        )
    validation = _validate_paper_authority_head_freshness(
        data=refreshed_head,
        artifact_path=artifact_path,
        target_day=target_day,
        stage=stage,
    )
    validation["authority_pointer_refresh"] = {
        "action": "MATERIALIZED",
        "authorization_verdict_path": str(verdict_path),
        "append_attempted": True,
        "head_materialization_attempted": True,
        "subcommands": subcommands,
    }
    if validation["status"] != "PASS":
        validation["detail"] = "pointer refresh did not produce a fresh same-day authority head"
    return validation


def _validate_same_day_authorization_verdict_for_pointer_refresh(
    *,
    verdict: dict[str, Any] | None,
    verdict_path: Path,
    target_day: str,
    stage: KernelStage,
) -> dict[str, Any]:
    if not isinstance(verdict, dict):
        return _blocked(
            "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
            "MISSING",
            "same-day PAPER authorization verdict is missing before authority pointer refresh",
            stage,
            failed_field="authorization_gate_verdict_v1",
            expected_value="same-day PASS or BOOTSTRAP_PASS verdict",
            actual_value=str(verdict_path),
        )
    schema_id = str(verdict.get("schema_id") or "").strip()
    schema_version = str(verdict.get("schema_version") or "").strip()
    verdict_day = str(verdict.get("day_utc") or "").strip()
    verdict_status = _status_of(verdict)
    allowed_statuses = {"PASS", "BOOTSTRAP_PASS"}
    if schema_id != "authorization_gate_verdict_v1" or schema_version not in {"1", "v1"}:
        return _blocked(
            "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
            verdict_status,
            "same-day PAPER authorization verdict schema is invalid",
            stage,
            failed_field="authorization_gate_verdict_v1.schema",
            expected_value="authorization_gate_verdict_v1/v1",
            actual_value=f"{schema_id}/{schema_version}",
        )
    if verdict_day != target_day:
        return _blocked(
            "AUTHORITY_HEAD_DAY_MISMATCH",
            verdict_status,
            "same-day PAPER authorization verdict day does not match target day",
            stage,
            failed_field="authorization_gate_verdict_v1.day_utc",
            expected_value=target_day,
            actual_value=verdict_day,
        )
    if verdict_status not in allowed_statuses:
        return _blocked(
            "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
            verdict_status,
            "same-day PAPER authorization verdict is not PASS or BOOTSTRAP_PASS",
            stage,
            failed_field="authorization_gate_verdict_v1.status",
            expected_value=sorted(allowed_statuses),
            actual_value=verdict_status,
        )
    return {"status": "PASS", "artifact_status": verdict_status}


def _authority_pointer_refresh_blocked(
    *,
    stage: KernelStage,
    blocker: str,
    detail: str,
    failed_field: str,
    expected_value: Any,
    actual_value: Any,
    verdict_path: Path,
    subcommands: list[dict[str, Any]],
) -> dict[str, Any]:
    result = _blocked(
        blocker,
        "FAILED",
        detail,
        stage,
        failed_field=failed_field,
        expected_value=expected_value,
        actual_value=actual_value,
    )
    result["authority_pointer_refresh"] = {
        "action": "FAILED",
        "authorization_verdict_path": str(verdict_path),
        "append_attempted": any(row.get("tool") == "run_pointer_append_v1" for row in subcommands),
        "head_materialization_attempted": any(row.get("tool") == "run_pointer_heads_materialize_v1" for row in subcommands),
        "subcommands": subcommands,
    }
    return result


def _validate_paper_authority_head_freshness(
    *,
    data: dict[str, Any],
    artifact_path: Path,
    target_day: str,
    stage: KernelStage,
) -> dict[str, Any]:
    schema_id = str(data.get("schema_id") or "").strip()
    schema_version = str(data.get("schema_version") or "").strip()
    observed_day = str(data.get("day_utc") or "").strip()
    artifact_status = str(data.get("status") or "").strip().upper() or "UNKNOWN"
    authoritative = data.get("authoritative") is True
    points_to = str(data.get("points_to") or "").strip()
    allowed_statuses = {"PASS", "BOOTSTRAP_PASS"}

    if schema_id != "c2_run_pointer_canonical_authority_head":
        return _authority_head_blocked(
            "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
            artifact_status,
            "PAPER canonical authority head schema_id is invalid",
            stage,
            target_day=target_day,
            artifact_path=artifact_path,
            head_data=data,
            failed_field="schema_id",
            expected_value="c2_run_pointer_canonical_authority_head",
            actual_value=schema_id,
        )
    if schema_version != "v1":
        return _authority_head_blocked(
            "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
            artifact_status,
            "PAPER canonical authority head schema_version is invalid",
            stage,
            target_day=target_day,
            artifact_path=artifact_path,
            head_data=data,
            failed_field="schema_version",
            expected_value="v1",
            actual_value=schema_version,
        )
    if observed_day != target_day:
        return _authority_head_blocked(
            "AUTHORITY_HEAD_DAY_MISMATCH",
            artifact_status,
            "PAPER canonical authority head does not match the target day",
            stage,
            target_day=target_day,
            artifact_path=artifact_path,
            head_data=data,
            failed_field="day_utc",
            expected_value=target_day,
            actual_value=observed_day,
        )
    if artifact_status not in allowed_statuses:
        return _authority_head_blocked(
            "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
            artifact_status,
            "PAPER canonical authority head is not PASS or BOOTSTRAP_PASS",
            stage,
            target_day=target_day,
            artifact_path=artifact_path,
            head_data=data,
            failed_field="status",
            expected_value=sorted(allowed_statuses),
            actual_value=artifact_status,
        )
    if not authoritative:
        return _authority_head_blocked(
            "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
            artifact_status,
            "PAPER canonical authority head is not authoritative",
            stage,
            target_day=target_day,
            artifact_path=artifact_path,
            head_data=data,
            failed_field="authoritative",
            expected_value=True,
            actual_value=data.get("authoritative"),
        )
    if not points_to:
        return _authority_head_blocked(
            "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
            artifact_status,
            "PAPER canonical authority head does not point to an authorization verdict",
            stage,
            target_day=target_day,
            artifact_path=artifact_path,
            head_data=data,
            failed_field="points_to",
            expected_value="same-day authorization_gate_verdict_v1",
            actual_value=points_to,
        )

    verdict_path = _resolve_authority_points_to(points_to=points_to, authority_head_path=artifact_path)
    verdict = _read_json(verdict_path)
    if verdict is None:
        return _authority_head_blocked(
            "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
            artifact_status,
            "PAPER canonical authority head points to a missing or invalid authorization verdict",
            stage,
            target_day=target_day,
            artifact_path=artifact_path,
            head_data=data,
            failed_field="points_to",
            expected_value="existing same-day authorization_gate_verdict_v1",
            actual_value=points_to,
        )
    verdict_schema_id = str(verdict.get("schema_id") or "").strip()
    verdict_schema_version = str(verdict.get("schema_version") or "").strip()
    verdict_day = str(verdict.get("day_utc") or "").strip()
    verdict_status = _status_of(verdict)
    verdict_name = verdict_path.name
    verdict_text = str(verdict_path)
    if verdict_name == "authorization_gate_verdict.v1.json" or "authorization_gate_verdict_v1" in verdict_text:
        if verdict_schema_id != "authorization_gate_verdict_v1" or verdict_schema_version not in {"1", "v1"}:
            return _authority_head_blocked(
                "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
                artifact_status,
                "PAPER canonical authority head points to an invalid authorization_gate_verdict_v1",
                stage,
                target_day=target_day,
                artifact_path=artifact_path,
                head_data=data,
                verdict_data=verdict,
                failed_field="points_to.schema",
                expected_value="authorization_gate_verdict_v1/v1",
                actual_value=f"{verdict_schema_id}/{verdict_schema_version}",
            )
    elif verdict_name == "gate_stack_verdict.v1.json" or "gate_stack_verdict_v1" in verdict_text:
        if verdict_schema_id != "gate_stack_verdict" or verdict_schema_version != "v1":
            return _authority_head_blocked(
                "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
                artifact_status,
                "PAPER canonical authority head points to an invalid gate_stack_verdict_v1",
                stage,
                target_day=target_day,
                artifact_path=artifact_path,
                head_data=data,
                verdict_data=verdict,
                failed_field="points_to.schema",
                expected_value="gate_stack_verdict/v1",
                actual_value=f"{verdict_schema_id}/{verdict_schema_version}",
            )
    else:
        return _authority_head_blocked(
            "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
            artifact_status,
            "PAPER canonical authority head points to an unsupported verdict artifact",
            stage,
            target_day=target_day,
            artifact_path=artifact_path,
            head_data=data,
            verdict_data=verdict,
            failed_field="points_to",
            expected_value="authorization_gate_verdict_v1 or gate_stack_verdict_v1",
            actual_value=points_to,
        )
    if verdict_day != target_day:
        return _authority_head_blocked(
            "AUTHORITY_HEAD_DAY_MISMATCH",
            artifact_status,
            "PAPER canonical authority head points to a verdict for the wrong day",
            stage,
            target_day=target_day,
            artifact_path=artifact_path,
            head_data=data,
            verdict_data=verdict,
            failed_field="points_to.day_utc",
            expected_value=target_day,
            actual_value=verdict_day,
        )
    if verdict_status not in allowed_statuses:
        return _authority_head_blocked(
            "AUTHORITY_HEAD_NOT_READY_FOR_DAY",
            artifact_status,
            "PAPER same-day authorization verdict is not PASS or BOOTSTRAP_PASS",
            stage,
            target_day=target_day,
            artifact_path=artifact_path,
            head_data=data,
            verdict_data=verdict,
            failed_field="points_to.status",
            expected_value=sorted(allowed_statuses),
            actual_value=verdict_status,
        )
    return {
        "status": "PASS",
        "artifact_status": artifact_status,
        "authority_head_freshness": _authority_head_diagnostics(
            target_day=target_day,
            artifact_path=artifact_path,
            head_data=data,
            verdict_data=verdict,
        ),
        "detail": "same-day PAPER canonical authority head is fresh before capital allocation",
    }


def _authority_head_blocked(
    blocker: str,
    artifact_status: str,
    detail: str,
    stage: KernelStage,
    *,
    target_day: str,
    artifact_path: Path,
    failed_field: str,
    expected_value: Any,
    actual_value: Any,
    head_data: dict[str, Any] | None = None,
    verdict_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = _blocked(
        blocker,
        artifact_status,
        detail,
        stage,
        failed_field=failed_field,
        expected_value=expected_value,
        actual_value=actual_value,
    )
    result["authority_head_freshness"] = _authority_head_diagnostics(
        target_day=target_day,
        artifact_path=artifact_path,
        head_data=head_data or {},
        verdict_data=verdict_data,
    )
    return result


def _authority_head_diagnostics(
    *,
    target_day: str,
    artifact_path: Path,
    head_data: dict[str, Any],
    verdict_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    points_to = str(head_data.get("points_to") or "").strip()
    pointed_verdict = verdict_data if isinstance(verdict_data, dict) else None
    if pointed_verdict is None and points_to:
        pointed_verdict = _read_json(_resolve_authority_points_to(points_to=points_to, authority_head_path=artifact_path))
    same_day_verdict = _read_json(_same_day_authorization_gate_verdict_path(authority_head_path=artifact_path, target_day=target_day))
    diagnostics_verdict = same_day_verdict if isinstance(same_day_verdict, dict) else pointed_verdict
    return {
        "expected_day_utc": target_day,
        "observed_head_day_utc": str(head_data.get("day_utc") or "").strip(),
        "observed_status": str(head_data.get("status") or "").strip().upper(),
        "authoritative": head_data.get("authoritative") is True,
        "points_to": points_to,
        "points_to_verdict_status": _status_of(pointed_verdict) if isinstance(pointed_verdict, dict) else "",
        "same_day_authorization_verdict_status": _status_of(same_day_verdict) if isinstance(same_day_verdict, dict) else "",
        "authorization_missing_inputs": _authorization_missing_inputs(diagnostics_verdict if isinstance(diagnostics_verdict, dict) else {}),
        "authorization_failure_reasons": _authorization_failure_reasons(diagnostics_verdict if isinstance(diagnostics_verdict, dict) else {}),
    }


def _resolve_authority_points_to(*, points_to: str, authority_head_path: Path) -> Path:
    raw = Path(points_to)
    if raw.is_absolute():
        return raw.resolve()
    sleeve_root = authority_head_path.parent.parent
    return (sleeve_root / raw).resolve()


def _same_day_authorization_gate_verdict_path(*, authority_head_path: Path, target_day: str) -> Path:
    sleeve_root = authority_head_path.parent.parent
    return sleeve_root / "reports" / "authorization_gate_verdict_v1" / target_day / "authorization_gate_verdict.v1.json"


def _authorization_failure_reasons(verdict: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("reason_codes", "blocking_reason_codes", "failure_reasons", "blocking_codes"):
        raw = verdict.get(key)
        if isinstance(raw, list):
            values.extend(str(item).strip() for item in raw if str(item).strip())
    return sorted(dict.fromkeys(values))


def _authorization_missing_inputs(verdict: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for reason in _authorization_failure_reasons(verdict):
        parts = [part for part in reason.split(":") if part]
        if parts and parts[-1].upper() == "MISSING" and len(parts) >= 2:
            missing.append(parts[-2])
    return sorted(dict.fromkeys(missing))


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
    for key in ("status", "final_status", "boundary_status", "state", "decision", "convergence_status"):
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


def _json_stdout(stdout: str | None) -> dict[str, Any]:
    text = str(stdout or "").strip()
    if not text:
        return {}
    try:
        obj = json.loads(text)
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _command_result(tool: str, command: list[str], completed: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    return {
        "tool": tool,
        "command": _command_text(command),
        "returncode": completed.returncode,
        "stdout_tail": (completed.stdout or "")[-500:],
        "stderr_tail": (completed.stderr or "")[-500:],
    }


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _iso_before(left: str, right: str) -> bool:
    left_dt = _parse_iso_utc(left)
    right_dt = _parse_iso_utc(right)
    if left_dt is None or right_dt is None:
        return True
    return left_dt < right_dt


def _parse_iso_utc(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
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
