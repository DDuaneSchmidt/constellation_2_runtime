#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.decision_authority_bridge_v1 import resolve_decision_truth_root_bridge_v1
from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from constellation_2.common.paper_submit_mode_status_v1 import classify_paper_submit_mode_status_v1
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1
from constellation_2.common.sleeve_execution_root_v1 import resolve_sleeve_execution_root_v1
from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
import ops.tools.run_c2_paper_day_orchestrator_v2 as orchestrator_v2


CommandRunner = Callable[[list[str], dict[str, str]], dict[str, Any]]


@dataclass(frozen=True)
class SubmitContext:
    day_utc: str
    produced_utc: str
    truth_root: Path
    execution_root: Path
    ib_account: str
    paper_session_ledger_path: Path
    dry_run_policy: str
    env: dict[str, str]


def _utc_now_isoz() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"], text=True).strip()
        return out or "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return payload


def _load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    return _read_json(path)


def _path_status(path: Path) -> dict[str, Any]:
    return {"path": str(path), "exists": path.exists()}


def _authority_path(truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "paper_trading_day_authority_v1" / day_utc / "paper_trading_day_authority.v1.json").resolve()


def _submit_boundary_path(truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json").resolve()


def _kill_switch_path(execution_root: Path, day_utc: str) -> Path:
    return (execution_root / "risk_v1" / "kill_switch_v1" / day_utc / "global_kill_switch_state.v1.json").resolve()


def _paper_session_ledger_path(truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json").resolve()


def _is_submit_boundary_authorized(payload: dict[str, Any]) -> bool:
    status = str(payload.get("status") or payload.get("boundary_status") or "").strip().upper()
    authorized = bool(payload.get("submission_authorized") is True or payload.get("submit_allowed") is True)
    decision = str(payload.get("decision") or payload.get("authorization_status") or "").strip().upper()
    return status == "READY" and (authorized or decision == "AUTHORIZED")


def _is_kill_switch_inactive(payload: dict[str, Any]) -> bool:
    state = str(payload.get("state") or payload.get("status") or "").strip().upper()
    if bool(payload.get("active") is True or payload.get("kill_switch_active") is True):
        return False
    return state in {"INACTIVE", "CLEAR", "PASS"}


def _phasec_attempt_dirs(day_root: Path) -> list[Path]:
    latest_pointer = (day_root / "latest_active_attempt.v1.json").resolve()
    if latest_pointer.exists() and latest_pointer.is_file():
        pointer = _load_json_if_exists(latest_pointer) or {}
        attempt_dir_text = str(pointer.get("attempt_dir") or "").strip()
        if attempt_dir_text:
            attempt_dir = Path(attempt_dir_text).resolve()
            if attempt_dir.exists() and attempt_dir.is_dir():
                return [attempt_dir]
        attempt_id = str(pointer.get("attempt_id") or "").strip()
        if attempt_id:
            attempt_dir = (day_root / f"attempt_{attempt_id}").resolve()
            if attempt_dir.exists() and attempt_dir.is_dir():
                return [attempt_dir]

    attempts = sorted(path.resolve() for path in day_root.glob("attempt_A*") if path.is_dir())
    return attempts[-1:] if attempts else []


def _released_phasec_candidates(*, truth_root: Path, day_utc: str, ib_account: str) -> list[Path]:
    del ib_account  # The caller has already resolved the governed execution root for this account.
    day_root = (truth_root / "phaseC_preflight_v1" / day_utc).resolve()
    if not day_root.exists() or not day_root.is_dir():
        return []
    candidates: list[Path] = []
    for attempt_dir in _phasec_attempt_dirs(day_root):
        candidates.extend(path.resolve() for path in sorted(attempt_dir.iterdir()) if path.is_dir())
    released: list[Path] = []
    for candidate in candidates:
        decision_path = (candidate / "submit_preflight_decision.v1.json").resolve()
        identity_path = (candidate / "execution_identity_record.v1.json").resolve()
        if not identity_path.exists() or not identity_path.is_file():
            continue
        decision_payload = _load_json_if_exists(decision_path)
        decision = str((decision_payload or {}).get("decision") or "").strip().upper()
        if decision == "ALLOW":
            released.append(candidate.resolve())
    return released


def evaluate_submit_guards_v1(ctx: SubmitContext) -> dict[str, Any]:
    blockers: list[dict[str, str]] = []

    authority_path = _authority_path(ctx.truth_root, ctx.day_utc)
    authority = _load_json_if_exists(authority_path)
    if authority is None:
        blockers.append({"code": "PAPER_DAY_AUTHORITY_MISSING", "path": str(authority_path)})
    else:
        state = str(authority.get("state") or "").strip().upper()
        can_submit = bool(authority.get("can_submit_paper_orders") is True)
        if state != "OPEN_READY":
            blockers.append({"code": "PAPER_DAY_AUTHORITY_NOT_OPEN_READY", "path": str(authority_path), "observed": state})
        if not can_submit:
            blockers.append({"code": "PAPER_DAY_AUTHORITY_CANNOT_SUBMIT", "path": str(authority_path)})

    boundary_path = _submit_boundary_path(ctx.truth_root, ctx.day_utc)
    boundary = _load_json_if_exists(boundary_path)
    if boundary is None:
        blockers.append({"code": "SUBMIT_BOUNDARY_STATUS_MISSING", "path": str(boundary_path)})
    elif not _is_submit_boundary_authorized(boundary):
        blockers.append(
            {
                "code": "SUBMIT_BOUNDARY_NOT_READY_AUTHORIZED",
                "path": str(boundary_path),
                "status": str(boundary.get("status") or boundary.get("boundary_status") or ""),
            }
        )

    kill_path = _kill_switch_path(ctx.execution_root, ctx.day_utc)
    kill_switch = _load_json_if_exists(kill_path)
    if kill_switch is None:
        blockers.append({"code": "GLOBAL_KILL_SWITCH_STATE_MISSING", "path": str(kill_path)})
    elif not _is_kill_switch_inactive(kill_switch):
        blockers.append(
            {
                "code": "GLOBAL_KILL_SWITCH_ACTIVE",
                "path": str(kill_path),
                "state": str(kill_switch.get("state") or kill_switch.get("status") or ""),
            }
        )

    ledger_path = ctx.paper_session_ledger_path
    if not ledger_path.exists() or not ledger_path.is_file():
        blockers.append({"code": "PAPER_SESSION_LEDGER_MISSING", "path": str(ledger_path)})

    released = _released_phasec_candidates(truth_root=ctx.execution_root, day_utc=ctx.day_utc, ib_account=ctx.ib_account)
    if not released:
        blockers.append(
            {
                "code": "PHASEC_RELEASED_CANDIDATE_MISSING",
                "path": str((ctx.execution_root / "phaseC_preflight_v1" / ctx.day_utc).resolve()),
            }
        )

    return {
        "ok": not blockers,
        "blockers": blockers,
        "released_candidate_paths": [str(path) for path in released],
        "authority_path": str(authority_path),
        "submit_boundary_path": str(boundary_path),
        "kill_switch_path": str(kill_path),
        "paper_session_ledger_path": str(ledger_path),
    }


def _default_command_runner(cmd: list[str], env: dict[str, str]) -> dict[str, Any]:
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "cmd": cmd,
        "return_code": int(proc.returncode),
        "stdout": str(proc.stdout or "").strip(),
        "stderr": str(proc.stderr or "").strip(),
    }


def _authority_step(name: str, cmd: list[str], env: dict[str, str]) -> dict[str, Any]:
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "name": name,
        "cmd": cmd,
        "return_code": int(proc.returncode),
        "stdout": str(proc.stdout or "").strip(),
        "stderr": str(proc.stderr or "").strip(),
    }


def _authorization_cmd(ctx: SubmitContext) -> list[str]:
    return [
        sys.executable,
        "ops/tools/run_authorization_artifacts_day_v1.py",
        "--day_utc",
        ctx.day_utc,
        "--truth_root",
        str(ctx.execution_root),
    ]


def _execution_build_cmd(candidate_path: Path) -> list[str]:
    return [
        sys.executable,
        "ops/tools/run_execution_build_authority_v1.py",
        "--operation_type",
        "fresh_paper_entry_v1",
        "--candidate_path",
        str(candidate_path.resolve()),
        "--materialize",
        "YES",
        "--emit_package",
        "YES",
    ]


def _trade_submit_readiness_cmd(ctx: SubmitContext) -> list[str]:
    return [
        sys.executable,
        "ops/tools/run_trade_submit_readiness_c2_v1.py",
        "--day_utc",
        ctx.day_utc,
        "--ib_account",
        ctx.ib_account,
        "--environment",
        "PAPER",
    ]


def _projection_cmds(ctx: SubmitContext) -> list[list[str]]:
    git_sha = _git_sha()
    return [
        [
            sys.executable,
            "-m",
            "constellation_2.phaseF.execution_evidence.run.run_execution_evidence_truth_day_v1",
            "--day_utc",
            ctx.day_utc,
            "--producer_git_sha",
            git_sha,
            "--producer_repo",
            REPO_ROOT.name,
            "--truth_root",
            str(ctx.truth_root),
            "--source_truth_root",
            str(ctx.execution_root),
        ],
        [
            sys.executable,
            "ops/tools/run_submit_decision_trace_v1.py",
            "--day_utc",
            ctx.day_utc,
            "--truth_root",
            str(ctx.truth_root),
            "--environment",
            "PAPER",
        ],
        [
            sys.executable,
            "ops/tools/run_sleeve_intent_trade_attribution_v1.py",
            "--day_utc",
            ctx.day_utc,
            "--truth_root",
            str(ctx.truth_root),
            "--environment",
            "PAPER",
        ],
        [sys.executable, "ops/tools/run_submission_index_v1_sleeve.py", "--day_utc", ctx.day_utc],
        [sys.executable, "ops/tools/run_execution_evidence_current_head_v1.py", "--day_utc", ctx.day_utc],
    ]


def _candidate_submission_id(candidate_path: Path) -> str:
    try:
        payload = orchestrator_v2._load_identity_submit_payload(candidate_path)  # noqa: SLF001
        return str(payload.get("submission_id") or "").strip().lower()
    except Exception:
        return ""


def collect_submit_evidence_v1(ctx: SubmitContext, candidate_paths: list[Path]) -> dict[str, Any]:
    submission_ids = [sid for sid in (_candidate_submission_id(path) for path in candidate_paths) if sid]
    intent_hashes = [path.name for path in candidate_paths]
    engine_auth_paths = [
        ctx.execution_root / "engine_activity_v1" / "authorization_v1" / ctx.day_utc / f"{intent_hash}.authorization.v1.json"
        for intent_hash in intent_hashes
    ]
    execution_build_paths = [
        ctx.truth_root / "reports" / "execution_build_v1" / ctx.day_utc / sid / "execution_build.v1.json"
        for sid in submission_ids
    ]
    execution_package_paths = [
        ctx.execution_root / "execution_package_v1" / ctx.day_utc / sid / "execution_package.v1.json"
        for sid in submission_ids
    ]
    submission_dirs = [
        ctx.execution_root / "execution_evidence_v1" / "submissions" / ctx.day_utc / sid
        for sid in submission_ids
    ]
    broker_submission_paths = [path / "broker_submission_record.v2.json" for path in submission_dirs]
    submit_decision_root = ctx.truth_root / "reports" / "submit_decision_trace_v1" / ctx.day_utc
    attribution_path = ctx.truth_root / "reports" / "sleeve_intent_trade_attribution_v1" / ctx.day_utc / "sleeve_intent_trade_attribution.v1.json"
    submission_index_path = ctx.execution_root / "submission_index_v1" / ctx.day_utc / "submission_index.v1.json"
    current_head_path = ctx.execution_root / "execution_evidence_v1" / "current_head" / ctx.day_utc / "current_head.v1.json"
    submissions_day_path = ctx.execution_root / "execution_evidence_v1" / "submissions" / ctx.day_utc
    return {
        "submission_ids": submission_ids,
        "engine_activity_authorization_v1": [_path_status(path.resolve()) for path in engine_auth_paths],
        "execution_build_v1": [_path_status(path.resolve()) for path in execution_build_paths],
        "execution_package_v1": [_path_status(path.resolve()) for path in execution_package_paths],
        "submit_decision_trace_v1": {
            "root": str(submit_decision_root.resolve()),
            "exists": submit_decision_root.exists(),
            "file_count": len(list(submit_decision_root.rglob("submit_decision_trace.v1.json"))) if submit_decision_root.exists() else 0,
        },
        "sleeve_intent_trade_attribution_v1": _path_status(attribution_path.resolve()),
        "submission_index_v1": _path_status(submission_index_path.resolve()),
        "execution_evidence_current_head_v1": _path_status(current_head_path.resolve()),
        "execution_evidence_submissions": {
            "path": str(submissions_day_path.resolve()),
            "exists": submissions_day_path.exists(),
            "submission_dirs": [str(path.resolve()) for path in submission_dirs if path.exists()],
            "broker_submission_record_paths": [str(path.resolve()) for path in broker_submission_paths if path.exists()],
            "broker_submission_record_count": len([path for path in broker_submission_paths if path.exists()]),
        },
    }


def run_aegis_paper_submit_v1(ctx: SubmitContext, *, runner: CommandRunner = _default_command_runner) -> dict[str, Any]:
    guards = evaluate_submit_guards_v1(ctx)
    submit_mode = classify_paper_submit_mode_status_v1(execution_root=ctx.execution_root, day_utc=ctx.day_utc)
    submit_mode_status = str(submit_mode.get("submit_mode_status") or "NO_SUBMIT_ATTEMPT").strip().upper()
    candidate_paths = [Path(path).resolve() for path in guards.get("released_candidate_paths", [])]
    report: dict[str, Any] = {
        "schema_id": "aegis_paper_submit_run.v1",
        "day_utc": ctx.day_utc,
        "status": "BLOCKED" if not guards["ok"] else "RUNNING",
        "dry_run_policy": ctx.dry_run_policy,
        "broker_transmit_enabled": ctx.dry_run_policy == "NO",
        "submit_mode_status": submit_mode_status,
        "broker_order_transmitted": bool(submit_mode.get("broker_order_transmitted") is True),
        "missing_broker_ids_blocker": bool(submit_mode.get("missing_broker_ids_blocker") is True),
        "missing_broker_ids_diagnostic": bool(submit_mode.get("missing_broker_ids_diagnostic") is True),
        "guards": guards,
        "steps": [],
        "governed_submit": {},
        "evidence": {},
    }
    if submit_mode_status == "DRY_RUN_COMPLETE":
        report["status"] = "DRY_RUN_COMPLETE"
        report["broker_transmit_enabled"] = False
        report["reason_codes"] = []
        report["evidence"] = collect_submit_evidence_v1(ctx, candidate_paths)
        report["governed_submit"] = {
            "producer": "ops/tools/run_c2_paper_day_orchestrator_v2.py::_run_governed_submit_stage",
            "return_code": 0,
            "reason_codes": ["DRY_RUN_ALREADY_COMPLETE"],
        }
        return report
    if not guards["ok"]:
        report["status"] = "BLOCKED"
        report["reason_codes"] = [str(row.get("code") or "UNKNOWN") for row in guards["blockers"]]
        return report

    auth_step = runner(_authorization_cmd(ctx), ctx.env)
    auth_step["name"] = "engine_activity_authorization_v1"
    report["steps"].append(auth_step)
    if int(auth_step.get("return_code") or 0) != 0:
        report["status"] = "FAILED"
        report["reason_codes"] = ["ENGINE_ACTIVITY_AUTHORIZATION_FAILED"]
        report["evidence"] = collect_submit_evidence_v1(ctx, candidate_paths)
        return report

    readiness_step = runner(_trade_submit_readiness_cmd(ctx), ctx.env)
    readiness_step["name"] = "trade_submit_readiness_c2_v1"
    report["steps"].append(readiness_step)
    if int(readiness_step.get("return_code") or 0) != 0:
        report["status"] = "FAILED"
        report["reason_codes"] = ["TRADE_SUBMIT_READINESS_FAILED"]
        report["evidence"] = collect_submit_evidence_v1(ctx, candidate_paths)
        return report

    for candidate_path in candidate_paths:
        build_step = runner(_execution_build_cmd(candidate_path), ctx.env)
        build_step["name"] = "execution_build_authority_v1"
        build_step["candidate_path"] = str(candidate_path)
        report["steps"].append(build_step)
        if int(build_step.get("return_code") or 0) != 0:
            report["status"] = "FAILED"
            report["reason_codes"] = ["EXECUTION_BUILD_AUTHORITY_FAILED"]
            report["evidence"] = collect_submit_evidence_v1(ctx, candidate_paths)
            return report

    gov_rc, gov_reason_codes = orchestrator_v2._run_governed_submit_stage(  # noqa: SLF001
        truth_root=ctx.execution_root,
        day=ctx.day_utc,
        produced_utc=ctx.produced_utc,
        ib_account=ctx.ib_account,
        env=ctx.env,
    )
    report["governed_submit"] = {
        "producer": "ops/tools/run_c2_paper_day_orchestrator_v2.py::_run_governed_submit_stage",
        "return_code": int(gov_rc),
        "reason_codes": list(gov_reason_codes),
    }
    if int(gov_rc) != 0:
        report["status"] = "FAILED"
        report["reason_codes"] = list(gov_reason_codes) or ["GOVERNED_SUBMIT_FAILED"]
        report["evidence"] = collect_submit_evidence_v1(ctx, candidate_paths)
        return report

    projection_failures: list[str] = []
    for cmd in _projection_cmds(ctx):
        step = runner(cmd, ctx.env)
        step["name"] = Path(cmd[1]).name if len(cmd) > 1 and not str(cmd[1]).startswith("-") else "execution_evidence_truth_day_v1"
        report["steps"].append(step)
        if int(step.get("return_code") or 0) != 0:
            projection_failures.append(str(step["name"]))

    report["evidence"] = collect_submit_evidence_v1(ctx, candidate_paths)
    if projection_failures:
        report["status"] = "DEGRADED"
        report["reason_codes"] = [f"PROJECTION_FAILED:{name}" for name in projection_failures]
    else:
        report["status"] = "PASS"
        report["reason_codes"] = []
    return report


def _resolve_context(args: argparse.Namespace) -> SubmitContext:
    day_utc = parse_day_utc_v1(args.day_utc)
    produced_utc = str(args.produced_utc or "").strip() or _utc_now_isoz()
    truth_root = resolve_decision_truth_root_bridge_v1(
        str(args.truth_root or ""),
        repo_root=REPO_ROOT,
        caller="ops/tools/run_aegis_paper_submit_v1.py",
    ).resolve()
    ib_account = str(args.ib_account or "").strip() or resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    execution_resolution = resolve_sleeve_execution_root_v1(
        repo_root=REPO_ROOT,
        environment="PAPER",
        ib_account=ib_account,
        sleeve_id="PRIMARY",
    )
    ledger_path = Path(str(args.paper_session_ledger_path or "").strip()).expanduser().resolve() if str(args.paper_session_ledger_path or "").strip() else _paper_session_ledger_path(truth_root, day_utc)
    env = dict(os.environ)
    dry_run_arg = str(args.dry_run or "ENV").strip().upper()
    if dry_run_arg not in {"ENV", "YES", "NO"}:
        raise SystemExit(f"FAIL: invalid --dry_run {args.dry_run!r}; expected ENV, YES, or NO")
    if dry_run_arg in {"YES", "NO"}:
        env["C2_GOVERNED_SUBMIT_DRY_RUN"] = dry_run_arg
    dry_run_policy = orchestrator_v2._resolve_governed_submit_dry_run(env)  # noqa: SLF001
    return SubmitContext(
        day_utc=day_utc,
        produced_utc=produced_utc,
        truth_root=truth_root,
        execution_root=execution_resolution.execution_root_path.resolve(),
        ib_account=ib_account,
        paper_session_ledger_path=ledger_path,
        dry_run_policy=dry_run_policy,
        env=env,
    )


def main(argv: list[str] | None = None) -> int:
    require_authoritative_repo_runtime_v1(REPO_ROOT)
    parser = argparse.ArgumentParser(prog="run_aegis_paper_submit_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--ib_account", default="")
    parser.add_argument("--paper_session_ledger_path", default="")
    parser.add_argument("--produced_utc", default="")
    parser.add_argument("--dry_run", default="ENV", choices=["ENV", "YES", "NO"])
    args = parser.parse_args(argv)

    ctx = _resolve_context(args)
    pre_authority_steps = [
        _authority_step(
            "execution_mode_authority_v1.pre_submit",
            [
                sys.executable,
                "ops/tools/run_execution_mode_authority_v1.py",
                "--day_utc",
                ctx.day_utc,
                "--truth_root",
                str(ctx.truth_root),
                "--execution_root",
                str(ctx.execution_root),
                "--environment",
                "PAPER",
            ],
            ctx.env,
        ),
        _authority_step(
            "runtime_service_authority_v1.pre_submit",
            [
                sys.executable,
                "ops/tools/run_runtime_service_authority_v1.py",
                "--day_utc",
                ctx.day_utc,
                "--truth_root",
                str(ctx.truth_root),
                "--expected_run_mode",
                "MANUAL",
            ],
            ctx.env,
        ),
    ]
    report = run_aegis_paper_submit_v1(ctx)
    post_authority_steps = [
        _authority_step(
            "execution_lifecycle_authority_v1.post_submit",
            [
                sys.executable,
                "ops/tools/run_execution_lifecycle_authority_v1.py",
                "--day_utc",
                ctx.day_utc,
                "--truth_root",
                str(ctx.truth_root),
                "--execution_root",
                str(ctx.execution_root),
                "--environment",
                "PAPER",
            ],
            ctx.env,
        ),
        _authority_step(
            "trade_lineage_graph_v1.post_submit",
            [
                sys.executable,
                "ops/tools/run_trade_lineage_graph_v1.py",
                "--day_utc",
                ctx.day_utc,
                "--truth_root",
                str(ctx.truth_root),
                "--execution_root",
                str(ctx.execution_root),
                "--environment",
                "PAPER",
            ],
            ctx.env,
        ),
        _authority_step(
            "trading_day_closure_authority_v1.post_submit",
            [
                sys.executable,
                "ops/tools/run_trading_day_closure_authority_v1.py",
                "--day_utc",
                ctx.day_utc,
                "--truth_root",
                str(ctx.truth_root),
                "--execution_root",
                str(ctx.execution_root),
            ],
            ctx.env,
        ),
    ]
    report["authority_steps"] = pre_authority_steps + post_authority_steps
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if str(report.get("status") or "").strip().upper() in {"PASS", "DEGRADED", "DRY_RUN_COMPLETE"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
