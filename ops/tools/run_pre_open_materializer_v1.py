#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.paper_execution_authority_v1 import (
    resolve_governed_paper_execution_profile,
    resolve_governed_paper_execution_roots,
)
from constellation_2.common.pre_open_materializer_v1 import (
    derive_pre_open_bundle_payload_v1,
    resolve_pre_open_bundle_path_v1,
    write_pre_open_bundle_v1,
)
from constellation_2.common.session_authority_v1 import resolve_session_authority_target_day_v1
from constellation_2.common.paper_session_fact_plane_v1 import read_json_object_v1
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_governed_sleeve_truth_bindings
from constellation_2.common.truth_root_v1 import resolve_truth_root
from constellation_2.common.trading_day_readiness_authority_v1 import read_or_evaluate_trading_day_readiness_authority_v1
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1


RUN_IB_API_HANDSHAKE_TOOL = (REPO_ROOT / "ops/tools/run_ib_api_handshake_spine_v1.py").resolve()
RUN_POINTER_HEADS_MATERIALIZE_TOOL = (REPO_ROOT / "ops/tools/run_pointer_heads_materialize_v1.py").resolve()
RUN_GLOBAL_KILL_SWITCH_TOOL = (REPO_ROOT / "ops/tools/run_global_kill_switch_v1.py").resolve()
RUN_BROKER_EVENTS_BOOTSTRAP_TOOL = (REPO_ROOT / "ops/ib/c2_execution_observer_v1.py").resolve()
RUN_BROKER_EVENTS_MANIFEST_TOOL = (REPO_ROOT / "ops/ib/run_broker_event_day_manifest_v1.py").resolve()
RUN_SESSION_AUTHORITY_TOOL = (REPO_ROOT / "ops/tools/run_session_authority_v1.py").resolve()
OWNER_TOOL = "ops/tools/run_pre_open_materializer_v1.py"
PRIMARY_SLEEVE_ID = "PRIMARY"
REASON_ROLLOVER_FAILED_STALE_AUTHORITY_HEAD = "ROLLOVER_FAILED_STALE_AUTHORITY_HEAD"


def _producer_contract_inputs(payload: Dict[str, Any]) -> List[str]:
    paths: List[str] = []
    for row in payload.get("prerequisite_checks") or []:
        if not isinstance(row, dict):
            continue
        for key in ("authority_path", "canonical_path"):
            value = str(row.get(key) or "").strip()
            if value:
                paths.append(value)
        for ref in row.get("source_refs") or []:
            if isinstance(ref, dict) and str(ref.get("artifact_path") or "").strip():
                paths.append(str(ref["artifact_path"]).strip())
    for row in payload.get("producer_results") or []:
        if not isinstance(row, dict):
            continue
        for ref in row.get("artifact_refs") or []:
            if isinstance(ref, dict) and str(ref.get("artifact_path") or "").strip():
                paths.append(str(ref["artifact_path"]).strip())
    market_calendar = payload.get("market_calendar_status") if isinstance(payload.get("market_calendar_status"), dict) else {}
    if str(market_calendar.get("artifact_path") or "").strip():
        paths.append(str(market_calendar["artifact_path"]).strip())
    if str(payload.get("readiness_authority_path") or "").strip():
        paths.append(str(payload["readiness_authority_path"]).strip())
    return list(dict.fromkeys(paths))


def _resolve_truth_root(raw: str) -> Path:
    value = str(raw or "").strip()
    if value:
        path = Path(value).expanduser().resolve()
        if not path.is_absolute():
            raise SystemExit(f"FAIL: --truth_root must be absolute: {path}")
        if not path.exists() or not path.is_dir():
            raise SystemExit(f"FAIL: --truth_root must exist and be a directory: {path}")
        return path
    return resolve_truth_root(repo_root=REPO_ROOT).resolve()


def _resolve_ib_account(raw: str) -> str:
    ib_account = str(raw or "").strip()
    if ib_account:
        return ib_account
    return resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)


def _resolve_primary_sleeve_truth_root(*, environment: str, ib_account: str) -> Path:
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=REPO_ROOT,
        environment=environment,
        requested_ib_account=ib_account,
    )
    for binding in bindings:
        if str(binding.sleeve_id).strip().upper() == "PRIMARY":
            return Path(binding.truth_root).resolve()
    return Path(bindings[0].truth_root).resolve()


def _resolve_primary_execution_truth_root(*, environment: str, ib_account: str) -> Path:
    roots = resolve_governed_paper_execution_roots(
        repo_root=REPO_ROOT,
        environment=environment,
        ib_account=ib_account,
        sleeve_id=PRIMARY_SLEEVE_ID,
    )
    return Path(roots.execution_root_path).resolve()


def _broker_event_log_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "execution_evidence_v1" / "broker_events" / day_utc / "broker_event_log.v1.jsonl").resolve()


def _run_tool(cmd: List[str], *, script: str, env: Dict[str, str] | None = None) -> Dict[str, Any]:
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        env=env,
    )
    return {
        "script": script,
        "command": list(cmd),
        "return_code": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "required_for_completion": True,
    }


def _resolve_active_session_days(*, truth_root: Path) -> tuple[str, str]:
    path = (truth_root / "active_session_v1" / "current.json").resolve()
    if not path.exists() or not path.is_file():
        return "", ""
    try:
        payload = read_json_object_v1(path)
    except Exception:
        return "", ""
    active_day = str(payload.get("active_day") or "").strip()
    target_day = str(payload.get("target_day") or "").strip()
    return active_day, target_day


def _is_target_day_live_or_past(*, target_day: str) -> bool:
    normalized = str(target_day or "").strip()
    if not normalized:
        return False
    try:
        target_date = date.fromisoformat(normalized)
    except ValueError:
        return False
    today_utc = resolve_session_authority_target_day_v1("")
    try:
        today_date = date.fromisoformat(str(today_utc).strip())
    except ValueError:
        return False
    return today_date >= target_date


def _stale_authority_head_detected(*, truth_root: Path, target_day: str) -> bool:
    active_day, _ = _resolve_active_session_days(truth_root=truth_root)
    return bool(active_day and active_day != target_day and _is_target_day_live_or_past(target_day=target_day))


def _primary_scoped_head_matches_target_day_from_sleeve_truth_root(
    *,
    primary_sleeve_truth_root: Path,
    target_day: str,
) -> bool:
    head_path = (Path(primary_sleeve_truth_root).resolve() / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()
    if not head_path.exists() or not head_path.is_file():
        return False
    try:
        payload = read_json_object_v1(head_path)
    except Exception:
        return False
    observed_day = str(payload.get("day_utc") or "").strip()
    observed_status = str(payload.get("status") or "").strip().upper()
    return observed_day == target_day and observed_status == "PASS"


def _run_session_authority_rollover_attempt(
    *,
    truth_root: Path,
    target_day: str,
    environment: str,
    ib_account: str,
) -> List[Dict[str, Any]]:
    phase_results: List[Dict[str, Any]] = []
    phase_env = {
        **os.environ,
        "C2_SKIP_SESSION_AUTHORITY_REENTRY": "YES",
        "C2_SKIP_STARTUP_MATERIALIZATION_REENTRY": "YES",
    }
    for phase in ("build", "admit", "activate"):
        result = _run_tool(
            [
                sys.executable,
                str(RUN_SESSION_AUTHORITY_TOOL),
                "--target_day",
                target_day,
                "--truth_root",
                str(truth_root),
                "--environment",
                environment,
                "--ib_account",
                ib_account,
                "--phase",
                phase,
            ],
            script="ops/tools/run_session_authority_v1.py",
            env=phase_env,
        )
        phase_results.append(result)
    return phase_results


def _primary_scoped_head_matches_target_day(*, payload: Dict[str, Any], target_day: str) -> bool:
    for check in payload.get("prerequisite_checks") or []:
        if str(check.get("artifact_id") or "").strip() != "primary_scoped_canonical_authority_head_v1":
            continue
        observed = str(check.get("target_day_observed") or "").strip()
        status = str(check.get("result_status") or "").strip().upper()
        return status == "PASS" and observed == target_day
    return False


def _enforce_rollover_failed_stale_head_invariant(*, payload: Dict[str, Any]) -> None:
    payload["completion_state"] = "INCOMPLETE"
    payload["materialization_state"] = "BLOCKED"
    codes = {str(code).strip() for code in (payload.get("blocking_reason_codes") or []) if str(code).strip()}
    codes.add(REASON_ROLLOVER_FAILED_STALE_AUTHORITY_HEAD)
    payload["blocking_reason_codes"] = sorted(codes)
    for check in payload.get("prerequisite_checks") or []:
        if str(check.get("artifact_id") or "").strip() != "primary_scoped_canonical_authority_head_v1":
            continue
        check["result_status"] = "FAIL"
        check["blocking_reason_code"] = REASON_ROLLOVER_FAILED_STALE_AUTHORITY_HEAD
        blocker_codes = {str(code).strip() for code in (check.get("blocker_codes") or []) if str(code).strip()}
        blocker_codes.add(REASON_ROLLOVER_FAILED_STALE_AUTHORITY_HEAD)
        check["blocker_codes"] = sorted(blocker_codes)
        check["closure_status"] = "OPEN"


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_pre_open_materializer_v1")
    parser.add_argument("--day_utc", default="")
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--ib_account", default="")
    args = parser.parse_args(argv)

    truth_root = _resolve_truth_root(args.truth_root)
    target_day = resolve_session_authority_target_day_v1(str(args.day_utc or "").strip())
    environment = str(args.environment or "PAPER").strip().upper()
    ib_account = _resolve_ib_account(args.ib_account)
    primary_sleeve_truth_root = _resolve_primary_sleeve_truth_root(environment=environment, ib_account=ib_account)
    execution_truth_root = _resolve_primary_execution_truth_root(
        environment=environment,
        ib_account=ib_account,
    )
    readiness_authority_path, readiness_payload = read_or_evaluate_trading_day_readiness_authority_v1(
        target_day=target_day,
        truth_root=truth_root,
        execution_root=execution_truth_root,
        environment=environment,
    )
    readiness_mode = str(readiness_payload.get("readiness_mode") or "").strip().upper()
    requires_same_day_broker_event_log = readiness_payload.get("requires_same_day_broker_event_log") is True
    requires_live_account_truth = readiness_payload.get("requires_live_account_truth") is True
    live_broker_evidence_required = readiness_mode not in {"PREOPEN_BUILD", "PREOPEN_ADMISSION"} or requires_same_day_broker_event_log or requires_live_account_truth

    producer_results: List[Dict[str, Any]] = []
    stale_rollover_condition = _stale_authority_head_detected(truth_root=truth_root, target_day=target_day)
    pre_rollover_head_aligned = _primary_scoped_head_matches_target_day_from_sleeve_truth_root(
        primary_sleeve_truth_root=primary_sleeve_truth_root,
        target_day=target_day,
    )
    should_attempt_rollover = stale_rollover_condition and not pre_rollover_head_aligned
    rollover_attempt_had_failures = False
    if should_attempt_rollover:
        rollover_results = _run_session_authority_rollover_attempt(
            truth_root=truth_root,
            target_day=target_day,
            environment=environment,
            ib_account=ib_account,
        )
        producer_results.extend(rollover_results)
        rollover_attempt_had_failures = any(int(item.get("return_code") or 0) != 0 for item in rollover_results)

    broker_events_path = _broker_event_log_path(
        truth_root=primary_sleeve_truth_root,
        day_utc=target_day,
    )
    if not rollover_attempt_had_failures and environment == "PAPER" and live_broker_evidence_required and not broker_events_path.exists():
        execution_profile = resolve_governed_paper_execution_profile(
            repo_root=REPO_ROOT,
            environment=environment,
            ib_account=ib_account,
            sleeve_id=PRIMARY_SLEEVE_ID,
        )
        producer_results.append(
            _run_tool(
                [
                    sys.executable,
                    str(RUN_BROKER_EVENTS_BOOTSTRAP_TOOL),
                    "--truth_root",
                    str(execution_truth_root),
                    "--host",
                    str(execution_profile.host),
                    "--port",
                    str(execution_profile.port),
                    "--client-id",
                    str(execution_profile.client_id_observer),
                    "--poll-seconds",
                    "10",
                    "--environment",
                    environment,
                    "--sleeve-id",
                    PRIMARY_SLEEVE_ID,
                    "--day-utc",
                    target_day,
                    "--bootstrap-handshake-only",
                    "--handshake-timeout-seconds",
                    "15",
                    "--disable-legacy-log",
                ],
                script="ops/ib/c2_execution_observer_v1.py",
            )
        )
        producer_results.append(
            _run_tool(
                [
                    sys.executable,
                    str(RUN_BROKER_EVENTS_MANIFEST_TOOL),
                    "--day_utc",
                    target_day,
                    "--truth_root",
                    str(execution_truth_root),
                ],
                script="ops/ib/run_broker_event_day_manifest_v1.py",
            )
        )

    if not rollover_attempt_had_failures:
        followup_results: List[Dict[str, Any]] = []
        if live_broker_evidence_required:
            followup_results.append(
                _run_tool(
                    [
                        sys.executable,
                        str(RUN_IB_API_HANDSHAKE_TOOL),
                        "--day_utc",
                        target_day,
                        "--truth_root",
                        str(primary_sleeve_truth_root),
                        "--environment",
                        environment,
                        "--ib_account",
                        ib_account,
                    ],
                    script="ops/tools/run_ib_api_handshake_spine_v1.py",
                )
            )
        followup_results.extend([
            _run_tool(
                [
                    sys.executable,
                    str(RUN_POINTER_HEADS_MATERIALIZE_TOOL),
                    "--fail_if_no_authority_head",
                    "NO",
                    "--expected_day_utc",
                    target_day,
                    "--truth_root",
                    str(primary_sleeve_truth_root),
                ],
                script="ops/tools/run_pointer_heads_materialize_v1.py",
            ),
            _run_tool(
                [
                    sys.executable,
                    str(RUN_GLOBAL_KILL_SWITCH_TOOL),
                    "--day_utc",
                    target_day,
                ],
                script="ops/tools/run_global_kill_switch_v1.py",
                env={
                    **os.environ,
                    "C2_TRUTH_ROOT": str(truth_root.parent),
                },
            ),
        ])
        producer_results.extend(followup_results)

    payload = derive_pre_open_bundle_payload_v1(
        truth_root=truth_root,
        target_day=target_day,
        environment=environment,
        ib_account=ib_account,
        producer_results=producer_results,
        owner_tool=OWNER_TOOL,
        repo_root=REPO_ROOT,
        readiness_payload=readiness_payload,
        readiness_authority_path=str(readiness_authority_path),
    )
    if stale_rollover_condition:
        authority_head_aligned = _primary_scoped_head_matches_target_day(payload=payload, target_day=target_day)
        if not authority_head_aligned:
            _enforce_rollover_failed_stale_head_invariant(payload=payload)
    attach_producer_contract_v1(
        payload,
        producer_name=OWNER_TOOL,
        producer_command=(
            f"python3 {OWNER_TOOL} --day_utc {target_day} --truth_root {truth_root} "
            f"--environment {environment} --ib_account {ib_account}"
        ),
        input_artifacts=_producer_contract_inputs(payload),
        output_artifacts=[resolve_pre_open_bundle_path_v1(truth_root=truth_root, day_utc=target_day)],
        schema_versions={"pre_open_bundle": "v1"},
    )
    ref = write_pre_open_bundle_v1(truth_root=truth_root, payload=payload)
    out = {
        "target_day": target_day,
        "path": str(ref.path),
        "sha256": ref.sha256,
        "materialization_state": str(ref.payload.get("materialization_state") or ""),
        "blocking_reason_codes": list(ref.payload.get("blocking_reason_codes") or []),
    }
    print(json.dumps(out, sort_keys=True))
    return 0 if out["materialization_state"] == "COMPLETE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
