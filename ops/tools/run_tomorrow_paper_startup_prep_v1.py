#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_validated_json_v1,
    now_utc_iso_v1,
    producer_block_v1,
    repo_git_sha_v1,
)
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1
from constellation_2.common.session_authority_v1 import resolve_session_authority_tomorrow_target_day_v1
from constellation_2.common.tomorrow_paper_startup_prep_v1 import (
    BLOCKER_FIXABILITY_EXTERNAL,
    BLOCKER_FIXABILITY_REFRESHABLE,
    BLOCKER_FIXABILITY_UNKNOWN,
    DAY_READINESS_BLOCKED_EXTERNAL,
    DAY_READINESS_CLASSIFICATION_REFRESHABLE,
    DAY_READINESS_READY_FOR_DAY,
    DAY_READINESS_WAITING_FOR_MARKET_DATA,
    classify_bootstrap_blocker_fixability_v1,
    classify_day_readiness_projection_v1,
    derive_tomorrow_readiness_state_v1,
    resolve_continuity_paper_seed_usd_v1,
    resolve_day_readiness_automation_path_v1,
)
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_governed_sleeve_truth_bindings

PAPER_ENVIRONMENT = "PAPER"
PRIMARY_SLEEVE_ID = "PRIMARY"
CANONICAL_SEQUENCE_OWNER = "ops/tools/run_c2_paper_day_orchestrator_v2.py"
MAX_ITERATIONS = 12

BOOTSTRAP_TOOL = (REPO_ROOT / "ops/tools/run_paper_session_bootstrap_v1.py").resolve()
SEED_TOOL = (REPO_ROOT / "ops/tools/ensure_paper_capital_seed_v1.py").resolve()
PRE_OPEN_TOOL = (REPO_ROOT / "ops/tools/run_pre_open_materializer_v1.py").resolve()
PIPELINE_MANIFEST_TOOL = (REPO_ROOT / "ops/tools/run_pipeline_manifest_v1.py").resolve()
HANDSHAKE_TOOL = (REPO_ROOT / "ops/tools/run_ib_api_handshake_spine_v1.py").resolve()
POINTER_HEADS_TOOL = (REPO_ROOT / "ops/tools/run_pointer_heads_materialize_v1.py").resolve()
POINTER_ATTEMPT_ALLOC_TOOL = (REPO_ROOT / "ops/tools/run_pointer_attempt_alloc_v1.py").resolve()
POINTER_APPEND_TOOL = (REPO_ROOT / "ops/tools/run_pointer_append_v1.py").resolve()
KILL_SWITCH_TOOL = (REPO_ROOT / "ops/tools/run_global_kill_switch_v1.py").resolve()
BROKER_RECON_TOOL = (REPO_ROOT / "ops/tools/run_broker_reconciliation_day_v2.py").resolve()
EXPOSURE_NET_TOOL = (REPO_ROOT / "ops/tools/run_exposure_net_day_v1.py").resolve()
BROKER_FACT_SPINE_TOOL = (REPO_ROOT / "ops/tools/run_broker_fact_spine_v1.py").resolve()
RECONCILED_TRADE_STATE_TOOL = (REPO_ROOT / "ops/tools/run_reconciled_trade_state_v1.py").resolve()
SLEEVE_EDGE_TOOL = (REPO_ROOT / "ops/tools/run_sleeve_edge_measurement_v1.py").resolve()
CAPITAL_AUTHORITY_TOOL = (REPO_ROOT / "ops/tools/run_capital_authority_allocation_day_v1.py").resolve()
STARTUP_AUTHZ_TOOL = (REPO_ROOT / "ops/tools/run_paper_startup_authorization_convergence_v1.py").resolve()
SESSION_AUTHORITY_TOOL = (REPO_ROOT / "ops/tools/run_session_authority_v1.py").resolve()
STARTUP_MATERIALIZATION_TOOL = (REPO_ROOT / "ops/tools/run_startup_materialization_v1.py").resolve()
STARTUP_MATERIALIZATION_INPUTS_PREP_TOOL = (REPO_ROOT / "ops/tools/run_startup_materialization_inputs_prep_v1.py").resolve()
BROKER_EVENTS_BOOTSTRAP_TOOL = (REPO_ROOT / "ops/ib/c2_execution_observer_v1.py").resolve()
BROKER_EVENTS_MANIFEST_TOOL = (REPO_ROOT / "ops/ib/run_broker_event_day_manifest_v1.py").resolve()
PAPER_DAY_ORCHESTRATOR_SERVICE_PATH = (REPO_ROOT / "ops/systemd/user/c2-paper-day-orchestrator.service").resolve()
GATE_HIERARCHY_POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/GATE_HIERARCHY_V1.json").resolve()
DEFAULT_OPERATOR_INPUT_ROOT = (REPO_ROOT / "constellation_2").resolve()
DAY_READINESS_AUTOMATION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/day_readiness_automation.v1.schema.json"

_BROKER_EVENT_LOG_PATH_RE = re.compile(r"(/[^\s'\"]*broker_event_log\.v1\.jsonl)")


def _run(cmd: list[str], *, extra_env: Dict[str, str] | None = None) -> dict[str, Any]:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False, env=env)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _parse_json_stdout(result: Dict[str, Any]) -> dict[str, Any]:
    stdout = str(result.get("stdout") or "").strip()
    if not stdout:
        return {}
    for line in reversed(stdout.splitlines()):
        text = str(line).strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return {}


def _result_text(result: Dict[str, Any]) -> str:
    return "\n".join(part for part in [str(result.get("stderr") or "").strip(), str(result.get("stdout") or "").strip()] if part)


def _write_day_readiness_automation_report(*, target_day: str, truth_root: Path, summary: Dict[str, Any]) -> dict[str, Any]:
    payload = {
        "schema_id": "day_readiness_automation",
        "schema_version": "v1",
        "target_day": target_day,
        "automation_run_at": str(summary.get("automation_run_at") or now_utc_iso_v1()),
        "readiness_state": str(summary.get("readiness_state") or "").strip(),
        "bootstrap_status": str(summary.get("bootstrap_status") or "").strip(),
        "earliest_failing_prerequisite": str(summary.get("earliest_failing_prerequisite") or "").strip(),
        "owner_tool": str(summary.get("owner_tool") or "").strip(),
        "artifact_path": str(summary.get("artifact_path") or "").strip(),
        "blocker_class": str(summary.get("blocker_class") or "").strip(),
        "reason_codes": [str(code).strip() for code in (summary.get("reason_codes") or []) if str(code).strip()],
        "classification": str(summary.get("classification") or DAY_READINESS_CLASSIFICATION_REFRESHABLE).strip(),
        "canonical_next_step": str(summary.get("canonical_next_step") or "").strip(),
        "bootstrap_report_path": str(summary.get("bootstrap_report_path") or "").strip(),
        "target_day_build_path": str(summary.get("target_day_build_path") or "").strip(),
        "target_day_build_status": str(summary.get("target_day_build_status") or "").strip(),
        "target_day_admission_path": str(summary.get("target_day_admission_path") or "").strip(),
        "target_day_admission_status": str(summary.get("target_day_admission_status") or "").strip(),
        "submit_boundary_status_path": str(summary.get("submit_boundary_status_path") or "").strip(),
        "submit_boundary_status": str(summary.get("submit_boundary_status") or "").strip(),
        "submit_boundary_submission_authorized": bool(summary.get("submit_boundary_submission_authorized") is True),
        "automation_owner_tool": "ops/tools/run_tomorrow_paper_startup_prep_v1.py",
        "automation_scope": "SUBORDINATE_OPERATOR_PROJECTION",
        "canonical_truth_note": "Bootstrap, pre_open_bundle_v1, and session_promotion_decision_v1 remain canonical startup truth; this report is an automation projection only.",
        "producer": producer_block_v1(module="ops/tools/run_tomorrow_paper_startup_prep_v1.py", git_sha=repo_git_sha_v1()),
    }
    ref = atomic_write_validated_json_v1(
        path=resolve_day_readiness_automation_path_v1(truth_root=truth_root, target_day=target_day),
        payload=payload,
        schema_relpath=DAY_READINESS_AUTOMATION_SCHEMA_RELPATH,
    )
    return {"path": str(ref.path), "sha256": ref.sha256}

def _sha256_file(path: Path) -> str:
    proc = subprocess.run(["sha256sum", str(path)], cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise SystemExit(f"FAIL: sha256sum_failed path={path} stderr={proc.stderr.strip()!r}")
    return str(proc.stdout).strip().split()[0]


def _git_sha() -> str:
    proc = subprocess.run(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise SystemExit(f"FAIL: git_rev_parse_failed stderr={proc.stderr.strip()!r}")
    return str(proc.stdout).strip()


def _loggable_step(action: str, result: Dict[str, Any], **extra: Any) -> dict[str, Any]:
    step = {
        "action": action,
        "action_returncode": int(result.get("returncode") or 0),
        "action_stdout": str(result.get("stdout") or ""),
        "action_stderr": str(result.get("stderr") or ""),
    }
    for key, value in extra.items():
        step[key] = value
    return step


def _action_to_nested_step(action: dict[str, Any]) -> dict[str, Any]:
    result = dict(action.get("result") or {})
    step = _loggable_step(
        str(action.get("action") or "").strip(),
        result,
        owner_tool=str(action.get("owner_tool") or "").strip(),
        nested_steps=action.get("nested_steps") or [],
    )
    continuity_seed_usd = str(action.get("continuity_seed_usd") or "").strip()
    if continuity_seed_usd:
        step["continuity_seed_usd"] = continuity_seed_usd
    external_blocker = action.get("external_blocker")
    if isinstance(external_blocker, dict) and external_blocker:
        step["external_blocker"] = dict(external_blocker)
    return step


def _extract_broker_event_log_path(text: str) -> str:
    match = _BROKER_EVENT_LOG_PATH_RE.search(str(text or ""))
    return str(match.group(1)).strip() if match else ""


def _bootstrap_summary(*, day_utc: str, truth_root: Path, operator_input_root: Path, ib_account: str) -> dict[str, Any]:
    result = _run(
        [
            sys.executable,
            str(BOOTSTRAP_TOOL),
            "--day_utc",
            day_utc,
            "--truth_root",
            str(truth_root),
            "--operator_input_root",
            str(operator_input_root),
            "--environment",
            PAPER_ENVIRONMENT,
            "--ib_account",
            ib_account,
            "--materialize",
            "YES",
            "--emit_report",
            "YES",
        ]
    )
    payload = _parse_json_stdout(result)
    if not payload:
        raise SystemExit(
            "FAIL: bootstrap_did_not_emit_json "
            f"returncode={result['returncode']} stdout={result['stdout']!r} stderr={result['stderr']!r}"
        )
    payload["_run_result"] = result
    return payload


def _reason_codes(summary: dict[str, Any]) -> list[str]:
    return [
        str(code).strip()
        for code in (summary.get("earliest_failing_prerequisite_reason_codes") or [])
        if str(code).strip()
    ]


def _owner_tool(summary: dict[str, Any]) -> str:
    return str(summary.get("earliest_failing_prerequisite_owner_tool") or "").strip()


def _load_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _startup_materialization_path(*, day_utc: str, truth_root: Path) -> Path:
    return (
        truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json"
    ).resolve()


def _startup_materialization_inputs_prep_path(*, day_utc: str, truth_root: Path) -> Path:
    return (
        truth_root
        / "reports"
        / "startup_materialization_inputs_prep_v1"
        / day_utc
        / "startup_materialization_inputs_prep.v1.json"
    ).resolve()


def _resolve_post_bootstrap_blocker(*, day_utc: str, truth_root: Path) -> dict[str, Any] | None:
    startup_path = _startup_materialization_path(day_utc=day_utc, truth_root=truth_root)
    startup_payload = _load_optional_json(startup_path)
    startup_status = str(startup_payload.get("status") or "").strip().upper()
    startup_blocking_codes = [
        str(code).strip()
        for code in (startup_payload.get("blocking_codes") or [])
        if str(code).strip()
    ]
    if startup_status and startup_status != "PASS":
        inputs_prep_path = _startup_materialization_inputs_prep_path(day_utc=day_utc, truth_root=truth_root)
        inputs_prep_payload = _load_optional_json(inputs_prep_path)
        inputs_prep_codes = [
            str(code).strip()
            for code in (inputs_prep_payload.get("blocking_codes") or [])
            if str(code).strip()
        ]
        merged_codes = list(dict.fromkeys(startup_blocking_codes + inputs_prep_codes))
        if "STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DEFAULT_EQUITY_REFERENCE_PRICE_UNAVAILABLE" in merged_codes:
            return {
                "blocker": "startup_materialization_inputs_prep_v1",
                "owner_tool": str(STARTUP_MATERIALIZATION_INPUTS_PREP_TOOL),
                "artifact_path": str(inputs_prep_path),
                "blocker_class": "MISSING_DEPENDENCY",
                "reason_codes": merged_codes,
                "fixability": BLOCKER_FIXABILITY_EXTERNAL,
                "canonical_next_action": (
                    f"Before 09:30 America/New_York, wait. At or after 09:30 America/New_York, rerun once a real positive same-day governed core-session price has landed for {day_utc}; if it is still absent, remain fail-closed and rerun {Path(__file__).resolve()} after the governed market-data path refreshes."
                ),
            }
        return {
            "blocker": "startup_materialization_v1",
            "owner_tool": str(STARTUP_MATERIALIZATION_TOOL),
            "artifact_path": str(startup_path),
            "blocker_class": "FAILED_VALIDATION",
            "reason_codes": merged_codes or [f"STARTUP_MATERIALIZATION_STATUS:{startup_status}"],
            "fixability": BLOCKER_FIXABILITY_UNKNOWN,
            "canonical_next_action": (
                f"Inspect {startup_path} and resolve the startup materialization failure through {STARTUP_MATERIALIZATION_TOOL}, then rerun {Path(__file__).resolve()} for {day_utc}."
            ),
        }
    return None


def _resolve_ib_account(raw: str) -> str:
    requested = str(raw or "").strip()
    if requested:
        return requested
    return resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)


def _resolve_sleeve_truth_root(*, ib_account: str) -> Path:
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=REPO_ROOT,
        environment=PAPER_ENVIRONMENT,
        requested_ib_account=ib_account,
        sleeve_id=PRIMARY_SLEEVE_ID,
    )
    if not bindings:
        raise SystemExit(f"FAIL: no governed sleeve truth binding for account={ib_account}")
    return bindings[0].truth_root.resolve()


def _authorization_verdict_path(*, day_utc: str, ib_account: str) -> Path:
    sleeve_truth_root = _resolve_sleeve_truth_root(ib_account=ib_account)
    return (
        sleeve_truth_root / "reports" / "authorization_gate_verdict_v1" / day_utc / "authorization_gate_verdict.v1.json"
    ).resolve()


def _replay_bundle_path(*, day_utc: str, ib_account: str) -> Path:
    sleeve_truth_root = _resolve_sleeve_truth_root(ib_account=ib_account)
    return (
        sleeve_truth_root / "reports" / "replay_certification_bundle_v1" / day_utc / "replay_certification_bundle.v1.json"
    ).resolve()


def _kill_switch_fixability(*, day_utc: str, ib_account: str) -> str:
    verdict = _load_optional_json(_authorization_verdict_path(day_utc=day_utc, ib_account=ib_account))
    verdict_status = str(verdict.get("status") or "").strip().upper()
    verdict_reasons = {str(code).strip() for code in (verdict.get("reason_codes") or []) if str(code).strip()}
    if verdict_status == "PASS":
        return BLOCKER_FIXABILITY_UNKNOWN
    if any(code.startswith("AUTHORIZATION_GATE_NOT_PASS:replay_certification_gate_v1:FAIL") for code in verdict_reasons):
        bundle = _load_optional_json(_replay_bundle_path(day_utc=day_utc, ib_account=ib_account))
        missing_types = {
            str(item).strip()
            for item in ((bundle.get("inputs") or {}).get("missing_types") or [])
            if str(item).strip()
        }
        if missing_types and missing_types.issubset({"input_manifest", "reconciliation"}):
            return BLOCKER_FIXABILITY_REFRESHABLE
        if missing_types:
            return BLOCKER_FIXABILITY_EXTERNAL
    return BLOCKER_FIXABILITY_UNKNOWN


def _effective_fixability(*, summary: dict[str, Any], day_utc: str, ib_account: str) -> str:
    owner_tool = _owner_tool(summary)
    base = classify_bootstrap_blocker_fixability_v1(
        owner_tool=owner_tool,
        blocker_class=str(summary.get("earliest_failing_prerequisite_blocker_class") or "").strip(),
        reason_codes=_reason_codes(summary),
    )
    if owner_tool == str(KILL_SWITCH_TOOL):
        hint = _kill_switch_fixability(day_utc=day_utc, ib_account=ib_account)
        if hint != BLOCKER_FIXABILITY_UNKNOWN:
            return hint
    return base


def _capital_authority_failure_hint(text: str) -> str:
    message = str(text or "")
    if "AUTHORITY_HEAD_DAY_MISMATCH" in message:
        return "AUTHORITY_HEAD_DAY_MISMATCH"
    if "EXPOSURE_NET_MISSING" in message:
        return "EXPOSURE_NET_MISSING"
    if "SLEEVE_EDGE_SNAPSHOT_DEPENDENCY" in message:
        return "SLEEVE_EDGE_SNAPSHOT_DEPENDENCY"
    return ""


def _sleeve_edge_failure_hint(text: str) -> str:
    return "SLEEVE_EDGE_CORE2_SUMMARY_MISSING" if "SLEEVE_EDGE_CORE2_SUMMARY_MISSING" in str(text or "") else ""


def _reconciled_trade_state_failure_hint(text: str) -> str:
    return "CORE1_HEALTH_MISSING" if "CORE1_HEALTH_MISSING" in str(text or "") else ""


def _broker_events_external_blocker(*, day_utc: str, truth_root: Path, result: Dict[str, Any]) -> dict[str, Any] | None:
    text = _result_text(result)
    if "broker_event_log.v1.jsonl" not in text and "CORE1_RAW_JOURNAL_MISSING" not in text and "BROKER_EVENTS_MISSING" not in text:
        return None
    artifact_path = _extract_broker_event_log_path(text) or str(
        (truth_root / "execution_evidence_v1" / "broker_events" / day_utc / "broker_event_log.v1.jsonl").resolve()
    )
    return {
        "blocker": "broker_event_log_v1_jsonl",
        "owner_tool": str(BROKER_EVENTS_BOOTSTRAP_TOOL),
        "surfaced_by_owner_tool": str(BROKER_FACT_SPINE_TOOL),
        "artifact_path": artifact_path,
        "blocker_class": "UPSTREAM_DEPENDENCY_FAILURE",
        "reason_codes": ["BROKER_EVENTS_MISSING"],
        "fixability": BLOCKER_FIXABILITY_EXTERNAL,
        "canonical_next_action": (
            f"Materialize {artifact_path} through the canonical broker observer runtime ({BROKER_EVENTS_BOOTSTRAP_TOOL}) "
            f"and day manifest owner ({BROKER_EVENTS_MANIFEST_TOOL}), then rerun canonical bootstrap for target_day={day_utc}."
        ),
    }


def _refresh_pointer_truth_root_for_authorization(
    *,
    day_utc: str,
    authorization_path: Path,
    pointer_truth_root: Path,
    git_sha: str,
    scope_label: str,
) -> dict[str, Any]:
    cfg_hash = _sha256_file(PAPER_DAY_ORCHESTRATOR_SERVICE_PATH)
    policy_hash = _sha256_file(GATE_HIERARCHY_POLICY_PATH)
    nested_steps: list[dict[str, Any]] = []

    attempt_result = _run(
        [
            sys.executable,
            str(POINTER_ATTEMPT_ALLOC_TOOL),
            "--day_utc",
            day_utc,
            "--mode",
            PAPER_ENVIRONMENT,
            "--orchestrator_config_hash",
            cfg_hash,
            "--git_sha",
            git_sha,
            "--truth_root",
            str(pointer_truth_root),
        ]
    )
    nested_steps.append(_loggable_step(f"run_pointer_attempt_alloc_v1[{scope_label}]", attempt_result, truth_root=str(pointer_truth_root)))
    if int(attempt_result.get("returncode") or 0) != 0:
        return {
            "status": "ERROR",
            "reason_codes": [f"PRIMARY_{scope_label}_POINTER_ATTEMPT_ALLOC_FAILED"],
            "nested_steps": nested_steps,
        }
    attempt_payload = _parse_json_stdout(attempt_result)
    attempt_id = str(attempt_payload.get("attempt_id") or "").strip()
    attempt_seq = int(attempt_payload.get("attempt_seq") or 0)
    if not attempt_id or attempt_seq <= 0:
        return {
            "status": "ERROR",
            "reason_codes": [f"PRIMARY_{scope_label}_POINTER_ATTEMPT_ALLOC_INVALID"],
            "nested_steps": nested_steps,
        }

    append_result = _run(
        [
            sys.executable,
            str(POINTER_APPEND_TOOL),
            "--day_utc",
            day_utc,
            "--attempt_id",
            attempt_id,
            "--attempt_seq",
            str(attempt_seq),
            "--mode",
            PAPER_ENVIRONMENT,
            "--status",
            "PASS",
            "--authoritative",
            "YES",
            "--policy_hash",
            policy_hash,
            "--orchestrator_config_hash",
            cfg_hash,
            "--produced_utc",
            f"{day_utc}T00:00:00Z",
            "--points_to",
            str(authorization_path),
            "--git_sha",
            git_sha,
            "--truth_root",
            str(pointer_truth_root),
        ]
    )
    nested_steps.append(_loggable_step(f"run_pointer_append_v1[{scope_label}]", append_result, truth_root=str(pointer_truth_root), authorization_path=str(authorization_path)))
    if int(append_result.get("returncode") or 0) != 0:
        return {
            "status": "ERROR",
            "reason_codes": [f"PRIMARY_{scope_label}_POINTER_APPEND_FAILED"],
            "nested_steps": nested_steps,
        }

    heads_result = _run(
        [
            sys.executable,
            str(POINTER_HEADS_TOOL),
            "--fail_if_no_authority_head",
            "YES",
            "--expected_day_utc",
            day_utc,
            "--truth_root",
            str(pointer_truth_root),
        ]
    )
    nested_steps.append(_loggable_step(f"run_pointer_heads_materialize_v1[{scope_label}]", heads_result, truth_root=str(pointer_truth_root)))
    heads_payload = _parse_json_stdout(heads_result)
    if int(heads_result.get("returncode") or 0) != 0 or str(heads_payload.get("result_state") or "").strip().upper() != "PASS":
        return {
            "status": "ERROR",
            "reason_codes": [f"PRIMARY_{scope_label}_POINTER_HEADS_MATERIALIZE_FAILED"],
            "nested_steps": nested_steps,
        }
    return {"status": "OK", "reason_codes": [], "nested_steps": nested_steps}


def _refresh_primary_authority_pointers(*, day_utc: str, truth_root: Path, ib_account: str, git_sha: str) -> dict[str, Any]:
    authorization_path = _authorization_verdict_path(day_utc=day_utc, ib_account=ib_account)
    nested_steps: list[dict[str, Any]] = []
    if not authorization_path.exists() or not authorization_path.is_file():
        return {
            "status": "ERROR",
            "reason_codes": ["PRIMARY_AUTHORIZATION_VERDICT_MISSING"],
            "nested_steps": nested_steps,
        }
    authorization_payload = _load_optional_json(authorization_path)
    authorization_status = str(authorization_payload.get("status") or "").strip().upper()
    if authorization_status not in {"PASS", "BOOTSTRAP_PASS"}:
        return {
            "status": "ERROR",
            "reason_codes": [f"PRIMARY_AUTHORIZATION_VERDICT_NOT_READY:{authorization_status or 'UNKNOWN'}"],
            "nested_steps": nested_steps,
        }
    sleeve_truth_root = _resolve_sleeve_truth_root(ib_account=ib_account)
    canonical_refresh = _refresh_pointer_truth_root_for_authorization(
        day_utc=day_utc,
        authorization_path=authorization_path,
        pointer_truth_root=truth_root,
        git_sha=git_sha,
        scope_label="CANONICAL",
    )
    scoped_refresh = _refresh_pointer_truth_root_for_authorization(
        day_utc=day_utc,
        authorization_path=authorization_path,
        pointer_truth_root=sleeve_truth_root,
        git_sha=git_sha,
        scope_label="SCOPED",
    )
    nested_steps.extend(canonical_refresh.get("nested_steps") or [])
    nested_steps.extend(scoped_refresh.get("nested_steps") or [])
    reason_codes = [
        str(code).strip()
        for row in [canonical_refresh, scoped_refresh]
        for code in (row.get("reason_codes") or [])
        if str(code).strip()
    ]
    return {
        "status": "OK" if not reason_codes else "ERROR",
        "reason_codes": sorted(set(reason_codes)),
        "nested_steps": nested_steps,
    }


def _invoke_broker_fact_chain(*, day_utc: str, truth_root: Path, ib_account: str) -> dict[str, Any]:
    sleeve_truth_root = _resolve_sleeve_truth_root(ib_account=ib_account)
    result = _run(
        [
            sys.executable,
            str(BROKER_FACT_SPINE_TOOL),
            "--day_utc",
            day_utc,
            "--truth_root",
            str(sleeve_truth_root),
            "--environment",
            PAPER_ENVIRONMENT,
            "--sleeve_id",
            PRIMARY_SLEEVE_ID,
            "--json",
        ]
    )
    return {
        "action": "run_broker_fact_spine_v1",
        "owner_tool": str(BROKER_FACT_SPINE_TOOL),
        "result": result,
        "nested_steps": [],
        "external_blocker": _broker_events_external_blocker(day_utc=day_utc, truth_root=truth_root, result=result),
    }


def _invoke_reconciled_trade_state_chain(*, day_utc: str, truth_root: Path, ib_account: str) -> dict[str, Any]:
    nested_steps: list[dict[str, Any]] = []
    ran_broker_fact = False
    last_result: Dict[str, Any] | None = None
    external_blocker: dict[str, Any] | None = None
    while True:
        result = _run(
            [
                sys.executable,
                str(RECONCILED_TRADE_STATE_TOOL),
                "--day_utc",
                day_utc,
                "--environment",
                PAPER_ENVIRONMENT,
                "--sleeve_id",
                PRIMARY_SLEEVE_ID,
            ]
        )
        last_result = result
        if int(result.get("returncode") or 0) == 0:
            break
        text = _result_text(result)
        if _reconciled_trade_state_failure_hint(text) == "CORE1_HEALTH_MISSING" and not ran_broker_fact:
            ran_broker_fact = True
            broker_action = _invoke_broker_fact_chain(day_utc=day_utc, truth_root=truth_root, ib_account=ib_account)
            nested_steps.append(_action_to_nested_step(broker_action))
            external_blocker = broker_action.get("external_blocker") if isinstance(broker_action.get("external_blocker"), dict) else None
            if external_blocker is not None or int((broker_action.get("result") or {}).get("returncode") or 0) != 0:
                break
            continue
        break
    return {
        "action": "run_reconciled_trade_state_v1",
        "owner_tool": str(RECONCILED_TRADE_STATE_TOOL),
        "result": last_result or {"returncode": 1, "stdout": "", "stderr": "FAIL: reconciled_trade_state_not_run"},
        "nested_steps": nested_steps,
        "external_blocker": external_blocker,
    }


def _invoke_sleeve_edge_chain(*, day_utc: str, truth_root: Path, ib_account: str) -> dict[str, Any]:
    sleeve_truth_root = _resolve_sleeve_truth_root(ib_account=ib_account)
    nested_steps: list[dict[str, Any]] = []
    ran_reconciled_state = False
    last_result: Dict[str, Any] | None = None
    external_blocker: dict[str, Any] | None = None
    while True:
        result = _run(
            [
                sys.executable,
                str(SLEEVE_EDGE_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(sleeve_truth_root),
            ]
        )
        last_result = result
        if int(result.get("returncode") or 0) == 0:
            break
        text = _result_text(result)
        if _sleeve_edge_failure_hint(text) == "SLEEVE_EDGE_CORE2_SUMMARY_MISSING" and not ran_reconciled_state:
            ran_reconciled_state = True
            recon_action = _invoke_reconciled_trade_state_chain(day_utc=day_utc, truth_root=truth_root, ib_account=ib_account)
            nested_steps.append(_action_to_nested_step(recon_action))
            external_blocker = recon_action.get("external_blocker") if isinstance(recon_action.get("external_blocker"), dict) else None
            if external_blocker is not None or int((recon_action.get("result") or {}).get("returncode") or 0) != 0:
                break
            continue
        break
    return {
        "action": "run_sleeve_edge_measurement_v1",
        "owner_tool": str(SLEEVE_EDGE_TOOL),
        "result": last_result or {"returncode": 1, "stdout": "", "stderr": "FAIL: sleeve_edge_not_run"},
        "nested_steps": nested_steps,
        "external_blocker": external_blocker,
    }


def _invoke_capital_authority_chain(*, day_utc: str, truth_root: Path, ib_account: str) -> dict[str, Any]:
    sleeve_truth_root = _resolve_sleeve_truth_root(ib_account=ib_account)
    git_sha = _git_sha()
    nested_steps: list[dict[str, Any]] = []
    handled_hints: set[str] = set()
    last_result: Dict[str, Any] | None = None
    external_blocker: dict[str, Any] | None = None
    for _ in range(5):
        result = _run(
            [
                sys.executable,
                str(CAPITAL_AUTHORITY_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(sleeve_truth_root),
                "--canonical_sequence_owner",
                CANONICAL_SEQUENCE_OWNER,
            ]
        )
        last_result = result
        if int(result.get("returncode") or 0) == 0:
            break
        hint = _capital_authority_failure_hint(_result_text(result))
        if not hint or hint in handled_hints:
            break
        handled_hints.add(hint)
        if hint == "AUTHORITY_HEAD_DAY_MISMATCH":
            pointer_refresh = _refresh_primary_authority_pointers(
                day_utc=day_utc,
                truth_root=truth_root,
                ib_account=ib_account,
                git_sha=git_sha,
            )
            nested_steps.extend(pointer_refresh.get("nested_steps") or [])
            if str(pointer_refresh.get("status") or "").strip().upper() != "OK":
                break
            continue
        if hint == "EXPOSURE_NET_MISSING":
            exposure_result = _run([sys.executable, str(EXPOSURE_NET_TOOL), "--day_utc", day_utc])
            nested_steps.append(_loggable_step("run_exposure_net_day_v1", exposure_result, owner_tool=str(EXPOSURE_NET_TOOL)))
            if int(exposure_result.get("returncode") or 0) != 0:
                break
            continue
        if hint == "SLEEVE_EDGE_SNAPSHOT_DEPENDENCY":
            sleeve_edge_action = _invoke_sleeve_edge_chain(day_utc=day_utc, truth_root=truth_root, ib_account=ib_account)
            nested_steps.append(_action_to_nested_step(sleeve_edge_action))
            external_blocker = sleeve_edge_action.get("external_blocker") if isinstance(sleeve_edge_action.get("external_blocker"), dict) else None
            if external_blocker is not None or int((sleeve_edge_action.get("result") or {}).get("returncode") or 0) != 0:
                break
            continue
    return {
        "action": "run_capital_authority_allocation_day_v1",
        "owner_tool": str(CAPITAL_AUTHORITY_TOOL),
        "result": last_result or {"returncode": 1, "stdout": "", "stderr": "FAIL: capital_authority_not_run"},
        "nested_steps": nested_steps,
        "external_blocker": external_blocker,
    }


def _next_action_text(*, day_utc: str, owner_tool: str, fixability: str) -> str:
    if not owner_tool:
        return "Inspect the canonical bootstrap report and resolve the earliest failing prerequisite through its canonical owner."
    if fixability == BLOCKER_FIXABILITY_EXTERNAL:
        return (
            f"External/runtime blocker remains for target_day={day_utc}; inspect {owner_tool} state and rerun canonical bootstrap "
            "only after the governed runtime condition is cleared."
        )
    return f"Invoke {owner_tool} for target_day={day_utc}, then rerun canonical bootstrap."


def _invoke_owner(
    *,
    owner_tool: str,
    day_utc: str,
    truth_root: Path,
    operator_input_root: Path,
    ib_account: str,
) -> dict[str, Any] | None:
    sleeve_truth_root = _resolve_sleeve_truth_root(ib_account=ib_account)
    if owner_tool == str(SEED_TOOL):
        seed_usd = resolve_continuity_paper_seed_usd_v1(
            operator_input_root=operator_input_root,
            target_day=day_utc,
        )
        result = _run(
            [
                sys.executable,
                str(SEED_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(operator_input_root),
                "--ib_account",
                ib_account,
                "--seed_usd",
                seed_usd,
                "--allow_create",
                "YES",
            ]
        )
        return {
            "action": "ensure_paper_capital_seed_v1",
            "continuity_seed_usd": seed_usd,
            "owner_tool": owner_tool,
            "result": result,
            "nested_steps": [],
        }
    if owner_tool == str(PRE_OPEN_TOOL):
        result = _run(
            [
                sys.executable,
                str(PRE_OPEN_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--environment",
                PAPER_ENVIRONMENT,
                "--ib_account",
                ib_account,
            ]
        )
        return {"action": "run_pre_open_materializer_v1", "owner_tool": owner_tool, "result": result, "nested_steps": []}
    if owner_tool == str(PIPELINE_MANIFEST_TOOL):
        result = _run([sys.executable, str(PIPELINE_MANIFEST_TOOL), "--day_utc", day_utc])
        return {"action": "run_pipeline_manifest_v1", "owner_tool": owner_tool, "result": result, "nested_steps": []}
    if owner_tool == str(HANDSHAKE_TOOL):
        result = _run(
            [
                sys.executable,
                str(HANDSHAKE_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(sleeve_truth_root),
                "--environment",
                PAPER_ENVIRONMENT,
                "--ib_account",
                ib_account,
            ]
        )
        return {"action": "run_ib_api_handshake_spine_v1", "owner_tool": owner_tool, "result": result, "nested_steps": []}
    if owner_tool == str(POINTER_HEADS_TOOL):
        result = _run(
            [
                sys.executable,
                str(POINTER_HEADS_TOOL),
                "--fail_if_no_authority_head",
                "NO",
                "--expected_day_utc",
                day_utc,
                "--truth_root",
                str(sleeve_truth_root),
            ]
        )
        return {"action": "run_pointer_heads_materialize_v1", "owner_tool": owner_tool, "result": result, "nested_steps": []}
    if owner_tool == str(KILL_SWITCH_TOOL):
        nested_steps: list[dict[str, Any]] = []
        if _kill_switch_fixability(day_utc=day_utc, ib_account=ib_account) == BLOCKER_FIXABILITY_REFRESHABLE:
            bundle = _load_optional_json(_replay_bundle_path(day_utc=day_utc, ib_account=ib_account))
            missing_types = {
                str(item).strip()
                for item in ((bundle.get("inputs") or {}).get("missing_types") or [])
                if str(item).strip()
            }
            if "input_manifest" in missing_types:
                manifest_result = _run([sys.executable, str(PIPELINE_MANIFEST_TOOL), "--day_utc", day_utc])
                nested_steps.append(_loggable_step("run_pipeline_manifest_v1", manifest_result, owner_tool=str(PIPELINE_MANIFEST_TOOL)))
            if "reconciliation" in missing_types:
                broker_recon_result = _run(
                    [
                        sys.executable,
                        str(BROKER_RECON_TOOL),
                        "--day_utc",
                        day_utc,
                        "--ib_account",
                        ib_account,
                        "--mode",
                        "WRITE",
                    ]
                )
                nested_steps.append(_loggable_step("run_broker_reconciliation_day_v2", broker_recon_result, owner_tool=str(BROKER_RECON_TOOL)))
            authz_result = _run(
                [
                    sys.executable,
                    str(STARTUP_AUTHZ_TOOL),
                    "--day_utc",
                    day_utc,
                    "--truth_root",
                    str(truth_root),
                    "--environment",
                    PAPER_ENVIRONMENT,
                    "--ib_account",
                    ib_account,
                ]
            )
            nested_steps.append(_loggable_step("run_paper_startup_authorization_convergence_v1", authz_result, owner_tool=str(STARTUP_AUTHZ_TOOL)))
        result = _run([sys.executable, str(KILL_SWITCH_TOOL), "--day_utc", day_utc])
        return {
            "action": "run_global_kill_switch_v1",
            "owner_tool": owner_tool,
            "result": result,
            "nested_steps": nested_steps,
        }
    if owner_tool == str(BROKER_RECON_TOOL):
        result = _run(
            [
                sys.executable,
                str(BROKER_RECON_TOOL),
                "--day_utc",
                day_utc,
                "--ib_account",
                ib_account,
                "--mode",
                "WRITE",
            ]
        )
        return {"action": "run_broker_reconciliation_day_v2", "owner_tool": owner_tool, "result": result, "nested_steps": []}
    if owner_tool == str(EXPOSURE_NET_TOOL):
        result = _run([sys.executable, str(EXPOSURE_NET_TOOL), "--day_utc", day_utc])
        return {"action": "run_exposure_net_day_v1", "owner_tool": owner_tool, "result": result, "nested_steps": []}
    if owner_tool == str(RECONCILED_TRADE_STATE_TOOL):
        return _invoke_reconciled_trade_state_chain(day_utc=day_utc, truth_root=truth_root, ib_account=ib_account)
    if owner_tool == str(SLEEVE_EDGE_TOOL):
        return _invoke_sleeve_edge_chain(day_utc=day_utc, truth_root=truth_root, ib_account=ib_account)
    if owner_tool == str(CAPITAL_AUTHORITY_TOOL):
        return _invoke_capital_authority_chain(day_utc=day_utc, truth_root=truth_root, ib_account=ib_account)
    if owner_tool == str(STARTUP_AUTHZ_TOOL):
        result = _run(
            [
                sys.executable,
                str(STARTUP_AUTHZ_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--environment",
                PAPER_ENVIRONMENT,
                "--ib_account",
                ib_account,
            ]
        )
        return {"action": "run_paper_startup_authorization_convergence_v1", "owner_tool": owner_tool, "result": result, "nested_steps": []}
    if owner_tool == str(SESSION_AUTHORITY_TOOL):
        result = _run(
            [
                sys.executable,
                str(SESSION_AUTHORITY_TOOL),
                "--target_day",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--environment",
                PAPER_ENVIRONMENT,
                "--ib_account",
                ib_account,
                "--phase",
                "all",
            ]
        )
        return {"action": "run_session_authority_v1", "owner_tool": owner_tool, "result": result, "nested_steps": []}
    return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_tomorrow_paper_startup_prep_v1")
    ap.add_argument("--target_day", default="")
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--operator_input_root", default=str(DEFAULT_OPERATOR_INPUT_ROOT))
    ap.add_argument("--ib_account", default="")
    args = ap.parse_args(argv)

    truth_root = resolve_decision_truth_root_v1(args.truth_root or "", repo_root=REPO_ROOT)
    operator_input_root = Path(str(args.operator_input_root).strip()).expanduser().resolve()
    if not operator_input_root.exists() or not operator_input_root.is_dir():
        raise SystemExit(f"FAIL: invalid_operator_input_root path={operator_input_root}")
    ib_account = _resolve_ib_account(args.ib_account)
    target_day = str(args.target_day or "").strip() or resolve_session_authority_tomorrow_target_day_v1()

    prep_log: list[dict[str, Any]] = []
    terminal_external_blocker: dict[str, Any] | None = None
    bootstrap = _bootstrap_summary(
        day_utc=target_day,
        truth_root=truth_root,
        operator_input_root=operator_input_root,
        ib_account=ib_account,
    )

    for _ in range(MAX_ITERATIONS):
        bootstrap_status = str(bootstrap.get("bootstrap_status") or "").strip().upper()
        owner_tool = _owner_tool(bootstrap)
        reason_codes = _reason_codes(bootstrap)
        blocker_class = str(bootstrap.get("earliest_failing_prerequisite_blocker_class") or "").strip()
        fixability = _effective_fixability(summary=bootstrap, day_utc=target_day, ib_account=ib_account)
        if bootstrap_status == "READY":
            break

        action = _invoke_owner(
            owner_tool=owner_tool,
            day_utc=target_day,
            truth_root=truth_root,
            operator_input_root=operator_input_root,
            ib_account=ib_account,
        )
        if action is None:
            break

        result = dict(action.get("result") or {})
        external_blocker = action.get("external_blocker") if isinstance(action.get("external_blocker"), dict) else None
        prep_log.append(
            {
                "blocker": str(bootstrap.get("earliest_failing_prerequisite_id") or "").strip(),
                "owner_tool": owner_tool,
                "artifact_path": str(bootstrap.get("earliest_failing_prerequisite_artifact_path") or "").strip(),
                "blocker_class": blocker_class,
                "reason_codes": reason_codes,
                "fixability": fixability,
                "action": str(action.get("action") or "").strip(),
                "continuity_seed_usd": str(action.get("continuity_seed_usd") or "").strip(),
                "action_returncode": int(result.get("returncode") or 0),
                "action_stdout": str(result.get("stdout") or ""),
                "action_stderr": str(result.get("stderr") or ""),
                "nested_steps": action.get("nested_steps") or [],
                "external_blocker": external_blocker or {},
            }
        )
        if external_blocker is not None:
            terminal_external_blocker = dict(external_blocker)
            break

        next_bootstrap = _bootstrap_summary(
            day_utc=target_day,
            truth_root=truth_root,
            operator_input_root=operator_input_root,
            ib_account=ib_account,
        )
        same_blocker = (
            str(next_bootstrap.get("earliest_failing_prerequisite_id") or "").strip()
            == str(bootstrap.get("earliest_failing_prerequisite_id") or "").strip()
            and _owner_tool(next_bootstrap) == owner_tool
            and _reason_codes(next_bootstrap) == reason_codes
        )
        bootstrap = next_bootstrap
        if int(result.get("returncode") or 0) != 0:
            break
        next_fixability = _effective_fixability(summary=bootstrap, day_utc=target_day, ib_account=ib_account)
        if same_blocker and next_fixability in {BLOCKER_FIXABILITY_EXTERNAL, BLOCKER_FIXABILITY_UNKNOWN}:
            break

    admission_path = (truth_root / "target_day_admission_v1" / f"{target_day}.json").resolve()
    build_path = (truth_root / "target_day_build_v1" / f"{target_day}.json").resolve()
    submit_boundary_path = (
        truth_root / "reports" / "submit_boundary_status_v1" / target_day / "submit_boundary_status.v1.json"
    ).resolve()
    admission_payload = _load_optional_json(admission_path)
    build_payload = _load_optional_json(build_path)
    submit_boundary_payload = _load_optional_json(submit_boundary_path)

    final_fixability = BLOCKER_FIXABILITY_UNKNOWN
    surfaced_by_owner_tool = ""
    if terminal_external_blocker is not None:
        final_owner_tool = str(terminal_external_blocker.get("owner_tool") or "").strip()
        final_blocker = str(terminal_external_blocker.get("blocker") or "").strip()
        final_artifact_path = str(terminal_external_blocker.get("artifact_path") or "").strip()
        final_blocker_class = str(terminal_external_blocker.get("blocker_class") or "").strip()
        final_reason_codes = [
            str(code).strip()
            for code in (terminal_external_blocker.get("reason_codes") or [])
            if str(code).strip()
        ]
        final_fixability = str(terminal_external_blocker.get("fixability") or BLOCKER_FIXABILITY_EXTERNAL).strip()
        canonical_next_action = str(terminal_external_blocker.get("canonical_next_action") or "").strip()
        surfaced_by_owner_tool = str(terminal_external_blocker.get("surfaced_by_owner_tool") or "").strip()
    else:
        final_owner_tool = _owner_tool(bootstrap)
        final_blocker = str(bootstrap.get("earliest_failing_prerequisite_id") or "").strip()
        final_artifact_path = str(bootstrap.get("earliest_failing_prerequisite_artifact_path") or "").strip()
        final_blocker_class = str(bootstrap.get("earliest_failing_prerequisite_blocker_class") or "").strip()
        final_reason_codes = _reason_codes(bootstrap)
        final_fixability = _effective_fixability(summary=bootstrap, day_utc=target_day, ib_account=ib_account)
        canonical_next_action = _next_action_text(
            day_utc=target_day,
            owner_tool=final_owner_tool,
            fixability=final_fixability,
        )
        if str(bootstrap.get("bootstrap_status") or "").strip().upper() == "READY":
            post_bootstrap_blocker = _resolve_post_bootstrap_blocker(day_utc=target_day, truth_root=truth_root)
            if post_bootstrap_blocker is not None:
                final_owner_tool = str(post_bootstrap_blocker.get("owner_tool") or "").strip()
                final_blocker = str(post_bootstrap_blocker.get("blocker") or "").strip()
                final_artifact_path = str(post_bootstrap_blocker.get("artifact_path") or "").strip()
                final_blocker_class = str(post_bootstrap_blocker.get("blocker_class") or "").strip()
                final_reason_codes = [
                    str(code).strip()
                    for code in (post_bootstrap_blocker.get("reason_codes") or [])
                    if str(code).strip()
                ]
                final_fixability = str(post_bootstrap_blocker.get("fixability") or BLOCKER_FIXABILITY_UNKNOWN).strip()
                canonical_next_action = str(post_bootstrap_blocker.get("canonical_next_action") or "").strip()

    downstream_ready = (
        str(bootstrap.get("bootstrap_status") or "").strip().upper() == "READY"
        and str(build_payload.get("build_status") or "").strip().upper() in {"COMPLETE", "READY"}
        and str(admission_payload.get("admission_status") or bootstrap.get("target_day_admission_status") or "").strip().upper() == "ADMIT"
        and str(submit_boundary_payload.get("boundary_status") or "").strip().upper() == "AUTHORIZED"
        and bool(submit_boundary_payload.get("submission_authorized") is True)
    )
    effective_fixability = BLOCKER_FIXABILITY_UNKNOWN if downstream_ready else final_fixability
    readiness_state = derive_tomorrow_readiness_state_v1(
        bootstrap_status=str(bootstrap.get("bootstrap_status") or "").strip(),
        blocker_fixability=effective_fixability,
        reason_codes=final_reason_codes,
        downstream_ready=downstream_ready,
    )
    classification = classify_day_readiness_projection_v1(
        blocker_fixability=effective_fixability,
        reason_codes=final_reason_codes,
    )
    if downstream_ready:
        canonical_next_action = (
            f"Rerun {BOOTSTRAP_TOOL} or the canonical operator/systemd entrypoint for target_day={target_day}; "
            "bootstrap, pre-open, promotion, Session Authority, and submit-boundary surfaces are aligned for deeper progression."
        )
    elif not canonical_next_action:
        canonical_next_action = _next_action_text(
            day_utc=target_day,
            owner_tool=final_owner_tool,
            fixability=effective_fixability,
        )

    automation_run_at = now_utc_iso_v1()
    summary = {
        "target_day": target_day,
        "tomorrow_target_day": target_day,
        "automation_run_at": automation_run_at,
        "readiness_state": readiness_state,
        "classification": classification,
        "bootstrap_status": str(bootstrap.get("bootstrap_status") or "").strip(),
        "bootstrap_report_path": str(bootstrap.get("path") or "").strip(),
        "earliest_failing_prerequisite": final_blocker,
        "latest_first_failing_prerequisite": final_blocker,
        "owner_tool": final_owner_tool,
        "surfaced_by_owner_tool": surfaced_by_owner_tool,
        "artifact_path": final_artifact_path,
        "blocker_class": final_blocker_class,
        "reason_codes": final_reason_codes,
        "canonical_next_step": canonical_next_action,
        "canonical_next_action": canonical_next_action,
        "rerun_canonical_bootstrap_required": readiness_state != DAY_READINESS_READY_FOR_DAY,
        "target_day_build_path": str(build_path),
        "target_day_build_status": str(build_payload.get("build_status") or "").strip(),
        "target_day_build_closure_status": str(build_payload.get("closure_status") or "").strip(),
        "target_day_admission_path": str(admission_path),
        "target_day_admission_status": str(admission_payload.get("admission_status") or bootstrap.get("target_day_admission_status") or "").strip(),
        "target_day_admission_closure_status": str(admission_payload.get("closure_status") or "").strip(),
        "submit_boundary_status_path": str(submit_boundary_path),
        "submit_boundary_status": str(submit_boundary_payload.get("boundary_status") or "").strip(),
        "submit_boundary_submission_authorized": bool(submit_boundary_payload.get("submission_authorized") is True),
        "prep_log": prep_log,
    }
    day_readiness_ref = _write_day_readiness_automation_report(target_day=target_day, truth_root=truth_root, summary=summary)
    summary["day_readiness_automation_path"] = str(day_readiness_ref["path"])
    summary["day_readiness_automation_sha256"] = str(day_readiness_ref["sha256"])
    print(json.dumps(summary, sort_keys=True))
    return 0 if readiness_state == DAY_READINESS_READY_FOR_DAY else 2

if __name__ == "__main__":
    raise SystemExit(main())
