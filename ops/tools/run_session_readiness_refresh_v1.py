#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.authority_kernel_v1 import run_day_authority_preflight_v1
from constellation_2.common.day_authority_decision_v1 import (
    build_day_authority_decision_v1,
    write_day_authority_decision_v1,
)
from constellation_2.common.operator_summary_v1 import write_operator_summary
from constellation_2.common.artifact_lifecycle_incident_v1 import (
    list_unresolved_authority_lifecycle_incidents,
    summarize_lifecycle_incidents,
)
from constellation_2.common.trade_submit_readiness_authority_v1 import (
    GovernedSleeveTruthBinding,
    resolve_governed_sleeve_truth_bindings,
)
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_operator_statement_path,
    resolve_paper_trading_posture_path,
    resolve_repo_operator_input_root,
    resolve_repo_truth_root,
    resolve_session_readiness_refresh_path,
)


GLOBAL_TRUTH_ROOT = resolve_repo_truth_root(REPO_ROOT)
ACCOUNT_REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_IB_ACCOUNT_REGISTRY_V1.json").resolve()
SLEEVE_REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_SLEEVE_REGISTRY_V1.json").resolve()
BROKER_EVENTS_BOOTSTRAP_TOOL = (REPO_ROOT / "ops/ib/c2_execution_observer_v1.py").resolve()
BROKER_EVENTS_BOOTSTRAP_PYTHON = REPO_ROOT / ".venv_c2/bin/python"
BROKER_EVENTS_BOOTSTRAP_CLIENT_ID = "179"
BROKER_EVENTS_MANIFEST_TOOL = (REPO_ROOT / "ops/ib/run_broker_event_day_manifest_v1.py").resolve()
HANDSHAKE_TOOL = (REPO_ROOT / "ops/tools/run_ib_api_handshake_spine_v1.py").resolve()
READINESS_TOOL = (REPO_ROOT / "ops/tools/run_trade_submit_readiness_c2_v1.py").resolve()
EXECUTION_RECONCILIATION_TOOL = (REPO_ROOT / "ops/tools/run_execution_reconciliation_day_v1.py").resolve()
OPERATOR_STATEMENT_TOOL = (REPO_ROOT / "ops/tools/ensure_cash_ledger_operator_statement_v1.py").resolve()
OPERATOR_STATEMENT_PYTHON = REPO_ROOT / ".venv_c2/bin/python"
OPERATOR_STATEMENT_CLIENT_ID = "92"
ACCOUNTING_NAV_TOOL = (REPO_ROOT / "ops/tools/run_accounting_nav_v2_day_v1.py").resolve()
MONITORING_REFRESH_TOOL = (REPO_ROOT / "ops/tools/run_c2_global_monitoring_refresh_v1.py").resolve()
GLOBAL_GATE_REFRESH_TOOL = (REPO_ROOT / "ops/tools/run_c2_global_gate_refresh_v1.py").resolve()
REGISTRY_COHERENCE_TOOL = (REPO_ROOT / "ops/tools/run_registry_coherence_check_v1.py").resolve()
ARTIFACT_FRESHNESS_SUMMARY_TOOL = (REPO_ROOT / "ops/tools/run_operator_artifact_freshness_summary_v1.py").resolve()
PNL_ATTRIBUTION_TOOL = (REPO_ROOT / "ops/tools/run_pnl_attribution_v1.py").resolve()
TRADING_DAY_STATE_MACHINE_TOOL = (REPO_ROOT / "ops/tools/run_trading_day_state_machine_v1.py").resolve()
TRADING_DAY_INTENT_GENERATION_TOOL = (REPO_ROOT / "ops/tools/run_trading_day_intent_generation_v1.py").resolve()
STARTUP_PROOF_VALIDATION_TOOL = (REPO_ROOT / "ops/tools/run_startup_proof_validation_v1.py").resolve()
STARTUP_MATERIALIZATION_TOOL = (REPO_ROOT / "ops/tools/run_startup_materialization_v1.py").resolve()
PAPER_TRADING_POSTURE_TOOL = (REPO_ROOT / "ops/tools/run_paper_trading_posture_v1.py").resolve()
SUBMIT_BOUNDARY_STATUS_TOOL = (REPO_ROOT / "ops/tools/run_submit_boundary_status_v1.py").resolve()
PAPER_SESSION_LEDGER_TOOL = (REPO_ROOT / "ops/tools/run_paper_session_ledger_v1.py").resolve()
SESSION_REPORT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/session_readiness_refresh.v1.schema.json"
PRIMARY_SLEEVE_ID = "PRIMARY"
OPERATOR_INPUT_TRUTH_ROOT = resolve_repo_operator_input_root(REPO_ROOT)


def _read_json(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: top_level_not_object path={path}")
    return obj


def _run(cmd: List[str], *, extra_env: Dict[str, str] | None = None) -> Dict[str, Any]:
    env = os.environ.copy()
    env["C2_TRUTH_ROOT"] = str(GLOBAL_TRUTH_ROOT)
    if extra_env:
        env.update(extra_env)
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _parse_json_stdout(result: Dict[str, Any]) -> Dict[str, Any]:
    stdout = str(result.get("stdout") or "").strip()
    if not stdout:
        return {}
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_broker_events_bootstrap_python() -> Path:
    candidates = [
        BROKER_EVENTS_BOOTSTRAP_PYTHON.resolve(),
        (REPO_ROOT.parent / "constellation_2_runtime" / ".venv_c2" / "bin" / "python").resolve(),
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return Path(sys.executable).resolve()


def _write_day_authority_decision_from_validation(
    *,
    day_utc: str,
    authority_result: Dict[str, Any],
    truth_root: Path,
    producer_module: str,
    producer_git_sha: str,
) -> Dict[str, Any]:
    summary = authority_result.get("validation_summary") or {}
    if not isinstance(summary, dict):
        return {
            "status": "SKIPPED_INVALID_AUTHORITY_RESULT",
            "reason": "validation_summary_missing",
        }
    first_failure = summary.get("first_failure")
    if first_failure is not None and not isinstance(first_failure, dict):
        first_failure = None
    payload = build_day_authority_decision_v1(
        trading_day=day_utc,
        decision_state="OPEN" if str(summary.get("validation_state") or "").strip().upper() == "PASS" else "BLOCKED",
        heartbeat={"status": "READY", "source_ref": "results.ib_api_handshake"},
        first_failure=first_failure,
        stage="PRE_ORCHESTRATION_PREFLIGHT",
        orchestrator_started=False,
        blocking_class=str(summary.get("blocking_class") or "NONE").strip() or "NONE",
        blocking_evidence=list(summary.get("blocking_evidence") or []),
        missing_or_invalid_prerequisite_refs=list(summary.get("missing_or_invalid_prerequisite_refs") or []),
        authority_attestation_refs_used=list(summary.get("validation_refs_used") or []),
        compatibility_status=dict(summary.get("compatibility_status") or {}),
        diagnostic_warnings=list(summary.get("diagnostic_warnings") or []),
        truth_root=truth_root,
        producer_module=producer_module,
        producer_git_sha=producer_git_sha,
    )
    ref = write_day_authority_decision_v1(truth_root=truth_root, payload=payload)
    return {
        "status": "OK",
        "path": str(ref.path),
        "sha256": ref.sha256,
        "decision_state": str(ref.payload.get("decision_state") or "").strip(),
    }


def _paper_noop_hard_failures_nonblocking(hard_failures: set[str]) -> bool:
    if not hard_failures:
        return False
    allowed_nonblocking_noop = {
        "canonical_intent_publication_v1",
        "canonical_market_data_preopen_prepare_v1",
        "signal_proof_publication_v1",
        "pointer_heads_materialize_v1",
    }
    return hard_failures.issubset(allowed_nonblocking_noop)


def _paper_expected_calendar_no_op(day_utc: str) -> bool:
    posture_path = resolve_paper_trading_posture_path(truth_root=GLOBAL_TRUTH_ROOT, day_utc=day_utc)
    if not posture_path.exists() or not posture_path.is_file():
        return False
    try:
        posture = _read_json(posture_path)
    except Exception:
        return False
    posture_class = str(posture.get("posture_class") or "").strip().upper()
    blocking_family = str(posture.get("blocking_family") or "").strip().upper()
    expected_no_op_today = bool(posture.get("expected_no_op_today") is True)
    blocking_reason_codes = {
        str(code).strip()
        for code in (posture.get("blocking_reason_codes") or [])
        if str(code).strip()
    }
    return (
        posture_class == "PAPER_READY_NO_OP"
        and blocking_family == "EXPECTED_NO_OP"
        and expected_no_op_today
        and "MARKET_CALENDAR_NON_TRADING_SESSION" in blocking_reason_codes
    )


def _paper_global_gate_refresh_nonblocking(result: Dict[str, Any], *, expected_calendar_no_op: bool = False) -> bool:
    if int(result.get("returncode") or 0) == 0:
        return False
    payload = _parse_json_stdout(result)
    hard_failures = {
        str(name).strip()
        for name in (payload.get("hard_failures") or [])
        if str(name).strip()
    }
    if not hard_failures:
        return True
    allowed_nonblocking = {
        "exit_reconciliation_v1",
        "pointer_heads_materialize_v1",
    }
    if hard_failures.issubset(allowed_nonblocking):
        return True
    if expected_calendar_no_op and _paper_noop_hard_failures_nonblocking(hard_failures):
        return True
    return False


def _ensure_execution_reconciliation_day_roots(day_utc: str) -> None:
    for rel in (
        f"execution_evidence_v1/submissions/{day_utc}",
        f"fill_ledger_v1/{day_utc}",
        f"execution_stream_v1/{day_utc}",
    ):
        (GLOBAL_TRUTH_ROOT / rel).resolve().mkdir(parents=True, exist_ok=True)


def _git_sha() -> str:
    try:
        return subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT)).decode("utf-8").strip()
    except Exception:
        # Clean runtime roots can be source-derived without .git metadata.
        return "0" * 40


def _resolve_paper_sleeve_truth_bindings(*, paper_account: str) -> List[GovernedSleeveTruthBinding]:
    bindings = resolve_governed_sleeve_truth_bindings(
        repo_root=REPO_ROOT,
        environment="PAPER",
        requested_ib_account=paper_account,
    )
    deduped: List[GovernedSleeveTruthBinding] = []
    seen: set[tuple[str, str]] = set()
    for binding in bindings:
        key = (str(binding.sleeve_id).strip().upper(), str(binding.truth_root.resolve()))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(binding)
    return deduped


def _seed_sleeve_positions_snapshots(*, day_utc: str, paper_account: str) -> Dict[str, Any]:
    seeded_paths: List[str] = []
    bindings = _resolve_paper_sleeve_truth_bindings(paper_account=paper_account)
    canonical_path = (GLOBAL_TRUTH_ROOT / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json").resolve()
    if not canonical_path.exists() or not canonical_path.is_file():
        return {"status": "OK", "seeded_paths": [], "seeded_count": 0}
    canonical_payload = _read_json(canonical_path)
    canonical_text = canonical_path.read_text(encoding="utf-8")
    for binding in bindings:
        sleeve_snapshot = (
            binding.truth_root.resolve()
            / "positions_v1"
            / "snapshots"
            / day_utc
            / "positions_snapshot.v2.json"
        ).resolve()
        if sleeve_snapshot.exists():
            continue
        sleeve_snapshot.parent.mkdir(parents=True, exist_ok=True)
        sleeve_snapshot.write_text(canonical_text, encoding="utf-8")
        written_payload = _read_json(sleeve_snapshot)
        if written_payload != canonical_payload:
            raise RuntimeError(f"SLEEVE_POSITIONS_SNAPSHOT_SEED_MISMATCH:sleeve_id={binding.sleeve_id}:day_utc={day_utc}")
        seeded_paths.append(str(sleeve_snapshot))
    return {
        "status": "OK",
        "seeded_paths": sorted(set(seeded_paths)),
        "seeded_count": len(sorted(set(seeded_paths))),
    }


def _annotate_scoped_gate_refresh_result(
    *,
    binding: GovernedSleeveTruthBinding,
    result: Dict[str, Any],
    expected_calendar_no_op: bool,
) -> Dict[str, Any]:
    payload = _parse_json_stdout(result)
    hard_failures = sorted({str(name).strip() for name in (payload.get("hard_failures") or []) if str(name).strip()})
    noop_nonblocking = expected_calendar_no_op and _paper_noop_hard_failures_nonblocking(set(hard_failures))
    annotated = dict(result)
    annotated["sleeve_id"] = str(binding.sleeve_id).strip().upper()
    annotated["environment"] = str(binding.environment).strip().upper()
    annotated["scope_truth_root"] = str(binding.truth_root.resolve())
    annotated["truth_partition"] = str(binding.truth_partition)
    annotated["hard_failures"] = hard_failures
    annotated["nonblocking_override"] = False
    if int(result.get("returncode") or 0) == 0:
        annotated["state"] = "READY"
        annotated["blocking_scope"] = "NONE"
        annotated["reason_codes"] = []
    elif annotated["sleeve_id"] == PRIMARY_SLEEVE_ID and noop_nonblocking:
        annotated["state"] = "READY_WITH_EXPECTED_NO_OP_GAPS"
        annotated["blocking_scope"] = "NONE"
        annotated["reason_codes"] = sorted(set(["EXPECTED_NO_OP_NON_TRADING_SESSION"] + hard_failures))
        annotated["nonblocking_override"] = True
    elif annotated["sleeve_id"] == PRIMARY_SLEEVE_ID:
        annotated["state"] = "BLOCKED"
        annotated["blocking_scope"] = "PRIMARY_SLEEVE"
        annotated["reason_codes"] = hard_failures or [f"SCOPED_GATE_REFRESH_FAILED:{annotated['sleeve_id']}"]
    else:
        annotated["state"] = "BLOCKED"
        annotated["blocking_scope"] = "SLEEVE"
        annotated["reason_codes"] = hard_failures or [f"SCOPED_GATE_REFRESH_FAILED:{annotated['sleeve_id']}"]
    return annotated


def _scope_summary_from_results(scoped_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    blocked_primary: List[str] = []
    blocked_other: List[str] = []
    primary_reason_codes: List[str] = []
    for row in scoped_results:
        if int(row.get("returncode") or 0) == 0 or bool(row.get("nonblocking_override") is True):
            continue
        sleeve_id = str(row.get("sleeve_id") or "").strip().upper()
        row_reason_codes = [str(code).strip() for code in (row.get("reason_codes") or []) if str(code).strip()]
        if sleeve_id == PRIMARY_SLEEVE_ID:
            blocked_primary.append(sleeve_id)
            primary_reason_codes.extend(row_reason_codes)
        elif sleeve_id:
            blocked_other.append(sleeve_id)
    if blocked_primary:
        global_state = "BLOCKED"
    elif blocked_other:
        global_state = "DEGRADED"
    else:
        global_state = "PASS"
    return {
        "primary_sleeve_id": PRIMARY_SLEEVE_ID,
        "global_readiness_state": global_state,
        "primary_ready": not blocked_primary,
        "blocked_primary_sleeve_ids": sorted(set(blocked_primary)),
        "nonblocking_blocked_sleeve_ids": sorted(set(blocked_other)),
        "primary_blocker_reason_codes": sorted(set(primary_reason_codes)),
    }


def _write_session_report(*, day_utc: str, results: Dict[str, Any], failures: List[str], truth_root: Path) -> tuple[Path, str]:
    produced_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    core_failure_prefixes = {
        "operator_statement",
        "positions_snapshot_refresh",
        "cash_ledger_snapshot_refresh",
        "accounting_nav_refresh",
        "broker_events_bootstrap",
        "broker_event_day_manifest",
        "ib_api_handshake",
        "global_gate_refresh",
        "trade_submit_readiness",
        "artifact_lifecycle_incidents",
        "scoped_gate_refresh:PRIMARY",
        "trading_day_state_machine",
    }
    monitoring_failure_prefixes = (
        "monitoring_refresh",
        "registry_coherence_check",
        "operator_artifact_freshness_summary",
        "pnl_attribution",
        "startup_proof_validation",
    )
    scope_summary = results.get("scope_summary") if isinstance(results.get("scope_summary"), dict) else {}
    nonblocking_blocked_sleeves = [
        str(item).strip().upper()
        for item in (scope_summary.get("nonblocking_blocked_sleeve_ids") or [])
        if str(item).strip()
    ]
    monitoring_failure = any(any(item.startswith(prefix) for prefix in monitoring_failure_prefixes) for item in failures) or bool(nonblocking_blocked_sleeves)
    core_failures = [item for item in failures if any(item == prefix or item.startswith(f"{prefix}:") for prefix in core_failure_prefixes)]
    ledger_result = results.get("paper_session_ledger")
    ledger_payload = _parse_json_stdout(ledger_result) if isinstance(ledger_result, dict) else {}
    ledger_authority_status = str(ledger_payload.get("authority_status") or "").strip().upper() or "UNKNOWN"
    if ledger_authority_status != "GRANTED":
        core_failures.append("paper_session_ledger_denied")
    status = "FAIL" if core_failures else ("OK_WITH_MONITORING_GAPS" if monitoring_failure else "OK")
    monitoring_failure_count = len([item for item in failures if any(item.startswith(prefix) for prefix in monitoring_failure_prefixes)]) + len(nonblocking_blocked_sleeves)
    report: Dict[str, Any] = {
        "schema_id": "C2_SESSION_READINESS_REFRESH_V1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": produced_utc,
        "producer": {
            "repo": REPO_ROOT.name,
            "git_sha": _git_sha(),
            "module": "ops/tools/run_session_readiness_refresh_v1.py",
        },
        "status": status,
        "overall_refresh_verdict": status,
        "core_failure_count": len(core_failures),
        "monitoring_failure_count": monitoring_failure_count,
        "failures": sorted(set(failures)),
        "results": {
            **results,
            "paper_session_ledger_gate": {
                "authority_scope": "DERIVED_ONLY_VIEW",
                "ledger_path": str(ledger_payload.get("path") or ""),
                "ledger_id": str(ledger_payload.get("ledger_id") or ""),
                "authority_status": ledger_authority_status,
            },
        },
        "canonical_json_hash": "",
    }
    report["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(report)
    validate_against_repo_schema_v1(report, REPO_ROOT, SESSION_REPORT_SCHEMA)
    out_path = resolve_session_readiness_refresh_path(truth_root=truth_root, day_utc=day_utc)
    write_day_artifact_refreshable_v1(
        path=out_path,
        data=canonical_json_bytes_v1(report) + b"\n",
        expected_day_utc=day_utc,
        expected_schema_id="C2_SESSION_READINESS_REFRESH_V1",
        expected_schema_version=1,
        preserve_statuses=(),
    )
    return out_path, status


def _load_accounts() -> Dict[str, List[str]]:
    sleeve_reg = _read_json(SLEEVE_REGISTRY_PATH)
    sleeve_rows = sleeve_reg.get("sleeves")
    if not isinstance(sleeve_rows, list):
        raise SystemExit("FAIL: sleeve_registry_sleeves_not_list")
    active_accounts_by_env: Dict[str, set[str]] = {"PAPER": set(), "LIVE": set()}
    for row in sleeve_rows:
        if not isinstance(row, dict):
            continue
        if row.get("enabled") is not True:
            continue
        env = str(row.get("mode") or "").strip().upper()
        account_id = str(row.get("ib_account") or "").strip()
        if env in active_accounts_by_env and account_id:
            active_accounts_by_env[env].add(account_id)

    reg = _read_json(ACCOUNT_REGISTRY_PATH)
    rows = reg.get("accounts")
    if not isinstance(rows, list):
        raise SystemExit("FAIL: account_registry_accounts_not_list")
    out: Dict[str, List[str]] = {"PAPER": [], "LIVE": []}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("enabled_for_submission") is not True:
            continue
        env = str(row.get("environment") or "").strip().upper()
        account_id = str(row.get("account_id") or "").strip()
        if env in out and account_id and account_id in active_accounts_by_env.get(env, set()):
            out[env].append(account_id)
    for env in out:
        out[env] = sorted(set(out[env]))
    return out


def _authority_lifecycle_result(day_utc: str, truth_root: Path) -> Dict[str, Any]:
    incidents = list_unresolved_authority_lifecycle_incidents(truth_root=truth_root, day_utc=day_utc)
    return {
        "status": "FAIL" if incidents else "OK",
        "incident_count": len(incidents),
        "incidents": summarize_lifecycle_incidents(incidents),
    }


def _write_operator_summary_safe(*, day_utc: str, truth_root: Path) -> Dict[str, Any]:
    try:
        ref = write_operator_summary(summary_kind="preopen", day_utc=day_utc, truth_root=truth_root)
        return {"status": "OK", "path": str(ref.path), "sha256": ref.sha256}
    except BaseException as exc:
        return {"status": "ERROR", "error": str(exc)}


def main() -> int:
    global GLOBAL_TRUTH_ROOT
    ap = argparse.ArgumentParser(prog="run_session_readiness_refresh_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args()

    if str(args.truth_root).strip():
        GLOBAL_TRUTH_ROOT = Path(str(args.truth_root)).resolve()

    day = str(args.day_utc).strip()
    paper_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    accounts = _load_accounts()
    git_sha = _git_sha()
    baseline_b2_python = str(Path(sys.executable).resolve())
    operator_statement_path = resolve_operator_statement_path(
        operator_input_root=OPERATOR_INPUT_TRUTH_ROOT,
        day_utc=day,
    )

    results: Dict[str, Any] = {}
    failures: List[str] = []
    posture_cmd = [
        sys.executable,
        str(PAPER_TRADING_POSTURE_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["paper_trading_posture"] = _run(posture_cmd)
    if results["paper_trading_posture"]["returncode"] != 0:
        failures.append("paper_trading_posture")
    expected_calendar_no_op = _paper_expected_calendar_no_op(day)

    operator_cmd = [
        str(OPERATOR_STATEMENT_PYTHON if OPERATOR_STATEMENT_PYTHON.exists() else Path(sys.executable).resolve()),
        str(OPERATOR_STATEMENT_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(OPERATOR_INPUT_TRUTH_ROOT),
        "--ib_account",
        paper_account,
        "--mode",
        "SEED_100K",
        "--allow_create",
        "YES",
    ]
    if "operator_statement" not in results:
        results["operator_statement"] = _run(operator_cmd)
        if results["operator_statement"]["returncode"] != 0:
            failures.append("operator_statement")

    intent_generation_cmd = [
        sys.executable,
        str(TRADING_DAY_INTENT_GENERATION_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["trading_day_intent_generation"] = _run(intent_generation_cmd)
    if results["trading_day_intent_generation"]["returncode"] != 0:
        failures.append("trading_day_intent_generation")

    startup_materialization_cmd = [
        sys.executable,
        str(STARTUP_MATERIALIZATION_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["startup_materialization"] = _run(startup_materialization_cmd)
    if results["startup_materialization"]["returncode"] != 0:
        failures.append("startup_materialization")
        authority_result = run_day_authority_preflight_v1(
            truth_root=GLOBAL_TRUTH_ROOT,
            day_utc=day,
            startup_materialization_result=results["startup_materialization"],
            handshake_result=None,
            global_gate_result=None,
            scope_summary=None,
            scoped_gate_results=[],
            producer_module="ops/tools/run_session_readiness_refresh_v1.py",
            producer_git_sha=git_sha,
            global_gate_nonblocking=False,
        )
        results["authority_kernel_validation"] = authority_result
        report_path, report_status = _write_session_report(day_utc=day, results=results, failures=failures, truth_root=GLOBAL_TRUTH_ROOT)
        results["session_readiness_report"] = {"path": str(report_path)}
        results["operator_summary"] = _write_operator_summary_safe(day_utc=day, truth_root=GLOBAL_TRUTH_ROOT)
        print(
            json.dumps(
                {
                    "day_utc": day,
                    "truth_root": str(GLOBAL_TRUTH_ROOT),
                    "results": results,
                    "failures": failures,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0 if report_status in {"OK", "OK_WITH_MONITORING_GAPS"} else 2

    operator_cmd = [
        str(OPERATOR_STATEMENT_PYTHON if OPERATOR_STATEMENT_PYTHON.exists() else Path(sys.executable).resolve()),
        str(OPERATOR_STATEMENT_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(OPERATOR_INPUT_TRUTH_ROOT),
        "--ib_account",
        paper_account,
        "--mode",
        "SEED_100K",
        "--allow_create",
        "YES",
    ]
    results["operator_statement"] = _run(operator_cmd)
    if results["operator_statement"]["returncode"] != 0:
        failures.append("operator_statement")

    positions_snapshot_cmd = [
        baseline_b2_python,
        "-m",
        "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2",
        "--day_utc",
        day,
        "--producer_git_sha",
        git_sha,
        "--producer_repo",
        REPO_ROOT.name,
    ]
    results["positions_snapshot_refresh"] = _run(positions_snapshot_cmd)
    if results["positions_snapshot_refresh"]["returncode"] != 0:
        failures.append("positions_snapshot_refresh")

    cash_ledger_snapshot_cmd = [
        baseline_b2_python,
        "-m",
        "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1",
        "--day_utc",
        day,
        "--operator_statement_json",
        str(operator_statement_path),
        "--producer_repo",
        REPO_ROOT.name,
        "--producer_git_sha",
        git_sha,
    ]
    results["cash_ledger_snapshot_refresh"] = _run(cash_ledger_snapshot_cmd)
    if results["cash_ledger_snapshot_refresh"]["returncode"] != 0:
        failures.append("cash_ledger_snapshot_refresh")

    accounting_nav_cmd = [
        baseline_b2_python,
        str(ACCOUNTING_NAV_TOOL),
        "--day_utc",
        day,
        "--producer_repo",
        REPO_ROOT.name,
        "--producer_git_sha",
        git_sha,
    ]
    results["accounting_nav_refresh"] = _run(accounting_nav_cmd)
    if results["accounting_nav_refresh"]["returncode"] != 0:
        failures.append("accounting_nav_refresh")

    try:
        results["sleeve_positions_snapshot_seed"] = _seed_sleeve_positions_snapshots(day_utc=day, paper_account=paper_account)
    except BaseException as exc:
        results["sleeve_positions_snapshot_seed"] = {"status": "ERROR", "error": str(exc)}
        failures.append("sleeve_positions_snapshot_seed")

    broker_events_cmd = [
        str(_resolve_broker_events_bootstrap_python()),
        str(BROKER_EVENTS_BOOTSTRAP_TOOL),
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
        "--host",
        "127.0.0.1",
        "--port",
        "4002",
        "--client-id",
        BROKER_EVENTS_BOOTSTRAP_CLIENT_ID,
        "--poll-seconds",
        "10",
        "--environment",
        "PAPER",
        "--log-root",
        "constellation_2/runtime/truth/execution_evidence_v1/broker_events",
        "--day-utc",
        day,
        "--bootstrap-handshake-only",
        "--handshake-timeout-seconds",
        "15",
    ]
    results["broker_events_bootstrap"] = _run(broker_events_cmd)
    if results["broker_events_bootstrap"]["returncode"] != 0:
        failures.append("broker_events_bootstrap")

    broker_events_manifest_cmd = [
        sys.executable,
        str(BROKER_EVENTS_MANIFEST_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["broker_event_day_manifest"] = _run(broker_events_manifest_cmd)
    if results["broker_event_day_manifest"]["returncode"] != 0:
        failures.append("broker_event_day_manifest")

    handshake_cmd = [
        sys.executable,
        str(HANDSHAKE_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["ib_api_handshake"] = _run(handshake_cmd)
    if results["ib_api_handshake"]["returncode"] != 0:
        failures.append("ib_api_handshake")

    global_gate_refresh_cmd = [
        sys.executable,
        str(GLOBAL_GATE_REFRESH_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
        "--mode",
        "PAPER",
    ]
    results["global_gate_refresh"] = _run(global_gate_refresh_cmd)
    if results["global_gate_refresh"]["returncode"] != 0 and not _paper_global_gate_refresh_nonblocking(
        results["global_gate_refresh"],
        expected_calendar_no_op=expected_calendar_no_op,
    ):
        failures.append("global_gate_refresh")

    scoped_gate_refresh_results: List[Dict[str, Any]] = []
    try:
        scoped_bindings = _resolve_paper_sleeve_truth_bindings(paper_account=paper_account)
    except ValueError as exc:
        scoped_gate_refresh_results.append(
            {
                "scope": "PAPER_SLEEVE_BINDINGS",
                "returncode": 2,
                "error": str(exc),
                "state": "BLOCKED",
                "blocking_scope": "GLOBAL",
                "reason_codes": [str(exc)],
            }
        )
        failures.append("scoped_gate_refresh")
        scoped_bindings = []

    for binding in scoped_bindings:
        cmd = [
            sys.executable,
            str(GLOBAL_GATE_REFRESH_TOOL),
            "--day_utc",
            day,
            "--truth_root",
            str(binding.truth_root.resolve()),
            "--mode",
            "PAPER",
        ]
        annotated = _annotate_scoped_gate_refresh_result(
            binding=binding,
            result=_run(cmd),
            expected_calendar_no_op=expected_calendar_no_op,
        )
        scoped_gate_refresh_results.append(annotated)
        if (
            int(annotated.get("returncode") or 0) != 0
            and str(annotated.get("sleeve_id") or "") == PRIMARY_SLEEVE_ID
            and not bool(annotated.get("nonblocking_override") is True)
        ):
            failures.append(f"scoped_gate_refresh:PRIMARY:{annotated['scope_truth_root']}")
    results["scoped_gate_refresh"] = scoped_gate_refresh_results
    results["scope_summary"] = _scope_summary_from_results(scoped_gate_refresh_results)

    authority_result = run_day_authority_preflight_v1(
        truth_root=GLOBAL_TRUTH_ROOT,
        day_utc=day,
        startup_materialization_result=results.get("startup_materialization"),
        handshake_result=results.get("ib_api_handshake"),
        global_gate_result=results.get("global_gate_refresh"),
        scope_summary=results.get("scope_summary"),
        scoped_gate_results=scoped_gate_refresh_results,
        producer_module="ops/tools/run_session_readiness_refresh_v1.py",
        producer_git_sha=git_sha,
        global_gate_nonblocking=_paper_global_gate_refresh_nonblocking(
            results["global_gate_refresh"],
            expected_calendar_no_op=expected_calendar_no_op,
        ),
    )
    results["authority_kernel_validation"] = authority_result
    results["day_authority_decision"] = _write_day_authority_decision_from_validation(
        day_utc=day,
        authority_result=authority_result,
        truth_root=GLOBAL_TRUTH_ROOT,
        producer_module="ops/tools/run_session_readiness_refresh_v1.py",
        producer_git_sha=git_sha,
    )

    readiness_results: List[Dict[str, Any]] = []
    for env in ("PAPER", "LIVE"):
        for account_id in accounts.get(env, []):
            cmd = [
                sys.executable,
                str(READINESS_TOOL),
                "--day_utc",
                day,
                "--ib_account",
                account_id,
                "--environment",
                env,
            ]
            result = _run(cmd)
            result["environment"] = env
            result["ib_account"] = account_id
            readiness_results.append(result)
            if result["returncode"] != 0:
                failures.append(f"trade_submit_readiness:{env}:{account_id}")
    results["trade_submit_readiness"] = readiness_results
    submit_boundary_cmd = [
        sys.executable,
        str(SUBMIT_BOUNDARY_STATUS_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["submit_boundary_status"] = _run(submit_boundary_cmd)
    if results["submit_boundary_status"]["returncode"] != 0:
        failures.append("submit_boundary_status")
    ledger_cmd = [
        sys.executable,
        str(PAPER_SESSION_LEDGER_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["paper_session_ledger"] = _run(ledger_cmd)
    if results["paper_session_ledger"]["returncode"] != 0:
        failures.append("paper_session_ledger")

    results["artifact_lifecycle_incidents"] = _authority_lifecycle_result(day, GLOBAL_TRUTH_ROOT)
    if results["artifact_lifecycle_incidents"]["status"] != "OK":
        failures.append("artifact_lifecycle_incidents")

    if results["ib_api_handshake"]["returncode"] == 0 and all(r["returncode"] == 0 for r in readiness_results):
        _ensure_execution_reconciliation_day_roots(day)
        execution_reconciliation_cmd = [sys.executable, str(EXECUTION_RECONCILIATION_TOOL), "--day_utc", day]
        results["execution_reconciliation"] = _run(execution_reconciliation_cmd)
        if results["execution_reconciliation"]["returncode"] != 0:
            failures.append("execution_reconciliation")
    else:
        results["execution_reconciliation"] = {
            "cmd": [sys.executable, str(EXECUTION_RECONCILIATION_TOOL), "--day_utc", day],
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "status": "SKIPPED_READINESS_NOT_GREEN",
        }

    nav_path = (GLOBAL_TRUTH_ROOT / "accounting_v2" / "nav" / day / "nav.v2.json").resolve()
    pnl_cmd = [sys.executable, str(PNL_ATTRIBUTION_TOOL), "--day_utc", day]
    if nav_path.exists():
        results["pnl_attribution"] = _run(pnl_cmd)
        if results["pnl_attribution"]["returncode"] != 0:
            failures.append("pnl_attribution")
    else:
        results["pnl_attribution"] = {
            "cmd": pnl_cmd,
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "status": "PENDING_NAV_NOT_AVAILABLE",
            "nav_path": str(nav_path),
        }

    monitoring_refresh_cmd = [sys.executable, str(MONITORING_REFRESH_TOOL), "--day_utc", day, "--truth_root", str(GLOBAL_TRUTH_ROOT)]
    results["monitoring_refresh"] = _run(
        monitoring_refresh_cmd,
        extra_env={"C2_SKIP_AUTHORITATIVE_ORCHESTRATOR_BACKFILL": "YES"},
    )
    if results["monitoring_refresh"]["returncode"] != 0:
        failures.append("monitoring_refresh")

    registry_coherence_cmd = [sys.executable, str(REGISTRY_COHERENCE_TOOL), "--day_utc", day, "--truth_root", str(GLOBAL_TRUTH_ROOT)]
    results["registry_coherence_check"] = _run(registry_coherence_cmd)
    if results["registry_coherence_check"]["returncode"] != 0:
        failures.append("registry_coherence_check")

    artifact_freshness_cmd = [sys.executable, str(ARTIFACT_FRESHNESS_SUMMARY_TOOL), "--day_utc", day, "--truth_root", str(GLOBAL_TRUTH_ROOT)]
    results["operator_artifact_freshness_summary"] = _run(artifact_freshness_cmd)
    if results["operator_artifact_freshness_summary"]["returncode"] != 0:
        failures.append("operator_artifact_freshness_summary")

    report_path, report_status = _write_session_report(
        day_utc=day,
        results=results,
        failures=failures,
        truth_root=GLOBAL_TRUTH_ROOT,
    )
    results["session_readiness_report"] = {"path": str(report_path)}

    startup_proof_cmd = [sys.executable, str(STARTUP_PROOF_VALIDATION_TOOL), "--day_utc", day, "--truth_root", str(GLOBAL_TRUTH_ROOT)]
    results["startup_proof_validation"] = _run(startup_proof_cmd)
    if results["startup_proof_validation"]["returncode"] != 0:
        failures.append("startup_proof_validation")
    trading_day_state_machine_cmd = [
        sys.executable,
        str(TRADING_DAY_STATE_MACHINE_TOOL),
        "--day_utc",
        day,
        "--truth_root",
        str(GLOBAL_TRUTH_ROOT),
    ]
    results["trading_day_state_machine"] = _run(trading_day_state_machine_cmd)
    if results["trading_day_state_machine"]["returncode"] != 0:
        failures.append("trading_day_state_machine")

    report_path, report_status = _write_session_report(day_utc=day, results=results, failures=failures, truth_root=GLOBAL_TRUTH_ROOT)
    results["session_readiness_report"] = {"path": str(report_path)}
    results["operator_summary"] = _write_operator_summary_safe(day_utc=day, truth_root=GLOBAL_TRUTH_ROOT)

    print(
        json.dumps(
            {
                "day_utc": day,
                "truth_root": str(GLOBAL_TRUTH_ROOT),
                "results": results,
                "failures": failures,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report_status in {"OK", "OK_WITH_MONITORING_GAPS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
