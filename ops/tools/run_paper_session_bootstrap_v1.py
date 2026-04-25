#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.paper_session_fact_plane_v1 import (
    atomic_write_validated_json_v1,
    build_fact_dependency_row_v1,
    canonical_paper_session_id_v1,
    now_utc_iso_v1,
    producer_block_v1,
    read_json_object_v1,
    repo_git_sha_v1,
    sha256_file_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_operator_statement_path,
    resolve_paper_capital_seed_path,
    resolve_paper_session_bootstrap_path,
)
from constellation_2.common.paper_session_authority_v1 import (
    write_paper_session_authority_v1,
)
from constellation_2.common.pre_open_materializer_v1 import resolve_pre_open_bundle_path_v1
from constellation_2.common.paper_startup_authorization_convergence_v1 import (
    resolve_paper_startup_authorization_convergence_path,
)
from constellation_2.common.runtime_ledger_v1 import (
    append_runtime_ledger_events_v1,
    projection_over_runtime_ledger_v1,
)
from constellation_2.common.decision_authority_bridge_v1 import (
    resolve_decision_truth_root_bridge_v1,
)
from constellation_2.common.runtime_authority_snapshot_bridge_v1 import (
    load_runtime_path_authority_bridge_v1,
)
from constellation_2.common.session_authority_v1 import resolve_session_authority_target_day_v1
from constellation_2.common.session_promotion_gate_v1 import (
    PROMOTION_STATE_PROMOTED,
    resolve_session_promotion_decision_path_v1,
)
from constellation_2.common.tomorrow_paper_startup_prep_v1 import (
    resolve_continuity_paper_seed_usd_v1,
)
from constellation_2.common.trade_submit_readiness_authority_v1 import resolve_governed_sleeve_truth_bindings
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1


OUTPUT_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_bootstrap.v1.schema.json"
STARTUP_MATERIALIZATION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/startup_materialization.v1.schema.json"
RUNTIME_PYTHON = Path("/home/node/constellation_2_runtime/.venv_c2/bin/python").resolve()
PRIMARY_SLEEVE_ID = "PRIMARY"
PAPER_ENVIRONMENT = "PAPER"

ENSURE_PAPER_CAPITAL_SEED_TOOL = (REPO_ROOT / "ops/tools/ensure_paper_capital_seed_v1.py").resolve()
ENSURE_OPERATOR_STATEMENT_TOOL = (REPO_ROOT / "ops/tools/ensure_cash_ledger_operator_statement_v1.py").resolve()
RUN_GLOBAL_KILL_SWITCH_TOOL = (REPO_ROOT / "ops/tools/run_global_kill_switch_v1.py").resolve()
RUN_ACCOUNTING_NAV_TOOL = (REPO_ROOT / "ops/tools/run_accounting_nav_v2_day_v1.py").resolve()
RUN_CAPITAL_RISK_ENVELOPE_TOOL = (REPO_ROOT / "ops/tools/run_c2_capital_risk_envelope_gate_v2.py").resolve()
RUN_EXPOSURE_NET_TOOL = (REPO_ROOT / "ops/tools/run_exposure_net_day_v1.py").resolve()
RUN_CAPITAL_AUTHORITY_ALLOCATION_TOOL = (REPO_ROOT / "ops/tools/run_capital_authority_allocation_day_v1.py").resolve()
RUN_STARTUP_AUTHORIZATION_CONVERGENCE_TOOL = (
    REPO_ROOT / "ops/tools/run_paper_startup_authorization_convergence_v1.py"
).resolve()
RUN_PRE_OPEN_MATERIALIZER_TOOL = (REPO_ROOT / "ops/tools/run_pre_open_materializer_v1.py").resolve()
RUN_SESSION_AUTHORITY_TOOL = (REPO_ROOT / "ops/tools/run_session_authority_v1.py").resolve()
RUN_DAY_ACTIVATION_TOOL = (REPO_ROOT / "ops/tools/run_day_activation_authority_v1.py").resolve()
RUN_POINTER_ATTEMPT_ALLOC_TOOL = (REPO_ROOT / "ops/tools/run_pointer_attempt_alloc_v1.py").resolve()
RUN_POINTER_APPEND_TOOL = (REPO_ROOT / "ops/tools/run_pointer_append_v1.py").resolve()
RUN_POINTER_HEADS_MATERIALIZE_TOOL = (REPO_ROOT / "ops/tools/run_pointer_heads_materialize_v1.py").resolve()
RUN_POSITIONS_SNAPSHOT_TOOL = (REPO_ROOT / "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v2.py").resolve()
RUN_CASH_LEDGER_SNAPSHOT_TOOL = (
    REPO_ROOT / "constellation_2/phaseF/cash_ledger/run/run_cash_ledger_snapshot_day_v1.py"
).resolve()
RUN_ALLOCATION_SUMMARY_TOOL = (REPO_ROOT / "constellation_2/phaseG/allocation/run/run_allocation_day_v2.py").resolve()
PAPER_DAY_ORCHESTRATOR_SERVICE_PATH = (REPO_ROOT / "ops/systemd/user/c2-paper-day-orchestrator.service").resolve()
GATE_HIERARCHY_POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/GATE_HIERARCHY_V1.json").resolve()
CANONICAL_FIX_THEN_RERUN_RULE = "FIX_EARLIEST_FAILING_PREREQUISITE_THEN_RERUN_CANONICAL_ENTRY"
DO_NOT_RUN_MANUALLY = [
    "ops/tools/run_day_open_attempt_v1.py",
    "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py",
    "ops/tools/run_c2_paper_day_orchestrator_v2.py",
]


def _ensure_dir(path: str) -> Path:
    resolved = Path(str(path).strip()).expanduser().resolve()
    if not resolved.is_absolute() or not resolved.exists() or not resolved.is_dir():
        raise SystemExit(f"FAIL: DIRECTORY_REQUIRED:{resolved}")
    return resolved


def _resolve_ib_account(raw: str) -> str:
    ib_account = str(raw or "").strip()
    if ib_account:
        return ib_account
    return resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)


def _json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    return read_json_object_v1(path)


def _first_nonempty(values: Iterable[Any]) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _extract_reason_codes(payload: Mapping[str, Any]) -> list[str]:
    for key in ("reason_codes", "blocking_reason_codes", "blocking_codes"):
        values = payload.get(key)
        if isinstance(values, list):
            return [str(item).strip() for item in values if str(item).strip()]
    return []


def _reason_codes(raw: Iterable[Any]) -> list[str]:
    return [str(item).strip() for item in raw if str(item).strip()]


def _classify_runtime_blocker(
    *,
    reason_codes: Iterable[str],
    freshness_status: str = "",
    date_binding_status: str = "",
) -> str:
    normalized = [str(code).strip().upper() for code in reason_codes if str(code).strip()]
    freshness = str(freshness_status or "").strip().upper()
    date_binding = str(date_binding_status or "").strip().upper()
    if not normalized and freshness in {"CURRENT", ""} and date_binding in {"MATCH", ""}:
        return "NONE"
    if any(code.endswith("_MISSING") or code == "TARGET_DAY_ARTIFACT_MISSING" for code in normalized):
        return "MISSING_ARTIFACT"
    if freshness == "STALE" or date_binding == "MISMATCH" or any("MISMATCH" in code for code in normalized):
        return "STALE_OR_MISMATCH"
    if any(
        code in {"BROKER_EVENTS_MISSING", "PARTIAL_BUILD"}
        or code.startswith("PRE_OPEN_PRODUCER_FAILED:")
        for code in normalized
    ):
        return "UPSTREAM_DEPENDENCY_FAILURE"
    return "BLOCKED_STATE"


def _runtime_prerequisite_state(
    *,
    ready: bool,
    blocker_class: str,
    freshness_status: str = "",
    date_binding_status: str = "",
    evaluated: bool = True,
) -> str:
    if ready:
        return "READY"
    if not evaluated:
        return "NOT_EVALUATED"
    if blocker_class == "MISSING_ARTIFACT":
        return "MISSING"
    if blocker_class == "STALE_OR_MISMATCH":
        if str(freshness_status or "").strip().upper() == "STALE":
            return "STALE"
        if str(date_binding_status or "").strip().upper() == "MISMATCH":
            return "MISMATCH"
        return "MISMATCH"
    if blocker_class == "UPSTREAM_DEPENDENCY_FAILURE":
        return "FAILED"
    return "BLOCKED"


def _runtime_prerequisite_row(
    *,
    prerequisite_id: str,
    stage: str,
    owner_tool: Path | str,
    consumer_tool: Path | str,
    artifact_path: str,
    ready: bool,
    reason_codes: Iterable[str],
    action_summary: str,
    freshness_status: str = "",
    date_binding_status: str = "",
    evaluated: bool = True,
) -> dict[str, Any]:
    normalized_reason_codes = _reason_codes(reason_codes)
    blocker_class = _classify_runtime_blocker(
        reason_codes=normalized_reason_codes,
        freshness_status=freshness_status,
        date_binding_status=date_binding_status,
    )
    return {
        "prerequisite_id": str(prerequisite_id),
        "stage": str(stage),
        "status": _runtime_prerequisite_state(
            ready=ready,
            blocker_class=blocker_class,
            freshness_status=freshness_status,
            date_binding_status=date_binding_status,
            evaluated=evaluated,
        ),
        "blocker_class": blocker_class,
        "owner_tool": str(owner_tool),
        "consumer_tool": str(consumer_tool),
        "artifact_path": str(artifact_path or ""),
        "reason_codes": normalized_reason_codes,
        "action_summary": str(action_summary),
    }


def _pre_open_checks_by_artifact_id(pre_open_payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for raw_row in pre_open_payload.get("prerequisite_checks") or []:
        if not isinstance(raw_row, Mapping):
            continue
        artifact_id = str(raw_row.get("artifact_id") or "").strip()
        if artifact_id:
            rows[artifact_id] = dict(raw_row)
    return rows


def _runtime_prerequisite_verification(
    *,
    seed_ref: Mapping[str, Any],
    operator_statement_ref: Mapping[str, Any],
    shared_control_state: Mapping[str, Any],
    cash_ref: Mapping[str, Any],
    nav_ref: Mapping[str, Any],
    envelope_ref: Mapping[str, Any],
    allocation_ref: Mapping[str, Any],
    authorization_ref: Mapping[str, Any],
    pre_open_ref: Mapping[str, Any],
    pre_open_payload: Mapping[str, Any],
    admission_ref: Mapping[str, Any],
    promotion_ref: Mapping[str, Any],
    day_activation_summary: Mapping[str, Any],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    canonical_kill_switch = shared_control_state.get("canonical_kill_switch", {})
    sleeve_projection = shared_control_state.get("sleeve_kill_switch_projection", {})
    pre_open_checks = _pre_open_checks_by_artifact_id(pre_open_payload)

    def _pre_open_row(
        *,
        artifact_id: str,
        owner_tool: Path | str,
        action_summary: str,
    ) -> dict[str, Any]:
        raw_row = pre_open_checks.get(artifact_id)
        if raw_row is None:
            return _runtime_prerequisite_row(
                prerequisite_id=artifact_id,
                stage="PRE_OPEN",
                owner_tool=owner_tool,
                consumer_tool=RUN_PRE_OPEN_MATERIALIZER_TOOL,
                artifact_path="",
                ready=False,
                reason_codes=[],
                action_summary=action_summary,
                evaluated=False,
            )
        reason_codes = _reason_codes(
            list(raw_row.get("blocker_codes") or [])
            + ([str(raw_row.get("blocking_reason_code") or "").strip()] if str(raw_row.get("blocking_reason_code") or "").strip() else [])
        )
        ready = str(raw_row.get("result_status") or "").strip().upper() == "PASS"
        return _runtime_prerequisite_row(
            prerequisite_id=artifact_id,
            stage="PRE_OPEN",
            owner_tool=owner_tool,
            consumer_tool=RUN_PRE_OPEN_MATERIALIZER_TOOL,
            artifact_path=str(raw_row.get("canonical_path") or raw_row.get("authority_path") or ""),
            ready=ready,
            reason_codes=reason_codes,
            action_summary=action_summary,
            freshness_status=str(raw_row.get("freshness_status") or ""),
            date_binding_status=str(raw_row.get("date_binding_status") or ""),
            evaluated=True,
        )

    rows.append(
        _runtime_prerequisite_row(
            prerequisite_id="paper_capital_seed_v1",
            stage="STARTUP_MATERIALIZATION",
            owner_tool=ENSURE_PAPER_CAPITAL_SEED_TOOL,
            consumer_tool=Path("ops/tools/run_paper_session_bootstrap_v1.py"),
            artifact_path=str(seed_ref.get("path") or ""),
            ready=bool(seed_ref.get("exists")),
            reason_codes=[] if bool(seed_ref.get("exists")) else ["PAPER_CAPITAL_SEED_MISSING"],
            action_summary="Materialize or refresh the governed paper capital seed through the canonical seed owner, then rerun the canonical entrypoint.",
        )
    )
    rows.append(
        _runtime_prerequisite_row(
            prerequisite_id="operator_statement_v1",
            stage="STARTUP_MATERIALIZATION",
            owner_tool=ENSURE_OPERATOR_STATEMENT_TOOL,
            consumer_tool=Path("ops/tools/run_paper_session_bootstrap_v1.py"),
            artifact_path=str(operator_statement_ref.get("path") or ""),
            ready=bool(operator_statement_ref.get("exists")),
            reason_codes=[] if bool(operator_statement_ref.get("exists")) else ["OPERATOR_STATEMENT_MISSING"],
            action_summary="Materialize or refresh the governed operator statement through the canonical owner, then rerun the canonical entrypoint.",
        )
    )
    rows.append(
        _runtime_prerequisite_row(
            prerequisite_id="canonical_kill_switch_state_v1",
            stage="STARTUP_MATERIALIZATION",
            owner_tool=RUN_GLOBAL_KILL_SWITCH_TOOL,
            consumer_tool=Path("ops/tools/run_paper_session_bootstrap_v1.py"),
            artifact_path=str(canonical_kill_switch.get("path") or ""),
            ready=bool(canonical_kill_switch.get("exists"))
            and str(canonical_kill_switch.get("state") or "").strip().upper() == "INACTIVE",
            reason_codes=(
                ["CANONICAL_KILL_SWITCH_MISSING"]
                if not bool(canonical_kill_switch.get("exists"))
                else ([] if str(canonical_kill_switch.get("state") or "").strip().upper() == "INACTIVE" else ["CANONICAL_KILL_SWITCH_ACTIVE"])
            ),
            action_summary="Refresh canonical kill-switch state through the canonical kill-switch owner; do not hand-edit kill-switch truth.",
        )
    )
    rows.append(
        _runtime_prerequisite_row(
            prerequisite_id="sleeve_kill_switch_projection_v1",
            stage="STARTUP_MATERIALIZATION",
            owner_tool=RUN_GLOBAL_KILL_SWITCH_TOOL,
            consumer_tool=Path("ops/tools/run_paper_session_bootstrap_v1.py"),
            artifact_path=str(sleeve_projection.get("path") or ""),
            ready=bool(sleeve_projection.get("synced_from_canonical")),
            reason_codes=[] if bool(sleeve_projection.get("synced_from_canonical")) else ["KILL_SWITCH_PROJECTION_NOT_SYNCED"],
            action_summary="Refresh the canonical kill-switch projection through the canonical kill-switch owner, then rerun the canonical entrypoint.",
        )
    )
    rows.append(
        _runtime_prerequisite_row(
            prerequisite_id="cash_ledger_snapshot_v1",
            stage="STARTUP_MATERIALIZATION",
            owner_tool=RUN_CASH_LEDGER_SNAPSHOT_TOOL,
            consumer_tool=Path("ops/tools/run_paper_session_bootstrap_v1.py"),
            artifact_path=str(cash_ref.get("path") or ""),
            ready=bool(cash_ref.get("exists")),
            reason_codes=[] if bool(cash_ref.get("exists")) else ["CASH_LEDGER_SNAPSHOT_MISSING"],
            action_summary="Refresh the cash-ledger snapshot through the canonical bootstrap materialization path, then rerun the canonical entrypoint.",
        )
    )
    rows.append(
        _runtime_prerequisite_row(
            prerequisite_id="accounting_nav_v2",
            stage="STARTUP_MATERIALIZATION",
            owner_tool=RUN_ACCOUNTING_NAV_TOOL,
            consumer_tool=Path("ops/tools/run_paper_session_bootstrap_v1.py"),
            artifact_path=str(nav_ref.get("path") or ""),
            ready=bool(nav_ref.get("exists")),
            reason_codes=[] if bool(nav_ref.get("exists")) else ["ACCOUNTING_NAV_MISSING"],
            action_summary="Refresh the accounting NAV through the canonical bootstrap materialization path, then rerun the canonical entrypoint.",
        )
    )
    rows.append(
        _runtime_prerequisite_row(
            prerequisite_id="capital_risk_envelope_v2",
            stage="STARTUP_MATERIALIZATION",
            owner_tool=RUN_CAPITAL_RISK_ENVELOPE_TOOL,
            consumer_tool=Path("ops/tools/run_paper_session_bootstrap_v1.py"),
            artifact_path=str(envelope_ref.get("path") or ""),
            ready=str(envelope_ref.get("status") or "").strip().upper() == "PASS",
            reason_codes=envelope_ref.get("reason_codes") or ([] if str(envelope_ref.get("status") or "").strip().upper() == "PASS" else ["CAPITAL_RISK_ENVELOPE_NOT_PASS"]),
            action_summary="Refresh the capital risk envelope through the canonical bootstrap materialization path, then rerun the canonical entrypoint.",
        )
    )
    rows.append(
        _runtime_prerequisite_row(
            prerequisite_id="capital_authority_allocation_v1",
            stage="STARTUP_MATERIALIZATION",
            owner_tool=RUN_CAPITAL_AUTHORITY_ALLOCATION_TOOL,
            consumer_tool=Path("ops/tools/run_paper_session_bootstrap_v1.py"),
            artifact_path=str(allocation_ref.get("path") or ""),
            ready=bool(allocation_ref.get("exists")),
            reason_codes=[] if bool(allocation_ref.get("exists")) else ["CAPITAL_AUTHORITY_ALLOCATION_MISSING"],
            action_summary="Materialize the canonical capital-authority allocation through its owner, then rerun the canonical entrypoint.",
        )
    )
    rows.append(
        _runtime_prerequisite_row(
            prerequisite_id="authorization_gate_verdict_v1",
            stage="STARTUP_MATERIALIZATION",
            owner_tool=RUN_STARTUP_AUTHORIZATION_CONVERGENCE_TOOL,
            consumer_tool=Path("ops/tools/run_paper_session_bootstrap_v1.py"),
            artifact_path=str(authorization_ref.get("path") or ""),
            ready=str(authorization_ref.get("status") or "").strip().upper() in {"PASS", "BOOTSTRAP_PASS"},
            reason_codes=authorization_ref.get("reason_codes")
            or ([] if str(authorization_ref.get("status") or "").strip().upper() in {"PASS", "BOOTSTRAP_PASS"} else ["AUTHORIZATION_GATE_NOT_READY"]),
            action_summary="Inspect authorization_gate_verdict_v1 and its upstream replay-certification inputs through the canonical startup authorization convergence owner, then rerun the canonical entrypoint.",
        )
    )
    rows.append(
        _pre_open_row(
            artifact_id="ib_api_handshake_latest_pointer_v1",
            owner_tool=REPO_ROOT / "ops/tools/run_ib_api_handshake_spine_v1.py",
            action_summary="Refresh the canonical IB handshake pointer through the IB handshake spine owner, then rerun the canonical entrypoint.",
        )
    )
    rows.append(
        _pre_open_row(
            artifact_id="ib_api_handshake_v1",
            owner_tool=REPO_ROOT / "ops/tools/run_ib_api_handshake_spine_v1.py",
            action_summary="Refresh same-day IB handshake truth through the IB handshake spine owner, then rerun the canonical entrypoint.",
        )
    )
    rows.append(
        _pre_open_row(
            artifact_id="primary_scoped_canonical_authority_head_v1",
            owner_tool=REPO_ROOT / "ops/tools/run_pointer_heads_materialize_v1.py",
            action_summary="Refresh the primary scoped canonical authority head through the pointer-head owner, then rerun the canonical entrypoint.",
        )
    )
    rows.append(
        _runtime_prerequisite_row(
            prerequisite_id="pre_open_bundle_v1",
            stage="PRE_OPEN",
            owner_tool=RUN_PRE_OPEN_MATERIALIZER_TOOL,
            consumer_tool=RUN_SESSION_AUTHORITY_TOOL,
            artifact_path=str(pre_open_ref.get("path") or ""),
            ready=str(pre_open_ref.get("materialization_state") or pre_open_ref.get("status") or "").strip().upper() == "COMPLETE",
            reason_codes=pre_open_ref.get("blocking_reason_codes") or pre_open_ref.get("reason_codes") or [],
            action_summary="Inspect the canonical pre-open bundle and fix its earliest failing upstream prerequisite before rerunning the canonical entrypoint.",
        )
    )
    rows.append(
        _runtime_prerequisite_row(
            prerequisite_id="target_day_admission_v1",
            stage="SESSION_ADMISSION",
            owner_tool=RUN_SESSION_AUTHORITY_TOOL,
            consumer_tool=Path("ops/tools/run_paper_session_bootstrap_v1.py"),
            artifact_path=str(admission_ref.get("path") or ""),
            ready=str(admission_ref.get("admission_status") or "").strip().upper() == "ADMIT",
            reason_codes=admission_ref.get("reason_codes") or [],
            action_summary="Inspect target-day admission/build truth through Session Authority, correct the earliest binding blocker, then rerun the canonical entrypoint.",
        )
    )
    rows.append(
        _runtime_prerequisite_row(
            prerequisite_id="session_promotion_decision_v1",
            stage="PROMOTION",
            owner_tool=RUN_SESSION_AUTHORITY_TOOL,
            consumer_tool=Path("ops/tools/run_paper_session_bootstrap_v1.py"),
            artifact_path=str(promotion_ref.get("path") or ""),
            ready=str(promotion_ref.get("promotion_state") or promotion_ref.get("status") or "").strip().upper()
            == PROMOTION_STATE_PROMOTED,
            reason_codes=promotion_ref.get("blocked_reason_codes") or promotion_ref.get("reason_codes") or [],
            action_summary="Inspect the session promotion decision through Session Authority, fix the earliest promoted-state blocker, then rerun the canonical entrypoint.",
        )
    )

    day_activation_reason_codes: list[str] = []
    first_real_blocker = day_activation_summary.get("first_real_blocker")
    if isinstance(first_real_blocker, Mapping):
        dependency_id = str(first_real_blocker.get("dependency_id") or "").strip()
        if dependency_id:
            day_activation_reason_codes = [dependency_id]
    if not day_activation_reason_codes and str(day_activation_summary.get("closure_status") or "").strip().upper() != "COMPLETE":
        day_activation_reason_codes = ["DAY_ACTIVATION_NOT_READY"]
    rows.append(
        _runtime_prerequisite_row(
            prerequisite_id="day_activation_readiness_v1",
            stage="ACTIVATION",
            owner_tool=RUN_DAY_ACTIVATION_TOOL,
            consumer_tool=Path("ops/tools/run_paper_session_bootstrap_v1.py"),
            artifact_path=str(day_activation_summary.get("package_path") or day_activation_summary.get("build_path") or ""),
            ready=str(day_activation_summary.get("closure_status") or "").strip().upper() == "COMPLETE",
            reason_codes=day_activation_reason_codes,
            action_summary="Inspect day-activation dependency truth through the day-activation authority, fix the earliest failing dependency, then rerun the canonical entrypoint.",
            evaluated=bool(day_activation_summary),
        )
    )

    earliest_failing = next(
        (
            row
            for row in rows
            if str(row.get("status") or "").strip().upper() not in {"READY", "NOT_EVALUATED"}
        ),
        None,
    )
    return {
        "status": "READY" if earliest_failing is None else "BLOCKED",
        "stage": "NONE" if earliest_failing is None else str(earliest_failing.get("stage") or ""),
        "fix_then_rerun_rule": CANONICAL_FIX_THEN_RERUN_RULE,
        "do_not_run_manually": list(DO_NOT_RUN_MANUALLY),
        "prerequisites": rows,
        "earliest_failing_prerequisite": earliest_failing,
    }


def _artifact_ref(
    path: Path,
    *,
    status_fields: tuple[str, ...] = (),
    extra_fields: tuple[str, ...] = (),
) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        missing_ref: dict[str, Any] = {
            "path": str(path),
            "exists": False,
            "sha256": "",
            "status": "",
            "reason_codes": [],
        }
        for field in extra_fields:
            missing_ref[field] = None
        return missing_ref
    payload = _json_or_empty(path)
    ref: dict[str, Any] = {
        "path": str(path),
        "exists": True,
        "sha256": sha256_file_v1(path),
        "status": _first_nonempty(payload.get(field) for field in status_fields),
        "reason_codes": _extract_reason_codes(payload),
    }
    for field in extra_fields:
        ref[field] = payload.get(field)
    return ref


def _ref_path(ref: Mapping[str, Any]) -> Path | None:
    path_text = str(ref.get("path") or "").strip()
    if not path_text:
        return None
    return Path(path_text).resolve()


def _run_command(cmd: list[str], *, env: dict[str, str] | None = None) -> dict[str, Any]:
    run_env = os.environ.copy()
    if env:
        run_env.update(env)
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        env=run_env,
    )
    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    parsed_stdout: dict[str, Any] = {}
    if stdout:
        try:
            obj = json.loads(stdout)
        except Exception:
            obj = {}
        if isinstance(obj, dict):
            parsed_stdout = obj
    return {
        "cmd": list(cmd),
        "return_code": int(proc.returncode),
        "stdout": stdout,
        "stderr": stderr,
        "json": parsed_stdout,
    }


def _refresh_pointer_truth_root_for_authorization(
    *,
    day_utc: str,
    authorization_path: Path,
    pointer_truth_root: Path,
    producer_git_sha: str,
    scope_label: str,
) -> dict[str, Any]:
    if not authorization_path.exists() or not authorization_path.is_file():
        return {
            "status": "ERROR",
            "reason_codes": [f"PRIMARY_{scope_label}_AUTHORIZATION_VERDICT_MISSING"],
            "pointer_truth_root": str(pointer_truth_root),
            "authorization_path": str(authorization_path),
        }

    cfg_hash = sha256_file_v1(PAPER_DAY_ORCHESTRATOR_SERVICE_PATH)
    policy_hash = sha256_file_v1(GATE_HIERARCHY_POLICY_PATH)
    attempt_result = _run_command(
        [
            sys.executable,
            str(RUN_POINTER_ATTEMPT_ALLOC_TOOL),
            "--day_utc",
            day_utc,
            "--mode",
            "PAPER",
            "--orchestrator_config_hash",
            cfg_hash,
            "--git_sha",
            producer_git_sha,
            "--truth_root",
            str(pointer_truth_root),
        ]
    )
    if int(attempt_result.get("return_code") or 0) != 0:
        return {
            "status": "ERROR",
            "reason_codes": [f"PRIMARY_{scope_label}_POINTER_ATTEMPT_ALLOC_FAILED"],
            "pointer_truth_root": str(pointer_truth_root),
            "authorization_path": str(authorization_path),
            "attempt_result": attempt_result,
        }
    attempt_payload = attempt_result.get("json") if isinstance(attempt_result.get("json"), Mapping) else {}
    attempt_id = str(attempt_payload.get("attempt_id") or "").strip()
    attempt_seq = int(attempt_payload.get("attempt_seq") or 0)
    if not attempt_id or attempt_seq <= 0:
        return {
            "status": "ERROR",
            "reason_codes": [f"PRIMARY_{scope_label}_POINTER_ATTEMPT_ALLOC_INVALID"],
            "pointer_truth_root": str(pointer_truth_root),
            "authorization_path": str(authorization_path),
            "attempt_result": attempt_result,
        }

    append_result = _run_command(
        [
            sys.executable,
            str(RUN_POINTER_APPEND_TOOL),
            "--day_utc",
            day_utc,
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
            f"{day_utc}T00:00:00Z",
            "--points_to",
            str(authorization_path),
            "--git_sha",
            producer_git_sha,
            "--truth_root",
            str(pointer_truth_root),
        ]
    )
    if int(append_result.get("return_code") or 0) != 0:
        return {
            "status": "ERROR",
            "reason_codes": [f"PRIMARY_{scope_label}_POINTER_APPEND_FAILED"],
            "pointer_truth_root": str(pointer_truth_root),
            "authorization_path": str(authorization_path),
            "attempt_result": attempt_result,
            "append_result": append_result,
        }

    heads_result = _run_command(
        [
            sys.executable,
            str(RUN_POINTER_HEADS_MATERIALIZE_TOOL),
            "--fail_if_no_authority_head",
            "YES",
            "--expected_day_utc",
            day_utc,
            "--truth_root",
            str(pointer_truth_root),
        ]
    )
    if int(heads_result.get("return_code") or 0) != 0:
        return {
            "status": "ERROR",
            "reason_codes": [f"PRIMARY_{scope_label}_POINTER_HEADS_MATERIALIZE_FAILED"],
            "pointer_truth_root": str(pointer_truth_root),
            "authorization_path": str(authorization_path),
            "attempt_result": attempt_result,
            "append_result": append_result,
            "heads_result": heads_result,
        }

    head_path = (pointer_truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()
    head_ref = _report_ref(path=head_path, status_fields=("status",), extra_fields=("day_utc", "points_to"))
    refreshed_for_day = bool(head_ref.get("exists")) and str(head_ref.get("day_utc") or "").strip() == day_utc
    reason_codes = [] if refreshed_for_day else [f"PRIMARY_{scope_label}_POINTER_HEAD_DAY_MISMATCH"]
    return {
        "status": "OK" if refreshed_for_day else "ERROR",
        "reason_codes": reason_codes,
        "pointer_truth_root": str(pointer_truth_root),
        "authorization_path": str(authorization_path),
        "head_path": str(head_path),
        "head_ref": head_ref,
        "attempt_result": attempt_result,
        "append_result": append_result,
        "heads_result": heads_result,
    }


def _refresh_primary_authority_pointers(
    *,
    day_utc: str,
    canonical_truth_root: Path,
    sleeve_truth_root: Path,
    authorization_path: Path,
    producer_git_sha: str,
) -> dict[str, Any]:
    canonical_refresh = _refresh_pointer_truth_root_for_authorization(
        day_utc=day_utc,
        authorization_path=authorization_path,
        pointer_truth_root=canonical_truth_root,
        producer_git_sha=producer_git_sha,
        scope_label="CANONICAL",
    )
    scoped_refresh = _refresh_pointer_truth_root_for_authorization(
        day_utc=day_utc,
        authorization_path=authorization_path,
        pointer_truth_root=sleeve_truth_root,
        producer_git_sha=producer_git_sha,
        scope_label="SCOPED",
    )
    reason_codes = sorted(
        set(
            str(code).strip()
            for row in (canonical_refresh, scoped_refresh)
            for code in (row.get("reason_codes") or [])
            if str(code).strip()
        )
    )
    return {
        "status": "OK" if not reason_codes else "ERROR",
        "reason_codes": reason_codes,
        "canonical_refresh": canonical_refresh,
        "scoped_refresh": scoped_refresh,
    }


def _step_result(
    *,
    step_id: str,
    owner_tool: Path,
    scope: str,
    path: Path | None,
    before_exists: bool,
    run_result: Mapping[str, Any] | None,
    status_fields: tuple[str, ...],
    materialize: bool,
) -> dict[str, Any]:
    if not materialize:
        action = "SKIPPED"
        status = "SKIPPED_MATERIALIZE_DISABLED"
    elif run_result is None:
        action = "SKIPPED"
        status = "SKIPPED_PREREQUISITE_BLOCKED"
    else:
        after_exists = bool(path is not None and path.exists() and path.is_file())
        if int(run_result.get("return_code") or 0) == 0 and after_exists:
            action = "REUSED" if before_exists else "MATERIALIZED"
            status = _artifact_ref(path, status_fields=status_fields).get("status") if path is not None else "OK"
        elif after_exists:
            action = "FAILED"
            status = _artifact_ref(path, status_fields=status_fields).get("status") if path is not None else "FAILED"
        else:
            action = "FAILED"
            status = "MISSING_OUTPUT"
    return {
        "step_id": step_id,
        "scope": scope,
        "owner_tool": str(owner_tool),
        "action": action,
        "return_code": None if run_result is None else run_result.get("return_code"),
        "path": "" if path is None else str(path),
        "status": str(status or ""),
        "stdout": "" if run_result is None else str(run_result.get("stdout") or ""),
        "stderr": "" if run_result is None else str(run_result.get("stderr") or ""),
    }


def _kill_switch_paths(*, canonical_truth_root: Path, sleeve_truth_root: Path, day_utc: str) -> tuple[Path, Path]:
    rel = Path("risk_v1") / "kill_switch_v1" / day_utc / "global_kill_switch_state.v1.json"
    return (canonical_truth_root / rel).resolve(), (sleeve_truth_root / rel).resolve()


def _shared_control_state(
    *,
    canonical_truth_root: Path,
    sleeve_truth_root: Path,
    day_utc: str,
) -> dict[str, Any]:
    canonical_path, sleeve_path = _kill_switch_paths(
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        day_utc=day_utc,
    )
    canonical_payload = _json_or_empty(canonical_path)
    sleeve_payload = _json_or_empty(sleeve_path)
    canonical_ref = _artifact_ref(
        canonical_path,
        status_fields=("status", "state"),
        extra_fields=("state", "state_sha256", "day_utc"),
    )
    sleeve_ref = _artifact_ref(
        sleeve_path,
        status_fields=("status", "state"),
        extra_fields=("state", "state_sha256", "day_utc"),
    )
    synced = (
        canonical_ref["exists"]
        and sleeve_ref["exists"]
        and canonical_ref["sha256"]
        and canonical_ref["sha256"] == sleeve_ref["sha256"]
    )
    canonical_input_manifest_sha256 = ""
    sleeve_input_manifest_sha256 = ""
    if isinstance(canonical_payload.get("input_manifest"), list) and canonical_payload["input_manifest"]:
        canonical_input_manifest_sha256 = str((canonical_payload["input_manifest"][0] or {}).get("sha256") or "").strip()
    if isinstance(sleeve_payload.get("input_manifest"), list) and sleeve_payload["input_manifest"]:
        sleeve_input_manifest_sha256 = str((sleeve_payload["input_manifest"][0] or {}).get("sha256") or "").strip()
    return {
        "canonical_kill_switch": {
            **canonical_ref,
            "input_manifest_sha256": canonical_input_manifest_sha256,
        },
        "sleeve_kill_switch_projection": {
            **sleeve_ref,
            "input_manifest_sha256": sleeve_input_manifest_sha256,
            "synced_from_canonical": synced,
        },
    }


def _pointer_refresh_result(*, canonical_truth_root: Path, day_utc: str) -> dict[str, Any]:
    path = (canonical_truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()
    ref = _artifact_ref(path, status_fields=("status",), extra_fields=("day_utc", "points_to"))
    payload = _json_or_empty(path)
    return {
        **ref,
        "refreshed_for_day": bool(ref["exists"] and str(payload.get("day_utc") or "").strip() == day_utc),
    }


def _existing_day_activation_result(
    *,
    canonical_truth_root: Path,
    sleeve_truth_root: Path,
    day_utc: str,
) -> dict[str, Any]:
    build_candidates = sorted(
        (canonical_truth_root / "reports" / "day_activation_build_v1" / day_utc).glob("*/day_activation_build.v1.json")
    )
    package_candidates = sorted(
        (sleeve_truth_root / "day_activation_package_v1" / day_utc).glob("*/day_activation_package.v1.json")
    )
    if not build_candidates and not package_candidates:
        return {}
    build_path = build_candidates[-1].resolve() if build_candidates else None
    package_path = package_candidates[-1].resolve() if package_candidates else None
    build_payload = _json_or_empty(build_path) if build_path is not None else {}
    return {
        "build_path": str(build_path or build_payload.get("build_path") or ""),
        "package_path": str(package_path or build_payload.get("package_path") or ""),
        "closure_status": str(build_payload.get("closure_status") or ""),
        "first_real_blocker": build_payload.get("first_real_blocker"),
    }


def _bool_materialize(raw: str) -> bool:
    value = str(raw or "").strip().upper()
    if value not in {"YES", "NO"}:
        raise SystemExit(f"FAIL: INVALID_BOOLEAN_CHOICE:{raw}")
    return value == "YES"


def _prerequisite_check(
    *,
    check_id: str,
    passed: bool,
    details: str,
    ref_path: str = "",
    reason_codes: Iterable[str] = (),
) -> dict[str, Any]:
    return {
        "check_id": str(check_id),
        "pass": bool(passed),
        "details": str(details),
        "ref_path": str(ref_path),
        "reason_codes": [str(code).strip() for code in reason_codes if str(code).strip()],
    }


def _group_status(*, passing_label: str, failing_label: str, checks: list[dict[str, Any]]) -> dict[str, Any]:
    unmet: list[str] = []
    for check in checks:
        if check["pass"]:
            continue
        reason_codes = [str(code).strip() for code in check.get("reason_codes") or [] if str(code).strip()]
        if reason_codes:
            unmet.extend(reason_codes)
        else:
            unmet.append(str(check["check_id"]))
    return {
        "status": passing_label if not unmet else failing_label,
        "unmet": list(dict.fromkeys(unmet)),
        "checks": checks,
    }


def _required_prerequisites_status(
    *,
    seed_ref: Mapping[str, Any],
    operator_statement_ref: Mapping[str, Any],
    pre_open_ref: Mapping[str, Any],
    shared_control_state: Mapping[str, Any],
) -> dict[str, Any]:
    canonical_kill_switch = shared_control_state.get("canonical_kill_switch", {})
    pre_open_status = str(pre_open_ref.get("materialization_state") or pre_open_ref.get("status") or "").strip().upper()
    pre_open_reason_codes = [
        str(code).strip()
        for code in (pre_open_ref.get("blocking_reason_codes") or pre_open_ref.get("reason_codes") or [])
        if str(code).strip()
    ]
    if not pre_open_reason_codes and pre_open_status != "COMPLETE":
        pre_open_reason_codes = ["PRE_OPEN_BUNDLE_INCOMPLETE"]

    checks = [
        _prerequisite_check(
            check_id="PAPER_CAPITAL_SEED_READY",
            passed=bool(seed_ref.get("exists")),
            details=str(seed_ref.get("seed_mode") or "MISSING"),
            ref_path=str(seed_ref.get("path") or ""),
            reason_codes=["PAPER_CAPITAL_SEED_MISSING"],
        ),
        _prerequisite_check(
            check_id="OPERATOR_STATEMENT_READY",
            passed=bool(operator_statement_ref.get("exists")),
            details=str(operator_statement_ref.get("observed_at_utc") or "MISSING"),
            ref_path=str(operator_statement_ref.get("path") or ""),
            reason_codes=["OPERATOR_STATEMENT_MISSING"],
        ),
        _prerequisite_check(
            check_id="PRE_OPEN_BUNDLE_COMPLETE",
            passed=pre_open_status == "COMPLETE",
            details=pre_open_status or "MISSING",
            ref_path=str(pre_open_ref.get("path") or ""),
            reason_codes=pre_open_reason_codes,
        ),
        _prerequisite_check(
            check_id="CANONICAL_KILL_SWITCH_PRESENT",
            passed=bool(canonical_kill_switch.get("exists")),
            details=str(canonical_kill_switch.get("state") or "MISSING"),
            ref_path=str(canonical_kill_switch.get("path") or ""),
            reason_codes=["CANONICAL_KILL_SWITCH_MISSING"],
        ),
        _prerequisite_check(
            check_id="CANONICAL_KILL_SWITCH_INACTIVE",
            passed=(not bool(canonical_kill_switch.get("exists")))
            or str(canonical_kill_switch.get("state") or "").strip().upper() == "INACTIVE",
            details=str(canonical_kill_switch.get("state") or "UNKNOWN"),
            ref_path=str(canonical_kill_switch.get("path") or ""),
            reason_codes=["CANONICAL_KILL_SWITCH_ACTIVE"],
        ),
    ]
    return _group_status(passing_label="PASS", failing_label="FAIL", checks=checks)


def _startup_materialization_phase_status(
    *,
    seed_ref: Mapping[str, Any],
    operator_statement_ref: Mapping[str, Any],
    cash_ref: Mapping[str, Any],
    nav_ref: Mapping[str, Any],
    envelope_ref: Mapping[str, Any],
    allocation_ref: Mapping[str, Any],
    startup_authorization_convergence_ref: Mapping[str, Any],
    authorization_ref: Mapping[str, Any],
) -> dict[str, Any]:
    convergence_status = str(startup_authorization_convergence_ref.get("status") or "").strip().upper()
    checks = [
        _prerequisite_check(
            check_id="PAPER_CAPITAL_SEED_MATERIALIZED",
            passed=bool(seed_ref.get("exists")),
            details=str(seed_ref.get("seed_mode") or "MISSING"),
            ref_path=str(seed_ref.get("path") or ""),
            reason_codes=["PAPER_CAPITAL_SEED_MISSING"],
        ),
        _prerequisite_check(
            check_id="OPERATOR_STATEMENT_MATERIALIZED",
            passed=bool(operator_statement_ref.get("exists")),
            details=str(operator_statement_ref.get("observed_at_utc") or "MISSING"),
            ref_path=str(operator_statement_ref.get("path") or ""),
            reason_codes=["OPERATOR_STATEMENT_MISSING"],
        ),
        _prerequisite_check(
            check_id="CASH_LEDGER_SNAPSHOT_MATERIALIZED",
            passed=bool(cash_ref.get("exists")),
            details=str(cash_ref.get("status") or "MISSING"),
            ref_path=str(cash_ref.get("path") or ""),
            reason_codes=["CASH_LEDGER_SNAPSHOT_MISSING"],
        ),
        _prerequisite_check(
            check_id="ACCOUNTING_NAV_MATERIALIZED",
            passed=bool(nav_ref.get("exists")),
            details=str(nav_ref.get("status") or "MISSING"),
            ref_path=str(nav_ref.get("path") or ""),
            reason_codes=["ACCOUNTING_NAV_MISSING"],
        ),
        _prerequisite_check(
            check_id="CAPITAL_RISK_ENVELOPE_MATERIALIZED",
            passed=bool(envelope_ref.get("exists")),
            details=str(envelope_ref.get("status") or "MISSING"),
            ref_path=str(envelope_ref.get("path") or ""),
            reason_codes=["CAPITAL_RISK_ENVELOPE_MISSING"],
        ),
        _prerequisite_check(
            check_id="CAPITAL_AUTHORITY_ALLOCATION_MATERIALIZED",
            passed=bool(allocation_ref.get("exists")),
            details=str(allocation_ref.get("status") or "MISSING"),
            ref_path=str(allocation_ref.get("path") or ""),
            reason_codes=["CAPITAL_AUTHORITY_ALLOCATION_MISSING"],
        ),
        _prerequisite_check(
            check_id="STARTUP_AUTH_CONVERGENCE_COMPLETE",
            passed=convergence_status == "SUCCESS",
            details=convergence_status or "MISSING",
            ref_path=str(startup_authorization_convergence_ref.get("path") or ""),
            reason_codes=startup_authorization_convergence_ref.get("reason_codes")
            or startup_authorization_convergence_ref.get("blocker_chain")
            or ["STARTUP_AUTH_CONVERGENCE_BLOCKED"],
        ),
        _prerequisite_check(
            check_id="AUTHORIZATION_GATE_ARTIFACT_MATERIALIZED",
            passed=bool(authorization_ref.get("exists")),
            details=str(authorization_ref.get("status") or "MISSING"),
            ref_path=str(authorization_ref.get("path") or ""),
            reason_codes=["AUTHORIZATION_GATE_ARTIFACT_MISSING"],
        ),
    ]
    return _group_status(passing_label="COMPLETE", failing_label="BLOCKED", checks=checks)


def _evaluation_phase_status(
    *,
    shared_control_state: Mapping[str, Any],
    envelope_ref: Mapping[str, Any],
    authorization_ref: Mapping[str, Any],
    admission_ref: Mapping[str, Any],
    include_admission: bool = True,
) -> dict[str, Any]:
    canonical_kill_switch = shared_control_state.get("canonical_kill_switch", {})
    sleeve_projection = shared_control_state.get("sleeve_kill_switch_projection", {})
    auth_status = str(authorization_ref.get("status") or "").strip().upper()
    admission_status = str(admission_ref.get("admission_status") or "").strip().upper()
    checks = [
        _prerequisite_check(
            check_id="CANONICAL_KILL_SWITCH_PRESENT",
            passed=bool(canonical_kill_switch.get("exists")),
            details=str(canonical_kill_switch.get("state") or "MISSING"),
            ref_path=str(canonical_kill_switch.get("path") or ""),
            reason_codes=["CANONICAL_KILL_SWITCH_MISSING"],
        ),
        _prerequisite_check(
            check_id="CANONICAL_KILL_SWITCH_INACTIVE",
            passed=str(canonical_kill_switch.get("state") or "").strip().upper() == "INACTIVE",
            details=str(canonical_kill_switch.get("state") or "UNKNOWN"),
            ref_path=str(canonical_kill_switch.get("path") or ""),
            reason_codes=["CANONICAL_KILL_SWITCH_ACTIVE"],
        ),
        _prerequisite_check(
            check_id="KILL_SWITCH_PROJECTION_SYNCED",
            passed=bool(sleeve_projection.get("synced_from_canonical")),
            details="synced_from_canonical"
            if sleeve_projection.get("synced_from_canonical")
            else "projection diverged from canonical control state",
            ref_path=str(sleeve_projection.get("path") or ""),
            reason_codes=["KILL_SWITCH_PROJECTION_NOT_SYNCED"],
        ),
        _prerequisite_check(
            check_id="CAPITAL_RISK_ENVELOPE_PASS",
            passed=str(envelope_ref.get("status") or "").strip().upper() == "PASS",
            details=str(envelope_ref.get("status") or "MISSING"),
            ref_path=str(envelope_ref.get("path") or ""),
            reason_codes=envelope_ref.get("reason_codes") or ["CAPITAL_RISK_ENVELOPE_NOT_PASS"],
        ),
        _prerequisite_check(
            check_id="AUTHORIZATION_READY",
            passed=auth_status in {"PASS", "BOOTSTRAP_PASS"},
            details=auth_status or "MISSING",
            ref_path=str(authorization_ref.get("path") or ""),
            reason_codes=authorization_ref.get("reason_codes") or ["AUTHORIZATION_GATE_NOT_READY"],
        ),
    ]
    if include_admission:
        checks.append(
            _prerequisite_check(
                check_id="TARGET_DAY_ADMISSION_ADMIT",
                passed=admission_status == "ADMIT",
                details=admission_status or "MISSING",
                ref_path=str(admission_ref.get("path") or ""),
                reason_codes=admission_ref.get("reason_codes") or ["TARGET_DAY_ADMISSION_NOT_ADMIT"],
            )
        )
    return _group_status(passing_label="READY", failing_label="BLOCKED", checks=checks)


def _activation_phase_status(*, day_activation_result: Mapping[str, Any]) -> dict[str, Any]:
    day_activation_status = str(day_activation_result.get("closure_status") or "").strip().upper()
    first_real_blocker = day_activation_result.get("first_real_blocker")
    reason_codes = ["DAY_ACTIVATION_NOT_READY"]
    if isinstance(first_real_blocker, dict):
        dependency_id = str(first_real_blocker.get("dependency_id") or "").strip()
        if dependency_id:
            reason_codes = [dependency_id]
    checks = [
        _prerequisite_check(
            check_id="DAY_ACTIVATION_READY",
            passed=day_activation_status == "COMPLETE",
            details=day_activation_status or "MISSING",
            ref_path=str(day_activation_result.get("build_path") or day_activation_result.get("package_path") or ""),
            reason_codes=reason_codes,
        ),
    ]
    return _group_status(passing_label="READY", failing_label="BLOCKED", checks=checks)


def _root_blocker_class(
    *,
    startup_materialization_phase: Mapping[str, Any],
    evaluation_phase: Mapping[str, Any],
    activation_phase: Mapping[str, Any],
) -> str:
    if str(startup_materialization_phase.get("status") or "").strip().upper() != "COMPLETE":
        return "STARTUP_MATERIALIZATION"
    if str(evaluation_phase.get("status") or "").strip().upper() != "READY":
        return "EVALUATION_SAFETY"
    if str(activation_phase.get("status") or "").strip().upper() != "READY":
        return "ACTIVATION"
    return "NONE"


def _paper_advisory_prerequisites_status(
    *,
    startup_materialization_ref: Mapping[str, Any],
    pointer_result: Mapping[str, Any],
) -> dict[str, Any]:
    checks = [
        _prerequisite_check(
            check_id="STARTUP_MATERIALIZATION_PRESENT",
            passed=bool(startup_materialization_ref.get("exists")),
            details=str(startup_materialization_ref.get("status") or "MISSING"),
            ref_path=str(startup_materialization_ref.get("path") or ""),
            reason_codes=["STARTUP_MATERIALIZATION_NOT_PRESENT"],
        ),
        _prerequisite_check(
            check_id="CANONICAL_AUTHORITY_HEAD_REFRESHED_FOR_DAY",
            passed=bool(pointer_result.get("refreshed_for_day")),
            details=str(pointer_result.get("day_utc") or "UNSET"),
            ref_path=str(pointer_result.get("path") or ""),
            reason_codes=["CANONICAL_AUTHORITY_HEAD_DAY_MISMATCH"],
        ),
    ]
    return _group_status(passing_label="PASS", failing_label="ADVISORY", checks=checks)


def _build_paper_session_authority_payload(
    *,
    day_utc: str,
    produced_utc: str,
    mode: str,
    bootstrap_report_path: Path,
    seed_ref: Mapping[str, Any],
    operator_statement_ref: Mapping[str, Any],
    pre_open_ref: Mapping[str, Any],
    shared_control_state: Mapping[str, Any],
    required_prerequisites_status: Mapping[str, Any],
    paper_advisory_prerequisites_status: Mapping[str, Any],
    production_only_prerequisites_status: Mapping[str, Any],
    submission_authorized: bool,
) -> dict[str, Any]:
    safety_checks: list[dict[str, Any]] = []
    blocking_reason_codes: list[str] = []
    blocking_reason_details: list[dict[str, Any]] = []
    for check in required_prerequisites_status.get("checks") or []:
        passed = bool(check.get("pass") is True)
        reason_codes = [str(code).strip() for code in (check.get("reason_codes") or []) if str(code).strip()]
        reason_code = "" if passed else (reason_codes[0] if reason_codes else str(check.get("check_id") or "UNKNOWN_BLOCKER"))
        summary = str(check.get("details") or "UNKNOWN")
        artifact_path = str(check.get("ref_path") or "")
        safety_checks.append(
            {
                "check_id": str(check.get("check_id") or ""),
                "status": "PASS" if passed else "FAIL",
                "reason_code": reason_code,
                "summary": summary,
                "artifact_path": artifact_path,
            }
        )
        if passed:
            continue
        codes_for_detail = reason_codes or [reason_code]
        for code in codes_for_detail:
            if code:
                blocking_reason_codes.append(code)
                blocking_reason_details.append(
                    {
                        "reason_code": code,
                        "blocker_class": "SAFETY_CRITICAL",
                        "check_id": str(check.get("check_id") or ""),
                        "summary": summary,
                        "artifact_path": artifact_path,
                    }
                )

    advisory_checks: list[dict[str, Any]] = []
    for status_group in (paper_advisory_prerequisites_status, production_only_prerequisites_status):
        for check in status_group.get("checks") or []:
            if bool(check.get("pass") is True):
                continue
            reason_codes = [str(code).strip() for code in (check.get("reason_codes") or []) if str(code).strip()]
            advisory_checks.append(
                {
                    "check_id": str(check.get("check_id") or ""),
                    "status": "ADVISORY",
                    "reason_code": reason_codes[0] if reason_codes else str(check.get("check_id") or ""),
                    "summary": str(check.get("details") or "UNKNOWN"),
                    "artifact_path": str(check.get("ref_path") or ""),
                }
            )

    normalized_blockers = list(dict.fromkeys(blocking_reason_codes))
    authority_status = "GRANTED" if not normalized_blockers else "DENIED"
    paper_open_allowed = authority_status == "GRANTED"
    degraded_mode = bool(paper_open_allowed and advisory_checks)

    return {
        "schema_id": "paper_session_authority",
        "schema_version": "v1",
        "authority_scope": "CANONICAL_PAPER_SESSION_AUTHORITY",
        "day_utc": str(day_utc),
        "produced_utc": str(produced_utc),
        "mode": str(mode).strip().upper(),
        "authority_status": authority_status,
        "paper_open_allowed": paper_open_allowed,
        "blocking_reason_codes": normalized_blockers,
        "blocking_reason_details": blocking_reason_details,
        "safety_checks": safety_checks,
        "advisory_checks": advisory_checks,
        "degraded_mode": degraded_mode,
        "submission_authorized": bool(submission_authorized),
        "upstream_refs": {
            "paper_session_bootstrap_v1": str(bootstrap_report_path.resolve()),
            "paper_capital_seed": str(seed_ref.get("path") or ""),
            "operator_statement": str(operator_statement_ref.get("path") or ""),
            "pre_open_bundle_v1": str(pre_open_ref.get("path") or ""),
            "canonical_kill_switch_v1": str((shared_control_state.get("canonical_kill_switch") or {}).get("path") or ""),
        },
        "producer": producer_block_v1(module="ops/tools/run_paper_session_bootstrap_v1.py"),
    }


def _startup_materialization_status_code(blocker_chain: Iterable[str]) -> str:
    normalized = [str(code).strip().upper() for code in blocker_chain if str(code).strip()]
    if not normalized:
        return "SUCCESS"
    if any(code.endswith("_MISSING") for code in normalized):
        return "MISSING_DEPENDENCY"
    return "FAIL"


def _startup_materialization_dependency_row(
    *,
    logical_name: str,
    ref: Mapping[str, Any],
    day_utc: str,
    fallback_reason_code: str,
) -> dict[str, Any]:
    exists = bool(ref.get("exists"))
    reason_codes = [str(code).strip() for code in ref.get("reason_codes") or [] if str(code).strip()]
    if not exists and not reason_codes:
        reason_codes = [fallback_reason_code]
    return build_fact_dependency_row_v1(
        logical_name=logical_name,
        absolute_path=_ref_path(ref),
        status="PRESENT" if exists else "MISSING",
        reason_codes=reason_codes,
        day_utc=day_utc,
    )


def _write_startup_materialization_artifact(
    *,
    startup_materialization_path: Path,
    day_utc: str,
    session_id: str,
    bootstrap_run_id: str,
    produced_utc: str,
    seed_ref: Mapping[str, Any],
    operator_statement_ref: Mapping[str, Any],
    positions_ref: Mapping[str, Any],
    cash_ref: Mapping[str, Any],
    nav_ref: Mapping[str, Any],
    envelope_ref: Mapping[str, Any],
    allocation_ref: Mapping[str, Any],
    startup_authorization_convergence_ref: Mapping[str, Any],
    authorization_ref: Mapping[str, Any],
    startup_materialization_phase: Mapping[str, Any],
) -> None:
    blocker_chain = [str(code).strip() for code in startup_materialization_phase.get("unmet") or [] if str(code).strip()]
    required_inputs_checked = [
        _startup_materialization_dependency_row(
            logical_name="paper_capital_seed",
            ref=seed_ref,
            day_utc=day_utc,
            fallback_reason_code="PAPER_CAPITAL_SEED_MISSING",
        ),
        _startup_materialization_dependency_row(
            logical_name="operator_statement",
            ref=operator_statement_ref,
            day_utc=day_utc,
            fallback_reason_code="OPERATOR_STATEMENT_MISSING",
        ),
        _startup_materialization_dependency_row(
            logical_name="paper_startup_authorization_convergence_v1",
            ref=startup_authorization_convergence_ref,
            day_utc=day_utc,
            fallback_reason_code="STARTUP_AUTH_CONVERGENCE_BLOCKED",
        ),
        _startup_materialization_dependency_row(
            logical_name="authorization_gate_verdict_v1",
            ref=authorization_ref,
            day_utc=day_utc,
            fallback_reason_code="AUTHORIZATION_GATE_ARTIFACT_MISSING",
        ),
    ]
    materialized_outputs = [
        _startup_materialization_dependency_row(
            logical_name="paper_capital_seed",
            ref=seed_ref,
            day_utc=day_utc,
            fallback_reason_code="PAPER_CAPITAL_SEED_MISSING",
        ),
        _startup_materialization_dependency_row(
            logical_name="operator_statement",
            ref=operator_statement_ref,
            day_utc=day_utc,
            fallback_reason_code="OPERATOR_STATEMENT_MISSING",
        ),
        _startup_materialization_dependency_row(
            logical_name="positions_snapshot_v2",
            ref=positions_ref,
            day_utc=day_utc,
            fallback_reason_code="POSITIONS_SNAPSHOT_MISSING",
        ),
        _startup_materialization_dependency_row(
            logical_name="cash_ledger_snapshot_v1",
            ref=cash_ref,
            day_utc=day_utc,
            fallback_reason_code="CASH_LEDGER_SNAPSHOT_MISSING",
        ),
        _startup_materialization_dependency_row(
            logical_name="accounting_nav_v2",
            ref=nav_ref,
            day_utc=day_utc,
            fallback_reason_code="ACCOUNTING_NAV_MISSING",
        ),
        _startup_materialization_dependency_row(
            logical_name="capital_risk_envelope_v2",
            ref=envelope_ref,
            day_utc=day_utc,
            fallback_reason_code="CAPITAL_RISK_ENVELOPE_MISSING",
        ),
        _startup_materialization_dependency_row(
            logical_name="capital_authority_allocation_v1",
            ref=allocation_ref,
            day_utc=day_utc,
            fallback_reason_code="CAPITAL_AUTHORITY_ALLOCATION_MISSING",
        ),
        _startup_materialization_dependency_row(
            logical_name="paper_startup_authorization_convergence_v1",
            ref=startup_authorization_convergence_ref,
            day_utc=day_utc,
            fallback_reason_code="STARTUP_AUTH_CONVERGENCE_BLOCKED",
        ),
        _startup_materialization_dependency_row(
            logical_name="authorization_gate_verdict_v1",
            ref=authorization_ref,
            day_utc=day_utc,
            fallback_reason_code="AUTHORIZATION_GATE_ARTIFACT_MISSING",
        ),
    ]
    payload = {
        "schema_id": "startup_materialization",
        "schema_version": "v1",
        "authority_scope": "NON_AUTHORITY_FACT",
        "day_utc": day_utc,
        "session_id": session_id,
        "status": _startup_materialization_status_code(blocker_chain),
        "required_inputs_checked": required_inputs_checked,
        "materialized_outputs": materialized_outputs,
        "blocking_codes": blocker_chain,
        "producer": producer_block_v1(module="ops/tools/run_paper_session_bootstrap_v1.py"),
        "produced_at_utc": produced_utc,
        "freshness_verdict": "CURRENT",
        "linkage_verdict": "LINKED",
        "producer_run_id": bootstrap_run_id,
        "materialization_scope": "BOOTSTRAP_SESSION_PREREQUISITES",
        "phase_summary": {
            "phase_status": str(startup_materialization_phase.get("status") or "BLOCKED"),
            "startup_authorization_convergence_status": str(startup_authorization_convergence_ref.get("status") or ""),
            "gate_artifacts_status": "READY" if bool(authorization_ref.get("exists")) else "BLOCKED",
            "materialized_output_count": sum(
                1 for row in materialized_outputs if str(row.get("status") or "").strip().upper() == "PRESENT"
            ),
            "root_blocker_code": blocker_chain[0] if blocker_chain else "",
        },
    }
    atomic_write_validated_json_v1(
        path=startup_materialization_path,
        payload=payload,
        schema_relpath=STARTUP_MATERIALIZATION_SCHEMA_RELPATH,
    )


def _production_only_prerequisites_status(*, admission_ref: Mapping[str, Any]) -> dict[str, Any]:
    build_ref = admission_ref.get("build_ref") if isinstance(admission_ref.get("build_ref"), dict) else {}
    build_path = str(build_ref.get("artifact_path") or admission_ref.get("path") or "")
    closure_status = str(admission_ref.get("closure_status") or "").strip().upper()
    hidden_dependency_result = (
        admission_ref.get("hidden_dependency_check_result")
        if isinstance(admission_ref.get("hidden_dependency_check_result"), dict)
        else {}
    )
    hidden_status = str(hidden_dependency_result.get("status") or "").strip().upper()
    hidden_dependency_reason_codes = []
    blocking_reason_code = str(hidden_dependency_result.get("blocking_reason_code") or "").strip()
    if blocking_reason_code:
        hidden_dependency_reason_codes.append(blocking_reason_code)
    if not hidden_dependency_reason_codes:
        hidden_dependency_reason_codes.append("PRODUCTION_ONLY_DEPENDENCIES_UNMET")

    checks = [
        _prerequisite_check(
            check_id="TARGET_DAY_BUILD_CLOSURE_CLOSED",
            passed=closure_status == "CLOSED",
            details=closure_status or "MISSING",
            ref_path=build_path,
            reason_codes=["TARGET_DAY_BUILD_CLOSURE_OPEN"],
        ),
        _prerequisite_check(
            check_id="TARGET_DAY_HIDDEN_DEPENDENCIES_PASS",
            passed=hidden_status == "PASS",
            details=str(hidden_dependency_result.get("summary") or hidden_status or "MISSING"),
            ref_path=build_path,
            reason_codes=hidden_dependency_reason_codes,
        ),
    ]
    return _group_status(passing_label="PASS", failing_label="UNMET", checks=checks)


def _bootstrap_semantic_status(
    *,
    required_prerequisites_status: Mapping[str, Any],
    paper_advisory_prerequisites_status: Mapping[str, Any],
    production_only_prerequisites_status: Mapping[str, Any],
) -> str:
    required_fail = str(required_prerequisites_status.get("status") or "").strip().upper() != "PASS"
    advisory_unmet = str(paper_advisory_prerequisites_status.get("status") or "").strip().upper() != "PASS"
    production_only_unmet = str(production_only_prerequisites_status.get("status") or "").strip().upper() != "PASS"
    if required_fail:
        return "BLOCKED"
    if production_only_unmet and advisory_unmet:
        return "READY_PAPER_ONLY_WITH_ADVISORIES"
    if production_only_unmet:
        return "READY_PAPER_ONLY"
    if advisory_unmet:
        return "READY_WITH_ADVISORIES"
    return "READY"


def _operator_guidance(
    *,
    bootstrap_status: str,
    required_prerequisites_status: Mapping[str, Any],
    runtime_prerequisite_verification: Mapping[str, Any],
) -> dict[str, Any]:
    escalation_blockers: list[str] = []
    retry_blockers: list[str] = []
    for blocker in required_prerequisites_status.get("unmet") or []:
        code = str(blocker).strip()
        if not code:
            continue
        if code == "CANONICAL_KILL_SWITCH_ACTIVE":
            escalation_blockers.append(code)
        else:
            retry_blockers.append(code)
    if str(bootstrap_status).strip().upper() == "READY":
        recommended_action = "PROCEED"
    elif escalation_blockers:
        recommended_action = "ESCALATE"
    else:
        recommended_action = "RETRY"
    earliest_failing = (
        runtime_prerequisite_verification.get("earliest_failing_prerequisite")
        if isinstance(runtime_prerequisite_verification.get("earliest_failing_prerequisite"), Mapping)
        else {}
    )
    return {
        "recommended_action": recommended_action,
        "retry_blocker_chain": retry_blockers,
        "escalation_blocker_chain": escalation_blockers,
        "first_blocker_stage": str(runtime_prerequisite_verification.get("stage") or ""),
        "first_blocker_prerequisite_id": str(earliest_failing.get("prerequisite_id") or ""),
        "first_blocker_owner_tool": str(earliest_failing.get("owner_tool") or ""),
        "first_blocker_artifact_path": str(earliest_failing.get("artifact_path") or ""),
        "first_blocker_class": str(earliest_failing.get("blocker_class") or ""),
        "first_blocker_reason_codes": _reason_codes(earliest_failing.get("reason_codes") or []),
        "first_blocker_action_summary": str(earliest_failing.get("action_summary") or ""),
        "fix_then_rerun_rule": CANONICAL_FIX_THEN_RERUN_RULE,
        "do_not_run_manually": list(DO_NOT_RUN_MANUALLY),
    }


def _admission_basis(admission_ref: Mapping[str, Any]) -> str:
    mode = str(admission_ref.get("mode") or "").strip()
    reason = str(admission_ref.get("reason") or "").strip()
    if mode and reason:
        return f"{mode}:{reason}"
    return mode or reason or "UNKNOWN"


def _build_blocker_chain(*, required_prerequisites_status: Mapping[str, Any]) -> list[str]:
    return [
        str(code).strip()
        for code in required_prerequisites_status.get("unmet") or []
        if str(code).strip()
    ]


def _report_ref(
    *,
    path: Path,
    status_fields: tuple[str, ...] = (),
    extra_fields: tuple[str, ...] = (),
) -> dict[str, Any]:
    return _artifact_ref(path, status_fields=status_fields, extra_fields=extra_fields)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_paper_session_bootstrap_v1")
    ap.add_argument("--day_utc", default="")
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--operator_input_root", default=str((REPO_ROOT / "constellation_2").resolve()))
    ap.add_argument("--environment", default=PAPER_ENVIRONMENT)
    ap.add_argument("--ib_account", default="")
    ap.add_argument("--seed_usd", default="")
    ap.add_argument("--materialize", default="YES", choices=["YES", "NO"])
    ap.add_argument("--emit_report", default="YES", choices=["YES", "NO"])
    args = ap.parse_args(argv)

    day_utc = resolve_session_authority_target_day_v1(str(args.day_utc or "").strip())
    environment = str(args.environment or "").strip().upper()
    if environment != PAPER_ENVIRONMENT:
        raise SystemExit(f"FAIL: PAPER_ONLY_BOOTSTRAP_TOOL:environment={environment}")
    materialize = _bool_materialize(args.materialize)
    emit_report = _bool_materialize(args.emit_report)
    operator_input_root = _ensure_dir(args.operator_input_root)
    canonical_truth_root = resolve_decision_truth_root_bridge_v1(
        args.truth_root or "",
        repo_root=REPO_ROOT,
        caller="ops/tools/run_paper_session_bootstrap_v1.py",
    )
    authority = load_runtime_path_authority_bridge_v1(
        repo_root=REPO_ROOT,
        caller="ops/tools/run_paper_session_bootstrap_v1.py",
    )
    if canonical_truth_root != authority.canonical_runtime_truth_root:
        raise SystemExit(
            f"FAIL: PAPER_BOOTSTRAP_CANONICAL_TRUTH_REQUIRED:requested={canonical_truth_root}:canonical={authority.canonical_runtime_truth_root}"
        )
    ib_account = _resolve_ib_account(args.ib_account)
    sleeve_binding = resolve_governed_sleeve_truth_bindings(
        repo_root=REPO_ROOT,
        environment=environment,
        requested_ib_account=ib_account,
        sleeve_id=PRIMARY_SLEEVE_ID,
    )[0]
    sleeve_truth_root = sleeve_binding.truth_root.resolve()

    produced_utc = now_utc_iso_v1()
    producer_git_sha = repo_git_sha_v1()
    report_path = resolve_paper_session_bootstrap_path(truth_root=canonical_truth_root, day_utc=day_utc)
    operation_id = f"paper_session_bootstrap:{day_utc}:{environment}"

    seed_path = resolve_paper_capital_seed_path(operator_input_root=operator_input_root, day_utc=day_utc)
    operator_statement_path = resolve_operator_statement_path(operator_input_root=operator_input_root, day_utc=day_utc)
    positions_snapshot_path = (
        sleeve_truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v2.json"
    ).resolve()
    cash_snapshot_path = (
        sleeve_truth_root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json"
    ).resolve()
    nav_path = (sleeve_truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json").resolve()
    allocation_summary_path = (
        sleeve_truth_root / "allocation_v1" / "summary" / day_utc / "summary.json"
    ).resolve()
    capital_risk_envelope_path = (
        sleeve_truth_root / "reports" / "capital_risk_envelope_v2" / day_utc / "capital_risk_envelope.v2.json"
    ).resolve()
    exposure_net_path = (
        sleeve_truth_root / "risk_v1" / "exposure_net_v1" / day_utc / "exposure_net.v1.json"
    ).resolve()
    capital_authority_allocation_path = (
        sleeve_truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day_utc / "capital_authority_allocation.v1.json"
    ).resolve()
    startup_authorization_convergence_path = resolve_paper_startup_authorization_convergence_path(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
    )
    authorization_gate_path = (
        sleeve_truth_root / "reports" / "authorization_gate_verdict_v1" / day_utc / "authorization_gate_verdict.v1.json"
    ).resolve()
    admission_path = (canonical_truth_root / "target_day_admission_v1" / f"{day_utc}.json").resolve()
    startup_materialization_path = (
        canonical_truth_root / "reports" / "startup_materialization_v1" / day_utc / "startup_materialization.v1.json"
    ).resolve()

    step_results: list[dict[str, Any]] = []

    seed_before = seed_path.exists()
    seed_run = None
    seed_usd = str(args.seed_usd or "").strip()
    continuity_seed_error = ""
    if materialize and not seed_before and not seed_usd:
        try:
            seed_usd = resolve_continuity_paper_seed_usd_v1(
                operator_input_root=operator_input_root,
                target_day=day_utc,
            )
        except Exception as exc:
            continuity_seed_error = (
                "CONTINUITY_PAPER_CAPITAL_SEED_RESOLUTION_FAILED:"
                f"{type(exc).__name__}:{exc}"
            )
    if materialize:
        if seed_usd:
            seed_run = _run_command(
                [
                    sys.executable,
                    str(ENSURE_PAPER_CAPITAL_SEED_TOOL),
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
        elif seed_before:
            seed_run = {
                "return_code": 0,
                "stdout": "OK: PAPER_CAPITAL_SEED_EXISTS_BOOTSTRAP_REUSE",
                "stderr": "",
                "json": {},
            }
        elif continuity_seed_error:
            seed_run = {
                "return_code": 2,
                "stdout": "",
                "stderr": continuity_seed_error,
                "json": {},
            }
    step_results.append(
        _step_result(
            step_id="paper_capital_seed",
            owner_tool=ENSURE_PAPER_CAPITAL_SEED_TOOL,
            scope="STARTUP_MATERIALIZATION",
            path=seed_path,
            before_exists=seed_before,
            run_result=seed_run,
            status_fields=("seed_mode",),
            materialize=materialize,
        )
    )

    operator_before = operator_statement_path.exists()
    operator_run = None
    if materialize:
        operator_run = _run_command(
            [
                sys.executable,
                str(ENSURE_OPERATOR_STATEMENT_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(operator_input_root),
                "--ib_account",
                ib_account,
                "--mode",
                "GOVERNED_SEED",
                "--allow_create",
                "YES",
            ]
        )
    step_results.append(
        _step_result(
            step_id="operator_statement",
            owner_tool=ENSURE_OPERATOR_STATEMENT_TOOL,
            scope="STARTUP_MATERIALIZATION",
            path=operator_statement_path,
            before_exists=operator_before,
            run_result=operator_run,
            status_fields=("observed_at_utc",),
            materialize=materialize,
        )
    )

    runtime_env = {
        "C2_TRUTH_ROOT": str(sleeve_truth_root),
        "C2_MODE": environment,
    }

    positions_before = positions_snapshot_path.exists()
    positions_run = _run_command(
        [
            str(RUNTIME_PYTHON),
            "-m",
            "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2",
            "--day_utc",
            day_utc,
            "--producer_repo",
            REPO_ROOT.name,
            "--producer_git_sha",
            producer_git_sha,
        ],
        env=runtime_env,
    ) if materialize else None
    step_results.append(
        _step_result(
            step_id="positions_snapshot",
            owner_tool=RUN_POSITIONS_SNAPSHOT_TOOL,
            scope="STARTUP_MATERIALIZATION",
            path=positions_snapshot_path,
            before_exists=positions_before,
            run_result=positions_run,
            status_fields=("status",),
            materialize=materialize,
        )
    )

    cash_before = cash_snapshot_path.exists()
    cash_run = _run_command(
        [
            str(RUNTIME_PYTHON),
            "-m",
            "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1",
            "--day_utc",
            day_utc,
            "--operator_statement_json",
            str(operator_statement_path),
            "--producer_repo",
            REPO_ROOT.name,
            "--producer_git_sha",
            producer_git_sha,
        ],
        env=runtime_env,
    ) if materialize else None
    step_results.append(
        _step_result(
            step_id="cash_ledger_snapshot",
            owner_tool=RUN_CASH_LEDGER_SNAPSHOT_TOOL,
            scope="STARTUP_MATERIALIZATION",
            path=cash_snapshot_path,
            before_exists=cash_before,
            run_result=cash_run,
            status_fields=("status",),
            materialize=materialize,
        )
    )

    nav_before = nav_path.exists()
    nav_run = _run_command(
        [
            sys.executable,
            str(RUN_ACCOUNTING_NAV_TOOL),
            "--day_utc",
            day_utc,
            "--truth_root",
            str(sleeve_truth_root),
            "--producer_repo",
            REPO_ROOT.name,
            "--producer_git_sha",
            producer_git_sha,
        ]
    ) if materialize else None
    step_results.append(
        _step_result(
            step_id="accounting_nav",
            owner_tool=RUN_ACCOUNTING_NAV_TOOL,
            scope="STARTUP_MATERIALIZATION",
            path=nav_path,
            before_exists=nav_before,
            run_result=nav_run,
            status_fields=("status", "nav_total"),
            materialize=materialize,
        )
    )

    allocation_before = allocation_summary_path.exists()
    allocation_run = _run_command(
        [
            str(RUNTIME_PYTHON),
            str(RUN_ALLOCATION_SUMMARY_TOOL),
            "--day_utc",
            day_utc,
            "--truth_root",
            str(sleeve_truth_root),
            "--producer_repo",
            REPO_ROOT.name,
            "--producer_git_sha",
            producer_git_sha,
        ]
    ) if materialize else None
    step_results.append(
        _step_result(
            step_id="allocation_summary",
            owner_tool=RUN_ALLOCATION_SUMMARY_TOOL,
            scope="STARTUP_MATERIALIZATION",
            path=allocation_summary_path,
            before_exists=allocation_before,
            run_result=allocation_run,
            status_fields=("status",),
            materialize=materialize,
        )
    )

    envelope_before = capital_risk_envelope_path.exists()
    envelope_run = _run_command(
        [
            sys.executable,
            str(RUN_CAPITAL_RISK_ENVELOPE_TOOL),
            "--out_day_utc",
            day_utc,
            "--input_day_utc",
            day_utc,
            "--produced_utc",
            produced_utc,
            "--truth_root",
            str(sleeve_truth_root),
        ]
    ) if materialize else None
    step_results.append(
        _step_result(
            step_id="capital_risk_envelope",
            owner_tool=RUN_CAPITAL_RISK_ENVELOPE_TOOL,
            scope="STARTUP_MATERIALIZATION",
            path=capital_risk_envelope_path,
            before_exists=envelope_before,
            run_result=envelope_run,
            status_fields=("status",),
            materialize=materialize,
        )
    )

    exposure_before = exposure_net_path.exists()
    exposure_run = None
    if materialize:
        if exposure_before:
            exposure_run = {"return_code": 0, "stdout": "REUSED_EXISTING_ARTIFACT", "stderr": ""}
        else:
            exposure_run = _run_command(
                [
                    sys.executable,
                    str(RUN_EXPOSURE_NET_TOOL),
                    "--day_utc",
                    day_utc,
                    "--truth_root",
                    str(sleeve_truth_root),
                ]
            )
    step_results.append(
        _step_result(
            step_id="exposure_net",
            owner_tool=RUN_EXPOSURE_NET_TOOL,
            scope="STARTUP_MATERIALIZATION",
            path=exposure_net_path,
            before_exists=exposure_before,
            run_result=exposure_run,
            status_fields=("status",),
            materialize=materialize,
        )
    )

    startup_authorization_convergence_before = startup_authorization_convergence_path.exists()
    startup_authorization_convergence_run = None
    if materialize:
        startup_authorization_convergence_run = _run_command(
            [
                sys.executable,
                str(RUN_STARTUP_AUTHORIZATION_CONVERGENCE_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(canonical_truth_root),
                "--environment",
                environment,
                "--ib_account",
                ib_account,
            ]
        )
    step_results.append(
        _step_result(
            step_id="startup_authorization_convergence",
            owner_tool=RUN_STARTUP_AUTHORIZATION_CONVERGENCE_TOOL,
            scope="STARTUP_MATERIALIZATION",
            path=startup_authorization_convergence_path,
            before_exists=startup_authorization_convergence_before,
            run_result=startup_authorization_convergence_run,
            status_fields=("status", "convergence_status"),
            materialize=materialize,
        )
    )

    capital_authority_before = capital_authority_allocation_path.exists()
    capital_authority_run = _run_command(
        [
            sys.executable,
            str(RUN_CAPITAL_AUTHORITY_ALLOCATION_TOOL),
            "--day_utc",
            day_utc,
            "--truth_root",
            str(sleeve_truth_root),
            "--canonical_sequence_owner",
            "ops/tools/run_c2_paper_day_orchestrator_v2.py",
            "--authority_verdict_path",
            str(authorization_gate_path),
        ]
    ) if materialize else None
    step_results.append(
        _step_result(
            step_id="capital_authority_allocation",
            owner_tool=RUN_CAPITAL_AUTHORITY_ALLOCATION_TOOL,
            scope="STARTUP_MATERIALIZATION",
            path=capital_authority_allocation_path,
            before_exists=capital_authority_before,
            run_result=capital_authority_run,
            status_fields=("status",),
            materialize=materialize,
        )
    )

    seed_ref = _report_ref(path=seed_path, status_fields=("seed_mode",), extra_fields=("seed_mode", "cash_total", "nlv_total"))
    operator_statement_ref = _report_ref(
        path=operator_statement_path,
        status_fields=("observed_at_utc",),
        extra_fields=("cash_total", "nlv_total", "observed_at_utc"),
    )
    positions_ref = _report_ref(
        path=positions_snapshot_path,
        status_fields=("status",),
        extra_fields=("day_utc", "schema_id"),
    )
    cash_ref = _report_ref(
        path=cash_snapshot_path,
        status_fields=("status",),
        extra_fields=("day_utc", "schema_id"),
    )
    nav_ref = _report_ref(
        path=nav_path,
        status_fields=("status",),
        extra_fields=("day_utc", "nav_total"),
    )
    envelope_ref = _report_ref(
        path=capital_risk_envelope_path,
        status_fields=("status",),
    )
    if envelope_ref.get("exists"):
        envelope_payload = _json_or_empty(capital_risk_envelope_path)
        envelope_ref["allowed_capital_at_risk_cents"] = (envelope_payload.get("envelope") or {}).get("allowed_capital_at_risk_cents")
        envelope_ref["headroom_cents"] = (envelope_payload.get("envelope") or {}).get("headroom_cents")
        envelope_ref["nav_total_cents"] = (envelope_payload.get("envelope") or {}).get("nav_total_cents")
    allocation_ref = _report_ref(
        path=capital_authority_allocation_path,
        status_fields=("status",),
    )
    if allocation_ref.get("exists"):
        allocation_payload = _json_or_empty(capital_authority_allocation_path)
        allocation_ref["portfolio_allowed_capital_at_risk_cents"] = (allocation_payload.get("portfolio") or {}).get(
            "allowed_capital_at_risk_cents"
        )
        allocation_ref["portfolio_headroom_cents"] = (allocation_payload.get("portfolio") or {}).get("headroom_cents")
    startup_authorization_convergence_ref = _report_ref(
        path=startup_authorization_convergence_path,
        status_fields=("status", "convergence_status"),
        extra_fields=("convergence_status", "authorization_verdict_ready", "blocker_chain"),
    )
    authorization_ref = _report_ref(path=authorization_gate_path, status_fields=("status",))
    startup_materialization_phase = _startup_materialization_phase_status(
        seed_ref=seed_ref,
        operator_statement_ref=operator_statement_ref,
        cash_ref=cash_ref,
        nav_ref=nav_ref,
        envelope_ref=envelope_ref,
        allocation_ref=allocation_ref,
        startup_authorization_convergence_ref=startup_authorization_convergence_ref,
        authorization_ref=authorization_ref,
    )
    session_id = canonical_paper_session_id_v1(day_utc)
    bootstrap_run_id = f"{day_utc}__{producer_git_sha[:12]}__{produced_utc.replace(':', '').replace('-', '')}"
    _write_startup_materialization_artifact(
        startup_materialization_path=startup_materialization_path,
        day_utc=day_utc,
        session_id=session_id,
        bootstrap_run_id=bootstrap_run_id,
        produced_utc=produced_utc,
        seed_ref=seed_ref,
        operator_statement_ref=operator_statement_ref,
        positions_ref=positions_ref,
        cash_ref=cash_ref,
        nav_ref=nav_ref,
        envelope_ref=envelope_ref,
        allocation_ref=allocation_ref,
        startup_authorization_convergence_ref=startup_authorization_convergence_ref,
        authorization_ref=authorization_ref,
        startup_materialization_phase=startup_materialization_phase,
    )

    kill_switch_before = _kill_switch_paths(
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        day_utc=day_utc,
    )[0].exists()
    kill_switch_run = None
    if materialize:
        kill_switch_run = _run_command(
            [
                sys.executable,
                str(RUN_GLOBAL_KILL_SWITCH_TOOL),
                "--day_utc",
                day_utc,
            ]
        )
    step_results.append(
        _step_result(
            step_id="global_kill_switch",
            owner_tool=RUN_GLOBAL_KILL_SWITCH_TOOL,
            scope="EVALUATION_SAFETY",
            path=_kill_switch_paths(
                canonical_truth_root=canonical_truth_root,
                sleeve_truth_root=sleeve_truth_root,
                day_utc=day_utc,
            )[0],
            before_exists=kill_switch_before,
            run_result=kill_switch_run,
            status_fields=("state",),
            materialize=materialize,
        )
    )

    shared_control_state = _shared_control_state(
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        day_utc=day_utc,
    )

    admission_ref = _report_ref(
        path=admission_path,
        status_fields=("admission_status",),
        extra_fields=("admission_status", "mode", "reason", "binding", "build_ref", "closure_status", "hidden_dependency_check_result"),
    )
    evaluation_phase = _evaluation_phase_status(
        shared_control_state=shared_control_state,
        envelope_ref=envelope_ref,
        authorization_ref=authorization_ref,
        admission_ref=admission_ref,
        include_admission=False,
    )

    core_prereqs_ready = (
        str(startup_materialization_phase.get("status") or "").strip().upper() == "COMPLETE"
        and str(evaluation_phase.get("status") or "").strip().upper() == "READY"
    )

    pointer_refresh_result = {"status": "SKIPPED", "reason_codes": []}
    pointer_refresh_authorization_ready = str(authorization_ref.get("status") or "").strip().upper() in {"PASS", "BOOTSTRAP_PASS"}
    if materialize and pointer_refresh_authorization_ready:
        pointer_refresh_result = _refresh_primary_authority_pointers(
            day_utc=day_utc,
            canonical_truth_root=canonical_truth_root,
            sleeve_truth_root=sleeve_truth_root,
            authorization_path=authorization_gate_path,
            producer_git_sha=producer_git_sha,
        )
    step_results.append(
        {
            "step_id": "primary_authority_pointer_refresh",
            "scope": "EVALUATION_SAFETY",
            "owner_tool": str(RUN_POINTER_APPEND_TOOL),
            "action": (
                "MATERIALIZED"
                if pointer_refresh_result.get("status") == "OK"
                else ("FAILED" if materialize and pointer_refresh_authorization_ready else "SKIPPED")
            ),
            "return_code": None,
            "path": str((sleeve_truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()),
            "status": str(pointer_refresh_result.get("status") or "SKIPPED"),
            "stdout": "",
            "stderr": ",".join(str(code).strip() for code in (pointer_refresh_result.get("reason_codes") or []) if str(code).strip()),
        }
    )

    pre_open_bundle_path = resolve_pre_open_bundle_path_v1(truth_root=canonical_truth_root, day_utc=day_utc)
    pre_open_before = pre_open_bundle_path.exists()
    pre_open_run = None
    if materialize and core_prereqs_ready:
        pre_open_run = _run_command(
            [
                sys.executable,
                str(RUN_PRE_OPEN_MATERIALIZER_TOOL),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(canonical_truth_root),
                "--environment",
                environment,
                "--ib_account",
                ib_account,
            ]
        )
    step_results.append(
        _step_result(
            step_id="pre_open_materialization",
            owner_tool=RUN_PRE_OPEN_MATERIALIZER_TOOL,
            scope="EVALUATION_SAFETY",
            path=pre_open_bundle_path,
            before_exists=pre_open_before,
            run_result=pre_open_run,
            status_fields=("materialization_state",),
            materialize=materialize and core_prereqs_ready,
        )
    )
    pre_open_ref = _report_ref(
        path=pre_open_bundle_path,
        status_fields=("materialization_state",),
        extra_fields=("materialization_state", "completion_state", "blocking_reason_codes"),
    )
    pre_open_payload = _json_or_empty(pre_open_bundle_path)
    pre_open_materialization_state = str(
        pre_open_ref.get("materialization_state") or pre_open_ref.get("status") or ""
    ).strip().upper()
    pre_open_ready = pre_open_materialization_state == "COMPLETE"

    session_authority_before = admission_path.exists()
    session_authority_run = None
    if materialize and core_prereqs_ready and pre_open_ready:
        session_authority_run = _run_command(
            [
                sys.executable,
                str(RUN_SESSION_AUTHORITY_TOOL),
                "--target_day",
                day_utc,
                "--truth_root",
                str(canonical_truth_root),
                "--environment",
                environment,
                "--ib_account",
                ib_account,
                "--phase",
                "all",
            ],
            env={
                "C2_SKIP_SESSION_AUTHORITY_REENTRY": "YES",
                "C2_SKIP_STARTUP_MATERIALIZATION_REENTRY": "YES",
            },
        )
    step_results.append(
        _step_result(
            step_id="session_authority",
            owner_tool=RUN_SESSION_AUTHORITY_TOOL,
            scope="EVALUATION_SAFETY",
            path=admission_path,
            before_exists=session_authority_before,
            run_result=session_authority_run,
            status_fields=("admission_status",),
            materialize=materialize and core_prereqs_ready,
        )
    )
    promotion_decision_path = resolve_session_promotion_decision_path_v1(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
    )
    promotion_before = promotion_decision_path.exists()
    step_results.append(
        _step_result(
            step_id="session_promotion_gate",
            owner_tool=RUN_SESSION_AUTHORITY_TOOL,
            scope="CURRENT_STATE_PROMOTION",
            path=promotion_decision_path,
            before_exists=promotion_before,
            run_result=session_authority_run,
            status_fields=("promotion_state",),
            materialize=materialize and core_prereqs_ready and pre_open_ready,
        )
    )

    admission_ref = _report_ref(
        path=admission_path,
        status_fields=("admission_status",),
        extra_fields=("admission_status", "mode", "reason", "binding", "build_ref", "closure_status", "hidden_dependency_check_result"),
    )
    promotion_ref = _report_ref(
        path=promotion_decision_path,
        status_fields=("promotion_state",),
        extra_fields=("promotion_state", "blocked_reason_codes"),
    )
    promotion_ref["blocked_reason_codes"] = [
        str(code).strip()
        for code in (promotion_ref.get("blocked_reason_codes") or [])
        if str(code).strip()
    ]
    promotion_state = str(promotion_ref.get("promotion_state") or promotion_ref.get("status") or "").strip().upper()
    promotion_ready = promotion_state == PROMOTION_STATE_PROMOTED
    evaluation_phase = _evaluation_phase_status(
        shared_control_state=shared_control_state,
        envelope_ref=envelope_ref,
        authorization_ref=authorization_ref,
        admission_ref=admission_ref,
        include_admission=True,
    )
    admission_granted = str(admission_ref.get("admission_status") or "").strip().upper() == "ADMIT"
    startup_materialization_ref = _report_ref(path=startup_materialization_path, status_fields=("status",))
    pointer_result = _pointer_refresh_result(canonical_truth_root=canonical_truth_root, day_utc=day_utc)
    existing_day_activation_summary = _existing_day_activation_result(
        canonical_truth_root=canonical_truth_root,
        sleeve_truth_root=sleeve_truth_root,
        day_utc=day_utc,
    )

    day_activation_run = None
    day_activation_summary: dict[str, Any] = {}
    if materialize and core_prereqs_ready and pre_open_ready and promotion_ready and admission_granted:
        day_activation_run = _run_command(
            [
                sys.executable,
                str(RUN_DAY_ACTIVATION_TOOL),
                "--operation_type",
                "fresh_paper_entry_v1",
                "--day_utc",
                day_utc,
                "--sleeve_id",
                PRIMARY_SLEEVE_ID,
                "--environment",
                environment,
                "--ib_account",
                ib_account,
                "--materialize",
                "YES",
                "--emit_package",
                "YES",
            ]
        )
        day_activation_summary = dict(day_activation_run.get("json") or {})
    elif existing_day_activation_summary:
        day_activation_summary = existing_day_activation_summary

    day_activation_action = "SKIPPED"
    day_activation_status = "SKIPPED_PREREQUISITE_BLOCKED" if materialize else "SKIPPED_MATERIALIZE_DISABLED"
    if day_activation_run is not None:
        day_activation_action = (
            "MATERIALIZED"
            if str(day_activation_summary.get("closure_status") or "").strip().upper() == "COMPLETE"
            else "FAILED"
        )
        day_activation_status = str(day_activation_summary.get("closure_status") or "FAILED")
    elif day_activation_summary:
        day_activation_action = "REUSED"
        day_activation_status = str(day_activation_summary.get("closure_status") or "UNKNOWN")
    step_results.append(
        {
            "step_id": "day_activation_readiness",
            "scope": "ACTIVATION_READINESS",
            "owner_tool": str(RUN_DAY_ACTIVATION_TOOL),
            "action": day_activation_action,
            "return_code": None if day_activation_run is None else day_activation_run.get("return_code"),
            "path": str(day_activation_summary.get("package_path") or day_activation_summary.get("build_path") or ""),
            "status": day_activation_status,
            "stdout": "" if day_activation_run is None else str(day_activation_run.get("stdout") or ""),
            "stderr": "" if day_activation_run is None else str(day_activation_run.get("stderr") or ""),
        }
    )

    runtime_prerequisite_verification = _runtime_prerequisite_verification(
        seed_ref=seed_ref,
        operator_statement_ref=operator_statement_ref,
        shared_control_state=shared_control_state,
        cash_ref=cash_ref,
        nav_ref=nav_ref,
        envelope_ref=envelope_ref,
        allocation_ref=allocation_ref,
        authorization_ref=authorization_ref,
        pre_open_ref=pre_open_ref,
        pre_open_payload=pre_open_payload,
        admission_ref=admission_ref,
        promotion_ref=promotion_ref,
        day_activation_summary=day_activation_summary,
    )

    activation_phase = _activation_phase_status(day_activation_result=day_activation_summary)

    required_prerequisites_status = _required_prerequisites_status(
        seed_ref=seed_ref,
        operator_statement_ref=operator_statement_ref,
        pre_open_ref=pre_open_ref,
        shared_control_state=shared_control_state,
    )
    paper_advisory_prerequisites_status = _paper_advisory_prerequisites_status(
        startup_materialization_ref=startup_materialization_ref,
        pointer_result=pointer_result,
    )
    production_only_prerequisites_status = _production_only_prerequisites_status(admission_ref=admission_ref)
    blocker_chain = _build_blocker_chain(required_prerequisites_status=required_prerequisites_status)
    root_blocker_class = _root_blocker_class(
        startup_materialization_phase=startup_materialization_phase,
        evaluation_phase=evaluation_phase,
        activation_phase=activation_phase,
    )
    bootstrap_status = "READY" if str(required_prerequisites_status.get("status") or "").strip().upper() == "PASS" else "BLOCKED"
    bootstrap_semantic_status = _bootstrap_semantic_status(
        required_prerequisites_status=required_prerequisites_status,
        paper_advisory_prerequisites_status=paper_advisory_prerequisites_status,
        production_only_prerequisites_status=production_only_prerequisites_status,
    )
    day_activation_ready = str(day_activation_summary.get("closure_status") or "").strip().upper() == "COMPLETE"
    smoke_submit_allowed = (
        bootstrap_status == "READY"
        and day_activation_ready
        and str(authorization_ref.get("status") or "").strip().upper() in {"PASS", "BOOTSTRAP_PASS"}
        and str(shared_control_state["canonical_kill_switch"].get("state") or "").strip().upper() == "INACTIVE"
        and bool(shared_control_state["sleeve_kill_switch_projection"].get("synced_from_canonical"))
    )
    paper_session_authority_payload = _build_paper_session_authority_payload(
        day_utc=day_utc,
        produced_utc=produced_utc,
        mode=environment,
        bootstrap_report_path=report_path,
        seed_ref=seed_ref,
        operator_statement_ref=operator_statement_ref,
        pre_open_ref=pre_open_ref,
        shared_control_state=shared_control_state,
        required_prerequisites_status=required_prerequisites_status,
        paper_advisory_prerequisites_status=paper_advisory_prerequisites_status,
        production_only_prerequisites_status=production_only_prerequisites_status,
        submission_authorized=smoke_submit_allowed,
    )
    paper_session_authority_ref = write_paper_session_authority_v1(
        truth_root=canonical_truth_root,
        payload=paper_session_authority_payload,
    )
    canonical_stop_surface = ""
    canonical_stop_artifact_path = ""
    canonical_stop_reason_codes: list[str] = []
    if bootstrap_status != "READY":
        if pre_open_materialization_state != "COMPLETE":
            canonical_stop_surface = "pre_open_bundle_v1"
            canonical_stop_artifact_path = str(pre_open_ref.get("path") or "")
            canonical_stop_reason_codes = [
                str(code).strip()
                for code in (pre_open_ref.get("blocking_reason_codes") or pre_open_ref.get("reason_codes") or [])
                if str(code).strip()
            ]
        elif promotion_state and promotion_state != PROMOTION_STATE_PROMOTED:
            canonical_stop_surface = "session_promotion_decision_v1"
            canonical_stop_artifact_path = str(promotion_ref.get("path") or "")
            canonical_stop_reason_codes = [
                str(code).strip()
                for code in (promotion_ref.get("blocked_reason_codes") or promotion_ref.get("reason_codes") or [])
                if str(code).strip()
            ]
        elif not admission_granted:
            canonical_stop_surface = "target_day_admission_v1"
            canonical_stop_artifact_path = str(admission_ref.get("path") or "")
            canonical_stop_reason_codes = [
                str(code).strip()
                for code in (admission_ref.get("reason_codes") or [])
                if str(code).strip()
            ]

    materialized_steps = [row["step_id"] for row in step_results if row["action"] == "MATERIALIZED"]
    reused_steps = [row["step_id"] for row in step_results if row["action"] == "REUSED"]
    skipped_steps = [row["step_id"] for row in step_results if row["action"] == "SKIPPED"]

    frozen_inputs = {
        "paper_capital_seed": seed_ref,
        "operator_statement": operator_statement_ref,
        "canonical_kill_switch": shared_control_state["canonical_kill_switch"],
        "sleeve_kill_switch_projection": shared_control_state["sleeve_kill_switch_projection"],
        "capital_risk_envelope": envelope_ref,
        "capital_authority_allocation": allocation_ref,
        "authorization_readiness": authorization_ref,
        "admission": admission_ref,
    }
    frozen_input_manifest_sha256 = canonical_hash_for_c2_artifact_v1({"frozen_inputs": frozen_inputs})
    owner_run_id = bootstrap_run_id
    startup_materialization_projection_hash = canonical_hash_for_c2_artifact_v1(
        {
            "status": startup_materialization_phase.get("status"),
            "blocker_chain": startup_materialization_phase.get("unmet"),
            "startup_authorization_convergence_status": startup_authorization_convergence_ref.get("status"),
            "authorization_status": authorization_ref.get("status"),
        }
    )
    evaluation_projection_hash = canonical_hash_for_c2_artifact_v1(
        {
            "status": evaluation_phase.get("status"),
            "blocker_chain": evaluation_phase.get("unmet"),
            "kill_switch_state": shared_control_state["canonical_kill_switch"].get("state"),
            "authorization_status": authorization_ref.get("status"),
            "admission_status": admission_ref.get("admission_status"),
        }
    )
    bootstrap_projection_hash = canonical_hash_for_c2_artifact_v1(
        {
            "bootstrap_status": bootstrap_status,
            "bootstrap_semantic_status": bootstrap_semantic_status,
            "smoke_submit_allowed": smoke_submit_allowed,
            "blocker_chain": blocker_chain,
            "required_prerequisites_status": required_prerequisites_status,
            "root_blocker_class": root_blocker_class,
        }
    )
    admission_projection_hash = canonical_hash_for_c2_artifact_v1(
        {
            "admission_status": admission_ref.get("admission_status"),
            "mode": admission_ref.get("mode"),
            "reason": admission_ref.get("reason"),
            "binding": admission_ref.get("binding"),
            "closure_status": admission_ref.get("closure_status"),
            "reason_codes": admission_ref.get("reason_codes"),
        }
    )
    activation_projection_hash = canonical_hash_for_c2_artifact_v1(
        {
            "closure_status": day_activation_summary.get("closure_status"),
            "first_real_blocker": day_activation_summary.get("first_real_blocker"),
            "package_path": str(day_activation_summary.get("package_path") or ""),
            "build_path": str(day_activation_summary.get("build_path") or ""),
        }
    )
    session_runtime_events = [
        {
            "event_type": "BOOTSTRAP_STARTED",
            "produced_utc": produced_utc,
            "owner_plane": "CANONICAL_CONTROL_PLANE",
            "owner_tool": "ops/tools/run_paper_session_bootstrap_v1.py",
            "owner_run_id": owner_run_id,
            "run_id": owner_run_id,
            "session_id": session_id,
            "submission_id": "",
            "order_id": "",
            "perm_id": "",
            "payload_ref": str(report_path),
            "payload_hash": frozen_input_manifest_sha256,
            "identity_key": f"{session_id}:BOOTSTRAP_STARTED",
            "identity_tuple": {
                "day_utc": day_utc,
                "owner_plane": "CANONICAL_CONTROL_PLANE",
                "owner_run_id": owner_run_id,
                "session_id": session_id,
                "submission_id": "",
                "order_id": "",
                "perm_id": "",
            },
            "event_payload_summary": {
                "operation_id": operation_id,
                "materialize": materialize,
                "frozen_input_manifest_sha256": frozen_input_manifest_sha256,
            },
        },
        {
            "event_type": "STARTUP_MATERIALIZATION_STARTED",
            "produced_utc": produced_utc,
            "owner_plane": "STARTUP_MATERIALIZATION_PLANE",
            "owner_tool": "ops/tools/run_paper_session_bootstrap_v1.py",
            "owner_run_id": owner_run_id,
            "run_id": owner_run_id,
            "session_id": session_id,
            "submission_id": "",
            "order_id": "",
            "perm_id": "",
            "payload_ref": str(report_path),
            "payload_hash": frozen_input_manifest_sha256,
            "identity_key": f"{session_id}:STARTUP_MATERIALIZATION_STARTED",
            "identity_tuple": {
                "day_utc": day_utc,
                "owner_plane": "STARTUP_MATERIALIZATION_PLANE",
                "owner_run_id": owner_run_id,
                "session_id": session_id,
                "submission_id": "",
                "order_id": "",
                "perm_id": "",
            },
            "event_payload_summary": {
                "materialized_steps": materialized_steps,
                "reused_steps": reused_steps,
                "skipped_steps": skipped_steps,
            },
        },
        {
            "event_type": (
                "STARTUP_AUTH_CONVERGENCE_COMPLETE"
                if str(startup_authorization_convergence_ref.get("status") or "").strip().upper() == "SUCCESS"
                else "STARTUP_AUTH_CONVERGENCE_BLOCKED"
            ),
            "produced_utc": produced_utc,
            "owner_plane": "STARTUP_MATERIALIZATION_PLANE",
            "owner_tool": "ops/tools/run_paper_session_bootstrap_v1.py",
            "owner_run_id": owner_run_id,
            "run_id": owner_run_id,
            "session_id": session_id,
            "submission_id": "",
            "order_id": "",
            "perm_id": "",
            "payload_ref": str(startup_authorization_convergence_path),
            "payload_hash": startup_materialization_projection_hash,
            "identity_key": f"{session_id}:STARTUP_AUTH_CONVERGENCE",
            "identity_tuple": {
                "day_utc": day_utc,
                "owner_plane": "STARTUP_MATERIALIZATION_PLANE",
                "owner_run_id": owner_run_id,
                "session_id": session_id,
                "submission_id": "",
                "order_id": "",
                "perm_id": "",
            },
            "event_payload_summary": {
                "startup_authorization_convergence_status": startup_authorization_convergence_ref.get("status"),
                "authorization_verdict_ready": startup_authorization_convergence_ref.get("authorization_verdict_ready"),
                "blocker_chain": startup_authorization_convergence_ref.get("blocker_chain"),
            },
        },
        {
            "event_type": (
                "STARTUP_GATE_ARTIFACTS_READY"
                if bool(authorization_ref.get("exists"))
                else "STARTUP_GATE_ARTIFACTS_BLOCKED"
            ),
            "produced_utc": produced_utc,
            "owner_plane": "STARTUP_MATERIALIZATION_PLANE",
            "owner_tool": "ops/tools/run_paper_session_bootstrap_v1.py",
            "owner_run_id": owner_run_id,
            "run_id": owner_run_id,
            "session_id": session_id,
            "submission_id": "",
            "order_id": "",
            "perm_id": "",
            "payload_ref": str(authorization_gate_path),
            "payload_hash": startup_materialization_projection_hash,
            "identity_key": f"{session_id}:STARTUP_GATE_ARTIFACTS",
            "identity_tuple": {
                "day_utc": day_utc,
                "owner_plane": "STARTUP_MATERIALIZATION_PLANE",
                "owner_run_id": owner_run_id,
                "session_id": session_id,
                "submission_id": "",
                "order_id": "",
                "perm_id": "",
            },
            "event_payload_summary": {
                "authorization_status": authorization_ref.get("status"),
                "blocker_chain": startup_materialization_phase.get("unmet"),
            },
        },
        {
            "event_type": (
                "STARTUP_MATERIALIZATION_COMPLETE"
                if str(startup_materialization_phase.get("status") or "").strip().upper() == "COMPLETE"
                else "STARTUP_MATERIALIZATION_BLOCKED"
            ),
            "produced_utc": produced_utc,
            "owner_plane": "STARTUP_MATERIALIZATION_PLANE",
            "owner_tool": "ops/tools/run_paper_session_bootstrap_v1.py",
            "owner_run_id": owner_run_id,
            "run_id": owner_run_id,
            "session_id": session_id,
            "submission_id": "",
            "order_id": "",
            "perm_id": "",
            "payload_ref": str(report_path),
            "payload_hash": startup_materialization_projection_hash,
            "identity_key": f"{session_id}:STARTUP_MATERIALIZATION_VERDICT",
            "identity_tuple": {
                "day_utc": day_utc,
                "owner_plane": "STARTUP_MATERIALIZATION_PLANE",
                "owner_run_id": owner_run_id,
                "session_id": session_id,
                "submission_id": "",
                "order_id": "",
                "perm_id": "",
            },
            "event_payload_summary": {
                "status": startup_materialization_phase.get("status"),
                "blocker_chain": startup_materialization_phase.get("unmet"),
            },
        },
        {
            "event_type": (
                "KILL_SWITCH_EVALUATED_INACTIVE"
                if str(shared_control_state["canonical_kill_switch"].get("state") or "").strip().upper() == "INACTIVE"
                else "KILL_SWITCH_EVALUATED_ACTIVE"
            ),
            "produced_utc": produced_utc,
            "owner_plane": "EVALUATION_SAFETY_PLANE",
            "owner_tool": "ops/tools/run_paper_session_bootstrap_v1.py",
            "owner_run_id": owner_run_id,
            "run_id": owner_run_id,
            "session_id": session_id,
            "submission_id": "",
            "order_id": "",
            "perm_id": "",
            "payload_ref": str(shared_control_state["canonical_kill_switch"].get("path") or report_path),
            "payload_hash": evaluation_projection_hash,
            "identity_key": f"{session_id}:KILL_SWITCH_EVALUATION",
            "identity_tuple": {
                "day_utc": day_utc,
                "owner_plane": "EVALUATION_SAFETY_PLANE",
                "owner_run_id": owner_run_id,
                "session_id": session_id,
                "submission_id": "",
                "order_id": "",
                "perm_id": "",
            },
            "event_payload_summary": {
                "kill_switch_state": shared_control_state["canonical_kill_switch"].get("state"),
                "reason_codes": shared_control_state["canonical_kill_switch"].get("reason_codes"),
                "synced_from_canonical": shared_control_state["sleeve_kill_switch_projection"].get("synced_from_canonical"),
            },
        },
        {
            "event_type": "ADMISSION_GRANTED"
            if str(admission_ref.get("admission_status") or "").strip().upper() == "ADMIT"
            else "ADMISSION_BLOCKED",
            "produced_utc": produced_utc,
            "owner_plane": "EVALUATION_SAFETY_PLANE",
            "owner_tool": "ops/tools/run_paper_session_bootstrap_v1.py",
            "owner_run_id": owner_run_id,
            "run_id": owner_run_id,
            "session_id": session_id,
            "submission_id": "",
            "order_id": "",
            "perm_id": "",
            "payload_ref": str(admission_path),
            "payload_hash": admission_projection_hash,
            "identity_key": f"{session_id}:ADMISSION",
            "identity_tuple": {
                "day_utc": day_utc,
                "owner_plane": "EVALUATION_SAFETY_PLANE",
                "owner_run_id": owner_run_id,
                "session_id": session_id,
                "submission_id": "",
                "order_id": "",
                "perm_id": "",
            },
            "event_payload_summary": {
                "admission_status": admission_ref.get("admission_status"),
                "mode": admission_ref.get("mode"),
                "reason": admission_ref.get("reason"),
                "closure_status": admission_ref.get("closure_status"),
                "reason_codes": admission_ref.get("reason_codes"),
            },
        },
        {
            "event_type": "ACTIVATION_READY" if day_activation_ready else "ACTIVATION_BLOCKED",
            "produced_utc": produced_utc,
            "owner_plane": "EVALUATION_SAFETY_PLANE",
            "owner_tool": "ops/tools/run_paper_session_bootstrap_v1.py",
            "owner_run_id": owner_run_id,
            "run_id": owner_run_id,
            "session_id": session_id,
            "submission_id": "",
            "order_id": "",
            "perm_id": "",
            "payload_ref": str(day_activation_summary.get("package_path") or day_activation_summary.get("build_path") or report_path),
            "payload_hash": activation_projection_hash,
            "identity_key": f"{session_id}:ACTIVATION",
            "identity_tuple": {
                "day_utc": day_utc,
                "owner_plane": "EVALUATION_SAFETY_PLANE",
                "owner_run_id": owner_run_id,
                "session_id": session_id,
                "submission_id": "",
                "order_id": "",
                "perm_id": "",
            },
            "event_payload_summary": {
                "closure_status": str(day_activation_summary.get("closure_status") or ""),
                "first_real_blocker": day_activation_summary.get("first_real_blocker"),
                "package_path": str(day_activation_summary.get("package_path") or ""),
                "build_path": str(day_activation_summary.get("build_path") or ""),
            },
        },
        {
            "event_type": "BOOTSTRAP_READY" if bootstrap_status == "READY" else "BOOTSTRAP_BLOCKED",
            "produced_utc": produced_utc,
            "owner_plane": "CANONICAL_CONTROL_PLANE",
            "owner_tool": "ops/tools/run_paper_session_bootstrap_v1.py",
            "owner_run_id": owner_run_id,
            "run_id": owner_run_id,
            "session_id": session_id,
            "submission_id": "",
            "order_id": "",
            "perm_id": "",
            "payload_ref": str(report_path),
            "payload_hash": bootstrap_projection_hash,
            "identity_key": f"{session_id}:BOOTSTRAP_VERDICT",
            "identity_tuple": {
                "day_utc": day_utc,
                "owner_plane": "CANONICAL_CONTROL_PLANE",
                "owner_run_id": owner_run_id,
                "session_id": session_id,
                "submission_id": "",
                "order_id": "",
                "perm_id": "",
            },
            "event_payload_summary": {
                "bootstrap_status": bootstrap_status,
                "bootstrap_semantic_status": bootstrap_semantic_status,
                "smoke_submit_allowed": smoke_submit_allowed,
                "blocker_chain": blocker_chain,
                "root_blocker_class": root_blocker_class,
            },
        },
    ]
    runtime_ledger_append = append_runtime_ledger_events_v1(
        truth_root=canonical_truth_root,
        day_utc=day_utc,
        events=session_runtime_events,
    )
    payload = {
        "schema_id": "paper_session_bootstrap",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "mode": str(admission_ref.get("mode") or "PAPER_BOOTSTRAP"),
        "sleeve_id": PRIMARY_SLEEVE_ID,
        "ib_account": ib_account,
        "operation_id": operation_id,
        "bootstrap_run_id": bootstrap_run_id,
        "owner_run_id": owner_run_id,
        "produced_utc": produced_utc,
        "producer": producer_block_v1(module="ops/tools/run_paper_session_bootstrap_v1.py"),
        "truth_roots": {
            "canonical_truth_root": str(canonical_truth_root),
            "sleeve_truth_root": str(sleeve_truth_root),
            "operator_input_root": str(operator_input_root),
        },
        "frozen_inputs": frozen_inputs,
        "frozen_input_manifest_sha256": frozen_input_manifest_sha256,
        "materialized_steps": materialized_steps,
        "reused_steps": reused_steps,
        "skipped_steps": skipped_steps,
        "step_results": step_results,
        "shared_control_state": shared_control_state,
        "startup_materialization_phase": {
            "status": str(startup_materialization_phase.get("status") or "BLOCKED"),
            "materialize_requested": materialize,
            "blocker_chain": startup_materialization_phase.get("unmet") or [],
            "startup_authorization_convergence": startup_authorization_convergence_ref,
            "authorization_readiness": authorization_ref,
            "gate_artifacts_materialized": bool(authorization_ref.get("exists")),
        },
        "evaluation_phase": {
            "status": str(evaluation_phase.get("status") or "BLOCKED"),
            "blocker_chain": evaluation_phase.get("unmet") or [],
            "kill_switch_state": str(shared_control_state["canonical_kill_switch"].get("state") or ""),
            "authorization_status": str(authorization_ref.get("status") or ""),
            "admission_status": str(admission_ref.get("admission_status") or ""),
        },
        "activation_phase": {
            "status": str(activation_phase.get("status") or "BLOCKED"),
            "blocker_chain": activation_phase.get("unmet") or [],
            "day_activation_ready": day_activation_ready,
            "closure_status": str(day_activation_summary.get("closure_status") or ""),
        },
        "session_bootstrap": {
            "cash_ledger_snapshot": cash_ref,
            "accounting_nav": nav_ref,
            "startup_materialization": startup_materialization_ref,
            "startup_authorization_convergence": startup_authorization_convergence_ref,
            "capital_risk_envelope": envelope_ref,
            "allocation_readiness": allocation_ref,
            "authorization_readiness": authorization_ref,
            "admission": admission_ref,
            "promotion_gate": promotion_ref,
            "pointer_refresh": pointer_result,
            "day_activation": {
                "build_path": str(day_activation_summary.get("build_path") or ""),
                "package_path": str(day_activation_summary.get("package_path") or ""),
                "closure_status": str(day_activation_summary.get("closure_status") or ""),
                "first_real_blocker": day_activation_summary.get("first_real_blocker"),
            },
        },
        "bootstrap_status": bootstrap_status,
        "bootstrap_semantic_status": bootstrap_semantic_status,
        "root_blocker_class": root_blocker_class,
        "admission_basis": _admission_basis(admission_ref),
        "required_prerequisites_status": required_prerequisites_status,
        "paper_advisory_prerequisites_status": paper_advisory_prerequisites_status,
        "production_only_prerequisites_status": production_only_prerequisites_status,
        "runtime_prerequisite_verification": runtime_prerequisite_verification,
        "operator_guidance": _operator_guidance(
            bootstrap_status=bootstrap_status,
            required_prerequisites_status=required_prerequisites_status,
            runtime_prerequisite_verification=runtime_prerequisite_verification,
        ),
        "day_activation_ready": day_activation_ready,
        "smoke_submit_allowed": smoke_submit_allowed,
        "blocker_chain": blocker_chain,
        "canonical_stop_surface": canonical_stop_surface,
        "canonical_stop_artifact_path": canonical_stop_artifact_path,
        "canonical_stop_reason_codes": canonical_stop_reason_codes,
        "runtime_ledger_projection": projection_over_runtime_ledger_v1(
            truth_root=canonical_truth_root,
            day_utc=day_utc,
            append_result=runtime_ledger_append,
            projection_notice=(
                "Operator-facing bootstrap report derived from canonical runtime ledger session truth "
                "plus canonical control-state artifacts."
            ),
        ),
    }

    ref = None
    if emit_report:
        ref = atomic_write_validated_json_v1(
            path=report_path,
            payload=payload,
            schema_relpath=OUTPUT_SCHEMA_RELPATH,
        )

    summary = {
        "target_day": day_utc,
        "path": str(report_path if ref is None else ref.path),
        "bootstrap_status": bootstrap_status,
        "bootstrap_semantic_status": bootstrap_semantic_status,
        "root_blocker_class": root_blocker_class,
        "pre_open_materialization_state": pre_open_materialization_state,
        "promotion_state": promotion_state,
        "promotion_decision_path": str(promotion_ref.get("path") or ""),
        "target_day_admission_status": str(admission_ref.get("admission_status") or ""),
        "smoke_submit_allowed": smoke_submit_allowed,
        "day_activation_ready": day_activation_ready,
        "blocker_chain": blocker_chain,
        "canonical_stop_surface": canonical_stop_surface,
        "canonical_stop_artifact_path": canonical_stop_artifact_path,
        "canonical_stop_reason_codes": canonical_stop_reason_codes,
        "operation_id": operation_id,
        "bootstrap_run_id": bootstrap_run_id,
        "owner_run_id": owner_run_id,
        "paper_session_authority_path": str(paper_session_authority_ref.path),
        "paper_session_authority_status": str(paper_session_authority_payload.get("authority_status") or ""),
        "paper_session_authority_degraded_mode": bool(paper_session_authority_payload.get("degraded_mode") is True),
        "runtime_prerequisite_stage": str(runtime_prerequisite_verification.get("stage") or ""),
        "earliest_failing_prerequisite_id": str(
            (
                runtime_prerequisite_verification.get("earliest_failing_prerequisite") or {}
            ).get("prerequisite_id")
            or ""
        ),
        "earliest_failing_prerequisite_owner_tool": str(
            (
                runtime_prerequisite_verification.get("earliest_failing_prerequisite") or {}
            ).get("owner_tool")
            or ""
        ),
        "earliest_failing_prerequisite_artifact_path": str(
            (
                runtime_prerequisite_verification.get("earliest_failing_prerequisite") or {}
            ).get("artifact_path")
            or ""
        ),
        "earliest_failing_prerequisite_blocker_class": str(
            (
                runtime_prerequisite_verification.get("earliest_failing_prerequisite") or {}
            ).get("blocker_class")
            or ""
        ),
        "earliest_failing_prerequisite_reason_codes": _reason_codes(
            (
                runtime_prerequisite_verification.get("earliest_failing_prerequisite") or {}
            ).get("reason_codes")
            or []
        ),
        "fix_then_rerun_rule": CANONICAL_FIX_THEN_RERUN_RULE,
        "do_not_run_manually": list(DO_NOT_RUN_MANUALLY),
    }
    print(json.dumps(summary, sort_keys=True))
    return 0 if bootstrap_status == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
