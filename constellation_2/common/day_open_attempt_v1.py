from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.common.day_open_trigger_v1 import (
    build_day_open_trigger_payload,
    consume_day_open_trigger_v1,
    write_day_open_trigger_v1,
)
from constellation_2.common.day_open_policy_v1 import (
    LATE_TRIGGER_KIND,
    NO_TRIGGER_KIND,
    build_day_open_policy_snapshot,
    normalize_day_open_environment,
)
from constellation_2.common.day_open_window_v1 import build_day_open_window_v1
from constellation_2.common.paper_session_fact_plane_v1 import (
    SurfaceRefV1,
    atomic_write_idempotent_validated_json_v1,
    now_utc_iso_v1,
    producer_block_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_day_open_attempt_path,
    resolve_day_open_trigger_path,
    resolve_operator_day_authority_summary_path,
    resolve_paper_session_ledger_path,
    resolve_sleeve_rollup_path,
    resolve_trading_day_state_machine_path,
    resolve_day_failure_causality_path,
)
from constellation_2.common.runtime_contract_v1 import resolve_truth_sleeves_root
from constellation_2.common.session_authority_v1 import (
    resolve_active_session_path,
    resolve_target_day_admission_path,
)


SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/day_open_attempt.v1.schema.json"


def _load_existing_attempt(*, truth_root: Path, day_utc: str) -> Dict[str, Any] | None:
    attempt_path = resolve_day_open_attempt_path(truth_root=truth_root, day_utc=day_utc)
    if not attempt_path.exists() or not attempt_path.is_file():
        return None
    payload = read_json_object_v1(attempt_path)
    return payload if isinstance(payload, dict) else None


def _find_per_sleeve_refs(*, day_utc: str) -> tuple[list[str], list[str]]:
    truth_sleeves_root = resolve_truth_sleeves_root()
    verdicts = sorted(
        str(path.resolve())
        for path in truth_sleeves_root.rglob("orchestrator_run_verdict.v2.json")
        if f"/orchestrator_run_verdict_v2/{day_utc}/" in str(path.resolve())
    )
    pointers = sorted(
        str(path.resolve())
        for path in truth_sleeves_root.rglob("canonical_pointer_index.v1.jsonl")
        if "run_pointer_v1" in str(path.resolve()) and day_utc in path.read_text(encoding="utf-8")
    )
    return verdicts, pointers


def _attempt_sequence(existing: Dict[str, Any] | None) -> int:
    if not isinstance(existing, dict):
        return 1
    try:
        prior = int(existing.get("attempt_sequence") or 0)
        history = existing.get("attempt_history") if isinstance(existing.get("attempt_history"), list) else []
        max_history = max((int(row.get("attempt_sequence") or 0) for row in history if isinstance(row, dict)), default=0)
        return max(prior, max_history) + 1
    except Exception:
        return 1


def _attempt_history(existing: Dict[str, Any] | None) -> List[Dict[str, Any]]:
    if not isinstance(existing, dict):
        return []
    history = existing.get("attempt_history")
    rows = [dict(row) for row in history if isinstance(row, dict)] if isinstance(history, list) else []
    for row in rows:
        row["trigger_kind"] = _normalize_trigger_kind(row.get("trigger_kind"))
    return rows


def _normalize_trigger_kind(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if normalized in {"INITIAL_BOD_TRIGGER", "LATE_OPEN_TRIGGER", "NO_TRIGGER"}:
        return normalized
    return NO_TRIGGER_KIND


def _attempt_entry(existing: Dict[str, Any], *, truth_root: Path, day_utc: str) -> Dict[str, Any]:
    return {
        "attempt_sequence": int(existing.get("attempt_sequence") or 0),
        "trigger_kind": _normalize_trigger_kind(existing.get("trigger_kind")),
        "final_classification": str(existing.get("final_classification") or "").strip().upper(),
        "result_code": str(existing.get("result_code") or "").strip(),
        "reason_codes": list(existing.get("reason_codes") or []),
        "started_at_utc": str(existing.get("started_at_utc") or "").strip(),
        "finished_at_utc": str(existing.get("finished_at_utc") or "").strip(),
        "attempt_ref": str(resolve_day_open_attempt_path(truth_root=truth_root, day_utc=day_utc)),
        "open_command_executed": bool(existing.get("open_command_executed") is True),
        "submit_stage_entered": bool(existing.get("submit_stage_entered") is True),
    }


def _append_prior_attempt(
    history: List[Dict[str, Any]],
    existing: Dict[str, Any] | None,
    *,
    truth_root: Path,
    day_utc: str,
) -> List[Dict[str, Any]]:
    if not isinstance(existing, dict):
        return history
    sequence = int(existing.get("attempt_sequence") or 0)
    classification = str(existing.get("final_classification") or "").strip().upper()
    if sequence <= 0 or not classification:
        return history
    if any(int(row.get("attempt_sequence") or 0) == sequence for row in history):
        return history
    history.append(_attempt_entry(existing, truth_root=truth_root, day_utc=day_utc))
    history.sort(key=lambda row: int(row.get("attempt_sequence") or 0))
    return history


def _terminal_or_inflight(
    existing: Dict[str, Any] | None,
    *,
    environment: str,
    next_trigger_kind: str,
    late_open_available: bool,
) -> bool:
    if not isinstance(existing, dict):
        return False
    classification = str(existing.get("final_classification") or "").strip().upper()
    current_trigger_kind = _normalize_trigger_kind(existing.get("trigger_kind"))
    normalized_environment = normalize_day_open_environment(environment)
    if classification == "OPEN_ATTEMPTED":
        return True
    if normalized_environment == "PAPER":
        return False
    if classification == "OPEN_SUCCEEDED":
        return True
    if classification in {"OPEN_FAILED", "OPEN_MISSED"}:
        if current_trigger_kind == LATE_TRIGGER_KIND:
            return True
        if next_trigger_kind == LATE_TRIGGER_KIND and late_open_available:
            return False
        return True
    return False


def _write_attempt(*, truth_root: Path, payload: Dict[str, Any]) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    return atomic_write_idempotent_validated_json_v1(
        path=resolve_day_open_attempt_path(truth_root=root, day_utc=str(payload.get("day_utc") or "").strip()),
        payload=payload,
        schema_relpath=SCHEMA_RELPATH,
        volatile_field_names=("started_at_utc", "finished_at_utc"),
    )


def _current_authority_state(*, truth_root: Path, day_utc: str, paper_session_ledger_path: Path) -> dict[str, str]:
    active_session = read_json_object_v1(resolve_active_session_path(truth_root=truth_root))
    admission = read_json_object_v1(resolve_target_day_admission_path(truth_root=truth_root, target_day=day_utc))
    ledger = read_json_object_v1(paper_session_ledger_path)
    control_state = ledger.get("control_state") if isinstance(ledger.get("control_state"), dict) else {}
    return {
        "active_day": str(active_session.get("active_day") or "").strip(),
        "rollover_status": str(active_session.get("rollover_status") or "").strip().upper(),
        "admission_status": str(admission.get("admission_status") or "").strip().upper(),
        "ledger_authority_status": str(
            control_state.get("authority_status")
            or ledger.get("authority_status")
            or ""
        ).strip().upper(),
    }


def _post_attempt_refresh(*, repo_root: Path, truth_root: Path, day_utc: str) -> None:
    env = os.environ.copy()
    env["C2_TRUTH_ROOT"] = str(truth_root)
    for tool in (
        "ops/tools/run_trading_day_state_machine_v1.py",
        "ops/tools/run_operator_day_authority_summary_v1.py",
        "ops/tools/run_day_failure_causality_v1.py",
    ):
        subprocess.run(
            [sys.executable, str((repo_root / tool).resolve()), "--day_utc", day_utc, "--truth_root", str(truth_root)],
            cwd=str(repo_root),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )


def _runtime_lifecycle_ref(
    *,
    runtime_run_id: str,
    runtime_identity_contract_path: str,
    runtime_identity_contract_sha256: str,
    startup_identity_receipt_path: str,
    lifecycle_start_receipt_path: str,
) -> Dict[str, str] | None:
    values = {
        "run_id": str(runtime_run_id or "").strip(),
        "runtime_identity_contract_path": str(runtime_identity_contract_path or "").strip(),
        "runtime_identity_contract_sha256": str(runtime_identity_contract_sha256 or "").strip().lower(),
        "startup_identity_receipt_path": str(startup_identity_receipt_path or "").strip(),
        "lifecycle_start_receipt_path": str(lifecycle_start_receipt_path or "").strip(),
    }
    if not any(values.values()):
        return None
    missing = [key for key, value in values.items() if not value]
    if missing:
        raise ValueError(f"DAY_OPEN_ATTEMPT_RUNTIME_LIFECYCLE_REF_INCOMPLETE:{','.join(sorted(missing))}")
    return values


def read_day_open_attempt_runtime_lifecycle_ref_v1(
    *,
    truth_root: Path | str,
    day_utc: str,
) -> tuple[Path, Dict[str, str] | None]:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    attempt_path = resolve_day_open_attempt_path(truth_root=root, day_utc=day_utc)
    if not attempt_path.exists() or not attempt_path.is_file():
        return attempt_path, None
    payload = read_json_object_v1(attempt_path)
    raw_ref = payload.get("runtime_lifecycle_ref")
    if raw_ref is None:
        return attempt_path, None
    if not isinstance(raw_ref, dict):
        raise ValueError("DAY_OPEN_ATTEMPT_RUNTIME_LIFECYCLE_REF_INVALID")
    return (
        attempt_path,
        _runtime_lifecycle_ref(
            runtime_run_id=str(raw_ref.get("run_id") or "").strip(),
            runtime_identity_contract_path=str(raw_ref.get("runtime_identity_contract_path") or "").strip(),
            runtime_identity_contract_sha256=str(raw_ref.get("runtime_identity_contract_sha256") or "").strip(),
            startup_identity_receipt_path=str(raw_ref.get("startup_identity_receipt_path") or "").strip(),
            lifecycle_start_receipt_path=str(raw_ref.get("lifecycle_start_receipt_path") or "").strip(),
        ),
    )


def run_day_open_attempt_v1(
    *,
    repo_root: Path,
    truth_root: Path,
    day_utc: str,
    paper_session_ledger_path: Path,
    actor_name: str,
    actor_path: str,
    environment: str = "PAPER",
    symbol: str = "SPY",
    runtime_run_id: str = "",
    runtime_identity_contract_path: str = "",
    runtime_identity_contract_sha256: str = "",
    startup_identity_receipt_path: str = "",
    lifecycle_start_receipt_path: str = "",
) -> SurfaceRefV1:
    root = resolve_fact_plane_truth_root_v1(truth_root)
    existing_attempt = _load_existing_attempt(truth_root=root, day_utc=day_utc)
    normalized_environment = normalize_day_open_environment(environment)
    trigger_payload = build_day_open_trigger_payload(
        repo_root=repo_root,
        truth_root=root,
        day_utc=day_utc,
        environment=normalized_environment,
    )
    policy = build_day_open_policy_snapshot(
        environment=normalized_environment,
        window_status=str(trigger_payload.get("open_window_status") or "").strip().upper(),
        trigger_payload=trigger_payload,
        attempt_payload=existing_attempt,
        attempt_path=resolve_day_open_attempt_path(truth_root=root, day_utc=day_utc),
    )
    next_trigger_kind = str(trigger_payload.get("trigger_kind") or "").strip().upper()
    if _terminal_or_inflight(
        existing_attempt,
        environment=normalized_environment,
        next_trigger_kind=next_trigger_kind,
        late_open_available=bool(policy.get("late_open_available") is True),
    ):
        return _write_attempt(truth_root=root, payload=dict(existing_attempt))
    trigger_ref = write_day_open_trigger_v1(truth_root=root, payload=trigger_payload)
    window = build_day_open_window_v1(repo_root=repo_root, day_utc=day_utc)
    rollup_path = resolve_sleeve_rollup_path(truth_root=root, day_utc=day_utc)
    attempt_history = _attempt_history(existing_attempt)
    attempt_history = _append_prior_attempt(attempt_history, existing_attempt, truth_root=root, day_utc=day_utc)

    base_payload = {
        "schema_id": "day_open_attempt",
        "schema_version": "v1",
        "day_utc": str(day_utc).strip(),
        "attempt_sequence": _attempt_sequence(existing_attempt),
        "trigger_kind": next_trigger_kind,
        "started_at_utc": now_utc_iso_v1(),
        "finished_at_utc": "",
        "trigger_ref": str(trigger_ref.path),
        "consumed_trigger_key": str(trigger_payload.get("dedupe_key") or "").strip(),
        "actor_identity": {
            "actor_name": str(actor_name).strip(),
            "actor_path": str(actor_path).strip(),
        },
        "open_command_executed": False,
        "submit_stage_entered": False,
        "submit_stage_owner": "ops/tools/run_c2_paper_day_orchestrator_v2.py",
        "result_code": "NOT_EVALUATED",
        "reason_codes": [],
        "consumed_authority_refs": {
            "paper_session_ledger_v1": str(paper_session_ledger_path),
            "day_open_trigger_v1": str(trigger_ref.path),
        },
        "direct_evidence_refs": {
            "sleeve_rollup_v1": str(rollup_path),
            "orchestrator_run_verdict_v2": [],
            "run_pointer_v1": [],
        },
        "final_classification": "NOT_EXECUTED",
        "attempt_history": attempt_history,
        "environment": normalized_environment,
        "open_policy": policy,
        "producer": producer_block_v1(module="constellation_2/common/day_open_attempt_v1.py"),
    }
    runtime_lifecycle_ref = _runtime_lifecycle_ref(
        runtime_run_id=runtime_run_id,
        runtime_identity_contract_path=runtime_identity_contract_path,
        runtime_identity_contract_sha256=runtime_identity_contract_sha256,
        startup_identity_receipt_path=startup_identity_receipt_path,
        lifecycle_start_receipt_path=lifecycle_start_receipt_path,
    )
    if runtime_lifecycle_ref:
        base_payload["runtime_lifecycle_ref"] = runtime_lifecycle_ref

    if str(trigger_payload.get("trigger_status") or "").strip().upper() != "EMITTED" or bool(trigger_payload.get("consumed") is True):
        authority_state = _current_authority_state(
            truth_root=root,
            day_utc=day_utc,
            paper_session_ledger_path=paper_session_ledger_path,
        )
        open_ready = (
            authority_state["admission_status"] == "ADMIT"
            and authority_state["active_day"] == day_utc
            and authority_state["rollover_status"] == "ACTIVE_SESSION_CONFIRMED"
            and authority_state["ledger_authority_status"] == "GRANTED"
        )
        if window.window_status == "POST_OPEN_WINDOW" and open_ready:
            if normalized_environment == "PAPER":
                base_payload["result_code"] = "NO_VALID_OPEN_TRIGGER"
                base_payload["reason_codes"] = [str(trigger_payload.get("trigger_reason_code") or "NO_VALID_OPEN_TRIGGER")]
                base_payload["final_classification"] = "NOT_EXECUTED"
            else:
                base_payload["result_code"] = "OPEN_WINDOW_EXPIRED"
                base_payload["reason_codes"] = [str(trigger_payload.get("trigger_reason_code") or "OPEN_WINDOW_EXPIRED")]
                base_payload["final_classification"] = "OPEN_MISSED"
        else:
            base_payload["result_code"] = "NO_VALID_OPEN_TRIGGER"
            base_payload["reason_codes"] = [str(trigger_payload.get("trigger_reason_code") or "NO_VALID_OPEN_TRIGGER")]
            base_payload["final_classification"] = "NOT_EXECUTED"
        base_payload["finished_at_utc"] = now_utc_iso_v1()
        base_payload["open_policy"] = build_day_open_policy_snapshot(
            environment=normalized_environment,
            window_status=str(trigger_payload.get("open_window_status") or "").strip().upper(),
            trigger_payload=trigger_payload,
            attempt_payload=base_payload,
            attempt_path=resolve_day_open_attempt_path(truth_root=root, day_utc=day_utc),
        )
        attempt_ref = _write_attempt(truth_root=root, payload=base_payload)
        _post_attempt_refresh(repo_root=repo_root, truth_root=root, day_utc=day_utc)
        return attempt_ref

    in_progress = dict(base_payload)
    in_progress["final_classification"] = "OPEN_ATTEMPTED"
    attempt_ref = _write_attempt(truth_root=root, payload=in_progress)

    consumed_trigger_ref = consume_day_open_trigger_v1(
        truth_root=root,
        day_utc=day_utc,
        actor_name=actor_name,
        actor_path=actor_path,
        environment=normalized_environment,
    )

    cmd = [
        sys.executable,
        str((repo_root / "ops/tools/run_c2_multi_sleeve_orchestrator_v1.py").resolve()),
        "--day_utc",
        day_utc,
        "--input_day_utc",
        day_utc,
        "--symbol",
        str(symbol).strip().upper() or "SPY",
        "--paper_session_ledger_path",
        str(paper_session_ledger_path.resolve()),
    ]
    proc = subprocess.run(cmd, cwd=str(repo_root), capture_output=True, text=True, check=False)
    verdict_refs, pointer_refs = _find_per_sleeve_refs(day_utc=day_utc)

    final_payload = dict(base_payload)
    final_payload["trigger_ref"] = str(consumed_trigger_ref.path)
    final_payload["open_command_executed"] = True
    final_payload["submit_stage_entered"] = True
    final_payload["finished_at_utc"] = now_utc_iso_v1()
    final_payload["result_code"] = f"ORCHESTRATOR_RC_{int(proc.returncode)}"
    final_payload["reason_codes"] = [f"ORCHESTRATOR_RC={int(proc.returncode)}"]
    final_payload["direct_evidence_refs"] = {
        "sleeve_rollup_v1": str(rollup_path),
        "orchestrator_run_verdict_v2": verdict_refs,
        "run_pointer_v1": pointer_refs,
    }

    rollup_payload = None
    if rollup_path.exists() and rollup_path.is_file():
        rollup_payload = read_json_object_v1(rollup_path)
    rollup_status = str((rollup_payload or {}).get("status") or "").strip().upper()
    if proc.returncode == 0 and rollup_status in {"PASS", "DEGRADED"}:
        final_payload["final_classification"] = "OPEN_SUCCEEDED"
    else:
        final_payload["final_classification"] = "OPEN_FAILED"
        if rollup_status:
            final_payload["reason_codes"].append(f"SLEEVE_ROLLUP_STATUS={rollup_status}")
        if proc.stderr.strip():
            final_payload["reason_codes"].append("ORCHESTRATOR_STDERR_PRESENT")

    final_payload["open_policy"] = build_day_open_policy_snapshot(
        environment=normalized_environment,
        window_status=str(trigger_payload.get("open_window_status") or "").strip().upper(),
        trigger_payload=json.loads(consumed_trigger_ref.path.read_text(encoding="utf-8")),
        attempt_payload=final_payload,
        attempt_path=resolve_day_open_attempt_path(truth_root=root, day_utc=day_utc),
    )
    final_attempt_ref = _write_attempt(truth_root=root, payload=final_payload)
    _post_attempt_refresh(repo_root=repo_root, truth_root=root, day_utc=day_utc)
    return final_attempt_ref
