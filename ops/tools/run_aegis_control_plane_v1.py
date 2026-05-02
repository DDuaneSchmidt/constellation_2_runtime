#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.aegis_runtime_mode_v1 import assert_candidate_cannot_write_production_v1, runtime_mode_from_truth_root_v1
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools.repo_protection_common_v1 import read_protection_status_v1

SCHEMA_VERSION = "aegis_control_plane.v1"
REGISTRY_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "aegis_control_plane_phase_registry_v1.json"


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


def control_plane_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "aegis_control_plane_v1" / day_utc / "control_plane.v1.json").resolve()


def load_phase_registry_v1() -> list[dict[str, Any]]:
    payload = _read_json(REGISTRY_PATH)
    phases = payload.get("phases") if isinstance(payload.get("phases"), list) else []
    rows = [row for row in phases if isinstance(row, dict)]
    return sorted(rows, key=lambda row: int(row.get("phase_order") or 999))


def _format_template(text: str, ctx: Any) -> str:
    return text.format(
        day_utc=ctx.day_utc,
        environment=ctx.environment,
        truth_root=str(ctx.truth_root),
        execution_root=str(ctx.execution_root),
        runtime_root=str(ctx.runtime_root),
        operator_input_root=str(ctx.operator_input_root),
        ib_account=str(ctx.ib_account),
    )


def _phase_paths(phase: dict[str, Any], ctx: Any) -> list[Path]:
    out: list[Path] = []
    for item in phase.get("required_artifacts") or []:
        text = str(item or "").strip()
        if text:
            out.append(Path(_format_template(text, ctx)).expanduser().resolve())
    return out


def _commands(phase: dict[str, Any], ctx: Any, key: str) -> list[str]:
    return [_format_template(str(item), ctx) for item in (phase.get(key) or []) if str(item or "").strip()]


def _source_repo_status() -> dict[str, Any]:
    proc = subprocess.run(
        ["git", "status", "--short"],
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    dirty = [line for line in str(proc.stdout or "").splitlines() if line.strip()]
    protection = read_protection_status_v1()
    return {
        "git_dirty_status": "DIRTY" if dirty else "CLEAN",
        "dirty_path_count": len(dirty),
        "canonical_repo_protection_status": str(protection.get("status") or "UNKNOWN").strip().upper(),
        "canonical_repo_protection_status_path": str(protection.get("protection_status_path") or "/home/node/constellation_runtime_data/repo_protection_v1/status.json"),
    }


def _collect_codes(value: Any) -> list[str]:
    codes: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"canonical_blocker", "first_blocker_code", "blocker", "reason_code"}:
                text = str(item or "").strip()
                if text:
                    codes.append(text)
            elif key in {"reason_codes", "blocking_reason_codes", "blocker_codes", "canonical_blockers"} and isinstance(item, list):
                codes.extend(str(row).strip() for row in item if str(row or "").strip())
            else:
                codes.extend(_collect_codes(item))
    elif isinstance(value, list):
        for item in value:
            codes.extend(_collect_codes(item))
    return list(dict.fromkeys(codes))


def _status(payload: dict[str, Any]) -> str:
    for key in ("status", "boundary_status", "authority_status", "bootstrap_status", "final_status"):
        text = str(payload.get(key) or "").strip().upper()
        if text:
            return text
    return "UNKNOWN"


def _phase_row(
    *,
    phase: dict[str, Any],
    ctx: Any,
    status: str,
    blocker_codes: list[str] | None = None,
    evidence_paths: list[str] | None = None,
    reason: str = "",
) -> dict[str, Any]:
    phase_id = str(phase.get("phase_id") or "")
    recovery_commands = _commands(phase, ctx, "recovery_commands")
    recovery_action = _recovery_action(phase_id, blocker_codes or [], recovery_commands)
    return {
        "phase_id": phase_id,
        "status": status,
        "blocker_codes": blocker_codes or [],
        "evidence_paths": list(dict.fromkeys(evidence_paths or [str(path) for path in _phase_paths(phase, ctx)])),
        "recovery_action": recovery_action,
        "recovery_commands": recovery_commands,
        "blocker_reason": reason,
    }


def _session_authority_paths(ctx: Any) -> dict[str, Path]:
    return {
        "active_session": (ctx.truth_root / "active_session_v1" / "current.json").resolve(),
        "target_day_build": (ctx.truth_root / "target_day_build_v1" / f"{ctx.day_utc}.json").resolve(),
        "target_day_admission": (ctx.truth_root / "target_day_admission_v1" / f"{ctx.day_utc}.json").resolve(),
        "session_promotion_decision": (
            ctx.truth_root
            / "reports"
            / "session_promotion_decision_v1"
            / ctx.day_utc
            / "session_promotion_decision.v1.json"
        ).resolve(),
        "pre_open_bundle": (
            ctx.truth_root
            / "reports"
            / "pre_open_bundle_v1"
            / ctx.day_utc
            / "pre_open_bundle.v1.json"
        ).resolve(),
    }


def _session_command(ctx: Any, *, phase: str = "all") -> str:
    return (
        f'PYTHONPATH="$PWD" python3 ops/tools/run_session_authority_v1.py '
        f"--target_day {ctx.day_utc} --truth_root {ctx.truth_root} "
        f"--environment {ctx.environment} --ib_account {ctx.ib_account} --phase {phase}"
    )


def _startup_convergence_command(ctx: Any) -> str:
    return (
        f'PYTHONPATH="$PWD" python3 ops/tools/run_startup_materialization_input_convergence_v1.py '
        f"--day_utc {ctx.day_utc} --truth_root {ctx.truth_root} --ib_account {ctx.ib_account}"
    )


def _compact_list(values: list[Any], *, limit: int = 6) -> str:
    items = [str(item).strip() for item in values if str(item or "").strip()]
    if not items:
        return ""
    suffix = "" if len(items) <= limit else f",...(+{len(items) - limit})"
    return ",".join(items[:limit]) + suffix


def _failed_build_dependency(build: dict[str, Any]) -> tuple[str, str]:
    artifacts = build.get("artifact_results") if isinstance(build.get("artifact_results"), list) else []
    for row in artifacts:
        if not isinstance(row, dict):
            continue
        if str(row.get("result_status") or "").strip().upper() in {"FAIL", "BLOCKED", "MISSING", "STALE"}:
            dep = str(row.get("artifact_id") or row.get("artifact_name") or "target_day_build_v1").strip()
            producer = row.get("producer") if isinstance(row.get("producer"), dict) else {}
            command = str(producer.get("module") or "").strip()
            return dep, command
    required = build.get("required_artifacts") if isinstance(build.get("required_artifacts"), list) else []
    for row in required:
        if isinstance(row, dict):
            dep = str(row.get("artifact_id") or row.get("artifact_name") or row.get("canonical_path") or "").strip()
            if dep:
                return dep, ""
    return "target_day_build_v1", ""


def _required_gate_dependency(admission: dict[str, Any], pre_open: dict[str, Any]) -> tuple[str, str]:
    chain = admission.get("blocker_chain") if isinstance(admission.get("blocker_chain"), list) else []
    for row in chain:
        if not isinstance(row, dict):
            continue
        if str(row.get("blocker_code") or "").strip() == "REQUIRED_GATE_FAIL":
            dep = str(row.get("artifact_id") or row.get("artifact_name") or row.get("artifact_path") or "").strip()
            return dep or "session_required_gate", str(row.get("artifact_path") or "").strip()
    codes = pre_open.get("blocking_reason_codes") if isinstance(pre_open.get("blocking_reason_codes"), list) else []
    if "REQUIRED_GATE_FAIL" in [str(item) for item in codes]:
        return "pre_open_bundle_v1", ""
    return "session_required_gate", ""


def _session_sub_blockers(ctx: Any) -> list[dict[str, Any]]:
    paths = _session_authority_paths(ctx)
    active = _read_json(paths["active_session"])
    build = _read_json(paths["target_day_build"])
    admission = _read_json(paths["target_day_admission"])
    promotion = _read_json(paths["session_promotion_decision"])
    pre_open = _read_json(paths["pre_open_bundle"])
    codes: list[str] = []
    for payload in (active, admission, promotion, pre_open):
        codes.extend(_collect_codes(payload))
    codes = list(dict.fromkeys(codes))
    details: list[dict[str, Any]] = []
    hidden = admission.get("hidden_dependency_check_result") if isinstance(admission.get("hidden_dependency_check_result"), dict) else {}
    if "HIDDEN_DEPENDENCY_DETECTED" in codes or str(hidden.get("status") or "").strip().upper() == "FAIL":
        undeclared = hidden.get("undeclared_dependency_artifacts") if isinstance(hidden.get("undeclared_dependency_artifacts"), list) else []
        failing = hidden.get("failing_producers") if isinstance(hidden.get("failing_producers"), list) else []
        dependency = (
            f"undeclared_dependency_artifacts={_compact_list(undeclared)}"
            if undeclared
            else f"failing_producers={_compact_list(failing)}"
            if failing
            else str(hidden.get("summary") or "hidden dependency check failed")
        )
        details.append(
            {
                "sub_blocker_code": "HIDDEN_DEPENDENCY_DETECTED",
                "owning_artifact": "target_day_admission_v1",
                "missing_or_failed_dependency": dependency,
                "producer_command": "constellation_2.common.session_authority_v1 hidden dependency check",
                "recovery_action": "Resolve undeclared session dependencies, then rerun governed session alignment.",
                "recovery_command": _session_command(ctx, phase="all"),
                "evidence_path": str(paths["target_day_admission"]),
            }
        )
    if "PARTIAL_BUILD" in codes or str(build.get("build_status") or "").strip().upper() == "BLOCKED":
        dependency, command = _failed_build_dependency(build)
        details.append(
            {
                "sub_blocker_code": "PARTIAL_BUILD",
                "owning_artifact": "target_day_build_v1",
                "missing_or_failed_dependency": dependency,
                "producer_command": command or "constellation_2.common.session_authority_v1 target day build",
                "recovery_action": "Complete the governed target-day build, then rerun session authority.",
                "recovery_command": _session_command(ctx, phase="build"),
                "evidence_path": str(paths["target_day_build"]),
            }
        )
    if "REQUIRED_GATE_FAIL" in codes:
        dependency, artifact_path = _required_gate_dependency(admission, pre_open)
        details.append(
            {
                "sub_blocker_code": "REQUIRED_GATE_FAIL",
                "owning_artifact": "target_day_admission_v1",
                "missing_or_failed_dependency": dependency,
                "producer_command": _startup_convergence_command(ctx) if dependency == "startup_materialization_input_convergence_v1" else _session_command(ctx, phase="build"),
                "recovery_action": "Regenerate the failed governed session prerequisite, then rerun session authority.",
                "recovery_command": _startup_convergence_command(ctx) if dependency == "startup_materialization_input_convergence_v1" else _session_command(ctx, phase="all"),
                "evidence_path": artifact_path or str(paths["target_day_admission"]),
            }
        )
    return details


def _primary_session_sub_blocker(sub_blockers: list[dict[str, Any]]) -> dict[str, Any]:
    priority = {"HIDDEN_DEPENDENCY_DETECTED": 0, "PARTIAL_BUILD": 1, "REQUIRED_GATE_FAIL": 2}
    if not sub_blockers:
        return {}
    return sorted(sub_blockers, key=lambda row: priority.get(str(row.get("sub_blocker_code") or ""), 99))[0]


def _recovery_action(phase_id: str, blockers: list[str], recovery_commands: list[str]) -> str:
    blocker = blockers[0] if blockers else ""
    if blocker == "TARGET_DAY_DATE_MISMATCH":
        return "Run governed session alignment for the target day; do not hand-edit session artifacts."
    if blocker == "BROKER_EVENT_LOG_MISSING":
        return "Start the broker observer, generate the broker event manifest, then rerun broker supply."
    if blocker == "C2_KILL_SWITCH_ACTIVE":
        return "Resolve the governed kill-switch authority inputs; do not deactivate manually without valid upstream evidence."
    if blocker in {"FAL_STALE", "STALE_LIQUIDITY_DATASET", "FEED_ATTESTATION_BLOCKED"}:
        return "Refresh governed feed/liquidity attestation evidence, then rerun the feed gate."
    if recovery_commands:
        return recovery_commands[0]
    return f"Resolve {phase_id} blocker and rerun the normal day-start pipeline."


def _owned_code(phase: dict[str, Any], codes: list[str]) -> str:
    owned = {str(code) for code in phase.get("blocker_codes_owned") or []}
    for code in codes:
        if code in owned:
            return code
        if code.startswith("AUTHORIZATION_GATE_NOT_PASS") and "AUTHORIZATION_GATE_NOT_PASS" in owned:
            return "AUTHORIZATION_GATE_NOT_PASS"
    return ""


def _payloads(paths: list[Path]) -> list[tuple[Path, dict[str, Any]]]:
    return [(path, _read_json(path)) for path in paths if path.exists() and path.is_file()]


def _evaluate_source_integrity(phase: dict[str, Any], ctx: Any) -> dict[str, Any]:
    source = _source_repo_status()
    paths = [str(path) for path in _phase_paths(phase, ctx)]
    if source["git_dirty_status"] != "CLEAN":
        return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=["SOURCE_REPRODUCIBILITY_BLOCKED"], evidence_paths=paths, reason="canonical repo has dirty source state")
    if source["canonical_repo_protection_status"] != "PROTECTED":
        return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=["CANONICAL_REPO_PROTECTION_BLOCKED"], evidence_paths=paths, reason="canonical repo protection is not PROTECTED")
    return _phase_row(phase=phase, ctx=ctx, status="PASS", evidence_paths=paths)


def _legacy_phase_row(phase_id: str, phase_results: dict[str, dict[str, Any]] | None) -> dict[str, Any]:
    if not phase_results:
        return {}
    aliases = {
        "MARKET_DATA": ["MARKET_DATA", "MARKET_DATA_BOD_PREP", "MARKET_OPEN_DATA_GATE"],
        "AUTHORIZATION": ["AUTHORIZATION", "AUTHORIZATION_FINAL", "AUTHORIZATION_PREP"],
    }
    candidates = [phase_id, *aliases.get(phase_id, [])]
    for candidate in candidates:
        row = phase_results.get(candidate)
        if isinstance(row, dict) and str(row.get("status") or "").strip().upper() in {"BLOCKED", "PENDING"}:
            return row
    for candidate in candidates:
        row = phase_results.get(candidate)
        if isinstance(row, dict):
            return row
    return {}


def _row_from_day_run_phase(phase: dict[str, Any], ctx: Any, row: dict[str, Any]) -> dict[str, Any]:
    status = str(row.get("status") or "").strip().upper()
    if status in {"", "SKIPPED"}:
        return {}
    phase_id = str(phase.get("phase_id") or "")
    evidence = [str(item) for item in [*(row.get("inputs") or []), *(row.get("outputs") or [])] if str(item or "").strip()]
    blocker = str(row.get("canonical_blocker") or "").strip()
    if status in {"BLOCKED", "PENDING"}:
        return _phase_row(
            phase=phase,
            ctx=ctx,
            status="BLOCKING_CURRENT_RUN",
            blocker_codes=[blocker or f"{phase_id}_BLOCKED"],
            evidence_paths=evidence or [str(path) for path in _phase_paths(phase, ctx)],
            reason=str(row.get("blocker_detail") or status),
        )
    if status == "PASS":
        return _phase_row(phase=phase, ctx=ctx, status="PASS", evidence_paths=evidence or [str(path) for path in _phase_paths(phase, ctx)])
    return {}


def _evaluate_session_authority(phase: dict[str, Any], ctx: Any, phase_results: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    session_paths = _session_authority_paths(ctx)
    all_paths = list(session_paths.values())
    payloads = _payloads(all_paths)
    codes: list[str] = []
    for _path, payload in payloads:
        codes.extend(_collect_codes(payload))
    sub_blockers = _session_sub_blockers(ctx)
    primary_sub = _primary_session_sub_blocker(sub_blockers)
    if primary_sub:
        row = _phase_row(
            phase=phase,
            ctx=ctx,
            status="BLOCKING_CURRENT_RUN",
            blocker_codes=[str(primary_sub["sub_blocker_code"])],
            evidence_paths=[str(primary_sub["evidence_path"])],
            reason=str(primary_sub["missing_or_failed_dependency"]),
        )
        row["session_sub_blockers"] = sub_blockers
        row["current_session_sub_blocker"] = primary_sub
        row["recovery_action"] = str(primary_sub["recovery_action"])
        row["recovery_commands"] = [str(primary_sub["recovery_command"])]
        return row
    owned = _owned_code(phase, codes)
    if owned:
        return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=[owned], evidence_paths=[str(path) for path in all_paths], reason=f"{owned} belongs to SESSION_AUTHORITY")
    day_row = _row_from_day_run_phase(phase, ctx, _legacy_phase_row("SESSION_AUTHORITY", phase_results))
    if day_row:
        return day_row
    if _legacy_phase_row("SESSION_AUTHORITY", phase_results).get("status") == "SKIPPED":
        return _phase_row(
            phase=phase,
            ctx=ctx,
            status="BLOCKING_CURRENT_RUN",
            blocker_codes=["SESSION_AUTHORITY_MISSING"],
            evidence_paths=[str(path) for path in all_paths],
            reason="session authority was not evaluated by day-run; production session authority evidence is required before downstream phases",
        )
    active_payload = _read_json(session_paths["active_session"])
    promotion_state = str(active_payload.get("promotion_state") or "").strip().upper()
    rollover_status = str(active_payload.get("rollover_status") or "").strip().upper()
    if promotion_state in {"PROMOTED", "PASS", "GRANTED"} or rollover_status == "ROLLED_OVER":
        return _phase_row(phase=phase, ctx=ctx, status="PASS", evidence_paths=[str(path) for path in all_paths])
    if active_payload or payloads:
        blocker = str(active_payload.get("rollover_reason_code") or active_payload.get("first_blocker_code") or "SESSION_AUTHORITY_DENIED").strip()
        return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=[blocker], evidence_paths=[str(path) for path in all_paths], reason=f"promotion_state={promotion_state or 'UNKNOWN'} rollover_status={rollover_status or 'UNKNOWN'}")
    return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=["SESSION_AUTHORITY_MISSING"], evidence_paths=[str(path) for path in all_paths], reason="session authority artifacts are missing")


def _evaluate_broker_health(phase: dict[str, Any], ctx: Any) -> dict[str, Any]:
    paths = _phase_paths(phase, ctx)
    payloads = _payloads(paths)
    codes: list[str] = []
    for _path, payload in payloads:
        codes.extend(_collect_codes(payload))
    owned = _owned_code(phase, codes)
    if owned:
        return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=[owned], evidence_paths=[str(path) for path in paths], reason=f"{owned} belongs to BROKER_HEALTH")
    log_path = paths[0] if paths else Path()
    supply_payload = _read_json(paths[2]) if len(paths) > 2 else {}
    if log_path and not log_path.exists():
        return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=["BROKER_EVENT_LOG_MISSING"], evidence_paths=[str(path) for path in paths], reason="required broker event log is missing")
    if _status(supply_payload) == "PASS":
        return _phase_row(phase=phase, ctx=ctx, status="PASS", evidence_paths=[str(path) for path in paths])
    return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=["BROKER_SUPPLY_BLOCKED"], evidence_paths=[str(path) for path in paths], reason=f"broker_supply_status={_status(supply_payload)}")


def _evaluate_bod_inputs(phase: dict[str, Any], ctx: Any) -> dict[str, Any]:
    paths = _phase_paths(phase, ctx)
    missing = [path for path in paths if not path.exists()]
    if missing:
        by_name = {
            "paper_capital_seed.v1.json": "PAPER_CAPITAL_SEED_MISSING",
            "operator_statement.v1.json": "OPERATOR_STATEMENT_MISSING",
            "pre_open_bundle.v1.json": "PRE_OPEN_BUNDLE_INCOMPLETE",
        }
        blocker = by_name.get(missing[0].name, "PRE_OPEN_BUNDLE_INCOMPLETE")
        return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=[blocker], evidence_paths=[str(path) for path in paths], reason=f"missing {missing[0]}")
    codes: list[str] = []
    for _path, payload in _payloads(paths):
        codes.extend(_collect_codes(payload))
    owned = _owned_code(phase, codes)
    if owned:
        return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=[owned], evidence_paths=[str(path) for path in paths], reason=f"{owned} belongs to BOD_INPUTS")
    return _phase_row(phase=phase, ctx=ctx, status="PASS", evidence_paths=[str(path) for path in paths])


def _evaluate_generic_payload_phase(phase: dict[str, Any], ctx: Any) -> dict[str, Any]:
    paths = _phase_paths(phase, ctx)
    payloads = _payloads(paths)
    codes: list[str] = []
    for _path, payload in payloads:
        codes.extend(_collect_codes(payload))
    owned = _owned_code(phase, codes)
    if owned:
        return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=[owned], evidence_paths=[str(path) for path in paths], reason=f"{owned} belongs to {phase.get('phase_id')}")
    for path, payload in payloads:
        status = _status(payload)
        if status in {"FAIL", "FAILED", "BLOCKED", "DENIED", "NOT_READY"}:
            fallback = f"{phase.get('phase_id')}_BLOCKED"
            return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=[fallback], evidence_paths=[str(path) for path in paths], reason=f"{path.name} status={status}")
    if payloads:
        return _phase_row(phase=phase, ctx=ctx, status="PASS", evidence_paths=[str(path) for path in paths])
    return _phase_row(phase=phase, ctx=ctx, status="UNKNOWN", evidence_paths=[str(path) for path in paths], reason="no phase artifact available")


def _evaluate_kill_switch(phase: dict[str, Any], ctx: Any, phase_results: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    paths = _phase_paths(phase, ctx)
    payloads = _payloads(paths)
    codes: list[str] = []
    for _path, payload in payloads:
        codes.extend(_collect_codes(payload))
        state = str(payload.get("state") or payload.get("kill_switch_state") or "").strip().upper()
        allow_entries = payload.get("allow_entries")
        if state and state != "INACTIVE":
            return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=["C2_KILL_SWITCH_ACTIVE"], evidence_paths=[str(path) for path in paths], reason=f"kill_switch_state={state}")
        if allow_entries is False and state != "INACTIVE":
            return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=["C2_KILL_SWITCH_ACTIVE"], evidence_paths=[str(path) for path in paths], reason="kill switch does not allow entries")
    owned = _owned_code(phase, codes)
    if owned:
        return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=[owned], evidence_paths=[str(path) for path in paths], reason=f"{owned} belongs to KILL_SWITCH")
    if payloads:
        return _phase_row(phase=phase, ctx=ctx, status="PASS", evidence_paths=[str(path) for path in paths])
    if phase_results is not None:
        return _phase_row(phase=phase, ctx=ctx, status="PASS", evidence_paths=[str(path) for path in paths], reason="no kill-switch artifact was emitted before the day-run stopped")
    return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=["KILL_SWITCH_STATE_MISSING"], evidence_paths=[str(path) for path in paths], reason="kill switch state artifact missing")


def _evaluate_submit_boundary(phase: dict[str, Any], ctx: Any) -> dict[str, Any]:
    paths = _phase_paths(phase, ctx)
    payload = _read_json(paths[0]) if paths else {}
    if payload.get("submission_authorized") is True or payload.get("submit_allowed") is True:
        return _phase_row(phase=phase, ctx=ctx, status="PASS", evidence_paths=[str(path) for path in paths])
    codes = _collect_codes(payload)
    owned = _owned_code(phase, codes) or "SUBMIT_BOUNDARY_NOT_AUTHORIZED"
    return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=[owned], evidence_paths=[str(path) for path in paths], reason=f"submit_boundary_status={_status(payload)}")


def _evaluate_execution(phase: dict[str, Any], ctx: Any) -> dict[str, Any]:
    paths = _phase_paths(phase, ctx)
    root = paths[0] if paths else Path()
    if root.exists() and any(root.rglob("broker_submission_record.v2.json")):
        return _phase_row(phase=phase, ctx=ctx, status="PASS", evidence_paths=[str(path) for path in paths])
    return _phase_row(phase=phase, ctx=ctx, status="PASS", evidence_paths=[str(path) for path in paths], reason="no submission evidence required before governed submit")


def _evaluate_phase(phase: dict[str, Any], ctx: Any, phase_results: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    phase_id = str(phase.get("phase_id") or "")
    if phase_id == "SOURCE_INTEGRITY":
        day_row = _row_from_day_run_phase(phase, ctx, _legacy_phase_row(phase_id, phase_results))
        if day_row:
            return day_row
        return _evaluate_source_integrity(phase, ctx)
    if phase_id == "SESSION_AUTHORITY":
        return _evaluate_session_authority(phase, ctx, phase_results)
    day_row = _row_from_day_run_phase(phase, ctx, _legacy_phase_row(phase_id, phase_results))
    if day_row:
        return day_row
    if phase_id == "BROKER_HEALTH":
        return _evaluate_broker_health(phase, ctx)
    if phase_id == "BOD_INPUTS":
        return _evaluate_bod_inputs(phase, ctx)
    if phase_id == "KILL_SWITCH":
        return _evaluate_kill_switch(phase, ctx, phase_results)
    if phase_id == "SUBMIT_BOUNDARY":
        return _evaluate_submit_boundary(phase, ctx)
    if phase_id == "EXECUTION":
        return _evaluate_execution(phase, ctx)
    return _evaluate_generic_payload_phase(phase, ctx)


def build_control_plane_v1(ctx: Any, phase_results: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    phases = load_phase_registry_v1()
    control_phase_results: list[dict[str, Any]] = []
    deferred_phases: list[str] = []
    diagnostic_findings: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    current_phase_order = 0

    for phase in phases:
        phase_order = int(phase.get("phase_order") or 999)
        if current is not None:
            raw = _phase_row(
                phase=phase,
                ctx=ctx,
                status="DEFERRED_BY_UPSTREAM_BLOCKER",
                blocker_codes=[],
                evidence_paths=[str(path) for path in _phase_paths(phase, ctx)],
                reason=f"deferred by {current['phase_id']}",
            )
            raw["recovery_action"] = f"Deferred until {current['phase_id']} clears."
            raw["recovery_commands"] = []
            deferred_phases.append(raw["phase_id"])
            control_phase_results.append(raw)
            continue
        raw = _evaluate_phase(phase, ctx, phase_results)
        if raw["status"] == "BLOCKING_CURRENT_RUN":
            current = raw
            current_phase_order = phase_order
        control_phase_results.append(raw)

    current_blockers = (current or {}).get("blocker_codes") if isinstance((current or {}).get("blocker_codes"), list) else []
    canonical_blocker = str(current_blockers[0] if current_blockers else "")
    recovery_commands = list((current or {}).get("recovery_commands") or [])
    submit_row = next((row for row in control_phase_results if row.get("phase_id") == "SUBMIT_BOUNDARY"), {})
    submit_allowed = bool(submit_row.get("status") == "PASS" and current is None)
    return {
        "schema_id": "aegis_control_plane",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "runtime_mode": runtime_mode_from_truth_root_v1(ctx.truth_root),
        "final_status": "NOT_READY" if current else "READY",
        "current_phase": str((current or {}).get("phase_id") or ""),
        "current_phase_order": current_phase_order,
        "canonical_blocker": canonical_blocker,
        "blocker_owner": _owner_for_phase(phases, str((current or {}).get("phase_id") or "")),
        "blocker_reason": str((current or {}).get("blocker_reason") or ""),
        "recovery_action": str((current or {}).get("recovery_action") or "No current blocker."),
        "recovery_commands": recovery_commands,
        "evidence_paths": list((current or {}).get("evidence_paths") or []),
        "current_session_sub_blocker": dict((current or {}).get("current_session_sub_blocker") or {}),
        "session_sub_blockers": list((current or {}).get("session_sub_blockers") or []),
        "phase_results": control_phase_results,
        "deferred_phases": deferred_phases,
        "diagnostic_findings": diagnostic_findings,
        "submit_allowed": submit_allowed,
        "generated_at_utc": _now_iso(),
        "phase_registry_path": str(REGISTRY_PATH),
    }


def _owner_for_phase(phases: list[dict[str, Any]], phase_id: str) -> str:
    for phase in phases:
        if phase.get("phase_id") == phase_id:
            return str(phase.get("owner") or phase_id)
    return ""


def run_control_plane_v1(day_utc: str, environment: str, truth_root: str = "", runtime_mode: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    mode = runtime_mode_from_truth_root_v1(ctx.truth_root, runtime_mode)
    payload = build_control_plane_v1(ctx)
    payload["runtime_mode"] = mode
    path = control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    assert_candidate_cannot_write_production_v1(runtime_mode=mode, output_path=path)
    input_paths: list[str] = [str(REGISTRY_PATH)]
    for row in payload["phase_results"]:
        input_paths.extend(str(item) for item in row.get("evidence_paths", []) if str(item or "").strip())
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_aegis_control_plane_v1.py",
        producer_command=f"python3 ops/tools/run_aegis_control_plane_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}",
        input_artifacts=input_paths,
        output_artifacts=[path],
        schema_versions={"aegis_control_plane": SCHEMA_VERSION},
    )
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_control_plane_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--runtime_mode", default="")
    args = parser.parse_args(argv)
    path, payload = run_control_plane_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""), str(args.runtime_mode or ""))
    print(json.dumps({"status": payload["final_status"], "current_phase": payload["current_phase"], "canonical_blocker": payload["canonical_blocker"], "control_plane_path": str(path)}, sort_keys=True))
    return 0 if payload["final_status"] == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
