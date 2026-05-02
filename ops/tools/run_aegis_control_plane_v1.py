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
    paths = _phase_paths(phase, ctx)
    pre_open = (ctx.truth_root / "reports" / "pre_open_bundle_v1" / ctx.day_utc / "pre_open_bundle.v1.json").resolve()
    all_paths = list(dict.fromkeys([*paths, pre_open]))
    payloads = _payloads(all_paths)
    codes: list[str] = []
    for _path, payload in payloads:
        codes.extend(_collect_codes(payload))
    owned = _owned_code(phase, codes)
    if owned:
        return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=[owned], evidence_paths=[str(path) for path in all_paths], reason=f"{owned} belongs to SESSION_AUTHORITY")
    day_row = _row_from_day_run_phase(phase, ctx, _legacy_phase_row("SESSION_AUTHORITY", phase_results))
    if day_row:
        return day_row
    if _legacy_phase_row("SESSION_AUTHORITY", phase_results).get("status") == "SKIPPED":
        return _phase_row(phase=phase, ctx=ctx, status="UNKNOWN", evidence_paths=[str(path) for path in all_paths], reason="session authority was not evaluated by day-run after an upstream block")
    session_payload = _read_json(paths[0]) if paths else {}
    authority_status = str(session_payload.get("authority_status") or "").strip().upper()
    if authority_status in {"GRANTED", "AUTHORIZED"}:
        return _phase_row(phase=phase, ctx=ctx, status="PASS", evidence_paths=[str(path) for path in all_paths])
    blocker = "SESSION_AUTHORITY_DENIED" if authority_status == "DENIED" else "SESSION_AUTHORITY_MISSING"
    return _phase_row(phase=phase, ctx=ctx, status="BLOCKING_CURRENT_RUN", blocker_codes=[blocker], evidence_paths=[str(path) for path in all_paths], reason=f"paper_session_authority={authority_status or 'MISSING'}")


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
        raw = _evaluate_phase(phase, ctx, phase_results)
        phase_order = int(phase.get("phase_order") or 999)
        if current is not None:
            if raw.get("blocker_codes"):
                diagnostic_findings.append(
                    {
                        "phase_id": raw["phase_id"],
                        "blocker_codes": raw.get("blocker_codes") or [],
                        "evidence_paths": raw.get("evidence_paths") or [],
                        "deferred_by_phase": current["phase_id"],
                    }
                )
            raw["status"] = "DEFERRED_BY_UPSTREAM_BLOCKER"
            raw["recovery_action"] = f"Deferred until {current['phase_id']} clears."
            raw["recovery_commands"] = []
            deferred_phases.append(raw["phase_id"])
        elif raw["status"] == "BLOCKING_CURRENT_RUN":
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
        "final_status": "NOT_READY" if current else "READY",
        "current_phase": str((current or {}).get("phase_id") or ""),
        "current_phase_order": current_phase_order,
        "canonical_blocker": canonical_blocker,
        "blocker_owner": _owner_for_phase(phases, str((current or {}).get("phase_id") or "")),
        "blocker_reason": str((current or {}).get("blocker_reason") or ""),
        "recovery_action": str((current or {}).get("recovery_action") or "No current blocker."),
        "recovery_commands": recovery_commands,
        "evidence_paths": list((current or {}).get("evidence_paths") or []),
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


def run_control_plane_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_control_plane_v1(ctx)
    path = control_plane_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
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
    args = parser.parse_args(argv)
    path, payload = run_control_plane_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["final_status"], "current_phase": payload["current_phase"], "canonical_blocker": payload["canonical_blocker"], "control_plane_path": str(path)}, sort_keys=True))
    return 0 if payload["final_status"] == "READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
