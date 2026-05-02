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
DOMAIN_REGISTRY_PATH = REPO_ROOT / "governance" / "02_REGISTRIES" / "aegis_readiness_domain_registry_v1.json"
SESSION_IDENTITY_FORBIDDEN_DEPENDENCIES = {
    "runtime_resilience_authority_v1",
    "broker_event_log",
    "broker_event_day_manifest_v1",
    "broker_supply_v1",
    "safety_state_authority_v1",
    "startup_materialization_input_convergence_v1",
    "paper_capital_seed",
    "operator_statement",
    "market_data_authority_v1",
    "feed_attestation_gate_v1",
    "trading_day_intent_generation_v1",
    "authorization_supply_v1",
    "global_kill_switch_state_v1",
    "trading_day_readiness_authority_v1",
    "submit_boundary_status_v1",
    "execution_evidence_v1",
}
SESSION_IDENTITY_FORBIDDEN_BLOCKERS = {
    "IB_DISCONNECTED",
    "BROKER_ACCOUNT_SUMMARY_MISSING",
    "BROKER_EVENT_LOG_MISSING",
    "NAV_INVALID",
    "SAFETY_STATE_NAV_INVALID",
    "CASH_LEDGER_SNAPSHOT_V1_MISSING",
    "OPERATOR_STATEMENT_MISSING",
    "OPERATOR_STATEMENT_V1_MISSING",
    "PAPER_CAPITAL_SEED_MISSING",
    "MARKET_DATA_AUTHORITY_BLOCKED",
    "MARKET_DATA_BLOCKED",
    "MISSING_REQUIRED_DATA",
    "FAL_STALE",
    "FEED_ATTESTATION_BLOCKED",
    "MISSING_REQUIRED_INPUTS",
    "POSITION_STATE_STALE",
    "AUTHORIZATION_GATE_NOT_PASS",
    "NO_ELIGIBLE_OPTION_STRUCTURE",
    "C2_KILL_SWITCH_ACTIVE",
    "C2_KILL_SWITCH_DEFAULT_ACTIVE_MISSING_INPUTS",
    "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE",
    "SUBMIT_BOUNDARY_NOT_AUTHORIZED",
    "SUBMIT_BOUNDARY_READINESS_POLICY_NOT_PASS",
    "EXECUTION_EVIDENCE_MISSING",
}


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


def load_readiness_domain_registry_v1() -> list[dict[str, Any]]:
    payload = _read_json(DOMAIN_REGISTRY_PATH)
    if not payload:
        raise RuntimeError(f"READINESS_DOMAIN_REGISTRY_MISSING:{DOMAIN_REGISTRY_PATH}")
    domains = payload.get("domains") if isinstance(payload.get("domains"), list) else []
    rows = [row for row in domains if isinstance(row, dict)]
    out = sorted(rows, key=lambda row: int(row.get("domain_order") or 999))
    _validate_readiness_domain_registry_v1(out)
    return out


def _validate_readiness_domain_registry_v1(domains: list[dict[str, Any]]) -> None:
    if not domains:
        raise RuntimeError("READINESS_DOMAIN_REGISTRY_EMPTY")
    owners: dict[str, str] = {}
    for domain in domains:
        domain_id = str(domain.get("domain_id") or "").strip()
        if not domain_id:
            raise RuntimeError("READINESS_DOMAIN_WITHOUT_ID")
        dependencies = domain.get("dependencies") if isinstance(domain.get("dependencies"), list) else []
        for dep in dependencies:
            if not isinstance(dep, dict):
                raise RuntimeError(f"READINESS_DOMAIN_INVALID_DEPENDENCY:{domain_id}")
            dependency_id = str(dep.get("dependency_id") or "").strip()
            owner = str(dep.get("domain_owner") or "").strip()
            owning_domain = str(dep.get("owning_domain") or "").strip()
            if not dependency_id:
                raise RuntimeError(f"READINESS_DEPENDENCY_WITHOUT_ID:{domain_id}")
            if not owner:
                raise RuntimeError(f"READINESS_DEPENDENCY_WITHOUT_OWNER:{dependency_id}")
            if owner != domain_id:
                raise RuntimeError(f"READINESS_DEPENDENCY_OWNER_MISMATCH:{dependency_id}:{owner}!={domain_id}")
            if owning_domain != domain_id:
                raise RuntimeError(f"READINESS_DEPENDENCY_OWNING_DOMAIN_MISMATCH:{dependency_id}:{owning_domain}!={domain_id}")
            if dependency_id in owners:
                raise RuntimeError(f"READINESS_DEPENDENCY_MULTIPLE_OWNERS:{dependency_id}:{owners[dependency_id]}:{owner}")
            owners[dependency_id] = owner
            for key in (
                "expected_path",
                "artifact_path",
                "schema_path",
                "producer_command",
                "governed_producer",
                "recovery_action",
                "recovery_command",
                "blocking_scope",
            ):
                if dep.get(key) in (None, ""):
                    raise RuntimeError(f"READINESS_DEPENDENCY_CONTRACT_INCOMPLETE:{dependency_id}:{key}")
            if str(dep.get("artifact_path") or "").strip() != str(dep.get("expected_path") or "").strip():
                raise RuntimeError(f"READINESS_DEPENDENCY_ARTIFACT_PATH_MISMATCH:{dependency_id}")
            if str(dep.get("governed_producer") or "").strip() != str(dep.get("producer_command") or "").strip():
                raise RuntimeError(f"READINESS_DEPENDENCY_PRODUCER_MISMATCH:{dependency_id}")
            if owner == "SESSION_IDENTITY":
                if dependency_id in SESSION_IDENTITY_FORBIDDEN_DEPENDENCIES:
                    raise RuntimeError(f"SESSION_IDENTITY_FORBIDDEN_DEPENDENCY:{dependency_id}")
                owned_codes = {str(code).strip() for code in (dep.get("blocker_codes_owned") or []) if str(code or "").strip()}
                leaked = sorted(owned_codes & SESSION_IDENTITY_FORBIDDEN_BLOCKERS)
                if leaked:
                    raise RuntimeError(f"SESSION_IDENTITY_FORBIDDEN_BLOCKER:{dependency_id}:{','.join(leaked)}")


def _registered_dependency_owners(domains: list[dict[str, Any]]) -> dict[str, str]:
    owners: dict[str, str] = {}
    for domain in domains:
        for dep in domain.get("dependencies") or []:
            if isinstance(dep, dict):
                owners[str(dep.get("dependency_id") or "").strip()] = str(dep.get("domain_owner") or "").strip()
    return owners


def _assert_evaluated_dependencies_registered(inventory: list[dict[str, Any]], owners: dict[str, str]) -> None:
    for row in inventory:
        dependency_id = str(row.get("dependency_id") or "").strip()
        owner = str(row.get("domain_owner") or "").strip()
        owning_domain = str(row.get("owning_domain") or "").strip()
        if not dependency_id or dependency_id not in owners:
            raise RuntimeError(f"READINESS_DEPENDENCY_NOT_REGISTERED:{dependency_id or '<blank>'}")
        if owner != owners[dependency_id]:
            raise RuntimeError(f"READINESS_DEPENDENCY_EVALUATED_UNDER_WRONG_OWNER:{dependency_id}:{owner}!={owners[dependency_id]}")
        if owning_domain != owners[dependency_id]:
            raise RuntimeError(f"READINESS_DEPENDENCY_EVALUATED_UNDER_WRONG_OWNING_DOMAIN:{dependency_id}:{owning_domain}!={owners[dependency_id]}")


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


def _current_git_commit_v1() -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return str(proc.stdout or "").strip() if proc.returncode == 0 else ""


def _nested_get(payload: dict[str, Any], path: tuple[str, ...]) -> str:
    value: Any = payload
    for key in path:
        if not isinstance(value, dict):
            return ""
        value = value.get(key)
    return str(value or "").strip()


def _artifact_metadata_issue_v1(
    *,
    payload: dict[str, Any],
    ctx: Any,
    dependency_id: str,
    require_producer_metadata: bool = False,
) -> tuple[str, str]:
    producer_contract = payload.get("producer_contract_v1") if isinstance(payload.get("producer_contract_v1"), dict) else {}
    producer = payload.get("producer") if isinstance(payload.get("producer"), dict) else {}
    git_commit = (
        str(producer_contract.get("code_version_git_commit") or "").strip()
        or str(producer.get("git_sha") or "").strip()
        or str(payload.get("git_commit") or payload.get("source_git_commit") or payload.get("code_version_git_commit") or "").strip()
    )
    if require_producer_metadata and not (producer_contract or producer or git_commit):
        return "PRODUCER_METADATA_MISSING", f"{dependency_id} has no governed producer metadata"
    current_commit = _current_git_commit_v1()
    if git_commit and current_commit and git_commit != current_commit:
        return "STALE_ARTIFACT_GIT_COMMIT_MISMATCH", f"artifact_git_commit={git_commit} current_git_commit={current_commit}"
    dirty_status = (
        str(producer_contract.get("source_dirty_status") or "").strip().upper()
        or str(payload.get("source_dirty_status") or "").strip().upper()
    )
    if dirty_status and dirty_status not in {"CLEAN", "PROTECTED"}:
        return "STALE_ARTIFACT_DIRTY_SOURCE", f"source_dirty_status={dirty_status}"
    truth_root = (
        str(payload.get("truth_root") or payload.get("runtime_truth_root") or payload.get("canonical_truth_root") or "").strip()
        or _nested_get(payload, ("truth_roots", "canonical_truth_root"))
        or _nested_get(payload, ("truth_roots", "truth_root"))
    )
    if truth_root:
        observed = Path(truth_root).expanduser().resolve()
        expected = Path(ctx.truth_root).expanduser().resolve()
        if observed != expected:
            return "TRUTH_ROOT_MISMATCH", f"artifact_truth_root={observed} evaluated_truth_root={expected}"
    runtime_root = str(payload.get("runtime_root") or "").strip() or _nested_get(payload, ("truth_roots", "runtime_root"))
    if runtime_root:
        observed_runtime = Path(runtime_root).expanduser().resolve()
        expected_runtime = Path(ctx.runtime_root).expanduser().resolve()
        if observed_runtime != expected_runtime:
            return "RUNTIME_ROOT_MISMATCH", f"artifact_runtime_root={observed_runtime} evaluated_runtime_root={expected_runtime}"
    generated_at = str(
        payload.get("generated_at_utc")
        or payload.get("generated_at")
        or payload.get("produced_at_utc")
        or payload.get("created_at_utc")
        or ""
    ).strip()
    if len(generated_at) >= 10 and generated_at[:10] != ctx.day_utc:
        return "STALE_ARTIFACT_GENERATED_AT_DAY_MISMATCH", f"generated_at_utc={generated_at} target_day={ctx.day_utc}"
    return "", ""


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


def _dependency_path(dep: dict[str, Any], ctx: Any) -> Path:
    return Path(_format_template(str(dep.get("artifact_path") or dep.get("expected_path") or ""), ctx)).expanduser().resolve()


def _dependency_command(dep: dict[str, Any], ctx: Any) -> str:
    return _format_template(str(dep.get("governed_producer") or dep.get("producer_command") or ""), ctx)


def _dependency_recovery_command(dep: dict[str, Any], ctx: Any) -> str:
    return _format_template(str(dep.get("recovery_command") or dep.get("producer_command") or ""), ctx)


def _dependency_action(dep: dict[str, Any], ctx: Any) -> str:
    return _format_template(str(dep.get("recovery_action") or ""), ctx)


def _code_matches(owned_code: str, observed_code: str) -> bool:
    owned = str(owned_code or "").strip()
    observed = str(observed_code or "").strip()
    if not owned or not observed:
        return False
    return observed == owned or observed.startswith(f"{owned}:") or f":{owned}" in observed


def _owned_dependency_blocker(dep: dict[str, Any], codes: list[str]) -> str:
    owned_codes = [str(code).strip() for code in (dep.get("blocker_codes_owned") or []) if str(code or "").strip()]
    for code in codes:
        for owned in owned_codes:
            if _code_matches(owned, code):
                return owned
    return ""


def _dependency_result(
    *,
    dep: dict[str, Any],
    ctx: Any,
    status: str,
    blocker: str = "",
    detail: str = "",
    path: Path | None = None,
    reason_codes: list[str] | None = None,
) -> dict[str, Any]:
    expected = path or _dependency_path(dep, ctx)
    command = _dependency_command(dep, ctx)
    recovery_command = _dependency_recovery_command(dep, ctx)
    action = _dependency_action(dep, ctx)
    dependency_id = str(dep.get("dependency_id") or "").strip()
    owning_domain = str(dep.get("owning_domain") or dep.get("domain_owner") or "").strip()
    normalized_reason_codes = [
        str(code).split(":", 1)[0].strip()
        for code in (reason_codes if reason_codes is not None else ([blocker] if blocker else []))
        if str(code or "").strip()
    ]
    normalized_reason_codes = list(dict.fromkeys(normalized_reason_codes))
    return {
        "dependency_id": dependency_id,
        "domain_owner": str(dep.get("domain_owner") or "").strip(),
        "owning_domain": owning_domain,
        "required_for": list(dep.get("required_for") if isinstance(dep.get("required_for"), list) else []),
        "blocking_scope": str(dep.get("blocking_scope") or "").strip(),
        "required": bool(dep.get("required") is True),
        "diagnostic_only": bool(dep.get("diagnostic_only") is True),
        "status": status,
        "expected_path": str(expected),
        "artifact_path": str(expected),
        "artifact": dependency_id,
        "schema_path": str(dep.get("schema_path") or "").strip(),
        "producer_command": command,
        "producer": command,
        "governed_producer": command,
        "recovery_action": action or f"Resolve {dependency_id}.",
        "recovery_command": recovery_command,
        "blocking_reason": blocker,
        "reason_codes": normalized_reason_codes,
        "evidence_path": str(expected),
        "detail": detail,
    }


def _payload_day(payload: dict[str, Any]) -> str:
    return str(
        payload.get("day_utc")
        or payload.get("target_day")
        or payload.get("active_day")
        or payload.get("trading_day")
        or ""
    ).strip()


def _session_identity_dependency_result(dep: dict[str, Any], ctx: Any) -> dict[str, Any]:
    path = _dependency_path(dep, ctx)
    dependency_id = str(dep.get("dependency_id") or "").strip()
    if not path.exists() or not path.is_file():
        return _dependency_result(
            dep=dep,
            ctx=ctx,
            status="MISSING",
            blocker=f"{dependency_id.upper()}_MISSING",
            detail="required session identity artifact is missing",
            path=path,
        )
    payload = _read_json(path)
    metadata_blocker, metadata_detail = _artifact_metadata_issue_v1(
        payload=payload,
        ctx=ctx,
        dependency_id=dependency_id,
    )
    if metadata_blocker:
        return _dependency_result(dep=dep, ctx=ctx, status="STALE", blocker=metadata_blocker, detail=metadata_detail, path=path)
    codes = _collect_codes(payload)
    owned = _owned_dependency_blocker(dep, codes)
    if dependency_id == "active_session_v1":
        day_ok = str(payload.get("target_day") or payload.get("active_day") or "").strip() == ctx.day_utc
        promoted = str(payload.get("promotion_state") or "").strip().upper() in {"PROMOTED", "PASS", "GRANTED"}
        rolled = str(payload.get("rollover_status") or "").strip().upper() == "ROLLED_OVER"
        if day_ok and (promoted or rolled):
            return _dependency_result(dep=dep, ctx=ctx, status="SATISFIED", path=path)
        return _dependency_result(dep=dep, ctx=ctx, status="FAIL", blocker=owned or "TARGET_DAY_DATE_MISMATCH", detail="active session is not bound to the target day", path=path)
    if dependency_id == "target_day_build_v1":
        hidden = payload.get("hidden_dependency_check_result") if isinstance(payload.get("hidden_dependency_check_result"), dict) else {}
        undeclared = [str(item).strip() for item in (hidden.get("undeclared_dependency_artifacts") or []) if str(item).strip()]
        if undeclared:
            return _dependency_result(dep=dep, ctx=ctx, status="FAIL", blocker="HIDDEN_DEPENDENCY_DETECTED", detail="undeclared session identity dependency", path=path)
        if _payload_day(payload) and _payload_day(payload) != ctx.day_utc:
            return _dependency_result(dep=dep, ctx=ctx, status="FAIL", blocker="TARGET_DAY_DATE_MISMATCH", detail=f"artifact day={_payload_day(payload)}", path=path)
        return _dependency_result(dep=dep, ctx=ctx, status="SATISFIED", path=path)
    if dependency_id == "target_day_admission_v1":
        if _payload_day(payload) and _payload_day(payload) != ctx.day_utc:
            return _dependency_result(dep=dep, ctx=ctx, status="FAIL", blocker="TARGET_DAY_DATE_MISMATCH", detail=f"artifact day={_payload_day(payload)}", path=path)
        if owned:
            return _dependency_result(dep=dep, ctx=ctx, status="FAIL", blocker=owned, detail="target-day admission identity blocker", path=path)
        return _dependency_result(dep=dep, ctx=ctx, status="SATISFIED", path=path)
    if dependency_id == "session_promotion_decision_v1":
        if _payload_day(payload) and _payload_day(payload) != ctx.day_utc:
            return _dependency_result(dep=dep, ctx=ctx, status="FAIL", blocker="TARGET_DAY_DATE_MISMATCH", detail=f"artifact day={_payload_day(payload)}", path=path)
        if owned and owned != "SESSION_PROMOTION_NOT_PROMOTED":
            return _dependency_result(dep=dep, ctx=ctx, status="FAIL", blocker=owned, detail="session promotion identity blocker", path=path)
        return _dependency_result(dep=dep, ctx=ctx, status="SATISFIED", path=path)
    if dependency_id in {"paper_session_authority_v1", "paper_session_bootstrap_v1"}:
        if _payload_day(payload) and _payload_day(payload) != ctx.day_utc:
            return _dependency_result(dep=dep, ctx=ctx, status="FAIL", blocker="TARGET_DAY_DATE_MISMATCH", detail=f"artifact day={_payload_day(payload)}", path=path)
        if owned:
            return _dependency_result(dep=dep, ctx=ctx, status="FAIL", blocker=owned, detail="session identity blocker", path=path, reason_codes=codes)
        if dependency_id == "paper_session_authority_v1":
            authority_status = str(payload.get("authority_status") or "").strip().upper()
            if authority_status != "GRANTED":
                codes = [str(code).strip() for code in (payload.get("blocking_reason_codes") or payload.get("reason_codes") or []) if str(code).strip()]
                blocker = codes[0] if codes else "PAPER_SESSION_AUTHORITY_NOT_GRANTED"
                return _dependency_result(
                    dep=dep,
                    ctx=ctx,
                    status="FAIL",
                    blocker=blocker,
                    detail=f"authority_status={authority_status or 'UNKNOWN'}",
                    path=path,
                    reason_codes=codes or [blocker],
                )
        if dependency_id == "paper_session_bootstrap_v1":
            bootstrap_status = str(payload.get("bootstrap_status") or payload.get("status") or "").strip().upper()
            if bootstrap_status not in {"PASS", "COMPLETE", "BOOTSTRAPPED", "READY"}:
                codes = [str(code).strip() for code in (payload.get("blocking_reason_codes") or payload.get("reason_codes") or payload.get("blocker_chain") or []) if str(code).strip()]
                blocker = "PAPER_SESSION_BOOTSTRAP_NOT_READY"
                detail = f"bootstrap_status={bootstrap_status or 'UNKNOWN'}"
                if codes:
                    detail = f"{detail} reason_codes={','.join(codes[:6])}"
                return _dependency_result(
                    dep=dep,
                    ctx=ctx,
                    status="FAIL",
                    blocker=blocker,
                    detail=detail,
                    path=path,
                    reason_codes=codes or [blocker],
                )
            producer_blocker, producer_detail = _artifact_metadata_issue_v1(
                payload=payload,
                ctx=ctx,
                dependency_id=dependency_id,
                require_producer_metadata=True,
            )
            if producer_blocker:
                return _dependency_result(dep=dep, ctx=ctx, status="FAIL", blocker=producer_blocker, detail=producer_detail, path=path)
        return _dependency_result(dep=dep, ctx=ctx, status="SATISFIED", path=path)
    if dependency_id == "market_calendar_day":
        if owned:
            return _dependency_result(dep=dep, ctx=ctx, status="FAIL", blocker=owned, detail="target-day admission calendar blocker", path=path)
        return _dependency_result(dep=dep, ctx=ctx, status="SATISFIED", path=path)
    return _dependency_result(dep=dep, ctx=ctx, status="SATISFIED", path=path)


def _generic_dependency_result(dep: dict[str, Any], ctx: Any) -> dict[str, Any]:
    path = _dependency_path(dep, ctx)
    dependency_id = str(dep.get("dependency_id") or "").strip()
    if not path.exists():
        return _dependency_result(
            dep=dep,
            ctx=ctx,
            status="MISSING",
            blocker=f"{dependency_id.upper()}_MISSING",
            detail="required dependency artifact is missing",
            path=path,
        )
    payload = _read_json(path) if path.is_file() else {}
    if payload:
        metadata_blocker, metadata_detail = _artifact_metadata_issue_v1(
            payload=payload,
            ctx=ctx,
            dependency_id=dependency_id,
        )
        if metadata_blocker:
            return _dependency_result(dep=dep, ctx=ctx, status="STALE", blocker=metadata_blocker, detail=metadata_detail, path=path)
    codes = _collect_codes(payload)
    owned = _owned_dependency_blocker(dep, codes)
    status = _status(payload) if payload else "PASS"
    if owned:
        return _dependency_result(dep=dep, ctx=ctx, status="FAIL", blocker=owned, detail=f"owned blocker observed in {dependency_id}", path=path)
    if status in {"FAIL", "FAILED", "BLOCKED", "DENIED", "NOT_READY"}:
        return _dependency_result(dep=dep, ctx=ctx, status="FAIL", blocker=f"{dependency_id.upper()}_BLOCKED", detail=f"artifact status={status}", path=path)
    return _dependency_result(dep=dep, ctx=ctx, status="SATISFIED", path=path)


def _evaluate_domain_dependency(dep: dict[str, Any], ctx: Any) -> dict[str, Any]:
    if str(dep.get("domain_owner") or "").strip() == "SESSION_IDENTITY":
        return _session_identity_dependency_result(dep, ctx)
    return _generic_dependency_result(dep, ctx)


def _domain_failures(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    failures = []
    for row in rows:
        if row.get("required") is not True or row.get("diagnostic_only") is True:
            continue
        if str(row.get("status") or "").strip().upper() not in {"SATISFIED", "NOT_APPLICABLE"}:
            failures.append(row)
    return failures


def _domain_phase_row(
    *,
    domain: dict[str, Any],
    ctx: Any,
    status: str,
    dependency_rows: list[dict[str, Any]],
    failures: list[dict[str, Any]] | None = None,
    deferred_by: str = "",
) -> dict[str, Any]:
    domain_id = str(domain.get("domain_id") or "")
    phase_id = str(domain.get("phase_id") or domain_id)
    failed = failures or []
    if deferred_by:
        return {
            "phase_id": phase_id,
            "domain_id": domain_id,
            "status": "DEFERRED_BY_UPSTREAM_DOMAIN",
            "blocker_codes": [],
            "evidence_paths": [str(row.get("expected_path") or "") for row in dependency_rows],
            "recovery_action": f"Deferred until {deferred_by} clears.",
            "recovery_commands": [],
            "blocker_reason": f"deferred by {deferred_by}",
            "dependency_results": dependency_rows,
        }
    blocker_codes = [str(row.get("blocking_reason") or "").split(":", 1)[0] for row in failed if str(row.get("blocking_reason") or "").strip()]
    if len(failed) > 1:
        blocker_codes = [f"{domain_id}_PRECHECK_FAILED"]
    evidence_paths = [str(row.get("evidence_path") or row.get("expected_path") or "") for row in failed] or [str(row.get("expected_path") or "") for row in dependency_rows]
    commands = [str(row.get("recovery_command") or row.get("producer_command") or "") for row in failed if str(row.get("recovery_command") or row.get("producer_command") or "").strip()]
    action = (
        f"Resolve all listed {domain_id} precheck failures, then rerun the control plane."
        if len(failed) > 1
        else str((failed[0] if failed else {}).get("recovery_action") or "No current blocker.")
    )
    return {
        "phase_id": phase_id,
        "domain_id": domain_id,
        "status": status,
        "blocker_codes": blocker_codes,
        "evidence_paths": list(dict.fromkeys(path for path in evidence_paths if path)),
        "recovery_action": action,
        "recovery_commands": list(dict.fromkeys(commands)),
        "blocker_reason": f"{len(failed)} required {domain_id} dependencies are missing or failed" if len(failed) > 1 else str((failed[0] if failed else {}).get("detail") or ""),
        "dependency_results": dependency_rows,
        "failed_dependencies": failed,
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
        "paper_session_authority": (
            ctx.truth_root
            / "reports"
            / "paper_session_authority_v1"
            / ctx.day_utc
            / "paper_session_authority.v1.json"
        ).resolve(),
        "paper_session_bootstrap": (
            ctx.truth_root
            / "reports"
            / "paper_session_bootstrap_v1"
            / ctx.day_utc
            / "paper_session_bootstrap.v1.json"
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


def _market_calendar_refresh_command(ctx: Any) -> str:
    return (
        f'PYTHONPATH="$PWD" python3 ops/tools/run_market_calendar_coverage_refresh_v1.py '
        f"--truth_root {ctx.truth_root} --required_target_day {ctx.day_utc} --mode REFRESH"
    )


def _bootstrap_command(ctx: Any) -> str:
    return (
        f'PYTHONPATH="$PWD" python3 ops/tools/run_paper_session_bootstrap_v1.py '
        f"--day_utc {ctx.day_utc} --truth_root {ctx.truth_root} "
        f"--operator_input_root {ctx.operator_input_root} --environment {ctx.environment} "
        f"--ib_account {ctx.ib_account}"
    )


def _compact_list(values: list[Any], *, limit: int = 6) -> str:
    items = [str(item).strip() for item in values if str(item or "").strip()]
    if not items:
        return ""
    suffix = "" if len(items) <= limit else f",...(+{len(items) - limit})"
    return ",".join(items[:limit]) + suffix


def _failed_build_dependency(build: dict[str, Any]) -> dict[str, str]:
    artifacts = build.get("artifact_results") if isinstance(build.get("artifact_results"), list) else []
    for row in artifacts:
        if not isinstance(row, dict):
            continue
        if str(row.get("result_status") or "").strip().upper() in {"FAIL", "BLOCKED", "MISSING", "STALE"}:
            dep = str(row.get("artifact_id") or row.get("artifact_name") or "target_day_build_v1").strip()
            producer = row.get("producer") if isinstance(row.get("producer"), dict) else {}
            blocker_codes = [str(code).strip() for code in (row.get("blocker_codes") or []) if str(code).strip()]
            blocker_code = blocker_codes[0] if blocker_codes else "PARTIAL_BUILD"
            command = str(producer.get("command") or producer.get("module") or "").strip()
            return {
                "dependency": dep,
                "command": command,
                "blocker_code": blocker_code,
                "artifact_path": str(row.get("canonical_path") or row.get("authority_path") or "").strip(),
            }
    required = build.get("required_artifacts") if isinstance(build.get("required_artifacts"), list) else []
    for row in required:
        if isinstance(row, dict):
            dep = str(row.get("artifact_id") or row.get("artifact_name") or row.get("canonical_path") or "").strip()
            if dep:
                return {"dependency": dep, "command": "", "blocker_code": "PARTIAL_BUILD", "artifact_path": ""}
    return {"dependency": "target_day_build_v1", "command": "", "blocker_code": "PARTIAL_BUILD", "artifact_path": ""}


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


def _schema_for_dependency(dependency_id: str) -> str:
    return {
        "active_session_v1": "governance/04_DATA/SCHEMAS/C2/REPORTS/active_session.v1.schema.json",
        "target_day_build_v1": "governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_build.v1.schema.json",
        "target_day_admission_v1": "governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_admission.v1.schema.json",
        "session_promotion_decision_v1": "governance/04_DATA/SCHEMAS/C2/REPORTS/session_promotion_decision.v1.schema.json",
        "paper_session_authority_v1": "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_authority.v1.schema.json",
        "paper_session_bootstrap_v1": "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_bootstrap.v1.schema.json",
        "pre_open_bundle_v1": "governance/04_DATA/SCHEMAS/C2/REPORTS/pre_open_bundle.v1.schema.json",
        "market_calendar_day": "governance/04_DATA/SCHEMAS/C2/MARKET_DATA/market_calendar.v1.schema.json",
        "runtime_resilience_authority_v1": "",
        "safety_state_authority_v1": "governance/04_DATA/SCHEMAS/C2/REPORTS/safety_state_authority.v1.schema.json",
        "trading_day_readiness_authority_v1": "governance/04_DATA/SCHEMAS/C2/REPORTS/trading_day_readiness_authority.v1.schema.json",
    }.get(dependency_id, "")


def _producer_command_for_dependency(dependency_id: str, ctx: Any) -> str:
    if dependency_id in {"target_day_build_v1", "target_day_admission_v1", "active_session_v1", "session_promotion_decision_v1", "paper_session_authority_v1"}:
        return _session_command(ctx, phase="all")
    if dependency_id == "paper_session_bootstrap_v1":
        return _bootstrap_command(ctx)
    if dependency_id == "market_calendar_day":
        return _market_calendar_refresh_command(ctx)
    if dependency_id == "runtime_resilience_authority_v1":
        return (
            f'PYTHONPATH="$PWD" python3 ops/tools/run_runtime_resilience_authority_v1.py '
            f"--day_utc {ctx.day_utc} --truth_root {ctx.truth_root}"
        )
    if dependency_id == "safety_state_authority_v1":
        return (
            f'PYTHONPATH="$PWD" python3 ops/tools/run_safety_state_authority_v1.py '
            f"--day_utc {ctx.day_utc} --truth_root {ctx.truth_root} --environment {ctx.environment} "
            f"--account {ctx.ib_account}"
        )
    if dependency_id == "trading_day_readiness_authority_v1":
        return (
            f'PYTHONPATH="$PWD" python3 ops/tools/run_trading_day_readiness_authority_v1.py '
            f"--target_day {ctx.day_utc} --truth_root {ctx.truth_root} --environment {ctx.environment}"
        )
    if dependency_id == "startup_materialization_input_convergence_v1":
        return _startup_convergence_command(ctx)
    return _session_command(ctx, phase="build")


def _recovery_action_for_dependency(dependency_id: str, blocker: str) -> str:
    if dependency_id == "market_calendar_day":
        return "Refresh governed market-calendar coverage, then rerun session authority."
    if dependency_id == "paper_session_bootstrap_v1":
        return "Run governed paper-session bootstrap, then rerun session authority."
    if dependency_id in {"runtime_resilience_authority_v1", "safety_state_authority_v1", "trading_day_readiness_authority_v1"}:
        return f"Produce or repair {dependency_id}, then rerun session authority."
    if dependency_id == "hidden_dependency_check":
        return "Declare the hidden dependency in the session authority inventory or remove the illegal undeclared consumer dependency."
    if blocker:
        return f"Resolve {blocker} for {dependency_id}, then rerun session authority."
    return f"Produce or repair {dependency_id}, then rerun session authority."


def _inventory_status_from_build_row(row: dict[str, Any]) -> str:
    required = bool(row.get("required") is True)
    result = str(row.get("result_status") or "").strip().upper()
    if not required and result != "PASS":
        return "NOT_APPLICABLE"
    if result == "PASS":
        return "SATISFIED"
    observed = str(row.get("observed_status") or "").strip().upper()
    schema = str(row.get("schema_status") or "").strip().upper()
    date_binding = str(row.get("date_binding_status") or "").strip().upper()
    freshness = str(row.get("freshness_status") or "").strip().upper()
    if observed == "MISSING" or schema == "MISSING" or date_binding == "MISSING":
        return "MISSING"
    if freshness in {"STALE", "EXPIRED"}:
        return "STALE"
    return "FAIL"


def _blocker_from_build_row(row: dict[str, Any], status: str) -> str:
    codes = [str(code).strip() for code in (row.get("blocker_codes") or []) if str(code).strip()]
    if codes:
        return codes[0]
    explicit = str(row.get("blocking_reason_code") or "").strip()
    if explicit:
        return explicit
    return "" if status in {"SATISFIED", "NOT_APPLICABLE"} else f"{str(row.get('artifact_id') or 'DEPENDENCY').upper()}_{status}"


def _inventory_row_from_build_row(row: dict[str, Any], ctx: Any) -> dict[str, Any]:
    dependency_id = str(row.get("artifact_id") or row.get("artifact_name") or "").strip()
    status = _inventory_status_from_build_row(row)
    blocker = _blocker_from_build_row(row, status)
    expected_path = str(row.get("canonical_path") or row.get("authority_path") or "").strip()
    producer = row.get("producer") if isinstance(row.get("producer"), dict) else {}
    command = str(producer.get("module") or "").strip() or _producer_command_for_dependency(dependency_id, ctx)
    return {
        "dependency_id": dependency_id,
        "required": bool(row.get("required") is True),
        "status": status,
        "expected_path": expected_path,
        "schema_path": str(row.get("schema_ref") or _schema_for_dependency(dependency_id)),
        "producer_command": command,
        "recovery_action": _recovery_action_for_dependency(dependency_id, blocker),
        "recovery_command": command,
        "blocking_reason": blocker,
        "evidence_path": expected_path,
    }


def _status_from_core_payload(*, dependency_id: str, path: Path, payload: dict[str, Any], ctx: Any) -> tuple[str, str]:
    if not path.exists() or not path.is_file():
        return "MISSING", f"{dependency_id.upper()}_MISSING"
    if dependency_id == "active_session_v1":
        ok = (
            str(payload.get("target_day") or payload.get("active_day") or "").strip() == ctx.day_utc
            and str(payload.get("promotion_state") or "").strip().upper() == "PROMOTED"
        )
        return ("SATISFIED", "") if ok else ("FAIL", str(payload.get("rollover_reason_code") or "ACTIVE_SESSION_NOT_PROMOTED").strip())
    if dependency_id == "target_day_build_v1":
        build_status = str(payload.get("build_status") or "").strip().upper()
        closure = str(payload.get("closure_status") or "").strip().upper()
        hidden_status = str((payload.get("hidden_dependency_check_result") or {}).get("status") or "").strip().upper()
        ok = (
            build_status in {"COMPLETE", "PASS"}
            and closure in {"", "CLOSED"}
            and hidden_status in {"", "PASS"}
        )
        hidden = payload.get("hidden_dependency_check_result") if isinstance(payload.get("hidden_dependency_check_result"), dict) else {}
        return ("SATISFIED", "") if ok else ("FAIL", str(hidden.get("blocking_reason_code") or "TARGET_DAY_BUILD_NOT_CLOSED").strip())
    if dependency_id == "target_day_admission_v1":
        ok = str(payload.get("admission_status") or "").strip().upper() in {"ADMIT", "PASS"}
        codes = [str(code).strip() for code in (payload.get("blocking_reason_codes") or []) if str(code).strip()]
        return ("SATISFIED", "") if ok else ("FAIL", codes[0] if codes else "TARGET_DAY_ADMISSION_BLOCKED")
    if dependency_id == "session_promotion_decision_v1":
        ok = str(payload.get("promotion_state") or "").strip().upper() in {"PROMOTED", "PASS"}
        codes = [str(code).strip() for code in (payload.get("blocked_reason_codes") or []) if str(code).strip()]
        return ("SATISFIED", "") if ok else ("FAIL", codes[0] if codes else "SESSION_PROMOTION_NOT_PROMOTED")
    if dependency_id == "paper_session_authority_v1":
        ok = str(payload.get("authority_status") or "").strip().upper() == "GRANTED"
        codes = [str(code).strip() for code in (payload.get("blocking_reason_codes") or []) if str(code).strip()]
        return ("SATISFIED", "") if ok else ("FAIL", codes[0] if codes else "PAPER_SESSION_AUTHORITY_NOT_GRANTED")
    if dependency_id == "paper_session_bootstrap_v1":
        status = str(payload.get("bootstrap_status") or payload.get("status") or "").strip().upper()
        ok = status in {"PASS", "COMPLETE", "BOOTSTRAPPED", "READY"}
        codes = [str(code).strip() for code in (payload.get("blocking_reason_codes") or payload.get("reason_codes") or []) if str(code).strip()]
        return ("SATISFIED", "") if ok else ("FAIL", codes[0] if codes else "PAPER_SESSION_BOOTSTRAP_NOT_READY")
    return "SATISFIED", ""


def _core_inventory_row(*, dependency_id: str, path: Path, required: bool, ctx: Any) -> dict[str, Any]:
    payload = _read_json(path)
    status, blocker = _status_from_core_payload(dependency_id=dependency_id, path=path, payload=payload, ctx=ctx)
    if not required and status != "SATISFIED":
        status = "NOT_APPLICABLE"
    command = _producer_command_for_dependency(dependency_id, ctx)
    return {
        "dependency_id": dependency_id,
        "required": required,
        "status": status,
        "expected_path": str(path),
        "schema_path": _schema_for_dependency(dependency_id),
        "producer_command": command,
        "recovery_action": _recovery_action_for_dependency(dependency_id, blocker),
        "recovery_command": command,
        "blocking_reason": "" if status in {"SATISFIED", "NOT_APPLICABLE"} else blocker,
        "evidence_path": str(path),
    }


def _session_dependency_inventory(ctx: Any) -> list[dict[str, Any]]:
    paths = _session_authority_paths(ctx)
    build = _read_json(paths["target_day_build"])
    admission = _read_json(paths["target_day_admission"])
    rows: list[dict[str, Any]] = []
    core_specs = (
        ("active_session_v1", paths["active_session"], True),
        ("target_day_build_v1", paths["target_day_build"], True),
        ("target_day_admission_v1", paths["target_day_admission"], True),
        ("session_promotion_decision_v1", paths["session_promotion_decision"], True),
        ("paper_session_authority_v1", paths["paper_session_authority"], True),
        ("paper_session_bootstrap_v1", paths["paper_session_bootstrap"], True),
    )
    for dependency_id, path, required in core_specs:
        rows.append(_core_inventory_row(dependency_id=dependency_id, path=path, required=required, ctx=ctx))
    artifact_results = build.get("artifact_results") if isinstance(build.get("artifact_results"), list) else []
    seen = {row["dependency_id"] for row in rows}
    for item in artifact_results:
        if not isinstance(item, dict):
            continue
        row = _inventory_row_from_build_row(item, ctx)
        dependency_id = str(row.get("dependency_id") or "").strip()
        if not dependency_id or dependency_id in seen:
            continue
        rows.append(row)
        seen.add(dependency_id)
    hidden = build.get("hidden_dependency_check_result") if isinstance(build.get("hidden_dependency_check_result"), dict) else {}
    if not hidden and isinstance(admission.get("hidden_dependency_check_result"), dict):
        hidden = admission["hidden_dependency_check_result"]
    undeclared = [str(item).strip() for item in (hidden.get("undeclared_dependency_artifacts") or []) if str(item).strip()]
    rows.append(
        {
            "dependency_id": "hidden_dependency_check",
            "required": True,
            "status": "FAIL" if undeclared else "SATISFIED",
            "expected_path": str(paths["target_day_build"]),
            "schema_path": "governance/04_DATA/SCHEMAS/C2/REPORTS/target_day_build.v1.schema.json",
            "producer_command": "constellation_2.common.session_authority_v1 hidden dependency check",
            "recovery_action": _recovery_action_for_dependency("hidden_dependency_check", "HIDDEN_DEPENDENCY_DETECTED"),
            "recovery_command": _session_command(ctx, phase="build"),
            "blocking_reason": f"HIDDEN_DEPENDENCY_DETECTED:{_compact_list(undeclared)}" if undeclared else "",
            "evidence_path": str(paths["target_day_build"]),
        }
    )
    return rows


def _required_session_inventory_failures(inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bad = []
    for row in inventory:
        if row.get("required") is not True:
            continue
        if str(row.get("status") or "").strip().upper() in {"SATISFIED", "NOT_APPLICABLE"}:
            continue
        bad.append(row)
    aggregate_ids = {"target_day_build_v1", "target_day_admission_v1", "session_promotion_decision_v1", "active_session_v1"}
    concrete = [row for row in bad if str(row.get("dependency_id") or "") not in aggregate_ids]
    return concrete if concrete else bad


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
    hidden_reason = str(hidden.get("blocking_reason_code") or "").strip().upper()
    undeclared = hidden.get("undeclared_dependency_artifacts") if isinstance(hidden.get("undeclared_dependency_artifacts"), list) else []
    if "HIDDEN_DEPENDENCY_DETECTED" in codes or hidden_reason == "HIDDEN_DEPENDENCY_DETECTED" or undeclared:
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
        dependency_detail = _failed_build_dependency(build)
        dependency = str(dependency_detail.get("dependency") or "target_day_build_v1")
        command = str(dependency_detail.get("command") or "")
        sub_blocker_code = str(dependency_detail.get("blocker_code") or "PARTIAL_BUILD")
        evidence_path = str(dependency_detail.get("artifact_path") or "") or str(paths["target_day_build"])
        details.append(
            {
                "sub_blocker_code": sub_blocker_code,
                "owning_artifact": "target_day_build_v1",
                "missing_or_failed_dependency": dependency,
                "producer_command": command or "constellation_2.common.session_authority_v1 target day build",
                "recovery_action": f"Produce or repair {dependency}, then rerun session authority.",
                "recovery_command": command or _session_command(ctx, phase="build"),
                "evidence_path": evidence_path,
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
    priority = {"HIDDEN_DEPENDENCY_DETECTED": 0, "PARTIAL_BUILD": 2, "REQUIRED_GATE_FAIL": 3}
    if not sub_blockers:
        return {}
    return sorted(sub_blockers, key=lambda row: priority.get(str(row.get("sub_blocker_code") or ""), 1))[0]


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
    inventory = _session_dependency_inventory(ctx)
    inventory_failures = _required_session_inventory_failures(inventory)
    if inventory_failures:
        if len(inventory_failures) > 1:
            blocker_code = "SESSION_AUTHORITY_PRECHECK_FAILED"
            reason = f"{len(inventory_failures)} required session dependencies are missing, stale, or failed"
            evidence = [
                str(row.get("evidence_path") or row.get("expected_path") or "")
                for row in inventory_failures
                if str(row.get("evidence_path") or row.get("expected_path") or "").strip()
            ]
            command_rows = [
                str(row.get("recovery_command") or row.get("producer_command") or "")
                for row in inventory_failures
                if str(row.get("recovery_command") or row.get("producer_command") or "").strip()
            ]
            current_sub = {
                "sub_blocker_code": blocker_code,
                "owning_artifact": "SESSION_AUTHORITY_PRECHECK",
                "missing_or_failed_dependency": ",".join(str(row.get("dependency_id") or "") for row in inventory_failures[:6]),
                "producer_command": command_rows[0] if command_rows else _session_command(ctx, phase="build"),
                "recovery_action": "Resolve all listed SESSION_AUTHORITY precheck failures, then rerun session authority.",
                "recovery_command": command_rows[0] if command_rows else _session_command(ctx, phase="build"),
                "evidence_path": evidence[0] if evidence else str(session_paths["target_day_build"]),
            }
            row = _phase_row(
                phase=phase,
                ctx=ctx,
                status="BLOCKING_CURRENT_RUN",
                blocker_codes=[blocker_code],
                evidence_paths=evidence or [str(session_paths["target_day_build"])],
                reason=reason,
            )
            row["session_dependency_inventory"] = inventory
            row["session_precheck_failures"] = inventory_failures
            row["session_sub_blockers"] = [
                {
                    "sub_blocker_code": str(item.get("blocking_reason") or item.get("status") or ""),
                    "owning_artifact": str(item.get("dependency_id") or ""),
                    "missing_or_failed_dependency": str(item.get("dependency_id") or ""),
                    "producer_command": str(item.get("producer_command") or ""),
                    "recovery_action": str(item.get("recovery_action") or ""),
                    "recovery_command": str(item.get("recovery_command") or ""),
                    "evidence_path": str(item.get("evidence_path") or item.get("expected_path") or ""),
                }
                for item in inventory_failures
            ]
            row["current_session_sub_blocker"] = current_sub
            row["recovery_action"] = str(current_sub["recovery_action"])
            row["recovery_commands"] = list(dict.fromkeys(command_rows))
            return row
        only = inventory_failures[0]
        blocker_code = str(only.get("blocking_reason") or "").split(":", 1)[0] or f"{str(only.get('dependency_id') or 'SESSION_DEPENDENCY').upper()}_{str(only.get('status') or 'FAIL')}"
        only_evidence_path = str(only.get("evidence_path") or only.get("expected_path") or session_paths["target_day_build"])
        current_sub = {
            "sub_blocker_code": blocker_code,
            "owning_artifact": str(only.get("dependency_id") or ""),
            "missing_or_failed_dependency": str(only.get("dependency_id") or ""),
            "producer_command": str(only.get("producer_command") or ""),
            "recovery_action": str(only.get("recovery_action") or ""),
            "recovery_command": str(only.get("recovery_command") or ""),
            "evidence_path": only_evidence_path,
        }
        row = _phase_row(
            phase=phase,
            ctx=ctx,
            status="BLOCKING_CURRENT_RUN",
            blocker_codes=[blocker_code],
            evidence_paths=[only_evidence_path],
            reason=str(only.get("dependency_id") or ""),
        )
        row["session_dependency_inventory"] = inventory
        row["session_precheck_failures"] = inventory_failures
        row["session_sub_blockers"] = [current_sub]
        row["current_session_sub_blocker"] = current_sub
        row["recovery_action"] = str(current_sub["recovery_action"])
        row["recovery_commands"] = [str(current_sub["recovery_command"])]
        return row
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
        row["session_dependency_inventory"] = inventory
        row["session_precheck_failures"] = inventory_failures
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
    domains = load_readiness_domain_registry_v1()
    registered_owners = _registered_dependency_owners(domains)
    control_phase_results: list[dict[str, Any]] = []
    domain_results: list[dict[str, Any]] = []
    deferred_phases: list[str] = []
    deferred_domains: list[str] = []
    diagnostic_findings: list[dict[str, Any]] = []
    readiness_inventory: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    current_phase_order = 0

    source_phase = next((phase for phase in phases if str(phase.get("phase_id") or "") == "SOURCE_INTEGRITY"), {})
    if source_phase:
        source_row = _evaluate_source_integrity(source_phase, ctx)
        control_phase_results.append(source_row)
        if source_row["status"] == "BLOCKING_CURRENT_RUN":
            current = source_row
            current_phase_order = int(source_phase.get("phase_order") or 0)

    for domain in domains:
        domain_id = str(domain.get("domain_id") or "")
        phase_id = str(domain.get("phase_id") or domain_id)
        dep_specs = [row for row in (domain.get("dependencies") or []) if isinstance(row, dict)]
        dependency_rows = [_evaluate_domain_dependency(dep, ctx) for dep in dep_specs]
        for row in dependency_rows:
            row["domain_order"] = int(domain.get("domain_order") or 999)
        readiness_inventory.extend(dependency_rows)
        if current is not None:
            raw = _domain_phase_row(
                domain=domain,
                ctx=ctx,
                status="DEFERRED_BY_UPSTREAM_DOMAIN",
                dependency_rows=dependency_rows,
                deferred_by=str(current.get("domain_id") or current.get("phase_id") or ""),
            )
            deferred_domains.append(domain_id)
            deferred_phases.append(phase_id)
            control_phase_results.append(raw)
            domain_results.append(raw)
            continue
        failures = _domain_failures(dependency_rows)
        raw = _domain_phase_row(
            domain=domain,
            ctx=ctx,
            status="BLOCKING_CURRENT_RUN" if failures else "PASS",
            dependency_rows=dependency_rows,
            failures=failures,
        )
        if failures:
            current = raw
            current_phase_order = int(domain.get("domain_order") or 0)
        control_phase_results.append(raw)
        domain_results.append(raw)

    current_blockers = (current or {}).get("blocker_codes") if isinstance((current or {}).get("blocker_codes"), list) else []
    canonical_blocker = str(current_blockers[0] if current_blockers else "")
    recovery_commands = list((current or {}).get("recovery_commands") or [])
    submit_row = next((row for row in control_phase_results if row.get("phase_id") == "SUBMIT_BOUNDARY"), {})
    submit_allowed = bool(submit_row.get("status") == "PASS" and current is None)
    if current is not None and submit_allowed:
        raise RuntimeError("CONTROL_PLANE_SUBMIT_ALLOWED_WHILE_NOT_READY")
    _assert_evaluated_dependencies_registered(readiness_inventory, registered_owners)
    failed_current = list((current or {}).get("failed_dependencies") if isinstance((current or {}).get("failed_dependencies"), list) else [])
    current_domain = str((current or {}).get("domain_id") or "")
    return {
        "schema_id": "aegis_control_plane",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "runtime_mode": runtime_mode_from_truth_root_v1(ctx.truth_root),
        "final_status": "NOT_READY" if current else "READY",
        "current_phase": str((current or {}).get("phase_id") or ""),
        "current_domain": current_domain or str((current or {}).get("phase_id") or ""),
        "current_phase_order": current_phase_order,
        "canonical_blocker": canonical_blocker,
        "blocker_owner": _owner_for_domain(domains, current_domain) or _owner_for_phase(phases, str((current or {}).get("phase_id") or "")),
        "blocker_reason": str((current or {}).get("blocker_reason") or ""),
        "domain_blocker_summary": str((current or {}).get("blocker_reason") or ""),
        "recovery_action": str((current or {}).get("recovery_action") or "No current blocker."),
        "recovery_commands": recovery_commands,
        "evidence_paths": list((current or {}).get("evidence_paths") or []),
        "failed_current_domain_dependencies": failed_current,
        "readiness_dependency_inventory": readiness_inventory,
        "current_session_sub_blocker": dict((current or {}).get("current_session_sub_blocker") or {}),
        "session_sub_blockers": list((current or {}).get("session_sub_blockers") or []),
        "session_dependency_inventory": [row for row in readiness_inventory if row.get("domain_owner") == "SESSION_IDENTITY"],
        "session_precheck_failures": [row for row in failed_current if row.get("domain_owner") == "SESSION_IDENTITY"],
        "phase_results": control_phase_results,
        "domain_results": domain_results,
        "deferred_phases": deferred_phases,
        "deferred_domains": deferred_domains,
        "diagnostic_findings": diagnostic_findings,
        "submit_allowed": submit_allowed,
        "generated_at_utc": _now_iso(),
        "phase_registry_path": str(REGISTRY_PATH),
        "readiness_domain_registry_path": str(DOMAIN_REGISTRY_PATH),
    }


def _owner_for_phase(phases: list[dict[str, Any]], phase_id: str) -> str:
    for phase in phases:
        if phase.get("phase_id") == phase_id:
            return str(phase.get("owner") or phase_id)
    return ""


def _owner_for_domain(domains: list[dict[str, Any]], domain_id: str) -> str:
    for domain in domains:
        if domain.get("domain_id") == domain_id:
            return str(domain.get("owner") or domain_id)
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
