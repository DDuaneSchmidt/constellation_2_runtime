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
from typing import Any


DEFAULT_RUNTIME_ROOT = Path("/home/node/constellation_runtime_data")
DEFAULT_ACTIVE_LINK = Path("/home/node/constellation_active")
PAPER_SERVICE = "aegis-paper-ready-kernel-v1.service"
PAPER_TIMER = "aegis-paper-ready-kernel-v1.timer"
KERNEL_TOOL = "ops/tools/run_aegis_paper_ready_kernel_v1.py"

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
KERNEL_STAGE_ORDER = {
    "broker_supply": 1,
    "cash_ledger_from_broker": 2,
    "positions_snapshot": 3,
    "accounting_nav": 4,
    "accounting_nav_compat_bridge": 5,
    "capital_supply": 6,
    "trading_day_intent_generation": 7,
    "portfolio_activation_gate": 8,
    "portfolio_scoring": 9,
    "intent_arbitration": 10,
    "risk_budget_supply": 11,
    "market_open_data_gate": 12,
    "structure_decision_supply": 13,
    "capital_authority_allocation": 14,
    "phasec_identity_materializer": 15,
    "authorization_artifacts": 16,
    "authorization_supply": 17,
    "global_kill_switch": 18,
    "trading_day_readiness_authority": 19,
    "submit_boundary_status": 20,
}
KERNEL_STAGE_GATE_STATE = {
    "trading_day_intent_generation": "INTENT_AVAILABLE",
    "intent_arbitration": "INTENT_SELECTED",
    "trading_day_readiness_authority": "TRADING_DAY_MODE_VALID",
    "risk_budget_supply": "RISK_VALID",
    "market_open_data_gate": "MARKET_SESSION_VALID",
    "authorization_supply": "AUTHORIZATION_VALID",
    "global_kill_switch": "KILL_SWITCH_CLEAR",
    "submit_boundary_status": "SUBMIT_BOUNDARY_READY",
}


@dataclass(frozen=True)
class ArtifactEvidence:
    logical_name: str
    path: str
    exists: bool
    timestamp: str = ""
    stale_status: str = ""
    reason: str = ""


@dataclass(frozen=True)
class GateResult:
    order: int
    state: str
    status: str
    reason_code: str
    explanation: str
    artifact_path: str
    artifact_timestamp: str
    stale_status: str
    next_safe_action: str
    classification: str


@dataclass(frozen=True)
class NoTradeExplanation:
    day_utc: str
    active_release_id: str
    first_blocker: str
    first_blocker_reason: str
    classification: str
    next_safe_action: str
    ordered_gate_results: tuple[GateResult, ...]
    stale_artifacts: tuple[ArtifactEvidence, ...]
    submit_allowed: bool
    submission_authorized: bool
    broker_transmit_enabled: bool
    order_submission_attempted: bool


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _timestamp(payload: dict[str, Any]) -> str:
    for key in (
        "generated_at_utc",
        "produced_at_utc",
        "produced_utc",
        "created_at_utc",
        "decision_timestamp_utc",
        "decided_at_utc",
        "activated_at_utc",
    ):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    lineage = payload.get("constitutional_lineage")
    if isinstance(lineage, dict):
        for key in ("generated_at_utc", "effective_at_utc"):
            value = str(lineage.get(key) or "").strip()
            if value:
                return value
    return ""


def _reason_codes(payload: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for key in (
        "reason_codes",
        "blocking_codes",
        "blocking_reason_codes",
        "hard_blockers",
        "failed_reason_codes",
    ):
        values = payload.get(key)
        if isinstance(values, list):
            out.extend(str(value).strip() for value in values if str(value).strip())
    return list(dict.fromkeys(out))


def _day_from_payload(payload: dict[str, Any]) -> str:
    for key in ("day_utc", "target_day", "current_day"):
        value = str(payload.get(key) or "").strip()
        if len(value) == 10:
            return value
    return ""


def _artifact(
    *,
    logical_name: str,
    path: Path,
    day_utc: str,
    required_timestamp: bool = True,
) -> tuple[dict[str, Any], ArtifactEvidence]:
    payload = _read_json(path) if path.is_file() else {}
    if not path.is_file():
        return payload, ArtifactEvidence(
            logical_name=logical_name,
            path=str(path),
            exists=False,
            stale_status="MISSING",
            reason="artifact missing",
        )

    observed_day = _day_from_payload(payload)
    ts = _timestamp(payload)
    stale_status = "CURRENT"
    reason = ""
    if observed_day and observed_day != day_utc:
        stale_status = "STALE_DAY_MISMATCH"
        reason = f"observed_day={observed_day} expected_day={day_utc}"
    elif required_timestamp and not ts:
        stale_status = "TIMESTAMP_MISSING"
        reason = "artifact has no recognized timestamp"

    return payload, ArtifactEvidence(
        logical_name=logical_name,
        path=str(path),
        exists=True,
        timestamp=ts,
        stale_status=stale_status,
        reason=reason,
    )


def _classification_for_missing_or_stale(evidence: ArtifactEvidence, default: str) -> str:
    if evidence.stale_status == "MISSING":
        return "MISSING_EVIDENCE"
    if evidence.stale_status and evidence.stale_status != "CURRENT":
        return "STALE_ARTIFACT"
    return default


def _evidence_current(evidence: ArtifactEvidence) -> bool:
    return evidence.exists and (not evidence.stale_status or evidence.stale_status == "CURRENT")


def _gate(
    order: int,
    state: str,
    status: str,
    reason_code: str,
    explanation: str,
    evidence: ArtifactEvidence | None,
    next_safe_action: str,
    classification: str,
) -> GateResult:
    evidence = evidence or ArtifactEvidence(logical_name=state, path="", exists=False)
    return GateResult(
        order=order,
        state=state,
        status=status,
        reason_code=reason_code,
        explanation=explanation,
        artifact_path=evidence.path,
        artifact_timestamp=evidence.timestamp,
        stale_status=evidence.stale_status,
        next_safe_action=next_safe_action,
        classification=classification,
    )


def _truth_root(runtime_root: Path) -> Path:
    return runtime_root / "truth"


def _sleeve_root(runtime_root: Path) -> Path:
    return runtime_root / "truth_sleeves" / "PRIMARY" / "PAPER"


def _discover_day(runtime_root: Path, explicit_day: str) -> str:
    if explicit_day:
        return explicit_day
    candidates: list[Path] = []
    for root in (
        _truth_root(runtime_root) / "reports" / "submit_boundary_status_v1",
        _truth_root(runtime_root) / "reports" / "aegis_paper_ready_kernel_v1",
        _sleeve_root(runtime_root) / "reports" / "market_open_data_gate_v1",
    ):
        if root.is_dir():
            candidates.extend(path for path in root.iterdir() if path.is_dir() and len(path.name) == 10)
    if candidates:
        return sorted(path.name for path in candidates)[-1]
    return datetime.now(UTC).date().isoformat()


def _load_journal_lines(day_utc: str, *, no_journal: bool = False) -> list[str]:
    if no_journal:
        return []
    try:
        proc = subprocess.run(
            [
                "journalctl",
                "--user",
                "-u",
                PAPER_SERVICE,
                "--since",
                f"{day_utc} 00:00:00",
                "--no-pager",
                "-o",
                "short-iso",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except Exception:
        return []
    if proc.returncode not in (0, 1):
        return []
    return [line for line in proc.stdout.splitlines() if line.strip()]


def _latest_service_run(lines: list[str]) -> dict[str, Any]:
    if not lines:
        return {"timer_fired": False, "kernel_started": False, "lines": []}
    start_indexes = [i for i, line in enumerate(lines) if f"Starting {PAPER_SERVICE}" in line]
    if not start_indexes:
        return {"timer_fired": False, "kernel_started": False, "lines": lines[-20:]}
    run_lines = lines[start_indexes[-1] :]
    failed = any("Failed to start" in line or "Failed with result" in line or "can't open file" in line for line in run_lines)
    finished = any(f"Finished {PAPER_SERVICE}" in line for line in run_lines)
    return {
        "timer_fired": True,
        "kernel_started": not failed and finished,
        "lines": run_lines,
        "started_at": _journal_timestamp(run_lines[0]),
        "failure_line": next((line for line in run_lines if "can't open file" in line or "Failed with result" in line), ""),
    }


def _journal_timestamp(line: str) -> str:
    token = line.split(" ", 1)[0].strip()
    try:
        value = datetime.fromisoformat(token.replace("Z", "+00:00"))
    except ValueError:
        return ""
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _kernel_completed_from_artifact(payload: dict[str, Any], evidence: ArtifactEvidence, *, latest_start_utc: str = "") -> bool:
    if not _evidence_current(evidence):
        return False
    final_status = str(payload.get("final_status") or "").strip().upper()
    completed = (
        str(payload.get("schema_version") or "").strip() == "aegis_paper_ready_kernel.v1"
        and payload.get("scheduled_run") is True
        and final_status in {"PAPER_READY", "MARKET_NOT_OPEN", "BLOCKED"}
    )
    if not completed:
        return False
    latest_start = _parse_utc(latest_start_utc) if latest_start_utc else None
    artifact_time = _parse_utc(evidence.timestamp) if evidence.timestamp else None
    if latest_start is not None:
        return artifact_time is not None and artifact_time >= latest_start
    return True


def _kernel_failed_before_stage(payload: dict[str, Any], stage_id: str) -> bool:
    if str(payload.get("final_status") or "").strip().upper() != "BLOCKED":
        return False
    failed_stage_id = str(payload.get("failed_stage_id") or "").strip()
    failed_order = KERNEL_STAGE_ORDER.get(failed_stage_id)
    target_order = KERNEL_STAGE_ORDER.get(stage_id)
    return isinstance(failed_order, int) and isinstance(target_order, int) and failed_order < target_order


def _kernel_preferred_blocker_gate(kernel_report: dict[str, Any], gates: list[GateResult]) -> GateResult | None:
    if str(kernel_report.get("final_status") or "").strip().upper() != "BLOCKED":
        return None
    failed_stage_id = str(kernel_report.get("failed_stage_id") or "").strip()
    state = KERNEL_STAGE_GATE_STATE.get(failed_stage_id)
    if not state:
        return None
    return next((gate for gate in gates if gate.state == state and gate.status == STATUS_FAIL), None)


def _kernel_start_blocker(failure_line: str) -> str:
    text = failure_line.lower()
    if "can't open file" in text or "no such file or directory" in text or "exec" in text:
        return "PAPER_READY_KERNEL_DID_NOT_START"
    return "PAPER_READY_KERNEL_FAILED"


def _read_submit_boundary(runtime_root: Path, day_utc: str) -> tuple[dict[str, Any], ArtifactEvidence]:
    path = _truth_root(runtime_root) / "reports" / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json"
    return _artifact(logical_name="submit_boundary_status_v1", path=path, day_utc=day_utc)


def _bool_from(payload: dict[str, Any], key: str) -> bool:
    return bool(payload.get(key) is True)


def _has_order_submission_attempt(runtime_root: Path, day_utc: str) -> bool:
    roots = [
        _truth_root(runtime_root) / "execution_evidence_v1" / "submissions" / day_utc,
        _sleeve_root(runtime_root) / "execution_evidence_v1" / "submissions" / day_utc,
    ]
    for root in roots:
        if not root.is_dir():
            continue
        for path in root.rglob("broker_submit_attempt_v1.json"):
            if path.is_file():
                return True
    return False


def _first_reason(payload: dict[str, Any], fallback: str) -> str:
    codes = _reason_codes(payload)
    return codes[0] if codes else fallback


def _add_stale_warnings(gates: list[GateResult]) -> tuple[ArtifactEvidence, ...]:
    out: list[ArtifactEvidence] = []
    for gate in gates:
        if gate.stale_status and gate.stale_status != "CURRENT":
            out.append(
                ArtifactEvidence(
                    logical_name=gate.state,
                    path=gate.artifact_path,
                    exists=gate.stale_status != "MISSING",
                    timestamp=gate.artifact_timestamp,
                    stale_status=gate.stale_status,
                    reason=gate.reason_code,
                )
            )
    return tuple(out)


def build_no_trade_explanation_v1(
    *,
    runtime_root: Path = DEFAULT_RUNTIME_ROOT,
    active_link: Path = DEFAULT_ACTIVE_LINK,
    day_utc: str = "",
    journal_lines: list[str] | None = None,
    no_journal: bool = False,
) -> NoTradeExplanation:
    runtime_root = Path(runtime_root).resolve()
    active_link = Path(active_link)
    day_utc = _discover_day(runtime_root, day_utc)
    truth = _truth_root(runtime_root)
    sleeve = _sleeve_root(runtime_root)
    gates: list[GateResult] = []

    current_release_path = truth / "releases" / "current_release.v1.json"
    current_release, current_release_evidence = _artifact(
        logical_name="current_release_v1",
        path=current_release_path,
        day_utc=day_utc,
        required_timestamp=False,
    )
    active_release_id = str(current_release.get("release_id") or "").strip()
    release_path = Path(str(current_release.get("release_path") or "")).expanduser()
    manifest_path = release_path / "release_manifest.v1.json"
    expected_manifest_hash = str(current_release.get("release_manifest_hash") or "").strip()
    release_valid = bool(_evidence_current(current_release_evidence) and active_release_id and release_path.is_dir() and manifest_path.is_file())
    if expected_manifest_hash and manifest_path.is_file():
        import hashlib

        release_valid = release_valid and hashlib.sha256(manifest_path.read_bytes()).hexdigest() == expected_manifest_hash
    gates.append(
        _gate(
            1,
            "RELEASE_VALID",
            STATUS_PASS if release_valid else STATUS_FAIL,
            "" if release_valid else "ACTIVE_RELEASE_INVALID",
            "Active release manifest is valid." if release_valid else "Active release pointer or release manifest is missing/invalid.",
            current_release_evidence,
            "Repair current release pointer or rebuild/activate a valid governed release.",
            _classification_for_missing_or_stale(current_release_evidence, "BUG"),
        )
    )

    active_target = ""
    try:
        active_target = str(active_link.resolve(strict=True))
        launcher_valid = bool(release_path and active_target == str(release_path.resolve()))
    except Exception:
        launcher_valid = False
    gates.append(
        _gate(
            2,
            "LAUNCHER_VALID",
            STATUS_PASS if launcher_valid else STATUS_FAIL,
            "" if launcher_valid else ("LAUNCHER_RESOLVES_WRONG_RELEASE" if active_target else "ACTIVE_RELEASE_LINK_MISSING"),
            "Current-release launcher target matches active release."
            if launcher_valid
            else f"Launcher/active symlink does not match current release pointer: active={active_target or 'MISSING'} expected={release_path}",
            ArtifactEvidence("constellation_active", str(active_link), bool(active_target), stale_status="CURRENT" if active_target else "MISSING"),
            "Repair current-release pointer/active symlink so the launcher resolves the intended release.",
            "BUG" if active_target else "MISSING_EVIDENCE",
        )
    )

    kernel_path = release_path / KERNEL_TOOL
    kernel_exists = kernel_path.is_file()
    gates.append(
        _gate(
            3,
            "KERNEL_EXECUTABLE_PRESENT",
            STATUS_PASS if kernel_exists else STATUS_FAIL,
            "" if kernel_exists else "KERNEL_EXECUTABLE_MISSING",
            "Paper-ready kernel executable exists in the resolved release."
            if kernel_exists
            else "Paper-ready kernel executable is missing from the resolved release.",
            ArtifactEvidence("paper_ready_kernel_executable", str(kernel_path), kernel_exists, stale_status="CURRENT" if kernel_exists else "MISSING"),
            "Restore the runtime tool to canonical source, build a release, and activate it through the governed path.",
            "BUG" if not kernel_exists else "EXPECTED_SAFETY",
        )
    )

    run = _latest_service_run(journal_lines if journal_lines is not None else _load_journal_lines(day_utc, no_journal=no_journal))
    timer_fired = bool(run.get("timer_fired") is True)
    gates.append(
        _gate(
            4,
            "TIMER_FIRED",
            STATUS_PASS if timer_fired else STATUS_FAIL,
            "" if timer_fired else "PAPER_READY_TIMER_DID_NOT_FIRE",
            "Scheduled paper-ready timer fired." if timer_fired else "No scheduled paper-ready service start was found for the target day.",
            ArtifactEvidence("systemd_journal", f"journalctl --user -u {PAPER_SERVICE}", timer_fired, stale_status="CURRENT" if timer_fired else "MISSING"),
            f"Check `systemctl --user status {PAPER_TIMER} --no-pager` and wait for or repair the scheduled timer.",
            "MISSING_EVIDENCE",
        )
    )

    kernel_report_path = truth / "reports" / "aegis_paper_ready_kernel_v1" / day_utc / "paper_ready_kernel.v1.json"
    kernel_report, kernel_report_evidence = _artifact(
        logical_name="paper_ready_kernel_v1",
        path=kernel_report_path,
        day_utc=day_utc,
    )
    kernel_completed = _kernel_completed_from_artifact(
        kernel_report,
        kernel_report_evidence,
        latest_start_utc=str(run.get("started_at") or ""),
    )
    kernel_started = bool(run.get("kernel_started") is True) or kernel_completed
    failure_line = str(run.get("failure_line") or "").strip()
    kernel_blocker = "" if kernel_started else _kernel_start_blocker(failure_line)
    kernel_evidence = (
        kernel_report_evidence
        if kernel_completed or kernel_report_evidence.exists
        else ArtifactEvidence("systemd_journal", f"journalctl --user -u {PAPER_SERVICE}", timer_fired, stale_status="CURRENT" if timer_fired else "MISSING")
    )
    gates.append(
        _gate(
            5,
            "KERNEL_STARTED",
            STATUS_PASS if kernel_started else STATUS_FAIL,
            kernel_blocker,
            "Paper-ready kernel started and completed."
            if kernel_started
            else (failure_line or "Paper-ready kernel failed before writing a valid scheduled-run artifact."),
            kernel_evidence,
            "Fix the service/launcher/runtime error, then wait for the next scheduled paper-ready run."
            if kernel_blocker == "PAPER_READY_KERNEL_DID_NOT_START"
            else "Fix the paper-ready kernel runtime failure, then wait for the next scheduled paper-ready run.",
            "BUG",
        )
    )

    intent_generation_path = sleeve / "reports" / "trading_day_intent_generation_v1" / day_utc / "trading_day_intent_generation.v1.json"
    intent_generation, intent_generation_evidence = _artifact(logical_name="trading_day_intent_generation_v1", path=intent_generation_path, day_utc=day_utc)
    intents_present = _evidence_current(intent_generation_evidence) and str(intent_generation.get("final_status") or intent_generation.get("status") or "").upper() in {"INTENTS_PRESENT", "PASS"}
    gates.append(
        _gate(6, "INTENT_AVAILABLE", STATUS_PASS if intents_present else STATUS_FAIL, "" if intents_present else "NO_INTENTS", "Trading-day intent generation produced intents." if intents_present else "No executable PAPER intent is available.", intent_generation_evidence, "Run or repair the governed PAPER intent generation pipeline.", _classification_for_missing_or_stale(intent_generation_evidence, "EXPECTED_SAFETY"))
    )

    selected_path = sleeve / "pointers" / "selected_intent_pointer.v1.json"
    selected, selected_evidence = _artifact(logical_name="selected_intent_pointer_v1", path=selected_path, day_utc=day_utc)
    selected_ok = _evidence_current(selected_evidence) and str(selected.get("status") or "").upper() == "SELECTED" and isinstance(selected.get("selected_intent"), dict)
    gates.append(
        _gate(7, "INTENT_SELECTED", STATUS_PASS if selected_ok else STATUS_FAIL, "" if selected_ok else "NO_SELECTED_INTENT", "Intent arbitration selected a PAPER intent." if selected_ok else "Intent arbitration has not selected a PAPER intent.", selected_evidence, "Run or repair intent arbitration and selected-intent pointer generation.", _classification_for_missing_or_stale(selected_evidence, "EXPECTED_SAFETY"))
    )

    trading_path = truth / "reports" / "trading_day_readiness_authority_v1" / day_utc / "trading_day_readiness_authority.v1.json"
    trading, trading_evidence = _artifact(logical_name="trading_day_readiness_authority_v1", path=trading_path, day_utc=day_utc)
    mode_ok = _evidence_current(trading_evidence) and bool(trading.get("submit_allowed_by_mode") is True)
    gates.append(
        _gate(8, "TRADING_DAY_MODE_VALID", STATUS_PASS if mode_ok else STATUS_FAIL, "" if mode_ok else str(trading.get("canonical_blocker") or "SUBMIT_NOT_ALLOWED_BY_TRADING_DAY_MODE"), "Trading-day mode allows submit." if mode_ok else "Trading-day readiness mode does not allow submit.", trading_evidence, "Refresh trading-day readiness during an intraday submit-eligible market session.", _classification_for_missing_or_stale(trading_evidence, "EXPECTED_SAFETY"))
    )

    market_path = sleeve / "reports" / "market_open_data_gate_v1" / day_utc / "market_open_data_gate.v1.json"
    market, market_evidence = _artifact(logical_name="market_open_data_gate_v1", path=market_path, day_utc=day_utc)
    market_ok = _evidence_current(market_evidence) and str(market.get("status") or "").upper() == "PASS" and not str(market.get("canonical_blocker") or "").strip()
    market_not_reached = kernel_completed and _kernel_failed_before_stage(kernel_report, "market_open_data_gate")
    gates.append(
        _gate(
            9,
            "MARKET_SESSION_VALID",
            STATUS_PASS if market_ok else STATUS_FAIL,
            "" if market_ok else ("MARKET_GATE_NOT_REACHED" if market_not_reached else str(market.get("canonical_blocker") or "MARKET_SESSION_INVALID")),
            "Market session/data gate passed."
            if market_ok
            else (
                f"Market-open data gate was not reached because the kernel stopped at {str(kernel_report.get('failed_stage_id') or '<unknown>')}."
                if market_not_reached
                else "Market session is not submit-valid or market-open data gate is blocking."
            ),
            market_evidence,
            f"Resolve {str(kernel_report.get('first_blocker') or 'the upstream kernel blocker')}, then wait for the next scheduled paper-ready run."
            if market_not_reached
            else "Wait for regular market session and require fresh quote-complete market data evidence.",
            "NOT_REACHED" if market_not_reached else _classification_for_missing_or_stale(market_evidence, "EXPECTED_SAFETY"),
        )
    )

    capture_attempted = _evidence_current(market_evidence) and bool(market.get("capture_attempted_by_gate") is True)
    gates.append(
        _gate(10, "MARKET_DATA_CAPTURE_ATTEMPTED", STATUS_PASS if capture_attempted else STATUS_FAIL, "" if capture_attempted else ("MARKET_DATA_CAPTURE_NOT_REACHED" if market_not_reached else "MARKET_DATA_CAPTURE_NOT_ATTEMPTED"), "Market data capture was attempted by the gate." if capture_attempted else ("Market data capture was not reached because an upstream kernel stage blocked." if market_not_reached else "Market data capture was not attempted by the gate."), market_evidence, f"Resolve {str(kernel_report.get('first_blocker') or 'the upstream kernel blocker')}, then wait for the next scheduled paper-ready run." if market_not_reached else "Run the scheduled market-open gate in a valid session with selected-intent symbol available.", "NOT_REACHED" if market_not_reached else _classification_for_missing_or_stale(market_evidence, "EXPECTED_SAFETY"))
    )

    supply_path = sleeve / "reports" / "market_data_supply_v1" / day_utc / "market_data_supply.v1.json"
    supply, supply_evidence = _artifact(logical_name="market_data_supply_v1", path=supply_path, day_utc=day_utc)
    quote_ok = _evidence_current(supply_evidence) and (
        str(supply.get("status") or "").upper() in {"PASS", "READY", "QUOTE_COMPLETE"}
        or str(market.get("quote_completeness_status") or "").upper() == "PASS"
    )
    gates.append(
        _gate(11, "QUOTE_COMPLETE", STATUS_PASS if quote_ok else STATUS_FAIL, "" if quote_ok else ("QUOTE_COMPLETENESS_NOT_REACHED" if market_not_reached else str(supply.get("canonical_blocker") or "QUOTE_COMPLETENESS_FAILED")), "Quote completeness passed." if quote_ok else ("Quote completeness was not reached because an upstream kernel stage blocked." if market_not_reached else "Fresh quote-complete market data evidence is missing or failed."), supply_evidence, f"Resolve {str(kernel_report.get('first_blocker') or 'the upstream kernel blocker')}, then wait for the next scheduled paper-ready run." if market_not_reached else "Produce fresh underlying/options quote-complete market data for the selected intent.", "NOT_REACHED" if market_not_reached else _classification_for_missing_or_stale(supply_evidence, "MISSING_EVIDENCE"))
    )

    runtime_path = truth / "reports" / "runtime_resilience_authority_v1" / day_utc / "runtime_resilience_authority.v1.json"
    runtime, runtime_evidence = _artifact(logical_name="runtime_resilience_authority_v1", path=runtime_path, day_utc=day_utc)
    runtime_codes = {code.upper() for code in _reason_codes(runtime)}
    broker_connected = _evidence_current(runtime_evidence) and "IB_DISCONNECTED" not in runtime_codes and str(runtime.get("ib_connection_state") or "").upper() in {"CONNECTED", "PRESENT"}
    gates.append(
        _gate(12, "BROKER_CONNECTED", STATUS_PASS if broker_connected else STATUS_FAIL, "" if broker_connected else str(runtime.get("canonical_blocker") or "IB_DISCONNECTED"), "IBKR PAPER connection is usable." if broker_connected else "IBKR PAPER connection is not proven usable.", runtime_evidence, "Restore IBKR Paper Gateway/TWS API connection, then refresh runtime resilience evidence.", _classification_for_missing_or_stale(runtime_evidence, "EXPECTED_SAFETY"))
    )

    account_ok = _evidence_current(runtime_evidence) and str(runtime.get("account_summary_state") or "").upper() == "PRESENT" and "ACCOUNT_SUMMARY_MISSING" not in runtime_codes
    gates.append(
        _gate(13, "ACCOUNT_SUMMARY_VALID", STATUS_PASS if account_ok else STATUS_FAIL, "" if account_ok else "ACCOUNT_SUMMARY_MISSING", "Account summary is present." if account_ok else "Account summary/account values are missing.", runtime_evidence, "Refresh broker account summary/account values through the governed PAPER broker evidence path.", _classification_for_missing_or_stale(runtime_evidence, "MISSING_EVIDENCE"))
    )

    safety_path = truth / "reports" / "safety_state_authority_v1" / day_utc / "safety_state_authority.v1.json"
    safety, safety_evidence = _artifact(logical_name="safety_state_authority_v1", path=safety_path, day_utc=day_utc)
    nav_ok = _evidence_current(safety_evidence) and bool(safety.get("nav_valid") is True) and "NAV_INVALID" not in {code.upper() for code in _reason_codes(safety)}
    gates.append(
        _gate(14, "NAV_VALID", STATUS_PASS if nav_ok else STATUS_FAIL, "" if nav_ok else "NAV_INVALID", "NAV is valid." if nav_ok else "NAV is missing, stale, zero, or otherwise invalid.", safety_evidence, "Refresh governed broker/cash/positions/NAV evidence, then rerun safety state authority.", _classification_for_missing_or_stale(safety_evidence, "EXPECTED_SAFETY"))
    )

    capital_path = sleeve / "reports" / "capital_supply_v1" / day_utc / "capital_supply.v1.json"
    capital, capital_evidence = _artifact(logical_name="capital_supply_v1", path=capital_path, day_utc=day_utc)
    capital_ok = _evidence_current(capital_evidence) and str(capital.get("status") or "").upper() == "PASS"
    gates.append(
        _gate(15, "CAPITAL_VALID", STATUS_PASS if capital_ok else STATUS_FAIL, "" if capital_ok else str(capital.get("canonical_blocker") or "CAPITAL_INVALID"), "Capital supply is valid." if capital_ok else "Capital evidence is missing or invalid.", capital_evidence, "Refresh capital supply and capital risk envelope from governed account/NAV evidence.", _classification_for_missing_or_stale(capital_evidence, "MISSING_EVIDENCE"))
    )

    risk_path = sleeve / "reports" / "risk_budget_supply_v1" / day_utc / "risk_budget_supply.v1.json"
    risk, risk_evidence = _artifact(logical_name="risk_budget_supply_v1", path=risk_path, day_utc=day_utc)
    risk_ok = _evidence_current(risk_evidence) and str(risk.get("status") or "").upper() == "PASS"
    gates.append(
        _gate(16, "RISK_VALID", STATUS_PASS if risk_ok else STATUS_FAIL, "" if risk_ok else str(risk.get("canonical_blocker") or "RISK_INVALID"), "Risk budget evidence is valid." if risk_ok else "Risk budget/sizing evidence is missing or invalid.", risk_evidence, "Refresh risk budget and risk sizing authority for the selected intent.", _classification_for_missing_or_stale(risk_evidence, "MISSING_EVIDENCE"))
    )

    auth_path = sleeve / "reports" / "authorization_gate_verdict_v1" / day_utc / "authorization_gate_verdict.v1.json"
    auth, auth_evidence = _artifact(logical_name="authorization_gate_verdict_v1", path=auth_path, day_utc=day_utc)
    auth_dir = sleeve / "engine_activity_v1" / "authorization_v1" / day_utc
    auth_ok = _evidence_current(auth_evidence) and str(auth.get("status") or "").upper() in {"PASS", "READY"} and auth_dir.is_dir() and any(auth_dir.rglob("*.json*"))
    gates.append(
        _gate(17, "AUTHORIZATION_VALID", STATUS_PASS if auth_ok else STATUS_FAIL, "" if auth_ok else _first_reason(auth, "AUTHORIZATION_MISSING"), "Selected intent is authorized." if auth_ok else "Selected intent authorization evidence is missing or failed.", auth_evidence, "Produce required authorization gates and governed selected-intent authorization evidence.", _classification_for_missing_or_stale(auth_evidence, "MISSING_EVIDENCE"))
    )

    kill_path = truth / "risk_v1" / "kill_switch_v1" / day_utc / "global_kill_switch_state.v1.json"
    kill, kill_evidence = _artifact(logical_name="global_kill_switch_state_v1", path=kill_path, day_utc=day_utc)
    kill_clear = _evidence_current(kill_evidence) and str(kill.get("state") or "").upper() == "INACTIVE" and bool(kill.get("allow_entries") is True)
    gates.append(
        _gate(18, "KILL_SWITCH_CLEAR", STATUS_PASS if kill_clear else STATUS_FAIL, "" if kill_clear else _first_reason(kill, "C2_KILL_SWITCH_ACTIVE"), "Kill switch is clear for PAPER entries." if kill_clear else "Kill switch is active or entries are not allowed.", kill_evidence, "Clear upstream authorization/session blockers, then refresh the governed kill-switch artifact.", _classification_for_missing_or_stale(kill_evidence, "EXPECTED_SAFETY"))
    )

    submit, submit_evidence = _read_submit_boundary(runtime_root, day_utc)
    submit_ready = _evidence_current(submit_evidence) and (
        str(submit.get("status") or "").upper() in {"READY", "PASS"} or str(submit.get("boundary_status") or "").upper() == "READY"
    )
    submit_allowed = _bool_from(submit, "submit_allowed") or _bool_from(submit, "can_submit_paper_orders")
    submission_authorized = _bool_from(submit, "submission_authorized")
    broker_transmit_enabled = _bool_from(submit, "broker_transmit_enabled")
    order_attempted = _has_order_submission_attempt(runtime_root, day_utc)

    gates.extend(
        [
            _gate(19, "SUBMIT_BOUNDARY_READY", STATUS_PASS if submit_ready else STATUS_FAIL, "" if submit_ready else str(submit.get("canonical_blocker") or "SUBMIT_BOUNDARY_NOT_READY"), "Submit boundary is ready." if submit_ready else "Submit boundary is not ready.", submit_evidence, "Refresh submit boundary after every upstream authority is current and passing.", _classification_for_missing_or_stale(submit_evidence, "EXPECTED_SAFETY")),
            _gate(20, "SUBMIT_ALLOWED_TRUE", STATUS_PASS if submit_allowed else STATUS_FAIL, "" if submit_allowed else "SUBMIT_ALLOWED_FALSE", "Submit boundary reports submit_allowed=true." if submit_allowed else "Submit boundary reports submit_allowed=false.", submit_evidence, "Do not submit; clear upstream blockers and refresh submit boundary.", "EXPECTED_SAFETY"),
            _gate(21, "SUBMISSION_AUTHORIZED_TRUE", STATUS_PASS if submission_authorized else STATUS_FAIL, "" if submission_authorized else "SUBMISSION_AUTHORIZED_FALSE", "Submission is authorized." if submission_authorized else "Submission is not authorized.", submit_evidence, "Do not submit; obtain only governed explicit PAPER authorization when required.", "EXPECTED_SAFETY"),
            _gate(22, "BROKER_TRANSMIT_ENABLED_TRUE", STATUS_PASS if broker_transmit_enabled else STATUS_FAIL, "" if broker_transmit_enabled else "BROKER_TRANSMIT_ENABLED_FALSE", "Broker transmit is enabled by governed path." if broker_transmit_enabled else "Broker transmit is disabled.", submit_evidence, "Do not enable transmit manually; wait for governed transmit authority if applicable.", "EXPECTED_SAFETY"),
            _gate(23, "ORDER_SUBMISSION_ATTEMPTED", STATUS_PASS if order_attempted else STATUS_FAIL, "" if order_attempted else "NO_ORDER_SUBMISSION_ATTEMPTED", "A broker submission attempt exists." if order_attempted else "No order submission was attempted.", ArtifactEvidence("execution_evidence_submissions", str(sleeve / "execution_evidence_v1" / "submissions" / day_utc), order_attempted, stale_status="CURRENT" if order_attempted else "MISSING"), "No action required unless all readiness and authorization gates pass.", "EXPECTED_SAFETY"),
        ]
    )

    first = _kernel_preferred_blocker_gate(kernel_report, gates) or next((gate for gate in gates if gate.status == STATUS_FAIL), gates[-1])
    return NoTradeExplanation(
        day_utc=day_utc,
        active_release_id=active_release_id,
        first_blocker=first.reason_code or first.state,
        first_blocker_reason=first.explanation,
        classification=first.classification,
        next_safe_action=first.next_safe_action,
        ordered_gate_results=tuple(gates),
        stale_artifacts=_add_stale_warnings(gates),
        submit_allowed=submit_allowed,
        submission_authorized=submission_authorized,
        broker_transmit_enabled=broker_transmit_enabled,
        order_submission_attempted=order_attempted,
    )


def render_explanation_v1(explanation: NoTradeExplanation) -> str:
    lines = [
        f"No PAPER trade because: {explanation.first_blocker}",
        "",
        f"Classification: {explanation.classification}",
        f"Active release: {explanation.active_release_id or 'UNKNOWN'}",
        f"Target day: {explanation.day_utc}",
        "",
        "Ordered blocker stack:",
    ]
    for gate in explanation.ordered_gate_results:
        if gate.status != STATUS_FAIL:
            continue
        lines.append(f"{gate.order}. {gate.state}: {gate.reason_code} [{gate.classification}]")
        lines.append(f"   proof: {gate.artifact_path or 'NO_ARTIFACT_PATH'}")
        if gate.artifact_timestamp:
            lines.append(f"   timestamp: {gate.artifact_timestamp}")
        if gate.stale_status and gate.stale_status != "CURRENT":
            lines.append(f"   stale/missing: {gate.stale_status}")
        lines.append(f"   next: {gate.next_safe_action}")

    lines.extend(["", "Stale/missing artifact warnings:"])
    if explanation.stale_artifacts:
        for item in explanation.stale_artifacts:
            lines.append(f"- {item.logical_name}: {item.stale_status} path={item.path}")
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            f"Exact next safe action: {explanation.next_safe_action}",
            "Submit flags:",
            f"- submit_allowed={str(explanation.submit_allowed).lower()}",
            f"- submission_authorized={str(explanation.submission_authorized).lower()}",
            f"- broker_transmit_enabled={str(explanation.broker_transmit_enabled).lower()}",
            f"- order_submission_attempted={str(explanation.order_submission_attempted).lower()}",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="explain_aegis_no_paper_trade_v1")
    parser.add_argument("--day_utc", default="")
    parser.add_argument("--runtime_root", default=os.environ.get("AEGIS_RUNTIME_ROOT", str(DEFAULT_RUNTIME_ROOT)))
    parser.add_argument("--active_link", default=str(DEFAULT_ACTIVE_LINK))
    parser.add_argument("--no-journal", action="store_true", help="Skip systemd journal inspection.")
    args = parser.parse_args(argv)

    explanation = build_no_trade_explanation_v1(
        runtime_root=Path(args.runtime_root),
        active_link=Path(args.active_link),
        day_utc=str(args.day_utc or "").strip(),
        no_journal=bool(args.no_journal),
    )
    sys.stdout.write(render_explanation_v1(explanation))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
