#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.repo_protection_common_v1 import (
    PROTECTION_STATUS_PATH,
    RUNTIME_DATA_ROOT,
    read_protection_status_v1,
    require_runtime_output_outside_repo_runtime_v1,
)
from ops.tools.require_canonical_repo_clean_v1 import evaluate_canonical_cleanliness_v1

LATEST_PACKET_PATH = (
    RUNTIME_DATA_ROOT / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md"
).resolve()
ARCHIVE_ROOT = (RUNTIME_DATA_ROOT / "exports" / "aegis_state" / "archive").resolve()
ACTIVE_RUNTIME_CONTRACT_PATH = (
    RUNTIME_DATA_ROOT
    / "runtime_contract_v1"
    / "active_runtime_contract.v1.json"
).resolve()

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MAX_FRESHNESS_DAYS = 2
MAX_PROVEN_DAYS = 14

SECRET_PATTERNS = [
    re.compile(r"(?i)\bapi[_-]?key\s*[:=]\s*\S+"),
    re.compile(r"(?i)\btoken\s*[:=]\s*\S+"),
    re.compile(r"(?i)\bpassword\s*[:=]\s*\S+"),
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9\-._~+/]+=*"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bDU[0-9]{6,}\b"),
]


@dataclass(frozen=True)
class RootResolution:
    canonical_truth_root: Path | None
    runtime_truth_root: Path | None
    truth_sleeves_root: Path | None
    authority_source: str
    evidence: str
    error: str


@dataclass(frozen=True)
class PaperStatus:
    section: str
    status: str
    freshness_status: str
    day_utc: str


@dataclass(frozen=True)
class CapabilityRow:
    capability: str
    status: str
    evidence: str
    last_validated_at_utc: str


@dataclass(frozen=True)
class ComponentRow:
    component: str
    status: str
    evidence: str


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _iso_utc(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _run_git(args: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _git_commit() -> str:
    rc, out, _ = _run_git(["rev-parse", "HEAD"])
    return out if rc == 0 and out else "UNKNOWN"


def _git_branch() -> str:
    rc, out, _ = _run_git(["rev-parse", "--abbrev-ref", "HEAD"])
    return out if rc == 0 and out else "UNKNOWN"


def _git_status_short_lines() -> list[str]:
    rc, out, _ = _run_git(["status", "--short"])
    if rc != 0 or not out:
        return []
    return [line.rstrip() for line in out.splitlines() if line.strip()]


def _git_diff_name_only_lines() -> list[str]:
    rc, out, _ = _run_git(["diff", "--name-only", "HEAD"])
    if rc != 0 or not out:
        return []
    return [line.strip() for line in out.splitlines() if line.strip()]


def _read_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or (not path.exists()) or (not path.is_file()):
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _mtime_utc(path: Path | None) -> str:
    if path is None or (not path.exists()):
        return "UNKNOWN"
    try:
        return _iso_utc(datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).replace(microsecond=0))
    except Exception:
        return "UNKNOWN"


def _freshness_status_for_day(day_utc: str) -> str:
    if not DATE_RE.match(str(day_utc or "")):
        return "UNKNOWN"
    today = _utc_now().date()
    try:
        day = datetime.strptime(day_utc, "%Y-%m-%d").date()
    except ValueError:
        return "UNKNOWN"
    age = (today - day).days
    if age < 0:
        return "UNKNOWN"
    if age <= MAX_FRESHNESS_DAYS:
        return "FRESH"
    return "STALE"


def _resolve_truth_roots() -> RootResolution:
    try:
        from constellation_2.common.runtime_authority_bridge_v1 import (
            load_release_current_runtime_authority_v1,
        )

        authority = load_release_current_runtime_authority_v1(
            caller="ops/tools/aegis_chatgpt_packet.py"
        )
        canonical_truth_root = Path(
            str(authority.get("canonical_truth_root") or "")
        ).expanduser().resolve()
        truth_sleeves_root = Path(
            str(authority.get("truth_sleeves_root") or "")
        ).expanduser().resolve()
        runtime_truth_root = (truth_sleeves_root / "PRIMARY" / "PAPER").resolve()
        if (
            canonical_truth_root.exists()
            and canonical_truth_root.is_dir()
            and runtime_truth_root.exists()
            and runtime_truth_root.is_dir()
        ):
            return RootResolution(
                canonical_truth_root=canonical_truth_root,
                runtime_truth_root=runtime_truth_root,
                truth_sleeves_root=truth_sleeves_root,
                authority_source="runtime_authority_bridge_v1",
                evidence=str(ACTIVE_RUNTIME_CONTRACT_PATH),
                error="",
            )
        return RootResolution(
            canonical_truth_root=None,
            runtime_truth_root=None,
            truth_sleeves_root=None,
            authority_source="runtime_authority_bridge_v1",
            evidence=str(ACTIVE_RUNTIME_CONTRACT_PATH),
            error="resolved_roots_missing_or_not_directories",
        )
    except Exception as exc:
        return RootResolution(
            canonical_truth_root=None,
            runtime_truth_root=None,
            truth_sleeves_root=None,
            authority_source="unresolved",
            evidence="NOT_FOUND",
            error=f"{type(exc).__name__}: {exc}",
        )


def _latest_day_with_file(
    root: Path | None,
    family_rel: str,
    filename: str,
) -> tuple[str | None, Path | None]:
    if root is None:
        return None, None
    family = (root / family_rel).resolve()
    if not family.exists() or not family.is_dir():
        return None, None
    today = _utc_now().date().isoformat()
    day_names = sorted(
        [item.name for item in family.iterdir() if item.is_dir() and DATE_RE.match(item.name) and item.name <= today],
        reverse=True,
    )
    for day in day_names:
        candidate = (family / day / filename).resolve()
        if candidate.exists() and candidate.is_file():
            return day, candidate
    return None, None


def _status_from_evidence(path: Path | None, *, fresh_days: int = MAX_PROVEN_DAYS) -> str:
    if path is None or (not path.exists()):
        return "UNKNOWN"
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    age = (_utc_now().date() - mtime.date()).days
    if age <= fresh_days:
        return "ACTIVE"
    return "PRESENT_UNPROVEN"


def _status_for_capability(path: Path | None, *, fresh_days: int = MAX_PROVEN_DAYS) -> str:
    if path is None or (not path.exists()):
        return "UNKNOWN"
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    age = (_utc_now().date() - mtime.date()).days
    if age <= fresh_days:
        return "PROVEN"
    return "PRESENT_UNPROVEN"


def _find_latest_broker_submission_record(
    canonical_truth_root: Path | None,
    runtime_truth_root: Path | None,
) -> Path | None:
    candidates: list[Path] = []
    today = _utc_now().date().isoformat()
    for root in (canonical_truth_root, runtime_truth_root):
        if root is None:
            continue
        base = (root / "execution_evidence_v1" / "submissions").resolve()
        if not base.exists() or not base.is_dir():
            continue
        for day_dir in base.iterdir():
            if (not day_dir.is_dir()) or (not DATE_RE.match(day_dir.name)) or day_dir.name > today:
                continue
            for p in day_dir.glob("*/broker_submission_record.v2.json"):
                if p.exists() and p.is_file():
                    candidates.append(p.resolve())
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _find_fill_ledger_for_submission(
    submission_id: str,
    submission_day: str,
    canonical_truth_root: Path | None,
    runtime_truth_root: Path | None,
) -> Path | None:
    if not submission_id or not submission_day:
        return None
    file_name = f"{submission_id}.fill_ledger.v1.json"
    for root in (runtime_truth_root, canonical_truth_root):
        if root is None:
            continue
        p = (root / "fill_ledger_v1" / submission_day / file_name).resolve()
        if p.exists() and p.is_file():
            return p
    return None


def _find_execution_stream_records_for_submission(
    submission_id: str,
    submission_day: str,
    canonical_truth_root: Path | None,
    runtime_truth_root: Path | None,
) -> list[Path]:
    if not submission_id or not submission_day:
        return []
    out: list[Path] = []
    for root in (runtime_truth_root, canonical_truth_root):
        if root is None:
            continue
        day_root = (root / "execution_stream_v1" / submission_day).resolve()
        if not day_root.exists() or not day_root.is_dir():
            continue
        for p in day_root.glob("*.execution_event_stream_record.v1.json"):
            obj = _read_json(p.resolve())
            if isinstance(obj, dict) and str(obj.get("submission_id") or "").strip() == submission_id:
                out.append(p.resolve())
    return sorted(set(out))


def _build_active_components(roots: RootResolution) -> str:
    rows: list[ComponentRow] = []
    rows.append(
        ComponentRow(
            component="Authoritative Repo Root",
            status="ACTIVE" if REPO_ROOT.exists() else "UNKNOWN",
            evidence=str(REPO_ROOT) if REPO_ROOT.exists() else "NOT_FOUND",
        )
    )
    rows.append(
        ComponentRow(
            component="Runtime Contract",
            status="ACTIVE" if ACTIVE_RUNTIME_CONTRACT_PATH.exists() else "UNKNOWN",
            evidence=str(ACTIVE_RUNTIME_CONTRACT_PATH) if ACTIVE_RUNTIME_CONTRACT_PATH.exists() else "NOT_FOUND",
        )
    )
    rows.append(
        ComponentRow(
            component="Canonical Truth Root",
            status=_status_from_evidence(roots.canonical_truth_root, fresh_days=3650),
            evidence=str(roots.canonical_truth_root) if roots.canonical_truth_root else "NOT_FOUND",
        )
    )
    rows.append(
        ComponentRow(
            component="PAPER Runtime Truth Root",
            status=_status_from_evidence(roots.runtime_truth_root, fresh_days=3650),
            evidence=str(roots.runtime_truth_root) if roots.runtime_truth_root else "NOT_FOUND",
        )
    )

    ui_shell = (REPO_ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "index.html").resolve()
    ui_api = (REPO_ROOT / "constellation_2" / "phaseL" / "ui_api" / "projection_contracts.py").resolve()
    rows.append(ComponentRow("Aegis PhaseL UI Shell", _status_from_evidence(ui_shell), str(ui_shell) if ui_shell.exists() else "NOT_FOUND"))
    rows.append(ComponentRow("Aegis Projection Contracts", _status_from_evidence(ui_api), str(ui_api) if ui_api.exists() else "NOT_FOUND"))

    _, submit_boundary_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/submit_boundary_status_v1",
        "submit_boundary_status.v1.json",
    )
    rows.append(
        ComponentRow(
            component="Submit Boundary Status Surface",
            status=_status_from_evidence(submit_boundary_path),
            evidence=str(submit_boundary_path) if submit_boundary_path else "NOT_FOUND",
        )
    )

    _, control_plane_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/trading_day_control_plane_v1",
        "trading_day_control_plane.v1.json",
    )
    rows.append(
        ComponentRow(
            component="Trading Day Control Plane Surface",
            status=_status_from_evidence(control_plane_path),
            evidence=str(control_plane_path) if control_plane_path else "NOT_FOUND",
        )
    )

    _, attribution_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/sleeve_intent_trade_attribution_v1",
        "sleeve_intent_trade_attribution.v1.json",
    )
    rows.append(
        ComponentRow(
            component="Sleeve Intent Trade Attribution Surface",
            status=_status_from_evidence(attribution_path),
            evidence=str(attribution_path) if attribution_path else "NOT_FOUND",
        )
    )

    submit_trace_root = (
        (roots.canonical_truth_root / "reports" / "submit_decision_trace_v1").resolve()
        if roots.canonical_truth_root
        else None
    )
    submit_trace_has_files = bool(
        submit_trace_root and submit_trace_root.exists() and any(submit_trace_root.rglob("submit_decision_trace.v1.json"))
    )
    rows.append(
        ComponentRow(
            component="Submit Decision Trace Surface",
            status="ACTIVE" if submit_trace_has_files else ("PRESENT_UNPROVEN" if submit_trace_root and submit_trace_root.exists() else "UNKNOWN"),
            evidence=str(submit_trace_root) if submit_trace_root else "NOT_FOUND",
        )
    )

    exec_stream_root = (
        (roots.runtime_truth_root / "execution_stream_v1").resolve() if roots.runtime_truth_root else None
    )
    fill_ledger_root = (
        (roots.runtime_truth_root / "fill_ledger_v1").resolve() if roots.runtime_truth_root else None
    )
    rows.append(
        ComponentRow(
            component="Execution Stream Surface",
            status="ACTIVE" if exec_stream_root and exec_stream_root.exists() and any(exec_stream_root.rglob("*.execution_event_stream_record.v1.json")) else ("PRESENT_UNPROVEN" if exec_stream_root and exec_stream_root.exists() else "UNKNOWN"),
            evidence=str(exec_stream_root) if exec_stream_root else "NOT_FOUND",
        )
    )
    rows.append(
        ComponentRow(
            component="Fill Ledger Surface",
            status="ACTIVE" if fill_ledger_root and fill_ledger_root.exists() and any(fill_ledger_root.rglob("*.fill_ledger.v1.json")) else ("PRESENT_UNPROVEN" if fill_ledger_root and fill_ledger_root.exists() else "UNKNOWN"),
            evidence=str(fill_ledger_root) if fill_ledger_root else "NOT_FOUND",
        )
    )

    lines: list[str] = [
        "## Current Active Components",
        "",
        "This section is generated from current repo/runtime truth where available.",
        "Closed-world rule: Only components listed as ACTIVE may be referenced by ChatGPT as current.",
        "",
    ]
    for row in rows:
        lines.append(f"- component: {row.component}")
        lines.append(f"  - status: {row.status}")
        lines.append(f"  - evidence: {row.evidence}")
    lines.append("")
    return "\n".join(lines)


def _build_current_functionality(roots: RootResolution) -> str:
    rows: list[CapabilityRow] = []

    def add(capability: str, status: str, evidence: str, last_validated_at_utc: str) -> None:
        rows.append(CapabilityRow(capability, status, evidence, last_validated_at_utc))

    exporter_path = (REPO_ROOT / "ops" / "tools" / "aegis_chatgpt_packet.py").resolve()
    add(
        capability="state export",
        status="PROVEN" if exporter_path.exists() else "UNKNOWN",
        evidence=str(exporter_path) if exporter_path.exists() else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(exporter_path),
    )

    _, submit_boundary_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/submit_boundary_status_v1",
        "submit_boundary_status.v1.json",
    )
    add(
        capability="readiness evaluation",
        status=_status_for_capability(submit_boundary_path),
        evidence=str(submit_boundary_path) if submit_boundary_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(submit_boundary_path),
    )

    _, intent_generation_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/trading_day_intent_generation_v1",
        "trading_day_intent_generation.v1.json",
    )
    add(
        capability="paper-trade intent generation",
        status=_status_for_capability(intent_generation_path),
        evidence=str(intent_generation_path) if intent_generation_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(intent_generation_path),
    )

    _, risk_envelope_path = _latest_day_with_file(
        roots.runtime_truth_root,
        "reports/capital_risk_envelope_v2",
        "capital_risk_envelope.v2.json",
    )
    add(
        capability="risk gate evaluation",
        status=_status_for_capability(risk_envelope_path),
        evidence=str(risk_envelope_path) if risk_envelope_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(risk_envelope_path),
    )

    add(
        capability="broker submit boundary",
        status=_status_for_capability(submit_boundary_path),
        evidence=str(submit_boundary_path) if submit_boundary_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(submit_boundary_path),
    )

    latest_broker = _find_latest_broker_submission_record(
        canonical_truth_root=roots.canonical_truth_root,
        runtime_truth_root=roots.runtime_truth_root,
    )
    latest_broker_obj = _read_json(latest_broker)
    broker_ids = latest_broker_obj.get("broker_ids") if isinstance(latest_broker_obj, dict) and isinstance(latest_broker_obj.get("broker_ids"), dict) else {}
    order_id = broker_ids.get("order_id")
    perm_id = broker_ids.get("perm_id")
    has_broker_ids = isinstance(order_id, int) or isinstance(perm_id, int)

    add(
        capability="IB acknowledgement tracking",
        status="PROVEN" if has_broker_ids else ("PRESENT_UNPROVEN" if latest_broker else "UNKNOWN"),
        evidence=str(latest_broker) if latest_broker else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(latest_broker),
    )
    add(
        capability="order_id / perm_id capture",
        status="PROVEN" if has_broker_ids else ("PRESENT_UNPROVEN" if latest_broker else "UNKNOWN"),
        evidence=str(latest_broker) if latest_broker else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(latest_broker),
    )

    latest_fill = None
    if latest_broker and DATE_RE.match(latest_broker.parent.parent.name):
        sid = str((latest_broker_obj or {}).get("submission_id") or latest_broker.parent.name)
        latest_fill = _find_fill_ledger_for_submission(
            sid,
            latest_broker.parent.parent.name,
            roots.canonical_truth_root,
            roots.runtime_truth_root,
        )
    add(
        capability="lifecycle tracking",
        status=_status_for_capability(latest_fill) if latest_fill else ("PRESENT_UNPROVEN" if latest_broker else "UNKNOWN"),
        evidence=str(latest_fill) if latest_fill else (str(latest_broker) if latest_broker else "NOT_FOUND"),
        last_validated_at_utc=_mtime_utc(latest_fill if latest_fill else latest_broker),
    )

    _, reconciliation_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/execution_reconciliation_v1",
        "execution_reconciliation.v1.json",
    )
    add(
        capability="reconciliation",
        status=_status_for_capability(reconciliation_path),
        evidence=str(reconciliation_path) if reconciliation_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(reconciliation_path),
    )

    _, control_plane_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/trading_day_control_plane_v1",
        "trading_day_control_plane.v1.json",
    )
    add(
        capability="blocker reporting",
        status=_status_for_capability(control_plane_path),
        evidence=str(control_plane_path) if control_plane_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(control_plane_path),
    )

    evidence_root = (
        (roots.runtime_truth_root / "execution_evidence_v1" / "submissions").resolve()
        if roots.runtime_truth_root
        else None
    )
    evidence_root_has_data = bool(evidence_root and evidence_root.exists() and any(evidence_root.rglob("*.json")))
    add(
        capability="evidence manifesting",
        status="PROVEN" if evidence_root_has_data else ("PRESENT_UNPROVEN" if evidence_root and evidence_root.exists() else "UNKNOWN"),
        evidence=str(evidence_root) if evidence_root else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(evidence_root),
    )

    ui_shell = (REPO_ROOT / "constellation_2" / "phaseL" / "ui" / "static" / "index.html").resolve()
    ui_and_cp_proven = ui_shell.exists() and control_plane_path is not None and control_plane_path.exists()
    add(
        capability="UI/control-plane surfaces",
        status="PROVEN" if ui_and_cp_proven else ("PRESENT_UNPROVEN" if ui_shell.exists() or (control_plane_path and control_plane_path.exists()) else "UNKNOWN"),
        evidence=f"{ui_shell if ui_shell.exists() else 'NOT_FOUND'} ; {control_plane_path if control_plane_path else 'NOT_FOUND'}",
        last_validated_at_utc=_mtime_utc(control_plane_path if control_plane_path else ui_shell),
    )

    lines: list[str] = ["## Current Aegis Functionality", "", "This section is derived only from current repo/runtime evidence.", ""]
    for row in rows:
        lines.append(f"- capability: {row.capability}")
        lines.append(f"  - status: {row.status}")
        lines.append(f"  - evidence: {row.evidence}")
        lines.append(f"  - last_validated_at_utc: {row.last_validated_at_utc}")
    lines.append("")
    return "\n".join(lines)


def _build_paper_status(roots: RootResolution) -> PaperStatus:
    status = "UNKNOWN"
    canonical_blocker = "AEGIS_STATE_NOT_PROVEN"
    owning_subsystem = "UNKNOWN"
    owning_gate = "UNKNOWN"
    evidence = "NOT_FOUND"
    reason = "submit boundary evidence not found"
    day_utc = "UNKNOWN"

    boundary_day, boundary_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/submit_boundary_status_v1",
        "submit_boundary_status.v1.json",
    )
    boundary = _read_json(boundary_path)

    control_plane_path: Path | None = None
    control_plane: dict[str, Any] | None = None
    if boundary_day and roots.canonical_truth_root:
        control_plane_path = (
            roots.canonical_truth_root
            / "reports"
            / "trading_day_control_plane_v1"
            / boundary_day
            / "trading_day_control_plane.v1.json"
        ).resolve()
        control_plane = _read_json(control_plane_path)

    if boundary_day and isinstance(boundary, dict):
        day_utc = boundary_day
        evidence = str(boundary_path) if boundary_path else "NOT_FOUND"
        boundary_status = str(boundary.get("boundary_status") or "").strip().upper()
        submission_authorized = boundary.get("submission_authorized")
        first_blocker = str(boundary.get("first_blocker_code") or "").strip()
        control_plane_decision = str((control_plane or {}).get("final_start_decision") or "").strip().upper()
        contradiction = (
            boundary_status == "AUTHORIZED"
            and submission_authorized is True
            and control_plane_decision == "BLOCKED_BY_DEFECT"
        )
        freshness = _freshness_status_for_day(boundary_day)

        if contradiction:
            status = "NOT_READY"
            canonical_blocker = "AEGIS_STATE_CONTRADICTORY_CONTROL_SURFACES"
            owning_subsystem = "trading_day_control_plane_v1"
            owning_gate = "consistency_surface_reconciliation"
            evidence = f"{boundary_path} ; {control_plane_path}" if control_plane_path else str(boundary_path)
            reason = "submit boundary says AUTHORIZED but trading_day_control_plane reports BLOCKED_BY_DEFECT"
        elif freshness == "STALE":
            status = "NOT_READY"
            canonical_blocker = "AEGIS_STATE_STALE_EVIDENCE"
            owning_subsystem = "submit_boundary_status_v1"
            owning_gate = "freshness_policy"
            reason = f"latest submit boundary day_utc={boundary_day} is stale"
        elif boundary_status == "AUTHORIZED" and submission_authorized is True:
            status = "READY"
            canonical_blocker = ""
            owning_subsystem = "submit_boundary_status_v1"
            owning_gate = "boundary_status"
            reason = "submit boundary reports AUTHORIZED and submission_authorized=true"
        else:
            status = "NOT_READY"
            canonical_blocker = first_blocker or "SUBMIT_BOUNDARY_NOT_AUTHORIZED"
            owning_subsystem = "submit_boundary_status_v1"
            owning_gate = "boundary_status"
            reason = f"boundary_status={boundary_status or 'UNKNOWN'} submission_authorized={submission_authorized!r}"

    section_lines = [
        "## Aegis Paper-Trading Status",
        "",
        f"- status: {status}",
        f"- canonical_blocker: {canonical_blocker}",
        f"- owning_subsystem: {owning_subsystem}",
        f"- owning_gate: {owning_gate}",
        f"- evidence: {evidence}",
        f"- reason: {reason}",
        "",
    ]
    return PaperStatus(
        section="\n".join(section_lines),
        status=status,
        freshness_status=_freshness_status_for_day(day_utc),
        day_utc=day_utc,
    )


def _build_latest_attempt_section(roots: RootResolution) -> str:
    lines = ["## Latest Paper-Trade Attempt", ""]
    broker_path = _find_latest_broker_submission_record(roots.canonical_truth_root, roots.runtime_truth_root)
    if broker_path is None:
        lines.extend(
            [
                "- attempt_id: UNKNOWN",
                "- timestamp: UNKNOWN",
                "- symbol: UNKNOWN",
                "- side: UNKNOWN",
                "- quantity: UNKNOWN",
                "- IB received order: UNKNOWN",
                "- broker order_id: UNKNOWN",
                "- broker perm_id: UNKNOWN",
                "- lifecycle updated: UNKNOWN",
                "- failure_stage: NO_SUBMISSION_EVIDENCE",
                "- evidence: NOT_FOUND",
                "",
            ]
        )
        return "\n".join(lines)

    broker = _read_json(broker_path) or {}
    submission_id = str(broker.get("submission_id") or broker_path.parent.name).strip() or "UNKNOWN"
    day_utc = broker_path.parent.parent.name if DATE_RE.match(broker_path.parent.parent.name) else "UNKNOWN"
    broker_ids = broker.get("broker_ids") if isinstance(broker.get("broker_ids"), dict) else {}
    order_id = broker_ids.get("order_id")
    perm_id = broker_ids.get("perm_id")
    error = broker.get("error") if isinstance(broker.get("error"), dict) else {}
    error_code = str(error.get("code") or "").strip().upper()

    ib_received = "UNKNOWN"
    if isinstance(order_id, int) or isinstance(perm_id, int):
        ib_received = "YES"
    elif error_code:
        ib_received = "NO"

    fill_path = _find_fill_ledger_for_submission(
        submission_id=submission_id,
        submission_day=day_utc,
        canonical_truth_root=roots.canonical_truth_root,
        runtime_truth_root=roots.runtime_truth_root,
    )
    fill = _read_json(fill_path)
    stream_paths = _find_execution_stream_records_for_submission(
        submission_id=submission_id,
        submission_day=day_utc,
        canonical_truth_root=roots.canonical_truth_root,
        runtime_truth_root=roots.runtime_truth_root,
    )

    lifecycle_updated = "YES" if fill_path is not None or bool(stream_paths) else "NO"
    lifecycle_status = str((fill or {}).get("lifecycle_status") or "").strip().upper()
    filled_qty = (fill or {}).get("filled_qty")
    if lifecycle_status == "OPEN" and filled_qty == 0:
        failure_stage = "LIFECYCLE_OPEN_NO_FILL"
    elif lifecycle_status in {"PARTIAL", "FILLED"}:
        failure_stage = "NONE"
    elif error_code:
        failure_stage = error_code
    elif stream_paths:
        failure_stage = "STREAM_PRESENT_LIFECYCLE_PENDING"
    else:
        failure_stage = "SUBMISSION_EVIDENCE_INCOMPLETE"

    evidence_paths = [str(broker_path)]
    if fill_path:
        evidence_paths.append(str(fill_path))
    if stream_paths:
        evidence_paths.extend(str(p) for p in stream_paths[:3])
        if len(stream_paths) > 3:
            evidence_paths.append(f"... +{len(stream_paths) - 3} more stream records")

    lines.extend(
        [
            f"- attempt_id: {submission_id}",
            f"- timestamp: {str(broker.get('submitted_at_utc') or 'UNKNOWN').strip() or 'UNKNOWN'}",
            f"- symbol: {str(broker.get('symbol') or 'UNKNOWN').strip() or 'UNKNOWN'}",
            f"- side: {str(broker.get('side') or 'UNKNOWN').strip() or 'UNKNOWN'}",
            f"- quantity: {str(broker.get('quantity') or 'UNKNOWN').strip() or 'UNKNOWN'}",
            f"- IB received order: {ib_received}",
            f"- broker order_id: {order_id if isinstance(order_id, int) else 'UNKNOWN'}",
            f"- broker perm_id: {perm_id if isinstance(perm_id, int) else 'UNKNOWN'}",
            f"- lifecycle updated: {lifecycle_updated}",
            f"- failure_stage: {failure_stage}",
            f"- evidence: {' ; '.join(evidence_paths)}",
            "",
        ]
    )
    return "\n".join(lines)


def _build_files_changed_section(status_lines: list[str], diff_lines: list[str]) -> str:
    lines = ["## Files Changed Since Last Commit", ""]
    lines.append("`git status --short`")
    lines.append("")
    lines.append("```text")
    lines.extend(status_lines[:400] if status_lines else ["(clean)"])
    lines.append("```")
    lines.append("")
    lines.append("`git diff --name-only HEAD`")
    lines.append("")
    lines.append("```text")
    lines.extend(diff_lines[:400] if diff_lines else ["(no diff names)"])
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def _build_evidence_section(roots: RootResolution) -> str:
    lines = ["## Evidence / Proof", ""]
    lines.append(f"- runtime_contract_evidence: {roots.evidence}")
    lines.append(f"- canonical_truth_root: {str(roots.canonical_truth_root) if roots.canonical_truth_root else 'NOT_FOUND'}")
    lines.append(f"- runtime_truth_root: {str(roots.runtime_truth_root) if roots.runtime_truth_root else 'NOT_FOUND'}")
    lines.append(f"- truth_root_resolution_source: {roots.authority_source}")
    lines.append(f"- truth_root_resolution_error: {roots.error or 'NONE'}")
    lines.append("")
    return "\n".join(lines)


def _build_packet() -> tuple[str, str]:
    now = _utc_now()
    generated_at_utc = _iso_utc(now)
    commit = _git_commit()
    branch = _git_branch()
    cleanliness = evaluate_canonical_cleanliness_v1(REPO_ROOT)
    status_lines = _git_status_short_lines()
    diff_lines = _git_diff_name_only_lines()
    dirty = "DIRTY" if status_lines else "CLEAN"
    dirty_path_count = int(cleanliness.get("dirty_path_count") or len(status_lines))
    protection_status_payload = read_protection_status_v1()
    canonical_repo_protection_status = str(
        protection_status_payload.get("status") or "UNKNOWN"
    ).strip() or "UNKNOWN"
    if dirty == "DIRTY":
        source_reproducibility_status = "NOT_REPRODUCIBLE_DIRTY_WORKTREE"
    elif canonical_repo_protection_status == "PROTECTED":
        source_reproducibility_status = "REPRODUCIBLE_CLEAN_SOURCE"
    else:
        source_reproducibility_status = "REPRODUCIBLE_CLEAN_SOURCE_UNPROTECTED"
    export_id = f"{now.strftime('%Y%m%dT%H%M%SZ')}_{(commit[:12] if commit != 'UNKNOWN' else 'nogit')}"

    roots = _resolve_truth_roots()
    paper_status = _build_paper_status(roots)

    lines: list[str] = [
        "# Aegis ChatGPT State Packet",
        "",
        "- export_id: " + export_id,
        "- generated_at_utc: " + generated_at_utc,
        "- source_repo_path: " + str(REPO_ROOT),
        "- git_branch: " + branch,
        "- git_commit: " + commit,
        "- git_dirty_status: " + dirty,
        "- dirty_path_count: " + str(dirty_path_count),
        "- source_reproducibility_status: " + source_reproducibility_status,
        "- canonical_repo_protection_status: " + canonical_repo_protection_status,
        "- canonical_repo_protection_status_path: " + str(PROTECTION_STATUS_PATH),
        "- freshness_status: " + paper_status.freshness_status,
        "- packet_freshness_policy: max_age=48h; stale/contradictory/unproven state => UNKNOWN or NOT_READY",
        "",
        "## User Handoff Instruction",
        "",
        "Paste everything between:",
        "",
        "===== BEGIN AEGIS CHATGPT PACKET =====",
        "and",
        "===== END AEGIS CHATGPT PACKET =====",
        "",
        "into ChatGPT and say:",
        "",
        "Use this packet as the only source of current Aegis truth. Ignore prior memory and prior conversation context for current Aegis functionality.",
        "",
        "## ChatGPT Closed-World Rules",
        "",
        "This packet is the only valid source of current Aegis functionality and state.",
        "",
        "ChatGPT must use only the components, gates, artifacts, commands, and capabilities explicitly listed as ACTIVE or PROVEN in this packet.",
        "",
        "Any prior component, engine, gate, artifact, workflow, or capability not listed as ACTIVE or PROVEN in this packet must be treated as absent from current Aegis state.",
        "",
        "Examples from prior conversations must not be converted into requirements.",
        "",
        "If current functionality is missing, stale, contradictory, or unproven, ChatGPT must say UNKNOWN or NOT_READY rather than infer from memory.",
        "",
        _build_current_functionality(roots),
        _build_active_components(roots),
        paper_status.section,
        _build_latest_attempt_section(roots),
        _build_files_changed_section(status_lines, diff_lines),
        _build_evidence_section(roots),
    ]
    return export_id, ("\n".join(lines).strip() + "\n")


def _secret_scan_or_fail(packet_text: str) -> None:
    hits: list[str] = []
    for pattern in SECRET_PATTERNS:
        match = pattern.search(packet_text)
        if match:
            hits.append(f"{pattern.pattern} :: {match.group(0)[:120]}")
    if hits:
        raise SystemExit("FAIL: packet_secret_scan_failed matches=" + json.dumps(hits, sort_keys=True))


def _write_outputs(export_id: str, packet_text: str) -> tuple[Path, Path]:
    latest_dir = LATEST_PACKET_PATH.parent
    archive_dir = (ARCHIVE_ROOT / export_id).resolve()
    require_runtime_output_outside_repo_runtime_v1(latest_dir)
    require_runtime_output_outside_repo_runtime_v1(archive_dir)
    latest_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = (archive_dir / "chatgpt_aegis_packet.md").resolve()
    LATEST_PACKET_PATH.write_text(packet_text, encoding="utf-8")
    archive_path.write_text(packet_text, encoding="utf-8")
    return LATEST_PACKET_PATH, archive_path


def _validate_packet_or_fail(packet_text: str, latest_path: Path, archive_path: Path) -> None:
    if not latest_path.exists():
        raise SystemExit(f"FAIL: latest packet missing: {latest_path}")
    if not archive_path.exists():
        raise SystemExit(f"FAIL: archive packet missing: {archive_path}")

    required_markers = [
        "generated_at_utc",
        "source_repo_path",
        "freshness_status",
        "## User Handoff Instruction",
        "## ChatGPT Closed-World Rules",
        "## Current Aegis Functionality",
        "## Current Active Components",
        "## Aegis Paper-Trading Status",
    ]
    missing = [marker for marker in required_markers if marker not in packet_text]
    if missing:
        raise SystemExit("FAIL: packet_validation_missing_markers=" + json.dumps(missing, sort_keys=True))

    old_phrase = "stale artifacts such as"
    if old_phrase in packet_text.lower():
        raise SystemExit("FAIL: packet_validation_found_stale_example_requirement")

    legacy_terms = [("E" + "78"), ("M" + "34")]
    found_legacy_terms = [term for term in legacy_terms if term in packet_text]
    if found_legacy_terms:
        raise SystemExit("FAIL: packet_validation_found_legacy_example_terms=" + json.dumps(found_legacy_terms))

    source_text = Path(__file__).read_text(encoding="utf-8")
    source_hits = [term for term in legacy_terms if term in source_text]
    if source_hits:
        raise SystemExit("FAIL: exporter_source_contains_legacy_hardcoding=" + json.dumps(source_hits))


def main() -> int:
    if REPO_ROOT != Path("/home/node/constellation").resolve():
        raise SystemExit(f"FAIL: wrong repo root: {REPO_ROOT}")

    export_id, packet_text = _build_packet()
    _secret_scan_or_fail(packet_text)
    latest_path, archive_path = _write_outputs(export_id, packet_text)
    _validate_packet_or_fail(packet_text, latest_path, archive_path)

    output = {
        "export_id": export_id,
        "generated_at_utc": _iso_utc(_utc_now()),
        "latest_packet_path": str(latest_path),
        "archive_packet_path": str(archive_path),
        "status": "OK",
    }
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
