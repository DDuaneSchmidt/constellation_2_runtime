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
from constellation_2.common.paper_submit_mode_status_v1 import classify_paper_submit_mode_status_v1

LATEST_PACKET_PATH = (
    RUNTIME_DATA_ROOT / "exports" / "aegis_state" / "latest" / "chatgpt_aegis_packet.md"
).resolve()
ARCHIVE_ROOT = (RUNTIME_DATA_ROOT / "exports" / "aegis_state" / "archive").resolve()
OPERATOR_GATE_STATE_ROOT = (Path.home() / ".local" / "state" / "constellation_2").resolve()
ACTIVE_RUNTIME_CONTRACT_PATH = (
    RUNTIME_DATA_ROOT
    / "runtime_contract_v1"
    / "active_runtime_contract.v1.json"
).resolve()

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MAX_FRESHNESS_DAYS = 2
MAX_PROVEN_DAYS = 14
DAY_RUN_SCHEMA_VERSION = "aegis_day_run.v1"

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
class SourceIntegrityGate:
    raw_git_dirty_status: str
    raw_dirty_path_count: int
    raw_source_reproducibility_status: str
    raw_canonical_repo_protection_status: str
    effective_status: str
    effective_blocker: str
    effective_owner: str
    effective_gate: str


@dataclass(frozen=True)
class ReadinessSignal:
    input_name: str
    classification: str
    status: str
    code: str
    owner: str
    gate: str
    root_cause_id: str


@dataclass(frozen=True)
class GradeProfile:
    overall_grade: int
    readiness_grade: int
    functional_completeness_grade: int
    architecture_operational_integrity_grade: int
    grade_cap: int
    cap_reason: str
    unique_root_cause_count: int


@dataclass(frozen=True)
class FinalReadinessDecision:
    status: str
    canonical_blocker: str
    owning_subsystem: str
    owning_gate: str
    reason: str
    signals: list[ReadinessSignal]
    grade_profile: GradeProfile


@dataclass(frozen=True)
class CurrentCalendarDayStatus:
    day_utc: str
    is_trading_session: str
    expected_non_trading_day: bool
    paper_session_authority_path: str
    paper_session_status: str
    status: str
    canonical_blocker: str
    service_status_summary: str
    evidence: str


@dataclass(frozen=True)
class LatestTradingDayEvidenceStatus:
    evidence_day_utc: str
    status: str
    canonical_blocker: str
    submit_boundary_status_path: str
    closure_authority_path: str
    trade_lineage_graph_path: str
    execution_lifecycle_authority_path: str
    runtime_service_authority_path: str
    market_data_authority_path: str
    strategy_decision_authority_path: str
    portfolio_account_authority_path: str
    risk_sizing_authority_path: str
    execution_mode_authority_path: str
    trading_day_closure_authority_path: str
    runtime_service_state: str
    market_data_state: str
    market_data_operator_impact: str
    strategy_decision_state: str
    strategy_intent_count: str
    strategy_zero_intent_reason: str
    portfolio_account_state: str
    portfolio_cash_total_cents: str
    portfolio_net_liquidation_cents: str
    risk_sizing_state: str
    risk_final_size: str
    execution_mode_state: str
    execution_mode_environment: str
    broker_transmit_enabled: str
    trading_day_closure_state: str
    current_head_path: str
    submission_index_path: str
    evidence: str


@dataclass(frozen=True)
class DayRunLedgerStatus:
    exists: bool
    path: str
    day_utc: str
    environment: str
    final_status: str
    canonical_phase: str
    canonical_blocker: str
    root_cause_chain: list[dict[str, Any]]
    downstream_consequences: list[dict[str, Any]]
    operator_next_action: str
    updated_at_utc: str


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


def _today_utc_day() -> str:
    return _utc_now().date().isoformat()


def _day_run_ledger_path(roots: RootResolution, day_utc: str) -> Path | None:
    if roots.canonical_truth_root is None or not DATE_RE.match(str(day_utc or "")):
        return None
    return (
        roots.canonical_truth_root
        / "reports"
        / "aegis_day_run_v1"
        / day_utc
        / "day_run.v1.json"
    ).resolve()


def _load_day_run_ledger_status(roots: RootResolution, day_utc: str) -> DayRunLedgerStatus:
    path = _day_run_ledger_path(roots, day_utc)
    payload = _read_json(path)
    if not isinstance(payload, dict):
        return DayRunLedgerStatus(
            exists=False,
            path=str(path) if path else "NOT_FOUND",
            day_utc=day_utc,
            environment="PAPER",
            final_status="NOT_READY",
            canonical_phase="DAY_RUN",
            canonical_blocker="DAY_RUN_LEDGER_MISSING",
            root_cause_chain=[
                {
                    "phase": "DAY_RUN",
                    "canonical_blocker": "DAY_RUN_LEDGER_MISSING",
                    "blocker_detail": "current-day Aegis day-run ledger does not exist",
                }
            ],
            downstream_consequences=[],
            operator_next_action=(
                f"Run python3 ops/tools/run_aegis_day_v1.py --day_utc {day_utc} --environment PAPER"
            ),
            updated_at_utc="UNKNOWN",
        )
    payload_day = str(payload.get("day_utc") or "").strip()
    if payload_day != day_utc:
        return DayRunLedgerStatus(
            exists=True,
            path=str(path),
            day_utc=day_utc,
            environment=str(payload.get("environment") or "PAPER").strip().upper(),
            final_status="NOT_READY",
            canonical_phase="DAY_RUN",
            canonical_blocker="WRONG_DAY_RUN_LEDGER",
            root_cause_chain=[
                {
                    "phase": "DAY_RUN",
                    "canonical_blocker": "WRONG_DAY_RUN_LEDGER",
                    "blocker_detail": f"ledger day_utc={payload_day or 'UNKNOWN'} does not match current day {day_utc}",
                }
            ],
            downstream_consequences=[],
            operator_next_action=(
                f"Regenerate current-day ledger via python3 ops/tools/run_aegis_day_v1.py --day_utc {day_utc} --environment PAPER"
            ),
            updated_at_utc=str(payload.get("updated_at_utc") or "UNKNOWN"),
        )
    root_chain = payload.get("root_cause_chain")
    downstream = payload.get("downstream_consequences")
    return DayRunLedgerStatus(
        exists=True,
        path=str(path),
        day_utc=payload_day,
        environment=str(payload.get("environment") or "PAPER").strip().upper(),
        final_status=str(payload.get("final_status") or "NOT_READY").strip().upper(),
        canonical_phase=str(payload.get("canonical_phase") or "").strip().upper(),
        canonical_blocker=str(payload.get("canonical_blocker") or "").strip(),
        root_cause_chain=root_chain if isinstance(root_chain, list) else [],
        downstream_consequences=downstream if isinstance(downstream, list) else [],
        operator_next_action=str(payload.get("operator_next_action") or "").strip(),
        updated_at_utc=str(payload.get("updated_at_utc") or "UNKNOWN"),
    )


def _reason_codes(payload: dict[str, Any] | None, key: str = "blocking_reason_codes") -> list[str]:
    if not isinstance(payload, dict):
        return []
    raw = payload.get(key)
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for item in raw:
        code = str(item or "").strip()
        if code:
            out.append(code)
    return out


def _first_reason_code(payload: dict[str, Any] | None, key: str = "blocking_reason_codes") -> str:
    codes = _reason_codes(payload, key=key)
    return codes[0] if codes else ""


def _market_calendar_record_for_day(
    canonical_truth_root: Path | None,
    day_utc: str,
) -> tuple[dict[str, Any] | None, Path | None]:
    if canonical_truth_root is None or not DATE_RE.match(day_utc):
        return None, None
    year = day_utc[:4]
    for exchange in ("NYSE", "NASDAQ"):
        path = (
            canonical_truth_root
            / "market_calendar_v1"
            / exchange
            / f"{year}.jsonl"
        ).resolve()
        if not path.exists() or not path.is_file():
            continue
        try:
            for raw in path.read_text(encoding="utf-8").splitlines():
                raw = raw.strip()
                if not raw:
                    continue
                obj = json.loads(raw)
                if isinstance(obj, dict) and str(obj.get("day_utc") or "").strip() == day_utc:
                    return obj, path
        except Exception:
            continue
    return None, None


def _market_is_trading_session(
    canonical_truth_root: Path | None,
    day_utc: str,
) -> tuple[bool | None, Path | None]:
    record, path = _market_calendar_record_for_day(canonical_truth_root, day_utc)
    if not isinstance(record, dict):
        return None, path
    raw = record.get("is_trading_session")
    if isinstance(raw, bool):
        return raw, path
    value = str(raw or "").strip().lower()
    if value in {"true", "1", "yes"}:
        return True, path
    if value in {"false", "0", "no"}:
        return False, path
    return None, path


def _latest_trading_day_from_calendar(
    canonical_truth_root: Path | None,
) -> tuple[str | None, Path | None]:
    if canonical_truth_root is None:
        return None, None
    today = _today_utc_day()
    best_day = ""
    best_path: Path | None = None
    years = sorted({_utc_now().year, _utc_now().year - 1}, reverse=True)
    for exchange in ("NYSE", "NASDAQ"):
        for year in years:
            path = (
                canonical_truth_root
                / "market_calendar_v1"
                / exchange
                / f"{year}.jsonl"
            ).resolve()
            if not path.exists() or not path.is_file():
                continue
            try:
                for raw in path.read_text(encoding="utf-8").splitlines():
                    raw = raw.strip()
                    if not raw:
                        continue
                    obj = json.loads(raw)
                    if not isinstance(obj, dict):
                        continue
                    day_utc = str(obj.get("day_utc") or "").strip()
                    if not DATE_RE.match(day_utc) or day_utc > today:
                        continue
                    if obj.get("is_trading_session") is True and day_utc > best_day:
                        best_day = day_utc
                        best_path = path
            except Exception:
                continue
        if best_day:
            return best_day, best_path
    return (best_day if best_day else None), best_path


def _read_operator_gate_status_for_day(day_utc: str) -> tuple[dict[str, Any] | None, Path]:
    path = (OPERATOR_GATE_STATE_ROOT / f"operator_gate_{day_utc}.v1.json").resolve()
    return _read_json(path), path


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


def _trade_lineage_graph_path(canonical_truth_root: Path | None, day_utc: str) -> Path | None:
    if canonical_truth_root is None or not DATE_RE.match(str(day_utc or "")):
        return None
    return (
        canonical_truth_root
        / "reports"
        / "trade_lineage_graph_v1"
        / day_utc
        / "trade_lineage_graph.v1.json"
    ).resolve()


def _trade_lineage_row_for_submission(
    canonical_truth_root: Path | None,
    day_utc: str,
    submission_id: str,
) -> tuple[Path | None, dict[str, Any] | None]:
    path = _trade_lineage_graph_path(canonical_truth_root, day_utc)
    graph = _read_json(path)
    if not isinstance(graph, dict):
        return path, None
    for row in graph.get("lineages", []):
        if isinstance(row, dict) and str(row.get("submission_id") or "").strip() == submission_id:
            return path, row
    return path, None


def _report_authority_path(
    canonical_truth_root: Path | None,
    family: str,
    day_utc: str,
    filename: str,
) -> Path | None:
    if canonical_truth_root is None or not DATE_RE.match(str(day_utc or "")):
        return None
    return (canonical_truth_root / "reports" / family / day_utc / filename).resolve()


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

    _, lifecycle_authority_path = _latest_day_with_file(
        roots.runtime_truth_root,
        "reports/execution_lifecycle_authority_v1",
        "execution_lifecycle_authority.v1.json",
    )
    _, trade_lineage_graph_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/trade_lineage_graph_v1",
        "trade_lineage_graph.v1.json",
    )
    _, runtime_service_authority_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/runtime_service_authority_v1",
        "runtime_service_authority.v1.json",
    )
    _, market_data_authority_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/market_data_authority_v1",
        "market_data_authority.v1.json",
    )
    _, execution_mode_authority_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/execution_mode_authority_v1",
        "execution_mode_authority.v1.json",
    )
    _, strategy_decision_authority_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/strategy_decision_authority_v1",
        "strategy_decision_authority.v1.json",
    )
    _, portfolio_account_authority_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/portfolio_account_authority_v1",
        "portfolio_account_authority.v1.json",
    )
    _, risk_sizing_authority_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/risk_sizing_authority_v1",
        "risk_sizing_authority.v1.json",
    )
    _, trading_day_closure_authority_path = _latest_day_with_file(
        roots.canonical_truth_root,
        "reports/trading_day_closure_authority_v1",
        "trading_day_closure_authority.v1.json",
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

    add(
        capability="lifecycle tracking",
        status=_status_for_capability(lifecycle_authority_path) if lifecycle_authority_path else ("PRESENT_UNPROVEN" if latest_broker else "UNKNOWN"),
        evidence=str(lifecycle_authority_path) if lifecycle_authority_path else (str(latest_broker) if latest_broker else "NOT_FOUND"),
        last_validated_at_utc=_mtime_utc(lifecycle_authority_path if lifecycle_authority_path else latest_broker),
    )
    add(
        capability="trade identity lineage",
        status=_status_for_capability(trade_lineage_graph_path),
        evidence=str(trade_lineage_graph_path) if trade_lineage_graph_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(trade_lineage_graph_path),
    )
    add(
        capability="runtime service authority",
        status=_status_for_capability(runtime_service_authority_path),
        evidence=str(runtime_service_authority_path) if runtime_service_authority_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(runtime_service_authority_path),
    )
    add(
        capability="market data authority",
        status=_status_for_capability(market_data_authority_path),
        evidence=str(market_data_authority_path) if market_data_authority_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(market_data_authority_path),
    )
    add(
        capability="execution mode authority",
        status=_status_for_capability(execution_mode_authority_path),
        evidence=str(execution_mode_authority_path) if execution_mode_authority_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(execution_mode_authority_path),
    )
    add(
        capability="strategy decision authority",
        status=_status_for_capability(strategy_decision_authority_path),
        evidence=str(strategy_decision_authority_path) if strategy_decision_authority_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(strategy_decision_authority_path),
    )
    add(
        capability="portfolio account authority",
        status=_status_for_capability(portfolio_account_authority_path),
        evidence=str(portfolio_account_authority_path) if portfolio_account_authority_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(portfolio_account_authority_path),
    )
    add(
        capability="risk sizing authority",
        status=_status_for_capability(risk_sizing_authority_path),
        evidence=str(risk_sizing_authority_path) if risk_sizing_authority_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(risk_sizing_authority_path),
    )
    add(
        capability="trading day closure authority",
        status=_status_for_capability(trading_day_closure_authority_path),
        evidence=str(trading_day_closure_authority_path) if trading_day_closure_authority_path else "NOT_FOUND",
        last_validated_at_utc=_mtime_utc(trading_day_closure_authority_path),
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


def _build_current_calendar_day_runtime_status(roots: RootResolution) -> CurrentCalendarDayStatus:
    day_utc = _today_utc_day()
    is_trading_session, market_calendar_path = _market_is_trading_session(
        roots.canonical_truth_root, day_utc
    )
    expected_non_trading_day = is_trading_session is False

    authority_path = (
        roots.canonical_truth_root
        / "reports"
        / "paper_session_authority_v1"
        / day_utc
        / "paper_session_authority.v1.json"
    ).resolve() if roots.canonical_truth_root else None
    authority_payload = _read_json(authority_path)
    authority_status = str(
        (authority_payload or {}).get("authority_status") or "MISSING"
    ).strip().upper()
    authority_blocker = _first_reason_code(authority_payload)
    day_authority_path = (
        roots.canonical_truth_root
        / "reports"
        / "paper_trading_day_authority_v1"
        / day_utc
        / "paper_trading_day_authority.v1.json"
    ).resolve() if roots.canonical_truth_root else None
    day_authority_payload = _read_json(day_authority_path)
    day_authority_state = str((day_authority_payload or {}).get("state") or "").strip().upper()
    day_authority_can_submit = bool(
        (day_authority_payload or {}).get("can_submit_paper_orders") is True
    )
    day_authority_blocker = str((day_authority_payload or {}).get("canonical_blocker") or "").strip()
    day_authority_submit_mode = str((day_authority_payload or {}).get("submit_mode_status") or "").strip().upper()
    if not day_authority_blocker:
        day_authority_blocker = _first_reason_code(day_authority_payload, key="reason_codes")

    boundary_path = (
        roots.canonical_truth_root
        / "reports"
        / "submit_boundary_status_v1"
        / day_utc
        / "submit_boundary_status.v1.json"
    ).resolve() if roots.canonical_truth_root else None
    boundary_payload = _read_json(boundary_path)
    boundary_status = str((boundary_payload or {}).get("boundary_status") or "").strip().upper()
    boundary_submission_authorized = (boundary_payload or {}).get("submission_authorized")
    boundary_blocker = str((boundary_payload or {}).get("first_blocker_code") or "").strip()

    bootstrap_path = (
        roots.canonical_truth_root
        / "reports"
        / "paper_session_bootstrap_v1"
        / day_utc
        / "paper_session_bootstrap.v1.json"
    ).resolve() if roots.canonical_truth_root else None
    bootstrap_payload = _read_json(bootstrap_path)
    bootstrap_status = str((bootstrap_payload or {}).get("bootstrap_status") or "").strip().upper()
    bootstrap_reason_codes = (bootstrap_payload or {}).get("canonical_stop_reason_codes")
    bootstrap_blocker = (
        str(bootstrap_reason_codes[0]).strip()
        if isinstance(bootstrap_reason_codes, list) and bootstrap_reason_codes
        else ""
    )

    status = "NOT_READY"
    canonical_blocker = "SESSION_AUTHORITY_MISSING"
    if day_authority_payload is not None:
        if day_authority_submit_mode == "DRY_RUN_COMPLETE":
            status = "DRY_RUN_COMPLETE"
            canonical_blocker = ""
        elif day_authority_state == "OPEN_READY" and day_authority_can_submit:
            status = "READY"
            canonical_blocker = ""
        else:
            status = "NOT_READY"
            canonical_blocker = day_authority_blocker or "CURRENT_DAY_NOT_READY"
    elif is_trading_session is None:
        status = "NOT_READY"
        canonical_blocker = "SESSION_AUTHORITY_MISSING"
    elif is_trading_session is False:
        status = "NOT_READY"
        canonical_blocker = authority_blocker or bootstrap_blocker or "NON_TRADING_DAY"
    else:
        authority_allows = authority_status in {"GRANTED", "AUTHORIZED"} and (
            (authority_payload or {}).get("submission_authorized") is True
        )
        boundary_allows = boundary_status == "AUTHORIZED" and boundary_submission_authorized is True
        if boundary_payload is not None:
            if authority_allows and boundary_allows:
                status = "READY"
                canonical_blocker = ""
            else:
                status = "NOT_READY"
                canonical_blocker = boundary_blocker or authority_blocker or "NO_ACTIVE_PAPER_SESSION"
        elif authority_payload is not None:
            if authority_allows:
                status = "READY"
                canonical_blocker = ""
            else:
                status = "NOT_READY"
                canonical_blocker = authority_blocker or "NO_ACTIVE_PAPER_SESSION"
        else:
            status = "NOT_READY"
            canonical_blocker = "SESSION_AUTHORITY_MISSING"

    operator_gate_payload, operator_gate_path = _read_operator_gate_status_for_day(day_utc)
    operator_gate_status = str((operator_gate_payload or {}).get("status") or "UNKNOWN").strip().upper()
    operator_gate_blocker = str((operator_gate_payload or {}).get("session_day_blocker") or "").strip()
    if not operator_gate_blocker:
        operator_gate_blocker = _first_reason_code(operator_gate_payload, key="reason_codes")
    operator_gate_produced = str((operator_gate_payload or {}).get("produced_utc") or "").strip()
    day_authority_produced = str((day_authority_payload or {}).get("produced_at_utc") or "").strip()
    operator_gate_is_stale = bool(
        operator_gate_produced and day_authority_produced and operator_gate_produced < day_authority_produced
    )
    if operator_gate_is_stale:
        operator_gate_status = "STALE_DIAGNOSTIC_ONLY"
        operator_gate_blocker = "IGNORED_CANONICAL_DAY_AUTHORITY_NEWER"

    paper_orchestrator_status = bootstrap_status or ("READY" if status == "READY" else "BLOCKED")
    paper_orchestrator_blocker = bootstrap_blocker or canonical_blocker or "UNKNOWN"

    global_monitoring_status = "UNKNOWN"
    global_monitoring_blocker = ""
    if status == "READY":
        global_monitoring_status = "OK"
    elif canonical_blocker in {"NON_TRADING_DAY", "NO_ACTIVE_PAPER_SESSION"}:
        global_monitoring_status = "DEGRADED"
        global_monitoring_blocker = "NO_ACTIVE_PAPER_SESSION"
    elif canonical_blocker:
        global_monitoring_status = "DEGRADED"
        global_monitoring_blocker = canonical_blocker
    elif operator_gate_status == "FAIL" and operator_gate_blocker:
        global_monitoring_status = "DEGRADED"
        global_monitoring_blocker = operator_gate_blocker

    service_status_summary = (
        f"paper_orchestrator={paper_orchestrator_status}/{paper_orchestrator_blocker or '<none>'}; "
        f"global_monitoring={global_monitoring_status}/{global_monitoring_blocker or '<none>'}; "
        f"operator_gate={operator_gate_status}/{operator_gate_blocker or '<none>'}"
    )

    evidence_parts = [
        str(path)
        for path in (
            market_calendar_path,
            authority_path,
            day_authority_path,
            boundary_path,
            bootstrap_path,
            operator_gate_path if operator_gate_path.exists() else None,
        )
        if path is not None
    ]
    evidence = " ; ".join(evidence_parts) if evidence_parts else "NOT_FOUND"

    if is_trading_session is True:
        is_trading_session_text = "true"
    elif is_trading_session is False:
        is_trading_session_text = "false"
    else:
        is_trading_session_text = "UNKNOWN"

    return CurrentCalendarDayStatus(
        day_utc=day_utc,
        is_trading_session=is_trading_session_text,
        expected_non_trading_day=expected_non_trading_day,
        paper_session_authority_path=str(authority_path) if authority_path else "NOT_FOUND",
        paper_session_status=authority_status,
        status=status,
        canonical_blocker=canonical_blocker,
        service_status_summary=service_status_summary,
        evidence=evidence,
    )


def _build_latest_trading_day_evidence_status(roots: RootResolution) -> LatestTradingDayEvidenceStatus:
    evidence_day_utc, _calendar_source = _latest_trading_day_from_calendar(
        roots.canonical_truth_root
    )
    if evidence_day_utc and roots.canonical_truth_root:
        calendar_submit_boundary = (
            roots.canonical_truth_root
            / "reports"
            / "submit_boundary_status_v1"
            / evidence_day_utc
            / "submit_boundary_status.v1.json"
        )
        if not calendar_submit_boundary.exists():
            evidence_day_utc = None
    if not evidence_day_utc:
        evidence_day_utc, _unused = _latest_day_with_file(
            roots.canonical_truth_root,
            "reports/submit_boundary_status_v1",
            "submit_boundary_status.v1.json",
        )
    if not evidence_day_utc:
        return LatestTradingDayEvidenceStatus(
            evidence_day_utc="UNKNOWN",
            status="UNKNOWN",
            canonical_blocker="AEGIS_STATE_NOT_PROVEN",
            submit_boundary_status_path="NOT_FOUND",
            closure_authority_path="NOT_FOUND",
            trade_lineage_graph_path="NOT_FOUND",
            execution_lifecycle_authority_path="NOT_FOUND",
            runtime_service_authority_path="NOT_FOUND",
            market_data_authority_path="NOT_FOUND",
            strategy_decision_authority_path="NOT_FOUND",
            portfolio_account_authority_path="NOT_FOUND",
            risk_sizing_authority_path="NOT_FOUND",
            execution_mode_authority_path="NOT_FOUND",
            trading_day_closure_authority_path="NOT_FOUND",
            runtime_service_state="UNKNOWN",
            market_data_state="UNKNOWN",
            market_data_operator_impact="UNKNOWN",
            strategy_decision_state="UNKNOWN",
            strategy_intent_count="UNKNOWN",
            strategy_zero_intent_reason="UNKNOWN",
            portfolio_account_state="UNKNOWN",
            portfolio_cash_total_cents="UNKNOWN",
            portfolio_net_liquidation_cents="UNKNOWN",
            risk_sizing_state="UNKNOWN",
            risk_final_size="UNKNOWN",
            execution_mode_state="UNKNOWN",
            execution_mode_environment="UNKNOWN",
            broker_transmit_enabled="UNKNOWN",
            trading_day_closure_state="UNKNOWN",
            current_head_path="NOT_FOUND",
            submission_index_path="NOT_FOUND",
            evidence="NOT_FOUND",
        )

    submit_boundary_path = (
        roots.canonical_truth_root
        / "reports"
        / "submit_boundary_status_v1"
        / evidence_day_utc
        / "submit_boundary_status.v1.json"
    ).resolve() if roots.canonical_truth_root else None
    closure_authority_path = (
        roots.canonical_truth_root
        / "reports"
        / "aegis_day_closure_authority_v1"
        / evidence_day_utc
        / "aegis_day_closure_authority.v1.json"
    ).resolve() if roots.canonical_truth_root else None
    execution_lifecycle_authority_path = (
        roots.runtime_truth_root
        / "reports"
        / "execution_lifecycle_authority_v1"
        / evidence_day_utc
        / "execution_lifecycle_authority.v1.json"
    ).resolve() if roots.runtime_truth_root else None
    trade_lineage_graph_path = _trade_lineage_graph_path(roots.canonical_truth_root, evidence_day_utc)
    runtime_service_authority_path = _report_authority_path(
        roots.canonical_truth_root,
        "runtime_service_authority_v1",
        evidence_day_utc,
        "runtime_service_authority.v1.json",
    )
    market_data_authority_path = _report_authority_path(
        roots.canonical_truth_root,
        "market_data_authority_v1",
        evidence_day_utc,
        "market_data_authority.v1.json",
    )
    execution_mode_authority_path = _report_authority_path(
        roots.canonical_truth_root,
        "execution_mode_authority_v1",
        evidence_day_utc,
        "execution_mode_authority.v1.json",
    )
    strategy_decision_authority_path = _report_authority_path(
        roots.canonical_truth_root,
        "strategy_decision_authority_v1",
        evidence_day_utc,
        "strategy_decision_authority.v1.json",
    )
    portfolio_account_authority_path = _report_authority_path(
        roots.canonical_truth_root,
        "portfolio_account_authority_v1",
        evidence_day_utc,
        "portfolio_account_authority.v1.json",
    )
    risk_sizing_authority_path = _report_authority_path(
        roots.canonical_truth_root,
        "risk_sizing_authority_v1",
        evidence_day_utc,
        "risk_sizing_authority.v1.json",
    )
    trading_day_closure_authority_path = _report_authority_path(
        roots.canonical_truth_root,
        "trading_day_closure_authority_v1",
        evidence_day_utc,
        "trading_day_closure_authority.v1.json",
    )
    current_head_path = (
        roots.runtime_truth_root
        / "execution_evidence_v1"
        / "current_head"
        / evidence_day_utc
        / "current_head.v1.json"
    ).resolve() if roots.runtime_truth_root else None
    submission_index_path = (
        roots.runtime_truth_root
        / "submission_index_v1"
        / evidence_day_utc
        / "submission_index.v1.json"
    ).resolve() if roots.runtime_truth_root else None

    submit_boundary_payload = _read_json(submit_boundary_path)
    closure_payload = _read_json(closure_authority_path)
    runtime_service_payload = _read_json(runtime_service_authority_path)
    market_data_payload = _read_json(market_data_authority_path)
    strategy_decision_payload = _read_json(strategy_decision_authority_path)
    portfolio_account_payload = _read_json(portfolio_account_authority_path)
    risk_sizing_payload = _read_json(risk_sizing_authority_path)
    execution_mode_payload = _read_json(execution_mode_authority_path)
    trading_day_closure_payload = _read_json(trading_day_closure_authority_path)
    current_head_payload = _read_json(current_head_path)
    submission_index_payload = _read_json(submission_index_path)

    status = "UNKNOWN"
    canonical_blocker = "AEGIS_STATE_NOT_PROVEN"
    if isinstance(closure_payload, dict):
        closure_status = str(closure_payload.get("status") or "").strip().upper()
        closure_blocker = str(closure_payload.get("canonical_blocker") or "").strip()
        if closure_status in {"PASS", "READY", "AUTHORIZED"}:
            status = "READY"
            canonical_blocker = ""
        else:
            status = "NOT_READY"
            canonical_blocker = closure_blocker or "AEGIS_STATE_NOT_PROVEN"
    elif isinstance(submit_boundary_payload, dict):
        boundary_status = str(submit_boundary_payload.get("boundary_status") or "").strip().upper()
        submission_authorized = submit_boundary_payload.get("submission_authorized")
        first_blocker = str(submit_boundary_payload.get("first_blocker_code") or "").strip()
        submit_mode_status = str(submit_boundary_payload.get("submit_mode_status") or "").strip().upper()
        if submit_mode_status == "DRY_RUN_COMPLETE" or boundary_status == "DRY_RUN_COMPLETE":
            status = "DRY_RUN_COMPLETE"
            canonical_blocker = ""
        elif boundary_status == "AUTHORIZED" and submission_authorized is True:
            status = "READY"
            canonical_blocker = ""
        else:
            status = "NOT_READY"
            canonical_blocker = first_blocker or "SUBMIT_BOUNDARY_NOT_AUTHORIZED"
    elif (
        str((current_head_payload or {}).get("status") or "").strip().upper() == "PASS"
        and str((submission_index_payload or {}).get("status") or "").strip().upper() == "PASS"
    ):
        status = "READY"
        canonical_blocker = ""
    elif current_head_payload is not None or submission_index_payload is not None:
        status = "NOT_READY"
        canonical_blocker = "LATEST_TRADING_DAY_EVIDENCE_INCOMPLETE"

    evidence_parts = [
        str(path)
        for path in (
            submit_boundary_path,
            closure_authority_path,
            trade_lineage_graph_path,
            execution_lifecycle_authority_path,
            runtime_service_authority_path,
            market_data_authority_path,
            strategy_decision_authority_path,
            portfolio_account_authority_path,
            risk_sizing_authority_path,
            execution_mode_authority_path,
            trading_day_closure_authority_path,
            current_head_path,
            submission_index_path,
        )
        if path is not None and path.exists()
    ]
    evidence = " ; ".join(evidence_parts) if evidence_parts else "NOT_FOUND"
    return LatestTradingDayEvidenceStatus(
        evidence_day_utc=evidence_day_utc,
        status=status,
        canonical_blocker=canonical_blocker,
        submit_boundary_status_path=str(submit_boundary_path) if submit_boundary_path else "NOT_FOUND",
        closure_authority_path=str(closure_authority_path) if closure_authority_path else "NOT_FOUND",
        trade_lineage_graph_path=str(trade_lineage_graph_path) if trade_lineage_graph_path else "NOT_FOUND",
        execution_lifecycle_authority_path=str(execution_lifecycle_authority_path) if execution_lifecycle_authority_path else "NOT_FOUND",
        runtime_service_authority_path=str(runtime_service_authority_path) if runtime_service_authority_path else "NOT_FOUND",
        market_data_authority_path=str(market_data_authority_path) if market_data_authority_path else "NOT_FOUND",
        strategy_decision_authority_path=str(strategy_decision_authority_path) if strategy_decision_authority_path else "NOT_FOUND",
        portfolio_account_authority_path=str(portfolio_account_authority_path) if portfolio_account_authority_path else "NOT_FOUND",
        risk_sizing_authority_path=str(risk_sizing_authority_path) if risk_sizing_authority_path else "NOT_FOUND",
        execution_mode_authority_path=str(execution_mode_authority_path) if execution_mode_authority_path else "NOT_FOUND",
        trading_day_closure_authority_path=str(trading_day_closure_authority_path) if trading_day_closure_authority_path else "NOT_FOUND",
        runtime_service_state=str((runtime_service_payload or {}).get("service_state") or "UNKNOWN").strip().upper(),
        market_data_state=str((market_data_payload or {}).get("market_data_state") or "UNKNOWN").strip().upper(),
        market_data_operator_impact=str((market_data_payload or {}).get("operator_impact") or "UNKNOWN").strip().upper(),
        strategy_decision_state=str((strategy_decision_payload or {}).get("strategy_decision_state") or "UNKNOWN").strip().upper(),
        strategy_intent_count=str((strategy_decision_payload or {}).get("intent_count") if isinstance(strategy_decision_payload, dict) else "UNKNOWN"),
        strategy_zero_intent_reason=str((strategy_decision_payload or {}).get("zero_intent_reason") or "<none>").strip() if isinstance(strategy_decision_payload, dict) else "UNKNOWN",
        portfolio_account_state=str((portfolio_account_payload or {}).get("account_state") or "UNKNOWN").strip().upper(),
        portfolio_cash_total_cents=str(((portfolio_account_payload or {}).get("account_values") or {}).get("cash_total_cents") if isinstance((portfolio_account_payload or {}).get("account_values"), dict) else "UNKNOWN"),
        portfolio_net_liquidation_cents=str(((portfolio_account_payload or {}).get("account_values") or {}).get("net_liquidation_cents") if isinstance((portfolio_account_payload or {}).get("account_values"), dict) else "UNKNOWN"),
        risk_sizing_state=str((risk_sizing_payload or {}).get("risk_sizing_state") or "UNKNOWN").strip().upper(),
        risk_final_size=json.dumps((risk_sizing_payload or {}).get("final_size_summary") or {}, sort_keys=True) if isinstance(risk_sizing_payload, dict) else "UNKNOWN",
        execution_mode_state=str((execution_mode_payload or {}).get("mode_state") or "UNKNOWN").strip().upper(),
        execution_mode_environment=str((execution_mode_payload or {}).get("environment") or "UNKNOWN").strip().upper(),
        broker_transmit_enabled=str((execution_mode_payload or {}).get("broker_transmit_enabled")).lower() if isinstance((execution_mode_payload or {}).get("broker_transmit_enabled"), bool) else "UNKNOWN",
        trading_day_closure_state=str((trading_day_closure_payload or {}).get("closure_state") or "UNKNOWN").strip().upper(),
        current_head_path=str(current_head_path) if current_head_path else "NOT_FOUND",
        submission_index_path=str(submission_index_path) if submission_index_path else "NOT_FOUND",
        evidence=evidence,
    )


def _build_source_integrity_gate(
    *,
    git_dirty_status: str,
    dirty_path_count: int,
    source_reproducibility_status: str,
    canonical_repo_protection_status: str,
) -> SourceIntegrityGate:
    raw_git_dirty_status = str(git_dirty_status or "UNKNOWN").strip().upper() or "UNKNOWN"
    raw_source_reproducibility_status = (
        str(source_reproducibility_status or "UNKNOWN").strip().upper() or "UNKNOWN"
    )
    raw_canonical_repo_protection_status = (
        str(canonical_repo_protection_status or "UNKNOWN").strip().upper() or "UNKNOWN"
    )
    if raw_git_dirty_status != "CLEAN" or raw_source_reproducibility_status != "REPRODUCIBLE":
        return SourceIntegrityGate(
            raw_git_dirty_status=raw_git_dirty_status,
            raw_dirty_path_count=int(dirty_path_count),
            raw_source_reproducibility_status=raw_source_reproducibility_status,
            raw_canonical_repo_protection_status=raw_canonical_repo_protection_status,
            effective_status="BLOCKED",
            effective_blocker="SOURCE_REPRODUCIBILITY_BLOCKED",
            effective_owner="source_reproducibility_authority",
            effective_gate="pre_submit_source_integrity_gate",
        )
    if raw_canonical_repo_protection_status != "PROTECTED":
        return SourceIntegrityGate(
            raw_git_dirty_status=raw_git_dirty_status,
            raw_dirty_path_count=int(dirty_path_count),
            raw_source_reproducibility_status=raw_source_reproducibility_status,
            raw_canonical_repo_protection_status=raw_canonical_repo_protection_status,
            effective_status="BLOCKED",
            effective_blocker="CANONICAL_REPO_PROTECTION_BLOCKED",
            effective_owner="canonical_repo_protection_authority",
            effective_gate="pre_submit_source_integrity_gate",
        )
    return SourceIntegrityGate(
        raw_git_dirty_status=raw_git_dirty_status,
        raw_dirty_path_count=int(dirty_path_count),
        raw_source_reproducibility_status=raw_source_reproducibility_status,
        raw_canonical_repo_protection_status=raw_canonical_repo_protection_status,
        effective_status="PASSED",
        effective_blocker="",
        effective_owner="",
        effective_gate="pre_submit_source_integrity_gate",
    )


def _signal(
    *,
    input_name: str,
    classification: str,
    status: str,
    code: str,
    owner: str,
    gate: str,
    root_cause_id: str,
) -> ReadinessSignal:
    return ReadinessSignal(
        input_name=input_name,
        classification=classification,
        status=status,
        code=code,
        owner=owner,
        gate=gate,
        root_cause_id=root_cause_id,
    )


def _runtime_artifact_status_lines_under_repo(status_lines: list[str]) -> list[str]:
    out: list[str] = []
    for line in status_lines:
        path = line[3:].strip() if len(line) >= 4 else line.strip()
        if path.startswith("runtime/logs/") or path.startswith("runtime/process_state/"):
            out.append(path)
    return out


def _collect_readiness_signals(
    *,
    roots: RootResolution,
    current_day: CurrentCalendarDayStatus,
    latest_trading_day: LatestTradingDayEvidenceStatus,
    source_integrity_gate: SourceIntegrityGate | None,
    freshness_status: str,
    status_lines: list[str],
) -> list[ReadinessSignal]:
    signals: list[ReadinessSignal] = []
    if source_integrity_gate is not None and source_integrity_gate.effective_status == "BLOCKED":
        signals.append(
            _signal(
                input_name="source_integrity_gate",
                classification="HARD_SAFETY_INVARIANT",
                status="FAIL",
                code=source_integrity_gate.effective_blocker,
                owner=source_integrity_gate.effective_owner,
                gate=source_integrity_gate.effective_gate,
                root_cause_id="source_integrity",
            )
        )
    if roots.canonical_truth_root is None or roots.runtime_truth_root is None:
        signals.append(
            _signal(
                input_name="truth_root_resolution",
                classification="HARD_SAFETY_INVARIANT",
                status="FAIL",
                code="TRUTH_ROOT_UNRESOLVED",
                owner="runtime_truth_root_authority",
                gate="pre_submit_truth_root_gate",
                root_cause_id="truth_root_resolution",
            )
        )
    if current_day.status == "READY" and freshness_status != "FRESH":
        signals.append(
            _signal(
                input_name="packet_freshness",
                classification="HARD_SAFETY_INVARIANT",
                status="FAIL",
                code="CURRENT_DAY_PACKET_FRESHNESS_BLOCKED",
                owner="packet_freshness_authority",
                gate="pre_submit_packet_freshness_gate",
                root_cause_id="packet_freshness",
            )
        )
    if latest_trading_day.execution_mode_state == "UNKNOWN":
        signals.append(
            _signal(
                input_name="execution_mode_state",
                classification="HARD_SAFETY_INVARIANT",
                status="FAIL",
                code="EXECUTION_MODE_UNKNOWN_BLOCKED",
                owner="execution_mode_authority",
                gate="pre_submit_execution_mode_gate",
                root_cause_id="execution_mode_unknown",
            )
        )
    if (
        latest_trading_day.broker_transmit_enabled == "true"
        and (
            latest_trading_day.execution_mode_state != "PAPER_TRANSMIT_ENABLED"
            or latest_trading_day.execution_mode_environment != "PAPER"
        )
    ):
        signals.append(
            _signal(
                input_name="broker_transmit_paper_mode_authority",
                classification="HARD_SAFETY_INVARIANT",
                status="FAIL",
                code="BROKER_TRANSMIT_WITHOUT_PAPER_MODE_AUTHORITY",
                owner="execution_mode_authority",
                gate="pre_submit_execution_mode_gate",
                root_cause_id="broker_transmit_without_paper_authority",
            )
        )
    if latest_trading_day.runtime_service_state == "MISSING_REQUIRED_SERVICE":
        signals.append(
            _signal(
                input_name="runtime_service_state",
                classification="HARD_SAFETY_INVARIANT",
                status="FAIL",
                code="MISSING_REQUIRED_RUNTIME_SERVICE",
                owner="runtime_service_authority",
                gate="pre_submit_runtime_service_gate",
                root_cause_id="runtime_service_missing",
            )
        )
    if current_day.status == "READY" and source_integrity_gate is not None and source_integrity_gate.effective_status == "BLOCKED":
        signals.append(
            _signal(
                input_name="submit_path_source_integrity_consistency",
                classification="HARD_SAFETY_INVARIANT",
                status="FAIL",
                code="SUBMIT_PATH_ENABLED_WHILE_SOURCE_INTEGRITY_BLOCKED",
                owner="source_reproducibility_authority",
                gate="pre_submit_source_integrity_gate",
                root_cause_id="source_integrity",
            )
        )
    if _runtime_artifact_status_lines_under_repo(status_lines):
        signals.append(
            _signal(
                input_name="repo_runtime_artifacts",
                classification="HARD_SAFETY_INVARIANT",
                status="FAIL",
                code="SOURCE_REPRODUCIBILITY_BLOCKED",
                owner="source_reproducibility_authority",
                gate="pre_submit_source_integrity_gate",
                root_cause_id="source_integrity",
            )
        )
    if current_day.status not in {"READY", "DRY_RUN_COMPLETE"} and current_day.canonical_blocker:
        signals.append(
            _signal(
                input_name="current_day_readiness_surfaces",
                classification="OPERATIONAL_BLOCKER",
                status="FAIL",
                code=current_day.canonical_blocker,
                owner="current_calendar_day_runtime_status",
                gate="current_day_readiness_surfaces",
                root_cause_id=f"current_day:{current_day.canonical_blocker}",
            )
        )
    if latest_trading_day.status == "NOT_READY" and latest_trading_day.canonical_blocker:
        signals.append(
            _signal(
                input_name="latest_trading_day_evidence_status",
                classification="OPERATIONAL_BLOCKER",
                status="FAIL",
                code=latest_trading_day.canonical_blocker,
                owner="latest_trading_day_evidence_status",
                gate="latest_trading_day_evidence_gate",
                root_cause_id=f"latest_trading_day:{latest_trading_day.canonical_blocker}",
            )
        )
    if (
        latest_trading_day.market_data_state == "STALE"
        and latest_trading_day.market_data_operator_impact != "OPERATIONAL_BLOCKER"
    ):
        signals.append(
            _signal(
                input_name="market_data_state",
                classification="DEGRADATION_SIGNAL",
                status="WARN",
                code="MARKET_DATA_STALE_DIAGNOSTIC",
                owner="market_data_authority",
                gate="market_data_degradation_gate",
                root_cause_id="market_data_stale",
            )
        )
    if (
        latest_trading_day.market_data_state == "STALE"
        and latest_trading_day.market_data_operator_impact == "OPERATIONAL_BLOCKER"
    ):
        signals.append(
            _signal(
                input_name="market_data_state",
                classification="OPERATIONAL_BLOCKER",
                status="FAIL",
                code="MARKET_DATA_STALE_OPERATIONAL_BLOCKER",
                owner="market_data_authority",
                gate="market_data_operational_gate",
                root_cause_id="market_data_stale",
            )
        )
    signals.append(
        _signal(
            input_name="latest_paper_trade_attempt",
            classification="DIAGNOSTIC_ONLY",
            status="INFO",
            code="BROKER_PATH_OBSERVATION_ONLY",
            owner="broker_observation_authority",
            gate="broker_attempt_diagnostic",
            root_cause_id="broker_observation",
        )
    )
    signals.append(
        _signal(
            input_name="files_changed_since_last_commit",
            classification="REPORTING_ONLY",
            status="INFO",
            code="DIRTY_PATH_LIST_REPORTING_ONLY",
            owner="packet_reporting",
            gate="files_changed_section",
            root_cause_id="dirty_path_reporting",
        )
    )
    return signals


def _dedupe_enforcing_signals(signals: list[ReadinessSignal]) -> list[ReadinessSignal]:
    out: list[ReadinessSignal] = []
    seen: set[tuple[str, str]] = set()
    for signal in signals:
        if signal.classification in {"DIAGNOSTIC_ONLY", "REPORTING_ONLY"}:
            continue
        key = (signal.classification, signal.root_cause_id)
        if key in seen:
            continue
        seen.add(key)
        out.append(signal)
    return out


def _build_grade_profile(signals: list[ReadinessSignal]) -> GradeProfile:
    enforcing = _dedupe_enforcing_signals(signals)
    classes = {signal.classification for signal in enforcing}
    if "HARD_SAFETY_INVARIANT" in classes:
        cap = 40
        cap_reason = "HARD_SAFETY_INVARIANT"
    elif "OPERATIONAL_BLOCKER" in classes:
        cap = 65
        cap_reason = "OPERATIONAL_BLOCKER"
    elif "DEGRADATION_SIGNAL" in classes:
        cap = 85
        cap_reason = "DEGRADATION_SIGNAL"
    else:
        cap = 92
        cap_reason = "READY_BASELINE"

    root_causes = {signal.root_cause_id for signal in enforcing}
    deductions = 0
    for signal in enforcing:
        if signal.classification == "HARD_SAFETY_INVARIANT":
            deductions += 8
        elif signal.classification == "OPERATIONAL_BLOCKER":
            deductions += 6
        elif signal.classification == "DEGRADATION_SIGNAL":
            deductions += 3
    overall = min(92 - deductions, cap)
    overall = max(0, overall)
    readiness = min(overall, cap)
    functional = max(0, min(88 - sum(4 for s in enforcing if s.classification == "OPERATIONAL_BLOCKER"), cap))
    architecture = max(0, min(88 - sum(5 for s in enforcing if s.classification == "HARD_SAFETY_INVARIANT"), cap))
    return GradeProfile(
        overall_grade=int(overall),
        readiness_grade=int(readiness),
        functional_completeness_grade=int(functional),
        architecture_operational_integrity_grade=int(architecture),
        grade_cap=int(cap),
        cap_reason=cap_reason,
        unique_root_cause_count=len(root_causes),
    )


def _decide_final_readiness(
    *,
    current_day: CurrentCalendarDayStatus,
    latest_trading_day: LatestTradingDayEvidenceStatus,
    signals: list[ReadinessSignal],
) -> FinalReadinessDecision:
    enforcing = _dedupe_enforcing_signals(signals)
    hard = [s for s in enforcing if s.classification == "HARD_SAFETY_INVARIANT"]
    operational = [s for s in enforcing if s.classification == "OPERATIONAL_BLOCKER"]
    degradation = [s for s in enforcing if s.classification == "DEGRADATION_SIGNAL"]
    if hard:
        first = hard[0]
        status = "BLOCKED"
        reason = f"hard safety invariant failed: {first.code}"
    elif operational:
        first = operational[0]
        status = "NOT_READY"
        reason = f"operational blocker failed: {first.code}"
    elif degradation:
        first = degradation[0]
        status = "DEGRADED_READY" if current_day.status == "READY" else "NOT_READY"
        reason = f"degradation signal present: {first.code}"
    elif current_day.status == "READY" and latest_trading_day.status == "READY":
        first = None
        status = "READY"
        reason = f"current trading day {current_day.day_utc} readiness surfaces report READY"
    elif current_day.status == "DRY_RUN_COMPLETE":
        first = None
        status = "NOT_READY"
        reason = f"current trading day {current_day.day_utc} completed in dry-run mode"
    else:
        first = _signal(
            input_name="current_day_readiness_surfaces",
            classification="OPERATIONAL_BLOCKER",
            status="FAIL",
            code=current_day.canonical_blocker or "CURRENT_DAY_NOT_READY",
            owner="current_calendar_day_runtime_status",
            gate="current_day_readiness_surfaces",
            root_cause_id=f"current_day:{current_day.canonical_blocker or 'CURRENT_DAY_NOT_READY'}",
        )
        status = "NOT_READY"
        reason = f"current trading day {current_day.day_utc} is not authorized for submission"
    return FinalReadinessDecision(
        status=status,
        canonical_blocker=first.code if first is not None else "",
        owning_subsystem=first.owner if first is not None else "current_calendar_day_runtime_status",
        owning_gate=first.gate if first is not None else "current_day_readiness_surfaces",
        reason=reason,
        signals=signals,
        grade_profile=_build_grade_profile(signals),
    )


def _build_paper_status(
    roots: RootResolution,
    source_integrity_gate: SourceIntegrityGate | None = None,
    status_lines: list[str] | None = None,
) -> PaperStatus:
    current_day = _build_current_calendar_day_runtime_status(roots)
    latest_trading_day = _build_latest_trading_day_evidence_status(roots)
    day_run = _load_day_run_ledger_status(roots, current_day.day_utc)

    freshness_day = (
        current_day.day_utc
        if DATE_RE.match(current_day.day_utc)
        else latest_trading_day.evidence_day_utc
    )
    freshness_status = _freshness_status_for_day(freshness_day)
    ledger_ready = day_run.final_status in {"PAPER_READY", "TRADING_ACTIVE", "EOD_COMPLETE"}
    signals: list[ReadinessSignal] = []
    source_blocked = source_integrity_gate is not None and source_integrity_gate.effective_status == "BLOCKED"
    if source_blocked:
        signals.append(
            _signal(
                input_name="source_integrity_gate",
                classification="HARD_SAFETY_INVARIANT",
                status="FAIL",
                code=source_integrity_gate.effective_blocker,
                owner=source_integrity_gate.effective_owner,
                gate=source_integrity_gate.effective_gate,
                root_cause_id="source_integrity",
            )
        )
    if (not ledger_ready) and (not source_blocked):
        signals.append(
            _signal(
                input_name="aegis_day_run_ledger",
                classification="OPERATIONAL_BLOCKER",
                status="FAIL",
                code=day_run.canonical_blocker or "DAY_RUN_LEDGER_MISSING",
                owner="aegis_day_run_ledger",
                gate=day_run.canonical_phase or "DAY_RUN",
                root_cause_id=f"day_run:{day_run.canonical_phase or 'DAY_RUN'}:{day_run.canonical_blocker or 'DAY_RUN_LEDGER_MISSING'}",
            )
        )
    signals.append(
        _signal(
            input_name="legacy_current_day_readiness_surfaces",
            classification="DIAGNOSTIC_ONLY",
            status="INFO" if current_day.status in {"READY", "DRY_RUN_COMPLETE"} else "WARN",
            code=current_day.canonical_blocker or current_day.status,
            owner="current_calendar_day_runtime_status",
            gate="legacy_current_day_readiness_surfaces",
            root_cause_id="legacy_current_day_supporting_evidence",
        )
    )
    signals.append(
        _signal(
            input_name="legacy_latest_trading_day_evidence_status",
            classification="DIAGNOSTIC_ONLY",
            status="INFO" if latest_trading_day.status in {"READY", "DRY_RUN_COMPLETE"} else "WARN",
            code=latest_trading_day.canonical_blocker or latest_trading_day.status,
            owner="latest_trading_day_evidence_status",
            gate="legacy_latest_trading_day_evidence_gate",
            root_cause_id="legacy_latest_trading_day_supporting_evidence",
        )
    )
    signals.append(
        _signal(
            input_name="latest_paper_trade_attempt",
            classification="DIAGNOSTIC_ONLY",
            status="INFO",
            code="BROKER_PATH_OBSERVATION_ONLY",
            owner="broker_observation_authority",
            gate="broker_attempt_diagnostic",
            root_cause_id="broker_observation",
        )
    )
    signals.append(
        _signal(
            input_name="files_changed_since_last_commit",
            classification="REPORTING_ONLY",
            status="INFO",
            code="DIRTY_PATH_LIST_REPORTING_ONLY",
            owner="packet_reporting",
            gate="files_changed_section",
            root_cause_id="dirty_path_reporting",
        )
    )
    if source_blocked and source_integrity_gate is not None:
        final_decision = FinalReadinessDecision(
            status="BLOCKED",
            canonical_blocker=source_integrity_gate.effective_blocker,
            owning_subsystem=source_integrity_gate.effective_owner,
            owning_gate=source_integrity_gate.effective_gate,
            reason=f"hard safety invariant failed: {source_integrity_gate.effective_blocker}",
            signals=signals,
            grade_profile=_build_grade_profile(signals),
        )
    else:
        final_decision = FinalReadinessDecision(
            status=day_run.final_status if ledger_ready else "NOT_READY",
            canonical_blocker="" if ledger_ready else (day_run.canonical_blocker or "DAY_RUN_LEDGER_MISSING"),
            owning_subsystem="aegis_day_run_ledger",
            owning_gate=day_run.canonical_phase or "DAY_RUN",
            reason=(
                f"day-run ledger final_status={day_run.final_status}"
                if day_run.exists
                else "current-day day-run ledger missing; run BOD/day ledger before audit"
            ),
            signals=signals,
            grade_profile=_build_grade_profile(signals),
        )

    section_lines = [
        "## Current Calendar Day Runtime Status",
        "",
        f"- day_utc: {current_day.day_utc}",
        f"- is_trading_session: {current_day.is_trading_session}",
        f"- paper_session_authority_path: {current_day.paper_session_authority_path}",
        f"- paper_session_status: {current_day.paper_session_status}",
        f"- service_status_summary: {current_day.service_status_summary}",
        f"- status: {current_day.status}",
        f"- canonical_blocker: {current_day.canonical_blocker}",
        f"- expected_non_trading_day: {'true' if current_day.expected_non_trading_day else 'false'}",
        "",
        "## Latest Trading Day Evidence Status",
        "",
        f"- evidence_day_utc: {latest_trading_day.evidence_day_utc}",
        f"- submit_boundary_status_path: {latest_trading_day.submit_boundary_status_path}",
        f"- closure_authority_path: {latest_trading_day.closure_authority_path}",
        f"- trade_lineage_graph_path: {latest_trading_day.trade_lineage_graph_path}",
        f"- execution_lifecycle_authority_path: {latest_trading_day.execution_lifecycle_authority_path}",
        f"- runtime_service_authority_path: {latest_trading_day.runtime_service_authority_path}",
        f"- market_data_authority_path: {latest_trading_day.market_data_authority_path}",
        f"- strategy_decision_authority_path: {latest_trading_day.strategy_decision_authority_path}",
        f"- portfolio_account_authority_path: {latest_trading_day.portfolio_account_authority_path}",
        f"- risk_sizing_authority_path: {latest_trading_day.risk_sizing_authority_path}",
        f"- execution_mode_authority_path: {latest_trading_day.execution_mode_authority_path}",
        f"- trading_day_closure_authority_path: {latest_trading_day.trading_day_closure_authority_path}",
        f"- runtime_service_state: {latest_trading_day.runtime_service_state}",
        f"- market_data_state: {latest_trading_day.market_data_state}",
        f"- market_data_operator_impact: {latest_trading_day.market_data_operator_impact}",
        f"- strategy_decision_state: {latest_trading_day.strategy_decision_state}",
        f"- strategy_intent_count: {latest_trading_day.strategy_intent_count}",
        f"- strategy_zero_intent_reason: {latest_trading_day.strategy_zero_intent_reason}",
        f"- portfolio_account_state: {latest_trading_day.portfolio_account_state}",
        f"- portfolio_cash_total_cents: {latest_trading_day.portfolio_cash_total_cents}",
        f"- portfolio_net_liquidation_cents: {latest_trading_day.portfolio_net_liquidation_cents}",
        f"- risk_sizing_state: {latest_trading_day.risk_sizing_state}",
        f"- risk_final_size: {latest_trading_day.risk_final_size}",
        f"- execution_mode_state: {latest_trading_day.execution_mode_state}",
        f"- execution_mode_environment: {latest_trading_day.execution_mode_environment}",
        f"- broker_transmit_enabled: {latest_trading_day.broker_transmit_enabled}",
        f"- trading_day_closure_state: {latest_trading_day.trading_day_closure_state}",
        f"- current_head_path: {latest_trading_day.current_head_path}",
        f"- submission_index_path: {latest_trading_day.submission_index_path}",
        f"- status: {latest_trading_day.status}",
        f"- canonical_blocker: {latest_trading_day.canonical_blocker}",
        "",
        "## Aegis Day Run Ledger",
        "",
        f"- day_run_ledger_path: {day_run.path}",
        f"- day_run_ledger_exists: {'true' if day_run.exists else 'false'}",
        f"- day_utc: {day_run.day_utc}",
        f"- environment: {day_run.environment}",
        f"- final_status: {day_run.final_status}",
        f"- canonical_phase: {day_run.canonical_phase}",
        f"- canonical_blocker: {day_run.canonical_blocker}",
        f"- root_cause_chain: {json.dumps(day_run.root_cause_chain, sort_keys=True)}",
        f"- downstream_consequences: {json.dumps(day_run.downstream_consequences, sort_keys=True)}",
        f"- operator_next_action: {day_run.operator_next_action}",
        f"- updated_at_utc: {day_run.updated_at_utc}",
        "",
        "## Aegis Paper-Trading Status",
        "",
        f"- status: {final_decision.status}",
        f"- canonical_blocker: {final_decision.canonical_blocker}",
        f"- latest_trading_day_blocker: {latest_trading_day.canonical_blocker}",
        f"- owning_subsystem: {final_decision.owning_subsystem}",
        f"- owning_gate: {final_decision.owning_gate}",
        (
            f"- effective_source_integrity_gate_status: "
            f"{source_integrity_gate.effective_status if source_integrity_gate else 'UNKNOWN'}"
        ),
        (
            f"- effective_source_integrity_gate_blocker: "
            f"{source_integrity_gate.effective_blocker if source_integrity_gate else 'UNKNOWN'}"
        ),
        (
            f"- effective_source_integrity_gate_owner: "
            f"{source_integrity_gate.effective_owner if source_integrity_gate else 'UNKNOWN'}"
        ),
        (
            f"- evidence: day_run={day_run.path} ; {current_day.evidence} ; latest_trading_day={latest_trading_day.evidence_day_utc}:{latest_trading_day.evidence}"
        ),
        f"- reason: {final_decision.reason}",
        "",
        "## Aegis Brittleness Map",
        "",
        "- final_readiness_authority: aegis_day_run_ledger_v1",
        "- decision_model: day_run ledger owns final readiness; legacy authority artifacts are supporting evidence only; downstream blockers do not override the first blocked ledger phase",
        "- inputs:",
    ]
    for signal in signals:
        section_lines.extend(
            [
                f"  - input: {signal.input_name}",
                f"    - classification: {signal.classification}",
                f"    - status: {signal.status}",
                f"    - code: {signal.code}",
                f"    - owner: {signal.owner}",
                f"    - gate: {signal.gate}",
                f"    - root_cause_id: {signal.root_cause_id}",
            ]
        )
    section_lines.extend(
        [
            "",
            "## Aegis Grade Stabilization",
            "",
            f"- overall_grade: {final_decision.grade_profile.overall_grade}",
            f"- readiness_grade: {final_decision.grade_profile.readiness_grade}",
            f"- functional_completeness_grade: {final_decision.grade_profile.functional_completeness_grade}",
            f"- architecture_operational_integrity_grade: {final_decision.grade_profile.architecture_operational_integrity_grade}",
            f"- grade_cap: {final_decision.grade_profile.grade_cap}",
            f"- cap_reason: {final_decision.grade_profile.cap_reason}",
            f"- unique_root_cause_count: {final_decision.grade_profile.unique_root_cause_count}",
            "- double_count_policy: one deduction per unique enforcing root_cause_id; diagnostic/reporting signals do not affect status or caps",
            "",
        ]
    )
    return PaperStatus(
        section="\n".join(section_lines),
        status=final_decision.status,
        freshness_status=freshness_status,
        day_utc=current_day.day_utc,
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
    submit_mode = (
        classify_paper_submit_mode_status_v1(execution_root=roots.runtime_truth_root, day_utc=day_utc)
        if roots.runtime_truth_root is not None and DATE_RE.match(day_utc)
        else {}
    )
    execution_mode_path = _report_authority_path(
        roots.canonical_truth_root,
        "execution_mode_authority_v1",
        day_utc,
        "execution_mode_authority.v1.json",
    )
    execution_mode_doc = _read_json(execution_mode_path)
    submit_mode_status = str(submit_mode.get("submit_mode_status") or "UNKNOWN").strip().upper()
    dry_run_policy = str(submit_mode.get("dry_run_policy") or "UNKNOWN").strip().upper()
    broker_transmit_enabled = submit_mode.get("broker_transmit_enabled")
    broker_order_transmitted = bool(submit_mode.get("broker_order_transmitted") is True)
    missing_broker_ids_blocker = bool(submit_mode.get("missing_broker_ids_blocker") is True)
    missing_broker_ids_diagnostic = bool(submit_mode.get("missing_broker_ids_diagnostic") is True)
    execution_mode_state = "UNKNOWN"
    if isinstance(execution_mode_doc, dict):
        execution_mode_state = str(execution_mode_doc.get("mode_state") or "UNKNOWN").strip().upper()
        dry_run_policy = str(execution_mode_doc.get("dry_run_policy") or dry_run_policy).strip().upper()
        broker_transmit_enabled = execution_mode_doc.get("broker_transmit_enabled")
        broker_order_transmitted = bool(execution_mode_doc.get("broker_transmit_enabled") is True)
        missing_broker_ids_blocker = bool(execution_mode_doc.get("missing_broker_ids_blocker") is True)
        missing_broker_ids_diagnostic = bool(execution_mode_doc.get("missing_broker_ids_diagnostic") is True)
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
    trade_lineage_path, trade_lineage_row = _trade_lineage_row_for_submission(
        roots.canonical_truth_root,
        day_utc,
        submission_id,
    )
    identity_state = str((trade_lineage_row or {}).get("identity_state") or "UNKNOWN").strip().upper()
    authority_lifecycle_state = str((trade_lineage_row or {}).get("lifecycle_state") or "").strip().upper()

    lifecycle_updated = "YES" if fill_path is not None or bool(stream_paths) else "NO"
    lifecycle_status = authority_lifecycle_state or str((fill or {}).get("lifecycle_status") or "").strip().upper()
    filled_qty = (fill or {}).get("filled_qty")
    if submit_mode_status == "DRY_RUN_COMPLETE":
        failure_stage = "NONE"
    elif lifecycle_status == "OPEN" and filled_qty == 0:
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
    if execution_mode_path is not None and execution_mode_path.exists():
        evidence_paths.append(str(execution_mode_path))
    if trade_lineage_path is not None and trade_lineage_path.exists():
        evidence_paths.append(str(trade_lineage_path))
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
            f"- dry_run_policy: {dry_run_policy}",
            f"- broker_transmit_enabled: {str(broker_transmit_enabled).lower() if isinstance(broker_transmit_enabled, bool) else 'UNKNOWN'}",
            f"- execution_mode: {execution_mode_state}",
            f"- submit_mode_status: {submit_mode_status}",
            f"- broker_order_transmitted: {'YES' if broker_order_transmitted else 'NO'}",
            f"- IB received order: {ib_received}",
            f"- broker order_id: {order_id if isinstance(order_id, int) else 'UNKNOWN'}",
            f"- broker perm_id: {perm_id if isinstance(perm_id, int) else 'UNKNOWN'}",
            f"- identity_state: {identity_state}",
            f"- lifecycle_state: {lifecycle_status or 'UNKNOWN'}",
            f"- missing_broker_ids: {'BLOCKER' if missing_broker_ids_blocker else ('DIAGNOSTIC' if missing_broker_ids_diagnostic else 'NO')}",
            f"- lifecycle updated: {lifecycle_updated}",
            f"- failure_stage: {failure_stage}",
            f"- evidence: {' ; '.join(evidence_paths)}",
            "",
        ]
    )
    return "\n".join(lines)


def _build_operating_contract_section(roots: RootResolution) -> str:
    lines = ["## Aegis Operating Contract", ""]
    if roots.canonical_truth_root is None:
        lines.extend(["- status: UNKNOWN", "- evidence: NOT_FOUND", ""])
        return "\n".join(lines)
    day_utc = _today_utc_day()
    today_summary = roots.canonical_truth_root / "reports" / "aegis_daily_operator_summary_v1" / day_utc / "aegis_daily_operator_summary.v1.json"
    if not today_summary.exists():
        latest_day, _latest_path = _latest_day_with_file(
            roots.canonical_truth_root,
            "reports/aegis_daily_operator_summary_v1",
            "aegis_daily_operator_summary.v1.json",
        )
        if latest_day:
            day_utc = latest_day
    contract_path = roots.canonical_truth_root / "reports" / "aegis_operating_contract_v1" / day_utc / "aegis_operating_contract.v1.json"
    graph_path = roots.canonical_truth_root / "reports" / "aegis_authority_graph_v1" / day_utc / "aegis_authority_graph.v1.json"
    ledger_path = roots.canonical_truth_root / "reports" / "aegis_day_evidence_ledger_v1" / day_utc / "aegis_day_evidence_ledger.v1.json"
    summary_path = roots.canonical_truth_root / "reports" / "aegis_daily_operator_summary_v1" / day_utc / "aegis_daily_operator_summary.v1.json"
    contract = _read_json(contract_path)
    graph = _read_json(graph_path)
    ledger = _read_json(ledger_path)
    summary = _read_json(summary_path)
    if not any(isinstance(item, dict) for item in (contract, graph, ledger, summary)):
        lines.extend(
            [
                f"- day_utc: {day_utc}",
                "- status: NOT_RECORDED",
                f"- operating_contract_path: {contract_path}",
                f"- authority_graph_path: {graph_path}",
                f"- evidence_ledger_path: {ledger_path}",
                f"- operator_summary_path: {summary_path}",
                "",
            ]
        )
        return "\n".join(lines)

    graph_nodes = graph.get("authority_nodes") if isinstance(graph, dict) else []
    graph_blockers = graph.get("blocking_nodes") if isinstance(graph, dict) else []
    ledger_commands = ledger.get("commands") if isinstance(ledger, dict) else []
    first_blocker = summary.get("first_blocker") if isinstance(summary, dict) and isinstance(summary.get("first_blocker"), dict) else {}
    lines.extend(
        [
            f"- day_utc: {day_utc}",
            f"- mode: {str((contract or summary or {}).get('mode') or 'UNKNOWN')}",
            f"- run_style: {str((contract or summary or {}).get('run_style') or 'UNKNOWN')}",
            f"- final_no_silent_day_outcome: {str((summary or ledger or {}).get('no_silent_day_outcome') or (ledger or {}).get('final_daily_outcome') or 'UNKNOWN')}",
            f"- authority_graph_nodes: {len(graph_nodes) if isinstance(graph_nodes, list) else 0}",
            f"- authority_graph_blockers: {len(graph_blockers) if isinstance(graph_blockers, list) else 0}",
            f"- evidence_ledger_command_count: {len(ledger_commands) if isinstance(ledger_commands, list) else 0}",
            f"- first_blocker: {str(first_blocker.get('code') or '<none>')}",
            f"- first_blocker_owner: {str(first_blocker.get('owner') or first_blocker.get('owning_subsystem') or '<none>')}",
            f"- dry_run: {str((summary or {}).get('dry_run') if isinstance(summary, dict) else 'UNKNOWN').lower()}",
            f"- broker_transmit_enabled: {str((summary or {}).get('broker_transmit_enabled') if isinstance(summary, dict) else 'UNKNOWN').lower()}",
            f"- broker_orders_transmitted: {str((summary or {}).get('broker_orders_transmitted') if isinstance(summary, dict) else 'UNKNOWN').lower()}",
            f"- closure_status: {str((summary or {}).get('closure_status') if isinstance(summary, dict) else 'UNKNOWN')}",
            f"- operating_contract_path: {contract_path}",
            f"- authority_graph_path: {graph_path}",
            f"- evidence_ledger_path: {ledger_path}",
            f"- operator_summary_path: {summary_path}",
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
    else:
        source_reproducibility_status = "REPRODUCIBLE"
    source_integrity_gate = _build_source_integrity_gate(
        git_dirty_status=dirty,
        dirty_path_count=dirty_path_count,
        source_reproducibility_status=source_reproducibility_status,
        canonical_repo_protection_status=canonical_repo_protection_status,
    )
    export_id = f"{now.strftime('%Y%m%dT%H%M%SZ')}_{(commit[:12] if commit != 'UNKNOWN' else 'nogit')}"

    roots = _resolve_truth_roots()
    paper_status = _build_paper_status(roots, source_integrity_gate, status_lines)

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
        "- raw_git_dirty_status: " + source_integrity_gate.raw_git_dirty_status,
        "- raw_dirty_path_count: " + str(source_integrity_gate.raw_dirty_path_count),
        "- raw_source_reproducibility_status: " + source_integrity_gate.raw_source_reproducibility_status,
        "- raw_canonical_repo_protection_status: " + source_integrity_gate.raw_canonical_repo_protection_status,
        "- effective_source_integrity_gate_status: " + source_integrity_gate.effective_status,
        "- effective_source_integrity_gate_blocker: " + source_integrity_gate.effective_blocker,
        "- effective_source_integrity_gate_owner: " + source_integrity_gate.effective_owner,
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
        _build_operating_contract_section(roots),
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
        "## Aegis Operating Contract",
        "## Current Calendar Day Runtime Status",
        "## Latest Trading Day Evidence Status",
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
