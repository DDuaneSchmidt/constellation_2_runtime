#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_RUNTIME_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
DEFAULT_PAPER_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_utc(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _str_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def _upper_set(values: List[str]) -> set[str]:
    return {str(item).strip().upper() for item in values if str(item).strip()}


def _first_timestamp(payload: Dict[str, Any]) -> Optional[datetime]:
    for key in (
        "generated_at_utc",
        "produced_at_utc",
        "evaluated_at_utc",
        "produced_utc",
        "decision_timestamp_utc",
        "as_of_utc",
    ):
        ts = _parse_iso_utc(payload.get(key))
        if ts is not None:
            return ts
    return None


def _latest_timestamp(payload: Dict[str, Any]) -> Optional[datetime]:
    candidates: List[datetime] = []
    direct = _first_timestamp(payload)
    if direct is not None:
        candidates.append(direct)
    for key in ("fact_refs", "required_boundary_checks", "failed_checks"):
        rows = payload.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            ts = _parse_iso_utc(row.get("artifact_timestamp_utc"))
            if ts is not None:
                candidates.append(ts)
    return max(candidates) if candidates else None


def _dedupe_sources(paths: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for path in paths:
        text = str(path or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return sorted(out)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_bool(value: Any) -> bool:
    return bool(value is True)


@dataclass
class ArtifactState:
    logical_name: str
    path: Path
    payload: Optional[Dict[str, Any]]
    error: Optional[str]
    day_mismatch: Optional[str]
    observed_timestamp: Optional[datetime]

    @property
    def usable(self) -> bool:
        return self.payload is not None and self.error is None and self.day_mismatch is None


def _read_json_artifact(logical_name: str, path: Path, expected_day_utc: str) -> ArtifactState:
    if not path.exists() or not path.is_file():
        return ArtifactState(
            logical_name=logical_name,
            path=path,
            payload=None,
            error="MISSING_ARTIFACT",
            day_mismatch=None,
            observed_timestamp=None,
        )
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # pragma: no cover - defensive
        return ArtifactState(
            logical_name=logical_name,
            path=path,
            payload=None,
            error=f"UNPARSABLE_ARTIFACT:{type(exc).__name__}",
            day_mismatch=None,
            observed_timestamp=None,
        )
    if not isinstance(raw, dict):
        return ArtifactState(
            logical_name=logical_name,
            path=path,
            payload=None,
            error="UNPARSABLE_ARTIFACT:NON_OBJECT_JSON",
            day_mismatch=None,
            observed_timestamp=None,
        )
    day_value = str(raw.get("day_utc") or raw.get("day") or "").strip()
    day_mismatch: Optional[str] = None
    if day_value and day_value != expected_day_utc:
        day_mismatch = f"DAY_MISMATCH:expected={expected_day_utc}:observed={day_value}"
    return ArtifactState(
        logical_name=logical_name,
        path=path,
        payload=raw,
        error=None,
        day_mismatch=day_mismatch,
        observed_timestamp=_latest_timestamp(raw),
    )


def _build_required_artifact_paths(day_utc: str, runtime_truth_root: Path, paper_truth_root: Path) -> Dict[str, Path]:
    reports_root = runtime_truth_root / "reports"
    return {
        "paper_session_authority_v1": reports_root / "paper_session_authority_v1" / day_utc / "paper_session_authority.v1.json",
        "submit_boundary_status_v1": reports_root / "submit_boundary_status_v1" / day_utc / "submit_boundary_status.v1.json",
        "paper_session_ledger_v1": reports_root / "paper_session_ledger_v1" / day_utc / "paper_session_ledger.v1.json",
        "trading_day_state_machine_v1": reports_root / "trading_day_state_machine_v1" / day_utc / "trading_day_state_machine.v1.json",
        "trading_day_intent_generation_v1": reports_root / "trading_day_intent_generation_v1" / day_utc / "trading_day_intent_generation.v1.json",
        "trade_readiness_decision_v1": reports_root / "trade_readiness_decision_v1" / day_utc / "trade_readiness_decision.v1.json",
        "operator_day_authority_summary_v1": reports_root / "operator_day_authority_summary_v1" / day_utc / "operator_day_authority_summary.v1.json",
        "aegis_day_closure_authority_v1": reports_root / "aegis_day_closure_authority_v1" / day_utc / "aegis_day_closure_authority.v1.json",
        "submission_index_v1": paper_truth_root / "submission_index_v1" / day_utc / "submission_index.v1.json",
        "reconciliation_report_v3": paper_truth_root / "reports" / "reconciliation_report_v3" / day_utc / "reconciliation_report.v3.json",
    }


def _find_latest_granted_trading_day(runtime_truth_root: Path, day_utc: str) -> Tuple[Optional[str], Optional[Path]]:
    search_root = runtime_truth_root / "reports" / "paper_session_authority_v1"
    if not search_root.exists():
        return None, None
    best_day: Optional[str] = None
    best_path: Optional[Path] = None
    for candidate in sorted(search_root.rglob("paper_session_authority.v1.json")):
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        observed_day = str(payload.get("day_utc") or "").strip()
        authority_status = str(payload.get("authority_status") or "").strip().upper()
        if not observed_day or observed_day > day_utc:
            continue
        if authority_status != "GRANTED":
            continue
        if best_day is None or observed_day > best_day:
            best_day = observed_day
            best_path = candidate
    return best_day, best_path


def _severity_rank(severity: str) -> int:
    normalized = str(severity or "").strip().upper()
    return {"INFO": 1, "WARN": 2, "BLOCKER": 3, "CRITICAL": 4}.get(normalized, 0)


def _overall_status(alerts: List[Dict[str, Any]]) -> str:
    actionable = [row for row in alerts if str(row.get("status") or "").upper() in {"ACTIVE", "UNKNOWN"}]
    if any(str(row.get("severity") or "").upper() == "CRITICAL" for row in actionable):
        return "CRITICAL"
    if any(str(row.get("severity") or "").upper() == "BLOCKER" for row in actionable):
        return "BLOCKED"
    if any(str(row.get("severity") or "").upper() == "WARN" for row in actionable):
        return "WARN"
    if alerts:
        return "OK"
    return "UNKNOWN"


def _alert_id_from_dedupe(category: str, dedupe_key: str) -> str:
    token = hashlib.sha256(dedupe_key.encode("utf-8")).hexdigest()[:12]
    return f"{category}:{token}"


def _load_prior_first_seen(path: Path, expected_day_utc: str) -> Dict[str, str]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    if str(payload.get("schema_version") or "") != "aegis_alert_ledger.v1":
        return {}
    if str(payload.get("trading_day_utc") or "") != expected_day_utc:
        return {}
    rows = payload.get("alerts")
    if not isinstance(rows, list):
        return {}
    out: Dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        dedupe = str(row.get("dedupe_key") or "").strip()
        first_seen = str(row.get("first_seen_at_utc") or "").strip()
        if dedupe and first_seen:
            out[dedupe] = first_seen
    return out


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_metrics(path: Path, rows: Dict[str, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Aegis Alert Authority metrics export.",
        "# This file is a plain text artifact and does not require Prometheus to be installed.",
        "# Metric semantics for unknown values: -1 means UNKNOWN / cannot be proven from authoritative artifacts.",
        "",
        "# TYPE aegis_alert_info_count gauge",
        f"aegis_alert_info_count {rows['aegis_alert_info_count']}",
        "# TYPE aegis_alert_warn_count gauge",
        f"aegis_alert_warn_count {rows['aegis_alert_warn_count']}",
        "# TYPE aegis_alert_blocker_count gauge",
        f"aegis_alert_blocker_count {rows['aegis_alert_blocker_count']}",
        "# TYPE aegis_alert_critical_count gauge",
        f"aegis_alert_critical_count {rows['aegis_alert_critical_count']}",
        "# TYPE aegis_can_trade gauge",
        f"aegis_can_trade {rows['aegis_can_trade']}",
        "# TYPE aegis_submit_boundary_authorized gauge",
        f"aegis_submit_boundary_authorized {rows['aegis_submit_boundary_authorized']}",
        "# TYPE aegis_contradictory_control_surfaces gauge",
        f"aegis_contradictory_control_surfaces {rows['aegis_contradictory_control_surfaces']}",
        "# TYPE aegis_runtime_truth_freshness_seconds gauge",
        f"aegis_runtime_truth_freshness_seconds {rows['aegis_runtime_truth_freshness_seconds']}",
        "# TYPE aegis_open_lifecycle_count gauge",
        f"aegis_open_lifecycle_count {rows['aegis_open_lifecycle_count']}",
        "# TYPE aegis_reconciliation_ok gauge",
        f"aegis_reconciliation_ok {rows['aegis_reconciliation_ok']}",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="run_aegis_alert_authority_v1")
    ap.add_argument("--day-utc", default=_utc_now().date().isoformat())
    ap.add_argument("--runtime-truth-root", default=str(DEFAULT_RUNTIME_TRUTH_ROOT))
    ap.add_argument("--paper-truth-root", default=str(DEFAULT_PAPER_TRUTH_ROOT))
    ap.add_argument("--stale-seconds", type=int, default=12 * 60 * 60)
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    now = _utc_now()
    generated_at_utc = _iso_utc(now)
    runtime_truth_root = Path(args.runtime_truth_root).resolve()
    paper_truth_root = Path(args.paper_truth_root).resolve()

    required_paths = _build_required_artifact_paths(day_utc, runtime_truth_root, paper_truth_root)
    artifacts: Dict[str, ArtifactState] = {
        name: _read_json_artifact(name, path, day_utc)
        for name, path in required_paths.items()
    }

    alert_rows: List[Dict[str, Any]] = []
    dedupe_seen = set()

    def add_alert(
        *,
        severity: str,
        status: str,
        category: str,
        owning_subsystem: str,
        owning_gate: str,
        summary: str,
        operator_action: str,
        source_artifacts: List[str],
        dedupe_key: Optional[str] = None,
    ) -> None:
        safe_sources = _dedupe_sources(source_artifacts)
        if not safe_sources:
            safe_sources = [str(runtime_truth_root)]
        computed_dedupe = dedupe_key or f"{category}|{owning_gate}|{day_utc}|{'|'.join(safe_sources)}|{status}|{severity}"
        if computed_dedupe in dedupe_seen:
            return
        dedupe_seen.add(computed_dedupe)
        alert_rows.append(
            {
                "alert_id": _alert_id_from_dedupe(category, computed_dedupe),
                "severity": str(severity).strip().upper(),
                "status": str(status).strip().upper(),
                "category": category,
                "owning_subsystem": owning_subsystem,
                "owning_gate": owning_gate,
                "dedupe_key": computed_dedupe,
                "summary": summary,
                "operator_action": operator_action,
                "source_artifacts": safe_sources,
            }
        )

    # Required evidence integrity alerts.
    for name, state in sorted(artifacts.items(), key=lambda item: item[0]):
        if state.error is not None:
            add_alert(
                severity="BLOCKER",
                status="UNKNOWN",
                category="REQUIRED_ARTIFACT_MISSING",
                owning_subsystem="AEGIS",
                owning_gate=name,
                summary=f"Required artifact unavailable: {name} ({state.error}).",
                operator_action=f"Restore authoritative artifact producer for {name} and rerun aegis alert authority.",
                source_artifacts=[str(state.path)],
                dedupe_key=f"REQUIRED_ARTIFACT_MISSING|{name}|{state.error}|{state.path}",
            )
        if state.day_mismatch is not None:
            add_alert(
                severity="BLOCKER",
                status="UNKNOWN",
                category="RUNTIME_TRUTH_STALE",
                owning_subsystem="AEGIS",
                owning_gate=name,
                summary=f"Artifact day mismatch for {name}: {state.day_mismatch}.",
                operator_action=f"Regenerate {name} for {day_utc} and verify canonical day linkage.",
                source_artifacts=[str(state.path)],
                dedupe_key=f"RUNTIME_TRUTH_STALE|{name}|{state.day_mismatch}|{state.path}",
            )

    paper_session = artifacts["paper_session_authority_v1"]
    submit_boundary = artifacts["submit_boundary_status_v1"]
    paper_ledger = artifacts["paper_session_ledger_v1"]
    intent_generation = artifacts["trading_day_intent_generation_v1"]
    trade_readiness = artifacts["trade_readiness_decision_v1"]
    state_machine = artifacts["trading_day_state_machine_v1"]
    aegis_closure = artifacts["aegis_day_closure_authority_v1"]
    submission_index = artifacts["submission_index_v1"]
    reconciliation = artifacts["reconciliation_report_v3"]

    non_trading_day = False
    non_trading_only = False
    if paper_session.usable:
        session_codes = _upper_set(_str_list(paper_session.payload.get("blocking_reason_codes")))
        authority_status = str(paper_session.payload.get("authority_status") or "").strip().upper()
        non_trading_day = "NON_TRADING_DAY" in session_codes
        non_trading_only = non_trading_day and session_codes.issubset({"NON_TRADING_DAY"})

        if authority_status == "DENIED":
            if non_trading_only:
                severity, status = "INFO", "EXPECTED"
                summary = "Paper session is denied as expected for a non-trading day."
            else:
                severity, status = "BLOCKER", "ACTIVE"
                summary = "Paper session authority is denied outside expected non-trading-day conditions."
            add_alert(
                severity=severity,
                status=status,
                category="PAPER_SESSION_DENIED",
                owning_subsystem="AEGIS",
                owning_gate="paper_session_authority_v1",
                summary=summary,
                operator_action="Validate paper_session_authority_v1 safety checks and blocker reason codes.",
                source_artifacts=[
                    str(paper_session.path),
                    *[str(item.get("artifact_path") or "") for item in (paper_session.payload.get("blocking_reason_details") or []) if isinstance(item, dict)],
                ],
            )

    submit_non_trading_codes = {"NON_TRADING_DAY", "MARKET_CALENDAR_NON_TRADING_SESSION", "PAPER_SESSION_LEDGER_EVIDENCE_DENIED"}
    submit_unexpected_codes: List[str] = []
    submit_boundary_authorized_metric = -1
    if submit_boundary.usable:
        boundary_status = str(submit_boundary.payload.get("boundary_status") or "").strip().upper()
        submit_codes = _upper_set(
            _str_list(submit_boundary.payload.get("reason_codes")) + _str_list(submit_boundary.payload.get("blocking_codes"))
        )
        submit_unexpected_codes = sorted(code for code in submit_codes if code not in submit_non_trading_codes)
        submit_authorized = _safe_bool(submit_boundary.payload.get("submission_authorized"))
        submit_boundary_authorized_metric = 1 if submit_authorized else 0

        if boundary_status in {"BLOCKED", "DENIED"}:
            if non_trading_day and not submit_unexpected_codes:
                severity, status = "INFO", "EXPECTED"
                summary = "Submit boundary is blocked as expected on a non-trading day."
            else:
                severity, status = "BLOCKER", "ACTIVE"
                summary = f"Submit boundary is {boundary_status}."
            add_alert(
                severity=severity,
                status=status,
                category="SUBMIT_BOUNDARY_BLOCKED",
                owning_subsystem="AEGIS",
                owning_gate="submit_boundary_status_v1",
                summary=summary,
                operator_action="Inspect submit_boundary_status_v1 failed checks and restore required dependencies.",
                source_artifacts=[
                    str(submit_boundary.path),
                    *[str(item.get("absolute_path") or "") for item in (submit_boundary.payload.get("failed_checks") or []) if isinstance(item, dict)],
                ],
            )

        freshness_verdict = str(submit_boundary.payload.get("freshness_verdict") or "").strip().upper()
        linkage_verdict = str(submit_boundary.payload.get("linkage_verdict") or "").strip().upper()
        known_boundary_states = {"AUTHORIZED", "BLOCKED", "DENIED", "STALE", "MALFORMED"}
        if (
            freshness_verdict in {"UNKNOWN", "STALE"}
            or linkage_verdict in {"UNKNOWN", "UNLINKED"}
            or boundary_status not in known_boundary_states
        ):
            add_alert(
                severity="WARN",
                status="UNKNOWN",
                category="SUBMIT_BOUNDARY_UNKNOWN",
                owning_subsystem="AEGIS",
                owning_gate="submit_boundary_status_v1",
                summary=(
                    f"Submit boundary evidence is not fully trustworthy "
                    f"(freshness={freshness_verdict or 'UNSPECIFIED'}, linkage={linkage_verdict or 'UNSPECIFIED'}, "
                    f"boundary_status={boundary_status or 'UNSPECIFIED'})."
                ),
                operator_action="Regenerate submit boundary and linked readiness artifacts; require linked current evidence before trading.",
                source_artifacts=[str(submit_boundary.path)],
            )
    else:
        add_alert(
            severity="BLOCKER",
            status="UNKNOWN",
            category="SUBMIT_BOUNDARY_UNKNOWN",
            owning_subsystem="AEGIS",
            owning_gate="submit_boundary_status_v1",
            summary="Submit boundary artifact is unavailable; authorization state cannot be proven.",
            operator_action="Restore submit_boundary_status_v1 artifact generation and rerun authority.",
            source_artifacts=[str(submit_boundary.path)],
        )

    if non_trading_day:
        if submit_unexpected_codes:
            add_alert(
                severity="WARN",
                status="ACTIVE",
                category="NON_TRADING_DAY_EXPECTED",
                owning_subsystem="AEGIS",
                owning_gate="paper_session_authority_v1",
                summary="Non-trading day detected, but additional blocker evidence indicates broken inactive state.",
                operator_action="Treat day as inactive but investigate non-non-trading blockers before next trading session.",
                source_artifacts=[str(paper_session.path), str(submit_boundary.path)],
            )
        else:
            add_alert(
                severity="INFO",
                status="EXPECTED",
                category="NON_TRADING_DAY_EXPECTED",
                owning_subsystem="AEGIS",
                owning_gate="paper_session_authority_v1",
                summary="Non-trading day is expected and non-actionable.",
                operator_action="No trading action required for this day.",
                source_artifacts=[str(paper_session.path)],
            )

    contradictory_paths: List[str] = []
    contradictory_day = day_utc
    if aegis_closure.usable:
        for row in (aegis_closure.payload.get("blocking_evidence") or []):
            if not isinstance(row, dict):
                continue
            if str(row.get("code") or "").strip().upper() == "AEGIS_STATE_CONTRADICTORY_CONTROL_SURFACES":
                contradictory_paths.append(str(row.get("path") or ""))
        source_surfaces = aegis_closure.payload.get("source_surfaces")
        if isinstance(source_surfaces, dict):
            state_surface = source_surfaces.get("trading_day_state_machine")
            if isinstance(state_surface, dict):
                if str(state_surface.get("canonical_blocker") or "").strip().upper() == "AEGIS_STATE_CONTRADICTORY_CONTROL_SURFACES":
                    contradictory_paths.append(str(state_surface.get("path") or ""))
    if state_machine.usable:
        blocker = state_machine.payload.get("first_true_blocker")
        blocker_code = ""
        blocker_path = ""
        if isinstance(blocker, dict):
            blocker_code = str(blocker.get("first_true_blocker_code") or "").strip().upper()
            blocker_path = str(blocker.get("first_true_blocker_artifact_path") or "").strip()
        blocking_codes = _upper_set(_str_list(state_machine.payload.get("blocking_codes")))
        if blocker_code == "AEGIS_STATE_CONTRADICTORY_CONTROL_SURFACES" or "AEGIS_STATE_CONTRADICTORY_CONTROL_SURFACES" in blocking_codes:
            contradictory_paths.extend([str(state_machine.path), blocker_path])

    latest_granted_day, _ = _find_latest_granted_trading_day(runtime_truth_root, day_utc)
    if not contradictory_paths and latest_granted_day and latest_granted_day != day_utc:
        latest_closure_path = runtime_truth_root / "reports" / "aegis_day_closure_authority_v1" / latest_granted_day / "aegis_day_closure_authority.v1.json"
        latest_closure_state = _read_json_artifact("aegis_day_closure_authority_v1", latest_closure_path, latest_granted_day)
        if latest_closure_state.usable:
            for row in (latest_closure_state.payload.get("blocking_evidence") or []):
                if not isinstance(row, dict):
                    continue
                if str(row.get("code") or "").strip().upper() == "AEGIS_STATE_CONTRADICTORY_CONTROL_SURFACES":
                    contradictory_day = latest_granted_day
                    contradictory_paths.extend([str(latest_closure_path), str(row.get("path") or "")])
        if not contradictory_paths:
            latest_state_machine_path = runtime_truth_root / "reports" / "trading_day_state_machine_v1" / latest_granted_day / "trading_day_state_machine.v1.json"
            latest_state_machine_state = _read_json_artifact("trading_day_state_machine_v1", latest_state_machine_path, latest_granted_day)
            if latest_state_machine_state.usable:
                latest_blocker = latest_state_machine_state.payload.get("first_true_blocker")
                latest_blocker_code = ""
                latest_blocker_path = ""
                if isinstance(latest_blocker, dict):
                    latest_blocker_code = str(latest_blocker.get("first_true_blocker_code") or "").strip().upper()
                    latest_blocker_path = str(latest_blocker.get("first_true_blocker_artifact_path") or "").strip()
                latest_codes = _upper_set(_str_list(latest_state_machine_state.payload.get("blocking_codes")))
                if (
                    latest_blocker_code == "AEGIS_STATE_CONTRADICTORY_CONTROL_SURFACES"
                    or "AEGIS_STATE_CONTRADICTORY_CONTROL_SURFACES" in latest_codes
                ):
                    contradictory_day = latest_granted_day
                    contradictory_paths.extend([str(latest_state_machine_path), latest_blocker_path])

    contradictory_paths = _dedupe_sources(contradictory_paths)
    if contradictory_paths:
        severity = "BLOCKER" if contradictory_day == day_utc else "WARN"
        add_alert(
            severity=severity,
            status="ACTIVE",
            category="CONTRADICTORY_CONTROL_SURFACES",
            owning_subsystem="AEGIS",
            owning_gate="trading_day_state_machine_v1",
            summary=(
                "Contradictory control-surface blocker is active for trading day "
                f"{contradictory_day}."
            ),
            operator_action="Resolve AEGIS_STATE_CONTRADICTORY_CONTROL_SURFACES before permitting next trade window.",
            source_artifacts=contradictory_paths,
            dedupe_key=f"CONTRADICTORY_CONTROL_SURFACES|{contradictory_day}|{'|'.join(contradictory_paths)}",
        )

    if intent_generation.usable and submission_index.usable:
        output_count = _safe_int((intent_generation.payload.get("canonical_outputs") or {}).get("output_count"), 0)
        attempts = submission_index.payload.get("attempts")
        attempts_count = len(attempts) if isinstance(attempts, list) else 0
        submission_status = str(submission_index.payload.get("status") or "").strip().upper()
        if output_count > 0 and (submission_status != "PASS" or attempts_count == 0):
            add_alert(
                severity="WARN",
                status="ACTIVE",
                category="INTENT_GENERATED_NOT_SUBMITTED",
                owning_subsystem="AEGIS",
                owning_gate="submission_index_v1",
                summary=(
                    "Intent outputs exist but authoritative submission lineage is incomplete "
                    f"(intent_count={output_count}, submission_attempts={attempts_count}, submission_index_status={submission_status or 'UNKNOWN'})."
                ),
                operator_action="Validate submit boundary, broker submission lineage, and submission_index_v1 generation.",
                source_artifacts=[str(intent_generation.path), str(submission_index.path)],
            )

    open_lifecycle_count_metric = -1
    if paper_ledger.usable:
        submit_lifecycle = paper_ledger.payload.get("submit_lifecycle")
        if isinstance(submit_lifecycle, dict):
            finalization_status = str(submit_lifecycle.get("finalization_status") or "").strip().upper()
            submit_attempted = _safe_bool(submit_lifecycle.get("submit_attempted"))
            open_lifecycle_count_metric = 1 if finalization_status == "OPEN" else 0
            has_broker_accept = False
            if submission_index.usable and isinstance(submission_index.payload.get("attempts"), list):
                for attempt in submission_index.payload.get("attempts"):
                    if not isinstance(attempt, dict):
                        continue
                    if attempt.get("broker_perm_id") is not None or attempt.get("broker_order_id") is not None:
                        has_broker_accept = True
                        break
            if finalization_status == "OPEN" and submit_attempted and has_broker_accept:
                add_alert(
                    severity="WARN",
                    status="ACTIVE",
                    category="BROKER_ACCEPTED_LIFECYCLE_OPEN",
                    owning_subsystem="AEGIS",
                    owning_gate="paper_session_ledger_v1",
                    summary="Broker acceptance is present while lifecycle finalization remains OPEN.",
                    operator_action="Complete post-submit lifecycle closure and reconciliation before treating day as finalized.",
                    source_artifacts=[str(paper_ledger.path), str(submission_index.path)],
                )

    reconciliation_ok_metric = -1
    if reconciliation.usable:
        recon_status = str(reconciliation.payload.get("status") or "").strip().upper()
        reconciliation_ok_metric = 1 if recon_status == "OK" else 0
        if recon_status != "OK":
            add_alert(
                severity="BLOCKER",
                status="ACTIVE",
                category="RECONCILIATION_NOT_OK",
                owning_subsystem="AEGIS",
                owning_gate="reconciliation_report_v3",
                summary=f"Reconciliation status is {recon_status or 'UNKNOWN'}, not OK.",
                operator_action="Regenerate reconciliation_report_v3 and resolve first blocker before next trade cycle.",
                source_artifacts=[str(reconciliation.path)],
            )
    else:
        add_alert(
            severity="WARN",
            status="UNKNOWN",
            category="RECONCILIATION_NOT_OK",
            owning_subsystem="AEGIS",
            owning_gate="reconciliation_report_v3",
            summary="Reconciliation status cannot be proven from authoritative artifacts.",
            operator_action="Restore reconciliation_report_v3 evidence for this day.",
            source_artifacts=[str(reconciliation.path)],
        )

    observed_timestamps = [state.observed_timestamp for state in artifacts.values() if state.observed_timestamp is not None]
    runtime_truth_freshness_seconds_metric = -1
    if observed_timestamps:
        freshness_seconds = int((now - max(observed_timestamps)).total_seconds())
        runtime_truth_freshness_seconds_metric = freshness_seconds if freshness_seconds >= 0 else 0
        if runtime_truth_freshness_seconds_metric > int(args.stale_seconds):
            add_alert(
                severity="BLOCKER",
                status="ACTIVE",
                category="RUNTIME_TRUTH_STALE",
                owning_subsystem="AEGIS",
                owning_gate="runtime_truth_freshness",
                summary=(
                    f"Runtime truth freshness exceeded threshold: "
                    f"{runtime_truth_freshness_seconds_metric}s > {int(args.stale_seconds)}s."
                ),
                operator_action="Refresh authoritative truth producers and rerun alert authority.",
                source_artifacts=[str(state.path) for state in artifacts.values()],
            )
    else:
        add_alert(
            severity="WARN",
            status="UNKNOWN",
            category="RUNTIME_TRUTH_STALE",
            owning_subsystem="AEGIS",
            owning_gate="runtime_truth_freshness",
            summary="Runtime truth freshness cannot be proven because no artifact timestamps were available.",
            operator_action="Ensure required artifacts expose generated timestamps and rerun authority.",
            source_artifacts=[str(state.path) for state in artifacts.values()],
        )

    actionable_alerts = [row for row in alert_rows if str(row.get("status") or "").upper() in {"ACTIVE", "UNKNOWN"}]
    contradictory_metric = 1 if any(row.get("category") == "CONTRADICTORY_CONTROL_SURFACES" for row in actionable_alerts) else 0
    if not (aegis_closure.usable or state_machine.usable):
        contradictory_metric = -1

    can_trade_evidence_complete = paper_session.usable and submit_boundary.usable and trade_readiness.usable
    can_trade_metric = -1
    can_aegis_trade = False
    if can_trade_evidence_complete:
        decision_yes = str(trade_readiness.payload.get("decision") or "").strip().upper() == "YES"
        submit_authorized = _safe_bool(submit_boundary.payload.get("submission_authorized"))
        session_granted = str(paper_session.payload.get("authority_status") or "").strip().upper() == "GRANTED"
        blocking_actionable = any(str(row.get("severity") or "").upper() in {"BLOCKER", "CRITICAL"} for row in actionable_alerts)
        can_aegis_trade = bool(decision_yes and submit_authorized and session_granted and not blocking_actionable)
        can_trade_metric = 1 if can_aegis_trade else 0

    alert_rows.sort(key=lambda row: (-_severity_rank(str(row.get("severity") or "")), str(row.get("category") or ""), str(row.get("dedupe_key") or "")))

    ledger_out_path = runtime_truth_root / "reports" / "aegis_alert_ledger_v1" / day_utc / "aegis_alert_ledger.v1.json"
    prior_first_seen_by_dedupe = _load_prior_first_seen(ledger_out_path, day_utc)

    finalized_alerts: List[Dict[str, Any]] = []
    for row in alert_rows:
        dedupe_key = str(row["dedupe_key"])
        first_seen = prior_first_seen_by_dedupe.get(dedupe_key, generated_at_utc)
        finalized_alerts.append(
            {
                "alert_id": row["alert_id"],
                "severity": row["severity"],
                "status": row["status"],
                "category": row["category"],
                "owning_subsystem": row["owning_subsystem"],
                "owning_gate": row["owning_gate"],
                "dedupe_key": dedupe_key,
                "summary": row["summary"],
                "operator_action": row["operator_action"],
                "source_artifacts": row["source_artifacts"],
                "first_seen_at_utc": first_seen,
                "last_seen_at_utc": generated_at_utc,
                "resolved_at_utc": None,
            }
        )

    alert_counts = {"INFO": 0, "WARN": 0, "BLOCKER": 0, "CRITICAL": 0}
    for row in finalized_alerts:
        sev = str(row.get("severity") or "").upper()
        if sev in alert_counts:
            alert_counts[sev] += 1

    overall = _overall_status(finalized_alerts)
    primary_blocker = "NONE"
    for row in finalized_alerts:
        if str(row.get("status") or "").upper() not in {"ACTIVE", "UNKNOWN"}:
            continue
        if str(row.get("severity") or "").upper() in {"BLOCKER", "CRITICAL", "WARN"}:
            primary_blocker = str(row.get("category") or "NONE")
            break

    ledger_payload = {
        "schema_version": "aegis_alert_ledger.v1",
        "generated_at_utc": generated_at_utc,
        "trading_day_utc": day_utc,
        "runtime_truth_root": str(runtime_truth_root),
        "paper_truth_root": str(paper_truth_root),
        "overall_status": overall,
        "alert_counts": alert_counts,
        "alerts": finalized_alerts,
    }

    expected_non_actionable_states = [
        f"{row['category']}: {row['summary']}"
        for row in finalized_alerts
        if str(row.get("status") or "").upper() == "EXPECTED"
    ]
    operator_actions = []
    seen_actions = set()
    for row in finalized_alerts:
        if str(row.get("status") or "").upper() not in {"ACTIVE", "UNKNOWN"}:
            continue
        action = str(row.get("operator_action") or "").strip()
        if action and action not in seen_actions:
            seen_actions.add(action)
            operator_actions.append(action)
    evidence_rows = [
        {
            "alert_id": row["alert_id"],
            "category": row["category"],
            "severity": row["severity"],
            "status": row["status"],
            "summary": row["summary"],
            "source_artifacts": row["source_artifacts"],
        }
        for row in finalized_alerts
    ]
    operator_summary_text = (
        "Aegis can trade: all required control surfaces are coherent and no actionable blockers are active."
        if can_aegis_trade
        else f"Aegis cannot trade safely for {day_utc}; primary blocker: {primary_blocker}."
    )
    digest_payload = {
        "schema_version": "aegis_operator_digest.v1",
        "generated_at_utc": generated_at_utc,
        "trading_day_utc": day_utc,
        "can_aegis_trade": can_aegis_trade,
        "overall_status": overall,
        "primary_blocker": primary_blocker,
        "operator_summary": operator_summary_text,
        "operator_actions": operator_actions,
        "expected_non_actionable_states": expected_non_actionable_states,
        "evidence": evidence_rows,
    }

    metrics_payload = {
        "aegis_alert_info_count": alert_counts["INFO"],
        "aegis_alert_warn_count": alert_counts["WARN"],
        "aegis_alert_blocker_count": alert_counts["BLOCKER"],
        "aegis_alert_critical_count": alert_counts["CRITICAL"],
        "aegis_can_trade": can_trade_metric,
        "aegis_submit_boundary_authorized": submit_boundary_authorized_metric,
        "aegis_contradictory_control_surfaces": contradictory_metric,
        "aegis_runtime_truth_freshness_seconds": runtime_truth_freshness_seconds_metric,
        "aegis_open_lifecycle_count": open_lifecycle_count_metric,
        "aegis_reconciliation_ok": reconciliation_ok_metric,
    }

    digest_out_path = runtime_truth_root / "reports" / "aegis_operator_digest_v1" / day_utc / "aegis_operator_digest.v1.json"
    metrics_out_path = runtime_truth_root / "reports" / "aegis_alert_metrics_v1" / day_utc / "aegis_alert_metrics.prom"

    _write_json(ledger_out_path, ledger_payload)
    _write_json(digest_out_path, digest_payload)
    _write_metrics(metrics_out_path, metrics_payload)

    print(
        json.dumps(
            {
                "overall_status": overall,
                "primary_blocker": primary_blocker,
                "alert_count": len(finalized_alerts),
                "ledger_path": str(ledger_out_path),
                "digest_path": str(digest_out_path),
                "metrics_path": str(metrics_out_path),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
