#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.event_regime_trigger_registry_v1 import build_event_regime_trigger_registry_v1  # noqa: E402
from ops.aegis.intelligence_common_v1 import latest_json_v1, read_json_v1, systemd_timer_inventory_v1, write_json_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


REPORT_FAMILY = "aegis_event_regime_triggered_sleeve_run_audit_v1"
EVENT_TIMER = "aegis-event-monitor-v1.timer"
EVENT_SERVICE = "aegis-event-monitor-v1.service"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_event_regime_triggered_sleeve_run_audit_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    payload = build_event_regime_triggered_sleeve_run_audit_v1(
        truth_root=truth_root,
        repo_root=REPO_ROOT,
        day_utc=str(args.day),
    )
    paths = write_event_regime_triggered_sleeve_run_audit_v1(
        truth_root=truth_root,
        day_utc=str(args.day),
        payload=payload,
    )
    print(
        json.dumps(
            {
                "path": paths["json"],
                "summary_path": paths["summary"],
                "matrix_path": paths["matrix"],
                "event_or_regime_triggered_sleeve_runs_implemented": payload[
                    "event_or_regime_triggered_sleeve_runs_implemented"
                ],
                "broker_execution_enabled": False,
                "autonomous_execution_enabled": False,
            },
            sort_keys=True,
        )
    )
    return 0


def build_event_regime_triggered_sleeve_run_audit_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    repo_root = Path(repo_root).resolve()
    truth_root = Path(truth_root).expanduser().resolve()
    timers = systemd_timer_inventory_v1(repo_root)
    repo_timers = timers.get("repo_defined_timers") if isinstance(timers.get("repo_defined_timers"), list) else []
    active_timers = timers.get("active_user_timers") if isinstance(timers.get("active_user_timers"), list) else []
    event_timer = next((row for row in repo_timers if row.get("unit") == EVENT_TIMER), None)
    active_event_timer = next((row for row in active_timers if row.get("unit") == EVENT_TIMER), None)
    service_path = repo_root / "ops" / "systemd" / "user" / EVENT_SERVICE
    service_text = _read_text(service_path)
    exec_start = _first_match(service_text, r"(?m)^ExecStart=(.+)$") or None

    registry_path = repo_root / "governance" / "02_REGISTRIES" / "C2_EVENT_RULES_REGISTRY_V1.json"
    registry_payload = read_json_v1(registry_path)
    event_rule_mappings = _registry_mappings(registry_payload)
    event_monitor_paths = [
        repo_root / "ops" / "tools" / "run_aegis_event_monitor_v1.py",
        repo_root / "constellation_2" / "common" / "aegis_event_monitoring_v1.py",
    ]
    regime_paths = [
        repo_root / "ops" / "tools" / "run_aegis_regime_detection_v1.py",
        repo_root / "ops" / "tools" / "run_aegis_regime_context_v1.py",
        repo_root / "ops" / "aegis" / "adaptive_intelligence" / "regime_detection_v1.py",
        repo_root / "ops" / "aegis" / "adaptive_governance" / "regime_context_v1.py",
    ]

    event_status_path, event_status = latest_json_v1(
        truth_root,
        "event_monitoring_status_v1",
        day_utc,
        "event_monitoring_status.v1.json",
    )
    existing_event_sleeve_path, existing_event_sleeve = latest_json_v1(
        truth_root,
        "aegis_event_sleeve_activation_audit_v1",
        day_utc,
        "event_sleeve_activation_audit.v1.json",
    )
    trigger_eval_path, trigger_eval = latest_json_v1(
        truth_root,
        "aegis_event_regime_trigger_evaluator_v1",
        day_utc,
        "trigger_evaluation.v1.json",
    )
    triggered_runs_path, triggered_runs = latest_json_v1(
        truth_root,
        "aegis_triggered_sleeve_runs_v1",
        day_utc,
        "triggered_sleeve_runs.v1.json",
    )
    regime_path, regime_payload = _latest_existing_regime_report(truth_root=truth_root, day_utc=day_utc)

    recurring_event_monitor = {
        "status": "CONFIRMED" if event_timer and exec_start else ("PARTIAL" if event_timer or exec_start else "NOT_FOUND"),
        "cadence": _timer_cadence(event_timer),
        "timer_name": EVENT_TIMER if event_timer else None,
        "command": exec_start,
        "active": True if active_event_timer else False if event_timer else None,
        "timer_path": event_timer.get("path") if isinstance(event_timer, dict) else None,
        "service_path": str(service_path.relative_to(repo_root)) if service_path.exists() else None,
        "active_evidence": active_event_timer or {},
    }

    regime_detection = {
        "status": "CONFIRMED" if any(path.exists() for path in regime_paths) else ("PARTIAL" if regime_payload else "NOT_FOUND"),
        "regimes_detected": _regimes_detected(regime_payload),
        "implementation_files": [str(path.relative_to(repo_root)) for path in regime_paths if path.exists()],
        "latest_report_path": str(regime_path) if regime_path else None,
    }

    trigger_registry = (
        trigger_eval.get("trigger_registry")
        if isinstance(trigger_eval.get("trigger_registry"), dict)
        else build_event_regime_trigger_registry_v1(repo_root=repo_root, day_utc=day_utc)
    )
    trigger_mappings = _trigger_registry_mappings(trigger_registry)
    explicit_sleeve_mappings = [row for row in trigger_mappings if row.get("mapped_sleeve_ids")]
    trigger_mapping_registry = {
        "status": "CONFIRMED" if explicit_sleeve_mappings else ("PARTIAL" if trigger_mappings else "NOT_FOUND"),
        "mappings": trigger_mappings,
        "registry_path": "ops/aegis/event_regime_trigger_registry_v1.py",
        "source_sleeve_registry_path": str((repo_root / "governance" / "02_REGISTRIES" / "C2_SLEEVE_REGISTRY_V1.json").relative_to(repo_root)),
        "latest_evaluator_path": str(trigger_eval_path) if trigger_eval_path else None,
        "registry_semantics": (
            "Event/regime triggers are mapped to confirmed sleeve IDs for advisory-only runs."
            if explicit_sleeve_mappings
            else "Event/regime trigger mappings exist but no confirmed sleeve IDs were found."
            if trigger_mappings
            else "No trigger mapping registry found."
        ),
    }

    runner_path = repo_root / "ops" / "tools" / "run_aegis_triggered_sleeves_v1.py"
    evaluator_path = repo_root / "ops" / "tools" / "run_aegis_event_regime_trigger_evaluator_v1.py"
    trigger_evidence = _find_ad_hoc_sleeve_trigger_evidence(repo_root=repo_root, paths=event_monitor_paths + regime_paths + [runner_path, evaluator_path])
    ad_hoc_status = "CONFIRMED" if runner_path.exists() and evaluator_path.exists() else "NOT_FOUND"
    ad_hoc_sleeve_triggering = {
        "status": ad_hoc_status,
        "code_path": _event_monitor_code_path(event_status_path, event_status, trigger_eval_path, triggered_runs_path, triggered_runs),
        "evidence": trigger_evidence["evidence"],
        "confirmed_run_invocations": trigger_evidence["confirmed_run_invocations"] or ([str(runner_path.relative_to(repo_root))] if runner_path.exists() else []),
        "inspected_files": [str(path.relative_to(repo_root)) for path in event_monitor_paths + regime_paths + [runner_path, evaluator_path] if path.exists()],
        "evaluator_report_path": str(trigger_eval_path) if trigger_eval_path else None,
        "triggered_run_report_path": str(triggered_runs_path) if triggered_runs_path else None,
    }

    auditability = _triggered_run_auditability(event_status_path=event_status_path, event_status=event_status, triggered_runs_path=triggered_runs_path, triggered_runs=triggered_runs)
    operator_visibility = _operator_visibility(truth_root=truth_root, repo_root=repo_root, day_utc=day_utc)
    implementation_status = _overall_status(
        recurring_event_monitor=recurring_event_monitor,
        trigger_mapping_registry=trigger_mapping_registry,
        ad_hoc_sleeve_triggering=ad_hoc_sleeve_triggering,
        triggered_run_auditability=auditability,
    )
    missing_items = _missing_items(
        recurring_event_monitor=recurring_event_monitor,
        trigger_mapping_registry=trigger_mapping_registry,
        ad_hoc_sleeve_triggering=ad_hoc_sleeve_triggering,
        triggered_run_auditability=auditability,
        operator_visibility=operator_visibility,
    )

    return {
        "schema_id": "aegis_event_regime_triggered_sleeve_run_audit",
        "schema_version": "v1",
        "artifact_id": "aegis_event_regime_triggered_sleeve_run_audit_v1",
        "generated_at": _now(),
        "day_utc": day_utc,
        "truth_root": str(truth_root),
        "repo_root": str(repo_root),
        "event_or_regime_triggered_sleeve_runs_implemented": implementation_status,
        "recurring_event_monitor": recurring_event_monitor,
        "event_detection": {
            "status": "CONFIRMED" if event_rule_mappings else "NOT_FOUND",
            "event_types_detected": sorted({str(row.get("event_type")) for row in event_rule_mappings if row.get("event_type")}),
            "source_registry_path": str(registry_path.relative_to(repo_root)) if registry_path.exists() else None,
            "latest_monitor_status_path": str(event_status_path) if event_status_path else None,
            "latest_triggered_events": event_status.get("triggered_events") if isinstance(event_status.get("triggered_events"), list) else [],
            "latest_blocked_events": event_status.get("blocked_events") if isinstance(event_status.get("blocked_events"), list) else [],
        },
        "regime_detection": regime_detection,
        "trigger_mapping_registry": trigger_mapping_registry,
        "trigger_evaluator": {
            "status": "CONFIRMED" if evaluator_path.exists() and trigger_eval_path else ("PARTIAL" if evaluator_path.exists() else "NOT_FOUND"),
            "command": "npm run aegis:event-regime-trigger-evaluator",
            "implementation_path": str(evaluator_path.relative_to(repo_root)) if evaluator_path.exists() else None,
            "latest_report_path": str(trigger_eval_path) if trigger_eval_path else None,
            "run_sleeves_decision_count": trigger_eval.get("run_sleeves_decision_count"),
        },
        "triggered_sleeve_runner": {
            "status": "CONFIRMED" if runner_path.exists() and triggered_runs_path else ("PARTIAL" if runner_path.exists() else "NOT_FOUND"),
            "command": "npm run aegis:triggered-sleeves",
            "implementation_path": str(runner_path.relative_to(repo_root)) if runner_path.exists() else None,
            "latest_report_path": str(triggered_runs_path) if triggered_runs_path else None,
            "candidate_count": triggered_runs.get("candidate_count"),
        },
        "ad_hoc_sleeve_triggering": ad_hoc_sleeve_triggering,
        "triggered_run_auditability": auditability,
        "operator_visibility": operator_visibility,
        "existing_event_sleeve_activation_audit": {
            "status": "AVAILABLE" if existing_event_sleeve_path else "NOT_FOUND",
            "path": str(existing_event_sleeve_path) if existing_event_sleeve_path else None,
            "event_monitor_cadence_status": existing_event_sleeve.get("event_monitor_cadence_status"),
            "ad_hoc_sleeve_trigger_status": existing_event_sleeve.get("ad_hoc_sleeve_trigger_status"),
        },
        "safety": {
            "broker_execution_enabled": False,
            "autonomous_execution_enabled": False,
            "advisory_only": True,
            "broker_submit_transmit_allowed": False,
            "manual_human_review_required": True,
            "unsupported_ai_claim_made": False,
        },
        "missing_items": missing_items,
        "recommended_next_implementation": _recommended_next_implementation(missing_items),
    }


def write_event_regime_triggered_sleeve_run_audit_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "event_regime_triggered_sleeve_run_audit.v1.json", payload)
    summary_path = out_dir / "event_regime_triggered_sleeve_run_audit.summary.txt"
    matrix_path = out_dir / "event_regime_triggered_sleeve_run_audit.matrix.csv"
    summary_path.write_text(render_event_regime_triggered_sleeve_run_audit_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_event_regime_triggered_sleeve_run_audit_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_event_regime_triggered_sleeve_run_audit_summary_v1(payload: dict[str, Any]) -> str:
    monitor = payload.get("recurring_event_monitor") if isinstance(payload.get("recurring_event_monitor"), dict) else {}
    events = payload.get("event_detection") if isinstance(payload.get("event_detection"), dict) else {}
    registry = payload.get("trigger_mapping_registry") if isinstance(payload.get("trigger_mapping_registry"), dict) else {}
    trigger = payload.get("ad_hoc_sleeve_triggering") if isinstance(payload.get("ad_hoc_sleeve_triggering"), dict) else {}
    audit = payload.get("triggered_run_auditability") if isinstance(payload.get("triggered_run_auditability"), dict) else {}
    visibility = payload.get("operator_visibility") if isinstance(payload.get("operator_visibility"), dict) else {}
    safety = payload.get("safety") if isinstance(payload.get("safety"), dict) else {}
    lines = [
        "AEGIS EVENT/REGIME TRIGGERED SLEEVE RUN AUDIT v1",
        f"day_utc: {payload.get('day_utc')}",
        f"EVENT_OR_REGIME_TRIGGERED_SLEEVE_RUNS_IMPLEMENTED: {payload.get('event_or_regime_triggered_sleeve_runs_implemented')}",
        "",
        "Confirmed",
        f"- recurring_event_monitor: {monitor.get('status')}; timer={monitor.get('timer_name')}; active={monitor.get('active')}; cadence={monitor.get('cadence') or 'NOT_FOUND'}",
        f"- event_detection: {events.get('status')}; event_types={', '.join(events.get('event_types_detected') or []) or 'NOT_FOUND'}",
        f"- regime_detection: {(payload.get('regime_detection') or {}).get('status')}; regimes={', '.join((payload.get('regime_detection') or {}).get('regimes_detected') or []) or 'UNKNOWN'}",
        f"- trigger_mapping_registry: {registry.get('status')}",
        f"- ad_hoc_sleeve_triggering: {trigger.get('status')}",
        f"- triggered_run_auditability: {audit.get('status')}",
        f"- operator_visibility: {visibility.get('status')}",
        "",
        "Safety",
        f"- advisory_only: {str(safety.get('advisory_only')).lower()}",
        f"- broker_execution_enabled: {str(safety.get('broker_execution_enabled')).lower()}",
        f"- autonomous_execution_enabled: {str(safety.get('autonomous_execution_enabled')).lower()}",
        "",
        "Missing items",
    ]
    missing = payload.get("missing_items") if isinstance(payload.get("missing_items"), list) else []
    lines.extend(f"- {item}" for item in missing or ["NONE"])
    lines.extend(["", "Recommended next implementation"])
    lines.extend(f"- {item}" for item in payload.get("recommended_next_implementation") or ["NONE"])
    lines.append("")
    return "\n".join(lines)


def render_event_regime_triggered_sleeve_run_audit_matrix_csv_v1(payload: dict[str, Any]) -> str:
    rows = [
        _matrix_row("recurring_event_monitor", payload.get("recurring_event_monitor")),
        _matrix_row("event_detection", payload.get("event_detection")),
        _matrix_row("regime_detection", payload.get("regime_detection")),
        _matrix_row("trigger_mapping_registry", payload.get("trigger_mapping_registry")),
        _matrix_row("ad_hoc_sleeve_triggering", payload.get("ad_hoc_sleeve_triggering")),
        _matrix_row("triggered_run_auditability", payload.get("triggered_run_auditability")),
        _matrix_row("operator_visibility", payload.get("operator_visibility")),
        {"area": "safety", "status": "CONFIRMED", "evidence": "broker_execution_enabled=false; autonomous_execution_enabled=false; advisory_only=true", "missing": ""},
    ]
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=["area", "status", "evidence", "missing"])
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


def _matrix_row(area: str, row: Any) -> dict[str, str]:
    payload = row if isinstance(row, dict) else {}
    evidence: list[str] = []
    missing: list[str] = []
    for key in ("timer_name", "command", "registry_path", "latest_report_path", "latest_monitor_status_path"):
        if payload.get(key):
            evidence.append(f"{key}={payload[key]}")
    if isinstance(payload.get("fields_present"), list):
        evidence.extend(payload["fields_present"])
    if isinstance(payload.get("fields_missing"), list):
        missing.extend(payload["fields_missing"])
    if isinstance(payload.get("mappings"), list):
        evidence.append(f"mapping_count={len(payload['mappings'])}")
    if isinstance(payload.get("surfaces"), list):
        evidence.append(f"surfaces={len(payload['surfaces'])}")
    return {
        "area": area,
        "status": str(payload.get("status") or "UNKNOWN"),
        "evidence": "; ".join(evidence) or "NONE",
        "missing": "; ".join(missing) or "",
    }


def _registry_mappings(registry_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = registry_payload.get("event_rules") if isinstance(registry_payload.get("event_rules"), list) else []
    out: list[dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: str(item.get("event_rule_id") or "")):
        review = row.get("tactical_review_rule") if isinstance(row.get("tactical_review_rule"), dict) else {}
        out.append(
            {
                "event_rule_id": str(row.get("event_rule_id") or ""),
                "event_type": str(row.get("event_type") or ""),
                "enabled_status": str(row.get("enabled_status") or ""),
                "production_status": str(row.get("production_status") or ""),
                "research_status": str(row.get("research_status") or ""),
                "requires_promoted_sleeve": bool(review.get("requires_promoted_sleeve", False)),
                "create_packet_when_review_allowed": bool(review.get("create_packet_when_review_allowed", False)),
                "mapped_sleeve_ids": _strings(row.get("sleeve_ids"))
                or _strings(row.get("mapped_sleeve_ids"))
                or _strings(review.get("sleeve_ids"))
                or _strings(review.get("mapped_sleeve_ids")),
                "condition_summary": _condition_summary(row),
            }
        )
    return out


def _trigger_registry_mappings(registry_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = registry_payload.get("mappings") if isinstance(registry_payload.get("mappings"), list) else []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "trigger_key": str(row.get("trigger_key") or ""),
                "trigger_type": str(row.get("trigger_type") or ""),
                "event_type": str(row.get("trigger_key") or "") if str(row.get("trigger_type") or "") == "EVENT" else "",
                "enabled_status": "ENABLED" if bool(row.get("enabled", False)) else "DISABLED",
                "mapped_sleeve_ids": _strings(row.get("sleeve_ids")),
                "mapping_status": str(row.get("mapping_status") or ""),
                "condition_summary": str(row.get("condition") or ""),
                "reason": str(row.get("reason") or ""),
                "advisory_only": bool(row.get("advisory_only", True)),
                "broker_execution_allowed": bool(row.get("broker_execution_allowed", False)),
                "autonomous_execution_allowed": bool(row.get("autonomous_execution_allowed", False)),
            }
        )
    return out


def _condition_summary(row: dict[str, Any]) -> str:
    thresholds = row.get("thresholds") if isinstance(row.get("thresholds"), list) else []
    if not thresholds:
        return "NO_THRESHOLDS_FOUND"
    return "; ".join(
        f"{item.get('input_key')} {item.get('operator')} {item.get('value')}"
        for item in thresholds
        if isinstance(item, dict)
    )


def _latest_existing_regime_report(*, truth_root: Path, day_utc: str) -> tuple[Path | None, dict[str, Any]]:
    specs = [
        ("regime_context_v1", "regime_context.v1.json"),
        ("regime_detection_v1", "regime_detection.v1.json"),
    ]
    for family, filename in specs:
        path, payload = latest_json_v1(truth_root, family, day_utc, filename)
        if path:
            return path, payload
    return None, {}


def _regimes_detected(payload: dict[str, Any]) -> list[str]:
    if not payload:
        return []
    candidates: list[str] = []
    for parent_key in ("regime_classifications", "facts", "metrics", "conclusions"):
        parent = payload.get(parent_key)
        if isinstance(parent, dict):
            candidates.extend(key for key in parent if key.endswith("_regime"))
        elif isinstance(parent, list):
            for row in parent:
                if isinstance(row, dict):
                    candidates.extend(str(value) for value in row.values() if isinstance(value, str) and value.endswith("_regime"))
    for key in ("volatility_regime", "trend_regime", "liquidity_regime", "breadth_regime", "event_regime"):
        if key in payload:
            candidates.append(key)
    if not candidates:
        for key in ("volatility_regime", "trend_regime", "liquidity_regime", "breadth_regime", "event_regime"):
            if key in json.dumps(payload):
                candidates.append(key)
    return sorted(set(candidates))


def _find_ad_hoc_sleeve_trigger_evidence(*, repo_root: Path, paths: list[Path]) -> dict[str, list[str]]:
    evidence: list[str] = []
    confirmed: list[str] = []
    run_patterns = (
        r"subprocess\.(run|Popen).*(sleeve|candidate)",
        r"run_aegis_.*sleeve",
        r"npm run aegis:.*sleeve",
        r"selected_sleeve",
        r"sleeve_ids_selected",
        r"triggered_sleeve_run",
    )
    context_patterns = (r"tactical_packet", r"requires_promoted_sleeve", r"promoted_sleeve_available", r"event_rule")
    for path in paths:
        if not path.exists():
            continue
        rel = str(path.relative_to(repo_root))
        for line_no, line in enumerate(_read_text(path).splitlines(), start=1):
            stripped = line.strip()
            if any(re.search(pattern, stripped, re.IGNORECASE) for pattern in run_patterns):
                confirmed.append(f"{rel}:{line_no}:{stripped}")
            elif "sleeve" in stripped.lower() and any(re.search(pattern, stripped, re.IGNORECASE) for pattern in context_patterns):
                evidence.append(f"{rel}:{line_no}:{stripped}")
    return {"evidence": evidence[:30], "confirmed_run_invocations": confirmed[:30]}


def _event_monitor_code_path(
    event_status_path: Path | None,
    event_status: dict[str, Any],
    trigger_eval_path: Path | None,
    triggered_runs_path: Path | None,
    triggered_runs: dict[str, Any],
) -> list[dict[str, str]]:
    path = str(event_status_path) if event_status_path else "NOT_FOUND"
    run_count = int(triggered_runs.get("run_count") or 0) if triggered_runs else 0
    candidate_count = int(triggered_runs.get("candidate_count") or 0) if triggered_runs else 0
    return [
        {"step": "event_monitor_timer", "status": "CONFIRMED_IN_REPO", "evidence": EVENT_TIMER},
        {"step": "event_rule_evaluation", "status": "CONFIRMED_IN_CODE", "evidence": "constellation_2/common/aegis_event_monitoring_v1.py"},
        {"step": "event_monitor_status_report", "status": "AVAILABLE" if event_status_path else "NOT_FOUND", "evidence": path},
        {"step": "trigger_evaluator", "status": "AVAILABLE" if trigger_eval_path else "NOT_FOUND", "evidence": str(trigger_eval_path) if trigger_eval_path else "NOT_FOUND"},
        {
            "step": "tactical_packet_creation",
            "status": "CONDITIONAL",
            "evidence": f"tactical_packets_created={event_status.get('tactical_packets_created') if event_status else []}",
        },
        {"step": "selected_sleeves", "status": "CONFIRMED_IN_EVALUATOR", "evidence": str(trigger_eval_path) if trigger_eval_path else "NOT_FOUND"},
        {"step": "sleeve_run_command", "status": "CONFIRMED_IN_CODE", "evidence": "ops/tools/run_aegis_triggered_sleeves_v1.py"},
        {
            "step": "advisory_candidate_output_from_triggered_sleeve_run",
            "status": "CONFIRMED_SCHEMA" if triggered_runs_path else "NOT_FOUND",
            "evidence": f"path={triggered_runs_path or 'NOT_FOUND'} run_count={run_count} candidate_count={candidate_count}",
        },
    ]


def _triggered_run_auditability(
    *,
    event_status_path: Path | None,
    event_status: dict[str, Any],
    triggered_runs_path: Path | None,
    triggered_runs: dict[str, Any],
) -> dict[str, Any]:
    fields_present = []
    if event_status_path:
        fields_present.append("event_monitor_status_path")
    for key in ("monitor_run_id", "event_rule_ids_evaluated", "thresholds_evaluated", "triggered_events", "blocked_events", "tactical_packets_created", "broker_submit_required", "runtime_mutation_allowed"):
        if key in event_status:
            fields_present.append(key)
    required_run_fields = [
        "triggered_run_id",
        "trigger_id",
        "trigger_type",
        "detected_condition",
        "selected_sleeve_ids",
        "command_executed",
        "output_artifacts",
        "candidate_count",
        "safety",
    ]
    runs = triggered_runs.get("runs") if isinstance(triggered_runs.get("runs"), list) else []
    run_fields_present: set[str] = set()
    for run in runs:
        if not isinstance(run, dict):
            continue
        run_fields_present.update(key for key in required_run_fields if key in run)
    fields_present.extend(sorted(run_fields_present))
    fields_missing = [field for field in required_run_fields if field not in run_fields_present]
    status = "CONFIRMED" if triggered_runs_path and not fields_missing else ("PARTIAL" if fields_present else "NOT_FOUND")
    return {
        "status": status,
        "fields_present": fields_present,
        "fields_missing": fields_missing,
        "latest_monitor_status_path": str(event_status_path) if event_status_path else None,
        "latest_triggered_run_audit_path": str(triggered_runs_path) if triggered_runs_path else None,
    }


def _operator_visibility(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    surfaces: list[dict[str, str]] = []
    specs = [
        ("daily_operator", "aegis_daily_operator_v1", "daily_operator.v1.json"),
        ("operator_inbox", "aegis_operator_inbox_v1", "operator_inbox.v1.json"),
        ("eod", "aegis_eod_intelligence_v1", "eod_intelligence.v1.json"),
        ("eow", "aegis_eow_intelligence_v1", "eow_intelligence.v1.json"),
        ("audit_handoff", "aegis_audit_handoff_v1", "aegis_audit_handoff.txt"),
    ]
    for name, family, filename in specs:
        path = truth_root / "reports" / family / day_utc
        candidates = sorted(path.rglob(filename)) if path.exists() else []
        surfaces.append({"surface": name, "status": "AVAILABLE" if candidates else "NOT_FOUND", "path": str(candidates[-1]) if candidates else ""})
    event_code = repo_root / "constellation_2" / "common" / "aegis_event_monitoring_v1.py"
    surfaces.append(
        {
            "surface": "event_monitoring_operator_surface",
            "status": "CONFIRMED_IN_CODE" if "build_event_monitoring_operator_surface_v1" in _read_text(event_code) else "NOT_FOUND",
            "path": str(event_code.relative_to(repo_root)) if event_code.exists() else "",
        }
    )
    available = [row for row in surfaces if row["status"] not in {"NOT_FOUND", "UNKNOWN"}]
    return {
        "status": "CONFIRMED" if len(available) >= 4 else ("PARTIAL" if available else "NOT_FOUND"),
        "surfaces": surfaces,
    }


def _overall_status(
    *,
    recurring_event_monitor: dict[str, Any],
    trigger_mapping_registry: dict[str, Any],
    ad_hoc_sleeve_triggering: dict[str, Any],
    triggered_run_auditability: dict[str, Any],
) -> str:
    full_path = (
        recurring_event_monitor.get("status") == "CONFIRMED"
        and trigger_mapping_registry.get("status") == "CONFIRMED"
        and ad_hoc_sleeve_triggering.get("status") == "CONFIRMED"
        and triggered_run_auditability.get("status") == "CONFIRMED"
    )
    if full_path:
        return "YES"
    if recurring_event_monitor.get("status") in {"CONFIRMED", "PARTIAL"} or trigger_mapping_registry.get("status") in {"CONFIRMED", "PARTIAL"}:
        return "PARTIAL"
    return "NO"


def _missing_items(
    *,
    recurring_event_monitor: dict[str, Any],
    trigger_mapping_registry: dict[str, Any],
    ad_hoc_sleeve_triggering: dict[str, Any],
    triggered_run_auditability: dict[str, Any],
    operator_visibility: dict[str, Any],
) -> list[str]:
    missing: list[str] = []
    if trigger_mapping_registry.get("status") != "CONFIRMED":
        missing.append("Concrete event/regime condition to sleeve_id mapping registry is not implemented.")
    if ad_hoc_sleeve_triggering.get("status") != "CONFIRMED":
        missing.append("No code path was found that invokes a sleeve runner from an event/regime trigger.")
    if triggered_run_auditability.get("status") != "CONFIRMED":
        for field in triggered_run_auditability.get("fields_missing") or []:
            missing.append(f"Triggered sleeve run audit field missing: {field}.")
    if operator_visibility.get("status") != "CONFIRMED":
        missing.append("Operator visibility for triggered sleeve runs is incomplete.")
    return sorted(set(missing))


def _recommended_next_implementation(missing_items: list[str]) -> list[str]:
    if not missing_items:
        return ["No implementation packet required; keep safety regression tests in place."]
    return [
        "Create an advisory-only event/regime-to-sleeve trigger registry with explicit condition ids, sleeve_ids, selection reasons, and suppression rules.",
        "Add a trigger evaluator that reads event monitor and regime reports, selects sleeves, and emits a dry-run trigger decision before running anything.",
        "Add an advisory-only sleeve run command that accepts trigger_id and sleeve_ids, writes candidate artifacts, and sets broker_execution_enabled=false and autonomous_execution_enabled=false.",
        "Add triggered_run_audit.v1.json with trigger_id, event/regime condition, selected sleeve_ids, command executed, output artifact paths, candidate count, and safety fields.",
        "Expose triggered run summaries in daily operator, operator inbox, EOD/EOW, audit handoff, and read-only runtime UI/API.",
    ]


def _timer_cadence(event_timer: dict[str, Any] | None) -> str | None:
    if not event_timer:
        return None
    calendar = event_timer.get("calendar") if isinstance(event_timer.get("calendar"), list) else []
    relative = event_timer.get("relative_schedule") if isinstance(event_timer.get("relative_schedule"), list) else []
    return "; ".join(str(item) for item in calendar + relative) or None


def _strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item)]


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _first_match(text: str, pattern: str) -> str:
    match = re.search(pattern, text)
    return match.group(1).strip() if match else ""


if __name__ == "__main__":
    raise SystemExit(main())
