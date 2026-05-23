from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.aegis.event_regime_trigger_registry_v1 import (
    EVENT_TRIGGERS,
    REGIME_TRIGGERS,
    build_event_regime_trigger_registry_v1,
    confirmed_sleeve_ids_v1,
    mapping_by_trigger_v1,
)
from ops.aegis.intelligence_common_v1 import latest_json_v1, read_json_v1, write_json_v1
from ops.aegis.runtime_truth_kernel_v1 import build_runtime_truth_kernel_v1


REPORT_FAMILY = "aegis_event_regime_trigger_evaluator_v1"


def build_event_regime_trigger_evaluation_v1(
    *,
    truth_root: Path,
    repo_root: Path,
    day_utc: str,
    triggered_sleeves_enabled: bool | None = None,
) -> dict[str, Any]:
    truth_root = Path(truth_root).expanduser().resolve()
    repo_root = Path(repo_root).resolve()
    enabled = _env_enabled() if triggered_sleeves_enabled is None else bool(triggered_sleeves_enabled)
    registry = build_event_regime_trigger_registry_v1(repo_root=repo_root, day_utc=day_utc)
    mappings = mapping_by_trigger_v1(registry)
    confirmed_sleeves = confirmed_sleeve_ids_v1(repo_root=repo_root)
    runtime_path, runtime = latest_json_v1(truth_root, "aegis_runtime_truth_kernel_v1", day_utc, "runtime_truth_kernel.v1.json")
    if not runtime:
        runtime = build_runtime_truth_kernel_v1(truth_root=truth_root, day_utc=day_utc)
        runtime_path = None
    event_path, event_status = latest_json_v1(truth_root, "event_monitoring_status_v1", day_utc, "event_monitoring_status.v1.json")
    validity_path, validity = latest_json_v1(truth_root, "event_validity_gate_v1", day_utc, "event_validity_gate.v1.json")
    regime_path, regime = _latest_regime(truth_root=truth_root, day_utc=day_utc)
    run_history_path, run_history = latest_json_v1(truth_root, "aegis_triggered_sleeve_runs_v1", day_utc, "triggered_sleeve_runs.v1.json")
    input_paths = [path for path in (runtime_path, event_path, validity_path, regime_path, run_history_path) if path]
    event_conditions = _detected_events(event_status)
    regime_conditions = _detected_regimes(regime)
    decisions: list[dict[str, Any]] = []
    for trigger in EVENT_TRIGGERS:
        decisions.append(
            _decision(
                trigger_key=trigger,
                trigger_type="EVENT",
                detected=trigger in event_conditions,
                evidence_path=event_path,
                evidence_payload=event_status,
                runtime=runtime,
                registry_mapping=mappings.get(trigger, {}),
                confirmed_sleeves=confirmed_sleeves,
                run_history=run_history,
                enabled=enabled,
            )
        )
    for trigger in REGIME_TRIGGERS:
        decisions.append(
            _decision(
                trigger_key=trigger,
                trigger_type="REGIME",
                detected=trigger in regime_conditions,
                evidence_path=regime_path,
                evidence_payload=regime,
                runtime=runtime,
                registry_mapping=mappings.get(trigger, {}),
                confirmed_sleeves=confirmed_sleeves,
                run_history=run_history,
                enabled=enabled,
            )
        )
    return {
        "schema_id": "aegis_event_regime_trigger_evaluation",
        "schema_version": "v1",
        "artifact_id": "aegis_event_regime_trigger_evaluator_v1",
        "generated_at": _now(),
        "day_utc": day_utc,
        "truth_root": str(truth_root),
        "triggered_sleeves_enabled": enabled,
        "runtime_truth_classification": runtime.get("runtime_truth_classification") or "UNKNOWN",
        "target_mode_ready": bool(runtime.get("human_approved_advisory_runtime_ready")),
        "trigger_registry": registry,
        "confirmed_sleeve_ids": confirmed_sleeves,
        "input_artifacts": [str(path) for path in input_paths],
        "input_hashes": [_sha256(path) for path in input_paths],
        "event_conditions_detected": sorted(event_conditions),
        "regime_conditions_detected": sorted(regime_conditions),
        "decisions": decisions,
        "run_sleeves_decision_count": sum(1 for row in decisions if row["decision"] == "RUN_SLEEVES"),
        "skip_decision_count": sum(1 for row in decisions if row["decision"] != "RUN_SLEEVES"),
        "safety": {
            "advisory_only": True,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "broker_submit_transmit_allowed": False,
        },
    }


def write_event_regime_trigger_evaluation_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "trigger_evaluation.v1.json", payload)
    summary_path = out_dir / "trigger_evaluation.summary.txt"
    summary_path.write_text(render_trigger_evaluation_summary_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path)}


def render_trigger_evaluation_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS EVENT/REGIME TRIGGER EVALUATION v1",
        f"day_utc: {payload.get('day_utc')}",
        f"triggered_sleeves_enabled: {str(payload.get('triggered_sleeves_enabled')).lower()}",
        f"runtime_truth_classification: {payload.get('runtime_truth_classification')}",
        f"target_mode_ready: {str(payload.get('target_mode_ready')).lower()}",
        f"confirmed_sleeve_ids: {', '.join(payload.get('confirmed_sleeve_ids') or []) or 'NONE'}",
        f"event_conditions_detected: {', '.join(payload.get('event_conditions_detected') or []) or 'NONE'}",
        f"regime_conditions_detected: {', '.join(payload.get('regime_conditions_detected') or []) or 'NONE'}",
        f"run_sleeves_decision_count: {payload.get('run_sleeves_decision_count')}",
        "",
        "decisions:",
    ]
    for row in payload.get("decisions") or []:
        lines.append(
            f"- {row.get('trigger_id')}: {row.get('decision')} sleeves={','.join(row.get('selected_sleeve_ids') or []) or 'NONE'} reason={row.get('reason_selected') or row.get('reason_skipped')}"
        )
    lines.append("")
    return "\n".join(lines)


def _decision(
    *,
    trigger_key: str,
    trigger_type: str,
    detected: bool,
    evidence_path: Path | None,
    evidence_payload: dict[str, Any],
    runtime: dict[str, Any],
    registry_mapping: dict[str, Any],
    confirmed_sleeves: list[str],
    run_history: dict[str, Any],
    enabled: bool,
) -> dict[str, Any]:
    trigger_id = f"{trigger_type.lower()}:{trigger_key}"
    evidence_quality = _evidence_quality(evidence_path=evidence_path, evidence_payload=evidence_payload, detected=detected)
    mapped_sleeves = [sleeve for sleeve in _strings(registry_mapping.get("sleeve_ids")) if sleeve in confirmed_sleeves]
    skipped_sleeves = [sleeve for sleeve in _strings(registry_mapping.get("sleeve_ids")) if sleeve not in confirmed_sleeves]
    cooldown_status = "PASS"
    max_runs_status = "PASS"
    prior_count = _prior_run_count(run_history=run_history, trigger_id=trigger_id)
    max_runs = int(registry_mapping.get("max_runs_per_day") or 0)
    if max_runs and prior_count >= max_runs:
        max_runs_status = "BLOCKED_MAX_RUNS_PER_DAY"
    runtime_ok = str(runtime.get("runtime_truth_classification") or "") == "REAL_RUNTIME" and bool(
        runtime.get("human_approved_advisory_runtime_ready")
    )
    decision = "RUN_SLEEVES"
    reason_selected = f"{trigger_key} detected and mapped to confirmed sleeve IDs."
    reason_skipped = ""
    if not enabled:
        decision = "SKIP"
        reason_skipped = "SKIPPED_CONFIG_DISABLED"
    elif not runtime_ok:
        decision = "SKIP"
        reason_skipped = "RUNTIME_TRUTH_NOT_TARGET_MODE_READY"
    elif not evidence_path or evidence_quality == "INSUFFICIENT":
        decision = "INSUFFICIENT_EVIDENCE"
        reason_skipped = "EVENT_OR_REGIME_EVIDENCE_MISSING_OR_STALE"
    elif not detected:
        decision = "SKIP"
        reason_skipped = "CONDITION_NOT_DETECTED"
    elif not registry_mapping:
        decision = "NO_MAPPING"
        reason_skipped = "NO_TRIGGER_MAPPING"
    elif not _strings(registry_mapping.get("sleeve_ids")):
        decision = "NO_MAPPING"
        reason_skipped = "MISSING_SLEEVE_MAPPING"
    elif not mapped_sleeves:
        decision = "NO_CONFIRMED_SLEEVE"
        reason_skipped = "NO_CONFIRMED_SLEEVE"
    elif max_runs_status != "PASS":
        decision = "SKIP"
        reason_skipped = max_runs_status
    if decision != "RUN_SLEEVES":
        reason_selected = ""
    return {
        "trigger_id": trigger_id,
        "detected_condition": trigger_key if detected else "",
        "trigger_type": trigger_type,
        "evidence_artifacts": [str(evidence_path)] if evidence_path else [],
        "evidence_hashes": [_sha256(evidence_path)] if evidence_path else [],
        "evidence_quality": evidence_quality,
        "selected_sleeve_ids": mapped_sleeves if decision == "RUN_SLEEVES" else [],
        "skipped_sleeve_ids": skipped_sleeves,
        "reason_selected": reason_selected,
        "reason_skipped": reason_skipped,
        "cooldown_status": cooldown_status,
        "max_runs_per_day_status": max_runs_status,
        "advisory_only": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "decision": decision,
        "human_review_required": True,
        "mapping_status": registry_mapping.get("mapping_status") or "NOT_FOUND",
    }


def _detected_events(event_status: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    triggered = event_status.get("triggered_events") if isinstance(event_status.get("triggered_events"), list) else []
    for item in triggered:
        text = str(item).upper()
        for event_type in EVENT_TRIGGERS:
            if event_type in text:
                out.add(event_type)
    return out


def _detected_regimes(regime: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    text = json.dumps(regime, sort_keys=True)
    for key in REGIME_TRIGGERS:
        if key not in text:
            continue
        value = _find_regime_value(regime, key)
        if value and value.upper() not in {"UNKNOWN", "INSUFFICIENT_DATA", "NOT_FOUND"}:
            out.add(key)
    return out


def _find_regime_value(payload: Any, key: str) -> str:
    if isinstance(payload, dict):
        if key in payload:
            value = payload[key]
            if isinstance(value, str):
                return value
            if isinstance(value, dict):
                return str(value.get("classification") or value.get("value") or "")
        for value in payload.values():
            found = _find_regime_value(value, key)
            if found:
                return found
    if isinstance(payload, list):
        for item in payload:
            found = _find_regime_value(item, key)
            if found:
                return found
    return ""


def _latest_regime(*, truth_root: Path, day_utc: str) -> tuple[Path | None, dict[str, Any]]:
    for family, filename in (("regime_context_v1", "regime_context.v1.json"), ("regime_detection_v1", "regime_detection.v1.json")):
        path, payload = latest_json_v1(truth_root, family, day_utc, filename)
        if path:
            return path, payload
    return None, {}


def _evidence_quality(*, evidence_path: Path | None, evidence_payload: dict[str, Any], detected: bool) -> str:
    if not evidence_path or not evidence_payload:
        return "INSUFFICIENT"
    stale = str(evidence_payload.get("market_snapshot_freshness_status") or evidence_payload.get("stale_data_status") or "").upper()
    if stale in {"STALE", "INVALID", "MISSING", "MISSING_INPUT", "INVALID_INPUT"} and not detected:
        return "INSUFFICIENT"
    return "MEDIUM" if detected else "LOW"


def _prior_run_count(*, run_history: dict[str, Any], trigger_id: str) -> int:
    runs = run_history.get("runs") if isinstance(run_history.get("runs"), list) else []
    return sum(1 for row in runs if isinstance(row, dict) and row.get("trigger_id") == trigger_id and row.get("status") == "SUCCESS")


def _env_enabled() -> bool:
    raw = os.environ.get("AEGIS_EVENT_REGIME_TRIGGERED_SLEEVES_ENABLED", "true").strip().lower()
    return raw in {"1", "true", "yes", "on", "enabled"}


def _strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip().upper() for item in value if str(item).strip()]


def _sha256(path: Path | None) -> str:
    if not path:
        return ""
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return ""


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
