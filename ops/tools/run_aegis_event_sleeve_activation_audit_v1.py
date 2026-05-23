#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.intelligence_common_v1 import systemd_timer_inventory_v1, write_json_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


REPORT_FAMILY = "aegis_event_sleeve_activation_audit_v1"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_event_sleeve_activation_audit_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    payload = build_event_sleeve_activation_audit_v1(truth_root=truth_root, repo_root=REPO_ROOT, day_utc=str(args.day))
    out_dir = truth_root / "reports" / REPORT_FAMILY / str(args.day)
    json_path = write_json_v1(out_dir / "event_sleeve_activation_audit.v1.json", payload)
    summary_path = out_dir / "event_sleeve_activation_audit.summary.txt"
    summary_path.write_text(render_event_sleeve_activation_summary_v1(payload), encoding="utf-8")
    print(json.dumps({"path": str(json_path), "summary_path": str(summary_path), "event_monitor_cadence_status": payload["event_monitor_cadence_status"], "ad_hoc_sleeve_trigger_status": payload["ad_hoc_sleeve_trigger_status"], "broker_submit_required": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


def build_event_sleeve_activation_audit_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    timers = systemd_timer_inventory_v1(repo_root)
    repo_timers = timers.get("repo_defined_timers") if isinstance(timers.get("repo_defined_timers"), list) else []
    active_timers = timers.get("active_user_timers") if isinstance(timers.get("active_user_timers"), list) else []
    event_timer = next((row for row in repo_timers if row.get("unit") == "aegis-event-monitor-v1.timer"), None)
    active_event_timer = next((row for row in active_timers if row.get("unit") == "aegis-event-monitor-v1.timer"), None)
    service_path = repo_root / "ops" / "systemd" / "user" / "aegis-event-monitor-v1.service"
    wrapper_path = repo_root / "ops" / "tools" / "run_aegis_event_monitor_v1.py"
    common_path = repo_root / "constellation_2" / "common" / "aegis_event_monitoring_v1.py"
    service_text = _read_text(service_path)
    wrapper_text = _read_text(wrapper_path)
    common_text = _read_text(common_path)
    event_monitor_cadence_status = "CONFIRMED" if event_timer and any("10..15:00/15:00" in str(item) for item in event_timer.get("calendar", [])) else ("PARTIAL" if event_timer else "NOT_FOUND")
    source_text = "\n".join([wrapper_text, common_text])
    sleeve_trigger_matches = [
        line.strip()
        for line in source_text.splitlines()
        if re.search(r"sleeve", line, re.IGNORECASE) and re.search(r"run|evaluate|candidate|trigger|orchestrat", line, re.IGNORECASE)
    ]
    ad_hoc_status = "CONFIRMED" if sleeve_trigger_matches else "NOT_FOUND"
    return {
        "schema_id": "aegis_event_sleeve_activation_audit",
        "schema_version": "v1",
        "artifact_id": "aegis_event_sleeve_activation_audit_v1",
        "day_utc": day_utc,
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "truth_root": str(truth_root),
        "event_monitor_cadence_status": event_monitor_cadence_status,
        "event_monitor_timer": event_timer or {"status": "NOT_FOUND"},
        "event_monitor_active_status": "ACTIVE_IN_SYSTEMCTL_OUTPUT" if active_event_timer else "NOT_ACTIVE_IN_SYSTEMCTL_OUTPUT",
        "event_monitor_service": {
            "path": str(service_path),
            "exec_start": _first_match(service_text, r"(?m)^ExecStart=(.+)$") or "NOT_FOUND",
            "manual_only_env": "AEGIS_LITE_MANUAL_ONLY=1" in service_text,
            "scheduled_env": "AEGIS_EVENT_MONITOR_SCHEDULED=1" in service_text,
        },
        "ad_hoc_sleeve_trigger_status": ad_hoc_status,
        "ad_hoc_sleeve_trigger_evidence": sleeve_trigger_matches[:20],
        "sleeve_runs_are_advisory_only": True,
        "can_execute_trades": False,
        "broker_submit_required": False,
        "autonomous_execution_allowed": False,
        "safety": {
            "event_driven_sleeve_activation_may_generate_advisory_candidates_only": True,
            "broker_submit_transmit_introduced": False,
            "autonomous_execution_introduced": False,
        },
    }


def render_event_sleeve_activation_summary_v1(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "AEGIS EVENT/SLEEVE ACTIVATION AUDIT v1",
            f"day_utc: {payload.get('day_utc')}",
            f"event_monitor_cadence_status: {payload.get('event_monitor_cadence_status')}",
            f"event_monitor_active_status: {payload.get('event_monitor_active_status')}",
            f"ad_hoc_sleeve_trigger_status: {payload.get('ad_hoc_sleeve_trigger_status')}",
            f"event_exec_start: {(payload.get('event_monitor_service') or {}).get('exec_start')}",
            "trade_execution: false",
            "broker_submit_required: false",
            "autonomous_execution_allowed: false",
            "",
        ]
    )


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
