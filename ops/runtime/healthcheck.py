#!/usr/bin/env python3
"""
Read-only runtime doctor checks for Constellation local runtime.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import supervisor


def _truth_file_check(path: Path) -> Dict[str, Any]:
    return {"path": str(path), "exists": path.exists(), "is_file": path.is_file()}


def _build_doctor_report(manifest_path: Path) -> Dict[str, Any]:
    specs = supervisor.load_manifest(manifest_path)
    snapshots = [supervisor.probe_service(spec) for spec in specs]
    overall_status = supervisor.summarize_overall_status(snapshots)

    truth_files = [
        _truth_file_check(supervisor.SERVICE_STATUS_PATH),
        _truth_file_check(supervisor.SUPERVISOR_STATE_PATH),
        _truth_file_check(supervisor.LAST_START_REPORT_PATH),
    ]

    required = [item for item in snapshots if bool(item.get("required"))]
    required_ready = [item for item in required if item.get("state") == supervisor.STATE_READY]
    required_not_running = [item for item in required if item.get("state") == supervisor.STATE_NOT_RUNNING]

    if required and len(required_not_running) == len(required):
        doctor_status = supervisor.STATE_NOT_RUNNING
        ok = False
    elif required and len(required_ready) == len(required):
        doctor_status = supervisor.STATE_READY
        ok = True
    else:
        doctor_status = supervisor.STATE_DEGRADED
        ok = False

    checks: List[Dict[str, Any]] = []
    for snapshot in snapshots:
        checks.append(
            {
                "service": snapshot["name"],
                "required": snapshot["required"],
                "state": snapshot["state"],
                "pid": snapshot["pid"],
                "pid_running": snapshot["pid_running"],
                "owned_by_supervisor": snapshot["owned_by_supervisor"],
                "port_open": snapshot["port_open"],
                "health_http_status": snapshot["health"]["http_status"],
                "health_ok": snapshot["health"]["ok"],
                "health_error": snapshot["health"]["error"],
            }
        )

    return {
        "schema_id": "runtime_doctor_report.v1",
        "generated_utc": supervisor.utc_now_iso(),
        "ok": ok,
        "status": doctor_status,
        "overall_status_from_supervisor_model": overall_status,
        "manifest_path": str(manifest_path),
        "runtime_root": str(supervisor.RUNTIME_ROOT),
        "truth_file_checks": truth_files,
        "checks": checks,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Constellation runtime doctor checks")
    parser.add_argument("--manifest", default=str(supervisor.DEFAULT_MANIFEST_PATH))
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    manifest_path = Path(args.manifest).resolve()
    try:
        report = _build_doctor_report(manifest_path)
    except Exception as exc:
        report = {
            "schema_id": "runtime_doctor_report.v1",
            "generated_utc": supervisor.utc_now_iso(),
            "ok": False,
            "status": "ERROR",
            "manifest_path": str(manifest_path),
            "error": str(exc),
        }
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2

    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
