from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .common import SLEEVE_TRUTH_ROOT, utc_now_iso


AUDIT_ROOT = (Path(__file__).resolve().parent / "runtime").resolve()
AUDIT_LOG_PATH = (AUDIT_ROOT / "operator_action_audit.v1.jsonl").resolve()

ACTION_SPECS: Dict[str, Dict[str, Any]] = {
    "refresh-lifecycle": {
        "action_name": "refresh-lifecycle",
        "method": "POST",
        "endpoint": "/api/actions/refresh-lifecycle",
        "supported": False,
        "reason": "Canonical runtime-data root does not prove execution_stream_v1 availability required by run_submission_lifecycle_refresh_v1.",
    },
    "run-reconciliation": {
        "action_name": "run-reconciliation",
        "method": "POST",
        "endpoint": "/api/actions/run-reconciliation",
        "supported": True,
        "reason": "Governed day-scoped reconciliation writer exists at ops/tools/run_reconciliation_report_v3.py.",
        "command": [
            "/usr/bin/python3",
            str((Path(__file__).resolve().parents[3] / "ops" / "tools" / "run_reconciliation_report_v3.py").resolve()),
        ],
    },
    "run-replay-check": {
        "action_name": "run-replay-check",
        "method": "POST",
        "endpoint": "/api/actions/run-replay-check",
        "supported": False,
        "reason": "No existing replay_integrity_v2 report family was proven under the canonical runtime-data root for safe CHECK-mode operator use.",
    },
    "cancel-working-order": {
        "action_name": "cancel-working-order",
        "method": "POST",
        "endpoint": "/api/actions/cancel-working-order",
        "supported": False,
        "reason": "No governed cancel-working-order command path was proven in repo beyond low-level adapter and boundary logic.",
    },
}


def _write_audit(entry: Dict[str, Any]) -> None:
    AUDIT_ROOT.mkdir(parents=True, exist_ok=True)
    with AUDIT_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")


def list_action_audit_entries(limit: int = 50) -> List[Dict[str, Any]]:
    if not AUDIT_LOG_PATH.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for raw in AUDIT_LOG_PATH.read_text(encoding="utf-8").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    rows.reverse()
    return rows[:limit]


def build_action_inventory() -> Dict[str, Any]:
    return {
        "as_of_utc": utc_now_iso(),
        "actions": list(ACTION_SPECS.values()),
        "audit_entries": list_action_audit_entries(),
    }


def run_action(action_name: str, body: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    spec = ACTION_SPECS.get(action_name)
    if spec is None:
        return {
            "ok": False,
            "action_name": action_name,
            "result": "UNKNOWN_ACTION",
            "message": f"Unsupported action: {action_name}",
        }
    if not spec.get("supported"):
        entry = {
            "action_name": action_name,
            "target": body or {},
            "time": utc_now_iso(),
            "result": "UNSUPPORTED",
            "message": spec.get("reason"),
            "affected_artifact_refs": [],
        }
        _write_audit(entry)
        return {"ok": False, **entry}

    payload = body or {}
    day = str(payload.get("day_utc") or "").strip()
    if not day:
        return {
            "ok": False,
            "action_name": action_name,
            "result": "DAY_REQUIRED",
            "message": "day_utc is required for governed actions.",
        }

    command = list(spec["command"]) + ["--day_utc", day, "--truth_root", str(SLEEVE_TRUTH_ROOT)]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    result = "OK" if completed.returncode == 0 else "FAILED"
    affected_refs = []
    if action_name == "run-reconciliation":
        affected_refs.append(
            str((SLEEVE_TRUTH_ROOT / "reports" / "reconciliation_report_v3" / day / "reconciliation_report.v3.json").resolve())
        )
    entry = {
        "action_name": action_name,
        "target": {"day_utc": day},
        "time": utc_now_iso(),
        "result": result,
        "returncode": completed.returncode,
        "stdout": completed.stdout[-4000:],
        "stderr": completed.stderr[-4000:],
        "affected_artifact_refs": affected_refs,
    }
    _write_audit(entry)
    return {"ok": completed.returncode == 0, **entry}
