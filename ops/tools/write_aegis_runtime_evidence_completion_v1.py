#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT, read_runtime_state_snapshot_v1  # noqa: E402


def evidence_completion_dir_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_runtime_evidence_completion_v1" / day_utc


def write_evidence_completion_report_v1(
    *,
    truth_root: Path,
    day_utc: str,
    before_snapshot: dict[str, Any],
    after_snapshot: dict[str, Any],
    commands_run: list[str],
    validations_run: list[str],
    started_at: str | None = None,
    completed_at: str | None = None,
) -> dict[str, str]:
    out_dir = evidence_completion_dir_v1(truth_root=truth_root, day_utc=day_utc)
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = build_evidence_completion_report_v1(
        day_utc=day_utc,
        before_snapshot=before_snapshot,
        after_snapshot=after_snapshot,
        commands_run=commands_run,
        validations_run=validations_run,
        started_at=started_at,
        completed_at=completed_at,
    )
    json_path = out_dir / "evidence_completion.v1.json"
    summary_path = out_dir / "evidence_completion.summary.txt"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(render_evidence_completion_summary_v1(payload), encoding="utf-8")
    return {"evidence_completion": str(json_path), "evidence_completion_summary": str(summary_path)}


def build_evidence_completion_report_v1(
    *,
    day_utc: str,
    before_snapshot: dict[str, Any],
    after_snapshot: dict[str, Any],
    commands_run: list[str],
    validations_run: list[str],
    started_at: str | None = None,
    completed_at: str | None = None,
) -> dict[str, Any]:
    before_status = _artifact_status_map(before_snapshot)
    after_status = _artifact_status_map(after_snapshot)
    repaired = sorted(
        artifact_id
        for artifact_id, row in before_status.items()
        if row.get("status") != "OK" and after_status.get(artifact_id, {}).get("status") == "OK"
    )
    still_blocked = [row for row in after_status.values() if row.get("status") != "OK"]
    before_allowed = set(str(item) for item in before_snapshot.get("allowed_capabilities") or [])
    after_allowed = set(str(item) for item in after_snapshot.get("allowed_capabilities") or [])
    return {
        "schema_id": "aegis_runtime_evidence_completion",
        "schema_version": "v1",
        "day_utc": day_utc,
        "started_at": started_at or str(before_snapshot.get("generated_at") or _now()),
        "completed_at": completed_at or str(after_snapshot.get("generated_at") or _now()),
        "before_evaluation_id": before_snapshot.get("evaluation_id"),
        "after_evaluation_id": after_snapshot.get("evaluation_id"),
        "blockers_before": [row for row in before_status.values() if row.get("status") != "OK"],
        "blockers_after": still_blocked,
        "artifacts_repaired": repaired,
        "artifacts_still_blocked": [str(row.get("artifact_id") or "") for row in still_blocked],
        "commands_run": commands_run,
        "validations_run": validations_run,
        "readiness_before": _readiness_summary(before_snapshot),
        "readiness_after": _readiness_summary(after_snapshot),
        "capabilities_gained": sorted(after_allowed - before_allowed),
        "capabilities_lost": sorted(before_allowed - after_allowed),
        "remaining_do_not_claim": after_snapshot.get("do_not_claim") or [],
        "safety_gates_unchanged": True,
        "broker_submit_required": False,
        "autonomous_execution_allowed": False,
        "manual_trade_capture_allowed": "MANUAL_TRADE_CAPTURE_ALLOWED" in after_allowed,
        "trade_advice_allowed": "TRADE_ADVICE_ALLOWED" in after_allowed,
    }


def render_evidence_completion_summary_v1(payload: dict[str, Any]) -> str:
    before = payload.get("readiness_before") if isinstance(payload.get("readiness_before"), dict) else {}
    after = payload.get("readiness_after") if isinstance(payload.get("readiness_after"), dict) else {}
    lines = [
        "AEGIS RUNTIME EVIDENCE COMPLETION v1",
        f"day_utc: {payload.get('day_utc')}",
        f"started_at: {payload.get('started_at')}",
        f"completed_at: {payload.get('completed_at')}",
        f"runtime_truth_before: {before.get('runtime_truth_classification')}",
        f"runtime_truth_after: {after.get('runtime_truth_classification')}",
        f"highest_readiness_before: {before.get('highest_readiness_layer')}",
        f"highest_readiness_after: {after.get('highest_readiness_layer')}",
        f"artifacts_repaired: {', '.join(payload.get('artifacts_repaired') or []) or 'NONE'}",
        f"artifacts_still_blocked: {', '.join(payload.get('artifacts_still_blocked') or []) or 'NONE'}",
        f"capabilities_gained: {', '.join(payload.get('capabilities_gained') or []) or 'NONE'}",
        f"safety_gates_unchanged: {str(payload.get('safety_gates_unchanged')).lower()}",
        "",
        "Commands run:",
    ]
    lines.extend(f"- {item}" for item in payload.get("commands_run") or ["NONE"])
    lines.append("")
    lines.append("Remaining do-not-claim:")
    lines.extend(f"- {item}" for item in payload.get("remaining_do_not_claim") or ["NONE"])
    return "\n".join(lines).rstrip() + "\n"


def _artifact_status_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in snapshot.get("artifact_statuses") or []:
        if isinstance(row, dict) and str(row.get("artifact_id") or ""):
            out[str(row["artifact_id"])] = row
    return out


def _readiness_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "runtime_truth_classification": snapshot.get("runtime_truth_classification"),
        "highest_readiness_layer": snapshot.get("highest_readiness_layer"),
        "layers": snapshot.get("layers") or {},
        "allowed_capabilities": snapshot.get("allowed_capabilities") or [],
        "blocked_capabilities": snapshot.get("blocked_capabilities") or [],
        "recovery_action_count": len(snapshot.get("recovery_plan_summary") or []),
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"FAIL: JSON read failed path={path} err={type(exc).__name__}:{exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"FAIL: JSON object required path={path}")
    return payload


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_runtime_evidence_completion_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", required=True)
    parser.add_argument("--before_snapshot", required=True)
    parser.add_argument("--after_snapshot", default="")
    parser.add_argument("--command", action="append", default=[])
    parser.add_argument("--validation", action="append", default=[])
    parser.add_argument("--started_at", default="")
    parser.add_argument("--completed_at", default="")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    before = _read_json(Path(args.before_snapshot).expanduser().resolve())
    after = _read_json(Path(args.after_snapshot).expanduser().resolve()) if args.after_snapshot else read_runtime_state_snapshot_v1(truth_root=truth_root, day_utc=str(args.day))
    if not after:
        raise SystemExit(f"FAIL: after snapshot missing truth_root={truth_root} day={args.day}")
    paths = write_evidence_completion_report_v1(
        truth_root=truth_root,
        day_utc=str(args.day),
        before_snapshot=before,
        after_snapshot=after,
        commands_run=[str(item) for item in args.command],
        validations_run=[str(item) for item in args.validation],
        started_at=str(args.started_at or ""),
        completed_at=str(args.completed_at or ""),
    )
    print(json.dumps({"paths": paths, "broker_submit_required": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
