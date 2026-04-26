#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.ib_reconciliation.paths_v1 import (  # noqa: E402
    daily_alert_candidate_path_v1,
    ensure_runtime_layout_v1,
    runtime_artifact_path_v1,
    validate_day_utc_v1,
)
from constellation_2.ib_reconciliation.schema_v1 import (  # noqa: E402
    read_json_object_v1,
    utc_now_z_v1,
    write_deterministic_json_v1,
)


FINAL_STATUS_VALUES = {"PASS", "WARN", "FAIL", "SKIPPED_NO_IB_REPORT"}
DEFAULT_IB_FLEX_FILENAME = "ib_flex_report.xml"


def _run_tool_step(step_name: str, command: list[str]) -> dict[str, Any]:
    proc = subprocess.run(
        command,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    stdout = str(proc.stdout or "").strip()
    stderr = str(proc.stderr or "").strip()
    if proc.returncode != 0:
        raise SystemExit(
            f"FAIL: step={step_name} rc={proc.returncode} cmd={' '.join(command)} stderr={stderr[-800:]}"
        )
    parsed: dict[str, Any] = {}
    if stdout:
        try:
            payload = json.loads(stdout)
            if isinstance(payload, dict):
                parsed = payload
        except Exception:
            parsed = {"raw_stdout": stdout}
    return {
        "step": step_name,
        "command": command,
        "stdout": stdout,
        "stderr": stderr,
        "parsed": parsed,
    }


def _skip_payload(day_utc: str, expected_path: Path, reason: str) -> dict[str, Any]:
    return {
        "schema_version": "ib_reconciliation_skip.v1",
        "day_utc": day_utc,
        "status": "SKIPPED_NO_IB_REPORT",
        "expected_path": str(expected_path),
        "reason": reason,
        "generated_utc": utc_now_z_v1(),
    }


def _write_daily_alert_candidate(
    *,
    day_utc: str,
    reconciliation_payload: dict[str, Any],
    reconciliation_path: Path,
    ai_packet_path: Path,
) -> Path:
    status = str(reconciliation_payload.get("status") or "").strip().upper()
    severity = "HIGH" if status == "FAIL" else "MEDIUM"
    mismatches = reconciliation_payload.get("mismatches")
    mismatch_rows = [row for row in mismatches if isinstance(row, dict)] if isinstance(mismatches, list) else []
    high_count = len([row for row in mismatch_rows if str(row.get("severity") or "").upper() == "HIGH"])
    medium_count = len([row for row in mismatch_rows if str(row.get("severity") or "").upper() == "MEDIUM"])
    evidence_paths = sorted(
        {
            str(path).strip()
            for row in mismatch_rows
            for path in (row.get("evidence_paths") or [])
            if str(path).strip()
        }
    )
    evidence_paths.extend([str(reconciliation_path), str(ai_packet_path)])
    payload = {
        "day_utc": day_utc,
        "severity": severity,
        "status": status,
        "subject": f"IB reconciliation {status} for {day_utc}",
        "body": (
            f"Deterministic IB reconciliation completed with status={status}. "
            f"high_mismatches={high_count}, medium_mismatches={medium_count}."
        ),
        "evidence_paths": sorted({p for p in evidence_paths if p}),
        "human_alert_required": bool(status == "FAIL" or high_count > 0),
    }
    alert_path = daily_alert_candidate_path_v1(day_utc)
    alert_path.parent.mkdir(parents=True, exist_ok=True)
    write_deterministic_json_v1(alert_path, payload)
    return alert_path


def _default_ib_report_path(day_utc: str) -> Path:
    return runtime_artifact_path_v1("ib_raw", day_utc, DEFAULT_IB_FLEX_FILENAME)


def run_daily_loop_v1(*, day_utc: str, ib_flex_xml: Path | None) -> dict[str, Any]:
    day = validate_day_utc_v1(day_utc)
    ensure_runtime_layout_v1(day)

    report_path = Path(ib_flex_xml).expanduser().resolve() if ib_flex_xml is not None else _default_ib_report_path(day)
    if not report_path.exists() or not report_path.is_file():
        if ib_flex_xml is not None:
            raise SystemExit(f"FAIL: provided --ib_flex_xml does not exist or is not a file: {report_path}")
        skip_path = runtime_artifact_path_v1("reconciliations", day, "ib_reconciliation_skip.v1.json")
        skip_payload = _skip_payload(
            day_utc=day,
            expected_path=report_path,
            reason="IB Flex report file is missing; reconciliation daily loop skipped safely.",
        )
        write_deterministic_json_v1(skip_path, skip_payload)
        return {
            "status": "OK",
            "final_status": "SKIPPED_NO_IB_REPORT",
            "day_utc": day,
            "ib_flex_xml": str(report_path),
            "skip_artifact_path": str(skip_path),
        }

    steps: list[dict[str, Any]] = []
    steps.append(
        _run_tool_step(
            "normalize_ib_flex",
            [
                sys.executable,
                str((REPO_ROOT / "ops/tools/run_ib_flex_normalize_v1.py").resolve()),
                "--day_utc",
                day,
                "--ib_flex_xml",
                str(report_path),
            ],
        )
    )
    steps.append(
        _run_tool_step(
            "extract_aegis_expected",
            [
                sys.executable,
                str((REPO_ROOT / "ops/tools/run_aegis_expected_activity_extract_v1.py").resolve()),
                "--day_utc",
                day,
            ],
        )
    )
    steps.append(
        _run_tool_step(
            "run_reconciliation",
            [
                sys.executable,
                str((REPO_ROOT / "ops/tools/run_ib_reconciliation_v1.py").resolve()),
                "--day_utc",
                day,
            ],
        )
    )
    steps.append(
        _run_tool_step(
            "build_ai_packet",
            [
                sys.executable,
                str((REPO_ROOT / "ops/tools/run_ib_reconciliation_ai_packet_v1.py").resolve()),
                "--day_utc",
                day,
            ],
        )
    )

    reconciliation_path = runtime_artifact_path_v1("reconciliations", day, "ib_reconciliation.v1.json")
    if not reconciliation_path.exists():
        raise SystemExit(f"FAIL: reconciliation output missing after loop: {reconciliation_path}")
    reconciliation_payload = read_json_object_v1(reconciliation_path)
    final_status = str(reconciliation_payload.get("status") or "").strip().upper()
    if final_status not in {"PASS", "WARN", "FAIL"}:
        raise SystemExit(f"FAIL: unexpected reconciliation status: {final_status!r}")

    ai_packet_path = runtime_artifact_path_v1("ai_reviews", day, "ib_reconciliation_ai_packet.v1.json")
    alert_path = None
    if final_status in {"WARN", "FAIL"}:
        alert_path = _write_daily_alert_candidate(
            day_utc=day,
            reconciliation_payload=reconciliation_payload,
            reconciliation_path=reconciliation_path,
            ai_packet_path=ai_packet_path,
        )

    return {
        "status": "OK",
        "final_status": final_status,
        "day_utc": day,
        "ib_flex_xml": str(report_path),
        "reconciliation_path": str(reconciliation_path),
        "ai_packet_path": str(ai_packet_path),
        "alert_candidate_path": str(alert_path) if alert_path else None,
        "steps": [{"step": row["step"], "command": row["command"]} for row in steps],
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run_ib_reconciliation_daily_loop_v1",
        description=(
            "Run daily IB reconciliation when an IB Flex XML report is present. "
            "No network/credential broker fetch is performed in this loop."
        ),
    )
    parser.add_argument("--day_utc", required=True, help="UTC reconciliation day (YYYY-MM-DD).")
    parser.add_argument(
        "--ib_flex_xml",
        default="",
        help=(
            "Optional IB Flex XML file path. If omitted, default lookup path is "
            "/home/node/constellation_runtime_data/ib_reconciliation/ib_raw/<day>/ib_flex_report.xml."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    day = validate_day_utc_v1(args.day_utc)
    ib_flex_xml = Path(str(args.ib_flex_xml).strip()).expanduser().resolve() if str(args.ib_flex_xml).strip() else None
    result = run_daily_loop_v1(day_utc=day, ib_flex_xml=ib_flex_xml)
    final_status = str(result.get("final_status") or "").strip()
    if final_status not in FINAL_STATUS_VALUES:
        raise SystemExit(f"FAIL: invalid final_status generated: {final_status!r}")
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
