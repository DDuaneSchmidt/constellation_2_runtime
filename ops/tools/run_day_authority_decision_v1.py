#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.day_authority_decision_v1 import (
    write_day_authority_decision_from_authority_result_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_session_readiness_refresh_path,
)

OWNER_TOOL = "ops/tools/run_day_authority_decision_v1.py"


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["/usr/bin/git", "rev-parse", "HEAD"],
            cwd=str(REPO_ROOT),
            stderr=subprocess.DEVNULL,
        ).decode("utf-8").strip()
    except Exception:
        return "0" * 40


def _read_json(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"FAIL: report_not_object path={path}")
    return payload


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_day_authority_decision_from_refresh_report_v1(
    *,
    day_utc: str,
    truth_root: Path,
    report_path: Path | None = None,
    producer_git_sha: str = "",
) -> Dict[str, Any]:
    resolved_truth_root = Path(truth_root).expanduser().resolve()
    resolved_report_path = (
        Path(report_path).expanduser().resolve()
        if report_path is not None
        else resolve_session_readiness_refresh_path(truth_root=resolved_truth_root, day_utc=day_utc)
    )
    if not resolved_report_path.exists() or not resolved_report_path.is_file():
        raise SystemExit(f"FAIL: session_readiness_refresh_missing path={resolved_report_path}")

    report = _read_json(resolved_report_path)
    observed_day = str(report.get("day_utc") or "").strip()
    if observed_day != day_utc:
        raise SystemExit(
            "FAIL: session_readiness_refresh_day_mismatch "
            f"expected_day_utc={day_utc} actual_day_utc={observed_day} path={resolved_report_path}"
        )

    results = report.get("results")
    if not isinstance(results, dict):
        raise SystemExit(f"FAIL: session_readiness_refresh_results_missing path={resolved_report_path}")
    authority_result = results.get("authority_kernel_validation")
    if not isinstance(authority_result, dict):
        raise SystemExit(f"FAIL: authority_kernel_validation_missing path={resolved_report_path}")

    write_result = write_day_authority_decision_from_authority_result_v1(
        day_utc=day_utc,
        authority_result=authority_result,
        truth_root=resolved_truth_root,
        producer_module=OWNER_TOOL,
        producer_git_sha=str(producer_git_sha or "").strip() or _git_sha(),
        upstream_artifact_refs=[
            {
                "artifact_id": "session_readiness_refresh_v1",
                "path": str(resolved_report_path),
                "sha256": _sha256_file(resolved_report_path),
                "artifact_class": "admission_result",
                "finality_state": "provisional",
            }
        ],
    )
    if str(write_result.get("status") or "").strip() != "OK":
        raise SystemExit(
            "FAIL: day_authority_decision_write_failed "
            f"status={write_result.get('status')} reason={write_result.get('reason', '')}"
        )
    return write_result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_day_authority_decision_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=True)
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc or "").strip()
    truth_root = Path(str(args.truth_root or "")).expanduser().resolve()
    if len(day_utc) != 10:
        raise SystemExit(f"FAIL: invalid_day_utc day_utc={day_utc!r}")
    if not truth_root.exists() or not truth_root.is_dir():
        raise SystemExit(f"FAIL: invalid_truth_root truth_root={truth_root}")

    report_path = resolve_session_readiness_refresh_path(truth_root=truth_root, day_utc=day_utc)
    write_result = write_day_authority_decision_from_refresh_report_v1(
        day_utc=day_utc,
        truth_root=truth_root,
        report_path=report_path,
        producer_git_sha=_git_sha(),
    )

    print(
        json.dumps(
            {
                "day_utc": day_utc,
                "source_session_readiness_refresh_path": str(report_path),
                "path": str(write_result["path"]),
                "sha256": str(write_result["sha256"]),
                "decision_state": str(write_result["decision_state"]),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
