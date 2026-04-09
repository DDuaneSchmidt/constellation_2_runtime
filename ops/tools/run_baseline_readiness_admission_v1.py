#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_operator_statement_path,
    resolve_repo_operator_input_root,
    resolve_repo_truth_root,
)


TRUTH_ROOT = resolve_repo_truth_root(REPO_ROOT)
OPERATOR_INPUT_ROOT = resolve_repo_operator_input_root(REPO_ROOT)


def _git_sha() -> str:
    try:
        return subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT)).decode("utf-8").strip()
    except Exception:
        # Clean runtime roots can be source-derived without .git metadata.
        return "0" * 40


def _run(cmd: list[str]) -> dict[str, object]:
    env = dict(os.environ)
    env["C2_AUTHORITY_MODE"] = "governance_primary"
    env["C2_TRUTH_ROOT"] = str(TRUTH_ROOT)
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": str(proc.stdout).strip(),
        "stderr": str(proc.stderr).strip(),
    }


def main() -> int:
    global TRUTH_ROOT
    ap = argparse.ArgumentParser(prog="run_baseline_readiness_admission_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args()

    if str(args.truth_root).strip():
        TRUTH_ROOT = Path(str(args.truth_root)).resolve()

    day = str(args.day_utc).strip()
    git_sha = _git_sha()
    python_bin = str(Path(sys.executable).resolve())
    paper_account = resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    operator_statement_path = resolve_operator_statement_path(
        operator_input_root=OPERATOR_INPUT_ROOT,
        day_utc=day,
    )
    nav_path = (TRUTH_ROOT / "accounting_v2" / "nav" / day / "nav.v2.json").resolve()
    attempt_id = f"{day}__ADMISSION__{datetime.now(timezone.utc).strftime('%H%M%S')}__{git_sha[:7]}"

    steps = [
        _run(
            [
                python_bin,
                str((REPO_ROOT / "ops/tools/ensure_cash_ledger_operator_statement_v1.py").resolve()),
                "--day_utc",
                day,
                "--truth_root",
                str(OPERATOR_INPUT_ROOT),
                "--ib_account",
                paper_account,
                "--mode",
                "SEED_100K",
                "--allow_create",
                "YES",
            ]
        ),
        _run(
            [
                python_bin,
                "-m",
                "constellation_2.phaseF.positions.run.run_positions_snapshot_day_v2",
                "--day_utc",
                day,
                "--producer_git_sha",
                git_sha,
                "--producer_repo",
                REPO_ROOT.name,
            ]
        ),
        _run(
            [
                python_bin,
                "-m",
                "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1",
                "--day_utc",
                day,
                "--operator_statement_json",
                str(operator_statement_path),
                "--producer_repo",
                REPO_ROOT.name,
                "--producer_git_sha",
                git_sha,
            ]
        ),
        _run(
            [
                python_bin,
                str((REPO_ROOT / "ops/tools/run_accounting_nav_v2_day_v1.py").resolve()),
                "--day_utc",
                day,
                "--truth_root",
                str(TRUTH_ROOT),
                "--producer_repo",
                REPO_ROOT.name,
                "--producer_git_sha",
                git_sha,
            ]
        ),
    ]

    if any(int(step["returncode"]) != 0 for step in steps):
        print(json.dumps({"day_utc": day, "status": "FAIL", "steps": steps}, indent=2, sort_keys=True))
        return 2

    nav_obj = json.loads(nav_path.read_text(encoding="utf-8"))
    nav = nav_obj.get("nav") or {}
    nav_total = nav.get("nav_total")
    status = str(nav_obj.get("status") or "").strip().upper()
    if status != "ACTIVE" or not isinstance(nav_total, int) or nav_total <= 0:
        print(
            json.dumps(
                {
                    "day_utc": day,
                    "status": "FAIL",
                    "reason": "NAV_NOT_ACTIVE_OR_POSITIVE",
                    "nav_path": str(nav_path),
                    "nav_status": status,
                    "nav_total": nav_total,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2

    publish = _run(
        [
            python_bin,
            str((REPO_ROOT / "ops/run/c2_baseline_readiness_publish_v1.py").resolve()),
            "--day_utc",
            day,
            "--truth_root",
            str(TRUTH_ROOT),
            "--attempt_id",
            attempt_id,
            "--ib_account",
            paper_account,
            "--nav_path",
            str(nav_path),
            "--producer_git_sha",
            git_sha,
        ]
    )
    status_text = "PASS" if int(publish["returncode"]) == 0 else "FAIL"
    print(
        json.dumps(
            {
                "day_utc": day,
                "status": status_text,
                "steps": steps + [publish],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if int(publish["returncode"]) == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
