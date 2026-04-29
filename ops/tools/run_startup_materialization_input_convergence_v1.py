#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools.c2_account_resolution_v1 import resolve_single_paper_ib_account_from_sleeve_registry  # noqa: E402
from constellation_2.common.paper_session_path_alignment_v1 import resolve_operator_statement_path  # noqa: E402
from constellation_2.common.runtime_path_authority_v1 import resolve_decision_truth_root_v1  # noqa: E402
from constellation_2.common.startup_materialization_input_convergence_v1 import (  # noqa: E402
    derive_startup_materialization_input_convergence_payload_v1,
    write_startup_materialization_input_convergence_v1,
)


def _critical_path_python() -> Path:
    return Path(sys.executable).resolve()


def _git_sha() -> str:
    try:
        return subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT)).decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _read_json(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return payload


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _tool_result(command: List[str], *, env_overrides: Dict[str, str] | None = None, logical_name: str = "") -> Dict[str, Any]:
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    proc = subprocess.run(command, cwd=str(REPO_ROOT), capture_output=True, text=True, env=env)
    return {
        "script": logical_name or (command[2] if len(command) > 2 and command[1] == "-m" else command[1]),
        "command": command,
        "return_code": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _artifact_result(
    *,
    artifact_id: str,
    artifact_path: Path,
    target_day: str,
    required: bool,
    acceptable_statuses: Iterable[str],
    presence_only: bool = False,
) -> Dict[str, Any]:
    path = Path(artifact_path).resolve()
    observed_status = "MISSING"
    observed_day = ""
    ready = False
    reason_codes: List[str] = []
    schema_id = ""
    if path.exists() and path.is_file():
        payload = _read_json(path)
        observed_status = (
            str(payload.get("status") or payload.get("overall_status") or payload.get("final_status") or "").strip().upper()
            or "UNKNOWN"
        )
        observed_day = str(payload.get("day_utc") or payload.get("target_day") or "").strip()
        schema_id = str(payload.get("schema_id") or "").strip()
        reason_codes = [
            str(code).strip()
            for code in (payload.get("reason_codes") or payload.get("blocking_codes") or [])
            if str(code).strip()
        ]
        if presence_only:
            ready = observed_day == target_day or not observed_day
        else:
            ready = observed_day == target_day and observed_status in {str(item).strip().upper() for item in acceptable_statuses}
    blocker_code = ""
    summary = ""
    if not ready:
        if not path.exists():
            blocker_code = f"{artifact_id.upper()}_MISSING"
            summary = f"{artifact_id} missing"
        elif observed_day != target_day:
            blocker_code = f"{artifact_id.upper()}_DAY_MISMATCH"
            summary = f"{artifact_id} day mismatch"
        else:
            blocker_code = f"{artifact_id.upper()}_NOT_READY"
            summary = f"{artifact_id} status={observed_status or 'UNKNOWN'}"
    return {
        "artifact_id": artifact_id,
        "required": bool(required),
        "artifact_path": str(path),
        "schema_id": schema_id,
        "target_day_expected": target_day,
        "target_day_observed": observed_day,
        "observed_status": observed_status,
        "ready": bool(ready),
        "reason_codes": reason_codes,
        "blocker_code": blocker_code,
        "summary": summary,
    }


def _runtime_operator_input_root(*, truth_root: Path, operator_input_root: str = "") -> Path:
    if str(operator_input_root or "").strip():
        return Path(str(operator_input_root).strip()).expanduser().resolve()
    root = Path(truth_root).resolve()
    if root.name == "truth":
        return root.parent.resolve()
    return root.resolve()


def _operator_statement_path(*, operator_input_root: Path, day_utc: str) -> Path:
    return resolve_operator_statement_path(operator_input_root=operator_input_root, day_utc=day_utc).resolve()


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_startup_materialization_input_convergence_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--operator_input_root", default="")
    ap.add_argument("--ib_account", default="")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    truth_root = resolve_decision_truth_root_v1(args.truth_root, repo_root=REPO_ROOT)
    operator_input_root = _runtime_operator_input_root(
        truth_root=truth_root,
        operator_input_root=str(args.operator_input_root or ""),
    )
    git_sha = _git_sha()
    ib_account = str(args.ib_account).strip() or resolve_single_paper_ib_account_from_sleeve_registry(REPO_ROOT)
    operator_statement_path = _operator_statement_path(operator_input_root=operator_input_root, day_utc=day_utc)
    runtime_python = _critical_path_python()

    source_refs: List[Dict[str, Any]] = []
    source_refs.append(
        _tool_result(
            [
                str(runtime_python),
                str((REPO_ROOT / "ops/tools/ensure_cash_ledger_operator_statement_v1.py").resolve()),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(operator_input_root),
                "--ib_account",
                ib_account,
                "--mode",
                "GOVERNED_SEED",
                "--allow_create",
                "NO",
            ],
            logical_name="ops/tools/ensure_cash_ledger_operator_statement_v1.py",
        )
    )
    source_refs.append(
        _tool_result(
            [
                str(runtime_python),
                "-m",
                "constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1",
                "--day_utc",
                day_utc,
                "--operator_statement_json",
                str(operator_statement_path),
                "--producer_repo",
                REPO_ROOT.name,
                "--producer_git_sha",
                git_sha,
            ],
            env_overrides={"C2_TRUTH_ROOT": str(truth_root), "C2_MODE": "PAPER"},
            logical_name="constellation_2.phaseF.cash_ledger.run.run_cash_ledger_snapshot_day_v1",
        )
    )
    source_refs.append(
        _tool_result(
            [
                str(runtime_python),
                str((REPO_ROOT / "ops/tools/run_accounting_nav_v2_day_v1.py").resolve()),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
                "--producer_repo",
                REPO_ROOT.name,
                "--producer_git_sha",
                git_sha,
            ],
            env_overrides={"C2_MODE": "PAPER"},
            logical_name="ops/tools/run_accounting_nav_v2_day_v1.py",
        )
    )
    source_refs.append(
        _tool_result(
            [
                str(runtime_python),
                str((REPO_ROOT / "ops/tools/run_startup_materialization_inputs_prep_v1.py").resolve()),
                "--day_utc",
                day_utc,
                "--truth_root",
                str(truth_root),
            ],
            logical_name="ops/tools/run_startup_materialization_inputs_prep_v1.py",
        )
    )

    artifact_results = [
        _artifact_result(
            artifact_id="operator_statement_v1",
            artifact_path=operator_statement_path,
            target_day=day_utc,
            required=True,
            acceptable_statuses=("OK",),
            presence_only=True,
        ),
        _artifact_result(
            artifact_id="cash_ledger_snapshot_v1",
            artifact_path=truth_root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json",
            target_day=day_utc,
            required=True,
            acceptable_statuses=("OK",),
            presence_only=True,
        ),
        _artifact_result(
            artifact_id="accounting_nav_v2",
            artifact_path=truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json",
            target_day=day_utc,
            required=True,
            acceptable_statuses=("OK", "PASS"),
            presence_only=True,
        ),
        _artifact_result(
            artifact_id="startup_materialization_inputs_prep_v1",
            artifact_path=truth_root / "reports" / "startup_materialization_inputs_prep_v1" / day_utc / "startup_materialization_inputs_prep.v1.json",
            target_day=day_utc,
            required=True,
            acceptable_statuses=("PASS",),
        ),
    ]

    payload = derive_startup_materialization_input_convergence_payload_v1(
        truth_root=truth_root,
        target_day=day_utc,
        required_inputs=[
            "operator_statement_v1",
            "cash_ledger_snapshot_v1",
            "accounting_nav_v2",
            "startup_materialization_inputs_prep_v1",
        ],
        artifact_results=artifact_results,
        source_refs=source_refs,
    )
    ref = write_startup_materialization_input_convergence_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "convergence_status": payload["convergence_status"],
                "target_day": day_utc,
            },
            sort_keys=True,
        )
    )
    return 0 if payload["convergence_status"] == "SUCCESS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
