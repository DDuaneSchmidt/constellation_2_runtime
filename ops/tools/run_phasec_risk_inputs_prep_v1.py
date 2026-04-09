#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (  # noqa: E402
    NON_AUTHORITY_SCOPE,
    atomic_write_validated_json_v1,
    build_fact_dependency_row_v1,
    canonical_paper_session_id_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    producer_block_v1,
    read_json_object_v1,
    repo_git_sha_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402


def _compat_nav_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "accounting_compat_v1" / "nav" / day_utc / "nav_snapshot.v1.json").resolve()


def _accounting_nav_v2_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "accounting_v2" / "nav" / day_utc / "nav.v2.json").resolve()


def _output_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "phasec_risk_inputs_prep_v1"
        / day_utc
        / "phasec_risk_inputs_prep.v1.json"
    ).resolve()


def _run_nav_compat_bridge(*, day_utc: str, truth_root: Path) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str((REPO_ROOT / "ops/tools/bridge_accounting_nav_v2_to_compat_v1.py").resolve()),
        "--day_utc",
        day_utc,
        "--truth_root",
        str(truth_root),
    ]
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_phasec_risk_inputs_prep_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    session_id = canonical_paper_session_id_v1(day_utc)
    produced_at_utc = now_utc_iso_v1()

    nav_v2_path = _accounting_nav_v2_path(truth_root=truth_root, day_utc=day_utc)
    compat_path = _compat_nav_path(truth_root=truth_root, day_utc=day_utc)
    checked_inputs: List[Dict[str, Any]] = [
        build_fact_dependency_row_v1(
            logical_name="accounting_nav_v2",
            absolute_path=nav_v2_path,
            status="PRESENT" if nav_v2_path.exists() else "MISSING",
            reason_codes=[] if nav_v2_path.exists() else ["PHASEC_RISK_INPUTS_PREP_MISSING_DEPENDENCY:ACCOUNTING_NAV_V2_MISSING"],
            day_utc=day_utc,
        )
    ]

    bridge_result = {"cmd": [], "returncode": -1, "stdout": "", "stderr": ""}
    blocking_codes: List[str] = []
    status = "PASS"
    drawdown_pct = ""

    if not nav_v2_path.exists():
        status = "BLOCKED_VALID"
        blocking_codes.append("PHASEC_RISK_INPUTS_PREP_MISSING_DEPENDENCY:ACCOUNTING_NAV_V2_MISSING")
    else:
        bridge_result = _run_nav_compat_bridge(day_utc=day_utc, truth_root=truth_root)
        if bridge_result["returncode"] != 0:
            bridge_text = "\n".join([str(bridge_result["stdout"]), str(bridge_result["stderr"])])
            if "NO_POSITIVE_PEAK_AVAILABLE_FOR_DRAWDOWN" in bridge_text:
                status = "BLOCKED_VALID"
                blocking_codes.append("PHASEC_RISK_INPUTS_PREP_MISSING_DEPENDENCY:NO_POSITIVE_PEAK_AVAILABLE_FOR_DRAWDOWN")
            else:
                status = "BLOCKED_BY_DEFECT"
                blocking_codes.append("PHASEC_RISK_INPUTS_PREP_FAIL:ACCOUNTING_NAV_COMPAT_BRIDGE_NONZERO")
        elif not compat_path.exists() or not compat_path.is_file():
            status = "BLOCKED_BY_DEFECT"
            blocking_codes.append("PHASEC_RISK_INPUTS_PREP_FAIL:ACCOUNTING_NAV_COMPAT_OUTPUT_MISSING")
        else:
            checked_inputs.append(
                build_fact_dependency_row_v1(
                    logical_name="accounting_nav_compat_v1",
                    absolute_path=compat_path,
                    status="PRESENT",
                    reason_codes=[],
                    day_utc=day_utc,
                )
            )
            try:
                compat_obj = read_json_object_v1(compat_path)
                validate_against_repo_schema_v1(
                    compat_obj,
                    REPO_ROOT,
                    "governance/04_DATA/SCHEMAS/C2/ACCOUNTING/accounting_nav.v1.schema.json",
                )
            except BaseException:
                status = "BLOCKED_BY_DEFECT"
                blocking_codes.append("PHASEC_RISK_INPUTS_PREP_MALFORMED:ACCOUNTING_NAV_COMPAT_INVALID")
            else:
                history = compat_obj.get("history") if isinstance(compat_obj.get("history"), dict) else {}
                drawdown_pct = str(history.get("drawdown_pct") or "").strip()
                if not drawdown_pct:
                    status = "BLOCKED_BY_DEFECT"
                    blocking_codes.append("PHASEC_RISK_INPUTS_PREP_MISSING_DEPENDENCY:DRAWDOWN_MISSING_FAIL_CLOSED")

    summary = (
        f"phase-C risk inputs ready for {day_utc}"
        if status == "PASS"
        else f"phase-C risk inputs blocked for {day_utc}: {', '.join(sorted(set(blocking_codes)))}"
    )

    payload: Dict[str, Any] = {
        "schema_id": "phasec_risk_inputs_prep",
        "schema_version": "v1",
        "authority_scope": NON_AUTHORITY_SCOPE,
        "day_utc": day_utc,
        "session_id": session_id,
        "status": status,
        "compatible_nav_path": str(compat_path),
        "drawdown_pct": drawdown_pct,
        "required_inputs_checked": checked_inputs,
        "blocking_codes": sorted(set(blocking_codes)),
        "producer": producer_block_v1(module="ops/tools/run_phasec_risk_inputs_prep_v1.py", git_sha=repo_git_sha_v1()),
        "produced_at_utc": produced_at_utc,
        "bridge_result": {
            "returncode": int(bridge_result["returncode"]),
            "stdout": str(bridge_result["stdout"]),
            "stderr": str(bridge_result["stderr"]),
            "artifact_path": str(compat_path),
            "artifact_status": "PRESENT" if compat_path.exists() else "MISSING",
        },
        "human_readable_summary": summary,
    }

    ref = atomic_write_validated_json_v1(
        path=_output_path(truth_root=truth_root, day_utc=day_utc),
        payload=payload,
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/phasec_risk_inputs_prep.v1.schema.json",
    )
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256, "status": status}, sort_keys=True))
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
