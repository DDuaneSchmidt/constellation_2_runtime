#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    NON_AUTHORITY_SCOPE,
    atomic_write_idempotent_validated_json_v1,
    build_fact_dependency_row_v1,
    canonical_paper_session_id_v1,
    collect_intent_files_v1,
    discover_phasec_identity_dirs_v1,
    now_utc_iso_v1,
    parse_day_utc_v1,
    phasec_root_v1,
    producer_block_v1,
    read_json_object_v1,
    repo_git_sha_v1,
    resolve_fact_plane_truth_root_v1,
    resolve_startup_materialization_path,
)


def _startup_materialization_inputs_prep_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "startup_materialization_inputs_prep_v1"
        / day_utc
        / "startup_materialization_inputs_prep.v1.json"
    ).resolve()


def _phasec_risk_inputs_prep_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        truth_root
        / "reports"
        / "phasec_risk_inputs_prep_v1"
        / day_utc
        / "phasec_risk_inputs_prep.v1.json"
    ).resolve()


def _run_inputs_prep(*, day_utc: str, truth_root: Path) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str((REPO_ROOT / "ops/tools/run_startup_materialization_inputs_prep_v1.py").resolve()),
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


def _run_phasec_risk_inputs_prep(*, day_utc: str, truth_root: Path) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str((REPO_ROOT / "ops/tools/run_phasec_risk_inputs_prep_v1.py").resolve()),
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


def _load_inputs_prep_payload(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return None
    try:
        return read_json_object_v1(path)
    except ValueError:
        return None


def _load_phasec_risk_inputs_prep_payload(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return None
    try:
        return read_json_object_v1(path)
    except ValueError:
        return None


def _run_phasec_materializer(
    *,
    day_utc: str,
    truth_root: Path,
    default_equity_reference_price: str = "",
) -> Dict[str, Any]:
    cmd = [
        sys.executable,
        str((REPO_ROOT / "ops/tools/run_phasec_identity_materializer_day_v1.py").resolve()),
        "--day_utc",
        day_utc,
        "--eval_time_utc",
        f"{day_utc}T00:00:00Z",
        "--truth_root",
        str(truth_root),
    ]
    if str(default_equity_reference_price or "").strip():
        cmd.extend(["--default_equity_reference_price", str(default_equity_reference_price).strip()])
    proc = subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True, check=False)
    return {
        "cmd": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def _identity_output_rows(identity_dirs: List[Path], *, day_utc: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for identity_dir in identity_dirs:
        rows.append(
            build_fact_dependency_row_v1(
                logical_name=f"phasec_identity_dir:{identity_dir.name}",
                absolute_path=identity_dir,
                status="PRESENT",
                reason_codes=[],
                day_utc=day_utc,
            )
        )
        for child in sorted(identity_dir.iterdir()):
            if not child.is_file():
                continue
            rows.append(
                build_fact_dependency_row_v1(
                    logical_name=f"phasec_materialized_output:{identity_dir.name}:{child.name}",
                    absolute_path=child,
                    status="PRESENT",
                    reason_codes=[],
                    day_utc=day_utc,
                )
            )
    return rows


def _latest_phasec_veto(*, phasec_root: Path, active_attempt_dir: Optional[Path]) -> Optional[Dict[str, Any]]:
    def _scan_attempt_dir(attempt_dir: Path) -> Optional[Dict[str, Any]]:
        veto_paths = sorted(attempt_dir.glob("*.veto_record.v1.json"), key=lambda item: item.name, reverse=True)
        for veto_path in veto_paths:
            try:
                veto_obj = read_json_object_v1(veto_path)
            except ValueError:
                continue
            return {
                "path": veto_path,
                "reason_code": str(veto_obj.get("reason_code") or "").strip(),
                "reason_detail": str(veto_obj.get("reason_detail") or "").strip(),
            }
        return None

    if active_attempt_dir is not None:
        return _scan_attempt_dir(active_attempt_dir)
    if not phasec_root.exists() or not phasec_root.is_dir():
        return None
    attempt_dirs = sorted(
        [item for item in phasec_root.iterdir() if item.is_dir() and item.name.startswith("attempt_")],
        key=lambda item: item.name,
        reverse=True,
    )
    for attempt_dir in attempt_dirs:
        veto = _scan_attempt_dir(attempt_dir)
        if veto is not None:
            return veto
    return None


def _phasec_veto_blocking_codes(*, phasec_root: Path, active_attempt_dir: Optional[Path]) -> List[str]:
    veto = _latest_phasec_veto(phasec_root=phasec_root, active_attempt_dir=active_attempt_dir)
    if veto is None:
        return []
    codes: List[str] = []
    reason_code = str(veto["reason_code"]).strip()
    reason_detail = str(veto["reason_detail"]).strip()
    if reason_code:
        codes.append(f"STARTUP_MATERIALIZATION_FAIL:PHASEC_VETO:{reason_code}")
    if "DRAWDOWN_MISSING_FAIL_CLOSED" in reason_detail:
        codes.append("STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:DRAWDOWN_MISSING_FAIL_CLOSED")
    missing_match = re.search(r"INPUT_FILE_MISSING:\s*([^'\n]+)", reason_detail)
    if missing_match:
        missing_path = missing_match.group(1).strip()
        codes.append(f"STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:INPUT_FILE_MISSING:{missing_path}")
    return codes


def _classify_status(*, blocking_codes: List[str], phasec_rc: int, identity_dirs: List[Path]) -> str:
    if not blocking_codes and phasec_rc == 0 and identity_dirs:
        return "SUCCESS"
    if any(code.startswith("STARTUP_MATERIALIZATION_MALFORMED") for code in blocking_codes):
        return "MALFORMED"
    if any(code.startswith("STARTUP_MATERIALIZATION_STALE") for code in blocking_codes):
        return "STALE"
    if any(code.startswith("STARTUP_MATERIALIZATION_MISSING_DEPENDENCY") for code in blocking_codes):
        return "MISSING_DEPENDENCY"
    if any(code.startswith("STARTUP_MATERIALIZATION_UNKNOWN") for code in blocking_codes):
        return "UNKNOWN"
    return "FAIL"


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_startup_materialization_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    produced_at_utc = now_utc_iso_v1()
    session_id = canonical_paper_session_id_v1(day_utc)

    intent_files = collect_intent_files_v1(truth_root=truth_root, day_utc=day_utc)
    inputs_checked: List[Dict[str, Any]] = [
        build_fact_dependency_row_v1(
            logical_name="intents_day_dir",
            absolute_path=(truth_root / "intents_v1" / "snapshots" / day_utc).resolve(),
            status="PRESENT" if intent_files else "MISSING",
            reason_codes=[] if intent_files else ["STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:INTENTS_DAY_DIR_EMPTY_OR_ABSENT"],
            day_utc=day_utc,
        )
    ]
    inputs_checked.extend(
        build_fact_dependency_row_v1(
            logical_name=f"intent_file:{path.name}",
            absolute_path=path,
            status="PRESENT",
            reason_codes=[],
            day_utc=day_utc,
        )
        for path in intent_files
    )

    inputs_prep_path = _startup_materialization_inputs_prep_path(truth_root=truth_root, day_utc=day_utc)
    phasec_risk_inputs_prep_path = _phasec_risk_inputs_prep_path(truth_root=truth_root, day_utc=day_utc)
    inputs_prep_result = _run_inputs_prep(day_utc=day_utc, truth_root=truth_root)
    inputs_prep_payload = _load_inputs_prep_payload(inputs_prep_path)
    prep_blocking_codes: List[str] = []
    phasec_risk_prep_blocking_codes: List[str] = []
    default_equity_reference_price = ""
    if inputs_prep_payload is None:
        inputs_checked.append(
            build_fact_dependency_row_v1(
                logical_name="startup_materialization_inputs_prep_v1",
                absolute_path=inputs_prep_path,
                status="MISSING",
                reason_codes=["STARTUP_MATERIALIZATION_FAIL:INPUTS_PREP_OUTPUT_MISSING"],
                day_utc=day_utc,
            )
        )
        prep_blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:INPUTS_PREP_OUTPUT_MISSING")
    else:
        prep_codes = [
            str(code).strip()
            for code in (inputs_prep_payload.get("blocking_codes") or [])
            if str(code).strip()
        ]
        prep_status = str(inputs_prep_payload.get("status") or "").strip().upper()
        inputs_checked.append(
            build_fact_dependency_row_v1(
                logical_name="startup_materialization_inputs_prep_v1",
                absolute_path=inputs_prep_path,
                status="PRESENT",
                reason_codes=prep_codes,
                day_utc=day_utc,
            )
        )
        if prep_status == "PASS":
            default_equity_reference_price = str(
                inputs_prep_payload.get("default_equity_reference_price") or ""
            ).strip()
        elif prep_status in {"BLOCKED_VALID", "BLOCKED_BY_DEFECT"}:
            prep_blocking_codes.extend(prep_codes or ["STARTUP_MATERIALIZATION_FAIL:INPUTS_PREP_BLOCKED"])
        else:
            prep_blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:INPUTS_PREP_STATUS_UNKNOWN")

    if not prep_blocking_codes:
        _run_phasec_risk_inputs_prep(day_utc=day_utc, truth_root=truth_root)
        phasec_risk_prep_payload = _load_phasec_risk_inputs_prep_payload(phasec_risk_inputs_prep_path)
        if phasec_risk_prep_payload is None:
            inputs_checked.append(
                build_fact_dependency_row_v1(
                    logical_name="phasec_risk_inputs_prep_v1",
                    absolute_path=phasec_risk_inputs_prep_path,
                    status="MISSING",
                    reason_codes=["STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_OUTPUT_MISSING"],
                    day_utc=day_utc,
                )
            )
            phasec_risk_prep_blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_OUTPUT_MISSING")
        else:
            risk_codes = [
                f"STARTUP_MATERIALIZATION_FAIL:{str(code).strip()}"
                for code in (phasec_risk_prep_payload.get("blocking_codes") or [])
                if str(code).strip()
            ]
            risk_status = str(phasec_risk_prep_payload.get("status") or "").strip().upper()
            inputs_checked.append(
                build_fact_dependency_row_v1(
                    logical_name="phasec_risk_inputs_prep_v1",
                    absolute_path=phasec_risk_inputs_prep_path,
                    status="PRESENT",
                    reason_codes=risk_codes,
                    day_utc=day_utc,
                )
            )
            if risk_status in {"BLOCKED_VALID", "BLOCKED_BY_DEFECT"}:
                phasec_risk_prep_blocking_codes.extend(risk_codes or ["STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_BLOCKED"])
            elif risk_status != "PASS":
                phasec_risk_prep_blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:PHASEC_RISK_INPUTS_PREP_STATUS_UNKNOWN")

    phasec_root = phasec_root_v1(truth_root=truth_root, day_utc=day_utc)
    latest_active_pointer = (phasec_root / "latest_active_attempt.v1.json").resolve()
    identity_dirs: List[Path] = []
    materialized_outputs: List[Dict[str, Any]] = []

    blocking_codes: List[str] = []
    if not intent_files:
        blocking_codes.append("STARTUP_MATERIALIZATION_MISSING_DEPENDENCY:INTENTS_DAY_DIR_EMPTY_OR_ABSENT")
    blocking_codes.extend(prep_blocking_codes)
    blocking_codes.extend(phasec_risk_prep_blocking_codes)
    if prep_blocking_codes or phasec_risk_prep_blocking_codes:
        phasec_result = {
            "cmd": [],
            "returncode": -1,
            "stdout": "",
            "stderr": "SKIPPED_PRESTART_PREP_BLOCKED",
        }
    else:
        phasec_result = _run_phasec_materializer(
            day_utc=day_utc,
            truth_root=truth_root,
            default_equity_reference_price=default_equity_reference_price,
        )
        identity_dirs = discover_phasec_identity_dirs_v1(truth_root=truth_root, day_utc=day_utc)
        materialized_outputs = _identity_output_rows(identity_dirs, day_utc=day_utc)
        active_attempt_dir: Optional[Path] = None
        if phasec_result["returncode"] != 0:
            blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:PHASEC_MATERIALIZER_NONZERO")
        if latest_active_pointer.exists() and latest_active_pointer.is_file():
            try:
                pointer_obj = read_json_object_v1(latest_active_pointer)
            except ValueError:
                blocking_codes.append("STARTUP_MATERIALIZATION_MALFORMED:LATEST_ACTIVE_ATTEMPT_POINTER_INVALID")
            else:
                if str(pointer_obj.get("day_utc") or "").strip() != day_utc:
                    blocking_codes.append("STARTUP_MATERIALIZATION_STALE:LATEST_ACTIVE_ATTEMPT_DAY_MISMATCH")
                else:
                    active_attempt_dir = Path(str(pointer_obj.get("attempt_dir") or "")).expanduser().resolve()
        phasec_veto_codes = _phasec_veto_blocking_codes(
            phasec_root=phasec_root,
            active_attempt_dir=active_attempt_dir,
        )
        if phasec_veto_codes:
            blocking_codes.extend(phasec_veto_codes)
        else:
            if not latest_active_pointer.exists() or not latest_active_pointer.is_file():
                blocking_codes.append("STARTUP_MATERIALIZATION_UNKNOWN:LATEST_ACTIVE_ATTEMPT_POINTER_MISSING")
            if not identity_dirs:
                blocking_codes.append("STARTUP_MATERIALIZATION_FAIL:NO_SUPPORTED_IDENTITY_OUTPUTS")

    status = _classify_status(
        blocking_codes=sorted(set(blocking_codes)),
        phasec_rc=int(phasec_result["returncode"]),
        identity_dirs=identity_dirs,
    )
    payload: Dict[str, Any] = {
        "schema_id": "startup_materialization",
        "schema_version": "v1",
        "authority_scope": NON_AUTHORITY_SCOPE,
        "day_utc": day_utc,
        "session_id": session_id,
        "status": status,
        "required_inputs_checked": inputs_checked,
        "materialized_outputs": materialized_outputs,
        "blocking_codes": sorted(set(blocking_codes)),
        "producer": producer_block_v1(module="ops/tools/run_startup_materialization_v1.py", git_sha=repo_git_sha_v1()),
        "produced_at_utc": produced_at_utc,
        "freshness_verdict": "CURRENT" if status == "SUCCESS" else "CURRENT" if latest_active_pointer.exists() else "UNKNOWN",
        "linkage_verdict": "LINKED" if status == "SUCCESS" else "UNLINKED",
        "path_resolution_evidence": {
            "phasec_root": str(phasec_root),
            "latest_active_attempt_path": str(latest_active_pointer),
        },
        "producer_run_id": f"startup_materialization_v1:{day_utc}",
        "phasec_materializer_result": {
            "returncode": int(phasec_result["returncode"]),
            "stdout": str(phasec_result["stdout"]),
            "stderr": str(phasec_result["stderr"]),
        },
    }
    ref = atomic_write_idempotent_validated_json_v1(
        path=resolve_startup_materialization_path(truth_root=truth_root, day_utc=day_utc),
        payload=payload,
        schema_relpath="governance/04_DATA/SCHEMAS/C2/REPORTS/startup_materialization.v1.schema.json",
        volatile_field_names=("produced_at_utc",),
    )
    print(json.dumps({"path": str(ref.path), "sha256": ref.sha256, "status": status}, sort_keys=True))
    return 0 if status == "SUCCESS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
