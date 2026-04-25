#!/usr/bin/env python3
"""
run_c2_daily_operator_gate_v1.py

Local operator gate derived strictly from canonical readiness truth.

Writes local-state-only artifact:
  ~/.local/state/constellation_2/operator_gate_<day_utc>.v1.json

Inputs (canonical truth only):
- submit_boundary_status_v1
- paper_session_ledger_v1
- paper_day_control_plane_v1
- next_day_readiness_consistency_gate_v1 (computed)

Hostile-review properties:
- Deterministic
- Fail-closed: missing or inconsistent canonical surfaces => FAIL
- Does NOT mutate truth spines
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.next_day_readiness_consistency_gate_v1 import (
    CONSISTENCY_GATE_STATUS_PASS,
    evaluate_next_day_readiness_consistency_gate_v1,
)
from constellation_2.common.paper_session_fact_plane_v1 import read_validated_surface_v1
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_paper_day_control_plane_path,
    resolve_paper_session_ledger_path,
    resolve_submit_boundary_status_path,
)
from constellation_2.common.runtime_authority_bridge_v1 import resolve_canonical_truth_root_bridge_v1


TRUTH = resolve_canonical_truth_root_bridge_v1(caller="ops/tools/run_c2_daily_operator_gate_v1.py").resolve()
STATE_ROOT = (Path.home() / ".local/state/constellation_2").resolve()

SUBMIT_BOUNDARY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/submit_boundary_status.v1.schema.json"
PAPER_SESSION_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_ledger.v1.schema.json"
PAPER_DAY_CONTROL_PLANE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_day_control_plane.v1.schema.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _write_json_atomic(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp.replace(path)


def _reason_codes_from_surfaces(*, boundary_payload: Dict[str, Any], ledger_payload: Dict[str, Any], control_payload: Dict[str, Any]) -> List[str]:
    reason_codes: List[str] = []
    for code in boundary_payload.get("blocking_codes") or []:
        text = str(code).strip()
        if text:
            reason_codes.append(text)
    control_state = ledger_payload.get("control_state") if isinstance(ledger_payload.get("control_state"), dict) else {}
    for code in control_state.get("blocking_codes") or []:
        text = str(code).strip()
        if text:
            reason_codes.append(text)
    for code in control_payload.get("blocking_codes") or []:
        text = str(code).strip()
        if text:
            reason_codes.append(text)
    if not reason_codes:
        if not bool(boundary_payload.get("submission_authorized") is True):
            reason_codes.append("SUBMIT_BOUNDARY_NOT_AUTHORIZED")
        if not bool(control_state.get("submission_authorized") is True):
            reason_codes.append("PAPER_SESSION_LEDGER_NOT_AUTHORIZED")
        if str(control_payload.get("final_start_decision") or "").strip().upper() != "READY_NOW":
            reason_codes.append("PAPER_DAY_CONTROL_PLANE_NOT_READY")
    deduped: List[str] = []
    seen: set[str] = set()
    for code in reason_codes:
        if code in seen:
            continue
        seen.add(code)
        deduped.append(code)
    return deduped


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_c2_daily_operator_gate_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD (UTC)")
    args = ap.parse_args(argv)

    day = _parse_day_utc(args.day_utc)

    boundary_path = resolve_submit_boundary_status_path(truth_root=TRUTH, day_utc=day)
    ledger_path = resolve_paper_session_ledger_path(truth_root=TRUTH, day_utc=day)
    control_path = resolve_paper_day_control_plane_path(truth_root=TRUTH, day_utc=day)

    consistency_result = evaluate_next_day_readiness_consistency_gate_v1(truth_root=TRUTH, day_utc=day)

    reasons: List[str] = []
    notes: List[str] = []

    try:
        boundary_ref = read_validated_surface_v1(path=boundary_path, schema_relpath=SUBMIT_BOUNDARY_SCHEMA)
        boundary_payload = dict(boundary_ref.payload)
    except Exception:
        boundary_ref = None
        boundary_payload = {}
        reasons.append("MISSING_SUBMIT_BOUNDARY_STATUS_V1")

    try:
        ledger_ref = read_validated_surface_v1(path=ledger_path, schema_relpath=PAPER_SESSION_LEDGER_SCHEMA)
        ledger_payload = dict(ledger_ref.payload)
    except Exception:
        ledger_ref = None
        ledger_payload = {}
        reasons.append("MISSING_PAPER_SESSION_LEDGER_V1")

    try:
        control_ref = read_validated_surface_v1(path=control_path, schema_relpath=PAPER_DAY_CONTROL_PLANE_SCHEMA)
        control_payload = dict(control_ref.payload)
    except Exception:
        control_ref = None
        control_payload = {}
        reasons.append("MISSING_PAPER_DAY_CONTROL_PLANE_V1")

    if consistency_result.status != CONSISTENCY_GATE_STATUS_PASS:
        reasons.extend(str(code).strip() for code in consistency_result.blocking_reason_codes if str(code).strip())

    canonical_ready = (
        consistency_result.status == CONSISTENCY_GATE_STATUS_PASS
        and bool(boundary_payload.get("submission_authorized") is True)
        and bool((ledger_payload.get("control_state") or {}).get("submission_authorized") is True)
        and str((ledger_payload.get("control_state") or {}).get("authority_status") or "").strip().upper() == "GRANTED"
        and str(control_payload.get("final_start_decision") or "").strip().upper() == "READY_NOW"
        and str((control_payload.get("authority_result") or {}).get("ledger_authority_status") or "").strip().upper() == "GRANTED"
    )

    if not canonical_ready and boundary_payload and ledger_payload and control_payload:
        reasons.extend(_reason_codes_from_surfaces(boundary_payload=boundary_payload, ledger_payload=ledger_payload, control_payload=control_payload))

    deduped_reasons: List[str] = []
    seen: set[str] = set()
    for reason in reasons:
        text = str(reason).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped_reasons.append(text)

    status = "PASS" if canonical_ready and not deduped_reasons else "FAIL"
    notes.append(f"consistency_gate_status={consistency_result.status}")
    notes.append(f"submit_boundary_status={str(boundary_payload.get('boundary_status') or 'MISSING').strip() or 'MISSING'}")
    notes.append(f"paper_session_ledger_authority_status={str((ledger_payload.get('control_state') or {}).get('authority_status') or 'MISSING').strip() or 'MISSING'}")
    notes.append(f"paper_day_control_plane_decision={str(control_payload.get('final_start_decision') or 'MISSING').strip() or 'MISSING'}")

    out = {
        "schema_id": "C2_OPERATOR_GATE_V1",
        "schema_version": 2,
        "produced_utc": _utc_now(),
        "day_utc": day,
        "producer": {
            "repo": "constellation",
            "module": "ops/tools/run_c2_daily_operator_gate_v1.py",
        },
        "inputs": {
            "submit_boundary_status_path": str(boundary_path),
            "paper_session_ledger_path": str(ledger_path),
            "paper_day_control_plane_path": str(control_path),
        },
        "status": status,
        "reason_codes": deduped_reasons,
        "notes": notes,
        "consistency_gate": {
            "status": consistency_result.status,
            "blocking_reason_codes": list(consistency_result.blocking_reason_codes),
            "kill_switch_reason_codes": list(consistency_result.kill_switch_reason_codes),
            "issues": [
                {
                    "reason_code": issue.reason_code,
                    "summary": issue.summary,
                    "surface_name": issue.surface_name,
                    "artifact_path": issue.artifact_path,
                    "observed": issue.observed,
                    "expected": issue.expected,
                }
                for issue in consistency_result.issues
            ],
        },
        "canonical_truth": {
            "submission_authorized": bool(boundary_payload.get("submission_authorized") is True),
            "submit_boundary_status": str(boundary_payload.get("boundary_status") or "").strip(),
            "paper_session_ledger_authority_status": str((ledger_payload.get("control_state") or {}).get("authority_status") or "").strip(),
            "paper_session_ledger_submission_authorized": bool((ledger_payload.get("control_state") or {}).get("submission_authorized") is True),
            "paper_day_control_plane_decision": str(control_payload.get("final_start_decision") or "").strip(),
            "paper_day_control_plane_ledger_authority_status": str((control_payload.get("authority_result") or {}).get("ledger_authority_status") or "").strip(),
        },
    }

    out_path = (STATE_ROOT / f"operator_gate_{day}.v1.json").resolve()
    _write_json_atomic(out_path, out)

    print(f"OK: OPERATOR_GATE_WRITTEN day_utc={day} status={status} path={out_path}")
    if status != "PASS":
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:  # noqa: BLE001
        print(f"FAIL: {e}", file=sys.stderr)
        raise
