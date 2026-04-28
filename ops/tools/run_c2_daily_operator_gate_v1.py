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
    resolve_paper_trading_day_authority_path,
    resolve_paper_session_ledger_path,
    resolve_submit_boundary_status_path,
)
from constellation_2.common.runtime_authority_bridge_v1 import resolve_canonical_truth_root_bridge_v1
from constellation_2.common.session_authority_monitor_v1 import (
    build_session_authority_status_payload_v1,
    write_session_authority_status_v1,
)
from constellation_2.common.session_authority_v1 import (
    resolve_active_session_path,
    resolve_target_day_admission_path,
)


TRUTH = resolve_canonical_truth_root_bridge_v1(caller="ops/tools/run_c2_daily_operator_gate_v1.py").resolve()
STATE_ROOT = (Path.home() / ".local/state/constellation_2").resolve()

SUBMIT_BOUNDARY_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/submit_boundary_status.v1.schema.json"
PAPER_SESSION_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_session_ledger.v1.schema.json"
PAPER_DAY_CONTROL_PLANE_SCHEMA = "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_day_control_plane.v1.schema.json"
PAPER_TRADING_DAY_AUTHORITY_SCHEMA = (
    "governance/04_DATA/SCHEMAS/C2/REPORTS/paper_trading_day_authority.v1.schema.json"
)
SESSION_AUTHORITY_STALE = "SESSION_AUTHORITY_STATUS_STALE"
NON_TRADING_SESSION_BLOCKERS = {"NON_TRADING_DAY", "NO_ACTIVE_PAPER_SESSION"}


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


def _read_optional_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _session_day_blocker(*, day_utc: str, truth_root: Path) -> str:
    active_session_path = resolve_active_session_path(truth_root=truth_root)
    target_day_admission_path = resolve_target_day_admission_path(truth_root=truth_root, target_day=day_utc)
    active_session = _read_optional_json(active_session_path)
    target_day_admission = _read_optional_json(target_day_admission_path)
    if not active_session:
        return "SESSION_AUTHORITY_MISSING"
    if not target_day_admission:
        return "SESSION_AUTHORITY_MISSING"
    blocking_reason_codes = (
        [str(item or "").strip().upper() for item in (target_day_admission.get("blocking_reason_codes") or [])]
        if isinstance(target_day_admission.get("blocking_reason_codes"), list)
        else []
    )
    if "NON_TRADING_DAY" in blocking_reason_codes:
        return "NON_TRADING_DAY"
    active_day = str(active_session.get("active_day") or "").strip()
    admission_status = str(target_day_admission.get("admission_status") or "").strip().upper()
    rollover_status = str(active_session.get("rollover_status") or "").strip().upper()
    if active_day != day_utc or admission_status != "ADMIT" or rollover_status == "ROLLOVER_WITHHELD":
        return "NO_ACTIVE_PAPER_SESSION"
    return ""


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


def _consistency_has_reason(consistency_result: Any, reason_code: str) -> bool:
    wanted = str(reason_code).strip().upper()
    if not wanted:
        return False
    for code in consistency_result.blocking_reason_codes:
        if str(code).strip().upper() == wanted:
            return True
    for issue in consistency_result.issues:
        if str(issue.reason_code).strip().upper() == wanted:
            return True
    return False


def _refresh_session_authority_status(truth_root: Path) -> None:
    payload = build_session_authority_status_payload_v1(
        truth_root=truth_root,
        environment="PAPER",
        now=datetime.now(timezone.utc),
    )
    write_session_authority_status_v1(truth_root=truth_root, payload=payload)


def _day_authority_has_manifest_required_inputs_v1(payload: Dict[str, Any]) -> bool:
    input_status = payload.get("input_status")
    if not isinstance(input_status, dict):
        return False
    required_inputs = {
        "correlation_envelope_gate_v1",
        "replay_certification_gate_v1",
        "authorization_gate_verdict_v1",
        "global_kill_switch_state_v1",
        "paper_session_authority_v1",
    }
    for logical_name in required_inputs:
        row = input_status.get(logical_name)
        if not isinstance(row, dict):
            return False
        if str(row.get("required_or_diagnostic") or "").strip().lower() != "required":
            return False
        if str(row.get("readiness_role") or "").strip().lower() != "authority_input":
            return False
        if str(row.get("status") or "").strip().upper() != "PASS":
            return False
    return True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_c2_daily_operator_gate_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD (UTC)")
    args = ap.parse_args(argv)

    day = _parse_day_utc(args.day_utc)

    boundary_path = resolve_submit_boundary_status_path(truth_root=TRUTH, day_utc=day)
    ledger_path = resolve_paper_session_ledger_path(truth_root=TRUTH, day_utc=day)
    control_path = resolve_paper_day_control_plane_path(truth_root=TRUTH, day_utc=day)
    day_authority_path = resolve_paper_trading_day_authority_path(truth_root=TRUTH, day_utc=day)

    session_day_blocker = ""
    consistency_result = evaluate_next_day_readiness_consistency_gate_v1(truth_root=TRUTH, day_utc=day)

    # On non-trading/no-active-session days, stale session-authority status is usually an
    # ordering artifact. Refresh it once before enforcing consistency.
    if (
        session_day_blocker in NON_TRADING_SESSION_BLOCKERS
        and _consistency_has_reason(consistency_result, SESSION_AUTHORITY_STALE)
    ):
        _refresh_session_authority_status(TRUTH)
        consistency_result = evaluate_next_day_readiness_consistency_gate_v1(truth_root=TRUTH, day_utc=day)

    reasons: List[str] = []
    notes: List[str] = []
    day_authority_payload: Dict[str, Any] = {}
    day_authority_state = ""
    day_authority_can_submit = False
    day_authority_manifest_backed = False

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

    try:
        day_authority_ref = read_validated_surface_v1(
            path=day_authority_path,
            schema_relpath=PAPER_TRADING_DAY_AUTHORITY_SCHEMA,
        )
        day_authority_payload = dict(day_authority_ref.payload)
        day_authority_state = str(day_authority_payload.get("state") or "").strip().upper()
        day_authority_can_submit = bool(day_authority_payload.get("can_submit_paper_orders") is True)
        day_authority_manifest_backed = _day_authority_has_manifest_required_inputs_v1(day_authority_payload)
        session_day_blocker = str(day_authority_payload.get("canonical_blocker") or "").strip()
        if not session_day_blocker:
            reason_codes = day_authority_payload.get("reason_codes")
            if isinstance(reason_codes, list):
                for code in reason_codes:
                    text = str(code or "").strip()
                    if text:
                        session_day_blocker = text
                        break
    except Exception:
        day_authority_payload = {}
        day_authority_state = ""
        day_authority_can_submit = False
        day_authority_manifest_backed = False
        session_day_blocker = _session_day_blocker(day_utc=day, truth_root=TRUTH)
        reasons.append("MISSING_PAPER_TRADING_DAY_AUTHORITY_V1")

    day_authority_present = bool(day_authority_payload)
    if (not day_authority_present or not day_authority_manifest_backed) and consistency_result.status != CONSISTENCY_GATE_STATUS_PASS:
        reasons.extend(str(code).strip() for code in consistency_result.blocking_reason_codes if str(code).strip())

    canonical_ready = (
        day_authority_state == "OPEN_READY"
        and day_authority_can_submit
        and (
            day_authority_manifest_backed
            or consistency_result.status == CONSISTENCY_GATE_STATUS_PASS
        )
    )

    if session_day_blocker:
        deduped_reasons = [session_day_blocker]
        status = "FAIL"
        notes.append(f"session_day_blocker={session_day_blocker}")
        notes.append("canonical_day_open_projection=WITHHELD")
    else:
        if (not day_authority_present or not day_authority_manifest_backed) and not canonical_ready and boundary_payload and ledger_payload and control_payload:
            reasons.extend(_reason_codes_from_surfaces(boundary_payload=boundary_payload, ledger_payload=ledger_payload, control_payload=control_payload))

        deduped_reasons = []
        seen: set[str] = set()
        for reason in reasons:
            text = str(reason).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            deduped_reasons.append(text)

        status = "PASS" if canonical_ready and not deduped_reasons else "FAIL"

    notes.append(f"consistency_gate_status={consistency_result.status}")
    if day_authority_manifest_backed:
        notes.append("legacy_consistency_gate_projection=DIAGNOSTIC_ONLY")
    elif day_authority_present:
        notes.append("legacy_consistency_gate_projection=ENFORCED_UNTIL_MANIFEST_BACKED_DAY_AUTHORITY")
    notes.append(f"paper_trading_day_authority_state={day_authority_state or 'MISSING'}")
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
            "paper_trading_day_authority_path": str(day_authority_path),
        },
        "status": status,
        "reason_codes": deduped_reasons,
        "notes": notes,
        "session_day_blocker": session_day_blocker,
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
            "paper_trading_day_authority_state": day_authority_state,
            "paper_trading_day_authority_can_submit_paper_orders": day_authority_can_submit,
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
