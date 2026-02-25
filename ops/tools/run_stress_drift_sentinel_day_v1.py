#!/usr/bin/env python3
"""
run_stress_drift_sentinel_day_v1.py

Bundle Z (A): Stress & Drift Sentinel v1 (monitoring-only)
- Deterministic produced_utc = <DAY>T00:00:00Z
- Immutable output under truth/monitoring_v2/stress_drift_sentinel_v1/<DAY>/
- Does NOT block trading directly; emits escalation_recommended boolean.
- Systemic Risk Gate consumes this and enforces operator override when escalation is recommended.

Inputs (truth):
- monitoring_v1/engine_daily_returns_v1/<DAY>/engine_daily_returns.v1.json (optional)
- monitoring_v1/engine_correlation_matrix/<DAY>/engine_correlation_matrix.v1.json (optional)
- reports/broker_reconciliation_v1/<DAY>/broker_reconciliation.v1.json (optional)

Policy (v1, conservative bootstrap):
- If broker_reconciliation_v1 status != PASS -> slippage_ok = False
- If correlation matrix max_pairwise >= threshold (0.75) AND matrix size > 1 -> correlation stress -> stress_ok = False
- Drift is OK unless engine_daily_returns status == ACTIVE with anomalies (v1: no anomalies computed; informational only)
- escalation_recommended = (not stress_ok) or (not slippage_ok) or (not drift_ok)

Rerun-safety (automation requirement):
- If the day-keyed sentinel already exists, treat it as authoritative for that day.
- DO NOT attempt rewrite (prevents immutable overwrite failure when git_sha changes or inputs differ across reruns).
"""

from __future__ import annotations

import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

import argparse
import hashlib
import json
import subprocess
from decimal import Decimal
from typing import Any, Dict, List, Tuple, Optional

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/stress_drift_sentinel.v1.schema.json"
OUT_ROOT = (TRUTH / "monitoring_v2" / "stress_drift_sentinel_v1").resolve()

PATH_DAILY_RET = (TRUTH / "monitoring_v1" / "engine_daily_returns_v1").resolve()
PATH_CORR = (TRUTH / "monitoring_v1" / "engine_correlation_matrix").resolve()
PATH_BROKER_RECON_V1 = (TRUTH / "reports" / "broker_reconciliation_v1").resolve()

CORR_THRESHOLD = Decimal("0.75")

DAY0_RC_BROKER_RECON_ALLOWED = "DAY0_BOOTSTRAP_BROKER_RECON_MISSING_ALLOWED"


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {p}")
    return obj


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _canonical_bytes(obj: Dict[str, Any]) -> bytes:
    b = canonical_json_bytes_v1(obj)
    if not b.endswith(b"\n"):
        b += b"\n"
    return b


def _self_sha(obj: Dict[str, Any], field: str) -> str:
    tmp = dict(obj)
    tmp[field] = None
    return _sha256_bytes(_canonical_bytes(tmp))


def _bootstrap_window_true(day_utc: str) -> bool:
    """
    Day-0 Bootstrap Window iff:
      TRUTH/execution_evidence_v1/submissions/<DAY>/ is missing OR contains zero submission dirs.
    """
    root = (TRUTH / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    if (not root.exists()) or (not root.is_dir()):
        return True
    try:
        for p in root.iterdir():
            if p.is_dir():
                return False
    except Exception:
        # Fail-closed: if we cannot enumerate, treat as NOT bootstrap.
        return False
    return True


def _return_if_existing_report(out_path: Path, expected_day_utc: str) -> Optional[int]:
    """
    Immutable rerun safety (automation requirement):
    - If the day-keyed artifact already exists, treat it as authoritative for that day.
    - DO NOT attempt rewrite.

    Returns:
      - None if no existing file (caller should compute/write)
      - 0 if existing status OK
      - 2 if existing status FAIL
    """
    if not out_path.exists():
        return None

    existing_sha = _sha256_file(out_path)
    existing = _read_json_obj(out_path)

    schema_id = str(existing.get("schema_id") or "").strip()
    schema_version = existing.get("schema_version")
    day_utc = str(existing.get("day_utc") or "").strip()
    status = str(existing.get("status") or "").strip().upper()

    if schema_id != "C2_STRESS_DRIFT_SENTINEL_V1":
        raise SystemExit(f"FAIL: EXISTING_SENTINEL_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
    if schema_version != 1:
        raise SystemExit(
            f"FAIL: EXISTING_SENTINEL_SCHEMA_VERSION_MISMATCH: schema_version={schema_version!r} path={out_path}"
        )
    if day_utc != expected_day_utc:
        raise SystemExit(
            f"FAIL: EXISTING_SENTINEL_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}"
        )
    if status not in ("OK", "FAIL"):
        raise SystemExit(f"FAIL: EXISTING_SENTINEL_STATUS_INVALID: status={status!r} path={out_path}")

    print(
        f"OK: STRESS_DRIFT_SENTINEL_V1_WRITTEN day_utc={expected_day_utc} status={status} path={out_path} sha256={existing_sha} action=EXISTS"
    )
    return 0 if status == "OK" else 2


def _load_daily_returns(day: str) -> Tuple[str, List[str], List[Dict[str, str]]]:
    notes: List[str] = []
    manifest: List[Dict[str, str]] = []
    p = (PATH_DAILY_RET / day / "engine_daily_returns.v1.json").resolve()
    if not p.exists():
        manifest.append({"type": "engine_daily_returns_v1_missing", "path": str(p), "sha256": _sha256_bytes(b"")})
        return ("NOT_AVAILABLE", ["MISSING_ENGINE_DAILY_RETURNS_V1"], manifest)
    manifest.append({"type": "engine_daily_returns_v1", "path": str(p), "sha256": _sha256_file(p)})
    try:
        o = _read_json_obj(p)
    except Exception:
        return ("NOT_AVAILABLE", ["ENGINE_DAILY_RETURNS_PARSE_ERROR"], manifest)
    st = str(o.get("status") or "NOT_AVAILABLE").strip()
    return (st, notes, manifest)


def _load_corr(day: str) -> Tuple[str, str, List[str], List[Dict[str, str]]]:
    notes: List[str] = []
    manifest: List[Dict[str, str]] = []
    p = (PATH_CORR / day / "engine_correlation_matrix.v1.json").resolve()
    if not p.exists():
        manifest.append({"type": "engine_correlation_matrix_v1_missing", "path": str(p), "sha256": _sha256_bytes(b"")})
        return ("MISSING", "0.000000", ["MISSING_ENGINE_CORRELATION_MATRIX_V1"], manifest)
    manifest.append({"type": "engine_correlation_matrix_v1", "path": str(p), "sha256": _sha256_file(p)})
    try:
        o = _read_json_obj(p)
    except Exception:
        return ("FAIL", "0.000000", ["ENGINE_CORR_PARSE_ERROR"], manifest)
    status = str(o.get("status") or "").strip().upper() or "UNKNOWN"
    max_pairwise = "0.000000"
    try:
        mat = o.get("matrix") or {}
        corr = mat.get("corr") or []
        engine_ids = mat.get("engine_ids") or []
        n = len(engine_ids) if isinstance(engine_ids, list) else 0
        max_abs = Decimal("0")
        if isinstance(corr, list) and n > 1:
            for i in range(n):
                for j in range(i + 1, n):
                    c = Decimal(str(corr[i][j]))
                    if abs(c) > abs(max_abs):
                        max_abs = c
        max_pairwise = f"{max_abs:.6f}"
    except Exception:
        notes.append("CORR_MAX_PAIRWISE_PARSE_ERROR")
    return (status, max_pairwise, notes, manifest)


def _load_broker_recon(day: str) -> Tuple[str, str, List[str], List[Dict[str, str]]]:
    notes: List[str] = []
    manifest: List[Dict[str, str]] = []
    p = (PATH_BROKER_RECON_V1 / day / "broker_reconciliation.v1.json").resolve()
    if not p.exists():
        manifest.append({"type": "broker_reconciliation_v1_missing", "path": str(p), "sha256": _sha256_bytes(b"")})
        return ("MISSING", "0", ["MISSING_BROKER_RECONCILIATION_V1"], manifest)
    manifest.append({"type": "broker_reconciliation_v1", "path": str(p), "sha256": _sha256_file(p)})
    try:
        o = _read_json_obj(p)
    except Exception:
        return ("FAIL", "0", ["BROKER_RECON_PARSE_ERROR"], manifest)
    status = str(o.get("status") or "").strip().upper() or "UNKNOWN"
    cash_diff = str(o.get("cash_diff") or "0").strip() or "0"
    return (status, cash_diff, notes, manifest)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_stress_drift_sentinel_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"

    out_path = (OUT_ROOT / day / "stress_drift_sentinel.v1.json").resolve()
    existing_rc = _return_if_existing_report(out_path=out_path, expected_day_utc=day)
    if existing_rc is not None:
        return int(existing_rc)

    bootstrap = _bootstrap_window_true(day)

    input_manifest: List[Dict[str, str]] = []
    reason_codes: List[str] = []

    # Drift (v1: informational)
    daily_status, daily_notes, man_dr = _load_daily_returns(day)
    input_manifest.extend(man_dr)

    drift_ok = True
    drift_notes: List[str] = []
    if isinstance(daily_notes, list):
        drift_notes.extend([str(x) for x in daily_notes if str(x).strip()])

    # Slippage proxy via broker reconciliation v1
    broker_status, cash_diff, broker_notes, man_br = _load_broker_recon(day)
    input_manifest.extend(man_br)

    # Day-0 bootstrap exception: missing broker reconciliation is allowed when there are no submissions yet.
    if bootstrap and broker_status == "MISSING":
        slippage_ok = True
        slip_notes: List[str] = [
            "DAY0_BOOTSTRAP: broker reconciliation v1 missing allowed (no broker statement yet; no submissions).",
        ]
        reason_codes.append(DAY0_RC_BROKER_RECON_ALLOWED)
    else:
        slippage_ok = (broker_status == "PASS")
        slip_notes = []

    if not slippage_ok:
        reason_codes.append("Z_SLIPPAGE_OR_RECONCILIATION_NOT_PASS")
        slip_notes.append(f"broker_reconciliation_v1_status={broker_status}")
    if isinstance(broker_notes, list):
        slip_notes.extend([str(x) for x in broker_notes if str(x).strip()])

    # Correlation stress proxy
    corr_status, max_pairwise, corr_notes, man_c = _load_corr(day)
    input_manifest.extend(man_c)

    stress_ok = True
    corr_notes_out: List[str] = []
    if corr_status not in ("OK", "DEGRADED_INSUFFICIENT_HISTORY"):
        # BOOTSTRAP_ALLOW_CORR_UNKNOWN
        # During Day-0 bootstrap (no submissions yet), correlation status may be UNKNOWN due to legacy/minimal inputs.
        # Do not fail the sentinel solely for UNKNOWN/MISSING in bootstrap; still record reason codes for operator visibility.
        reason_codes.append("Z_CORRELATION_MATRIX_NOT_OK")
        corr_notes_out.append(f"engine_corr_status={corr_status}")
        if not (bootstrap and corr_status in ("UNKNOWN", "MISSING")):
            stress_ok = False
    try:
        mp = Decimal(max_pairwise)
        # Only meaningful when multi-engine; 1x1 implies max_pairwise=0.000000
        if mp >= CORR_THRESHOLD and mp != Decimal("0"):
            stress_ok = False
            reason_codes.append("Z_CORRELATION_THRESHOLD_BREACH")
            corr_notes_out.append(f"max_pairwise={max_pairwise} threshold=0.75")
    except Exception:
        # If we cannot parse max_pairwise, treat as degraded and recommend escalation.
        stress_ok = False
        reason_codes.append("Z_CORRELATION_MAX_PAIRWISE_PARSE_ERROR")
        corr_notes_out.append("max_pairwise_parse_error")

    if isinstance(corr_notes, list):
        corr_notes_out.extend([str(x) for x in corr_notes if str(x).strip()])

    escalation_recommended = (not stress_ok) or (not drift_ok) or (not slippage_ok)
    status = "OK" if not escalation_recommended else "FAIL"
    if escalation_recommended:
        reason_codes.append("Z_ESCALATION_RECOMMENDED")

    reason_codes = sorted(list(dict.fromkeys(reason_codes)))

    payload: Dict[str, Any] = {
        "schema_id": "C2_STRESS_DRIFT_SENTINEL_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_stress_drift_sentinel_day_v1.py"},
        "status": status,
        "stress_ok": bool(stress_ok),
        "drift_ok": bool(drift_ok),
        "slippage_ok": bool(slippage_ok),
        "escalation_recommended": bool(escalation_recommended),
        "metrics": {
            "drift": {"engine_daily_returns_status": str(daily_status), "notes": drift_notes or ["NONE"]},
            "slippage": {"broker_reconciliation_status": str(broker_status), "cash_diff": str(cash_diff), "notes": slip_notes or ["NONE"]},
            "correlation": {"engine_corr_status": str(corr_status), "max_pairwise": str(max_pairwise), "threshold_max_pairwise": "0.75", "notes": corr_notes_out or ["NONE"]},
        },
        "reason_codes": reason_codes,
        "input_manifest": input_manifest if input_manifest else [{"type": "truth_root", "path": str(TRUTH), "sha256": _sha256_bytes(b"")}],
        "sentinel_sha256": None,
    }
    payload["sentinel_sha256"] = _self_sha(payload, "sentinel_sha256")

    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)

    try:
        wr = write_file_immutable_v1(path=out_path, data=_canonical_bytes(payload), create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(f"OK: STRESS_DRIFT_SENTINEL_V1_WRITTEN day_utc={day} status={status} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return 0 if status == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
