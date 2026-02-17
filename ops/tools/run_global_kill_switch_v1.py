#!/usr/bin/env python3
"""
run_global_kill_switch_v1.py

Bundled C: global_kill_switch_state.v1.json writer (immutable truth artifact).

Deterministic + audit-grade.
Fail-closed default: if required inputs are missing or invalid => state=ACTIVE.

Writes:
  constellation_2/runtime/truth/risk_v1/kill_switch_v1/<DAY>/global_kill_switch_state.v1.json

Validates schema:
  governance/04_DATA/SCHEMAS/C2/RISK/global_kill_switch_state.v1.schema.json

Run:
  python3 ops/tools/run_global_kill_switch_v1.py --day_utc YYYY-MM-DD
"""

from __future__ import annotations

# --- Import bootstrap (audit-grade, deterministic, fail-closed) ---
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
# .../ops/tools/run_global_kill_switch_v1.py -> repo root is parents[2]
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

# Fail-closed: verify expected repo structure exists
if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/global_kill_switch_state.v1.schema.json"
OUT_ROOT = (TRUTH / "risk_v1" / "kill_switch_v1").resolve()

PATH_OPERATOR_VERDICT = (TRUTH / "reports" / "operator_gate_verdict_v1").resolve()
PATH_CAPITAL_ENV = (TRUTH / "reports" / "capital_risk_envelope_v1").resolve()
PATH_RECON_V2 = (TRUTH / "reports" / "reconciliation_report_v2").resolve()


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


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
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(p)}")
    return obj


def _canonical_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _compute_self_sha(obj: Dict[str, Any], field: str) -> str:
    o2 = dict(obj)
    o2[field] = None
    return _sha256_bytes(_canonical_bytes(o2))


def _load_inputs(day: str) -> Tuple[List[Dict[str, str]], List[str], Dict[str, Any]]:
    """
    Returns:
      (input_manifest, reason_codes, decisions)
    decisions contains parsed statuses to aid explainability.
    """
    input_manifest: List[Dict[str, str]] = []
    rc: List[str] = []
    decisions: Dict[str, Any] = {}

    verdict_path = (PATH_OPERATOR_VERDICT / day / "operator_gate_verdict.v1.json").resolve()
    cap_path = (PATH_CAPITAL_ENV / day / "capital_risk_envelope.v1.json").resolve()
    recon_path = (PATH_RECON_V2 / day / "reconciliation_report.v2.json").resolve()

    def add(t: str, p: Path) -> None:
        if p.exists() and p.is_file():
            input_manifest.append({"type": t, "path": str(p), "sha256": _sha256_file(p)})
        else:
            input_manifest.append({"type": f"{t}_missing", "path": str(p), "sha256": _sha256_bytes(b"")})
            rc.append("C2_KILL_SWITCH_DEFAULT_ACTIVE_MISSING_INPUTS")

    add("operator_gate_verdict_v1", verdict_path)
    add("capital_risk_envelope_v1", cap_path)
    add("reconciliation_report_v2", recon_path)

    if verdict_path.exists():
        try:
            v = _read_json_obj(verdict_path)
            decisions["operator_gate_ready"] = bool(v.get("ready"))
        except Exception as e:  # noqa: BLE001
            rc.append("C2_KILL_SWITCH_INPUT_SCHEMA_INVALID")
            decisions["operator_gate_parse_error"] = str(e)

    if cap_path.exists():
        try:
            ce = _read_json_obj(cap_path)
            decisions["capital_risk_envelope_status"] = str(ce.get("status") or "")
        except Exception as e:  # noqa: BLE001
            rc.append("C2_KILL_SWITCH_INPUT_SCHEMA_INVALID")
            decisions["capital_risk_envelope_parse_error"] = str(e)

    if recon_path.exists():
        try:
            rr = _read_json_obj(recon_path)
            decisions["recon_status"] = str(rr.get("status") or "")
            decisions["recon_verdict"] = str(rr.get("verdict") or "")
        except Exception as e:  # noqa: BLE001
            rc.append("C2_KILL_SWITCH_INPUT_SCHEMA_INVALID")
            decisions["recon_parse_error"] = str(e)

    return (input_manifest, rc, decisions)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_global_kill_switch_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)
    produced_utc = _now_utc_iso()

    input_manifest, reason_codes, decisions = _load_inputs(day)

    state = "ACTIVE" if ("C2_KILL_SWITCH_DEFAULT_ACTIVE_MISSING_INPUTS" in reason_codes or "C2_KILL_SWITCH_INPUT_SCHEMA_INVALID" in reason_codes) else "INACTIVE"

    if state == "INACTIVE":
        if decisions.get("operator_gate_ready") is not True:
            state = "ACTIVE"
            reason_codes.append("C2_KILL_SWITCH_ACTIVE")

        st = str(decisions.get("capital_risk_envelope_status") or "").strip().upper()
        if st != "PASS":
            state = "ACTIVE"
            reason_codes.append("C2_KILL_SWITCH_ACTIVE")

        recon_v = str(decisions.get("recon_verdict") or "").strip().upper()
        recon_s = str(decisions.get("recon_status") or "").strip().upper()
        if not (recon_v == "PASS" or recon_s == "OK"):
            state = "ACTIVE"
            reason_codes.append("C2_KILL_SWITCH_ACTIVE")

    allow_entries = (state == "INACTIVE")
    allow_exits = True
    forced_mode = "NORMAL" if state == "INACTIVE" else "FLATTEN_ONLY"

    reason_codes = sorted(list(dict.fromkeys(reason_codes)))

    payload: Dict[str, Any] = {
        "schema_id": "global_kill_switch_state",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_global_kill_switch_v1.py", "git_sha": _git_sha()},
        "state": state,
        "allow_entries": bool(allow_entries),
        "allow_exits": bool(allow_exits),
        "forced_mode": forced_mode,
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "state_sha256": None,
    }
    payload["state_sha256"] = _compute_self_sha(payload, "state_sha256")

    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH)

    out_path = (OUT_ROOT / day / "global_kill_switch_state.v1.json").resolve()
    try:
        wr = write_file_immutable_v1(path=out_path, data=_canonical_bytes(payload), create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e

    print(f"OK: GLOBAL_KILL_SWITCH_STATE_WRITTEN day_utc={day} state={state} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return 0 if state == "INACTIVE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
