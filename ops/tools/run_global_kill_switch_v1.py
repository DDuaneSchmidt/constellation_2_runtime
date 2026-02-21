#!/usr/bin/env python3
"""
run_global_kill_switch_v1.py

Bundled C: global_kill_switch_state.v1.json writer (immutable truth artifact).

Deterministic + audit-grade.
Fail-closed default: if required inputs are missing or invalid => state=ACTIVE.

BATCH-1 CHANGE (Single Final Verdict Consumption):
- Kill switch consumes ONLY gate_stack_verdict_v1 as the decision authority.
- Legacy per-surface inputs (operator_gate_verdict / capital_risk_envelope / reconciliation_report)
  are NOT consumed for state decisions.

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
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

import argparse
import hashlib
import json
import subprocess
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/global_kill_switch_state.v1.schema.json"
OUT_ROOT = (TRUTH / "risk_v1" / "kill_switch_v1").resolve()

# Single Final Verdict Consumption (authoritative input)
PATH_GATE_STACK_VERDICT_V1 = (TRUTH / "reports" / "gate_stack_verdict_v1").resolve()


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


def _gate_stack_all_required_pass(gs: Dict[str, Any]) -> bool:
    gates = gs.get("gates", [])
    if not isinstance(gates, list):
        return False
    for g in gates:
        if not isinstance(g, dict):
            return False
        required = bool(g.get("required"))
        status = str(g.get("status") or "").strip().upper()
        if required and status != "PASS":
            return False
    return True


def _load_inputs(day: str) -> Tuple[List[Dict[str, str]], List[str], Dict[str, Any]]:
    input_manifest: List[Dict[str, str]] = []
    rc: List[str] = []
    decisions: Dict[str, Any] = {}

    gs_type = "gate_stack_verdict_v1_missing"
    gs_path = (PATH_GATE_STACK_VERDICT_V1 / day / "gate_stack_verdict.v1.json").resolve()

    if gs_path.exists() and gs_path.is_file():
        input_manifest.append({"type": "gate_stack_verdict_v1", "path": str(gs_path), "sha256": _sha256_file(gs_path)})
        try:
            gs = _read_json_obj(gs_path)
            decisions["gate_stack_status"] = str(gs.get("status") or "")
            decisions["gate_stack_required_all_pass"] = bool(_gate_stack_all_required_pass(gs))
        except Exception as e:  # noqa: BLE001
            rc.append("C2_KILL_SWITCH_INPUT_SCHEMA_INVALID")
            decisions["gate_stack_parse_error"] = str(e)
    else:
        input_manifest.append(
            {"type": gs_type, "path": str(gs_path), "sha256": _sha256_bytes(b"")}
        )
        rc.append("C2_KILL_SWITCH_DEFAULT_ACTIVE_MISSING_INPUTS")

    return (input_manifest, rc, decisions)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_global_kill_switch_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)

    # Deterministic produced_utc for replay (schema requires non-empty string).
    produced_utc = f"{day}T00:00:00Z"

    input_manifest, reason_codes, decisions = _load_inputs(day)

    # PAPER bootstrap policy:
    # - If required inputs are missing/invalid BUT there are no submissions yet, allow entries (paper trading bootstrap).
    # - Once submissions exist, enforce strict fail-closed rules.
    subs_dir = (TRUTH / "execution_evidence_v1" / "submissions" / day).resolve()
    submissions_present = False
    if subs_dir.exists() and subs_dir.is_dir():
        for p in subs_dir.iterdir():
            if p.is_dir():
                submissions_present = True
                break

    missing_or_invalid = (
        ("C2_KILL_SWITCH_DEFAULT_ACTIVE_MISSING_INPUTS" in reason_codes)
        or ("C2_KILL_SWITCH_INPUT_SCHEMA_INVALID" in reason_codes)
    )

    if missing_or_invalid and (not submissions_present):
        state = "INACTIVE"
        reason_codes.append("C2_PAPER_BOOTSTRAP_ALLOW_ENTRIES_NO_SUBMISSIONS_YET")
    else:
        state = "ACTIVE" if missing_or_invalid else "INACTIVE"

    # Single Final Verdict Consumption:
    # INACTIVE only if gate_stack_verdict exists AND status==PASS AND all REQUIRED gates are PASS.
    if state == "INACTIVE":
        st = str(decisions.get("gate_stack_status") or "").strip().upper()
        all_required_pass = bool(decisions.get("gate_stack_required_all_pass") is True)
        if not (st == "PASS" and all_required_pass):
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

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "global_kill_switch_state.v1.json").resolve()

    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        write_file_immutable_v1(out_path, _canonical_bytes(payload))
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL_IMMUTABLE_WRITE: {e}") from e

    print(_canonical_bytes(payload).decode("utf-8"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
