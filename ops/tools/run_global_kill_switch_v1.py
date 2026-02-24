#!/usr/bin/env python3
"""
run_global_kill_switch_v1.py

Bundled C: global_kill_switch_state.v1.json writer (immutable truth artifact).

Deterministic + audit-grade.
Fail-closed default: if required inputs are missing or invalid => state=ACTIVE.

Decision authority:
- Kill switch consumes ONLY gate_stack_verdict_v1 as the decision authority when present.
- If verdict is missing/invalid, fail-closed EXCEPT for Day-0 bootstrap rule.

Day-0 bootstrap (best-practice, audit-grade):
- Allow entries only if:
  (a) required inputs are missing/invalid, AND
  (b) submissions are absent for the day, AND
  (c) broker_baseline_snapshot_v1 exists for the day (anchor).

Rerun-safety:
- If artifact exists, treat as authoritative (do not rewrite),
  EXCEPT for provably invalid bootstrap artifacts (self-heal quarantine).

Writes:
  constellation_2/runtime/truth/risk_v1/kill_switch_v1/<DAY>/global_kill_switch_state.v1.json
"""

from __future__ import annotations

# --- deterministic import bootstrap (required for systemd execution) ---
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
PATH_GATE_STACK_VERDICT_V1 = (TRUTH / "reports" / "gate_stack_verdict_v1").resolve()

# Day-0 baseline anchor
PATH_BASELINE_V1 = (TRUTH / "execution_evidence_v1" / "broker_baseline_snapshot_v1").resolve()

RC_BOOTSTRAP_ALLOW = "C2_DAY0_BOOTSTRAP_ALLOW_ENTRIES_BASELINE_OK_NO_SUBMISSIONS"
RC_MISSING_INPUTS = "C2_KILL_SWITCH_DEFAULT_ACTIVE_MISSING_INPUTS"
RC_INPUT_INVALID = "C2_KILL_SWITCH_INPUT_SCHEMA_INVALID"


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
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(p)}")
    return obj


def _canonical_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _compute_self_sha(obj: Dict[str, Any], field: str) -> str:
    o2 = dict(obj)
    o2[field] = None
    return _sha256_bytes(_canonical_bytes(o2))


def _bootstrap_invariant_ok(existing: Dict[str, Any]) -> bool:
    rcs = existing.get("reason_codes", [])
    if not isinstance(rcs, list):
        return True
    if RC_BOOTSTRAP_ALLOW not in [str(x) for x in rcs]:
        return True

    state = str(existing.get("state") or "").strip().upper()
    allow_entries = bool(existing.get("allow_entries") is True)
    forced_mode = str(existing.get("forced_mode") or "").strip().upper()
    return bool(state == "INACTIVE" and allow_entries and forced_mode == "NORMAL")


def _return_if_existing_report(out_path: Path, expected_day_utc: str) -> int | None:
    if not out_path.exists():
        return None

    existing_sha = _sha256_file(out_path)
    existing = _read_json_obj(out_path)

    schema_id = str(existing.get("schema_id") or "").strip()
    schema_version = str(existing.get("schema_version") or "").strip()
    day_utc = str(existing.get("day_utc") or "").strip()

    if schema_id != "global_kill_switch_state":
        raise SystemExit(f"FAIL: EXISTING_KILL_SWITCH_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
    if schema_version != "v1":
        raise SystemExit(f"FAIL: EXISTING_KILL_SWITCH_SCHEMA_VERSION_MISMATCH: schema_version={schema_version!r} path={out_path}")
    if day_utc != expected_day_utc:
        raise SystemExit(
            f"FAIL: EXISTING_KILL_SWITCH_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}"
        )

    state = str(existing.get("state") or "").strip().upper()
    if state == "":
        raise SystemExit(f"FAIL: EXISTING_KILL_SWITCH_STATE_MISSING: path={out_path}")

    if not _bootstrap_invariant_ok(existing):
        invalid_path = out_path.with_name(f"global_kill_switch_state.v1.json.INVALID_{existing_sha}.json")
        if invalid_path.exists():
            raise SystemExit(
                f"FAIL: INVALID_EXISTING_KILL_SWITCH_ALREADY_QUARANTINED: day_utc={expected_day_utc} existing_sha={existing_sha} "
                f"out_path={out_path} invalid_path={invalid_path}"
            )
        out_path.rename(invalid_path)
        print(
            f"WARN: QUARANTINED_INVALID_EXISTING_KILL_SWITCH day_utc={expected_day_utc} "
            f"old_path={out_path} quarantined_path={invalid_path} sha256={existing_sha}"
        )
        return None

    print(
        f"OK: GLOBAL_KILL_SWITCH_STATE_V1_WRITTEN day_utc={expected_day_utc} state={state} path={out_path} sha256={existing_sha} action=EXISTS"
    )
    return 0


def _gate_stack_all_required_pass(gs: Dict[str, Any]) -> bool:
    gates = gs.get("gates", [])
    if not isinstance(gates, list):
        return False
    for g in gates:
        if not isinstance(g, dict):
            return False
        required = bool(g.get("required"))
        status = str(g.get("status") or "").strip().upper()
        if required and status not in ("PASS", "OK"):
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
            rc.append(RC_INPUT_INVALID)
            decisions["gate_stack_parse_error"] = str(e)
    else:
        input_manifest.append({"type": gs_type, "path": str(gs_path), "sha256": _sha256_bytes(b"")})
        rc.append(RC_MISSING_INPUTS)

    return (input_manifest, rc, decisions)


def _submissions_present(day: str) -> bool:
    subs_dir = (TRUTH / "execution_evidence_v1" / "submissions" / day).resolve()
    if subs_dir.exists() and subs_dir.is_dir():
        for p in subs_dir.iterdir():
            if p.is_dir():
                return True
    return False


def _baseline_present(day: str) -> bool:
    p = (PATH_BASELINE_V1 / day / "broker_baseline_snapshot.v1.json").resolve()
    return bool(p.exists() and p.is_file())


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_global_kill_switch_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {day!r}")

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "global_kill_switch_state.v1.json").resolve()

    existing_rc = _return_if_existing_report(out_path=out_path, expected_day_utc=day)
    if existing_rc is not None:
        return int(existing_rc)

    produced_utc = f"{day}T00:00:00Z"

    input_manifest, reason_codes, decisions = _load_inputs(day)

    missing_or_invalid = (RC_MISSING_INPUTS in reason_codes) or (RC_INPUT_INVALID in reason_codes)
    subs_present = _submissions_present(day)
    base_present = _baseline_present(day)

    bootstrap_allow_entries = bool(missing_or_invalid and (not subs_present) and base_present)

    if bootstrap_allow_entries:
        state = "INACTIVE"
        reason_codes.append(RC_BOOTSTRAP_ALLOW)
    else:
        state = "ACTIVE" if missing_or_invalid else "INACTIVE"

    if (not bootstrap_allow_entries) and (state == "INACTIVE"):
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

    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        _ = write_file_immutable_v1(path=out_path, data=_canonical_bytes(payload), create_dirs=False)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL_IMMUTABLE_WRITE: {e}") from e

    print(_canonical_bytes(payload).decode("utf-8"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
