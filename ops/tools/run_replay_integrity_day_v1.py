#!/usr/bin/env python3
"""
run_replay_integrity_day_v1.py

Bundle Y: Deterministic Replay & Audit Sealing (v1)

Writes immutable truth:
- constellation_2/runtime/truth/reports/replay_integrity_v1/<DAY>/replay_integrity.v1.json

Deterministic properties:
- produced_utc = <DAY>T00:00:00Z
- canonical JSON bytes via phaseD canonicalizer
- directory hashing uses sorted (path, sha256(file)) pairs

Modes:
- WRITE (default): compute + write immutable report (SKIP_IDENTICAL allowed)
- CHECK: recompute and compare against existing report; fail-closed on mismatch
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
from typing import Any, Dict, List, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()

DEFAULT_TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/replay_integrity.v1.schema.json"


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


def _canonical_report_bytes(obj: Dict[str, Any]) -> bytes:
    # ensure newline termination
    b = canonical_json_bytes_v1(obj)
    if not b.endswith(b"\n"):
        b += b"\n"
    return b


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _hash_dir_listing(root: Path) -> str:
    """
    Deterministic directory hash:
    sha256( canonical_json_bytes_v1([{"rel": "...", "sha256": "..."}...]) )
    - Includes only files (not dirs)
    - Includes all files under root (recursive)
    - Sorted by rel path (posix)
    """
    if not root.exists() or not root.is_dir():
        return _sha256_bytes(b"")
    rows: List[Dict[str, str]] = []
    for p in sorted([x for x in root.rglob("*") if x.is_file()], key=lambda x: str(x.relative_to(root)).replace("\\", "/")):
        rel = str(p.relative_to(root)).replace("\\", "/")
        rows.append({"rel": rel, "sha256": _sha256_file(p)})
    b = canonical_json_bytes_v1(rows)
    return _sha256_bytes(b)


def _add_input(inputs: List[Dict[str, Any]], t: str, p: Path, is_dir: bool) -> None:
    if is_dir:
        present = p.exists() and p.is_dir()
        sha = _hash_dir_listing(p) if present else _sha256_bytes(b"")
    else:
        present = p.exists() and p.is_file()
        sha = _sha256_file(p) if present else _sha256_bytes(b"")
    inputs.append({"type": t, "path": str(p), "sha256": sha, "present": bool(present)})


def _compute_replay_hash(day: str, inputs: List[Dict[str, Any]]) -> str:
    # Sort deterministically by type then path
    rows = sorted(
        [{"type": x["type"], "path": x["path"], "sha256": x["sha256"], "present": bool(x["present"])} for x in inputs],
        key=lambda r: (r["type"], r["path"]),
    )
    payload = {"day_utc": day, "inputs": rows}
    return _sha256_bytes(canonical_json_bytes_v1(payload))


def _load_existing_report(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"REPLAY_REPORT_NOT_OBJECT: {path}")
    return obj


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_replay_integrity_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--mode", default="WRITE", choices=["WRITE", "CHECK"])
    ap.add_argument("--truth_root", default=str(DEFAULT_TRUTH), help="Override truth root (for tamper-check runs)")
    args = ap.parse_args()

    day = _parse_day_utc(args.day_utc)
    mode = str(args.mode).strip().upper()
    truth = Path(str(args.truth_root)).resolve()

    produced_utc = f"{day}T00:00:00Z"

    # Canonical inputs (v1): only include surfaces we can prove exist in this repo snapshot.
    inputs: List[Dict[str, Any]] = []

    # Intents
    _add_input(inputs, "intents_day_dir", truth / "intents_v1" / "snapshots" / day, is_dir=True)
    _add_input(inputs, "intents_day_rollup_v1", truth / "intents_v1" / "day_rollup" / day / "intents_day_rollup.v1.json", is_dir=False)

    # Preflight / OMS / Allocation
    _add_input(inputs, "phaseC_preflight_day_dir", truth / "phaseC_preflight_v1" / day, is_dir=True)
    _add_input(inputs, "oms_day_dir", truth / "oms_decisions_v1" / "decisions" / day, is_dir=True)
    _add_input(inputs, "allocation_day_dir", truth / "allocation_v1" / "summary" / day, is_dir=True)

    # Execution evidence / fills
    _add_input(inputs, "exec_evidence_submissions_day_dir", truth / "execution_evidence_v1" / "submissions" / day, is_dir=True)
    _add_input(inputs, "submission_index_v1", truth / "execution_evidence_v1" / "submission_index" / day / "submission_index.v1.json", is_dir=False)
    _add_input(inputs, "fill_ledger_day_dir", truth / "fill_ledger_v1" / day, is_dir=True)

    # Positions / cash / accounting
    _add_input(inputs, "positions_snapshot_v2", truth / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json", is_dir=False)
    _add_input(inputs, "cash_ledger_snapshot_v1", truth / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json", is_dir=False)
    _add_input(inputs, "accounting_nav_v1", truth / "accounting_v1" / "nav" / day / "nav.json", is_dir=False)

    # Reports: broker reconciliation v1 + pipeline manifest v1 (present in your orchestrator)
    _add_input(inputs, "broker_reconciliation_v1", truth / "reports" / "broker_reconciliation_v1" / day / "broker_reconciliation.v1.json", is_dir=False)
    _add_input(inputs, "pipeline_manifest_v1", truth / "reports" / "pipeline_manifest_v1" / day / "pipeline_manifest.v1.json", is_dir=False)

    # Deterministic replay hash
    replay_hash = _compute_replay_hash(day, inputs)

    out_dir = (truth / "reports" / "replay_integrity_v1" / day).resolve()
    out_path = (out_dir / "replay_integrity.v1.json").resolve()

    mismatch_diff = {"missing_types": [], "sha_mismatches": []}
    reason_codes: List[str] = []
    status = "OK"

    expected_hash = None
    pass_check = True

    if mode == "CHECK":
        if not out_path.exists():
            status = "FAIL"
            pass_check = False
            reason_codes.append("MISSING_EXISTING_REPLAY_INTEGRITY_REPORT")
        else:
            existing = _load_existing_report(out_path)
            expected_hash = existing.get("replay_hash")
            if not isinstance(expected_hash, str) or len(expected_hash) != 64:
                status = "FAIL"
                pass_check = False
                reason_codes.append("EXISTING_REPLAY_HASH_INVALID")
                expected_hash = None
            else:
                if expected_hash != replay_hash:
                    status = "FAIL"
                    pass_check = False
                    reason_codes.append("REPLAY_HASH_MISMATCH")
                    ...
                else:
                    pass_check = True

                    # Build diff: compare expected input_hash_set to observed
                    exp_set = existing.get("input_hash_set", [])
                    exp_map: Dict[Tuple[str, str], str] = {}
                    if isinstance(exp_set, list):
                        for row in exp_set:
                            if isinstance(row, dict):
                                t = str(row.get("type") or "")
                                p = str(row.get("path") or "")
                                s = str(row.get("sha256") or "")
                                if t and p and len(s) == 64:
                                    exp_map[(t, p)] = s
                    obs_map = {(x["type"], x["path"]): x["sha256"] for x in inputs}

                    # Missing types: anything expected but absent now
                    missing = sorted([k[0] for k in exp_map.keys() if k not in obs_map])
                    mismatch_diff["missing_types"] = sorted(list(dict.fromkeys(missing)))

                    # SHA mismatches
                    mismatches = []
                    for k, exp_sha in exp_map.items():
                        if k in obs_map:
                            obs_sha = obs_map[k]
                            if exp_sha != obs_sha:
                                mismatches.append(
                                    {"type": k[0], "path": k[1], "expected_sha256": exp_sha, "observed_sha256": obs_sha}
                                )
                    mismatch_diff["sha_mismatches"] = mismatches

    # Fail closed if any required input is missing (institutional default for audit sealing).
    # We only mark as blocking missing for CHECK/WRITE equally; status FAIL reflects missing.
    for row in inputs:
        if not bool(row["present"]):
            status = "FAIL"
            reason_codes.append(f"MISSING_INPUT:{row['type']}")

    reason_codes = sorted(list(dict.fromkeys(reason_codes)))

    report: Dict[str, Any] = {
        "schema_id": "C2_REPLAY_INTEGRITY_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_replay_integrity_day_v1.py"},
        "status": status,
        "replay_hash": replay_hash,
        "input_hash_set": sorted(
            [{"type": x["type"], "path": x["path"], "sha256": x["sha256"], "present": bool(x["present"])} for x in inputs],
            key=lambda r: (r["type"], r["path"]),
        ),
        "reproducibility_check": {
            "mode": mode,
            "expected_replay_hash": expected_hash,
            "observed_replay_hash": replay_hash,
            "pass": bool(pass_check),
        },
        "mismatch_diff": mismatch_diff,
        "reason_codes": reason_codes,
        "report_sha256": None,
    }
    # self-hash excluding report_sha256
    tmp = dict(report)
    tmp["report_sha256"] = None
    report["report_sha256"] = _sha256_bytes(_canonical_report_bytes(tmp))

    validate_against_repo_schema_v1(report, REPO_ROOT, SCHEMA_RELPATH)

    if mode == "WRITE":
        try:
            wr = write_file_immutable_v1(path=out_path, data=_canonical_report_bytes(report), create_dirs=True)
        except ImmutableWriteError as e:
            raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e
        print(f"OK: REPLAY_INTEGRITY_V1_WRITTEN day_utc={day} status={status} path={wr.path} sha256={wr.sha256} action={wr.action}")
        return 0 if status == "OK" else 2

    # CHECK mode: do not write; return 0 on pass, non-zero on fail
    print(f"OK: REPLAY_INTEGRITY_V1_CHECK day_utc={day} status={status} replay_hash={replay_hash} pass={report['reproducibility_check']['pass']}")
    return 0 if report["reproducibility_check"]["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
