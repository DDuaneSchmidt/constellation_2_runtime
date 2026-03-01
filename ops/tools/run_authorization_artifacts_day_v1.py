#!/usr/bin/env python3
"""
run_authorization_artifacts_day_v1.py

Bundle A (A1): Per-intent Authorization Artifacts (day-scoped, deterministic, fail-closed).

Inputs:
- allocation_v1/capital_authority_allocation_v1/<DAY>/capital_authority_allocation.v1.json
- intents_v1/snapshots/<DAY>/*.exposure_intent.v1.json
- policy manifest (sha in input manifest only)

Outputs:
- constellation_2/runtime/truth/engine_activity_v1/authorization_v1/<DAY>/<INTENT_SHA>.authorization.v1.json

Note:
- Uses sha256(file bytes) of intent file as the stable intent_hash reference.
- decision_hash is sha256(canonical JSON of authorization block excluding decision_hash).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

NO_INTENTS_MARKER = lambda day: (INTENTS_DIR(day) / "no_intents_day.v1.json").resolve()
NO_INTENTS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/no_intents_day.v1.schema.json"

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

AUTHORITY_HEAD_PATH = (TRUTH / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()


def _require_authority_head_pass_authoritative(day: str) -> Dict[str, Any]:
    p = AUTHORITY_HEAD_PATH
    if not p.exists() or not p.is_file():
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_MISSING: {str(p)}")
    ah = _read_json_obj(p)
    schema_id = str(ah.get("schema_id") or "").strip()
    schema_ver = str(ah.get("schema_version") or "").strip()
    status = str(ah.get("status") or "").strip().upper()
    authoritative = bool(ah.get("authoritative") is True)
    day_utc = str(ah.get("day_utc") or "").strip()

    if schema_id != "c2_run_pointer_canonical_authority_head" or schema_ver != "v1":
        raise SystemExit("FAIL: AUTHORITY_HEAD_SCHEMA_MISMATCH")
    if day_utc != day:
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_DAY_MISMATCH head_day={day_utc!r} expected_day={day!r}")
    if status != "PASS":
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_NOT_PASS status={status!r}")
    if not authoritative:
        raise SystemExit("FAIL: AUTHORITY_HEAD_NOT_AUTHORITATIVE")
    return ah

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_hash_excluding_fields_v1, canonical_json_bytes_v1  # noqa: E402
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/authorization.v1.schema.json"

POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()
ALLOC_PATH = lambda day: (TRUTH / "allocation_v1/capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json").resolve()
INTENTS_DIR = lambda day: (TRUTH / "intents_v1/snapshots" / day).resolve()
OUT_ROOT = (TRUTH / "engine_activity_v1/authorization_v1").resolve()


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        o = json.load(f)
    if not isinstance(o, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {str(p)}")
    return o


def _atomic_write_refuse_overwrite(path: Path, data: bytes) -> None:
    if path.exists():
        raise SystemExit(f"FAIL: REFUSE_OVERWRITE_EXISTING_FILE: {str(path)}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        raise SystemExit(f"FAIL: TEMP_FILE_ALREADY_EXISTS: {str(tmp)}")
    if not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(str(tmp), str(path))


def _parse_day(day: str) -> str:
    s = (day or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="run_authorization_artifacts_day_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args(argv)

    day = _parse_day(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"

    # Fail-closed: authorization artifacts only exist on authority PASS+authoritative days.
    _require_authority_head_pass_authoritative(day)
    p_alloc = ALLOC_PATH(day)
    if not p_alloc.exists():
        raise SystemExit(f"FAIL: ALLOCATION_AUTHORITY_MISSING: {str(p_alloc)}")
    alloc_sha = _sha256_file(p_alloc)
    alloc_obj = _read_json_obj(p_alloc)

    intents_dir = INTENTS_DIR(day)

    intent_files = sorted(
        [
            p for p in intents_dir.iterdir()
            if p.is_file()
            and p.name.endswith(".json")
            and p.name != "no_intents_day.v1.json"
        ],
        key=lambda p: p.name,
    )

    marker_path = (intents_dir / "no_intents_day.v1.json").resolve()

    if not intent_files:
        if marker_path.exists():
            print(f"OK: NO_INTENTS_FOR_DAY_MARKER_PRESENT day_utc={day}")
            return 0
        raise SystemExit("FAIL: NO_INTENTS_FOR_DAY")

    if not intent_files:
        m = NO_INTENTS_MARKER(day)
        if m.exists() and m.is_file():
            obj = _read_json_obj(m)
            validate_against_repo_schema_v1(obj, REPO_ROOT, NO_INTENTS_SCHEMA)
            print(f"OK: NO_INTENTS_FOR_DAY_MARKER_PRESENT day_utc={day} path={str(m)}")
            return 0
        raise SystemExit("FAIL: NO_INTENTS_FOR_DAY (no marker)")

    pol_sha = _sha256_file(POLICY_PATH) if POLICY_PATH.exists() else "0" * 64

    # Build lookup of per_intent decisions from allocation artifact
    decisions = {}
    for row in alloc_obj.get("per_intent", []):
        if not isinstance(row, dict):
            continue
        decisions[str(row.get("intent_hash") or "")] = row

    out_day_dir = (OUT_ROOT / day).resolve()
    out_day_dir.mkdir(parents=True, exist_ok=True)

    wrote = 0
    for p in intent_files:
        intent_obj = _read_json_obj(p)
        engine_id = str(((intent_obj.get("engine") or {}).get("engine_id") or "")).strip()
        intent_id = str(intent_obj.get("intent_id") or "").strip()
        if not engine_id or not intent_id:
            raise SystemExit(f"FAIL: INTENT_MISSING_ENGINE_OR_ID: {str(p)}")

        intent_sha = _sha256_file(p)
        dec = decisions.get(intent_sha, None)
        if not isinstance(dec, dict):
            raise SystemExit(f"FAIL: ALLOCATION_MISSING_INTENT_HASH: {intent_sha} file={str(p)}")

        decision = str(dec.get("decision") or "REJECTED").strip().upper()
        auth_qty = int(dec.get("authorized_quantity") or 0)
        rc = list(dec.get("reason_codes") or ["CAPAUTH_REJECTED", "CAPAUTH_FAIL_CLOSED_REQUIRED"])
        status = "AUTHORIZED" if decision == "AUTHORIZED" and auth_qty > 0 else "REJECTED"

        auth_block: Dict[str, Any] = {
            "decision": decision,
            "authorized_quantity": int(auth_qty),
            "constraints": [],
            "decision_hash": None,
        }
        auth_block["decision_hash"] = canonical_hash_excluding_fields_v1(auth_block, fields=("decision_hash",))

        out_obj: Dict[str, Any] = {
            "schema_id": "C2_AUTHORIZATION_V1",
            "schema_version": 1,
            "produced_utc": produced_utc,
            "day_utc": day,
            "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_authorization_artifacts_day_v1.py"},
            "status": status,
            "reason_codes": rc,
            "input_manifest": [
                {"type": "intent", "path": str(p.resolve()), "sha256": intent_sha, "day_utc": day, "producer": "intents_v1"},
                {"type": "capital_authority_allocation", "path": str(p_alloc), "sha256": alloc_sha, "day_utc": day, "producer": "allocation_v1"},
                {"type": "policy_manifest", "path": str(POLICY_PATH), "sha256": pol_sha, "day_utc": None, "producer": "governance"},
            ],
            "engine_id": engine_id,
            "intent_id": intent_id,
            "intent_hash": intent_sha,
            "authorization": auth_block,
        }

        validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_RELPATH)

        try:
            payload = canonical_json_bytes_v1(out_obj) + b"\n"
        except CanonicalizationError as e:
            raise SystemExit(f"FAIL: CANONICALIZATION_FAILED: {e}") from e

        out_path = (out_day_dir / f"{intent_sha}.authorization.v1.json").resolve()
        _atomic_write_refuse_overwrite(out_path, payload)
        wrote += 1

    print(f"OK: AUTHORIZATION_ARTIFACTS_WRITTEN day_utc={day} wrote={wrote} out_dir={out_day_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
