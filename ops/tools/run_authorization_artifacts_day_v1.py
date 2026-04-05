#!/usr/bin/env python3
"""
run_authorization_artifacts_day_v1.py

Bundle A (A1): Per-intent Authorization Artifacts (day-scoped, deterministic, fail-closed).

Inputs:
- allocation_v1/capital_authority_allocation_v1/<DAY>/capital_authority_allocation.v1.json
- intents_v1/snapshots/<DAY>/*.exposure_intent.v1.json
- policy manifest (sha in input manifest only)

Outputs:
- engine_activity_v1/authorization_v1/<DAY>/<INTENT_SHA>.authorization.v1.json

Truth-root selection order:
  1) --truth_root
  2) C2_TRUTH_ROOT
  3) constellation_2.common.truth_root_v1.resolve_truth_root(repo_root=REPO_ROOT)

Note:
- Uses sha256(file bytes) of intent file as the stable intent_hash reference.
- decision_hash is sha256(canonical JSON of authorization block excluding decision_hash).

Governed same-day freshness behavior:
- existence of day-key output is NOT sufficient to reuse
- existing output may be reused only if its recorded input_manifest matches the current authoritative input_manifest
- if input_manifest differs, existing day-key artifact is stale and must be quarantined then replaced
- if input_manifest is identical but bytes differ, fail closed
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

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.truth_root_v1 import resolve_truth_root  # noqa: E402
from constellation_2.phaseD.lib.canon_json_v1 import (  # noqa: E402
    CanonicalizationError,
    canonical_hash_excluding_fields_v1,
    canonical_json_bytes_v1,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # noqa: E402

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/authorization.v1.schema.json"
NO_INTENTS_SCHEMA = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/no_intents_day.v1.schema.json"
POLICY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json").resolve()


def _require_truth_root_under_repo(truth_root: Path) -> Path:
    pr = truth_root.expanduser().resolve()
    if not pr.is_absolute():
        raise SystemExit(f"FAIL: truth_root must be absolute: {pr}")
    if not pr.exists() or not pr.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not dir: {pr}")
    return pr


def _resolve_truth_root(truth_root_arg: str) -> Path:
    arg = (truth_root_arg or "").strip()
    if arg:
        return _require_truth_root_under_repo(Path(arg))
    env_root = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if env_root:
        return _require_truth_root_under_repo(Path(env_root))
    return _require_truth_root_under_repo(resolve_truth_root(repo_root=REPO_ROOT))


def _parse_day(day: str) -> str:
    s = (day or "").strip()
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {s!r}")
    return s


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


def _canonical_output_bytes(obj: Dict[str, Any]) -> bytes:
    try:
        return canonical_json_bytes_v1(obj) + b"\n"
    except CanonicalizationError as e:
        raise SystemExit(f"FAIL: CANONICALIZATION_FAILED: {e}") from e


def _canonical_manifest_bytes(items: List[Dict[str, Any]]) -> bytes:
    return (json.dumps(items, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _input_manifest_hash_from_obj(obj: Dict[str, Any]) -> str:
    im = obj.get("input_manifest")
    if not isinstance(im, list):
        raise SystemExit("FAIL: INPUT_MANIFEST_MISSING_OR_INVALID")
    return _sha256_bytes(_canonical_manifest_bytes(im))


def _replace_stale_existing(out_path: Path, existing_bytes: bytes, candidate_bytes: bytes) -> str:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    existing_sha = _sha256_bytes(existing_bytes)
    candidate_sha = _sha256_bytes(candidate_bytes)

    quarantine = out_path.with_name(f"{out_path.name}.INVALID_{existing_sha}.json")
    if quarantine.exists():
        if not quarantine.is_file():
            raise SystemExit(f"FAIL: QUARANTINE_PATH_NOT_FILE: {quarantine}")
        q_sha = _sha256_file(quarantine)
        if q_sha != existing_sha:
            raise SystemExit(
                "FAIL: QUARANTINE_SHA_MISMATCH "
                f"path={quarantine} expected_sha={existing_sha} actual_sha={q_sha}"
            )
    else:
        tmp_q = quarantine.with_name(f".{quarantine.name}.tmp.{os.getpid()}")
        tmp_q.write_bytes(existing_bytes)
        fdq = os.open(str(tmp_q), os.O_RDONLY)
        try:
            os.fsync(fdq)
        finally:
            os.close(fdq)
        os.replace(str(tmp_q), str(quarantine))

    tmp = out_path.with_name(f".{out_path.name}.tmp.{os.getpid()}")
    tmp.write_bytes(candidate_bytes)
    fd = os.open(str(tmp), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

    os.replace(str(tmp), str(out_path))

    dfd = os.open(str(out_path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)

    return candidate_sha


def _write_daykey_with_freshness(out_path: Path, out_obj: Dict[str, Any]) -> str:
    candidate_bytes = _canonical_output_bytes(out_obj)
    candidate_sha = _sha256_bytes(candidate_bytes)
    candidate_manifest_hash = _input_manifest_hash_from_obj(out_obj)

    if out_path.exists():
        if not out_path.is_file():
            raise SystemExit(f"FAIL: TARGET_NOT_FILE: {out_path}")

        existing_bytes = out_path.read_bytes()
        existing_sha = _sha256_bytes(existing_bytes)
        existing = _read_json_obj(out_path)

        existing_schema_id = str(existing.get("schema_id") or "").strip()
        existing_schema_version = existing.get("schema_version")
        existing_day = str(existing.get("day_utc") or "").strip()

        if existing_schema_id != "C2_AUTHORIZATION_V1":
            raise SystemExit(
                f"FAIL: EXISTING_SCHEMA_ID_MISMATCH path={out_path} schema_id={existing_schema_id!r}"
            )
        if int(existing_schema_version or 0) != 1:
            raise SystemExit(
                f"FAIL: EXISTING_SCHEMA_VERSION_MISMATCH path={out_path} schema_version={existing_schema_version!r}"
            )
        if existing_day != str(out_obj.get("day_utc") or "").strip():
            raise SystemExit(
                f"FAIL: EXISTING_DAY_MISMATCH path={out_path} existing_day={existing_day!r} "
                f"candidate_day={str(out_obj.get('day_utc') or '').strip()!r}"
            )

        existing_manifest_hash = _input_manifest_hash_from_obj(existing)
        if existing_manifest_hash == candidate_manifest_hash:
            if existing_sha != candidate_sha:
                raise SystemExit(
                    "FAIL: SAME_INPUT_MANIFEST_BUT_DIFFERENT_OUTPUT_BYTES "
                    f"path={out_path} manifest_hash={candidate_manifest_hash} "
                    f"existing_sha={existing_sha} candidate_sha={candidate_sha}"
                )
            print(
                f"OK: AUTHORIZATION_ARTIFACT_WRITTEN day_utc={out_obj['day_utc']} "
                f"path={out_path} sha256={existing_sha} action=EXISTS_IDENTICAL"
            )
            return existing_sha

        wrote_sha = _replace_stale_existing(out_path, existing_bytes, candidate_bytes)
        print(
            f"OK: AUTHORIZATION_ARTIFACT_WRITTEN day_utc={out_obj['day_utc']} "
            f"path={out_path} sha256={wrote_sha} action=REPLACED_STALE existing_sha={existing_sha}"
        )
        return wrote_sha

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(f".{out_path.name}.tmp.{os.getpid()}")
    tmp.write_bytes(candidate_bytes)
    fd = os.open(str(tmp), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(str(tmp), str(out_path))

    dfd = os.open(str(out_path.parent), os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)

    print(
        f"OK: AUTHORIZATION_ARTIFACT_WRITTEN day_utc={out_obj['day_utc']} "
        f"path={out_path} sha256={candidate_sha} action=WROTE"
    )
    return candidate_sha


def _require_authority_head_pass_authoritative(day: str, truth_root: Path) -> Dict[str, Any]:
    authority_head_path = (truth_root / "run_pointer_v2" / "canonical_authority_head.v1.json").resolve()
    if not authority_head_path.exists() or not authority_head_path.is_file():
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_MISSING: {str(authority_head_path)}")
    ah = _read_json_obj(authority_head_path)
    schema_id = str(ah.get("schema_id") or "").strip()
    schema_ver = str(ah.get("schema_version") or "").strip()
    status = str(ah.get("status") or "").strip().upper()
    authoritative = bool(ah.get("authoritative") is True)
    day_utc = str(ah.get("day_utc") or "").strip()

    if schema_id != "c2_run_pointer_canonical_authority_head" or schema_ver != "v1":
        raise SystemExit("FAIL: AUTHORITY_HEAD_SCHEMA_MISMATCH")
    if day_utc != day:
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_DAY_MISMATCH head_day={day_utc!r} expected_day={day!r}")
    if status not in ("PASS", "BOOTSTRAP_PASS"):
        raise SystemExit(f"FAIL: AUTHORITY_HEAD_NOT_EXECUTION_AUTHORIZED status={status!r}")
    if not authoritative:
        raise SystemExit("FAIL: AUTHORITY_HEAD_NOT_AUTHORITATIVE")
    points_to = str(ah.get("points_to") or "").strip()
    if "authorization_gate_verdict_v1" not in points_to:
        raise SystemExit("FAIL: AUTHORITY_HEAD_NOT_AUTHORIZATION_VERDICT")
    return ah


def _alloc_path(truth_root: Path, day: str) -> Path:
    return (truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json").resolve()


def _intents_dir(truth_root: Path, day: str) -> Path:
    return (truth_root / "intents_v1" / "snapshots" / day).resolve()


def _out_root(truth_root: Path) -> Path:
    return (truth_root / "engine_activity_v1" / "authorization_v1").resolve()


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="run_authorization_artifacts_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="", help="Absolute truth root; defaults to C2_TRUTH_ROOT or repo resolver")
    args = ap.parse_args(argv)

    day = _parse_day(args.day_utc)
    produced_utc = f"{day}T00:00:00Z"
    truth_root = _resolve_truth_root(args.truth_root)

    _require_authority_head_pass_authoritative(day, truth_root)

    p_alloc = _alloc_path(truth_root, day)
    if not p_alloc.exists():
        raise SystemExit(f"FAIL: ALLOCATION_AUTHORITY_MISSING: {str(p_alloc)}")
    alloc_sha = _sha256_file(p_alloc)
    alloc_obj = _read_json_obj(p_alloc)

    intents_dir = _intents_dir(truth_root, day)
    if not intents_dir.exists() or not intents_dir.is_dir():
        raise SystemExit(f"FAIL: INTENTS_DAY_DIR_MISSING: {str(intents_dir)}")

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
            obj = _read_json_obj(marker_path)
            validate_against_repo_schema_v1(obj, REPO_ROOT, NO_INTENTS_SCHEMA)
            print(f"OK: NO_INTENTS_FOR_DAY_MARKER_PRESENT day_utc={day} path={str(marker_path)}")
            return 0
        raise SystemExit("FAIL: NO_INTENTS_FOR_DAY")

    pol_sha = _sha256_file(POLICY_PATH) if POLICY_PATH.exists() else "0" * 64
    producer_git_sha = _git_sha()
    producer_git_sha_hash = _sha256_bytes((producer_git_sha + "\n").encode("utf-8"))

    decisions: Dict[str, Dict[str, Any]] = {}
    for row in alloc_obj.get("per_intent", []):
        if not isinstance(row, dict):
            continue
        decisions[str(row.get("intent_hash") or "")] = row

    out_day_dir = (_out_root(truth_root) / day).resolve()
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
            "producer": {
                "repo": "constellation_2_runtime",
                "git_sha": producer_git_sha,
                "module": "ops/tools/run_authorization_artifacts_day_v1.py",
            },
            "status": status,
            "reason_codes": rc,
            "input_manifest": [
                {"type": "intent", "path": str(p.resolve()), "sha256": intent_sha, "day_utc": day, "producer": "intents_v1"},
                {"type": "capital_authority_allocation", "path": str(p_alloc), "sha256": alloc_sha, "day_utc": day, "producer": "allocation_v1"},
                {"type": "policy_manifest", "path": str(POLICY_PATH), "sha256": pol_sha, "day_utc": None, "producer": "governance"},
                {"type": "other", "path": "git:HEAD", "sha256": producer_git_sha_hash, "day_utc": None, "producer": "git"},
            ],
            "engine_id": engine_id,
            "intent_id": intent_id,
            "intent_hash": intent_sha,
            "authorization": auth_block,
        }

        validate_against_repo_schema_v1(out_obj, REPO_ROOT, SCHEMA_RELPATH)

        out_path = (out_day_dir / f"{intent_sha}.authorization.v1.json").resolve()
        _write_daykey_with_freshness(out_path, out_obj)
        wrote += 1

    print(f"OK: AUTHORIZATION_ARTIFACTS_WRITTEN day_utc={day} wrote={wrote} out_dir={out_day_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
