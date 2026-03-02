#!/usr/bin/env python3
"""
run_replay_certification_gate_v1.py

Deterministic, immutable, idempotent gate:

- Ensures replay_certification_bundle_v1/<DAY>/replay_certification_bundle.v1.json exists (writer is immutable/idempotent).
- Writes replay_certification_gate_v1/<DAY>/replay_certification_gate.v1.json ONCE.
- On subsequent runs:
  - does NOT rewrite gate file
  - compares recomputed candidate bundle sha to the stored candidate sha
  - if match => PASS (two-run equality proven)
  - if mismatch => FAIL (tamper/drift)

This matches Constellation immutability doctrine.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/replay_certification_gate.v1.schema.json"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    h.update(p.read_bytes())
    return h.hexdigest()


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _canonical_json_bytes_v1(obj: Any) -> bytes:
    try:
        from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # type: ignore
        return canonical_json_bytes_v1(obj)
    except Exception:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _validate(obj: Any) -> None:
    from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # type: ignore
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)


def _write_once_or_refuse(path: Path, obj: Dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _canonical_json_bytes_v1(obj) + b"\n"
    sha = _sha256_bytes(payload)

    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == sha:
            return sha
        raise SystemExit(f"FAIL: refusing overwrite (different bytes): {path}")

    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    os.replace(tmp, path)
    return sha


def _read_json_obj(p: Path) -> Dict[str, Any]:
    o = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(o, dict):
        raise SystemExit(f"FAIL: gate_file_not_object: {p}")
    return o


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_replay_certification_gate_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")
    produced_utc = f"{day}T00:00:00Z"

    bundle_path = (TRUTH_ROOT / "reports" / "replay_certification_bundle_v1" / day / "replay_certification_bundle.v1.json").resolve()
    gate_path = (TRUTH_ROOT / "reports" / "replay_certification_gate_v1" / day / "replay_certification_gate.v1.json").resolve()

    # Ensure bundle exists (writer is immutable/idempotent)
    subprocess.check_call(["python3", "ops/tools/run_replay_certification_bundle_v1.py", "--day_utc", day], cwd=str(REPO_ROOT))
    candidate_sha = _sha256_file(bundle_path)

    if gate_path.exists():
        # SECOND (or later) RUN: do not rewrite. Compare.
        gate_obj = _read_json_obj(gate_path)
        stored_candidate = str(gate_obj.get("candidate_bundle_sha256") or "").strip()

        if stored_candidate != candidate_sha:
            raise SystemExit(
                f"FAIL: REPLAY_CERT_MISMATCH candidate_sha={candidate_sha} stored_candidate_sha={stored_candidate} gate_path={gate_path}"
            )

        # Write second-run proof artifact (immutable, separate file)
        proof_path = (TRUTH_ROOT / "reports" / "replay_certification_gate_v1" / day / "replay_certification_gate.second_run.v1.json").resolve()

        proof_obj: Dict[str, Any] = {
            "schema_id": "C2_REPLAY_CERTIFICATION_SECOND_RUN_PROOF_V1",
            "schema_version": 1,
            "day_utc": day,
            "produced_utc": produced_utc,
            "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_replay_certification_gate_v1.py"},
            "status": "PASS",
            "two_run_equality": True,
            "stored_candidate_bundle_sha256": stored_candidate,
            "recomputed_candidate_bundle_sha256": candidate_sha,
            "reason_codes": ["REPLAY_CERT_TWO_RUN_EQUALITY_TRUE"],
            "proof_sha256": None
        }

        unsigned_p = dict(proof_obj)
        unsigned_p["proof_sha256"] = None
        proof_obj["proof_sha256"] = _sha256_bytes(_canonical_json_bytes_v1(unsigned_p) + b"\n")

        # Validate against new schema
        from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # type: ignore
        validate_against_repo_schema_v1(proof_obj, REPO_ROOT, "governance/04_DATA/SCHEMAS/C2/REPORTS/replay_certification_second_run_proof.v1.schema.json")

        # Immutable write (write once; if exists, must be identical)
        _ = _write_once_or_refuse(proof_path, proof_obj)

        # Proven: two-run equality (candidate recomputation matches stored)
        print(gate_obj.get("gate_sha256"))
        return 0

    # FIRST RUN: write gate once
    out: Dict[str, Any] = {
        "schema_id": "C2_REPLAY_CERTIFICATION_GATE_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_replay_certification_gate_v1.py"},
        "status": "PASS",
        "first_run": True,
        "two_run_equality": None,
        "existing_bundle_sha256": None,
        "candidate_bundle_sha256": candidate_sha,
        "reason_codes": ["REPLAY_CERT_FIRST_RUN"],
        "gate_sha256": None
    }

    unsigned = dict(out)
    unsigned["gate_sha256"] = None
    out["gate_sha256"] = _sha256_bytes(_canonical_json_bytes_v1(unsigned) + b"\n")

    _validate(out)
    _ = _write_once_or_refuse(gate_path, out)

    print(out["gate_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
