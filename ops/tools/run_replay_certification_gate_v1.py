#!/usr/bin/env python3
"""
run_replay_certification_gate_v1.py

Deterministic, immutable, idempotent gate:

- Ensures replay_certification_bundle_v1/<DAY>/replay_certification_bundle.v1.json exists (writer is immutable/idempotent).
- Writes replay_certification_gate_v1/<DAY>/replay_certification_gate.v1.json ONCE.
- On subsequent runs:
  - preserves an existing PASS gate only when its candidate bundle sha still matches
  - refreshes stale derived gate files through the governed day-artifact refresh path
  - if recomputed candidate bundle sha still matches the stored PASS gate => PASS (two-run equality proven)

This preserves stable PASS artifacts while allowing stale derived replay artifacts to refresh safely as authoritative submission evidence materializes.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict

from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()

def _require_truth_root(p: str) -> Path:
    s = (p or "").strip()
    if not s:
        raise SystemExit("FAIL: --truth_root empty")
    pr = Path(s).expanduser().resolve()
    if not pr.is_absolute():
        raise SystemExit(f"FAIL: --truth_root must be absolute: {pr}")
    if not pr.exists() or (not pr.is_dir()):
        raise SystemExit(f"FAIL: --truth_root must exist and be a directory: {pr}")
    return pr
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


def _write_refreshable_gate(path: Path, obj: Dict[str, Any], *, day_utc: str) -> str:
    payload = _canonical_json_bytes_v1(obj) + b"\n"
    wr = write_day_artifact_refreshable_v1(
        path=path,
        data=payload,
        expected_day_utc=day_utc,
        expected_schema_id="C2_REPLAY_CERTIFICATION_GATE_V1",
        expected_schema_version=1,
        preserve_statuses=(),
    )
    if wr.action == "REFRESHED":
        print(
            "WARN: REPLAY_CERTIFICATION_GATE_REFRESHED_STALE_ARTIFACT "
            f"day_utc={day_utc} path={path} prior_sha256={wr.prior_sha256} "
            f"quarantined_path={wr.quarantined_path}"
        )
    return wr.sha256


def _read_json_obj(p: Path) -> Dict[str, Any]:
    o = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(o, dict):
        raise SystemExit(f"FAIL: gate_file_not_object: {p}")
    return o


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_replay_certification_gate_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=True)
    ap.add_argument("--produced_utc", required=True)
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")

    expected = f"{day}T00:00:00Z"
    if str(args.produced_utc).strip() != expected:
        raise SystemExit(
            f"FAIL: produced_utc_must_equal_day_marker expected={expected!r} got={str(args.produced_utc).strip()!r}"
        )
    produced_utc = expected
    _ = str(args.mode).strip().upper()

    TRUTH_ROOT = _require_truth_root(str(args.truth_root))
    bundle_path = (TRUTH_ROOT / "reports" / "replay_certification_bundle_v1" / day / "replay_certification_bundle.v1.json").resolve()
    gate_path = (TRUTH_ROOT / "reports" / "replay_certification_gate_v1" / day / "replay_certification_gate.v1.json").resolve()

    # Ensure bundle exists (writer is immutable/idempotent)
    env = dict(os.environ)
    env["C2_TRUTH_ROOT"] = str(TRUTH_ROOT)
    env["C2_PRODUCED_UTC"] = produced_utc
    completed = subprocess.run(
        ["python3", "ops/tools/run_replay_certification_bundle_v1.py", "--day_utc", day],
        cwd=str(REPO_ROOT),
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0 and not bundle_path.exists():
        raise SystemExit(
            f"FAIL: REPLAY_CERT_BUNDLE_BUILD_FAILED rc={completed.returncode} stderr={completed.stderr.strip()!r}"
        )
    bundle_obj = _read_json_obj(bundle_path)
    candidate_sha = _sha256_file(bundle_path)
    bundle_status = str(bundle_obj.get("status") or "").strip().upper()
    if bundle_status not in {"PASS", "FAIL"}:
        raise SystemExit(f"FAIL: REPLAY_CERT_BUNDLE_STATUS_INVALID: {bundle_status!r}")

    candidate_gate_obj: Dict[str, Any] = {
        "schema_id": "C2_REPLAY_CERTIFICATION_GATE_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_replay_certification_gate_v1.py"},
        "status": bundle_status,
        "first_run": True,
        "two_run_equality": None,
        "existing_bundle_sha256": None,
        "candidate_bundle_sha256": candidate_sha,
        "reason_codes": [
            "REPLAY_CERT_FIRST_RUN",
            "REPLAY_CERT_BUNDLE_PASS" if bundle_status == "PASS" else "REPLAY_CERT_BUNDLE_FAIL_CLOSED",
        ],
        "gate_sha256": None,
    }
    unsigned_candidate = dict(candidate_gate_obj)
    unsigned_candidate["gate_sha256"] = None
    candidate_gate_obj["gate_sha256"] = _sha256_bytes(_canonical_json_bytes_v1(unsigned_candidate) + b"\n")
    _validate(candidate_gate_obj)

    if gate_path.exists():
        # SECOND (or later) RUN: do not rewrite. Compare.
        gate_obj = _read_json_obj(gate_path)
        stored_candidate = str(gate_obj.get("candidate_bundle_sha256") or "").strip()
        stored_status = str(gate_obj.get("status") or "").strip().upper()

        if stored_candidate != candidate_sha:
            gate_sha = _write_refreshable_gate(gate_path, candidate_gate_obj, day_utc=day)
            print(gate_sha)
            return 0

        if stored_status != "PASS" or stored_status != bundle_status:
            gate_sha = _write_refreshable_gate(gate_path, candidate_gate_obj, day_utc=day)
            print(gate_sha)
            return 0

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
    _ = _write_once_or_refuse(gate_path, candidate_gate_obj)
    print(candidate_gate_obj["gate_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
