#!/usr/bin/env python3
"""
run_replay_certification_bundle_v1.py

Writes:
  constellation_2/runtime/truth/reports/replay_certification_bundle_v1/<DAY>/replay_certification_bundle.v1.json

Schema expectations (governance/04_DATA/SCHEMAS/C2/REPORTS/replay_certification_bundle.v1.schema.json):
- inputs: object summary (manifest_path, manifest_sha256, present_types, missing_types)
- input_entries: detailed list of entries (type,path,sha256,present)
- hashes: includes depth_stress_artifact_hash

Fail-closed:
- If any required input is missing, status=FAIL and fail_closed=true.
- Bundle is still written immutably for auditability.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/replay_certification_bundle.v1.schema.json"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
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


def _write_immutable(path: Path, obj: Dict[str, Any]) -> str:
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


def _input_entry(type_: str, relpath: str) -> Dict[str, Any]:
    p = (REPO_ROOT / relpath).resolve()
    if p.exists() and p.is_file():
        return {"type": type_, "path": relpath, "sha256": _sha256_file(p), "present": True}
    return {"type": type_, "path": relpath, "sha256": "0" * 64, "present": False}


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_replay_certification_bundle_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")
    produced_utc = f"{day}T00:00:00Z"

    # Canonical required inputs (repo-relative)
    input_manifest = f"constellation_2/runtime/truth/reports/pipeline_manifest_v1/{day}/pipeline_manifest.v1.json"
    allocation = f"constellation_2/runtime/truth/allocation_v1/capital_authority_allocation_v1/{day}/capital_authority_allocation.v1.json"
    liquidity = "constellation_2/runtime/truth/market_data_snapshot_v1/dataset_manifest.json"
    corr = f"constellation_2/runtime/truth/monitoring_v1/engine_correlation_matrix/{day}/engine_correlation_matrix.v1.json"
    convex = f"constellation_2/runtime/truth/reports/convex_risk_assessment_v1/{day}/convex_risk_assessment.v1.json"
    depth = f"constellation_2/runtime/truth/reports/depth_liquidity_stress_v1/{day}/depth_liquidity_stress.v1.json"
    reconciliation = f"constellation_2/runtime/truth/reports/broker_reconciliation_v2/{day}/broker_reconciliation.v2.json"
    nav = f"constellation_2/runtime/truth/accounting_compat_v1/nav/{day}/nav_snapshot.v1.json"
    submission_index = f"constellation_2/runtime/truth/execution_evidence_v1/submissions/{day}/submission_index.v1.json"
    gate_stack = f"constellation_2/runtime/truth/reports/gate_stack_verdict_v1/{day}/gate_stack_verdict.v1.json"

    input_entries: List[Dict[str, Any]] = [
        _input_entry("input_manifest", input_manifest),
        _input_entry("allocation_summary", allocation),
        _input_entry("liquidity_artifact", liquidity),
        _input_entry("correlation_artifact", corr),
        _input_entry("convex_shock_artifact", convex),
        _input_entry("depth_stress_artifact", depth),
        _input_entry("submission_index", submission_index),
        _input_entry("reconciliation", reconciliation),
        _input_entry("nav", nav),
        _input_entry("gate_stack_verdict", gate_stack),
    ]

    present_types = [e["type"] for e in input_entries if e["present"]]
    missing_types = [e["type"] for e in input_entries if not e["present"]]
    fail_closed = bool(missing_types)
    status = "FAIL" if fail_closed else "PASS"

    def _h(type_: str) -> str:
        for e in input_entries:
            if e["type"] == type_:
                return str(e["sha256"])
        return "0" * 64

    submission_bundle_hashes: List[str] = []
    for e in input_entries:
        if e["type"] in ("submission_index",):
            submission_bundle_hashes.append(str(e["sha256"]))

    lines: List[str] = []
    for e in input_entries:
        if e["present"]:
            lines.append(f"{e['sha256']}  {e['path']}")
    tree_digest = _sha256_bytes(("\n".join(sorted(lines)) + "\n").encode("utf-8"))

    out: Dict[str, Any] = {
        "schema_id": "C2_REPLAY_CERTIFICATION_BUNDLE_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {
            "repo": "constellation_2_runtime",
            "git_sha": _git_sha(),
            "module": "ops/tools/run_replay_certification_bundle_v1.py",
        },
        "status": status,
        "fail_closed": fail_closed,
        "inputs": {
            "manifest_path": input_manifest,
            "manifest_sha256": _h("input_manifest"),
            "present_types": present_types,
            "missing_types": missing_types,
        },
        "input_entries": input_entries,
        "hashes": {
            "input_manifest_hash": _h("input_manifest"),
            "allocation_summary_hash": _h("allocation_summary"),
            "liquidity_artifact_hash": _h("liquidity_artifact"),
            "correlation_artifact_hash": _h("correlation_artifact"),
            "convex_shock_artifact_hash": _h("convex_shock_artifact"),
            "depth_stress_artifact_hash": _h("depth_stress_artifact"),
            "submission_bundle_hashes": submission_bundle_hashes,
            "reconciliation_hash": _h("reconciliation"),
            "nav_hash": _h("nav"),
        },
        "sha256_tree_digest": tree_digest,
        "overall_run_hash": tree_digest,
    }

    _validate(out)

    out_path = (TRUTH_ROOT / "reports" / "replay_certification_bundle_v1" / day / "replay_certification_bundle.v1.json").resolve()
    sha = _write_immutable(out_path, out)
    print(sha)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
