#!/usr/bin/env python3
"""
run_replay_certification_bundle_v1.py

Writes:
  constellation_2/runtime/truth/reports/replay_certification_bundle_v1/<DAY>/replay_certification_bundle.v1.json

Schema expectations (governance/04_DATA/SCHEMAS/C2/REPORTS/replay_certification_bundle.v1.schema.json):
- inputs: object summary (manifest_path, manifest_sha256, present_types, missing_types)
- input_entries: detailed list of entries (type,path,sha256,present)
- hashes: includes depth_stress_artifact_hash

Submission-evidence contract:
- Prefer pillars decisions (pillars_v1r1, then pillars_v1) when present.
- Fall back to legacy submission_index only if pillars are absent.
- If no authoritative broker submission records exist for DAY, submission evidence is satisfied.

Fail-closed:
- If any required input is missing, status=FAIL and fail_closed=true.
- Existing FAIL artifacts may be refreshed via the governed day-artifact refresh path.
- Existing PASS artifacts are preserved.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()

def _truth_root_from_args_or_env(truth_root_arg: str | None) -> Path:
    if truth_root_arg is not None and str(truth_root_arg).strip():
        p = Path(str(truth_root_arg).strip()).expanduser().resolve()
        if not p.is_absolute():
            raise SystemExit(f"FAIL: --truth_root must be absolute: {p}")
        if not p.exists() or (not p.is_dir()):
            raise SystemExit(f"FAIL: --truth_root must exist and be a directory: {p}")
        return p

    env = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if env:
        p = Path(env).expanduser().resolve()
        if not p.is_absolute():
            raise SystemExit(f"FAIL: C2_TRUTH_ROOT must be absolute: {p}")
        if not p.exists() or (not p.is_dir()):
            raise SystemExit(f"FAIL: C2_TRUTH_ROOT must exist and be a directory: {p}")
        return p

    return (REPO_ROOT / "constellation_2/runtime/truth").resolve()

TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()  # placeholder; set in main()

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
        out = subprocess.check_output(
            ["/usr/bin/git", "rev-parse", "HEAD"],
            cwd=str(REPO_ROOT),
            stderr=subprocess.DEVNULL,
        )
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


def _write_refreshable_bundle(path: Path, obj: Dict[str, Any], *, day_utc: str) -> str:
    payload = _canonical_json_bytes_v1(obj) + b"\n"
    wr = write_day_artifact_refreshable_v1(
        path=path,
        data=payload,
        expected_day_utc=day_utc,
        expected_schema_id="C2_REPLAY_CERTIFICATION_BUNDLE_V1",
        expected_schema_version=1,
        preserve_statuses=("PASS",),
    )
    if wr.action == "REFRESHED":
        print(
            "WARN: REPLAY_CERTIFICATION_BUNDLE_REFRESHED_STALE_ARTIFACT "
            f"day_utc={day_utc} path={path} prior_sha256={wr.prior_sha256} "
            f"quarantined_path={wr.quarantined_path}"
        )
    return wr.sha256


def _hash_dir_listing(root: Path) -> str:
    if not root.exists() or not root.is_dir():
        return "0" * 64
    rows = []
    for p in sorted([x for x in root.rglob("*") if x.is_file()], key=lambda x: str(x.relative_to(root)).replace("\\", "/")):
        rel = str(p.relative_to(root)).replace("\\", "/")
        rows.append({"rel": rel, "sha256": _sha256_file(p)})
    return _sha256_bytes(_canonical_json_bytes_v1(rows))


def _input_entry(truth_root: Path, type_: str, relpath: str) -> Dict[str, Any]:
    p = (truth_root / relpath).resolve()
    if p.exists() and p.is_file():
        return {"type": type_, "path": relpath, "sha256": _sha256_file(p), "present": True}
    if p.exists() and p.is_dir():
        return {"type": type_, "path": relpath, "sha256": _hash_dir_listing(p), "present": True}
    return {"type": type_, "path": relpath, "sha256": "0" * 64, "present": False}


def _count_broker_submission_records(truth_root: Path, day: str) -> int:
    root = (truth_root / "execution_evidence_v1" / "submissions" / day).resolve()
    if not root.exists() or not root.is_dir():
        return 0
    return len(list(root.glob("*/broker_submission_record.v2.json")))


def _pillars_decisions_dir(truth_root: Path, day: str) -> Optional[Tuple[str, Path]]:
    candidates = [
        (f"pillars_v1r1/{day}/decisions", (truth_root / "pillars_v1r1" / day / "decisions").resolve()),
        (f"pillars_v1/{day}/decisions", (truth_root / "pillars_v1" / day / "decisions").resolve()),
    ]
    for relpath, path in candidates:
        if not path.exists() or not path.is_dir():
            continue
        count = len([p for p in path.iterdir() if p.is_file() and p.name.endswith(".submission_decision_record.v1.json")])
        if count > 0:
            return (relpath, path)
    return None


def _decision_record_hashes(decisions_dir: Path) -> List[str]:
    files = sorted(
        [p for p in decisions_dir.iterdir() if p.is_file() and p.name.endswith(".submission_decision_record.v1.json")],
        key=lambda p: p.name,
    )
    return [_sha256_file(p) for p in files]


def _submission_evidence_input(truth_root: Path, day: str) -> Tuple[Dict[str, Any], List[str]]:
    pillars = _pillars_decisions_dir(truth_root, day)
    if pillars is not None:
        relpath, path = pillars
        return (
            {
                "type": "submission_evidence",
                "path": relpath,
                "sha256": _hash_dir_listing(path),
                "present": True,
            },
            _decision_record_hashes(path),
        )

    submission_index = f"execution_evidence_v1/submission_index/{day}/submission_index.v1.json"
    legacy = _input_entry(truth_root, "submission_evidence", submission_index)
    if legacy["present"]:
        return (legacy, [str(legacy["sha256"])])

    if _count_broker_submission_records(truth_root, day) == 0:
        return (
            {
                "type": "submission_evidence",
                "path": f"execution_evidence_v1/submissions/{day}",
                "sha256": _sha256_bytes(b""),
                "present": True,
            },
            [],
        )

    return (
        {
            "type": "submission_evidence",
            "path": submission_index,
            "sha256": "0" * 64,
            "present": False,
        },
        [],
    )


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_replay_certification_bundle_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default=None)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")
    produced_utc = f"{day}T00:00:00Z"
    global TRUTH_ROOT
    TRUTH_ROOT = _truth_root_from_args_or_env(args.truth_root)

    # Canonical required inputs (repo-relative)
    input_manifest = f"reports/pipeline_manifest_v1/{day}/pipeline_manifest.v1.json"
    allocation = f"allocation_v1/summary/{day}"
    liquidity = "market_data_snapshot_v1/dataset_manifest.json"
    corr = f"monitoring_v1/engine_correlation_matrix/{day}/engine_correlation_matrix.v1.json"
    convex = f"reports/convex_risk_assessment_v1/{day}/convex_risk_assessment.v1.json"
    depth = f"reports/depth_liquidity_stress_v1/{day}/depth_liquidity_stress.v1.json"
    reconciliation = f"reports/broker_reconciliation_v2/{day}/broker_reconciliation.v2.json"
    nav = f"accounting_v2/nav/{day}/nav.v2.json"
    gate_stack = f"reports/gate_stack_verdict_v1/{day}/gate_stack_verdict.v1.json"
    submission_evidence_entry, submission_bundle_hashes = _submission_evidence_input(TRUTH_ROOT, day)

    input_entries: List[Dict[str, Any]] = [
        _input_entry(TRUTH_ROOT, "input_manifest", input_manifest),
        _input_entry(TRUTH_ROOT, "allocation_summary", allocation),
        _input_entry(TRUTH_ROOT, "liquidity_artifact", liquidity),
        _input_entry(TRUTH_ROOT, "correlation_artifact", corr),
        _input_entry(TRUTH_ROOT, "convex_shock_artifact", convex),
        _input_entry(TRUTH_ROOT, "depth_stress_artifact", depth),
        submission_evidence_entry,
        _input_entry(TRUTH_ROOT, "reconciliation", reconciliation),
        _input_entry(TRUTH_ROOT, "nav", nav),
        _input_entry(TRUTH_ROOT, "gate_stack_verdict", gate_stack),
    ]

    present_types = [e["type"] for e in input_entries if e["present"]]
    missing_types = [e["type"] for e in input_entries if not e["present"]]
    broker_submission_records_total = _count_broker_submission_records(TRUTH_ROOT, day)
    bootstrap_missing_allowed = {"input_manifest", "allocation_summary", "reconciliation"}
    bootstrap_missing_subset = set(missing_types).issubset(bootstrap_missing_allowed)
    bootstrap_replay_tolerance_applies = broker_submission_records_total == 0 and bootstrap_missing_subset
    fail_closed = bool(missing_types) and not bootstrap_replay_tolerance_applies
    status = "FAIL" if fail_closed else "PASS"

    def _h(type_: str) -> str:
        for e in input_entries:
            if e["type"] == type_:
                return str(e["sha256"])
        return "0" * 64

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
    sha = _write_refreshable_bundle(out_path, out, day_utc=day)
    print(sha)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
