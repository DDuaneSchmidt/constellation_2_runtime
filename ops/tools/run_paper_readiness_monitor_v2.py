#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA = "governance/04_DATA/SCHEMAS/C2/MONITORING/paper_readiness_report.v1.schema.json"


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        o = json.load(f)
    if not isinstance(o, dict):
        raise ValueError("TOP_LEVEL_NOT_OBJECT")
    return o


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_paper_readiness_monitor_v2")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = args.day_utc.strip()

    checks: List[Dict[str, Any]] = []
    reason_codes: List[str] = []
    status = "OK"

    subs_dir = (TRUTH / "execution_evidence_v1/submissions" / day).resolve()
    broker_dir = (TRUTH / "execution_evidence_v1/broker_events" / day).resolve()
    man = (broker_dir / "broker_event_day_manifest.v1.json").resolve()
    log = (broker_dir / "broker_event_log.v1.jsonl").resolve()

    # Check: submissions exist => broker evidence must exist
    sub_dirs = []
    if subs_dir.exists():
        sub_dirs = [p for p in subs_dir.iterdir() if p.is_dir()]

    if sub_dirs:
        if not man.exists() or not log.exists():
            status = "FAIL"
            reason_codes.append("BROKER_EVIDENCE_MISSING_FOR_SUBMISSIONS")
            checks.append({"name": "broker_evidence_present", "status": "FAIL", "details": {"manifest": str(man), "log": str(log)}})
        else:
            checks.append({"name": "broker_evidence_present", "status": "OK", "details": {"manifest": str(man), "log": str(log)}})
    else:
        checks.append({"name": "submissions_present", "status": "OK", "details": {"submissions_day_dir": str(subs_dir), "count": 0}})

    # Check: each submission has linked execution event v2 + broker submission v3 + no SYNTH
    missing = 0
    synth = 0
    missing_lineage = 0
    for sd in sub_dirs:
        p_evt2 = sd / "execution_event_record.v2.json"
        p_bsr3 = sd / "broker_submission_record.v3.json"

        if not p_evt2.exists():
            missing += 1
            continue
        if not p_bsr3.exists():
            missing_lineage += 1
            continue

        evt2 = _read_json_obj(p_evt2)
        raw_status = str(evt2.get("raw_broker_status") or "")
        if raw_status.upper().startswith("SYNTH"):
            synth += 1

        bsr = _read_json_obj(p_bsr3)
        engine_id = str(bsr.get("engine_id") or "").strip()
        source_intent_id = str(bsr.get("source_intent_id") or "").strip()
        intent_sha256 = str(bsr.get("intent_sha256") or "").strip()
        if not engine_id or not source_intent_id or len(intent_sha256) != 64:
            missing_lineage += 1

    if missing or synth or missing_lineage:
        status = "FAIL"
        if missing:
            reason_codes.append("MISSING_EXECUTION_EVENT_RECORD_V2")
        if synth:
            reason_codes.append("SYNTH_STATUS_FORBIDDEN_IN_PAPER")
        if missing_lineage:
            reason_codes.append("MISSING_LINEAGE_IN_BROKER_SUBMISSION_RECORD_V3")

    checks.append(
        {
            "name": "submission_linkage",
            "status": "FAIL" if (missing or synth or missing_lineage) else "OK",
            "details": {"submissions": len(sub_dirs), "missing_evt2": missing, "synth_status": synth, "missing_lineage": missing_lineage},
        }
    )

    produced_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    out: Dict[str, Any] = {
        "schema_id": "C2_PAPER_READINESS_REPORT",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "git_sha": _git_sha(), "module": "ops/tools/run_paper_readiness_monitor_v2.py"},
        "status": status,
        "reason_codes": reason_codes,
        "checks": checks,
        "canonical_json_hash": None,
    }
    out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
    validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA)

    try:
        payload = canonical_json_bytes_v1(out) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    out_path = (TRUTH / "monitoring_v1/paper_readiness" / day / "paper_readiness_report.v1.json").resolve()
    try:
        _ = write_file_immutable_v1(path=out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {e}", file=sys.stderr)
        return 4

    if status != "OK":
        print("FAIL: PAPER_READINESS_MONITOR_V2")
        return 2

    print("OK: PAPER_READINESS_MONITOR_V2")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
