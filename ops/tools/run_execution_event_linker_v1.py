#!/usr/bin/env python3
"""
run_execution_event_linker_v1.py

A+2 Broker truth bridge:
- Reads broker raw log + broker day manifest
- Reads execution_evidence submissions day directories
- Writes execution_event_record.v2.json per submission directory

Fail-closed:
- If submissions exist for a day and broker log/manifest missing => FAIL
- If manifest schema fails => FAIL (manifest writer already validates, but we verify existence/keys)
- If PAPER and status would be synthetic => FAIL (PAPER forbids SYNTH readiness)

Matching model (v1):
- Extract broker ids from broker_submission_record (preferred) or mapping_ledger_record
- Match broker raw events by searching args strings for those ids
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseD.lib.lineage_assert_v1 import assert_no_synth_status_in_paper

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

MAN_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/broker_event_day_manifest.v1.schema.json"
RAW_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/broker_event_raw.v1.schema.json"
OUT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/execution_event_record.v2.schema.json"


class LinkerError(RuntimeError):
    pass


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        o = json.load(f)
    if not isinstance(o, dict):
        raise LinkerError(f"TOP_LEVEL_NOT_OBJECT: {str(path)}")
    return o


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            ln = raw.strip()
            if not ln:
                continue
            o = json.loads(ln)
            if not isinstance(o, dict):
                raise LinkerError("BROKER_EVENT_RAW_LINE_NOT_OBJECT")
            validate_against_repo_schema_v1(o, REPO_ROOT, RAW_SCHEMA)
            events.append(o)
    return events


def _list_subdirs(p: Path) -> List[Path]:
    if not p.exists():
        return []
    return sorted([x for x in p.iterdir() if x.is_dir()], key=lambda x: x.name)


def _extract_ids_from_broker_submission(sd: Path) -> Dict[str, Optional[str]]:
    """
    Preferred: broker_submission_record.v3.json if present (Phase D v2 boundary).
    Fallback: broker_submission_record.v2.json or mapping ledger.
    """
    for name in ["broker_submission_record.v3.json", "broker_submission_record.v2.json", "broker_submission_record.v1.json"]:
        p = sd / name
        if p.exists():
            b = _read_json_obj(p)
            ids = b.get("broker_ids")
            if isinstance(ids, dict):
                order_id = ids.get("order_id")
                perm_id = ids.get("perm_id")
                return {
                    "order_id": str(order_id) if order_id is not None else None,
                    "perm_id": str(perm_id) if perm_id is not None else None,
                }
    # fallback: mapping ledger record
    for name in ["mapping_ledger_record.v2.json", "mapping_ledger_record.v1.json"]:
        p = sd / name
        if p.exists():
            m = _read_json_obj(p)
            ids = m.get("broker_ids")
            if isinstance(ids, dict):
                order_id = ids.get("order_id")
                perm_id = ids.get("perm_id")
                return {
                    "order_id": str(order_id) if order_id is not None else None,
                    "perm_id": str(perm_id) if perm_id is not None else None,
                }
    return {"order_id": None, "perm_id": None}


def _match_events(events: List[Dict[str, Any]], order_id: Optional[str], perm_id: Optional[str]) -> List[Dict[str, Any]]:
    needles: List[str] = []
    if order_id and order_id.strip() and not order_id.strip().upper().startswith("SYNTH"):
        needles.append(order_id.strip())
    if perm_id and perm_id.strip() and not perm_id.strip().upper().startswith("SYNTH"):
        needles.append(perm_id.strip())

    if not needles:
        return []

    out: List[Dict[str, Any]] = []
    for ev in events:
        ibf = ev.get("ib_fields")
        args = []
        if isinstance(ibf, dict):
            a = ibf.get("args")
            if isinstance(a, list):
                for it in a:
                    if isinstance(it, dict):
                        args.append(str(it.get("value", "")))
        blob = " ".join(args)
        for nd in needles:
            if nd in blob:
                out.append(ev)
                break
    return out


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_execution_event_linker_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--environment", required=True, choices=["PAPER"])
    args = ap.parse_args()

    day = args.day_utc.strip()
    env = args.environment.strip().upper()

    submissions_day = (TRUTH / "execution_evidence_v1/submissions" / day).resolve()
    broker_day = (TRUTH / "execution_evidence_v1/broker_events" / day).resolve()

    subs = _list_subdirs(submissions_day)
    if subs and not broker_day.exists():
        raise LinkerError(f"SUBMISSIONS_EXIST_BUT_BROKER_DAY_DIR_MISSING: {str(broker_day)}")

    man_path = broker_day / "broker_event_day_manifest.v1.json"
    log_path = broker_day / "broker_event_log.v1.jsonl"
    if subs:
        if not man_path.exists():
            raise LinkerError(f"MISSING_BROKER_EVENT_DAY_MANIFEST: {str(man_path)}")
        if not log_path.exists():
            raise LinkerError(f"MISSING_BROKER_EVENT_LOG: {str(log_path)}")

    manifest = _read_json_obj(man_path)
    validate_against_repo_schema_v1(manifest, REPO_ROOT, MAN_SCHEMA)

    # We bind to the raw log sha256 stored in manifest["log"]["log_sha256"]
    log_obj = manifest.get("log")
    if not isinstance(log_obj, dict):
        raise LinkerError("BROKER_MANIFEST_LOG_SECTION_MISSING")
    log_sha = log_obj.get("log_sha256")
    if not isinstance(log_sha, str) or len(log_sha.strip()) != 64:
        raise LinkerError("BROKER_MANIFEST_LOG_SHA256_MISSING_OR_INVALID")

    events = _read_jsonl(log_path)

    git_sha = _git_sha()

    for sd in subs:
        submission_id = sd.name.strip()
        ids = _extract_ids_from_broker_submission(sd)
        matched = _match_events(events, ids.get("order_id"), ids.get("perm_id"))

        status = "NO_BROKER_MATCH"
        if matched:
            status = "BROKER_EVENTS_MATCHED"

        assert_no_synth_status_in_paper(env, status)

        rec: Dict[str, Any] = {
            "schema_id": "EXECUTION_EVENT_RECORD",
            "schema_version": 2,
            "day_utc": day,
            "submission_id": submission_id,
            "environment": env,
            "raw_broker_status": status,
            "broker_ids": {"order_id": ids.get("order_id"), "perm_id": ids.get("perm_id")},
            "broker_manifest_log_sha256": log_sha,
            "matched_broker_event_sha256": [str(ev.get("sha256")) for ev in matched if isinstance(ev.get("sha256"), str)],
            "broker_event_count": int(len(matched)),
            "producer": {"repo": "constellation_2_runtime", "git_sha": git_sha, "module": "ops/tools/run_execution_event_linker_v1.py"},
            "canonical_json_hash": None,
        }

        rec["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(rec)
        validate_against_repo_schema_v1(rec, REPO_ROOT, OUT_SCHEMA)

        try:
            payload = canonical_json_bytes_v1(rec) + b"\n"
        except CanonicalizationError as e:
            raise LinkerError(f"CANONICALIZATION_ERROR: {e}") from e

        outp = sd / "execution_event_record.v2.json"
        try:
            _ = write_file_immutable_v1(path=outp, data=payload, create_dirs=True)
        except ImmutableWriteError as e:
            raise LinkerError(f"IMMUTABLE_WRITE_FAILED: {e}") from e

    print(f"OK: EXECUTION_EVENT_LINKER_V1 day_utc={day} submissions={len(subs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
