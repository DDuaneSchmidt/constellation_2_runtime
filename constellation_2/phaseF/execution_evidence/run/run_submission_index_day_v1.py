#!/usr/bin/env python3
"""
Phase F — Execution Evidence — Submission Index v1

Writes:
  constellation_2/runtime/truth/execution_evidence_v1/submissions/YYYY-MM-DD/submission_index.v1.json

Inputs (canonical truth only):
  - manifests: constellation_2/runtime/truth/execution_evidence_v1/manifests/YYYY-MM-DD/*.manifest.json
  - submission dirs referenced by manifests (broker_submission_record, execution_event_record, order_plan, binding_record, mapping_ledger_record)

Read-only / deterministic / fail-closed:
  - never talks to broker
  - never mutates existing truth
  - emits missing_paths + warnings instead of guessing
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1


THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[4]
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

MANIFESTS_ROOT = TRUTH_ROOT / "execution_evidence_v1/manifests"
SUBMISSIONS_ROOT = TRUTH_ROOT / "execution_evidence_v1/submissions"
SCHEMA_PATH = (REPO_ROOT / "constellation_2/schemas/submission_index.v1.schema.json").resolve()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_read_json(path: Path) -> Tuple[Optional[Any], Optional[str]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f), None
    except FileNotFoundError:
        return None, "FILE_NOT_FOUND"
    except json.JSONDecodeError:
        return None, "JSON_DECODE_ERROR"
    except Exception:
        return None, "READ_ERROR"


def _mtime(path: Path) -> Optional[float]:
    try:
        return path.stat().st_mtime
    except Exception:
        return None


def _is_day_str(s: str) -> bool:
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return True
    except Exception:
        return False


def _list_manifest_files(day_utc: str) -> List[Path]:
    d = MANIFESTS_ROOT / day_utc
    if not d.exists() or not d.is_dir():
        return []
    out = []
    for p in d.iterdir():
        if p.is_file() and p.name.endswith(".manifest.json"):
            out.append(p)
    out.sort(key=lambda p: p.name)
    return out


def _canonical_json_bytes(obj: Any) -> bytes:
    # Deterministic JSON bytes (stable)
    return (json.dumps(obj, sort_keys=True, indent=2) + "\n").encode("utf-8")


def _extract_manifest_sha256(obj: Any) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    # sha256 of this manifest file is not embedded; this function is a placeholder for future
    return None


def _extract_pointer(entry: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    entry: {"path": "...", "sha256": "..."} or None
    """
    if not isinstance(entry, dict):
        return None, None
    p = entry.get("path")
    h = entry.get("sha256")
    return (p if isinstance(p, str) else None, h if isinstance(h, str) else None)


def build_submission_index(day_utc: str) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "schema_id": "C2_SUBMISSION_INDEX_V1",
        "schema_version": 1,
        "day_utc": day_utc,
        "generated_utc": _utc_now_iso(),
        "status": "OK",
        "items": [],
        "source_paths": [],
        "source_mtimes": {},
        "missing_paths": [],
        "warnings": [],
    }

    if not _is_day_str(day_utc):
        resp["status"] = "FAIL"
        resp["warnings"].append("DAY_INVALID")
        return resp

    if not TRUTH_ROOT.exists():
        resp["status"] = "FAIL"
        resp["missing_paths"].append(str(TRUTH_ROOT))
        resp["warnings"].append("TRUTH_ROOT_MISSING")
        return resp

    if not MANIFESTS_ROOT.exists():
        resp["status"] = "FAIL"
        resp["missing_paths"].append(str(MANIFESTS_ROOT))
        resp["warnings"].append("MANIFESTS_ROOT_MISSING")
        return resp

    mfiles = _list_manifest_files(day_utc)
    if not mfiles:
        resp["status"] = "DEGRADED"
        resp["warnings"].append("NO_MANIFESTS_FOUND_FOR_DAY")
        return resp

    for mp in mfiles:
        resp["source_paths"].append(str(mp))
        mt = _mtime(mp)
        if mt is not None:
            resp["source_mtimes"][str(mp)] = mt

        man, err = _safe_read_json(mp)
        if man is None:
            resp["warnings"].append(f"MANIFEST_UNREADABLE:{err}")
            resp["missing_paths"].append(str(mp))
            resp["status"] = "DEGRADED"
            continue

        if not isinstance(man, dict):
            resp["warnings"].append("MANIFEST_NOT_OBJECT")
            resp["status"] = "DEGRADED"
            continue

        if man.get("day_utc") != day_utc:
            resp["warnings"].append("MANIFEST_DAY_MISMATCH")
            resp["status"] = "DEGRADED"

        sub = man.get("submission")
        if not isinstance(sub, dict):
            resp["warnings"].append("MANIFEST_SUBMISSION_MISSING_OR_INVALID")
            resp["status"] = "DEGRADED"
            continue

        submission_id = sub.get("submission_id") if isinstance(sub.get("submission_id"), str) else None
        art_dir = sub.get("artifact_dir") if isinstance(sub.get("artifact_dir"), str) else None

        # pointers from manifest
        p_broker, h_broker = _extract_pointer(sub.get("broker_submission_record"))
        p_exec, h_exec = _extract_pointer(sub.get("execution_event_record"))
        p_plan, h_plan = _extract_pointer(sub.get("order_plan"))
        p_bind, h_bind = _extract_pointer(sub.get("binding_record"))
        p_map, h_map = _extract_pointer(sub.get("mapping_ledger_record"))

        item_warnings: List[str] = []
        if submission_id is None:
            item_warnings.append("SUBMISSION_ID_MISSING")
        if art_dir is None:
            item_warnings.append("ARTIFACT_DIR_MISSING")

        # Load broker submission record
        broker_obj = None
        if p_broker:
            broker_obj, berr = _safe_read_json(Path(p_broker))
            if broker_obj is None:
                item_warnings.append(f"BROKER_SUBMISSION_RECORD_UNREADABLE:{berr}")
                resp["missing_paths"].append(p_broker)
        else:
            item_warnings.append("BROKER_SUBMISSION_RECORD_POINTER_MISSING")

        # Load execution event record (optional)
        exec_obj = None
        if p_exec:
            exec_obj, eerr = _safe_read_json(Path(p_exec))
            if exec_obj is None:
                item_warnings.append(f"EXECUTION_EVENT_RECORD_UNREADABLE:{eerr}")
                # treat as missing (optional) but still record
                resp["missing_paths"].append(p_exec)

        # Load order plan (optional)
        plan_obj = None
        if p_plan:
            plan_obj, perr = _safe_read_json(Path(p_plan))
            if plan_obj is None:
                item_warnings.append(f"ORDER_PLAN_UNREADABLE:{perr}")
                resp["missing_paths"].append(p_plan)

        # Load binding + mapping (optional)
        bind_obj = None
        if p_bind:
            bind_obj, berr2 = _safe_read_json(Path(p_bind))
            if bind_obj is None:
                item_warnings.append(f"BINDING_RECORD_UNREADABLE:{berr2}")
                resp["missing_paths"].append(p_bind)

        map_obj = None
        if p_map:
            map_obj, merr = _safe_read_json(Path(p_map))
            if map_obj is None:
                item_warnings.append(f"MAPPING_LEDGER_RECORD_UNREADABLE:{merr}")
                resp["missing_paths"].append(p_map)

        # Extract summary fields (conservative)
        binding_hash = None
        broker = None
        broker_status = None
        submitted_at_utc = None
        broker_ids = None
        if isinstance(broker_obj, dict):
            binding_hash = broker_obj.get("binding_hash") if isinstance(broker_obj.get("binding_hash"), str) else None
            broker = broker_obj.get("broker") if isinstance(broker_obj.get("broker"), str) else None
            broker_status = broker_obj.get("status") if isinstance(broker_obj.get("status"), str) else None
            submitted_at_utc = broker_obj.get("submitted_at_utc") if isinstance(broker_obj.get("submitted_at_utc"), str) else None
            broker_ids = broker_obj.get("broker_ids") if isinstance(broker_obj.get("broker_ids"), dict) else None

        exec_summary = {
            "status": None,
            "filled_qty": None,
            "avg_price": None,
            "event_time_utc": None,
            "perm_id": None,
            "broker_order_id": None,
        }
        if isinstance(exec_obj, dict):
            exec_summary["status"] = exec_obj.get("status") if isinstance(exec_obj.get("status"), str) else None
            exec_summary["filled_qty"] = exec_obj.get("filled_qty")
            exec_summary["avg_price"] = exec_obj.get("avg_price")
            exec_summary["event_time_utc"] = exec_obj.get("event_time_utc") if isinstance(exec_obj.get("event_time_utc"), str) else None
            exec_summary["perm_id"] = exec_obj.get("perm_id")
            exec_summary["broker_order_id"] = exec_obj.get("broker_order_id")

        plan_summary = {
            "plan_id": None,
            "intent_hash": None,
            "underlying": None,
            "structure": None,
            "schema_id": None,
            "schema_version": None,
        }
        if isinstance(plan_obj, dict):
            plan_summary["plan_id"] = plan_obj.get("plan_id") if isinstance(plan_obj.get("plan_id"), str) else None
            plan_summary["intent_hash"] = plan_obj.get("intent_hash") if isinstance(plan_obj.get("intent_hash"), str) else None
            plan_summary["underlying"] = plan_obj.get("underlying") if isinstance(plan_obj.get("underlying"), str) else None
            plan_summary["structure"] = plan_obj.get("structure") if isinstance(plan_obj.get("structure"), str) else None
            plan_summary["schema_id"] = plan_obj.get("schema_id") if isinstance(plan_obj.get("schema_id"), str) else None
            plan_summary["schema_version"] = plan_obj.get("schema_version")

        # If plan_summary intent_hash missing, try mapping ledger intent_hash
        if plan_summary["intent_hash"] is None and isinstance(map_obj, dict):
            ih = map_obj.get("intent_hash")
            if isinstance(ih, str):
                plan_summary["intent_hash"] = ih

        # Engine join not provable from day inputs -> unknown
        engine_id = "unknown"
        item_warnings.append("ENGINE_JOIN_NOT_POSSIBLE_WITHOUT_ENGINE_LINKAGE")

        item = {
            "submission_id": submission_id or (Path(art_dir).name if art_dir else "unknown"),
            "day_utc": day_utc,
            "engine_id": engine_id,
            "binding_hash": binding_hash,
            "broker": broker,
            "broker_status": broker_status,
            "submitted_at_utc": submitted_at_utc,
            "broker_ids": broker_ids,
            "paths": {
                "submission_dir": art_dir,
                "broker_submission_record": p_broker,
                "execution_event_record": p_exec,
                "order_plan": p_plan,
                "binding_record": p_bind,
                "mapping_ledger_record": p_map,
                "manifest": str(mp),
            },
            "sha256": {
                "broker_submission_record": h_broker,
                "execution_event_record": h_exec,
                "order_plan": h_plan,
                "binding_record": h_bind,
                "mapping_ledger_record": h_map,
                "manifest": None,
            },
            "execution": exec_summary,
            "order_plan": plan_summary,
            "warnings": sorted(set(item_warnings)),
        }

        resp["items"].append(item)

        # record mtimes for referenced paths if they exist
        for p in [p_broker, p_exec, p_plan, p_bind, p_map]:
            if not p:
                continue
            mt2 = _mtime(Path(p))
            if mt2 is not None:
                resp["source_mtimes"][p] = mt2
                resp["source_paths"].append(p)

    # Finalize status
    if resp["missing_paths"]:
        resp["status"] = "DEGRADED" if resp["status"] == "OK" else resp["status"]

    resp["source_paths"] = sorted(set(resp["source_paths"]))
    resp["missing_paths"] = sorted(set(resp["missing_paths"]))
    resp["warnings"] = sorted(set(resp["warnings"]))
    return resp


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True, help="UTC day YYYY-MM-DD")
    ns = ap.parse_args(argv)

    day = ns.day
    out_dir = SUBMISSIONS_ROOT / day
    out_path = out_dir / "submission_index.v1.json"

    obj = build_submission_index(day_utc=day)
    b = _canonical_json_bytes(obj)

    try:
        _ = write_file_immutable_v1(path=out_path, data=b, create_dirs=True)
    except ImmutableWriteError as e:
        sys.stderr.write(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}\n")
        return 2

    sys.stderr.write(f"OK: SUBMISSION_INDEX_V1_WRITTEN day={day} path={out_path}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
