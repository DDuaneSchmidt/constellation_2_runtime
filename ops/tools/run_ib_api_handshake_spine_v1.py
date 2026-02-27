#!/usr/bin/env python3
"""
run_ib_api_handshake_spine_v1.py

C2: IB API Handshake Spine v1

Produces an authoritative handshake artifact by consuming C2 execution observer broker events.
Fail-closed semantics:
- If broker_event_log missing -> FAIL
- If no nextValidId observed -> FAIL
- If any "Not connected" (504) appears AFTER last nextValidId -> FAIL
- Else -> OK

Writes:
- truth/ib_api_handshake/<DAY_UTC>/ib_api_handshake.v1.json (immutable)
- truth/ib_api_handshake/latest.json (immutable pointer; monotonic day only)
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1


REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_HANDSHAKE = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/ib_api_handshake.v1.schema.json"
SCHEMA_LATEST_PTR = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/ib_api_handshake_latest_pointer.v1.schema.json"


@dataclass(frozen=True)
class Paths:
    day_dir: Path
    out_path: Path
    latest_path: Path
    broker_events_path: Path


def _read_jsonl_lines(path: Path) -> List[str]:
    return path.read_text(encoding="utf-8", errors="replace").splitlines()


def _parse_broker_event_line(line: str) -> Optional[Dict[str, Any]]:
    try:
        obj = json.loads(line)
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    return obj


def _event_has_504_not_connected(evt: Dict[str, Any]) -> bool:
    if evt.get("event_type") != "error":
        return False
    args = (((evt.get("ib_fields") or {}).get("args")) or [])
    s = " ".join([str(a.get("value", "")) for a in args if isinstance(a, dict)])
    return ("errorCode=504" in s) and ("Not connected" in s)


def _event_is_next_valid_id(evt: Dict[str, Any]) -> bool:
    return evt.get("event_type") == "nextValidId"


def _extract_order_id_from_next_valid_id(evt: Dict[str, Any]) -> Optional[int]:
    args = (((evt.get("ib_fields") or {}).get("args")) or [])
    s = " ".join([str(a.get("value", "")) for a in args if isinstance(a, dict)])
    # example: "orderId=29"
    for tok in s.split():
        if tok.startswith("orderId="):
            v = tok.split("=", 1)[1].strip()
            if v.isdigit():
                return int(v)
    return None


def _paths_for_day(day_utc: str) -> Paths:
    day_dir = (TRUTH_ROOT / "ib_api_handshake" / day_utc).resolve()
    out_path = (day_dir / "ib_api_handshake.v1.json").resolve()
    latest_path = (TRUTH_ROOT / "ib_api_handshake" / "latest.json").resolve()
    broker_events_path = (TRUTH_ROOT / "execution_evidence_v2/broker_events" / day_utc / "broker_event_log.v1.jsonl").resolve()
    return Paths(day_dir=day_dir, out_path=out_path, latest_path=latest_path, broker_events_path=broker_events_path)


def _build_latest_ptr(day_utc: str, out_path: Path, out_sha256: str) -> Dict[str, Any]:
    return {
        "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
        "schema_version": 1,
        "day_utc": day_utc,
        "pointers": {
            "handshake_path": str(out_path),
            "handshake_sha256": out_sha256,
        },
    }


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_ib_api_handshake_spine_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    p = _paths_for_day(day_utc)

    if not p.broker_events_path.exists():
        doc = {
            "schema_id": "C2_IB_API_HANDSHAKE_V1",
            "schema_version": 1,
            "day_utc": day_utc,
            "status": "FAIL",
            "ok": False,
            "reason_codes": ["BROKER_EVENTS_MISSING"],
            "inputs": {"broker_event_log": str(p.broker_events_path)},
            "observations": {},
        }
        validate_against_repo_schema_v1(doc, REPO_ROOT, SCHEMA_HANDSHAKE)
        payload = canonical_json_bytes_v1(doc) + b"\n"
        try:
            _ = write_file_immutable_v1(path=p.out_path, data=payload, create_dirs=True)
        except ImmutableWriteError as e:
            print(f"FAIL: {e}", file=sys.stderr)
            return 4
        print(f"FAIL: BROKER_EVENTS_MISSING day_utc={day_utc} path={p.out_path}")
        return 2

    lines = _read_jsonl_lines(p.broker_events_path)

    last_next_valid_idx: Optional[int] = None
    last_next_valid_order_id: Optional[int] = None

    parsed: List[Dict[str, Any]] = []
    for i, line in enumerate(lines):
        evt = _parse_broker_event_line(line)
        if evt is None:
            continue
        parsed.append(evt)
        if _event_is_next_valid_id(evt):
            last_next_valid_idx = i
            last_next_valid_order_id = _extract_order_id_from_next_valid_id(evt)

    if last_next_valid_idx is None:
        doc = {
            "schema_id": "C2_IB_API_HANDSHAKE_V1",
            "schema_version": 1,
            "day_utc": day_utc,
            "status": "FAIL",
            "ok": False,
            "reason_codes": ["NO_NEXT_VALID_ID_OBSERVED"],
            "inputs": {"broker_event_log": str(p.broker_events_path)},
            "observations": {"lines_total": len(lines)},
        }
        validate_against_repo_schema_v1(doc, REPO_ROOT, SCHEMA_HANDSHAKE)
        payload = canonical_json_bytes_v1(doc) + b"\n"
        try:
            _ = write_file_immutable_v1(path=p.out_path, data=payload, create_dirs=True)
        except ImmutableWriteError as e:
            print(f"FAIL: {e}", file=sys.stderr)
            return 4
        print(f"FAIL: NO_NEXT_VALID_ID_OBSERVED day_utc={day_utc} path={p.out_path}")
        return 2

    # Now enforce: no 504 Not connected AFTER last nextValidId
    any_504_after = False
    for i in range(last_next_valid_idx + 1, len(lines)):
        evt = _parse_broker_event_line(lines[i])
        if evt is None:
            continue
        if _event_has_504_not_connected(evt):
            any_504_after = True
            break

    if any_504_after:
        status = "FAIL"
        ok = False
        reason_codes = ["NOT_CONNECTED_AFTER_HANDSHAKE"]
    else:
        status = "OK"
        ok = True
        reason_codes = ["HANDSHAKE_OK_NEXTVALIDID_SEEN_NO_504_AFTER"]

    doc = {
        "schema_id": "C2_IB_API_HANDSHAKE_V1",
        "schema_version": 1,
        "day_utc": day_utc,
        "status": status,
        "ok": ok,
        "reason_codes": reason_codes,
        "inputs": {"broker_event_log": str(p.broker_events_path)},
        "observations": {
            "lines_total": len(lines),
            "last_next_valid_id_line": int(last_next_valid_idx + 1),
            "next_valid_order_id": last_next_valid_order_id,
        },
    }

    validate_against_repo_schema_v1(doc, REPO_ROOT, SCHEMA_HANDSHAKE)
    try:
        payload = canonical_json_bytes_v1(doc) + b"\n"
    except CanonicalizationError as e:
        print(f"FAIL: CANONICALIZATION_ERROR: {e}", file=sys.stderr)
        return 4

    try:
        wr = write_file_immutable_v1(path=p.out_path, data=payload, create_dirs=True)
    except ImmutableWriteError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 4

    # Latest pointer: monotonic day only. If latest exists with a later day, skip.
    skip_latest = False
    if p.latest_path.exists():
        try:
            latest_obj = json.loads(p.latest_path.read_text(encoding="utf-8"))
            latest_day = str(latest_obj.get("day_utc") or "").strip()
        except Exception:
            latest_day = ""
        if latest_day and day_utc < latest_day:
            skip_latest = True

    if not skip_latest:
        latest_ptr = _build_latest_ptr(day_utc, p.out_path, wr.sha256)
        validate_against_repo_schema_v1(latest_ptr, REPO_ROOT, SCHEMA_LATEST_PTR)
        latest_bytes = canonical_json_bytes_v1(latest_ptr) + b"\n"
        try:
            _ = write_file_immutable_v1(path=p.latest_path, data=latest_bytes, create_dirs=True)
        except ImmutableWriteError as e:
            print(f"FAIL: {e}", file=sys.stderr)
            return 4

    print(f"OK: IB_API_HANDSHAKE_V1_WRITTEN day_utc={day_utc} ok={ok} path={p.out_path} sha256={wr.sha256}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
