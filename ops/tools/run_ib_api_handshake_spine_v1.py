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
- truth/ib_api_handshake/latest_pointer.v1.json (immutable pointer; monotonic day only)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root
from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.day_artifact_refresh_v1 import write_day_artifact_refreshable_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1


TRUTH_ROOT = resolve_canonical_truth_root().resolve()

SCHEMA_HANDSHAKE = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/ib_api_handshake.v1.schema.json"
SCHEMA_LATEST_PTR = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/ib_api_handshake_latest_pointer.v1.schema.json"
# Authoritative broker-events surface is execution_evidence_v1.
AUTH_BROKER_EVENTS_ROOT = (TRUTH_ROOT / "execution_evidence_v1" / "broker_events").resolve()
DEFAULT_ENVIRONMENT = "PAPER"


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


def _paths_for_day(day_utc: str, *, truth_root: Path, broker_events_root: Path) -> Paths:
    day_dir = (truth_root / "ib_api_handshake" / day_utc).resolve()
    out_path = (day_dir / "ib_api_handshake.v1.json").resolve()
    latest_path = (truth_root / "ib_api_handshake" / "latest_pointer.v1.json").resolve()
    broker_events_path = (broker_events_root / day_utc / "broker_event_log.v1.jsonl").resolve()
    return Paths(day_dir=day_dir, out_path=out_path, latest_path=latest_path, broker_events_path=broker_events_path)


def _resolve_truth_and_events_roots(truth_root_arg: str) -> tuple[Path, Path]:
    raw = str(truth_root_arg or "").strip()
    if raw:
        truth_root = Path(raw).resolve()
        broker_events_root = (truth_root / "execution_evidence_v1" / "broker_events").resolve()
        return truth_root, broker_events_root
    return TRUTH_ROOT, AUTH_BROKER_EVENTS_ROOT


def _build_latest_ptr(day_utc: str, out_path: Path, out_sha256: str, *, status: str, reason_codes: List[str]) -> Dict[str, Any]:
    return {
        "schema_id": "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "status": str(status).strip().upper(),
        "reason_codes": [str(code).strip() for code in reason_codes if str(code).strip()],
        "pointers": {
            "handshake_path": str(out_path),
            "handshake_sha256": out_sha256,
        },
    }


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:path={path}")
    return obj


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify_written_handshake_outputs(
    *,
    day_utc: str,
    paths: Paths,
    out_sha256: str,
    expected_status: str,
    expected_ok: bool,
    expected_reason_codes: List[str],
    expected_environment: str,
    expected_ib_account: str,
) -> None:
    if not paths.out_path.exists() or not paths.out_path.is_file():
        raise SystemExit(f"FAIL: IB_API_HANDSHAKE_ARTIFACT_VERIFY_MISSING:path={paths.out_path}")
    handshake = _read_json_obj(paths.out_path)
    if str(handshake.get("schema_id") or "").strip() != "C2_IB_API_HANDSHAKE_V1" or int(handshake.get("schema_version") or 0) != 1:
        raise SystemExit(f"FAIL: IB_API_HANDSHAKE_ARTIFACT_VERIFY_SCHEMA:path={paths.out_path}")
    if str(handshake.get("day_utc") or "").strip() != day_utc:
        raise SystemExit(f"FAIL: IB_API_HANDSHAKE_ARTIFACT_VERIFY_DAY:path={paths.out_path}")
    if str(handshake.get("status") or "").strip().upper() != expected_status:
        raise SystemExit(f"FAIL: IB_API_HANDSHAKE_ARTIFACT_VERIFY_STATUS:path={paths.out_path}")
    if bool(handshake.get("ok") is True) != bool(expected_ok):
        raise SystemExit(f"FAIL: IB_API_HANDSHAKE_ARTIFACT_VERIFY_OK:path={paths.out_path}")
    if sorted(str(code).strip() for code in (handshake.get("reason_codes") or []) if str(code).strip()) != sorted(expected_reason_codes):
        raise SystemExit(f"FAIL: IB_API_HANDSHAKE_ARTIFACT_VERIFY_REASON_CODES:path={paths.out_path}")
    if str(handshake.get("environment") or "").strip().upper() != expected_environment:
        raise SystemExit(f"FAIL: IB_API_HANDSHAKE_ARTIFACT_VERIFY_ENVIRONMENT:path={paths.out_path}")
    if str(handshake.get("ib_account") or "").strip() != expected_ib_account:
        raise SystemExit(f"FAIL: IB_API_HANDSHAKE_ARTIFACT_VERIFY_ACCOUNT:path={paths.out_path}")
    if _sha256_file(paths.out_path) != out_sha256:
        raise SystemExit(f"FAIL: IB_API_HANDSHAKE_ARTIFACT_VERIFY_SHA256:path={paths.out_path}")

    if not paths.latest_path.exists() or not paths.latest_path.is_file():
        raise SystemExit(f"FAIL: IB_API_HANDSHAKE_POINTER_VERIFY_MISSING:path={paths.latest_path}")
    latest = _read_json_obj(paths.latest_path)
    if str(latest.get("schema_id") or "").strip() != "C2_IB_API_HANDSHAKE_LATEST_POINTER_V1" or int(latest.get("schema_version") or 0) != 1:
        raise SystemExit(f"FAIL: IB_API_HANDSHAKE_POINTER_VERIFY_SCHEMA:path={paths.latest_path}")
    latest_day = str(latest.get("day_utc") or "").strip()
    if not latest_day or latest_day < day_utc:
        raise SystemExit(f"FAIL: IB_API_HANDSHAKE_POINTER_VERIFY_DAY:path={paths.latest_path}")
    if latest_day == day_utc:
        if str(latest.get("status") or "").strip().upper() != expected_status:
            raise SystemExit(f"FAIL: IB_API_HANDSHAKE_POINTER_VERIFY_STATUS:path={paths.latest_path}")
        if sorted(str(code).strip() for code in (latest.get("reason_codes") or []) if str(code).strip()) != sorted(expected_reason_codes):
            raise SystemExit(f"FAIL: IB_API_HANDSHAKE_POINTER_VERIFY_REASON_CODES:path={paths.latest_path}")
        pointers = latest.get("pointers")
        if not isinstance(pointers, dict):
            raise SystemExit(f"FAIL: IB_API_HANDSHAKE_POINTER_VERIFY_POINTERS:path={paths.latest_path}")
        if str(pointers.get("handshake_path") or "").strip() != str(paths.out_path):
            raise SystemExit(f"FAIL: IB_API_HANDSHAKE_POINTER_VERIFY_PATH:path={paths.latest_path}")
        if str(pointers.get("handshake_sha256") or "").strip() != out_sha256:
            raise SystemExit(f"FAIL: IB_API_HANDSHAKE_POINTER_VERIFY_SHA256:path={paths.latest_path}")


def _atomic_replace_file(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    tmp.write_bytes(data)
    fd = os.open(str(tmp), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(str(tmp), str(path))
    dir_fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _write_latest_pointer_if_monotonic(
    *,
    day_utc: str,
    paths: Paths,
    out_sha256: str,
    status: str,
    reason_codes: List[str],
) -> None:
    skip_latest = False
    if paths.latest_path.exists():
        try:
            latest_obj = json.loads(paths.latest_path.read_text(encoding="utf-8"))
            latest_day = str(latest_obj.get("day_utc") or "").strip()
        except Exception:
            latest_day = ""
        if latest_day and day_utc < latest_day:
            skip_latest = True

    if skip_latest:
        return

    latest_ptr = _build_latest_ptr(day_utc, paths.out_path, out_sha256, status=status, reason_codes=reason_codes)
    validate_against_repo_schema_v1(latest_ptr, REPO_ROOT, SCHEMA_LATEST_PTR)
    latest_bytes = canonical_json_bytes_v1(latest_ptr) + b"\n"
    if paths.latest_path.exists() and paths.latest_path.read_bytes() == latest_bytes:
        return
    _atomic_replace_file(paths.latest_path, latest_bytes)


def _write_handshake_doc(*, day_utc: str, paths: Paths, doc: Dict[str, Any]) -> str:
    validate_against_repo_schema_v1(doc, REPO_ROOT, SCHEMA_HANDSHAKE)
    try:
        payload = canonical_json_bytes_v1(doc) + b"\n"
    except CanonicalizationError as e:
        raise SystemExit(f"FAIL: CANONICALIZATION_ERROR: {e}") from e
    wr = write_day_artifact_refreshable_v1(
        path=paths.out_path,
        data=payload,
        expected_day_utc=day_utc,
        expected_schema_id="C2_IB_API_HANDSHAKE_V1",
        expected_schema_version=1,
        preserve_statuses=(),
    )
    return wr.sha256


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_ib_api_handshake_spine_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--truth_root", default="", help="Optional canonical truth root override.")
    ap.add_argument("--environment", default=DEFAULT_ENVIRONMENT, help="Expected environment label written into the handshake artifact.")
    ap.add_argument("--ib_account", default="", help="Expected IB account written into the handshake artifact.")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    environment = str(args.environment or DEFAULT_ENVIRONMENT).strip().upper()
    ib_account = str(args.ib_account or "").strip()
    truth_root, broker_events_root = _resolve_truth_and_events_roots(args.truth_root)
    p = _paths_for_day(day_utc, truth_root=truth_root, broker_events_root=broker_events_root)

    if not p.broker_events_path.exists():
        doc = {
            "schema_id": "C2_IB_API_HANDSHAKE_V1",
            "schema_version": 1,
            "day_utc": day_utc,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "status": "FAIL",
            "ok": False,
            "reason_codes": ["BROKER_EVENTS_MISSING"],
            "environment": environment,
            "ib_account": ib_account,
            "inputs": {"broker_event_log": str(p.broker_events_path)},
            "observations": {},
        }
        try:
            out_sha256 = _write_handshake_doc(day_utc=day_utc, paths=p, doc=doc)
            _write_latest_pointer_if_monotonic(
                day_utc=day_utc,
                paths=p,
                out_sha256=out_sha256,
                status=doc["status"],
                reason_codes=list(doc["reason_codes"]),
            )
            _verify_written_handshake_outputs(
                day_utc=day_utc,
                paths=p,
                out_sha256=out_sha256,
                expected_status=doc["status"],
                expected_ok=bool(doc["ok"]),
                expected_reason_codes=list(doc["reason_codes"]),
                expected_environment=environment,
                expected_ib_account=ib_account,
            )
        except (ImmutableWriteError, SystemExit) as e:
            print(str(e), file=sys.stderr)
            return 4
        print(f"FAIL: BROKER_EVENTS_MISSING day_utc={day_utc} path={p.out_path}")
        return 2

    lines = _read_jsonl_lines(p.broker_events_path)

    last_next_valid_idx: Optional[int] = None
    last_next_valid_order_id: Optional[int] = None

    for i, line in enumerate(lines):
        evt = _parse_broker_event_line(line)
        if evt is None:
            continue
        if _event_is_next_valid_id(evt):
            last_next_valid_idx = i
            last_next_valid_order_id = _extract_order_id_from_next_valid_id(evt)

    if last_next_valid_idx is None:
        doc = {
            "schema_id": "C2_IB_API_HANDSHAKE_V1",
            "schema_version": 1,
            "day_utc": day_utc,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "status": "FAIL",
            "ok": False,
            "reason_codes": ["NO_NEXT_VALID_ID_OBSERVED"],
            "environment": environment,
            "ib_account": ib_account,
            "inputs": {"broker_event_log": str(p.broker_events_path)},
            "observations": {"lines_total": len(lines)},
        }
        try:
            out_sha256 = _write_handshake_doc(day_utc=day_utc, paths=p, doc=doc)
            _write_latest_pointer_if_monotonic(
                day_utc=day_utc,
                paths=p,
                out_sha256=out_sha256,
                status=doc["status"],
                reason_codes=list(doc["reason_codes"]),
            )
            _verify_written_handshake_outputs(
                day_utc=day_utc,
                paths=p,
                out_sha256=out_sha256,
                expected_status=doc["status"],
                expected_ok=bool(doc["ok"]),
                expected_reason_codes=list(doc["reason_codes"]),
                expected_environment=environment,
                expected_ib_account=ib_account,
            )
        except (ImmutableWriteError, SystemExit) as e:
            print(str(e), file=sys.stderr)
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
        "produced_utc": f"{day_utc}T00:00:00Z",
        "status": status,
        "ok": ok,
        "reason_codes": reason_codes,
        "environment": environment,
        "ib_account": ib_account,
        "inputs": {"broker_event_log": str(p.broker_events_path)},
        "observations": {
            "lines_total": len(lines),
            "last_next_valid_id_line": int(last_next_valid_idx + 1),
            "next_valid_order_id": last_next_valid_order_id,
        },
    }

    try:
        out_sha256 = _write_handshake_doc(day_utc=day_utc, paths=p, doc=doc)
    except (ImmutableWriteError, SystemExit) as e:
        print(str(e), file=sys.stderr)
        return 4

    try:
        _write_latest_pointer_if_monotonic(
            day_utc=day_utc,
            paths=p,
            out_sha256=out_sha256,
            status=doc["status"],
            reason_codes=list(doc["reason_codes"]),
        )
        _verify_written_handshake_outputs(
            day_utc=day_utc,
            paths=p,
            out_sha256=out_sha256,
            expected_status=doc["status"],
            expected_ok=bool(doc["ok"]),
            expected_reason_codes=list(doc["reason_codes"]),
            expected_environment=environment,
            expected_ib_account=ib_account,
        )
    except ImmutableWriteError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 4

    print(f"OK: IB_API_HANDSHAKE_V1_WRITTEN day_utc={day_utc} ok={ok} path={p.out_path} sha256={out_sha256}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
