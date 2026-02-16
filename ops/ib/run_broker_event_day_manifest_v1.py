#!/usr/bin/env python3
"""
run_broker_event_day_manifest_v1.py

Writes immutable, governed manifests ("seals") for:
  constellation_2/runtime/truth/execution_evidence_v1/broker_events/<DAY>/broker_event_log.v1.jsonl

Institutional-grade properties:
- Deterministic
- Validates every parsed line against broker_event_raw.v1 schema (fail-closed)
- Computes sha256 for the full log file and basic counters
- Immutable (refuses rewrite unless identical)
- Runs without PYTHONPATH setup

Sealing model (hostile-review safe):
- If broker_event_day_manifest.v1.json does NOT exist: write it (first seal).
- Always also write a content-addressed seal:
    broker_event_day_manifest.v1.<manifest_sha256>.json
  This allows additional immutable seals if the log changes over time, without overwriting history.
"""

from __future__ import annotations

# --- Import bootstrap (audit-grade, deterministic, fail-closed) ---
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

RAW_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/broker_event_raw.v1.schema.json"
MAN_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/broker_event_day_manifest.v1.schema.json"

BROKER_EVENTS_ROOT = (TRUTH / "execution_evidence_v1/broker_events").resolve()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _parse_day_utc(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise ValueError(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {d!r}")
    return d


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _read_json_line(line: str) -> Dict[str, Any]:
    o = json.loads(line)
    if not isinstance(o, dict):
        raise ValueError("LINE_NOT_OBJECT")
    return o


def _seal_write(path: Path, payload_bytes: bytes) -> str:
    """
    Immutable write. Returns sha256 of payload bytes.
    """
    try:
        wr = write_file_immutable_v1(path=path, data=payload_bytes, create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL: IMMUTABLE_WRITE_ERROR: {e}") from e
    return wr.sha256


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_broker_event_day_manifest_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    day = _parse_day_utc(args.day_utc)

    day_dir = (BROKER_EVENTS_ROOT / day).resolve()
    log_path = (day_dir / "broker_event_log.v1.jsonl").resolve()

    # Fixed-name first seal (only if absent)
    out_fixed = (day_dir / "broker_event_day_manifest.v1.json").resolve()

    input_manifest: List[Dict[str, str]] = []
    reason_codes: List[str] = []
    notes: List[str] = []

    if not log_path.exists():
        status = "FAIL"
        reason_codes.append("MISSING_BROKER_EVENT_LOG")
        input_manifest.append({"type": "broker_event_log_missing", "path": str(log_path), "sha256": _sha256_bytes(b"")})

        manifest = {
            "schema_id": "broker_event_day_manifest",
            "schema_version": "v1",
            "day_utc": day,
            "produced_utc": _utc_now(),
            "producer": {"repo": "constellation_2_runtime", "module": "ops/ib/run_broker_event_day_manifest_v1.py", "git_sha": _git_sha()},
            "status": status,
            "reason_codes": reason_codes,
            "notes": notes,
            "input_manifest": input_manifest,
            "log": {
                "log_path": str(log_path),
                "log_sha256": _sha256_bytes(b""),
                "line_count": 0,
                "sequence_first": None,
                "sequence_last": None,
                "event_type_counts": {},
            },
        }

        validate_against_repo_schema_v1(manifest, REPO_ROOT, MAN_SCHEMA_RELPATH)

        canon = json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
        payload = canon.encode("utf-8")
        payload_sha = _sha256_text(canon)

        # Always write content-addressed seal
        out_hashed = (day_dir / f"broker_event_day_manifest.v1.{payload_sha}.json").resolve()
        _seal_write(out_hashed, payload)

        # Write fixed-name only if absent
        if not out_fixed.exists():
            _seal_write(out_fixed, payload)

        print(
            "OK: BROKER_EVENT_DAY_MANIFEST_SEALED "
            f"day_utc={day} status={status} fixed_exists={out_fixed.exists()} hashed_path={out_hashed} sha256={payload_sha}"
        )
        return 1

    log_sha = _sha256_file(log_path)
    input_manifest.append({"type": "broker_event_log_v1_jsonl", "path": str(log_path), "sha256": log_sha})

    line_count = 0
    seq_first: Optional[int] = None
    seq_last: Optional[int] = None
    event_type_counts: Dict[str, int] = {}

    with log_path.open("r", encoding="utf-8") as f:
        for raw in f:
            ln = raw.strip()
            if not ln:
                continue
            line_count += 1
            o = _read_json_line(ln)

            validate_against_repo_schema_v1(o, REPO_ROOT, RAW_SCHEMA_RELPATH)

            s = int(o.get("sequence_number"))
            if seq_first is None:
                seq_first = s
            seq_last = s

            et = str(o.get("event_type") or "")
            event_type_counts[et] = int(event_type_counts.get(et, 0) + 1)

    status = "OK"
    if line_count == 0:
        status = "FAIL"
        reason_codes.append("EMPTY_BROKER_EVENT_LOG")

    manifest = {
        "schema_id": "broker_event_day_manifest",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": _utc_now(),
        "producer": {"repo": "constellation_2_runtime", "module": "ops/ib/run_broker_event_day_manifest_v1.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": reason_codes,
        "notes": notes,
        "input_manifest": input_manifest,
        "log": {
            "log_path": str(log_path),
            "log_sha256": log_sha,
            "line_count": int(line_count),
            "sequence_first": seq_first,
            "sequence_last": seq_last,
            "event_type_counts": event_type_counts,
        },
    }

    validate_against_repo_schema_v1(manifest, REPO_ROOT, MAN_SCHEMA_RELPATH)

    canon = json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
    payload = canon.encode("utf-8")
    payload_sha = _sha256_text(canon)

    out_hashed = (day_dir / f"broker_event_day_manifest.v1.{payload_sha}.json").resolve()
    _seal_write(out_hashed, payload)

    if not out_fixed.exists():
        _seal_write(out_fixed, payload)

    print(
        "OK: BROKER_EVENT_DAY_MANIFEST_SEALED "
        f"day_utc={day} status={status} fixed_exists={out_fixed.exists()} hashed_path={out_hashed} sha256={payload_sha}"
    )
    return 0 if status == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
