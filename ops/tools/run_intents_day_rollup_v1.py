#!/usr/bin/env python3
"""
Bundle A: intents_day_rollup.v1.json writer (immutable truth artifact).

Institutional posture:
- Deterministic, audit-grade, fail-closed.
- Reads intents from:
    constellation_2/runtime/truth/intents_v1/snapshots/<DAY>/*.exposure_intent.v1.json
- Verifies filename hash matches file bytes sha256.
- Parses each intent JSON, validates against governed exposure intent schema.
- Uses engine attribution from intent["engine"]["engine_id"] (required by schema).
- Emits rollup enumerating the configured engines, including explicit zero-intent engines.

Output:
  constellation_2/runtime/truth/intents_v1/day_rollup/<DAY>/intents_day_rollup.v1.json

Schema (governed):
  governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/intents_day_rollup.v1.schema.json

Run:
  python3 ops/tools/run_intents_day_rollup_v1.py --day_utc YYYY-MM-DD
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


# --- Import bootstrap (audit-grade, deterministic) ---
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # type: ignore
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # type: ignore


TRUTH = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

ROLLUP_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/intents_day_rollup.v1.schema.json"
EXPOSURE_INTENT_SCHEMA_RELPATH = "constellation_2/schemas/exposure_intent.v1.schema.json"

SNAP_ROOT = (TRUTH / "intents_v1" / "snapshots").resolve()
OUT_ROOT = (TRUTH / "intents_v1" / "day_rollup").resolve()

DAY_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")

# Institutional: explicit engine inventory for Bundle A.
ALLOWED_ENGINE_IDS: List[str] = [
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
]


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _compute_self_sha_field(obj: Dict[str, Any], field_name: str) -> str:
    obj2 = dict(obj)
    obj2[field_name] = None
    canon = canonical_json_bytes_v1(obj2) + b"\n"
    return _sha256_bytes(canon)


@dataclass(frozen=True)
class _WriteResult:
    path: str
    sha256: str
    action: str


def _write_immutable_canonical_json(path: Path, obj: Dict[str, Any]) -> _WriteResult:
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = canonical_json_bytes_v1(obj) + b"\n"
    sha = _sha256_bytes(payload)

    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == sha:
            return _WriteResult(path=str(path), sha256=sha, action="EXISTS_IDENTICAL")
        raise SystemExit(f"FAIL: refusing overwrite (different bytes): {path}")

    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    os.replace(tmp, path)
    return _WriteResult(path=str(path), sha256=sha, action="WRITTEN")


def _list_intent_files(day_dir: Path) -> List[Path]:
    if not day_dir.exists():
        return []
    return sorted(day_dir.glob("*.exposure_intent.v1.json"))


def _validate_intent_file_hash(p: Path) -> str:
    # File name begins with sha prefix.
    prefix = p.name.split(".")[0].strip().lower()
    b = p.read_bytes()
    sha = _sha256_bytes(b).lower()
    if prefix != sha:
        raise SystemExit(f"FAIL: intent file hash mismatch: file={p} name_prefix={prefix} sha256={sha}")
    return sha


def _extract_engine_id(intent: Dict[str, Any], p: Path) -> str:
    eng = intent.get("engine")
    if not isinstance(eng, dict):
        raise SystemExit(f"FAIL: intent missing engine object: file={p}")
    eid = str(eng.get("engine_id") or "").strip()
    if eid == "":
        raise SystemExit(f"FAIL: intent missing engine.engine_id: file={p}")
    return eid


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_intents_day_rollup_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if not DAY_RE.match(day):
        raise SystemExit(f"FAIL: bad --day_utc (expected YYYY-MM-DD): {day!r}")

    produced_utc = _now_utc_iso()

    day_dir = (SNAP_ROOT / day).resolve()
    files = _list_intent_files(day_dir)

    # Engine -> list of intent hashes
    grouped: Dict[str, List[str]] = {eid: [] for eid in ALLOWED_ENGINE_IDS}

    # Validate and group
    for p in files:
        ih = _validate_intent_file_hash(p)
        intent = _read_json(p)

        # Validate intent schema (governed schema lives in repo at the path below)
        validate_against_repo_schema_v1(intent, REPO_ROOT, EXPOSURE_INTENT_SCHEMA_RELPATH)

        eid = _extract_engine_id(intent, p)
        if eid not in grouped:
            raise SystemExit(f"FAIL: intent engine_id not allowed: engine_id={eid!r} file={p}")

        grouped[eid].append(ih)

    engines_out: List[Dict[str, Any]] = []
    for eid in ALLOWED_ENGINE_IDS:
        hs = grouped.get(eid, [])
        engines_out.append(
            {
                "engine_id": eid,
                "intent_type": "exposure_intent.v1",
                "intent_hashes": hs,
                "intent_count": int(len(hs)),
            }
        )

    payload: Dict[str, Any] = {
        "schema_id": "intents_day_rollup.v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"component": "ops/tools/run_intents_day_rollup_v1.py", "version": "v1", "git_sha": _git_sha()},
        "inputs": {
            # This rollup is intentionally limited to intent enumeration + attribution.
            # Market snapshot hashes belong in engine runtime provenance; this contract does not infer them.
            "market_data_snapshot_hashes": [],
            "market_calendar_hash": "",
            "engine_config_hashes": [],
        },
        "engines": engines_out,
        "rollup_sha256": None,
    }
    payload["rollup_sha256"] = _compute_self_sha_field(payload, "rollup_sha256")

    # Validate rollup against governed schema
    validate_against_repo_schema_v1(intent, REPO_ROOT, EXPOSURE_INTENT_SCHEMA_RELPATH)

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "intents_day_rollup.v1.json").resolve()
    wr = _write_immutable_canonical_json(out_path, payload)

    print(f"OK: INTENTS_DAY_ROLLUP_WRITTEN day_utc={day} path={wr.path} sha256={wr.sha256} action={wr.action} intents_total={len(files)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
