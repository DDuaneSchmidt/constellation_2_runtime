#!/usr/bin/env python3
"""
run_activity_ledger_rollup_day_v1.py

Writes cumulative rollup as-of a given day (immutable per day):
  constellation_2/runtime/truth/monitoring_v1/activity_ledger_rollup_v1/<ASOF_DAY>/activity_ledger_rollup.v1.json

Rollup inputs:
- monitoring_v1/intents_summary_v1/<DAY>/intents_summary.v1.json
- monitoring_v1/submissions_summary_v1/<DAY>/submissions_summary.v1.json

If a day has no summary yet, it is treated as zero for that category.
"""

from __future__ import annotations

import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

import argparse
import hashlib
import json
import subprocess
from typing import Any, Dict, List, Optional

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

INT_SUM_ROOT = (TRUTH / "monitoring_v1" / "intents_summary_v1").resolve()
SUB_SUM_ROOT = (TRUTH / "monitoring_v1" / "submissions_summary_v1").resolve()
OUT_ROOT = (TRUTH / "monitoring_v1" / "activity_ledger_rollup_v1").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/activity_ledger_rollup.v1.schema.json"


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    return out.decode("utf-8").strip()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _canonical_bytes(obj: Dict[str, Any]) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _read_json_obj(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        o = json.load(f)
    if not isinstance(o, dict):
        raise ValueError("TOP_LEVEL_NOT_OBJECT")
    return o


def _return_if_existing(out_path: Path, asof_day: str) -> int | None:
    if not out_path.exists():
        return None
    existing = _read_json_obj(out_path)
    if str(existing.get("schema_id") or "") != "activity_ledger_rollup":
        raise SystemExit(f"FAIL: EXISTING_SCHEMA_MISMATCH path={out_path}")
    if str(existing.get("schema_version") or "") != "v1":
        raise SystemExit(f"FAIL: EXISTING_SCHEMA_VERSION_MISMATCH path={out_path}")
    if str(existing.get("asof_day_utc") or "") != asof_day:
        raise SystemExit(f"FAIL: EXISTING_ASOF_DAY_MISMATCH path={out_path}")
    sha = _sha256_file(out_path)
    print(f"OK: ACTIVITY_LEDGER_ROLLUP_V1_WRITTEN asof_day_utc={asof_day} path={out_path} sha256={sha} action=EXISTS")
    return 0


def _list_days() -> List[str]:
    days = set()
    for root in [INT_SUM_ROOT, SUB_SUM_ROOT]:
        if root.exists() and root.is_dir():
            for p in root.iterdir():
                if p.is_dir() and len(p.name) == 10 and p.name[4] == "-" and p.name[7] == "-":
                    days.add(p.name)
    return sorted(days)


def _load_count(path: Path, key: str) -> int:
    if not path.exists():
        return 0
    o = _read_json_obj(path)
    counts = o.get("counts")
    if isinstance(counts, dict) and key in counts:
        try:
            return int(counts.get(key))
        except Exception:
            return 0
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_activity_ledger_rollup_day_v1")
    ap.add_argument("--asof_day_utc", required=True)
    args = ap.parse_args()

    asof = str(args.asof_day_utc).strip()
    if len(asof) != 10 or asof[4] != "-" or asof[7] != "-":
        raise SystemExit(f"BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {asof!r}")

    out_path = (OUT_ROOT / asof / "activity_ledger_rollup.v1.json").resolve()
    existing_rc = _return_if_existing(out_path, asof)
    if existing_rc is not None:
        return int(existing_rc)

    days_all = _list_days()
    days = [d for d in days_all if d <= asof]

    input_manifest: List[Dict[str, str]] = []
    reason_codes: List[str] = []
    notes: List[str] = []

    if not days:
        notes.append("No daily summaries found; totals are zero.")
        reason_codes.append("NO_SUMMARY_DAYS_FOUND_TREAT_AS_ZERO")

    rows: List[Dict[str, Any]] = []
    total_intents = 0
    total_subs = 0

    for d in days:
        ip = (INT_SUM_ROOT / d / "intents_summary.v1.json").resolve()
        sp = (SUB_SUM_ROOT / d / "submissions_summary.v1.json").resolve()

        intents = _load_count(ip, "intents_total")
        subs = _load_count(sp, "submissions_total")

        total_intents += intents
        total_subs += subs

        rows.append({"day_utc": d, "intents_total": intents, "submissions_total": subs})

        input_manifest.append({"type": "intents_summary_v1", "path": str(ip), "sha256": (_sha256_file(ip) if ip.exists() else _sha256_bytes(b""))})
        input_manifest.append({"type": "submissions_summary_v1", "path": str(sp), "sha256": (_sha256_file(sp) if sp.exists() else _sha256_bytes(b""))})

    obj: Dict[str, Any] = {
        "schema_id": "activity_ledger_rollup",
        "schema_version": "v1",
        "produced_utc": f"{asof}T00:00:00Z",
        "asof_day_utc": asof,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_activity_ledger_rollup_day_v1.py", "git_sha": _git_sha()},
        "status": "OK",
        "totals": {"intents_total": int(total_intents), "submissions_total": int(total_subs)},
        "days": rows,
        "input_manifest": input_manifest,
        "reason_codes": sorted(list(dict.fromkeys(reason_codes))),
        "notes": notes,
    }

    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)

    try:
        (OUT_ROOT / asof).mkdir(parents=True, exist_ok=True)
        _ = write_file_immutable_v1(path=out_path, data=_canonical_bytes(obj), create_dirs=False)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL_IMMUTABLE_WRITE: {e}") from e

    sha = _sha256_file(out_path)
    print(f"OK: ACTIVITY_LEDGER_ROLLUP_V1_WRITTEN asof_day_utc={asof} path={out_path} sha256={sha} action=WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
