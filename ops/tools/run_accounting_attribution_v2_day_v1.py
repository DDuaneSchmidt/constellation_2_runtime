#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = REPO_ROOT / "constellation_2/runtime/truth"

DAY0_RC_ALLOWED = "DAY0_BOOTSTRAP_ATTRIB_DEGRADED_OK"

def _bootstrap_window_true(day_utc: str) -> bool:
    """
    Day-0 Bootstrap Window iff:
      TRUTH/execution_evidence_v1/submissions/<DAY>/ is missing OR contains zero submission dirs.
    """
    root = (TRUTH_ROOT / "execution_evidence_v1" / "submissions" / day_utc).resolve()
    if (not root.exists()) or (not root.is_dir()):
        return True
    try:
        for p in root.iterdir():
            if p.is_dir():
                return False
    except Exception:
        return False
    return True



def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _immut_write(path: Path, content: bytes) -> None:
    if path.exists():
        if hashlib.sha256(path.read_bytes()).hexdigest() != hashlib.sha256(content).hexdigest():
            raise RuntimeError(f"ImmutableWriteError: ATTEMPTED_REWRITE path={path}")
        return
    _atomic_write(path, content)


def _load_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _return_if_existing_report(out_path: Path, expected_day_utc: str) -> int | None:
    """
    Immutable truth rule (audit-grade):
    - If report already exists at day-keyed path, DO NOT rewrite.
    - Treat existing report as authoritative for that day.
    - Return rc based on existing status:
        ACTIVE -> 0
        otherwise -> 2
    """
    if not out_path.exists():
        return None

    existing = _load_json(out_path)

    schema_id = str(existing.get("schema_id") or "").strip()
    day_utc = str(existing.get("day_utc") or "").strip()
    status = str(existing.get("status") or "").strip().upper()

    if schema_id != "C2_ACCOUNTING_ENGINE_ATTRIBUTION_V2":
        raise SystemExit(f"FAIL: EXISTING_REPORT_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
    if day_utc != expected_day_utc:
        raise SystemExit(f"FAIL: EXISTING_REPORT_DAY_MISMATCH: day_utc={day_utc!r} expected={expected_day_utc!r} path={out_path}")
    if status == "":
        raise SystemExit(f"FAIL: EXISTING_REPORT_STATUS_MISSING: path={out_path}")

    sha = _sha256_file(out_path)
    print(f"OK: accounting_attribution_v2_exists day_utc={expected_day_utc} status={status} path={out_path} sha256={sha} action=EXISTS")
    if status == "ACTIVE":
        return 0
    # Day-0 bootstrap: allow DEGRADED_MISSING_INPUTS to pass strict orchestrator stages.
    if status == "DEGRADED_MISSING_INPUTS" and _bootstrap_window_true(expected_day_utc):
        return 0
    return 2

def main() -> int:
    ap = argparse.ArgumentParser(prog="run_accounting_attribution_v2_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--producer_git_sha", required=True)
    ap.add_argument("--producer_repo", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()

    out_dir = TRUTH_ROOT / "accounting_v2" / "attribution" / day
    out_path = out_dir / "engine_attribution.v2.json"

    existing_rc = _return_if_existing_report(out_path=out_path, expected_day_utc=day)
    if existing_rc is not None:
        return int(existing_rc)

    pos_path = TRUTH_ROOT / "positions_v1" / "snapshots" / day / "positions_snapshot.v2.json"
    marks_path = TRUTH_ROOT / "market_data_snapshot_v1" / "broker_marks_v1" / day / "broker_marks.v1.json"
    link_path = TRUTH_ROOT / "engine_linkage_v1" / "snapshots" / day / "engine_linkage.v1.json"

    missing = []
    for p in [pos_path, marks_path, link_path]:
        if not p.exists():
            missing.append(str(p.relative_to(TRUTH_ROOT)))

    status = "ACTIVE"
    reason_codes: List[str] = []
    notes: List[str] = []

    if missing:
        status = "DEGRADED_MISSING_INPUTS"
        reason_codes.append("MISSING_INPUTS")
        notes.extend([f"MISSING: {m}" for m in missing])

    by_engine: List[Dict[str, Any]] = []
    currency = "USD"

    if status == "ACTIVE":
        pos = _load_json(pos_path)
        items = (((pos.get("positions") or {}).get("items")) or [])
        if not items:
            status = "ACTIVE"
            reason_codes.append("NO_POSITIONS")
            notes.append("SAFE_IDLE: positions snapshot empty; attribution empty.")
        else:
            status = "DEGRADED_NOT_IMPLEMENTED"
            reason_codes.append("JOIN_KEYS_NOT_PROVEN")
            notes.append("Positions present but join keys for linkage+marks not yet proven in this environment.")

    out = {
        "schema_id": "C2_ACCOUNTING_ENGINE_ATTRIBUTION_V2",
        "schema_version": 2,
        "produced_utc": f"{day}T00:00:00Z",
        "day_utc": day,
        "producer": {"repo": args.producer_repo, "git_sha": args.producer_git_sha, "module": "ops/tools/run_accounting_attribution_v2_day_v1.py"},
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "input_manifest": [
            {"type": "positions_truth", "path": str(pos_path), "sha256": _sha256_file(pos_path) if pos_path.exists() else "0" * 64, "day_utc": day, "producer": "positions_v1"},
            {"type": "broker_marks", "path": str(marks_path), "sha256": _sha256_file(marks_path) if marks_path.exists() else "0" * 64, "day_utc": day, "producer": "broker_marks_v1"},
            {"type": "engine_linkage", "path": str(link_path), "sha256": _sha256_file(link_path) if link_path.exists() else "0" * 64, "day_utc": day, "producer": "engine_linkage_v1"},
        ],
        "attribution": {
            "currency": currency,
            "by_engine": by_engine,
            "notes": notes,
        },
    }

    _immut_write(out_path, _json_bytes(out))

    print(f"OK: wrote {out_path}")
    if status == "ACTIVE":
        return 0
    if status == "DEGRADED_MISSING_INPUTS" and _bootstrap_window_true(day):
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
