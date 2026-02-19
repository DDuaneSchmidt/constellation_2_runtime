#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH_ROOT = REPO_ROOT / "constellation_2/runtime/truth"

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

def main() -> int:
    ap = argparse.ArgumentParser(prog="run_accounting_attribution_v2_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--producer_git_sha", required=True)
    ap.add_argument("--producer_repo", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()

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
            # Non-empty path will be implemented once you have real positions + identity keys.
            status = "DEGRADED_NOT_IMPLEMENTED"
            reason_codes.append("JOIN_KEYS_NOT_PROVEN")
            notes.append("Positions present but join keys for linkage+marks not yet proven in this environment.")

    out = {
        "schema_id": "C2_ACCOUNTING_ENGINE_ATTRIBUTION_V2",
        "schema_version": 2,
        "produced_utc": __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "day_utc": day,
        "producer": {"repo": args.producer_repo, "git_sha": args.producer_git_sha, "module": "ops/tools/run_accounting_attribution_v2_day_v1.py"},
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "input_manifest": [
            {"type": "positions_truth", "path": str(pos_path), "sha256": _sha256_file(pos_path) if pos_path.exists() else "0"*64, "day_utc": day, "producer": "positions_v1"},
            {"type": "broker_marks", "path": str(marks_path), "sha256": _sha256_file(marks_path) if marks_path.exists() else "0"*64, "day_utc": day, "producer": "broker_marks_v1"},
            {"type": "engine_linkage", "path": str(link_path), "sha256": _sha256_file(link_path) if link_path.exists() else "0"*64, "day_utc": day, "producer": "engine_linkage_v1"}
        ],
        "attribution": {
            "currency": currency,
            "by_engine": by_engine,
            "notes": notes
        }
    }

    out_dir = TRUTH_ROOT / "accounting_v2" / "attribution" / day
    out_path = out_dir / "engine_attribution.v2.json"
    _immut_write(out_path, _json_bytes(out))

    latest = TRUTH_ROOT / "accounting_v2" / "attribution" / "latest.json"
    latest_obj = {
        "schema_id": "C2_LATEST_POINTER_V1",
        "produced_utc": out["produced_utc"],
        "producer": "ops/tools/run_accounting_attribution_v2_day_v1.py",
        "path": str(out_path.relative_to(TRUTH_ROOT)),
        "artifact_sha256": _sha256_file(out_path),
        "status": status
    }
    _atomic_write(latest, _json_bytes(latest_obj))

    print(f"OK: wrote {out_path}")
    print(f"OK: updated {latest}")
    return 0 if status == "ACTIVE" else 2

if __name__ == "__main__":
    raise SystemExit(main())
