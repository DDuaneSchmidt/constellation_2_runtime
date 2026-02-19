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
    ap = argparse.ArgumentParser(prog="run_engine_daily_returns_day_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--prev_day_utc", default="", help="Optional previous day; if empty, returns NOT_AVAILABLE")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    prev = str(args.prev_day_utc).strip()

    today_path = TRUTH_ROOT / "accounting_v2" / "attribution" / day / "engine_attribution.v2.json"
    prev_path = TRUTH_ROOT / "accounting_v2" / "attribution" / prev / "engine_attribution.v2.json" if prev else None

    status = "ACTIVE"
    reason_codes: List[str] = []
    returns: List[Dict[str, str]] = []

    if not today_path.exists():
        status = "NOT_AVAILABLE"
        reason_codes.append("MISSING_ATTRIBUTION_TODAY")

    if prev == "":
        status = "NOT_AVAILABLE"
        reason_codes.append("MISSING_PREV_DAY_KEY")
    else:
        if prev_path is None or not prev_path.exists():
            status = "NOT_AVAILABLE"
            reason_codes.append("MISSING_ATTRIBUTION_PREV")

    # For now: SAFE_IDLE-compatible default.
    # We only compute returns once we have a proven per-engine NAV basis in attribution v2.
    # (At the moment, your attribution v2 is empty because positions are empty.)
    if status == "ACTIVE":
        o_today = _load_json(today_path)
        o_prev = _load_json(prev_path)  # type: ignore[arg-type]

        by_today = (o_today.get("attribution") or {}).get("by_engine", [])
        by_prev = (o_prev.get("attribution") or {}).get("by_engine", [])

        if not by_today and not by_prev:
            status = "NOT_AVAILABLE"
            reason_codes.append("NO_ENGINE_DATA_SAFE_IDLE")
        else:
            # Placeholder-free rule: we fail closed on unknown basis.
            status = "NOT_AVAILABLE"
            reason_codes.append("ENGINE_RETURN_BASIS_NOT_PROVEN_YET")
            # returns stays empty until we implement a proven basis from non-empty attribution.

    out = {
        "schema_id": "C2_MONITORING_ENGINE_DAILY_RETURNS_V1",
        "schema_version": "1.0.0",
        "produced_utc": __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "day_utc": day,
        "producer": "ops/tools/run_engine_daily_returns_day_v1.py",
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "inputs": {
            "attribution_today_path": str(today_path.relative_to(TRUTH_ROOT)),
            "attribution_today_sha256": _sha256_file(today_path) if today_path.exists() else "0"*64,
            "attribution_prev_path": str(prev_path.relative_to(TRUTH_ROOT)) if prev_path else "",
            "attribution_prev_sha256": _sha256_file(prev_path) if (prev_path and prev_path.exists()) else ("0"*64 if prev else "")
        },
        "returns": returns
    }

    out_dir = TRUTH_ROOT / "monitoring_v1" / "engine_daily_returns_v1" / day
    out_path = out_dir / "engine_daily_returns.v1.json"
    # Idempotency: if the day artifact already exists, do not attempt rewrite.
    if out_path.exists():
        print(f"SKIP: already exists {out_path}")
        return 0

    _immut_write(out_path, _json_bytes(out))

    latest = TRUTH_ROOT / "monitoring_v1" / "engine_daily_returns_v1" / "latest.json"
    latest_obj = {
        "schema_id": "C2_LATEST_POINTER_V1",
        "produced_utc": out["produced_utc"],
        "producer": "ops/tools/run_engine_daily_returns_day_v1.py",
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
