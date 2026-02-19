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

def _json_dumps(obj: Any) -> bytes:
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
    ap = argparse.ArgumentParser(prog="run_engine_pnl_proxy_day_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    day = str(args.day_utc).strip()

    src = TRUTH_ROOT / "accounting_v1" / "attribution" / day / "engine_attribution.json"
    if not src.exists():
        raise SystemExit(f"FATAL: missing engine attribution: {src}")

    o = _load_json(src)
    attr = o.get("attribution", {})
    by_engine = attr.get("by_engine", [])
    currency = attr.get("currency", "USD")
    status = o.get("status", "UNKNOWN")
    reason_codes = o.get("reason_codes", [])

    out = {
        "schema_id": "C2_MONITORING_ENGINE_PNL_PROXY_V1",
        "schema_version": "1.0.0",
        "produced_utc": __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "day_utc": day,
        "producer": "ops/tools/run_engine_pnl_proxy_day_v1.py",
        "source_attribution_path": str(src.relative_to(TRUTH_ROOT)),
        "source_attribution_sha256": _sha256_file(src),
        "status": status,
        "reason_codes": [str(x) for x in reason_codes],
        "currency": str(currency),
        "by_engine": by_engine,
        "not_valid_for_return_correlation": True,
        "notes": [
            "Derived from accounting_v1 attribution; not marks-based returns.",
            "Correlation based on returns is blocked until marks+linkage are available."
        ]
    }

    out_dir = TRUTH_ROOT / "monitoring_v1" / "engine_pnl_proxy_v1" / day
    out_path = out_dir / "engine_pnl_proxy.v1.json"
    _immut_write(out_path, _json_dumps(out))

    latest = TRUTH_ROOT / "monitoring_v1" / "engine_pnl_proxy_v1" / "latest.json"
    latest_obj = {
        "schema_id": "C2_LATEST_POINTER_V1",
        "produced_utc": __import__("datetime").datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "producer": "ops/tools/run_engine_pnl_proxy_day_v1.py",
        "path": str(out_path.relative_to(TRUTH_ROOT)),
        "artifact_sha256": _sha256_file(out_path),
        "status": status
    }
    _atomic_write(latest, _json_dumps(latest_obj))

    print(f"OK: wrote {out_path}")
    print(f"OK: updated {latest}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
