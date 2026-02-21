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
    ap = argparse.ArgumentParser(prog="run_correlation_preconditions_gate_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    day = str(args.day_utc).strip()
    produced_utc = f"{day}T00:00:00Z"

    notes: List[str] = []

    nav_path = TRUTH_ROOT / "accounting_v1" / "nav" / day / "nav.json"
    attr_path = TRUTH_ROOT / "accounting_v1" / "attribution" / day / "engine_attribution.json"

    nav_ok = False
    attr_ok = False
    linkage_ok = False

    if not nav_path.exists():
        notes.append(f"MISSING: {nav_path.relative_to(TRUTH_ROOT)}")
    else:
        nav = _load_json(nav_path)
        nav_status = str(nav.get("status", "UNKNOWN"))
        if nav_status == "ACTIVE":
            nav_ok = True
        else:
            notes.append(f"NAV_STATUS_NOT_OK: {nav_status}")

    if not attr_path.exists():
        notes.append(f"MISSING: {attr_path.relative_to(TRUTH_ROOT)}")
    else:
        attr = _load_json(attr_path)
        attr_status = str(attr.get("status", "UNKNOWN"))
        if attr_status == "ACTIVE":
            attr_ok = True
        else:
            notes.append(f"ATTRIBUTION_STATUS_NOT_OK: {attr_status}")

        try:
            by_engine = attr.get("attribution", {}).get("by_engine", [])
            if isinstance(by_engine, list) and all(
                str(e.get("engine_id", "")).strip() != "unknown" for e in by_engine if isinstance(e, dict)
            ):
                linkage_ok = True
            else:
                notes.append("ENGINE_LINKAGE_UNKNOWN: engine_id 'unknown' present")
        except Exception:
            notes.append("ENGINE_LINKAGE_PARSE_ERROR")

    checks = {
        "accounting_nav_status_ok": nav_ok,
        "accounting_attribution_status_ok": attr_ok,
        "engine_linkage_not_unknown": linkage_ok,
    }

    if nav_ok and attr_ok and linkage_ok:
        status = "PASS"
        fail_closed = False
    else:
        status = "FAIL"
        fail_closed = True

    out = {
        "schema_id": "C2_CORRELATION_PRECONDITIONS_GATE_V1",
        "schema_version": "1.0.0",
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": "ops/tools/run_correlation_preconditions_gate_v1.py",
        "status": status,
        "fail_closed": fail_closed,
        "checks": checks,
        "notes": notes,
        "input_manifest": [
            {
                "type": "accounting_nav_v1",
                "path": str(nav_path.relative_to(TRUTH_ROOT)),
                "sha256": _sha256_file(nav_path) if nav_path.exists() else "0" * 64,
            },
            {
                "type": "engine_attribution_v1",
                "path": str(attr_path.relative_to(TRUTH_ROOT)),
                "sha256": _sha256_file(attr_path) if attr_path.exists() else "0" * 64,
            },
        ],
    }

    out_dir = TRUTH_ROOT / "reports" / "correlation_preconditions_gate_v1" / day
    out_path = out_dir / "correlation_preconditions_gate.v1.json"
    _immut_write(out_path, _json_dumps(out))

    print(f"OK: CORRELATION_PRECONDITIONS_GATE_V1_WRITTEN day_utc={day} status={status} path={out_path} sha256={_sha256_file(out_path)}")
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
