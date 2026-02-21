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
    ap = argparse.ArgumentParser(prog="run_correlation_preconditions_gate_v2")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()
    day = str(args.day_utc).strip()
    produced_utc = f"{day}T00:00:00Z"

    notes: List[str] = []

    nav_path = TRUTH_ROOT / "accounting_v2" / "nav" / day / "nav.v2.json"
    attr_path = TRUTH_ROOT / "accounting_v2" / "attribution" / day / "engine_attribution.v2.json"
    link_path = TRUTH_ROOT / "engine_linkage_v1" / "snapshots" / day / "engine_linkage.v1.json"
    ret_path = TRUTH_ROOT / "monitoring_v1" / "engine_daily_returns_v1" / day / "engine_daily_returns.v1.json"

    nav_ok = False
    attr_ok = False
    link_ok = False
    ret_ok = False

    if not nav_path.exists():
        notes.append(f"MISSING: {nav_path.relative_to(TRUTH_ROOT)}")
    else:
        nav = _load_json(nav_path)
        st = str(nav.get("status", "UNKNOWN"))
        if st == "ACTIVE":
            nav_ok = True
        else:
            notes.append(f"NAV_V2_STATUS_NOT_OK: {st}")

    if not attr_path.exists():
        notes.append(f"MISSING: {attr_path.relative_to(TRUTH_ROOT)}")
    else:
        attr = _load_json(attr_path)
        st = str(attr.get("status", "UNKNOWN"))
        if st == "ACTIVE":
            attr_ok = True
        else:
            notes.append(f"ATTRIBUTION_V2_STATUS_NOT_OK: {st}")

    if not link_path.exists():
        notes.append(f"MISSING: {link_path.relative_to(TRUTH_ROOT)}")
    else:
        link = _load_json(link_path)
        st = str(link.get("status", "UNKNOWN"))
        if st != "NOT_AVAILABLE":
            link_ok = True
        else:
            notes.append("ENGINE_LINKAGE_NOT_AVAILABLE")

    if not ret_path.exists():
        notes.append(f"MISSING: {ret_path.relative_to(TRUTH_ROOT)}")
    else:
        r = _load_json(ret_path)
        st = str(r.get("status", "UNKNOWN"))
        if st == "ACTIVE":
            ret_ok = True
        else:
            notes.append(f"ENGINE_DAILY_RETURNS_STATUS_NOT_OK: {st}")

    checks = {
        "accounting_nav_v2_active": nav_ok,
        "accounting_attribution_v2_active": attr_ok,
        "engine_linkage_available": link_ok,
        "engine_daily_returns_active": ret_ok,
    }

    if nav_ok and attr_ok and link_ok and ret_ok:
        status = "PASS"
        fail_closed = False
    else:
        status = "FAIL"
        fail_closed = True

    out = {
        "schema_id": "C2_CORRELATION_PRECONDITIONS_GATE_V2",
        "schema_version": "1.0.0",
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": "ops/tools/run_correlation_preconditions_gate_v2.py",
        "status": status,
        "fail_closed": fail_closed,
        "checks": checks,
        "notes": notes,
        "input_manifest": [
            {"type": "accounting_nav_v2", "path": str(nav_path.relative_to(TRUTH_ROOT)), "sha256": _sha256_file(nav_path) if nav_path.exists() else "0" * 64},
            {"type": "engine_attribution_v2", "path": str(attr_path.relative_to(TRUTH_ROOT)), "sha256": _sha256_file(attr_path) if attr_path.exists() else "0" * 64},
            {"type": "engine_linkage_v1", "path": str(link_path.relative_to(TRUTH_ROOT)), "sha256": _sha256_file(link_path) if link_path.exists() else "0" * 64},
            {"type": "engine_daily_returns_v1", "path": str(ret_path.relative_to(TRUTH_ROOT)), "sha256": _sha256_file(ret_path) if ret_path.exists() else "0" * 64},
        ],
    }

    out_dir = TRUTH_ROOT / "reports" / "correlation_preconditions_gate_v2" / day
    out_path = out_dir / "correlation_preconditions_gate.v2.json"
    _immut_write(out_path, _json_bytes(out))

    print(f"OK: CORRELATION_PRECONDITIONS_GATE_V2_WRITTEN day_utc={day} status={status} path={out_path} sha256={_sha256_file(out_path)}")
    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
