#!/usr/bin/env python3
"""
run_heartbeat_gate_v1.py

Bundle C (C1+C2) — Heartbeat Gate v1 (REPORTS)

Purpose:
- Read ACTIVE engines from ENGINE_MODEL_REGISTRY_V1.json (registry-driven).
- Require a heartbeat artifact per ACTIVE engine under:
    truth/monitoring_v1/engine_heartbeat_v1/<DAY>/<ENGINE_ID>/engine_heartbeat.v1.json
- Fail if any ACTIVE engine heartbeat is missing or stale beyond policy.
- Emit a single immutable gate artifact:
    truth/reports/heartbeat_gate_v1/<DAY>/heartbeat_gate.v1.json

Non-negotiable:
- Pure evaluation (no mutation of inputs)
- Deterministic serialization
- Fail-closed
- Schema-validated at write-time
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

REG_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()

OUT_ROOT = (DEFAULT_TRUTH_ROOT / "reports" / "heartbeat_gate_v1").resolve()
HB_ROOT = (DEFAULT_TRUTH_ROOT / "monitoring_v1" / "engine_heartbeat_v1").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/heartbeat_gate.v1.schema.json"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _read_json_obj(p: Path) -> Dict[str, Any]:
    o = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(o, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: {p}")
    return o


def _parse_utc_z(s: str) -> datetime:
    t = (s or "").strip()
    if not t.endswith("Z"):
        raise ValueError(f"NOT_UTC_Z: {t!r}")
    return datetime.fromisoformat(t[:-1] + "+00:00").astimezone(timezone.utc).replace(microsecond=0)


def _canonical_json_bytes_v1(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _validate_against_repo_schema_v1(obj: Dict[str, Any]) -> None:
    # Reuse existing validator
    from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # type: ignore

    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)


def _write_immutable(path: Path, obj: Dict[str, Any]) -> Tuple[str, str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _canonical_json_bytes_v1(obj) + b"\n"
    sha = _sha256_bytes(payload)

    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == sha:
            return (str(path), sha, "EXISTS_IDENTICAL")
        raise SystemExit(f"FAIL: refusing overwrite (different bytes): {path}")

    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    os.replace(tmp, path)
    return (str(path), sha, "WRITTEN")


def _resolve_truth_root(arg_truth_root: str) -> Path:
    tr = (arg_truth_root or "").strip()
    if not tr:
        tr = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if not tr:
        tr = str(DEFAULT_TRUTH_ROOT)

    truth_root = Path(tr).resolve()
    if not truth_root.exists() or not truth_root.is_dir():
        raise SystemExit(f"FAIL: truth_root missing or not dir: {truth_root}")
    try:
        truth_root.relative_to(REPO_ROOT)
    except Exception:
        raise SystemExit(f"FAIL: truth_root not under repo root: truth_root={truth_root} repo_root={REPO_ROOT}")
    return truth_root


def _active_engines_from_registry(reg: Dict[str, Any]) -> List[Dict[str, str]]:
    engines = reg.get("engines") or []
    if not isinstance(engines, list):
        raise SystemExit("FAIL: registry engines not list")

    out: List[Dict[str, str]] = []
    for e in engines:
        if not isinstance(e, dict):
            continue
        if str(e.get("activation_status") or "") != "ACTIVE":
            continue
        eid = str(e.get("engine_id") or "").strip()
        if not eid:
            raise SystemExit("FAIL: ACTIVE engine missing engine_id")
        out.append({"engine_id": eid, "activation_status": "ACTIVE"})
    out.sort(key=lambda x: x["engine_id"])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_heartbeat_gate_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument(
        "--truth_root",
        default="",
        help="Override truth root (must be under repo root). If omitted, uses env C2_TRUTH_ROOT, else canonical.",
    )
    ap.add_argument("--expected_period_seconds", type=int, default=86400)
    ap.add_argument("--stale_after_seconds", type=int, default=172800)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    produced_utc = f"{day}T00:00:00Z"

    truth_root = _resolve_truth_root(str(args.truth_root))
    hb_root = (truth_root / "monitoring_v1" / "engine_heartbeat_v1").resolve()
    out_root = (truth_root / "reports" / "heartbeat_gate_v1").resolve()

    if not REG_PATH.exists():
        raise SystemExit(f"FAIL: missing engine registry: {REG_PATH}")

    reg = _read_json_obj(REG_PATH)
    active_engines = _active_engines_from_registry(reg)

    exp = int(args.expected_period_seconds)
    stale_after = int(args.stale_after_seconds)
    if exp < 60 or stale_after < 60:
        raise SystemExit("FAIL: policy seconds must be >=60")
    if stale_after < exp:
        raise SystemExit("FAIL: stale_after_seconds must be >= expected_period_seconds")

    results: List[Dict[str, Any]] = []
    rc: List[str] = []

    now_ref = _parse_utc_z(produced_utc)

    missing = 0
    stale = 0
    present = 0

    input_manifest: List[Dict[str, str]] = []
    input_manifest.append({"type": "engine_registry_v1", "path": str(REG_PATH), "sha256": _sha256_file(REG_PATH)})

    for e in active_engines:
        eid = e["engine_id"]
        hb_path = (hb_root / day / eid / "engine_heartbeat.v1.json").resolve()
        row_rc: List[str] = []
        hb_present = hb_path.exists() and hb_path.is_file()
        hb_sha = _sha256_bytes(b"")
        hb_status = None
        hb_prod = None
        is_stale = False

        if not hb_present:
            missing += 1
            row_rc.append("HB_MISSING")
            rc.append(f"C2_HB_MISSING:{eid}")
        else:
            present += 1
            hb_sha = _sha256_file(hb_path)
            try:
                hb = _read_json_obj(hb_path)
                hb_status = str(hb.get("status") or "").strip() or None
                hb_prod = str(hb.get("produced_utc") or "").strip() or None
                if hb_prod:
                    hb_dt = _parse_utc_z(hb_prod)
                    age = int((now_ref - hb_dt).total_seconds())
                    if age > stale_after:
                        is_stale = True
                        stale += 1
                        row_rc.append("HB_STALE")
                        rc.append(f"C2_HB_STALE:{eid}")
            except Exception:
                row_rc.append("HB_PARSE_ERROR")
                rc.append(f"C2_HB_PARSE_ERROR:{eid}")

            input_manifest.append({"type": f"heartbeat:{eid}", "path": str(hb_path), "sha256": hb_sha})

        ok = (hb_present and (not is_stale) and ("HB_PARSE_ERROR" not in row_rc))

        results.append(
            {
                "engine_id": eid,
                "activation_status": "ACTIVE",
                "heartbeat_present": bool(hb_present),
                "heartbeat_path": str(hb_path),
                "heartbeat_sha256": hb_sha,
                "heartbeat_status": hb_status,
                "heartbeat_produced_utc": hb_prod,
                "stale": bool(is_stale),
                "ok": bool(ok),
                "reason_codes": row_rc,
            }
        )

    status = "PASS"
    if missing > 0 or stale > 0:
        status = "FAIL"
    if not active_engines:
        # Fail-closed: if registry has no ACTIVE engines, this is not an acceptable monitoring posture.
        status = "FAIL"
        rc.append("C2_HB_NO_ACTIVE_ENGINES_FAILCLOSED")

    out: Dict[str, Any] = {
        "schema_id": "C2_HEARTBEAT_GATE_V1",
        "schema_version": 1,
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_heartbeat_gate_v1.py", "git_sha": _git_sha()},
        "status": status,
        "fail_closed": True,
        "policy": {
            "expected_period_seconds": exp,
            "stale_after_seconds": stale_after,
            "require_all_active_engines": True,
        },
        "results": {
            "by_engine": results,
            "totals": {
                "active_engines": len(active_engines),
                "heartbeats_present": present,
                "missing": missing,
                "stale": stale,
            },
        },
        "reason_codes": rc,
        "input_manifest": input_manifest,
        "gate_sha256": None,
    }
    out["gate_sha256"] = _sha256_bytes(_canonical_json_bytes_v1({k: v for k, v in out.items() if k != "gate_sha256"}) + b"\n")

    _validate_against_repo_schema_v1(out)

    out_path = (out_root / day / "heartbeat_gate.v1.json").resolve()
    path_s, sha, action = _write_immutable(out_path, out)

    if status == "PASS":
        print(f"OK: HEARTBEAT_GATE_V1_WRITTEN day_utc={day} status={status} path={path_s} sha256={sha} action={action}")
        return 0
    print(f"FAIL: HEARTBEAT_GATE_V1_WRITTEN day_utc={day} status={status} path={path_s} sha256={sha} action={action} reason_codes={rc}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
