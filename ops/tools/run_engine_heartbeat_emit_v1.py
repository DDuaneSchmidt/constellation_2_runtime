#!/usr/bin/env python3
"""
run_engine_heartbeat_emit_v1.py

Bundle C — Engine Heartbeat Spine (authoritative)

Writes deterministic, schema-validated heartbeats under:
  constellation_2/runtime/truth/monitoring_v1/engine_heartbeat_v1/<DAY_UTC>/<ENGINE_ID>/engine_heartbeat.v1.json

This tool does NOT decide whether an engine "should have run".
It emits a heartbeat only when invoked by an engine runner wrapper (or an orchestrator stage)
and records input fingerprints provided by the caller.

Fail-closed:
- schema validation enforced
- atomic write
- refuses to overwrite an existing heartbeat for the same (day, engine_id) unless identical schema_id/version/day/engine match
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
DEFAULT_TRUTH_ROOT = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/MONITORING/engine_heartbeat.v1.schema.json"
SCHEMA_ID = "C2_ENGINE_HEARTBEAT_V1"
SCHEMA_VERSION = 1

REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json").resolve()


def _require_repo_root_cwd() -> None:
    cwd = Path.cwd().resolve()
    if cwd != REPO_ROOT:
        raise SystemExit(f"FATAL: must run from repo root cwd={cwd} expected={REPO_ROOT}")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _canonical_json_bytes_v1(obj: Dict[str, Any]) -> bytes:
    # Deterministic encoding consistent with other tools: sorted keys, compact separators, trailing newline.
    return (json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def _validate_against_repo_schema(obj: Dict[str, Any]) -> None:
    # Reuse repo schema validator (already used widely across tools)
    from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # type: ignore

    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)


def _resolve_truth_root(arg_truth_root: str) -> Path:
    tr = (arg_truth_root or "").strip()
    if not tr:
        tr = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if not tr:
        tr = str(DEFAULT_TRUTH_ROOT)

    root = Path(tr).resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"FATAL: truth_root missing or not directory: {root}")
    try:
        root.relative_to(REPO_ROOT)
    except Exception:
        raise SystemExit(f"FATAL: truth_root not under repo root: truth_root={root} repo_root={REPO_ROOT}")
    return root


def _read_registry_engines_active() -> List[str]:
    if not REGISTRY_PATH.exists() or not REGISTRY_PATH.is_file():
        raise SystemExit(f"FATAL: missing engine registry: {REGISTRY_PATH}")
    obj = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    engines = obj.get("engines") or []
    if not isinstance(engines, list):
        raise SystemExit("FATAL: engine registry engines not list")
    out: List[str] = []
    for e in engines:
        if not isinstance(e, dict):
            continue
        if str(e.get("activation_status") or "").strip().upper() != "ACTIVE":
            continue
        eid = str(e.get("engine_id") or "").strip()
        if not eid:
            raise SystemExit("FATAL: ACTIVE engine missing engine_id")
        out.append(eid)
    out.sort()
    return out


def _atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = _canonical_json_bytes_v1(obj)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(raw)
    os.replace(tmp, path)


def _out_path(truth_root: Path, day_utc: str, engine_id: str) -> Path:
    return (truth_root / "monitoring_v1" / "engine_heartbeat_v1" / day_utc / engine_id / "engine_heartbeat.v1.json").resolve()


def _parse_fingerprint(s: str) -> Dict[str, Any]:
    """
    Fingerprint format:
      name|path|sha256|present

    Example:
      accounting_nav|accounting_v2/nav/2026-02-28/nav.v2.json|<64hex>|true
    """
    parts = [p.strip() for p in (s or "").split("|")]
    if len(parts) != 4:
        raise SystemExit(f"FATAL: fingerprint must have 4 fields name|path|sha256|present: {s!r}")
    name, path, sha, present = parts
    if not name or not path:
        raise SystemExit(f"FATAL: fingerprint name/path empty: {s!r}")
    if len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha.lower()):
        raise SystemExit(f"FATAL: fingerprint sha256 invalid (must be 64 hex): {sha!r}")
    if present.lower() not in ("true", "false"):
        raise SystemExit(f"FATAL: fingerprint present must be true/false: {present!r}")
    return {"name": name, "path": path, "sha256": sha.lower(), "present": (present.lower() == "true")}


def _mk_input_manifest_from_fingerprints(fps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    man: List[Dict[str, Any]] = []
    for fp in fps:
        man.append({"type": str(fp["name"]), "path": str(fp["path"]), "sha256": str(fp["sha256"])})
    return man


def main() -> int:
    _require_repo_root_cwd()

    ap = argparse.ArgumentParser(prog="run_engine_heartbeat_emit_v1")
    ap.add_argument("--day_utc", required=True, help="YYYY-MM-DD")
    ap.add_argument("--engine_id", required=True, help="Engine id (must be ACTIVE unless --allow_inactive)")
    ap.add_argument("--status", required=True, choices=["OK", "WARN", "FAIL"])
    ap.add_argument("--reason_code", action="append", default=[], help="Repeatable reason code(s). At least one required.")
    ap.add_argument("--last_run_utc", required=True, help="UTC ISO-8601 Z timestamp")
    ap.add_argument("--last_signal_utc", default="", help="Optional UTC ISO-8601 Z timestamp")
    ap.add_argument("--expected_period_seconds", type=int, required=True)
    ap.add_argument("--stale_after_seconds", type=int, required=True)
    ap.add_argument("--fingerprint", action="append", default=[], help="Repeatable fingerprints: name|path|sha256|present")
    ap.add_argument("--truth_root", default="", help="Override truth root (must be under repo root)")
    ap.add_argument("--producer_repo", default="constellation_2_runtime")
    ap.add_argument("--producer_module", default="ops/tools/run_engine_heartbeat_emit_v1.py")
    ap.add_argument("--producer_git_sha", default="")
    ap.add_argument("--allow_inactive", action="store_true", help="Allow emitting for non-ACTIVE engines (default: forbidden)")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    engine_id = str(args.engine_id).strip()
    status = str(args.status).strip().upper()

    rcs = [str(x).strip() for x in (args.reason_code or []) if str(x).strip()]
    if not rcs:
        raise SystemExit("FATAL: at least one --reason_code required")

    last_run_utc = str(args.last_run_utc).strip()
    last_signal_utc = (str(args.last_signal_utc).strip() or None)

    exp = int(args.expected_period_seconds)
    stale = int(args.stale_after_seconds)
    if exp <= 0 or stale <= 0:
        raise SystemExit("FATAL: cadence seconds must be > 0")

    truth_root = _resolve_truth_root(str(args.truth_root))

    active = _read_registry_engines_active()
    if (engine_id not in active) and (not bool(args.allow_inactive)):
        raise SystemExit(f"FATAL: engine_id not ACTIVE in registry (use --allow_inactive only for governed exceptions): {engine_id}")

    # fingerprints
    fps: List[Dict[str, Any]] = []
    for raw in (args.fingerprint or []):
        fps.append(_parse_fingerprint(raw))
    fps_sorted = sorted(fps, key=lambda d: (str(d["name"]), str(d["path"])))

    # producer sha
    git_sha = str(args.producer_git_sha).strip()
    if not git_sha:
        git_sha = (
            os.popen("cd /home/node/constellation_2_runtime && /usr/bin/git rev-parse HEAD").read().strip()
        )
    if not git_sha:
        raise SystemExit("FATAL: unable to determine producer git sha")

    out_path = _out_path(truth_root, day, engine_id)

    payload: Dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "day_utc": day,
        "engine_id": engine_id,
        "status": status,
        "reason_codes": rcs,
        "last_run_utc": last_run_utc,
        "last_signal_utc": last_signal_utc,
        "cadence": {"expected_period_seconds": exp, "stale_after_seconds": stale},
        "inputs": {"fingerprints": fps_sorted},
        "input_manifest": _mk_input_manifest_from_fingerprints(fps_sorted),
        "produced_utc": str(os.environ.get("C2_PRODUCED_UTC") or last_run_utc),
        "producer": {"repo": str(args.producer_repo), "module": str(args.producer_module), "git_sha": git_sha},
        "heartbeat_sha256": None,
    }

    # self-hash
    tmp = dict(payload)
    tmp["heartbeat_sha256"] = None
    payload["heartbeat_sha256"] = _sha256_bytes(_canonical_json_bytes_v1(tmp))

    # validate
    _validate_against_repo_schema(payload)

    # idempotency: if file exists, it must match key identity fields (schema/day/engine)
    if out_path.exists() and out_path.is_file():
        try:
            existing = json.loads(out_path.read_text(encoding="utf-8"))
        except Exception as e:
            raise SystemExit(f"FATAL: existing heartbeat unreadable: {out_path} err={e!r}")

        if str(existing.get("schema_id") or "") != SCHEMA_ID:
            raise SystemExit(f"FATAL: existing heartbeat schema mismatch path={out_path}")
        if int(existing.get("schema_version") or 0) != SCHEMA_VERSION:
            raise SystemExit(f"FATAL: existing heartbeat schema_version mismatch path={out_path}")
        if str(existing.get("day_utc") or "") != day:
            raise SystemExit(f"FATAL: existing heartbeat day mismatch path={out_path}")
        if str(existing.get("engine_id") or "") != engine_id:
            raise SystemExit(f"FATAL: existing heartbeat engine mismatch path={out_path}")

        # If identical bytes, treat as OK; otherwise fail-closed (immutable)
        if _sha256_file(out_path) == _sha256_bytes(_canonical_json_bytes_v1(existing)):
            print(f"OK: ENGINE_HEARTBEAT_V1_EXISTS day_utc={day} engine_id={engine_id} path={out_path}")
            return 0
        raise SystemExit(f"FATAL: refusing to overwrite existing heartbeat (immutable): {out_path}")

    _atomic_write_json(out_path, payload)
    print(f"OK: ENGINE_HEARTBEAT_V1_WRITTEN day_utc={day} engine_id={engine_id} path={out_path} sha256={payload['heartbeat_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
