#!/usr/bin/env python3
"""
run_gate_stack_verdict_v1.py

Bundle A8 — Gate Hierarchy & Precedence Formalization

Purpose:
- Deterministically evaluate a governed set of gate artifacts for a day.
- Apply precedence (CLASS1 > CLASS2 > CLASS3 > CLASS4).
- Emit a single immutable verdict artifact.

Writes:
  truth/reports/gate_stack_verdict_v1/<DAY>/gate_stack_verdict.v1.json

Run:
  python3 ops/tools/run_gate_stack_verdict_v1.py --day_utc YYYY-MM-DD

Non-negotiable:
- Pure evaluation (no mutation of inputs)
- Fail-closed for required gates
- Immutable write
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple


_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[2]
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # type: ignore

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/GATE_HIERARCHY_V1.json").resolve()
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/gate_stack_verdict.v1.schema.json"
OUT_ROOT = (TRUTH / "reports" / "gate_stack_verdict_v1").resolve()

DAY_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"


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
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {p}")
    return o


def _write_immutable(path: Path, obj: Dict[str, Any]) -> Tuple[str, str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b"\n"
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
    import os
    os.replace(tmp, path)
    return (str(path), sha, "WRITTEN")


def _class_precedence(reg: Dict[str, Any]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for c in reg.get("classes") or []:
        cid = str(c.get("class_id") or "").strip()
        prec = int(c.get("precedence") or 9999)
        if cid:
            out[cid] = prec
    return out


def _eval_gate(day: str, gate: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
    # Returns (gate_result, input_manifest_entries)
    gate_id = str(gate.get("gate_id") or "").strip()
    gclass = str(gate.get("gate_class") or "").strip()
    required = bool(gate.get("required"))
    blocking = bool(gate.get("blocking"))
    rel = str(gate.get("artifact_relpath") or "").replace("{DAY}", day)
    status_field = str(gate.get("status_field") or "status")
    pass_vals = [str(x).strip().upper() for x in (gate.get("pass_status_values") or [])]

    path = (REPO_ROOT / rel).resolve()
    manifest: List[Dict[str, str]] = []

    if not path.exists():
        manifest.append({"type": f"{gate_id}_missing", "path": str(path), "sha256": _sha256_bytes(b"")})
        status = "MISSING"
        rc = ["MISSING_GATE_ARTIFACT"]
        sha = _sha256_bytes(b"")
    else:
        sha = _sha256_file(path)
        manifest.append({"type": gate_id, "path": str(path), "sha256": sha})
        rc = []
        try:
            o = _read_json_obj(path)
            raw = str(o.get(status_field) or "").strip().upper()
            status = raw if raw else "UNKNOWN"
            # Normalize common patterns
            if raw in ("OK", "PASS"):
                status_norm = raw
            elif raw in ("FAIL", "BLOCK", "BLOCKED"):
                status_norm = "FAIL"
            else:
                status_norm = raw
            status = status_norm
            # Collect reason codes if present
            rcs = o.get("reason_codes")
            if isinstance(rcs, list):
                rc = [str(x) for x in rcs]
        except Exception:
            status = "UNKNOWN"
            rc = ["PARSE_ERROR"]

    # Compute pass/fail using pass status values
    status_upper = str(status).strip().upper()
    gate_pass = status_upper in pass_vals

    # Required gates that are not passing become FAIL.
    if required and not gate_pass:
        eff = "FAIL"
    else:
        eff = "PASS" if gate_pass else "UNKNOWN"
        if status_upper == "MISSING":
            eff = "MISSING"

    # Keep actual observed status for transparency.
    out = {
        "gate_id": gate_id,
        "gate_class": gclass,
        "required": required,
        "blocking": blocking,
        "status": status_upper,
        "artifact_path": str(path),
        "artifact_sha256": sha,
        "reason_codes": rc,
        "evaluated_state": eff,
    }
    return out, manifest


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_gate_stack_verdict_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    import re
    if not re.match(DAY_RE, day):
        raise SystemExit(f"FAIL: bad --day_utc (expected YYYY-MM-DD): {day!r}")

    if not REGISTRY_PATH.exists():
        raise SystemExit(f"FATAL: missing gate hierarchy registry: {REGISTRY_PATH}")

    reg = _read_json_obj(REGISTRY_PATH)
    prec = _class_precedence(reg)

    produced_utc = f"{day}T00:00:00Z"

    gates: List[Dict[str, Any]] = []
    manifest: List[Dict[str, str]] = []

    for g in (reg.get("gates") or []):
        if not isinstance(g, dict):
            continue
        gr, man = _eval_gate(day, g)
        gates.append(gr)
        manifest.extend(man)

    # Deterministic sort: precedence, then gate_id
    def sort_key(x: Dict[str, Any]) -> Tuple[int, str]:
        cid = str(x.get("gate_class") or "")
        return (int(prec.get(cid, 9999)), str(x.get("gate_id") or ""))

    gates_sorted = sorted(gates, key=sort_key)

    blocking_class = "NONE"
    reason_codes: List[str] = []

    # Evaluate fail-closed with precedence.
    status = "PASS"
    for g in gates_sorted:
        cid = str(g.get("gate_class") or "")
        required = bool(g.get("required"))
        blocking = bool(g.get("blocking"))
        observed = str(g.get("status") or "")
        pass_vals = None

        # A required non-pass is a fail.
        is_missing = (observed == "MISSING")
        is_failish = (observed not in ("PASS", "OK"))

        if required and is_failish:
            status = "FAIL"
            blocking_class = cid
            reason_codes.append(f"GATE_REQUIRED_NOT_PASS:{g.get('gate_id')}:{observed}")
            if is_missing:
                reason_codes.append(f"GATE_MISSING:{g.get('gate_id')}")
            break

        # Non-required but blocking gates can still fail closed.
        if (not required) and blocking and is_failish and not is_missing:
            status = "FAIL"
            blocking_class = cid
            reason_codes.append(f"GATE_BLOCKING_NOT_PASS:{g.get('gate_id')}:{observed}")
            break

    out = {
        "schema_id": "gate_stack_verdict",
        "schema_version": "v1",
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_gate_stack_verdict_v1.py", "git_sha": _git_sha()},
        "status": status,
        "blocking_class": blocking_class,
        "reason_codes": reason_codes,
        "input_manifest": manifest,
        "gates": [
            {
                "gate_id": str(g.get("gate_id")),
                "gate_class": str(g.get("gate_class")),
                "required": bool(g.get("required")),
                "blocking": bool(g.get("blocking")),
                "status": str(g.get("status")),
                "artifact_path": str(g.get("artifact_path")),
                "artifact_sha256": str(g.get("artifact_sha256")),
                "reason_codes": list(g.get("reason_codes") or []),
            }
            for g in gates_sorted
        ],
    }

    # Schema validation is intentionally not enforced here to avoid introducing a dependency loop.
    out_path = (OUT_ROOT / day / "gate_stack_verdict.v1.json").resolve()
    path, sha, action = _write_immutable(out_path, out)

    print(f"OK: gate_stack_verdict_v1: action={action} sha256={sha} path={path}")
    if status != "PASS":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
