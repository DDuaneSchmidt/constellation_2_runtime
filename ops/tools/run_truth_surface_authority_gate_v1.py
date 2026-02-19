#!/usr/bin/env python3
"""
run_truth_surface_authority_gate_v1.py

Bundle A10 — Truth Surface Authority Consolidation (Gate)

Purpose:
- Enforce that mutually-exclusive truth surfaces have a single authoritative version for a given day.
- Fail-closed if multiple versions exist for the same surface+day when registry declares exclusive.

Writes:
  truth/reports/truth_surface_authority_gate_v1/<DAY>/truth_surface_authority_gate.v1.json

Run:
  python3 ops/tools/run_truth_surface_authority_gate_v1.py --day_utc YYYY-MM-DD
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

REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/TRUTH_SURFACE_AUTHORITY_V1.json").resolve()
OUT_ROOT = (TRUTH / "reports" / "truth_surface_authority_gate_v1").resolve()

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


def _expand(template: str, day: str) -> Path:
    return (REPO_ROOT / template.replace("{DAY}", day)).resolve()


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_truth_surface_authority_gate_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    import re
    if not re.match(DAY_RE, day):
        raise SystemExit(f"FAIL: bad --day_utc (expected YYYY-MM-DD): {day!r}")

    if not REGISTRY_PATH.exists():
        raise SystemExit(f"FATAL: missing truth surface authority registry: {REGISTRY_PATH}")

    reg = _read_json_obj(REGISTRY_PATH)
    mapping = reg.get("mapping") or {}

    produced_utc = f"{day}T00:00:00Z"

    findings: List[Dict[str, Any]] = []
    reason_codes: List[str] = []
    manifest: List[Dict[str, str]] = []

    for s in (reg.get("surfaces") or []):
        if not isinstance(s, dict):
            continue
        surface = str(s.get("surface") or "").strip()
        active = str(s.get("active") or "").strip()
        versions = [str(v) for v in (s.get("versions") or [])]
        exclusive = bool(s.get("exclusive"))
        enforce_from = str(s.get("enforce_from_day_utc") or "").strip()

        m = mapping.get(surface) or {}
        present: Dict[str, Dict[str, str]] = {}
        for v in versions:
            tmpl = m.get(v)
            if not tmpl:
                continue
            p = _expand(str(tmpl), day)
            if p.exists():
                sha = _sha256_file(p)
                present[v] = {"path": str(p), "sha256": sha}
                manifest.append({"type": f"{surface}_{v}", "path": str(p), "sha256": sha})
            else:
                manifest.append({"type": f"{surface}_{v}_missing", "path": str(p), "sha256": _sha256_bytes(b"")})

        # Exclusive surfaces may not have >1 version present (enforced from registry day, otherwise warn-only).
        present_versions = sorted(list(present.keys()))
        ok = True
        enforce = False
        if enforce_from:
            enforce = (day >= enforce_from)
        if exclusive and len(present_versions) > 1:
            if enforce:
                ok = False
                reason_codes.append(f"MULTIPLE_VERSIONS_PRESENT:{surface}:{','.join(present_versions)}")
            else:
                reason_codes.append(f"WARN_MULTIPLE_VERSIONS_PRESENT_PRE_ENFORCEMENT:{surface}:{','.join(present_versions)}")

        # Active version should be the only present one when exclusive, or at least present when non-exclusive.
        if active:
            if exclusive and active not in present_versions and len(present_versions) > 0:
                ok = False
                reason_codes.append(f"ACTIVE_VERSION_NOT_PRESENT:{surface}:active={active}:present={','.join(present_versions)}")

        findings.append({
            "surface": surface,
            "exclusive": exclusive,
            "active": active,
            "present_versions": present_versions,
            "present": present,
            "status": "OK" if ok else "FAIL",
        })

    status = "PASS" if len(reason_codes) == 0 else "FAIL"

    out = {
        "schema_id": "truth_surface_authority_gate",
        "schema_version": "v1",
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_truth_surface_authority_gate_v1.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": reason_codes,
        "findings": findings,
        "input_manifest": manifest,
    }

    out_path = (OUT_ROOT / day / "truth_surface_authority_gate.v1.json").resolve()
    path, sha, action = _write_immutable(out_path, out)
    print(f"OK: truth_surface_authority_gate_v1: action={action} sha256={sha} path={path}")
    if status != "PASS":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
