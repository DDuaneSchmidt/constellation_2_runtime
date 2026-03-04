#!/usr/bin/env python3
"""
run_gate_completeness_gate_v1.py

Fail-closed gate: proves that all REQUIRED gates in GATE_HIERARCHY_V1
have produced their artifacts for a given day before gate_stack_verdict runs.

Writes (immutable-safe):
  <truth_root>/reports/gate_completeness_gate_v1/<DAY>/gate_completeness_gate.v1.json

Exit:
  0 if PASS (no missing required gate artifacts)
  2 if FAIL (missing required artifacts)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
REGISTRY = (REPO_ROOT / "governance/02_REGISTRIES/GATE_HIERARCHY_V1.json").resolve()


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _truth_root() -> Path:
    tr = (os.environ.get("C2_TRUTH_ROOT") or "").strip()
    if tr:
        p = Path(tr).resolve()
        if p.exists() and p.is_dir():
            return p
        raise SystemExit(f"FAIL: C2_TRUTH_ROOT invalid: {p}")
    return (REPO_ROOT / "constellation_2/runtime/truth").resolve()


def _read_json_obj(p: Path) -> Dict[str, Any]:
    o = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(o, dict):
        raise SystemExit(f"FAIL: registry_not_object: {p}")
    return o


def _resolve_gate_artifact_path(truth: Path, day: str, gate_id: str, artifact_relpath: str) -> Path:
    rel = (artifact_relpath or "").strip().lstrip("/")
    if not rel:
        # invalid registry entry
        return (truth / "reports" / gate_id / day / "MISSING_RELPATH").resolve()

    # If registry ever supplies TRUTH-relative prefixes, support them:
    if rel.startswith(("reports/", "risk_v1/", "monitoring_v1/", "monitoring_v2/", "execution_evidence_v1/", "accounting_v1/", "accounting_v2/")):
        return (truth / rel).resolve()

    # Default canonical reports layout:
    return (truth / "reports" / gate_id / day / rel).resolve()


def _immut_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) != _sha256_bytes(content):
            raise SystemExit(f"FAIL: REFUSE_OVERWRITE_DIFFERENT_BYTES: {path}")
        return
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(content)
    os.replace(tmp, path)


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_gate_completeness_gate_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {day!r}")

    truth = _truth_root()

    reg = _read_json_obj(REGISTRY)
    gates = reg.get("gates")
    if not isinstance(gates, list):
        raise SystemExit("FAIL: registry_missing_gates_list")

    missing: List[Dict[str, Any]] = []
    present: List[Dict[str, Any]] = []

    for g in gates:
        if not isinstance(g, dict):
            continue
        gate_id = str(g.get("gate_id") or "").strip()
        if not gate_id:
            continue
        required = bool(g.get("required"))
        if not required:
            continue

        rel = str(g.get("artifact_relpath") or "").strip()
        p = _resolve_gate_artifact_path(truth, day, gate_id, rel)

        if not p.exists():
            missing.append(
                {
                    "gate_id": gate_id,
                    "expected_path": str(p),
                    "artifact_relpath": rel,
                    "reason": "MISSING_REQUIRED_GATE_ARTIFACT",
                }
            )
        else:
            # Must be a file, not dir
            if p.is_dir():
                missing.append(
                    {
                        "gate_id": gate_id,
                        "expected_path": str(p),
                        "artifact_relpath": rel,
                        "reason": "REQUIRED_GATE_PATH_IS_DIRECTORY",
                    }
                )
            else:
                present.append(
                    {
                        "gate_id": gate_id,
                        "path": str(p),
                        "sha256": _sha256_file(p),
                    }
                )

    status = "PASS" if len(missing) == 0 else "FAIL"
    reason_codes = []
    if status != "PASS":
        reason_codes.append("MISSING_REQUIRED_GATE_ARTIFACTS")

    out_dir = (truth / "reports" / "gate_completeness_gate_v1" / day).resolve()
    out_path = (out_dir / "gate_completeness_gate.v1.json").resolve()

    payload = {
        "schema_id": "C2_GATE_COMPLETENESS_GATE_V1",
        "day_utc": day,
        "status": status,
        "reason_codes": reason_codes,
        "truth_root": str(truth),
        "registry_path": str(REGISTRY),
        "missing_required": missing,
        "present_required": present,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_gate_completeness_gate_v1.py"},
    }

    content = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    _immut_write(out_path, content)

    print(f"OK: gate_completeness_gate_v1_written day_utc={day} status={status} path={out_path}")

    return 0 if status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
