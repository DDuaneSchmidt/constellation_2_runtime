#!/usr/bin/env python3
"""
run_failure_injection_harness_v1.py

Bundle A9 — Failure Injection & Stress Harness (Report-only, deterministic)

This harness is intentionally NON-MUTATING.
It produces a governed report that:
- Records requested injections
- Reads existing gate artifacts
- Computes a deterministic *simulated* gate delta summary

It does NOT rewrite truth, does not attempt to redirect other tools, and does not
pretend to be a full sandbox. It is an empirical-proof scaffold that can be
extended into a full sandbox once all gate tools accept an injectable truth root.

Writes:
  truth/reports/failure_injection_harness_v1/<DAY>/<SCENARIO>/failure_injection_harness.v1.json

Run:
  python3 ops/tools/run_failure_injection_harness_v1.py --day_utc YYYY-MM-DD --scenario_id NAME [--correlation_override 0.8] ...
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

GATE_REGISTRY = (REPO_ROOT / "governance/02_REGISTRIES/GATE_HIERARCHY_V1.json").resolve()
OUT_ROOT = (TRUTH / "reports" / "failure_injection_harness_v1").resolve()

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


def _gate_artifact_path(day: str, gate: Dict[str, Any]) -> Path:
    rel = str(gate.get("artifact_relpath") or "").replace("{DAY}", day)
    return (REPO_ROOT / rel).resolve()


def _read_gate_status(path: Path, status_field: str) -> str:
    if not path.exists():
        return "MISSING"
    try:
        o = _read_json_obj(path)
        st = str(o.get(status_field) or "").strip().upper()
        return st if st else "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def _simulate_delta(base: str, injections: Dict[str, Any], gate_id: str) -> Tuple[str, str]:
    """Return (simulated_status, reason). Conservative: only worsens."""
    b = (base or "UNKNOWN").upper()

    # Mark delay / missing economic truth primarily impacts systemic risk and capital envelope.
    if injections.get("nav_missing") and gate_id in ("capital_risk_envelope_gate_v1",):
        if b in ("PASS","OK"):
            return ("FAIL", "SIM_NAV_MISSING_FAILCLOSED")
    if injections.get("attribution_missing") and gate_id in ("operator_gate_verdict_v3",):
        if b in ("PASS","OK"):
            return ("FAIL", "SIM_ATTRIBUTION_MISSING_FAILCLOSED")

    # Correlation override worsens systemic risk gate.
    if injections.get("correlation_override") is not None and gate_id == "systemic_risk_gate_v3":
        if b in ("PASS","OK") and float(str(injections["correlation_override"])) >= 0.75:
            return ("FAIL", "SIM_CORRELATION_OVERRIDE_EXCEEDS_THRESHOLD")

    # Replay drift is advisory here (non-blocking by registry default), but we still record degradation.
    if injections.get("replay_drift_bps") is not None and gate_id == "replay_integrity_v2":
        if b in ("PASS","OK"):
            return ("DEGRADED", "SIM_REPLAY_DRIFT")

    return (b, "NO_CHANGE")


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_failure_injection_harness_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--scenario_id", required=True, help="Scenario name (e.g., CORR_SPIKE_080)")
    ap.add_argument("--mark_delay_seconds", type=int, default=0)
    ap.add_argument("--correlation_override", type=float, default=None)
    ap.add_argument("--replay_drift_bps", type=float, default=None)
    ap.add_argument("--nav_missing", action="store_true")
    ap.add_argument("--attribution_missing", action="store_true")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    import re
    if not re.match(DAY_RE, day):
        raise SystemExit(f"FAIL: bad --day_utc (expected YYYY-MM-DD): {day!r}")

    if not GATE_REGISTRY.exists():
        raise SystemExit(f"FATAL: missing gate hierarchy registry: {GATE_REGISTRY}")

    reg = _read_json_obj(GATE_REGISTRY)

    injections: Dict[str, Any] = {
        "mark_delay_seconds": int(args.mark_delay_seconds),
        "correlation_override": (None if args.correlation_override is None else f"{float(args.correlation_override):.6f}"),
        "replay_drift_bps": (None if args.replay_drift_bps is None else f"{float(args.replay_drift_bps):.6f}"),
        "nav_missing": bool(args.nav_missing),
        "attribution_missing": bool(args.attribution_missing),
    }

    produced_utc = f"{day}T00:00:00Z"

    simulated: List[Dict[str, Any]] = []
    manifest: List[Dict[str, str]] = []

    for g in (reg.get("gates") or []):
        if not isinstance(g, dict):
            continue
        gate_id = str(g.get("gate_id") or "")
        status_field = str(g.get("status_field") or "status")
        path = _gate_artifact_path(day, g)
        base = _read_gate_status(path, status_field)
        sha = _sha256_file(path) if path.exists() else _sha256_bytes(b"")
        manifest.append({"type": f"gate_artifact_{gate_id}", "path": str(path), "sha256": sha})
        sim, why = _simulate_delta(base, injections, gate_id)
        delta = "UNCHANGED" if sim == base else f"{base}->{sim}"
        simulated.append({"gate_id": gate_id, "base_status": base, "simulated_status": sim, "delta": delta, "reason": why})

    out = {
        "schema_id": "failure_injection_harness",
        "schema_version": "v1",
        "produced_utc": produced_utc,
        "day_utc": day,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_failure_injection_harness_v1.py", "git_sha": _git_sha()},
        "scenario_id": str(args.scenario_id).strip(),
        "injections": injections,
        "simulated_gates": simulated,
        "notes": [
            "REPORT_ONLY: This harness does not rewrite truth or redirect tools.",
            "To enable full sandbox injection, gate tools must accept an explicit TRUTH_ROOT override.",
        ],
        "input_manifest": manifest,
    }

    out_path = (OUT_ROOT / day / str(args.scenario_id).strip() / "failure_injection_harness.v1.json").resolve()
    path, sha, action = _write_immutable(out_path, out)
    print(f"OK: failure_injection_harness_v1: action={action} sha256={sha} path={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
