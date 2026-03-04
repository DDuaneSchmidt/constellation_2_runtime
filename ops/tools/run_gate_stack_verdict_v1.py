#!/usr/bin/env python3
"""
run_gate_stack_verdict_v1.py

Bundle A8 — Gate Hierarchy & Precedence Formalization

Purpose:
- Deterministically evaluate a governed set of gate artifacts for a day.
- Apply precedence (CLASS1 > CLASS2 > CLASS3 > CLASS4).
- Emit a single immutable verdict artifact.

Writes:
  <truth_root>/reports/gate_stack_verdict_v1/<DAY>/gate_stack_verdict.v1.json

Run:
  python3 ops/tools/run_gate_stack_verdict_v1.py --day_utc YYYY-MM-DD --truth_root /abs/path --produced_utc YYYY-MM-DDT00:00:00Z --mode PAPER

Non-negotiable:
- Pure evaluation (no mutation of inputs)
- Fail-closed for required gates
- Immutable write
- Deterministic: NO wall clock. produced_utc must be supplied and must equal <DAY>T00:00:00Z.

SELF-HEAL (governed correctness under registry changes):
- If canonical verdict exists and is PASS: never replace it.
- If canonical verdict exists and is FAIL:
    - If recomputation is byte-identical: no-op.
    - If recomputation differs (e.g., registry/gate set changed), quarantine old canonical
      as gate_stack_verdict.v1.json.INVALID_<old_sha>.json and write the new canonical,
      even if the new status is still FAIL.
This ensures the canonical verdict always reflects the current governed registry while preserving
audit history of prior canonicals.
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
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # type: ignore
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1  # type: ignore

REPO_ROOT = _REPO_ROOT_FROM_FILE.resolve()

REGISTRY_PATH = (REPO_ROOT / "governance/02_REGISTRIES/GATE_HIERARCHY_V1.json").resolve()
SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/gate_stack_verdict.v1.schema.json"

DAY_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"

# Must match schema enum for top-level blocking_class.
_ALLOWED_BLOCKING_CLASSES = {
    "CLASS1_SYSTEM_HARD_STOP",
    "CLASS2_RISK_HARD_STOP",
    "CLASS3_CONTROLLED_DEGRADATION",
    "CLASS4_ADVISORY",
    "NONE",
}


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> str:
    # Gate stack registry must point to a FILE artifact, never a directory.
    if p.exists() and p.is_dir():
        raise ValueError(f"GATE_STACK_BAD_ARTIFACT_PATH_IS_DIRECTORY: {str(p)}")
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


def _class_precedence(reg: Dict[str, Any]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for c in reg.get("classes") or []:
        if not isinstance(c, dict):
            continue
        cid = str(c.get("class_id") or "").strip()
        prec = int(c.get("precedence") or 9999)
        if cid:
            out[cid] = prec
    return out


def _require_truth_root(p: str) -> Path:
    s = (p or "").strip()
    if not s:
        raise SystemExit("FAIL: --truth_root empty")
    pr = Path(s).expanduser().resolve()
    if not pr.is_absolute():
        raise SystemExit(f"FAIL: --truth_root must be absolute: {pr}")
    if not pr.exists() or (not pr.is_dir()):
        raise SystemExit(f"FAIL: --truth_root must exist and be a directory: {pr}")
    return pr


def _require_produced_utc_for_day(day: str, produced_utc: str) -> str:
    v = (produced_utc or "").strip()
    expected = f"{day}T00:00:00Z"
    if v != expected:
        raise SystemExit(f"FAIL: produced_utc_must_equal_day_marker expected={expected!r} got={v!r}")
    return v


def _eval_gate(*, truth_root: Path, day: str, gate: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, str]]]:
    # Returns (gate_result, input_manifest_entries)
    gate_id = str(gate.get("gate_id") or "").strip()

    # GOVERNANCE: registry uses key "class" (authoritative). Support "gate_class" for backward compatibility.
    gclass = str(gate.get("class") or gate.get("gate_class") or "").strip()

    required = bool(gate.get("required"))
    blocking = bool(gate.get("blocking"))
    rel = str(gate.get("artifact_relpath") or "").replace("{DAY}", day)
    status_field = str(gate.get("status_field") or "status")
    pass_vals = [str(x).strip().upper() for x in (gate.get("pass_status_values") or [])]
    if not pass_vals:
        # Governance default: PASS/OK are the canonical passing values.
        pass_vals = ["PASS", "OK"]
    # IMPORTANT: empty relpath must be treated as invalid/missing (fail-closed).
    #
    # Registry remediation 2026-03-02: artifact_relpath is file-backed and NOT a directory.
    # Resolve artifact paths under truth_root, NOT repo root.
    #
    # Supported patterns:
    # - If rel starts with "reports/" or "risk_v1/" or "monitoring_v1/" etc -> treat as truth_root-relative.
    # - Otherwise, default to canonical reports layout:
    #     truth_root/reports/<gate_id>/<DAY>/<rel>
    if not rel:
        # Deterministic sentinel under truth root (do NOT point at repo root).
        path = (truth_root / "reports" / gate_id / day / "__MISSING_RELPATH__").resolve()
    else:
        rel_norm = rel.lstrip("/")

        if rel_norm.startswith(
            (
                "reports/",
                "risk_v1/",
                "monitoring_v1/",
                "monitoring_v2/",
                "execution_evidence_v1/",
                "accounting_v1/",
                "accounting_v2/",
            )
        ):
            path = (truth_root / rel_norm).resolve()
        else:
            path = (truth_root / "reports" / gate_id / day / rel_norm).resolve()

    manifest: List[Dict[str, str]] = []

    if not rel:
        # Empty relpath is a governance breach; represent as MISSING (schema-safe) and fail-closed via required logic.
        manifest.append({"type": f"{gate_id}_missing", "path": str(path), "sha256": _sha256_bytes(b"")})
        status = "MISSING"
        rc = ["MISSING_GATE_ARTIFACT_RELPATH_EMPTY"]
        sha = _sha256_bytes(b"")
    elif not path.exists():
        manifest.append({"type": f"{gate_id}_missing", "path": str(path), "sha256": _sha256_bytes(b"")})
        status = "MISSING"
        rc = ["MISSING_GATE_ARTIFACT"]
        sha = _sha256_bytes(b"")
    else:
        rc = []
        try:
            sha = _sha256_file(path)
        except ValueError as e:
            # Deterministic fail-closed: a directory is never a valid artifact file.
            # Schema-safe status: treat as MISSING (do not emit ungoverned enums like INVALID_PATH).
            manifest.append({"type": f"{gate_id}_bad_path", "path": str(path), "sha256": _sha256_bytes(b"")})
            status = "MISSING"
            rc = [str(e)]
            sha = _sha256_bytes(b"")
        else:
            manifest.append({"type": gate_id, "path": str(path), "sha256": sha})
            try:
                o = _read_json_obj(path)
                raw = str(o.get(status_field) or "").strip().upper()
                status = raw if raw else "UNKNOWN"
                # Normalize to schema-safe status enums:
                # allowed: PASS, FAIL, OK, DEGRADED, MISSING, UNKNOWN
                if raw in ("OK", "PASS"):
                    status_norm = raw
                elif raw in ("FAIL", "BLOCK", "BLOCKED", "BLOCK_ALL", "MISSING_INPUTS"):
                    status_norm = "FAIL"
                elif raw in ("SCALE", "DEGRADED"):
                    status_norm = "DEGRADED"
                elif raw in ("MISSING",):
                    status_norm = "MISSING"
                else:
                    status_norm = "UNKNOWN"
                status = status_norm

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


def _compute_verdict(*, truth_root: Path, day: str, produced_utc: str) -> Dict[str, Any]:
    if not REGISTRY_PATH.exists():
        raise SystemExit(f"FATAL: missing gate hierarchy registry: {REGISTRY_PATH}")

    reg = _read_json_obj(REGISTRY_PATH)
    prec = _class_precedence(reg)

    gates: List[Dict[str, Any]] = []
    manifest: List[Dict[str, str]] = []

    for g in (reg.get("gates") or []):
        if not isinstance(g, dict):
            continue
        gr, man = _eval_gate(truth_root=truth_root, day=day, gate=g)
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
        cid = str(g.get("gate_class") or "").strip()
        required = bool(g.get("required"))
        blocking = bool(g.get("blocking"))
        observed = str(g.get("status") or "").strip().upper()

        is_missing = (observed == "MISSING")
        is_pass = (observed in ("PASS", "OK"))
        is_failish = (not is_pass)

        def _clamp_blocking_class(x: str) -> str:
            x2 = str(x or "").strip()
            if x2 in _ALLOWED_BLOCKING_CLASSES:
                return x2
            # Fail-closed: if we are failing due to a gate, use the strictest allowed enum.
            return "CLASS1_SYSTEM_HARD_STOP"

        # Required non-pass is a fail.
        if required and is_failish:
            status = "FAIL"
            blocking_class = _clamp_blocking_class(cid)
            reason_codes.append(f"GATE_REQUIRED_NOT_PASS:{g.get('gate_id')}:{observed}")
            if is_missing:
                reason_codes.append(f"GATE_MISSING:{g.get('gate_id')}")
            break

        # Non-required but blocking gates can still fail closed if present and not passing.
        if (not required) and blocking and is_failish and (not is_missing):
            status = "FAIL"
            blocking_class = _clamp_blocking_class(cid)
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
                "reason_codes": g.get("reason_codes") if isinstance(g.get("reason_codes"), list) else [],
            }
            for g in gates_sorted
        ],
    }

    validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA_RELPATH)
    return out


def _self_heal_if_needed(day: str, out_path: Path, new_obj: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Returns (self_heal_performed, action_string)
    action_string is one of: EXISTS, WROTE, EXISTS_IDENTICAL
    """
    if not out_path.exists():
        return (False, "WROTE")

    existing_sha = _sha256_file(out_path)
    existing = _read_json_obj(out_path)

    schema_id = str(existing.get("schema_id") or "").strip()
    schema_version = str(existing.get("schema_version") or "").strip()
    day_utc = str(existing.get("day_utc") or "").strip()
    status = str(existing.get("status") or "").strip().upper()

    if schema_id != "gate_stack_verdict":
        raise SystemExit(f"FAIL: EXISTING_GATE_STACK_SCHEMA_MISMATCH: schema_id={schema_id!r} path={out_path}")
    if schema_version != "v1":
        raise SystemExit(f"FAIL: EXISTING_GATE_STACK_SCHEMA_VERSION_MISMATCH: schema_version={schema_version!r} path={out_path}")
    if day_utc != day:
        raise SystemExit(f"FAIL: EXISTING_GATE_STACK_DAY_MISMATCH: day_utc={day_utc!r} expected={day!r} path={out_path}")
    if status not in ("PASS", "FAIL"):
        raise SystemExit(f"FAIL: EXISTING_GATE_STACK_STATUS_INVALID: status={status!r} path={out_path}")

    new_bytes = canonical_json_bytes_v1(new_obj) + b"\n"
    new_sha = _sha256_bytes(new_bytes)
    if new_sha == existing_sha:
        return (False, "EXISTS_IDENTICAL")

    # If canonical is PASS, never replace it.
    if status == "PASS":
        return (False, "EXISTS")

    # Canonical is FAIL and differs from recomputation: quarantine old and write new (even if new is FAIL).
    invalid_path = out_path.with_name(f"gate_stack_verdict.v1.json.INVALID_{existing_sha}.json")
    if invalid_path.exists():
        raise SystemExit(
            f"FAIL: INVALID_EXISTING_GATE_STACK_ALREADY_QUARANTINED: day_utc={day} existing_sha={existing_sha} "
            f"out_path={out_path} invalid_path={invalid_path}"
        )

    out_path.rename(invalid_path)
    print(
        f"WARN: QUARANTINED_STALE_GATE_STACK_VERDICT day_utc={day} "
        f"old_path={out_path} quarantined_path={invalid_path} sha256={existing_sha}"
    )
    return (True, "WROTE")


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_gate_stack_verdict_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    ap.add_argument("--truth_root", required=True, help="Absolute truth root directory (sleeve truth root)")
    ap.add_argument("--produced_utc", required=True, help="Must equal <DAY>T00:00:00Z")
    ap.add_argument("--mode", required=True, choices=["PAPER", "LIVE"])  # accepted for wiring determinism (not emitted unless schema allows)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    import re
    if not re.match(DAY_RE, day):
        raise SystemExit(f"FAIL: bad --day_utc (expected YYYY-MM-DD): {day!r}")

    truth_root = _require_truth_root(args.truth_root)
    produced_utc = _require_produced_utc_for_day(day, str(args.produced_utc))
    _ = str(args.mode).strip().upper()  # validated by argparse choices

    out_dir = (truth_root / "reports" / "gate_stack_verdict_v1" / day).resolve()
    out_path = (out_dir / "gate_stack_verdict.v1.json").resolve()

    out_obj = _compute_verdict(truth_root=truth_root, day=day, produced_utc=produced_utc)
    self_heal, intended_action = _self_heal_if_needed(day, out_path, out_obj)

    payload = canonical_json_bytes_v1(out_obj) + b"\n"

    if intended_action in ("EXISTS", "EXISTS_IDENTICAL"):
        sha = _sha256_file(out_path)
        action = intended_action
    else:
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            wr = write_file_immutable_v1(path=out_path, data=payload, create_dirs=False)
            sha = wr.sha256
            action = "WRITTEN" if wr.action == "WROTE" else str(wr.action)
        except ImmutableWriteError as e:
            raise SystemExit(f"FAIL_IMMUTABLE_WRITE: {e}") from e

    print(f"OK: gate_stack_verdict_v1: action={action} sha256={sha} path={out_path}" + (f" self_heal=1" if self_heal else ""))
    return 0 if str(out_obj.get("status") or "").strip().upper() == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
