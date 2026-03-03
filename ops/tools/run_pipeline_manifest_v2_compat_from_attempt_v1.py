#!/usr/bin/env python3
"""
run_pipeline_manifest_v2_compat_from_attempt_v1.py

Compatibility publisher:
- Reads Orchestrator V2 attempt manifest (v2) and emits legacy pipeline_manifest_v2 artifact.

Writes (immutable, one-time):
  constellation_2/runtime/truth/reports/pipeline_manifest_v2/<DAY>/pipeline_manifest.v2.json

Idempotency rule (non-bricking):
- If the legacy output already exists:
    - load + validate against schema
    - if schema-valid => print OK: ... EXISTS_VALID_SKIP and exit 0
    - else FAIL (corrupt legacy artifact)
- If missing: write once immutably

Determinism:
- produced_utc is day-scoped: <DAY>T00:00:00Z
- schema validation required (pipeline_manifest.v2.schema.json)

Top-level status mapping (v2 allows OK|FAIL|DEGRADED):
- If any effective_blocking stage FAIL => FAIL
- Else if any effective_required stage FAIL => FAIL
- Else if any non-required stage FAIL OR activity=false => DEGRADED
- Else OK
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1

REPO_ROOT = Path("/home/node/constellation_2_runtime").resolve()
TRUTH = (REPO_ROOT / "constellation_2/runtime/truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/pipeline_manifest.v2.schema.json"
OUT_ROOT = (TRUTH / "reports" / "pipeline_manifest_v2").resolve()

STAGE_IDS_V2: List[str] = [
    "INTENTS",
    "PREFLIGHT",
    "OMS",
    "ALLOCATION",
    "PHASED_SUBMISSIONS",
    "EXEC_EVIDENCE_TRUTH",
    "EXEC_EVIDENCE_MANIFEST",
    "SUBMISSION_INDEX",
    "ENGINE_RISK_BUDGET_LEDGER",
    "CAPITAL_RISK_ENVELOPE",
    "REGIME_CLASSIFICATION",
    "POSITIONS",
    "CASH_LEDGER",
    "ACCOUNTING",
    "RECONCILIATION",
    "OPERATOR_GATE",
    "BUNDLED_C_KILL_SWITCH",
    "BUNDLED_C_LIFECYCLE_LEDGER",
    "BUNDLED_C_EXPOSURE_RECONCILIATION",
    "BUNDLED_C_DELTA_ORDER_PLAN",
]


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _sha256_dir_deterministic(root: Path) -> str:
    if (not root.exists()) or (not root.is_dir()):
        return _sha256_bytes(b"")
    items: List[tuple[str, str]] = []
    for p in root.rglob("*"):
        if p.is_file():
            rel = str(p.relative_to(root)).replace("\\", "/")
            fsha = hashlib.sha256(p.read_bytes()).hexdigest()
            items.append((rel, fsha))
    items.sort(key=lambda x: x[0])
    h2 = hashlib.sha256()
    for rel, fsha in items:
        h2.update(rel.encode("utf-8"))
        h2.update(b"\n")
        h2.update(fsha.encode("utf-8"))
        h2.update(b"\n")
    return h2.hexdigest()


def _git_sha() -> str:
    out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
    s = out.decode("utf-8").strip()
    if len(s) != 40:
        raise SystemExit(f"FAIL: bad git sha: {s!r}")
    return s


def _require_day(s: str) -> str:
    d = (s or "").strip()
    if len(d) != 10 or d[4] != "-" or d[7] != "-":
        raise SystemExit(f"FAIL: bad --day_utc: {d!r}")
    return d


def _read_attempt_manifest(path: Path) -> Dict[str, Any]:
    if (not path.exists()) or (not path.is_file()):
        raise SystemExit(f"FAIL: attempt manifest missing: {path}")
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: attempt manifest invalid JSON: {e!r}") from e
    if not isinstance(obj, dict):
        raise SystemExit("FAIL: attempt manifest not object")
    return obj


def _stage_row(stage_id: str, status: str, blocking: bool, reason_codes: List[str], root: Path) -> Dict[str, Any]:
    present = root.exists() and root.is_dir()
    sha = _sha256_dir_deterministic(root) if present else _sha256_bytes(b"")
    items_total = 0
    if present:
        items_total = len([p for p in root.rglob("*") if p.is_file()])
    return {
        "stage_id": stage_id,
        "status": status,
        "blocking": bool(blocking),
        "reason_codes": list(reason_codes),
        "counts": {"items_total": int(items_total), "items_ok": None, "items_fail": None},
        "artifacts": {"root": str(root), "present": bool(present), "sha256": str(sha)},
    }


def _validate_existing_or_fail(path: Path) -> Dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise SystemExit(f"FAIL: existing legacy manifest invalid JSON: {path} err={e!r}") from e
    validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
    return obj


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_pipeline_manifest_v2_compat_from_attempt_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--attempt_manifest_path", required=True)
    args = ap.parse_args()

    day = _require_day(args.day_utc)

    out_path = (OUT_ROOT / day / "pipeline_manifest.v2.json").resolve()

    # Idempotency: if already present, validate and exit OK.
    if out_path.exists():
        _validate_existing_or_fail(out_path)
        ex_sha = hashlib.sha256(out_path.read_bytes()).hexdigest()
        print(f"OK: PIPELINE_MANIFEST_V2_COMPAT_EXISTS_VALID_SKIP day_utc={day} path={out_path} sha256={ex_sha}")
        return 0

    produced_utc = f"{day}T00:00:00Z"

    man_path = Path(str(args.attempt_manifest_path)).resolve()
    man = _read_attempt_manifest(man_path)
    man_sha = hashlib.sha256(man_path.read_bytes()).hexdigest()

    stages_in = man.get("stages") or []
    if not isinstance(stages_in, list):
        raise SystemExit("FAIL: attempt manifest stages not list")

    act = man.get("activity") or {}
    activity = bool(isinstance(act, dict) and act.get("activity"))

    blocking_failures = 0
    required_failures = 0
    nonblocking_degradations = 0

    for s in stages_in:
        if not isinstance(s, dict):
            continue
        st = str(s.get("status") or "").strip().upper()
        cls = s.get("classification") or {}
        if not isinstance(cls, dict):
            cls = {}
        eff_req = bool(cls.get("effective_required"))
        eff_blk = bool(cls.get("effective_blocking"))

        if st == "FAIL" and eff_blk:
            blocking_failures += 1
        elif st == "FAIL" and eff_req:
            required_failures += 1
        elif st == "FAIL":
            nonblocking_degradations += 1

    reason_codes: List[str] = []
    notes: List[str] = []

    if blocking_failures > 0 or required_failures > 0:
        status_top = "FAIL"
        reason_codes.append("ATTEMPT_DERIVED_REQUIRED_FAILURES")
        if blocking_failures > 0:
            reason_codes.append("ATTEMPT_DERIVED_BLOCKING_FAILURES")
    else:
        if (nonblocking_degradations > 0) or (not activity):
            status_top = "DEGRADED"
            if nonblocking_degradations > 0:
                reason_codes.append("ATTEMPT_DERIVED_NONBLOCKING_DEGRADATIONS")
            if not activity:
                reason_codes.append("NO_ACTIVITY_DAY")
        else:
            status_top = "OK"

    notes.append("compat_publisher=v2_from_orchestrator_attempt_manifest_v2")
    notes.append("idempotent: existing legacy file validated then skipped (no rewrites)")
    notes.append("status_mapping: required/blocking FAIL=>FAIL; else optional/no-activity=>DEGRADED; else OK")

    # Evidence roots (legacy v2 stage ids + evidence dirs)
    intents_day = (TRUTH / "intents_v1/snapshots" / day).resolve()
    preflight_day = (TRUTH / "phaseC_preflight_v1" / day).resolve()
    oms_day = (TRUTH / "oms_decisions_v1/decisions" / day).resolve()
    alloc_day = (TRUTH / "allocation_v1/summary" / day).resolve()
    phased_root = (REPO_ROOT / "constellation_2/phaseD/outputs/submissions").resolve()
    exec_truth_day = (TRUTH / "execution_evidence_v1/submissions" / day).resolve()
    exec_manifest_day = (TRUTH / "execution_evidence_v1/manifests" / day).resolve()
    sub_index_day = (TRUTH / "execution_evidence_v1/submission_index" / day).resolve()
    risk_budget_day = (TRUTH / "risk_v1/engine_budget" / day).resolve()
    cap_risk_day = (TRUTH / "reports/capital_risk_envelope_v2" / day).resolve()
    regime_day = (TRUTH / "monitoring_v1/regime_snapshot_v3" / day).resolve()
    positions_day = (TRUTH / "positions_v1/snapshots" / day).resolve()
    cash_day = (TRUTH / "cash_ledger_v1/snapshots" / day).resolve()
    accounting_day = (TRUTH / "accounting_v1" / day).resolve()
    recon_day = (TRUTH / "reports/reconciliation_report_v3" / day).resolve()
    op_gate_day = (TRUTH / "reports/operator_daily_gate_v1" / day).resolve()
    kill_day = (TRUTH / "risk_v1/kill_switch_v1" / day).resolve()
    life_day = (TRUTH / "position_lifecycle_v1/ledger" / day).resolve()
    crecon_day = (TRUTH / "reports/exposure_reconciliation_report_v1" / day).resolve()
    plan_day = (TRUTH / "reports/delta_order_plan_v1" / day).resolve()

    base_ok = "OK" if status_top in ("OK", "DEGRADED") else "FAIL"

    stages: List[Dict[str, Any]] = []
    stages.append(_stage_row("INTENTS", base_ok, False, [], intents_day))
    stages.append(_stage_row("PREFLIGHT", base_ok, False, [], preflight_day))
    stages.append(_stage_row("OMS", base_ok, False, [], oms_day))
    stages.append(_stage_row("ALLOCATION", base_ok, False, [], alloc_day))
    stages.append(_stage_row("PHASED_SUBMISSIONS", base_ok, False, [], phased_root))
    stages.append(_stage_row("EXEC_EVIDENCE_TRUTH", base_ok, False, [], exec_truth_day))
    stages.append(_stage_row("EXEC_EVIDENCE_MANIFEST", base_ok, False, [], exec_manifest_day))
    stages.append(_stage_row("SUBMISSION_INDEX", base_ok, False, [], sub_index_day))
    stages.append(_stage_row("ENGINE_RISK_BUDGET_LEDGER", base_ok, False, [], risk_budget_day))
    stages.append(_stage_row("CAPITAL_RISK_ENVELOPE", base_ok, False, [], cap_risk_day))
    stages.append(_stage_row("REGIME_CLASSIFICATION", base_ok, False, [], regime_day))
    stages.append(_stage_row("POSITIONS", base_ok, False, [], positions_day))
    stages.append(_stage_row("CASH_LEDGER", base_ok, False, [], cash_day))
    stages.append(_stage_row("ACCOUNTING", base_ok, False, [], accounting_day))
    stages.append(_stage_row("RECONCILIATION", base_ok, False, [], recon_day))
    stages.append(_stage_row("OPERATOR_GATE", base_ok, False, [], op_gate_day))
    stages.append(_stage_row("BUNDLED_C_KILL_SWITCH", base_ok, False, [], kill_day))
    stages.append(_stage_row("BUNDLED_C_LIFECYCLE_LEDGER", base_ok, False, [], life_day))
    stages.append(_stage_row("BUNDLED_C_EXPOSURE_RECONCILIATION", base_ok, False, [], crecon_day))
    stages.append(_stage_row("BUNDLED_C_DELTA_ORDER_PLAN", base_ok, False, [], plan_day))

    emitted = [x.get("stage_id") for x in stages]
    if any(sid not in STAGE_IDS_V2 for sid in emitted):
        raise SystemExit("FAIL: emitted stage_id not allowed by schema (internal bug)")

    out = {
        "schema_id": "pipeline_manifest",
        "schema_version": "v2",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {
            "repo": "constellation_2_runtime",
            "module": "ops/tools/run_pipeline_manifest_v2_compat_from_attempt_v1.py",
            "git_sha": _git_sha(),
        },
        "status": status_top,
        "reason_codes": list(reason_codes),
        "notes": list(notes),
        "input_manifest": [{"type": "orchestrator_attempt_manifest_v2", "path": str(man_path), "sha256": str(man_sha)}],
        "stages": stages,
        "summary": {
            "blocking_failures": int(blocking_failures + required_failures),
            "nonblocking_degradations": int(nonblocking_degradations + (0 if activity else 1)),
        },
    }

    validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA_RELPATH)

    try:
        wr = write_file_immutable_v1(path=out_path, data=_json_bytes(out), create_dirs=True)
    except ImmutableWriteError as e:
        raise SystemExit(f"FAIL_IMMUTABLE_WRITE: {e}") from e

    print(
        f"OK: PIPELINE_MANIFEST_V2_COMPAT_WRITTEN day_utc={day} status={status_top} "
        f"path={wr.path} sha256={wr.sha256} action={wr.action}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
