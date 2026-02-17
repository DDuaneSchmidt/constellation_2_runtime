#!/usr/bin/env python3
"""
Bundle A: operator_gate_verdict.v1.json writer (immutable truth artifact).

- Deterministic and audit-grade.
- MUST write a verdict artifact even on failure.
- Fail-closed: exits nonzero when ready=false.

Reads required artifacts:
  intents_day_rollup.v1.json
  submission_index.v1.json
  reconciliation_report.v2.json
  pipeline_manifest.v1.json

Also enforces Bundle B gates when present:
  engine_risk_budget_ledger.v1.json status=OK
  capital_risk_envelope.v1.json status=PASS

Bundled C enforcement (blocking readiness):
  global_kill_switch_state.v1.json exists and state=INACTIVE
  position_lifecycle_ledger.v1.json exists
  exposure_reconciliation_report.v1.json exists
  delta_order_plan.v1.json exists

Writes:
  constellation_2/runtime/truth/reports/operator_gate_verdict_v1/<DAY>/operator_gate_verdict.v1.json

Validates schema:
  governance/04_DATA/SCHEMAS/C2/REPORTS/operator_gate_verdict.v1.schema.json

Run:
  python3 ops/tools/run_operator_gate_verdict_v1.py --day_utc YYYY-MM-DD
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1  # type: ignore


TRUTH = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/operator_gate_verdict.v1.schema.json"

PATH_INTENTS_ROLLUP = TRUTH / "intents_v1" / "day_rollup"
PATH_SUBMISSION_INDEX = TRUTH / "execution_evidence_v1" / "submission_index"
PATH_RECON_V2 = TRUTH / "reports" / "reconciliation_report_v2"
PATH_PIPELINE_MANIFEST = TRUTH / "reports" / "pipeline_manifest_v1"
PATH_EXEC_SUBMISSIONS = TRUTH / "execution_evidence_v1" / "submissions"
PATH_BROKER_DAY_MANIFEST = TRUTH / "execution_evidence_v1" / "broker_events"
PATH_ENGINE_RISK_BUDGET_LEDGER = TRUTH / "risk_v1" / "engine_budget"
PATH_CAPITAL_RISK_ENVELOPE = TRUTH / "reports" / "capital_risk_envelope_v1"

# Bundled C
PATH_KILL_SWITCH = TRUTH / "risk_v1" / "kill_switch_v1"
PATH_LIFECYCLE_LEDGER = TRUTH / "position_lifecycle_v1" / "ledger"
PATH_EXPOSURE_RECON = TRUTH / "reports" / "exposure_reconciliation_report_v1"
PATH_DELTA_PLAN = TRUTH / "reports" / "delta_order_plan_v1"

OUT_ROOT = TRUTH / "reports" / "operator_gate_verdict_v1"

DAY_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _git_sha() -> str:
    import subprocess
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_schema_available(schema_path: Path) -> None:
    if not schema_path.exists():
        raise SystemExit(f"FAIL: missing governed schema: {schema_path}")


def _validate_jsonschema_or_fail(obj: Dict[str, Any], schema_path: Path) -> None:
    try:
        import jsonschema  # type: ignore
    except Exception as e:
        raise SystemExit(f"FAIL: jsonschema not available for validation: {e}")

    schema = _read_json(schema_path)
    try:
        jsonschema.validate(instance=obj, schema=schema)
    except Exception as e:
        raise SystemExit(f"FAIL: schema validation failed: {e}")


@dataclass(frozen=True)
class _WriteResult:
    path: str
    sha256: str
    action: str


def _write_immutable_canonical_json(path: Path, obj: Dict[str, Any]) -> _WriteResult:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes_v1(obj) + b"\n"
    sha = _sha256_bytes(payload)

    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == sha:
            return _WriteResult(path=str(path), sha256=sha, action="EXISTS_IDENTICAL")
        raise SystemExit(f"FAIL: refusing overwrite (different bytes): {path}")

    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    os.replace(tmp, path)
    return _WriteResult(path=str(path), sha256=sha, action="WRITTEN")


def _compute_self_sha_field(obj: Dict[str, Any], field_name: str) -> str:
    obj2 = dict(obj)
    obj2[field_name] = None
    canon = canonical_json_bytes_v1(obj2) + b"\n"
    return _sha256_bytes(canon)


def _check_exists(path: Path) -> bool:
    return path.exists() and path.is_file()


def _scan_for_synth_markers(day: str) -> bool:
    root = (PATH_EXEC_SUBMISSIONS / day).resolve()
    if not root.exists():
        return False
    for p in sorted(root.rglob("*.json")):
        try:
            t = p.read_text(encoding="utf-8")
        except Exception:
            continue
        if "SYNTH_" in t:
            return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_operator_gate_verdict_v1")
    ap.add_argument("--day_utc", required=True, help="UTC day key YYYY-MM-DD")
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if not DAY_RE.match(day):
        raise SystemExit(f"FAIL: bad --day_utc (expected YYYY-MM-DD): {day!r}")

    schema_path = (REPO_ROOT / SCHEMA_RELPATH).resolve()
    _validate_schema_available(schema_path)
    produced_utc = _now_utc_iso()

    intents_path = (PATH_INTENTS_ROLLUP / day / "intents_day_rollup.v1.json").resolve()
    subidx_path = (PATH_SUBMISSION_INDEX / day / "submission_index.v1.json").resolve()
    recon_path = (PATH_RECON_V2 / day / "reconciliation_report.v2.json").resolve()
    pipe_path = (PATH_PIPELINE_MANIFEST / day / "pipeline_manifest.v1.json").resolve()
    risk_ledger_path = (PATH_ENGINE_RISK_BUDGET_LEDGER / day / "engine_risk_budget_ledger.v1.json").resolve()
    cap_env_path = (PATH_CAPITAL_RISK_ENVELOPE / day / "capital_risk_envelope.v1.json").resolve()

    # Bundled C paths
    kill_path = (PATH_KILL_SWITCH / day / "global_kill_switch_state.v1.json").resolve()
    life_path = (PATH_LIFECYCLE_LEDGER / day / "position_lifecycle_ledger.v1.json").resolve()
    exp_path = (PATH_EXPOSURE_RECON / day / "exposure_reconciliation_report.v1.json").resolve()
    plan_path = (PATH_DELTA_PLAN / day / "delta_order_plan.v1.json").resolve()

    checks: List[Dict[str, Any]] = []
    missing: List[str] = []
    mismatches: List[Dict[str, Any]] = []

    # Required existence
    for cid, p in [
        ("REQ_INTENTS_DAY_ROLLUP", intents_path),
        ("REQ_SUBMISSION_INDEX", subidx_path),
        ("REQ_RECONCILIATION_V2", recon_path),
        ("REQ_PIPELINE_MANIFEST", pipe_path),
        ("REQ_ENGINE_RISK_BUDGET_LEDGER", risk_ledger_path),
        ("REQ_CAPITAL_RISK_ENVELOPE", cap_env_path),

        # Bundled C required artifacts
        ("REQ_BUNDLED_C_KILL_SWITCH", kill_path),
        ("REQ_BUNDLED_C_LIFECYCLE_LEDGER", life_path),
        ("REQ_BUNDLED_C_EXPOSURE_RECON", exp_path),
        ("REQ_BUNDLED_C_DELTA_PLAN", plan_path),
    ]:
        ok = _check_exists(p)
        if not ok:
            missing.append(str(p))
        checks.append({"check_id": cid, "pass": ok, "evidence_paths": [str(p)], "details": "exists" if ok else "missing"})

    # No SYNTH markers
    has_synth = _scan_for_synth_markers(day)
    checks.append(
        {
            "check_id": "NO_SYNTH_MARKERS",
            "pass": (not has_synth),
            "evidence_paths": [str((PATH_EXEC_SUBMISSIONS / day).resolve())],
            "details": "scan for SYNTH_ markers under execution evidence submissions",
        }
    )

    # Engine risk budget ledger must be OK (Bundle B fail-closed)
    ledger_ok = False
    ledger_details = "missing"
    if _check_exists(risk_ledger_path):
        try:
            rl = _read_json(risk_ledger_path)
            st = str(rl.get("status") or "").strip().upper()
            ledger_ok = (st == "OK")
            ledger_details = f"status={st}"
        except Exception as e:
            ledger_ok = False
            ledger_details = f"parse_error={e!r}"

    checks.append(
        {
            "check_id": "ENGINE_RISK_BUDGET_LEDGER_OK",
            "pass": ledger_ok,
            "evidence_paths": [str(risk_ledger_path)],
            "details": ledger_details,
        }
    )

    # Capital risk envelope must be PASS (Bundle B fail-closed)
    cap_ok = False
    cap_details = "missing"
    if _check_exists(cap_env_path):
        try:
            ce = _read_json(cap_env_path)
            st = str(ce.get("status") or "").strip().upper()
            cap_ok = (st == "PASS")
            cap_details = f"status={st}"
        except Exception as e:
            cap_ok = False
            cap_details = f"parse_error={e!r}"

    checks.append(
        {
            "check_id": "CAPITAL_RISK_ENVELOPE_PASS",
            "pass": cap_ok,
            "evidence_paths": [str(cap_env_path)],
            "details": cap_details,
        }
    )

    # Broker truth manifest must be OK if submissions exist
    broker_ok = True
    broker_details = "no submissions => broker manifest not required"
    subs_dir = (PATH_EXEC_SUBMISSIONS / day).resolve()
    has_submissions = subs_dir.exists() and any(p.is_dir() for p in subs_dir.iterdir())
    manifest_path = (PATH_BROKER_DAY_MANIFEST / day / "broker_event_day_manifest.v1.json").resolve()

    if has_submissions:
        broker_details = "submissions present => broker manifest must exist and status OK"
        if not manifest_path.exists():
            broker_ok = False
            missing.append(str(manifest_path))
        else:
            try:
                bm = _read_json(manifest_path)
                st = str(bm.get("status") or "").strip().upper()
                if st not in ("OK", "PASS"):
                    broker_ok = False
            except Exception:
                broker_ok = False

    checks.append(
        {
            "check_id": "BROKER_DAY_MANIFEST_OK_IF_SUBMISSIONS",
            "pass": broker_ok,
            "evidence_paths": [str(manifest_path)],
            "details": broker_details,
        }
    )

    # Submission index mode constraints
    sim_found = False
    if _check_exists(subidx_path):
        subidx = _read_json(subidx_path)
        for r in subidx.get("records", []):
            if str(r.get("mode") or "").strip() != "REAL_IB_PAPER":
                sim_found = True
                break
    checks.append(
        {
            "check_id": "SUBMISSION_INDEX_REAL_IB_PAPER_ONLY",
            "pass": (not sim_found),
            "evidence_paths": [str(subidx_path)],
            "details": "all submission_index records must be REAL_IB_PAPER",
        }
    )

    # Reconciliation verdict PASS
    recon_ok = False
    if _check_exists(recon_path):
        recon = _read_json(recon_path)
        verdict = str(recon.get("verdict") or recon.get("status") or "").strip().upper()
        recon_ok = (verdict == "PASS")
    checks.append(
        {
            "check_id": "RECONCILIATION_PASS",
            "pass": recon_ok,
            "evidence_paths": [str(recon_path)],
            "details": "reconciliation_report.v2 must have verdict PASS",
        }
    )

    # Bundled C: kill switch must be INACTIVE
    kill_inactive = False
    kill_details = "missing"
    if _check_exists(kill_path):
        try:
            ks = _read_json(kill_path)
            st = str(ks.get("state") or "").strip().upper()
            kill_inactive = (st == "INACTIVE")
            kill_details = f"state={st}"
        except Exception as e:
            kill_inactive = False
            kill_details = f"parse_error={e!r}"
    checks.append(
        {
            "check_id": "BUNDLED_C_KILL_SWITCH_INACTIVE",
            "pass": kill_inactive,
            "evidence_paths": [str(kill_path)],
            "details": kill_details,
        }
    )

    # Ready iff all checks pass and no missing artifacts
    all_pass = all(bool(c.get("pass")) for c in checks)
    ready = bool(all_pass and (len(missing) == 0))
    exit_code = 0 if ready else 2

    verdict_obj: Dict[str, Any] = {
        "schema_id": "operator_gate_verdict.v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"component": "ops/tools/run_operator_gate_verdict_v1.py", "version": "v1", "git_sha": _git_sha()},
        "checks": checks,
        "missing_artifacts": missing,
        "hash_mismatches": mismatches,
        "ready": ready,
        "exit_code": int(exit_code),
        "verdict_sha256": None,
    }
    verdict_obj["verdict_sha256"] = _compute_self_sha_field(verdict_obj, "verdict_sha256")

    _validate_jsonschema_or_fail(verdict_obj, schema_path)

    out_dir = (OUT_ROOT / day).resolve()
    out_path = (out_dir / "operator_gate_verdict.v1.json").resolve()
    wr = _write_immutable_canonical_json(out_path, verdict_obj)

    print(f"OK: OPERATOR_GATE_VERDICT_WRITTEN day_utc={day} ready={ready} exit_code={exit_code} path={wr.path} sha256={wr.sha256} action={wr.action}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
