#!/usr/bin/env python3
"""
Bundle B (Component 1): Engine Risk Budget Ledger v1 (immutable truth artifact)

Writes:
  constellation_2/runtime/truth/risk_v1/engine_budget/<DAY>/engine_risk_budget_ledger.v1.json

Schema (governed):
  governance/04_DATA/SCHEMAS/C2/RISK/engine_risk_budget_ledger.v1.schema.json

Institutional posture:
- Deterministic, audit-grade, fail-closed.
- ALWAYS writes an artifact with status OK|FAIL (even on failure).
- Budgets MUST be provable from governed registry:
    governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json
  If budget fields are missing, ledger FAILs closed (no silent default budgets).
- Used exposure attribution is conservative:
    If submissions exist for DAY and engine attribution is missing => FAIL.
    If no submissions exist => used_notional_pct is 0 for all engines (OK).

Run:
  python3 ops/tools/run_engine_risk_budget_ledger_v1.py --day_utc YYYY-MM-DD
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
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

TRUTH = (REPO_ROOT / "constellation_2" / "runtime" / "truth").resolve()

SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/RISK/engine_risk_budget_ledger.v1.schema.json"
REGISTRY_RELPATH = "governance/02_REGISTRIES/ENGINE_MODEL_REGISTRY_V1.json"

OUT_ROOT = (TRUTH / "risk_v1" / "engine_budget").resolve()
SUBMISSIONS_ROOT = (TRUTH / "execution_evidence_v1" / "submissions").resolve()

# Deterministic engine set (institutional: explicit inventory, no discovery surprise)
ALLOWED_ENGINE_IDS = [
    "C2_MEAN_REVERSION_EQ_V1",
    "C2_TREND_EQ_PRIMARY_V1",
    "C2_VOL_INCOME_DEFINED_RISK_V1",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _canonical_dumps(obj: Any) -> bytes:
    # canonical, stable bytes with no whitespace
    s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return (s + "\n").encode("utf-8")


def _git_sha() -> str:
    try:
        out = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        return out.decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_against_repo_schema(instance: Any, schema_relpath: str) -> None:
    # Use existing Phase D validator (fail-closed, float-forbidden)
    from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1  # type: ignore

    validate_against_repo_schema_v1(instance, REPO_ROOT, schema_relpath)


def _write_immutable(path: Path, payload_obj: Dict[str, Any]) -> Tuple[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    b = _canonical_dumps(payload_obj)
    sha = _sha256_bytes(b)

    if path.exists():
        existing = path.read_bytes()
        if _sha256_bytes(existing) == sha:
            return ("EXISTS_IDENTICAL", sha)
        raise SystemExit(f"FAIL: refusing overwrite (different bytes): {path}")

    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(b)
    os.replace(tmp, path)
    return ("WRITTEN", sha)


def _pct_str(x: float) -> str:
    # conservative string format, stable
    if x <= 0.0:
        return "0"
    if x >= 1.0:
        return "1"
    # 6dp fixed max; strip trailing zeros
    s = f"{x:.6f}".rstrip("0").rstrip(".")
    if s == "":
        return "0"
    return s


def _load_engine_budgets() -> Tuple[Dict[str, float], List[Dict[str, str]], List[str]]:
    """
    Returns:
      budgets: engine_id -> budget_pct (0..1)
      input_manifest entries
      reason_codes
    """
    rc: List[str] = []
    im: List[Dict[str, str]] = []

    reg_path = (REPO_ROOT / REGISTRY_RELPATH).resolve()
    if not reg_path.exists():
        rc.append("MISSING_ENGINE_MODEL_REGISTRY")
        im.append({"type": "engine_model_registry_missing", "path": str(reg_path), "sha256": _sha256_bytes(b"")})
        return ({}, im, rc)

    reg_sha = _sha256_file(reg_path)
    im.append({"type": "engine_model_registry_v1", "path": str(reg_path), "sha256": reg_sha})

    reg = _read_json(reg_path)
    if not isinstance(reg, dict):
        rc.append("ENGINE_MODEL_REGISTRY_NOT_OBJECT")
        return ({}, im, rc)

    # Accept either top-level "engines" array or top-level map; fail-closed if unreadable.
    budgets: Dict[str, float] = {}

    engines_obj = reg.get("engines")
    if isinstance(engines_obj, list):
        for e in engines_obj:
            if not isinstance(e, dict):
                continue
            eid = str(e.get("engine_id") or "").strip()
            if eid == "":
                continue
            if eid not in ALLOWED_ENGINE_IDS:
                continue
            # Fail-closed: require an explicit budget field.
            # Supported keys (first match wins):
            #   - budget_notional_pct
            #   - risk_budget_notional_pct
            #   - max_notional_pct
            raw = e.get("budget_notional_pct")
            if raw is None:
                raw = e.get("risk_budget_notional_pct")
            if raw is None:
                raw = e.get("max_notional_pct")
            if raw is None:
                continue
            try:
                v = float(str(raw))
            except Exception:
                rc.append(f"BAD_ENGINE_BUDGET_FIELD: engine_id={eid}")
                continue
            if v < 0.0 or v > 1.0:
                rc.append(f"ENGINE_BUDGET_OUT_OF_RANGE: engine_id={eid}")
                continue
            budgets[eid] = v
    elif isinstance(engines_obj, dict):
        # map form: engines[engine_id] = {...}
        for eid in ALLOWED_ENGINE_IDS:
            e = engines_obj.get(eid)
            if not isinstance(e, dict):
                continue
            raw = e.get("budget_notional_pct")
            if raw is None:
                raw = e.get("risk_budget_notional_pct")
            if raw is None:
                raw = e.get("max_notional_pct")
            if raw is None:
                continue
            try:
                v = float(str(raw))
            except Exception:
                rc.append(f"BAD_ENGINE_BUDGET_FIELD: engine_id={eid}")
                continue
            if v < 0.0 or v > 1.0:
                rc.append(f"ENGINE_BUDGET_OUT_OF_RANGE: engine_id={eid}")
                continue
            budgets[eid] = v
    else:
        rc.append("ENGINE_MODEL_REGISTRY_MISSING_ENGINES_FIELD")

    # Hard requirement: budgets must exist for all allowed engines.
    missing = [eid for eid in ALLOWED_ENGINE_IDS if eid not in budgets]
    if missing:
        rc.append("MISSING_ENGINE_BUDGETS_FOR_ALLOWED_ENGINES:" + ",".join(missing))

    return (budgets, im, rc)


def _submissions_present(day: str) -> Tuple[bool, str]:
    day_dir = (SUBMISSIONS_ROOT / day).resolve()
    if not day_dir.exists():
        return (False, str(day_dir))
    # Any dir indicates submissions exist.
    for p in day_dir.iterdir():
        if p.is_dir():
            return (True, str(day_dir))
    return (False, str(day_dir))


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_engine_risk_budget_ledger_v1")
    ap.add_argument("--day_utc", required=True)
    args = ap.parse_args()

    day = str(args.day_utc).strip()
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {day!r}")

    produced_utc = _utc_now()

    # Inputs
    budgets, input_manifest, rc = _load_engine_budgets()

    subs_present, subs_dir = _submissions_present(day)
    input_manifest.append(
        {"type": "exec_evidence_truth_day_dir", "path": subs_dir, "sha256": _sha256_bytes(b"present") if subs_present else _sha256_bytes(b"")}
    )

    # Used notional attribution:
    # For v1, if submissions exist, engine attribution must exist in broker submission records.
    # Current broker_submission_record.v2 may not carry engine_id, so we fail closed if submissions exist.
    used: Dict[str, float] = {eid: 0.0 for eid in ALLOWED_ENGINE_IDS}
    if subs_present:
        rc.append("ENGINE_EXPOSURE_ATTRIBUTION_NOT_IMPLEMENTED_FOR_SUBMISSIONS")
        # Fail-closed: cannot claim OK if exposure exists but cannot attribute.
    # If no submissions exist: used remains 0.

    engines_out: List[Dict[str, Any]] = []
    for eid in ALLOWED_ENGINE_IDS:
        e_rc: List[str] = []
        status = "OK"

        bud = budgets.get(eid)
        if bud is None:
            bud = 0.0
            status = "UNKNOWN"
            e_rc.append("MISSING_BUDGET")

        u = float(used.get(eid, 0.0))
        rem = max(0.0, float(bud) - u)

        # If budget missing OR global attribution missing while submissions exist -> engine status UNKNOWN/THROTTLED
        if subs_present:
            status = "UNKNOWN"
            e_rc.append("SUBMISSIONS_PRESENT_BUT_ENGINE_ATTRIBUTION_MISSING")

        engines_out.append(
            {
                "engine_id": eid,
                "budget_notional_pct": _pct_str(float(bud)),
                "used_notional_pct": _pct_str(u),
                "remaining_notional_pct": _pct_str(rem),
                "rolling_drawdown_pct": None,
                "rolling_volatility": None,
                "status": status,
                "reason_codes": e_rc,
            }
        )

    status = "OK"
    reason_codes = list(rc)

    # Fail-closed conditions for the ledger:
    if rc:
        status = "FAIL"
    if subs_present:
        status = "FAIL"

    payload: Dict[str, Any] = {
        "schema_id": "engine_risk_budget_ledger",
        "schema_version": "v1",
        "day_utc": day,
        "produced_utc": produced_utc,
        "producer": {"repo": "constellation_2_runtime", "module": "ops/tools/run_engine_risk_budget_ledger_v1.py", "git_sha": _git_sha()},
        "status": status,
        "reason_codes": reason_codes,
        "input_manifest": input_manifest,
        "engines": engines_out,
        "ledger_sha256": None,
    }

    payload["ledger_sha256"] = _sha256_bytes(_canonical_dumps({**payload, "ledger_sha256": None}))

    # Governed schema validation (fail-closed)
    _validate_against_repo_schema(payload, SCHEMA_RELPATH)

    out_path = (OUT_ROOT / day / "engine_risk_budget_ledger.v1.json").resolve()
    action, sha = _write_immutable(out_path, payload)

    print(f"OK: ENGINE_RISK_BUDGET_LEDGER_WRITTEN day_utc={day} status={status} path={out_path} sha256={sha} action={action}")
    return 0 if status == "OK" else 2


if __name__ == "__main__":
    raise SystemExit(main())
