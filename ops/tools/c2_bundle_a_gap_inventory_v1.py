#!/usr/bin/env python3
"""
c2_bundle_a_gap_inventory_v1.py

PHASE 1 — Structural Gap Inventory (Deterministic)

Guarantees:
- READ-ONLY: never writes or mutates anything.
- Reads ONLY canonical truth paths beneath truth_root.
- Deterministic output ordering; no timestamps; stable JSON.
- Hashable output: embeds output_sha256 computed over canonical JSON WITHOUT the hash field.
- Fail-closed: non-zero exit if any REQUIRED artifact is missing or not in an acceptable state.

This tool does NOT assume exact filenames for day artifacts.
Instead, for each artifact family it searches the canonical per-day directory for JSON files and:
- 0 files => MISSING
- 1+ files => PRESENT (and it will evaluate each, deterministically sorted)

Status evaluation:
- Generic: prefers top-level keys in order: status, verdict.status, result, pass/fail, ready
- Capital envelope: PASS is required (if field present)
- Operator gate: requires ready==true if present, else status==OK if present
- Reconciliation: requires PASS if present, else status==OK if present
- Pipeline manifest: requires status==OK if present
- Engine risk budget ledger: requires status==OK if present
- Regime snapshot: requires blocking==false if present, else status==OK if present

NOTE:
- pipeline_manifest_v2 and operator_gate_verdict_v2 are not present in your proof; this script
  inventories both v1 (present) and v2 (target) surfaces as separate requirements.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


REASON = {
    "MISSING_DIR": "MISSING_DIR",
    "MISSING_FILES": "MISSING_FILES",
    "MULTIPLE_FILES": "MULTIPLE_FILES",
    "JSON_PARSE_ERROR": "JSON_PARSE_ERROR",
    "STATUS_NOT_OK": "STATUS_NOT_OK",
    "VERDICT_NOT_READY": "VERDICT_NOT_READY",
    "ENVELOPE_NOT_PASS": "ENVELOPE_NOT_PASS",
    "REGIME_BLOCKING_TRUE": "REGIME_BLOCKING_TRUE",
    "UNKNOWN_STATUS": "UNKNOWN_STATUS",
}


def _read_truth_root_from_manifest(manifest_path: Path) -> Path:
    if not manifest_path.exists():
        raise SystemExit(f"FATAL: governance manifest missing: {manifest_path}")
    # Minimal, deterministic extraction of a single scalar: truth_root:
    truth_root: Optional[str] = None
    for raw in manifest_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("truth_root:"):
            # allow "truth_root: /path"
            truth_root = line.split("truth_root:", 1)[1].strip()
            break
    if not truth_root:
        raise SystemExit(f"FATAL: truth_root not found in manifest: {manifest_path}")
    return Path(truth_root)


def _list_json_files(day_dir: Path) -> List[Path]:
    if not day_dir.exists():
        return []
    files = [p for p in day_dir.iterdir() if p.is_file() and p.name.endswith(".json")]
    files.sort(key=lambda p: p.name)
    return files


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_json(path: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None, "JSON_NOT_OBJECT"
        return data, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def _get_nested(d: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


def _normalize_bool(v: Any) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "yes", "1"):
            return True
        if s in ("false", "no", "0"):
            return False
    if isinstance(v, (int, float)):
        if v == 1:
            return True
        if v == 0:
            return False
    return None


def _eval_generic_status(doc: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    """
    Returns (state, reason_code_or_None)
    state in: OK, FAIL, UNKNOWN
    """
    # 1) direct status
    status = doc.get("status")
    if isinstance(status, str):
        s = status.strip().upper()
        if s in ("OK", "PASS", "READY"):
            return "OK", None
        if s in ("FAIL", "ERROR", "BLOCKED"):
            return "FAIL", REASON["STATUS_NOT_OK"]
        return "UNKNOWN", REASON["UNKNOWN_STATUS"]

    # 2) verdict.status
    vstatus = _get_nested(doc, ["verdict", "status"])
    if isinstance(vstatus, str):
        s = vstatus.strip().upper()
        if s in ("OK", "PASS", "READY"):
            return "OK", None
        if s in ("FAIL", "ERROR", "BLOCKED"):
            return "FAIL", REASON["STATUS_NOT_OK"]
        return "UNKNOWN", REASON["UNKNOWN_STATUS"]

    # 3) result
    result = doc.get("result")
    if isinstance(result, str):
        s = result.strip().upper()
        if s in ("OK", "PASS", "READY"):
            return "OK", None
        if s in ("FAIL", "ERROR", "BLOCKED"):
            return "FAIL", REASON["STATUS_NOT_OK"]
        return "UNKNOWN", REASON["UNKNOWN_STATUS"]

    # 4) pass/fail boolean-ish
    p = doc.get("pass")
    pb = _normalize_bool(p)
    if pb is True:
        return "OK", None
    if pb is False:
        return "FAIL", REASON["STATUS_NOT_OK"]

    # 5) ready boolean-ish
    r = doc.get("ready")
    rb = _normalize_bool(r)
    if rb is True:
        return "OK", None
    if rb is False:
        return "FAIL", REASON["VERDICT_NOT_READY"]

    return "UNKNOWN", REASON["UNKNOWN_STATUS"]


def _eval_pipeline_manifest(doc: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    status = doc.get("status")
    if isinstance(status, str):
        if status.strip().upper() == "OK":
            return "OK", None
        return "FAIL", REASON["STATUS_NOT_OK"]
    # fall back
    return _eval_generic_status(doc)


def _eval_operator_gate(doc: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    # Prefer explicit ready boolean if present
    rb = _normalize_bool(doc.get("ready"))
    if rb is True:
        return "OK", None
    if rb is False:
        return "FAIL", REASON["VERDICT_NOT_READY"]
    # fall back to status
    return _eval_generic_status(doc)


def _eval_reconciliation(doc: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    # Prefer PASS/FAIL
    status = doc.get("status")
    if isinstance(status, str):
        s = status.strip().upper()
        if s == "PASS":
            return "OK", None
        if s == "FAIL":
            return "FAIL", REASON["STATUS_NOT_OK"]
    # fall back
    return _eval_generic_status(doc)


def _eval_capital_envelope(doc: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    # Prefer explicit PASS field(s)
    status = doc.get("status")
    if isinstance(status, str):
        s = status.strip().upper()
        if s == "PASS":
            return "OK", None
        if s in ("FAIL", "ERROR", "BLOCKED"):
            return "FAIL", REASON["ENVELOPE_NOT_PASS"]
    # fall back
    st, reason = _eval_generic_status(doc)
    if st == "OK":
        # OK is insufficient; must be PASS if status exists but isn't PASS
        return "FAIL", REASON["ENVELOPE_NOT_PASS"]
    return st, reason


def _eval_engine_risk_budget(doc: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    status = doc.get("status")
    if isinstance(status, str) and status.strip().upper() == "OK":
        return "OK", None
    if isinstance(status, str):
        return "FAIL", REASON["STATUS_NOT_OK"]
    return _eval_generic_status(doc)


def _eval_regime_snapshot(doc: Dict[str, Any]) -> Tuple[str, Optional[str]]:
    b = doc.get("blocking")
    bb = _normalize_bool(b)
    if bb is True:
        return "FAIL", REASON["REGIME_BLOCKING_TRUE"]
    if bb is False:
        return "OK", None
    return _eval_generic_status(doc)


EVALUATORS = {
    "pipeline_manifest": _eval_pipeline_manifest,
    "operator_gate": _eval_operator_gate,
    "reconciliation": _eval_reconciliation,
    "capital_risk_envelope": _eval_capital_envelope,
    "engine_risk_budget_ledger": _eval_engine_risk_budget,
    "regime_snapshot": _eval_regime_snapshot,
    "generic": _eval_generic_status,
}


def _daterange(start_day: dt.date, end_day: dt.date) -> List[str]:
    if end_day < start_day:
        raise SystemExit("FATAL: end_day_utc < start_day_utc")
    days: List[str] = []
    cur = start_day
    while cur <= end_day:
        days.append(cur.isoformat())
        cur += dt.timedelta(days=1)
    return days


def _artifact_spec(truth_root: Path) -> List[Dict[str, Any]]:
    """
    The REQUIRED inventory surface for Phase 1. Derived from your stated closure targets,
    and anchored to proven canonical directories.

    IMPORTANT: we do not assume filenames; we check day directories for JSON files.
    """
    return [
        # Bundle A / readiness surfaces
        {
            "id": "intents_day_rollup_v1",
            "kind": "generic",
            "required": True,
            "day_dir": truth_root / "intents_v1" / "day_rollup",
        },
        {
            "id": "reconciliation_report_v2",
            "kind": "reconciliation",
            "required": True,
            "day_dir": truth_root / "reports" / "reconciliation_report_v2",
        },
        # operator_daily_gate_v1 target (not present in proof); inventory separately.
        {
            "id": "operator_daily_gate_v1",
            "kind": "operator_gate",
            "required": True,
            "day_dir": truth_root / "reports" / "operator_daily_gate_v1",
        },
        # existing operator gate verdict v1 (present) — still required for current readiness
        {
            "id": "operator_gate_verdict_v1",
            "kind": "operator_gate",
            "required": True,
            "day_dir": truth_root / "reports" / "operator_gate_verdict_v1",
        },
        # v2 targets (missing in proof)
        {
            "id": "pipeline_manifest_v2",
            "kind": "pipeline_manifest",
            "required": True,
            "day_dir": truth_root / "reports" / "pipeline_manifest_v2",
        },
        {
            "id": "operator_gate_verdict_v2",
            "kind": "operator_gate",
            "required": True,
            "day_dir": truth_root / "reports" / "operator_gate_verdict_v2",
        },
        # Bundled C control plane targets
        {
            "id": "global_kill_switch_state_v1",
            "kind": "generic",
            "required": True,
            "day_dir": truth_root / "risk_v1" / "kill_switch_v1",
        },
        {
            "id": "position_lifecycle_ledger_v1",
            "kind": "generic",
            "required": True,
            "day_dir": truth_root / "position_lifecycle_v1" / "ledger",
        },
        {
            "id": "exposure_reconciliation_report_v1",
            "kind": "generic",
            "required": True,
            "day_dir": truth_root / "reports" / "exposure_reconciliation_report_v1",
        },
        {
            "id": "delta_order_plan_v1",
            "kind": "generic",
            "required": True,
            "day_dir": truth_root / "reports" / "delta_order_plan_v1",
        },
        # Risk posture surfaces
        {
            "id": "engine_risk_budget_ledger_v1",
            "kind": "engine_risk_budget_ledger",
            "required": True,
            "day_dir": truth_root / "risk_v1" / "engine_budget",
        },
        {
            "id": "capital_risk_envelope_v1",
            "kind": "capital_risk_envelope",
            "required": True,
            "day_dir": truth_root / "reports" / "capital_risk_envelope_v1",
        },
        {
            "id": "regime_snapshot_v2",
            "kind": "regime_snapshot",
            "required": True,
            "day_dir": truth_root / "monitoring_v1" / "regime_snapshot_v2",
        },
    ]


def _inventory_one(day: str, spec: Dict[str, Any]) -> Dict[str, Any]:
    day_dir_base: Path = spec["day_dir"]
    day_dir = day_dir_base / day

    item: Dict[str, Any] = {
        "artifact_id": spec["id"],
        "required": bool(spec["required"]),
        "day": day,
        "day_dir": str(day_dir),
        "present": False,
        "status": "MISSING",
        "blocking_reason_codes": [],
        "files": [],
    }

    if not day_dir_base.exists():
        item["blocking_reason_codes"].append(REASON["MISSING_DIR"])
        return item

    files = _list_json_files(day_dir)
    if len(files) == 0:
        item["blocking_reason_codes"].append(REASON["MISSING_FILES"])
        return item

    item["present"] = True

    kind = spec.get("kind", "generic")
    evaluator = EVALUATORS.get(kind, _eval_generic_status)

    file_entries: List[Dict[str, Any]] = []
    worst_state = "OK"  # OK > UNKNOWN > FAIL
    worst_reason: Optional[str] = None

    for p in files:
        sha = _sha256_file(p)
        doc, err = _load_json(p)
        entry: Dict[str, Any] = {
            "name": p.name,
            "path": str(p),
            "sha256": sha,
            "parse_error": err,
            "evaluated_state": None,
            "evaluated_reason": None,
        }
        if doc is None:
            entry["evaluated_state"] = "FAIL"
            entry["evaluated_reason"] = REASON["JSON_PARSE_ERROR"]
            state = "FAIL"
            reason = REASON["JSON_PARSE_ERROR"]
        else:
            state, reason = evaluator(doc)
            entry["evaluated_state"] = state
            entry["evaluated_reason"] = reason
        file_entries.append(entry)

        if state == "FAIL":
            worst_state = "FAIL"
            worst_reason = reason or REASON["STATUS_NOT_OK"]
        elif state == "UNKNOWN" and worst_state != "FAIL":
            worst_state = "UNKNOWN"
            worst_reason = reason or REASON["UNKNOWN_STATUS"]

    # deterministic order already ensured by filename sort
    item["files"] = file_entries

    if worst_state == "OK":
        item["status"] = "OK"
    elif worst_state == "UNKNOWN":
        item["status"] = "FAIL"
        item["blocking_reason_codes"].append(worst_reason or REASON["UNKNOWN_STATUS"])
    else:
        item["status"] = "FAIL"
        item["blocking_reason_codes"].append(worst_reason or REASON["STATUS_NOT_OK"])

    # If multiple files exist, flag it (still evaluated deterministically, but audit wants clarity)
    if len(files) > 1:
        item["blocking_reason_codes"].append(REASON["MULTIPLE_FILES"])

    # de-dup reasons deterministically
    item["blocking_reason_codes"] = sorted(set(item["blocking_reason_codes"]))
    return item


def _canonical_json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day_utc", help="YYYY-MM-DD (single day)")
    ap.add_argument("--start_day_utc", help="YYYY-MM-DD (start, inclusive)")
    ap.add_argument("--end_day_utc", help="YYYY-MM-DD (end, inclusive)")
    ap.add_argument("--governance_manifest", default="governance/00_MANIFEST.yaml")
    ap.add_argument("--truth_root", default=None, help="Override truth root (must be canonical); otherwise read from governance manifest")
    args = ap.parse_args()

    if args.day_utc:
        days = [args.day_utc]
    else:
        if not args.start_day_utc or not args.end_day_utc:
            raise SystemExit("FATAL: provide either --day_utc or (--start_day_utc and --end_day_utc)")
        s = dt.date.fromisoformat(args.start_day_utc)
        e = dt.date.fromisoformat(args.end_day_utc)
        days = _daterange(s, e)

    repo_root = Path(os.getcwd())

    if args.truth_root is None:
        truth_root = _read_truth_root_from_manifest(repo_root / args.governance_manifest)
    else:
        truth_root = Path(args.truth_root)

    if not truth_root.exists():
        raise SystemExit(f"FATAL: truth_root does not exist: {truth_root}")

    # Hard guard: truth_root must be inside the repo root to avoid accidental non-canonical reads.
    try:
        truth_root.relative_to(repo_root)
    except Exception:
        raise SystemExit(f"FATAL: truth_root is not under repo root (non-canonical): truth_root={truth_root} repo_root={repo_root}")

    specs = _artifact_spec(truth_root)

    out: Dict[str, Any] = {
        "schema_version": "c2_bundle_a_gap_inventory.v1",
        "repo_root": str(repo_root),
        "truth_root": str(truth_root),
        "days": days,
        "required_artifacts": [s["id"] for s in specs if s["required"]],
        "inventory": [],
        "summary": {
            "required_total": 0,
            "required_ok": 0,
            "required_fail_or_missing": 0,
            "blocking": [],
        },
        "output_sha256": None,
        "exit_code": None,
    }

    blocking: List[str] = []
    required_total = 0
    required_ok = 0
    required_fail = 0

    inv: List[Dict[str, Any]] = []
    for day in days:
        for spec in specs:
            item = _inventory_one(day, spec)
            inv.append(item)
            if spec["required"]:
                required_total += 1
                if item["status"] == "OK":
                    required_ok += 1
                else:
                    required_fail += 1
                    # include reason codes in summary blocking
                    for rc in item["blocking_reason_codes"]:
                        blocking.append(f"{spec['id']}@{day}:{rc}")

    blocking_sorted = sorted(set(blocking))

    out["inventory"] = inv
    out["summary"]["required_total"] = required_total
    out["summary"]["required_ok"] = required_ok
    out["summary"]["required_fail_or_missing"] = required_fail
    out["summary"]["blocking"] = blocking_sorted

    # Compute hash over canonical JSON WITHOUT output_sha256 and exit_code fields
    unsigned = dict(out)
    unsigned["output_sha256"] = None
    unsigned["exit_code"] = None
    digest = hashlib.sha256(_canonical_json_bytes(unsigned)).hexdigest()

    # Fail-closed: any required_fail => exit 2
    exit_code = 0 if required_fail == 0 else 2

    out["output_sha256"] = digest
    out["exit_code"] = exit_code

    # Emit final JSON only (hashable). No extra lines.
    sys_bytes = _canonical_json_bytes(out)
    print(sys_bytes.decode("utf-8"))

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
