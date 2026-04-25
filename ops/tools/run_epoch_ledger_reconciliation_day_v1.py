#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.allocation_authority_v1 import read_allocation_authority_v1
from constellation_2.common.truth_root_v1 import resolve_truth_root
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


def _parse_day(day: str) -> str:
    value = str(day or "").strip()
    if len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise SystemExit(f"FAIL: BAD_DAY_UTC_FORMAT_EXPECTED_YYYY_MM_DD: {value!r}")
    return value


def _require_truth_root(raw: str) -> Path:
    if str(raw or "").strip():
        path = Path(raw).expanduser().resolve()
    else:
        path = resolve_truth_root(repo_root=REPO_ROOT).resolve()
    if not path.is_absolute() or not path.exists() or not path.is_dir():
        raise SystemExit(f"FAIL: invalid truth_root: {path}")
    return path


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise SystemExit(f"FAIL: TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_sha() -> str:
    try:
        output = subprocess.check_output(["/usr/bin/git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT))
        value = output.decode("utf-8").strip()
    except Exception:
        value = "0" * 40
    if len(value) != 40:
        value = "0" * 40
    return value


def _write_replace_if_changed(path: Path, payload: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    candidate_sha = _sha256_bytes(payload)
    if path.exists():
        existing_sha = _sha256_file(path)
        if existing_sha == candidate_sha:
            return candidate_sha
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    tmp.replace(path)
    return candidate_sha


def _load_auth_ledgers(day_utc: str, truth_root: Path) -> Dict[str, Dict[str, Any]]:
    auth_dir = (truth_root / "engine_activity_v1" / "intent_authorization_ledger_v1" / day_utc).resolve()
    if not auth_dir.exists() or not auth_dir.is_dir():
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    for path in sorted(auth_dir.glob("*.intent_authorization_ledger.v1.jsonl")):
        events: List[Dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                events.append(json.loads(line))
        if not events:
            continue
        result[str(events[-1].get("intent_hash") or "").strip()] = {
            "path": path,
            "events": events,
            "latest": events[-1],
        }
    return result


def _load_exposure_ledgers(day_utc: str, truth_root: Path) -> List[Dict[str, Any]]:
    exposure_dir = (truth_root / "exposure_v1" / "exposure_ledger_v1" / day_utc).resolve()
    if not exposure_dir.exists() or not exposure_dir.is_dir():
        return []
    records: List[Dict[str, Any]] = []
    for path in sorted(exposure_dir.glob("*.exposure_ledger.v1.json")):
        records.append({"path": path, "obj": _read_json_obj(path)})
    return records


def _load_budget_validations(day_utc: str, truth_root: Path) -> Dict[str, Dict[str, Any]]:
    validation_dir = (truth_root / "reports" / "intent_budget_validation_v1" / day_utc).resolve()
    if not validation_dir.exists() or not validation_dir.is_dir():
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    for path in sorted(validation_dir.glob("*.intent_budget_validation.v1.json")):
        obj = _read_json_obj(path)
        intent_hash = str(obj.get("intent_hash") or "").strip()
        if intent_hash:
            result[intent_hash] = {"path": path, "obj": obj}
    return result


def _positions_state(day_utc: str, truth_root: Path) -> Dict[str, Any]:
    positions_path = (truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v4.json").resolve()
    if not positions_path.exists() or not positions_path.is_file():
        return {"positions_exist": False, "position_count": 0, "path": positions_path}
    obj = _read_json_obj(positions_path)
    positions = obj.get("positions")
    items = positions.get("items") if isinstance(positions, dict) else None
    if not isinstance(items, list):
        raise SystemExit("FAIL: POSITIONS_ITEMS_INVALID")
    return {"positions_exist": bool(items), "position_count": len(items), "path": positions_path}


def _append_mismatch(mismatches: List[Dict[str, Any]], *, reason_code: str, scope: str, expected: int | str, actual: int | str) -> None:
    mismatches.append(
        {
            "reason_code": reason_code,
            "scope": scope,
            "expected": expected,
            "actual": actual,
        }
    )


def _derive_exposure_risk_cents(*, qty: int, validation_obj: Dict[str, Any]) -> int:
    requested_quantity = int(validation_obj.get("requested_quantity") or 0)
    risk_cents = int(validation_obj.get("risk_cents") or 0)
    if requested_quantity <= 0:
        raise SystemExit("FAIL: VALIDATION_REQUESTED_QUANTITY_INVALID")
    scaled = risk_cents * abs(int(qty or 0))
    if scaled % requested_quantity != 0:
        raise SystemExit("FAIL: EXPOSURE_RISK_SCALING_UNSUPPORTED")
    return scaled // requested_quantity


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_epoch_ledger_reconciliation_day_v1")
    ap.add_argument("--day", required=True, help="UTC day in YYYY-MM-DD")
    ap.add_argument("--truth_root", default="", help="Optional explicit truth root")
    args = ap.parse_args()

    day_utc = _parse_day(args.day)
    truth_root = _require_truth_root(args.truth_root)
    allocation_authority = read_allocation_authority_v1(day_utc=day_utc, truth_root=truth_root, repo_root=REPO_ROOT)
    auth_ledgers = _load_auth_ledgers(day_utc, truth_root)
    exposure_ledgers = _load_exposure_ledgers(day_utc, truth_root)
    budget_validations = _load_budget_validations(day_utc, truth_root)
    positions_state = _positions_state(day_utc, truth_root)

    per_sleeve = allocation_authority["obj"].get("per_sleeve") or []
    if not isinstance(per_sleeve, list):
        raise SystemExit("FAIL: ALLOCATION_AUTHORITY_PER_SLEEVE_INVALID")

    mismatches: List[Dict[str, Any]] = []
    active_by_sleeve: Dict[str, int] = {}
    committed_by_sleeve: Dict[str, int] = {}
    exposure_by_sleeve: Dict[str, int] = {}

    if not auth_ledgers:
        _append_mismatch(
            mismatches,
            reason_code="AUTHORIZATION_LEDGER_MISSING",
            scope=day_utc,
            expected=">=1 ledger file",
            actual=0,
        )
    if not exposure_ledgers and positions_state["positions_exist"]:
        _append_mismatch(
            mismatches,
            reason_code="EXPOSURE_LEDGER_MISSING",
            scope=day_utc,
            expected=">=1 exposure file",
            actual=0,
        )

    for entry in auth_ledgers.values():
        latest = entry["latest"]
        intent_hash = str(latest.get("intent_hash") or "").strip()
        validation_entry = budget_validations.get(intent_hash)
        if validation_entry is None:
            _append_mismatch(
                mismatches,
                reason_code="BUDGET_VALIDATION_MISSING",
                scope=intent_hash,
                expected="validation file",
                actual="missing",
            )
            continue
        validation_obj = validation_entry["obj"]
        sleeve_id = str(latest.get("canonical_sleeve_id") or "").strip()
        authorized_qty = int(latest.get("authorized_quantity") or 0)
        reserved_qty = int(latest.get("reserved_quantity") or 0)
        committed_qty = int(latest.get("committed_quantity") or 0)
        released_qty = int(latest.get("released_quantity") or 0)
        remaining_reserved_qty = int(latest.get("remaining_reserved_quantity") or 0)
        requested_quantity = int(validation_obj.get("requested_quantity") or 0)
        risk_cents = int(validation_obj.get("risk_cents") or 0)
        if requested_quantity <= 0:
            raise SystemExit("FAIL: VALIDATION_REQUESTED_QUANTITY_INVALID")
        active_risk = (risk_cents * (committed_qty + remaining_reserved_qty)) // requested_quantity
        committed_risk = (risk_cents * committed_qty) // requested_quantity
        active_by_sleeve[sleeve_id] = active_by_sleeve.get(sleeve_id, 0) + active_risk
        committed_by_sleeve[sleeve_id] = committed_by_sleeve.get(sleeve_id, 0) + committed_risk
        if committed_qty > reserved_qty:
            _append_mismatch(
                mismatches,
                reason_code="LIFECYCLE_INCONSISTENCY",
                scope=intent_hash,
                expected=f"committed_quantity<={reserved_qty}",
                actual=committed_qty,
            )
        if released_qty + committed_qty > authorized_qty:
            _append_mismatch(
                mismatches,
                reason_code="LIFECYCLE_INCONSISTENCY",
                scope=intent_hash,
                expected=f"released_plus_committed<={authorized_qty}",
                actual=released_qty + committed_qty,
            )
        if bool(validation_obj.get("total_risk_exceeds_budget") is True):
            _append_mismatch(
                mismatches,
                reason_code="TOTAL_RISK_EXCEEDS_BUDGET",
                scope=sleeve_id,
                expected=int(validation_obj.get("allocation_budget_cents") or 0),
                actual=int(validation_obj.get("total_risk_cents") or 0),
            )

    for entry in exposure_ledgers:
        obj = entry["obj"]
        sleeve_id = str(obj.get("canonical_sleeve_id") or "").strip()
        intent_hash = str(obj.get("intent_hash") or "").strip()
        validation_entry = budget_validations.get(intent_hash)
        if validation_entry is None:
            _append_mismatch(
                mismatches,
                reason_code="BUDGET_VALIDATION_MISSING",
                scope=intent_hash,
                expected="validation file",
                actual="missing",
            )
            continue
        exposure_by_sleeve[sleeve_id] = exposure_by_sleeve.get(sleeve_id, 0) + _derive_exposure_risk_cents(
            qty=int(obj.get("qty") or 0),
            validation_obj=validation_entry["obj"],
        )

    portfolio_allowed = int(allocation_authority["obj"].get("portfolio_allowed_capital_at_risk_cents") or 0)
    assigned_total = 0
    for item in per_sleeve:
        if not isinstance(item, dict):
            raise SystemExit("FAIL: ALLOCATION_AUTHORITY_SLEEVE_NOT_OBJECT")
        sleeve_id = str(item.get("canonical_sleeve_id") or "").strip()
        assigned = int(item.get("assigned_budget_cents") or 0)
        assigned_total += assigned
        active_reserved = active_by_sleeve.get(sleeve_id, 0)
        if active_reserved > assigned:
            _append_mismatch(
                mismatches,
                reason_code="RESERVED_EXCEEDS_ALLOCATION",
                scope=sleeve_id,
                expected=assigned,
                actual=active_reserved,
            )
        exposure_qty = exposure_by_sleeve.get(sleeve_id, 0)
        committed_only = committed_by_sleeve.get(sleeve_id, 0)
        if exposure_qty > committed_only:
            _append_mismatch(
                mismatches,
                reason_code="LIFECYCLE_INCONSISTENCY",
                scope=sleeve_id,
                expected=committed_only,
                actual=exposure_qty,
            )

    if assigned_total > portfolio_allowed:
        _append_mismatch(
            mismatches,
            reason_code="ALLOCATION_EXCEEDS_PORTFOLIO_BUDGET",
            scope=day_utc,
            expected=portfolio_allowed,
            actual=assigned_total,
        )

    reason_codes = sorted({str(item["reason_code"]) for item in mismatches})
    if not positions_state["positions_exist"] and not exposure_ledgers:
        reason_codes.append("NO_EXPOSURE")
    hard_violation_codes = {
        "LIFECYCLE_INCONSISTENCY",
        "RESERVED_EXCEEDS_ALLOCATION",
        "ALLOCATION_EXCEEDS_PORTFOLIO_BUDGET",
        "TOTAL_RISK_EXCEEDS_BUDGET",
    }
    if any(code in hard_violation_codes for code in reason_codes):
        reason_codes.append("HARD_VIOLATION")
    status = "PASS"
    enforcement_recommendation = "ALLOW"
    if any(code in hard_violation_codes for code in reason_codes):
        status = "HARD_VIOLATION"
        enforcement_recommendation = "BLOCK"
    elif any(code in {"AUTHORIZATION_LEDGER_MISSING", "BUDGET_VALIDATION_MISSING", "EXPOSURE_LEDGER_MISSING"} for code in reason_codes):
        status = "FAIL"
    out_obj = {
        "schema_id": "C2_EPOCH_LEDGER_RECONCILIATION_V1_UNGOVERNED",
        "schema_version": 1,
        "day_utc": day_utc,
        "produced_utc": f"{day_utc}T00:00:00Z",
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "allocation_authority_source": str(allocation_authority["authority_source"]),
        "allocation_authority_ref": {
            "path": str(allocation_authority["path"]),
            "epoch_id": str(allocation_authority["epoch_id"]),
            "sha256": _sha256_file(Path(allocation_authority["path"])),
        },
        "authorization_ledger_count": len(auth_ledgers),
        "budget_validation_count": len(budget_validations),
        "exposure_ledger_count": len(exposure_ledgers),
        "positions_exist": bool(positions_state["positions_exist"]),
        "position_count": int(positions_state["position_count"]),
        "enforcement_recommendation": enforcement_recommendation,
        "mismatches": mismatches,
    }
    out_path = (
        truth_root / "reports" / "epoch_ledger_reconciliation_v1" / day_utc / "epoch_ledger_reconciliation.v1.json"
    ).resolve()
    payload = canonical_json_bytes_v1(out_obj) + b"\n"
    out_sha = _write_replace_if_changed(out_path, payload)
    print(
        "OK: EPOCH_LEDGER_RECONCILIATION_WRITTEN "
        f"day_utc={day_utc} path={out_path} sha256={out_sha} git_sha={_git_sha()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
