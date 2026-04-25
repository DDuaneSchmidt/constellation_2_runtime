from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict


VALID_ENFORCEMENT_MODES_V1 = {"SHADOW", "SOFT", "HARD"}


def _read_json_obj(path: Path) -> Dict[str, Any]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT: {path}")
    return obj


def _env_bool(name: str) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def configured_enforcement_mode_v1() -> str:
    raw = str(os.environ.get("ENFORCEMENT_MODE", "SHADOW")).strip().upper() or "SHADOW"
    if raw not in VALID_ENFORCEMENT_MODES_V1:
        raise ValueError(f"ENFORCEMENT_MODE_INVALID: {raw}")
    return raw


def enforce_budget_enabled_v1() -> bool:
    return _env_bool("ENFORCE_BUDGET")


def effective_enforcement_mode_v1() -> str:
    configured = configured_enforcement_mode_v1()
    if configured == "SHADOW":
        return "SHADOW"
    if not enforce_budget_enabled_v1():
        return "SHADOW"
    return configured


def load_budget_validation_index_v1(*, day_utc: str, truth_root: Path) -> Dict[str, Dict[str, Any]]:
    validation_dir = (Path(truth_root).resolve() / "reports" / "intent_budget_validation_v1" / str(day_utc).strip()).resolve()
    if not validation_dir.exists() or not validation_dir.is_dir():
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    for path in sorted(validation_dir.glob("*.intent_budget_validation.v1.json")):
        obj = _read_json_obj(path)
        intent_hash = str(obj.get("intent_hash") or "").strip()
        if intent_hash:
            result[intent_hash] = {"path": path, "obj": obj}
    return result


def load_authorization_ledger_index_v1(*, day_utc: str, truth_root: Path) -> Dict[str, Dict[str, Any]]:
    ledger_dir = (Path(truth_root).resolve() / "engine_activity_v1" / "intent_authorization_ledger_v1" / str(day_utc).strip()).resolve()
    if not ledger_dir.exists() or not ledger_dir.is_dir():
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    for path in sorted(ledger_dir.glob("*.intent_authorization_ledger.v1.jsonl")):
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if not lines:
            continue
        latest = json.loads(lines[-1])
        if not isinstance(latest, dict):
            raise ValueError(f"LEDGER_LATEST_NOT_OBJECT: {path}")
        intent_hash = str(latest.get("intent_hash") or "").strip()
        if intent_hash:
            result[intent_hash] = {"path": path, "latest": latest}
    return result


def check_budget_enforcement_day_v1(*, day_utc: str, truth_root: Path) -> Dict[str, Any]:
    configured_mode = configured_enforcement_mode_v1()
    effective_mode = effective_enforcement_mode_v1()
    validations = load_budget_validation_index_v1(day_utc=day_utc, truth_root=truth_root)
    ledgers = load_authorization_ledger_index_v1(day_utc=day_utc, truth_root=truth_root)
    validation_dir = (Path(truth_root).resolve() / "reports" / "intent_budget_validation_v1" / str(day_utc).strip()).resolve()
    if not validations and not ledgers:
        if effective_mode == "HARD":
            return {
                "configured_mode": configured_mode,
                "effective_mode": effective_mode,
                "blocking": True,
                "status": "MISSING",
                "reason_codes": ["BUDGET_ENFORCEMENT_VALIDATIONS_MISSING"],
                "path": validation_dir,
                "violation_count": 0,
            }
        return {
            "configured_mode": configured_mode,
            "effective_mode": effective_mode,
            "blocking": False,
            "status": "SKIPPED",
            "reason_codes": ["BUDGET_ENFORCEMENT_NOT_ACTIVE"],
            "path": validation_dir,
            "violation_count": 0,
        }
    violations = []
    if ledgers:
        for intent_hash, entry in ledgers.items():
            latest = entry["latest"]
            latest_state = str(latest.get("state") or "").strip().upper()
            validation_entry = validations.get(intent_hash)
            if latest_state == "REJECTED":
                violations.append(
                    {
                        "path": entry["path"],
                        "latest": latest,
                        "validation": None if validation_entry is None else validation_entry["obj"],
                    }
                )
    else:
        for entry in validations.values():
            obj = entry["obj"]
            enforcement_decision = str(obj.get("enforcement_decision") or "").strip().upper()
            total_risk_exceeds_budget = bool(obj.get("total_risk_exceeds_budget") is True)
            if enforcement_decision == "REJECT" or total_risk_exceeds_budget:
                violations.append(entry)
    reason_codes = []
    if violations:
        reason_codes.append("BUDGET_ENFORCEMENT_VIOLATION_PRESENT")
    if any(
        bool((entry.get("validation") or {}).get("total_risk_exceeds_budget") is True)
        if isinstance(entry, dict) and "validation" in entry
        else bool(entry["obj"].get("total_risk_exceeds_budget") is True)
        for entry in violations
    ):
        reason_codes.append("TOTAL_RISK_EXCEEDS_BUDGET")
    if any(
        str(entry.get("latest", {}).get("state") or "").strip().upper() == "REJECTED"
        if isinstance(entry, dict) and "latest" in entry
        else str(entry["obj"].get("enforcement_decision") or "").strip().upper() == "REJECT"
        for entry in violations
    ):
        reason_codes.append("ENFORCEMENT_DECISION_REJECT")
    blocking = effective_mode == "HARD" and bool(violations)
    status = "PASS"
    if blocking:
        status = "FAIL"
    elif effective_mode == "SHADOW":
        status = "PASS"
    elif effective_mode == "SOFT":
        status = "PASS"
    return {
        "configured_mode": configured_mode,
        "effective_mode": effective_mode,
        "blocking": blocking,
        "status": status,
        "reason_codes": reason_codes or ["BUDGET_ENFORCEMENT_CLEAR"],
        "path": next(iter(ledgers.values()))["path"] if ledgers else next(iter(validations.values()))["path"],
        "violation_count": len(violations),
    }
