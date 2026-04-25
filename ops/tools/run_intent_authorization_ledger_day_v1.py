#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.allocation_authority_v1 import read_allocation_authority_v1
from constellation_2.common.budget_enforcement_v1 import (
    configured_enforcement_mode_v1,
    effective_enforcement_mode_v1,
    enforce_budget_enabled_v1,
    load_budget_validation_index_v1,
)
from constellation_2.common.canonical_sleeve_identity_v1 import canonical_sleeve_id_from_engine_id
from constellation_2.common.intent_risk_cents_v1 import load_authorization_risk_context_v1
from constellation_2.common.truth_root_v1 import resolve_truth_root
from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/intent_authorization_ledger.v1.schema.json"
AUTH_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/ENGINE_ACTIVITY/authorization.v1.schema.json"
FILL_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/fill_ledger.v1.schema.json"


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


def _write_append_only_jsonl(path: Path, payload: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    candidate_sha = _sha256_bytes(payload)
    if path.exists():
        if not path.is_file():
            raise SystemExit(f"FAIL: TARGET_NOT_FILE: {path}")
        existing = path.read_bytes()
        if _sha256_bytes(existing) == candidate_sha:
            return candidate_sha
        if not payload.startswith(existing):
            raise SystemExit(f"FAIL: LEDGER_APPEND_ONLY_VIOLATION: {path}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    tmp.write_bytes(payload)
    tmp.replace(path)
    return candidate_sha


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


def _remaining_budget_cents(authority_obj: Dict[str, Any], canonical_sleeve_id: str) -> int:
    per_sleeve = authority_obj.get("per_sleeve")
    if not isinstance(per_sleeve, list):
        raise SystemExit("FAIL: ALLOCATION_AUTHORITY_PER_SLEEVE_INVALID")
    for item in per_sleeve:
        if not isinstance(item, dict):
            raise SystemExit("FAIL: ALLOCATION_AUTHORITY_SLEEVE_NOT_OBJECT")
        if str(item.get("canonical_sleeve_id") or "").strip() == canonical_sleeve_id:
            return int(item.get("remaining_budget_cents") or 0)
    raise SystemExit(f"FAIL: SLEEVE_NOT_PRESENT_IN_ALLOCATION_AUTHORITY: {canonical_sleeve_id}")


def _assigned_budget_cents(authority_obj: Dict[str, Any], canonical_sleeve_id: str) -> int:
    per_sleeve = authority_obj.get("per_sleeve")
    if not isinstance(per_sleeve, list):
        raise SystemExit("FAIL: ALLOCATION_AUTHORITY_PER_SLEEVE_INVALID")
    for item in per_sleeve:
        if not isinstance(item, dict):
            raise SystemExit("FAIL: ALLOCATION_AUTHORITY_SLEEVE_NOT_OBJECT")
        if str(item.get("canonical_sleeve_id") or "").strip() == canonical_sleeve_id:
            return int(item.get("assigned_budget_cents") or 0)
    raise SystemExit(f"FAIL: SLEEVE_NOT_PRESENT_IN_ALLOCATION_AUTHORITY: {canonical_sleeve_id}")


def _positions_exist(day_utc: str, truth_root: Path) -> bool:
    positions_path = (truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v4.json").resolve()
    if not positions_path.exists() or not positions_path.is_file():
        return False
    obj = _read_json_obj(positions_path)
    positions = obj.get("positions")
    items = positions.get("items") if isinstance(positions, dict) else None
    if not isinstance(items, list):
        raise SystemExit("FAIL: POSITIONS_ITEMS_INVALID")
    return bool(items)


def _scaled_risk_cents(*, qty: int, requested_quantity: int, risk_cents: int) -> int:
    if requested_quantity <= 0:
        raise SystemExit("FAIL: VALIDATION_REQUESTED_QUANTITY_INVALID")
    scaled = int(risk_cents) * abs(int(qty or 0))
    if scaled % int(requested_quantity) != 0:
        raise SystemExit("FAIL: EXPOSURE_RISK_SCALING_UNSUPPORTED")
    return scaled // int(requested_quantity)


def _load_existing_events(path: Path) -> List[Dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    events: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        if not isinstance(obj, dict):
            raise SystemExit(f"FAIL: AUTH_LEDGER_LINE_NOT_OBJECT: {path}")
        validate_against_repo_schema_v1(obj, REPO_ROOT, SCHEMA_RELPATH)
        events.append(obj)
    return events


def _load_fill_aggregates(day_utc: str, truth_root: Path) -> Dict[str, Dict[str, Any]]:
    fill_dir = (truth_root / "fill_ledger_v1" / day_utc).resolve()
    if not fill_dir.exists() or not fill_dir.is_dir():
        return {}
    aggregates: Dict[str, Dict[str, Any]] = {}
    for path in sorted(fill_dir.glob("*.fill_ledger.v1.json")):
        obj = _read_json_obj(path)
        validate_against_repo_schema_v1(obj, REPO_ROOT, FILL_SCHEMA_RELPATH)
        intent_hash = str(obj.get("intent_sha256") or "").strip()
        if not intent_hash:
            raise SystemExit(f"FAIL: FILL_LEDGER_INTENT_HASH_MISSING: {path}")
        entry = aggregates.setdefault(
            intent_hash,
            {
                "paths": [],
                "filled_qty": 0,
                "remaining_qty": 0,
                "statuses": [],
                "produced_utc": "",
                "source_intent_id": str(obj.get("source_intent_id") or "").strip(),
                "engine_id": str(obj.get("engine_id") or "").strip(),
            },
        )
        if entry["source_intent_id"] != str(obj.get("source_intent_id") or "").strip():
            raise SystemExit(f"FAIL: FILL_LEDGER_SOURCE_INTENT_CONFLICT: {intent_hash}")
        if entry["engine_id"] != str(obj.get("engine_id") or "").strip():
            raise SystemExit(f"FAIL: FILL_LEDGER_ENGINE_CONFLICT: {intent_hash}")
        entry["paths"].append(str(path))
        entry["filled_qty"] += int(obj.get("filled_qty") or 0)
        entry["remaining_qty"] += int(obj.get("remaining_qty") or 0)
        entry["statuses"].append(str(obj.get("lifecycle_status") or "").strip().upper())
        produced_utc = str(obj.get("produced_utc") or "").strip()
        if produced_utc and produced_utc >= str(entry["produced_utc"]):
            entry["produced_utc"] = produced_utc
    return aggregates


def _current_active_spend_cents(
    *,
    day_utc: str,
    truth_root: Path,
    canonical_sleeve_id: str,
    exclude_intent_hash: str,
) -> int:
    ledger_dir = (truth_root / "engine_activity_v1" / "intent_authorization_ledger_v1" / day_utc).resolve()
    if not ledger_dir.exists() or not ledger_dir.is_dir():
        return 0
    validations = load_budget_validation_index_v1(day_utc=day_utc, truth_root=truth_root)
    total = 0
    for path in sorted(ledger_dir.glob("*.intent_authorization_ledger.v1.jsonl")):
        events = _load_existing_events(path)
        if not events:
            continue
        latest = events[-1]
        intent_hash = str(latest.get("intent_hash") or "").strip()
        if not intent_hash or intent_hash == exclude_intent_hash:
            continue
        if str(latest.get("canonical_sleeve_id") or "").strip() != canonical_sleeve_id:
            continue
        validation_entry = validations.get(intent_hash)
        if validation_entry is None:
            raise SystemExit(f"FAIL: VALIDATION_REQUIRED_FOR_EXISTING_LEDGER: {intent_hash}")
        validation_obj = validation_entry["obj"]
        requested_quantity = int(validation_obj.get("requested_quantity") or 0)
        risk_cents = int(validation_obj.get("risk_cents") or 0)
        active_qty = int(latest.get("committed_quantity") or 0) + int(latest.get("remaining_reserved_quantity") or 0)
        total += _scaled_risk_cents(qty=active_qty, requested_quantity=requested_quantity, risk_cents=risk_cents)
    return total


def _intent_fill_state(
    *,
    day_utc: str,
    truth_root: Path,
    intent_hash: str,
    auth_obj: Dict[str, Any],
    authorized_quantity: int,
) -> Dict[str, Any]:
    fill_aggregates = _load_fill_aggregates(day_utc, truth_root)
    aggregate = fill_aggregates.get(intent_hash)
    if aggregate is None:
        return {
            "filled_quantity": 0,
            "release_quantity": 0,
            "remaining_quantity": authorized_quantity,
            "terminal_release": False,
            "produced_utc": str(auth_obj.get("produced_utc") or ""),
            "source_refs": [],
        }
    if aggregate["source_intent_id"] != str(auth_obj.get("intent_id") or "").strip():
        raise SystemExit(f"FAIL: FILL_LEDGER_SOURCE_INTENT_MISMATCH: {intent_hash}")
    if aggregate["engine_id"] != str(auth_obj.get("engine_id") or "").strip():
        raise SystemExit(f"FAIL: FILL_LEDGER_ENGINE_MISMATCH: {intent_hash}")
    filled_quantity = int(aggregate["filled_qty"])
    if filled_quantity > authorized_quantity:
        raise SystemExit(
            f"FAIL: FILL_QTY_EXCEEDS_AUTHORIZED intent_hash={intent_hash} filled={filled_quantity} authorized={authorized_quantity}"
        )
    remaining_quantity = authorized_quantity - filled_quantity
    terminal_release = any(status in {"CANCELLED", "REJECTED", "INACTIVE"} for status in aggregate["statuses"])
    release_quantity = remaining_quantity if terminal_release else 0
    return {
        "filled_quantity": filled_quantity,
        "release_quantity": release_quantity,
        "remaining_quantity": remaining_quantity,
        "terminal_release": terminal_release,
        "produced_utc": str(aggregate["produced_utc"] or str(auth_obj.get("produced_utc") or "")),
        "source_refs": list(aggregate["paths"]),
    }


def _exposure_risk_cents_for_sleeve(*, day_utc: str, truth_root: Path, canonical_sleeve_id: str) -> int:
    exposure_dir = (truth_root / "exposure_v1" / "exposure_ledger_v1" / day_utc).resolve()
    if not exposure_dir.exists() or not exposure_dir.is_dir():
        if _positions_exist(day_utc, truth_root):
            raise SystemExit(f"FAIL: EXPOSURE_LEDGER_REQUIRED_FOR_POSITIONS: {exposure_dir}")
        return 0
    validation_index = load_budget_validation_index_v1(day_utc=day_utc, truth_root=truth_root)
    total = 0
    for path in sorted(exposure_dir.glob("*.exposure_ledger.v1.json")):
        obj = _read_json_obj(path)
        if str(obj.get("canonical_sleeve_id") or "").strip() != canonical_sleeve_id:
            continue
        intent_hash = str(obj.get("intent_hash") or "").strip()
        validation_entry = validation_index.get(intent_hash)
        if validation_entry is None:
            raise SystemExit(f"FAIL: EXPOSURE_LEDGER_VALIDATION_MISSING intent_hash={intent_hash}")
        validation_obj = validation_entry["obj"]
        total += _scaled_risk_cents(
            qty=int(obj.get("qty") or 0),
            requested_quantity=int(validation_obj.get("requested_quantity") or 0),
            risk_cents=int(validation_obj.get("risk_cents") or 0),
        )
    return total


def _list_auth_paths(day_utc: str, truth_root: Path) -> List[Path]:
    auth_dir = (truth_root / "engine_activity_v1" / "authorization_v1" / day_utc).resolve()
    if not auth_dir.exists() or not auth_dir.is_dir():
        raise SystemExit(f"FAIL: AUTHORIZATION_DIR_MISSING: {auth_dir}")
    paths = sorted(auth_dir.glob("*.authorization.v1.json"))
    if not paths:
        raise SystemExit(f"FAIL: AUTHORIZATION_FILES_MISSING: {auth_dir}")
    return paths


def _require_core_authorization_fields(auth_path: Path, auth_obj: Dict[str, Any]) -> int:
    if str(auth_obj.get("schema_id") or "").strip() != "C2_AUTHORIZATION_V1":
        raise SystemExit(f"FAIL: AUTH_SCHEMA_ID_INVALID: {auth_path}")
    if int(auth_obj.get("schema_version") or 0) != 1:
        raise SystemExit(f"FAIL: AUTH_SCHEMA_VERSION_INVALID: {auth_path}")
    for key in ("day_utc", "intent_hash", "intent_id", "engine_id", "produced_utc", "status"):
        if not str(auth_obj.get(key) or "").strip():
            raise SystemExit(f"FAIL: AUTH_REQUIRED_FIELD_MISSING key={key} path={auth_path}")
    authorization_block = auth_obj.get("authorization")
    if not isinstance(authorization_block, dict):
        raise SystemExit(f"FAIL: AUTHORIZATION_BLOCK_INVALID: {auth_path}")
    if str(authorization_block.get("decision") or "").strip() == "":
        raise SystemExit(f"FAIL: AUTH_DECISION_MISSING: {auth_path}")
    authorized_quantity = authorization_block.get("authorized_quantity")
    if not isinstance(authorized_quantity, int) or authorized_quantity < 0:
        raise SystemExit(f"FAIL: AUTHORIZED_QUANTITY_INVALID: {auth_path}")
    return authorized_quantity


def _validation_reason_codes(
    *,
    auth_obj: Dict[str, Any],
    configured_mode: str,
    effective_mode: str,
    risk_cents: int,
    available_budget_cents: int,
    total_risk_cents: int,
    assigned_budget_cents: int,
    final_decision: str,
    fill_state: Dict[str, Any],
) -> List[str]:
    reason_codes = [str(code) for code in auth_obj.get("reason_codes") or [] if str(code).strip()]
    reason_codes.append("BUDGET_UNIT_RISK_CENTS")
    if risk_cents > available_budget_cents:
        reason_codes.append("BUDGET_VIOLATION_DETECTED")
        reason_codes.append("WOULD_BLOCK_UNDER_ENFORCEMENT")
    if total_risk_cents > assigned_budget_cents:
        reason_codes.append("TOTAL_RISK_EXCEEDS_BUDGET")
    if final_decision == "REJECT":
        reason_codes.append("ENFORCEMENT_DECISION_REJECT")
        reason_codes.append("WOULD_REJECT_UNDER_ENFORCEMENT")
    if effective_mode == "HARD":
        reason_codes.append("ENFORCEMENT_MODE_HARD")
    elif effective_mode == "SOFT":
        reason_codes.append("ENFORCEMENT_MODE_SOFT")
    else:
        reason_codes.append("ENFORCEMENT_MODE_SHADOW")
    if int(fill_state["filled_quantity"]) > 0:
        reason_codes.append("FILL_COMMIT_OBSERVED")
    if int(fill_state["release_quantity"]) > 0:
        reason_codes.append("RESERVATION_RELEASE_OBSERVED")
    if bool(fill_state["terminal_release"]):
        reason_codes.append("TERMINAL_RELEASE_OBSERVED")
    return sorted(set(code for code in reason_codes if code))


def _budget_validation_payload(
    *,
    day_utc: str,
    auth_path: Path,
    auth_obj: Dict[str, Any],
    authority: Dict[str, Any],
    canonical_sleeve_id: str,
    available_budget_cents: int,
    assigned_budget_cents: int,
    exposure_risk_cents: int,
    risk_context: Dict[str, Any],
    final_decision: str,
    ledger_authoritative_state: str,
    authorized_quantity: int,
    committed_quantity: int,
    released_quantity: int,
    remaining_reserved_quantity: int,
    other_active_spend_cents: int,
    fill_state: Dict[str, Any],
    decision_source: str,
) -> Dict[str, Any]:
    configured_mode = configured_enforcement_mode_v1()
    effective_mode = effective_enforcement_mode_v1()
    enforce_budget = enforce_budget_enabled_v1()
    risk_cents = int(risk_context["risk_cents"])
    total_risk_cents = exposure_risk_cents + risk_cents
    would_block = risk_cents > available_budget_cents
    total_risk_exceeds_budget = total_risk_cents > assigned_budget_cents
    reason_codes = _validation_reason_codes(
        auth_obj=auth_obj,
        configured_mode=configured_mode,
        effective_mode=effective_mode,
        risk_cents=risk_cents,
        available_budget_cents=available_budget_cents,
        total_risk_cents=total_risk_cents,
        assigned_budget_cents=assigned_budget_cents,
        final_decision=final_decision,
        fill_state=fill_state,
    )
    return {
        "schema_id": "C2_INTENT_BUDGET_VALIDATION_V1_UNGOVERNED",
        "schema_version": 1,
        "day_utc": day_utc,
        "intent_hash": str(auth_obj.get("intent_hash") or ""),
        "intent_id": str(auth_obj.get("intent_id") or ""),
        "engine_id": str(auth_obj.get("engine_id") or ""),
        "canonical_sleeve_id": canonical_sleeve_id,
        "allocation_epoch_id": str(authority["epoch_id"] or ""),
        "allocation_authority_source": str(authority["authority_source"] or ""),
        "enforcement_mode_configured": configured_mode,
        "enforcement_mode_effective": effective_mode,
        "risk_unit": "risk_cents",
        "risk_cents": risk_cents,
        "requested_quantity": int(risk_context["requested_quantity"]),
        "authorized_quantity": int(authorized_quantity),
        "committed_quantity": int(committed_quantity),
        "released_quantity": int(released_quantity),
        "remaining_reserved_quantity": int(remaining_reserved_quantity),
        "exposure_risk_cents": int(exposure_risk_cents),
        "total_risk_cents": int(total_risk_cents),
        "allocation_budget_cents": int(assigned_budget_cents),
        "remaining_budget_cents": int(available_budget_cents),
        "other_active_spend_cents": int(other_active_spend_cents),
        "enforce_budget": enforce_budget,
        "would_block_under_enforcement": bool(would_block),
        "total_risk_exceeds_budget": bool(total_risk_exceeds_budget),
        "decision_shadow": "WOULD_REJECT" if final_decision == "REJECT" else "ALLOW",
        "enforcement_decision": "REJECT" if final_decision == "REJECT" else "ALLOW",
        "final_decision": final_decision,
        "ledger_authoritative_state": ledger_authoritative_state,
        "decision_source": decision_source,
        "reason_codes": reason_codes,
        "source_refs": [
            str(auth_path),
            str(authority["path"]),
            str(risk_context["path"]),
            *[str(ref) for ref in fill_state["source_refs"]],
        ],
    }


def _build_event(
    *,
    auth_obj: Dict[str, Any],
    intent_hash: str,
    intent_id: str,
    engine_id: str,
    canonical_sleeve_id: str,
    epoch_id: str,
    requested_quantity: int,
    authorized_quantity: int,
    state: str,
    event_utc: str,
    reserved_quantity: int,
    committed_quantity: int,
    released_quantity: int,
    reason_codes: List[str],
    source_refs: List[str],
) -> Dict[str, Any]:
    remaining_reserved_quantity = max(0, int(reserved_quantity) - int(committed_quantity) - int(released_quantity))
    stable_seed = "|".join(
        [
            intent_hash,
            state,
            str(requested_quantity),
            str(authorized_quantity),
            str(reserved_quantity),
            str(committed_quantity),
            str(released_quantity),
            "v2",
        ]
    )
    event = {
        "schema_id": "C2_INTENT_AUTHORIZATION_LEDGER_V1",
        "schema_version": 1,
        "day_utc": str(auth_obj.get("day_utc") or ""),
        "intent_hash": intent_hash,
        "intent_id": intent_id,
        "engine_id": engine_id,
        "canonical_sleeve_id": canonical_sleeve_id,
        "allocation_epoch_id": epoch_id,
        "event_id": hashlib.sha256(stable_seed.encode("utf-8")).hexdigest(),
        "event_utc": event_utc,
        "state": state,
        "requested_quantity": requested_quantity,
        "authorized_quantity": authorized_quantity,
        "reserved_quantity": reserved_quantity,
        "committed_quantity": committed_quantity,
        "released_quantity": released_quantity,
        "remaining_reserved_quantity": remaining_reserved_quantity,
        "reason_codes": sorted(set(code for code in reason_codes if str(code).strip())),
        "source_refs": [str(ref) for ref in source_refs if str(ref).strip()],
        "idempotency_key": stable_seed,
    }
    validate_against_repo_schema_v1(event, REPO_ROOT, SCHEMA_RELPATH)
    return event


def _initial_final_decision(
    *,
    auth_obj: Dict[str, Any],
    risk_cents: int,
    available_budget_cents: int,
    total_risk_cents: int,
    assigned_budget_cents: int,
) -> str:
    status = str(auth_obj.get("status") or "").strip().upper()
    if status != "AUTHORIZED":
        return "REJECT"
    effective_mode = effective_enforcement_mode_v1()
    if effective_mode in {"SOFT", "HARD"} and (
        risk_cents > available_budget_cents or total_risk_cents > assigned_budget_cents
    ):
        return "REJECT"
    return "APPROVE"


def _append_needed(new_events: List[Dict[str, Any]], candidate: Dict[str, Any]) -> None:
    if new_events and new_events[-1]["idempotency_key"] == candidate["idempotency_key"]:
        return
    new_events.append(candidate)


def _build_authorization_materialization(
    *,
    auth_path: Path,
    auth_obj: Dict[str, Any],
    authority: Dict[str, Any],
    truth_root: Path,
    existing_events: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    authorized_quantity = _require_core_authorization_fields(auth_path, auth_obj)
    intent_hash = str(auth_obj.get("intent_hash") or "").strip()
    intent_id = str(auth_obj.get("intent_id") or "").strip()
    engine_id = str(auth_obj.get("engine_id") or "").strip()
    produced_utc = str(auth_obj.get("produced_utc") or "")
    epoch_path = Path(authority["path"]).resolve()
    epoch_id = str(authority["epoch_id"] or "").strip()
    if not epoch_id:
        raise SystemExit("FAIL: ALLOCATION_AUTHORITY_EPOCH_ID_MISSING")
    canonical_sleeve_id = canonical_sleeve_id_from_engine_id(engine_id, repo_root=REPO_ROOT)
    assigned_budget_cents = _assigned_budget_cents(authority["obj"], canonical_sleeve_id)
    allocation_remaining_budget_cents = _remaining_budget_cents(authority["obj"], canonical_sleeve_id)
    other_active_spend_cents = _current_active_spend_cents(
        day_utc=str(auth_obj.get("day_utc") or "").strip(),
        truth_root=truth_root,
        canonical_sleeve_id=canonical_sleeve_id,
        exclude_intent_hash=intent_hash,
    )
    available_budget_cents = min(
        allocation_remaining_budget_cents,
        max(0, assigned_budget_cents - other_active_spend_cents),
    )
    risk_context = load_authorization_risk_context_v1(
        day_utc=str(auth_obj.get("day_utc") or "").strip(),
        intent_hash=intent_hash,
        auth_obj=auth_obj,
        truth_root=truth_root,
        repo_root=REPO_ROOT,
    )
    risk_cents = int(risk_context["risk_cents"])
    exposure_risk_cents = _exposure_risk_cents_for_sleeve(
        day_utc=str(auth_obj.get("day_utc") or "").strip(),
        truth_root=truth_root,
        canonical_sleeve_id=canonical_sleeve_id,
    )
    total_risk_cents = exposure_risk_cents + risk_cents
    fill_state = _intent_fill_state(
        day_utc=str(auth_obj.get("day_utc") or "").strip(),
        truth_root=truth_root,
        intent_hash=intent_hash,
        auth_obj=auth_obj,
        authorized_quantity=authorized_quantity,
    )

    if existing_events:
        latest_existing = existing_events[-1]
        final_decision = "REJECT" if str(latest_existing.get("state") or "").strip().upper() == "REJECTED" else "APPROVE"
        decision_source = "AUTHORIZATION_LEDGER_V1"
    else:
        final_decision = _initial_final_decision(
            auth_obj=auth_obj,
            risk_cents=risk_cents,
            available_budget_cents=available_budget_cents,
            total_risk_cents=total_risk_cents,
            assigned_budget_cents=assigned_budget_cents,
        )
        decision_source = "AUTHORIZATION_LEDGER_V1_FROM_ARTIFACT_INPUT"

    base_reason_codes = _validation_reason_codes(
        auth_obj=auth_obj,
        configured_mode=configured_enforcement_mode_v1(),
        effective_mode=effective_enforcement_mode_v1(),
        risk_cents=risk_cents,
        available_budget_cents=available_budget_cents,
        total_risk_cents=total_risk_cents,
        assigned_budget_cents=assigned_budget_cents,
        final_decision=final_decision,
        fill_state=fill_state,
    )
    decision_source_refs = [
        str(auth_path),
        str(epoch_path),
        f"allocation_authority_source={authority['authority_source']}",
        f"decision_source={decision_source}",
        f"risk_unit=risk_cents",
        f"risk_cents={risk_cents}",
        f"exposure_risk_cents={exposure_risk_cents}",
        f"total_risk_cents={total_risk_cents}",
        f"assigned_budget_cents={assigned_budget_cents}",
        f"remaining_budget_cents={available_budget_cents}",
        f"risk_model={risk_context['risk_model']}",
        *[str(ref) for ref in fill_state["source_refs"]],
    ]

    new_events: List[Dict[str, Any]] = []
    latest = existing_events[-1] if existing_events else None
    if latest is None:
        _append_needed(
            new_events,
            _build_event(
                auth_obj=auth_obj,
                intent_hash=intent_hash,
                intent_id=intent_id,
                engine_id=engine_id,
                canonical_sleeve_id=canonical_sleeve_id,
                epoch_id=epoch_id,
                requested_quantity=int(risk_context["requested_quantity"]),
                authorized_quantity=max(0, authorized_quantity),
                state="REQUESTED",
                event_utc=produced_utc,
                reserved_quantity=0,
                committed_quantity=0,
                released_quantity=0,
                reason_codes=base_reason_codes,
                source_refs=decision_source_refs,
            ),
        )
        if final_decision == "REJECT":
            _append_needed(
                new_events,
                _build_event(
                    auth_obj=auth_obj,
                    intent_hash=intent_hash,
                    intent_id=intent_id,
                    engine_id=engine_id,
                    canonical_sleeve_id=canonical_sleeve_id,
                    epoch_id=epoch_id,
                    requested_quantity=int(risk_context["requested_quantity"]),
                    authorized_quantity=0,
                    state="REJECTED",
                    event_utc=produced_utc,
                    reserved_quantity=0,
                    committed_quantity=0,
                    released_quantity=0,
                    reason_codes=base_reason_codes,
                    source_refs=decision_source_refs,
                ),
            )
        else:
            _append_needed(
                new_events,
                _build_event(
                    auth_obj=auth_obj,
                    intent_hash=intent_hash,
                    intent_id=intent_id,
                    engine_id=engine_id,
                    canonical_sleeve_id=canonical_sleeve_id,
                    epoch_id=epoch_id,
                    requested_quantity=int(risk_context["requested_quantity"]),
                    authorized_quantity=authorized_quantity,
                    state="AUTHORIZED",
                    event_utc=produced_utc,
                    reserved_quantity=0,
                    committed_quantity=0,
                    released_quantity=0,
                    reason_codes=base_reason_codes,
                    source_refs=decision_source_refs,
                ),
            )
            _append_needed(
                new_events,
                _build_event(
                    auth_obj=auth_obj,
                    intent_hash=intent_hash,
                    intent_id=intent_id,
                    engine_id=engine_id,
                    canonical_sleeve_id=canonical_sleeve_id,
                    epoch_id=epoch_id,
                    requested_quantity=int(risk_context["requested_quantity"]),
                    authorized_quantity=authorized_quantity,
                    state="RESERVED",
                    event_utc=produced_utc,
                    reserved_quantity=authorized_quantity,
                    committed_quantity=0,
                    released_quantity=0,
                    reason_codes=base_reason_codes,
                    source_refs=decision_source_refs,
                ),
            )

    combined = [*existing_events, *new_events]
    latest = combined[-1] if combined else None
    if latest is not None and str(latest.get("state") or "").strip().upper() != "REJECTED":
        latest_authorized = int(latest.get("authorized_quantity") or authorized_quantity)
        latest_committed = int(latest.get("committed_quantity") or 0)
        latest_released = int(latest.get("released_quantity") or 0)
        if int(fill_state["filled_quantity"]) > latest_committed:
            _append_needed(
                new_events,
                _build_event(
                    auth_obj=auth_obj,
                    intent_hash=intent_hash,
                    intent_id=intent_id,
                    engine_id=engine_id,
                    canonical_sleeve_id=canonical_sleeve_id,
                    epoch_id=epoch_id,
                    requested_quantity=int(risk_context["requested_quantity"]),
                    authorized_quantity=latest_authorized,
                    state="COMMITTED",
                    event_utc=str(fill_state["produced_utc"]),
                    reserved_quantity=authorized_quantity,
                    committed_quantity=int(fill_state["filled_quantity"]),
                    released_quantity=latest_released,
                    reason_codes=base_reason_codes,
                    source_refs=decision_source_refs,
                ),
            )
            combined = [*existing_events, *new_events]
            latest = combined[-1]
            latest_committed = int(latest.get("committed_quantity") or 0)
            latest_released = int(latest.get("released_quantity") or 0)
        if int(fill_state["release_quantity"]) > latest_released:
            _append_needed(
                new_events,
                _build_event(
                    auth_obj=auth_obj,
                    intent_hash=intent_hash,
                    intent_id=intent_id,
                    engine_id=engine_id,
                    canonical_sleeve_id=canonical_sleeve_id,
                    epoch_id=epoch_id,
                    requested_quantity=int(risk_context["requested_quantity"]),
                    authorized_quantity=latest_authorized,
                    state="RELEASED",
                    event_utc=str(fill_state["produced_utc"]),
                    reserved_quantity=authorized_quantity,
                    committed_quantity=max(int(fill_state["filled_quantity"]), latest_committed),
                    released_quantity=int(fill_state["release_quantity"]),
                    reason_codes=base_reason_codes,
                    source_refs=decision_source_refs,
                ),
            )
            combined = [*existing_events, *new_events]
            latest = combined[-1]

    latest = ([*existing_events, *new_events][-1]) if (existing_events or new_events) else None
    ledger_authoritative_state = "" if latest is None else str(latest.get("state") or "").strip().upper()
    validation_obj = _budget_validation_payload(
        day_utc=str(auth_obj.get("day_utc") or "").strip(),
        auth_path=auth_path,
        auth_obj=auth_obj,
        authority=authority,
        canonical_sleeve_id=canonical_sleeve_id,
        available_budget_cents=available_budget_cents,
        assigned_budget_cents=assigned_budget_cents,
        exposure_risk_cents=exposure_risk_cents,
        risk_context=risk_context,
        final_decision=final_decision,
        ledger_authoritative_state=ledger_authoritative_state,
        authorized_quantity=int(0 if latest is None else latest.get("authorized_quantity") or 0),
        committed_quantity=int(0 if latest is None else latest.get("committed_quantity") or 0),
        released_quantity=int(0 if latest is None else latest.get("released_quantity") or 0),
        remaining_reserved_quantity=int(0 if latest is None else latest.get("remaining_reserved_quantity") or 0),
        other_active_spend_cents=other_active_spend_cents,
        fill_state=fill_state,
        decision_source=decision_source,
    )
    return validation_obj, new_events


def main() -> int:
    ap = argparse.ArgumentParser(prog="run_intent_authorization_ledger_day_v1")
    ap.add_argument("--day", required=True, help="UTC day in YYYY-MM-DD")
    ap.add_argument("--truth_root", default="", help="Optional explicit truth root")
    args = ap.parse_args()

    day_utc = _parse_day(args.day)
    truth_root = _require_truth_root(args.truth_root)
    authority = read_allocation_authority_v1(day_utc=day_utc, truth_root=truth_root, repo_root=REPO_ROOT)

    written = 0
    for auth_path in _list_auth_paths(day_utc, truth_root):
        auth_obj = _read_json_obj(auth_path)
        out_path = (
            truth_root
            / "engine_activity_v1"
            / "intent_authorization_ledger_v1"
            / day_utc
            / f"{auth_obj['intent_hash']}.intent_authorization_ledger.v1.jsonl"
        ).resolve()
        existing_events = _load_existing_events(out_path)
        existing_payload = b"" if not out_path.exists() else out_path.read_bytes()

        validation_obj, new_events = _build_authorization_materialization(
            auth_path=auth_path,
            auth_obj=auth_obj,
            authority=authority,
            truth_root=truth_root,
            existing_events=existing_events,
        )
        validation_path = (
            truth_root
            / "reports"
            / "intent_budget_validation_v1"
            / day_utc
            / f"{auth_obj['intent_hash']}.intent_budget_validation.v1.json"
        ).resolve()
        validation_payload = canonical_json_bytes_v1(validation_obj) + b"\n"
        validation_sha = _write_replace_if_changed(validation_path, validation_payload)
        print(
            "OK: INTENT_BUDGET_VALIDATION_WRITTEN "
            f"day_utc={day_utc} path={validation_path} sha256={validation_sha} git_sha={_git_sha()}"
        )

        append_payload = b"".join(canonical_json_bytes_v1(event) + b"\n" for event in new_events)
        ledger_payload = existing_payload + append_payload
        out_sha = _write_append_only_jsonl(out_path, ledger_payload)
        if not existing_events and new_events:
            print(
                "OK: INTENT_AUTHORIZATION_LEDGER_WRITTEN "
                f"day_utc={day_utc} path={out_path} sha256={out_sha} git_sha={_git_sha()}"
            )
        elif new_events:
            print(
                "OK: INTENT_AUTHORIZATION_LEDGER_APPENDED "
                f"day_utc={day_utc} path={out_path} sha256={out_sha} git_sha={_git_sha()} added_events={len(new_events)}"
            )
        else:
            print(
                "OK: INTENT_AUTHORIZATION_LEDGER_PRESERVED "
                f"day_utc={day_utc} path={out_path} sha256={out_sha} git_sha={_git_sha()}"
            )
        written += 1
    print(f"OK: INTENT_AUTHORIZATION_LEDGER_COUNT day_utc={day_utc} count={written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
