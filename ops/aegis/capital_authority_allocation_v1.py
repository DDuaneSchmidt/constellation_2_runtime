from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1
from ops.aegis.event_append_transaction_v1 import (
    contract_input_hashes_for_paths_v1,
    emit_artifact_evidence_transaction_v1,
    sha256_file_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import read_canonical_runtime_evaluation_v1, runtime_evaluation_path_v1

SCHEMA_ID = "capital_authority_allocation"
SCHEMA_VERSION = "v1"
PRODUCER_ID = "ops/tools/build_capital_authority_allocation_v1.py"
PRODUCER_VERSION = "v1"
POLICY_RELPATH = Path("governance/02_REGISTRIES/C2_CAPITAL_AUTHORITY_POLICY_V1.json")
CONSTRUCTION_FAMILY = "paper_trade_construction_v1"


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")).hexdigest()


def allocation_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "allocation_v1" / "capital_authority_allocation_v1" / day_utc / "capital_authority_allocation.v1.json"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _money(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")


def _cents(value: Decimal | None) -> int:
    if value is None:
        return 0
    return int((value * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _construction_path(root: Path, day: str) -> Path:
    return root / "reports" / CONSTRUCTION_FAMILY / day / "paper_trade_construction.v1.json"


def _policy_row(policy: Mapping[str, Any], sleeve_id: str) -> dict[str, Any]:
    rows = policy.get("sleeves") if isinstance(policy.get("sleeves"), list) else []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        engine_ids = {str(item) for item in row.get("engine_ids", []) if str(item)} if isinstance(row.get("engine_ids"), list) else set()
        if sleeve_id == str(row.get("sleeve_id") or "") or sleeve_id in engine_ids:
            return dict(row)
    return {}


def _source_artifact_hash(payload: Mapping[str, Any], artifact_id: str) -> str:
    rows = payload.get("source_artifacts") if isinstance(payload.get("source_artifacts"), list) else []
    for row in rows:
        if isinstance(row, Mapping) and str(row.get("artifact_id") or "") == artifact_id:
            return str(row.get("sha256") or "")
    return ""


def _sleeve_allocations(policy: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], Decimal]:
    allowed: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    total = Decimal("0")
    for row in policy.get("sleeves") if isinstance(policy.get("sleeves"), list) else []:
        if not isinstance(row, Mapping):
            continue
        limits = row.get("limits") if isinstance(row.get("limits"), Mapping) else {}
        paper_enabled = bool(limits.get("paper_enabled") is True)
        max_notional = _dec(limits.get("max_notional_per_trade")) or Decimal("0")
        max_risk = _dec(limits.get("max_risk_per_trade")) or Decimal("0")
        sleeve = {
            "sleeve_id": str(row.get("sleeve_id") or ""),
            "engine_ids": [str(item) for item in row.get("engine_ids", [])] if isinstance(row.get("engine_ids"), list) else [],
            "allocation_source": "POLICY_DERIVED" if paper_enabled else "FORBIDDEN",
            "paper_enabled": paper_enabled,
            "max_notional": _money(max_notional),
            "max_risk": _money(max_risk),
            "max_capital_at_risk_cents": int(limits.get("max_capital_at_risk_cents") or 0),
            "max_symbols": int(limits.get("max_symbols") or 0),
            "validation_status": "VALID" if paper_enabled and max_notional > 0 and max_risk > 0 else "FORBIDDEN",
        }
        if sleeve["validation_status"] == "VALID":
            allowed.append(sleeve)
            total += _dec(limits.get("paper_test_capital_base")) or Decimal("0")
        else:
            blocked.append({**sleeve, "blocker_code": "POLICY_FORBIDS_PAPER_ALLOCATION"})
    return allowed, blocked, total


def build_capital_authority_allocation_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated = generated_at_utc or now_utc_v1()
    repo = _repo_root()
    policy_path = (repo / POLICY_RELPATH).resolve()
    policy = _read_json(policy_path)
    construction_path = _construction_path(root, day_utc)
    construction = _read_json(construction_path)
    runtime_path = runtime_evaluation_path_v1(truth_root=root, day_utc=day_utc)
    runtime = read_canonical_runtime_evaluation_v1(truth_root=root, day_utc=day_utc)
    runtime_hash = str(runtime.get("deterministic_output_hash") or "")
    construction_runtime_hash = str(construction.get("runtime_evaluation_hash") or "")
    allowed_sleeves, blocked_sleeves, total_capital = _sleeve_allocations(policy)

    blockers: list[str] = []
    authorization_source = "POLICY_DERIVED"
    manual_command = ""
    authorized_rows: list[dict[str, Any]] = []
    per_intent: list[dict[str, Any]] = []
    selected_id = str(construction.get("selected_exposure_intent_id") or "")
    selected_hash = str(construction.get("selected_exposure_intent_hash") or _source_artifact_hash(construction, "selected_exposure_intent") or "")
    sleeve_id = str(construction.get("sleeve_id") or construction.get("engine_id") or "")
    symbol = str(construction.get("symbol") or "").upper()
    quantity = int(construction.get("suggested_quantity") or 0) if str(construction.get("suggested_quantity") or "").isdigit() else 0
    notional = _dec(construction.get("suggested_notional")) or Decimal("0")
    risk = _dec(construction.get("max_loss_estimate")) or Decimal("0")
    policy_row = _policy_row(policy, sleeve_id)
    limits = policy_row.get("limits") if isinstance(policy_row.get("limits"), Mapping) else {}
    max_notional = _dec(limits.get("max_notional_per_trade"))
    max_risk = _dec(limits.get("max_risk_per_trade"))
    paper_enabled = bool(limits.get("paper_enabled") is True)

    if not policy:
        blockers.append("CAPITAL_AUTHORITY_POLICY_MISSING")
        authorization_source = "FORBIDDEN"
    if not runtime_hash:
        blockers.append("RUNTIME_EVALUATION_MISSING")
        authorization_source = "MANUAL_REQUIRED"
    if construction and construction_runtime_hash and runtime_hash and construction_runtime_hash != runtime_hash:
        blockers.append("RUNTIME_HASH_MISMATCH")
    if not construction:
        blockers.append("PAPER_TRADE_CONSTRUCTION_MISSING")
        authorization_source = "MANUAL_REQUIRED"
    if construction and not selected_id:
        blockers.append("NO_SELECTED_EXPOSURE")
        authorization_source = "MANUAL_REQUIRED"
    if construction and not policy_row:
        blockers.append("SLEEVE_NOT_AUTHORIZED")
        authorization_source = "FORBIDDEN"
    if construction and policy_row and not paper_enabled:
        blockers.append("SLEEVE_PAPER_ALLOCATION_FORBIDDEN")
        authorization_source = "FORBIDDEN"
    if construction and quantity <= 0:
        blockers.append("AUTHORIZED_QUANTITY_MISSING")
    if construction and max_notional is not None and notional > max_notional:
        blockers.append("ALLOCATION_LIMIT_EXCEEDED")
    if construction and max_risk is not None and risk > max_risk:
        blockers.append("ALLOCATION_LIMIT_EXCEEDED")
    if construction and policy_row and (max_notional is None or max_risk is None):
        blockers.append("ALLOCATION_LIMIT_MISSING")
        if authorization_source != "FORBIDDEN":
            authorization_source = "MANUAL_REQUIRED"

    validation_status = "VALID" if not blockers else ("MANUAL_REQUIRED" if authorization_source == "MANUAL_REQUIRED" else "REJECTED")
    if validation_status == "MANUAL_REQUIRED":
        manual_command = f"python3 ops/tools/build_capital_authority_allocation_v1.py --truth-root {root} --day-utc {day_utc} # after governed operator approval artifact is present"

    limit_used = {
        "ticket_id": str(construction.get("ticket_id") or ""),
        "selected_exposure_intent_id": selected_id,
        "sleeve_id": sleeve_id,
        "symbol": symbol,
        "requested_quantity": quantity,
        "requested_notional": _money(notional),
        "requested_risk": _money(risk),
        "max_notional": _money(max_notional),
        "max_risk": _money(max_risk),
        "remaining_notional": _money((max_notional - notional) if max_notional is not None else None),
        "remaining_risk": _money((max_risk - risk) if max_risk is not None else None),
        "validation_status": "VALID" if validation_status == "VALID" else "BLOCKED",
        "blocker_codes": sorted(set(blockers)),
    }
    if validation_status == "VALID":
        authorized_rows.append(
            {
                "intent_id": selected_id,
                "intent_hash": selected_hash,
                "sleeve_id": sleeve_id,
                "engine_id": sleeve_id,
                "execution_sleeve_id": "PRIMARY",
                "account_id": "DUO847203",
                "action_type": "OPEN",
                "symbol": symbol,
                "authorization_outcome": "APPROVED",
                "authorized_quantity": quantity,
                "authorized_notional": _money(notional),
                "authorized_risk": _money(risk),
                "allocation_source": authorization_source,
                "allocation_limit_used": limit_used,
                "runtime_evaluation_hash": runtime_hash,
            }
        )
    per_intent.append({**limit_used, "authorization_outcome": "APPROVED" if validation_status == "VALID" else "BLOCKED"})

    input_paths = [policy_path, construction_path, runtime_path]
    input_hashes = contract_input_hashes_for_paths_v1(
        [path for path in input_paths if path.exists()],
        extra={"runtime_evaluation_hash": runtime_hash, "authorization_source": authorization_source},
    )
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "artifact_id": "",
        "day_utc": day_utc,
        "generated_at_utc": generated,
        "runtime_evaluation_hash": runtime_hash,
        "allocation_policy_version": str(policy.get("schema_id") or "C2_CAPITAL_AUTHORITY_POLICY_V1"),
        "allocation_policy_path": str(policy_path),
        "allocation_policy_hash": sha256_file_v1(policy_path) if policy_path.exists() else "",
        "allocation_source": authorization_source,
        "operator_manual_authority_source": "NOT_REQUIRED" if authorization_source == "POLICY_DERIVED" else authorization_source,
        "manual_required_command": manual_command,
        "total_capital_authorized": _money(total_capital),
        "total_capital_authorized_cents": _cents(total_capital),
        "allowed_sleeves": allowed_sleeves,
        "blocked_sleeves": blocked_sleeves,
        "sleeve_level_allocations": allowed_sleeves,
        "symbol_ticket_allocation_limits": [limit_used] if selected_id else [],
        "max_notional": _money(max_notional),
        "max_risk": _money(max_risk),
        "decision_chain": {"candidate_actions": [], "trade_intents": [], "authorized_trade_intents": authorized_rows},
        "per_sleeve": allowed_sleeves,
        "per_intent": per_intent,
        "portfolio": {
            "allowed_capital_at_risk_cents": sum(int(row.get("max_capital_at_risk_cents") or 0) for row in allowed_sleeves),
            "used_capital_at_risk_cents": _cents(risk) if validation_status == "VALID" else 0,
            "headroom_cents": max(0, sum(int(row.get("max_capital_at_risk_cents") or 0) for row in allowed_sleeves) - (_cents(risk) if validation_status == "VALID" else 0)),
        },
        "input_hashes": input_hashes,
        "output_hash": "",
        "validation_status": validation_status,
        "status": "OK" if validation_status == "VALID" else "BLOCK",
        "reason_codes": sorted(set(blockers)) or ["POLICY_DERIVED_ALLOCATION_AUTHORIZED"],
        "broker_execution_allowed": False,
        "broker_submit_transmit_allowed": False,
        "order_routing_allowed": False,
        "autonomous_execution_allowed": False,
        "trade_advice_allowed": False,
    }
    payload["artifact_id"] = f"capital_authority_allocation_v1:{day_utc}:{stable_hash_v1({**payload, 'artifact_id': '', 'output_hash': ''})[:20]}"
    payload["output_hash"] = stable_hash_v1({**payload, "output_hash": ""})
    return payload


def write_capital_authority_allocation_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any], emit_events: bool = True) -> Path:
    root = Path(truth_root).expanduser().resolve()
    path = allocation_path_v1(truth_root=root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(dict(payload)) + b"\n")
    if emit_events:
        emit_artifact_evidence_transaction_v1(
            truth_root=root,
            day_utc=day_utc,
            artifact_path=path,
            payload=dict(payload),
            producer_id=PRODUCER_ID,
            producer_version=PRODUCER_VERSION,
            created_at_utc=str(payload.get("generated_at_utc") or ""),
            input_hashes=dict(payload.get("input_hashes") or {}),
            validation_status="VALID" if payload.get("validation_status") == "VALID" else "REJECTED",
        )
    return path


def build_and_write_capital_authority_allocation_v1(*, truth_root: Path | str, day_utc: str, generated_at_utc: str | None = None, emit_events: bool = True) -> tuple[dict[str, Any], Path]:
    payload = build_capital_authority_allocation_v1(truth_root=truth_root, day_utc=day_utc, generated_at_utc=generated_at_utc)
    path = write_capital_authority_allocation_v1(truth_root=truth_root, day_utc=day_utc, payload=payload, emit_events=emit_events)
    return {**payload, "artifact_path": str(path), "artifact_hash": sha256_file_v1(path)}, path
