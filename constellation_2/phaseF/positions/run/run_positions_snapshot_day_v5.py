from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from constellation_2.common.truth_root_v1 import resolve_truth_root
from constellation_2.phaseD.lib.canon_json_v1 import CanonicalizationError, canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1
from constellation_2.phaseF.accounting.lib.immut_write_v1 import ImmutableWriteError, write_file_immutable_v1
from constellation_2.phaseF.positions.lib.write_failure_v1 import build_failure_obj_v1, write_failure_immutable_v1


REPO_ROOT = Path(__file__).resolve().parents[4]
SCHEMA_OUT = "governance/04_DATA/SCHEMAS/C2/POSITIONS/positions_snapshot.v5.schema.json"
EQUITY_ORDER_PLAN_V1_SCHEMA = "constellation_2/schemas/equity_order_plan.v1.schema.json"
EQUITY_ORDER_PLAN_V2_SCHEMA = "constellation_2/schemas/equity_order_plan.v2.schema.json"
ORDER_PLAN_V1_SCHEMA = "constellation_2/schemas/order_plan.v1.schema.json"
BROKER_SUB_REC_V2_SCHEMA = "constellation_2/schemas/broker_submission_record.v2.schema.json"
FILL_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/fill_ledger.v1.schema.json"
BROKER_STATEMENT_SCHEMA = "governance/04_DATA/SCHEMAS/C2/EXECUTION_EVIDENCE/broker_statement_normalized.v1.schema.json"
CASH_LEDGER_SCHEMA = "governance/04_DATA/SCHEMAS/C2/CASH_LEDGER/cash_ledger_snapshot.v1.schema.json"

ZERO_SHA = "0" * 64
ZERO_INTENT_SHA = "0" * 64


@dataclass(frozen=True)
class SubmissionProjection:
    submission_id: str
    account_id: str
    position_key: str
    instrument: Dict[str, Any]
    sec_type: str
    binding_hash: str
    engine_id: str
    source_intent_id: str
    intent_sha256: str
    signed_filled_qty: int
    avg_cost_cents: int
    event_time_utc: str


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json_obj(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"TOP_LEVEL_NOT_OBJECT:{path}")
    return obj


def _resolve_truth_root(raw: str) -> Path:
    if str(raw or "").strip():
        path = Path(str(raw).strip()).expanduser().resolve()
        if not path.exists() or not path.is_dir():
            raise SystemExit(f"FAIL: TRUTH_ROOT_INVALID:{path}")
        return path
    return resolve_truth_root(repo_root=REPO_ROOT)


def _paths_for_day(*, truth_root: Path, day_utc: str) -> Dict[str, Path]:
    return {
        "snapshot": (truth_root / "positions_v1" / "snapshots" / day_utc / "positions_snapshot.v5.json").resolve(),
        "failure": (truth_root / "positions_v1" / "failures" / day_utc / "positions_snapshot.v5.failure.json").resolve(),
        "submissions_dir": (truth_root / "execution_evidence_v1" / "submissions" / day_utc).resolve(),
        "fill_ledger_dir": (truth_root / "fill_ledger_v1" / day_utc).resolve(),
        "broker_statement": (
            truth_root / "execution_evidence_v1" / "broker_statement_normalized_v1" / day_utc / "broker_statement_normalized.v1.json"
        ).resolve(),
        "cash_snapshot": (truth_root / "cash_ledger_v1" / "snapshots" / day_utc / "cash_ledger_snapshot.v1.json").resolve(),
    }


def _parse_price_to_cents(value: Any) -> int:
    text = str(value).strip()
    if not text:
        raise ValueError("PRICE_EMPTY")
    try:
        dec = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"PRICE_INVALID:{value!r}") from exc
    return int((dec * Decimal(100)).quantize(Decimal("1")))


def _parse_decimal_to_int_qty(value: Any, *, label: str) -> int:
    try:
        dec = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{label}_INVALID:{value!r}") from exc
    if dec != dec.to_integral_value():
        raise ValueError(f"{label}_NON_INTEGER:{value!r}")
    return int(dec)


def _stable_sha(payload: Dict[str, Any]) -> str:
    return hashlib.sha256((json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")).hexdigest()


def _read_runtime_hash(truth_root: Path, day_utc: str) -> str:
    runtime_path = truth_root / "reports" / "aegis_runtime_truth_kernel_v1" / day_utc / "runtime_evaluation.v1.json"
    try:
        obj = json.loads(runtime_path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    return str(obj.get("deterministic_output_hash") or obj.get("runtime_evaluation_hash") or "")


def _cash_source_type(path: Path) -> str:
    try:
        obj = _read_json_obj(path)
    except Exception:
        return "UNKNOWN"
    return str(obj.get("source_type") or "UNKNOWN").strip().upper() or "UNKNOWN"


def _previous_day(day_utc: str) -> str:
    return (date.fromisoformat(day_utc) - timedelta(days=1)).isoformat()


def _load_previous_snapshot(*, truth_root: Path, day_utc: str) -> Dict[str, Any] | None:
    prev_path = _paths_for_day(truth_root=truth_root, day_utc=_previous_day(day_utc))["snapshot"]
    if not prev_path.exists():
        return None
    prev = _read_json_obj(prev_path)
    validate_against_repo_schema_v1(prev, REPO_ROOT, SCHEMA_OUT)
    return prev


def _normalize_right_v1(value: str) -> str:
    raw = (value or "").strip().upper()
    if raw in {"C", "CALL"}:
        return "C"
    if raw in {"P", "PUT"}:
        return "P"
    raise ValueError(f"RIGHT_INVALID:{value!r}")


def _instrument_from_equity_plan(sd: Path) -> tuple[Dict[str, Any], str]:
    p_v2 = sd / "equity_order_plan.v2.json"
    p_v1 = sd / "equity_order_plan.v1.json"
    if p_v2.exists():
        obj = _read_json_obj(p_v2)
        validate_against_repo_schema_v1(obj, REPO_ROOT, EQUITY_ORDER_PLAN_V2_SCHEMA)
        symbol = str(obj.get("symbol") or "").strip()
        currency = str(obj.get("currency") or "USD").strip() or "USD"
        return (
            {
                "kind": "EQUITY",
                "symbol": symbol,
                "currency": currency,
                "ib_conId": None,
                "ib_localSymbol": None,
            },
            str(obj.get("action") or "").strip().upper(),
        )
    if p_v1.exists():
        obj = _read_json_obj(p_v1)
        validate_against_repo_schema_v1(obj, REPO_ROOT, EQUITY_ORDER_PLAN_V1_SCHEMA)
        symbol = str(obj.get("symbol") or "").strip()
        currency = str(obj.get("currency") or "USD").strip() or "USD"
        return (
            {
                "kind": "EQUITY",
                "symbol": symbol,
                "currency": currency,
                "ib_conId": None,
                "ib_localSymbol": None,
            },
            str(obj.get("action") or "").strip().upper(),
        )
    raise ValueError(f"EQUITY_PLAN_MISSING:{sd}")


def _instrument_from_options_plan(sd: Path) -> tuple[Dict[str, Any], bool]:
    plan_path = sd / "order_plan.v1.json"
    obj = _read_json_obj(plan_path)
    validate_against_repo_schema_v1(obj, REPO_ROOT, ORDER_PLAN_V1_SCHEMA)
    underlying = obj.get("underlying")
    if isinstance(underlying, dict):
        underlying_symbol = str(underlying.get("symbol") or "").strip()
    else:
        underlying_symbol = str(underlying or "").strip()
    legs = obj.get("legs")
    if not isinstance(legs, list) or not legs:
        raise ValueError(f"ORDER_PLAN_LEGS_MISSING:{sd}")
    out_legs: List[Dict[str, Any]] = []
    for leg in legs:
        if not isinstance(leg, dict):
            raise ValueError(f"ORDER_PLAN_LEG_INVALID:{sd}")
        out_legs.append(
            {
                "action": str(leg.get("action") or "").strip().upper(),
                "expiry_utc": str(leg.get("expiry_utc") or "").strip(),
                "strike": str(leg.get("strike") or "").strip(),
                "right": _normalize_right_v1(str(leg.get("right") or "")),
                "ratio": int(leg.get("ratio") or 1),
                "ib_conId": int(leg.get("ib_conId") or 0),
                "ib_localSymbol": str(leg.get("ib_localSymbol") or "").strip(),
            }
        )
    summary = {
        "expiry_utc": out_legs[0]["expiry_utc"] if len(out_legs) == 1 else None,
        "strike": out_legs[0]["strike"] if len(out_legs) == 1 else None,
        "right": out_legs[0]["right"] if len(out_legs) == 1 else None,
    }
    return (
        {
            "kind": "OPTION_SINGLE" if len(out_legs) == 1 else "OPTION_MULTI",
            "underlying": underlying_symbol,
            "legs": out_legs,
            "summary": summary,
        },
        bool((obj.get("order_terms") or {}).get("is_credit") is True),
    )


def _instrument_key_payload(instrument: Dict[str, Any]) -> Dict[str, Any]:
    kind = str(instrument.get("kind") or "").strip().upper()
    if kind == "EQUITY":
        return {
            "kind": "EQUITY",
            "symbol": str(instrument.get("symbol") or "").strip(),
            "currency": str(instrument.get("currency") or "").strip(),
        }
    if kind in {"OPTION_SINGLE", "OPTION_MULTI"}:
        legs = []
        for leg in instrument.get("legs") or []:
            if not isinstance(leg, dict):
                continue
            legs.append(
                {
                    "expiry_utc": str(leg.get("expiry_utc") or "").strip(),
                    "strike": str(leg.get("strike") or "").strip(),
                    "right": str(leg.get("right") or "").strip(),
                    "ratio": int(leg.get("ratio") or 1),
                    "ib_conId": int(leg.get("ib_conId") or 0),
                    "ib_localSymbol": str(leg.get("ib_localSymbol") or "").strip(),
                }
            )
        return {
            "kind": kind,
            "underlying": str(instrument.get("underlying") or "").strip(),
            "legs": legs,
        }
    return instrument


def _position_key(*, account_id: str, instrument: Dict[str, Any]) -> str:
    return _stable_sha({"account_id": account_id, "instrument": _instrument_key_payload(instrument)})


def _instrument_to_sec_type(instrument: Dict[str, Any]) -> str:
    kind = str(instrument.get("kind") or "").strip().upper()
    if kind == "EQUITY":
        return "STK"
    if kind.startswith("OPTION"):
        return "OPT"
    return "OTHER"


def _instrument_to_symbol(instrument: Dict[str, Any]) -> str:
    kind = str(instrument.get("kind") or "").strip().upper()
    if kind == "EQUITY":
        return str(instrument.get("symbol") or "").strip()
    if kind.startswith("OPTION"):
        return str(instrument.get("underlying") or "").strip()
    return str(instrument.get("symbol") or instrument.get("underlying") or "UNKNOWN").strip() or "UNKNOWN"


def _projection_from_submission_dir(sd: Path, ledger: Dict[str, Any], default_account_id: str) -> SubmissionProjection:
    validate_against_repo_schema_v1(ledger, REPO_ROOT, FILL_LEDGER_SCHEMA)
    bsr_path = sd / "broker_submission_record.v2.json"
    if not bsr_path.exists():
        raise ValueError(f"BROKER_SUBMISSION_RECORD_MISSING:{sd}")
    bsr = _read_json_obj(bsr_path)
    validate_against_repo_schema_v1(bsr, REPO_ROOT, BROKER_SUB_REC_V2_SCHEMA)

    if (sd / "equity_order_plan.v2.json").exists() or (sd / "equity_order_plan.v1.json").exists():
        instrument, action = _instrument_from_equity_plan(sd)
        sign = 1 if action == "BUY" else -1
    elif (sd / "order_plan.v1.json").exists():
        instrument, is_credit = _instrument_from_options_plan(sd)
        sign = -1 if is_credit else 1
    else:
        raise ValueError(f"SUBMISSION_PLAN_MISSING:{sd}")

    account_id = str(default_account_id or "").strip()
    if not account_id:
        account_id = str(bsr.get("account_id") or "").strip()
    if not account_id:
        raise ValueError(f"ACCOUNT_ID_MISSING:{sd}")

    filled_qty = int(ledger.get("filled_qty") or 0)
    signed_qty = sign * filled_qty
    event_record_path = sd / "execution_event_record.v1.json"
    event_time_utc = f"{ledger['day_utc']}T00:00:00Z"
    if event_record_path.exists():
        event_time_utc = str(_read_json_obj(event_record_path).get("event_time_utc") or event_time_utc).strip() or event_time_utc
    binding_hash = str(ledger.get("binding_hash") or sd.name).strip() or sd.name
    engine_id = str(ledger.get("engine_id") or "").strip() or "unknown"
    source_intent_id = str(ledger.get("source_intent_id") or "").strip() or f"NATIVE_POSITION:{binding_hash}"
    intent_sha256 = str(ledger.get("intent_sha256") or "").strip()
    if len(intent_sha256) != 64:
        raise ValueError(f"INTENT_SHA_INVALID:{sd}")
    return SubmissionProjection(
        submission_id=sd.name,
        account_id=account_id,
        position_key=_position_key(account_id=account_id, instrument=instrument),
        instrument=instrument,
        sec_type=_instrument_to_sec_type(instrument),
        binding_hash=binding_hash,
        engine_id=engine_id,
        source_intent_id=source_intent_id,
        intent_sha256=intent_sha256,
        signed_filled_qty=signed_qty,
        avg_cost_cents=_parse_price_to_cents(ledger.get("avg_fill_price_weighted") or "0"),
        event_time_utc=str(event_time_utc).strip() or f"{ledger['day_utc']}T00:00:00Z",
    )


def _weighted_avg_cost_cents(lots: List[Dict[str, Any]]) -> int:
    active = [lot for lot in lots if int(lot.get("remaining_qty_abs") or 0) > 0]
    total_qty = sum(int(lot["remaining_qty_abs"]) for lot in active)
    if total_qty <= 0:
        return 0
    total_cost = sum(int(lot["remaining_qty_abs"]) * int(lot["cost_basis_cents"]) for lot in active)
    return int(total_cost / total_qty)


def _direction_from_qty(qty: int) -> str:
    return "LONG" if qty >= 0 else "SHORT"


def _clone_lots(lots: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(lot) for lot in lots if isinstance(lot, dict)]


def _position_state_from_previous(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "position_id": str(item.get("position_id") or "").strip(),
        "account_id": str(item.get("account_id") or "").strip(),
        "origin": str(item.get("origin") or "").strip() or "NATIVE",
        "engine_id": str(item.get("engine_id") or "").strip() or "unknown",
        "source_intent_id": str(item.get("source_intent_id") or "").strip() or f"POSITION:{item.get('position_id')}",
        "intent_sha256": str(item.get("intent_sha256") or ZERO_INTENT_SHA).strip() or ZERO_INTENT_SHA,
        "instrument": dict(item.get("instrument") or {}),
        "qty": int(item.get("qty") or 0),
        "avg_cost_cents": int(item.get("avg_cost_cents") or 0),
        "opened_day_utc": str(item.get("opened_day_utc") or "").strip(),
        "last_transition_utc": str(item.get("last_transition_utc") or "").strip(),
        "last_transition_type": str(item.get("last_transition_type") or "CARRY_FORWARD").strip(),
        "lifecycle_state": str(item.get("lifecycle_state") or "MANAGING").strip(),
        "lifecycle_reason_code": str(item.get("lifecycle_reason_code") or "CARRY_FORWARD_FROM_PREVIOUS_V5").strip(),
        "status": str(item.get("status") or "OPEN").strip(),
        "lots": _clone_lots(item.get("lots") or []),
        "reconciliation": dict(item.get("reconciliation") or {}),
    }


def _apply_signed_fill(position: Dict[str, Any], projection: SubmissionProjection, day_utc: str) -> None:
    signed_qty = projection.signed_filled_qty
    if signed_qty == 0:
        return
    current_qty = int(position.get("qty") or 0)
    current_direction = 0 if current_qty == 0 else (1 if current_qty > 0 else -1)
    fill_direction = 1 if signed_qty > 0 else -1
    lots = _clone_lots(position.get("lots") or [])

    if current_direction == 0 or current_direction == fill_direction:
        lots.append(
            {
                "lot_id": f"{projection.submission_id}:{len(lots) + 1}",
                "direction": "LONG" if fill_direction > 0 else "SHORT",
                "opened_day_utc": day_utc,
                "remaining_qty_abs": abs(signed_qty),
                "cost_basis_cents": int(projection.avg_cost_cents),
                "source_kind": "NATIVE_FILL",
                "source_ref": projection.submission_id,
            }
        )
        position["qty"] = current_qty + signed_qty
        position["last_transition_type"] = "OPEN" if current_direction == 0 else "ADD"
        position["lifecycle_state"] = "OPEN" if current_direction == 0 else "MANAGING"
        position["lifecycle_reason_code"] = "NATIVE_FILL_APPLIED"
    else:
        remaining_to_close = abs(signed_qty)
        retained_lots: List[Dict[str, Any]] = []
        for lot in lots:
            qty_abs = int(lot.get("remaining_qty_abs") or 0)
            if remaining_to_close <= 0 or qty_abs <= 0:
                if qty_abs > 0:
                    retained_lots.append(lot)
                continue
            close_qty = min(qty_abs, remaining_to_close)
            qty_abs -= close_qty
            remaining_to_close -= close_qty
            if qty_abs > 0:
                updated_lot = dict(lot)
                updated_lot["remaining_qty_abs"] = qty_abs
                retained_lots.append(updated_lot)
        lots = retained_lots
        new_qty = current_qty + signed_qty
        if current_qty != 0 and new_qty == 0:
            position["last_transition_type"] = "FULL_CLOSE"
            position["lifecycle_state"] = "CLOSED"
            position["lifecycle_reason_code"] = "NATIVE_FILL_FULL_CLOSE"
            position["status"] = "CLOSED"
        elif current_qty != 0 and ((current_qty > 0 and new_qty > 0) or (current_qty < 0 and new_qty < 0)):
            position["last_transition_type"] = "PARTIAL_CLOSE"
            position["lifecycle_state"] = "MANAGING"
            position["lifecycle_reason_code"] = "NATIVE_FILL_PARTIAL_CLOSE"
            position["status"] = "OPEN"
        else:
            remainder_qty = abs(new_qty)
            if remainder_qty > 0:
                lots.append(
                    {
                        "lot_id": f"{projection.submission_id}:remainder",
                        "direction": "LONG" if new_qty > 0 else "SHORT",
                        "opened_day_utc": day_utc,
                        "remaining_qty_abs": remainder_qty,
                        "cost_basis_cents": int(projection.avg_cost_cents),
                        "source_kind": "NATIVE_FILL",
                        "source_ref": projection.submission_id,
                    }
                )
            position["last_transition_type"] = "OPEN"
            position["lifecycle_state"] = "OPEN" if new_qty != 0 else "CLOSED"
            position["lifecycle_reason_code"] = "STOP_AND_REVERSE_REMAINDER_OPENED"
            position["status"] = "OPEN" if new_qty != 0 else "CLOSED"
        position["qty"] = new_qty

    position["last_transition_utc"] = projection.event_time_utc
    position["lots"] = sorted(lots, key=lambda row: str(row.get("lot_id") or ""))
    position["avg_cost_cents"] = _weighted_avg_cost_cents(position["lots"])
    if not position.get("opened_day_utc"):
        position["opened_day_utc"] = day_utc
    if int(position.get("qty") or 0) == 0 and position.get("status") != "CLOSED":
        position["status"] = "CLOSED"
        position["lifecycle_state"] = "CLOSED"


def _build_broker_position(*, account_id: str, row: Dict[str, Any], day_utc: str) -> Dict[str, Any]:
    sec_type = str(row.get("sec_type") or "").strip().upper()
    symbol = str(row.get("symbol") or "").strip()
    currency = str(row.get("currency") or "USD").strip() or "USD"
    qty = _parse_decimal_to_int_qty(row.get("qty") or "0", label="BROKER_QTY")
    avg_cost_cents = _parse_price_to_cents(row.get("avg_cost") or "0")
    if sec_type == "STK":
        instrument = {"kind": "EQUITY", "symbol": symbol, "currency": currency, "ib_conId": None, "ib_localSymbol": None}
    else:
        instrument = {"kind": f"BROKER_{sec_type or 'OTHER'}", "symbol": symbol, "sec_type": sec_type, "currency": currency}
    direction = _direction_from_qty(qty)
    position_id = _position_key(account_id=account_id, instrument=instrument)
    return {
        "position_id": position_id,
        "account_id": account_id,
        "origin": "IMPORTED",
        "engine_id": "imported_position",
        "source_intent_id": f"IMPORTED_POSITION:{position_id}",
        "intent_sha256": ZERO_INTENT_SHA,
        "instrument": instrument,
        "qty": qty,
        "avg_cost_cents": avg_cost_cents,
        "opened_day_utc": day_utc,
        "last_transition_utc": f"{day_utc}T00:00:00Z",
        "last_transition_type": "IMPORTED_SNAPSHOT",
        "lifecycle_state": "MANAGING" if qty != 0 else "CLOSED",
        "lifecycle_reason_code": "IMPORTED_FROM_BROKER_STATEMENT",
        "status": "OPEN" if qty != 0 else "CLOSED",
        "lots": [
            {
                "lot_id": f"broker:{position_id}",
                "direction": direction,
                "opened_day_utc": day_utc,
                "remaining_qty_abs": abs(qty),
                "cost_basis_cents": avg_cost_cents,
                "source_kind": "BROKER_AGGREGATE",
                "source_ref": "broker_statement_normalized.v1.json",
            }
        ],
        "reconciliation": {
            "broker_position_present": True,
            "broker_qty": str(row.get("qty") or "0"),
            "status": "MATCH",
            "reason_codes": ["BROKER_POSITION_IMPORTED"],
        },
    }


def _sync_to_broker(
    *,
    positions_by_key: Dict[str, Dict[str, Any]],
    broker_rows: List[Dict[str, Any]],
    account_id: str,
    day_utc: str,
) -> None:
    broker_keys_seen: set[str] = set()
    for row in broker_rows:
        broker_position = _build_broker_position(account_id=account_id, row=row, day_utc=day_utc)
        position_key = broker_position["position_id"]
        broker_keys_seen.add(position_key)
        existing = positions_by_key.get(position_key)
        if existing is None:
            positions_by_key[position_key] = broker_position
            continue
        broker_qty = int(broker_position["qty"])
        existing["reconciliation"] = {
            "broker_position_present": True,
            "broker_qty": str(row.get("qty") or "0"),
            "status": "MATCH" if int(existing.get("qty") or 0) == broker_qty else "MISMATCH",
            "reason_codes": ["BROKER_STATEMENT_OVERLAY_APPLIED"],
        }
        if int(existing.get("qty") or 0) != broker_qty or int(existing.get("avg_cost_cents") or 0) != int(broker_position["avg_cost_cents"]):
            existing["qty"] = broker_qty
            existing["avg_cost_cents"] = int(broker_position["avg_cost_cents"])
            existing["lots"] = broker_position["lots"]
            existing["last_transition_utc"] = f"{day_utc}T00:00:00Z"
            if broker_qty == 0:
                existing["last_transition_type"] = "FULL_CLOSE"
                existing["lifecycle_state"] = "CLOSED"
                existing["lifecycle_reason_code"] = "BROKER_STATEMENT_FULL_CLOSE"
                existing["status"] = "CLOSED"
            else:
                existing["last_transition_type"] = "BROKER_RECONCILED"
                existing["lifecycle_state"] = "MANAGING"
                existing["lifecycle_reason_code"] = "BROKER_STATEMENT_RECONCILED"
                existing["status"] = "OPEN"
    for key, existing in list(positions_by_key.items()):
        if key in broker_keys_seen:
            continue
        if int(existing.get("qty") or 0) == 0:
            continue
        existing["reconciliation"] = {
            "broker_position_present": False,
            "broker_qty": None,
            "status": "INTERNAL_ONLY",
            "reason_codes": ["POSITION_MISSING_FROM_BROKER_STATEMENT"],
        }
        existing["qty"] = 0
        existing["avg_cost_cents"] = 0
        existing["lots"] = []
        existing["last_transition_utc"] = f"{day_utc}T00:00:00Z"
        existing["last_transition_type"] = "FULL_CLOSE"
        existing["lifecycle_state"] = "CLOSED"
        existing["lifecycle_reason_code"] = "BROKER_STATEMENT_FULL_CLOSE"
        existing["status"] = "CLOSED"


def _cash_snapshot_state(cash_obj: Dict[str, Any]) -> Tuple[int, str]:
    validate_against_repo_schema_v1(cash_obj, REPO_ROOT, CASH_LEDGER_SCHEMA)
    snapshot = cash_obj.get("snapshot")
    if not isinstance(snapshot, dict):
        raise ValueError("CASH_LEDGER_SNAPSHOT_OBJECT_MISSING")
    return int(snapshot.get("cash_total_cents") or 0), str(snapshot.get("currency") or "USD").strip() or "USD"


def _broker_cash_cents(broker_obj: Dict[str, Any]) -> int:
    validate_against_repo_schema_v1(broker_obj, REPO_ROOT, BROKER_STATEMENT_SCHEMA)
    return _parse_price_to_cents(broker_obj.get("cash_end") or "0")


def _aggregate_internal_positions(items: Iterable[Dict[str, Any]]) -> Dict[Tuple[str, str], int]:
    out: Dict[Tuple[str, str], int] = {}
    for item in items:
        if str(item.get("status") or "").strip().upper() != "OPEN":
            continue
        qty = int(item.get("qty") or 0)
        if qty == 0:
            continue
        instrument = item.get("instrument") if isinstance(item.get("instrument"), dict) else {}
        key = (_instrument_to_symbol(instrument), _instrument_to_sec_type(instrument))
        out[key] = out.get(key, 0) + qty
    return out


def _aggregate_broker_positions(broker_rows: Iterable[Dict[str, Any]]) -> Dict[Tuple[str, str], int]:
    out: Dict[Tuple[str, str], int] = {}
    for row in broker_rows:
        symbol = str(row.get("symbol") or "").strip() or "UNKNOWN"
        sec_type = str(row.get("sec_type") or "").strip() or "OTHER"
        out[(symbol, sec_type)] = out.get((symbol, sec_type), 0) + _parse_decimal_to_int_qty(row.get("qty") or "0", label="BROKER_QTY")
    return out


def _build_reconciliation_summary(
    *,
    broker_statement_path: Path,
    broker_rows: List[Dict[str, Any]],
    items: List[Dict[str, Any]],
    cash_total_cents: int | None,
    broker_cash_cents: int | None,
) -> Dict[str, Any]:
    if not broker_statement_path.exists():
        return {
            "broker_statement_present": False,
            "broker_statement_path": None,
            "cash_status": "UNKNOWN",
            "cash_delta_cents": None,
            "positions_status": "UNKNOWN",
            "reason_codes": ["BROKER_STATEMENT_MISSING"],
            "position_mismatches": [],
        }
    internal_map = _aggregate_internal_positions(items)
    broker_map = _aggregate_broker_positions(broker_rows)
    mismatches: List[Dict[str, Any]] = []
    for key in sorted(set(internal_map) | set(broker_map)):
        if internal_map.get(key, 0) == broker_map.get(key, 0):
            continue
        mismatches.append(
            {
                "symbol": key[0],
                "sec_type": key[1],
                "internal_qty": str(internal_map.get(key, 0)),
                "broker_qty": str(broker_map.get(key, 0)),
            }
        )
    cash_delta = None if cash_total_cents is None or broker_cash_cents is None else int(cash_total_cents - broker_cash_cents)
    return {
        "broker_statement_present": True,
        "broker_statement_path": str(broker_statement_path),
        "cash_status": "MATCH" if cash_delta == 0 else "MISMATCH",
        "cash_delta_cents": cash_delta,
        "positions_status": "MATCH" if not mismatches else "MISMATCH",
        "reason_codes": [] if not mismatches and cash_delta == 0 else ["BROKER_RECONCILIATION_DIFF_PRESENT"],
        "position_mismatches": mismatches,
    }


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_positions_snapshot_day_v5")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--producer_git_sha", required=True)
    ap.add_argument("--producer_repo", default="constellation_2_runtime")
    ap.add_argument("--truth_root", default="")
    ap.add_argument("--ib_account", default="")
    args = ap.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    producer_sha = str(args.producer_git_sha).strip()
    producer_repo = str(args.producer_repo).strip() or "constellation_2_runtime"
    truth_root = _resolve_truth_root(str(args.truth_root))
    paths = _paths_for_day(truth_root=truth_root, day_utc=day_utc)

    try:
        previous = _load_previous_snapshot(truth_root=truth_root, day_utc=day_utc)
        positions_by_key: Dict[str, Dict[str, Any]] = {}
        if previous is not None:
            for item in previous.get("items") or []:
                if not isinstance(item, dict):
                    continue
                state = _position_state_from_previous(item)
                if state["position_id"]:
                    state["last_transition_type"] = "CARRY_FORWARD"
                    state["lifecycle_reason_code"] = "CARRY_FORWARD_FROM_PREVIOUS_V5"
                    positions_by_key[state["position_id"]] = state

        input_manifest: List[Dict[str, Any]] = []
        for name in ("submissions_dir", "fill_ledger_dir", "broker_statement", "cash_snapshot"):
            path = paths[name]
            if path.exists():
                input_manifest.append(
                    {
                        "type": name,
                        "path": str(path),
                        "sha256": _sha256_file(path) if path.is_file() else ZERO_SHA,
                        "day_utc": day_utc,
                        "producer": name,
                    }
                )
            else:
                input_manifest.append({"type": f"{name}_missing", "path": str(path), "sha256": ZERO_SHA, "day_utc": day_utc, "producer": None})

        default_account_id = str(args.ib_account or "").strip()
        broker_obj: Dict[str, Any] | None = None
        broker_rows: List[Dict[str, Any]] = []
        broker_cash_cents: int | None = None
        if paths["broker_statement"].exists():
            broker_obj = _read_json_obj(paths["broker_statement"])
            validate_against_repo_schema_v1(broker_obj, REPO_ROOT, BROKER_STATEMENT_SCHEMA)
            broker_rows = [row for row in broker_obj.get("positions") or [] if isinstance(row, dict)]
            broker_cash_cents = _broker_cash_cents(broker_obj)
            default_account_id = default_account_id or str(broker_obj.get("account_id") or "").strip()

        cash_total_cents: int | None = None
        cash_currency = "USD"
        if paths["cash_snapshot"].exists():
            cash_obj = _read_json_obj(paths["cash_snapshot"])
            cash_total_cents, cash_currency = _cash_snapshot_state(cash_obj)

        if not default_account_id:
            raise ValueError("IB_ACCOUNT_UNPROVEN")

        fill_dir = paths["fill_ledger_dir"]
        if fill_dir.exists():
            for ledger_path in sorted(fill_dir.glob("*.fill_ledger.v1.json")):
                ledger = _read_json_obj(ledger_path)
                submission_id = str(ledger.get("submission_id") or "").strip()
                if not submission_id:
                    raise ValueError(f"FILL_LEDGER_SUBMISSION_ID_MISSING:{ledger_path}")
                if int(ledger.get("filled_qty") or 0) <= 0:
                    continue
                subdir = (paths["submissions_dir"] / submission_id).resolve()
                if not subdir.exists() or not subdir.is_dir():
                    raise ValueError(f"SUBMISSION_DIR_MISSING_FOR_FILL_LEDGER:{submission_id}")
                projection = _projection_from_submission_dir(subdir, ledger, default_account_id)
                position = positions_by_key.get(projection.position_key)
                if position is None:
                    position = {
                        "position_id": projection.position_key,
                        "account_id": projection.account_id,
                        "origin": "NATIVE",
                        "engine_id": projection.engine_id,
                        "source_intent_id": projection.source_intent_id,
                        "intent_sha256": projection.intent_sha256,
                        "instrument": projection.instrument,
                        "qty": 0,
                        "avg_cost_cents": 0,
                        "opened_day_utc": day_utc,
                        "last_transition_utc": f"{day_utc}T00:00:00Z",
                        "last_transition_type": "OPEN",
                        "lifecycle_state": "OPEN",
                        "lifecycle_reason_code": "NATIVE_FILL_APPLIED",
                        "status": "OPEN",
                        "lots": [],
                        "reconciliation": {
                            "broker_position_present": False,
                            "broker_qty": None,
                            "status": "UNKNOWN",
                            "reason_codes": ["BROKER_RECONCILIATION_PENDING"],
                        },
                    }
                    positions_by_key[projection.position_key] = position
                position["engine_id"] = projection.engine_id
                position["source_intent_id"] = projection.source_intent_id
                position["intent_sha256"] = projection.intent_sha256
                position["instrument"] = projection.instrument
                position["origin"] = "NATIVE"
                _apply_signed_fill(position, projection, day_utc)

        if broker_rows:
            _sync_to_broker(
                positions_by_key=positions_by_key,
                broker_rows=broker_rows,
                account_id=default_account_id,
                day_utc=day_utc,
            )

        items = sorted(
            [value for value in positions_by_key.values() if value.get("position_id")],
            key=lambda row: (str(row.get("account_id") or ""), str(row.get("position_id") or "")),
        )

        accounts = [
            {
                "account_id": default_account_id,
                "currency": cash_currency or str((broker_obj or {}).get("currency") or "USD").strip() or "USD",
                "cash_total_cents": int(cash_total_cents or 0),
                "broker_cash_cents": broker_cash_cents,
                "cash_source": (
                    "CASH_LEDGER_AND_BROKER_STATEMENT"
                    if cash_total_cents is not None and broker_cash_cents is not None
                    else ("CASH_LEDGER_ONLY" if cash_total_cents is not None else "BROKER_STATEMENT_ONLY")
                ),
                "reason_codes": (
                    []
                    if cash_total_cents is not None
                    else ["CASH_LEDGER_MISSING"]
                ),
            }
        ]

        reconciliation = _build_reconciliation_summary(
            broker_statement_path=paths["broker_statement"],
            broker_rows=broker_rows,
            items=items,
            cash_total_cents=cash_total_cents,
            broker_cash_cents=broker_cash_cents,
        )

        out: Dict[str, Any] = {
            "schema_id": "C2_POSITIONS_SNAPSHOT_V5",
            "schema_version": 5,
            "day_utc": day_utc,
            "produced_utc": f"{day_utc}T00:00:00Z",
            "producer": {
                "repo": producer_repo,
                "git_sha": producer_sha,
                "module": "constellation_2/phaseF/positions/run/run_positions_snapshot_day_v5.py",
            },
            "status": "OK",
            "reason_codes": (
                ["BUNDLE_A_CANONICAL_STATE_V5", "BROKER_STATEMENT_PRESENT"]
                if broker_rows
                else ["BUNDLE_A_CANONICAL_STATE_V5", "BROKER_STATEMENT_MISSING_INTERNAL_STATE_ONLY"]
            ),
            "input_manifest": input_manifest,
            "accounts": accounts,
            "items": items,
            "reconciliation": reconciliation,
            "runtime_evaluation_hash": _read_runtime_hash(truth_root, day_utc),
            "source_type": "BROKER_EXPORT" if broker_rows else "SIMULATION_LEDGER",
            "cash_ledger_source_type": _cash_source_type(paths["cash_snapshot"]) if paths["cash_snapshot"].exists() else "UNKNOWN",
            "source_hash": _stable_sha({"input_manifest": input_manifest, "items": items, "accounts": accounts}),
            "downstream_dependency_hashes": {"cash_ledger_snapshot_v1": _sha256_file(paths["cash_snapshot"]) if paths["cash_snapshot"].exists() else ZERO_SHA},
            "output_hash": None,
            "canonical_json_hash": None,
        }
        out["output_hash"] = _stable_sha({**out, "output_hash": "", "canonical_json_hash": None})
        out["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(out)
        validate_against_repo_schema_v1(out, REPO_ROOT, SCHEMA_OUT)
        payload = canonical_json_bytes_v1(out) + b"\n"
        _ = write_file_immutable_v1(path=paths["snapshot"], data=payload, create_dirs=True)
    except ImmutableWriteError as exc:
        print(f"FAIL: IMMUTABLE_WRITE_FAILED: {exc}", file=sys.stderr)
        return 4
    except CanonicalizationError as exc:
        print(f"FAIL: CANONICALIZATION_ERROR: {exc}", file=sys.stderr)
        return 4
    except Exception as exc:  # noqa: BLE001
        failure = build_failure_obj_v1(
            day_utc=day_utc,
            producer_repo=producer_repo,
            producer_git_sha=producer_sha,
            producer_module="constellation_2/phaseF/positions/run/run_positions_snapshot_day_v5.py",
            status="FAIL_CORRUPT_INPUTS",
            reason_codes=["BUNDLE_A_V5_BUILD_FAILED"],
            input_manifest=[],
            code="FAIL_CORRUPT_INPUTS",
            message=repr(exc),
            details={"day_utc": day_utc, "truth_root": str(truth_root)},
            attempted_outputs=[{"path": str(paths["snapshot"]), "sha256": None}],
        )
        _ = write_failure_immutable_v1(failure_path=paths["failure"], failure_obj=failure)
        print(f"FAIL: BUNDLE_A_V5_BUILD_FAILED: {exc}", file=sys.stderr)
        return 2

    print("OK: POSITIONS_SNAPSHOT_V5_WRITTEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
