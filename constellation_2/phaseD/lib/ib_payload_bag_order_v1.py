"""
ib_payload_bag_order_v1.py

Constellation 2.0 Phase D
Deterministic Interactive Brokers BAG order payload digest builder (MATCHES Phase A EXACTLY).

Authority:
- constellation_2/governance/C2_DETERMINISM_STANDARD.md
- constellation_2/phaseA/lib/map_vertical_spread_v1.py (authoritative broker_payload digest object shape)
- constellation_2/schemas/order_plan.v1.schema.json
- constellation_2/schemas/binding_record.v1.schema.json (broker_payload_digest.format == IB_BAG_ORDER_V1)

Purpose:
- Build the exact deterministic broker_payload object used in Phase A BindingRecord:
  broker_payload = {
    format, underlying, structure, order_type, limit_price, time_in_force, legs[]
  }
- Compute digest (SHA-256) over canonical JSON bytes (sorted keys, separators, UTF-8)
- Provide a convenience binder that returns payload + digest + notes (notes are not part of digest)

Hard rules:
- Supports ONLY OrderPlan.v1 with structure == VERTICAL_SPREAD and exactly 2 legs.
- No floats anywhere in payload (Decimal strings already in OrderPlan; ints for conId/ratio).
- The digest MUST match Phase A for identical OrderPlan.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple

from .canon_json_v1 import canonical_json_bytes_v1, sha256_hex_v1


class IBPayloadError(Exception):
    pass


def build_ib_bag_order_payload_v1(order_plan: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produce the exact Phase A broker_payload object (JSON-safe, deterministic).

    Keys and shape MUST match Phase A map_vertical_spread_v1.py broker_payload.
    """
    if not isinstance(order_plan, dict):
        raise IBPayloadError("ORDER_PLAN_NOT_OBJECT")

    if order_plan.get("schema_id") != "order_plan" or order_plan.get("schema_version") != "v1":
        raise IBPayloadError("ORDER_PLAN_SCHEMA_MISMATCH")

    if order_plan.get("structure") != "VERTICAL_SPREAD":
        raise IBPayloadError("ONLY_VERTICAL_SPREAD_SUPPORTED")

    underlying = order_plan.get("underlying")
    if not isinstance(underlying, dict):
        raise IBPayloadError("UNDERLYING_MISSING")
    sym = underlying.get("symbol")
    ccy = underlying.get("currency")
    if not isinstance(sym, str) or not sym:
        raise IBPayloadError("UNDERLYING_SYMBOL_INVALID")
    if not isinstance(ccy, str) or len(ccy) != 3:
        raise IBPayloadError("UNDERLYING_CURRENCY_INVALID")

    terms = order_plan.get("order_terms")
    if not isinstance(terms, dict):
        raise IBPayloadError("ORDER_TERMS_MISSING")
    if terms.get("order_type") != "LIMIT":
        raise IBPayloadError("ONLY_LIMIT_SUPPORTED")
    limit_price = terms.get("limit_price")
    tif = terms.get("time_in_force")
    if not isinstance(limit_price, str) or not limit_price:
        raise IBPayloadError("LIMIT_PRICE_INVALID")
    if tif not in ("DAY", "GTC"):
        raise IBPayloadError("TIME_IN_FORCE_INVALID")

    legs = order_plan.get("legs")
    if not isinstance(legs, list) or len(legs) != 2:
        raise IBPayloadError("VERTICAL_SPREAD_REQUIRES_EXACTLY_2_LEGS")

    def _leg(i: int) -> Dict[str, Any]:
        leg = legs[i]
        if not isinstance(leg, dict):
            raise IBPayloadError(f"LEG_NOT_OBJECT[{i}]")
        conid = leg.get("ib_conId")
        act = leg.get("action")
        if not isinstance(conid, int) or conid <= 0:
            raise IBPayloadError(f"LEG_CONID_INVALID[{i}]")
        if act not in ("BUY", "SELL"):
            raise IBPayloadError(f"LEG_ACTION_INVALID[{i}]")
        return {"conId": int(conid), "action": str(act), "ratio": 1}

    payload = {
        "format": "IB_BAG_ORDER_V1",
        "underlying": {"symbol": sym, "currency": ccy},
        "structure": "VERTICAL_SPREAD",
        "order_type": "LIMIT",
        "limit_price": limit_price,
        "time_in_force": tif,
        "legs": [_leg(0), _leg(1)],
    }
    return payload


def digest_ib_payload_v1(payload: Dict[str, Any]) -> str:
    """
    SHA-256 digest of canonical JSON bytes (lowercase hex).
    """
    b = canonical_json_bytes_v1(payload)
    return sha256_hex_v1(b)


@dataclass(frozen=True)
class IBPayloadDigestV1:
    digest_sha256: str
    format: str
    notes: str


def build_binding_digest_for_order_plan_v1(order_plan: Dict[str, Any]) -> Tuple[Dict[str, Any], IBPayloadDigestV1]:
    """
    Convenience: build payload + digest object for BindingRecord.broker_payload_digest.

    NOTE: notes are human-readable and not part of digest.
    """
    payload = build_ib_bag_order_payload_v1(order_plan)
    dig = digest_ib_payload_v1(payload)

    # Best-effort notes (not used for digest matching)
    try:
        legs = order_plan["legs"]
        expiry = legs[0].get("expiry_utc")
        strike0 = legs[0].get("strike")
        strike1 = legs[1].get("strike")
        right = legs[0].get("right")
        notes = f"Bound vertical spread {payload['underlying']['symbol']} {right} {expiry} {strike0}/{strike1} limit={payload['limit_price']}"
    except Exception:  # noqa: BLE001
        notes = "Bound vertical spread (details unavailable)"

    return payload, IBPayloadDigestV1(digest_sha256=dig, format="IB_BAG_ORDER_V1", notes=notes)
