"""
ib_payload_stock_order_v2.py

Constellation 2.0 Phase D
Deterministic Interactive Brokers STK order payload digest builder (v2).

Purpose:
- Same payload format/digest as v1 (IB_STK_ORDER_V1)
- Accepts EquityOrderPlan v2 (schema_version=v2) which adds lineage fields
- Ignores additional lineage fields for payload construction

Hard rules:
- Supports ONLY EquityOrderPlan v2 (schema_id=equity_order_plan, schema_version=v2)
- No floats anywhere in payload
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Tuple

from .canon_json_v1 import canonical_json_bytes_v1, sha256_hex_v1


class IBPayloadError(Exception):
    pass


def build_ib_stk_order_payload_v2(equity_order_plan: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(equity_order_plan, dict):
        raise IBPayloadError("EQUITY_ORDER_PLAN_NOT_OBJECT")

    if equity_order_plan.get("schema_id") != "equity_order_plan" or equity_order_plan.get("schema_version") != "v2":
        raise IBPayloadError("EQUITY_ORDER_PLAN_SCHEMA_MISMATCH")

    if equity_order_plan.get("structure") != "EQUITY_SPOT":
        raise IBPayloadError("ONLY_EQUITY_SPOT_SUPPORTED")

    sym = equity_order_plan.get("symbol")
    ccy = equity_order_plan.get("currency")
    action = equity_order_plan.get("action")
    qty = equity_order_plan.get("qty_shares")
    if not isinstance(sym, str) or not sym.strip():
        raise IBPayloadError("SYMBOL_INVALID")
    if not isinstance(ccy, str) or len(ccy) != 3:
        raise IBPayloadError("CURRENCY_INVALID")
    if action not in ("BUY", "SELL"):
        raise IBPayloadError("ACTION_INVALID")
    if not isinstance(qty, int) or qty <= 0:
        raise IBPayloadError("QTY_INVALID")

    terms = equity_order_plan.get("order_terms")
    if not isinstance(terms, dict):
        raise IBPayloadError("ORDER_TERMS_MISSING")
    ot = terms.get("order_type")
    tif = terms.get("time_in_force")
    lp = terms.get("limit_price")
    if ot not in ("LIMIT", "MARKET"):
        raise IBPayloadError("ORDER_TYPE_INVALID")
    if tif not in ("DAY", "GTC"):
        raise IBPayloadError("TIME_IN_FORCE_INVALID")
    if ot == "LIMIT":
        if not isinstance(lp, str) or not lp.strip():
            raise IBPayloadError("LIMIT_PRICE_REQUIRED_FOR_LIMIT")
    else:
        if lp is not None:
            raise IBPayloadError("LIMIT_PRICE_MUST_BE_NULL_FOR_MARKET")

    payload = {
        "format": "IB_STK_ORDER_V1",
        "secType": "STK",
        "symbol": sym.strip(),
        "currency": ccy,
        "action": action,
        "qty_shares": int(qty),
        "order_type": ot,
        "limit_price": lp if ot == "LIMIT" else None,
        "time_in_force": tif,
    }
    return payload


def digest_ib_payload_v1(payload: Dict[str, Any]) -> str:
    b = canonical_json_bytes_v1(payload)
    return sha256_hex_v1(b)


@dataclass(frozen=True)
class IBPayloadDigestV1:
    digest_sha256: str
    format: str
    notes: str


def build_binding_digest_for_equity_order_plan_v2(equity_order_plan: Dict[str, Any]) -> Tuple[Dict[str, Any], IBPayloadDigestV1]:
    payload = build_ib_stk_order_payload_v2(equity_order_plan)
    dig = digest_ib_payload_v1(payload)
    notes = f"Bound equity {payload['symbol']} {payload['action']} qty={payload['qty_shares']} type={payload['order_type']} tif={payload['time_in_force']}"
    if payload["order_type"] == "LIMIT":
        notes = notes + f" limit={payload['limit_price']}"
    return payload, IBPayloadDigestV1(digest_sha256=dig, format="IB_STK_ORDER_V1", notes=notes)
