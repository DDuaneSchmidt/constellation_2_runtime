"""
ib_paper_adapter_v1.py

Constellation 2.0 Phase D
Interactive Brokers PAPER adapter using ib_insync.

Authority:
- constellation_2/governance/C2_EXECUTION_CONTRACT.md
- constellation_2/governance/C2_INVARIANTS_AND_REASON_CODES.md
- constellation_2/governance/C2_DETERMINISM_STANDARD.md

Hard rules:
- PAPER only. LIVE is forbidden in Phase D (C2_BROKER_ENV_NOT_PAPER).
- No floats cross the adapter boundary: normalize outputs to decimal strings.
- No hidden defaults: connection spec required.
- Adapter does not write truth artifacts (submit boundary handles evidence chain).
- Any broker connection failure must raise (submit boundary converts to VetoRecord).

Supported plan types:
- Options: OrderPlan v1 (schema_id=order_plan, structure=VERTICAL_SPREAD) => IB BAG combo order (existing behavior)
- Equity: EquityOrderPlan v1 (schema_id=equity_order_plan, structure=EQUITY_SPOT) => IB STK order (new)
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Tuple

from .broker_adapter_v1 import (
    BrokerAdapterV1,
    BrokerConnectionSpec,
    BrokerSubmitResult,
    BrokerWhatIfResult,
)


class IBAdapterError(Exception):
    pass


def _dec_str_from_any(x: Any, name: str) -> str:
    """
    Normalize numeric values to deterministic decimal strings.
    Accepts int/str/float from broker internals; returns string Decimal(str(x)).
    """
    if x is None:
        return "0"
    try:
        if isinstance(x, bool):
            raise IBAdapterError(f"NUMERIC_BOOL_FORBIDDEN: {name}")
        if isinstance(x, (int, float, str)):
            return str(Decimal(str(x)))
        return str(Decimal(str(x)))
    except (InvalidOperation, ValueError) as e:
        raise IBAdapterError(f"NUMERIC_NORMALIZATION_FAILED: {name}={x!r}") from e


def _order_plan_require_vertical_v1(order_plan: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    Minimal structural checks (submit boundary already validates schema/invariants).
    This adapter still refuses obvious invalids to fail-closed.
    """
    if not isinstance(order_plan, dict):
        raise IBAdapterError("ORDER_PLAN_NOT_OBJECT")
    if order_plan.get("schema_id") != "order_plan" or order_plan.get("schema_version") != "v1":
        raise IBAdapterError("ORDER_PLAN_SCHEMA_MISMATCH")
    if order_plan.get("structure") != "VERTICAL_SPREAD":
        raise IBAdapterError("ONLY_VERTICAL_SPREAD_SUPPORTED")
    legs = order_plan.get("legs")
    if not isinstance(legs, list) or len(legs) != 2:
        raise IBAdapterError("VERTICAL_SPREAD_REQUIRES_EXACTLY_2_LEGS")
    l0 = legs[0]
    l1 = legs[1]
    if not isinstance(l0, dict) or not isinstance(l1, dict):
        raise IBAdapterError("LEG_NOT_OBJECT")
    return order_plan, l0, l1


def _equity_plan_require_v1(plan: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(plan, dict):
        raise IBAdapterError("EQUITY_ORDER_PLAN_NOT_OBJECT")
    if plan.get("schema_id") != "equity_order_plan" or plan.get("schema_version") != "v1":
        raise IBAdapterError("EQUITY_ORDER_PLAN_SCHEMA_MISMATCH")
    if plan.get("structure") != "EQUITY_SPOT":
        raise IBAdapterError("ONLY_EQUITY_SPOT_SUPPORTED")
    sym = plan.get("symbol")
    ccy = plan.get("currency")
    act = plan.get("action")
    qty = plan.get("qty_shares")
    if not isinstance(sym, str) or not sym.strip():
        raise IBAdapterError("EQUITY_SYMBOL_INVALID")
    if not isinstance(ccy, str) or len(ccy) != 3:
        raise IBAdapterError("EQUITY_CURRENCY_INVALID")
    if act not in ("BUY", "SELL"):
        raise IBAdapterError("EQUITY_ACTION_INVALID")
    if not isinstance(qty, int) or qty <= 0:
        raise IBAdapterError("EQUITY_QTY_INVALID")
    terms = plan.get("order_terms")
    if not isinstance(terms, dict):
        raise IBAdapterError("EQUITY_ORDER_TERMS_MISSING")
    ot = terms.get("order_type")
    tif = terms.get("time_in_force")
    lp = terms.get("limit_price")
    if ot not in ("LIMIT", "MARKET"):
        raise IBAdapterError("EQUITY_ORDER_TYPE_INVALID")
    if tif not in ("DAY", "GTC"):
        raise IBAdapterError("EQUITY_TIF_INVALID")
    if ot == "LIMIT":
        if not isinstance(lp, str) or not lp.strip():
            raise IBAdapterError("EQUITY_LIMIT_PRICE_REQUIRED_FOR_LIMIT")
    else:
        if lp is not None:
            raise IBAdapterError("EQUITY_LIMIT_PRICE_MUST_BE_NULL_FOR_MARKET")
    return plan


def _build_ib_bag_and_order_from_order_plan_v1(order_plan: Dict[str, Any]) -> Tuple[Any, Any, Dict[str, Any]]:
    """
    Build ib_insync Contract (BAG) + Order from OrderPlan.v1.

    Deterministic constant routing:
    - exchange = "SMART" (explicit; schema does not provide routing fields)

    Returns:
      (contract, order, raw_payload_dict_json_safe)
    """
    try:
        from ib_insync import Contract, ComboLeg, Order  # type: ignore
    except Exception as e:  # noqa: BLE001
        raise IBAdapterError("C2_BROKER_ADAPTER_NOT_AVAILABLE: ib_insync import failed") from e

    op, l0, l1 = _order_plan_require_vertical_v1(order_plan)

    def _leg(leg: Dict[str, Any]) -> Tuple[int, str]:
        conid = leg.get("ib_conId")
        act = leg.get("action")
        if not isinstance(conid, int) or conid <= 0:
            raise IBAdapterError("LEG_CONID_INVALID")
        if act not in ("BUY", "SELL"):
            raise IBAdapterError("LEG_ACTION_INVALID")
        return int(conid), str(act)

    con0, act0 = _leg(l0)
    con1, act1 = _leg(l1)

    acts = sorted([act0, act1])
    if acts != ["BUY", "SELL"]:
        raise IBAdapterError("C2_DEFINED_RISK_REQUIRED: expected BUY/SELL pair")

    terms = op.get("order_terms")
    if not isinstance(terms, dict):
        raise IBAdapterError("ORDER_TERMS_MISSING")
    if terms.get("order_type") != "LIMIT":
        raise IBAdapterError("ONLY_LIMIT_SUPPORTED")

    tif = terms.get("time_in_force")
    if tif not in ("DAY", "GTC"):
        raise IBAdapterError("TIME_IN_FORCE_INVALID")

    limit_price_s = terms.get("limit_price")
    if not isinstance(limit_price_s, str) or not limit_price_s:
        raise IBAdapterError("LIMIT_PRICE_INVALID")
    try:
        limit_price_f = float(str(Decimal(limit_price_s)))
    except Exception as e:  # noqa: BLE001
        raise IBAdapterError("LIMIT_PRICE_PARSE_FAILED") from e

    contracts = 1
    rp = op.get("risk_proof")
    if isinstance(rp, dict) and isinstance(rp.get("contracts"), int) and int(rp["contracts"]) > 0:
        contracts = int(rp["contracts"])

    exchange = "SMART"

    bag = Contract(secType="BAG", symbol=str(op["underlying"]["symbol"]), currency=str(op["underlying"]["currency"]), exchange=exchange)

    legA = ComboLeg(conId=con0, ratio=1, action=act0, exchange=exchange)
    legB = ComboLeg(conId=con1, ratio=1, action=act1, exchange=exchange)
    bag.comboLegs = [legA, legB]

    order = Order()
    order.action = "BUY"  # Combo order action convention; legs encode BUY/SELL
    order.orderType = "LMT"
    order.totalQuantity = contracts
    order.lmtPrice = limit_price_f
    order.tif = tif

    raw = {
        "format": "IB_BAG_ORDER_V1",
        "routing": {"exchange": exchange},
        "bag": {
            "symbol": str(op["underlying"]["symbol"]),
            "currency": str(op["underlying"]["currency"]),
            "secType": "BAG",
            "exchange": exchange,
            "legs": [
                {"conId": con0, "ratio": 1, "action": act0, "exchange": exchange},
                {"conId": con1, "ratio": 1, "action": act1, "exchange": exchange},
            ],
        },
        "order": {
            "orderType": "LMT",
            "tif": tif,
            "totalQuantity": contracts,
            "lmtPrice": str(Decimal(limit_price_s)),
        },
    }

    return bag, order, raw


def _build_ib_stock_and_order_from_equity_order_plan_v1(plan: Dict[str, Any]) -> Tuple[Any, Any, Dict[str, Any]]:
    """
    Build ib_insync Stock contract + Order from EquityOrderPlan.v1.

    Deterministic constant routing:
    - exchange = "SMART"
    - primaryExchange not specified (schema has none)

    Returns:
      (contract, order, raw_payload_dict_json_safe)
    """
    try:
        from ib_insync import Stock, Order  # type: ignore
    except Exception as e:  # noqa: BLE001
        raise IBAdapterError("C2_BROKER_ADAPTER_NOT_AVAILABLE: ib_insync import failed") from e

    ep = _equity_plan_require_v1(plan)

    exchange = "SMART"
    sym = str(ep["symbol"]).strip()
    ccy = str(ep["currency"]).strip()

    contract = Stock(sym, exchange, ccy)

    terms = ep["order_terms"]
    ot = terms["order_type"]
    tif = terms["time_in_force"]

    order = Order()
    order.action = str(ep["action"])
    order.totalQuantity = int(ep["qty_shares"])
    order.tif = tif

    if ot == "MARKET":
        order.orderType = "MKT"
        limit_price_s = None
    else:
        order.orderType = "LMT"
        limit_price_s = str(terms["limit_price"])
        try:
            order.lmtPrice = float(str(Decimal(limit_price_s)))
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError("EQUITY_LIMIT_PRICE_PARSE_FAILED") from e

    raw = {
        "format": "IB_STK_ORDER_V1",
        "routing": {"exchange": exchange},
        "contract": {"secType": "STK", "symbol": sym, "currency": ccy, "exchange": exchange},
        "order": {
            "orderType": "MKT" if ot == "MARKET" else "LMT",
            "tif": tif,
            "action": str(ep["action"]),
            "totalQuantity": int(ep["qty_shares"]),
            "lmtPrice": None if limit_price_s is None else str(Decimal(limit_price_s)),
        },
    }

    return contract, order, raw


@dataclass
class IBPaperAdapterV1(BrokerAdapterV1):
    """
    Real IB adapter, PAPER-only, ib_insync backend.

    Construct with explicit connection parameters.
    """

    conn: BrokerConnectionSpec
    env: str  # must be "PAPER"

    def __post_init__(self) -> None:
        if self.env != "PAPER":
            raise IBAdapterError("C2_BROKER_ENV_NOT_PAPER: Phase D forbids LIVE broker mode")
        if not isinstance(self.conn.port, int) or self.conn.port <= 0 or self.conn.port > 65535:
            raise IBAdapterError(f"INVALID_PORT: {self.conn.port!r}")
        if not isinstance(self.conn.client_id, int) or self.conn.client_id < 0 or self.conn.client_id > 2_000_000_000:
            raise IBAdapterError(f"INVALID_CLIENT_ID: {self.conn.client_id!r}")
        self._ib = None  # created on connect()

    def broker_name(self) -> str:
        return "INTERACTIVE_BROKERS"

    def broker_env(self) -> str:
        return self.env

    def connect(self) -> None:
        try:
            from ib_insync import IB  # type: ignore
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError("C2_BROKER_ADAPTER_NOT_AVAILABLE: ib_insync import failed") from e

        ib = IB()
        try:
            ok = ib.connect(self.conn.host, int(self.conn.port), clientId=int(self.conn.client_id))
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError(f"BROKER_CONNECT_FAILED: {self.conn.host}:{self.conn.port} client_id={self.conn.client_id}: {e}") from e
        if not ok:
            raise IBAdapterError(f"BROKER_CONNECT_FAILED: {self.conn.host}:{self.conn.port} client_id={self.conn.client_id}")
        self._ib = ib

    def disconnect(self) -> None:
        try:
            if self._ib is not None:
                self._ib.disconnect()
        finally:
            self._ib = None

    def _require_connected(self) -> Any:
        if self._ib is None:
            raise IBAdapterError("BROKER_NOT_CONNECTED")
        return self._ib

    def whatif_order(self, *, order_plan: Dict[str, Any]) -> BrokerWhatIfResult:
        """
        Deterministic WhatIf precheck:
        - Builds BAG+LMT order from OrderPlan OR STK order from EquityOrderPlan
        - Calls whatIfOrder
        - Normalizes margin deltas to decimal strings
        """
        ib = self._require_connected()

        schema_id = str(order_plan.get("schema_id") or "").strip()

        if schema_id == "order_plan":
            contract, order, raw_payload = _build_ib_bag_and_order_from_order_plan_v1(order_plan)
        elif schema_id == "equity_order_plan":
            contract, order, raw_payload = _build_ib_stock_and_order_from_equity_order_plan_v1(order_plan)
        else:
            raise IBAdapterError(f"UNSUPPORTED_PLAN_SCHEMA_ID: {schema_id!r}")

        order.whatIf = True

        try:
            trade = ib.whatIfOrder(contract, order)
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError(f"WHATIF_CALL_FAILED: {e}") from e

        st = getattr(trade, "orderState", None)
        init_change = None
        maint_change = None
        try:
            if st is not None:
                init_change = getattr(st, "initMarginChange", None)
                maint_change = getattr(st, "maintMarginChange", None)
        except Exception:  # noqa: BLE001
            pass

        margin_change = maint_change if maint_change not in (None, "", "0") else init_change
        margin_change_s = _dec_str_from_any(margin_change, "marginChange")

        notional_s = "0"
        rp = order_plan.get("risk_proof")
        if isinstance(rp, dict):
            ml = rp.get("max_loss_usd")
            if isinstance(ml, str) and ml.strip():
                notional_s = str(Decimal(ml.strip()))
        if schema_id == "equity_order_plan":
            # crude deterministic proxy: qty_shares * limit_price if limit, else 0
            try:
                qty = int(order_plan.get("qty_shares") or 0)
                terms = order_plan.get("order_terms") if isinstance(order_plan.get("order_terms"), dict) else {}
                if isinstance(terms, dict) and terms.get("order_type") == "LIMIT":
                    lp = terms.get("limit_price")
                    if isinstance(lp, str) and lp.strip():
                        notional_s = str(Decimal(lp.strip()) * Decimal(qty))
            except Exception:
                pass

        return BrokerWhatIfResult(
            ok=True,
            margin_change_usd=margin_change_s,
            notional_usd=str(Decimal(notional_s)),
            raw_payload=raw_payload,
        )

    def submit_order(self, *, order_plan: Dict[str, Any]) -> BrokerSubmitResult:
        """
        Submit order to IB (PAPER only).
        """
        ib = self._require_connected()

        schema_id = str(order_plan.get("schema_id") or "").strip()

        if schema_id == "order_plan":
            contract, order, _raw_payload = _build_ib_bag_and_order_from_order_plan_v1(order_plan)
        elif schema_id == "equity_order_plan":
            contract, order, _raw_payload = _build_ib_stock_and_order_from_equity_order_plan_v1(order_plan)
        else:
            raise IBAdapterError(f"UNSUPPORTED_PLAN_SCHEMA_ID: {schema_id!r}")

        try:
            trade = ib.placeOrder(contract, order)
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError(f"SUBMIT_CALL_FAILED: {e}") from e

        oid = None
        pid = None
        try:
            oid = getattr(getattr(trade, "order", None), "orderId", None)
        except Exception:
            oid = None
        try:
            pid = getattr(getattr(trade, "order", None), "permId", None)
        except Exception:
            pid = None

        status = "SUBMITTED"
        try:
            st = getattr(trade, "orderStatus", None)
            s = getattr(st, "status", None) if st is not None else None
            if isinstance(s, str) and s.strip():
                status = s.strip().upper()
        except Exception:
            pass

        # Best-effort normalization to known surface
        if status not in ("SUBMITTED", "ACKNOWLEDGED", "REJECTED", "CANCELLED", "UNKNOWN"):
            status = "UNKNOWN"

        ok = True
        err_code = None
        err_msg = None

        # If orderId is missing, treat as non-ok submission
        if oid is None or pid is None:
            ok = False
            err_code = "BROKER_REJECTED"
            err_msg = "Missing broker order identifiers"

        return BrokerSubmitResult(
            ok=ok,
            status=status,
            order_id=int(oid) if isinstance(oid, int) else None,
            perm_id=int(pid) if isinstance(pid, int) else None,
            error_code=err_code,
            error_message=err_msg,
        )
