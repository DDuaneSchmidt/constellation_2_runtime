"""
ib_paper_adapter_v2.py

Constellation 2.0 Phase D
Interactive Brokers PAPER adapter v2 using ib_insync.

Change from v1:
- Equity order plans accept schema_version in ("v1","v2").
- v2 plans may include extra lineage fields; they are ignored by the adapter.
- Returns BrokerWhatIfResult / BrokerSubmitResult with required fields (ok/detail/error_code/error_message).

Hard rules:
- PAPER only.
- No floats cross the adapter boundary: normalize outputs to decimal strings.
- Any broker connection failure must raise (submit boundary converts to VetoRecord).
"""

from __future__ import annotations

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


def _equity_plan_require_v1_or_v2(plan: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(plan, dict):
        raise IBAdapterError("EQUITY_ORDER_PLAN_NOT_OBJECT")
    if plan.get("schema_id") != "equity_order_plan":
        raise IBAdapterError("EQUITY_ORDER_PLAN_SCHEMA_MISMATCH")
    sv = plan.get("schema_version")
    if sv not in ("v1", "v2"):
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


def _build_ib_stock_and_order_from_equity_order_plan_v1_or_v2(plan: Dict[str, Any]) -> Tuple[Any, Any, Dict[str, Any]]:
    try:
        from ib_insync import Stock, Order  # type: ignore
    except Exception as e:  # noqa: BLE001
        raise IBAdapterError("C2_BROKER_ADAPTER_NOT_AVAILABLE: ib_insync import failed") from e

    ep = _equity_plan_require_v1_or_v2(plan)
    exchange = "SMART"

    contract = Stock(symbol=str(ep["symbol"]), currency=str(ep["currency"]), exchange=exchange)

    order = Order()
    order.action = str(ep["action"])
    terms = ep["order_terms"]
    if terms["order_type"] == "MARKET":
        order.orderType = "MKT"
        order.lmtPrice = None
    else:
        order.orderType = "LMT"
        try:
            order.lmtPrice = float(str(Decimal(str(terms["limit_price"]))))
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError("EQUITY_LIMIT_PRICE_PARSE_FAILED") from e

    order.totalQuantity = int(ep["qty_shares"])
    order.tif = str(terms["time_in_force"])

    raw = {
        "format": "IB_STK_ORDER_V1",
        "routing": {"exchange": exchange},
        "stock": {"symbol": str(ep["symbol"]), "currency": str(ep["currency"]), "secType": "STK", "exchange": exchange},
        "order": {
            "action": str(ep["action"]),
            "orderType": str(terms["order_type"]),
            "tif": str(terms["time_in_force"]),
            "totalQuantity": int(ep["qty_shares"]),
            "limitPrice": str(Decimal(str(terms["limit_price"]))) if terms["order_type"] == "LIMIT" else None,
        },
    }
    return contract, order, raw


class IBPaperAdapterV2(BrokerAdapterV1):
    def __init__(self, *, conn: BrokerConnectionSpec, env: str) -> None:
        self.conn = conn
        self.env = str(env).strip().upper()
        self._ib = None

    def broker_name(self) -> str:
        return "INTERACTIVE_BROKERS"

    def broker_env(self) -> str:
        return self.env

    def connect(self) -> None:
        if self.env != "PAPER":
            raise IBAdapterError("C2_BROKER_ENV_NOT_PAPER")
        try:
            from ib_insync import IB  # type: ignore
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError("C2_BROKER_ADAPTER_NOT_AVAILABLE: ib_insync import failed") from e

        ib = IB()
        try:
            ok = ib.connect(self.conn.host, int(self.conn.port), clientId=int(self.conn.client_id))
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError(f"BROKER_CONNECT_FAILED: {self.conn.host}:{self.conn.port} client_id={self.conn.client_id}: {e!r}") from e

        if not ok:
            raise IBAdapterError(f"BROKER_CONNECT_FAILED: {self.conn.host}:{self.conn.port} client_id={self.conn.client_id}")
        self._ib = ib

    def disconnect(self) -> None:
        if self._ib is not None:
            try:
                self._ib.disconnect()
            except Exception:
                pass
        self._ib = None

    def whatif_order(self, *, order_plan: Dict[str, Any]) -> BrokerWhatIfResult:
        if self._ib is None:
            raise IBAdapterError("BROKER_NOT_CONNECTED")

        if order_plan.get("schema_id") == "order_plan":
            raise IBAdapterError("OPTIONS_NOT_SUPPORTED_IN_ADAPTER_V2")

        contract, order, raw_payload = _build_ib_stock_and_order_from_equity_order_plan_v1_or_v2(order_plan)

        try:
            res = self._ib.whatIfOrder(contract, order)
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError(f"WHATIF_FAILED: {e!r}") from e

        # ib_insync WhatIfOrder fields vary; keep deterministic best-effort.
        margin_change = getattr(res, "initMarginChange", 0)
        # there is no stable notional field in whatIfOrder; keep as "0" and include raw summary
        notional = 0

        return BrokerWhatIfResult(
            ok=True,
            margin_change_usd=_dec_str_from_any(margin_change, "initMarginChange"),
            notional_usd=_dec_str_from_any(notional, "notional_usd"),
            detail="WHATIF_OK",
            raw={"whatif": str(res), "payload": raw_payload},
        )

    def submit_order(self, *, order_plan: Dict[str, Any]) -> BrokerSubmitResult:
        if self._ib is None:
            raise IBAdapterError("BROKER_NOT_CONNECTED")

        if order_plan.get("schema_id") == "order_plan":
            raise IBAdapterError("OPTIONS_NOT_SUPPORTED_IN_ADAPTER_V2")

        contract, order, raw_payload = _build_ib_stock_and_order_from_equity_order_plan_v1_or_v2(order_plan)

        try:
            trade = self._ib.placeOrder(contract, order)
            self._ib.sleep(0.2)
        except Exception as e:  # noqa: BLE001
            return BrokerSubmitResult(
                ok=False,
                status="REJECTED",
                order_id=None,
                perm_id=None,
                error_code="SUBMIT_FAILED",
                error_message=str(e),
                raw={"payload": raw_payload},
            )

        status = getattr(getattr(trade, "orderStatus", None), "status", None)
        order_id = getattr(getattr(trade, "order", None), "orderId", None)
        perm_id = getattr(getattr(trade, "order", None), "permId", None)

        st = str(status) if status is not None else "UNKNOWN"
        ok = st.upper() in ("SUBMITTED", "PRESUBMITTED", "FILLED", "INACTIVE")

        return BrokerSubmitResult(
            ok=bool(ok),
            status=st.upper(),
            order_id=int(order_id) if isinstance(order_id, int) else None,
            perm_id=int(perm_id) if isinstance(perm_id, int) else None,
            error_code=None if ok else "UNKNOWN_STATUS",
            error_message=None if ok else f"status={st}",
            raw={"trade": str(trade), "payload": raw_payload},
        )

    def cancel_order(self, *, order_id: int) -> BrokerSubmitResult:
        raise IBAdapterError("CANCEL_NOT_SUPPORTED_IN_ADAPTER_V2")
