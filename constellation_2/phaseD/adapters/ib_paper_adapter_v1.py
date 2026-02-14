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

    # Legs: require BUY/SELL and conId present
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
    # Convert to float only at the last moment (IB API requirement); we keep deterministic string in raw payload.
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

    # Deterministic raw payload (JSON-safe, no floats)
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
            "lmtPrice": str(Decimal(limit_price_s)),  # string, not float
        },
    }

    return bag, order, raw


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
        - Builds BAG + LMT order from OrderPlan
        - Calls whatIfOrder
        - Normalizes margin deltas to decimal strings
        """
        ib = self._require_connected()

        contract, order, raw_payload = _build_ib_bag_and_order_from_order_plan_v1(order_plan)
        order.whatIf = True

        try:
            trade = ib.whatIfOrder(contract, order)
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError(f"WHATIF_CALL_FAILED: {e}") from e

        # IB returns OrderState values (often strings); capture safely.
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

        # Use max_loss_usd as a deterministic proxy for notional if present (schema carries it as string).
        notional_s = "0"
        rp = order_plan.get("risk_proof")
        if isinstance(rp, dict) and isinstance(rp.get("max_loss_usd"), str) and rp["max_loss_usd"]:
            notional_s = _dec_str_from_any(rp["max_loss_usd"], "risk_proof.max_loss_usd")

        return BrokerWhatIfResult(
            ok=True,
            margin_change_usd=margin_change_s,
            notional_usd=notional_s,
            detail="OK",
            raw={
                "raw_payload": raw_payload,
                "initMarginChange": str(init_change) if init_change is not None else None,
                "maintMarginChange": str(maint_change) if maint_change is not None else None,
            },
        )

    def submit_order(self, *, order_plan: Dict[str, Any]) -> BrokerSubmitResult:
        """
        Submit BAG order (PAPER only):
        - Builds BAG + LMT order from OrderPlan
        - Calls placeOrder
        - Returns broker ids + normalized status
        """
        ib = self._require_connected()

        contract, order, raw_payload = _build_ib_bag_and_order_from_order_plan_v1(order_plan)
        order.whatIf = False

        try:
            trade = ib.placeOrder(contract, order)
        except Exception as e:  # noqa: BLE001
            return BrokerSubmitResult(
                ok=False,
                status="REJECTED",
                order_id=None,
                perm_id=None,
                error_code="BROKER_PLACE_ORDER_FAILED",
                error_message=str(e),
                raw={"raw_payload": raw_payload},
            )

        # Try to extract ids/status deterministically
        oid = None
        pid = None
        status = "UNKNOWN"
        try:
            os_ = getattr(trade, "orderStatus", None)
            if os_ is not None:
                status = str(getattr(os_, "status", "UNKNOWN") or "UNKNOWN").upper()
                oid_val = getattr(os_, "orderId", None)
                pid_val = getattr(os_, "permId", None)
                if isinstance(oid_val, int):
                    oid = oid_val
                if isinstance(pid_val, int):
                    pid = pid_val
        except Exception:  # noqa: BLE001
            pass

        # Normalize status to schema enums
        norm = status
        if norm not in ("SUBMITTED", "ACKNOWLEDGED", "REJECTED", "CANCELLED", "UNKNOWN"):
            # Common IB statuses: Submitted, PreSubmitted, Filled, Cancelled, ApiCancelled, Inactive
            s = norm
            if "SUBMITTED" in s:
                norm = "SUBMITTED"
            elif "PRESUBMITTED" in s:
                norm = "ACKNOWLEDGED"
            elif "CANCEL" in s:
                norm = "CANCELLED"
            elif "INACTIVE" in s or "REJECT" in s:
                norm = "REJECTED"
            else:
                norm = "UNKNOWN"

        return BrokerSubmitResult(
            ok=(norm in ("SUBMITTED", "ACKNOWLEDGED")),
            status=norm,
            order_id=oid,
            perm_id=pid,
            error_code=None,
            error_message=None,
            raw={"raw_payload": raw_payload, "ib_status_raw": status},
        )

    def cancel_order(self, *, order_id: int) -> BrokerSubmitResult:
        """
        Deterministic cancellation requires an Order object; without stored trade/order object, refuse.
        """
        _ = self._require_connected()
        return BrokerSubmitResult(
            ok=False,
            status="UNKNOWN",
            order_id=int(order_id),
            perm_id=None,
            error_code="C2_SUBMIT_FAIL_CLOSED_REQUIRED",
            error_message="CANCEL_NOT_IMPLEMENTED: deterministic cancellation requires stored broker order object",
            raw=None,
        )
