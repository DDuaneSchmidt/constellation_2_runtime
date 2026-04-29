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


_IB_INVALID_MARGIN_THRESHOLD = Decimal("1E308")


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


def _decimal_from_any_or_none(x: Any, name: str) -> Optional[Decimal]:
    if x in (None, ""):
        return None
    try:
        if isinstance(x, bool):
            raise IBAdapterError(f"NUMERIC_BOOL_FORBIDDEN: {name}")
        return Decimal(str(x))
    except (InvalidOperation, ValueError) as e:
        raise IBAdapterError(f"NUMERIC_NORMALIZATION_FAILED: {name}={x!r}") from e


def _is_invalid_margin_value(value: Optional[Decimal]) -> bool:
    if value is None:
        return True
    if not value.is_finite():
        return True
    return value.copy_abs() >= _IB_INVALID_MARGIN_THRESHOLD


def _options_defined_risk_margin_fallback(order_plan: Dict[str, Any]) -> Optional[Decimal]:
    rp = order_plan.get("risk_proof")
    if not isinstance(rp, dict):
        return None
    if rp.get("defined_risk_proven") is not True:
        return None
    max_loss_usd = rp.get("max_loss_usd")
    if not isinstance(max_loss_usd, str) or not max_loss_usd.strip():
        return None
    try:
        value = Decimal(max_loss_usd.strip())
    except Exception as e:  # noqa: BLE001
        raise IBAdapterError("WHATIF_NOTIONAL_PARSE_FAILED") from e
    if value <= 0:
        raise IBAdapterError("WHATIF_INVALID_DEFINED_RISK_MAX_LOSS")
    return value


def _select_margin_change(result: Any) -> Tuple[Optional[Decimal], Dict[str, Optional[str]]]:
    init_raw = getattr(result, "initMarginChange", None)
    maint_raw = getattr(result, "maintMarginChange", None)
    init_margin = _decimal_from_any_or_none(init_raw, "initMarginChange")
    maint_margin = _decimal_from_any_or_none(maint_raw, "maintMarginChange")
    preferred = maint_margin if maint_margin not in (None, Decimal("0")) else init_margin
    return preferred, {
        "initMarginChange": None if init_raw is None else str(init_raw),
        "maintMarginChange": None if maint_raw is None else str(maint_raw),
    }


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

    stop = plan.get("protective_stop")
    if not isinstance(stop, dict):
        raise IBAdapterError("PROTECTIVE_STOP_REQUIRED_BUT_MISSING: protective_stop missing")
    stop_order_type = str(stop.get("order_type") or "").strip().upper()
    stop_price = stop.get("stop_price")
    stop_tif = str(stop.get("time_in_force") or "").strip().upper()
    stop_basis = str(stop.get("basis") or "").strip().upper()
    stop_loss_bps = stop.get("stop_loss_bps")
    if stop_order_type != "STOP":
        raise IBAdapterError("PROTECTIVE_STOP_REQUIRED_BUT_MISSING: protective_stop.order_type must be STOP")
    if not isinstance(stop_price, str) or not stop_price.strip():
        raise IBAdapterError("PROTECTIVE_STOP_REQUIRED_BUT_MISSING: protective_stop.stop_price missing")
    if stop_tif not in ("DAY", "GTC"):
        raise IBAdapterError("PROTECTIVE_STOP_REQUIRED_BUT_MISSING: protective_stop.time_in_force invalid")
    if stop_basis != "ENTRY_REFERENCE_PRICE":
        raise IBAdapterError("PROTECTIVE_STOP_REQUIRED_BUT_MISSING: protective_stop.basis invalid")
    if not isinstance(stop_loss_bps, int) or stop_loss_bps <= 0:
        raise IBAdapterError("PROTECTIVE_STOP_REQUIRED_BUT_MISSING: protective_stop.stop_loss_bps missing or non-positive")

    return plan


def _options_plan_require_vertical_v1(plan: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    if not isinstance(plan, dict):
        raise IBAdapterError("ORDER_PLAN_NOT_OBJECT")
    if plan.get("schema_id") != "order_plan" or plan.get("schema_version") != "v1":
        raise IBAdapterError("ORDER_PLAN_SCHEMA_MISMATCH")
    if plan.get("structure") != "VERTICAL_SPREAD":
        raise IBAdapterError("ONLY_VERTICAL_SPREAD_SUPPORTED")
    underlying = plan.get("underlying")
    if not isinstance(underlying, dict):
        raise IBAdapterError("ORDER_PLAN_UNDERLYING_INVALID")
    symbol = underlying.get("symbol")
    currency = underlying.get("currency")
    if not isinstance(symbol, str) or not symbol.strip():
        raise IBAdapterError("ORDER_PLAN_UNDERLYING_SYMBOL_INVALID")
    if not isinstance(currency, str) or len(currency.strip()) != 3:
        raise IBAdapterError("ORDER_PLAN_UNDERLYING_CURRENCY_INVALID")
    legs = plan.get("legs")
    if not isinstance(legs, list) or len(legs) != 2:
        raise IBAdapterError("VERTICAL_SPREAD_REQUIRES_EXACTLY_2_LEGS")
    l0 = legs[0]
    l1 = legs[1]
    if not isinstance(l0, dict) or not isinstance(l1, dict):
        raise IBAdapterError("LEG_NOT_OBJECT")
    return plan, l0, l1


def _build_ib_bag_and_order_from_options_plan_v1(order_plan: Dict[str, Any]) -> Tuple[Any, Any, Dict[str, Any]]:
    try:
        from ib_insync import ComboLeg, Contract, Order, TagValue  # type: ignore
    except Exception as e:  # noqa: BLE001
        raise IBAdapterError("C2_BROKER_ADAPTER_NOT_AVAILABLE: ib_insync import failed") from e

    op, l0, l1 = _options_plan_require_vertical_v1(order_plan)

    def _leg(leg: Dict[str, Any]) -> Tuple[int, str]:
        conid = leg.get("ib_conId", leg.get("conId"))
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
    if not isinstance(limit_price_s, str) or not limit_price_s.strip():
        raise IBAdapterError("LIMIT_PRICE_INVALID")
    try:
        limit_price_f = float(str(Decimal(limit_price_s)))
    except Exception as e:  # noqa: BLE001
        raise IBAdapterError("LIMIT_PRICE_PARSE_FAILED") from e

    is_credit = bool(terms.get("is_credit") is True)
    order_action = "BUY"

    contracts = 1
    rp = op.get("risk_proof")
    if isinstance(rp, dict) and isinstance(rp.get("contracts"), int) and int(rp["contracts"]) > 0:
        contracts = int(rp["contracts"])

    underlying = op["underlying"]
    exchange = "SMART"
    bag = Contract(secType="BAG", symbol=str(underlying["symbol"]), currency=str(underlying["currency"]), exchange=exchange)
    leg_a = ComboLeg(conId=con0, ratio=1, action=act0, exchange=exchange)
    leg_b = ComboLeg(conId=con1, ratio=1, action=act1, exchange=exchange)
    bag.comboLegs = [leg_a, leg_b]

    order = Order()
    order.action = order_action
    order.orderType = "LMT"
    order.totalQuantity = contracts
    order.lmtPrice = limit_price_f
    order.tif = str(tif)
    order.smartComboRoutingParams = [TagValue("NonGuaranteed", "1")]

    raw = {
        "format": "IB_BAG_ORDER_V2",
        "routing": {
            "exchange": exchange,
            "smart_combo_routing_params": [{"tag": "NonGuaranteed", "value": "1"}],
            "smart_combo_routing_policy": "NON_GUARANTEED_SMART_COMBO",
        },
        "bag": {
            "symbol": str(underlying["symbol"]),
            "currency": str(underlying["currency"]),
            "secType": "BAG",
            "exchange": exchange,
            "legs": [
                {"conId": con0, "ratio": 1, "action": act0, "exchange": exchange},
                {"conId": con1, "ratio": 1, "action": act1, "exchange": exchange},
            ],
        },
        "order": {
            "action": order_action,
            "combo_action_convention": "BUY_PARENT_LEG_ACTIONS_ENCODE_SPREAD",
            "orderType": "LMT",
            "tif": str(tif),
            "totalQuantity": contracts,
            "limitPrice": str(Decimal(limit_price_s)),
            "isCredit": is_credit,
        },
    }
    return bag, order, raw


def _json_safe_text(value: Any) -> str:
    try:
        return str(value)
    except Exception:
        return repr(value)


def _trade_log_entries(trade: Any) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    raw_log = getattr(trade, "log", None)
    if not isinstance(raw_log, list):
        return entries
    for item in raw_log:
        entries.append(
            {
                "status": _json_safe_text(getattr(item, "status", "")),
                "message": _json_safe_text(getattr(item, "message", "")),
                "errorCode": _json_safe_text(getattr(item, "errorCode", "")),
            }
        )
    return entries


def _order_state_summary(trade: Any) -> dict[str, str]:
    state = getattr(trade, "orderState", None)
    if state is None:
        return {}
    out: dict[str, str] = {}
    for name in ("status", "warningText", "completedStatus", "completedTime", "rejectReason"):
        value = getattr(state, name, None)
        if value not in (None, ""):
            out[name] = _json_safe_text(value)
    return out


def _detect_ib_error_201(*values: Any) -> bool:
    text = " ".join(_json_safe_text(value) for value in values if value is not None).upper()
    return (
        "ERROR 201" in text
        or "ERRORCODE=201" in text
        or "IB_ERROR_201" in text
        or "RISKLESS COMBINATION ORDERS ARE NOT ALLOWED" in text
        or "COMBOPAYOUT" in text
    )


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


def _opposite_action(action: str) -> str:
    upper = str(action).strip().upper()
    if upper == "BUY":
        return "SELL"
    if upper == "SELL":
        return "BUY"
    raise IBAdapterError(f"BRACKET_SUBMISSION_FAILED: unsupported action={action!r}")


def _build_ib_bracket_orders_from_equity_order_plan_v1_or_v2(plan: Dict[str, Any]) -> Tuple[Any, Any, Any, Optional[Any], Dict[str, Any]]:
    try:
        from ib_insync import Stock, Order  # type: ignore
    except Exception as e:  # noqa: BLE001
        raise IBAdapterError("C2_BROKER_ADAPTER_NOT_AVAILABLE: ib_insync import failed") from e

    ep = _equity_plan_require_v1_or_v2(plan)
    exchange = "SMART"
    contract = Stock(symbol=str(ep["symbol"]), currency=str(ep["currency"]), exchange=exchange)

    entry_terms = ep["order_terms"]
    protective_stop = ep["protective_stop"]
    take_profit = ep.get("take_profit") if isinstance(ep.get("take_profit"), dict) else None
    bracket_cfg = ep.get("bracket") if isinstance(ep.get("bracket"), dict) else {}

    entry = Order()
    entry.action = str(ep["action"])
    if entry_terms["order_type"] == "MARKET":
        entry.orderType = "MKT"
        entry.lmtPrice = None
    else:
        entry.orderType = "LMT"
        try:
            entry.lmtPrice = float(str(Decimal(str(entry_terms["limit_price"]))))
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError("BRACKET_SUBMISSION_FAILED: entry limit price parse failed") from e
    entry.totalQuantity = int(ep["qty_shares"])
    entry.tif = str(entry_terms["time_in_force"])
    entry.transmit = False

    stop = Order()
    stop.action = _opposite_action(entry.action)
    stop.orderType = "STP"
    try:
        stop.auxPrice = float(str(Decimal(str(protective_stop["stop_price"]))))
    except Exception as e:  # noqa: BLE001
        raise IBAdapterError("BRACKET_SUBMISSION_FAILED: stop price parse failed") from e
    stop.totalQuantity = int(ep["qty_shares"])
    stop.tif = str(protective_stop["time_in_force"])

    target: Optional[Any] = None
    if take_profit is not None:
        target = Order()
        target.action = _opposite_action(entry.action)
        target.orderType = "LMT"
        try:
            target.lmtPrice = float(str(Decimal(str(take_profit["limit_price"]))))
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError("BRACKET_SUBMISSION_FAILED: take_profit limit price parse failed") from e
        target.totalQuantity = int(ep["qty_shares"])
        target.tif = str(take_profit["time_in_force"])

    if target is None:
        stop.transmit = True
    else:
        stop.transmit = False
        target.transmit = True
        oca_group = str(bracket_cfg.get("oca_group") or "").strip()
        if not oca_group:
            intent_hash = str(ep.get("intent_hash") or "").strip().lower()
            suffix = intent_hash[:16] if intent_hash else f"{str(ep['symbol']).upper()}_{int(ep['qty_shares'])}"
            oca_group = f"C2_BRACKET_{suffix}"
        stop.ocaGroup = oca_group
        target.ocaGroup = oca_group
        stop.ocaType = 1
        target.ocaType = 1

    raw = {
        "format": "IB_STK_BRACKET_V1",
        "routing": {"exchange": exchange},
        "stock": {"symbol": str(ep["symbol"]), "currency": str(ep["currency"]), "secType": "STK", "exchange": exchange},
        "entry_order": {
            "action": str(entry.action),
            "orderType": str(entry.orderType),
            "tif": str(entry.tif),
            "totalQuantity": int(entry.totalQuantity),
            "limitPrice": None if entry.orderType == "MKT" else str(Decimal(str(entry_terms["limit_price"]))),
            "transmit": False,
        },
        "protective_stop_order": {
            "action": str(stop.action),
            "orderType": "STP",
            "auxPrice": str(Decimal(str(protective_stop["stop_price"]))),
            "tif": str(stop.tif),
            "totalQuantity": int(stop.totalQuantity),
            "transmit": bool(stop.transmit),
        },
        "take_profit_order": None,
        "bracket": {
            "enabled": True,
            "transmit_sequence": "PARENT_FALSE_FINAL_CHILD_TRUE",
            "oca_group": None,
        },
    }
    if target is not None:
        raw["take_profit_order"] = {
            "action": str(target.action),
            "orderType": "LMT",
            "limitPrice": str(Decimal(str(take_profit["limit_price"]))),
            "tif": str(target.tif),
            "totalQuantity": int(target.totalQuantity),
            "transmit": bool(target.transmit),
        }
        raw["bracket"]["oca_group"] = str(getattr(stop, "ocaGroup", None) or "")

    return contract, entry, stop, target, raw


def _market_margin_probe_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    probe_plan = dict(plan)
    order_terms = dict(plan.get("order_terms") or {})
    order_terms["order_type"] = "MARKET"
    order_terms["limit_price"] = None
    probe_plan["order_terms"] = order_terms
    return probe_plan


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

        schema_id = str(order_plan.get("schema_id") or "").strip()
        if schema_id == "order_plan":
            contract, order, raw_payload = _build_ib_bag_and_order_from_options_plan_v1(order_plan)
            try:
                res = self._ib.whatIfOrder(contract, order)
            except Exception as e:  # noqa: BLE001
                if _detect_ib_error_201(e):
                    raise IBAdapterError("IB_ERROR_201_RISKLESS_COMBINATION: Riskless combination orders are not allowed.") from e
                raise IBAdapterError(f"WHATIF_FAILED: {e!r}") from e

            margin_change, raw_margin_fields = _select_margin_change(res)
            margin_source = "LIMIT_OR_NATIVE"
            if _is_invalid_margin_value(margin_change):
                fallback_margin = _options_defined_risk_margin_fallback(order_plan)
                if fallback_margin is None:
                    raise IBAdapterError(
                        "WHATIF_INVALID_MARGIN_CHANGE"
                        f": source=LIMIT_OR_NATIVE"
                        f" native={raw_margin_fields!r}"
                        f" market_probe={None!r}"
                    )
                margin_change = fallback_margin
                margin_source = "RISK_PROOF_MAX_LOSS_FALLBACK"
            notional = _options_defined_risk_margin_fallback(order_plan) or Decimal("0")
            return BrokerWhatIfResult(
                ok=True,
                margin_change_usd=_dec_str_from_any(margin_change, "marginChange"),
                notional_usd=_dec_str_from_any(notional, "notional_usd"),
                detail="WHATIF_OK",
                raw={
                    "whatif": str(res),
                    "payload": raw_payload,
                    "margin_source": margin_source,
                    "native_margin_fields": raw_margin_fields,
                    "market_probe_margin_fields": None,
                },
            )
        if schema_id != "equity_order_plan":
            raise IBAdapterError(f"UNSUPPORTED_PLAN_SCHEMA_ID: {schema_id!r}")

        contract, order, raw_payload = _build_ib_stock_and_order_from_equity_order_plan_v1_or_v2(order_plan)

        try:
            res = self._ib.whatIfOrder(contract, order)
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError(f"WHATIF_FAILED: {e!r}") from e

        margin_change, raw_margin_fields = _select_margin_change(res)
        margin_source = "LIMIT_OR_NATIVE"
        market_probe_raw: Optional[Dict[str, Any]] = None

        order_terms = order_plan.get("order_terms") if isinstance(order_plan.get("order_terms"), dict) else {}
        if (
            isinstance(order_terms, dict)
            and order_terms.get("order_type") == "LIMIT"
            and _is_invalid_margin_value(margin_change)
        ):
            probe_plan = _market_margin_probe_plan(order_plan)
            probe_contract, probe_order, probe_raw_payload = _build_ib_stock_and_order_from_equity_order_plan_v1_or_v2(probe_plan)
            try:
                probe_res = self._ib.whatIfOrder(probe_contract, probe_order)
            except Exception as e:  # noqa: BLE001
                raise IBAdapterError(f"WHATIF_FAILED: market_margin_probe={e!r}") from e
            margin_change, market_probe_raw = _select_margin_change(probe_res)
            margin_source = "MARKET_MARGIN_PROBE"
            raw_payload["margin_probe_payload"] = probe_raw_payload

        if _is_invalid_margin_value(margin_change):
            raise IBAdapterError(
                "WHATIF_INVALID_MARGIN_CHANGE"
                f": source={margin_source}"
                f" native={raw_margin_fields!r}"
                f" market_probe={market_probe_raw!r}"
            )

        # there is no stable notional field in whatIfOrder; keep as "0" and include raw summary
        notional = 0

        return BrokerWhatIfResult(
            ok=True,
            margin_change_usd=_dec_str_from_any(margin_change, "marginChange"),
            notional_usd=_dec_str_from_any(notional, "notional_usd"),
            detail="WHATIF_OK",
            raw={
                "whatif": str(res),
                "payload": raw_payload,
                "margin_source": margin_source,
                "native_margin_fields": raw_margin_fields,
                "market_probe_margin_fields": market_probe_raw,
            },
        )

    def submit_order(self, *, order_plan: Dict[str, Any]) -> BrokerSubmitResult:
        if self._ib is None:
            raise IBAdapterError("BROKER_NOT_CONNECTED")

        schema_id = str(order_plan.get("schema_id") or "").strip()
        if schema_id == "order_plan":
            contract, order, raw_payload = _build_ib_bag_and_order_from_options_plan_v1(order_plan)
            try:
                trade = self._ib.placeOrder(contract, order)
                self._ib.sleep(0.2)
            except Exception as e:  # noqa: BLE001
                return BrokerSubmitResult(
                    ok=False,
                    status="REJECTED",
                    order_id=None,
                    perm_id=None,
                    error_code="BAG_SUBMISSION_FAILED",
                    error_message=str(e),
                    raw={"payload": raw_payload},
                )

            status = getattr(getattr(trade, "orderStatus", None), "status", None)
            order_id = getattr(getattr(trade, "order", None), "orderId", None)
            perm_id = getattr(getattr(trade, "order", None), "permId", None)
            st = str(status) if status is not None else "UNKNOWN"
            advanced_error = _json_safe_text(getattr(trade, "advancedError", ""))
            order_state = _order_state_summary(trade)
            trade_log = _trade_log_entries(trade)
            error_201 = _detect_ib_error_201(advanced_error, order_state, trade_log)
            ok = (
                isinstance(order_id, int)
                and isinstance(perm_id, int)
                and not error_201
                and st.upper() in (
                    "PENDINGSUBMIT",
                    "PRESUBMITTED",
                    "SUBMITTED",
                    "APIPENDING",
                    "FILLED",
                    "INACTIVE",
                )
            )
            return BrokerSubmitResult(
                ok=bool(ok),
                status=st.upper(),
                order_id=int(order_id) if isinstance(order_id, int) else None,
                perm_id=int(perm_id) if isinstance(perm_id, int) else None,
                error_code=None if ok else ("IB_ERROR_201" if error_201 else "BAG_SUBMISSION_FAILED"),
                error_message=None if ok else (
                    "Riskless combination orders are not allowed." if error_201 else f"status={st}"
                ),
                raw={
                    "trade": str(trade),
                    "payload": raw_payload,
                    "advanced_error": advanced_error,
                    "order_state": order_state,
                    "trade_log": trade_log,
                    "ib_error_201_detected": bool(error_201),
                    "broker_ids": {
                        "parent_order_id": int(order_id) if isinstance(order_id, int) else None,
                        "parent_perm_id": int(perm_id) if isinstance(perm_id, int) else None,
                        "stop_order_id": None,
                        "stop_perm_id": None,
                        "take_profit_order_id": None,
                        "take_profit_perm_id": None,
                    },
                },
            )
        if schema_id != "equity_order_plan":
            raise IBAdapterError(f"UNSUPPORTED_PLAN_SCHEMA_ID: {schema_id!r}")

        contract, parent_order, stop_order, take_profit_order, raw_payload = _build_ib_bracket_orders_from_equity_order_plan_v1_or_v2(order_plan)

        try:
            parent_trade = self._ib.placeOrder(contract, parent_order)
            self._ib.sleep(0.2)
            parent_order_id = getattr(getattr(parent_trade, "order", None), "orderId", None)
            if not isinstance(parent_order_id, int):
                raise IBAdapterError("BRACKET_SUBMISSION_FAILED: parent order_id missing")

            stop_order.parentId = int(parent_order_id)
            stop_trade = self._ib.placeOrder(contract, stop_order)
            self._ib.sleep(0.2)

            target_trade = None
            if take_profit_order is not None:
                take_profit_order.parentId = int(parent_order_id)
                target_trade = self._ib.placeOrder(contract, take_profit_order)
                self._ib.sleep(0.2)
        except Exception as e:  # noqa: BLE001
            return BrokerSubmitResult(
                ok=False,
                status="REJECTED",
                order_id=None,
                perm_id=None,
                error_code="BRACKET_SUBMISSION_FAILED",
                error_message=str(e),
                raw={"payload": raw_payload},
            )

        status = (
            getattr(getattr(parent_trade, "orderStatus", None), "status", None)
            or getattr(getattr(stop_trade, "orderStatus", None), "status", None)
            or getattr(getattr(target_trade, "orderStatus", None), "status", None)
        )
        parent_order_id = getattr(getattr(parent_trade, "order", None), "orderId", None)
        parent_perm_id = getattr(getattr(parent_trade, "order", None), "permId", None)
        stop_order_id = getattr(getattr(stop_trade, "order", None), "orderId", None)
        stop_perm_id = getattr(getattr(stop_trade, "order", None), "permId", None)
        take_profit_order_id = getattr(getattr(target_trade, "order", None), "orderId", None) if target_trade is not None else None
        take_profit_perm_id = getattr(getattr(target_trade, "order", None), "permId", None) if target_trade is not None else None

        st = str(status) if status is not None else "UNKNOWN"
        ok = (
            isinstance(parent_order_id, int)
            and isinstance(stop_order_id, int)
            and st.upper() in ("SUBMITTED", "PRESUBMITTED", "FILLED", "INACTIVE")
        )

        return BrokerSubmitResult(
            ok=bool(ok),
            status=st.upper(),
            order_id=int(parent_order_id) if isinstance(parent_order_id, int) else None,
            perm_id=int(parent_perm_id) if isinstance(parent_perm_id, int) else None,
            error_code=None if ok else "BRACKET_SUBMISSION_FAILED",
            error_message=None if ok else f"status={st}",
            raw={
                "parent_trade": str(parent_trade),
                "stop_trade": str(stop_trade),
                "take_profit_trade": None if target_trade is None else str(target_trade),
                "payload": raw_payload,
                "broker_ids": {
                    "parent_order_id": int(parent_order_id) if isinstance(parent_order_id, int) else None,
                    "parent_perm_id": int(parent_perm_id) if isinstance(parent_perm_id, int) else None,
                    "stop_order_id": int(stop_order_id) if isinstance(stop_order_id, int) else None,
                    "stop_perm_id": int(stop_perm_id) if isinstance(stop_perm_id, int) else None,
                    "take_profit_order_id": int(take_profit_order_id) if isinstance(take_profit_order_id, int) else None,
                    "take_profit_perm_id": int(take_profit_perm_id) if isinstance(take_profit_perm_id, int) else None,
                },
            },
        )

    def cancel_order(self, *, order_id: int) -> BrokerSubmitResult:
        if not isinstance(order_id, int) or order_id <= 0:
            raise IBAdapterError(f"CANCEL_ORDER_ID_INVALID: {order_id!r}")
        if self._ib is None:
            raise IBAdapterError("BROKER_NOT_CONNECTED")
        try:
            from ib_insync import Order
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError(f"IB_INSYNC_IMPORT_FAILED: {e}") from e

        order = Order()
        order.orderId = int(order_id)
        try:
            trade = self._ib.cancelOrder(order)
            if hasattr(self._ib, "sleep"):
                self._ib.sleep(0.5)
        except Exception as e:  # noqa: BLE001
            raise IBAdapterError(f"CANCEL_CALL_FAILED: {e}") from e

        status = "CANCELLED"
        try:
            raw_status = getattr(getattr(trade, "orderStatus", None), "status", None)
            if isinstance(raw_status, str) and raw_status.strip():
                status = raw_status.strip().upper()
        except Exception:
            status = "CANCELLED"

        ok = status in {"CANCELLED", "PENDINGCANCEL", "APICANCELLED", "API_CANCELLED"}
        return BrokerSubmitResult(
            ok=ok,
            status=status,
            order_id=int(order_id),
            perm_id=None,
            error_code=None if ok else "CANCEL_NOT_ACKNOWLEDGED",
            error_message=None if ok else f"cancel status={status}",
            raw={
                "cancel_order_id": int(order_id),
                "trade": str(trade),
                "status": status,
            },
        )
