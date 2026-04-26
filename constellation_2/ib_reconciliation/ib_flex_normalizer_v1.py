from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from .paths_v1 import validate_day_utc_v1
from .schema_v1 import (
    NORMALIZED_IB_CASH_SCHEMA_VERSION,
    NORMALIZED_IB_POSITIONS_SCHEMA_VERSION,
    NORMALIZED_IB_TRADES_SCHEMA_VERSION,
    SchemaValidationError,
    decimal_to_str_v1,
    validate_normalized_ib_cash_v1,
    validate_normalized_ib_positions_v1,
    validate_normalized_ib_trades_v1,
)


class IBFlexNormalizationError(RuntimeError):
    pass


def _coerce_text(attrs: dict[str, str], *keys: str, default: str = "") -> str:
    for key in keys:
        value = attrs.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return str(default)


def _normalize_side(value: str) -> str:
    text = str(value or "").strip().upper()
    if text in {"BOT", "BUY", "B"}:
        return "BUY"
    if text in {"SLD", "SELL", "S"}:
        return "SELL"
    raise IBFlexNormalizationError(f"TRADE_SIDE_INVALID:{value!r}")


def _to_utc_iso(value: str, *, fallback_day_utc: str) -> str:
    text = str(value or "").strip()
    if not text:
        return f"{fallback_day_utc}T00:00:00Z"
    text = text.replace("/", "-")
    text = text.replace(" UTC", "Z")
    text = text.replace(" ", "T")
    if text.endswith("Z"):
        iso_candidate = text[:-1] + "+00:00"
    elif "+" in text[10:] or text.count("-") > 2:
        iso_candidate = text
    else:
        iso_candidate = text + "+00:00"
    try:
        parsed = datetime.fromisoformat(iso_candidate)
    except ValueError:
        # Handle compact timestamp forms like YYYYMMDD;HHMMSS.
        compact = text.replace(";", "").replace(":", "").replace("T", "")
        if len(compact) >= 14 and compact[:8].isdigit():
            try:
                parsed = datetime.strptime(compact[:14], "%Y%m%d%H%M%S")
                parsed = parsed.replace(tzinfo=UTC)
            except ValueError as exc:
                raise IBFlexNormalizationError(f"TRADE_TIME_INVALID:{value!r}") from exc
        else:
            raise IBFlexNormalizationError(f"TRADE_TIME_INVALID:{value!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    parsed = parsed.astimezone(UTC).replace(microsecond=0)
    return parsed.isoformat().replace("+00:00", "Z")


def _extract_trade_time(attrs: dict[str, str], *, day_utc: str) -> str:
    direct = _coerce_text(
        attrs,
        "dateTime",
        "dateTimeUTC",
        "dateTimeGMT",
        "tradeTime",
        "transactionTime",
        "time",
        default="",
    )
    if direct:
        return _to_utc_iso(direct, fallback_day_utc=day_utc)
    trade_date = _coerce_text(attrs, "tradeDate", "date", default=day_utc)
    trade_time = _coerce_text(attrs, "tradeTime", "time", default="00:00:00")
    return _to_utc_iso(f"{trade_date} {trade_time}", fallback_day_utc=day_utc)


def _normalized_trade_row(attrs: dict[str, str], *, day_utc: str, idx: int) -> dict[str, Any]:
    symbol = _coerce_text(attrs, "symbol", "underlyingSymbol", "description")
    if not symbol:
        raise IBFlexNormalizationError(f"TRADE_SYMBOL_MISSING:index={idx}")
    side = _normalize_side(_coerce_text(attrs, "buySell", "side", "action"))
    account_id = _coerce_text(attrs, "accountId", "acctId", "account", default="UNKNOWN_ACCOUNT")
    asset_class = _coerce_text(attrs, "assetClass", "assetCategory", "securityType", "secType", default="UNKNOWN")
    quantity = decimal_to_str_v1(
        _coerce_text(attrs, "quantity", "qty", "tradeQuantity", "shares"),
        field=f"trades[{idx}].quantity",
    )
    price = decimal_to_str_v1(_coerce_text(attrs, "price", "tradePrice", "tPrice"), field=f"trades[{idx}].price")
    commission = decimal_to_str_v1(
        _coerce_text(attrs, "ibCommission", "commission", "commissionAmount", default="0"),
        field=f"trades[{idx}].commission",
    )
    ib_order_id = _coerce_text(attrs, "orderID", "orderId", "ibOrderId", default="0")
    ib_perm_id = _coerce_text(attrs, "permID", "permId", "ibPermId", default="0")
    currency = _coerce_text(attrs, "currency", "currencyPrimary", default="USD")
    trade_time_utc = _extract_trade_time(attrs, day_utc=day_utc)
    return {
        "account_id": account_id,
        "symbol": symbol.upper(),
        "asset_class": asset_class.upper(),
        "side": side,
        "quantity": quantity,
        "price": price,
        "currency": currency.upper(),
        "trade_time_utc": trade_time_utc,
        "ib_order_id": ib_order_id,
        "ib_perm_id": ib_perm_id,
        "commission": commission,
        "raw_ref": {
            "tag": "Trade",
            "row_index": idx,
            "attributes": dict(sorted(attrs.items())),
        },
    }


def _normalized_position_row(attrs: dict[str, str], *, idx: int) -> dict[str, Any]:
    symbol = _coerce_text(attrs, "symbol", "underlyingSymbol", "description")
    if not symbol:
        raise IBFlexNormalizationError(f"POSITION_SYMBOL_MISSING:index={idx}")
    return {
        "account_id": _coerce_text(attrs, "accountId", "acctId", "account", default="UNKNOWN_ACCOUNT"),
        "symbol": symbol.upper(),
        "asset_class": _coerce_text(attrs, "assetClass", "assetCategory", "securityType", "secType", default="UNKNOWN").upper(),
        "quantity": decimal_to_str_v1(_coerce_text(attrs, "quantity", "position", "qty"), field=f"positions[{idx}].quantity"),
        "market_price": decimal_to_str_v1(
            _coerce_text(attrs, "markPrice", "marketPrice", "closePrice", default="0"),
            field=f"positions[{idx}].market_price",
        ),
        "market_value": decimal_to_str_v1(
            _coerce_text(attrs, "marketValue", "positionValue", default="0"),
            field=f"positions[{idx}].market_value",
        ),
        "currency": _coerce_text(attrs, "currency", default="USD").upper(),
        "raw_ref": {
            "tag": "OpenPosition",
            "row_index": idx,
            "attributes": dict(sorted(attrs.items())),
        },
    }


def _normalized_cash_row(attrs: dict[str, str], *, idx: int) -> dict[str, Any] | None:
    account = _coerce_text(attrs, "accountId", "acctId", "account", default="UNKNOWN_ACCOUNT")
    currency = _coerce_text(attrs, "currency", default="")
    cash_balance = _coerce_text(
        attrs,
        "cashBalance",
        "endingCash",
        "totalCashBalance",
        "cash",
        "settledCash",
        default="",
    )
    if not currency or not cash_balance:
        return None
    row: dict[str, Any] = {
        "account_id": account,
        "currency": currency.upper(),
        "cash_balance": decimal_to_str_v1(cash_balance, field=f"balances[{idx}].cash_balance"),
        "raw_ref": {
            "tag": "CashBalance",
            "row_index": idx,
            "attributes": dict(sorted(attrs.items())),
        },
    }
    net_liq = _coerce_text(attrs, "netLiquidationValue", "netLiquidation", "netLiq", default="")
    if net_liq:
        row["net_liquidation_value"] = decimal_to_str_v1(net_liq, field=f"balances[{idx}].net_liquidation_value")
    return row


def normalize_ib_flex_xml_v1(day_utc: str, source_report_path: Path) -> dict[str, Any]:
    validate_day_utc_v1(day_utc)
    source = Path(source_report_path).expanduser().resolve()
    if not source.exists() or not source.is_file():
        raise IBFlexNormalizationError(f"IB_FLEX_XML_MISSING:{source}")
    try:
        tree = ET.parse(source)
    except ET.ParseError as exc:
        raise IBFlexNormalizationError(f"IB_FLEX_XML_MALFORMED:{source}") from exc

    root = tree.getroot()
    trade_nodes = list(root.iter("Trade"))
    position_nodes = list(root.iter("OpenPosition"))
    cash_nodes = list(root.iter("CashReportCurrency")) + list(root.iter("CashBalance"))

    warnings: list[str] = []
    if not trade_nodes:
        warnings.append("MISSING_TRADES_SECTION")
    if not position_nodes:
        warnings.append("MISSING_POSITIONS_SECTION")
    if not cash_nodes:
        warnings.append("MISSING_CASH_SECTION")

    trades: list[dict[str, Any]] = []
    for idx, node in enumerate(trade_nodes):
        trades.append(_normalized_trade_row(dict(node.attrib), day_utc=day_utc, idx=idx))
    trades.sort(
        key=lambda row: (
            str(row["trade_time_utc"]),
            str(row["symbol"]),
            str(row["side"]),
            str(row["ib_perm_id"]),
            str(row["ib_order_id"]),
            str(row["quantity"]),
            str(row["price"]),
        )
    )

    positions: list[dict[str, Any]] = []
    for idx, node in enumerate(position_nodes):
        positions.append(_normalized_position_row(dict(node.attrib), idx=idx))
    positions.sort(key=lambda row: (str(row["account_id"]), str(row["symbol"]), str(row["asset_class"])))

    balances: list[dict[str, Any]] = []
    for idx, node in enumerate(cash_nodes):
        row = _normalized_cash_row(dict(node.attrib), idx=idx)
        if row is not None:
            balances.append(row)
    balances.sort(key=lambda row: (str(row["account_id"]), str(row["currency"])))

    trades_payload = {
        "schema_version": NORMALIZED_IB_TRADES_SCHEMA_VERSION,
        "day_utc": day_utc,
        "source_report_path": str(source),
        "trades": trades,
    }
    validate_normalized_ib_trades_v1(trades_payload)

    positions_payload: dict[str, Any] | None = None
    if positions:
        positions_payload = {
            "schema_version": NORMALIZED_IB_POSITIONS_SCHEMA_VERSION,
            "day_utc": day_utc,
            "source_report_path": str(source),
            "positions": positions,
        }
        validate_normalized_ib_positions_v1(positions_payload)

    cash_payload: dict[str, Any] | None = None
    if balances:
        cash_payload = {
            "schema_version": NORMALIZED_IB_CASH_SCHEMA_VERSION,
            "day_utc": day_utc,
            "source_report_path": str(source),
            "balances": balances,
        }
        validate_normalized_ib_cash_v1(cash_payload)

    return {
        "warnings": warnings,
        "normalized_trades": trades_payload,
        "normalized_positions": positions_payload,
        "normalized_cash": cash_payload,
    }
