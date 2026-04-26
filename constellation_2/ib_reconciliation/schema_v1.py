from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Mapping

from .paths_v1 import validate_day_utc_v1


class SchemaValidationError(RuntimeError):
    pass


NORMALIZED_IB_TRADES_SCHEMA_VERSION = "normalized_ib_trades.v1"
NORMALIZED_IB_POSITIONS_SCHEMA_VERSION = "normalized_ib_positions.v1"
NORMALIZED_IB_CASH_SCHEMA_VERSION = "normalized_ib_cash.v1"
AEGIS_EXPECTED_ACTIVITY_SCHEMA_VERSION = "aegis_expected_activity.v1"
IB_RECONCILIATION_SCHEMA_VERSION = "ib_reconciliation.v1"
IB_RECONCILIATION_AI_PACKET_SCHEMA_VERSION = "ib_reconciliation_ai_packet.v1"
IB_RECONCILIATION_AI_REVIEW_SCHEMA_VERSION = "ib_reconciliation_ai_review.v1"

RECON_STATUS_VALUES = {"PASS", "WARN", "FAIL"}
SEVERITY_VALUES = {"LOW", "MEDIUM", "HIGH"}
REVIEW_STATUS_VALUES = {"NO_ACTION", "MONITOR", "ACTION_REQUIRED"}

MISMATCH_TYPES = {
    "UNEXPECTED_IB_TRADE",
    "MISSING_IB_TRADE",
    "ORDER_ID_MISMATCH",
    "PERM_ID_MISMATCH",
    "PARTIAL_FILL_MISMATCH",
    "QUANTITY_MISMATCH",
    "PRICE_SLIPPAGE_EXCEEDED",
    "COMMISSION_MISMATCH",
    "POSITION_MISMATCH",
    "CASH_BALANCE_MISMATCH",
    "PNL_MISMATCH",
    "MANUAL_TRADE_DETECTED",
    "DUPLICATE_ORDER_DETECTED",
    "LOW_CONFIDENCE_MATCH_ONLY",
}

DAY_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def utc_now_z_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_decimal_v1(value: Any, *, field: str) -> Decimal:
    if isinstance(value, bool):
        raise SchemaValidationError(f"{field}_INVALID_DECIMAL_BOOL")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SchemaValidationError(f"{field}_INVALID_DECIMAL:{value!r}") from exc


def decimal_to_str_v1(value: Any, *, field: str) -> str:
    dec = parse_decimal_v1(value, field=field)
    text = format(dec, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text if text and text != "-0" else "0"


def _require_mapping(payload: Any, *, field: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise SchemaValidationError(f"{field}_NOT_OBJECT")
    return payload


def _require_list(payload: Any, *, field: str) -> list[Any]:
    if not isinstance(payload, list):
        raise SchemaValidationError(f"{field}_NOT_LIST")
    return payload


def require_text_v1(payload: Mapping[str, Any], key: str) -> str:
    text = str(payload.get(key) or "").strip()
    if not text:
        raise SchemaValidationError(f"FIELD_REQUIRED:{key}")
    return text


def require_day_v1(payload: Mapping[str, Any], key: str = "day_utc") -> str:
    day = require_text_v1(payload, key)
    if not DAY_PATTERN.match(day):
        raise SchemaValidationError(f"FIELD_DAY_INVALID:{key}={day!r}")
    validate_day_utc_v1(day)
    return day


def _require_schema_version(payload: Mapping[str, Any], *, expected: str) -> None:
    got = str(payload.get("schema_version") or "").strip()
    if got != expected:
        raise SchemaValidationError(f"SCHEMA_VERSION_MISMATCH expected={expected!r} got={got!r}")


def _assert_no_floats(value: Any, path: str = "$") -> None:
    if isinstance(value, float):
        raise SchemaValidationError(f"FLOAT_FORBIDDEN:{path}")
    if isinstance(value, Mapping):
        for key, inner in value.items():
            if not isinstance(key, str):
                raise SchemaValidationError(f"NON_STRING_KEY_FORBIDDEN:{path}")
            _assert_no_floats(inner, f"{path}.{key}")
    elif isinstance(value, list):
        for idx, inner in enumerate(value):
            _assert_no_floats(inner, f"{path}[{idx}]")


def canonical_json_bytes_v1(payload: Any) -> bytes:
    _assert_no_floats(payload)
    return (json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


def write_deterministic_json_v1(path: Path, payload: Mapping[str, Any]) -> None:
    if not path.is_absolute():
        raise SchemaValidationError(f"WRITE_PATH_NOT_ABSOLUTE:{path}")
    body = canonical_json_bytes_v1(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(prefix=f".{path.name}.tmp.", dir=str(path.parent), delete=False) as tmp:
        tmp.write(body)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def read_json_object_v1(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SchemaValidationError(f"JSON_TOP_LEVEL_NOT_OBJECT:{path}")
    return payload


def validate_normalized_ib_trades_v1(payload: Mapping[str, Any]) -> None:
    _require_schema_version(payload, expected=NORMALIZED_IB_TRADES_SCHEMA_VERSION)
    require_day_v1(payload)
    require_text_v1(payload, "source_report_path")
    trades = _require_list(payload.get("trades"), field="trades")
    for idx, trade in enumerate(trades):
        row = _require_mapping(trade, field=f"trades[{idx}]")
        for field in (
            "account_id",
            "symbol",
            "asset_class",
            "side",
            "quantity",
            "price",
            "currency",
            "trade_time_utc",
            "ib_order_id",
            "ib_perm_id",
            "commission",
        ):
            require_text_v1(row, field)
        _require_mapping(row.get("raw_ref"), field=f"trades[{idx}].raw_ref")


def validate_normalized_ib_positions_v1(payload: Mapping[str, Any]) -> None:
    _require_schema_version(payload, expected=NORMALIZED_IB_POSITIONS_SCHEMA_VERSION)
    require_day_v1(payload)
    require_text_v1(payload, "source_report_path")
    positions = _require_list(payload.get("positions"), field="positions")
    for idx, row_obj in enumerate(positions):
        row = _require_mapping(row_obj, field=f"positions[{idx}]")
        for field in ("account_id", "symbol", "asset_class", "quantity", "market_price", "market_value", "currency"):
            require_text_v1(row, field)
        _require_mapping(row.get("raw_ref"), field=f"positions[{idx}].raw_ref")


def validate_normalized_ib_cash_v1(payload: Mapping[str, Any]) -> None:
    _require_schema_version(payload, expected=NORMALIZED_IB_CASH_SCHEMA_VERSION)
    require_day_v1(payload)
    require_text_v1(payload, "source_report_path")
    balances = _require_list(payload.get("balances"), field="balances")
    for idx, row_obj in enumerate(balances):
        row = _require_mapping(row_obj, field=f"balances[{idx}]")
        for field in ("account_id", "currency", "cash_balance"):
            require_text_v1(row, field)
        if "net_liquidation_value" in row and row.get("net_liquidation_value") is not None:
            require_text_v1(row, "net_liquidation_value")
        _require_mapping(row.get("raw_ref"), field=f"balances[{idx}].raw_ref")


def validate_aegis_expected_activity_v1(payload: Mapping[str, Any]) -> None:
    _require_schema_version(payload, expected=AEGIS_EXPECTED_ACTIVITY_SCHEMA_VERSION)
    require_day_v1(payload)
    source_artifacts = _require_list(payload.get("source_artifacts"), field="source_artifacts")
    for idx, item in enumerate(source_artifacts):
        text = str(item or "").strip()
        if not text:
            raise SchemaValidationError(f"SOURCE_ARTIFACT_EMPTY:{idx}")

    orders = _require_list(payload.get("expected_orders"), field="expected_orders")
    for idx, row_obj in enumerate(orders):
        row = _require_mapping(row_obj, field=f"expected_orders[{idx}]")
        for field in (
            "attempt_id",
            "submission_id",
            "source_intent_id",
            "symbol",
            "side",
            "quantity",
            "expected_order_type",
            "broker_order_id",
            "broker_perm_id",
            "status",
        ):
            require_text_v1(row, field)

    fills = _require_list(payload.get("expected_fills"), field="expected_fills")
    for idx, row_obj in enumerate(fills):
        row = _require_mapping(row_obj, field=f"expected_fills[{idx}]")
        for field in ("attempt_id", "submission_id", "symbol", "side", "quantity", "broker_order_id", "broker_perm_id"):
            require_text_v1(row, field)
        if "expected_price" in row and row.get("expected_price") is not None:
            require_text_v1(row, "expected_price")

    _require_list(payload.get("expected_positions"), field="expected_positions")


def validate_reconciliation_v1(payload: Mapping[str, Any]) -> None:
    _require_schema_version(payload, expected=IB_RECONCILIATION_SCHEMA_VERSION)
    require_day_v1(payload)
    status = require_text_v1(payload, "status")
    if status not in RECON_STATUS_VALUES:
        raise SchemaValidationError(f"STATUS_INVALID:{status!r}")
    require_text_v1(payload, "aegis_expected_path")
    _require_mapping(payload.get("ib_actual_paths"), field="ib_actual_paths")
    _require_list(payload.get("matched_items"), field="matched_items")
    mismatches = _require_list(payload.get("mismatches"), field="mismatches")
    _require_mapping(payload.get("summary"), field="summary")
    require_text_v1(payload, "generated_utc")
    for idx, row_obj in enumerate(mismatches):
        row = _require_mapping(row_obj, field=f"mismatches[{idx}]")
        require_text_v1(row, "mismatch_id")
        mismatch_type = require_text_v1(row, "type")
        if mismatch_type not in MISMATCH_TYPES:
            raise SchemaValidationError(f"MISMATCH_TYPE_INVALID:{mismatch_type!r}")
        severity = require_text_v1(row, "severity")
        if severity not in SEVERITY_VALUES:
            raise SchemaValidationError(f"MISMATCH_SEVERITY_INVALID:{severity!r}")
        require_text_v1(row, "symbol")
        require_text_v1(row, "description")
        evidence_paths = _require_list(row.get("evidence_paths"), field=f"mismatches[{idx}].evidence_paths")
        for path_idx, evidence in enumerate(evidence_paths):
            if not str(evidence or "").strip():
                raise SchemaValidationError(f"MISMATCH_EVIDENCE_EMPTY:{idx}:{path_idx}")
        require_text_v1(row, "deterministic_rule")


def validate_ai_packet_v1(payload: Mapping[str, Any]) -> None:
    _require_schema_version(payload, expected=IB_RECONCILIATION_AI_PACKET_SCHEMA_VERSION)
    require_day_v1(payload)
    status = require_text_v1(payload, "reconciliation_status")
    if status not in RECON_STATUS_VALUES:
        raise SchemaValidationError(f"AI_PACKET_RECON_STATUS_INVALID:{status!r}")
    _require_list(payload.get("questions"), field="questions")
    _require_list(payload.get("mismatches"), field="mismatches")


def validate_ai_review_v1(payload: Mapping[str, Any]) -> None:
    _require_schema_version(payload, expected=IB_RECONCILIATION_AI_REVIEW_SCHEMA_VERSION)
    require_day_v1(payload)
    review_status = require_text_v1(payload, "review_status")
    if review_status not in REVIEW_STATUS_VALUES:
        raise SchemaValidationError(f"AI_REVIEW_STATUS_INVALID:{review_status!r}")
    require_text_v1(payload, "primary_risk")
    _require_list(payload.get("findings"), field="findings")
    require_text_v1(payload, "recommended_action")
    if payload.get("codex_task") is not None:
        require_text_v1(payload, "codex_task")
    if not isinstance(payload.get("human_alert_required"), bool):
        raise SchemaValidationError("AI_REVIEW_HUMAN_ALERT_REQUIRED_NOT_BOOL")
    require_text_v1(payload, "generated_utc")
