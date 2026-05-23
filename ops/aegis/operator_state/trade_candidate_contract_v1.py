from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

SCHEMA_ID = "trade_candidate_contract"
SCHEMA_VERSION = "v1"

READINESS_STATUSES = {
    "review_only_blocked",
    "incomplete_trade_definition",
    "manual_capture_ready",
    "paper_package_ready",
    "paper_intent_created",
    "skipped",
    "captured_manually",
}

BLOCKER_MESSAGES = {
    "MISSING_TRADE_CANDIDATE_ID": "Selected exposure lineage is missing.",
    "MISSING_ENTRY_REFERENCE_PRICE": "Entry reference price is required before a selected exposure can be treated as a trade candidate.",
    "MISSING_STOP_OR_INVALIDATION_LEVEL": "A stop price or explicit invalidation level is required.",
    "MISSING_POSITION_SIZE": "Quantity or sizing basis is required.",
    "MISSING_RISK_BUDGET": "Risk budget reference or allocation percent is required.",
    "STALE_MARKET_DATA_BLOCKS_CONVERSION": "Current market data is stale or missing.",
    "MISSING_SYMBOL_AUTHORITY": "Symbol authority source is required.",
    "MISSING_PORTFOLIO_GATE_DECISION": "Portfolio gate decision id is required.",
    "MISSING_SUBMIT_BOUNDARY_STATUS": "Submit boundary status is required.",
    "MISSING_SELECTED_EXPOSURE_LINEAGE": "Selected exposure lineage is required.",
    "BLOCKED_BY_CONVERSION": "Converter/package diagnostic reports a blocker.",
}


def now_iso_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_hash_v1(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def canonical_trade_candidate_id_v1(*, selected_exposure_intent_id: str, source_day: str, source_run_id: str) -> str:
    if not selected_exposure_intent_id:
        return ""
    suffix = stable_hash_v1(
        {
            "selected_exposure_intent_id": selected_exposure_intent_id,
            "source_day": source_day,
            "source_run_id": source_run_id,
            "contract": f"{SCHEMA_ID}.{SCHEMA_VERSION}",
        }
    )[:16]
    return f"trade-candidate:{selected_exposure_intent_id}:{suffix}"


def blocker_v1(code: str, *, field: str = "", message: str = "", severity: str = "HIGH") -> dict[str, Any]:
    return {
        "code": code,
        "field": field,
        "message": message or BLOCKER_MESSAGES.get(code, code),
        "severity": severity,
        "recoverable": True,
    }


def is_positive_decimal_text_v1(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        return Decimal(text) > 0
    except (InvalidOperation, ValueError):
        return False


def normalize_decimal_text_v1(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError):
        return text
    return format(parsed.normalize(), "f")


def positive_int_or_none_v1(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = int(Decimal(text))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed > 0 else None


def first_text_v1(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def nested_dict_v1(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    return dict(value) if isinstance(value, Mapping) else {}


def read_json_object_v1(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}
    return payload if isinstance(payload, dict) else {}


def content_hash_file_v1(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
