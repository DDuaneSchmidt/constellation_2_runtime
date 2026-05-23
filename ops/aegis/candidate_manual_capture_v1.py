from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPORT_FAMILY = "aegis_manual_external_capture_v1"


def append_manual_external_capture_v1(
    *,
    truth_root: Path,
    day_utc: str,
    candidate_id: str,
    manually_captured: Any,
    quantity: Any,
    capture_timestamp: str,
    review_decision: str,
    operator: str,
    external_execution_venue: str = "",
    operator_notes: str = "",
    confidence_override: str = "",
    paper_trade_only: Any = True,
) -> dict[str, Any]:
    candidate = str(candidate_id or "").strip()
    if not candidate:
        raise ValueError("candidate_id is required")
    normalized_decision = str(review_decision or "").strip().upper().replace("-", "_")
    if normalized_decision not in {"REVIEWED", "WATCHLIST", "NEEDS_MORE_EVIDENCE", "DISMISS", "PAPER_OBSERVATION", "MANUAL_CAPTURE_RECORDED"}:
        raise ValueError("review_decision must be REVIEWED, WATCHLIST, NEEDS_MORE_EVIDENCE, DISMISS, PAPER_OBSERVATION, or MANUAL_CAPTURE_RECORDED")
    if not str(operator or "").strip():
        raise ValueError("operator is required")
    captured = _parse_bool(manually_captured)
    paper_only = _parse_bool(paper_trade_only)
    parsed_quantity = _parse_quantity(quantity)
    if captured and parsed_quantity is None:
        raise ValueError("quantity is required when manually_captured=true")
    timestamp = str(capture_timestamp or "").strip() or _now()
    _parse_timestamp(timestamp)
    event: dict[str, Any] = {
        "schema_id": "aegis_manual_external_capture",
        "schema_version": "v1",
        "event_type": "MANUAL_EXTERNAL_CAPTURE_RECORDED",
        "capture_event_id": "",
        "candidate_id": candidate,
        "day_utc": day_utc,
        "timestamp_utc": _now(),
        "capture_timestamp": timestamp,
        "manually_captured": captured,
        "quantity": parsed_quantity,
        "external_execution_venue": str(external_execution_venue or "").strip(),
        "operator": str(operator or "").strip(),
        "operator_notes": str(operator_notes or "").strip(),
        "confidence_override": str(confidence_override or "").strip(),
        "paper_trade_only": paper_only,
        "review_decision": normalized_decision,
        "operator_statement": "Aegis did not execute this trade.",
        "record_semantics": "OPERATOR_DECLARED_EXTERNAL_ACTION_ONLY",
        "broker_execution_allowed": False,
        "order_routing_allowed": False,
        "live_trading_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_approval_allowed": False,
        "automatic_promotion_allowed": False,
        "broker_submit_transmit_called": False,
    }
    event["capture_event_id"] = f"manual-capture:{candidate}:{_stable_hash(event)[:20]}"
    event["content_hash"] = _stable_hash(event)
    path = manual_external_capture_path_v1(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    return {**event, "path": str(path)}


def manual_external_capture_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc / "manual_external_capture.v1.jsonl"


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_quantity(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except ValueError as exc:
        raise ValueError("quantity must be an integer") from exc
    if parsed <= 0:
        raise ValueError("quantity must be positive")
    return parsed


def _parse_timestamp(value: str) -> None:
    text = str(value or "").strip()
    try:
        if text.endswith("Z"):
            datetime.fromisoformat(text.replace("Z", "+00:00"))
        else:
            datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("capture_timestamp must be ISO-8601") from exc


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
