from __future__ import annotations

from dataclasses import dataclass


UNKNOWN_BROKER_ORDER_STATUS = "UNKNOWN_BROKER_ORDER_STATUS"


@dataclass(frozen=True)
class NormalizedBrokerOrderStatusV1:
    raw_broker_status: str
    normalized_lifecycle_status: str
    is_terminal: bool


_MAPPING: dict[str, tuple[str, bool]] = {
    "PENDINGSUBMIT": ("OPEN_PENDING_SUBMIT", False),
    "PRESUBMITTED": ("OPEN_PRE_SUBMITTED", False),
    "SUBMITTED": ("OPEN_SUBMITTED", False),
    "FILLED": ("TERMINAL_FILLED", True),
    "CANCELLED": ("TERMINAL_CANCELLED", True),
    "APICANCELLED": ("TERMINAL_CANCELLED", True),
    "PENDINGCANCEL": ("OPEN_PENDING_CANCEL", False),
    "INACTIVE": ("TERMINAL_INACTIVE", True),
    "REJECTED": ("TERMINAL_REJECTED", True),
}


def normalize_broker_order_status_v1(raw_status: str) -> NormalizedBrokerOrderStatusV1:
    raw = str(raw_status or "").strip()
    key = raw.upper()
    mapped = _MAPPING.get(key)
    if mapped is None:
        raise ValueError(f"{UNKNOWN_BROKER_ORDER_STATUS}:raw_broker_status={raw or '<empty>'}")
    normalized, is_terminal = mapped
    return NormalizedBrokerOrderStatusV1(
        raw_broker_status=raw,
        normalized_lifecycle_status=normalized,
        is_terminal=bool(is_terminal),
    )
