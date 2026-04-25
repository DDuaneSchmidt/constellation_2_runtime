from __future__ import annotations

from typing import Any

from .schemas import content_hash


SUPPORTED_RUNTIME_MODES = {"development", "paper", "live_disabled", "live_ready", "live_active"}
PAPER_ADAPTER = "paper_simulated"
NULL_LIVE_ADAPTER = "null_live_disabled"


def adapter_readiness(*, adapter_name: str, runtime_mode: str) -> dict[str, Any]:
    if runtime_mode not in SUPPORTED_RUNTIME_MODES:
        return {"status": "failed", "blocking": True, "reason": f"UNKNOWN_RUNTIME_MODE:{runtime_mode}"}
    if adapter_name == PAPER_ADAPTER:
        if runtime_mode in {"development", "paper"}:
            return {"status": "healthy", "blocking": False, "reason": "paper simulated adapter ready"}
        return {"status": "failed", "blocking": True, "reason": f"PAPER_ADAPTER_RUNTIME_MODE_MISMATCH:{runtime_mode}"}
    if adapter_name == NULL_LIVE_ADAPTER:
        return {"status": "failed", "blocking": True, "reason": "LIVE_EXECUTION_DISABLED"}
    return {"status": "failed", "blocking": True, "reason": f"UNSUPPORTED_EXECUTION_ADAPTER:{adapter_name}"}


def submit_via_adapter(*, adapter_name: str, runtime_mode: str, payload: dict[str, Any]) -> dict[str, Any]:
    readiness = adapter_readiness(adapter_name=adapter_name, runtime_mode=runtime_mode)
    if readiness["blocking"]:
        raise ValueError(readiness["reason"])
    if adapter_name == PAPER_ADAPTER:
        request_hash = content_hash({"adapter_name": adapter_name, "runtime_mode": runtime_mode, "payload": payload})
        return {
            "broker_request_ref": f"paper-request-{request_hash[:12]}",
            "receipt_type": "ack",
            "order_status": "ACCEPTED",
            "external_execution_ref": f"paper-order-{request_hash[:12]}",
            "filled_quantity": 0.0,
            "remaining_quantity": float(payload["target_quantity"] or 0.0),
            "average_fill_price": None,
            "fee_amount": None,
            "message": "paper simulated order accepted",
        }
    raise ValueError(f"UNSUPPORTED_EXECUTION_ADAPTER:{adapter_name}")
