"""
broker_adapter_v1.py

Constellation 2.0 Phase D
Deterministic broker adapter boundary interface.

Authority:
- constellation_2/governance/C2_EXECUTION_CONTRACT.md
- constellation_2/governance/C2_INVARIANTS_AND_REASON_CODES.md
- constellation_2/governance/C2_DETERMINISM_STANDARD.md

Rules:
- Broker-facing logic MUST be behind this boundary.
- Phase D MUST operate in PAPER only.
- Adapter implementations must be deterministic at the boundary:
  * no hidden defaults
  * explicit connection parameters
  * explicit environment selection
  * structured responses (JSON-compatible, no floats)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol


@dataclass(frozen=True)
class BrokerEnv:
    """
    Broker environment selection.

    NOTE: Phase D forbids LIVE. Any attempt to construct an adapter in LIVE must be vetoed.
    """
    name: str  # "PAPER" only in Phase D


@dataclass(frozen=True)
class BrokerConnectionSpec:
    """
    Explicit connection parameters for the adapter.

    These must be included in evidence notes/pointers by the submit boundary.
    """
    host: str
    port: int
    client_id: int


@dataclass(frozen=True)
class BrokerWhatIfResult:
    """
    Deterministic WhatIf result surface (no floats).

    All numeric values are decimal strings to avoid float ambiguity.
    """
    ok: bool
    margin_change_usd: str  # decimal string
    notional_usd: str       # decimal string
    detail: str             # concise human-readable summary (no secrets)
    raw: Optional[Dict[str, Any]] = None  # optional, must remain JSON-safe (no floats)


@dataclass(frozen=True)
class BrokerSubmitResult:
    """
    Deterministic submit result surface (no floats).

    order_id / perm_id may be None if broker refused or not provided.
    """
    ok: bool
    status: str  # "SUBMITTED" | "ACKNOWLEDGED" | "REJECTED" | "CANCELLED" | "UNKNOWN"
    order_id: Optional[int]
    perm_id: Optional[int]
    error_code: Optional[str]
    error_message: Optional[str]
    raw: Optional[Dict[str, Any]] = None  # optional, must remain JSON-safe (no floats)


class BrokerAdapterV1(Protocol):
    """
    Broker adapter interface v1.

    The submit boundary is responsible for:
    - schema validation (OrderPlan v1)
    - invariants enforcement
    - BindingRecord written BEFORE any broker call
    - RiskBudget enforcement using whatif_order()
    """

    def broker_name(self) -> str:
        ...

    def broker_env(self) -> str:
        ...

    def connect(self) -> None:
        """
        Establish broker session (fail-closed).

        If not reachable or authentication fails, raise an exception.
        Submit boundary must convert to VetoRecord (fail-closed).
        """
        ...

    def disconnect(self) -> None:
        """
        Best-effort disconnect. Must not mutate truth artifacts.
        """
        ...

    def whatif_order(self, *, order_plan: Dict[str, Any]) -> BrokerWhatIfResult:
        """
        Deterministic margin/risk precheck.
        Must not submit an order.

        Input must be a validated OrderPlan v1 (caller responsibility).
        """
        ...

    def submit_order(self, *, order_plan: Dict[str, Any]) -> BrokerSubmitResult:
        """
        Submit order to broker (PAPER only).

        Input must be a validated OrderPlan v1 (caller responsibility).
        """
        ...

    def cancel_order(self, *, order_id: int) -> BrokerSubmitResult:
        """
        Cancel an existing broker order id.
        """
        ...
