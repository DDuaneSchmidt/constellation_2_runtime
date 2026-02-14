"""
risk_budget_gate_v1.py

Constellation 2.0 Phase D
Deterministic RiskBudget enforcement gate.

Authority:
- constellation_2/governance/C2_INVARIANTS_AND_REASON_CODES.md
  - C2_RISK_BUDGET_SCHEMA_INVALID
  - C2_RISK_BUDGET_EXCEEDED
- constellation_2/governance/C2_DETERMINISM_STANDARD.md (no floats)
- constellation_2/schemas/risk_budget.v1.schema.json

Rules:
- RiskBudget must validate against schema (fail-closed => veto)
- WhatIf results must be compared deterministically (Decimal, not float)
- All numeric inputs are decimal strings
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Tuple

from .validate_against_schema_v1 import SchemaValidationError, validate_against_repo_schema_v1


class RiskBudgetGateError(Exception):
    pass


def _d(s: str, name: str) -> Decimal:
    if not isinstance(s, str) or not s:
        raise RiskBudgetGateError(f"DECIMAL_STRING_REQUIRED: {name}")
    try:
        return Decimal(s)
    except InvalidOperation as e:
        raise RiskBudgetGateError(f"DECIMAL_PARSE_FAILED: {name}={s!r}") from e


@dataclass(frozen=True)
class RiskBudgetDecisionV1:
    allow: bool
    reason_code: Optional[str]
    reason_detail: str


def validate_risk_budget_or_raise_v1(repo_root, risk_budget: Dict[str, Any]) -> None:
    """
    Raises RiskBudgetGateError (or SchemaValidationError) on invalid budget.
    """
    validate_against_repo_schema_v1(
        risk_budget,
        repo_root,
        "constellation_2/schemas/risk_budget.v1.schema.json",
    )
    if risk_budget.get("schema_id") != "risk_budget" or risk_budget.get("schema_version") != "v1":
        raise RiskBudgetGateError("RISK_BUDGET_SCHEMA_ID_VERSION_MISMATCH")


def enforce_risk_budget_against_whatif_v1(
    *,
    repo_root,
    risk_budget: Dict[str, Any],
    whatif_margin_change_usd: str,
    whatif_notional_usd: str,
    engine_id: Optional[str],
) -> RiskBudgetDecisionV1:
    """
    Compare WhatIf projections against RiskBudget.

    - margin_change_usd: decimal string (projected incremental margin for this order)
    - notional_usd: decimal string (projected notional for this order)
    - engine_id: optional, if per-engine limits are present
    """
    try:
        validate_risk_budget_or_raise_v1(repo_root, risk_budget)
    except (SchemaValidationError, RiskBudgetGateError) as e:
        return RiskBudgetDecisionV1(
            allow=False,
            reason_code="C2_RISK_BUDGET_SCHEMA_INVALID",
            reason_detail=str(e),
        )

    m = _d(whatif_margin_change_usd, "whatif_margin_change_usd")
    n = _d(whatif_notional_usd, "whatif_notional_usd")

    # Portfolio-level caps
    cap_m = _d(str(risk_budget["max_margin_usd"]), "risk_budget.max_margin_usd")
    cap_n = _d(str(risk_budget["max_notional_usd"]), "risk_budget.max_notional_usd")

    # Enforce per-engine caps if provided
    per_engine = None
    eng_limits = risk_budget.get("engine_limits")
    if engine_id and isinstance(eng_limits, dict) and engine_id in eng_limits and isinstance(eng_limits[engine_id], dict):
        per_engine = eng_limits[engine_id]

    if per_engine is not None:
        cap_m = _d(str(per_engine["max_margin_usd"]), f"engine_limits[{engine_id}].max_margin_usd")
        cap_n = _d(str(per_engine["max_notional_usd"]), f"engine_limits[{engine_id}].max_notional_usd")

    if m > cap_m:
        return RiskBudgetDecisionV1(
            allow=False,
            reason_code="C2_RISK_BUDGET_EXCEEDED",
            reason_detail=f"Projected margin {m} exceeds cap {cap_m}",
        )

    if n > cap_n:
        return RiskBudgetDecisionV1(
            allow=False,
            reason_code="C2_RISK_BUDGET_EXCEEDED",
            reason_detail=f"Projected notional {n} exceeds cap {cap_n}",
        )

    return RiskBudgetDecisionV1(allow=True, reason_code=None, reason_detail="OK")
