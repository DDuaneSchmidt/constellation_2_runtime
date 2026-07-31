from __future__ import annotations

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "no_trading": True,
    "no_broker_execution": True,
    "no_real_capital_allocation": True,
    "no_recommendations": True,
}


def opportunity_scope() -> dict[str, object]:
    return {
        "purpose": "research ranking inputs for validation only",
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "emits_recommendations": False,
    }

