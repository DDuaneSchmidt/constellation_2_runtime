from __future__ import annotations

V1_FACTORS = [
    "growth",
    "quality",
    "valuation",
    "income",
    "momentum",
    "risk",
]

V1_FACTOR_EXCLUSIONS = ["adaptive factor weights", "AI prediction systems"]


def v1_factor_scope() -> dict[str, list[str]]:
    return {
        "included_factors": list(V1_FACTORS),
        "excluded_factor_methods": list(V1_FACTOR_EXCLUSIONS),
        "weighting_policy": ["fixed transparent weights only", "no optimized factor weights in V1"],
    }

