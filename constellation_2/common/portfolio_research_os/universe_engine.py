from __future__ import annotations

V1_INCLUDED_UNIVERSES = ["US equities", "major ETFs", "cash"]
V1_EXCLUDED_UNIVERSES = ["international equities", "options overlays"]


def v1_universe_scope() -> dict[str, list[str]]:
    return {
        "included": list(V1_INCLUDED_UNIVERSES),
        "excluded": list(V1_EXCLUDED_UNIVERSES),
    }

