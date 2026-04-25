from __future__ import annotations

from typing import Any, Mapping


THRESHOLD_MODEL_VERSION = "constellation_2.common.policy_evidence_window_threshold_v1"


RULES: tuple[dict[str, Any], ...] = (
    {
        "threshold_result": "PRIOR_POLICY_ROLLBACK_REQUIRED",
        "predicate": lambda c: str(c.get("trust_override_state") or "") == "rollback_required",
        "evolution_strength": "required",
    },
    {
        "threshold_result": "TRUST_OVERRIDE_ACTIVE",
        "predicate": lambda c: str(c.get("trust_override_state") or "") in {"block_reduce_emphasis", "block_more_compression"},
        "evolution_strength": "required",
    },
    {
        "threshold_result": "INSUFFICIENT_HISTORY",
        "predicate": lambda c: bool(c.get("insufficient_history")),
        "evolution_strength": "withheld",
    },
    {
        "threshold_result": "UNSTABLE_HISTORY",
        "predicate": lambda c: bool(c.get("unstable_history")),
        "evolution_strength": "withheld",
    },
    {
        "threshold_result": "CONSISTENT_STRENGTHEN_SUPPORT",
        "predicate": lambda c: str(c.get("dominant_support") or "") == "strengthen",
        "evolution_strength": "supported",
    },
    {
        "threshold_result": "CONSISTENT_REDUCE_SUPPORT",
        "predicate": lambda c: str(c.get("dominant_support") or "") == "reduce",
        "evolution_strength": "supported",
    },
    {
        "threshold_result": "CONSISTENT_COMPRESSION_SUPPORT",
        "predicate": lambda c: str(c.get("dominant_support") or "") == "compress",
        "evolution_strength": "supported",
    },
    {
        "threshold_result": "PRIOR_POLICY_EXPIRED",
        "predicate": lambda c: bool(c.get("prior_policy_expired")),
        "evolution_strength": "expired",
    },
    {
        "threshold_result": "NO_EVOLUTION_PRESSURE",
        "predicate": lambda c: True,
        "evolution_strength": "limited",
    },
)


def evaluate_policy_evidence_window_threshold_v1(candidate: Mapping[str, Any]) -> dict[str, str]:
    for rule in RULES:
        if bool(rule["predicate"](candidate)):
            return {
                "threshold_result": str(rule["threshold_result"]),
                "evolution_strength": str(rule["evolution_strength"]),
            }
    return {
        "threshold_result": "INSUFFICIENT_HISTORY",
        "evolution_strength": "withheld",
    }
