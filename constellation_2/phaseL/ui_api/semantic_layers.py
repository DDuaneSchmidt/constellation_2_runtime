from __future__ import annotations

from copy import deepcopy
from typing import Any


TRUTH_STATE_VALUES = (
    "canonical",
    "derived",
    "reconstructed",
)

DATA_CONDITION_VALUES = (
    "fresh",
    "stale",
    "degraded",
    "fail_closed",
    "unknown",
)

OPERATOR_SEVERITY_VALUES = (
    "info",
    "attention",
    "action_required",
    "critical",
)

DOMAIN_CONTEXT_VALUES = (
    "operations",
    "advisory",
    "admin",
)

LAYER_UNKNOWN = "UNKNOWN"


LAYERED_SEMANTICS = {
    "truth_state": TRUTH_STATE_VALUES,
    "data_condition": DATA_CONDITION_VALUES,
    "operator_severity": OPERATOR_SEVERITY_VALUES,
    "domain_context": DOMAIN_CONTEXT_VALUES,
}


CURRENT_FLAT_TO_LAYERED: dict[str, dict[str, Any]] = {
    "canonical": {
        "truth_state": "canonical",
        "data_condition": LAYER_UNKNOWN,
        "operator_severity": LAYER_UNKNOWN,
        "domain_context": "operations",
        "notes": "Current flat semantic is a truth-state marker.",
    },
    "derived": {
        "truth_state": "derived",
        "data_condition": LAYER_UNKNOWN,
        "operator_severity": LAYER_UNKNOWN,
        "domain_context": "operations",
        "notes": "Current flat semantic is a truth-state marker.",
    },
    "reconstructed": {
        "truth_state": "reconstructed",
        "data_condition": LAYER_UNKNOWN,
        "operator_severity": LAYER_UNKNOWN,
        "domain_context": "operations",
        "notes": "Current flat semantic is a truth-state marker.",
    },
    "stale": {
        "truth_state": LAYER_UNKNOWN,
        "data_condition": "stale",
        "operator_severity": LAYER_UNKNOWN,
        "domain_context": "operations",
        "notes": "Current flat semantic is a data-condition marker.",
    },
    "unknown": {
        "truth_state": LAYER_UNKNOWN,
        "data_condition": "unknown",
        "operator_severity": LAYER_UNKNOWN,
        "domain_context": LAYER_UNKNOWN,
        "notes": "Current flat semantic is overloaded between data absence and state ambiguity.",
    },
    "healthy": {
        "truth_state": LAYER_UNKNOWN,
        "data_condition": LAYER_UNKNOWN,
        "operator_severity": LAYER_UNKNOWN,
        "domain_context": "operations",
        "notes": "Current flat semantic is overloaded and cannot be losslessly mapped without refactor.",
    },
    "warning": {
        "truth_state": LAYER_UNKNOWN,
        "data_condition": LAYER_UNKNOWN,
        "operator_severity": LAYER_UNKNOWN,
        "domain_context": "operations",
        "notes": "Current flat semantic does not prove a single layered meaning yet.",
    },
    "degraded": {
        "truth_state": LAYER_UNKNOWN,
        "data_condition": "degraded",
        "operator_severity": LAYER_UNKNOWN,
        "domain_context": "operations",
        "notes": "Current flat semantic may describe degraded posture, but severity is not uniform across sources.",
    },
    "blocked": {
        "truth_state": LAYER_UNKNOWN,
        "data_condition": LAYER_UNKNOWN,
        "operator_severity": LAYER_UNKNOWN,
        "domain_context": "operations",
        "notes": "Current flat semantic implies operator impact, but exact layered mapping is not proven.",
    },
    "fail_closed": {
        "truth_state": LAYER_UNKNOWN,
        "data_condition": "fail_closed",
        "operator_severity": "critical",
        "domain_context": "operations",
        "notes": "Fail-closed posture is proven as a data-condition and critical operator state.",
    },
    "advisory": {
        "truth_state": LAYER_UNKNOWN,
        "data_condition": LAYER_UNKNOWN,
        "operator_severity": LAYER_UNKNOWN,
        "domain_context": "advisory",
        "notes": "Current flat semantic is a domain-context marker, not truth-state or severity.",
    },
}


def layered_semantics() -> dict[str, tuple[str, ...]]:
    return deepcopy(LAYERED_SEMANTICS)


def layered_mapping_for(flat_semantic: str) -> dict[str, Any]:
    return deepcopy(
        CURRENT_FLAT_TO_LAYERED.get(
            flat_semantic,
            {
                "truth_state": LAYER_UNKNOWN,
                "data_condition": LAYER_UNKNOWN,
                "operator_severity": LAYER_UNKNOWN,
                "domain_context": LAYER_UNKNOWN,
                "notes": "Current flat semantic is unproven.",
            },
        )
    )


def current_flat_semantics() -> tuple[str, ...]:
    return tuple(CURRENT_FLAT_TO_LAYERED.keys())
