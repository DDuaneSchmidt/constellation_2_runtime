from __future__ import annotations

from typing import Any, Mapping


LINKAGE_VERSION = "constellation_2.common.value_sleeve_linkage_v1"


def evaluate_value_sleeve_linkage_v1(
    *,
    scope_sleeve_id: str,
    opportunity_payload: Mapping[str, Any],
) -> dict[str, Any]:
    explicit_sleeves = [
        str(item).strip()
        for item in (opportunity_payload.get("economic_sleeve_ids") or [])
        if str(item).strip()
    ]
    if len(explicit_sleeves) > 1:
        return {
            "sleeve_refs": [
                {
                    "sleeve_id": sleeve_id,
                    "linkage_state": "multi_sleeve_overlap",
                    "reason_id": "VALUE_MULTI_SLEEVE_OVERLAP",
                }
                for sleeve_id in explicit_sleeves
            ],
            "primary_linkage_state": "multi_sleeve_overlap",
        }
    if len(explicit_sleeves) == 1:
        return {
            "sleeve_refs": [
                {
                    "sleeve_id": explicit_sleeves[0],
                    "linkage_state": "direct_governed_sleeve",
                    "reason_id": "VALUE_DIRECT_GOVERNED_SLEEVE",
                }
            ],
            "primary_linkage_state": "direct_governed_sleeve",
        }
    normalized_scope = str(scope_sleeve_id or "").strip()
    if normalized_scope:
        return {
            "sleeve_refs": [
                {
                    "sleeve_id": normalized_scope,
                    "linkage_state": "execution_scope_only",
                    "reason_id": "VALUE_EXECUTION_SCOPE_ONLY",
                }
            ],
            "primary_linkage_state": "execution_scope_only",
        }
    return {
        "sleeve_refs": [],
        "primary_linkage_state": "insufficient_sleeve_attribution",
    }
