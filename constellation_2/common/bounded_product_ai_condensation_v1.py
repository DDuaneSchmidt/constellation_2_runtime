from __future__ import annotations

import hashlib
from typing import Any, Mapping, Sequence

from constellation_2.phaseD.lib.canon_json_v1 import canonical_json_bytes_v1


ASSIST_VERSION = "constellation_2.common.bounded_product_ai_condensation_v1"
ALLOWED_SUMMARY_TYPES = (
    "daily_review_brief",
    "weekly_review_brief",
    "blocked_explanation_brief",
    "domain_condensed_summary",
    "scenario_comparison_brief",
)


def _ref_list(summary: Mapping[str, Any]) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for key in (
        "governing_opportunity_refs",
        "governing_advisory_refs",
        "governing_tax_refs",
        "governing_readiness_refs",
    ):
        for row in summary.get(key) or ():
            if isinstance(row, dict):
                refs.append(dict(row))
    return refs


def _render_lines(summary: Mapping[str, Any], summary_type: str) -> list[str]:
    actionable = list(summary.get("top_actionable_items") or [])
    blocked = list(summary.get("top_blocked_items") or [])
    deltas = list(summary.get("top_review_deltas") or [])
    degraded = list(summary.get("critical_degraded_states") or [])
    if summary_type == "blocked_explanation_brief":
        labels = [str(item.get("label") or item.get("item_id") or "blocked item") for item in blocked[:3]]
        return ["Blocked now:", *labels] if labels else ["No blocked-but-important items are currently selected."]
    if summary_type == "scenario_comparison_brief":
        labels = [
            str(item.get("label") or item.get("item_id") or "scenario item")
            for item in actionable + deltas
            if str(item.get("scenario_significance_state") or "") == "review_now"
        ][:3]
        return ["Scenario items that matter now:", *labels] if labels else ["No bounded scenario comparisons are currently elevated."]
    lead = "Daily review" if summary_type == "daily_review_brief" else "Weekly review"
    lines = [
        f"{lead}: {len(actionable)} actionable, {len(blocked)} blocked, {len(deltas)} changed, {len(degraded)} degraded."
    ]
    for item in actionable[:2]:
        lines.append(f"Act: {item.get('label') or item.get('item_id')}")
    for item in blocked[:2]:
        lines.append(f"Block: {item.get('label') or item.get('item_id')}")
    for item in degraded[:2]:
        lines.append(f"Degraded: {item.get('label') or item.get('item_id')}")
    return lines


def build_bounded_product_ai_summary_v1(
    *,
    summary: Mapping[str, Any],
    source_summary_ref: Mapping[str, str] | None,
    summary_type: str,
    ai_available: bool = False,
) -> dict[str, Any]:
    normalized_type = str(summary_type or "").strip()
    if normalized_type not in ALLOWED_SUMMARY_TYPES:
        raise ValueError(f"UNSUPPORTED_PRODUCT_AI_SUMMARY_TYPE:{normalized_type or 'missing'}")
    rendered_text = "\n".join(_render_lines(summary, normalized_type))
    degraded_summary = ", ".join(
        str(item.get("label") or item.get("item_id") or "degraded state")
        for item in (summary.get("critical_degraded_states") or [])[:3]
    )
    blocked_summary = ", ".join(
        str(item.get("label") or item.get("item_id") or "blocked item")
        for item in (summary.get("top_blocked_items") or [])[:3]
    )
    ai_summary_id = hashlib.sha256(
        canonical_json_bytes_v1(
            {
                "source_summary_id": str(summary.get("summary_id") or ""),
                "summary_type": normalized_type,
                "assist_status": "generated" if ai_available else "fallback_deterministic",
                "rendered_text": rendered_text,
            }
        )
    ).hexdigest()
    provenance_refs = _ref_list(summary)
    payload = {
        "ai_summary_id": ai_summary_id,
        "summary_type": normalized_type,
        "rendered_text": rendered_text,
        "degraded_state_summary": degraded_summary,
        "blocked_state_summary": blocked_summary,
        "provenance_refs": provenance_refs,
        "assist_status": "generated" if ai_available else "fallback_deterministic",
        "version": ASSIST_VERSION,
    }
    if source_summary_ref is not None:
        payload["source_summary_ref"] = dict(source_summary_ref)
    return payload
