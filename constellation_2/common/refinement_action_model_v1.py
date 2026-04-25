from __future__ import annotations

from typing import Any, Mapping


ACTION_MODEL_VERSION = "constellation_2.common.refinement_action_model_v1"


def _compress_message(message: str) -> str:
    text = " ".join(str(message or "").split())
    if len(text) <= 88:
        return text or "Refined summary preserved."
    return f"{text[:85].rstrip()}..."


def build_refinement_action_v1(
    *,
    threshold: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    action = str(threshold["refinement_action"])
    original_message = str(candidate.get("summary_message") or candidate.get("target_label") or "Refinement target")
    if action == "compress_summary":
        final_message = _compress_message(original_message)
        after_bucket = "top_level_compressed"
    elif action == "demote_to_secondary":
        final_message = original_message
        after_bucket = "secondary"
    elif action == "preserve_drilldown_only":
        final_message = f"Preserved in drill-down only. {original_message}"
        after_bucket = "drilldown_only"
    elif action == "preserve_top_level":
        final_message = original_message
        after_bucket = "top_level"
    elif action == "retire_from_default_surface":
        final_message = f"Retired from default surface. {original_message}"
        after_bucket = "retired_from_default_surface"
    else:
        final_message = f"Refinement withheld. {original_message}"
        after_bucket = str(candidate.get("source_bucket") or "top_level")
    return {
        "refinement_action": action,
        "refinement_strength": str(threshold["refinement_strength"]),
        "visibility_effect": str(threshold["visibility_effect"]),
        "reversibility_state": str(threshold["reversibility_state"]),
        "summary_message": final_message,
        "after_bucket": after_bucket,
    }
