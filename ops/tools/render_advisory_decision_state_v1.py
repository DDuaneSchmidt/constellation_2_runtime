#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.advisory_decision_state_kernel_v1 import (  # noqa: E402
    find_latest_advisory_decision_state_v1,
    list_advisory_decision_states_v1,
)


def _render_view(payload: dict, *, view: str) -> dict:
    if view == "current":
        return {
            "authority_label": payload.get("authority_label"),
            "advisory_item_id": payload.get("advisory_item_id"),
            "decision_state": payload.get("decision_state"),
            "actionability_state": payload.get("actionability_state"),
            "freshness_state": payload.get("freshness_state"),
            "visibility_state": payload.get("visibility_state"),
            "promotion_eligibility_state": payload.get("promotion_eligibility_state"),
            "governing_refs": {
                "stage": payload.get("governing_stage_refs"),
                "transition": payload.get("governing_transition_refs"),
                "certification": payload.get("governing_certification_refs"),
                "release": payload.get("governing_release_refs"),
            },
        }
    if view == "blocked":
        return {
            "authority_label": payload.get("authority_label"),
            "advisory_item_id": payload.get("advisory_item_id"),
            "decision_state": payload.get("decision_state"),
            "invalidation_rule_id": payload.get("invalidation_rule_id"),
            "primary_explanation": payload.get("primary_explanation"),
        }
    if view == "lineage":
        return {
            "authority_label": payload.get("authority_label"),
            "advisory_item_id": payload.get("advisory_item_id"),
            "freshness_state": payload.get("freshness_state"),
            "visibility_state": payload.get("visibility_state"),
            "historical_visibility": payload.get("historical_visibility"),
            "supersedes_ref": payload.get("supersedes_ref"),
            "superseded_by_ref": payload.get("superseded_by_ref"),
            "evidence_refs": payload.get("evidence_refs"),
        }
    if view == "promotion":
        return {
            "authority_label": payload.get("authority_label"),
            "advisory_item_id": payload.get("advisory_item_id"),
            "decision_state": payload.get("decision_state"),
            "promotion_eligibility_state": payload.get("promotion_eligibility_state"),
            "freshness_state": payload.get("freshness_state"),
            "invalidation_rule_id": payload.get("invalidation_rule_id"),
            "primary_explanation": payload.get("primary_explanation"),
        }
    raise ValueError(f"UNKNOWN_RENDER_VIEW:{view}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Render durable advisory_decision_state_v1 artifacts only.")
    parser.add_argument("--canonical-truth-root", required=True)
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--scope-id")
    parser.add_argument("--advisory-item-id")
    parser.add_argument("--view", choices=("current", "blocked", "lineage", "promotion"), default="current")
    parser.add_argument("--list-all", action="store_true")
    args = parser.parse_args()
    if args.list_all:
        refs = list_advisory_decision_states_v1(
            canonical_truth_root=args.canonical_truth_root,
            day_utc=args.day_utc,
            scope_id=args.scope_id,
        )
        payload = [_render_view(ref.payload, view=args.view) for ref in refs]
    else:
        ref = find_latest_advisory_decision_state_v1(
            canonical_truth_root=args.canonical_truth_root,
            day_utc=args.day_utc,
            scope_id=args.scope_id,
            advisory_item_id=args.advisory_item_id,
        )
        if ref is None:
            json.dump({"ok": False, "error": "ADVISORY_DECISION_STATE_NOT_FOUND"}, sys.stdout, indent=2, sort_keys=True)
            sys.stdout.write("\n")
            return 1
        payload = _render_view(ref.payload, view=args.view)
    json.dump({"ok": True, "view": args.view, "result": payload}, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
