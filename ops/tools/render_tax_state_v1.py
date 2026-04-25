#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.tax_state_kernel_v1 import find_latest_tax_state_v1, list_tax_states_v1  # noqa: E402


def _render_view(
    payload: dict[str, object],
    *,
    view: str,
    derived_superseded_by: dict[str, object] | None = None,
) -> dict[str, object]:
    if view == "current":
        return {
            "authority_label": payload.get("authority_label"),
            "tax_state_id": payload.get("tax_state_id"),
            "completeness_state": payload.get("completeness_state"),
            "freshness_state": payload.get("freshness_state"),
            "visibility_state": payload.get("visibility_state"),
            "blocker_states": payload.get("blocker_states"),
            "opportunity_states": payload.get("opportunity_states"),
            "advisory_binding_state": payload.get("advisory_binding_state"),
        }
    if view == "blocked":
        return {
            "authority_label": payload.get("authority_label"),
            "tax_state_id": payload.get("tax_state_id"),
            "primary_rule_id": payload.get("primary_rule_id"),
            "blocker_states": payload.get("blocker_states"),
            "primary_explanation": payload.get("primary_explanation"),
        }
    if view == "lineage":
        return {
            "authority_label": payload.get("authority_label"),
            "tax_state_id": payload.get("tax_state_id"),
            "freshness_state": payload.get("freshness_state"),
            "visibility_state": payload.get("visibility_state"),
            "historical_visibility": payload.get("historical_visibility"),
            "supersedes_ref": payload.get("supersedes_ref"),
            "superseded_by_ref": payload.get("superseded_by_ref") or derived_superseded_by,
            "governing_refs": payload.get("governing_refs"),
        }
    if view == "advisory":
        return {
            "authority_label": payload.get("authority_label"),
            "tax_state_id": payload.get("tax_state_id"),
            "advisory_binding_state": payload.get("advisory_binding_state"),
            "primary_rule_id": payload.get("primary_rule_id"),
            "primary_explanation": payload.get("primary_explanation"),
        }
    raise ValueError(f"UNKNOWN_TAX_RENDER_VIEW:{view}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Render durable tax_state_v1 artifacts only.")
    parser.add_argument("--canonical-truth-root", required=True)
    parser.add_argument("--day-utc", required=True)
    parser.add_argument("--scope-id")
    parser.add_argument("--view", choices=("current", "blocked", "lineage", "advisory"), default="current")
    parser.add_argument("--list-all", action="store_true")
    args = parser.parse_args()
    refs = list_tax_states_v1(
        canonical_truth_root=args.canonical_truth_root,
        day_utc=args.day_utc,
        scope_id=args.scope_id,
    )
    superseded_by_map = {
        str((ref.payload.get("supersedes_ref") or {}).get("artifact_path") or "").strip(): {
            "artifact_id": "tax_state_v1",
            "artifact_path": str(ref.path.resolve()),
            "artifact_sha256": str(ref.sha256),
        }
        for ref in refs
        if isinstance(ref.payload.get("supersedes_ref"), dict)
    }
    if args.list_all:
        payload = [
            _render_view(
                ref.payload,
                view=args.view,
                derived_superseded_by=superseded_by_map.get(str(ref.path.resolve())),
            )
            for ref in refs
        ]
    else:
        ref = find_latest_tax_state_v1(
            canonical_truth_root=args.canonical_truth_root,
            day_utc=args.day_utc,
            scope_id=args.scope_id,
        )
        if ref is None:
            json.dump({"ok": False, "error": "TAX_STATE_NOT_FOUND"}, sys.stdout, indent=2, sort_keys=True)
            sys.stdout.write("\n")
            return 1
        payload = _render_view(
            ref.payload,
            view=args.view,
            derived_superseded_by=superseded_by_map.get(str(ref.path.resolve())),
        )
    json.dump({"ok": True, "view": args.view, "result": payload}, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
