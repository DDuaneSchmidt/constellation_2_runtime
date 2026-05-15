#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.aegis_operator_inbox_v1 import (  # noqa: E402
    build_operator_inbox_review_report_v1,
    list_operator_inbox_items_v1,
    load_operator_inbox_item_v1,
    now_utc_v1,
    promote_operator_inbox_to_research_idea_v1,
    transition_operator_inbox_item_v1,
    validate_operator_inbox_artifact_v1,
    write_operator_inbox_artifact_v1,
)


ACTION_STATUS = {
    "mark-reviewed": "REVIEWED",
    "archive": "ARCHIVED",
    "reject": "REJECTED",
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="aegis_operator_inbox_review_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--action", required=True, choices=["list", "report", "mark-reviewed", "archive", "reject", "promote-to-idea", "promote-to-hypothesis"])
    parser.add_argument("--inbox_item_id", default="")
    parser.add_argument("--include_closed", action="store_true")
    parser.add_argument("--updated_at", default="")
    parser.add_argument("--day_utc", default="")
    parser.add_argument("--notes", default="")
    parser.add_argument("--hypothesis_id", default="")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root)
    updated_at = args.updated_at or now_utc_v1()
    if args.action == "list":
        rows = list_operator_inbox_items_v1(truth_root=truth_root, include_closed=bool(args.include_closed))
        print(json.dumps({"items": rows, "count": len(rows), "research_task_created": False, "trade_created": False, "sleeve_created": False}, sort_keys=True))
        return 0
    if args.action == "report":
        rows = list_operator_inbox_items_v1(truth_root=truth_root, include_closed=True)
        report = build_operator_inbox_review_report_v1(truth_root=truth_root, generated_at_utc=updated_at, items=rows)
        validate_operator_inbox_artifact_v1(report)
        path = write_operator_inbox_artifact_v1(truth_root=truth_root, payload=report)
        print(json.dumps({"path": str(path), "item_count": report["item_counts"]["total"], "lineage_gap_count": len(report["lineage_gaps"])}, sort_keys=True))
        return 0

    if not args.inbox_item_id:
        raise SystemExit("--inbox_item_id is required for item actions")
    item = load_operator_inbox_item_v1(truth_root=truth_root, inbox_item_id=args.inbox_item_id)
    if args.action in ACTION_STATUS:
        updated = transition_operator_inbox_item_v1(
            item=item,
            status=ACTION_STATUS[args.action],
            updated_at=updated_at,
            notes=args.notes,
        )
        path = write_operator_inbox_artifact_v1(truth_root=truth_root, payload=updated)
        print(json.dumps({"inbox_item_id": updated["inbox_item_id"], "status": updated["status"], "path": str(path), "research_task_created": False, "trade_created": False, "sleeve_created": False}, sort_keys=True))
        return 0
    if args.action == "promote-to-idea":
        day_utc = args.day_utc or updated_at[:10]
        updated, idea, idea_path = promote_operator_inbox_to_research_idea_v1(
            truth_root=truth_root,
            item=item,
            day_utc=day_utc,
            updated_at=updated_at,
        )
        print(
            json.dumps(
                {
                    "inbox_item_id": updated["inbox_item_id"],
                    "status": updated["status"],
                    "research_idea_id": idea["inbox_id"],
                    "research_idea_path": str(idea_path),
                    "research_task_created": False,
                    "trade_created": False,
                    "sleeve_created": False,
                },
                sort_keys=True,
            )
        )
        return 0
    if args.action == "promote-to-hypothesis":
        if not args.hypothesis_id:
            raise SystemExit("--hypothesis_id is required; create the hypothesis through Research Lab intake first")
        updated = transition_operator_inbox_item_v1(
            item=item,
            status="PROMOTED_TO_HYPOTHESIS",
            updated_at=updated_at,
            promoted_to_hypothesis_id=args.hypothesis_id,
            notes=args.notes or f"Linked to existing Research Lab hypothesis {args.hypothesis_id}. Hypothesis must be created through Research Lab intake.",
        )
        path = write_operator_inbox_artifact_v1(truth_root=truth_root, payload=updated)
        print(
            json.dumps(
                {
                    "inbox_item_id": updated["inbox_item_id"],
                    "status": updated["status"],
                    "hypothesis_id": args.hypothesis_id,
                    "path": str(path),
                    "hypothesis_created_by_inbox": False,
                    "research_task_created": False,
                    "trade_created": False,
                    "sleeve_created": False,
                },
                sort_keys=True,
            )
        )
        return 0
    raise SystemExit(f"unsupported action {args.action}")


if __name__ == "__main__":
    raise SystemExit(main())
