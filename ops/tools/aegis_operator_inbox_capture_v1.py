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
    capture_operator_inbox_item_v1,
    now_utc_v1,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="aegis_operator_inbox_capture_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--description", required=True)
    parser.add_argument("--category", required=True)
    parser.add_argument("--source", default="MANUAL")
    parser.add_argument("--priority", default="NORMAL")
    parser.add_argument("--tags", default="")
    parser.add_argument("--related_refs", default="")
    parser.add_argument("--notes", default="")
    parser.add_argument("--created_at", default="")
    args = parser.parse_args(argv)

    item, path = capture_operator_inbox_item_v1(
        truth_root=Path(args.truth_root),
        title=args.title,
        description=args.description,
        category=args.category,
        source=args.source,
        priority=args.priority,
        tags=args.tags,
        related_artifact_refs=args.related_refs,
        notes=args.notes,
        created_at=args.created_at or now_utc_v1(),
    )
    print(
        json.dumps(
            {
                "inbox_item_id": item["inbox_item_id"],
                "status": item["status"],
                "category": item["category"],
                "path": str(path),
                "research_task_created": False,
                "trade_created": False,
                "sleeve_created": False,
                "broker_submit_required": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
