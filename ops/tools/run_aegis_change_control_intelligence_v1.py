from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from ops.aegis.change_control_intelligence_v1 import (
    advisor_path_v1,
    ai_review_path_v1,
    build_all_v1,
    snapshot_path_v1,
    validate_intelligence_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build or validate Aegis Change Control Intelligence V1")
    parser.add_argument("mode", choices=["build", "self-check"], nargs="?", default="build")
    parser.add_argument("--truth-root", default=os.environ.get("AEGIS_TRUTH_ROOT", "/home/node/constellation_runtime_data/truth"))
    parser.add_argument("--day", default=os.environ.get("TARGET_DAY") or os.environ.get("DAY") or "2026-05-30")
    args = parser.parse_args()
    truth_root = Path(args.truth_root)
    if args.mode == "build":
        result = build_all_v1(truth_root=truth_root, day=args.day, write=True)
        print(json.dumps({"ok": result["ok"], "paths": result["paths"], "top_recommended_record_id": result["advisor_score"].get("top_recommended_record_id"), "validation": result["validation"]}, indent=2, sort_keys=True))
        return 0 if result["ok"] else 1
    snapshot = json.loads(snapshot_path_v1(truth_root, args.day).read_text(encoding="utf-8"))
    advisor = json.loads(advisor_path_v1(truth_root, args.day).read_text(encoding="utf-8"))
    review = json.loads(ai_review_path_v1(truth_root, args.day).read_text(encoding="utf-8"))
    validation = validate_intelligence_v1(snapshot, advisor, review)
    print(json.dumps(validation, indent=2, sort_keys=True))
    return 0 if validation["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
