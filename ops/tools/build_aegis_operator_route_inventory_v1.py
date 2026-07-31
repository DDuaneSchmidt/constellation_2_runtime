from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.operator_route_inventory_v1 import build_operator_route_inventory_v1, write_operator_route_inventory_v1  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", required=True)
    parser.add_argument("--day", required=True)
    args = parser.parse_args()
    payload = build_operator_route_inventory_v1(truth_root=args.truth_root, day_utc=args.day)
    path = write_operator_route_inventory_v1(truth_root=args.truth_root, day_utc=args.day, payload=payload)
    print(json.dumps({"ok": True, "day_utc": args.day, "path": str(path), "summary": payload.get("summary"), **payload.get("safety", {})}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
