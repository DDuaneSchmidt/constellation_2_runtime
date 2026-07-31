#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.lite_migration_equivalence_v1 import build_and_write_lite_migration_equivalence_v1, validate_lite_migration_equivalence_v1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_lite_migration_equivalence_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", required=True)
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    parser.add_argument("--require-pass", action="store_true")
    args = parser.parse_args(argv)
    payload, path = build_and_write_lite_migration_equivalence_v1(truth_root=Path(args.truth_root), day_utc=args.day_utc)
    failures = validate_lite_migration_equivalence_v1(payload)
    if args.require_pass and (failures or not payload.get("runtime_truth_migration_safe")):
        print(json.dumps({"ok": False, "path": str(path), "failures": failures, "blocking_comparisons": payload.get("blocking_comparisons", [])}, indent=2, sort_keys=True))
        return 1
    print(json.dumps({"ok": not failures, "path": str(path), "runtime_truth_migration_safe": payload.get("runtime_truth_migration_safe"), "blocking_comparison_count": payload.get("blocking_comparison_count"), "failures": failures}, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
