#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.candidate_to_paper_lifecycle_v1 import build_candidate_to_paper_lifecycle_v1, write_candidate_to_paper_lifecycle_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_candidate_to_paper_lifecycle_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", "--day_utc", dest="day_utc", required=True)
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_candidate_to_paper_lifecycle_v1(truth_root=root, day_utc=str(args.day_utc))
    paths = write_candidate_to_paper_lifecycle_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({"ok": True, "path": paths["json"], "day_utc": str(args.day_utc), "summary": payload.get("summary") or {}, "safety": payload.get("safety") or {}}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
