#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.context_requirement_profile_v1 import (  # noqa: E402
    build_context_requirement_profile_v1,
    write_context_requirement_profile_v1,
)
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_aegis_context_requirement_profile_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day_utc", "--day", "--day-utc", dest="day_utc", required=True)
    parser.add_argument("--active-profile", default="")
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_context_requirement_profile_v1(
        truth_root=root,
        day_utc=str(args.day_utc),
        active_profile_id=str(args.active_profile or "") or None,
    )
    paths = write_context_requirement_profile_v1(truth_root=root, day_utc=str(args.day_utc), payload=payload)
    print(json.dumps({**paths, "active_profile_id": payload.get("active_profile_id")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
