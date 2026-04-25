#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.tools import run_market_calendar_coverage_authority_v1 as authority_runner


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if "--mode" not in args:
        args.extend(["--mode", "CHECK"])
    return authority_runner.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
