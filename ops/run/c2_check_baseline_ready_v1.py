#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    target = repo_root / "ops" / "run" / "c2_paper_day_orchestrator_systemd_entry_v1.sh"
    print(f"FAIL: LEGACY_ENTRYPOINT_QUARANTINED use {target}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
