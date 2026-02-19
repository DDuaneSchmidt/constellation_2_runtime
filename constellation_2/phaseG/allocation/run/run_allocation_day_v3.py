#!/usr/bin/env python3
"""
run_allocation_day_v3.py

Wrapper: Allocation with audit-grade bootstrap sys.path injection.
Delegates to run_allocation_day_v2.main().

Reason:
- Avoid ModuleNotFoundError when invoked as a file path.
- No semantic change from v2.
"""

from __future__ import annotations

import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT_FROM_FILE = _THIS_FILE.parents[4]  # .../constellation_2/phaseG/allocation/run -> constellation_2 -> repo root
if str(_REPO_ROOT_FROM_FILE) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT_FROM_FILE))

if not (_REPO_ROOT_FROM_FILE / "constellation_2").exists():
    raise SystemExit(f"FATAL: repo_root_missing_constellation_2: derived={_REPO_ROOT_FROM_FILE}")
if not (_REPO_ROOT_FROM_FILE / "governance").exists():
    raise SystemExit(f"FATAL: repo_root_missing_governance: derived={_REPO_ROOT_FROM_FILE}")

from constellation_2.phaseG.allocation.run.run_allocation_day_v2 import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
