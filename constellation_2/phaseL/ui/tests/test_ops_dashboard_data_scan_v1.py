#!/usr/bin/env python3
"""
Acceptance test: Phase L Ops Dashboard data scan (no network, no IB).
- Ensures data scan does not crash
- Ensures days list is returned
- Ensures day submissions response is JSON-serializable and includes missing_paths list
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Correct repo root resolution:
# .../constellation_2/phaseL/ui/tests/test_ops_dashboard_data_scan_v1.py
# parents:
# [tests, ui, phaseL, constellation_2, <repo_root>, ...]
REPO_ROOT = Path(__file__).resolve().parents[4]

server_path = REPO_ROOT / "constellation_2/phaseL/ui/server/run_ops_dashboard_v1.py"
if not server_path.exists():
    print("FAIL: SERVER_FILE_MISSING:", server_path)
    raise SystemExit(2)

sys.path.insert(0, str(REPO_ROOT))

from constellation_2.phaseL.ui.server.run_ops_dashboard_v1 import _days_list, _day_submissions  # type: ignore


def main() -> int:
    days_resp = _days_list()
    if not isinstance(days_resp, dict):
        print("FAIL: DAYS_RESPONSE_NOT_DICT")
        return 2
    if "days" not in days_resp or not isinstance(days_resp["days"], list):
        print("FAIL: DAYS_RESPONSE_MISSING_DAYS_LIST")
        return 2

    days = days_resp["days"]
    if not days:
        json.dumps(days_resp, sort_keys=True)
        if not days_resp.get("warnings"):
            print("FAIL: NO_DAYS_BUT_NO_WARNINGS")
            return 2
        print("OK: no days present; warnings surfaced:", days_resp.get("warnings"))
        return 0

    day = days[-1]
    subs_resp = _day_submissions(day)
    if not isinstance(subs_resp, dict):
        print("FAIL: SUBMISSIONS_RESPONSE_NOT_DICT")
        return 2
    if "missing_paths" not in subs_resp or not isinstance(subs_resp["missing_paths"], list):
        print("FAIL: SUBMISSIONS_RESPONSE_MISSING_MISSING_PATHS_LIST")
        return 2
    if "submissions" not in subs_resp or not isinstance(subs_resp["submissions"], list):
        print("FAIL: SUBMISSIONS_RESPONSE_MISSING_SUBMISSIONS_LIST")
        return 2

    json.dumps(subs_resp, sort_keys=True)

    print("OK: test_ops_dashboard_data_scan_v1 day=", day, "subs=", len(subs_resp["submissions"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
