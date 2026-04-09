#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.operator_summary_v1 import write_operator_summary
from constellation_2.common.paper_session_fact_plane_v1 import resolve_fact_plane_truth_root_v1


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_operator_summary_v1")
    ap.add_argument("--summary_kind", required=True)
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", default="")
    args = ap.parse_args(argv)

    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    ref = write_operator_summary(summary_kind=args.summary_kind, day_utc=args.day_utc, truth_root=truth_root)
    payload = dict(ref.payload)
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "sha256": ref.sha256,
                "summary_kind": str(payload.get("summary_kind") or "").strip(),
                "control_plane_id": str(payload.get("control_plane_id") or "").strip(),
                "state_machine_id": str(payload.get("state_machine_id") or "").strip(),
                "final_start_decision": str(payload.get("final_start_decision") or "").strip(),
                "ledger_id": str(payload.get("ledger_id") or "").strip(),
                "authority_status": str(payload.get("authority_status") or "").strip(),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
