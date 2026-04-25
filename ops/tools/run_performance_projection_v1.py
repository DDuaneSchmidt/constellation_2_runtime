#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = _THIS_FILE.parents[2].resolve()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.execution_journal_v1 import read_execution_journal_v1
from constellation_2.common.paper_session_fact_plane_v1 import now_utc_iso_v1, resolve_fact_plane_truth_root_v1
from constellation_2.common.performance_projection_v1 import (
    build_performance_projection_v1,
    write_performance_projection_v1,
)
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_performance_projection_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument(
        "--truth_root",
        default=str(resolve_canonical_truth_root().resolve()),
    )
    args = ap.parse_args(argv)

    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    day_utc = str(args.day_utc).strip()
    journal_ref = read_execution_journal_v1(truth_root=truth_root, day_utc=day_utc)
    payload = build_performance_projection_v1(
        journal_payload=dict(journal_ref.payload),
        journal_ref=str(journal_ref.path),
        journal_sha256=str(journal_ref.sha256),
        journal_generated_at_utc=str(journal_ref.payload.get("generated_at_utc") or "").strip(),
        generated_at_utc=now_utc_iso_v1(),
        producer_module="ops/tools/run_performance_projection_v1.py",
    )
    ref = write_performance_projection_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "report_path": str(ref.path),
                "projection_id": str(payload.get("projection_id") or ""),
                "overall_wall_time_ms": payload.get("overall_wall_time_ms"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
