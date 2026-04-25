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

from constellation_2.common.alerts_projection_v1 import (
    build_alerts_projection_v1,
    write_alerts_projection_v1,
)
from constellation_2.common.runtime_contract_v1 import resolve_canonical_truth_root
from constellation_2.common.paper_session_fact_plane_v1 import (
    now_utc_iso_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
)
from constellation_2.common.paper_session_path_alignment_v1 import (
    resolve_current_system_projection_path,
)
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


CURRENT_PROJECTION_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/current_system_projection.v1.schema.json"
DEFAULT_TRUTH_ROOT = resolve_canonical_truth_root().resolve()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_alerts_projection_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument(
        "--truth_root",
        default=str(DEFAULT_TRUTH_ROOT),
    )
    args = ap.parse_args(argv)

    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    day_utc = str(args.day_utc).strip()
    current_projection_path = resolve_current_system_projection_path(truth_root=truth_root, day_utc=day_utc)
    current_projection_payload = read_json_object_v1(current_projection_path)
    validate_against_repo_schema_v1(current_projection_payload, REPO_ROOT, CURRENT_PROJECTION_SCHEMA_RELPATH)

    payload = build_alerts_projection_v1(
        current_projection_payload={
            **current_projection_payload,
            "projection_ref": str(current_projection_path),
        },
        generated_at_utc=now_utc_iso_v1(),
        producer_module="ops/tools/run_alerts_projection_v1.py",
    )
    ref = write_alerts_projection_v1(truth_root=truth_root, payload=payload)
    print(
        json.dumps(
            {
                "report_path": str(ref.path),
                "projection_id": str(payload.get("projection_id") or ""),
                "first_actionable_alert": payload.get("first_actionable_alert"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
