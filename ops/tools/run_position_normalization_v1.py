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

from constellation_2.common.paper_session_fact_plane_v1 import canonical_paper_session_id_v1, now_utc_iso_v1
from constellation_2.common.position_normalization_v1 import (
    derive_position_normalization_payload_v1,
    write_position_normalization_v1,
)
from constellation_2.common.runtime_ledger_v1 import append_runtime_ledger_events_v1, projection_over_runtime_ledger_v1


def _resolve_truth_root(raw: str) -> Path:
    truth_root = Path(str(raw or "").strip()).expanduser().resolve()
    if not truth_root.is_absolute():
        raise SystemExit(f"FAIL: ABSOLUTE_TRUTH_ROOT_REQUIRED:{truth_root}")
    return truth_root


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_position_normalization_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=True)
    ap.add_argument("--position_id", required=True)
    ap.add_argument("--origin", required=True)
    ap.add_argument("--entry_price", required=True)
    ap.add_argument("--initial_stop_price", required=True)
    ap.add_argument("--initial_r_value", required=True)
    ap.add_argument("--reference_entry_source", default="")
    ap.add_argument("--synthetic_stop_source", default="")
    ap.add_argument("--scoring_eligibility", default="")
    args = ap.parse_args(argv)

    truth_root = _resolve_truth_root(args.truth_root)
    payload = derive_position_normalization_payload_v1(
        day_utc=args.day_utc,
        position_id=args.position_id,
        origin=args.origin,
        entry_price=args.entry_price,
        initial_stop_price=args.initial_stop_price,
        initial_r_value=args.initial_r_value,
        reference_entry_source=args.reference_entry_source,
        synthetic_stop_source=args.synthetic_stop_source,
        scoring_eligibility=args.scoring_eligibility,
    )
    ref = write_position_normalization_v1(truth_root=truth_root, payload=payload)
    session_id = canonical_paper_session_id_v1(args.day_utc)
    owner_run_id = f"position_normalization_v1:{args.day_utc}:{args.position_id}"
    append_result = append_runtime_ledger_events_v1(
        truth_root=truth_root,
        day_utc=args.day_utc,
        events=[
            {
                "event_type": "POSITION_NORMALIZATION_RECORDED",
                "produced_utc": now_utc_iso_v1(),
                "owner_plane": "POSITION_NORMALIZATION_PLANE",
                "owner_tool": "ops/tools/run_position_normalization_v1.py",
                "owner_run_id": owner_run_id,
                "run_id": owner_run_id,
                "session_id": session_id,
                "submission_id": "",
                "order_id": "",
                "perm_id": "",
                "payload_ref": str(ref.path),
                "payload_hash": ref.sha256,
                "identity_key": f"{session_id}:POSITION_NORMALIZATION:{args.position_id}",
                "identity_tuple": {
                    "day_utc": args.day_utc,
                    "owner_plane": "POSITION_NORMALIZATION_PLANE",
                    "owner_run_id": owner_run_id,
                    "session_id": session_id,
                    "submission_id": "",
                    "order_id": "",
                    "perm_id": "",
                },
                "event_payload_summary": {
                    "position_id": args.position_id,
                    "origin": payload["origin"],
                    "risk_basis": payload["risk_basis"],
                    "normalization_status": payload["normalization_status"],
                    "scoring_eligibility": payload["scoring_eligibility"],
                },
            }
        ],
    )
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "status": payload["normalization_status"],
                "risk_basis": payload["risk_basis"],
                "runtime_ledger_projection": projection_over_runtime_ledger_v1(
                    truth_root=truth_root,
                    day_utc=args.day_utc,
                    append_result=append_result,
                    projection_notice="Position normalization is canonical runtime truth; this envelope is a projection.",
                ),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
