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

from constellation_2.common.exit_decision_engine_v1 import EXIT_DECISION_SCHEMA_RELPATH, resolve_exit_decision_path
from constellation_2.common.paper_session_fact_plane_v1 import (
    canonical_paper_session_id_v1,
    now_utc_iso_v1,
    read_validated_surface_v1,
)
from constellation_2.common.position_normalization_v1 import (
    POSITION_NORMALIZATION_SCHEMA_RELPATH,
    resolve_position_normalization_path,
)
from constellation_2.common.runtime_ledger_v1 import append_runtime_ledger_events_v1, projection_over_runtime_ledger_v1
from constellation_2.common.trade_result_closure_v1 import derive_trade_result_payload_v1, write_trade_result_v1


def _resolve_truth_root(raw: str) -> Path:
    truth_root = Path(str(raw or "").strip()).expanduser().resolve()
    if not truth_root.is_absolute():
        raise SystemExit(f"FAIL: ABSOLUTE_TRUTH_ROOT_REQUIRED:{truth_root}")
    return truth_root


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_trade_result_closure_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=True)
    ap.add_argument("--position_id", required=True)
    ap.add_argument("--exit_price", required=True)
    ap.add_argument("--entry_time_utc", default="")
    ap.add_argument("--exit_time_utc", default="")
    ap.add_argument("--mfe_r", default="")
    ap.add_argument("--mae_r", default="")
    args = ap.parse_args(argv)

    truth_root = _resolve_truth_root(args.truth_root)
    normalization_ref = read_validated_surface_v1(
        path=resolve_position_normalization_path(
            truth_root=truth_root,
            day_utc=args.day_utc,
            position_id=args.position_id,
        ),
        schema_relpath=POSITION_NORMALIZATION_SCHEMA_RELPATH,
    )
    exit_decision_ref = read_validated_surface_v1(
        path=resolve_exit_decision_path(
            truth_root=truth_root,
            day_utc=args.day_utc,
            position_id=args.position_id,
        ),
        schema_relpath=EXIT_DECISION_SCHEMA_RELPATH,
    )
    payload = derive_trade_result_payload_v1(
        day_utc=args.day_utc,
        position_id=args.position_id,
        normalization_payload=normalization_ref.payload,
        exit_decision_payload=exit_decision_ref.payload,
        exit_price=args.exit_price,
        entry_time_utc=args.entry_time_utc,
        exit_time_utc=args.exit_time_utc,
        mfe_r=args.mfe_r,
        mae_r=args.mae_r,
        provenance_refs=[
            {
                "logical_name": "position_normalization_v1",
                "artifact_path": str(normalization_ref.path),
                "artifact_sha256": normalization_ref.sha256,
            },
            {
                "logical_name": "exit_decision_v1",
                "artifact_path": str(exit_decision_ref.path),
                "artifact_sha256": exit_decision_ref.sha256,
            },
        ],
    )
    ref = write_trade_result_v1(truth_root=truth_root, payload=payload)
    session_id = canonical_paper_session_id_v1(args.day_utc)
    owner_run_id = f"trade_result_closure_v1:{args.day_utc}:{args.position_id}"
    append_result = append_runtime_ledger_events_v1(
        truth_root=truth_root,
        day_utc=args.day_utc,
        events=[
            {
                "event_type": "TRADE_RESULT_RECORDED",
                "produced_utc": now_utc_iso_v1(),
                "owner_plane": "TRADE_RESULT_CLOSURE_PLANE",
                "owner_tool": "ops/tools/run_trade_result_closure_v1.py",
                "owner_run_id": owner_run_id,
                "run_id": owner_run_id,
                "session_id": session_id,
                "submission_id": "",
                "order_id": "",
                "perm_id": "",
                "payload_ref": str(ref.path),
                "payload_hash": ref.sha256,
                "identity_key": f"{session_id}:TRADE_RESULT:{args.position_id}",
                "identity_tuple": {
                    "day_utc": args.day_utc,
                    "owner_plane": "TRADE_RESULT_CLOSURE_PLANE",
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
                    "realized_r": payload["realized_r"],
                    "analytics_eligibility": payload["analytics_eligibility"],
                },
            }
        ],
    )
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "realized_r": payload["realized_r"],
                "analytics_eligibility": payload["analytics_eligibility"],
                "runtime_ledger_projection": projection_over_runtime_ledger_v1(
                    truth_root=truth_root,
                    day_utc=args.day_utc,
                    append_result=append_result,
                    projection_notice="Trade-result closure is canonical runtime truth; this envelope is a projection.",
                ),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
