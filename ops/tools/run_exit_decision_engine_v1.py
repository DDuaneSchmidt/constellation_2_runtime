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

from constellation_2.common.exit_decision_engine_v1 import (
    derive_exit_decision_payload_v1,
    resolve_exit_decision_path,
    write_exit_decision_v1,
)
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


def _resolve_truth_root(raw: str) -> Path:
    truth_root = Path(str(raw or "").strip()).expanduser().resolve()
    if not truth_root.is_absolute():
        raise SystemExit(f"FAIL: ABSOLUTE_TRUTH_ROOT_REQUIRED:{truth_root}")
    return truth_root


def _yes_no(raw: str) -> bool:
    value = str(raw or "").strip().upper()
    if value not in {"YES", "NO"}:
        raise SystemExit(f"FAIL: INVALID_BOOLEAN_CHOICE:{raw}")
    return value == "YES"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run_exit_decision_engine_v1")
    ap.add_argument("--day_utc", required=True)
    ap.add_argument("--truth_root", required=True)
    ap.add_argument("--position_id", required=True)
    ap.add_argument("--side", required=True)
    ap.add_argument("--quantity", required=True)
    ap.add_argument("--mark_price", required=True)
    ap.add_argument("--current_stop_price", default="")
    ap.add_argument("--structure_stop_price", default="")
    ap.add_argument("--volatility_stop_price", default="")
    ap.add_argument("--partial_taken", default="NO")
    ap.add_argument("--time_stop_reached", default="NO")
    ap.add_argument("--regime_invalidated", default="NO")
    ap.add_argument("--override_action", default="")
    ap.add_argument("--execution_status", default="NONE")
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
    payload = derive_exit_decision_payload_v1(
        day_utc=args.day_utc,
        position_id=args.position_id,
        normalization_payload=normalization_ref.payload,
        side=args.side,
        quantity=args.quantity,
        mark_price=args.mark_price,
        current_stop_price=args.current_stop_price,
        structure_stop_price=args.structure_stop_price,
        volatility_stop_price=args.volatility_stop_price,
        partial_taken=_yes_no(args.partial_taken),
        time_stop_reached=_yes_no(args.time_stop_reached),
        regime_invalidated=_yes_no(args.regime_invalidated),
        override_action=args.override_action,
        execution_status=args.execution_status,
        provenance_refs=[
            {
                "logical_name": "position_normalization_v1",
                "artifact_path": str(normalization_ref.path),
                "artifact_sha256": normalization_ref.sha256,
            }
        ],
    )
    ref = write_exit_decision_v1(truth_root=truth_root, payload=payload)
    session_id = canonical_paper_session_id_v1(args.day_utc)
    owner_run_id = f"exit_decision_engine_v1:{args.day_utc}:{args.position_id}"
    append_result = append_runtime_ledger_events_v1(
        truth_root=truth_root,
        day_utc=args.day_utc,
        events=[
            {
                "event_type": "EXIT_DECISION_RECORDED",
                "produced_utc": now_utc_iso_v1(),
                "owner_plane": "EXIT_DECISION_PLANE",
                "owner_tool": "ops/tools/run_exit_decision_engine_v1.py",
                "owner_run_id": owner_run_id,
                "run_id": owner_run_id,
                "session_id": session_id,
                "submission_id": "",
                "order_id": "",
                "perm_id": "",
                "payload_ref": str(ref.path),
                "payload_hash": ref.sha256,
                "identity_key": f"{session_id}:EXIT_DECISION:{args.position_id}",
                "identity_tuple": {
                    "day_utc": args.day_utc,
                    "owner_plane": "EXIT_DECISION_PLANE",
                    "owner_run_id": owner_run_id,
                    "session_id": session_id,
                    "submission_id": "",
                    "order_id": "",
                    "perm_id": "",
                },
                "event_payload_summary": {
                    "position_id": args.position_id,
                    "management_state": payload["management_state"],
                    "decision_action": payload["decision_action"],
                    "risk_basis": payload["risk_basis"],
                    "origin": payload["origin"],
                },
            }
        ],
    )
    print(
        json.dumps(
            {
                "path": str(ref.path),
                "management_state": payload["management_state"],
                "decision_action": payload["decision_action"],
                "runtime_ledger_projection": projection_over_runtime_ledger_v1(
                    truth_root=truth_root,
                    day_utc=args.day_utc,
                    append_result=append_result,
                    projection_notice="Exit decisions are canonical runtime truth; this envelope is a projection.",
                ),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
