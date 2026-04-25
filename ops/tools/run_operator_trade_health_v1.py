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

from constellation_2.common.operator_trade_health_v1 import (
    materialize_operator_trade_health_for_scope_v1,
    materialize_operator_trade_health_v1,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_operator_trade_health_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--trade_identity_id", required=True)
    parser.add_argument("--core2_materialization_set_id", required=True)
    parser.add_argument("--core3_materialization_set_id", required=True)
    parser.add_argument("--environment", default="PAPER")
    parser.add_argument("--sleeve_id", default="PRIMARY")
    parser.add_argument("--execution_root", default="")
    parser.add_argument("--core4_boundary_path", default="")
    parser.add_argument("--evaluation_utc", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    if str(args.execution_root or "").strip():
        result = materialize_operator_trade_health_v1(
            repo_root=REPO_ROOT,
            execution_root_path=Path(str(args.execution_root).strip()).expanduser().resolve(),
            day_utc=str(args.day_utc),
            trade_identity_id=str(args.trade_identity_id).strip(),
            core2_materialization_set_id=str(args.core2_materialization_set_id).strip(),
            core3_materialization_set_id=str(args.core3_materialization_set_id).strip(),
            core4_boundary_path=str(args.core4_boundary_path or "").strip(),
            evaluation_utc=str(args.evaluation_utc or "").strip(),
        )
    else:
        result = materialize_operator_trade_health_for_scope_v1(
            repo_root=REPO_ROOT,
            environment=str(args.environment or "").strip().upper(),
            sleeve_id=str(args.sleeve_id or "").strip().upper(),
            day_utc=str(args.day_utc),
            trade_identity_id=str(args.trade_identity_id).strip(),
            core2_materialization_set_id=str(args.core2_materialization_set_id).strip(),
            core3_materialization_set_id=str(args.core3_materialization_set_id).strip(),
            core4_boundary_path=str(args.core4_boundary_path or "").strip(),
            evaluation_utc=str(args.evaluation_utc or "").strip(),
        )

    payload = dict(result.summary)
    output = {
        "materialization_set_id": result.materialization_set_id,
        "trade_identity_id": result.trade_identity_id,
        "snapshot_binding_path": str(result.snapshot_binding_path),
        "operator_health_path": str(result.operator_health_path),
        "summary_provenance_path": str(result.provenance_path),
        "timeline_view_path": str(result.timeline_view_path),
        "overall_operator_state": str(payload.get("overall_operator_state") or ""),
        "highest_severity": str(payload.get("highest_severity") or ""),
        "required_operator_action": str(payload.get("required_operator_action") or ""),
        "blocked_reason_class": str(payload.get("blocked_reason_class") or ""),
        "review_reason_class": str(payload.get("review_reason_class") or ""),
        "stale_degraded_class": str(payload.get("stale_degraded_class") or ""),
    }
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
