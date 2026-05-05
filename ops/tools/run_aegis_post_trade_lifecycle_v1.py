#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.aegis_truth.post_trade_lifecycle_v1 import materialize_post_trade_lifecycle_v1
from constellation_2.common.runtime_path_authority_v1 import require_authoritative_repo_runtime_v1


DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")
DEFAULT_EXECUTION_ROOT = Path("/home/node/constellation_runtime_data/truth_sleeves/PRIMARY/PAPER")


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize governed PAPER post-trade lifecycle and reconciliation reports.")
    parser.add_argument("--day_utc", "--target-day", dest="day_utc", default=datetime.now(UTC).date().isoformat())
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--execution_root", "--execution-root", dest="execution_root", default=str(DEFAULT_EXECUTION_ROOT))
    parser.add_argument("--evaluation_utc", "--evaluation-utc", dest="evaluation_utc", default="")
    parser.add_argument("--max_broker_event_age_seconds", type=int, default=900)
    parser.add_argument("--skip_runtime_guard", action="store_true", help="Test-only escape hatch; production runs must use release-first runtime guard.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if not args.skip_runtime_guard:
        require_authoritative_repo_runtime_v1(REPO_ROOT)

    result = materialize_post_trade_lifecycle_v1(
        day_utc=args.day_utc,
        truth_root=Path(args.truth_root),
        execution_root=Path(args.execution_root),
        evaluation_utc=str(args.evaluation_utc or "").strip() or None,
        max_broker_event_age_seconds=int(args.max_broker_event_age_seconds),
    )
    payload = {
        "status": result.lifecycle_report["status"],
        "final_state": result.lifecycle_report["final_state"],
        "first_blocker": result.lifecycle_report["first_blocker"],
        "operator_next_action": result.lifecycle_report["operator_next_action"],
        "lifecycle_report_path": str(result.lifecycle_report_path),
        "reconciliation_report_path": str(result.reconciliation_report_path),
        "order_lifecycle_paths": [str(path) for path in result.order_lifecycle_paths],
    }
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(
            " ".join(
                [
                    f"POST_TRADE_{payload['status']}",
                    f"day_utc={args.day_utc}",
                    f"final_state={payload['final_state']}",
                    f"first_blocker={payload['first_blocker']}",
                    f"lifecycle_report_path={payload['lifecycle_report_path']}",
                ]
            )
        )
    return 0 if result.lifecycle_report["status"] in {"PASS", "OPEN"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
