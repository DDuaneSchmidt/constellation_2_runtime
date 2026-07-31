#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.intelligence_common_v1 import read_json_v1
from ops.aegis.run_history_v1 import append_candidate_diagnostics_run_history_v1, run_history_path_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_run_history_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day_utc)
    diagnostics_path = root / "reports" / "aegis_candidate_generation_diagnostics_v1" / day / "candidate_generation_diagnostics.v1.json"
    contracts_path = root / "reports" / "aegis_candidate_contracts_v1" / day / "candidate_contracts.v1.json"
    queue_path = root / "reports" / "aegis_paper_review_queue_v1" / day / "paper_review_queue.v1.json"
    diagnostics = read_json_v1(diagnostics_path)
    if not diagnostics:
        raise SystemExit(f"candidate diagnostics missing: {diagnostics_path}")
    payload = append_candidate_diagnostics_run_history_v1(
        truth_root=root,
        day_utc=day,
        command="npm run aegis:candidate-diagnostics",
        diagnostics=diagnostics,
        diagnostics_path=str(diagnostics_path),
        candidate_contracts_path=str(contracts_path),
        paper_review_queue_path=str(queue_path),
    )
    print(json.dumps({"day_utc": day, "path": str(run_history_path_v1(truth_root=root, day_utc=day)), "latest_run_id": payload.get("latest_run_id"), "run_count": payload.get("run_count"), "broker_execution_allowed": False, "autonomous_execution_allowed": False, "trade_advice_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
