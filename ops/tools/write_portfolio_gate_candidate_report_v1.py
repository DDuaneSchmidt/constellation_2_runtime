#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.portfolio_gate_candidate_report_v1 import build_portfolio_gate_candidate_report_v1, write_portfolio_gate_candidate_report_v1
from ops.tools.run_portfolio_activation_gate_v1 import PAPER_MODE


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_portfolio_gate_candidate_report_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--gate_path", default="")
    parser.add_argument("--scoring_path", default="")
    parser.add_argument("--environment", default=PAPER_MODE)
    args = parser.parse_args(argv)
    truth_root = Path(args.truth_root).expanduser().resolve()
    report = build_portfolio_gate_candidate_report_v1(
        day_utc=str(args.day_utc),
        truth_root=truth_root,
        gate_path=Path(args.gate_path).expanduser().resolve() if str(args.gate_path).strip() else None,
        scoring_path=Path(args.scoring_path).expanduser().resolve() if str(args.scoring_path).strip() else None,
    )
    written = write_portfolio_gate_candidate_report_v1(truth_root=truth_root, report=report)
    print(json.dumps({
        "status": "PASS",
        "path": written["artifact_path"],
        "selected_candidate_id": written["selected_candidate_id"],
        "suppressed_count": written["suppressed_count"],
        "trades_created": False,
        "paper_submit_created": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
