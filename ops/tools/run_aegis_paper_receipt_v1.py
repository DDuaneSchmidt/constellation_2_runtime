#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.human_reviewed_paper_mode_v1 import (  # noqa: E402
    build_paper_review_queue_v1,
    build_paper_trade_outcomes_v1,
    paper_trade_outcomes_path_v1,
    paper_trade_receipts_path_v1,
    record_paper_trade_exit_v1,
    record_paper_trade_receipt_v1,
    write_paper_trade_outcomes_v1,
)
from ops.aegis.paper_position_ledger_v1 import paper_position_ledger_path_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_paper_receipt_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day_utc", "--day-utc", dest="day_utc", required=True)
    parser.add_argument("--candidate-id", default="")
    parser.add_argument("--action", default="BUY")
    parser.add_argument("--paper-entry-price", default="")
    parser.add_argument("--paper-exit-price", default="")
    parser.add_argument("--quantity", default="")
    parser.add_argument("--notional", default="")
    parser.add_argument("--timestamp", default="")
    parser.add_argument("--notes", default="")
    parser.add_argument("--operator", default="David")
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    if str(args.candidate_id).strip() and str(args.paper_exit_price).strip():
        receipt = record_paper_trade_exit_v1(
            truth_root=root,
            day_utc=str(args.day_utc),
            candidate_id=str(args.candidate_id).strip(),
            paper_exit_price=str(args.paper_exit_price).strip(),
            timestamp_utc=str(args.timestamp).strip(),
            operator=str(args.operator).strip() or "David",
            notes=str(args.notes).strip(),
        )
        outcomes = build_paper_trade_outcomes_v1(truth_root=root, day_utc=str(args.day_utc))
        write_paper_trade_outcomes_v1(truth_root=root, day_utc=str(args.day_utc), payload=outcomes)
        print(json.dumps({
            "updated": True,
            "receipt": receipt,
            "paper_trade_receipts_path": str(paper_trade_receipts_path_v1(truth_root=root, day_utc=str(args.day_utc))),
            "paper_trade_outcomes_path": str(paper_trade_outcomes_path_v1(truth_root=root, day_utc=str(args.day_utc))),
            "paper_position_ledger_path": str(paper_position_ledger_path_v1(truth_root=root, day_utc=str(args.day_utc))),
            "paper_only": True,
            "human_review_required": True,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        }, sort_keys=True))
        return 0
    if str(args.candidate_id).strip() and str(args.paper_entry_price).strip() and (str(args.quantity).strip() or str(args.notional).strip()):
        receipt = record_paper_trade_receipt_v1(
            truth_root=root,
            day_utc=str(args.day_utc),
            candidate_id=str(args.candidate_id).strip(),
            action=str(args.action).strip(),
            paper_entry_price=str(args.paper_entry_price).strip(),
            quantity=str(args.quantity).strip(),
            notional=str(args.notional).strip(),
            timestamp_utc=str(args.timestamp).strip(),
            operator=str(args.operator).strip() or "David",
            notes=str(args.notes).strip(),
        )
        outcomes = build_paper_trade_outcomes_v1(truth_root=root, day_utc=str(args.day_utc))
        write_paper_trade_outcomes_v1(truth_root=root, day_utc=str(args.day_utc), payload=outcomes)
        print(json.dumps({
            "updated": True,
            "receipt": receipt,
            "paper_trade_receipts_path": str(paper_trade_receipts_path_v1(truth_root=root, day_utc=str(args.day_utc))),
            "paper_trade_outcomes_path": str(paper_trade_outcomes_path_v1(truth_root=root, day_utc=str(args.day_utc))),
            "paper_position_ledger_path": str(paper_position_ledger_path_v1(truth_root=root, day_utc=str(args.day_utc))),
            "paper_only": True,
            "human_review_required": True,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        }, sort_keys=True))
        return 0
    queue = build_paper_review_queue_v1(truth_root=root, day_utc=str(args.day_utc))
    outcomes = build_paper_trade_outcomes_v1(truth_root=root, day_utc=str(args.day_utc))
    write_paper_trade_outcomes_v1(truth_root=root, day_utc=str(args.day_utc), payload=outcomes)
    eligible = [row for row in (queue.get("rows") or []) if isinstance(row, dict) and str(row.get("status") or "") == "APPROVED_FOR_PAPER"]
    print(json.dumps({
        "updated": False,
        "paper_trade_receipts_path": str(paper_trade_receipts_path_v1(truth_root=root, day_utc=str(args.day_utc))),
        "paper_trade_outcomes_path": str(paper_trade_outcomes_path_v1(truth_root=root, day_utc=str(args.day_utc))),
        "eligible_candidates": eligible,
        "rows": outcomes.get("open_trades") or [],
        "paper_only": True,
        "human_review_required": True,
        "note": "Pass --candidate-id, --paper-entry-price, and --quantity or --notional to record a SIMULATED_PAPER entry, or --paper-exit-price to record a simulated paper exit.",
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
