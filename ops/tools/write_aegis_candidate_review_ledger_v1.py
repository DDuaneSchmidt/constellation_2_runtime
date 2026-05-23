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

from ops.aegis.candidate_review_ledger_v1 import build_candidate_review_ledger_v1, write_candidate_review_ledger_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_candidate_review_ledger_v1")
    parser.add_argument("--truth-root", "--truth_root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--filter", default="all", choices=["active", "watchlisted", "dismissed", "needs_more_evidence", "needs-more-evidence", "expired", "all"])
    args = parser.parse_args(argv)
    payload = build_candidate_review_ledger_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), filter_name=str(args.filter))
    paths = write_candidate_review_ledger_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(
        json.dumps(
            {
                **paths,
                "candidate_count": payload["candidate_count"],
                "filtered_candidate_count": payload["filtered_candidate_count"],
                "active_count": payload["active_count"],
                "watchlisted_count": payload["watchlisted_count"],
                "dismissed_count": payload["dismissed_count"],
                "needs_more_evidence_count": payload["needs_more_evidence_count"],
                "expired_count": payload["expired_count"],
                "expired_watchlisted_count": payload["expired_watchlisted_count"],
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
                "automatic_approval_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
