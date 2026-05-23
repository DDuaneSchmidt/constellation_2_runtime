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

from ops.aegis.research_lab.hypothesis_priority_engine_v1 import (  # noqa: E402
    build_hypothesis_priority_report_v1,
    write_hypothesis_priority_report_v1,
)
from ops.aegis.research_lab.research_pipeline_v1 import build_research_pipeline_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="build_hypothesis_priority_report_v1")
    parser.add_argument("--truth_root", "--truth-root", dest="truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", "--day-utc", dest="day_utc", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    pipeline = build_research_pipeline_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc))
    payload = build_hypothesis_priority_report_v1(pipeline_payload=pipeline, day_utc=str(args.day_utc))
    paths = write_hypothesis_priority_report_v1(truth_root=Path(args.truth_root), day_utc=str(args.day_utc), payload=payload)
    print(
        json.dumps(
            {
                **paths,
                "hypothesis_count": len(payload.get("ranked_hypotheses") or []),
                "top_ranked_hypotheses": [
                    {
                        "rank": row.get("rank"),
                        "hypothesis_id": row.get("hypothesis_id"),
                        "title": row.get("title"),
                        "tier": row.get("tier"),
                        "priority_score": row.get("priority_score"),
                    }
                    for row in (payload.get("ranked_hypotheses") or [])[:5]
                ],
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
                "trade_advice_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
