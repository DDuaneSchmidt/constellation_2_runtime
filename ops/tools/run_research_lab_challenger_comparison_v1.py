#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.challengers.challenger_comparison import write_challenger_comparison_report  # noqa: E402


DEFAULT_CHALLENGER_EVIDENCE_BATCH_ID = "cheb_26e3b1c8da0edff1"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_challenger_comparison_v1")
    parser.add_argument("--challenger-evidence-batch-id", default=DEFAULT_CHALLENGER_EVIDENCE_BATCH_ID)
    parser.add_argument("--challenger-track-id", default=None)
    parser.add_argument("--store-root", default=None)
    parser.add_argument("--actor", default="Aegis")
    args = parser.parse_args(argv)
    store_root = Path(args.store_root).resolve() if args.store_root else None
    result = write_challenger_comparison_report(
        challenger_evidence_batch_id=args.challenger_evidence_batch_id,
        challenger_track_id=args.challenger_track_id,
        store_root=store_root,
        actor=args.actor,
    )
    report = result["report"]
    active = [row for row in report["comparison_table"] if row["entity_type"] == "challenger" and row["exclusion_status"] == "included_for_human_review"]
    excluded = [row for row in report["comparison_table"] if row["entity_type"] != "incumbent" and row["exclusion_status"] == "excluded"]
    print(
        json.dumps(
            {
                "challenger_comparison_report_id": report["challenger_comparison_report_id"],
                "challenger_track_id": report["challenger_track_id"],
                "challenger_evidence_batch_id": report["challenger_evidence_batch_id"],
                "incumbent_sleeve_id": report["incumbent_sleeve_id"],
                "total_comparison_rows": len(report["comparison_table"]),
                "active_challenger_count": len(active),
                "excluded_challenger_count": len(excluded),
                "evidence_sufficiency": report["evidence_sufficiency"],
                "top_research_review_candidate_id": report["deterministic_rank_order"][0]["challenger_hypothesis_id"] if report["deterministic_rank_order"] else "",
                "recommended_next_action": report["recommended_next_action"],
                "research_label_present": report.get("research_label") == "RESEARCH_ONLY",
                "immutable_registry_entry_present": bool(result.get("registry_row")),
                "broker_execution_allowed": False,
                "live_trading_allowed": False,
                "autonomous_trading_allowed": False,
                "sleeve_mutation_allowed": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
