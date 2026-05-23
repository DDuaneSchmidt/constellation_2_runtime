#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.challengers.human_review_dossier import write_human_review_dossier  # noqa: E402


DEFAULT_CHALLENGER_COMPARISON_REPORT_ID = "chcmp_9678581f68a5f8e3"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_human_review_dossier_v1")
    parser.add_argument("--challenger-comparison-report-id", default=DEFAULT_CHALLENGER_COMPARISON_REPORT_ID)
    parser.add_argument("--store-root", default=None)
    parser.add_argument("--actor", default="Aegis")
    args = parser.parse_args(argv)
    store_root = Path(args.store_root).resolve() if args.store_root else None
    result = write_human_review_dossier(
        challenger_comparison_report_id=args.challenger_comparison_report_id,
        store_root=store_root,
        actor=args.actor,
    )
    dossier = result["dossier"]
    print(
        json.dumps(
            {
                "human_review_dossier_id": dossier["human_review_dossier_id"],
                "dossier_type": dossier["dossier_type"],
                "incumbent_sleeve_id": dossier["incumbent_sleeve_id"],
                "challenger_track_id": dossier["challenger_track_id"],
                "challenger_comparison_report_id": dossier["challenger_comparison_report_id"],
                "active_review_candidate_count": len(dossier["review_candidates"]),
                "blocked_or_excluded_count": len(dossier["blocked_or_excluded_items"]),
                "top_research_review_candidate_id": dossier["executive_summary"]["top_research_review_candidate_id"],
                "recommended_next_action": dossier["recommended_next_action"],
                "research_label_present": dossier.get("research_label") == "RESEARCH_ONLY",
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
