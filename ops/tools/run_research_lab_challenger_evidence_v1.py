#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.challengers.challenger_evidence import evidence_quality_summary, write_challenger_evidence_batch  # noqa: E402


DEFAULT_CHALLENGER_TRACK_ID = "chtrk_slv_etf_drop_reversion_v1_3e4313496418"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_challenger_evidence_v1")
    parser.add_argument("--challenger-track-id", default=DEFAULT_CHALLENGER_TRACK_ID)
    parser.add_argument("--store-root", default=None)
    parser.add_argument("--actor", default="Aegis")
    args = parser.parse_args(argv)
    store_root = Path(args.store_root).resolve() if args.store_root else None
    result = write_challenger_evidence_batch(
        challenger_track_id=args.challenger_track_id,
        store_root=store_root,
        actor=args.actor,
    )
    batch = result["batch"]
    print(
        json.dumps(
            {
                "challenger_evidence_batch_id": batch["challenger_evidence_batch_id"],
                "challenger_track_id": batch["challenger_track_id"],
                "incumbent_sleeve_id": batch["incumbent_sleeve_id"],
                "total_hypotheses": len(batch["source_challenger_hypothesis_ids"]),
                "generated_item_count": sum(1 for item in batch["challenger_evidence_items"] if item["status"] == "generated"),
                "blocked_hypothesis_count": len(batch["blocked_hypotheses"]),
                "evidence_quality_summary": evidence_quality_summary(batch),
                "recommended_next_action": batch["recommended_next_action"],
                "research_label_present": batch.get("research_label") == "RESEARCH_ONLY",
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
