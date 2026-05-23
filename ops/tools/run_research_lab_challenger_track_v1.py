#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from research_lab.challengers.challenger_track import write_challenger_research_track  # noqa: E402


DEFAULT_SLEEVE_ID = "slv_etf_drop_reversion_v1"
DEFAULT_STABILITY_REPORT_ID = "ssr_3b67fdfd56db3e09"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_research_lab_challenger_track_v1")
    parser.add_argument("--sleeve-id", default=DEFAULT_SLEEVE_ID)
    parser.add_argument("--sleeve-stability-report-id", default=DEFAULT_STABILITY_REPORT_ID)
    parser.add_argument("--store-root", default=None)
    parser.add_argument("--actor", default="Aegis")
    args = parser.parse_args(argv)
    store_root = Path(args.store_root).resolve() if args.store_root else None
    result = write_challenger_research_track(
        sleeve_id=args.sleeve_id,
        sleeve_stability_report_id=args.sleeve_stability_report_id,
        store_root=store_root,
        actor=args.actor,
    )
    track = result["track"]
    print(
        json.dumps(
            {
                "challenger_track_id": track["challenger_track_id"],
                "incumbent_sleeve_id": track["incumbent_sleeve_id"],
                "trigger_report_ids": track["trigger_report_ids"],
                "hypothesis_count": len(track["challenger_hypothesis_set"]),
                "status": track["status"],
                "recommended_next_action": track["recommended_next_action"],
                "research_label_present": track.get("research_label") == "RESEARCH_ONLY",
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
