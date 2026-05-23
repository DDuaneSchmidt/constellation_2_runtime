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

from ops.aegis.research_prioritizer_v1 import build_research_priorities_v1, split_research_priority_outputs_v1  # noqa: E402
from ops.aegis.intelligence_common_v1 import write_json_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_research_prioritizer_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    day = str(args.day)
    payload = build_research_priorities_v1(truth_root=root, repo_root=REPO_ROOT, day_utc=day)
    queue, opportunities, rejected = split_research_priority_outputs_v1(payload)
    out_dir = root / "reports" / "aegis_research_priorities_v1" / day
    paths = {
        "prioritized_research_queue": write_json_v1(out_dir / "prioritized_research_queue.v1.json", queue),
        "research_opportunities": write_json_v1(out_dir / "research_opportunities.v1.json", opportunities),
        "rejected_or_deferred_ideas": write_json_v1(out_dir / "rejected_or_deferred_ideas.v1.json", rejected),
    }
    print(json.dumps({"path": str(paths["prioritized_research_queue"]), "opportunity_count": payload["opportunity_count"], "ai_used": payload["ai_used"], "deterministic_fallback": payload["deterministic_fallback"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
