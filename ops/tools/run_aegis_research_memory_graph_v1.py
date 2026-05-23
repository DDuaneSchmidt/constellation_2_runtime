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

from ops.aegis.adaptive_governance.evidence_model_v1 import write_report_v1
from ops.aegis.adaptive_governance.research_memory_graph_v1 import build_research_memory_graph_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_research_memory_graph_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_research_memory_graph_v1(truth_root=root, repo_root=REPO_ROOT, day_utc=str(args.day))
    paths = write_report_v1(truth_root=root, day_utc=str(args.day), family="research_memory_graph_v1", filename="research_memory_graph.v1.json", summary_filename="research_memory_graph.summary.txt", payload=payload, title="AEGIS RESEARCH MEMORY GRAPH v1")
    print(json.dumps({**paths, "node_count": len(payload.get("nodes") or []), "edge_count": len(payload.get("edges") or []), "broker_execution_allowed": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
