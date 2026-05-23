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

from ops.aegis.intelligence_governance_kernel_v1 import build_intelligence_governance_kernel_v1, write_intelligence_governance_reports_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_intelligence_governance_kernel_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    root = Path(args.truth_root).expanduser().resolve()
    payload = build_intelligence_governance_kernel_v1(truth_root=root, repo_root=REPO_ROOT, day_utc=str(args.day))
    paths = write_intelligence_governance_reports_v1(truth_root=root, repo_root=REPO_ROOT, payload=payload)
    result = {
        "path": paths["intelligence_governance_kernel"],
        "recommendation_count": payload["recommendation_count"],
        "approval_ledger_path": paths["intelligence_approval_ledger_jsonl"],
        "ai_used": payload["ai_usage"]["ai_used"],
        "deterministic_fallback": payload["ai_usage"]["deterministic_fallback"],
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "runtime_truth_mutation_allowed": False,
        "report_paths": paths,
    }
    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print("AEGIS INTELLIGENCE GOVERNANCE KERNEL")
        print(f"recommendation_count: {payload['recommendation_count']}")
        print(f"awaiting_approval: {payload['approval_summary']['awaiting_approval']}")
        print(f"needs_more_evidence: {payload['approval_summary']['needs_more_evidence']}")
        print(f"ai_used: {str(payload['ai_usage']['ai_used']).lower()}")
        print(f"deterministic_fallback: {str(payload['ai_usage']['deterministic_fallback']).lower()}")
        print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
