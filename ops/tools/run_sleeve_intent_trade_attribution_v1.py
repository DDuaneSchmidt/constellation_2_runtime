#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.opportunity_lineage_attribution_v1 import (  # noqa: E402
    materialize_lineage_and_write_attribution_v1,
    read_submit_decision_traces_v1,
)
from constellation_2.common.runtime_authority_bridge_v1 import (  # noqa: E402
    resolve_truth_root_bridge_v1,
)


def _resolve_truth_root(raw_truth_root: str) -> Path:
    text = str(raw_truth_root or "").strip()
    if text:
        root = Path(text).expanduser().resolve()
    else:
        root = resolve_truth_root_bridge_v1(
            repo_root=REPO_ROOT,
            caller="ops/tools/run_sleeve_intent_trade_attribution_v1.py",
        )
    if (not root.is_absolute()) or (not root.exists()) or (not root.is_dir()):
        raise SystemExit(f"FAIL: invalid truth root: {root}")
    return root


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_sleeve_intent_trade_attribution_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--truth_root", default="")
    parser.add_argument("--environment", default="PAPER")
    args = parser.parse_args(argv)

    day_utc = str(args.day_utc).strip()
    if len(day_utc) != 10 or day_utc[4] != "-" or day_utc[7] != "-":
        raise SystemExit(f"FAIL: invalid day_utc: {day_utc}")
    environment = str(args.environment).strip().upper() or "PAPER"
    truth_root = _resolve_truth_root(str(args.truth_root))

    lineage, attribution = materialize_lineage_and_write_attribution_v1(
        truth_root=truth_root,
        day_utc=day_utc,
        environment=environment,
        emitted_by="ops/tools/run_sleeve_intent_trade_attribution_v1.py",
    )
    submit_decisions = read_submit_decision_traces_v1(
        truth_root=truth_root,
        day_utc=day_utc,
    )
    submit_decision_opportunity_count = len(
        {
            (
                str(row.get("sleeve_id") or "").strip(),
                str(row.get("opportunity_id") or "").strip(),
            )
            for row in submit_decisions
        }
    )
    print(
        json.dumps(
            {
                "day_utc": day_utc,
                "environment": environment,
                "lineage_event_count": lineage.event_count,
                "lineage_evidence_cutoff_utc": lineage.evidence_cutoff_utc,
                "lineage_warnings": list(lineage.warnings),
                "submit_decision_trace_count": submit_decision_opportunity_count,
                "attribution_report_path": attribution.report_path,
                "attribution_versioned_report_path": attribution.versioned_report_path,
                "attribution_report_sha256": attribution.report_sha256,
                "attribution_event_count": attribution.event_count,
                "attribution_opportunity_count": attribution.opportunity_count,
                "attribution_completeness_status": attribution.completeness_status,
                "classification_counts": attribution.classification_counts,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
