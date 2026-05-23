#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops.aegis.intelligence_common_v1 import intelligence_summaries_v1, latest_json_v1, week_days_ending_v1, write_json_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT, build_runtime_truth_kernel_v1  # noqa: E402
from ops.tools.write_aegis_eod_intelligence_v1 import render_period_summary_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_eow_intelligence_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    truth_root = Path(args.truth_root).expanduser().resolve()
    payload = build_eow_intelligence_v1(truth_root=truth_root, day_utc=str(args.day))
    out_dir = truth_root / "reports" / "aegis_eow_intelligence_v1" / str(args.day)
    json_path = write_json_v1(out_dir / "eow_intelligence.v1.json", payload)
    summary_path = out_dir / "eow_intelligence.summary.txt"
    summary_path.write_text(render_period_summary_v1("AEGIS EOW INTELLIGENCE v1", payload), encoding="utf-8")
    print(json.dumps({"path": str(json_path), "summary_path": str(summary_path), "broker_submit_required": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


def build_eow_intelligence_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    kernel = build_runtime_truth_kernel_v1(truth_root=truth_root, day_utc=day_utc)
    week_days = week_days_ending_v1(day_utc)
    _, sleeve = latest_json_v1(truth_root, "aegis_sleeve_attribution_v1", day_utc, "sleeve_attribution.v1.json")
    _, research = latest_json_v1(truth_root, "aegis_research_priorities_v1", day_utc, "prioritized_research_queue.v1.json")
    _, risk = latest_json_v1(truth_root, "aegis_risk_governance_v1", day_utc, "risk_governance.v1.json")
    return {
        "schema_id": "aegis_eow_intelligence",
        "schema_version": "v1",
        "artifact_id": "aegis_eow_intelligence_v1",
        "day_utc": day_utc,
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "week_days_in_scope": sorted(week_days),
        "weekly_summary": {"runtime_truth_classification": kernel.get("runtime_truth_classification"), "highest_readiness_layer": kernel.get("highest_readiness_layer"), "risk_state": risk.get("risk_state") or "UNKNOWN"},
        "weekly_sleeve_performance": sleeve.get("sleeves") or [],
        "research_progress": research.get("prioritized_research_queue") or [],
        "recurring_issues": kernel.get("do_not_claim") or [],
        "regime_observations": risk.get("risk_reasons") or ["UNKNOWN"],
        "sleeves_to_watch": [row for row in sleeve.get("sleeves", []) if isinstance(row, dict) and row.get("status") in {"WATCH", "DEGRADED", "SUSPEND_CANDIDATE", "UNKNOWN"}],
        "hypotheses_to_prioritize": research.get("prioritized_research_queue") or [],
        "operator_action_plan_for_next_week": ["Run daily operator report before market decisions.", "Review sleeve warnings.", "Review research priorities.", "Keep broker execution disabled by design."],
        "consumed_summaries": intelligence_summaries_v1(truth_root, day_utc),
        "safety": {"report_only": True, "broker_submit_required": False, "autonomous_execution_allowed": False, "recommendations_are_advisory_only": True},
    }


if __name__ == "__main__":
    raise SystemExit(main())
