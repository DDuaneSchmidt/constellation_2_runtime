#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools.aegis_truth_integrity_common_v1 import now_iso_v1, read_json_v1, report_path_v1, status_of_v1, write_json_v1

SCHEMA_VERSION = "post_promotion_monitor.v1"


def post_promotion_monitor_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "post_promotion_monitor_v1" / day_utc / "post_promotion_monitor.v1.json").resolve()


def _first(rows: Any) -> dict[str, Any]:
    return rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {}


def build_post_promotion_monitor_v1(ctx: bod.BodContext) -> dict[str, Any]:
    gate_path = report_path_v1(ctx, "strategy_promotion_gate_v1", "strategy_promotion_gate.v1.json")
    proposal_path = report_path_v1(ctx, "strategy_change_proposal_v1", "strategy_change_proposal.v1.json")
    outcome_path = report_path_v1(ctx, "trade_outcome_v1", "trade_outcome.v1.json")
    insight_path = report_path_v1(ctx, "insight_engine_v1", "insight_engine.v1.json")
    action_path = report_path_v1(ctx, "action_validity_v1", "action_validity.v1.json")
    gate = read_json_v1(gate_path)
    proposal = _first(read_json_v1(proposal_path).get("proposals"))
    outcome = read_json_v1(outcome_path)
    insight = read_json_v1(insight_path)
    drift_alerts = insight.get("drift_alerts") if isinstance(insight.get("drift_alerts"), list) else []
    outcome_status = str(outcome.get("outcome_status") or status_of_v1(outcome))
    promoted = gate.get("promotion_status") == "APPROVED_FOR_PROMOTION"
    failure = outcome_status in {"FAIL", "NEGATIVE", "LOSS"} or bool(drift_alerts)
    success = promoted and outcome_status in {"PASS", "POSITIVE", "WIN"} and not drift_alerts
    rollback_recommended = bool(promoted and failure)
    monitor = {
        "promoted_proposal_id": str(gate.get("proposal_id") or proposal.get("proposal_id") or ""),
        "activation_date": ctx.day_utc if promoted else "",
        "expected_effect": str(proposal.get("expected_effect") or ""),
        "observed_effect": outcome_status if outcome else "UNKNOWN",
        "success_criteria_status": "PASS" if success else ("UNKNOWN" if promoted else "NOT_APPLICABLE"),
        "failure_criteria_status": "FAIL" if failure else ("PASS" if promoted else "NOT_APPLICABLE"),
        "drift_after_promotion": len(drift_alerts),
        "trade_quality_after_promotion": outcome_status if outcome else "UNKNOWN",
        "rollback_recommended": rollback_recommended,
        "human_review_required": bool(rollback_recommended or promoted),
        "operator_next_action": "Human review rollback recommendation." if rollback_recommended else ("Continue monitoring promoted change." if promoted else "No promoted strategy change to monitor."),
    }
    return {
        "schema_id": "post_promotion_monitor",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": now_iso_v1(),
        "status": "ROLLBACK_RECOMMENDED" if rollback_recommended else ("PASS" if success else ("INCONCLUSIVE" if promoted else "NOT_APPLICABLE")),
        "canonical_blocker": "",
        "operator_next_action": monitor["operator_next_action"],
        "monitors": [monitor],
        "rollback_recommended": rollback_recommended,
        "human_review_required": bool(monitor["human_review_required"]),
        "submit_allowed": False,
        "readiness_effect": "NONE",
        "active_strategy_mutation": False,
        "evidence_paths": [str(gate_path), str(proposal_path), str(outcome_path), str(insight_path), str(action_path)],
    }


def run_post_promotion_monitor_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_post_promotion_monitor_v1(ctx)
    path = post_promotion_monitor_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(payload, producer_name="ops/tools/run_post_promotion_monitor_v1.py", producer_command=f"python3 ops/tools/run_post_promotion_monitor_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}", input_artifacts=payload["evidence_paths"], output_artifacts=[path], schema_versions={"post_promotion_monitor": SCHEMA_VERSION})
    write_json_v1(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_post_promotion_monitor_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_post_promotion_monitor_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "rollback_recommended": payload["rollback_recommended"], "post_promotion_monitor_path": str(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
