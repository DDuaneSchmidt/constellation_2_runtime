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

from constellation_2.common.paper_session_fact_plane_v1 import parse_day_utc_v1
from ops.tools import run_aegis_bod_prepare_v1 as bod
from ops.tools.aegis_producer_contract_v1 import attach_producer_contract_v1
from ops.tools.run_unified_truth_kernel_v1 import unified_truth_kernel_path

SCHEMA_VERSION = "aegis_live_intelligence.v1"
READY_STATUSES = {"PRE_MARKET_READY", "PAPER_READY", "PAPER_READY_WITH_DELAYED_DATA", "TRADING_ACTIVE", "EOD_COMPLETE"}


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def live_intelligence_path(*, truth_root: Path, day_utc: str) -> Path:
    return (truth_root / "reports" / "aegis_live_intelligence_v1" / day_utc / "live_intelligence.v1.json").resolve()


def _report_path(ctx: bod.BodContext, family: str, filename: str) -> Path:
    return (ctx.truth_root / "reports" / family / ctx.day_utc / filename).resolve()


def _allowed_action_ids(kernel: dict[str, Any]) -> list[str]:
    rows = kernel.get("allowed_operator_actions") if isinstance(kernel.get("allowed_operator_actions"), list) else []
    return [str(row.get("action_id") or "") for row in rows if isinstance(row, dict) and str(row.get("action_id") or "").strip()]


def _confidence_from_kernel(kernel: dict[str, Any]) -> str:
    final_status = str(kernel.get("final_status") or "UNKNOWN").strip().upper()
    if final_status not in READY_STATUSES:
        return "CAPPED_BY_HARD_BLOCKER"
    if str(kernel.get("consistency_status") or "").upper() == "FAIL":
        return "CAPPED_BY_HARD_CONSISTENCY_FAILURE"
    if str(kernel.get("freshness_status") or "").upper() == "FAIL":
        return "CAPPED_BY_STALE_AUTHORITATIVE_ARTIFACT"
    if str(kernel.get("lineage_status") or "").upper() == "FAIL":
        return "CAPPED_BY_AUTHORITATIVE_LINEAGE_FAILURE"
    if str(kernel.get("truth_confidence") or "").upper() in {"LOW", "UNKNOWN"}:
        return "CAPPED_BY_UNTRUSTED_TRUTH"
    return "ADVISORY_READY"


def build_live_intelligence_v1(ctx: bod.BodContext) -> dict[str, Any]:
    kernel_path = unified_truth_kernel_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    recommendation_queue_path = _report_path(ctx, "ai_recommendation_queue_v1", "ai_recommendation_queue.v1.json")
    kernel = _read_json(kernel_path)
    final_status = str(kernel.get("final_status") or "UNKNOWN").strip().upper()
    blocker = str(kernel.get("canonical_blocker") or kernel.get("first_blocker") or "").strip()
    allowed_ids = _allowed_action_ids(kernel)
    trade_actions_allowed = final_status in READY_STATUSES and any(action_id in allowed_ids for action_id in {"submit_paper_order", "enable_broker_transmit"})
    confidence = _confidence_from_kernel(kernel)
    trade_health = kernel.get("trade_health") if isinstance(kernel.get("trade_health"), dict) else {}
    opportunity = {"state": "UNKNOWN", "actionable_recommendations": []}
    edge = {"state": str(trade_health.get("edge_status") or "UNKNOWN"), "proven_facts": [], "advisory_analysis": []}
    drift = {"state": "UNKNOWN", "alerts": []}
    quality = {"state": "UNKNOWN", "score": None}
    status = "PASS" if final_status in READY_STATUSES else "NOT_READY"
    upstream_ready = final_status in READY_STATUSES
    if trade_actions_allowed:
        opportunity = {"state": "ADVISORY_ONLY", "actionable_recommendations": [], "requires_human_review": True}
        edge["proven_facts"] = ["unified_truth_kernel_final_status=" + final_status]
        drift = {"state": "ADVISORY_ONLY", "alerts": []}
        quality = {"state": "ADVISORY_ONLY", "score": None}
    return {
        "schema_id": "aegis_live_intelligence",
        "schema_version": SCHEMA_VERSION,
        "day_utc": ctx.day_utc,
        "environment": ctx.environment,
        "generated_at_utc": _now_iso(),
        "status": status,
        "canonical_blocker": blocker,
        "operator_next_action": "Resolve day-run blocker before using advisory intelligence." if not upstream_ready else "Human review required before strategy-changing action.",
        "advisory_only": True,
        "readiness_authority": "aegis_day_run_ledger_v1",
        "final_status_observed": final_status,
        "system_confidence": confidence,
        "opportunity_radar": opportunity,
        "trade_challenges": {"state": "UNKNOWN" if not upstream_ready else "ADVISORY_ONLY", "items": []},
        "edge_metrics": edge,
        "drift_alerts": drift,
        "trade_quality_score": quality,
        "state_consistency_status": str(kernel.get("consistency_status") or "UNKNOWN"),
        "truth_freshness_status": str(kernel.get("freshness_status") or "UNKNOWN"),
        "action_validity_status": str(kernel.get("action_validity_status") or "UNKNOWN"),
        "outcome_attribution_status": str(trade_health.get("outcome_status") or "UNKNOWN"),
        "valid_action_ids": allowed_ids,
        "facts_vs_advisory": {
            "proven_facts": ["unified_truth_kernel_final_status=" + final_status] if final_status != "UNKNOWN" else [],
            "advisory_analysis": [],
        },
        "requires_human_review": True,
        "cannot_modify_readiness": True,
        "submit_boundary_effect": "NONE",
        "kernel_final_status_source": str(kernel.get("final_status_source") or ""),
        "trade_recommendations_suppressed": not trade_actions_allowed,
        "learning_loop_output_policy": "Live intelligence may inform ai_recommendation_queue_v1 only; it cannot write proposals or active strategy state.",
        "recommendation_queue_path": str(recommendation_queue_path),
        "direct_proposal_write_allowed": False,
        "active_strategy_mutation_allowed": False,
        "input_artifact_paths": [str(kernel_path), str(recommendation_queue_path)],
    }


def run_live_intelligence_v1(day_utc: str, environment: str, truth_root: str = "") -> tuple[Path, dict[str, Any]]:
    ctx = bod._resolve_context(day_utc, environment, truth_root)
    payload = build_live_intelligence_v1(ctx)
    path = live_intelligence_path(truth_root=ctx.truth_root, day_utc=ctx.day_utc)
    attach_producer_contract_v1(
        payload,
        producer_name="ops/tools/run_aegis_live_intelligence_v1.py",
        producer_command=f"python3 ops/tools/run_aegis_live_intelligence_v1.py --day_utc {ctx.day_utc} --environment {ctx.environment}",
        input_artifacts=payload["input_artifact_paths"],
        output_artifacts=[path],
        schema_versions={"aegis_live_intelligence": SCHEMA_VERSION},
    )
    _write_json(path, payload)
    return path, payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_aegis_live_intelligence_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default="PAPER", choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    path, payload = run_live_intelligence_v1(parse_day_utc_v1(args.day_utc), str(args.environment).strip().upper(), str(args.truth_root or ""))
    print(json.dumps({"status": payload["status"], "canonical_blocker": payload["canonical_blocker"], "live_intelligence_path": str(path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
