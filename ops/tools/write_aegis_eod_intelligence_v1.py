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

from ops.aegis.intelligence_common_v1 import intelligence_summaries_v1, latest_json_v1, write_json_v1  # noqa: E402
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT, build_runtime_truth_kernel_v1  # noqa: E402
from ops.aegis.candidate_decision_support_v1 import build_candidate_decision_support_payload_v1  # noqa: E402
from ops.aegis.eod_opportunity_outcome_report_v1 import build_eod_opportunity_outcome_report_v1, write_eod_opportunity_outcome_report_v1  # noqa: E402
from ops.aegis.operator_command_v1 import build_operator_projections_v1  # noqa: E402
from ops.aegis.data_remediation_v1 import run_data_remediation_v1  # noqa: E402
from ops.aegis.run_context_v1 import child_run_context_v1, run_context_from_env_v1, step_allowed_v1  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="write_aegis_eod_intelligence_v1")
    parser.add_argument("--truth_root", default=str(DEFAULT_TRUTH_ROOT))
    parser.add_argument("--day", default=datetime.now(UTC).strftime("%Y-%m-%d"))
    args = parser.parse_args(argv)
    truth_root = Path(args.truth_root).expanduser().resolve()
    context = run_context_from_env_v1("eod")
    allowed, _reason = step_allowed_v1(context, "self_heal")
    if allowed:
        run_data_remediation_v1(
            truth_root=truth_root,
            repo_root=REPO_ROOT,
            day_utc=str(args.day),
            run_context=child_run_context_v1(
                context,
                "self-heal-data",
                allow_market_data_refresh=False,
                allow_self_heal=True,
                allow_projection_rebuild=True,
            ),
        )
    payload = build_eod_intelligence_v1(truth_root=truth_root, day_utc=str(args.day))
    eod_outcome_report = build_and_write_eod_opportunity_outcome_report_if_available_v1(truth_root=truth_root, day_utc=str(args.day))
    if eod_outcome_report:
        payload["eod_opportunity_outcome_report"] = {
            "eod_outcome_report_id": eod_outcome_report.get("eod_outcome_report_id"),
            "path": eod_outcome_report.get("path"),
            "determinism_fingerprint": eod_outcome_report.get("determinism_fingerprint"),
            "operator_eligible_count": eod_outcome_report.get("operator_eligible_count"),
            "unsupported_observation_count": eod_outcome_report.get("unsupported_observation_count"),
            "blocked_sleeve_count": eod_outcome_report.get("blocked_sleeve_count"),
        }
    out_dir = truth_root / "reports" / "aegis_eod_intelligence_v1" / str(args.day)
    json_path = write_json_v1(out_dir / "eod_intelligence.v1.json", payload)
    summary_path = out_dir / "eod_intelligence.summary.txt"
    summary_path.write_text(render_period_summary_v1("AEGIS EOD INTELLIGENCE v1", payload), encoding="utf-8")
    print(json.dumps({"path": str(json_path), "summary_path": str(summary_path), "eod_outcome_report_path": str((eod_outcome_report or {}).get("path") or ""), "broker_submit_required": False, "autonomous_execution_allowed": False}, sort_keys=True))
    return 0


def build_eod_intelligence_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    kernel = build_runtime_truth_kernel_v1(truth_root=truth_root, day_utc=day_utc)
    receipt_path, receipts = latest_json_v1(truth_root, "manual_execution_receipt_v1", day_utc, "manual_execution_receipt.v1.json")
    summaries = intelligence_summaries_v1(truth_root, day_utc)
    return {
        "schema_id": "aegis_eod_intelligence",
        "schema_version": "v1",
        "artifact_id": "aegis_eod_intelligence_v1",
        "day_utc": day_utc,
        "generated_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "what_happened_today": {"runtime_truth_classification": kernel.get("runtime_truth_classification"), "highest_readiness_layer": kernel.get("highest_readiness_layer"), "advisory_status": kernel.get("advisory_status")},
        "what_changed": "SEE_RUNTIME_STATE_TRANSITIONS",
        "what_worked": [key for key, row in summaries.items() if isinstance(row, dict) and row.get("status") == "AVAILABLE"],
        "what_failed": kernel.get("missing_or_stale_sources") or [],
        "what_was_captured": {"manual_execution_receipt_path": str(receipt_path or ""), "manual_execution_receipt": receipts},
        "review_tomorrow": ["Review operator inbox.", "Review risk governance.", "Review research priorities.", "Capture any external manual fills."],
        "consumed_summaries": summaries,
        "safety": {"report_only": True, "broker_submit_required": False, "autonomous_execution_allowed": False, "recommendations_are_advisory_only": True},
    }


def build_and_write_eod_opportunity_outcome_report_if_available_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any] | None:
    canonical_path = truth_root / "reports" / "aegis_canonical_operator_state_v1" / day_utc / "canonical_operator_state.v1.json"
    if not canonical_path.exists():
        return None
    canonical = json.loads(canonical_path.read_text(encoding="utf-8"))
    cockpit = {
        "ok": True,
        "status": "AVAILABLE",
        "day_utc": day_utc,
        "generated_at_utc": canonical.get("generated_at_utc"),
        "source_paths": {"canonical_operator_state": str(canonical_path)},
        "runtime": canonical.get("runtime") if isinstance(canonical.get("runtime"), dict) else {},
        "top_candidates": canonical.get("top_candidates") if isinstance(canonical.get("top_candidates"), list) else [],
        "opportunities": canonical.get("opportunities") if isinstance(canonical.get("opportunities"), dict) else {},
        "candidate_decisions_corrections": canonical.get("candidates") if isinstance(canonical.get("candidates"), dict) else {},
        "sleeve_warnings": canonical.get("sleeves") if isinstance(canonical.get("sleeves"), dict) else {},
    }
    decision_support = build_candidate_decision_support_payload_v1(cockpit)
    support_by_id = decision_support.get("by_candidate_id") if isinstance(decision_support.get("by_candidate_id"), dict) else {}
    enriched = []
    for row in cockpit["top_candidates"]:
        if not isinstance(row, dict):
            enriched.append(row)
            continue
        candidate_id = str(row.get("candidate_id") or row.get("id") or "").strip()
        enriched.append({**row, "decision_support_brief": support_by_id.get(candidate_id, row.get("decision_support_brief"))})
    cockpit["top_candidates"] = enriched
    cockpit.update(build_operator_projections_v1(cockpit))
    report = build_eod_opportunity_outcome_report_v1(cockpit, trading_session=day_utc, generated_at=str(canonical.get("generated_at_utc") or ""))
    return write_eod_opportunity_outcome_report_v1(truth_root=truth_root, report=report)


def render_period_summary_v1(title: str, payload: dict[str, Any]) -> str:
    happened = payload.get("what_happened_today") or payload.get("weekly_summary") or {}
    lines = [title, f"day_utc: {payload.get('day_utc')}", f"runtime_truth_classification: {happened.get('runtime_truth_classification')}", f"highest_readiness_layer: {happened.get('highest_readiness_layer')}", "broker_submit_required: false", "autonomous_execution_allowed: false", "", "operator_review:"]
    lines.extend(f"- {item}" for item in payload.get("review_tomorrow") or payload.get("operator_action_plan_for_next_week") or ["NONE"])
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
