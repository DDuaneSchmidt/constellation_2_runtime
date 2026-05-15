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

from constellation_2.common.aegis_research_lab_v1 import (  # noqa: E402
    build_research_evidence_packet_v1,
    build_research_inbox_item_v1,
    build_research_queue_task_v1,
    build_research_result_ledger_v1,
    build_research_result_v1,
    build_research_task_queue_v1,
    validate_research_lab_artifact_v1,
    write_research_lab_artifact_v1,
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: str) -> dict[str, Any]:
    if not path:
        return {}
    return json.loads(Path(path).expanduser().resolve().read_text(encoding="utf-8"))


def _ledger_path(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "research_lab" / "research_result_ledger_v1" / day_utc / "index" / "research_result_ledger.v1.json"


def _queue_path(truth_root: Path, day_utc: str) -> Path:
    return truth_root / "research_lab" / "research_task_queue_v1" / day_utc / "index" / "research_task_queue.v1.json"


def _existing_results(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [row for row in payload.get("results", []) if isinstance(row, dict)]


def _existing_tasks(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [row for row in payload.get("tasks", []) if isinstance(row, dict)]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _result_status(row: dict[str, Any], decision: dict[str, Any]) -> str:
    skipped = _text(row.get("skipped_trade_outcome"))
    realized = _text(row.get("realized_pnl"))
    sleeve_quality = _text(row.get("sleeve_signal_quality")).upper()
    decision_value = _text(decision.get("decision")).upper()
    if decision_value == "SKIPPED" or skipped:
        return "NEEDS_MORE_RESEARCH" if not skipped or skipped.upper() in {"UNKNOWN", "N/A"} else "WEAK_SUPPORT"
    if realized.startswith("-") or sleeve_quality in {"POOR", "BAD", "FAIL"}:
        return "CONTRADICTS_HYPOTHESIS"
    if realized:
        return "SUPPORTS_HYPOTHESIS"
    return "NEEDS_MORE_RESEARCH"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ingest_trade_outcome_attribution_to_research_v1")
    parser.add_argument("--truth_root", required=True)
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--trade_outcome_attribution_json", required=True)
    parser.add_argument("--manual_operator_decision_json", default="")
    parser.add_argument("--manual_execution_event_json", default="")
    parser.add_argument("--edge_cluster_json", default="")
    parser.add_argument("--hypothesis_id", default="")
    parser.add_argument("--generated_at_utc", default="")
    args = parser.parse_args(argv)

    truth_root = Path(args.truth_root).expanduser().resolve()
    generated_at = args.generated_at_utc or _now()
    attribution = _read_json(args.trade_outcome_attribution_json)
    decision = _read_json(args.manual_operator_decision_json)
    execution = _read_json(args.manual_execution_event_json)
    _edge_cluster = _read_json(args.edge_cluster_json)
    results: list[dict[str, Any]] = []
    tasks: list[dict[str, Any]] = []
    inbox_count = 0
    evidence_count = 0
    for row in attribution.get("attributions", []):
        if not isinstance(row, dict):
            continue
        candidate_id = _text(row.get("candidate_id"))
        hypothesis_id = _text(args.hypothesis_id) or _text(row.get("hypothesis_id")) or _text(row.get("sleeve_id")) or "UNKNOWN_HYPOTHESIS"
        status = _result_status(row, decision)
        evidence_refs: list[str] = []
        executed = bool(_text(row.get("actual_entry_price")) or _text(execution.get("actual_entry_price")))
        if executed:
            evidence = build_research_evidence_packet_v1(
                research_id=f"lite-feedback:{candidate_id}",
                hypothesis_id=hypothesis_id,
                task_id=f"lite-feedback:{candidate_id or len(results) + 1}",
                title=f"Lite feedback for {candidate_id}",
                hypothesis_description="Operational Lite recommendation feedback imported into Research Lab.",
                research_type="EDGE",
                created_at_utc=generated_at,
                source_data_summary="Aegis Lite trade_outcome_attribution.v1 plus optional manual decision/execution artifacts.",
                replay_window=str(attribution.get("day_utc") or args.day_utc),
                instruments_tested=[_text(row.get("candidate_id"))],
                regimes_tested=[],
                edge_family=_text(row.get("edge_cluster_id")),
                expected_holding_period="operational_feedback",
                methodology_summary="Deterministic import of Lite outcome attribution; informational only.",
                metrics_summary={
                    "realized_pnl": _text(row.get("realized_pnl")),
                    "unrealized_pnl": _text(row.get("unrealized_pnl")),
                    "MAE": _text(row.get("MAE")),
                    "MFE": _text(row.get("MFE")),
                    "operator_slippage": _text(row.get("operator_slippage")),
                    "skipped_trade_outcome": _text(row.get("skipped_trade_outcome")),
                    "governance_adjustment_effect": _text(row.get("governance_adjustment_effect")),
                    "sleeve_signal_quality": _text(row.get("sleeve_signal_quality")),
                    "implementation_quality": _text(row.get("implementation_quality")),
                    "operator_execution_quality": _text(row.get("operator_execution_quality")),
                },
                expectancy_summary=_text(row.get("sleeve_signal_quality")),
                drawdown_summary="operational_feedback_only",
                MAE_MFE_summary=f"MAE={_text(row.get('MAE'))}; MFE={_text(row.get('MFE'))}",
                failure_modes=[_text(row.get("governance_adjustment_effect"))],
                known_limitations=["Single operational observation; not a validated sample."],
                reproducibility_notes="Source artifacts are recorded in result evidence refs.",
                artifact_lineage=[{"artifact_type": "trade_outcome_attribution_v1", "path": str(Path(args.trade_outcome_attribution_json).resolve())}],
                reason_codes=["LITE_FEEDBACK_IMPORT", f"RESULT_STATUS:{status}"],
                research_status="UNDER_REVIEW",
            )
            evidence_path = write_research_lab_artifact_v1(truth_root=truth_root, day_utc=args.day_utc, payload=evidence)
            evidence_refs.append(str(evidence_path))
            evidence_count += 1
        recommendation = "REJECT" if status == "CONTRADICTS_HYPOTHESIS" else "CONTINUE_RESEARCH"
        results.append(
            build_research_result_v1(
                result_id=f"lite-feedback:{candidate_id or len(results) + 1}",
                hypothesis_id=hypothesis_id,
                task_id=f"lite-feedback:{candidate_id or len(results) + 1}",
                result_status=status,
                evidence_refs=evidence_refs,
                conclusion_summary="Lite feedback imported for research learning only.",
                confidence_before="LOW",
                confidence_after="LOW",
                confidence_change="0",
                metrics_summary={
                    "skipped_trade_outcome": _text(row.get("skipped_trade_outcome")),
                    "realized_pnl": _text(row.get("realized_pnl")),
                    "operator_slippage": _text(row.get("operator_slippage")),
                    "governance_adjustment_effect": _text(row.get("governance_adjustment_effect")),
                    "sleeve_signal_quality": _text(row.get("sleeve_signal_quality")),
                    "edge_cluster_id": _text(row.get("edge_cluster_id")),
                },
                failure_mode_notes=_text(row.get("governance_adjustment_effect")),
                invalidation_notes=_text(row.get("sleeve_signal_quality")) if status == "CONTRADICTS_HYPOTHESIS" else "",
                next_recommended_task="FAILURE_MODE_REVIEW" if status == "CONTRADICTS_HYPOTHESIS" else "FORWARD_OBSERVATION",
                promotion_recommendation=recommendation,
                created_at_utc=generated_at,
                artifact_lineage=[{"artifact_type": "trade_outcome_attribution_v1", "path": str(Path(args.trade_outcome_attribution_json).resolve())}],
                reason_codes=["LITE_FEEDBACK_IMPORT", f"RESULT_STATUS:{status}"],
                reproducibility_notes="Deterministic informational import from Lite outcome attribution; no Lite mutation.",
            )
        )
        if _text(row.get("governance_adjustment_effect")) or _text(row.get("operator_slippage")) or status == "CONTRADICTS_HYPOTHESIS":
            tasks.append(
                build_research_queue_task_v1(
                    task_id=f"task:{hypothesis_id}:lite_feedback:{candidate_id or len(tasks) + 1}",
                    hypothesis_id=hypothesis_id,
                    task_type="FAILURE_MODE_REVIEW" if status == "CONTRADICTS_HYPOTHESIS" else "FORWARD_OBSERVATION",
                    priority="HIGH" if status == "CONTRADICTS_HYPOTHESIS" else "NORMAL",
                    status="QUEUED",
                    required_inputs=[str(Path(args.trade_outcome_attribution_json).resolve()), *evidence_refs],
                    output_artifact_refs=[],
                    blocker_reason_codes=[],
                    created_at_utc=generated_at,
                )
            )
        if hypothesis_id == "UNKNOWN_HYPOTHESIS" or (status == "NEEDS_MORE_RESEARCH" and not evidence_refs):
            inbox = build_research_inbox_item_v1(
                inbox_id=f"lite-feedback-inbox:{candidate_id or len(results)}",
                created_at_utc=generated_at,
                source="LITE_FEEDBACK",
                raw_text=f"Ambiguous Lite feedback observation for candidate {candidate_id or 'unknown'} requires triage before formal hypothesis use.",
                tags=["lite_feedback", "ambiguous_observation"],
                related_symbols=[_text(row.get("symbol")) or _text(row.get("candidate_id"))],
                related_edge_family=_text(row.get("edge_cluster_id")),
                suggested_program="",
                triage_status="NEW",
                notes="Created by informational Lite feedback ingestion. Does not mutate Lite or authorize promotion.",
            )
            write_research_lab_artifact_v1(truth_root=truth_root, day_utc=args.day_utc, payload=inbox)
            inbox_count += 1
    ledger = build_research_result_ledger_v1(
        generated_at_utc=generated_at,
        results=[*_existing_results(_ledger_path(truth_root, args.day_utc)), *results],
    )
    queue = build_research_task_queue_v1(
        generated_at_utc=generated_at,
        tasks=[*_existing_tasks(_queue_path(truth_root, args.day_utc)), *tasks],
    )
    for payload in (ledger, queue):
        validate_research_lab_artifact_v1(payload)
        write_research_lab_artifact_v1(truth_root=truth_root, day_utc=args.day_utc, payload=payload)
    print(
        json.dumps(
            {
                "result_count": len(results),
                "follow_up_task_count": len(tasks),
                "evidence_count": evidence_count,
                "inbox_count": inbox_count,
                "lite_runtime_mutation_allowed": False,
                "automatic_promotion_allowed": False,
                "broker_submit_required": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
