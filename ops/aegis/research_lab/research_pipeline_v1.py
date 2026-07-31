from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import read_json_v1, write_json_v1
from ops.aegis.research_hypothesis_classification_v1 import build_research_hypothesis_classification_v1
from ops.aegis.research_lab.hypothesis_priority_engine_v1 import (
    build_hypothesis_priority_report_v1,
    enrich_pipeline_items_with_priority_v1,
)
from ops.aegis.research_lab.event_earnings_data_v1 import (
    NVIDIA_HYPOTHESIS_ID,
    event_earnings_data_status_v1,
    event_upload_format_v1,
    maybe_import_staged_event_data_v1,
    run_forward_return_event_study_v1,
)
from ops.aegis.research_lab.research_data_readiness_v1 import (
    governed_market_data_availability_v1,
    market_symbols_from_required_inputs_v1,
)


REPORT_FAMILY = "aegis_research_pipeline_v1"
TRIAGE_FAMILY = "aegis_research_triage_v1"
PLAN_FAMILY = "aegis_research_plan_v1"
RESULT_FAMILY = "aegis_research_test_results_v1"
ETF_DROP_MEAN_REVERSION_HYPOTHESIS_ID = "rh-process-test-etf-drop-mean-reversion-v1"
REVIEW_FAMILY = "aegis_research_review_decisions_v1"

PIPELINE_COLUMNS = [
    "INBOX",
    "TRIAGE",
    "TEST_PLAN",
    "TESTING",
    "RESULT_REVIEW",
    "PAPER_TRIAL",
    "SLEEVE_REVIEW",
    "ACTIVE_OR_ADOPTED",
    "REJECTED_ARCHIVED",
]

OPERATOR_LIFECYCLE_STATES = [
    "IDEA",
    "RESEARCHING",
    "VALIDATING",
    "PAPER_TRIAL",
    "READY",
    "CAPTURE_READY",
    "CAPTURED",
    "BLOCKED",
    "ARCHIVED",
]

TRIAGE_DECISIONS = {"QUEUE_TEST_PLAN", "REJECT", "ARCHIVE", "DUPLICATE", "NEEDS_CLARIFICATION"}
REVIEW_DECISIONS = {
    "PAPER_TEST_CANDIDATE",
    "SLEEVE_REVIEW_CANDIDATE",
    "REJECT",
    "REJECTED",
    "ARCHIVE",
    "NEEDS_MORE_EVIDENCE",
}


def build_research_pipeline_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    classification = build_research_hypothesis_classification_v1(truth_root=root, day_utc=day_utc)
    triage_by_id = _latest_events_by_hypothesis(_read_jsonl_family(root, TRIAGE_FAMILY, "research_triage.v1.jsonl"))
    plan_by_id = _latest_payload_by_hypothesis(root, PLAN_FAMILY, "research_plan.v1.json")
    result_by_id = _latest_payload_by_hypothesis(root, RESULT_FAMILY, "research_test_result.v1.json")
    review_by_id = _latest_events_by_hypothesis(_read_jsonl_family(root, REVIEW_FAMILY, "research_review_decisions.v1.jsonl"))

    items: list[dict[str, Any]] = []
    for hypothesis in classification.get("hypotheses") or []:
        if not isinstance(hypothesis, dict):
            continue
        items.append(
            _pipeline_item(
                hypothesis=hypothesis,
                triage=triage_by_id.get(str(hypothesis.get("hypothesis_id") or "")),
                plan=plan_by_id.get(str(hypothesis.get("hypothesis_id") or "")),
                result=result_by_id.get(str(hypothesis.get("hypothesis_id") or "")),
                review=review_by_id.get(str(hypothesis.get("hypothesis_id") or "")),
                truth_root=root,
                day_utc=day_utc,
            )
        )

    priority_report = build_hypothesis_priority_report_v1(
        pipeline_payload={"items": items},
        day_utc=day_utc,
    )
    items = enrich_pipeline_items_with_priority_v1(items, priority_report)
    priority_report = build_hypothesis_priority_report_v1(
        pipeline_payload={"items": items},
        day_utc=day_utc,
    )
    items = enrich_pipeline_items_with_priority_v1(items, priority_report)

    columns = {key.lower(): [] for key in PIPELINE_COLUMNS}
    for item in items:
        columns[str(item["current_gate"]).lower()].append(item)
    counts = {key: len(columns[key.lower()]) for key in PIPELINE_COLUMNS}
    blocked = [item for item in items if item.get("gate_status") in {"BLOCKED", "NEEDS_OPERATOR"} or item.get("blocker")]
    next_actions = [
        {
            "hypothesis_id": item["hypothesis_id"],
            "current_gate": item["current_gate"],
            "next_action": item["next_action"],
            "next_command": item["next_command"],
            "owner": item["owner"],
            "blocker": item.get("blocker"),
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
        }
        for item in items
        if item["current_gate"] not in {"REJECTED_ARCHIVED", "ACTIVE_OR_ADOPTED"}
    ]
    return {
        "schema_id": "aegis_research_pipeline",
        "schema_version": "v1",
        "artifact_id": "aegis_research_pipeline_v1",
        "generated_at_utc": _now(),
        "day_utc": day_utc,
        "pipeline_columns": PIPELINE_COLUMNS,
        "operator_lifecycle_states": OPERATOR_LIFECYCLE_STATES,
        "hypothesis_tiers": priority_report.get("tier_definitions") or {},
        "priority_report": priority_report,
        "priority_pipeline": priority_report.get("priority_pipeline") or {},
        "priority_counts": priority_report.get("priority_counts") or {},
        "recommended_focus_today": priority_report.get("recommended_focus_today") or [],
        "operator_lifecycle_rules": [
            "Every hypothesis appears in one operator lifecycle state.",
            "Operator attention is derived from blocked, stale, failed, or review-required states.",
            "Research lifecycle state does not authorize broker execution, autonomous execution, sleeve mutation, or trade advice.",
        ],
        "gate_rules": [
            "No triage -> no test plan",
            "No test plan -> no test run",
            "No test result -> no review decision",
            "No review approval -> no paper trial",
            "No paper trial evidence -> no sleeve review",
            "No human approval -> no sleeve activation/change",
        ],
        "items": sorted(items, key=lambda row: (PIPELINE_COLUMNS.index(row["current_gate"]), row["hypothesis_id"])),
        "pipeline": columns,
        "counts": counts,
        "blocked_items": blocked,
        "next_operator_actions": next_actions,
        "human_approval_required": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
        "unsupported_ai_claims_allowed": False,
    }


def write_research_pipeline_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    main = write_json_v1(out_dir / "research_pipeline.v1.json", payload)
    summary = out_dir / "research_pipeline.summary.txt"
    matrix = out_dir / "research_pipeline.matrix.csv"
    summary.write_text(render_research_pipeline_summary_v1(payload), encoding="utf-8")
    matrix.write_text(render_research_pipeline_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(main), "summary": str(summary), "matrix": str(matrix)}


def render_research_pipeline_summary_v1(payload: dict[str, Any]) -> str:
    counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
    lines = [
        "AEGIS RESEARCH PIPELINE v1",
        f"day_utc: {payload.get('day_utc')}",
        f"hypothesis_count: {len(payload.get('items') or [])}",
        "pipeline_counts:",
    ]
    for column in PIPELINE_COLUMNS:
        lines.append(f"- {column}: {counts.get(column, 0)}")
    lines.extend(
        [
            f"blocked_items: {len(payload.get('blocked_items') or [])}",
            f"next_operator_actions: {len(payload.get('next_operator_actions') or [])}",
            "human_approval_required: true",
            "broker_execution_allowed: false",
            "autonomous_execution_allowed: false",
            "automatic_sleeve_mutation_allowed: false",
            "",
        ]
    )
    return "\n".join(lines)


def render_research_pipeline_matrix_csv_v1(payload: dict[str, Any]) -> str:
    fieldnames = [
        "hypothesis_id",
        "title",
        "classification",
        "current_gate",
        "gate_status",
        "blocker",
        "next_action",
        "next_command",
        "owner",
        "allowed_transitions",
    ]
    out = StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    for item in payload.get("items") or []:
        writer.writerow({key: ", ".join(item.get(key, [])) if isinstance(item.get(key), list) else item.get(key, "") for key in fieldnames})
    return out.getvalue()


def append_triage_record_v1(*, truth_root: Path, day_utc: str, hypothesis_id: str, decision: str, reason: str, operator: str) -> dict[str, Any]:
    normalized = str(decision or "").upper()
    if normalized not in TRIAGE_DECISIONS:
        raise ValueError(f"decision must be one of {sorted(TRIAGE_DECISIONS)}")
    _require_fields(hypothesis_id=hypothesis_id, reason=reason, operator=operator)
    event = _event("RESEARCH_HYPOTHESIS_TRIAGED", hypothesis_id, operator, reason)
    event.update(
        {
            "decision": normalized,
            "append_only": True,
            "human_approval_required": True,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
        }
    )
    path = _append_event(truth_root, TRIAGE_FAMILY, day_utc, "research_triage.v1.jsonl", event)
    event["path"] = str(path)
    return event


def build_research_plan_v1(*, truth_root: Path, day_utc: str, hypothesis_id: str) -> dict[str, Any]:
    if not hypothesis_id:
        raise ValueError("hypothesis_id is required")
    root = Path(truth_root).expanduser().resolve()
    pipeline = build_research_pipeline_v1(truth_root=root, day_utc=day_utc)
    item = _find_item(pipeline, hypothesis_id)
    spec = _research_spec(hypothesis_id, item)
    ready = item.get("current_gate") == "TEST_PLAN"
    data_status = _event_study_data_status(root, spec, day_utc=day_utc, hypothesis_id=hypothesis_id)
    return {
        "schema_id": "aegis_research_plan",
        "schema_version": "v1",
        "artifact_id": "aegis_research_plan_v1",
        "generated_at_utc": _now(),
        "day_utc": day_utc,
        "hypothesis_id": hypothesis_id,
        "title": item.get("title"),
        "plan_status": "READY_FOR_TESTING" if ready else "DRAFT_BLOCKED_PENDING_TRIAGE",
        "test_type": spec["test_type"],
        "test_question": spec["test_question"],
        "dataset_status": data_status["status"],
        "event_data_status": data_status.get("event_data_status", ""),
        "event_data_availability": data_status.get("event_data_availability", {}),
        "market_data_availability": data_status.get("market_data_availability", {}),
        "universe": spec["universe"],
        "required_outputs": spec["required_outputs"],
        "required_inputs": spec["required_inputs"],
        "success_criteria": spec["success_criteria"],
        "failure_criteria": spec["failure_criteria"],
        "cost_assumption": spec["cost_assumption"],
        "limitations": [
            "No test may advance the pipeline until triage evidence exists.",
            "No automatic sleeve mutation.",
            "The event-study runner must return DATA_NEEDED or TEST_NOT_IMPLEMENTED instead of fabricated results when data/engine support is absent.",
        ],
        "source_pipeline_gate": item.get("current_gate"),
        "gate_blocked": not ready,
        "blocker": None if ready else "TRIAGE_REQUIRED",
        "human_approval_required": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
    }


def write_research_plan_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / PLAN_FAMILY / day_utc / str(payload["hypothesis_id"])
    path = write_json_v1(out_dir / "research_plan.v1.json", payload)
    return {"json": str(path)}



def _float_or_none(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _row_price(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _float_or_none(row.get(key))
        if value is not None:
            return value
    return None


def _is_etf_drop_mean_reversion_hypothesis(hypothesis_id: str, spec: dict[str, Any]) -> bool:
    text = f"{hypothesis_id} {spec.get('test_question') or ''} {spec.get('test_type') or ''}".lower()
    return hypothesis_id == ETF_DROP_MEAN_REVERSION_HYPOTHESIS_ID or ("mean reversion" in text or "mean-reversion" in text) and "1-day drop" in text


def run_etf_drop_mean_reversion_study_v1(*, truth_root: Path, day_utc: str, hypothesis_id: str, spec: dict[str, Any], data_status: dict[str, Any]) -> dict[str, Any]:
    """Run the governed Phase 1 ETF drop mean-reversion evidence pass.

    Phase 1 deliberately does not fabricate historical forward returns. It checks
    the current governed market rows for reproducible large-drop trigger inputs,
    records the event/filter observations, and returns INCONCLUSIVE_SAMPLE_SIZE
    until enough governed forward-return observations exist.
    """
    symbol_rows = data_status.get("market_data_availability", {}).get("symbol_rows")
    symbol_rows = symbol_rows if isinstance(symbol_rows, dict) else {}
    etf_symbols = [symbol for symbol in ["SPY", "QQQ", "IWM"] if isinstance(symbol_rows.get(symbol), dict)]
    observations: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    vix_row = symbol_rows.get("VIX") if isinstance(symbol_rows.get("VIX"), dict) else {}
    vix_close = _row_price(vix_row, "close", "last", "last_price") if vix_row else None
    vix_filter_pass = vix_close is not None and vix_close < 35.0
    drop_threshold = -0.015
    for symbol in etf_symbols:
        row = symbol_rows.get(symbol) if isinstance(symbol_rows.get(symbol), dict) else {}
        close = _row_price(row, "close", "last", "last_price")
        previous = _row_price(row, "previous_close", "chart_previous_close", "chartPreviousClose", "prior_close")
        if previous is None:
            open_price = _row_price(row, "open")
            previous = open_price
        daily_return = None if close is None or previous in (None, 0) else (close - previous) / previous
        event_detected = daily_return is not None and daily_return <= drop_threshold and vix_filter_pass
        obs = {
            "symbol": symbol,
            "close": close,
            "comparison_price": previous,
            "daily_return": daily_return,
            "large_drop_threshold": drop_threshold,
            "large_drop_event": bool(event_detected),
            "vix_close": vix_close,
            "vix_filter_pass": bool(vix_filter_pass),
            "source": row.get("availability_source") or "market_data_report",
        }
        observations.append(obs)
        if event_detected:
            event_rows.append(obs)
    sample_size = len(event_rows)
    minimum_sample_size = 20
    metrics = {
        "event_count": {"value": sample_size, "metric_status": "COMPUTED", "sample_size": sample_size, "minimum_sample_size": minimum_sample_size},
        "forward_return_1d": {"value": None, "metric_status": "INSUFFICIENT_FORWARD_RETURN_SAMPLE", "sample_size": sample_size, "minimum_sample_size": minimum_sample_size},
        "forward_return_3d": {"value": None, "metric_status": "INSUFFICIENT_FORWARD_RETURN_SAMPLE", "sample_size": sample_size, "minimum_sample_size": minimum_sample_size},
        "win_rate_1d": {"value": None, "metric_status": "INSUFFICIENT_FORWARD_RETURN_SAMPLE", "sample_size": sample_size, "minimum_sample_size": minimum_sample_size},
        "win_rate_3d": {"value": None, "metric_status": "INSUFFICIENT_FORWARD_RETURN_SAMPLE", "sample_size": sample_size, "minimum_sample_size": minimum_sample_size},
        "expectancy": {"value": None, "metric_status": "INSUFFICIENT_FORWARD_RETURN_SAMPLE", "sample_size": sample_size, "minimum_sample_size": minimum_sample_size},
        "cost_adjusted_expectancy": {"value": None, "metric_status": "INSUFFICIENT_FORWARD_RETURN_SAMPLE", "sample_size": sample_size, "minimum_sample_size": minimum_sample_size},
    }
    return {
        "schema_id": "aegis_research_test_result",
        "schema_version": "v1",
        "artifact_id": "aegis_research_test_result_v1",
        "generated_at_utc": _now(),
        "day_utc": day_utc,
        "hypothesis_id": hypothesis_id,
        "test_type": spec["test_type"],
        "test_question": spec["test_question"],
        "test_status": "INCONCLUSIVE_SAMPLE_SIZE",
        "latest_result": "INCONCLUSIVE_SAMPLE_SIZE",
        "result_summary": f"ETF drop mean-reversion runner executed against governed market rows. sample_size={sample_size}/{minimum_sample_size}; no paper validation until enough forward-return events exist.",
        "blocker": "INSUFFICIENT_FORWARD_RETURN_SAMPLE",
        "missing_runner": "",
        "required_inputs": data_status["required_inputs"],
        "next_engine_to_build": "historical_forward_return_observation_store_v1" if sample_size < minimum_sample_size else "",
        "market_data_found": data_status["market_data_found"],
        "market_data_paths": data_status["market_data_paths"],
        "required_symbols": data_status.get("required_symbols") or [],
        "market_data_availability": data_status.get("market_data_availability") or {},
        "event_data_status": "ETF_DROP_EVENT_SCAN_EXECUTED",
        "event_data_availability": {
            "status": "AVAILABLE",
            "event_rule": "daily_return <= -1.5% and VIX < 35",
            "observed_rows": observations,
            "event_rows": event_rows,
        },
        "minimum_sample_size": minimum_sample_size,
        "sample_size": sample_size,
        "metric_status": "COMPUTED_INCONCLUSIVE_SAMPLE_SIZE",
        "fabricated_results": False,
        "metrics": metrics,
        "human_approval_required": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
        "trade_advice_allowed": False,
    }

def run_research_test_v1(*, truth_root: Path, day_utc: str, hypothesis_id: str) -> dict[str, Any]:
    if not hypothesis_id:
        raise ValueError("hypothesis_id is required")
    root = Path(truth_root).expanduser().resolve()
    pipeline = build_research_pipeline_v1(truth_root=root, day_utc=day_utc)
    item = _find_item(pipeline, hypothesis_id)
    spec = _research_spec(hypothesis_id, item)
    if hypothesis_id == NVIDIA_HYPOTHESIS_ID:
        maybe_import_staged_event_data_v1(truth_root=root, day_utc=day_utc, hypothesis_id=hypothesis_id)
    data_status = _event_study_data_status(root, spec, day_utc=day_utc, hypothesis_id=hypothesis_id, write_missing=True)
    if _is_etf_drop_mean_reversion_hypothesis(hypothesis_id, spec) and data_status.get("status") == "AVAILABLE":
        return run_etf_drop_mean_reversion_study_v1(truth_root=root, day_utc=day_utc, hypothesis_id=hypothesis_id, spec=spec, data_status=data_status)
    if hypothesis_id == NVIDIA_HYPOTHESIS_ID and data_status.get("status") == "AVAILABLE":
        study = run_forward_return_event_study_v1(truth_root=root, day_utc=day_utc, hypothesis_id=hypothesis_id)
        if study.get("schema_id") == "forward_return_event_study_v1":
            return {
                "schema_id": "aegis_research_test_result",
                "schema_version": "v1",
                "artifact_id": "aegis_research_test_result_v1",
                "generated_at_utc": _now(),
                "day_utc": day_utc,
                "hypothesis_id": hypothesis_id,
                "test_type": spec["test_type"],
                "test_question": spec["test_question"],
                "test_status": str(study.get("status") or "RESULT_READY"),
                "latest_result": str(study.get("status") or "RESULT_READY"),
                "result_summary": f"Forward-return event study computed from governed earnings/event-window data. sample_size={study.get('sample_size')}",
                "blocker": "",
                "missing_runner": "",
                "required_inputs": data_status["required_inputs"],
                "next_engine_to_build": "",
                "market_data_found": data_status["market_data_found"],
                "market_data_paths": data_status["market_data_paths"],
                "required_symbols": data_status.get("required_symbols") or [],
                "market_data_availability": data_status.get("market_data_availability") or {},
                "event_data_status": data_status.get("event_data_status", ""),
                "event_data_availability": data_status.get("event_data_availability", {}),
                "event_calendar_path": data_status.get("event_data_availability", {}).get("calendar_artifact_path", ""),
                "event_window_dataset_path": data_status.get("event_data_availability", {}).get("event_window_artifact_path", ""),
                "forward_return_event_study_path": str(study.get("artifact_path") or ""),
                "minimum_sample_size": int(study.get("minimum_sample_size") or 20),
                "sample_size": int(study.get("sample_size") or 0),
                "metric_status": "COMPUTED",
                "fabricated_results": False,
                "metrics": study.get("metrics") if isinstance(study.get("metrics"), dict) else {},
                "human_approval_required": True,
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
                "automatic_sleeve_mutation_allowed": False,
                "trade_advice_allowed": False,
            }
    status = "DATA_NEEDED" if data_status["status"] != "AVAILABLE" else "TEST_NOT_IMPLEMENTED"
    blocker = "TRIAGE_REQUIRED" if item.get("current_gate") == "INBOX" else data_status.get("blocker") or "MISSING_EVENT_STUDY_RUNNER"
    return {
        "schema_id": "aegis_research_test_result",
        "schema_version": "v1",
        "artifact_id": "aegis_research_test_result_v1",
        "generated_at_utc": _now(),
        "day_utc": day_utc,
        "hypothesis_id": hypothesis_id,
        "test_type": spec["test_type"],
        "test_question": spec["test_question"],
        "test_status": status,
        "latest_result": status,
        "result_summary": data_status["summary"],
        "blocker": blocker,
        "missing_runner": "" if status == "DATA_NEEDED" else "research_event_study_runner_v1",
        "required_inputs": data_status["required_inputs"],
        "next_engine_to_build": "ops/aegis/research_lab/research_backtest_runner_v1.py",
        "market_data_found": data_status["market_data_found"],
        "market_data_paths": data_status["market_data_paths"],
        "required_symbols": data_status.get("required_symbols") or [],
        "market_data_availability": data_status.get("market_data_availability") or {},
        "event_data_status": data_status.get("event_data_status", ""),
        "event_data_availability": data_status.get("event_data_availability", {}),
        "accepted_upload_format": data_status.get("accepted_upload_format", {}),
        "minimum_sample_size": 20,
        "sample_size": data_status["sample_size"],
        "metric_status": "MISSING_INPUT" if status == "DATA_NEEDED" else "TEST_NOT_IMPLEMENTED",
        "fabricated_results": False,
        "metrics": {key: {"value": None, "metric_status": "MISSING_INPUT" if status == "DATA_NEEDED" else "TEST_NOT_IMPLEMENTED", "sample_size": data_status["sample_size"], "minimum_sample_size": 20} for key in spec["metric_keys"]},
        "human_approval_required": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
        "trade_advice_allowed": False,
    }


def write_research_test_result_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / RESULT_FAMILY / day_utc / str(payload["hypothesis_id"])
    path = write_json_v1(out_dir / "research_test_result.v1.json", payload)
    return {"json": str(path)}


def append_research_review_decision_v1(*, truth_root: Path, day_utc: str, hypothesis_id: str, decision: str, reason: str, operator: str) -> dict[str, Any]:
    normalized = str(decision or "").upper()
    if normalized not in REVIEW_DECISIONS:
        raise ValueError(f"decision must be one of {sorted(REVIEW_DECISIONS)}")
    stored_decision = "REJECT" if normalized == "REJECTED" else normalized
    _require_fields(hypothesis_id=hypothesis_id, reason=reason, operator=operator)
    root = Path(truth_root).expanduser().resolve()
    pipeline = build_research_pipeline_v1(truth_root=root, day_utc=day_utc)
    item = _find_item(pipeline, hypothesis_id)
    if item.get("current_gate") != "RESULT_REVIEW":
        raise ValueError(f"hypothesis {hypothesis_id} is not ready for RESULT_REVIEW; current_gate={item.get('current_gate')}")
    event = _event("RESEARCH_REVIEW_DECISION_RECORDED", hypothesis_id, operator, reason)
    event.update(
        {
            "decision": normalized,
            "normalized_decision": stored_decision,
            "append_only": True,
            "human_approval_required": True,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
        }
    )
    path = _append_event(root, REVIEW_FAMILY, day_utc, "research_review_decisions.v1.jsonl", event)
    event["path"] = str(path)
    return event


def _pipeline_item(*, hypothesis: dict[str, Any], triage: dict[str, Any] | None, plan: dict[str, Any] | None, result: dict[str, Any] | None, review: dict[str, Any] | None, truth_root: Path | None = None, day_utc: str = "") -> dict[str, Any]:
    hypothesis_id = str(hypothesis.get("hypothesis_id") or "")
    classification = str(hypothesis.get("classification") or "UNKNOWN")
    normalized_status = str(hypothesis.get("normalized_status") or hypothesis.get("status") or "UNKNOWN").upper()
    source_artifacts = [value for value in [hypothesis.get("source_artifact") or hypothesis.get("path"), triage and triage.get("path"), plan and plan.get("path"), result and result.get("path"), review and review.get("path")] if value]
    source_hashes = {str(hypothesis.get("source_artifact") or hypothesis.get("path") or hypothesis_id): hypothesis.get("source_hash", "")}

    if classification in {"TEST_FIXTURE", "LEGACY_DUPLICATE", "REJECTED", "ARCHIVED"} or normalized_status in {"TEST_FIXTURE", "DUPLICATE", "REJECTED", "ARCHIVED"}:
        return _item(hypothesis, "REJECTED_ARCHIVED", "ARCHIVED" if classification in {"TEST_FIXTURE", "LEGACY_DUPLICATE", "ARCHIVED"} else "REJECTED", None, "No active research action", "", "AEGIS", [], source_artifacts, source_hashes, latest_result=result)

    if not triage:
        return _item(hypothesis, "INBOX", "READY", None, "Run triage", _triage_command(hypothesis_id, "QUEUE_TEST_PLAN"), "OPERATOR", ["TRIAGE"], source_artifacts, source_hashes, latest_result=result)

    triage_decision = str(triage.get("decision") or "").upper()
    if triage_decision in {"REJECT", "ARCHIVE", "DUPLICATE"}:
        return _item(hypothesis, "REJECTED_ARCHIVED", "REJECTED" if triage_decision == "REJECT" else "ARCHIVED", None, "No active research action", "", "OPERATOR", [], source_artifacts, source_hashes, latest_result=result)
    if triage_decision == "NEEDS_CLARIFICATION":
        return _item(hypothesis, "TRIAGE", "NEEDS_OPERATOR", "NEEDS_CLARIFICATION", "Clarify hypothesis", _triage_command(hypothesis_id, "QUEUE_TEST_PLAN"), "OPERATOR", ["QUEUE_TEST_PLAN", "REJECT", "ARCHIVE", "DUPLICATE"], source_artifacts, source_hashes, latest_result=result)

    if not plan:
        return _item(hypothesis, "TEST_PLAN", "READY", None, "Build test plan", f"npm run aegis:build-research-plan -- --hypothesis-id {hypothesis_id}", "AEGIS", ["TESTING"], source_artifacts, source_hashes, latest_result=result)

    spec = _research_spec(hypothesis_id, {"title": str(hypothesis.get("title") or ""), "short_summary": str(hypothesis.get("hypothesis_summary") or "")})
    current_data_status = _event_study_data_status(Path(truth_root) if truth_root else Path(""), spec, day_utc=day_utc, hypothesis_id=hypothesis_id)
    if not result:
        dataset_status = str(plan.get("dataset_status") or "UNKNOWN").upper()
        if dataset_status in {"MISSING", "DATA_NEEDED"} and current_data_status.get("status") != "AVAILABLE":
            current_blocker = str(current_data_status.get("blocker") or "CURRENT_INTRADAY_MARKET_DATA_NEEDED")
            current_status = "WAITING_FOR_EVENT_DATA" if current_blocker in {"EARNINGS_EVENT_CALENDAR_REQUIRED", "EVENT_WINDOW_OHLCV_DATA_REQUIRED"} else "BLOCKED"
            return _item(hypothesis, "TESTING", current_status, current_blocker, "Run research test", f"npm run aegis:run-research-test -- --hypothesis-id {hypothesis_id}", "OPERATOR", [], source_artifacts, source_hashes, latest_result=result)
        return _item(hypothesis, "TESTING", "READY", None, "Run research test", f"npm run aegis:run-research-test -- --hypothesis-id {hypothesis_id}", "AEGIS", ["RESULT_REVIEW"], source_artifacts, source_hashes, latest_result=result)

    result_status = str(result.get("test_status") or result.get("latest_result") or "").upper()
    if result_status in {"DATA_NEEDED", "MISSING_INPUT", "INSUFFICIENT_DATA"}:
        if current_data_status.get("status") == "AVAILABLE":
            refreshed_result = dict(result)
            refreshed_result["test_status"] = "DATA_NOW_AVAILABLE_RERUN_REQUIRED"
            refreshed_result["latest_result"] = "DATA_NOW_AVAILABLE_RERUN_REQUIRED"
            refreshed_result["result_summary"] = current_data_status.get("summary") or "Current intraday market data is available; rerun the research test."
            return _item(hypothesis, "TESTING", "READY", None, "Run research test", f"npm run aegis:run-research-test -- --hypothesis-id {hypothesis_id}", "AEGIS", ["RESULT_REVIEW"], source_artifacts, source_hashes, latest_result=refreshed_result)
        result_blocker = str(result.get("blocker") or current_data_status.get("blocker") or "CURRENT_INTRADAY_MARKET_DATA_NEEDED")
        result_gate_status = "WAITING_FOR_EVENT_DATA" if result_blocker in {"EARNINGS_EVENT_CALENDAR_REQUIRED", "EVENT_WINDOW_OHLCV_DATA_REQUIRED"} else "BLOCKED"
        return _item(hypothesis, "TESTING", result_gate_status, result_blocker, "Acquire required data", f"npm run aegis:run-research-test -- --hypothesis-id {hypothesis_id}", "AEGIS", [], source_artifacts, source_hashes, latest_result=result)

    if not review:
        return _item(hypothesis, "RESULT_REVIEW", "NEEDS_OPERATOR", None, "Review test result", f'npm run aegis:record-research-review -- --hypothesis-id {hypothesis_id} --decision PAPER_TEST_CANDIDATE --reason "..." --operator David', "OPERATOR", ["PAPER_TRIAL", "SLEEVE_REVIEW", "REJECTED_ARCHIVED"], source_artifacts, source_hashes, latest_result=result)

    review_decision = str(review.get("normalized_decision") or review.get("decision") or "").upper()
    if review_decision == "PAPER_TEST_CANDIDATE":
        return _item(hypothesis, "PAPER_TRIAL", "READY", None, "Track paper trial outcomes", "", "OPERATOR", ["SLEEVE_REVIEW", "REJECTED_ARCHIVED"], source_artifacts, source_hashes, latest_result=result)
    if review_decision == "SLEEVE_REVIEW_CANDIDATE":
        return _item(hypothesis, "SLEEVE_REVIEW", "NEEDS_OPERATOR", None, "Review sleeve adoption/modification", "", "OPERATOR", ["ACTIVE_OR_ADOPTED", "REJECTED_ARCHIVED"], source_artifacts, source_hashes, latest_result=result)
    if review_decision in {"REJECT", "REJECTED", "ARCHIVE"}:
        return _item(hypothesis, "REJECTED_ARCHIVED", "REJECTED" if review_decision in {"REJECT", "REJECTED"} else "ARCHIVED", None, "No active research action", "", "OPERATOR", [], source_artifacts, source_hashes, latest_result=result)
    return _item(hypothesis, "RESULT_REVIEW", "BLOCKED", "NEEDS_MORE_EVIDENCE", "Collect more evidence", f"npm run aegis:run-research-test -- --hypothesis-id {hypothesis_id}", "AEGIS", ["RESULT_REVIEW"], source_artifacts, source_hashes, latest_result=result)


def _item(hypothesis: dict[str, Any], gate: str, status: str, blocker: str | None, next_action: str, next_command: str, owner: str, transitions: list[str], source_artifacts: list[str], source_hashes: dict[str, str], *, latest_result: dict[str, Any] | None) -> dict[str, Any]:
    hypothesis_id = str(hypothesis.get("hypothesis_id") or "")
    title = str(hypothesis.get("title") or hypothesis.get("hypothesis_summary") or hypothesis_id)
    result_status = ""
    result_summary = ""
    if isinstance(latest_result, dict):
        result_status = str(latest_result.get("test_status") or latest_result.get("latest_result") or "")
        result_summary = str(latest_result.get("result_summary") or latest_result.get("latest_result") or "")
    spec = _research_spec(hypothesis_id, {"title": title})
    operator_blocker = _operator_blocker_v1(hypothesis=hypothesis, gate=gate, status=status, blocker=blocker, spec=spec)
    operator_next_action = operator_blocker.get("next_action") if operator_blocker.get("state") != "NO_BLOCKER" else ""
    return {
        "hypothesis_id": hypothesis_id,
        "title": title,
        "readable_title": title,
        "short_summary": str(hypothesis.get("hypothesis_summary") or title)[:220],
        "source": _source_type(hypothesis),
        "symbols": _unique_texts(hypothesis.get("symbols"), hypothesis.get("instruments"), spec.get("universe")),
        "related_symbols": _unique_texts(hypothesis.get("required_symbols"), hypothesis.get("symbols"), hypothesis.get("instruments"), spec.get("universe")),
        "event_type": str(hypothesis.get("event_type") or ""),
        "required_data": _unique_texts(hypothesis.get("required_data"), hypothesis.get("missing_datasets"), spec.get("required_inputs")),
        "classification": hypothesis.get("classification", "UNKNOWN"),
        "normalized_status": hypothesis.get("normalized_status", "UNKNOWN"),
        "current_gate": gate,
        "operator_lifecycle_state": _operator_lifecycle_state(gate, status, blocker),
        "gate_status": status,
        "blocker": blocker,
        "operator_blocker": operator_blocker,
        "recovery_strategy": operator_blocker.get("recovery_strategy", "NO_BLOCKER"),
        "expected_recovery": operator_blocker.get("expected_recovery", "No recovery needed."),
        "operator_action_required": bool(operator_blocker.get("operator_action_required", False)),
        "data_acquisition_plan": operator_blocker.get("data_acquisition_plan", {}),
        "next_action": operator_next_action or next_action,
        "next_command": next_command,
        "primary_command": next_command,
        "alternate_commands": _alternate_commands(hypothesis_id, gate),
        "owner": owner,
        "evidence_required": _evidence_required_for_gate(gate),
        "evidence_needed": _evidence_required_for_gate(gate),
        "evidence_available": source_artifacts,
        "missing_evidence": _missing_evidence_for_gate(gate, status),
        "success_criteria": ["Evidence-backed result", "Human review before promotion", "No automatic sleeve mutation"],
        "failure_criteria": ["Duplicate", "Not testable", "Insufficient evidence", "Overfit risk"],
        "latest_result": latest_result,
        "latest_result_status": result_status or "No result yet",
        "latest_result_summary": result_summary or "No test result has been recorded.",
        "test_type": spec["test_type"],
        "test_question": spec["test_question"],
        "required_datasets": spec["required_inputs"],
        "expected_output": spec["required_outputs"],
        "allowed_transitions": transitions,
        "source_artifacts": source_artifacts,
        "source_hashes": source_hashes,
        "human_approval_required": True,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
        "automatic_sleeve_mutation_allowed": False,
    }


def _operator_blocker_v1(*, hypothesis: dict[str, Any], gate: str, status: str, blocker: str | None, spec: dict[str, Any]) -> dict[str, Any]:
    raw_codes = [str(item) for item in [blocker, status, gate, hypothesis.get("recommended_next_action")] if str(item or "")]
    raw_codes.extend(str(item) for item in hypothesis.get("blocking_items") or [] if str(item))
    raw_codes.extend(str(item) for item in hypothesis.get("why_blocked") or [] if str(item))
    readiness = hypothesis.get("readiness") if isinstance(hypothesis.get("readiness"), dict) else {}
    raw_codes.extend(str(item) for item in readiness.get("blocking_items") or [] if str(item))
    missing_symbols = _unique_texts(
        hypothesis.get("missing_symbols"),
        readiness.get("missing_symbols"),
        hypothesis.get("symbols_missing"),
    )
    required_symbols = _unique_texts(
        hypothesis.get("required_symbols"),
        readiness.get("required_symbols"),
        hypothesis.get("proposed_universe"),
        spec.get("universe"),
    )
    missing_datasets = _unique_texts(
        hypothesis.get("missing_datasets"),
        readiness.get("missing_datasets"),
        hypothesis.get("required_dataset"),
    )
    raw_text = " ".join(raw_codes).lower()
    if str(gate or "").upper() == "RESULT_REVIEW" and not blocker:
        plan = _data_acquisition_plan_v1(
            data_needed=[],
            source_type="OPERATOR_DECISION_REQUIRED",
            why_needed="The event-study result is ready for human review before any lifecycle promotion.",
            action_label="Review result",
            action_kind="REVIEW_RESULT",
            action_available=True,
            next_action="Review test result.",
        )
        return _operator_blocker_payload_v1(
            state="WAITING_ON_RESULT_REVIEW",
            title="Result review required",
            summary="Governed research evidence is available; an operator must review the result before promotion.",
            why_it_matters="Research promotion requires explicit human review and does not authorize trading.",
            missing_items=[],
            recovery_strategy="WAITING_ON_OPERATOR_DECISION",
            expected_recovery="Recovery after an operator records a research review decision.",
            operator_action_required=True,
            next_action=plan["next_action"],
            action_label=plan["action_label"],
            action_kind=plan["action_kind"],
            raw_codes=raw_codes,
            data_acquisition_plan=plan,
        )
    if "provider_not_configured" in raw_text or "market data provider is not configured" in raw_text or "provider_not_available" in raw_text:
        plan = _data_acquisition_plan_v1(
            data_needed=missing_symbols or required_symbols or ["Market data provider"],
            source_type="USER_MUST_CONFIGURE_PROVIDER",
            why_needed="Aegis cannot fetch market data until a provider is configured.",
            action_label="Configure provider",
            action_kind="CONFIGURE_PROVIDER",
            action_available=False,
            next_action="Configure AEGIS_MARKET_DATA_PRIMARY_PROVIDER or AEGIS_MARKET_DATA_FALLBACK_PROVIDER, then refresh the Research Pipeline.",
            required_config_keys=["AEGIS_MARKET_DATA_PRIMARY_PROVIDER", "AEGIS_MARKET_DATA_FALLBACK_PROVIDER"],
            last_attempt_status="PROVIDER_NOT_CONFIGURED",
            last_failure_reason="No configured market data provider was available.",
        )
        return _operator_blocker_payload_v1(
            state="BLOCKED_PROVIDER_CONFIGURATION_REQUIRED",
            title="Blocked: Market data provider is not configured",
            summary="Aegis cannot fetch the required data until a market data provider is configured.",
            why_it_matters=plan["why_needed"],
            missing_items=plan["data_needed"],
            recovery_strategy="WAITING_ON_PROVIDER_CONFIGURATION",
            expected_recovery="Recovery after provider configuration is present and the pipeline refreshes.",
            operator_action_required=True,
            next_action=plan["next_action"],
            action_label=plan["action_label"],
            action_kind=plan["action_kind"],
            raw_codes=raw_codes,
            data_acquisition_plan=plan,
        )
    if "earnings_event_calendar_required" in raw_text or "external earnings calendar" in raw_text or "nvidia historical earnings" in raw_text:
        plan = _data_acquisition_plan_v1(
            data_needed=missing_datasets or ["NVDA historical earnings dates/times"],
            source_type="USER_MUST_UPLOAD",
            why_needed="The NVIDIA event-dislocation sample cannot be defined without validated earnings dates and release timing.",
            action_label="Upload earnings calendar",
            action_kind="UPLOAD_EARNINGS_EVENT_CALENDAR",
            action_available=False,
            next_action="External earnings calendar required. Place an approved CSV in /home/node/constellation_runtime_data/research_data/manual_drop/earnings_event_calendar.csv or import it with the governed event-data importer.",
            accepted_format=event_upload_format_v1()["earnings_event_calendar_v1"]["accepted_format"],
            last_attempt_status="UPLOAD_REQUIRED",
            last_failure_reason="A governed earnings calendar artifact is not present for this hypothesis.",
        )
        return _operator_blocker_payload_v1(
            state="BLOCKED_EXTERNAL_EARNINGS_CALENDAR_REQUIRED",
            title="External earnings calendar required",
            summary="Aegis cannot run the NVIDIA event-dislocation study until validated earnings dates and timing are available.",
            why_it_matters=plan["why_needed"],
            missing_items=plan["data_needed"],
            recovery_strategy="WAITING_ON_OPERATOR_UPLOAD",
            expected_recovery="Recovery after the earnings calendar CSV is imported and the Research Pipeline refreshes.",
            operator_action_required=True,
            next_action=plan["next_action"],
            action_label=plan["action_label"],
            action_kind=plan["action_kind"],
            raw_codes=raw_codes,
            data_acquisition_plan=plan,
        )
    if "event_window_ohlcv_data_required" in raw_text or "event-window ohlcv" in raw_text:
        plan = _data_acquisition_plan_v1(
            data_needed=["NVDA, SMH, SOXX, QQQ, XLK, SPY, and VIX event-window OHLCV"],
            source_type="USER_MUST_UPLOAD",
            why_needed="The earnings calendar is present, but the event-window study needs validated OHLCV rows around each event.",
            action_label="Upload event-window OHLCV",
            action_kind="UPLOAD_EVENT_WINDOW_OHLCV",
            action_available=False,
            next_action="Upload event-window OHLCV, then rerun the NVIDIA event study.",
            accepted_format=event_upload_format_v1()["research_event_window_dataset_v1"]["accepted_format"],
            last_attempt_status="UPLOAD_REQUIRED",
            last_failure_reason="A governed research_event_window_dataset_v1 artifact is not present for this hypothesis.",
        )
        return _operator_blocker_payload_v1(
            state="BLOCKED_EVENT_WINDOW_DATASET_REQUIRED",
            title="Event-window OHLCV dataset required",
            summary="Aegis has the event-data path but cannot compute forward returns until event-window OHLCV is available.",
            why_it_matters=plan["why_needed"],
            missing_items=plan["data_needed"],
            recovery_strategy="WAITING_ON_OPERATOR_UPLOAD",
            expected_recovery="Recovery after the event-window OHLCV CSV is imported and the research test reruns.",
            operator_action_required=True,
            next_action=plan["next_action"],
            action_label=plan["action_label"],
            action_kind=plan["action_kind"],
            raw_codes=raw_codes,
            data_acquisition_plan=plan,
        )
    if "macro_event_calendar" in raw_text or "macro event calendar" in raw_text:
        missing = missing_datasets or ["Macro event calendar"]
        plan = _data_acquisition_plan_v1(
            data_needed=missing,
            source_type="USER_MUST_UPLOAD",
            why_needed="The event-study sample cannot be defined without the macro event calendar.",
            action_label="Upload macro calendar",
            action_kind="UPLOAD_DATASET",
            action_available=False,
            next_action="Upload a macro calendar when dataset upload is available, or place the approved file through the governed research evidence intake.",
            accepted_format="CSV with columns: date,event_name,country,impact",
            last_attempt_status="UPLOAD_REQUIRED",
            last_failure_reason="Aegis does not currently have this external dataset in the evidence store.",
        )
        return _operator_blocker_payload_v1(
            state="BLOCKED_EXTERNAL_DATASET_REQUIRED",
            title="Blocked: External dataset required",
            summary="Aegis cannot continue until a required research dataset is available.",
            why_it_matters="The test would be incomplete without the event calendar or dataset that defines the research sample.",
            missing_items=missing,
            recovery_strategy="WAITING_ON_OPERATOR_UPLOAD",
            expected_recovery="Recovery after the required dataset is uploaded and the research projection refreshes.",
            operator_action_required=True,
            next_action=plan["next_action"],
            action_label=plan["action_label"],
            action_kind=plan["action_kind"],
            raw_codes=raw_codes,
            data_acquisition_plan=plan,
        )
    if "not_supported" in raw_text or "unsupported" in raw_text:
        plan = _data_acquisition_plan_v1(
            data_needed=missing_datasets or missing_symbols or required_symbols or ["Required research data"],
            source_type="NOT_CURRENTLY_SUPPORTED",
            why_needed="The required data source is not connected to an Aegis acquisition path yet.",
            action_label="Mark not available",
            action_kind="MARK_NOT_AVAILABLE",
            action_available=False,
            next_action="Mark the idea unsupported or archive it until a governed acquisition source exists.",
            last_attempt_status="NOT_SUPPORTED",
            last_failure_reason="No governed acquisition path is implemented for this data type.",
        )
        return _operator_blocker_payload_v1(
            state="BLOCKED_DATA_SOURCE_UNSUPPORTED",
            title="Blocked: Data source not currently supported",
            summary="Aegis does not currently have an acquisition path for this required data.",
            why_it_matters=plan["why_needed"],
            missing_items=plan["data_needed"],
            recovery_strategy="NOT_CURRENTLY_SUPPORTED",
            expected_recovery="Recovery requires a new governed data acquisition source.",
            operator_action_required=True,
            next_action=plan["next_action"],
            action_label=plan["action_label"],
            action_kind=plan["action_kind"],
            raw_codes=raw_codes,
            data_acquisition_plan=plan,
        )
    if "final_eod_certification_pending" in raw_text or "final eod certification" in raw_text:
        plan = _market_data_acquisition_plan_v1(hypothesis=hypothesis, readiness=readiness, data_needed=missing_symbols or required_symbols, stale=False)
        plan["source_type"] = "FINAL_EOD_CERTIFICATION_PENDING"
        plan["action_available"] = False
        plan["action_label"] = "Wait for final EOD"
        plan["action_kind"] = "WAIT_FINAL_EOD"
        plan["last_attempt_status"] = "FINAL_EOD_PENDING"
        plan["last_failure_reason"] = "Current intraday data is usable, but this research step explicitly requires final EOD certification."
        plan["next_action"] = "Wait for final EOD certification, then rerun the Research Pipeline."
        return _operator_blocker_payload_v1(
            state="BLOCKED_WAITING_FOR_FINAL_EOD_CERTIFICATION",
            title="Final EOD certification pending",
            summary="Current intraday data may be present, but this research step explicitly requires final certified EOD data.",
            why_it_matters="Final-only research must not use provisional intraday data.",
            missing_items=plan["data_needed"],
            recovery_strategy="WAITING_ON_FINAL_EOD_CERTIFICATION",
            expected_recovery="Recovery after final EOD certification completes.",
            operator_action_required=False,
            next_action=plan["next_action"],
            action_label=plan["action_label"],
            action_kind=plan["action_kind"],
            raw_codes=raw_codes,
            data_acquisition_plan=plan,
        )
    if "stale" in raw_text:
        plan = _market_data_acquisition_plan_v1(hypothesis=hypothesis, readiness=readiness, data_needed=missing_symbols or required_symbols, stale=True)
        return _operator_blocker_payload_v1(
            state="BLOCKED_STALE_MARKET_DATA",
            title="Blocked: Market data is stale",
            summary="Latest market-session data is not fresh enough for this research step.",
            why_it_matters="Aegis will not validate a hypothesis against stale market data.",
            missing_items=plan["data_needed"],
            recovery_strategy="WAITING_ON_MARKET_REFRESH",
            expected_recovery="Recovery after the next market data refresh completes.",
            operator_action_required=False,
            next_action=plan["next_action"],
            action_label=plan["action_label"],
            action_kind=plan["action_kind"],
            raw_codes=raw_codes,
            data_acquisition_plan=plan,
        )
    if "data_needed" in raw_text or "current_intraday_market_data_needed" in raw_text or "missing_required_symbols" in raw_text or "missing_symbols" in raw_text:
        data_needed = _unique_texts(missing_datasets, missing_symbols, required_symbols, spec.get("required_inputs"))
        if missing_datasets:
            plan = _data_acquisition_plan_v1(
                data_needed=data_needed,
                source_type="EXTERNAL_SOURCE_REQUIRED",
                why_needed="The event-study sample cannot be validated until the required earnings, OHLCV, volatility, and forward-return inputs are present.",
                action_label="Refresh pipeline",
                action_kind="REFRESH_PIPELINE",
                action_available=True,
                next_action="Acquire the governed research data, then refresh the Research Pipeline and rerun the research test.",
                last_attempt_status="DATA_NEEDED",
                last_failure_reason="Required event-study inputs are not present in the current research evidence store.",
            )
            return _operator_blocker_payload_v1(
                state="BLOCKED_RESEARCH_DATA_REQUIRED",
                title="Blocked: Research data required",
                summary="Aegis needs governed earnings, market, volatility, and forward-return inputs before this hypothesis can be tested.",
                why_it_matters=plan["why_needed"],
                missing_items=plan["data_needed"],
                recovery_strategy="WAITING_ON_RESEARCH_DATA",
                expected_recovery="Recovery after the required datasets are acquired and the pipeline refreshes.",
                operator_action_required=False,
                next_action=plan["next_action"],
                action_label=plan["action_label"],
                action_kind=plan["action_kind"],
                raw_codes=raw_codes,
                data_acquisition_plan=plan,
            )
        plan = _market_data_acquisition_plan_v1(hypothesis=hypothesis, readiness=readiness, data_needed=data_needed, stale=False)
        return _operator_blocker_payload_v1(
            state="BLOCKED_WAITING_FOR_MARKET_DATA",
            title="Waiting for current intraday market data",
            summary="Current intraday market data is not yet available for every required symbol.",
            why_it_matters="The hypothesis cannot be tested until required current price data is present and fresh.",
            missing_items=plan["data_needed"],
            recovery_strategy="WAITING_ON_MARKET_REFRESH",
            expected_recovery="Aegis will retry automatically at the next market refresh window.",
            operator_action_required=False,
            next_action=plan["next_action"],
            action_label=plan["action_label"],
            action_kind=plan["action_kind"],
            raw_codes=raw_codes,
            data_acquisition_plan=plan,
        )
    if "needs_clarification" in raw_text or str(status or "").upper() == "NEEDS_OPERATOR":
        plan = _data_acquisition_plan_v1(
            data_needed=[],
            source_type="OPERATOR_DECISION_REQUIRED",
            why_needed="Research lifecycle changes require explicit human review.",
            action_label="Review hypothesis",
            action_kind="REVIEW_HYPOTHESIS",
            action_available=True,
            next_action="Review the hypothesis and choose the next lifecycle step.",
        )
        return _operator_blocker_payload_v1(
            state="BLOCKED_WAITING_ON_OPERATOR_DECISION",
            title="Needs review",
            summary="Aegis needs an operator decision before this idea can move forward.",
            why_it_matters="Research lifecycle changes require explicit human review.",
            missing_items=[],
            recovery_strategy="WAITING_ON_OPERATOR_DECISION",
            expected_recovery="Recovery after an operator records a review decision.",
            operator_action_required=True,
            next_action=plan["next_action"],
            action_label=plan["action_label"],
            action_kind=plan["action_kind"],
            raw_codes=raw_codes,
            data_acquisition_plan=plan,
        )
    if blocker:
        plan = _data_acquisition_plan_v1(
            data_needed=missing_datasets or missing_symbols or required_symbols,
            source_type="EXTERNAL_SOURCE_REQUIRED",
            why_needed="The next research result would not be reliable until the missing input is resolved.",
            action_label="Refresh pipeline",
            action_kind="REFRESH_PIPELINE",
            action_available=True,
            next_action="Refresh the Research Pipeline after the missing input becomes available.",
            last_attempt_status="WAITING_FOR_INPUT",
        )
        return _operator_blocker_payload_v1(
            state="BLOCKED_RESEARCH_INPUT_REQUIRED",
            title="Blocked: Research input required",
            summary="Aegis cannot continue this research step with the currently available inputs.",
            why_it_matters=plan["why_needed"],
            missing_items=plan["data_needed"],
            recovery_strategy="WAITING_ON_EXTERNAL_SOURCE",
            expected_recovery="Recovery after the missing input becomes available and the pipeline refreshes.",
            operator_action_required=False,
            next_action=plan["next_action"],
            action_label=plan["action_label"],
            action_kind=plan["action_kind"],
            raw_codes=raw_codes,
            data_acquisition_plan=plan,
        )
    plan = _data_acquisition_plan_v1(
        data_needed=[],
        source_type="NO_DATA_REQUIRED",
        why_needed="No data recovery is needed.",
        action_label="",
        action_kind="NONE",
        action_available=False,
        next_action="No action needed.",
    )
    return _operator_blocker_payload_v1(
        state="NO_BLOCKER",
        title="No blocker",
        summary="No blocker is active.",
        why_it_matters="No recovery is needed.",
        missing_items=[],
        recovery_strategy="NO_BLOCKER",
        expected_recovery="No recovery needed.",
        operator_action_required=False,
        next_action="No action needed.",
        action_label="",
        action_kind="NONE",
        raw_codes=raw_codes,
        data_acquisition_plan=plan,
    )


def _operator_blocker_payload_v1(*, state: str, title: str, summary: str, why_it_matters: str, missing_items: list[str], recovery_strategy: str, expected_recovery: str, operator_action_required: bool, next_action: str, action_label: str, action_kind: str, raw_codes: list[str], data_acquisition_plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "state": state,
        "title": title,
        "summary": summary,
        "why_it_matters": why_it_matters,
        "missing_items": missing_items,
        "recovery_strategy": recovery_strategy,
        "expected_recovery": expected_recovery,
        "operator_action_required": operator_action_required,
        "next_action": next_action,
        "action_label": action_label,
        "action_kind": action_kind,
        "action_available": bool(data_acquisition_plan.get("action_available")),
        "data_acquisition_plan": data_acquisition_plan,
        "raw_blockers": sorted(set(raw_codes)),
    }


def _data_acquisition_plan_v1(*, data_needed: list[str], source_type: str, why_needed: str, action_label: str, action_kind: str, action_available: bool, next_action: str, retry_at_utc: str = "", next_scheduled_retry: str = "", retry_schedule: list[str] | None = None, last_attempt_status: str = "NOT_ATTEMPTED", last_failure_reason: str = "", accepted_format: str = "", required_config_keys: list[str] | None = None) -> dict[str, Any]:
    return {
        "data_needed": data_needed,
        "why_needed": why_needed,
        "source_type": source_type,
        "action_label": action_label,
        "action_kind": action_kind,
        "action_available": action_available,
        "next_action": next_action,
        "retry_at_utc": retry_at_utc,
        "next_scheduled_retry": next_scheduled_retry,
        "retry_schedule": retry_schedule or [],
        "last_attempt_status": last_attempt_status,
        "last_failure_reason": last_failure_reason,
        "accepted_format": accepted_format,
        "required_config_keys": required_config_keys or [],
    }


def _market_data_acquisition_plan_v1(*, hypothesis: dict[str, Any], readiness: dict[str, Any], data_needed: list[str], stale: bool) -> dict[str, Any]:
    retry_at_utc = str(hypothesis.get("retry_at_utc") or readiness.get("retry_at_utc") or _default_market_retry_at_utc(str(hypothesis.get("day_utc") or readiness.get("day_utc") or "")))
    last_attempt_status = str(hypothesis.get("last_fetch_attempt_status") or readiness.get("last_fetch_attempt_status") or ("STALE" if stale else "MISSING"))
    failure_reason = str(hypothesis.get("last_fetch_failure_reason") or readiness.get("last_fetch_failure_reason") or ("Latest provider data is stale under the current-session policy." if stale else "Required symbols were not present in the current market data cache."))
    return _data_acquisition_plan_v1(
        data_needed=data_needed,
        source_type="AEGIS_CAN_FETCH_AUTOMATICALLY",
        why_needed="Current OHLCV/market data is required before Aegis can test this hypothesis.",
        action_label="Fetch market data now",
        action_kind="FETCH_MARKET_DATA_NOW",
        action_available=True,
        next_action="Fetch market data now, or wait for the next automatic market refresh.",
        retry_at_utc=retry_at_utc,
        next_scheduled_retry=retry_at_utc or "Next configured market refresh window",
        retry_schedule=["09:35 America/New_York", "11:55 America/New_York", "15:45 America/New_York", "16:10 America/New_York"],
        last_attempt_status=last_attempt_status,
        last_failure_reason=failure_reason,
    )


def _default_market_retry_at_utc(day_utc: str) -> str:
    day = str(day_utc or "").strip()
    if not day:
        return ""
    # Market-data refresh timer is 09:35 America/New_York. For current Aegis dates
    # in May this is 13:35 UTC; explicit timer strings remain in retry_schedule.
    return f"{day}T13:35:00Z"


def _unique_texts(*values: Any) -> list[str]:
    out: list[str] = []
    for value in values:
        if value is None:
            continue
        rows = value if isinstance(value, list) else [value]
        for row in rows:
            text = str(row or "").strip()
            if text and text not in out:
                out.append(text)
    return out


def _operator_lifecycle_state(gate: str, status: str, blocker: str | None) -> str:
    normalized_status = str(status or "").upper()
    if normalized_status == "WAITING_FOR_EVENT_DATA" or str(blocker or "").upper() in {"EARNINGS_EVENT_CALENDAR_REQUIRED", "EVENT_WINDOW_OHLCV_DATA_REQUIRED"}:
        return "RESEARCHING"
    if blocker or normalized_status == "BLOCKED":
        return "BLOCKED"
    normalized = str(gate or "").upper()
    if normalized == "INBOX":
        return "IDEA"
    if normalized in {"TRIAGE", "TEST_PLAN"}:
        return "RESEARCHING"
    if normalized in {"TESTING", "RESULT_REVIEW"}:
        return "VALIDATING"
    if normalized == "PAPER_TRIAL":
        return "PAPER_TRIAL"
    if normalized in {"SLEEVE_REVIEW", "ACTIVE_OR_ADOPTED"}:
        return "READY"
    if normalized == "REJECTED_ARCHIVED":
        return "ARCHIVED"
    return "IDEA"


def _alternate_commands(hypothesis_id: str, gate: str) -> list[dict[str, str]]:
    if gate == "INBOX":
        return [
            {"label": "Queue for testing", "command": f'npm run aegis:triage-hypothesis -- --hypothesis-id {hypothesis_id} --decision QUEUE_TEST_PLAN --reason "Worth testing" --operator David'},
            {"label": "Reject", "command": f'npm run aegis:triage-hypothesis -- --hypothesis-id {hypothesis_id} --decision REJECT --reason "Not worth testing" --operator David'},
            {"label": "Needs clarification", "command": f'npm run aegis:triage-hypothesis -- --hypothesis-id {hypothesis_id} --decision NEEDS_CLARIFICATION --reason "Needs clearer test definition" --operator David'},
        ]
    if gate == "TEST_PLAN":
        return [{"label": "Build test plan", "command": f"npm run aegis:build-research-plan -- --hypothesis-id {hypothesis_id}"}]
    if gate == "TESTING":
        return [{"label": "Run research test", "command": f"npm run aegis:run-research-test -- --hypothesis-id {hypothesis_id}"}]
    if gate == "RESULT_REVIEW":
        return [
            {"label": "Promote to paper trial", "command": f'npm run aegis:record-research-review -- --hypothesis-id {hypothesis_id} --decision PAPER_TEST_CANDIDATE --reason "Passed initial research criteria" --operator David'},
            {"label": "Reject result", "command": f'npm run aegis:record-research-review -- --hypothesis-id {hypothesis_id} --decision REJECTED --reason "Research result not strong enough" --operator David'},
        ]
    return []


def _research_spec(hypothesis_id: str, item: dict[str, Any]) -> dict[str, Any]:
    specs = {
        "rh-edge-2026-0101": {
            "test_type": "EVENT_STUDY / REGIME_ANALYSIS",
            "test_question": "After volatility overshoot events in liquid ETFs, do 1-3 session forward returns show statistically useful mean reversion after costs?",
            "universe": ["SPY", "QQQ", "IWM", "DIA"],
            "required_outputs": [
                "event_count",
                "average_1d_forward_return",
                "average_2d_forward_return",
                "average_3d_forward_return",
                "win_rate",
                "expectancy",
                "max_adverse_excursion_if_available",
                "regime_filter_used",
                "sample_size",
                "cost_assumption",
                "pass_fail_inconclusive",
            ],
            "required_inputs": [
                "daily_ohlc_history_for_SPY_QQQ_IWM_DIA",
                "volatility_overshoot_event_labels_or_rule",
                "1d_2d_3d_forward_return_engine",
                "cost_model",
            ],
            "metric_keys": [
                "event_count",
                "forward_return_1d",
                "forward_return_2d",
                "forward_return_3d",
                "win_rate",
                "expectancy",
                "max_adverse_excursion",
                "cost_adjusted_expectancy",
            ],
            "success_criteria": [
                "At least 20 confirmed overshoot events.",
                "Cost-adjusted expectancy is positive over the evaluated forward windows.",
                "Win rate and adverse excursion do not contradict the mean-reversion thesis.",
            ],
            "failure_criteria": [
                "Fewer than 20 confirmed overshoot events.",
                "Cost-adjusted expectancy is non-positive.",
                "Forward-return evidence is unstable across 1-3 sessions.",
            ],
            "cost_assumption": "Explicit ETF round-trip cost model required before pass/fail classification.",
        },
        "rh-nvidia-earnings-event-dislocation-v1": {
            "test_type": "EVENT_STUDY / EVENT_DISLOCATION",
            "test_question": "After NVIDIA earnings releases, do abnormal gaps, volatility compression or expansion, and follow-through or reversal patterns create statistically useful 1-day, 3-day, and 5-day signals across NVDA, semiconductor ETFs, and mega-cap technology indices?",
            "universe": ["NVDA", "SMH", "SOXX", "QQQ", "XLK", "SPY", "VIX"],
            "required_outputs": [
                "event_count",
                "earnings_timing_breakdown",
                "gap_size_distribution",
                "volume_surprise_distribution",
                "forward_return_1d",
                "forward_return_3d",
                "forward_return_5d",
                "continuation_vs_reversal_summary",
                "sector_spillover_summary",
                "index_spillover_summary",
                "volatility_regime_breakdown",
                "cost_assumption",
                "sample_size",
                "failure_cases",
                "pass_fail_inconclusive",
            ],
            "required_inputs": [
                "nvidia_historical_earnings_dates_times",
                "daily_ohlcv_history_for_NVDA_SMH_SOXX_QQQ_XLK_SPY",
                "vix_or_implied_volatility_proxy",
                "forward_returns_1d_3d_5d",
                "event_day_gap_size",
                "volume_surprise",
                "earnings_timing_pre_market_vs_after_hours",
                "baseline_non_event_days",
                "cost_model",
            ],
            "metric_keys": [
                "event_count",
                "gap_size",
                "volume_surprise",
                "forward_return_1d",
                "forward_return_3d",
                "forward_return_5d",
                "continuation_rate",
                "reversal_rate",
                "sector_spillover_expectancy",
                "index_spillover_expectancy",
                "cost_adjusted_expectancy",
            ],
            "success_criteria": [
                "Sample size is disclosed for all NVIDIA earnings events in the available history.",
                "At least one clearly defined regime or trigger shows repeatable positive expectancy after estimated costs.",
                "Continuation, reversal, volatility-crush, and spillover tests beat comparable non-event baselines without lookahead leakage.",
                "Failure cases and unstable regimes are documented before any paper-trial consideration.",
            ],
            "failure_criteria": [
                "Earnings dates or earnings timing cannot be reproduced from governed source data.",
                "Forward returns are indistinguishable from non-event baselines after estimated costs.",
                "The apparent effect is dominated by one event, one symbol, or a lookahead/survivorship leak.",
                "No stable trigger can be specified for large positive gap, large negative gap, muted reaction, high-volatility, or low-volatility regimes.",
            ],
            "cost_assumption": "Explicit ETF/equity round-trip cost and slippage assumptions are required before pass/fail classification or paper-trial promotion.",
        },
        "rh-edge-2026-0103": {
            "test_type": "EVENT_STUDY",
            "test_question": "After volatility compression conditions in liquid index ETFs, does next-session return show positive expectancy after estimated costs?",
            "universe": ["SPY", "QQQ", "IWM", "DIA"],
            "required_outputs": [
                "event_count",
                "next_session_average_return",
                "win_rate",
                "expectancy",
                "volatility_compression_rule",
                "cost_assumption",
                "sample_size",
                "pass_fail_inconclusive",
            ],
            "required_inputs": [
                "daily_ohlc_history_for_SPY_QQQ_IWM_DIA",
                "volatility_compression_event_labels_or_rule",
                "next_session_return_engine",
                "cost_model",
            ],
            "metric_keys": [
                "event_count",
                "next_session_average_return",
                "win_rate",
                "expectancy",
                "cost_adjusted_expectancy",
            ],
            "success_criteria": [
                "At least 20 confirmed compression events.",
                "Next-session cost-adjusted expectancy is positive.",
                "Win rate supports the positive-expectancy claim after costs.",
            ],
            "failure_criteria": [
                "Fewer than 20 confirmed compression events.",
                "Next-session cost-adjusted expectancy is non-positive.",
                "Compression rule cannot be reproduced from source data.",
            ],
            "cost_assumption": "Explicit ETF round-trip cost model required before pass/fail classification.",
        },
    }
    if hypothesis_id in specs:
        return specs[hypothesis_id]
    title = str(item.get("title") or hypothesis_id)
    summary = str(item.get("short_summary") or item.get("hypothesis_summary") or "")
    text = f"{hypothesis_id} {title} {summary}".lower()
    if hypothesis_id == ETF_DROP_MEAN_REVERSION_HYPOTHESIS_ID or (
        ("mean-reversion" in text or "mean reversion" in text)
        and ("etf" in text or "spy" in text or "qqq" in text or "iwm" in text)
        and ("sharp" in text or "large down" in text or "down day" in text or "1-day drop" in text)
    ):
        return {
            "test_type": "EVENT_STUDY / REGIME_ANALYSIS",
            "test_question": "After unusually large 1-day drops in broad-market ETFs, do next-day or 3-day forward returns show positive mean reversion when VIX is below extreme-stress levels?",
            "universe": ["SPY", "QQQ", "IWM", "VIX"],
            "required_outputs": [
                "event_count",
                "average_next_day_forward_return",
                "average_3d_forward_return",
                "win_rate_next_day",
                "win_rate_3d",
                "expectancy",
                "vix_filter_used",
                "sample_size",
                "cost_assumption",
                "pass_fail_inconclusive",
            ],
            "required_inputs": [
                "daily_ohlc_history_for_SPY_QQQ_IWM",
                "daily_vix_close_history",
                "large_1d_down_day_event_rule",
                "vix_extreme_stress_filter_rule",
                "next_day_and_3d_forward_return_engine",
                "cost_model",
            ],
            "metric_keys": [
                "event_count",
                "forward_return_1d",
                "forward_return_3d",
                "win_rate_1d",
                "win_rate_3d",
                "expectancy",
                "cost_adjusted_expectancy",
            ],
            "success_criteria": [
                "At least 20 confirmed broad-market ETF down-day events after applying the VIX filter.",
                "Cost-adjusted expectancy is positive for next-day or 3-day forward windows.",
                "Results are not dominated by one ETF, one regime, or one isolated date cluster.",
            ],
            "failure_criteria": [
                "Fewer than 20 confirmed events after the VIX filter.",
                "Cost-adjusted expectancy is non-positive across both forward windows.",
                "The VIX filter or large-down-day rule cannot be reproduced from source data.",
            ],
            "cost_assumption": "Explicit ETF round-trip cost model required before pass/fail classification.",
        }
    return {
        "test_type": "RESEARCH_TEST_SPEC_NEEDED",
        "test_question": f"What deterministic evidence would validate or reject {title}?",
        "universe": [],
        "required_outputs": ["test_specification", "required_dataset", "sample_size", "pass_fail_inconclusive"],
        "required_inputs": ["operator_test_definition", "dataset_binding", "metric_formula"],
        "metric_keys": ["sample_size"],
        "success_criteria": ["Operator-defined test plan exists.", "Required data exists.", "No unsupported result is claimed."],
        "failure_criteria": ["Hypothesis is not testable.", "Required data is unavailable.", "Metric formula is not defined."],
        "cost_assumption": "Not defined until the test plan is specified.",
    }


def _event_study_data_status(root: Path, spec: dict[str, Any] | None = None, *, day_utc: str = "", hypothesis_id: str = "", write_missing: bool = False) -> dict[str, Any]:
    candidate_paths: list[str] = []
    for relative in [
        Path("reports") / "aegis_market_data_v1" / day_utc / "market_data.v1.json" if day_utc else Path("reports") / "aegis_market_data_v1",
        Path("reports") / "market_data_intraday_operational_v1" / day_utc / "market_data_intraday_operational.v1.json" if day_utc else Path("reports") / "market_data_intraday_operational_v1",
        Path("market_data_snapshot_v1") / "snapshots",
        Path("reports") / "decision_price_snapshot_v1",
        Path("reports") / "event_market_snapshot_v1",
    ]:
        base = root / relative
        if base.is_file():
            candidate_paths.append(str(base))
            continue
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.json"))[:12]:
            candidate_paths.append(str(path))
    required_inputs = list((spec or {}).get("required_inputs") or []) or [
        "daily_ohlc_history_for_SPY_QQQ_IWM_DIA",
        "volatility_overshoot_event_labels_or_rule",
        "volatility_compression_event_labels_or_rule",
        "forward_return_event_study_runner",
        "cost_model",
    ]
    universe = [str(item).upper() for item in ((spec or {}).get("universe") or []) if str(item).strip()]
    market_symbols = universe or market_symbols_from_required_inputs_v1(required_inputs)
    final_eod_required = any("final_eod" in str(item).lower() or "certified" in str(item).lower() for item in required_inputs)
    availability = governed_market_data_availability_v1(
        truth_root=root,
        day_utc=day_utc or datetime.now(UTC).strftime("%Y-%m-%d"),
        required_symbols=market_symbols,
        final_eod_required=final_eod_required,
    ) if market_symbols else {"ready": False, "required_symbols": [], "missing_symbols": [], "stale_symbols": [], "operator_status": "No market symbols were resolved for this research spec."}
    event_data = {}
    needs_earnings_path = hypothesis_id == NVIDIA_HYPOTHESIS_ID or any("earnings" in str(item).lower() for item in required_inputs)
    if needs_earnings_path:
        event_data = event_earnings_data_status_v1(
            truth_root=root,
            day_utc=day_utc or datetime.now(UTC).strftime("%Y-%m-%d"),
            hypothesis_id=hypothesis_id or NVIDIA_HYPOTHESIS_ID,
            write_missing=write_missing,
        )
        for key in ["calendar_artifact_path", "event_window_artifact_path", "forward_return_artifact_path"]:
            value = str(event_data.get(key) or "")
            if value:
                candidate_paths.append(value)
        if event_data.get("status") != "AVAILABLE":
            return {
                "status": "DATA_NEEDED",
                "blocker": event_data.get("blocker") or "EARNINGS_EVENT_CALENDAR_REQUIRED",
                "summary": event_data.get("summary") or "External earnings calendar required.",
                "market_data_found": bool(candidate_paths),
                "market_data_paths": candidate_paths[:12],
                "sample_size": int(event_data.get("sample_size") or 0),
                "event_count": int(event_data.get("event_count") or 0),
                "required_inputs": required_inputs,
                "required_symbols": market_symbols,
                "market_data_availability": availability,
                "event_data_status": event_data.get("event_data_status", ""),
                "event_data_availability": event_data,
                "accepted_upload_format": event_data.get("accepted_upload_format") or event_upload_format_v1(),
            }
    if availability.get("ready") is True:
        summary = availability.get("operator_status") or "Intraday market data available."
        if event_data:
            summary = f"{summary} {event_data.get('operator_status') or 'Governed event data available'}."
        return {
            "status": "AVAILABLE",
            "blocker": "",
            "summary": f"{summary} Event-study execution is not fabricated; a runner must generate evidence.",
            "market_data_found": True,
            "market_data_paths": candidate_paths[:12] or [availability.get("market_data_report_path", "")],
            "sample_size": int(event_data.get("sample_size") or 0),
            "event_count": int(event_data.get("event_count") or 0),
            "required_inputs": required_inputs,
            "required_symbols": market_symbols,
            "market_data_availability": availability,
            "event_data_status": event_data.get("event_data_status", ""),
            "event_data_availability": event_data,
            "accepted_upload_format": event_data.get("accepted_upload_format") if event_data else {},
        }
    blocker = "FINAL_EOD_CERTIFICATION_PENDING" if final_eod_required else "CURRENT_INTRADAY_MARKET_DATA_NEEDED"
    if availability.get("stale_symbols"):
        blocker = "STALE_INTRADAY_MARKET_DATA"
    summary = availability.get("operator_status") or "Waiting for current intraday market data."
    return {
        "status": "DATA_NEEDED",
        "blocker": blocker,
        "summary": summary,
        "market_data_found": bool(candidate_paths),
        "market_data_paths": candidate_paths[:12],
        "sample_size": int(event_data.get("sample_size") or 0),
        "event_count": int(event_data.get("event_count") or 0),
        "required_inputs": required_inputs,
        "required_symbols": market_symbols,
        "market_data_availability": availability,
        "event_data_status": event_data.get("event_data_status", ""),
        "event_data_availability": event_data,
        "accepted_upload_format": event_data.get("accepted_upload_format") if event_data else {},
    }

def _expected_output(hypothesis_id: str) -> list[str]:
    return _research_spec(hypothesis_id, {"title": hypothesis_id})["required_outputs"]


def _source_type(hypothesis: dict[str, Any]) -> str:
    if hypothesis.get("legacy_source"):
        return "LEGACY"
    source = str(hypothesis.get("source") or "").upper()
    if source in {"MANUAL", "OPERATOR", "USER", "HUMAN", "HUMAN_OPERATOR"}:
        return "OPERATOR"
    if "SLEEVE" in source:
        return "SLEEVE_CHALLENGER"
    if "REGIME" in source:
        return "REGIME_FAILURE"
    if "CANDIDATE" in source:
        return "CANDIDATE_OUTCOME"
    if "AI" in source or "GPT" in source:
        return "AI"
    return "SYSTEM"


def _evidence_required_for_gate(gate: str) -> list[str]:
    return {
        "INBOX": ["triage decision"],
        "TRIAGE": ["clarification or triage decision"],
        "TEST_PLAN": ["research_plan.v1.json"],
        "TESTING": ["research_plan.v1.json", "research_test_result.v1.json"],
        "RESULT_REVIEW": ["human review decision"],
        "PAPER_TRIAL": ["paper/advisory trial evidence"],
        "SLEEVE_REVIEW": ["sleeve review decision"],
        "ACTIVE_OR_ADOPTED": ["human-approved operational adoption"],
        "REJECTED_ARCHIVED": [],
    }.get(gate, [])


def _missing_evidence_for_gate(gate: str, status: str) -> list[str]:
    if status == "BLOCKED":
        return _evidence_required_for_gate(gate)
    return []


def _triage_command(hypothesis_id: str, decision: str) -> str:
    return f'npm run aegis:triage-hypothesis -- --hypothesis-id {hypothesis_id} --decision {decision} --reason "..." --operator David'


def _read_jsonl_family(root: Path, family: str, filename: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    base = root / "reports" / family
    if not base.exists():
        return rows
    for path in sorted(base.glob(f"*/{filename}")):
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                row = json.loads(line)
                if isinstance(row, dict):
                    row["path"] = str(path)
                    rows.append(row)
        except (OSError, json.JSONDecodeError):
            continue
    return rows


def _latest_events_by_hypothesis(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        hypothesis_id = str(row.get("hypothesis_id") or "")
        if not hypothesis_id:
            continue
        current = out.get(hypothesis_id)
        if not current or str(row.get("timestamp_utc") or "") >= str(current.get("timestamp_utc") or ""):
            out[hypothesis_id] = row
    return out


def _latest_payload_by_hypothesis(root: Path, family: str, filename: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    base = root / "reports" / family
    if not base.exists():
        return out
    for path in sorted(base.rglob(filename)):
        payload = read_json_v1(path)
        hypothesis_id = str(payload.get("hypothesis_id") or path.parent.name)
        if not hypothesis_id:
            continue
        payload["path"] = str(path)
        payload["source_hash"] = _file_hash(path)
        current = out.get(hypothesis_id)
        if not current or str(payload.get("generated_at_utc") or "") >= str(current.get("generated_at_utc") or ""):
            out[hypothesis_id] = payload
    return out


def _find_item(pipeline: dict[str, Any], hypothesis_id: str) -> dict[str, Any]:
    for item in pipeline.get("items") or []:
        if item.get("hypothesis_id") == hypothesis_id:
            return item
    raise ValueError(f"hypothesis_id not found: {hypothesis_id}")


def _event(event_type: str, hypothesis_id: str, operator: str, reason: str) -> dict[str, Any]:
    timestamp = _now()
    digest = hashlib.sha256(f"{event_type}|{hypothesis_id}|{operator}|{reason}|{timestamp}".encode("utf-8")).hexdigest()[:24]
    return {
        "event_type": event_type,
        "event_id": digest,
        "timestamp_utc": timestamp,
        "hypothesis_id": hypothesis_id,
        "operator": operator,
        "reason": reason,
    }


def _append_event(truth_root: Path, family: str, day_utc: str, filename: str, event: dict[str, Any]) -> Path:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / family / day_utc
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    return path


def _require_fields(*, hypothesis_id: str, reason: str, operator: str) -> None:
    if not hypothesis_id or not reason or not operator:
        raise ValueError("hypothesis_id, reason, and operator are required")


def _file_hash(path: Path | None) -> str:
    if not path:
        return ""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
