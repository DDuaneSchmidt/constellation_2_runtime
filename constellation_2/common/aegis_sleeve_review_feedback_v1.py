from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from constellation_2.common.aegis_research_lab_v1 import build_research_task_queue_v1, build_research_task_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
EOD_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/eod_sleeve_review.v1.schema.json"
EOW_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/eow_sleeve_review.v1.schema.json"
AI_MODEL_SOURCE = "DETERMINISTIC_PLACEHOLDER_NO_LLM_RUNTIME"
ALLOWED_FEEDBACK_TASK_TYPES = {
    "sleeve_failure_review",
    "sleeve_success_review",
    "regime_dependency_review",
    "event_false_positive_review",
    "slippage_review",
    "stop_behavior_review",
    "edge_overlap_review",
    "hypothesis_refinement_review",
    "promotion_candidate_review",
    "demotion_candidate_review",
}


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def validate_eod_sleeve_review_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, EOD_SCHEMA_RELPATH)


def validate_eow_sleeve_review_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, EOW_SCHEMA_RELPATH)


def eod_sleeve_review_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "eod_sleeve_review_v1" / day_utc / "eod_sleeve_review.v1.json"


def eow_sleeve_review_path_v1(*, truth_root: Path, week_ending: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "eow_sleeve_review_v1" / week_ending / "eow_sleeve_review.v1.json"


def write_eod_sleeve_review_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_eod_sleeve_review_v1(payload)
    path = eod_sleeve_review_path_v1(truth_root=truth_root, day_utc=str(payload["period_end"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def write_eow_sleeve_review_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_eow_sleeve_review_v1(payload)
    path = eow_sleeve_review_path_v1(truth_root=truth_root, week_ending=str(payload["period_end"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_eod_sleeve_review_v1(
    *,
    day_utc: str,
    generated_at_utc: str,
    sleeve_performance_report: dict[str, Any] | None,
    event_awareness_ledgers: list[dict[str, Any]] | None = None,
    event_rules_registries: list[dict[str, Any]] | None = None,
    input_artifact_refs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    report = sleeve_performance_report or {}
    rows = _objects(report.get("trade_lifecycle_rows"))
    portfolio = report.get("portfolio_summary") if isinstance(report.get("portfolio_summary"), dict) else {}
    execution = report.get("execution_quality") if isinstance(report.get("execution_quality"), dict) else {}
    sleeves = _objects(report.get("sleeve_summary"))
    ledgers = _objects(event_awareness_ledgers)
    rules = _objects(event_rules_registries)

    missing_receipts = [row for row in rows if row.get("lifecycle_status") == "MISSING_RECEIPT"]
    missing_outcomes = [row for row in rows if row.get("lifecycle_status") == "MISSING_OUTCOME"]
    trades_closed = [row for row in rows if row.get("lifecycle_status") == "EXECUTED_CLOSED"]
    trades_opened = [row for row in rows if row.get("lifecycle_status") in {"EXECUTED_OPEN", "MISSING_OUTCOME"}]
    slippage_issues = _slippage_issues(rows)
    stop_issues = _stop_behavior_issues(rows)
    event_ids = sorted({str(row.get("event_id") or "") for row in rows if row.get("event_id")} | _event_ids(ledgers))
    recommendations = _recommendations_from_rows(rows, scope="EOD")
    tasks = _research_tasks_from_recommendations(recommendations, generated_at_utc=generated_at_utc, period_ref=f"eod_sleeve_review.v1:{day_utc}")
    reason_codes = _reason_codes(
        report=report,
        rows=rows,
        missing_receipts=missing_receipts,
        missing_outcomes=missing_outcomes,
        slippage_issues=slippage_issues,
        stop_issues=stop_issues,
        event_ledgers=ledgers,
        event_rules=rules,
    )
    payload = {
        "schema_id": "eod_sleeve_review",
        "schema_version": "v1",
        "artifact_id": "eod_sleeve_review_v1",
        "review_id": f"eod_sleeve_review:{day_utc}",
        "review_type": "EOD",
        "period_start": day_utc,
        "period_end": day_utc,
        "generated_at_utc": generated_at_utc,
        "input_artifact_refs": input_artifact_refs or [],
        "sleeve_ids_reviewed": sorted({str(row.get("sleeve_id") or "") for row in sleeves + rows if row.get("sleeve_id")}),
        "event_ids_reviewed": event_ids,
        "outcome_rows_reviewed": len([row for row in rows if row.get("outcome_status")]),
        "ai_used": False,
        "ai_model_source": AI_MODEL_SOURCE,
        "deterministic_reason_codes": reason_codes,
        "review_summary": {
            "recommended_trades": int(portfolio.get("total_recommended_trades") or len(rows)),
            "executed_trades": int(portfolio.get("total_executed_trades") or 0),
            "closed_trades": len(trades_closed),
            "open_or_pending_outcome_trades": len(trades_opened),
            "missing_receipt_count": len(missing_receipts),
            "missing_outcome_count": len(missing_outcomes),
            "realized_return": str(portfolio.get("realized_return") or ""),
            "average_slippage": str(execution.get("average_slippage") or ""),
            "summary_text": _eod_summary_text(portfolio=portfolio, rows=rows, recommendations=recommendations),
        },
        "trades_opened": [_trade_digest(row) for row in trades_opened],
        "trades_closed": [_trade_digest(row) for row in trades_closed],
        "missing_receipts": [_trade_digest(row) for row in missing_receipts],
        "missing_outcomes": [_trade_digest(row) for row in missing_outcomes],
        "slippage_issues": slippage_issues,
        "stop_behavior_issues": stop_issues,
        "event_regime_attribution": _event_regime_attribution(rows=rows, ledgers=ledgers),
        "sleeve_warnings": _sleeve_warnings(sleeves=sleeves, rows=rows),
        "recommendations": recommendations,
        "research_tasks_created_or_recommended": tasks,
        "safety_boundaries_confirmed": _safety_boundaries(),
        "manual_execution_only": True,
        "broker_submit_required": False,
        "runtime_mutation_allowed": False,
        "trade_creation_allowed": False,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_eow_sleeve_review_v1(
    *,
    week_ending: str,
    generated_at_utc: str,
    daily_sleeve_performance_reports: list[dict[str, Any]],
    daily_eod_reviews: list[dict[str, Any]] | None = None,
    event_awareness_ledgers: list[dict[str, Any]] | None = None,
    research_task_queues: list[dict[str, Any]] | None = None,
    input_artifact_refs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    week_start = (date.fromisoformat(week_ending) - timedelta(days=4)).isoformat()
    reports = _objects(daily_sleeve_performance_reports)
    reviews = _objects(daily_eod_reviews)
    rows = [row for report in reports for row in _objects(report.get("trade_lifecycle_rows"))]
    event_ledgers = _objects(event_awareness_ledgers)
    queue_tasks = [task for queue in _objects(research_task_queues) for task in _objects(queue.get("tasks"))]
    scorecard = _weekly_scorecard(rows)
    strongest = sorted(scorecard, key=lambda row: (_number(row.get("total_return")) or 0), reverse=True)[:3]
    weakest = sorted(scorecard, key=lambda row: (_number(row.get("total_return")) or 0))[:3]
    repeated_failures = _repeated_failure_modes(rows)
    event_false_positives = _event_false_positives(rows)
    recommendations = _recommendations_from_rows(rows, scope="EOW")
    for failure in repeated_failures:
        recommendations.append(
            _recommendation(
                "hypothesis_refinement_review",
                failure.get("hypothesis_id") or "",
                failure.get("sleeve_id") or "",
                "Review repeated failure pattern before changing any Lite behavior.",
                ["REPEATED_FAILURE_MODE"],
            )
        )
    tasks = _research_tasks_from_recommendations(recommendations, generated_at_utc=generated_at_utc, period_ref=f"eow_sleeve_review.v1:{week_ending}")
    payload = {
        "schema_id": "eow_sleeve_review",
        "schema_version": "v1",
        "artifact_id": "eow_sleeve_review_v1",
        "review_id": f"eow_sleeve_review:{week_ending}",
        "review_type": "EOW",
        "period_start": week_start,
        "period_end": week_ending,
        "generated_at_utc": generated_at_utc,
        "input_artifact_refs": input_artifact_refs or [],
        "sleeve_ids_reviewed": sorted({str(row.get("sleeve_id") or "") for row in rows if row.get("sleeve_id")}),
        "event_ids_reviewed": sorted({str(row.get("event_id") or "") for row in rows if row.get("event_id")} | _event_ids(event_ledgers)),
        "outcome_rows_reviewed": len([row for row in rows if row.get("outcome_status")]),
        "ai_used": False,
        "ai_model_source": AI_MODEL_SOURCE,
        "deterministic_reason_codes": _eow_reason_codes(reports=reports, rows=rows, reviews=reviews, queues=queue_tasks),
        "weekly_scorecard": scorecard,
        "strongest_sleeves": strongest,
        "weakest_sleeves": weakest,
        "regime_dependent_sleeves": _regime_dependent_sleeves(rows),
        "repeated_failure_modes": repeated_failures,
        "event_false_positives": event_false_positives,
        "promotion_review_candidates": _promotion_candidates(scorecard),
        "demotion_review_candidates": _demotion_candidates(scorecard),
        "recommendations": recommendations,
        "research_tasks_created_or_recommended": tasks,
        "safety_boundaries_confirmed": _safety_boundaries(),
        "manual_execution_only": True,
        "broker_submit_required": False,
        "runtime_mutation_allowed": False,
        "trade_creation_allowed": False,
        "automatic_promotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def write_research_task_queue_from_review_v1(
    *,
    truth_root: Path,
    day_utc: str,
    generated_at_utc: str,
    tasks: list[dict[str, Any]],
) -> Path:
    root = Path(truth_root).resolve()
    existing_path = root / "research_lab" / "research_task_queue_v1" / day_utc / "index" / "research_task_queue.v1.json"
    existing = _read_json(existing_path)
    existing_tasks = _objects(existing.get("tasks"))
    merged: dict[str, dict[str, Any]] = {str(task.get("task_id")): task for task in existing_tasks if task.get("task_id")}
    for task in tasks:
        if task.get("task_id"):
            merged[str(task["task_id"])] = task
    queue = build_research_task_queue_v1(generated_at_utc=generated_at_utc, tasks=list(merged.values()))
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_bytes(canonical_json_bytes_v1(queue) + b"\n")
    return existing_path


def _research_tasks_from_recommendations(recommendations: list[dict[str, Any]], *, generated_at_utc: str, period_ref: str) -> list[dict[str, Any]]:
    tasks = []
    for idx, rec in enumerate(sorted(recommendations, key=lambda row: (str(row.get("hypothesis_id") or ""), str(row.get("task_type") or ""), str(row.get("sleeve_id") or ""), str(row.get("reason_codes") or ""))), start=1):
        hypothesis_id = str(rec.get("hypothesis_id") or "")
        if not hypothesis_id:
            continue
        task_type = str(rec.get("task_type") or "hypothesis_refinement_review")
        if task_type not in ALLOWED_FEEDBACK_TASK_TYPES:
            task_type = "hypothesis_refinement_review"
        task_id = f"task:{_safe(hypothesis_id)}:{_safe(task_type)}:{_safe(str(rec.get('sleeve_id') or 'review'))}:{idx:03d}"
        tasks.append(
            build_research_task_v1(
                task_id=task_id,
                task_type=task_type,
                hypothesis_id=hypothesis_id,
                source_trigger=period_ref,
                priority=str(rec.get("priority") or "normal").lower(),
                requested_action=str(rec.get("recommendation") or "Review sleeve performance feedback."),
                created_at=generated_at_utc,
                output_expected="research_result_ledger.v1",
            )
        )
    return tasks


def _recommendations_from_rows(rows: list[dict[str, Any]], *, scope: str) -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []
    for row in rows:
        status = str(row.get("lifecycle_status") or "")
        outcome = str(row.get("outcome_status") or "").lower()
        failure = str(row.get("failure_reason") or "").lower()
        hypothesis_id = str(row.get("source_hypothesis_id") or "")
        sleeve_id = str(row.get("sleeve_id") or "")
        if not hypothesis_id:
            continue
        if status == "MISSING_RECEIPT":
            recommendations.append(_recommendation("slippage_review", hypothesis_id, sleeve_id, f"{scope}: resolve missing receipt for {row.get('trade_id')}.", ["MISSING_RECEIPT"], "high"))
        if status == "MISSING_OUTCOME":
            recommendations.append(_recommendation("sleeve_failure_review", hypothesis_id, sleeve_id, f"{scope}: resolve missing outcome for {row.get('trade_id')}.", ["MISSING_OUTCOME"], "high"))
        if not row.get("entry_slippage_respected") or str(row.get("operator_deviation") or ""):
            recommendations.append(_recommendation("slippage_review", hypothesis_id, sleeve_id, f"{scope}: review operator slippage/deviation for {row.get('trade_id')}.", ["SLIPPAGE_OR_OPERATOR_DEVIATION"]))
        if not row.get("stop_entered") or not row.get("stop_matched_recommendation"):
            recommendations.append(_recommendation("stop_behavior_review", hypothesis_id, sleeve_id, f"{scope}: review stop behavior for {row.get('trade_id')}.", ["STOP_BEHAVIOR_ISSUE"], "high"))
        if outcome in {"loss", "stopped_out", "underperformed"} or "stop" in failure:
            recommendations.append(_recommendation("sleeve_failure_review", hypothesis_id, sleeve_id, f"{scope}: review failed sleeve outcome for {row.get('trade_id')}.", ["SLEEVE_FAILURE"], "high"))
        if outcome in {"win", "profitable", "outperformed"}:
            recommendations.append(_recommendation("sleeve_success_review", hypothesis_id, sleeve_id, f"{scope}: preserve successful sleeve outcome evidence for {row.get('trade_id')}.", ["SLEEVE_SUCCESS"]))
        if "regime" in failure:
            recommendations.append(_recommendation("regime_dependency_review", hypothesis_id, sleeve_id, f"{scope}: review regime dependency for {row.get('trade_id')}.", ["REGIME_DEPENDENCY"]))
        if "overlap" in failure or "overlap" in str(row.get("edge_overlap_attribution") or "").lower() or "correlation" in failure:
            recommendations.append(_recommendation("edge_overlap_review", hypothesis_id, sleeve_id, f"{scope}: review edge overlap for {row.get('trade_id')}.", ["EDGE_OVERLAP"]))
        if row.get("event_id") and (outcome in {"loss", "stopped_out"} or not row.get("valid_until_respected")):
            recommendations.append(_recommendation("event_false_positive_review", hypothesis_id, sleeve_id, f"{scope}: review event false-positive/stale-entry risk for {row.get('trade_id')}.", ["EVENT_FALSE_POSITIVE_OR_STALE"]))
    return _dedupe_recommendations(recommendations)


def _recommendation(task_type: str, hypothesis_id: str, sleeve_id: str, text: str, reason_codes: list[str], priority: str = "normal") -> dict[str, Any]:
    return {
        "task_type": task_type,
        "hypothesis_id": hypothesis_id,
        "sleeve_id": sleeve_id,
        "recommendation": text,
        "reason_codes": sorted(set(reason_codes)),
        "priority": priority,
        "offline_research_only": True,
        "production_action_allowed": False,
    }


def _dedupe_recommendations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("task_type") or ""),
            str(row.get("hypothesis_id") or ""),
            str(row.get("sleeve_id") or ""),
            ",".join(_strings(row.get("reason_codes"))),
        )
        out[key] = row
    return [out[key] for key in sorted(out)]


def _reason_codes(**kwargs: Any) -> list[str]:
    codes = ["AI_ASSISTED_INTERPRETATION_DESIGNED_NOT_IMPLEMENTED", "DETERMINISTIC_REVIEW_USED"]
    if not kwargs.get("report"):
        codes.append("SLEEVE_PERFORMANCE_REPORT_MISSING")
    if not kwargs.get("rows"):
        codes.append("NO_TRADE_ROWS_REVIEWED")
    if kwargs.get("missing_receipts"):
        codes.append("MISSING_RECEIPTS_PRESENT")
    if kwargs.get("missing_outcomes"):
        codes.append("MISSING_OUTCOMES_PRESENT")
    if kwargs.get("slippage_issues"):
        codes.append("SLIPPAGE_ISSUES_PRESENT")
    if kwargs.get("stop_issues"):
        codes.append("STOP_BEHAVIOR_ISSUES_PRESENT")
    if not kwargs.get("event_ledgers"):
        codes.append("EVENT_LEDGER_NOT_AVAILABLE")
    if not kwargs.get("event_rules"):
        codes.append("EVENT_RULES_REGISTRY_NOT_REFERENCED")
    return sorted(set(codes))


def _eow_reason_codes(*, reports: list[dict[str, Any]], rows: list[dict[str, Any]], reviews: list[dict[str, Any]], queues: list[dict[str, Any]]) -> list[str]:
    codes = ["AI_ASSISTED_INTERPRETATION_DESIGNED_NOT_IMPLEMENTED", "DETERMINISTIC_WEEKLY_REVIEW_USED"]
    if not reports:
        codes.append("NO_DAILY_SLEEVE_PERFORMANCE_REPORTS")
    if not rows:
        codes.append("NO_WEEKLY_TRADE_ROWS_REVIEWED")
    if not reviews:
        codes.append("NO_EOD_REVIEWS_REFERENCED")
    if not queues:
        codes.append("NO_RESEARCH_TASK_QUEUE_REFERENCED")
    return sorted(set(codes))


def _slippage_issues(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        _trade_digest(row, extra={"reason_code": "ENTRY_SLIPPAGE_NOT_RESPECTED"})
        for row in rows
        if row.get("entry_slippage_respected") is False or row.get("max_entry_slippage_respected") is False or str(row.get("operator_deviation") or "")
    ]


def _stop_behavior_issues(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        _trade_digest(row, extra={"reason_code": "STOP_NOT_ENTERED_OR_MISMATCHED"})
        for row in rows
        if not row.get("stop_entered") or row.get("stop_matched_recommendation") is False
    ]


def _sleeve_warnings(*, sleeves: list[dict[str, Any]], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    warnings = []
    for sleeve in sleeves:
        sleeve_id = str(sleeve.get("sleeve_id") or "")
        total_return = _number(sleeve.get("total_return"))
        if total_return is not None and total_return < 0:
            warnings.append({"sleeve_id": sleeve_id, "reason_code": "NEGATIVE_SLEEVE_RETURN", "total_return": str(sleeve.get("total_return") or "")})
    for sleeve_id in sorted({str(row.get("sleeve_id") or "") for row in rows if row.get("sleeve_id")}):
        failures = [row for row in rows if row.get("sleeve_id") == sleeve_id and str(row.get("outcome_status") or "").lower() in {"loss", "stopped_out", "underperformed"}]
        if len(failures) >= 2:
            warnings.append({"sleeve_id": sleeve_id, "reason_code": "REPEATED_SLEEVE_FAILURES", "failure_count": len(failures)})
    return warnings


def _event_regime_attribution(*, rows: list[dict[str, Any]], ledgers: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "event_trade_count": sum(1 for row in rows if row.get("event_id")),
        "event_types": sorted({str(row.get("event_type") or "") for row in rows if row.get("event_type")}),
        "regimes": sorted({str(row.get("regime_state") or "") for row in rows if row.get("regime_state")}),
        "event_ledger_entries": sum(len(_objects(ledger.get("events"))) for ledger in ledgers),
    }


def _weekly_scorecard(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("sleeve_id") or "UNKNOWN"), []).append(row)
    scorecard = []
    for sleeve_id in sorted(grouped):
        sleeve_rows = grouped[sleeve_id]
        returns = [_number(row.get("return_pct")) for row in sleeve_rows if _number(row.get("return_pct")) is not None and row.get("lifecycle_status") == "EXECUTED_CLOSED"]
        scorecard.append(
            {
                "sleeve_id": sleeve_id,
                "source_hypothesis_id": str(sleeve_rows[0].get("source_hypothesis_id") or ""),
                "trade_count": len(sleeve_rows),
                "closed_trade_count": len(returns),
                "total_return": _fmt(sum(returns)) if returns else "",
                "win_count": sum(1 for value in returns if value > 0),
                "loss_count": sum(1 for value in returns if value < 0),
                "missing_receipts": sum(1 for row in sleeve_rows if row.get("lifecycle_status") == "MISSING_RECEIPT"),
                "missing_outcomes": sum(1 for row in sleeve_rows if row.get("lifecycle_status") == "MISSING_OUTCOME"),
            }
        )
    return scorecard


def _repeated_failure_modes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        failure = str(row.get("failure_reason") or "").strip().lower()
        if failure:
            grouped.setdefault((str(row.get("sleeve_id") or ""), failure), []).append(row)
    return [
        {"sleeve_id": sleeve_id, "failure_reason": failure, "count": len(items), "hypothesis_id": str(items[0].get("source_hypothesis_id") or "")}
        for (sleeve_id, failure), items in sorted(grouped.items())
        if len(items) >= 2 or any(str(row.get("outcome_status") or "").lower() in {"loss", "stopped_out"} for row in items)
    ]


def _event_false_positives(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        _trade_digest(row, extra={"reason_code": "EVENT_FALSE_POSITIVE_OR_STALE_ENTRY"})
        for row in rows
        if row.get("event_id") and (str(row.get("outcome_status") or "").lower() in {"loss", "stopped_out"} or row.get("valid_until_respected") is False)
    ]


def _regime_dependent_sleeves(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for sleeve_id in sorted({str(row.get("sleeve_id") or "") for row in rows if row.get("sleeve_id")}):
        regimes = sorted({str(row.get("regime_state") or "") for row in rows if row.get("sleeve_id") == sleeve_id and row.get("regime_state")})
        failures = [row for row in rows if row.get("sleeve_id") == sleeve_id and "regime" in str(row.get("failure_reason") or "").lower()]
        if len(regimes) > 1 or failures:
            out.append({"sleeve_id": sleeve_id, "regimes": regimes, "regime_failure_count": len(failures)})
    return out


def _promotion_candidates(scorecard: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"sleeve_id": row["sleeve_id"], "reason_code": "POSITIVE_WEEKLY_RETURN_REVIEW_ONLY", "total_return": row.get("total_return", "")}
        for row in scorecard
        if (_number(row.get("total_return")) or 0) > 0 and int(row.get("closed_trade_count") or 0) >= 2
    ]


def _demotion_candidates(scorecard: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"sleeve_id": row["sleeve_id"], "reason_code": "NEGATIVE_WEEKLY_RETURN_REVIEW_ONLY", "total_return": row.get("total_return", "")}
        for row in scorecard
        if (_number(row.get("total_return")) or 0) < 0
    ]


def _eod_summary_text(*, portfolio: dict[str, Any], rows: list[dict[str, Any]], recommendations: list[dict[str, Any]]) -> str:
    return (
        f"Reviewed {len(rows)} trade lifecycle rows; "
        f"executed={portfolio.get('total_executed_trades', 0)}; "
        f"realized_return={portfolio.get('realized_return') or 'n/a'}; "
        f"research_recommendations={len(recommendations)}."
    )


def _trade_digest(row: dict[str, Any], extra: dict[str, Any] | None = None) -> dict[str, Any]:
    digest = {
        "trade_id": str(row.get("trade_id") or ""),
        "sleeve_id": str(row.get("sleeve_id") or ""),
        "hypothesis_id": str(row.get("source_hypothesis_id") or ""),
        "symbol": str(row.get("symbol") or ""),
        "lifecycle_status": str(row.get("lifecycle_status") or ""),
        "outcome_status": str(row.get("outcome_status") or ""),
        "return_pct": str(row.get("return_pct") or ""),
        "failure_reason": str(row.get("failure_reason") or ""),
    }
    if extra:
        digest.update(extra)
    return digest


def _event_ids(ledgers: list[dict[str, Any]]) -> set[str]:
    return {str(event.get("event_id") or "") for ledger in ledgers for event in _objects(ledger.get("events")) if event.get("event_id")}


def _safety_boundaries() -> dict[str, bool]:
    return {
        "manual_execution_only": True,
        "broker_submit_required": False,
        "runtime_mutation_allowed": False,
        "trade_creation_allowed": False,
        "automatic_promotion_allowed": False,
        "automatic_demotion_allowed": False,
        "strategy_logic_mutation_allowed": False,
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _objects(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _strings(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if value is None:
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _number(value: Any) -> float | None:
    try:
        return float(str(value).strip().replace("%", ""))
    except (TypeError, ValueError):
        return None


def _fmt(value: float) -> str:
    text = f"{value:.4f}".rstrip("0").rstrip(".")
    return text or "0"


def _safe(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip()) or "unknown"
