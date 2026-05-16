from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from constellation_2.common.aegis_market_context_v1 import market_context_summary_v1
from constellation_2.common.aegis_research_lab_v1 import build_research_task_queue_v1, build_research_task_v1
from constellation_2.phaseD.lib.canon_json_v1 import canonical_hash_for_c2_artifact_v1, canonical_json_bytes_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
EVIDENCE_GATE_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/evidence_gate.v1.schema.json"
AI_FEEDBACK_SCHEMA_RELPATH = "governance/04_DATA/SCHEMAS/C2/REPORTS/ai_feedback_review.v1.schema.json"
MODEL_USED = "DETERMINISTIC_FALLBACK_NO_LLM_RUNTIME"
FINDING_LIMITS = {"EOD": 3, "EOW": 5}
ALLOWED_TASK_TYPES = {
    "sleeve_failure_review",
    "sleeve_success_review",
    "regime_dependency_review",
    "event_false_positive_review",
    "event_success_review",
    "slippage_review",
    "stop_behavior_review",
    "edge_overlap_review",
    "hypothesis_refinement_review",
    "promotion_candidate_review",
    "demotion_candidate_review",
}
FINDING_TASK_MAP = {
    "SLEEVE_DEGRADATION": "sleeve_failure_review",
    "SLEEVE_IMPROVEMENT": "sleeve_success_review",
    "REGIME_DEPENDENCY": "regime_dependency_review",
    "EVENT_FALSE_POSITIVE": "event_false_positive_review",
    "EVENT_SUCCESS_PATTERN": "event_success_review",
    "STOP_BEHAVIOR_ISSUE": "stop_behavior_review",
    "SLIPPAGE_ISSUE": "slippage_review",
    "EDGE_OVERLAP": "edge_overlap_review",
    "DATA_QUALITY_ISSUE": "",
    "MISSED_TRADE_PATTERN": "hypothesis_refinement_review",
    "HYPOTHESIS_REFINEMENT": "hypothesis_refinement_review",
    "PROMOTION_CANDIDATE": "promotion_candidate_review",
    "DEMOTION_CANDIDATE": "demotion_candidate_review",
}


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def validate_evidence_gate_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, EVIDENCE_GATE_SCHEMA_RELPATH)


def validate_ai_feedback_review_v1(payload: dict[str, Any]) -> None:
    validate_against_repo_schema_v1(payload, REPO_ROOT, AI_FEEDBACK_SCHEMA_RELPATH)


def evidence_gate_path_v1(*, truth_root: Path, review_type: str, period_end: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "evidence_gate_v1" / review_type.upper() / period_end / "evidence_gate.v1.json"


def ai_feedback_review_path_v1(*, truth_root: Path, review_type: str, period_end: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "ai_feedback_review_v1" / review_type.upper() / period_end / "ai_feedback_review.v1.json"


def write_evidence_gate_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_evidence_gate_v1(payload)
    path = evidence_gate_path_v1(truth_root=truth_root, review_type=str(payload["review_type"]), period_end=str(payload["period_end"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def write_ai_feedback_review_v1(*, truth_root: Path, payload: dict[str, Any]) -> Path:
    validate_ai_feedback_review_v1(payload)
    path = ai_feedback_review_path_v1(truth_root=truth_root, review_type=str(payload["review_type"]), period_end=str(payload["period_end"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(payload) + b"\n")
    return path


def build_evidence_gate_v1(
    *,
    review_type: str,
    period_start: str,
    period_end: str,
    generated_at_utc: str,
    sleeve_performance_reports: list[dict[str, Any]],
    event_awareness_ledgers: list[dict[str, Any]] | None = None,
    alert_ledgers: list[dict[str, Any]] | None = None,
    market_context_snapshots: list[dict[str, Any]] | None = None,
    input_artifact_refs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    kind = review_type.upper()
    reports = _objects(sleeve_performance_reports)
    rows = _trade_rows(reports)
    market_context = _market_context_rollup(market_context_snapshots)
    sample_count = len(rows)
    sample_band = _sample_band(sample_count)
    missing_receipts = sum(1 for row in rows if row.get("lifecycle_status") == "MISSING_RECEIPT")
    missing_outcomes = sum(1 for row in rows if row.get("lifecycle_status") == "MISSING_OUTCOME")
    stale_data = _has_stale_data(reports=reports, rows=rows, period_start=period_start, period_end=period_end)
    missing_regime_labels = sum(1 for row in rows if not str(row.get("regime_state") or ""))
    missing_event_labels = sum(1 for row in rows if row.get("event_id") and not row.get("event_type"))
    missing_price_outcome_data = sum(1 for row in rows if _missing_price_or_outcome(row))
    score = _data_quality_score(
        sample_count=sample_count,
        missing_receipts=missing_receipts,
        missing_outcomes=missing_outcomes,
        stale_data=stale_data,
        missing_regime_labels=missing_regime_labels,
        missing_event_labels=missing_event_labels,
        missing_price_outcome_data=missing_price_outcome_data,
    )
    quality_status = "PASS" if score >= 80 else "WARN" if score >= 50 else "BLOCK"
    passable_band = sample_band in {"WEAK_SIGNAL", "REVIEWABLE_PATTERN", "STRONGER_PATTERN"}
    evidence_gate_pass = passable_band and not stale_data and quality_status != "BLOCK"
    if not rows or stale_data or quality_status == "BLOCK":
        status = "BLOCKED"
    elif sample_band == "OBSERVATION_ONLY":
        status = "OBSERVATION_ONLY"
    else:
        status = "PASS"
    strong_allowed = sample_band in {"REVIEWABLE_PATTERN", "STRONGER_PATTERN"} and evidence_gate_pass and quality_status == "PASS"
    blocked = _blocked_reason_codes(
        sample_count=sample_count,
        sample_band=sample_band,
        missing_receipts=missing_receipts,
        missing_outcomes=missing_outcomes,
        stale_data=stale_data,
        missing_regime_labels=missing_regime_labels,
        missing_event_labels=missing_event_labels,
        missing_price_outcome_data=missing_price_outcome_data,
        data_quality_status=quality_status,
    )
    payload = {
        "schema_id": "evidence_gate",
        "schema_version": "v1",
        "artifact_id": "evidence_gate_v1",
        "evidence_gate_id": f"evidence_gate:{kind}:{period_start}:{period_end}",
        "review_type": kind,
        "period_start": period_start,
        "period_end": period_end,
        "generated_at_utc": generated_at_utc,
        "input_artifact_refs": input_artifact_refs or [],
        "sample_count": sample_count,
        "sample_band": sample_band,
        "evidence_gate_status": status,
        "evidence_gate_pass": evidence_gate_pass,
        "missing_receipts": missing_receipts,
        "missing_outcomes": missing_outcomes,
        "stale_data": stale_data,
        "missing_regime_labels": missing_regime_labels,
        "missing_event_labels": missing_event_labels,
        "missing_price_outcome_data": missing_price_outcome_data,
        "data_quality_score": score,
        "data_quality_status": quality_status,
        "strong_conclusions_allowed": strong_allowed,
        "automatic_research_task_creation_allowed": evidence_gate_pass,
        "blocked_conclusion_reason_codes": blocked,
        "market_context_summary": market_context,
        "manual_execution_only": True,
        "broker_action_allowed": False,
        "production_mutation_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def build_ai_feedback_review_v1(
    *,
    review_type: str,
    period_start: str,
    period_end: str,
    generated_at_utc: str,
    sleeve_performance_reports: list[dict[str, Any]],
    evidence_gate: dict[str, Any],
    event_awareness_ledgers: list[dict[str, Any]] | None = None,
    alert_ledgers: list[dict[str, Any]] | None = None,
    market_context_snapshots: list[dict[str, Any]] | None = None,
    input_artifact_refs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    kind = review_type.upper()
    reports = _objects(sleeve_performance_reports)
    rows = _trade_rows(reports)
    event_ledgers = _objects(event_awareness_ledgers)
    market_context = _market_context_rollup(market_context_snapshots)
    alerts = _alert_ids(_objects(alert_ledgers), rows)
    findings = _top_findings(
        rows=rows,
        evidence_gate=evidence_gate,
        input_artifact_refs=input_artifact_refs or [],
        limit=FINDING_LIMITS.get(kind, 3),
    )
    tasks = _research_tasks_from_findings(
        findings=findings,
        review_id=f"ai_feedback_review:{kind}:{period_start}:{period_end}",
        evidence_gate=evidence_gate,
        generated_at_utc=generated_at_utc,
        input_artifact_refs=input_artifact_refs or [],
    )
    payload = {
        "schema_id": "ai_feedback_review",
        "schema_version": "v1",
        "artifact_id": "ai_feedback_review_v1",
        "review_id": f"ai_feedback_review:{kind}:{period_start}:{period_end}",
        "review_type": kind,
        "period_start": period_start,
        "period_end": period_end,
        "generated_at_utc": generated_at_utc,
        "input_artifact_refs": input_artifact_refs or [],
        "evidence_gate_id": str(evidence_gate["evidence_gate_id"]),
        "evidence_gate_status": str(evidence_gate["evidence_gate_status"]),
        "data_quality_status": str(evidence_gate["data_quality_status"]),
        "sleeves_reviewed": sorted({str(row.get("sleeve_id") or "") for row in rows if row.get("sleeve_id")}),
        "trades_reviewed": sorted({str(row.get("trade_id") or "") for row in rows if row.get("trade_id")}),
        "events_reviewed": sorted({str(row.get("event_id") or "") for row in rows if row.get("event_id")} | _event_ids(event_ledgers)),
        "regimes_reviewed": sorted({str(row.get("regime_state") or "") for row in rows if row.get("regime_state")} | {row for row in [market_context.get("regime_label")] if row and row != "UNKNOWN"}),
        "alerts_reviewed": alerts,
        "market_context_summary": market_context,
        "findings": findings,
        "hypothesis_suggestions": _hypothesis_suggestions(findings),
        "research_tasks_created": tasks,
        "ai_used": False,
        "model_used": MODEL_USED,
        "deterministic_fallback_used": True,
        "human_review_required": True,
        "production_mutation": False,
        "broker_action_allowed": False,
        "auto_promotion_allowed": False,
        "auto_demotion_allowed": False,
        "canonical_json_hash": None,
    }
    payload["canonical_json_hash"] = canonical_hash_for_c2_artifact_v1(payload)
    return payload


def write_research_task_queue_from_ai_feedback_v1(
    *,
    truth_root: Path,
    period_end: str,
    generated_at_utc: str,
    tasks: list[dict[str, Any]],
) -> Path:
    root = Path(truth_root).resolve()
    path = root / "research_lab" / "research_task_queue_v1" / period_end / "index" / "research_task_queue.v1.json"
    existing = _read_json(path)
    merged: dict[str, dict[str, Any]] = {
        str(task.get("task_id")): task
        for task in _objects(existing.get("tasks"))
        if task.get("task_id")
    }
    for task in tasks:
        legacy = _legacy_task_from_ai_task(task, generated_at_utc=generated_at_utc)
        merged[str(legacy["task_id"])] = legacy
    queue = build_research_task_queue_v1(generated_at_utc=generated_at_utc, tasks=list(merged.values()))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes_v1(queue) + b"\n")
    return path


def _market_context_rollup(snapshots: list[dict[str, Any]] | None) -> dict[str, Any]:
    rows = [market_context_summary_v1(row) for row in _objects(snapshots)]
    if not rows:
        return market_context_summary_v1(None)
    latest = sorted(rows, key=lambda row: (str(row.get("day_utc") or ""), str(row.get("generated_at_utc") or "")))[-1]
    return {
        **latest,
        "snapshots_reviewed": len(rows),
        "regime_labels_reviewed": sorted({str(row.get("regime_label") or "UNKNOWN") for row in rows}),
        "volatility_labels_reviewed": sorted({str(row.get("volatility_classification") or "UNKNOWN") for row in rows}),
        "breadth_labels_reviewed": sorted({str(row.get("breadth_classification") or "UNKNOWN") for row in rows}),
        "macro_event_types_reviewed": sorted({str(row.get("macro_event_type") or "NONE") for row in rows}),
    }


def _top_findings(*, rows: list[dict[str, Any]], evidence_gate: dict[str, Any], input_artifact_refs: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    findings = _candidate_findings(rows=rows, evidence_gate=evidence_gate, input_artifact_refs=input_artifact_refs)
    deduped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for finding in findings:
        if not finding.get("evidence_refs"):
            continue
        key = (
            str(finding.get("finding_type") or ""),
            str(finding.get("sleeve_id") or ""),
            str(finding.get("hypothesis_id") or ""),
            ",".join(_strings(finding.get("trade_ids"))),
        )
        deduped[key] = finding
    ordered = sorted(
        deduped.values(),
        key=lambda item: (
            _severity_rank(str(item.get("severity") or "")),
            -int(item.get("sample_count") or 0),
            str(item.get("finding_type") or ""),
            str(item.get("sleeve_id") or ""),
            str(item.get("finding_id") or ""),
        ),
    )
    return ordered[:limit]


def _candidate_findings(*, rows: list[dict[str, Any]], evidence_gate: dict[str, Any], input_artifact_refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    if evidence_gate["data_quality_status"] != "PASS" or evidence_gate["blocked_conclusion_reason_codes"]:
        findings.append(
            _finding(
                finding_type="DATA_QUALITY_ISSUE",
                rows=rows,
                evidence_gate=evidence_gate,
                input_artifact_refs=input_artifact_refs,
                summary="Data quality limits AI-assisted sleeve conclusions.",
                severity="HIGH" if evidence_gate["data_quality_status"] == "BLOCK" else "MEDIUM",
                task_type="",
            )
        )
    findings.extend(_row_group_finding("SLIPPAGE_ISSUE", [row for row in rows if row.get("entry_slippage_respected") is False or row.get("max_entry_slippage_respected") is False or row.get("operator_deviation")], evidence_gate, input_artifact_refs, "Operator slippage/deviation requires review."))
    findings.extend(_row_group_finding("STOP_BEHAVIOR_ISSUE", [row for row in rows if not row.get("stop_entered") or row.get("stop_matched_recommendation") is False], evidence_gate, input_artifact_refs, "Stop behavior did not match the recommendation."))
    findings.extend(_row_group_finding("SLEEVE_DEGRADATION", [row for row in rows if str(row.get("outcome_status") or "").lower() in {"loss", "stopped_out", "underperformed"}], evidence_gate, input_artifact_refs, "Sleeve had adverse or stopped-out outcomes.", severity="HIGH"))
    findings.extend(_row_group_finding("SLEEVE_IMPROVEMENT", [row for row in rows if str(row.get("outcome_status") or "").lower() in {"win", "profitable", "outperformed"}], evidence_gate, input_artifact_refs, "Sleeve had positive outcomes worth preserving as evidence."))
    findings.extend(_row_group_finding("REGIME_DEPENDENCY", [row for row in rows if "regime" in str(row.get("failure_reason") or "").lower()], evidence_gate, input_artifact_refs, "Outcome appears regime-dependent."))
    findings.extend(_row_group_finding("EDGE_OVERLAP", [row for row in rows if "overlap" in str(row.get("failure_reason") or "").lower() or "overlap" in str(row.get("edge_overlap_attribution") or "").lower()], evidence_gate, input_artifact_refs, "Edge overlap may be affecting sleeve performance."))
    findings.extend(_row_group_finding("EVENT_FALSE_POSITIVE", [row for row in rows if row.get("event_id") and (str(row.get("outcome_status") or "").lower() in {"loss", "stopped_out"} or row.get("valid_until_respected") is False)], evidence_gate, input_artifact_refs, "Event packet may have been false-positive or stale."))
    findings.extend(_row_group_finding("EVENT_SUCCESS_PATTERN", [row for row in rows if row.get("event_id") and str(row.get("outcome_status") or "").lower() in {"win", "profitable", "outperformed"}], evidence_gate, input_artifact_refs, "Event packet had successful outcome evidence."))
    findings.extend(_row_group_finding("MISSED_TRADE_PATTERN", [row for row in rows if str(row.get("lifecycle_status") or "") in {"MISSING_RECEIPT", "RECOMMENDED_NOT_EXECUTED", "IGNORED", "MISSED_VALIDITY_WINDOW"}], evidence_gate, input_artifact_refs, "Recommended trades were missed, ignored, or lacked receipts."))
    findings.extend(_promotion_demote_findings(rows=rows, evidence_gate=evidence_gate, input_artifact_refs=input_artifact_refs))
    return findings


def _row_group_finding(
    finding_type: str,
    rows: list[dict[str, Any]],
    evidence_gate: dict[str, Any],
    input_artifact_refs: list[dict[str, Any]],
    summary: str,
    severity: str = "MEDIUM",
) -> list[dict[str, Any]]:
    if not rows:
        return []
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((str(row.get("sleeve_id") or ""), str(row.get("source_hypothesis_id") or "")), []).append(row)
    return [
        _finding(
            finding_type=finding_type,
            rows=items,
            evidence_gate=evidence_gate,
            input_artifact_refs=input_artifact_refs,
            summary=summary,
            severity=severity,
            task_type=FINDING_TASK_MAP.get(finding_type, ""),
        )
        for _key, items in sorted(grouped.items())
    ]


def _promotion_demote_findings(*, rows: list[dict[str, Any]], evidence_gate: dict[str, Any], input_artifact_refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not evidence_gate.get("strong_conclusions_allowed"):
        return []
    out = []
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((str(row.get("sleeve_id") or ""), str(row.get("source_hypothesis_id") or "")), []).append(row)
    for _key, items in sorted(grouped.items()):
        closed = [row for row in items if row.get("lifecycle_status") == "EXECUTED_CLOSED"]
        returns = [_number(row.get("return_pct")) for row in closed if _number(row.get("return_pct")) is not None]
        if len(returns) < 10:
            continue
        total = sum(returns)
        if total > 0:
            out.append(_finding("PROMOTION_CANDIDATE", closed, evidence_gate, input_artifact_refs, "Review-only promotion candidate based on stronger positive pattern.", "MEDIUM", "promotion_candidate_review"))
        if total < 0:
            out.append(_finding("DEMOTION_CANDIDATE", closed, evidence_gate, input_artifact_refs, "Review-only demotion candidate based on stronger negative pattern.", "HIGH", "demotion_candidate_review"))
    return out


def _finding(
    finding_type: str,
    rows: list[dict[str, Any]],
    evidence_gate: dict[str, Any],
    input_artifact_refs: list[dict[str, Any]],
    summary: str,
    severity: str,
    task_type: str,
) -> dict[str, Any]:
    trade_ids = sorted({str(row.get("trade_id") or "") for row in rows if row.get("trade_id")})
    sleeve_id = _common_value(rows, "sleeve_id")
    hypothesis_id = _common_value(rows, "source_hypothesis_id")
    event_type = _common_value(rows, "event_type")
    regime_state = _common_value(rows, "regime_state")
    confidence = _confidence_for_gate(evidence_gate)
    finding_id = "finding:" + ":".join(
        _safe(part)
        for part in [
            finding_type,
            sleeve_id or "portfolio",
            hypothesis_id or "unknown_hypothesis",
            ",".join(trade_ids) or evidence_gate["evidence_gate_id"],
        ]
    )
    return {
        "finding_id": finding_id,
        "finding_type": finding_type,
        "sleeve_id": sleeve_id,
        "hypothesis_id": hypothesis_id,
        "event_type": event_type,
        "regime_state": regime_state,
        "evidence_refs": _evidence_refs(input_artifact_refs=input_artifact_refs, rows=rows, evidence_gate=evidence_gate),
        "trade_ids": trade_ids,
        "sample_count": len(rows),
        "summary": _downgraded_summary(summary, evidence_gate),
        "severity": severity,
        "confidence": confidence,
        "recommended_action": "Human review required; route only to offline Research Lab if task gate allows.",
        "recommended_research_task_type": task_type,
        "production_action_allowed": False,
    }


def _research_tasks_from_findings(
    *,
    findings: list[dict[str, Any]],
    review_id: str,
    evidence_gate: dict[str, Any],
    generated_at_utc: str,
    input_artifact_refs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not evidence_gate.get("automatic_research_task_creation_allowed"):
        return []
    priority = _task_priority(str(evidence_gate.get("sample_band") or ""))
    tasks = []
    for idx, finding in enumerate(findings, start=1):
        task_type = str(finding.get("recommended_research_task_type") or "")
        if task_type not in ALLOWED_TASK_TYPES:
            continue
        hypothesis_id = str(finding.get("hypothesis_id") or "")
        if not hypothesis_id:
            continue
        task_id = f"task:{_safe(review_id)}:{_safe(str(finding['finding_id']))}:{idx:03d}"
        tasks.append(
            {
                "task_id": task_id,
                "task_type": task_type,
                "hypothesis_id": hypothesis_id,
                "priority": priority,
                "status": "open",
                "requested_action": str(finding["summary"]),
                "ai_feedback_review_id": review_id,
                "finding_id": str(finding["finding_id"]),
                "evidence_gate_id": str(evidence_gate["evidence_gate_id"]),
                "source_artifact_refs": input_artifact_refs,
                "offline_research_only": True,
                "production_action_allowed": False,
                "created_at": generated_at_utc,
            }
        )
    return tasks


def _legacy_task_from_ai_task(task: dict[str, Any], *, generated_at_utc: str) -> dict[str, Any]:
    return build_research_task_v1(
        task_id=str(task["task_id"]),
        task_type=str(task["task_type"]),
        hypothesis_id=str(task["hypothesis_id"]),
        source_trigger=f"ai_feedback_review.v1:{task['ai_feedback_review_id']}|finding:{task['finding_id']}|evidence_gate:{task['evidence_gate_id']}",
        priority=str(task.get("priority") or "normal"),
        requested_action=str(task.get("requested_action") or "Review AI feedback finding offline."),
        created_at=generated_at_utc,
        output_expected="research_result_ledger.v1",
    )


def _hypothesis_suggestions(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    suggestions = []
    for finding in findings:
        if not finding.get("hypothesis_id"):
            continue
        suggestions.append(
            {
                "hypothesis_id": finding["hypothesis_id"],
                "finding_id": finding["finding_id"],
                "suggestion": "Review hypothesis wording or falsification criteria against observed sleeve outcomes.",
                "human_review_required": True,
                "production_action_allowed": False,
            }
        )
    return suggestions


def _trade_rows(reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [row for report in reports for row in _objects(report.get("trade_lifecycle_rows"))]
    return sorted(rows, key=lambda row: (str(row.get("sleeve_id") or ""), str(row.get("trade_id") or "")))


def _sample_band(sample_count: int) -> str:
    if sample_count <= 0:
        return "NO_EVIDENCE"
    if sample_count < 3:
        return "OBSERVATION_ONLY"
    if sample_count < 10:
        return "WEAK_SIGNAL"
    if sample_count < 20:
        return "REVIEWABLE_PATTERN"
    return "STRONGER_PATTERN"


def _data_quality_score(
    *,
    sample_count: int,
    missing_receipts: int,
    missing_outcomes: int,
    stale_data: bool,
    missing_regime_labels: int,
    missing_event_labels: int,
    missing_price_outcome_data: int,
) -> int:
    score = 100
    if sample_count == 0:
        score -= 80
    score -= min(45, missing_receipts * 15)
    score -= min(50, missing_outcomes * 20)
    score -= 40 if stale_data else 0
    score -= min(20, missing_regime_labels * 5)
    score -= min(20, missing_event_labels * 5)
    score -= min(40, missing_price_outcome_data * 20)
    return max(0, min(100, score))


def _blocked_reason_codes(**kwargs: Any) -> list[str]:
    codes = []
    if kwargs["sample_count"] == 0:
        codes.append("NO_TRADE_EVIDENCE")
    if kwargs["sample_band"] == "OBSERVATION_ONLY":
        codes.append("OBSERVATION_ONLY_SAMPLE")
    if kwargs["missing_receipts"]:
        codes.append("MISSING_RECEIPTS")
    if kwargs["missing_outcomes"]:
        codes.append("MISSING_OUTCOMES")
    if kwargs["stale_data"]:
        codes.append("STALE_DATA")
    if kwargs["missing_regime_labels"]:
        codes.append("MISSING_REGIME_LABELS")
    if kwargs["missing_event_labels"]:
        codes.append("MISSING_EVENT_LABELS")
    if kwargs["missing_price_outcome_data"]:
        codes.append("MISSING_PRICE_OR_OUTCOME_DATA")
    if kwargs["data_quality_status"] == "BLOCK":
        codes.append("DATA_QUALITY_BLOCK")
    return sorted(codes)


def _has_stale_data(*, reports: list[dict[str, Any]], rows: list[dict[str, Any]], period_start: str, period_end: str) -> bool:
    if not reports:
        return True
    days = _period_days(period_start, period_end)
    for report in reports:
        report_day = str(report.get("day_utc") or report.get("period_end") or "")
        if report_day and report_day not in days:
            return True
        if str(report.get("data_status") or "").upper() in {"STALE", "DATA_STALE"}:
            return True
    return any(str(row.get("data_status") or "").upper() in {"STALE", "DATA_STALE"} for row in rows)


def _period_days(period_start: str, period_end: str) -> set[str]:
    try:
        start = date.fromisoformat(period_start)
        end = date.fromisoformat(period_end)
    except ValueError:
        return {period_start, period_end}
    out = set()
    current = start
    while current <= end:
        out.add(current.isoformat())
        current += timedelta(days=1)
    return out


def _missing_price_or_outcome(row: dict[str, Any]) -> bool:
    status = str(row.get("lifecycle_status") or "")
    if status == "EXECUTED_CLOSED":
        return not str(row.get("return_pct") or "") or not str(row.get("outcome_status") or "")
    if status == "MISSING_OUTCOME":
        return True
    return False


def _evidence_refs(*, input_artifact_refs: list[dict[str, Any]], rows: list[dict[str, Any]], evidence_gate: dict[str, Any]) -> list[dict[str, Any]]:
    refs = list(input_artifact_refs)
    refs.append({"artifact_type": "evidence_gate.v1", "artifact_id": evidence_gate["evidence_gate_id"]})
    for row in rows:
        if row.get("trade_id"):
            refs.append({"artifact_type": "sleeve_performance_report.trade_lifecycle_row", "trade_id": str(row["trade_id"])})
    return refs


def _task_priority(sample_band: str) -> str:
    if sample_band == "STRONGER_PATTERN":
        return "high"
    if sample_band == "REVIEWABLE_PATTERN":
        return "normal"
    return "low"


def _confidence_for_gate(evidence_gate: dict[str, Any]) -> str:
    if evidence_gate.get("stale_data") or evidence_gate.get("data_quality_status") == "BLOCK":
        return "LOW"
    band = str(evidence_gate.get("sample_band") or "")
    if band == "STRONGER_PATTERN":
        return "HIGH"
    if band == "REVIEWABLE_PATTERN":
        return "MEDIUM"
    return "LOW"


def _downgraded_summary(summary: str, evidence_gate: dict[str, Any]) -> str:
    if not evidence_gate.get("strong_conclusions_allowed"):
        return f"{summary} Evidence gate blocks strong conclusions; treat as review input only."
    return summary


def _common_value(rows: list[dict[str, Any]], key: str) -> str:
    values = sorted({str(row.get(key) or "") for row in rows if row.get(key)})
    return values[0] if len(values) == 1 else ""


def _event_ids(ledgers: list[dict[str, Any]]) -> set[str]:
    return {str(event.get("event_id") or "") for ledger in ledgers for event in _objects(ledger.get("events")) if event.get("event_id")}


def _alert_ids(alert_ledgers: list[dict[str, Any]], rows: list[dict[str, Any]]) -> list[str]:
    ids = {str(row.get("alert_id") or "") for row in rows if row.get("alert_id")}
    for ledger in alert_ledgers:
        for attempt in _objects(ledger.get("alert_attempts")):
            if attempt.get("alert_id"):
                ids.add(str(attempt["alert_id"]))
    return sorted(ids)


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


def _severity_rank(value: str) -> int:
    return {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(value, 3)


def _safe(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or "").strip()) or "unknown"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}
