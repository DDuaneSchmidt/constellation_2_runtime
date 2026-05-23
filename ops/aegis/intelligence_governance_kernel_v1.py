from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import ai_evidence_v1, latest_json_v1, now_utc_v1, write_json_v1
from ops.aegis.intelligence_governance.ai_output_governance_v1 import govern_ai_output_v1
from ops.aegis.intelligence_governance.evidence_chain_v1 import evidence_chain_item_v1, object_hash_v1, validate_evidence_chain_v1
from ops.aegis.intelligence_governance.metric_rules_v1 import formula_catalog_v1, metric_result_v1
from ops.aegis.runtime_truth_kernel_v1 import DEFAULT_TRUTH_ROOT


KERNEL_VERSION = "aegis_intelligence_governance_kernel.v1"
RECOMMENDATION_STATES = {"PROPOSED", "NEEDS_MORE_EVIDENCE", "APPROVED_BY_HUMAN", "REJECTED_BY_HUMAN", "DEFERRED", "SUPERSEDED", "EXPIRED", "EXECUTED_EXTERNALLY", "AUDITED"}


def report_dir_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_intelligence_governance_kernel_v1" / day_utc


def approval_ledger_jsonl_path_v1(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / "aegis_intelligence_approval_ledger_v1" / day_utc / "intelligence_approval_ledger.v1.jsonl"


def build_intelligence_governance_kernel_v1(*, truth_root: Path, repo_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    generated_at = now_utc_v1()
    inputs = _input_reports(root, day_utc)
    recommendations = _govern_recommendations(root=root, repo_root=repo_root, day_utc=day_utc, generated_at=generated_at, inputs=inputs)
    ledger_events = _current_ledger_events(truth_root=root, day_utc=day_utc)
    recommendations = [_apply_approval_state(rec, ledger_events) for rec in recommendations]
    validation = _validation_summary(recommendations)
    ai = ai_evidence_v1(repo_root)
    evidence_quality_summary = _evidence_quality_summary(recommendations)
    return {
        "schema_id": "aegis_intelligence_governance_kernel",
        "schema_version": "v1",
        "artifact_id": "aegis_intelligence_governance_kernel_v1",
        "kernel_version": KERNEL_VERSION,
        "day_utc": day_utc,
        "generated_at_utc": generated_at,
        "truth_root": str(root),
        "dual_kernel_boundary": {
            "runtime_truth_kernel_is_sole_authority_for_readiness_permissions": True,
            "intelligence_governance_kernel_scope": ["facts", "metrics", "interpretations", "recommendations", "ai_outputs", "human_approvals"],
            "may_mutate_runtime_truth": False,
            "may_execute_trades": False,
            "may_mutate_sleeves": False,
        },
        "ai_usage": {
            "ai_used": bool(ai["ai_used"]),
            "deterministic_fallback": bool(ai["deterministic_fallback"]),
            "model_provider": "" if not ai["ai_used"] else ai.get("model_used", "UNKNOWN"),
        },
        "input_reports": {key: {"path": row["path"], "status": row["status"]} for key, row in inputs.items()},
        "metric_rules": formula_catalog_v1(),
        "metric_rule_validation_examples": {
            "sharpe_empty": metric_result_v1("sharpe", []),
            "win_rate_empty": metric_result_v1("win_rate", []),
        },
        "recommendation_lifecycle_states": sorted(RECOMMENDATION_STATES),
        "recommendations": recommendations,
        "recommendation_count": len(recommendations),
        "approval_summary": _approval_summary(recommendations),
        "evidence_quality_summary": evidence_quality_summary,
        "validation": validation,
        "validation_summary": validation,
        "ai_output_governance": [
            govern_ai_output_v1(
                repo_root=repo_root,
                output_type="RECOMMENDATION",
                prompt="deterministic adaptive governance recommendation synthesis",
                input_artifacts=[row["path"] for row in inputs.values() if row["path"]],
                output_payload={"recommendation_ids": [rec["recommendation_id"] for rec in recommendations]},
                evidence_citations=[row["path"] for row in inputs.values() if row["path"]],
            )
        ],
        "approval_ledger_path": str(approval_ledger_jsonl_path_v1(truth_root=root, day_utc=day_utc)),
        "safety": {
            "human_approval_required": True,
            "automated_change_allowed": False,
            "runtime_truth_mutation_allowed": False,
            "broker_execution_allowed": False,
            "autonomous_execution_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
        },
    }


def write_intelligence_governance_reports_v1(*, truth_root: Path, repo_root: Path, payload: dict[str, Any]) -> dict[str, str]:
    root = Path(truth_root).expanduser().resolve()
    day_utc = str(payload["day_utc"])
    out_dir = report_dir_v1(truth_root=root, day_utc=day_utc)
    kernel_path = write_json_v1(out_dir / "intelligence_governance_kernel.v1.json", payload)
    summary_path = out_dir / "intelligence_governance_kernel.summary.txt"
    summary_path.write_text(render_intelligence_governance_summary_v1(payload), encoding="utf-8")
    rec_path = write_json_v1(
        out_dir / "intelligence_recommendations.v1.json",
        {
            "schema_id": "aegis_intelligence_recommendations",
            "schema_version": "v1",
            "day_utc": day_utc,
            "generated_at_utc": payload.get("generated_at_utc"),
            "recommendations": payload.get("recommendations") or [],
            "recommendation_count": payload.get("recommendation_count", 0),
        },
    )
    ledger_path = _ensure_proposed_events(root=root, repo_root=repo_root, day_utc=day_utc, recommendations=payload.get("recommendations") or [])
    ledger_snapshot_path = write_json_v1(
        out_dir / "intelligence_approval_ledger.v1.json",
        {
            "schema_id": "aegis_intelligence_approval_ledger_snapshot",
            "schema_version": "v1",
            "day_utc": day_utc,
            "jsonl_path": str(ledger_path),
            "events": _current_ledger_events(truth_root=root, day_utc=day_utc),
        },
    )
    return {
        "intelligence_governance_kernel": str(kernel_path),
        "summary": str(summary_path),
        "intelligence_recommendations": str(rec_path),
        "intelligence_approval_ledger": str(ledger_snapshot_path),
        "intelligence_approval_ledger_jsonl": str(ledger_path),
    }


def render_intelligence_governance_summary_v1(payload: dict[str, Any]) -> str:
    approval = payload.get("approval_summary") if isinstance(payload.get("approval_summary"), dict) else {}
    validation = payload.get("validation") if isinstance(payload.get("validation"), dict) else {}
    ai = payload.get("ai_usage") if isinstance(payload.get("ai_usage"), dict) else {}
    lines = [
        "AEGIS INTELLIGENCE GOVERNANCE KERNEL v1",
        f"day_utc: {payload.get('day_utc')}",
        f"recommendation_count: {payload.get('recommendation_count')}",
        f"awaiting_approval: {approval.get('awaiting_approval', 0)}",
        f"needs_more_evidence: {approval.get('needs_more_evidence', 0)}",
        f"approved: {approval.get('approved', 0)}",
        f"rejected: {approval.get('rejected', 0)}",
        f"ai_used: {str(ai.get('ai_used', False)).lower()}",
        f"deterministic_fallback: {str(ai.get('deterministic_fallback', True)).lower()}",
        f"validation_issue_count: {validation.get('issue_count', 0)}",
        f"approval_ledger_path: {payload.get('approval_ledger_path')}",
        "runtime_truth_mutation_allowed: false",
        "broker_execution_allowed: false",
        "autonomous_execution_allowed: false",
        "",
        "top_recommendations:",
    ]
    for rec in (payload.get("recommendations") or [])[:10]:
        lines.append(f"- {rec.get('recommendation_id')}: status={rec.get('approval_status')} confidence={rec.get('confidence')} target={rec.get('target')}")
    lines.append("")
    return "\n".join(lines)


def replay_intelligence_governance_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    path, kernel = latest_json_v1(truth_root, "aegis_intelligence_governance_kernel_v1", day_utc, "intelligence_governance_kernel.v1.json")
    events = _current_ledger_events(truth_root=truth_root, day_utc=day_utc)
    return {
        "schema_id": "aegis_intelligence_governance_replay",
        "schema_version": "v1",
        "day_utc": day_utc,
        "kernel_path": str(path or ""),
        "recommendations": kernel.get("recommendations") or [],
        "approval_events": events,
        "lineage": [
            {
                "recommendation_id": rec.get("recommendation_id"),
                "evidence_chain": rec.get("evidence_chain") or [],
                "approval_status": rec.get("approval_status"),
            }
            for rec in kernel.get("recommendations", [])
            if isinstance(rec, dict)
        ],
    }


def record_intelligence_approval_v1(*, truth_root: Path, day_utc: str, recommendation_id: str, decision: str, reason: str, operator: str = "operator") -> dict[str, Any]:
    decision = decision.upper()
    event_type = {"APPROVED": "APPROVED", "REJECTED": "REJECTED", "DEFERRED": "DEFERRED"}.get(decision)
    if event_type is None:
        raise ValueError("decision must be APPROVED, REJECTED, or DEFERRED")
    if not recommendation_id or not reason:
        raise ValueError("recommendation_id and reason are required")
    ledger_path = approval_ledger_jsonl_path_v1(truth_root=truth_root, day_utc=day_utc)
    prior_hash = _ledger_tail_hash(ledger_path)
    entry = _ledger_event(recommendation_id=recommendation_id, event_type=event_type, operator=operator, reason=reason, source_hash="", prior_state_hash=prior_hash)
    _append_jsonl(ledger_path, entry)
    return entry


def _input_reports(root: Path, day_utc: str) -> dict[str, dict[str, Any]]:
    specs = {
        "regime_context": ("regime_context_v1", "regime_context.v1.json"),
        "sleeve_performance_analytics": ("sleeve_performance_analytics_v1", "sleeve_performance_analytics.v1.json"),
        "failure_analysis": ("failure_analysis_v1", "failure_analysis.v1.json"),
        "research_memory_graph": ("research_memory_graph_v1", "research_memory_graph.v1.json"),
        "research_queue_optimizer": ("research_queue_optimizer_v1", "research_queue_optimizer.v1.json"),
        "cross_sleeve_analysis": ("cross_sleeve_analysis_v1", "cross_sleeve_analysis.v1.json"),
        "adaptive_governance": ("adaptive_governance_v1", "adaptive_governance.v1.json"),
        "operator_inbox": ("aegis_operator_inbox_v1", "operator_inbox.v1.json"),
        "eod_intelligence": ("aegis_eod_intelligence_v1", "eod_intelligence.v1.json"),
        "eow_intelligence": ("aegis_eow_intelligence_v1", "eow_intelligence.v1.json"),
    }
    out: dict[str, dict[str, Any]] = {}
    for key, (family, filename) in specs.items():
        path, payload = latest_json_v1(root, family, day_utc, filename)
        out[key] = {"path": str(path or ""), "payload": payload, "status": "AVAILABLE" if path and payload else "NOT_FOUND"}
    return out


def _govern_recommendations(*, root: Path, repo_root: Path, day_utc: str, generated_at: str, inputs: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    adaptive = inputs.get("adaptive_governance", {}).get("payload") or {}
    raw_recs = adaptive.get("recommendations") or adaptive.get("governance_recommendations") or []
    if not isinstance(raw_recs, list) or not raw_recs:
        raw_recs = [{"type": "NO_ACTION", "target": "SYSTEM", "interpretation": "No adaptive recommendation found.", "confidence": "UNKNOWN", "evidence": []}]
    governed = []
    for idx, rec in enumerate(raw_recs):
        if not isinstance(rec, dict):
            continue
        rec_type = str(rec.get("type") or rec.get("recommendation") or "NO_ACTION")
        target = str(rec.get("target") or rec.get("sleeve_id") or rec.get("item_id") or "SYSTEM")
        rec_id = str(rec.get("recommendation_id") or f"{rec_type}:{target}:{idx + 1}")
        evidence = [str(item) for item in (rec.get("evidence") or []) if str(item)]
        if not evidence:
            evidence = [row["path"] for row in inputs.values() if row.get("path")]
        confidence = str(rec.get("confidence") or "UNKNOWN").upper()
        sample_size = len(evidence)
        minimum = 1
        quality = "LOW" if sample_size >= minimum else "INSUFFICIENT"
        chain = [
            evidence_chain_item_v1(conclusion_id=f"{rec_id}:fact", layer="FACT", source_artifacts=evidence, evidence_quality=quality, confidence="LOW", repo_root=repo_root),
            evidence_chain_item_v1(conclusion_id=f"{rec_id}:metric", layer="METRIC", source_artifacts=evidence, formula_or_rule="count(source_artifacts)", sample_size=sample_size, minimum_sample_size=minimum, evidence_quality=quality, confidence="LOW", repo_root=repo_root),
            evidence_chain_item_v1(conclusion_id=f"{rec_id}:interpretation", layer="INTERPRETATION", source_artifacts=evidence, formula_or_rule="recommendation cites adaptive governance interpretation", sample_size=sample_size, minimum_sample_size=minimum, evidence_quality=quality, confidence=confidence, repo_root=repo_root),
            evidence_chain_item_v1(conclusion_id=f"{rec_id}:recommendation", layer="RECOMMENDATION", source_artifacts=evidence, formula_or_rule="recommendation requires human approval", sample_size=sample_size, minimum_sample_size=minimum, evidence_quality=quality, confidence=confidence, repo_root=repo_root),
        ]
        issues = validate_evidence_chain_v1(chain)
        approval_status = "NEEDS_MORE_EVIDENCE" if issues or quality == "INSUFFICIENT" else "PROPOSED"
        governed.append(
            {
                "recommendation_id": rec_id,
                "type": rec_type,
                "target": target,
                "created_at": generated_at,
                "source_engine": "adaptive_governance_v1",
                "evidence_chain": chain,
                "evidence_chain_validation_issues": issues,
                "proposed_change": rec.get("interpretation") or rec.get("reason") or "Review recommendation manually.",
                "expected_impact": rec.get("expected_impact") or "UNKNOWN",
                "risk": rec.get("risk") or "UNKNOWN",
                "confidence": chain[-1]["confidence"],
                "approval_status": approval_status,
                "approval_required": True,
                "approved_by": None,
                "approved_at": None,
                "rejection_reason": None,
                "validity_window": "1d",
                "expires_at": None,
                "supersedes": None,
                "superseded_by": None,
                "audit_history": [],
                "human_approval_required": True,
                "automated_change_allowed": False,
                "runtime_truth_mutation_allowed": False,
                "broker_execution_allowed": False,
                "autonomous_execution_allowed": False,
            }
        )
    return governed


def _apply_approval_state(rec: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    matching = [event for event in events if event.get("recommendation_id") == rec.get("recommendation_id")]
    out = dict(rec)
    out["audit_history"] = matching
    if not matching:
        return out
    latest = matching[-1]
    event_type = latest.get("event_type")
    if event_type == "APPROVED":
        out["approval_status"] = "APPROVED_BY_HUMAN"
        out["approved_by"] = latest.get("operator")
        out["approved_at"] = latest.get("timestamp")
    elif event_type == "REJECTED":
        out["approval_status"] = "REJECTED_BY_HUMAN"
        out["rejection_reason"] = latest.get("reason")
    elif event_type == "DEFERRED":
        out["approval_status"] = "DEFERRED"
    elif event_type in RECOMMENDATION_STATES:
        out["approval_status"] = event_type
    return out


def _validation_summary(recommendations: list[dict[str, Any]]) -> dict[str, Any]:
    issues = []
    for rec in recommendations:
        for issue in rec.get("evidence_chain_validation_issues") or []:
            issues.append(f"{rec.get('recommendation_id')}:{issue}")
        for field in ("automated_change_allowed", "runtime_truth_mutation_allowed", "broker_execution_allowed", "autonomous_execution_allowed"):
            if bool(rec.get(field)):
                issues.append(f"{rec.get('recommendation_id')}:{field.upper()}_FORBIDDEN")
    return {"status": "PASS" if not issues else "WARN", "issue_count": len(issues), "issues": issues}


def _approval_summary(recommendations: list[dict[str, Any]]) -> dict[str, int]:
    statuses = [str(rec.get("approval_status") or "UNKNOWN") for rec in recommendations]
    return {
        "awaiting_approval": statuses.count("PROPOSED"),
        "needs_more_evidence": statuses.count("NEEDS_MORE_EVIDENCE"),
        "approved": statuses.count("APPROVED_BY_HUMAN"),
        "rejected": statuses.count("REJECTED_BY_HUMAN"),
        "deferred": statuses.count("DEFERRED"),
    }


def _evidence_quality_summary(recommendations: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "INSUFFICIENT": 0, "UNKNOWN": 0}
    for rec in recommendations:
        for item in rec.get("evidence_chain") or []:
            quality = str(item.get("evidence_quality") or "UNKNOWN").upper() if isinstance(item, dict) else "UNKNOWN"
            counts[quality if quality in counts else "UNKNOWN"] += 1
    return counts


def _ensure_proposed_events(root: Path, repo_root: Path, day_utc: str, recommendations: list[dict[str, Any]]) -> Path:
    ledger_path = approval_ledger_jsonl_path_v1(truth_root=root, day_utc=day_utc)
    existing = _current_ledger_events(truth_root=root, day_utc=day_utc)
    proposed_ids = {event.get("recommendation_id") for event in existing if event.get("event_type") == "PROPOSED"}
    for rec in recommendations:
        rec_id = str(rec.get("recommendation_id") or "")
        if not rec_id or rec_id in proposed_ids:
            continue
        prior_hash = _ledger_tail_hash(ledger_path)
        event = _ledger_event(recommendation_id=rec_id, event_type="PROPOSED", operator="kernel", reason="Recommendation proposed by Intelligence Governance Kernel.", source_hash=object_hash_v1(rec), prior_state_hash=prior_hash)
        _append_jsonl(ledger_path, event)
    return ledger_path


def _ledger_event(*, recommendation_id: str, event_type: str, operator: str, reason: str, source_hash: str, prior_state_hash: str) -> dict[str, Any]:
    base = {
        "timestamp": now_utc_v1(),
        "recommendation_id": recommendation_id,
        "event_type": event_type,
        "operator": operator,
        "reason": reason,
        "source_hash": source_hash,
        "prior_state_hash": prior_state_hash,
    }
    base["new_state_hash"] = object_hash_v1(base)
    base["ledger_event_id"] = f"{event_type}:{recommendation_id}:{base['new_state_hash'][:12]}"
    return base


def _append_jsonl(path: Path, event: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")


def _current_ledger_events(*, truth_root: Path, day_utc: str) -> list[dict[str, Any]]:
    path = approval_ledger_jsonl_path_v1(truth_root=truth_root, day_utc=day_utc)
    if not path.exists():
        return []
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            events.append(payload)
    return events


def _ledger_tail_hash(path: Path) -> str:
    if not path.exists():
        return ""
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return object_hash_v1(lines[-1]) if lines else ""
