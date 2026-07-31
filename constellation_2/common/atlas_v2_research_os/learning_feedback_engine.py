from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_quality_models import MEASUREMENT_ONLY_LIMITATION
from .failure_patterns import can_failure_influence_priority, get_repeated_failures, list_failure_patterns
from .learning_feedback_governance import LearningFeedbackGovernanceError, validate_learning_feedback_allowed, validate_learning_feedback_memory_influence
from .learning_feedback_models import BacklogCreationRecord, CandidateQualityFeedbackRecord, MemoryInfluenceRecord, PriorityAdjustmentRecord
from .memory_index import add_memory_object, get_memory_object, list_memory_objects
from .memory_models import MemoryEvidenceMaturity, MemoryLifecycleState, MemoryType, create_memory_object
from .priority_engine import score_item
from .regime_context import list_regime_contexts
from .research_backlog import ResearchBacklog, compute_priority_score, priority_reasons_for_scores
from .semantic_deduplication import find_potential_duplicates

NOW_FALLBACK = "2026-06-04T00:00:00Z"
UNRESOLVED_BACKLOG_STATES = {"NEW", "READY", "IN_PROGRESS", "BLOCKED"}


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def derive_priority_adjustments_from_memory(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> list[dict[str, Any]]:
    created = created_at or now_utc()
    records: list[dict[str, Any]] = []
    for failure in get_repeated_failures(root, 2):
        records.append(_priority_record(
            created, [failure["failure_id"]], failure.get("source_artifact_ids", []), "FAILURE_ANALYSIS",
            "repeated failure increases FAILURE_ANALYSIS priority",
            {"failure_reduction_score_delta": 0.8, "expected_learning_value_delta": 0.3, "source_kind": "FailurePattern", "failure_id": failure["failure_id"], "blocked": not can_failure_influence_priority(failure)},
            failure.get("evidence_level", MemoryEvidenceMaturity.GENERATED_ONLY.value),
            "REOPENED" if can_failure_influence_priority(failure) else "RETIRED",
        ))
    for match in find_potential_duplicates(root):
        if match["status"] == "LIKELY_DUPLICATE":
            records.append(_priority_record(
                created, [match["left_memory_id"], match["right_memory_id"]], [], "DUPLICATE_REVIEW",
                "likely duplicate decreases novelty score and creates review priority",
                {"novelty_score_delta": -0.5, "expected_learning_value_delta": 0.2, "duplicate_score": match["duplicate_score"]},
                MemoryEvidenceMaturity.GENERATED_ONLY.value,
                "NEW",
            ))
    regime_by_id = {row["regime_context_id"]: row for row in list_regime_contexts(root)}
    for memory in list_memory_objects(root):
        if memory.get("lifecycle_state") == MemoryLifecycleState.STALE.value:
            records.append(_priority_record(created, [memory["memory_id"]], memory.get("source_artifact_ids", []), "STALE_LEARNING_REVIEW", "STALE lifecycle state creates review priority", {"expected_learning_value_delta": 0.4, "evidence_gap_score_delta": 0.2}, memory.get("evidence_level", "GENERATED_ONLY"), memory.get("lifecycle_state", "NEW")))
        if memory.get("lifecycle_state") == MemoryLifecycleState.FALSIFIED.value:
            continue
        contexts = [regime_by_id.get(context_id, {}) for context_id in memory.get("regime_context_ids", [])]
        missing_or_unknown = not contexts or any("UNKNOWN" in context.get("labels", []) for context in contexts)
        if missing_or_unknown:
            records.append(_priority_record(created, [memory["memory_id"]], memory.get("source_artifact_ids", []), "REGIME_GAP", "missing or UNKNOWN regime context increases REGIME_GAP priority", {"regime_gap_score_delta": 0.7, "expected_learning_value_delta": 0.2}, memory.get("evidence_level", "GENERATED_ONLY"), memory.get("lifecycle_state", "NEW")))
        if memory.get("lifecycle_state") == MemoryLifecycleState.REOPENED.value:
            records.append(_priority_record(created, [memory["memory_id"]], memory.get("source_artifact_ids", []), "STALE_LEARNING_REVIEW", "reopened knowledge creates review priority", {"expected_learning_value_delta": 0.5}, memory.get("evidence_level", "GENERATED_ONLY"), memory.get("lifecycle_state", "NEW")))
    return records


def apply_memory_priority_adjustments(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> list[dict[str, Any]]:
    records = derive_priority_adjustments_from_memory(root, created_at=created_at)
    backlog = ResearchBacklog(root)
    rows = backlog._read()
    by_type = {row["item_type"]: row for row in rows if row.get("state") in UNRESOLVED_BACKLOG_STATES}
    applied: list[dict[str, Any]] = []
    for record in records:
        if record.get("governance_status") == "BLOCKED":
            applied.append(record)
            continue
        item = by_type.get(record["influence_type"])
        if not item:
            applied.append(record)
            continue
        metadata = record["metadata"]
        for score_name, delta_name in [
            ("expected_learning_value", "expected_learning_value_delta"),
            ("novelty_score", "novelty_score_delta"),
            ("failure_reduction_score", "failure_reduction_score_delta"),
            ("evidence_gap_score", "evidence_gap_score_delta"),
            ("regime_gap_score", "regime_gap_score_delta"),
        ]:
            if delta_name in metadata:
                item[score_name] = round(float(item.get(score_name, 0.0)) + float(metadata[delta_name]), 6)
        item["priority_score"] = score_item(item)
        item["priority_reasons"] = priority_reasons_for_scores(item.get("expected_learning_value", 0.0), item.get("novelty_score", 0.0), item.get("candidate_impact_estimate", 0.0), item.get("failure_reduction_score", 0.0), item.get("evidence_gap_score", 0.0), item.get("regime_gap_score", 0.0), item.get("cost_estimate", 0.0))
        item.setdefault("source_memory_ids", [])
        for memory_id in record["source_memory_ids"]:
            if memory_id not in item["source_memory_ids"]:
                item["source_memory_ids"].append(memory_id)
        record["affected_backlog_item_ids"] = [item["backlog_item_id"]]
        record["affected_priority_scores"] = {item["backlog_item_id"]: item["priority_score"]}
        applied.append(record)
    backlog._write(rows)
    return applied


def explain_priority_adjustment(record: dict[str, Any]) -> str:
    label = "generated-only; research-priority-only" if record.get("evidence_level") == MemoryEvidenceMaturity.GENERATED_ONLY.value else "research-priority-only"
    return f"{record['influence_type']}: {record['influence_reason']} ({label})."


def create_backlog_items_from_memory(root: str | Path = DEFAULT_STORE_ROOT, *, created_at: str | None = None) -> list[dict[str, Any]]:
    created = created_at or now_utc()
    created_rows: list[dict[str, Any]] = []
    for failure in get_repeated_failures(root, 2):
        if can_failure_influence_priority(failure):
            created_rows.append(create_failure_analysis_backlog_item(root, failure, created_at=created))
    for match in find_potential_duplicates(root):
        if match["status"] == "LIKELY_DUPLICATE":
            created_rows.append(create_duplicate_review_backlog_item(root, match, created_at=created))
    regime_by_id = {row["regime_context_id"]: row for row in list_regime_contexts(root)}
    for memory in list_memory_objects(root):
        if memory.get("lifecycle_state") == MemoryLifecycleState.FALSIFIED.value:
            continue
        if memory.get("lifecycle_state") == MemoryLifecycleState.STALE.value:
            created_rows.append(create_stale_learning_review_backlog_item(root, memory, created_at=created))
        contexts = [regime_by_id.get(context_id, {}) for context_id in memory.get("regime_context_ids", [])]
        if not contexts or any("UNKNOWN" in context.get("labels", []) for context in contexts):
            created_rows.append(create_regime_gap_backlog_item(root, memory, created_at=created))
    return created_rows


def create_failure_analysis_backlog_item(root: str | Path, failure: dict[str, Any], *, created_at: str | None = None) -> dict[str, Any]:
    return _create_feedback_backlog_item(root, item_type="FAILURE_ANALYSIS", issue_key=failure["failure_id"], title=f"Analyze repeated failure {failure['failure_id']}", description=failure.get("reason", "Repeated failure requires research analysis."), source_memory_ids=[failure["failure_id"]], source_artifact_ids=failure.get("source_artifact_ids", []), scores={"expected_learning_value": 0.6, "failure_reduction_score": 0.9}, created_at=created_at)


def create_duplicate_review_backlog_item(root: str | Path, duplicate: dict[str, Any], *, created_at: str | None = None) -> dict[str, Any]:
    memory_ids = [duplicate["left_memory_id"], duplicate["right_memory_id"]]
    return _create_feedback_backlog_item(root, item_type="DUPLICATE_REVIEW", issue_key="-".join(sorted(memory_ids)), title="Review likely duplicate memory cluster", description="Likely duplicate research memory needs review; does not imply validation.", source_memory_ids=memory_ids, source_artifact_ids=[], scores={"expected_learning_value": 0.3, "novelty_score": -0.2}, created_at=created_at)


def create_regime_gap_backlog_item(root: str | Path, memory: dict[str, Any], *, created_at: str | None = None) -> dict[str, Any]:
    return _create_feedback_backlog_item(root, item_type="REGIME_GAP", issue_key=memory["memory_id"], title=f"Investigate regime gap for {memory['memory_id']}", description="Missing or UNKNOWN regime context needs research review.", source_memory_ids=[memory["memory_id"]], source_artifact_ids=memory.get("source_artifact_ids", []), scores={"expected_learning_value": 0.4, "regime_gap_score": 0.8}, created_at=created_at)


def create_stale_learning_review_backlog_item(root: str | Path, memory: dict[str, Any], *, created_at: str | None = None) -> dict[str, Any]:
    return _create_feedback_backlog_item(root, item_type="STALE_LEARNING_REVIEW", issue_key=memory["memory_id"], title=f"Review stale learning {memory['memory_id']}", description="Stale learning requires review before reuse.", source_memory_ids=[memory["memory_id"]], source_artifact_ids=memory.get("source_artifact_ids", []), scores={"expected_learning_value": 0.5, "evidence_gap_score": 0.4}, created_at=created_at)


def record_candidate_quality_feedback_to_memory(root: str | Path, evaluation: dict[str, Any], *, created_at: str | None = None) -> dict[str, Any]:
    created = created_at or now_utc()
    validate_learning_feedback_allowed({"metadata": evaluation.get("metadata", {}), "recommendation": "Continue measurement."})
    metadata = {
        "measurement_only": True,
        "limitations": [MEASUREMENT_ONLY_LIMITATION],
        "evaluation_status": evaluation.get("evaluation_status"),
        "delta": evaluation.get("delta", {}),
        "failure_patterns": derive_failure_patterns_from_candidate_quality(evaluation),
        "regime_gaps": derive_regime_gaps_from_candidate_quality(evaluation),
        "repeated_failure_changes": derive_repeated_failure_changes(evaluation),
        "certification_result": evaluation.get("certification_result", {}),
    }
    if not evaluation.get("comparable", True):
        metadata["review_backlog_required"] = True
    memory_id = f"mem-cq-feedback-{evaluation['evaluation_id']}"
    existing = {row["memory_id"] for row in list_memory_objects(root)}
    if memory_id not in existing:
        add_memory_object(create_memory_object(memory_id=memory_id, memory_type=MemoryType.LEARNING_NODE.value, created_at=created, source_artifact_ids=evaluation.get("source_artifact_ids", []) or ["candidate-quality-measurement"], evidence_level=_lowest_evidence_from_evaluation(evaluation), lifecycle_state=MemoryLifecycleState.NEW.value, labels=["candidate_quality_feedback", "measurement_only"], metadata=metadata), root)
    record = CandidateQualityFeedbackRecord(feedback_id=f"lf-cq-{evaluation['evaluation_id']}", created_at=created, source_memory_ids=list(evaluation.get("learning_input_ids", [])) + [memory_id], source_artifact_ids=evaluation.get("source_artifact_ids", []), source_candidate_quality_evaluation_ids=[evaluation["evaluation_id"]], influence_type="CANDIDATE_QUALITY_MEASUREMENT", influence_reason="candidate quality measurement routed back to memory without authority escalation", evidence_level=_lowest_evidence_from_evaluation(evaluation), lifecycle_state=MemoryLifecycleState.NEW.value, governance_status="PASS", lineage_status="LINKED", metadata=metadata).to_dict()
    return record


def derive_failure_patterns_from_candidate_quality(evaluation: dict[str, Any]) -> list[dict[str, Any]]:
    delta = evaluation.get("delta", {}).get("failure_category_distribution_delta", {}) or {}
    return [{"failure_category": key, "delta": value} for key, value in sorted(delta.items()) if value is not None]


def derive_regime_gaps_from_candidate_quality(evaluation: dict[str, Any]) -> list[dict[str, Any]]:
    categories = evaluation.get("metric_set", {}).get("treatment", {}).get("failure_category_distribution", {}).get("counts", {})
    if categories.get("REGIME_MISMATCH", 0) or categories.get("UNKNOWN", 0):
        return [{"gap_type": "REGIME_GAP", "reason": "candidate quality measurement included REGIME_MISMATCH or UNKNOWN failures"}]
    return []


def derive_repeated_failure_changes(evaluation: dict[str, Any]) -> dict[str, Any]:
    delta = evaluation.get("delta", {}).get("repeated_failure_reduction_delta")
    if delta is None:
        return {"status": "NO_COMPARABLE_DELTA", "delta": None}
    if delta < 0:
        return {"status": "REDUCTION", "delta": delta}
    if delta > 0:
        return {"status": "REGRESSION", "delta": delta}
    return {"status": "UNCHANGED", "delta": delta}


def run_learning_feedback_demo(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    from .candidate_quality_evaluation import evaluate_candidate_quality
    from .cli import seed_demo_candidate_quality, seed_demo_memory
    from .candidate_quality_baseline import load_candidate_quality_baseline
    from .candidate_quality_treatment import load_candidate_quality_treatment

    seed_demo_memory(Path(root))
    create_backlog_items_from_memory(root, created_at=NOW_FALLBACK)
    priority_records = apply_memory_priority_adjustments(root, created_at=NOW_FALLBACK)
    seed_demo_candidate_quality()
    baseline = load_candidate_quality_baseline("cq-baseline-demo")
    treatment = load_candidate_quality_treatment("cq-treatment-demo")
    evaluation = evaluate_candidate_quality(baseline, treatment, evaluation_id="cq-eval-learning-feedback-demo", created_by="atlas_research_os_learning_feedback", source_artifact_ids=["q-demo-memory-opening-range"])
    cq_record = record_candidate_quality_feedback_to_memory(root, evaluation, created_at=NOW_FALLBACK)
    top = ResearchBacklog(root).get_top_priority_items(3)
    return {
        "feedback_run_id": "learning-feedback-demo",
        "repeated_failure_memory": "failure-demo-opening-range",
        "backlog_items": [row for row in ResearchBacklog(root).list_backlog_items() if row.get("source_memory_ids")],
        "priority_adjustments": priority_records,
        "top_backlog": top,
        "candidate_quality_evaluation_id": evaluation["evaluation_id"],
        "candidate_quality_feedback": cq_record,
        "lineage_links": {"memory_to_backlog": ["failure-demo-opening-range"], "candidate_quality_to_memory": cq_record["source_memory_ids"]},
        "governance_result": "PASS",
        "limitations": ["Demo only; no autonomous research, scheduler, live candidate factory mutation, trading, capital, sleeve, portfolio, or promotion authority."],
    }


def _priority_record(created: str, memory_ids: list[str], artifact_ids: list[str], influence_type: str, reason: str, metadata: dict[str, Any], evidence_level: str, lifecycle_state: str) -> dict[str, Any]:
    governance_status = "PASS"
    blocked_reason = ""
    for memory_id in memory_ids:
        try:
            if metadata.get("source_kind") == "FailurePattern":
                if metadata.get("blocked"):
                    raise LearningFeedbackGovernanceError("retired failure pattern blocked")
            else:
                validate_learning_feedback_memory_influence(get_memory_object(memory_id), target="RESEARCH_PRIORITY")
        except Exception as exc:
            governance_status = "BLOCKED"
            blocked_reason = str(exc)
    metadata = dict(metadata)
    if blocked_reason:
        metadata["blocked_reason"] = blocked_reason
    return PriorityAdjustmentRecord(feedback_id=f"lf-priority-{_stable_id(memory_ids + [influence_type])}", created_at=created, source_memory_ids=memory_ids, source_artifact_ids=artifact_ids, influence_type=influence_type, influence_reason=reason, evidence_level=evidence_level, lifecycle_state=lifecycle_state, governance_status=governance_status, lineage_status="LINKED" if memory_ids or artifact_ids else "PARTIAL", metadata=metadata).to_dict()


def _create_feedback_backlog_item(root: str | Path, *, item_type: str, issue_key: str, title: str, description: str, source_memory_ids: list[str], source_artifact_ids: list[str], scores: dict[str, float], created_at: str | None) -> dict[str, Any]:
    backlog = ResearchBacklog(root)
    for row in backlog.list_backlog_items(item_type=item_type):
        if row.get("state") in UNRESOLVED_BACKLOG_STATES and row.get("metadata", {}).get("learning_feedback_issue_key") == issue_key:
            return row
    backlog_item_id = f"lf-{item_type.lower().replace('_', '-')}-{_stable_id([issue_key])}"
    item = backlog.create_backlog_item(backlog_item_id=backlog_item_id, item_type=item_type, title=title, description=description, created_at=created_at or now_utc(), created_by="atlas_research_os_learning_feedback", source_artifact_ids=source_artifact_ids, state="READY", expected_learning_value=scores.get("expected_learning_value", 0.0), novelty_score=scores.get("novelty_score", 0.0), failure_reduction_score=scores.get("failure_reduction_score", 0.0), evidence_gap_score=scores.get("evidence_gap_score", 0.0), regime_gap_score=scores.get("regime_gap_score", 0.0), priority_reasons=["learning_feedback_source"])
    rows = backlog._read()
    for row in rows:
        if row["backlog_item_id"] == item["backlog_item_id"]:
            row["source_memory_ids"] = source_memory_ids
            row["metadata"] = {"learning_feedback_issue_key": issue_key, "does_not_imply_validation": True}
            item = row
    backlog._write(rows)
    return item


def _lowest_evidence_from_evaluation(evaluation: dict[str, Any]) -> str:
    levels = []
    for side in ["baseline", "treatment"]:
        evidence_metric = evaluation.get("metric_set", {}).get(side, {}).get("evidence_maturity_score", {})
        levels.extend(evidence_metric.get("notes", []))
    evidence_levels = set(evaluation.get("metadata", {}).get("evidence_levels", []))
    if not evidence_levels:
        evidence_levels = {"HISTORICAL_REPLAY"}
    if evidence_levels <= {"MOCK_ONLY", "GENERATED_ONLY"}:
        return MemoryEvidenceMaturity.GENERATED_ONLY.value if "GENERATED_ONLY" in evidence_levels else MemoryEvidenceMaturity.MOCK_ONLY.value
    return MemoryEvidenceMaturity.HISTORICAL_REPLAY.value


def _stable_id(parts: list[str]) -> str:
    return hashlib.sha1(json.dumps(parts, sort_keys=True).encode("utf-8")).hexdigest()[:12]
