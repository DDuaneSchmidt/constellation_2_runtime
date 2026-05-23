from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.event_intake.event_intake_registry import load_event_cluster, load_event_observation, load_hypothesis_proposal, load_intent_candidate
from research_lab.research.research_plan import build_research_plan
from research_lab.research.research_plan_registry import store_research_plan
from research_lab.research_intake.intake_dossier import build_research_intake_dossier, render_research_intake_dossier_markdown, validate_research_intake_dossier
from research_lab.research_intake.proposal_priority import build_proposal_priority_score, validate_proposal_priority_score
from research_lab.research_intake.proposal_review import build_hypothesis_proposal_review, validate_hypothesis_proposal_review
from research_lab.research_intake.readiness_assessment import build_research_readiness_assessment, validate_research_readiness_assessment
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout

ROOT = "research_intake"


def _store(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root)


def _artifact_path(store: Path, group: str, artifact_id: str, suffix: str = ".json") -> Path:
    return store / ROOT / group / f"{artifact_id}{suffix}"


def _registry_path(store: Path, name: str) -> Path:
    return store / "registries" / f"{name}.jsonl"


def _audit(*, store: Path, actor: str, entity_type: str, entity_id: str, action: str, new_state_hash: str, reason: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    return write_audit_event(actor=actor, entity_type=entity_type, entity_id=entity_id, action=action, previous_state_hash="", new_state_hash=new_state_hash, reason=reason, metadata=metadata or {}, store_root=store)


def proposal_context(hypothesis_proposal_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    proposal = load_hypothesis_proposal(hypothesis_proposal_id, store_root=store_root)
    intent = load_intent_candidate(proposal["intent_candidate_id"], store_root=store_root)
    cluster = load_event_cluster(proposal.get("event_cluster_id") or intent["event_cluster_id"], store_root=store_root)
    observations = [load_event_observation(item, store_root=store_root) for item in cluster.get("observation_ids") or []]
    return {"proposal": proposal, "intent": intent, "cluster": cluster, "observations": observations}


def max_confidence(observations: list[dict[str, Any]]) -> str:
    rank = {"unknown": 0, "low": 1, "medium": 2, "high": 3}
    out = "unknown"
    for observation in observations:
        value = str(observation.get("confidence_level") or "unknown")
        if rank.get(value, 0) > rank.get(out, 0):
            out = value
    return out


def store_research_readiness_assessment(assessment: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_research_readiness_assessment(assessment)
    store = _store(store_root)
    base_id = str(assessment["research_readiness_assessment_id"])
    path = _artifact_path(store, "readiness_assessments", base_id)
    if path.exists():
        sequence = 2
        while path.exists():
            assessment["research_readiness_assessment_id"] = f"{base_id}_{sequence}"
            path = _artifact_path(store, "readiness_assessments", assessment["research_readiness_assessment_id"])
            sequence += 1
    write_json(path, assessment, overwrite=False)
    row = {
        "research_readiness_assessment_id": assessment["research_readiness_assessment_id"],
        "hypothesis_proposal_id": assessment["hypothesis_proposal_id"],
        "event_family_id": assessment["event_family_id"],
        "ready_for_research": assessment["ready_for_research"],
        "data_requirement_status": assessment["data_requirement_status"],
        "blocking_items": assessment["blocking_items"],
        "content_hash": assessment["content_hash"],
        "assessed_at": assessment["assessed_at"],
        "storage_uri": f"research://research_intake/readiness_assessments/{assessment['research_readiness_assessment_id']}.json",
        "schema_version": assessment["schema_version"],
    }
    append_jsonl(_registry_path(store, "research_readiness_assessments"), row)
    audit = _audit(store=store, actor=actor, entity_type="research_readiness_assessment", entity_id=assessment["research_readiness_assessment_id"], action="research_readiness_assessed", new_state_hash=assessment["content_hash"], reason="Assessed hypothesis proposal readiness for research intake gate.", metadata={"registry_row": row})
    return {"readiness_assessment": assessment, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def assess_hypothesis_readiness(*, hypothesis_proposal_id: str, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    context = proposal_context(hypothesis_proposal_id, store_root=store_root)
    assessment = build_research_readiness_assessment(proposal=context["proposal"], store_root=store_root, assessed_by=actor)
    return store_research_readiness_assessment(assessment, store_root=store_root, actor=actor)


def latest_readiness_assessment(hypothesis_proposal_id: str, *, store_root: Path | None = None) -> dict[str, Any] | None:
    store = _store(store_root)
    rows = [row for row in read_jsonl(_registry_path(store, "research_readiness_assessments")) if row.get("hypothesis_proposal_id") == hypothesis_proposal_id]
    if not rows:
        return None
    row = rows[-1]
    return read_json(_artifact_path(store, "readiness_assessments", str(row["research_readiness_assessment_id"])))


def store_proposal_priority_score(priority: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_proposal_priority_score(priority)
    store = _store(store_root)
    path = _artifact_path(store, "priority_scores", priority["proposal_priority_score_id"])
    write_json(path, priority, overwrite=False)
    row = {
        "proposal_priority_score_id": priority["proposal_priority_score_id"],
        "hypothesis_proposal_id": priority["hypothesis_proposal_id"],
        "score": priority["score"],
        "priority_bucket": priority["priority_bucket"],
        "recommended_next_action": priority["recommended_next_action"],
        "content_hash": priority["content_hash"],
        "scored_at": priority["scored_at"],
        "storage_uri": f"research://research_intake/priority_scores/{priority['proposal_priority_score_id']}.json",
        "schema_version": priority["schema_version"],
    }
    append_jsonl(_registry_path(store, "proposal_priority_scores"), row)
    audit = _audit(store=store, actor=actor, entity_type="proposal_priority_score", entity_id=priority["proposal_priority_score_id"], action="proposal_priority_scored", new_state_hash=priority["content_hash"], reason="Scored hypothesis proposal priority for research intake queue.", metadata={"registry_row": row})
    return {"proposal_priority_score": priority, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def score_hypothesis_proposal(*, hypothesis_proposal_id: str, readiness: dict[str, Any] | None = None, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    context = proposal_context(hypothesis_proposal_id, store_root=store_root)
    actual_readiness = readiness or latest_readiness_assessment(hypothesis_proposal_id, store_root=store_root) or build_research_readiness_assessment(proposal=context["proposal"], store_root=store_root, assessed_by=actor)
    priority = build_proposal_priority_score(proposal=context["proposal"], readiness=actual_readiness, confidence_level=max_confidence(context["observations"]))
    return store_proposal_priority_score(priority, store_root=store_root, actor=actor)


def latest_priority_score(hypothesis_proposal_id: str, *, store_root: Path | None = None) -> dict[str, Any] | None:
    store = _store(store_root)
    rows = [row for row in read_jsonl(_registry_path(store, "proposal_priority_scores")) if row.get("hypothesis_proposal_id") == hypothesis_proposal_id]
    if not rows:
        return None
    row = rows[-1]
    return read_json(_artifact_path(store, "priority_scores", str(row["proposal_priority_score_id"])))


def store_research_intake_dossier(dossier: dict[str, Any], markdown: str, *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_research_intake_dossier(dossier)
    store = _store(store_root)
    path = _artifact_path(store, "dossiers", dossier["research_intake_dossier_id"])
    md_path = _artifact_path(store, "dossiers", dossier["research_intake_dossier_id"], ".md")
    write_json(path, dossier, overwrite=False)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    if md_path.exists():
        raise FileExistsError(f"dossier markdown path already exists: {md_path}")
    md_path.write_text(markdown, encoding="utf-8")
    row = {
        "research_intake_dossier_id": dossier["research_intake_dossier_id"],
        "hypothesis_proposal_id": dossier["hypothesis_proposal_id"],
        "event_family_id": dossier["event_family_id"],
        "recommended_next_action": dossier["recommended_next_action"],
        "content_hash": dossier["content_hash"],
        "created_at": dossier["created_at"],
        "storage_uri": f"research://research_intake/dossiers/{dossier['research_intake_dossier_id']}.json",
        "markdown_uri": f"research://research_intake/dossiers/{dossier['research_intake_dossier_id']}.md",
        "schema_version": dossier["schema_version"],
    }
    append_jsonl(_registry_path(store, "research_intake_dossiers"), row)
    audit = _audit(store=store, actor=actor, entity_type="research_intake_dossier", entity_id=dossier["research_intake_dossier_id"], action="research_intake_dossier_created", new_state_hash=dossier["content_hash"], reason="Created research intake dossier for human review.", metadata={"registry_row": row})
    return {"research_intake_dossier": dossier, "markdown": markdown, "registry_row": row, "audit_event": audit, "json_path": str(path), "markdown_path": str(md_path)}


def build_and_store_research_intake_dossier(*, hypothesis_proposal_id: str, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    context = proposal_context(hypothesis_proposal_id, store_root=store_root)
    readiness = latest_readiness_assessment(hypothesis_proposal_id, store_root=store_root)
    if readiness is None:
        readiness = store_research_readiness_assessment(build_research_readiness_assessment(proposal=context["proposal"], store_root=store_root, assessed_by=actor), store_root=store_root, actor=actor)["readiness_assessment"]
    priority = latest_priority_score(hypothesis_proposal_id, store_root=store_root)
    if priority is None:
        priority = store_proposal_priority_score(build_proposal_priority_score(proposal=context["proposal"], readiness=readiness, confidence_level=max_confidence(context["observations"])), store_root=store_root, actor=actor)["proposal_priority_score"]
    review = latest_hypothesis_proposal_review(hypothesis_proposal_id, store_root=store_root)
    dossier = build_research_intake_dossier(proposal=context["proposal"], readiness=readiness, priority=priority, observations=context["observations"], cluster=context["cluster"], intent=context["intent"], latest_review_decision=str((review or {}).get("review_decision") or ""))
    markdown = render_research_intake_dossier_markdown(dossier, proposal=context["proposal"], readiness=readiness, priority=priority)
    return store_research_intake_dossier(dossier, markdown, store_root=store_root, actor=actor)


def latest_research_intake_dossier(hypothesis_proposal_id: str, *, store_root: Path | None = None) -> dict[str, Any] | None:
    store = _store(store_root)
    rows = [row for row in read_jsonl(_registry_path(store, "research_intake_dossiers")) if row.get("hypothesis_proposal_id") == hypothesis_proposal_id]
    if not rows:
        return None
    return read_json(_artifact_path(store, "dossiers", str(rows[-1]["research_intake_dossier_id"])))


def latest_hypothesis_proposal_review(hypothesis_proposal_id: str, *, store_root: Path | None = None) -> dict[str, Any] | None:
    store = _store(store_root)
    rows = [row for row in read_jsonl(_registry_path(store, "hypothesis_proposal_reviews")) if row.get("hypothesis_proposal_id") == hypothesis_proposal_id]
    if not rows:
        return None
    review_id = rows[-1].get("hypothesis_proposal_review_id")
    path = _artifact_path(store, "reviews", str(review_id))
    if path.exists():
        return read_json(path)
    return rows[-1]


def store_hypothesis_proposal_review(review: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_hypothesis_proposal_review(review)
    store = _store(store_root)
    path = _artifact_path(store, "reviews", review["hypothesis_proposal_review_id"])
    write_json(path, review, overwrite=False)
    row = {
        "hypothesis_proposal_review_id": review["hypothesis_proposal_review_id"],
        "hypothesis_proposal_id": review["hypothesis_proposal_id"],
        "review_decision": review["review_decision"],
        "next_status": review["next_status"],
        "content_hash": review["content_hash"],
        "reviewed_at": review["reviewed_at"],
        "reviewed_by": review["reviewed_by"],
        "storage_uri": f"research://research_intake/reviews/{review['hypothesis_proposal_review_id']}.json",
        "schema_version": review["schema_version"],
    }
    append_jsonl(_registry_path(store, "hypothesis_proposal_reviews"), row)
    audit = _audit(store=store, actor=actor, entity_type="hypothesis_proposal_review", entity_id=review["hypothesis_proposal_review_id"], action="hypothesis_proposal_review_recorded", new_state_hash=review["content_hash"], reason="Recorded append-only hypothesis proposal review.", metadata={"registry_row": row})
    return {"hypothesis_proposal_review": review, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def review_hypothesis_proposal(*, hypothesis_proposal_id: str, decision: str, reason: str, reviewed_by: str, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    context = proposal_context(hypothesis_proposal_id, store_root=store_root)
    readiness = latest_readiness_assessment(hypothesis_proposal_id, store_root=store_root)
    if readiness is None:
        readiness = store_research_readiness_assessment(build_research_readiness_assessment(proposal=context["proposal"], store_root=store_root, assessed_by=actor), store_root=store_root, actor=actor)["readiness_assessment"]
    review = build_hypothesis_proposal_review(proposal=context["proposal"], readiness=readiness, review_decision=decision, review_reason=reason, reviewed_by=reviewed_by)
    return store_hypothesis_proposal_review(review, store_root=store_root, actor=actor)


def convert_hypothesis_proposal_to_research_plan(
    *,
    hypothesis_proposal_id: str,
    approve: bool,
    dataset_snapshot_id: str | None = None,
    universe_snapshot_id: str | None = None,
    start: str | None = None,
    end: str | None = None,
    store_root: Path | None = None,
    actor: str = "Aegis",
) -> dict[str, Any]:
    store = _store(store_root)
    context = proposal_context(hypothesis_proposal_id, store_root=store)
    proposal = context["proposal"]
    if not approve:
        _audit(store=store, actor=actor, entity_type="hypothesis_proposal", entity_id=hypothesis_proposal_id, action="hypothesis_proposal_conversion_blocked", new_state_hash=proposal["content_hash"], reason="conversion attempted without --approve true", metadata={})
        raise RuntimeError("conversion requires explicit --approve true")
    review = latest_hypothesis_proposal_review(hypothesis_proposal_id, store_root=store)
    if not review or review.get("next_status") != "accepted_for_research":
        _audit(store=store, actor=actor, entity_type="hypothesis_proposal", entity_id=hypothesis_proposal_id, action="hypothesis_proposal_conversion_blocked", new_state_hash=proposal["content_hash"], reason="conversion attempted without accepted_for_research", metadata={"latest_review": review or {}})
        raise RuntimeError("conversion attempted without accepted_for_research")
    readiness = latest_readiness_assessment(hypothesis_proposal_id, store_root=store)
    if not readiness or readiness.get("ready_for_research") is not True:
        _audit(store=store, actor=actor, entity_type="hypothesis_proposal", entity_id=hypothesis_proposal_id, action="hypothesis_proposal_conversion_blocked", new_state_hash=proposal["content_hash"], reason="conversion attempted with missing data", metadata={"readiness": readiness or {}})
        raise RuntimeError("conversion attempted with missing data")
    if not dataset_snapshot_id or not universe_snapshot_id or not start or not end:
        raise RuntimeError("dataset_snapshot_id, universe_snapshot_id, start, and end are required for conversion")
    definitions = (proposal.get("proposed_event_definition") or {}).get("default_event_definitions") or []
    if not definitions:
        raise RuntimeError("unsupported event definition")
    plan = build_research_plan(hypothesis_id=proposal["hypothesis_proposal_id"], title=proposal["title"], hypothesis=proposal["hypothesis"], dataset_snapshot_id=dataset_snapshot_id, universe_snapshot_id=universe_snapshot_id, symbols=list(proposal["proposed_universe"]), start=start, end=end, event_definition=definitions[0], forward_return_windows=list(proposal["proposed_forward_windows"]), created_by=actor)
    registry_row = store_research_plan(plan, store_root=store)
    audit = _audit(store=store, actor=actor, entity_type="hypothesis_proposal", entity_id=hypothesis_proposal_id, action="hypothesis_proposal_converted_to_research_plan", new_state_hash=plan["content_hash"], reason="Explicitly approved proposal converted to ResearchPlan only; no study, sleeve, trade, or allocation created.", metadata={"research_plan_id": plan["research_plan_id"], "registry_row": registry_row})
    return {"research_plan": plan, "registry_row": registry_row, "audit_event": audit}
