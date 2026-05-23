from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.event_intake.event_cluster import build_event_cluster, validate_event_cluster
from research_lab.event_intake.event_family import build_event_family, validate_event_family
from research_lab.event_intake.event_hypothesis_templates import default_event_family_specs, intent_template
from research_lab.event_intake.event_observation import build_event_observation, validate_event_observation
from research_lab.event_intake.hypothesis_proposal import build_hypothesis_proposal, validate_hypothesis_proposal
from research_lab.event_intake.intent_candidate import build_intent_candidate, validate_intent_candidate
from research_lab.research.research_plan import build_research_plan
from research_lab.research.research_plan_registry import store_research_plan
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout

EVENT_INTAKE_ROOT = "event_intake"


def _store(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root)


def _artifact_path(store: Path, group: str, artifact_id: str) -> Path:
    return store / EVENT_INTAKE_ROOT / group / f"{artifact_id}.json"


def _registry_path(store: Path, name: str) -> Path:
    return store / "registries" / f"{name}.jsonl"


def _audit(*, store: Path, actor: str, entity_type: str, entity_id: str, action: str, new_state_hash: str, reason: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    return write_audit_event(actor=actor, entity_type=entity_type, entity_id=entity_id, action=action, previous_state_hash="", new_state_hash=new_state_hash, reason=reason, metadata=metadata or {}, store_root=store)


def store_event_family(family: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_event_family(family)
    store = _store(store_root)
    path = _artifact_path(store, "event_families", family["event_family_id"])
    if path.exists():
        existing = read_json(path)
        if existing.get("content_hash") != family["content_hash"]:
            raise FileExistsError(f"event family path already exists with different content: {path}")
    else:
        write_json(path, family, overwrite=False)
    row = {
        "event_family_id": family["event_family_id"],
        "name": family["name"],
        "content_hash": family["content_hash"],
        "storage_uri": f"research://event_intake/event_families/{family['event_family_id']}.json",
        "schema_version": family["schema_version"],
    }
    append_jsonl(_registry_path(store, "event_families"), row)
    return row


def seed_event_families(*, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    store = _store(store_root)
    rows = [store_event_family(build_event_family(spec), store_root=store, actor=actor) for spec in default_event_family_specs()]
    seed_hash = content_hash({"event_family_hashes": [row["content_hash"] for row in rows]}, sort_lists=False)
    audit = _audit(store=store, actor=actor, entity_type="event_family_registry", entity_id="default_event_families", action="event_families_seeded", new_state_hash=seed_hash, reason="Seeded default Packet 30 event families.", metadata={"event_family_count": len(rows), "registry_rows": rows})
    return {"event_families": [load_event_family(row["event_family_id"], store_root=store) for row in rows], "registry_rows": rows, "audit_event": audit}


def load_event_family(event_family_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = _store(store_root)
    path = _artifact_path(store, "event_families", str(event_family_id).strip().lower())
    if not path.exists():
        raise RuntimeError(f"unknown event family: {event_family_id}")
    family = read_json(path)
    validate_event_family(family)
    return family


def store_event_observation(observation: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_event_observation(observation)
    store = _store(store_root)
    path = _artifact_path(store, "observations", observation["event_observation_id"])
    write_json(path, observation, overwrite=False)
    row = {
        "event_observation_id": observation["event_observation_id"],
        "event_family_id": observation["event_family_id"],
        "source_type": observation["source_type"],
        "confidence_level": observation["confidence_level"],
        "research_priority": observation["research_priority"],
        "content_hash": observation["content_hash"],
        "observed_at": observation["observed_at"],
        "storage_uri": f"research://event_intake/observations/{observation['event_observation_id']}.json",
        "schema_version": observation["schema_version"],
    }
    append_jsonl(_registry_path(store, "event_observations"), row)
    audit = _audit(store=store, actor=actor, entity_type="event_observation", entity_id=observation["event_observation_id"], action="event_observation_captured", new_state_hash=observation["content_hash"], reason="Captured event observation for research intake only.", metadata={"registry_row": row})
    return {"event_observation": observation, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def capture_event_observation(*, store_root: Path | None = None, actor: str = "Aegis", **kwargs: Any) -> dict[str, Any]:
    load_event_family(kwargs["event_family_id"], store_root=store_root)
    return store_event_observation(build_event_observation(observed_by=actor, **kwargs), store_root=store_root, actor=actor)


def load_event_observation(event_observation_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = _store(store_root)
    observation = read_json(_artifact_path(store, "observations", event_observation_id))
    validate_event_observation(observation)
    return observation


def store_event_cluster(cluster: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_event_cluster(cluster)
    store = _store(store_root)
    path = _artifact_path(store, "clusters", cluster["event_cluster_id"])
    write_json(path, cluster, overwrite=False)
    row = {
        "event_cluster_id": cluster["event_cluster_id"],
        "event_family_id": cluster["event_family_id"],
        "sample_observation_count": cluster["sample_observation_count"],
        "data_requirement_status": cluster["data_requirement_status"],
        "content_hash": cluster["content_hash"],
        "created_at": cluster["created_at"],
        "storage_uri": f"research://event_intake/clusters/{cluster['event_cluster_id']}.json",
        "schema_version": cluster["schema_version"],
    }
    append_jsonl(_registry_path(store, "event_clusters"), row)
    audit = _audit(store=store, actor=actor, entity_type="event_cluster", entity_id=cluster["event_cluster_id"], action="event_cluster_created", new_state_hash=cluster["content_hash"], reason="Created event cluster for research intake only.", metadata={"registry_row": row})
    return {"event_cluster": cluster, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def create_event_cluster(*, event_family_id: str, observation_ids: list[str], cluster_title: str, cluster_description: str = "", store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    family = load_event_family(event_family_id, store_root=store_root)
    observations = [load_event_observation(item, store_root=store_root) for item in observation_ids]
    cluster = build_event_cluster(event_family=family, observations=observations, cluster_title=cluster_title, cluster_description=cluster_description)
    return store_event_cluster(cluster, store_root=store_root, actor=actor)


def load_event_cluster(event_cluster_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = _store(store_root)
    cluster = read_json(_artifact_path(store, "clusters", event_cluster_id))
    validate_event_cluster(cluster)
    return cluster


def store_intent_candidate(intent: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_intent_candidate(intent)
    store = _store(store_root)
    path = _artifact_path(store, "intents", intent["intent_candidate_id"])
    write_json(path, intent, overwrite=False)
    row = {
        "intent_candidate_id": intent["intent_candidate_id"],
        "event_cluster_id": intent["event_cluster_id"],
        "event_family_id": intent["event_family_id"],
        "candidate_edge_type": intent["candidate_edge_type"],
        "expected_fragility": intent["expected_fragility"],
        "content_hash": intent["content_hash"],
        "created_at": intent["created_at"],
        "storage_uri": f"research://event_intake/intents/{intent['intent_candidate_id']}.json",
        "schema_version": intent["schema_version"],
    }
    append_jsonl(_registry_path(store, "intent_candidates"), row)
    audit = _audit(store=store, actor=actor, entity_type="intent_candidate", entity_id=intent["intent_candidate_id"], action="intent_candidate_generated", new_state_hash=intent["content_hash"], reason="Generated intent candidate from event cluster for research intake only.", metadata={"registry_row": row})
    return {"intent_candidate": intent, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def generate_intent_candidate(*, event_cluster_id: str, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    cluster = load_event_cluster(event_cluster_id, store_root=store_root)
    template = intent_template(cluster["event_family_id"])
    intent = build_intent_candidate(event_cluster=cluster, template=template)
    return store_intent_candidate(intent, store_root=store_root, actor=actor)


def load_intent_candidate(intent_candidate_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = _store(store_root)
    intent = read_json(_artifact_path(store, "intents", intent_candidate_id))
    validate_intent_candidate(intent)
    return intent


def store_hypothesis_proposal(proposal: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_hypothesis_proposal(proposal)
    store = _store(store_root)
    path = _artifact_path(store, "hypothesis_proposals", proposal["hypothesis_proposal_id"])
    if path.exists():
        raise FileExistsError(f"proposal path already exists: {path}")
    write_json(path, proposal, overwrite=False)
    row = {
        "hypothesis_proposal_id": proposal["hypothesis_proposal_id"],
        "intent_candidate_id": proposal["intent_candidate_id"],
        "event_cluster_id": proposal.get("event_cluster_id", ""),
        "event_family_id": proposal["event_family_id"],
        "proposal_status": proposal["proposal_status"],
        "governance_classification": proposal["governance_classification"],
        "data_requirement_status": proposal["data_requirement_status"],
        "content_hash": proposal["content_hash"],
        "created_at": proposal["created_at"],
        "storage_uri": f"research://event_intake/hypothesis_proposals/{proposal['hypothesis_proposal_id']}.json",
        "schema_version": proposal["schema_version"],
    }
    append_jsonl(_registry_path(store, "hypothesis_proposals"), row)
    audit = _audit(store=store, actor=actor, entity_type="hypothesis_proposal", entity_id=proposal["hypothesis_proposal_id"], action="hypothesis_proposal_generated", new_state_hash=proposal["content_hash"], reason="Generated HypothesisProposal for human review only.", metadata={"registry_row": row, "research_only": True})
    return {"hypothesis_proposal": proposal, "registry_row": row, "audit_event": audit, "json_path": str(path)}


def generate_hypothesis_proposal(*, intent_candidate_id: str, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    intent = load_intent_candidate(intent_candidate_id, store_root=store_root)
    family = load_event_family(intent["event_family_id"], store_root=store_root)
    template = intent_template(intent["event_family_id"])
    proposal = build_hypothesis_proposal(intent_candidate=intent, event_family=family, template=template, created_by=actor)
    return store_hypothesis_proposal(proposal, store_root=store_root, actor=actor)


def load_hypothesis_proposal(hypothesis_proposal_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    store = _store(store_root)
    proposal = read_json(_artifact_path(store, "hypothesis_proposals", hypothesis_proposal_id))
    validate_hypothesis_proposal(proposal)
    return proposal


def list_hypothesis_proposals(*, status: str | None = None, store_root: Path | None = None) -> list[dict[str, Any]]:
    store = _store(store_root)
    rows = read_jsonl(_registry_path(store, "hypothesis_proposals"))
    if status:
        rows = [row for row in rows if row.get("proposal_status") == status]
    return rows


def _review_registry_path(store: Path) -> Path:
    return _registry_path(store, "hypothesis_proposal_reviews")


def latest_review_status(hypothesis_proposal_id: str, *, store_root: Path | None = None) -> str | None:
    store = _store(store_root)
    rows = [row for row in read_jsonl(_review_registry_path(store)) if row.get("hypothesis_proposal_id") == hypothesis_proposal_id]
    if not rows:
        return None
    return str(rows[-1].get("decision") or "")


def review_hypothesis_proposal(*, hypothesis_proposal_id: str, decision: str, reason: str, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    if decision not in {"proposed", "accepted_for_research", "rejected", "archived", "watchlist"}:
        raise RuntimeError(f"invalid review decision: {decision}")
    proposal = load_hypothesis_proposal(hypothesis_proposal_id, store_root=store_root)
    store = _store(store_root)
    reviewed_at = utc_now_iso()
    row = {
        "hypothesis_proposal_review_id": f"ehprev_{short_hash(content_hash({'proposal': hypothesis_proposal_id, 'decision': decision, 'reason': reason, 'reviewed_at': reviewed_at}), 16)}",
        "hypothesis_proposal_id": hypothesis_proposal_id,
        "source_proposal_status": proposal["proposal_status"],
        "decision": decision,
        "reason": str(reason),
        "reviewed_at": reviewed_at,
        "reviewed_by": actor,
        "source_proposal_hash": proposal["content_hash"],
        "schema_version": "event_hypothesis_proposal_review.v1",
        "research_label": "RESEARCH_ONLY",
    }
    row["content_hash"] = content_hash(row, exclude={"hypothesis_proposal_review_id", "reviewed_at", "content_hash"}, sort_lists=False)
    append_jsonl(_review_registry_path(store), row)
    audit = _audit(store=store, actor=actor, entity_type="hypothesis_proposal", entity_id=hypothesis_proposal_id, action="hypothesis_proposal_reviewed", new_state_hash=row["content_hash"], reason=reason, metadata={"review_row": row, "append_only": True})
    return {"hypothesis_proposal": proposal, "review_row": row, "audit_event": audit}


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
    proposal = load_hypothesis_proposal(hypothesis_proposal_id, store_root=store)
    if not approve:
        _audit(store=store, actor=actor, entity_type="hypothesis_proposal", entity_id=hypothesis_proposal_id, action="hypothesis_proposal_conversion_failed", new_state_hash=proposal["content_hash"], reason="conversion attempted without explicit approval", metadata={})
        raise RuntimeError("conversion requires explicit --approve true")
    effective_status = latest_review_status(hypothesis_proposal_id, store_root=store) or proposal["proposal_status"]
    if effective_status != "accepted_for_research":
        _audit(store=store, actor=actor, entity_type="hypothesis_proposal", entity_id=hypothesis_proposal_id, action="hypothesis_proposal_conversion_failed", new_state_hash=proposal["content_hash"], reason="conversion attempted without accepted_for_research status", metadata={"effective_status": effective_status})
        raise RuntimeError("conversion attempted without accepted_for_research status")
    if proposal["data_requirement_status"] != "available":
        _audit(store=store, actor=actor, entity_type="hypothesis_proposal", entity_id=hypothesis_proposal_id, action="hypothesis_proposal_conversion_failed", new_state_hash=proposal["content_hash"], reason="conversion attempted with unavailable required data", metadata={"data_requirement_status": proposal["data_requirement_status"]})
        raise RuntimeError("conversion attempted with unavailable required data")
    if not dataset_snapshot_id or not universe_snapshot_id or not start or not end:
        raise RuntimeError("dataset_snapshot_id, universe_snapshot_id, start, and end are required for conversion")
    definitions = proposal["proposed_event_definition"].get("default_event_definitions") or []
    if not definitions:
        raise RuntimeError("proposal has no event definition")
    plan = build_research_plan(
        hypothesis_id=proposal["hypothesis_proposal_id"],
        title=proposal["title"],
        hypothesis=proposal["hypothesis"],
        dataset_snapshot_id=dataset_snapshot_id,
        universe_snapshot_id=universe_snapshot_id,
        symbols=list(proposal["proposed_universe"]),
        start=start,
        end=end,
        event_definition=definitions[0],
        forward_return_windows=list(proposal["proposed_forward_windows"]),
        created_by=actor,
    )
    registry_row = store_research_plan(plan, store_root=store)
    audit = _audit(store=store, actor=actor, entity_type="hypothesis_proposal", entity_id=hypothesis_proposal_id, action="hypothesis_proposal_converted_to_research_plan", new_state_hash=plan["content_hash"], reason="Explicitly approved proposal converted to ResearchPlan only; no study run, sleeve, or trading action created.", metadata={"research_plan_id": plan["research_plan_id"], "registry_row": registry_row})
    return {"research_plan": plan, "registry_row": registry_row, "audit_event": audit}
