from __future__ import annotations

from dataclasses import replace

from .lifecycle import validate_state, validate_transition
from .schemas import content_hash
from .store import ArtifactStore
from .tiers import validate_tier
from .types import MutationProposal


def _proposal_artifact_id(proposal: MutationProposal) -> str:
    return f"{proposal.proposal_id}__{proposal.status}__{content_hash(proposal)[:12]}"


def create_proposal(store: ArtifactStore, proposal: MutationProposal) -> str:
    validate_tier(proposal.target_tier)
    validate_state(proposal.status)
    artifact_id = _proposal_artifact_id(proposal)
    store.write_immutable("proposals", artifact_id, proposal, artifact_type="MutationProposal")
    latest = store.read_pointer("latest_proposals") or {}
    latest[proposal.proposal_id] = artifact_id
    store.write_pointer("latest_proposals", latest)
    return artifact_id


def get_latest_proposal(store: ArtifactStore, proposal_id: str) -> dict:
    latest = store.read_pointer("latest_proposals") or {}
    artifact_id = latest.get(proposal_id)
    if not artifact_id:
        raise FileNotFoundError(f"PROPOSAL_UNKNOWN:{proposal_id}")
    return store.read("proposals", artifact_id)


def transition_proposal(store: ArtifactStore, proposal_id: str, next_status: str) -> str:
    current = get_latest_proposal(store, proposal_id)["record"]
    validate_transition(current["status"], next_status)
    next_proposal = replace(MutationProposal(**current), status=next_status)
    return create_proposal(store, next_proposal)
