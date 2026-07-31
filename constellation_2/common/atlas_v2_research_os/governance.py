from __future__ import annotations

from pathlib import Path
from typing import Any

from .artifact_models import EvidenceLevel, FORBIDDEN_ARTIFACT_TYPES


class GovernanceError(ValueError):
    pass


def validate_artifact_allowed(artifact: dict[str, Any]) -> bool:
    artifact_type = str(artifact.get("artifact_type", ""))
    if artifact_type in FORBIDDEN_ARTIFACT_TYPES:
        raise GovernanceError(f"forbidden Research OS output: {artifact_type}")
    validate_label_integrity(artifact)
    validate_authority_boundaries(artifact)
    return True


def validate_no_forbidden_artifacts(root: str | Path) -> tuple[bool, list[str]]:
    root_path = Path(root)
    failures: list[str] = []
    if not root_path.exists():
        return True, failures
    forbidden_terms = {term.lower() for term in FORBIDDEN_ARTIFACT_TYPES} | {"livetrade", "traderecommendation", "capitalallocation", "sleevedeployment", "productioncandidatepromotion", "portfoliorecommendation", "positionsizing"}
    for path in root_path.rglob("*.json"):
        text = path.read_text(encoding="utf-8").lower()
        name = path.name.lower()
        for term in forbidden_terms:
            if term in name or f'"artifact_type": "{term}"' in text:
                failures.append(f"forbidden artifact marker {term}: {path.as_posix()}")
    return not failures, failures


def validate_label_integrity(artifact: dict[str, Any]) -> bool:
    evidence = str(artifact.get("evidence_level", ""))
    labels = {str(label) for label in artifact.get("labels", [])}
    metadata = artifact.get("metadata", {}) or {}
    source_evidence_levels = [str(item) for item in metadata.get("source_evidence_levels", [])]
    if "generated" in labels and evidence == EvidenceLevel.EXTERNALLY_VALIDATED.value:
        raise GovernanceError("generated artifacts must never be mislabeled as externally validated")
    if EvidenceLevel.GENERATED_ONLY.value in source_evidence_levels and evidence == EvidenceLevel.EXTERNALLY_VALIDATED.value:
        raise GovernanceError("generated source evidence cannot become externally validated in Research OS foundation")
    if EvidenceLevel.MOCK_ONLY.value in source_evidence_levels and evidence in {EvidenceLevel.HISTORICAL_REPLAY.value, EvidenceLevel.PAPER_FORWARD_OBSERVATION.value, EvidenceLevel.EXTERNALLY_VALIDATED.value}:
        raise GovernanceError("mock artifacts must never be promoted to historical, paper, or external evidence")
    if evidence == EvidenceLevel.OPERATOR_APPROVED.value and metadata.get("capital_approval") is True:
        raise GovernanceError("OPERATOR_APPROVED is not capital approval")
    return True


def validate_authority_boundaries(artifact: dict[str, Any]) -> bool:
    text = str(artifact).lower()
    forbidden = ["live trade", "trade recommendation", "capital allocation", "sleeve deployment", "production candidate promotion", "portfolio recommendation", "position sizing", "broker execution"]
    if any(term in text for term in forbidden):
        raise GovernanceError("Research OS foundation cannot emit trading/capital/sleeve/candidate authority")
    if artifact.get("metadata", {}).get("candidate_generation_certified") is True:
        raise GovernanceError("candidate generation influence requires explicit future certification, not this build")
    return True


def validate_research_os_write(artifact: dict[str, Any]) -> bool:
    if not artifact.get("artifact_id"):
        raise GovernanceError("Research OS artifact requires artifact_id")
    if not artifact.get("artifact_type"):
        raise GovernanceError("Research OS artifact requires artifact_type")
    if artifact.get("confidence") is None or not 0.0 <= float(artifact.get("confidence")) <= 1.0:
        raise GovernanceError("Research OS artifact confidence must be between 0 and 1")
    if str(artifact.get("evidence_level")) not in {item.value for item in EvidenceLevel}:
        raise GovernanceError(f"invalid evidence_level: {artifact.get('evidence_level')}")
    return validate_artifact_allowed(artifact)



def validate_memory_allowed(memory: dict[str, Any]) -> bool:
    from .memory_models import FORBIDDEN_MEMORY_ARTIFACT_TYPES, MemoryEvidenceMaturity, MemoryLifecycleState, MemoryType

    memory_type = str(memory.get("memory_type") or memory.get("artifact_type") or "")
    if memory_type in FORBIDDEN_MEMORY_ARTIFACT_TYPES or str(memory.get("artifact_type", "")) in FORBIDDEN_MEMORY_ARTIFACT_TYPES:
        raise GovernanceError(f"forbidden Research OS memory output: {memory_type}")
    if memory.get("artifact_type") in FORBIDDEN_MEMORY_ARTIFACT_TYPES:
        raise GovernanceError(f"forbidden Research OS memory artifact: {memory.get('artifact_type')}")
    if memory_type and memory_type not in {item.value for item in MemoryType}:
        raise GovernanceError(f"invalid memory_type: {memory_type}")
    evidence = str(memory.get("evidence_level", ""))
    if evidence and evidence not in {item.value for item in MemoryEvidenceMaturity}:
        raise GovernanceError(f"invalid memory evidence maturity: {evidence}")
    lifecycle_state = str(memory.get("lifecycle_state", ""))
    if lifecycle_state and lifecycle_state not in {item.value for item in MemoryLifecycleState}:
        raise GovernanceError(f"invalid memory lifecycle state: {lifecycle_state}")
    return validate_memory_no_authority_escalation(memory) and validate_memory_evidence_labels(memory) and validate_memory_candidate_isolation(memory)


def validate_memory_no_authority_escalation(memory: dict[str, Any]) -> bool:
    text = str(memory).lower()
    forbidden = ["live trade", "trade recommendation", "capital allocation", "sleeve deployment", "production candidate promotion", "portfolio recommendation", "position sizing", "broker execution"]
    if any(term in text for term in forbidden):
        raise GovernanceError("Research OS memory cannot emit trading/capital/sleeve/candidate authority")
    metadata = memory.get("metadata", {}) or {}
    if metadata.get("trading_authority") or metadata.get("capital_authority") or metadata.get("candidate_generation_authority"):
        raise GovernanceError("Research OS memory cannot carry authority escalation flags")
    return True


def validate_memory_evidence_labels(memory: dict[str, Any]) -> bool:
    from .memory_models import MemoryEvidenceMaturity

    evidence = str(memory.get("evidence_level", ""))
    if evidence == EvidenceLevel.OPERATOR_APPROVED.value:
        raise GovernanceError("OPERATOR_APPROVED is not memory evidence maturity")
    labels = {str(label).lower() for label in memory.get("labels", [])}
    metadata = memory.get("metadata", {}) or {}
    source_levels = {str(item) for item in metadata.get("source_evidence_levels", [])}
    if "generated_only" in labels and evidence == MemoryEvidenceMaturity.EXTERNALLY_VALIDATED.value:
        raise GovernanceError("generated-only memory must not be externally validated")
    if MemoryEvidenceMaturity.MOCK_ONLY.value in source_levels and evidence in {MemoryEvidenceMaturity.HISTORICAL_REPLAY.value, MemoryEvidenceMaturity.PAPER_FORWARD_OBSERVATION.value, MemoryEvidenceMaturity.EXTERNALLY_VALIDATED.value}:
        raise GovernanceError("mock-only memory must not be promoted to stronger evidence")
    return True


def validate_memory_candidate_isolation(memory: dict[str, Any]) -> bool:
    metadata = memory.get("metadata", {}) or {}
    if metadata.get("candidate_generation_certified") or metadata.get("can_influence_candidate_generation"):
        raise GovernanceError("memory candidate influence requires future certification and is disabled in this build")
    return True
