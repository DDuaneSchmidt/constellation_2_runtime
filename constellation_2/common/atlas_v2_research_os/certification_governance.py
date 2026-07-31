from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .artifact_models import EvidenceLevel, FORBIDDEN_ARTIFACT_TYPES
from .artifact_store import ArtifactStore
from .governance import validate_artifact_allowed, validate_memory_allowed
from .memory_index import list_memory_objects

FORBIDDEN_AUTHORITY_TERMS = {
    "live trade",
    "trade recommendation",
    "capital allocation",
    "sleeve deployment",
    "production candidate promotion",
    "portfolio recommendation",
    "position sizing",
    "broker execution",
    "autonomous trading",
}

FORBIDDEN_AUTHORITY_FLAGS = {
    "trading_authority",
    "capital_authority",
    "candidate_generation_authority",
    "candidate_promotion_authority",
    "portfolio_authority",
    "broker_authority",
    "live_trading_authorized",
    "capital_authorized",
    "capital_authority_allowed",
    "broker_execution_authorized",
    "broker_execution_allowed",
    "candidate_promotion_allowed",
    "candidate_promotion_authorized",
    "production_promotion_authorized",
    "position_sizing_authorized",
    "portfolio_recommendation_authorized",
}

AUTHORITY_GRANT_PATTERNS = {
    "live trade": [r"\bauthori[sz]e[sd]?\s+live\s+trade", r"\blive\s+trading\s+(?:allowed|enabled|authorized)\b"],
    "trade recommendation": [r"\btrade\s+recommendation\s+(?:allowed|enabled|authorized|created)\b"],
    "capital allocation": [r"\bcapital\s+allocation\s+(?:allowed|enabled|authorized)\b", r"\ballocate\s+capital\b"],
    "sleeve deployment": [r"\bsleeve\s+deployment\s+(?:allowed|enabled|authorized)\b", r"\bdeploy\s+sleeve\b"],
    "production candidate promotion": [r"\bproduction\s+candidate\s+promotion\s+(?:allowed|enabled|authorized)\b", r"\bpromote\s+candidate\b"],
    "portfolio recommendation": [r"\bportfolio\s+recommendation\s+(?:allowed|enabled|authorized)\b", r"\bconstruct\s+portfolio\b"],
    "position sizing": [r"\bposition\s+sizing\s+(?:allowed|enabled|authorized)\b", r"\bincrease\s+position\s+size\b"],
    "broker execution": [r"\bbroker\s+execution\s+(?:allowed|enabled|authorized)\b", r"\bsend\s+broker\s+order\b"],
    "autonomous trading": [r"\bautonomous\s+trading\s+(?:allowed|enabled|authorized)\b"],
}


def scan_authority_boundary(root: str | Path) -> tuple[bool, list[str]]:
    failures: list[str] = []
    root_path = Path(root)
    if not root_path.exists():
        return True, failures
    forbidden_type_terms = {item.lower() for item in FORBIDDEN_ARTIFACT_TYPES}
    for path in root_path.rglob("*.json"):
        raw_text = path.read_text(encoding="utf-8")
        text = raw_text.lower()
        name = path.name.lower()
        for term in sorted(forbidden_type_terms):
            if term in name:
                failures.append(f"forbidden artifact marker {term}: {path.as_posix()}")
        failures.extend(_authority_flag_failures(path, raw_text))
        failures.extend(_authority_grant_failures(path, raw_text, text))
        for term in sorted(forbidden_type_terms):
            if _contains_forbidden_artifact_marker(text, term):
                failures.append(f"forbidden artifact marker {term}: {path.as_posix()}")
    return not failures, failures

def _authority_grant_failures(path: Path, raw_text: str, lowered_text: str) -> list[str]:
    failures: list[str] = []
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError:
        for term in sorted(FORBIDDEN_AUTHORITY_TERMS):
            if _contains_authority_grant(lowered_text, term):
                failures.append(f"authority boundary grant {term}: {path.as_posix()}")
        return failures
    for key_path, value in _walk_json(payload):
        if not isinstance(value, str) or _is_diagnostic_path(key_path):
            continue
        lowered_value = value.lower()
        for term in sorted(FORBIDDEN_AUTHORITY_TERMS):
            if _contains_authority_grant(lowered_value, term):
                failures.append(f"authority boundary grant {term}: {path.as_posix()}")
    return failures


def _is_diagnostic_path(key_path: tuple[str, ...]) -> bool:
    diagnostic_keys = {
        "blockers",
        "details",
        "errors",
        "failures",
        "limitations",
        "recommended_human_review_items",
        "warnings",
    }
    return any(key.lower() in diagnostic_keys for key in key_path)


def _authority_flag_failures(path: Path, raw_text: str) -> list[str]:
    failures: list[str] = []
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError:
        return failures
    for key_path, value in _walk_json(payload):
        key_value = key_path[-1].lower() if key_path else ""
        parent_key = key_path[-2].lower() if len(key_path) > 1 else ""
        if parent_key in {"required_fields_present", "fields_present", "required_configuration_present"}:
            continue
        if key_value in FORBIDDEN_AUTHORITY_FLAGS and value is True:
            failures.append(f"authority boundary flag {key_value}=true: {path.as_posix()}")
    return failures


def _walk_json(value: Any, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], Any]]:
    rows: list[tuple[tuple[str, ...], Any]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = (*path, str(key))
            rows.append((child_path, child))
            rows.extend(_walk_json(child, child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            rows.extend(_walk_json(child, (*path, str(index))))
    return rows


def _contains_authority_grant(text: str, term: str) -> bool:
    patterns = AUTHORITY_GRANT_PATTERNS.get(term, [])
    if any(re.search(pattern, text) for pattern in patterns):
        return True
    if term in text and not _term_appears_only_in_denial_context(text, term):
        return True
    return False


def _contains_forbidden_artifact_marker(text: str, term: str) -> bool:
    patterns = [
        rf'"artifact_type"\s*:\s*"{re.escape(term)}"',
        rf'"artifact_types"\s*:\s*\[[^\]]*"{re.escape(term)}"',
        r'"forbidden_artifacts"\s*:\s*true',
    ]
    return any(re.search(pattern, text) for pattern in patterns)


def _term_appears_only_in_denial_context(text: str, term: str) -> bool:
    found = list(re.finditer(re.escape(term), text))
    if not found:
        return False
    for match in found:
        prefix = text[max(0, match.start() - 240):match.start()]
        suffix = text[match.end():match.end() + 160]
        context = f"{prefix}{term}{suffix}"
        if not _is_denial_context(context):
            return False
    return True


def _is_denial_context(context: str) -> bool:
    denial_markers = [
        "no ",
        "not ",
        "never ",
        "without ",
        "forbidden",
        "cannot",
        "can not",
        "does not",
        "do not",
        "disallow",
        "disabled",
        "false",
        "non-authoritative",
        "research-only",
        "human-reviewed paper testing",
    ]
    return any(marker in context for marker in denial_markers)


def validate_artifact_governance(store: ArtifactStore) -> tuple[bool, list[str]]:
    failures: list[str] = []
    for artifact in store.list_artifacts():
        try:
            validate_artifact_allowed(artifact)
        except Exception as exc:
            failures.append(f"{artifact.get('artifact_id', '<missing>')}: {exc}")
    return not failures, failures


def validate_memory_governance(root: str | Path) -> tuple[bool, list[str]]:
    failures: list[str] = []
    for memory in list_memory_objects(root):
        try:
            validate_memory_allowed(memory)
        except Exception as exc:
            failures.append(f"{memory.get('memory_id', '<missing>')}: {exc}")
    return not failures, failures


def validate_evidence_maturity_boundaries(store: ArtifactStore, root: str | Path) -> tuple[bool, list[str]]:
    failures: list[str] = []
    for artifact in store.list_artifacts():
        evidence = str(artifact.get("evidence_level", ""))
        metadata = artifact.get("metadata", {}) or {}
        source_levels = {str(item) for item in metadata.get("source_evidence_levels", [])}
        if evidence == EvidenceLevel.OPERATOR_APPROVED.value:
            failures.append(f"operator disposition used as artifact evidence level: {artifact.get('artifact_id')}")
        if EvidenceLevel.MOCK_ONLY.value in source_levels and evidence in {
            EvidenceLevel.HISTORICAL_REPLAY.value,
            EvidenceLevel.PAPER_FORWARD_OBSERVATION.value,
            EvidenceLevel.EXTERNALLY_VALIDATED.value,
        }:
            failures.append(f"mock-only source promoted across evidence boundary: {artifact.get('artifact_id')}")
    for memory in list_memory_objects(root):
        evidence = str(memory.get("evidence_level", ""))
        if evidence == EvidenceLevel.OPERATOR_APPROVED.value:
            failures.append(f"operator disposition used as memory evidence level: {memory.get('memory_id')}")
    return not failures, failures


def authority_boundary_metadata() -> dict[str, Any]:
    return {
        "certifies_trading": False,
        "certifies_capital": False,
        "certifies_candidate_promotion": False,
        "certifies_live_readiness": False,
        "allowed_scope": "read-only Research OS safety, governance, lineage, memory, backlog, and lifecycle certification",
    }
