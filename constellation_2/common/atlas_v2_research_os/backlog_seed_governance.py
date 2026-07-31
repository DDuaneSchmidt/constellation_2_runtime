from __future__ import annotations

from typing import Any

from .artifact_models import FORBIDDEN_ARTIFACT_TYPES
from .backlog_seeding_models import SEED_ITEM_TYPES

FORBIDDEN_SEED_AUTHORITY_FIELDS = {
    "live_trading_allowed",
    "live_trading_authorized",
    "broker_execution_allowed",
    "broker_execution_authorized",
    "capital_authority_allowed",
    "capital_authorized",
    "candidate_promotion_allowed",
    "candidate_promotion_authorized",
    "position_sizing_authorized",
    "portfolio_construction_authorized",
    "sleeve_deployment_authorized",
    "trade_recommendation_authorized",
    "automatic_paper_trade_placement_allowed",
}

FORBIDDEN_SEED_TEXT = {
    "trade this",
    "buy ",
    "sell ",
    "allocate capital",
    "capital allocation",
    "broker execution",
    "send broker order",
    "live trade",
    "live trading",
    "position size",
    "position sizing",
    "portfolio construction",
    "deploy sleeve",
    "sleeve deployment",
    "promote candidate",
    "production candidate promotion",
    "automatic paper trade placement",
}

ALLOWED_AUTHORITY_NOTE = "research workload generation only"


class BacklogSeedGovernanceError(ValueError):
    pass


def validate_backlog_seed_allowed(seed_item: dict[str, Any]) -> dict[str, Any]:
    violations: list[str] = []
    try:
        validate_backlog_seed_research_only(seed_item)
        validate_backlog_seed_no_authority_escalation(seed_item)
        validate_backlog_seed_no_forbidden_artifacts(seed_item)
    except BacklogSeedGovernanceError as exc:
        violations.append(str(exc))
    return {"status": "PASS" if not violations else "FAIL", "violations": violations, "warnings": []}


def validate_seed_item_governance(seed_item: dict[str, Any]) -> dict[str, Any]:
    return validate_backlog_seed_allowed(seed_item)


def validate_backlog_seed_research_only(seed_item: dict[str, Any]) -> bool:
    item_type = str(seed_item.get("item_type") or "")
    if item_type not in SEED_ITEM_TYPES:
        raise BacklogSeedGovernanceError(f"unsupported seed item_type: {item_type}")
    if str(seed_item.get("state") or "READY") not in {"READY", "BLOCKED"}:
        raise BacklogSeedGovernanceError("seeded backlog items may only start READY or BLOCKED")
    metadata = seed_item.get("metadata", {}) if isinstance(seed_item.get("metadata"), dict) else {}
    if metadata.get("research_only") is False:
        raise BacklogSeedGovernanceError("seed item must remain research_only")
    return True


def validate_backlog_seed_no_authority_escalation(seed_item: dict[str, Any]) -> bool:
    metadata = seed_item.get("metadata", {}) if isinstance(seed_item.get("metadata"), dict) else {}
    for field in FORBIDDEN_SEED_AUTHORITY_FIELDS:
        if seed_item.get(field) is True or metadata.get(field) is True:
            raise BacklogSeedGovernanceError(f"seed item cannot set authority flag: {field}")
    scan = _scan_text_payload(seed_item)
    for term in sorted(FORBIDDEN_SEED_TEXT):
        if term in scan and not _safe_denial(scan, term):
            raise BacklogSeedGovernanceError(f"seed item cannot contain authority text: {term}")
    return True


def validate_backlog_seed_no_forbidden_artifacts(seed_item: dict[str, Any]) -> bool:
    artifact_types = set(seed_item.get("artifact_types", []) or [])
    metadata = seed_item.get("metadata", {}) if isinstance(seed_item.get("metadata"), dict) else {}
    artifact_types |= set(metadata.get("artifact_types", []) or [])
    forbidden = sorted(artifact_types & FORBIDDEN_ARTIFACT_TYPES)
    if forbidden:
        raise BacklogSeedGovernanceError(f"seed item cannot create forbidden artifacts: {forbidden}")
    return True


def _scan_text_payload(seed_item: dict[str, Any]) -> str:
    metadata = seed_item.get("metadata", {}) if isinstance(seed_item.get("metadata"), dict) else {}
    safe_metadata = {k: v for k, v in metadata.items() if k not in {"limitations", "authority_boundary"}}
    return str({
        "title": seed_item.get("title", ""),
        "description": seed_item.get("description", ""),
        "metadata": safe_metadata,
    }).lower()


def _safe_denial(scan: str, term: str) -> bool:
    idx = scan.find(term)
    if idx < 0:
        return False
    context = scan[max(0, idx - 80): idx + len(term) + 80]
    return any(marker in context for marker in ["no ", "not ", "cannot", "forbidden", "without", "human-reviewed paper testing consideration"])
