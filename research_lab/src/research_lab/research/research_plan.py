from __future__ import annotations

import re
from typing import Any

from research_lab.contracts.schemas import validate_contract
from research_lab.events.event_definitions import validate_event_definition
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso


RESEARCH_PLAN_SCHEMA_VERSION = "research_plan.v1"
EVENT_STUDY_RUNNER_NAME = "event_study_v1"
EVENT_STUDY_RUNNER_VERSION = "event_study_v1.0"


def slugify(value: str, *, max_length: int = 48) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
    return (slug or "research_plan")[:max_length].strip("_")


def recompute_research_plan_hash(plan: dict[str, Any]) -> str:
    return content_hash(plan, exclude={"research_plan_id", "created_at", "content_hash"}, sort_lists=False)


def build_research_plan(
    *,
    hypothesis_id: str,
    title: str,
    dataset_snapshot_id: str,
    universe_snapshot_id: str,
    symbols: list[str],
    start: str,
    end: str,
    event_definition: dict[str, Any],
    forward_return_windows: list[int],
    hypothesis: str | None = None,
    filters: dict[str, Any] | None = None,
    success_criteria: dict[str, Any] | None = None,
    created_by: str = "Aegis",
    created_at: str | None = None,
) -> dict[str, Any]:
    validate_event_definition(event_definition)
    cleaned_symbols = sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})
    cleaned_windows = sorted({int(window) for window in forward_return_windows})
    if not cleaned_symbols:
        raise ValueError("ResearchPlan requires at least one symbol")
    if not cleaned_windows or any(window <= 0 for window in cleaned_windows):
        raise ValueError("ResearchPlan requires positive forward return windows")

    body: dict[str, Any] = {
        "hypothesis_id": hypothesis_id,
        "title": title,
        "hypothesis": hypothesis or title,
        "dataset_snapshot_id": dataset_snapshot_id,
        "universe_snapshot_id": universe_snapshot_id,
        "symbols": cleaned_symbols,
        "date_range": {"start": start, "end": end},
        "event_definition": event_definition,
        "forward_return_windows": cleaned_windows,
        "filters": filters or {},
        "success_criteria": success_criteria or {},
        "runner_name": EVENT_STUDY_RUNNER_NAME,
        "runner_version": EVENT_STUDY_RUNNER_VERSION,
        "created_at": created_at or utc_now_iso(),
        "created_by": created_by,
        "schema_version": RESEARCH_PLAN_SCHEMA_VERSION,
    }
    plan_hash = recompute_research_plan_hash(body)
    body["research_plan_id"] = f"rp_{slugify(hypothesis_id)}_{short_hash(plan_hash, 10)}"
    body["content_hash"] = plan_hash
    validate_contract("research_plan", body)
    return body


def validate_research_plan(plan: dict[str, Any]) -> None:
    validate_contract("research_plan", plan)
    validate_event_definition(plan["event_definition"])
    if plan["runner_name"] != EVENT_STUDY_RUNNER_NAME:
        raise ValueError(f"Unsupported runner_name: {plan['runner_name']}")
    expected = plan["content_hash"]
    actual = recompute_research_plan_hash(plan)
    if expected != actual:
        raise ValueError(f"ResearchPlan content_hash mismatch: expected {expected}, got {actual}")

