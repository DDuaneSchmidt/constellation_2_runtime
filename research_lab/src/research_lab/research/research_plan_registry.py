from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.research.research_plan import validate_research_plan
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


def _registry_path(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / "research_plans.jsonl"


def research_plan_path(research_plan_id: str, *, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "research_plans" / research_plan_id / "research_plan.json"


def store_research_plan(plan: dict[str, Any], *, store_root: Path | None = None) -> dict[str, Any]:
    validate_research_plan(plan)
    store = ensure_store_layout(store_root)
    path = research_plan_path(plan["research_plan_id"], store_root=store)
    write_json(path, plan, overwrite=False)
    registry_row = {
        "research_plan_id": plan["research_plan_id"],
        "hypothesis_id": plan["hypothesis_id"],
        "dataset_snapshot_id": plan["dataset_snapshot_id"],
        "universe_snapshot_id": plan["universe_snapshot_id"],
        "runner_name": plan["runner_name"],
        "runner_version": plan["runner_version"],
        "content_hash": plan["content_hash"],
        "created_at": plan["created_at"],
        "storage_uri": f"research://research_plans/{plan['research_plan_id']}",
        "schema_version": plan["schema_version"],
    }
    append_jsonl(_registry_path(store), registry_row)
    write_audit_event(
        actor=plan["created_by"],
        entity_type="research_plan",
        entity_id=plan["research_plan_id"],
        action="research_plan_created",
        previous_state_hash="",
        new_state_hash=plan["content_hash"],
        reason="Created immutable ResearchPlan for deterministic event study.",
        metadata={"registry_row": registry_row},
        store_root=store,
    )
    return registry_row


def load_research_plan(research_plan_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    plan = read_json(research_plan_path(research_plan_id, store_root=store_root))
    validate_research_plan(plan)
    return plan


def list_research_plans(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    return read_jsonl(_registry_path(store_root))

