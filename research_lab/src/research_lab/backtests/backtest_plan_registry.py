from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.backtests.backtest_plan import validate_backtest_plan
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


def _registry_path(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "registries" / "backtest_plans.jsonl"


def backtest_plan_path(backtest_plan_id: str, *, store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root) / "backtest_plans" / backtest_plan_id / "backtest_plan.json"


def store_backtest_plan(plan: dict[str, Any], *, store_root: Path | None = None) -> dict[str, Any]:
    validate_backtest_plan(plan)
    store = ensure_store_layout(store_root)
    write_json(backtest_plan_path(plan["backtest_plan_id"], store_root=store), plan, overwrite=False)
    registry_row = {
        "backtest_plan_id": plan["backtest_plan_id"],
        "hypothesis_id": plan["hypothesis_id"],
        "dataset_snapshot_id": plan["dataset_snapshot_id"],
        "regime_snapshot_id": plan["regime_snapshot_id"],
        "cost_model_snapshot_id": plan["cost_model_snapshot_id"],
        "runner_name": plan["runner_name"],
        "runner_version": plan["runner_version"],
        "content_hash": plan["content_hash"],
        "created_at": plan["created_at"],
        "storage_uri": f"research://backtest_plans/{plan['backtest_plan_id']}",
        "schema_version": plan["schema_version"],
    }
    append_jsonl(_registry_path(store), registry_row)
    write_audit_event(
        actor=plan["created_by"],
        entity_type="backtest_plan",
        entity_id=plan["backtest_plan_id"],
        action="backtest_plan_created",
        new_state_hash=plan["content_hash"],
        reason="Created immutable BacktestPlan for deterministic holding-period research simulation.",
        metadata={"registry_row": registry_row},
        store_root=store,
    )
    return registry_row


def load_backtest_plan(backtest_plan_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    plan = read_json(backtest_plan_path(backtest_plan_id, store_root=store_root))
    validate_backtest_plan(plan)
    return plan


def list_backtest_plans(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    return read_jsonl(_registry_path(store_root))

