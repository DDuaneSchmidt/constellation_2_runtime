from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

ENV_KEY = "AEGIS_RUN_CONTEXT_JSON"
DEFAULT_MAX_RECURSION_DEPTH = 2


def now_utc_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class RunContext:
    run_id: str
    command_name: str
    started_at: str
    allow_market_data_refresh: bool = True
    allow_self_heal: bool = True
    allow_projection_rebuild: bool = True
    parent_run_id: str = ""
    recursion_depth: int = 0
    visited_steps: tuple[str, ...] = field(default_factory=tuple)
    max_recursion_depth: int = DEFAULT_MAX_RECURSION_DEPTH

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "command_name": self.command_name,
            "started_at": self.started_at,
            "allow_market_data_refresh": self.allow_market_data_refresh,
            "allow_self_heal": self.allow_self_heal,
            "allow_projection_rebuild": self.allow_projection_rebuild,
            "parent_run_id": self.parent_run_id,
            "recursion_depth": self.recursion_depth,
            "visited_steps": list(self.visited_steps),
            "max_recursion_depth": self.max_recursion_depth,
        }

    def env(self) -> dict[str, str]:
        return {ENV_KEY: json.dumps(self.to_dict(), sort_keys=True)}


def _bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def run_context_from_env_v1(command_name: str, *, default_allow_market_data_refresh: bool = True, default_allow_self_heal: bool = True, default_allow_projection_rebuild: bool = True) -> RunContext:
    raw = os.environ.get(ENV_KEY, "")
    if raw:
        try:
            data = json.loads(raw)
        except Exception:
            data = {}
        if isinstance(data, dict) and data.get("run_id"):
            return RunContext(
                run_id=str(data.get("run_id") or f"aegis-run:{uuid.uuid4().hex}"),
                command_name=command_name or str(data.get("command_name") or "unknown"),
                started_at=str(data.get("started_at") or now_utc_v1()),
                allow_market_data_refresh=_bool(data.get("allow_market_data_refresh"), default_allow_market_data_refresh),
                allow_self_heal=_bool(data.get("allow_self_heal"), default_allow_self_heal),
                allow_projection_rebuild=_bool(data.get("allow_projection_rebuild"), default_allow_projection_rebuild),
                parent_run_id=str(data.get("parent_run_id") or ""),
                recursion_depth=int(data.get("recursion_depth") or 0),
                visited_steps=tuple(str(item) for item in data.get("visited_steps", []) if str(item)),
                max_recursion_depth=int(data.get("max_recursion_depth") or DEFAULT_MAX_RECURSION_DEPTH),
            )
    return RunContext(
        run_id=f"aegis-run:{uuid.uuid4().hex}",
        command_name=command_name,
        started_at=now_utc_v1(),
        allow_market_data_refresh=default_allow_market_data_refresh,
        allow_self_heal=default_allow_self_heal,
        allow_projection_rebuild=default_allow_projection_rebuild,
    )


def child_run_context_v1(parent: RunContext, command_name: str, *, allow_market_data_refresh: bool | None = None, allow_self_heal: bool | None = None, allow_projection_rebuild: bool | None = None, add_step: str = "") -> RunContext:
    visited = list(parent.visited_steps)
    if add_step and add_step not in visited:
        visited.append(add_step)
    return RunContext(
        run_id=f"aegis-run:{uuid.uuid4().hex}",
        command_name=command_name,
        started_at=now_utc_v1(),
        allow_market_data_refresh=parent.allow_market_data_refresh if allow_market_data_refresh is None else bool(allow_market_data_refresh),
        allow_self_heal=parent.allow_self_heal if allow_self_heal is None else bool(allow_self_heal),
        allow_projection_rebuild=parent.allow_projection_rebuild if allow_projection_rebuild is None else bool(allow_projection_rebuild),
        parent_run_id=parent.run_id,
        recursion_depth=parent.recursion_depth + 1,
        visited_steps=tuple(visited),
        max_recursion_depth=parent.max_recursion_depth,
    )


def step_allowed_v1(context: RunContext, step: str) -> tuple[bool, str]:
    if context.recursion_depth > context.max_recursion_depth:
        return False, "MAX_RECURSION_DEPTH_EXCEEDED"
    if step in context.visited_steps:
        return False, "STEP_ALREADY_VISITED"
    if step == "market_data_refresh" and not context.allow_market_data_refresh:
        return False, "MARKET_DATA_REFRESH_NOT_ALLOWED"
    if step == "self_heal" and not context.allow_self_heal:
        return False, "SELF_HEAL_NOT_ALLOWED"
    if step == "projection_rebuild" and not context.allow_projection_rebuild:
        return False, "PROJECTION_REBUILD_NOT_ALLOWED"
    return True, "ALLOWED"


def subprocess_env_v1(context: RunContext) -> dict[str, str]:
    env = dict(os.environ)
    env.update(context.env())
    return env
