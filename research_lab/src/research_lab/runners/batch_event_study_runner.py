from __future__ import annotations

from research_lab.runners.event_study_runner import run_event_study


def run_batch_event_study(*, research_plan_id: str, store_root=None, actor: str = "Aegis", allow_json_fallback: bool = False) -> dict:
    return run_event_study(
        research_plan_id=research_plan_id,
        store_root=store_root,
        actor=actor,
        allow_json_fallback=allow_json_fallback,
    )
