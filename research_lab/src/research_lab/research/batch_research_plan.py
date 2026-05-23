from __future__ import annotations

from research_lab.datasets.dataset_registry import load_dataset_snapshot
from research_lab.research.research_plan import build_research_plan


def build_dataset_wide_event_study_plan(
    *,
    hypothesis_id: str,
    title: str,
    dataset_snapshot_id: str,
    universe_snapshot_id: str,
    start: str,
    end: str,
    event_type: str,
    threshold: float,
    forward_return_windows: list[int],
    store_root=None,
    created_by: str = "Aegis",
) -> dict:
    dataset = load_dataset_snapshot(dataset_snapshot_id, store_root=store_root)
    return build_research_plan(
        hypothesis_id=hypothesis_id,
        title=title,
        hypothesis=title,
        dataset_snapshot_id=dataset_snapshot_id,
        universe_snapshot_id=universe_snapshot_id,
        symbols=list(dataset["symbols"]),
        start=start,
        end=end,
        event_definition={"type": event_type, "params": {"return_column": "adj_close", "threshold": threshold}},
        forward_return_windows=forward_return_windows,
        created_by=created_by,
    )

