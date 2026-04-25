from __future__ import annotations

from typing import Any

from .store import ArtifactStore
from .types import ProposalDiff


def _diff(before: Any, after: Any, path: str = "") -> list[dict[str, Any]]:
    if type(before) is not type(after):
        return [{"path": path or "$", "before": before, "after": after}]
    if isinstance(before, dict):
        changes: list[dict[str, Any]] = []
        all_keys = sorted(set(before) | set(after))
        for key in all_keys:
            child = f"{path}.{key}" if path else str(key)
            if key not in before:
                changes.append({"path": child, "before": None, "after": after[key]})
            elif key not in after:
                changes.append({"path": child, "before": before[key], "after": None})
            else:
                changes.extend(_diff(before[key], after[key], child))
        return changes
    if isinstance(before, list):
        if before != after:
            return [{"path": path or "$", "before": before, "after": after}]
        return []
    if before != after:
        return [{"path": path or "$", "before": before, "after": after}]
    return []


def _permission_changes(changes: list[dict[str, Any]]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    added: set[str] = set()
    removed: set[str] = set()
    for change in changes:
        path = str(change["path"]).lower()
        if "permission" not in path and "autonomy" not in path:
            continue
        before = change["before"]
        after = change["after"]
        before_values = set(before) if isinstance(before, list) else ({str(before)} if before is not None else set())
        after_values = set(after) if isinstance(after, list) else ({str(after)} if after is not None else set())
        added.update(str(item) for item in (after_values - before_values))
        removed.update(str(item) for item in (before_values - after_values))
    return tuple(sorted(added)), tuple(sorted(removed))


def _threshold_changes(changes: list[dict[str, Any]]) -> tuple[str, ...]:
    tracked = []
    for change in changes:
        path = str(change["path"]).lower()
        if any(token in path for token in ("threshold", "limit", "band", "multiplier", "approval")):
            tracked.append(change["path"])
            continue
        if isinstance(change["before"], (int, float)) or isinstance(change["after"], (int, float)):
            tracked.append(change["path"])
    return tuple(sorted(set(str(item) for item in tracked)))


def compute_proposal_diff(
    store: ArtifactStore,
    proposal_id: str,
    before: dict[str, Any],
    after: dict[str, Any],
) -> str:
    changes = _diff(before, after)
    new_permissions, removed_permissions = _permission_changes(changes)
    affected_surfaces = tuple(sorted({str(change["path"]).split(".", 1)[0] for change in changes}))
    proposal_diff = ProposalDiff(
        proposal_id=proposal_id,
        structural_diff=tuple(changes),
        semantic_diff={
            "change_count": len(changes),
            "changed_paths": [change["path"] for change in changes],
        },
        affected_decision_surfaces=affected_surfaces,
        new_permissions=new_permissions,
        removed_permissions=removed_permissions,
        changed_thresholds=_threshold_changes(changes),
    )
    store.write_immutable(
        "proposal_diffs",
        proposal_id,
        proposal_diff,
        artifact_type="ProposalDiff",
    )
    return proposal_id
