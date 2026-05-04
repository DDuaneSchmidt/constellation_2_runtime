from __future__ import annotations

from pathlib import Path
from typing import Iterable

from constellation_2.aegis_truth.evidence_event_v1 import build_event
from constellation_2.aegis_truth.evidence_ledger_v1 import append_event

DEFAULT_PROTECTED_PATHS = [
    Path("/home/node/constellation"),
    Path("/home/node/constellation_runtime_data"),
    Path("/home/node/projects/sv_portal/site"),
]


def is_protected(path: str | Path, protected_paths: Iterable[str | Path] = DEFAULT_PROTECTED_PATHS) -> bool:
    target = Path(path).expanduser().resolve()
    for protected in protected_paths:
        root = Path(protected).expanduser().resolve()
        if target == root or root in target.parents:
            return True
    return False


def mutation_event_for(
    *,
    path: str | Path,
    operation: str,
    target_day: str,
    environment: str,
    allowed: bool,
    reason: str,
) -> dict:
    event_type = "PROTECTED_MUTATION_OBSERVED" if allowed else "PROTECTED_MUTATION_BREACH_ATTEMPT"
    return build_event(
        event_type=event_type,
        producer="aegis.protected_mutation_v1",
        target_day=target_day,
        environment=environment,
        status="WARN" if allowed else "FORBIDDEN",
        blocker=None if allowed else "protected mutation attempted without explicit unlock",
        owner="protected_mutation_boundary",
        severity="WARN" if allowed else "CRITICAL",
        payload={"path": str(path), "operation": operation, "allowed": allowed, "reason": reason},
        next_action="Proceed only inside an explicit unlock window." if allowed else "Stop implementation and request explicit protected-path unlock.",
        evidence_path=str(path),
    )


def record_mutation_check(*, truth_root: str | Path, path: str | Path, operation: str, target_day: str, environment: str, unlock_token: str | None = None) -> dict:
    protected = is_protected(path)
    allowed = bool(unlock_token) or not protected
    event = mutation_event_for(path=path, operation=operation, target_day=target_day, environment=environment, allowed=allowed, reason="unlock_token_present" if unlock_token else "no_unlock_token")
    append_event(event, truth_root=truth_root)
    return event


def wrong_repo_guard(*, expected_root: str | Path, actual_root: str | Path, target_day: str, environment: str, truth_root: str | Path) -> dict | None:
    expected = Path(expected_root).resolve()
    actual = Path(actual_root).resolve()
    if expected == actual:
        return None
    event = build_event(
        event_type="CODEX_WRONG_REPO_GUARD_TRIGGERED",
        producer="aegis.protected_mutation_v1",
        target_day=target_day,
        environment=environment,
        status="FORBIDDEN",
        blocker="wrong repository for protected mutation",
        owner="protected_mutation_boundary",
        severity="CRITICAL",
        payload={"expected_root": str(expected), "actual_root": str(actual)},
        next_action="Stop implementation and switch to the expected repository/worktree.",
        evidence_path=str(actual),
    )
    append_event(event, truth_root=truth_root)
    return event
