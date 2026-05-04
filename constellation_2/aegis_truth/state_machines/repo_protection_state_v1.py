from __future__ import annotations

from pathlib import Path
from typing import Any

from constellation_2.aegis_truth.state_machines.kernel_state_types_v1 import base_state, latest_matching, read_day_events, read_json_file, write_report

SCHEMA_VERSION = "repo_protection_state.v1"
REPORT_NAME = "repo_protection_state_v1"
FILENAME = "repo_protection_state.v1.json"
REPO_EVENT_TYPES = {"PROTECTED_PATH_UNLOCKED", "PROTECTED_PATH_RELOCKED", "PROTECTED_MUTATION_OBSERVED", "PROTECTED_MUTATION_BREACH_ATTEMPT", "CODEX_WRONG_REPO_GUARD_TRIGGERED"}
DEFAULT_STATUS_PATH = Path("/home/node/constellation_runtime_data/repo_protection_v1/status.json")


def build_repo_protection_state(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN", protection_status_path: str | Path = DEFAULT_STATUS_PATH) -> dict[str, Any]:
    root = Path(truth_root)
    local_status = root / "repo_protection_v1" / "status.json"
    effective_status_path = local_status if local_status.exists() else Path(protection_status_path)
    events = read_day_events(truth_root=root, target_day=target_day)
    repo_events = [event for event in events if event.get("event_type") in REPO_EVENT_TYPES]
    breach = latest_matching(repo_events, lambda event: event.get("event_type") in {"PROTECTED_MUTATION_BREACH_ATTEMPT", "CODEX_WRONG_REPO_GUARD_TRIGGERED"} or event.get("status") == "FORBIDDEN")
    mutation = latest_matching(repo_events, lambda event: event.get("event_type") == "PROTECTED_MUTATION_OBSERVED")
    status_payload = read_json_file(effective_status_path) or {}
    if breach is not None:
        status, blocker, severity = "BREACH_ATTEMPT", breach.get("blocker") or breach.get("event_type"), breach.get("severity", "CRITICAL")
        next_action = breach.get("next_action") or "Stop mutation and inspect protected boundary evidence."
    elif mutation is not None:
        status, blocker, severity = "MUTATION_OBSERVED", mutation.get("blocker") or "protected mutation observed", mutation.get("severity", "ERROR")
        next_action = mutation.get("next_action") or "Inspect mutation and verify authorization."
    elif status_payload.get("protected") is True or status_payload.get("status") == "PROTECTED":
        status, blocker, severity = "PROTECTED", None, "INFO"
        next_action = "No operator action required."
    elif status_payload.get("protected") is False or status_payload.get("status") == "UNPROTECTED":
        status, blocker, severity = "UNLOCKED", "canonical repo is unlocked", "WARN"
        next_action = "Relock canonical repo when source changes are complete."
    else:
        status, blocker, severity = "UNKNOWN", "repo protection status unavailable", "ERROR"
        next_action = "Run repo protection status check."
    result = base_state(schema_version=SCHEMA_VERSION, target_day=target_day, environment=environment, status=status, blocker=blocker, severity=severity, owner="repo_protection", next_action=next_action, events=repo_events[-20:], extra_paths=[effective_status_path])
    result.update({"current_protection_status": status_payload, "latest_repo_event": repo_events[-1] if repo_events else None})
    return result


def write_repo_protection_state(*, target_day: str, truth_root: str | Path, environment: str = "UNKNOWN", protection_status_path: str | Path = DEFAULT_STATUS_PATH) -> Path:
    return write_report(truth_root, REPORT_NAME, target_day, FILENAME, build_repo_protection_state(target_day=target_day, truth_root=truth_root, environment=environment, protection_status_path=protection_status_path))
