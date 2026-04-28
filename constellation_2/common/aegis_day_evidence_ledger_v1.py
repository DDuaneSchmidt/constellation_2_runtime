from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


OUTCOMES = {
    "SUCCESS_DRY_RUN",
    "SUCCESS_TRANSMITTED",
    "NO_INTENT_EXPECTED",
    "BLOCKED_WITH_REASON",
    "FAILED_WITH_OWNER",
}


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def evidence_ledger_output_path(*, truth_root: Path, day_utc: str) -> Path:
    return (
        Path(truth_root).resolve()
        / "reports"
        / "aegis_day_evidence_ledger_v1"
        / day_utc
        / "aegis_day_evidence_ledger.v1.json"
    ).resolve()


def new_aegis_day_evidence_ledger_v1(*, day_utc: str, mode: str, run_style: str, started_utc: str | None = None) -> dict[str, Any]:
    return {
        "schema_id": "aegis_day_evidence_ledger",
        "schema_version": "v1",
        "day_utc": day_utc,
        "mode": mode,
        "run_style": run_style,
        "started_utc": started_utc or utc_now_iso(),
        "finished_utc": "",
        "commands": [],
        "cycles": [],
        "phases": [],
        "authority_states_before": {},
        "authority_states_after": {},
        "blockers": [],
        "diagnostics": [],
        "artifact_paths": {},
        "no_silent_day_outcome": "",
        "final_daily_outcome": "",
    }


def record_phase_v1(ledger: dict[str, Any], *, phase: str, status: str, reason: str = "") -> None:
    ledger.setdefault("phases", []).append({"phase": phase, "status": status, "reason": reason})


def record_command_v1(
    ledger: dict[str, Any],
    *,
    phase: str,
    name: str,
    command: list[str],
    start_utc: str,
    end_utc: str,
    exit_code: int,
    inputs: list[str] | None = None,
    outputs: list[str] | None = None,
    stdout: str = "",
    stderr: str = "",
) -> None:
    ledger.setdefault("commands", []).append(
        {
            "phase": phase,
            "name": name,
            "command": list(command),
            "start_utc": start_utc,
            "end_utc": end_utc,
            "exit_code": int(exit_code),
            "inputs": list(inputs or []),
            "outputs": list(outputs or []),
            "stdout_tail": str(stdout or "")[-4000:],
            "stderr_tail": str(stderr or "")[-4000:],
        }
    )


def record_cycle_v1(
    ledger: dict[str, Any],
    *,
    cycle_id: str,
    started_at_utc: str,
    ended_at_utc: str,
    market_session_state: str,
    authorities_before: dict[str, Any] | None = None,
    actions_taken: list[str] | None = None,
    submit_attempted: bool = False,
    skip_reason: str = "",
    outputs: dict[str, Any] | None = None,
    blockers: list[dict[str, Any]] | None = None,
    next_wake_at_utc: str = "",
) -> None:
    ledger.setdefault("cycles", []).append(
        {
            "cycle_id": str(cycle_id),
            "started_at_utc": str(started_at_utc),
            "ended_at_utc": str(ended_at_utc),
            "market_session_state": str(market_session_state),
            "authorities_before": dict(authorities_before or {}),
            "actions_taken": list(actions_taken or []),
            "submit_attempted": bool(submit_attempted),
            "skip_reason": str(skip_reason or ""),
            "outputs": dict(outputs or {}),
            "blockers": list(blockers or []),
            "next_wake_at_utc": str(next_wake_at_utc or ""),
        }
    )


def finalize_aegis_day_evidence_ledger_v1(
    ledger: dict[str, Any],
    *,
    final_daily_outcome: str,
    blockers: list[dict[str, Any]] | None = None,
    diagnostics: list[dict[str, Any]] | None = None,
    artifact_paths: dict[str, str] | None = None,
    authority_states_after: dict[str, str] | None = None,
    finished_utc: str | None = None,
) -> dict[str, Any]:
    if final_daily_outcome not in OUTCOMES:
        raise ValueError(f"INVALID_NO_SILENT_DAY_OUTCOME:{final_daily_outcome}")
    ledger["finished_utc"] = finished_utc or utc_now_iso()
    ledger["blockers"] = list(blockers or [])
    ledger["diagnostics"] = list(diagnostics or [])
    ledger["artifact_paths"] = dict(artifact_paths or {})
    ledger["authority_states_after"] = dict(authority_states_after or {})
    ledger["no_silent_day_outcome"] = final_daily_outcome
    ledger["final_daily_outcome"] = final_daily_outcome
    return ledger


def write_aegis_day_evidence_ledger_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> Path:
    path = evidence_ledger_output_path(truth_root=truth_root, day_utc=day_utc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return path
