from __future__ import annotations

import os
from collections import Counter
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import now_utc_v1, write_json_v1
from ops.aegis.research_lab.research_console_v1 import research_console_v1
from ops.aegis.research_lab.research_run_ledger_v1 import (
    append_research_run_v1,
    read_research_run_ledger_v1,
    research_run_ledger_path_v1,
)
from ops.aegis.research_lab.research_store_reader import default_research_store_root

REPORT_FAMILY = "aegis_research_doctor_v1"
ENABLED_VALUES = {"1", "true", "yes", "on", "enabled"}
DISABLED_VALUES = {"0", "false", "no", "off", "disabled"}


def autonomous_research_enabled_v1() -> bool:
    raw = str(os.environ.get("AEGIS_RESEARCH_AUTONOMOUS_ENABLED", "true")).strip().lower()
    if raw in DISABLED_VALUES:
        return False
    if raw in ENABLED_VALUES:
        return True
    return True


def _last_run_timestamp(run: dict[str, Any]) -> str:
    return str(run.get("started_at") or run.get("queued_at") or run.get("requested_at") or run.get("completed_at") or "")


def _hypothesis_message(row: dict[str, Any], scheduler_enabled: bool) -> tuple[str, str, str, bool]:
    status = str(row.get("user_facing_status") or "Ready to Start")
    blocker = str(row.get("blocker_reason") or row.get("blocker_summary") or "").strip()
    if blocker.lower() in {"none", "no blocker", "no blockers", "not reported"}:
        blocker = ""
    explanation = str(row.get("user_facing_explanation") or "").strip()
    hypothesis_id = str(row.get("hypothesis_id") or row.get("hypothesis_proposal_id") or row.get("item_id") or "")
    if status in {"Queued", "Scheduled"}:
        return "already_queued", "Eligible; queued for next run", "Wait for the research runner to process the queued run.", False
    if status == "Researching":
        return "researching", "Research run in progress", "Wait for findings or failure artifact.", False
    if status == "Recommendation Ready":
        return "recommendation_ready", "Findings produced; recommendation review is ready", "Review findings before any manual follow-up.", False
    if status == "Complete":
        return "completed", "Findings produced", "Review findings and decide whether follow-up research is needed.", False
    if status == "Blocked" or blocker:
        reason = blocker or explanation or "Blocked before research can continue."
        return "blocked", reason, "Resolve blocker, then rerun research doctor.", False
    if not hypothesis_id:
        return "blocked", "Missing hypothesis_id", "Repair hypothesis registry identity before queueing.", False
    if not scheduler_enabled:
        return "scheduler_disabled", "Research runner disabled", "Set AEGIS_RESEARCH_AUTONOMOUS_ENABLED=true and rerun research doctor.", False
    return "eligible", "Eligible; queued for next run", "Research Doctor will queue this hypothesis automatically.", True


def build_research_doctor_v1(
    *,
    truth_root: Path,
    day_utc: str,
    store_root: Path | None = None,
    auto_queue: bool = True,
) -> dict[str, Any]:
    store = store_root or default_research_store_root()
    scheduler_enabled = autonomous_research_enabled_v1()
    before_console = research_console_v1(store_root=store)
    before_rows = list(before_console.get("all_hypotheses") or [])
    queued_runs: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []

    for row in before_rows:
        hypothesis_id = str(row.get("hypothesis_id") or row.get("hypothesis_proposal_id") or row.get("item_id") or "")
        status = str(row.get("user_facing_status") or "Ready to Start")
        blocker_class, blocker_reason, repair_action, should_queue = _hypothesis_message(row, scheduler_enabled)
        queued = None
        if auto_queue and should_queue:
            queued = append_research_run_v1(
                hypothesis_id=hypothesis_id,
                trigger_source="SYSTEM_MONITOR",
                trigger_reason="Autonomous Research Doctor queued eligible research-only hypothesis.",
                run_status="QUEUED",
                input_artifact_ids=[hypothesis_id],
                lineage_metadata={
                    "created_by": "aegis_research_doctor_v1",
                    "day_utc": day_utc,
                    "research_only": True,
                    "broker_execution_allowed": False,
                    "live_trading_allowed": False,
                    "autonomous_trading_allowed": False,
                    "trade_advice_allowed": False,
                },
                store_root=store,
            )
            queued_runs.append(queued)
            status = "Queued"
            blocker_class = "queued"
            blocker_reason = "Eligible; queued for next run"
            repair_action = "Wait for the research runner to process the queued run."
        latest_run = queued or (row.get("last_research_run") if isinstance(row.get("last_research_run"), dict) else {})
        rows.append(
            {
                "hypothesis_id": hypothesis_id,
                "title": str(row.get("title") or hypothesis_id),
                "current_autonomous_state": status,
                "last_attempted_run": _last_run_timestamp(latest_run),
                "last_research_run_id": str(latest_run.get("research_run_id") or ""),
                "next_action": repair_action,
                "blocker_reason": blocker_reason,
                "blocker_classification": blocker_class,
                "operator_action_required": bool(row.get("operator_action_required")),
                "autonomous_engine_enabled": scheduler_enabled,
                "queued_by_doctor": bool(queued),
                "diagnostics_link": "/research-lab/hypotheses",
            }
        )

    after_console = research_console_v1(store_root=store)
    after_rows = list(after_console.get("all_hypotheses") or [])
    counts = Counter(str(row.get("user_facing_status") or "Ready to Start") for row in after_rows)
    ledger = read_research_run_ledger_v1(store_root=store)
    last_run = ledger[-1] if ledger else {}
    payload = {
        "schema_id": "aegis_research_doctor",
        "schema_version": "v1",
        "artifact_id": "aegis_research_doctor_v1",
        "generated_at": now_utc_v1(),
        "day_utc": day_utc,
        "total_hypotheses": len(after_rows),
        "ready_to_start": counts.get("Ready to Start", 0) + counts.get("Monitoring", 0),
        "currently_researching": counts.get("Researching", 0),
        "blocked": counts.get("Blocked", 0),
        "waiting": counts.get("Waiting", 0) + counts.get("Queued", 0) + counts.get("Scheduled", 0),
        "recommendations_ready": counts.get("Recommendation Ready", 0),
        "completed": counts.get("Complete", 0),
        "scheduler_enabled": scheduler_enabled,
        "autonomous_research_enabled": scheduler_enabled,
        "next_scheduled_research_run": "queued immediately by aegis:research-doctor" if queued_runs else ("on next aegis:research-doctor run" if scheduler_enabled else "not scheduled; research runner disabled"),
        "last_run_timestamp": _last_run_timestamp(last_run) or "No research run yet",
        "research_run_ledger_path": str(research_run_ledger_path_v1(store_root=store)),
        "queued_run_count": len(queued_runs),
        "hypotheses": rows,
        "safety": {
            "research_only": True,
            "broker_execution_allowed": False,
            "live_trading_allowed": False,
            "autonomous_trading_allowed": False,
            "trade_advice_allowed": False,
            "automatic_sleeve_mutation_allowed": False,
        },
    }
    return payload


def write_research_doctor_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    path = write_json_v1(out_dir / "research_doctor.v1.json", payload)
    summary = out_dir / "research_doctor.summary.txt"
    summary.write_text(render_research_doctor_summary_v1(payload), encoding="utf-8")
    return {"json": str(path), "summary": str(summary)}


def render_research_doctor_summary_v1(payload: dict[str, Any]) -> str:
    lines = [
        "AEGIS RESEARCH DOCTOR v1",
        f"day_utc: {payload.get('day_utc')}",
        f"total hypotheses: {payload.get('total_hypotheses', 0)}",
        f"ready to start: {payload.get('ready_to_start', 0)}",
        f"currently researching: {payload.get('currently_researching', 0)}",
        f"blocked: {payload.get('blocked', 0)}",
        f"waiting: {payload.get('waiting', 0)}",
        f"recommendations ready: {payload.get('recommendations_ready', 0)}",
        f"last run timestamp: {payload.get('last_run_timestamp')}",
        f"scheduler enabled: {payload.get('scheduler_enabled')}",
        f"next scheduled research run: {payload.get('next_scheduled_research_run')}",
        f"queued by doctor: {payload.get('queued_run_count', 0)}",
        "",
        "Per-hypothesis status:",
    ]
    for row in payload.get("hypotheses") or []:
        lines.append(
            f"- {row.get('hypothesis_id')}: state={row.get('current_autonomous_state')} blocker={row.get('blocker_reason')} repair={row.get('next_action')}"
        )
    lines.extend(
        [
            "",
            "Safety: research-only; no broker execution; no live trading; no autonomous trading; no trade advice.",
        ]
    )
    return "\n".join(lines)
