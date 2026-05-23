from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from ops.aegis.research_lab.research_store_reader import default_research_store_root
from research_lab.storage.hashing import utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_jsonl
from research_lab.storage.paths import ensure_store_layout

TRIGGER_SOURCES = {
    "USER_INITIATED",
    "SCHEDULED_RUN",
    "SYSTEM_MONITOR",
    "IMPORTED_LEGACY_STATE",
    "BACKGROUND_REFRESH",
}
RUN_STATUSES = {
    "REQUESTED",
    "QUEUED",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
    "SKIPPED",
}
SYSTEM_TRIGGER_SOURCES = {"SYSTEM_MONITOR", "IMPORTED_LEGACY_STATE", "BACKGROUND_REFRESH"}
ACTIVE_RUN_STATUSES = {"RUNNING"}
QUEUED_RUN_STATUSES = {"REQUESTED", "QUEUED"}
TERMINAL_RUN_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED", "SKIPPED"}


def _store(store_root: Path | None = None) -> Path:
    return ensure_store_layout(store_root or default_research_store_root())


def research_run_ledger_path_v1(*, store_root: Path | None = None) -> Path:
    return _store(store_root) / "registries" / "research_run_ledger.v1.jsonl"


def _stable_hash(payload: dict[str, Any]) -> str:
    comparable = {key: value for key, value in payload.items() if key != "content_hash"}
    encoded = json.dumps(comparable, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalized_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if value in (None, ""):
        return []
    return [str(value)]


def append_research_run_v1(
    *,
    hypothesis_id: str,
    trigger_source: str,
    trigger_reason: str,
    run_status: str,
    requested_at: str | None = None,
    queued_at: str | None = None,
    started_at: str | None = None,
    completed_at: str | None = None,
    active_job_id: str = "",
    input_artifact_ids: list[str] | None = None,
    output_artifact_ids: list[str] | None = None,
    evidence_artifact_ids: list[str] | None = None,
    failure_reason: str = "",
    lineage_metadata: dict[str, Any] | None = None,
    research_run_id: str | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    source = str(trigger_source or "").strip().upper()
    status = str(run_status or "").strip().upper()
    if source not in TRIGGER_SOURCES:
        raise ValueError(f"Unsupported research run trigger_source: {trigger_source}")
    if status not in RUN_STATUSES:
        raise ValueError(f"Unsupported research run status: {run_status}")

    now = requested_at or utc_now_iso()
    queued = queued_at or (now if status in QUEUED_RUN_STATUSES else "")
    started = started_at or (now if status == "RUNNING" else "")
    completed = completed_at or (now if status in TERMINAL_RUN_STATUSES else "")
    row = {
        "schema_id": "research_run_ledger.v1",
        "research_run_id": research_run_id or "",
        "hypothesis_id": str(hypothesis_id or "").strip(),
        "trigger_source": source,
        "trigger_reason": str(trigger_reason or "").strip(),
        "requested_at": str(now),
        "queued_at": str(queued or ""),
        "started_at": str(started or ""),
        "completed_at": str(completed or ""),
        "run_status": status,
        "active_job_id": str(active_job_id or ""),
        "input_artifact_ids": _normalized_list(input_artifact_ids or []),
        "output_artifact_ids": _normalized_list(output_artifact_ids or []),
        "evidence_artifact_ids": _normalized_list(evidence_artifact_ids or []),
        "failure_reason": str(failure_reason or ""),
        "lineage_metadata": {
            "schema_version": "research_run_ledger.v1",
            **(lineage_metadata or {}),
        },
    }
    if not row["hypothesis_id"]:
        raise ValueError("hypothesis_id is required for research run ledger events.")
    if not row["research_run_id"]:
        seed = {
            "hypothesis_id": row["hypothesis_id"],
            "trigger_source": row["trigger_source"],
            "run_status": row["run_status"],
            "requested_at": row["requested_at"],
            "active_job_id": row["active_job_id"],
        }
        row["research_run_id"] = f"rrun_{_stable_hash(seed)[:16]}"
    row["content_hash"] = _stable_hash(row)
    append_jsonl(research_run_ledger_path_v1(store_root=store_root), row)
    return row


def read_research_run_ledger_v1(*, store_root: Path | None = None) -> list[dict[str, Any]]:
    return read_jsonl(research_run_ledger_path_v1(store_root=store_root))


def _run_sort_key(run: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(run.get("started_at") or ""),
        str(run.get("queued_at") or ""),
        str(run.get("requested_at") or ""),
        str(run.get("completed_at") or ""),
        str(run.get("research_run_id") or ""),
    )


def _runs_for_hypothesis(runs: list[dict[str, Any]], hypothesis_id: str) -> list[dict[str, Any]]:
    target = str(hypothesis_id or "")
    return sorted([run for run in runs if str(run.get("hypothesis_id") or "") == target], key=_run_sort_key)


def latest_research_run_by_hypothesis_v1(*, store_root: Path | None = None) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for run in sorted(read_research_run_ledger_v1(store_root=store_root), key=_run_sort_key):
        hypothesis_id = str(run.get("hypothesis_id") or "")
        if hypothesis_id:
            latest[hypothesis_id] = run
    return latest


def _has_blocker(hypothesis: dict[str, Any]) -> bool:
    blocker = str(hypothesis.get("blocker_reason") or hypothesis.get("blocker_summary") or "").strip().lower()
    blocking_items = hypothesis.get("blocking_items") if isinstance(hypothesis.get("blocking_items"), list) else []
    if blocking_items:
        return True
    return bool(blocker and blocker not in {"none", "no blocker", "no blockers", "not reported"})


def _has_manual_recommendation(run: dict[str, Any], hypothesis: dict[str, Any]) -> bool:
    text = " ".join(
        [
            str(hypothesis.get("recommendation_status") or ""),
            str(hypothesis.get("capture_guidance") or ""),
            " ".join(_normalized_list(run.get("output_artifact_ids"))),
            " ".join(_normalized_list(run.get("evidence_artifact_ids"))),
        ]
    ).lower()
    return any(term in text for term in ["manual_ib", "manual ib", "capture recommendation", "recommendation ready", "ib capture"])


def _has_findings(run: dict[str, Any]) -> bool:
    return bool(_normalized_list(run.get("output_artifact_ids")) or _normalized_list(run.get("evidence_artifact_ids")))


def _legacy_monitoring_hint(hypothesis: dict[str, Any]) -> bool:
    text = " ".join(
        str(hypothesis.get(key) or "")
        for key in [
            "lifecycle_state",
            "lane",
            "proposal_status",
            "status",
            "current_status",
            "current_gate",
            "source_type",
            "source_system",
        ]
    ).lower()
    return any(term in text for term in ["researching", "validating", "running", "accepted_for_research", "governed research", "system"])


def _latest_by_status(runs: list[dict[str, Any]], statuses: set[str]) -> dict[str, Any] | None:
    matching = [run for run in runs if str(run.get("run_status") or "").upper() in statuses]
    return matching[-1] if matching else None


def _latest_user_started_run(runs: list[dict[str, Any]]) -> dict[str, Any] | None:
    matching = [run for run in runs if str(run.get("trigger_source") or "").upper() == "USER_INITIATED"]
    return matching[-1] if matching else None


def research_run_projection_for_hypothesis_v1(hypothesis: dict[str, Any], runs: list[dict[str, Any]]) -> dict[str, Any]:
    hypothesis_id = str(hypothesis.get("hypothesis_id") or hypothesis.get("hypothesis_proposal_id") or hypothesis.get("item_id") or "")
    matching_runs = _runs_for_hypothesis(runs, hypothesis_id)
    latest = matching_runs[-1] if matching_runs else {}
    active = _latest_by_status(matching_runs, ACTIVE_RUN_STATUSES)
    queued = _latest_by_status(matching_runs, QUEUED_RUN_STATUSES)
    user_started = _latest_user_started_run(matching_runs)
    source = str((active or queued or latest).get("trigger_source") or "")
    status = "Ready to Start"
    explanation = "Ready for you to start AI research."

    if active:
        status = "Researching"
        explanation = "Started by you." if str(active.get("trigger_source") or "").upper() == "USER_INITIATED" else "An active research run is in progress."
    elif queued:
        status = "Scheduled" if str(queued.get("trigger_source") or "").upper() == "SCHEDULED_RUN" else "Queued"
        explanation = "Queued for overnight research." if str(queued.get("trigger_source") or "").upper() == "SCHEDULED_RUN" else "Queued for AI research."
    elif latest and str(latest.get("run_status") or "").upper() == "SUCCEEDED" and _has_manual_recommendation(latest, hypothesis):
        status = "Recommendation Ready"
        explanation = "Manual IB capture recommendation is ready to review."
    elif latest and str(latest.get("run_status") or "").upper() == "SUCCEEDED" and _has_findings(latest):
        status = "Complete"
        explanation = "Findings ready."
    elif _has_blocker(hypothesis):
        status = "Blocked"
        explanation = str(hypothesis.get("next_action") or hypothesis.get("blocker_summary") or "Resolve the blocker before research can continue.")
    elif matching_runs and source in SYSTEM_TRIGGER_SOURCES:
        status = "Monitoring"
        explanation = "System is tracking this. You have not started AI research."
    elif _legacy_monitoring_hint(hypothesis):
        status = "Monitoring"
        explanation = "System is tracking this. You have not started AI research."

    if status == "Researching" and active:
        when = active.get("started_at") or active.get("requested_at") or ""
        if str(active.get("trigger_source") or "").upper() == "USER_INITIATED" and when:
            explanation = f"Started by you at {when}."

    primary_action = {
        "Ready to Start": "Start Research",
        "Monitoring": "Start Research",
        "Scheduled": "View Queue",
        "Queued": "View Queue",
        "Researching": "View Progress",
        "Complete": "View Findings",
        "Blocked": "View Blocker",
        "Recommendation Ready": "View Recommendation",
    }.get(status, "Start Research")
    return {
        "schema_id": "hypothesis_run_state_projection.v1",
        "hypothesis_id": hypothesis_id,
        "user_facing_status": status,
        "user_facing_explanation": explanation,
        "last_research_run": latest,
        "last_run": latest,
        "trigger_source": source,
        "started_by_user": bool(user_started),
        "user_started_run_id": str((user_started or {}).get("research_run_id") or ""),
        "active_research_run_id": str((active or {}).get("research_run_id") or ""),
        "queued_research_run_id": str((queued or {}).get("research_run_id") or ""),
        "research_run_ids": [str(run.get("research_run_id") or "") for run in matching_runs if run.get("research_run_id")],
        "run_count": len(matching_runs),
        "has_active_run": bool(active),
        "has_queued_run": bool(queued),
        "primary_action_label": primary_action,
        "content_hash": _stable_hash(
            {
                "hypothesis_id": hypothesis_id,
                "status": status,
                "latest_run_id": latest.get("research_run_id") if latest else "",
                "run_ids": [run.get("research_run_id") for run in matching_runs],
            }
        ),
    }


def record_user_initiated_research_run_v1(
    *,
    hypothesis_id: str,
    trigger_reason: str,
    active_job_id: str = "research_console.start_research_v1",
    input_artifact_ids: list[str] | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    now = utc_now_iso()
    return append_research_run_v1(
        hypothesis_id=hypothesis_id,
        trigger_source="USER_INITIATED",
        trigger_reason=trigger_reason,
        run_status="QUEUED",
        requested_at=now,
        queued_at=now,
        active_job_id=active_job_id,
        input_artifact_ids=input_artifact_ids or [],
        lineage_metadata={"created_by": "operator-ui", "queue_semantics": "async_research_job_requested"},
        store_root=store_root,
    )
