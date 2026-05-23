from __future__ import annotations

from pathlib import Path
from typing import Any

from ops.aegis.candidate_lifecycle_v1 import build_candidate_lifecycle_v1
from ops.aegis.intelligence_common_v1 import latest_json_v1, now_utc_v1, write_json_v1


REPORT_FAMILY = "aegis_research_lab_execution_loop_v1"


def build_research_lab_execution_loop_v1(*, truth_root: Path, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    lifecycle = build_candidate_lifecycle_v1(truth_root=root, day_utc=day_utc)
    challenger_path, challenger = latest_json_v1(root, "aegis_sleeve_challenger_v1", day_utc, "sleeve_challenger.v1.json")
    tasks = []
    for candidate in lifecycle.get("candidates", []):
        if candidate.get("outcome_status") == "OUTCOME_LOST" or candidate.get("operator_decision") == "IGNORED":
            tasks.append(_task(source="candidate_lifecycle", target=str(candidate.get("candidate_id")), title="Investigate candidate outcome or ignored setup"))
    for challenge in challenger.get("challenges", []) if isinstance(challenger.get("challenges"), list) else []:
        if challenge.get("recommendation") in {"INVESTIGATE", "MODIFY_RESEARCH", "SUSPENSION_REVIEW", "RETIREMENT_REVIEW", "NEEDS_MORE_EVIDENCE"}:
            tasks.append(_task(source="sleeve_challenger", target=str(challenge.get("sleeve_id")), title="Research sleeve challenge finding"))
    if not tasks:
        tasks.append(_task(source="daily_review", target="Aegis", title="Continue collecting candidate and sleeve evidence", status="NEEDS_MORE_EVIDENCE"))
    results = [
        {
            "research_result_id": f"result:{task['research_task_id']}",
            "research_task_id": task["research_task_id"],
            "status": "RESULT_REVIEW_REQUIRED" if task["lifecycle_state"] != "NEEDS_MORE_EVIDENCE" else "NEEDS_MORE_EVIDENCE",
            "evidence": task["source_evidence"],
            "assumptions": ["No automatic sleeve mutation."],
            "limitations": ["Backtest execution is not performed by this report."],
            "overfit_warning": "Review sample size before promotion.",
            "human_approval_required": True,
        }
        for task in tasks
    ]
    sleeve_reviews = [
        {
            "sleeve_review_candidate_id": f"sleeve-review:{task['target']}",
            "source_task_id": task["research_task_id"],
            "status": "SLEEVE_REVIEW_CANDIDATE",
            "human_approval_required": True,
            "automatic_promotion_allowed": False,
        }
        for task in tasks
        if task["source"] == "sleeve_challenger"
    ]
    return {
        "schema_id": "aegis_research_lab_execution_loop",
        "schema_version": "v1",
        "artifact_id": "aegis_research_lab_execution_loop_v1",
        "generated_at": now_utc_v1(),
        "day_utc": day_utc,
        "lifecycle_states_supported": [
            "IDEA_CAPTURED",
            "RESEARCH_QUEUED",
            "DATA_NEEDED",
            "BACKTEST_READY",
            "BACKTEST_RUNNING",
            "BACKTEST_COMPLETE",
            "RESULT_REVIEW_REQUIRED",
            "PAPER_TEST_CANDIDATE",
            "SLEEVE_REVIEW_CANDIDATE",
            "REJECTED",
            "RETIRED",
            "NEEDS_MORE_EVIDENCE",
        ],
        "research_tasks": tasks,
        "research_results": results,
        "sleeve_review_candidates": sleeve_reviews,
        "input_artifacts": {
            "candidate_lifecycle": str(root / "reports" / "aegis_candidate_lifecycle_v1" / day_utc / "candidate_lifecycle.v1.json"),
            "sleeve_challenger": str(challenger_path or ""),
        },
        "human_approval_required": True,
        "automatic_sleeve_activation_allowed": False,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def write_research_lab_execution_loop_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    main = write_json_v1(out_dir / "research_lab_execution_loop.v1.json", payload)
    tasks = write_json_v1(out_dir / "research_tasks.v1.json", {"schema_id": "aegis_research_tasks", "tasks": payload["research_tasks"]})
    results = write_json_v1(out_dir / "research_results.v1.json", {"schema_id": "aegis_research_results", "results": payload["research_results"]})
    reviews = write_json_v1(out_dir / "sleeve_review_candidates.v1.json", {"schema_id": "aegis_sleeve_review_candidates", "sleeve_review_candidates": payload["sleeve_review_candidates"]})
    summary = out_dir / "research_lab_execution_loop.summary.txt"
    summary.write_text(render_research_lab_summary_v1(payload), encoding="utf-8")
    return {"main": str(main), "tasks": str(tasks), "results": str(results), "sleeve_reviews": str(reviews), "summary": str(summary)}


def render_research_lab_summary_v1(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "AEGIS RESEARCH LAB EXECUTION LOOP v1",
            f"day_utc: {payload.get('day_utc')}",
            f"research_task_count: {len(payload.get('research_tasks') or [])}",
            f"research_result_count: {len(payload.get('research_results') or [])}",
            f"sleeve_review_candidate_count: {len(payload.get('sleeve_review_candidates') or [])}",
            "human_approval_required: true",
            "automatic_sleeve_activation_allowed: false",
            "",
        ]
    )


def _task(*, source: str, target: str, title: str, status: str = "RESEARCH_QUEUED") -> dict[str, Any]:
    return {
        "research_task_id": f"research-task:{source}:{target}",
        "source": source,
        "target": target,
        "title": title,
        "lifecycle_state": status,
        "source_evidence": [source],
        "human_approval_required": True,
        "automatic_change_allowed": False,
    }
