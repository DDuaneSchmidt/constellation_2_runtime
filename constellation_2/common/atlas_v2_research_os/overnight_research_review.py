from __future__ import annotations

import importlib.util
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_models import ArtifactType
from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .autonomous_research_execution import dry_run_bounded_research, run_bounded_research_once
from .funnel_progression_balancer import select_next_funnel_balanced_item
from .funnel_progression_models import FunnelBalancingPolicy, stage_group_for_item_type
from .autonomous_research_models import AutonomousResearchExecutionStatus
from .research_backlog import ResearchBacklog

PROFILE_NAME = "OVERNIGHT_RESEARCH_REVIEW"
REPORT_DIRNAME = "overnight_review"

COMPLETED_STATUSES = {
    AutonomousResearchExecutionStatus.COMPLETED.value,
    AutonomousResearchExecutionStatus.DRY_RUN_COMPLETED.value,
    AutonomousResearchExecutionStatus.SKIPPED_NO_READY_BACKLOG.value,
    AutonomousResearchExecutionStatus.SKIPPED_NO_COMPATIBLE_WORKER.value,
}
FAILURE_STATUSES = {
    AutonomousResearchExecutionStatus.FAILED.value,
    AutonomousResearchExecutionStatus.FAILED_SAFETY_GATE.value,
}


@dataclass(frozen=True)
class OvernightResearchReviewProfile:
    name: str = PROFILE_NAME
    cadence_minutes: int = 30
    max_runs: int = 16
    max_backlog_items_per_run: int = 3
    max_worker_executions_per_run: int = 3
    max_artifacts_per_run: int = 10
    max_total_artifacts_per_session: int = 50
    stop_on_governance_failure: bool = True
    stop_on_certification_failure: bool = True
    stop_on_forbidden_artifact_attempt: bool = True
    stop_on_consecutive_failures: int = 2
    research_only: bool = True
    paper_trade_candidate_generation_allowed: bool = False
    live_trading_allowed: bool = False
    capital_authority_allowed: bool = False
    broker_execution_allowed: bool = False
    candidate_promotion_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def default_overnight_research_review_profile(root: str | Path = DEFAULT_STORE_ROOT) -> OvernightResearchReviewProfile:
    return OvernightResearchReviewProfile(
        paper_trade_candidate_generation_allowed=builds_013_015_present(root),
    )


def run_overnight_research_review(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    day: str | None = None,
    dry_run: bool = False,
    profile: OvernightResearchReviewProfile | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    profile_value = profile or default_overnight_research_review_profile(root_path)
    _validate_profile(profile_value, root_path)
    started_at = now_utc()
    runs: list[dict[str, Any]] = []
    stop_reason = ""
    consecutive_failures = 0
    total_artifacts = 0
    stage_counts_so_far: dict[str, int] = {}
    balancer_policy = FunnelBalancingPolicy()
    for run_index in range(1, profile_value.max_runs + 1):
        if total_artifacts >= profile_value.max_total_artifacts_per_session:
            stop_reason = "MAX_TOTAL_ARTIFACTS_PER_SESSION_REACHED"
            break
        ready_items = ResearchBacklog(root_path).get_ready_items()
        selection = select_next_funnel_balanced_item(ready_items, runs_completed=len(runs), runs_remaining=profile_value.max_runs - len(runs), stage_counts_so_far=stage_counts_so_far, profile=profile_value, root=root_path, policy=balancer_policy)
        preferred_id = (selection.selected_backlog_item or {}).get("backlog_item_id") if selection.selected_backlog_item else None
        result = dry_run_bounded_research(root_path, day=day, preferred_backlog_item_id=preferred_id) if dry_run else run_bounded_research_once(root_path, day=day, preferred_backlog_item_id=preferred_id)
        result.setdefault("metadata", {})["funnel_balancer"] = selection.to_dict()
        artifacts_created = list(result.get("output_artifact_ids", []))
        total_artifacts += len(artifacts_created)
        run_summary = _summarize_run(root_path, run_index, result, artifacts_created)
        runs.append(run_summary)
        if run_summary.get("funnel_stage_group"):
            stage_counts_so_far[run_summary["funnel_stage_group"]] = stage_counts_so_far.get(run_summary["funnel_stage_group"], 0) + 1
        status = str(result.get("status") or "")
        consecutive_failures = consecutive_failures + 1 if status in FAILURE_STATUSES else 0
        stop_reason = _stop_reason(profile_value, result, len(artifacts_created), total_artifacts, consecutive_failures)
        if stop_reason:
            break
        if dry_run:
            stop_reason = "DRY_RUN_PREVIEW_COMPLETED"
            break
        if status == AutonomousResearchExecutionStatus.SKIPPED_NO_READY_BACKLOG.value:
            stop_reason = "NO_READY_BACKLOG"
            break
    completed_at = now_utc()
    report = _build_report(
        root_path,
        profile=profile_value,
        dry_run=dry_run,
        day=day or day_from_timestamp(started_at),
        started_at=started_at,
        completed_at=completed_at,
        runs=runs,
        stop_reason=stop_reason or "MAX_RUNS_REACHED",
    )
    write_overnight_research_review_report(report, root_path)
    return report


def dry_run_overnight_research_review(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Any]:
    return run_overnight_research_review(root, day=day, dry_run=True)


def build_overnight_research_summary(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Any]:
    day_value = day or today_utc()
    path = overnight_review_root(root) / day_value / "overnight_research_review.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return _build_report(
        Path(root),
        profile=default_overnight_research_review_profile(root),
        dry_run=False,
        day=day_value,
        started_at="",
        completed_at="",
        runs=[],
        stop_reason="NO_OVERNIGHT_REVIEW_REPORT",
    )


def write_overnight_research_review_report(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    day_dir = overnight_review_root(root) / str(report["day"])
    day_dir.mkdir(parents=True, exist_ok=True)
    json_path = day_dir / "overnight_research_review.json"
    md_path = day_dir / "overnight_research_review_summary.md"
    latest_json = overnight_review_root(root) / "latest.json"
    latest_md = overnight_review_root(root) / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_overnight_research_review_summary(report)
    json_path.write_text(payload, encoding="utf-8")
    md_path.write_text(summary, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    latest_md.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": md_path, "latest_json": latest_json, "latest_summary": latest_md}


def render_overnight_research_review_summary(report: dict[str, Any]) -> str:
    metrics = report.get("metrics", {})
    return "\n".join(
        [
            "# Atlas V2 Research OS Overnight Research Review",
            "",
            f"Profile: {report.get('profile_name', PROFILE_NAME)}",
            f"Day: {report.get('day', '')}",
            f"Dry run: {report.get('dry_run', False)}",
            f"Runs attempted: {metrics.get('runs_attempted', 0)}",
            f"Runs completed: {metrics.get('runs_completed', 0)}",
            f"Backlog items processed: {metrics.get('backlog_items_processed', 0)}",
            f"Claims generated: {metrics.get('claims_generated', 0)}",
            f"Hypotheses generated: {metrics.get('hypotheses_generated', 0)}",
            f"Experiment specs generated: {metrics.get('experiment_specs_generated', 0)}",
            f"Historical replays executed: {metrics.get('historical_replays_executed', 0)}",
            f"Edge qualifications attempted: {metrics.get('edge_qualifications_attempted', 0)}",
            f"Paper trade candidates created: {metrics.get('paper_trade_candidates_created', 0)}",
            f"Rejected candidates: {metrics.get('rejected_candidates', 0)}",
            f"Funnel stage counts: {json.dumps(report.get('funnel_stage_counts', {}), sort_keys=True)}",
            f"Downstream starvation detected: {report.get('downstream_starvation_detected', False)}",
            f"Governance failures: {metrics.get('governance_failures', 0)}",
            f"Certification failures: {metrics.get('certification_failures', 0)}",
            f"Top items for human review: {json.dumps(report.get('top_items_for_human_review', []), sort_keys=True)}",
            f"Stop reason: {report.get('stop_reason', '')}",
            f"Limitations: {json.dumps(report.get('limitations', []), sort_keys=True)}",
            "",
        ]
    )


def builds_013_015_present(root: str | Path = DEFAULT_STORE_ROOT) -> bool:
    return _build_markers_present(root) or _paper_candidate_pipeline_modules_and_governance_present()


def _build_markers_present(root: str | Path = DEFAULT_STORE_ROOT) -> bool:
    root_path = Path(root)
    store = ArtifactStore(root_path)
    present = set()
    for build_id in ("013", "014", "015"):
        marker_paths = [
            root_path / "builds" / f"build_{build_id}.json",
            root_path / "builds" / f"{build_id}.json",
            root_path / "builds" / f"{build_id}.present",
            root_path / "builds" / f"BUILD_{build_id}.present",
        ]
        if any(path.exists() for path in marker_paths):
            present.add(build_id)
    for artifact in store.list_artifacts():
        metadata = artifact.get("metadata", {}) or {}
        build_id = str(metadata.get("build_id") or metadata.get("build") or "")
        if build_id in {"013", "014", "015", "BUILD_013", "BUILD_014", "BUILD_015"}:
            present.add(build_id[-3:])
    return present == {"013", "014", "015"}


def _paper_candidate_pipeline_modules_and_governance_present() -> bool:
    module_names = [
        "constellation_2.common.atlas_v2_research_os.edge_qualification",
        "constellation_2.common.atlas_v2_research_os.paper_trade_candidate_models",
        "constellation_2.common.atlas_v2_research_os.paper_trading_queue",
        "constellation_2.common.atlas_v2_research_os.paper_trade_outcomes",
        "constellation_2.common.atlas_v2_research_os.candidate_survival_analytics",
    ]
    if not all(importlib.util.find_spec(module_name) is not None for module_name in module_names):
        return False
    return _paper_candidate_pipeline_governance_passes()


def _paper_candidate_pipeline_governance_passes() -> bool:
    try:
        from .paper_trade_candidate_governance import validate_candidate_governance
        from .paper_trading_queue_governance import validate_paper_trading_queue_allowed
        from .paper_trade_outcome_governance import validate_paper_trade_outcome_allowed

        validate_candidate_governance({
            "artifact_type": "PaperTradeCandidate",
            "paper_trade_eligible": True,
            "human_review_required": True,
            "authority_level": "HUMAN_REVIEWED_PAPER_TESTING_CONSIDERATION",
            "forbidden_artifacts": False,
            "metadata": {},
        })
        validate_paper_trading_queue_allowed({"metadata": {"artifact_types": []}, "recommendation": "Review paper-trading queue item."})
        validate_paper_trade_outcome_allowed({"metadata": {"influence_target": "RESEARCH_MEMORY", "recommendation": "Record paper outcome for research review."}})
    except Exception:
        return False
    return True


def overnight_review_root(root: str | Path = DEFAULT_STORE_ROOT) -> Path:
    path = Path(root) / REPORT_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def _validate_profile(profile: OvernightResearchReviewProfile, root: Path) -> None:
    if profile.name != PROFILE_NAME:
        raise ValueError(f"overnight profile name must be {PROFILE_NAME}")
    positive_fields = [
        "cadence_minutes",
        "max_runs",
        "max_backlog_items_per_run",
        "max_worker_executions_per_run",
        "max_artifacts_per_run",
        "max_total_artifacts_per_session",
        "stop_on_consecutive_failures",
    ]
    for field_name in positive_fields:
        if int(getattr(profile, field_name)) <= 0:
            raise ValueError(f"{field_name} must be positive")
    if not profile.research_only:
        raise ValueError("overnight research review must remain research_only")
    if profile.live_trading_allowed or profile.capital_authority_allowed or profile.broker_execution_allowed or profile.candidate_promotion_allowed:
        raise ValueError("overnight research review cannot allow live trading, capital authority, broker execution, or candidate promotion")
    if profile.paper_trade_candidate_generation_allowed and not builds_013_015_present(root):
        raise ValueError("paper trade candidate generation requires Builds 013-015")


def _summarize_run(root: Path, run_index: int, result: dict[str, Any], artifacts_created: list[str]) -> dict[str, Any]:
    balancer = ((result.get("metadata", {}) or {}).get("funnel_balancer", {}) or {})
    selected_item = balancer.get("selected_backlog_item") or {}
    stage_group = balancer.get("stage_group") or stage_group_for_item_type(str(selected_item.get("item_type") or ""))
    continuation_created = [update for update in result.get("backlog_updates", []) if str(update.get("state_transition_reason", "")).endswith(("HYPOTHESIS_VALIDATION", "EDGE_QUALIFICATION_REVIEW"))]
    return {
        "run_index": run_index,
        "execution_id": result.get("execution_id", ""),
        "status": result.get("status", ""),
        "selected_backlog_item_id": result.get("selected_backlog_item_id", ""),
        "selected_worker_id": result.get("selected_worker_id", ""),
        "artifacts_created": artifacts_created,
        "artifact_counts": _artifact_counts(root, result),
        "governance_status": (result.get("governance_result", {}) or {}).get("status", "UNKNOWN"),
        "certification_status": (result.get("certification_result", {}) or {}).get("status", "UNKNOWN"),
        "errors": list(result.get("errors", [])),
        "warnings": list(result.get("warnings", [])),
        "funnel_stage_group": stage_group,
        "funnel_selection_reason": balancer.get("selection_reason", ""),
        "downstream_starvation_detected": bool(balancer.get("downstream_starvation_detected", False)),
        "continuation_items_created": continuation_created,
        "executed_same_session_continuation": _executed_same_session_continuation(selected_item),
    }


def _executed_same_session_continuation(selected_item: dict[str, Any]) -> bool:
    metadata = selected_item.get("metadata", {}) or {}
    return bool(metadata.get("same_session_eligible") and metadata.get("priority_boost_reason") == "FUNNEL_CONTINUATION")


def _funnel_stage_counts(runs: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for run in runs:
        stage = str(run.get("funnel_stage_group") or "UNKNOWN")
        counts[stage] = counts.get(stage, 0) + 1
    return counts


def _stage_share_by_run(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    rows = []
    for run in runs:
        stage = str(run.get("funnel_stage_group") or "UNKNOWN")
        counts[stage] = counts.get(stage, 0) + 1
        total = sum(counts.values())
        rows.append({"run_index": run.get("run_index"), "stage_group": stage, "stage_shares": {key: round(value / total, 6) for key, value in counts.items()}})
    return rows


def _continuation_items_created(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for run in runs:
        for item in run.get("continuation_items_created", []) or []:
            rows.append({"run_index": run.get("run_index"), **item})
    return rows


def _artifact_counts(root: Path, result: dict[str, Any]) -> dict[str, int]:
    counts = {
        "claims_generated": 0,
        "hypotheses_generated": 0,
        "experiment_specs_generated": 0,
        "historical_replays_executed": 0,
        "edge_qualifications_attempted": 0,
        "paper_trade_candidates_created": 0,
        "rejected_candidates": 0,
    }
    worker_result = ((result.get("metadata", {}) or {}).get("worker_result", {}) or {})
    preview = ((result.get("metadata", {}) or {}).get("dry_run_preview_output_artifact_ids", []) or [])
    for artifact in ((worker_result.get("metadata", {}) or {}).get("dry_run_output_preview", []) or []):
        _increment_artifact_count(counts, str(artifact.get("artifact_type") or ""))
    if preview and not any(counts.values()):
        worker_id = str(result.get("selected_worker_id") or "")
        if "claim_worker" in worker_id:
            counts["claims_generated"] += len(preview)
        elif "hypothesis_worker" in worker_id:
            counts["hypotheses_generated"] += len(preview)
        elif "experiment_design_worker" in worker_id:
            counts["experiment_specs_generated"] += len(preview)
    metadata = result.get("metadata", {}) or {}
    if metadata.get("historical_replay_attempted"):
        counts["historical_replays_executed"] += 1
    if metadata.get("edge_qualification_attempted"):
        counts["edge_qualifications_attempted"] += 1
    if metadata.get("paper_trade_candidate_created"):
        counts["paper_trade_candidates_created"] += 1
    elif metadata.get("edge_qualification_attempted") and metadata.get("paper_trade_candidate"):
        counts["rejected_candidates"] += 1
    store = ArtifactStore(root)
    for artifact_id in result.get("output_artifact_ids", []):
        try:
            artifact = store.get_artifact(str(artifact_id))
        except Exception:
            continue
        _increment_artifact_count(counts, str(artifact.get("artifact_type") or ""))
    return counts


def _increment_artifact_count(counts: dict[str, int], artifact_type: str) -> None:
    if artifact_type == ArtifactType.GENERATED_RESEARCH_CLAIM.value:
        counts["claims_generated"] += 1
    elif artifact_type == ArtifactType.RESEARCH_HYPOTHESIS.value:
        counts["hypotheses_generated"] += 1
    elif artifact_type == ArtifactType.CHEAP_EXPERIMENT_SPEC.value:
        counts["experiment_specs_generated"] += 1
    elif artifact_type == "EdgeQualification":
        counts["edge_qualifications_attempted"] += 1
    elif artifact_type == "PaperTradeCandidate":
        counts["paper_trade_candidates_created"] += 1
    elif artifact_type == "RejectedCandidate":
        counts["rejected_candidates"] += 1


def _stop_reason(
    profile: OvernightResearchReviewProfile,
    result: dict[str, Any],
    artifacts_created: int,
    total_artifacts: int,
    consecutive_failures: int,
) -> str:
    governance_status = (result.get("governance_result", {}) or {}).get("status", "")
    certification_status = (result.get("certification_result", {}) or {}).get("status", "")
    errors = list(result.get("errors", []))
    if profile.stop_on_governance_failure and governance_status == "FAIL":
        return "GOVERNANCE_FAILURE"
    if profile.stop_on_certification_failure and certification_status in {"FAIL", "BLOCKED"}:
        return "CERTIFICATION_FAILURE"
    if profile.stop_on_forbidden_artifact_attempt and any("forbidden" in str(error).lower() for error in errors):
        return "FORBIDDEN_ARTIFACT_ATTEMPT"
    if artifacts_created > profile.max_artifacts_per_run:
        return "MAX_ARTIFACTS_PER_RUN_EXCEEDED"
    if total_artifacts >= profile.max_total_artifacts_per_session:
        return "MAX_TOTAL_ARTIFACTS_PER_SESSION_REACHED"
    if consecutive_failures >= profile.stop_on_consecutive_failures:
        return "CONSECUTIVE_FAILURE_LIMIT_REACHED"
    return ""


def _build_report(
    root: Path,
    *,
    profile: OvernightResearchReviewProfile,
    dry_run: bool,
    day: str,
    started_at: str,
    completed_at: str,
    runs: list[dict[str, Any]],
    stop_reason: str,
) -> dict[str, Any]:
    metrics = _metrics(runs)
    return {
        "schema_id": "atlas_v2_research_os_overnight_research_review_v1",
        "schema_version": "v1",
        "profile_name": profile.name,
        "profile": profile.to_dict(),
        "day": day,
        "started_at": started_at,
        "completed_at": completed_at,
        "dry_run": dry_run,
        "runs": runs,
        "metrics": metrics,
        "funnel_stage_counts": _funnel_stage_counts(runs),
        "stage_share_by_run": _stage_share_by_run(runs),
        "continuation_items_created": _continuation_items_created(runs),
        "continuation_items_executed_same_session": sum(1 for run in runs if run.get("executed_same_session_continuation")),
        "downstream_starvation_detected": any(run.get("downstream_starvation_detected") for run in runs),
        "balancer_selection_reasons": [run.get("funnel_selection_reason", "") for run in runs if run.get("funnel_selection_reason")],
        "funnel_balancing_policy": FunnelBalancingPolicy().to_dict(),
        "stop_reason": stop_reason,
        "top_items_for_human_review": _top_items_for_human_review(root, runs),
        "limitations": [
            "Research-only overnight review workload profile.",
            "No live trading, capital allocation, broker execution, candidate production promotion, sleeve deployment, portfolio construction, or position sizing.",
            "Cadence is recorded as workload policy; the CLI executes bounded review passes synchronously and does not run a daemon.",
            "Paper trade candidate generation is fail-closed unless Builds 013-015 modules and governance checks are present or explicit local build markers exist.",
        ],
    }


def _metrics(runs: list[dict[str, Any]]) -> dict[str, int]:
    metrics = {
        "runs_attempted": len(runs),
        "runs_completed": sum(1 for run in runs if run.get("status") in COMPLETED_STATUSES),
        "backlog_items_processed": sum(1 for run in runs if run.get("selected_backlog_item_id")),
        "claims_generated": 0,
        "hypotheses_generated": 0,
        "experiment_specs_generated": 0,
        "historical_replays_executed": 0,
        "edge_qualifications_attempted": 0,
        "paper_trade_candidates_created": 0,
        "rejected_candidates": 0,
        "governance_failures": sum(1 for run in runs if run.get("governance_status") == "FAIL"),
        "certification_failures": sum(1 for run in runs if run.get("certification_status") in {"FAIL", "BLOCKED"}),
    }
    for run in runs:
        for key, value in (run.get("artifact_counts", {}) or {}).items():
            if key in metrics:
                metrics[key] += int(value)
    return metrics


def _top_items_for_human_review(root: Path, runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    backlog = ResearchBacklog(root)
    selected_ids = {str(run.get("selected_backlog_item_id") or "") for run in runs if run.get("selected_backlog_item_id")}
    candidates = []
    for item in backlog._read():
        if item.get("state") in {"BLOCKED", "READY"} or item.get("backlog_item_id") in selected_ids:
            candidates.append(
                {
                    "backlog_item_id": item.get("backlog_item_id", ""),
                    "state": item.get("state", ""),
                    "title": item.get("title", ""),
                    "priority_score": item.get("priority_score", 0.0),
                    "blocked_reason": item.get("blocked_reason", ""),
                }
            )
    return sorted(candidates, key=lambda row: (-float(row.get("priority_score") or 0.0), str(row.get("backlog_item_id") or "")))[:10]


def day_from_timestamp(value: str) -> str:
    if len(value) >= 10 and value[4:5] == "-" and value[7:8] == "-":
        return value[:10]
    return today_utc()


def today_utc() -> str:
    return datetime.now(UTC).date().isoformat()


def now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
