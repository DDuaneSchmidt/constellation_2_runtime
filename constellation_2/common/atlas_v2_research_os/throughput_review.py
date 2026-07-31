from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifact_models import ArtifactType
from .artifact_store import ArtifactStore, DEFAULT_STORE_ROOT
from .autonomous_research_execution import _candidate_worker_types_for_backlog_item, find_compatible_worker_for_backlog_item
from .research_backlog import ResearchBacklog
from .research_effectiveness_reports import build_research_effectiveness_report
from .worker_adapters import register_default_worker_adapters

REPORT_DIRNAME = "throughput_review"


def run_throughput_repair_and_review(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Any]:
    root_path = Path(root)
    day_value = day or _today()
    effectiveness_paths = ensure_research_effectiveness_latest(root_path, day=day_value)
    cleanup = cleanup_incompatible_ready_backlog(root_path)
    seed_result = seed_compatible_funnel_work(root_path)
    report = build_throughput_review(root_path, day=day_value, cleanup=cleanup, seed_result=seed_result, effectiveness_paths=effectiveness_paths)
    paths = write_throughput_review(report, root_path, day=day_value)
    report["report_paths"] = {key: str(value) for key, value in paths.items()}
    paths["json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def ensure_research_effectiveness_latest(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, str]:
    report = build_research_effectiveness_report([])
    day_value = day or _today()
    out_root = Path(root) / "research_effectiveness"
    out_dir = out_root / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "research_effectiveness_report.json"
    summary_path = out_dir / "research_effectiveness_summary.md"
    latest_json = out_root / "latest.json"
    latest_summary = out_root / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = "\n".join([
        "# Atlas V2 Research OS Research Effectiveness",
        "",
        f"- core_question: {report['core_question']}",
        f"- activity_count: {report['activity_count']}",
        f"- certification: {report['certification']['result']}",
        "- status: INSUFFICIENT_DATA" if report['certification']['result'] == "INSUFFICIENT_DATA" else "- status: MEASURED",
        "- authority: research prioritization only; no trading, capital allocation, or candidate promotion authority",
        "",
    ])
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "latest_json": str(latest_json), "latest_summary": str(latest_summary)}


def cleanup_incompatible_ready_backlog(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    root_path = Path(root)
    backlog = ResearchBacklog(root_path)
    store = ArtifactStore(root_path)
    register_default_worker_adapters(replace=True)
    repaired: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    rows = backlog._read()
    changed = False
    for row in rows:
        missing_sources = [str(artifact_id) for artifact_id in row.get("source_artifact_ids", []) if not store.exists(str(artifact_id))]
        if missing_sources:
            original_sources = [str(artifact_id) for artifact_id in row.get("source_artifact_ids", [])]
            existing_sources = [artifact_id for artifact_id in original_sources if store.exists(artifact_id)]
            metadata = row.setdefault("metadata", {})
            metadata["missing_source_artifact_ids"] = sorted(set(list(metadata.get("missing_source_artifact_ids", [])) + missing_sources))
            metadata.setdefault("source_artifact_ids_before_missing_source_cleanup", original_sources)
            row["source_artifact_ids"] = existing_sources
            if row.get("state") != "COMPLETED":
                row["state"] = "BLOCKED"
                row["blocked_reason"] = "MISSING_SOURCE_ARTIFACT"
                row["state_transition_reason"] = "MISSING_SOURCE_ARTIFACT"
            blocked.append({"backlog_item_id": row["backlog_item_id"], "reason": "MISSING_SOURCE_ARTIFACT", "missing_source_artifact_ids": missing_sources})
            changed = True
        if row.get("state") != "READY":
            continue
        artifacts = _load_sources(store, row)
        source_types = [item.get("artifact_type") for item in artifacts]
        if row.get("item_type") == "EDGE_QUALIFICATION_REVIEW":
            hypothesis_ids = [item["artifact_id"] for item in artifacts if item.get("artifact_type") == ArtifactType.RESEARCH_HYPOTHESIS.value]
            replay_ids = [item["artifact_id"] for item in artifacts if item.get("artifact_type") == "HistoricalReplayResult"]
            metadata = row.setdefault("metadata", {})
            replay_exception = bool(metadata.get("historical_replay_unavailable") or metadata.get("historical_replay_skipped") or metadata.get("historical_replay_insufficient_data"))
            if hypothesis_ids and (replay_ids or replay_exception):
                compatible_sources = hypothesis_ids[:1] + replay_ids[:1]
                if row.get("source_artifact_ids") != compatible_sources:
                    row["source_artifact_ids"] = compatible_sources
                    metadata["compatibility_repair"] = "ROUTED_TO_RESEARCH_HYPOTHESIS_AND_REPLAY" if replay_ids else "ROUTED_TO_RESEARCH_HYPOTHESIS_WITH_REPLAY_EXCEPTION"
                    repaired.append({"backlog_item_id": row["backlog_item_id"], "reason": metadata["compatibility_repair"], "source_artifact_ids": compatible_sources})
                    changed = True
            elif hypothesis_ids:
                row["state"] = "BLOCKED"
                row["blocked_reason"] = "MISSING_HISTORICAL_REPLAY_INPUT"
                row["state_transition_reason"] = "MISSING_HISTORICAL_REPLAY_INPUT"
                blocked.append({"backlog_item_id": row["backlog_item_id"], "reason": "MISSING_HISTORICAL_REPLAY_INPUT", "source_artifact_types": source_types})
                changed = True
            else:
                row["state"] = "BLOCKED"
                row["blocked_reason"] = "MISSING_RESEARCH_HYPOTHESIS_INPUT"
                row["state_transition_reason"] = "MISSING_RESEARCH_HYPOTHESIS_INPUT"
                blocked.append({"backlog_item_id": row["backlog_item_id"], "reason": "MISSING_RESEARCH_HYPOTHESIS_INPUT", "source_artifact_types": source_types})
                changed = True
        elif row.get("item_type") == "HYPOTHESIS_VALIDATION":
            claim_ids = [item["artifact_id"] for item in artifacts if item.get("artifact_type") == ArtifactType.GENERATED_RESEARCH_CLAIM.value]
            if claim_ids:
                if row.get("source_artifact_ids") != claim_ids:
                    row["source_artifact_ids"] = claim_ids
                    row.setdefault("metadata", {})["compatibility_repair"] = "ROUTED_TO_GENERATED_RESEARCH_CLAIM_ONLY"
                    repaired.append({"backlog_item_id": row["backlog_item_id"], "reason": "HYPOTHESIS_VALIDATION_SOURCE_FILTERED", "source_artifact_ids": claim_ids})
                    changed = True
            else:
                row["state"] = "BLOCKED"
                row["blocked_reason"] = "MISSING_GENERATED_RESEARCH_CLAIM_INPUT"
                row["state_transition_reason"] = "MISSING_GENERATED_RESEARCH_CLAIM_INPUT"
                blocked.append({"backlog_item_id": row["backlog_item_id"], "reason": "MISSING_GENERATED_RESEARCH_CLAIM_INPUT", "source_artifact_types": source_types})
                changed = True
        elif row.get("item_type") in {"MECHANISM_VARIATION", "REGIME_GAP", "FAILURE_ANALYSIS"}:
            question_ids = [item["artifact_id"] for item in artifacts if item.get("artifact_type") == ArtifactType.QUESTION.value]
            if question_ids and not find_compatible_worker_for_backlog_item(row, artifacts):
                row["source_artifact_ids"] = question_ids[:1]
                row.setdefault("metadata", {})["compatibility_repair"] = "ROUTED_TO_QUESTION_ONLY"
                repaired.append({"backlog_item_id": row["backlog_item_id"], "reason": "SOURCE_FILTERED_TO_COMPATIBLE_QUESTION", "source_artifact_ids": row["source_artifact_ids"]})
                changed = True
        artifacts = _load_sources(store, row) if row.get("state") == "READY" else artifacts
        if row.get("state") == "READY" and not find_compatible_worker_for_backlog_item(row, artifacts) and row.get("item_type") != "EDGE_QUALIFICATION_REVIEW":
            reason = "NO_COMPATIBLE_CONNECTED_WORKER"
            row["state"] = "BLOCKED"
            row["blocked_reason"] = reason
            row["state_transition_reason"] = reason
            blocked.append({"backlog_item_id": row["backlog_item_id"], "reason": reason, "source_artifact_types": [item.get("artifact_type") for item in artifacts]})
            changed = True
    if changed:
        backlog._write(rows)
    return {"repaired_count": len(repaired), "blocked_count": len(blocked), "repaired": repaired, "blocked": blocked}


def seed_compatible_funnel_work(root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Any]:
    root_path = Path(root)
    store = ArtifactStore(root_path)
    backlog = ResearchBacklog(root_path)
    created: list[dict[str, str]] = []
    now = _now()
    questions = [row for row in store.list_artifacts(ArtifactType.QUESTION.value)]
    claims = [row for row in store.list_artifacts(ArtifactType.GENERATED_RESEARCH_CLAIM.value)]
    hypotheses = [row for row in store.list_artifacts(ArtifactType.RESEARCH_HYPOTHESIS.value)]
    for idx in range(10):
        artifact_id = f"q-throughput-demo-{idx+1:02d}"
        if not store.exists(artifact_id):
            store.create_artifact(
                artifact_id=artifact_id,
                artifact_type=ArtifactType.QUESTION.value,
                created_at=now,
                created_by="atlas_v2_research_os_throughput_repair",
                confidence=0.35,
                evidence_level="GENERATED_ONLY",
                labels=["generated", "demo", "throughput_seed"],
                metadata={"question": f"Generated demo research question {idx+1} for candidate funnel throughput repair.", "demo": True, "research_only": True, "mechanism_family": "MEAN_REVERSION"},
                is_root=True,
            )
        item_id = f"ci-throughput-demo-{idx+1:02d}"
        if not backlog.get(item_id, required=False):
            item = backlog.create_backlog_item(
                backlog_item_id=item_id,
                item_type="CLAIM_INVESTIGATION",
                title=f"Throughput demo claim investigation {idx+1}",
                description="Generated/demo compatible claim work for Research OS funnel repair; research only.",
                created_at=now,
                created_by="atlas_v2_research_os_throughput_repair",
                source_artifact_ids=[artifact_id],
                state="READY",
                expected_learning_value=0.45,
                novelty_score=0.1,
                evidence_gap_score=0.15,
                cost_estimate=0.05,
                mechanism_tags=["MEAN_REVERSION"],
                regime_context="UNKNOWN",
                metadata={"demo": True, "generated": True, "research_only": True},
            )
            created.append({"backlog_item_id": item["backlog_item_id"], "item_type": item["item_type"]})
    claims = [row for row in store.list_artifacts(ArtifactType.GENERATED_RESEARCH_CLAIM.value)]
    for claim in claims[:10]:
        claim_id = claim["artifact_id"]
        item_id = f"hv-throughput-{_short_id(claim_id)}"
        if _unresolved_for_source(backlog, "HYPOTHESIS_VALIDATION", claim_id) or backlog.get(item_id, required=False):
            continue
        item = backlog.create_backlog_item(
            backlog_item_id=item_id,
            item_type="HYPOTHESIS_VALIDATION",
            title=f"Throughput hypothesis from claim {claim_id}",
            description="Compatible HYPOTHESIS_VALIDATION work seeded from an existing GeneratedResearchClaim.",
            created_at=now,
            created_by="atlas_v2_research_os_throughput_repair",
            source_artifact_ids=[claim_id],
            state="READY",
            expected_learning_value=0.55,
            novelty_score=0.1,
            evidence_gap_score=0.1,
            cost_estimate=0.05,
            metadata={"research_only": True, "throughput_seed": True},
        )
        created.append({"backlog_item_id": item["backlog_item_id"], "item_type": item["item_type"]})
    hypotheses = [row for row in store.list_artifacts(ArtifactType.RESEARCH_HYPOTHESIS.value)]
    for hypothesis in hypotheses[:5]:
        hypothesis_id = hypothesis["artifact_id"]
        item_id = f"hrp-throughput-{_short_id(hypothesis_id)}"
        if _unresolved_for_source(backlog, "HISTORICAL_REPLAY_REVIEW", hypothesis_id) or backlog.get(item_id, required=False):
            continue
        item = backlog.create_backlog_item(
            backlog_item_id=item_id,
            item_type="HISTORICAL_REPLAY_REVIEW",
            title=f"Throughput historical replay for hypothesis {hypothesis_id}",
            description="Compatible HISTORICAL_REPLAY_REVIEW work seeded from an existing ResearchHypothesis before edge qualification.",
            created_at=now,
            created_by="atlas_v2_research_os_throughput_repair",
            source_artifact_ids=[hypothesis_id],
            state="READY",
            expected_learning_value=0.55,
            candidate_impact_estimate=0.25,
            evidence_gap_score=0.1,
            cost_estimate=0.05,
            metadata={"research_only": True, "throughput_seed": True},
        )
        created.append({"backlog_item_id": item["backlog_item_id"], "item_type": item["item_type"]})
    return {"created_count": len(created), "created": created}


def build_throughput_review(root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None, cleanup: dict[str, Any] | None = None, seed_result: dict[str, Any] | None = None, effectiveness_paths: dict[str, str] | None = None) -> dict[str, Any]:
    root_path = Path(root)
    backlog = ResearchBacklog(root_path)
    store = ArtifactStore(root_path)
    register_default_worker_adapters(replace=True)
    ready = backlog.get_ready_items()
    blocked = backlog.list_backlog_items(state="BLOCKED")
    compatible = []
    incompatible = []
    for item in ready:
        artifacts = _load_sources(store, item)
        if item.get("item_type") == "EDGE_QUALIFICATION_REVIEW":
            has_hypothesis = any(row.get("artifact_type") == ArtifactType.RESEARCH_HYPOTHESIS.value for row in artifacts)
            has_replay = any(row.get("artifact_type") == "HistoricalReplayResult" for row in artifacts)
            metadata = item.get("metadata", {}) or {}
            replay_exception = bool(metadata.get("historical_replay_unavailable") or metadata.get("historical_replay_skipped") or metadata.get("historical_replay_insufficient_data"))
            ok = has_hypothesis and (has_replay or replay_exception)
            worker_id = "atlas_v2_edge_qualification" if ok else ""
        else:
            worker = find_compatible_worker_for_backlog_item(item, artifacts)
            ok = worker is not None
            worker_id = getattr(worker, "worker_id", "")
        row = {"backlog_item_id": item.get("backlog_item_id"), "item_type": item.get("item_type"), "priority_score": item.get("priority_score"), "source_artifact_types": [a.get("artifact_type") for a in artifacts], "worker_id": worker_id, "candidate_worker_types": _candidate_worker_types_for_backlog_item(item)}
        (compatible if ok else incompatible).append(row)
    day_value = day or _today()
    overnight = _read_json(root_path / "overnight_review" / "latest.json", {})
    metrics = overnight.get("metrics", {}) or {}
    artifact_counts = _count_artifacts_created_on_day(store, day_value)
    latest_execution_report = _read_json(root_path / "autonomous_research" / "latest.json", {})
    latest_execution = latest_execution_report.get("execution", latest_execution_report) or {}
    latest_execution_metadata = latest_execution.get("metadata", {}) or {}
    edge_attempts = int(metrics.get("edge_qualifications_attempted") or 0)
    if latest_execution_metadata.get("edge_qualification_attempted"):
        edge_attempts = max(edge_attempts, 1)
    paper_candidates = int(metrics.get("paper_trade_candidates_created") or 0)
    if latest_execution_metadata.get("paper_trade_candidate_created"):
        paper_candidates = max(paper_candidates, 1)
    ptc = _read_json(root_path / "paper_trade_candidates" / "latest.json", {})
    if str(ptc.get("created_at") or "").startswith(day_value) and ptc.get("candidate"):
        edge_attempts = max(edge_attempts, 1)
        paper_candidates = max(paper_candidates, 1)
    queue = _read_json(root_path / "paper_trading_queue" / "latest.json", {})
    split_experiment = _read_json(root_path / "observation_cluster_split_experiment" / "latest.json", {})
    split_summary = {
        "exists": bool(split_experiment),
        "post_split_clusters": (split_experiment.get("post_split") or {}).get("clusters"),
        "post_split_claims": (split_experiment.get("post_split") or {}).get("claims"),
        "post_split_hypotheses": (split_experiment.get("post_split") or {}).get("hypotheses"),
        "post_split_eligible_candidates": (split_experiment.get("post_split") or {}).get("eligible_candidates"),
        "post_split_backtest_supported_candidates": (split_experiment.get("post_split") or {}).get("backtest_supported_candidates"),
        "post_split_paper_forward_ready_candidates": (split_experiment.get("post_split") or {}).get("paper_forward_ready_candidates"),
    }
    return {
        "schema_id": "atlas_v2_research_os_throughput_review_v1",
        "schema_version": "v1",
        "day": day_value,
        "created_at": _now(),
        "metrics": {
            "ready_backlog_count": len(ready),
            "compatible_ready_backlog_count": len(compatible),
            "compatibility_rate": round(len(compatible) / len(ready), 6) if ready else 0.0,
            "blocked_backlog_count": len(blocked),
            "claim_generation_count": max(int(metrics.get("claims_generated") or 0), artifact_counts.get(ArtifactType.GENERATED_RESEARCH_CLAIM.value, 0)),
            "hypothesis_generation_count": max(int(metrics.get("hypotheses_generated") or 0), artifact_counts.get(ArtifactType.RESEARCH_HYPOTHESIS.value, 0)),
            "experiment_spec_count": max(int(metrics.get("experiment_specs_generated") or 0), artifact_counts.get(ArtifactType.CHEAP_EXPERIMENT_SPEC.value, 0)),
            "historical_replay_count": max(int(metrics.get("historical_replays_executed") or 0), artifact_counts.get("HistoricalReplayResult", 0)),
            "edge_qualification_attempt_count": edge_attempts,
            "paper_trade_candidate_count": paper_candidates,
            "candidate_conversion_rate": 0.0,
            "worker_block_count_by_type": dict(Counter(f"{item.get('item_type')}::{item.get('blocked_reason')}" for item in blocked)),
            "top_blocked_backlog_types": dict(Counter(item.get("item_type") for item in blocked)),
            "missing_latest_reports": [] if (root_path / "research_effectiveness" / "latest.json").exists() else [{"report": "research_effectiveness", "path": str(root_path / "research_effectiveness" / "latest.json")}],
        },
        "ready_backlog_by_type": dict(Counter(item.get("item_type") for item in ready)),
        "compatible_ready_backlog_by_type": dict(Counter(item.get("item_type") for item in compatible)),
        "incompatible_ready_backlog_by_type": dict(Counter(item.get("item_type") for item in incompatible)),
        "remaining_incompatibilities": incompatible,
        "cleanup": cleanup or {},
        "seed_result": seed_result or {},
        "research_effectiveness_latest_paths": effectiveness_paths or {},
        "latest_candidate_id": (ptc.get("candidate") or {}).get("candidate_id", ""),
        "latest_candidate_eligible": (ptc.get("candidate") or {}).get("paper_trade_eligible", False),
        "paper_trading_queue_count": len(queue.get("prioritized_queue") or []),
        "observation_cluster_split_experiment": split_summary,
        "funnel_stage_counts": overnight.get("funnel_stage_counts", {}),
        "stage_share_by_run": overnight.get("stage_share_by_run", []),
        "continuation_items_created": overnight.get("continuation_items_created", []),
        "continuation_items_executed_same_session": overnight.get("continuation_items_executed_same_session", 0),
        "downstream_starvation_detected": overnight.get("downstream_starvation_detected", False),
        "balancer_selection_reasons": overnight.get("balancer_selection_reasons", []),
        "guardrails": {"live_trading_added": False, "broker_execution_added": False, "capital_authority_added": False, "candidate_promotion_added": False},
    }


def write_throughput_review(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT, *, day: str | None = None) -> dict[str, Path]:
    root_path = Path(root) / REPORT_DIRNAME
    day_value = day or str(report.get("day") or _today())
    out_dir = root_path / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "throughput_review.json"
    summary_path = out_dir / "throughput_review_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_throughput_review_summary(report)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_throughput_review_summary(report: dict[str, Any]) -> str:
    metrics = report.get("metrics", {})
    lines = [
        "# Atlas Research OS Throughput Review",
        "",
        f"Day: {report.get('day', '')}",
        f"READY backlog count: {metrics.get('ready_backlog_count', 0)}",
        f"Compatible READY backlog count: {metrics.get('compatible_ready_backlog_count', 0)}",
        f"Compatibility rate: {metrics.get('compatibility_rate', 0.0)}",
        f"Claim generation count: {metrics.get('claim_generation_count', 0)}",
        f"Hypothesis generation count: {metrics.get('hypothesis_generation_count', 0)}",
        f"Experiment spec count: {metrics.get('experiment_spec_count', 0)}",
        f"Historical replay count: {metrics.get('historical_replay_count', 0)}",
        f"Edge qualification attempts: {metrics.get('edge_qualification_attempt_count', 0)}",
        f"PaperTradeCandidate count: {metrics.get('paper_trade_candidate_count', 0)}",
        f"Blocked item count: {metrics.get('blocked_backlog_count', 0)}",
        f"Remaining incompatibilities: {len(report.get('remaining_incompatibilities', []))}",
        f"Cleanup: {json.dumps(report.get('cleanup', {}), sort_keys=True)}",
        f"Seed result: {json.dumps(report.get('seed_result', {}), sort_keys=True)}",
        "Authority: research-only; no live trading, broker execution, capital authority, candidate promotion, portfolio construction, sleeve deployment, position sizing, or automatic paper placement.",
        "",
    ]
    return "\n".join(lines)


def _load_sources(store: ArtifactStore, item: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for artifact_id in item.get("source_artifact_ids", []):
        try:
            rows.append(store.get_artifact(str(artifact_id)))
        except Exception:
            pass
    return rows


def _unresolved_for_source(backlog: ResearchBacklog, item_type: str, source_artifact_id: str) -> bool:
    return any(row.get("item_type") == item_type and row.get("state") in {"NEW", "READY", "IN_PROGRESS", "BLOCKED"} and source_artifact_id in row.get("source_artifact_ids", []) for row in backlog.list_backlog_items())



def _count_artifacts_created_on_day(store: ArtifactStore, day: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for artifact_type in (ArtifactType.GENERATED_RESEARCH_CLAIM.value, ArtifactType.RESEARCH_HYPOTHESIS.value, ArtifactType.CHEAP_EXPERIMENT_SPEC.value, "HistoricalReplayResult"):
        for artifact in store.list_artifacts(artifact_type):
            if str(artifact.get("created_at") or "").startswith(day):
                counts[artifact_type] += 1
    return dict(counts)

def _read_json(path: Path, default: Any) -> Any:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default


def _short_id(value: str) -> str:
    import hashlib
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]


def _today() -> str:
    return datetime.now(UTC).date().isoformat()


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
