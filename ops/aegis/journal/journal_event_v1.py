from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

from ops.aegis.intelligence_common_v1 import latest_json_v1, read_json_v1, write_json_v1


REPORT_FAMILY = "aegis_journal_timeline_v1"
PIPELINE_FAMILY = "aegis_research_pipeline_v1"
TRIAGE_FAMILY = "aegis_research_triage_v1"
PLAN_FAMILY = "aegis_research_plan_v1"
RESULT_FAMILY = "aegis_research_test_results_v1"
REVIEW_FAMILY = "aegis_research_review_decisions_v1"

EVENT_FAMILIES = ["CANDIDATE", "EDGE", "SLEEVE", "PERFORMANCE", "RUNTIME", "GOVERNANCE", "SYSTEM"]


def build_journal_timeline_v1(*, truth_root: Path, day_utc: str, include_diagnostics: bool = False) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    events: list[dict[str, Any]] = []

    events.extend(_research_pipeline_events(root, day_utc, include_diagnostics=include_diagnostics))
    events.extend(_research_triage_events(root, day_utc))
    events.extend(_research_plan_events(root, day_utc))
    events.extend(_research_test_events(root, day_utc))
    events.extend(_research_review_events(root, day_utc))
    events.extend(_candidate_events(root, day_utc))
    events.extend(_runtime_events(root, day_utc))
    events.extend(_performance_events(root, day_utc))
    events.extend(_sleeve_events(root, day_utc))
    events.extend(_governance_events(root, day_utc))
    events.extend(_system_events(root, day_utc))

    events = _dedupe_events(events)
    events.sort(key=lambda row: (str(row.get("timestamp_utc") or ""), str(row.get("event_id") or "")), reverse=True)
    entity_index = _entity_index(events)
    summary = _timeline_summary(events)
    diagnostics = [] if include_diagnostics else _fixture_diagnostics(root, day_utc)
    return {
        "schema_id": "aegis_journal_timeline",
        "schema_version": "v1",
        "artifact_id": "aegis_journal_timeline_v1",
        "day_utc": day_utc,
        "generated_at_utc": _now(),
        "main_feed_excludes_test_fixtures": not include_diagnostics,
        "timeline_summary": summary,
        "filters": ["All", "Candidates", "Edges", "Sleeves", "Performance", "Runtime", "Governance", "System"],
        "recent_events": events,
        "entity_index": entity_index,
        "audit_drilldowns": _audit_drilldowns(events),
        "diagnostics": diagnostics,
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def write_journal_timeline_v1(*, truth_root: Path, day_utc: str, payload: dict[str, Any]) -> dict[str, str]:
    out_dir = Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / day_utc
    json_path = write_json_v1(out_dir / "journal_timeline.v1.json", payload)
    summary_path = out_dir / "journal_timeline.summary.txt"
    matrix_path = out_dir / "journal_timeline.matrix.csv"
    summary_path.write_text(render_journal_timeline_summary_v1(payload), encoding="utf-8")
    matrix_path.write_text(render_journal_timeline_matrix_csv_v1(payload), encoding="utf-8")
    return {"json": str(json_path), "summary": str(summary_path), "matrix": str(matrix_path)}


def render_journal_timeline_summary_v1(payload: dict[str, Any]) -> str:
    summary = payload.get("timeline_summary") if isinstance(payload.get("timeline_summary"), dict) else {}
    lines = [
        "AEGIS JOURNAL TIMELINE v1",
        f"day_utc: {payload.get('day_utc')}",
        f"total_events: {summary.get('total_events', 0)}",
        f"latest_event_at: {summary.get('latest_event_at', 'UNKNOWN')}",
        "family_counts:",
    ]
    for family in EVENT_FAMILIES:
        lines.append(f"- {family}: {summary.get(f'{family.lower()}_events', 0)}")
    lines.extend(
        [
            f"fixtures_excluded_from_main_feed: {payload.get('main_feed_excludes_test_fixtures') is True}",
            "broker_execution_allowed: false",
            "autonomous_execution_allowed: false",
            "",
        ]
    )
    return "\n".join(lines)


def render_journal_timeline_matrix_csv_v1(payload: dict[str, Any]) -> str:
    out = StringIO()
    fieldnames = ["timestamp_utc", "event_family", "event_type", "entity_type", "entity_id", "title", "source_artifact_path", "source_hash"]
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    for event in payload.get("recent_events") or []:
        writer.writerow({key: event.get(key, "") for key in fieldnames})
    return out.getvalue()


def _research_pipeline_events(root: Path, day_utc: str, *, include_diagnostics: bool) -> list[dict[str, Any]]:
    path, payload = latest_json_v1(root, PIPELINE_FAMILY, day_utc, "research_pipeline.v1.json")
    if not payload:
        return []
    out: list[dict[str, Any]] = []
    for item in payload.get("items") or []:
        if not isinstance(item, dict):
            continue
        classification = str(item.get("classification") or "")
        if classification == "TEST_FIXTURE" and not include_diagnostics:
            continue
        hypothesis_id = str(item.get("hypothesis_id") or "")
        if not hypothesis_id:
            continue
        title = str(item.get("readable_title") or item.get("title") or hypothesis_id)
        gate = str(item.get("current_gate") or "UNKNOWN")
        event_type = "hypothesis.classified"
        if gate == "REJECTED_ARCHIVED":
            event_type = "hypothesis.rejected" if str(item.get("gate_status") or "") == "REJECTED" else "hypothesis.archived"
        out.append(
            _event(
                event_type=event_type,
                event_family="EDGE",
                entity_type="hypothesis",
                entity_id=hypothesis_id,
                title=f"{title}",
                summary=f"Hypothesis is in {gate} with status {item.get('gate_status', 'UNKNOWN')}.",
                why_it_matters="This places the edge idea in the research pipeline without approving any sleeve change.",
                actor="Aegis",
                source_path=Path(path) if path else None,
                timestamp=item.get("generated_at_utc") or payload.get("generated_at_utc"),
                related_entities=item.get("linked_sleeve_ids") or [],
                after_state={"current_gate": gate, "gate_status": item.get("gate_status")},
            )
        )
    return out


def _research_triage_events(root: Path, day_utc: str) -> list[dict[str, Any]]:
    out = []
    for path, row in _jsonl_rows(root / "reports" / TRIAGE_FAMILY / day_utc / "research_triage.v1.jsonl"):
        decision = str(row.get("decision") or "UNKNOWN")
        hypothesis_id = str(row.get("hypothesis_id") or "")
        event_type = "hypothesis.triaged"
        if decision == "REJECT":
            event_type = "hypothesis.rejected"
        elif decision == "ARCHIVE":
            event_type = "hypothesis.archived"
        out.append(
            _event(
                event_type=event_type,
                event_family="EDGE",
                entity_type="hypothesis",
                entity_id=hypothesis_id,
                title=f"Hypothesis triage: {decision}",
                summary=str(row.get("reason") or f"Triage decision {decision} was recorded."),
                why_it_matters="Triage determines the next research gate and is append-only.",
                actor=str(row.get("operator") or "David"),
                source_path=path,
                timestamp=row.get("timestamp_utc"),
                after_state={"decision": decision},
            )
        )
    return out


def _research_plan_events(root: Path, day_utc: str) -> list[dict[str, Any]]:
    out = []
    base = root / "reports" / PLAN_FAMILY / day_utc
    for path in sorted(base.rglob("research_plan.v1.json")) if base.exists() else []:
        payload = read_json_v1(path)
        hypothesis_id = str(payload.get("hypothesis_id") or path.parent.name)
        out.append(
            _event(
                event_type="research.plan_built",
                event_family="EDGE",
                entity_type="hypothesis",
                entity_id=hypothesis_id,
                title="Research plan built",
                summary=f"{payload.get('test_type', 'UNKNOWN')} plan status: {payload.get('plan_status', 'UNKNOWN')}.",
                why_it_matters="A hypothesis cannot be tested until the plan and required evidence are explicit.",
                actor="Aegis",
                source_path=path,
                timestamp=payload.get("generated_at_utc"),
                after_state={"plan_status": payload.get("plan_status"), "test_type": payload.get("test_type")},
            )
        )
    return out


def _research_test_events(root: Path, day_utc: str) -> list[dict[str, Any]]:
    out = []
    base = root / "reports" / RESULT_FAMILY / day_utc
    for path in sorted(base.rglob("research_test_result.v1.json")) if base.exists() else []:
        payload = read_json_v1(path)
        hypothesis_id = str(payload.get("hypothesis_id") or path.parent.name)
        status = str(payload.get("test_status") or "UNKNOWN")
        out.append(
            _event(
                event_type="research.test_run",
                event_family="EDGE",
                entity_type="hypothesis",
                entity_id=hypothesis_id,
                title=f"Research test result: {status}",
                summary=str(payload.get("result_summary") or f"Research test produced {status}."),
                why_it_matters="Test output moves evidence into result review without fabricating pass/fail claims.",
                actor="Aegis",
                source_path=path,
                timestamp=payload.get("generated_at_utc"),
                after_state={"test_status": status, "sample_size": payload.get("sample_size"), "metric_status": payload.get("metric_status")},
            )
        )
    return out


def _research_review_events(root: Path, day_utc: str) -> list[dict[str, Any]]:
    out = []
    for path, row in _jsonl_rows(root / "reports" / REVIEW_FAMILY / day_utc / "research_review_decisions.v1.jsonl"):
        decision = str(row.get("normalized_decision") or row.get("decision") or "UNKNOWN")
        event_type = "research.review_decision_recorded"
        if decision == "PAPER_TEST_CANDIDATE":
            event_type = "hypothesis.promoted_to_paper_trial"
        elif decision == "SLEEVE_REVIEW_CANDIDATE":
            event_type = "hypothesis.promoted_to_sleeve_review"
        elif decision == "REJECT":
            event_type = "hypothesis.rejected"
        elif decision == "ARCHIVE":
            event_type = "hypothesis.archived"
        out.append(
            _event(
                event_type=event_type,
                event_family="EDGE",
                entity_type="hypothesis",
                entity_id=str(row.get("hypothesis_id") or ""),
                title=f"Research review decision: {decision}",
                summary=str(row.get("reason") or f"Review decision {decision} was recorded."),
                why_it_matters="Review decisions require human approval and do not automatically mutate sleeves.",
                actor=str(row.get("operator") or "David"),
                source_path=path,
                timestamp=row.get("timestamp_utc"),
                after_state={"decision": decision},
            )
        )
    return out


def _candidate_events(root: Path, day_utc: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    lifecycle_path, lifecycle = latest_json_v1(root, "aegis_candidate_lifecycle_v1", day_utc, "candidate_lifecycle.v1.json")
    for row in lifecycle.get("candidates") or [] if isinstance(lifecycle.get("candidates"), list) else []:
        candidate_id = str(row.get("candidate_id") or "")
        if not candidate_id:
            continue
        out.append(
            _event(
                event_type="candidate.generated",
                event_family="CANDIDATE",
                entity_type="candidate",
                entity_id=candidate_id,
                title=f"Candidate {candidate_id}",
                summary=f"{row.get('symbol', 'UNKNOWN')} {row.get('direction', '')} candidate state: {row.get('current_operator_decision') or row.get('operator_decision') or 'GENERATED'}.",
                why_it_matters="Candidate lifecycle records operator decisions and outcomes for attribution.",
                actor="Aegis",
                source_path=Path(lifecycle_path) if lifecycle_path else None,
                timestamp=row.get("generated_at") or row.get("generated_at_utc") or lifecycle.get("generated_at_utc"),
                related_entities=[row.get("sleeve_id")] if row.get("sleeve_id") else [],
                after_state={"decision": row.get("current_operator_decision"), "outcome": row.get("outcome_status")},
            )
        )
    for path, row in _jsonl_rows(root / "reports" / "aegis_candidate_lifecycle_v1" / day_utc / "candidate_decisions.v1.jsonl"):
        event_type = "candidate.corrected" if row.get("event_type") == "CANDIDATE_DECISION_CORRECTED" else "candidate.decision_recorded"
        out.append(
            _event(
                event_type=event_type,
                event_family="CANDIDATE",
                entity_type="candidate",
                entity_id=str(row.get("candidate_id") or ""),
                title="Candidate decision corrected" if event_type.endswith("corrected") else "Candidate decision recorded",
                summary=str(row.get("reason") or row.get("decision_reason") or row.get("decision") or event_type),
                why_it_matters="Operator decisions and corrections are append-only and feed attribution.",
                actor=str(row.get("operator") or "David"),
                source_path=path,
                timestamp=row.get("timestamp_utc") or row.get("created_at"),
                after_state={"decision": row.get("decision"), "corrected_fields": row.get("corrected_fields")},
            )
        )
    for path, row in _jsonl_rows(root / "reports" / "aegis_position_management_v1" / day_utc / "position_events.v1.jsonl"):
        raw_type = str(row.get("event_type") or "")
        event_type = {
            "POSITION_RISK_PLAN_RECORDED": "position.risk_plan_recorded",
            "STOP_EVENT_RECORDED": "position.stop_event_recorded",
            "POSITION_EVENT_CORRECTED": "position.corrected",
            "POSITION_FINAL_OUTCOME_RECORDED": "position.final_outcome_recorded",
        }.get(raw_type, "position.event_recorded")
        out.append(
            _event(
                event_type=event_type,
                event_family="CANDIDATE",
                entity_type="candidate",
                entity_id=str(row.get("candidate_id") or ""),
                title={
                    "position.risk_plan_recorded": "Position risk plan recorded",
                    "position.stop_event_recorded": "Stop event recorded",
                    "position.corrected": "Position event corrected",
                    "position.final_outcome_recorded": "Position final outcome recorded",
                }.get(event_type, "Position event recorded"),
                summary=str(row.get("reason") or row.get("exit_reason") or row.get("event_type") or event_type),
                why_it_matters="Position management events are append-only operator records. Aegis tracks stops but does not execute them.",
                actor=str(row.get("operator") or "David"),
                source_path=path,
                timestamp=row.get("timestamp_utc") or row.get("exit_timestamp_utc") or row.get("entry_timestamp_utc"),
                related_entities=[row.get("position_id"), row.get("sleeve_id")],
                after_state={"event_type": raw_type, "stop_triggered": row.get("stop_triggered"), "exit_reason": row.get("exit_reason"), "corrected_fields": row.get("corrected_fields")},
            )
        )
    return out


def _runtime_events(root: Path, day_utc: str) -> list[dict[str, Any]]:
    out = []
    path, payload = latest_json_v1(root, "aegis_runtime_truth_kernel_v1", day_utc, "runtime_truth_kernel.v1.json")
    if payload:
        classification = str(payload.get("runtime_truth_classification") or "UNKNOWN")
        out.append(
            _event(
                event_type="runtime.truth_generated",
                event_family="RUNTIME",
                entity_type="runtime",
                entity_id=day_utc,
                title=f"Runtime truth generated: {classification}",
                summary=f"Highest readiness layer: {payload.get('highest_readiness_layer', 'UNKNOWN')}. Missing/stale count: {payload.get('missing_or_stale_source_count', 0)}.",
                why_it_matters="Runtime Truth is the authority for readiness and permissions.",
                actor="Aegis",
                source_path=Path(path) if path else None,
                timestamp=payload.get("generated_at") or payload.get("generated_at_utc"),
                after_state={"runtime_truth_classification": classification, "highest_readiness_layer": payload.get("highest_readiness_layer")},
            )
        )
        if classification == "PARTIAL_CONTEXT":
            out.append(
                _event(
                    event_type="runtime.partial_context",
                    event_family="RUNTIME",
                    entity_type="runtime",
                    entity_id=day_utc,
                    title="Runtime evidence is partial",
                    summary="Runtime evidence is incomplete for current-day operation.",
                    why_it_matters="Partial context limits readiness and keeps claims conservative.",
                    actor="Aegis",
                    source_path=Path(path) if path else None,
                    timestamp=payload.get("generated_at") or payload.get("generated_at_utc"),
                    after_state={"blocked_capabilities": payload.get("blocked_capabilities") or []},
                )
            )
    transition_path, transition = latest_json_v1(root, "aegis_runtime_state_transitions_v1", day_utc, "runtime_state_transitions.v1.json")
    if transition:
        out.append(
            _event(
                event_type="runtime.state_changed",
                event_family="RUNTIME",
                entity_type="runtime",
                entity_id=day_utc,
                title="Runtime state transition log updated",
                summary=f"{len(transition.get('transitions') or transition.get('changes') or [])} runtime transitions recorded.",
                why_it_matters="Transitions explain what changed since the prior runtime snapshot.",
                actor="Aegis",
                source_path=Path(transition_path) if transition_path else None,
                timestamp=transition.get("generated_at_utc") or transition.get("generated_at"),
            )
        )
    return out


def _performance_events(root: Path, day_utc: str) -> list[dict[str, Any]]:
    out = []
    path, payload = latest_json_v1(root, "aegis_sleeve_performance_analytics_v1", day_utc, "advisory_quality.v1.json")
    if payload:
        metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
        statuses = {str(row.get("metric_status") or "UNKNOWN") for row in metrics.values() if isinstance(row, dict)}
        insufficient = bool(statuses) and statuses <= {"INSUFFICIENT_DATA", "MISSING_INPUT", "UNKNOWN"}
        out.append(
            _event(
                event_type="performance.insufficient_data" if insufficient else "performance.attribution_updated",
                event_family="PERFORMANCE",
                entity_type="report",
                entity_id="advisory_quality",
                title="Advisory quality attribution updated",
                summary="Attribution metrics are initialized but not meaningful yet." if insufficient else "Advisory quality metrics were updated.",
                why_it_matters="Performance attribution determines whether candidate quality evidence is strong enough to learn from.",
                actor="Aegis",
                source_path=Path(path) if path else None,
                timestamp=payload.get("generated_at_utc") or payload.get("generated_at"),
                after_state={"metric_statuses": sorted(statuses)},
            )
        )
    return out


def _sleeve_events(root: Path, day_utc: str) -> list[dict[str, Any]]:
    path, payload = latest_json_v1(root, "aegis_sleeve_challenger_v1", day_utc, "sleeve_challenger.v1.json")
    out = []
    for row in payload.get("sleeve_challenges") or payload.get("challenges") or [] if isinstance(payload, dict) else []:
        recommendation = str(row.get("recommendation") or row.get("status") or "UNKNOWN")
        event_type = "sleeve.challenged"
        if recommendation == "WATCH":
            event_type = "sleeve.watch"
        elif recommendation in {"DEGRADED", "SUSPENSION_REVIEW", "RETIREMENT_REVIEW"}:
            event_type = "sleeve.degraded"
        out.append(
            _event(
                event_type=event_type,
                event_family="SLEEVE",
                entity_type="sleeve",
                entity_id=str(row.get("sleeve_id") or "UNKNOWN"),
                title=f"Sleeve review: {recommendation}",
                summary=str(row.get("reason") or row.get("summary") or "Sleeve challenge finding recorded."),
                why_it_matters="Sleeve challenge findings guide human research and review without automatic mutation.",
                actor="Aegis",
                source_path=Path(path) if path else None,
                timestamp=payload.get("generated_at_utc") or payload.get("generated_at"),
                after_state={"recommendation": recommendation},
            )
        )
    return out


def _governance_events(root: Path, day_utc: str) -> list[dict[str, Any]]:
    out = []
    path, payload = latest_json_v1(root, "aegis_intelligence_governance_kernel_v1", day_utc, "intelligence_recommendations.v1.json")
    for row in payload.get("recommendations") or [] if isinstance(payload, dict) else []:
        out.append(
            _event(
                event_type="governance.recommendation_created",
                event_family="GOVERNANCE",
                entity_type="approval",
                entity_id=str(row.get("recommendation_id") or ""),
                title=f"Governance recommendation: {row.get('type', 'UNKNOWN')}",
                summary=str(row.get("interpretation") or row.get("proposed_change") or "Recommendation created."),
                why_it_matters="Governance recommendations require human approval and cannot directly become actions.",
                actor="Aegis",
                source_path=Path(path) if path else None,
                timestamp=row.get("created_at") or payload.get("generated_at_utc"),
                after_state={"approval_status": row.get("approval_status")},
            )
        )
    ledger = root / "reports" / "aegis_intelligence_approval_ledger_v1" / day_utc / "intelligence_approval_ledger.v1.jsonl"
    for ledger_path, row in _jsonl_rows(ledger):
        event_type = {
            "APPROVED": "governance.approval_recorded",
            "REJECTED": "governance.rejected",
            "DEFERRED": "governance.deferred",
        }.get(str(row.get("event_type") or "").upper(), "governance.approval_recorded")
        out.append(
            _event(
                event_type=event_type,
                event_family="GOVERNANCE",
                entity_type="approval",
                entity_id=str(row.get("recommendation_id") or ""),
                title=f"Governance ledger event: {row.get('event_type', 'UNKNOWN')}",
                summary=str(row.get("reason") or "Approval ledger event recorded."),
                why_it_matters="Approval history is append-only and audit-replayable.",
                actor=str(row.get("operator") or "David"),
                source_path=ledger_path,
                timestamp=row.get("timestamp"),
                after_state={"event_type": row.get("event_type")},
            )
        )
    return out


def _system_events(root: Path, day_utc: str) -> list[dict[str, Any]]:
    specs = [
        ("canonical_state.generated", "SYSTEM", "report", "canonical_operator_state", "Canonical operator state generated", "aegis_canonical_operator_state_v1", "canonical_operator_state.v1.json"),
        ("operator_brief.generated", "SYSTEM", "report", "operator_brief", "Operator brief generated", "aegis_operator_brief_v1", "operator_brief.v1.json"),
        ("audit_handoff.generated", "SYSTEM", "report", "audit_handoff", "Audit handoff generated", "aegis_audit_handoff_v1", "aegis_audit_handoff.txt"),
    ]
    out = []
    for event_type, family, entity_type, entity_id, title, report_family, filename in specs:
        path = _latest_report_path(root, report_family, day_utc, filename)
        if not path:
            continue
        payload = read_json_v1(path) if path.suffix == ".json" else {}
        out.append(
            _event(
                event_type=event_type,
                event_family=family,
                entity_type=entity_type,
                entity_id=entity_id,
                title=title,
                summary=f"{title}.",
                why_it_matters="System artifacts preserve the operator memory and audit trail.",
                actor="Aegis",
                source_path=path,
                timestamp=payload.get("generated_at_utc") or payload.get("generated_at") or _mtime_iso(path),
            )
        )
    return out


def _event(*, event_type: str, event_family: str, entity_type: str, entity_id: str, title: str, summary: str, why_it_matters: str, actor: str, source_path: Path | None, timestamp: Any = None, related_entities: list[Any] | None = None, before_state: Any = None, after_state: Any = None) -> dict[str, Any]:
    source_hash = _file_hash(source_path)
    timestamp_utc, quality = _timestamp(timestamp, source_path)
    seed = "|".join([event_type, entity_type, str(entity_id), str(source_path or ""), source_hash])
    before_hash = _state_hash(before_state)
    after_hash = _state_hash(after_state)
    return {
        "event_id": hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24],
        "timestamp_utc": timestamp_utc,
        "timestamp_quality": quality,
        "event_type": event_type,
        "event_family": event_family,
        "entity_type": entity_type,
        "entity_id": str(entity_id),
        "title": title,
        "summary": summary,
        "why_it_matters": why_it_matters,
        "actor": actor or "Aegis",
        "source_artifact_path": str(source_path or ""),
        "source_hash": source_hash,
        "related_entities": [str(value) for value in (related_entities or []) if value],
        "before_state": before_state,
        "after_state": after_state,
        "before_state_hash": before_hash,
        "after_state_hash": after_hash,
        "drilldown_links": [{"label": "Source artifact", "path": str(source_path)}] if source_path else [],
        "audit_level": "STANDARD",
        "broker_execution_allowed": False,
        "autonomous_execution_allowed": False,
    }


def _jsonl_rows(path: Path) -> list[tuple[Path, dict[str, Any]]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append((path, payload))
    return rows


def _latest_report_path(root: Path, family: str, day_utc: str, filename: str) -> Path | None:
    base = root / "reports" / family / day_utc
    if not base.exists():
        return None
    direct = base / filename
    if direct.exists():
        return direct
    matches = sorted(base.rglob(filename))
    return matches[-1] if matches else None


def _dedupe_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = {}
    for event in events:
        key = str(event.get("event_id") or "")
        if key and key not in out:
            out[key] = event
    return list(out.values())


def _timeline_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    latest = max([str(row.get("timestamp_utc") or "") for row in events] or [""])
    family_counts = {family: len([row for row in events if row.get("event_family") == family]) for family in EVENT_FAMILIES}
    summary = {
        "total_events": len(events),
        "latest_event_at": latest or "UNKNOWN",
        "family_counts": family_counts,
    }
    for family in EVENT_FAMILIES:
        summary[f"{family.lower()}_events"] = family_counts[family]
    return summary


def _entity_index(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    buckets = {"candidates": {}, "hypotheses": {}, "sleeves": {}, "regimes": {}, "runtime": {}, "approvals": {}}
    mapping = {
        "candidate": "candidates",
        "hypothesis": "hypotheses",
        "sleeve": "sleeves",
        "regime": "regimes",
        "runtime": "runtime",
        "approval": "approvals",
    }
    for event in events:
        bucket = mapping.get(str(event.get("entity_type") or ""))
        if not bucket:
            continue
        entity_id = str(event.get("entity_id") or "")
        if not entity_id:
            continue
        row = buckets[bucket].setdefault(entity_id, {"entity_id": entity_id, "event_count": 0, "latest_event_at": "", "events": []})
        row["event_count"] += 1
        row["latest_event_at"] = max(str(row.get("latest_event_at") or ""), str(event.get("timestamp_utc") or ""))
        row["events"].append(event.get("event_id"))
    return {key: sorted(value.values(), key=lambda row: row["latest_event_at"], reverse=True) for key, value in buckets.items()}


def _audit_drilldowns(events: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows = []
    seen = set()
    for event in events:
        path = str(event.get("source_artifact_path") or "")
        if not path or path in seen:
            continue
        seen.add(path)
        rows.append({"event_id": str(event.get("event_id")), "path": path, "hash": str(event.get("source_hash") or "")})
    return rows


def _fixture_diagnostics(root: Path, day_utc: str) -> list[dict[str, Any]]:
    path, payload = latest_json_v1(root, PIPELINE_FAMILY, day_utc, "research_pipeline.v1.json")
    fixtures = []
    for item in payload.get("items") or [] if isinstance(payload, dict) else []:
        if item.get("classification") == "TEST_FIXTURE":
            fixtures.append({"hypothesis_id": item.get("hypothesis_id"), "source_artifact_path": str(path or ""), "reason": "Excluded from main journal feed as TEST_FIXTURE."})
    return fixtures


def _timestamp(value: Any, source_path: Path | None) -> tuple[str, str]:
    text = str(value or "").strip()
    if text:
        if text.endswith("Z"):
            return text, "HIGH"
        return text.replace("+00:00", "Z"), "HIGH"
    if source_path and source_path.exists():
        return _mtime_iso(source_path), "LOW"
    return _now(), "LOW"


def _mtime_iso(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except OSError:
        return _now()


def _state_hash(value: Any) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _file_hash(path: Path | None) -> str:
    if not path:
        return ""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
