from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.projections.hypothesis_queue_projection import blocked_items_from_projection, projection_api_payload, read_hypothesis_queue_projection
from research_lab.projections.projection_contracts import PROJECTION_BUILD_SCHEMA_VERSION, RESEARCH_LABEL, validate_projection_build
from research_lab.projections.projection_store import projection_output_hash, read_latest_projection, safe_json_load, write_projection_artifacts
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout

PROJECTION_TYPE = "operator_home"
QUEUE_NAMES = ["research_plans", "evidence", "paper_trials", "sleeve_reviews", "blocked_work", "research_backlog"]
SOURCE_REGISTRIES = {
    "research_plans": "research_plans.jsonl",
    "evidence_packages": "evidence_packages.jsonl",
    "paper_trials": "paper_trials.jsonl",
    "paper_trial_operations_reports": "paper_trial_operations_reports.jsonl",
    "sleeve_definitions": "sleeve_definitions.jsonl",
    "sleeve_reviews": "sleeve_reviews.jsonl",
    "sleeve_health_snapshots": "sleeve_health_snapshots.jsonl",
    "projection_builds": "projection_builds.jsonl",
}


def read_operator_queue_projection(*, store_root: Path | None = None) -> dict[str, Any] | None:
    return read_latest_projection(PROJECTION_TYPE, store_root=store_root)


def build_operator_queue_projection(*, store_root: Path | None = None, actor: str = "AegisProjection", strict: bool = False, write: bool = True) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    built_at = utc_now_iso()
    build_sequence = len(read_jsonl(store / "registries" / "projection_builds.jsonl")) + 1
    warnings: list[str] = []
    errors: list[str] = []
    write_audit_event(actor=actor, entity_type="research_projection", entity_id=PROJECTION_TYPE, action="research_projection_rebuild_started", previous_state_hash="", new_state_hash="", reason="Started operator queue projection rebuild.", metadata={"strict": strict}, store_root=store)
    queues = {
        "research_plans": _research_plans_queue(store, warnings),
        "evidence": _evidence_queue(store, warnings),
        "paper_trials": _paper_trials_queue(store, warnings),
        "sleeve_reviews": _sleeve_reviews_queue(store, warnings),
        "blocked_work": _blocked_work_queue(store, warnings),
    }
    queues["research_backlog"] = _research_backlog_queue(queues, store)
    source_summary = _source_summary(store)
    source_summary["source_warnings"] = sorted(set(warnings))
    source_summary["queue_counts"] = {name: len(queues.get(name) or []) for name in QUEUE_NAMES}
    integrity_status = "error" if errors else ("warning" if warnings or any(_needs_review(items) for items in queues.values()) else "ok")
    empty_state = {
        name: _empty_state_for_queue(name, queues.get(name) or [], source_summary)
        for name in QUEUE_NAMES
    }
    projection = {
        "projection_id": f"ohp_{short_hash(content_hash({'built_at': built_at, 'build_sequence': build_sequence, 'queues': {k: [item.get('item_id') for item in v] for k, v in queues.items()}}), 16)}",
        "projection_type": PROJECTION_TYPE,
        "built_at": built_at,
        "source_summary": source_summary,
        "integrity_status": integrity_status,
        "integrity_warnings": sorted(set(warnings)),
        "integrity_errors": sorted(set(errors)),
        "queues": queues,
        "empty_state": empty_state,
        "operator_message": _operator_message(integrity_status, queues),
        "research_label": RESEARCH_LABEL,
        "schema_version": "operator_queue_projection.v1",
    }
    projection["content_hash"] = projection_output_hash(projection)
    build_status = "failed" if errors and strict else ("success_with_warnings" if warnings or errors else "success")
    build = {
        "projection_build_id": f"pb_{short_hash(content_hash({'projection_type': PROJECTION_TYPE, 'built_at': built_at, 'build_sequence': build_sequence, 'output': projection['content_hash']}), 20)}",
        "projection_type": PROJECTION_TYPE,
        "built_at": built_at,
        "built_by": actor,
        "source_registries": {name: str(store / "registries" / filename) for name, filename in SOURCE_REGISTRIES.items()},
        "source_artifact_counts": source_summary["artifact_counts"],
        "source_hashes": source_summary["source_hashes"],
        "output_uri": "research://projections/operator_home/latest.json",
        "output_hash": projection["content_hash"],
        "status": build_status,
        "warnings": sorted(set(warnings)),
        "errors": sorted(set(errors)),
        "schema_version": PROJECTION_BUILD_SCHEMA_VERSION,
    }
    validate_projection_build(build)
    if strict and build_status == "failed":
        write_audit_event(actor=actor, entity_type="research_projection", entity_id=PROJECTION_TYPE, action="research_projection_rebuild_failed", previous_state_hash="", new_state_hash=projection["content_hash"], reason="Operator projection rebuild failed strict validation.", metadata={"errors": errors, "warnings": warnings}, store_root=store)
        raise RuntimeError("operator projection validation failed in strict mode")
    paths = {}
    if write:
        paths = write_projection_artifacts(projection=projection, build=build, markdown=_projection_markdown(projection, build), store_root=store)
        _write_health(store, projection, build)
        action = "research_projection_rebuild_completed" if build_status != "failed" else "research_projection_rebuild_failed"
        write_audit_event(actor=actor, entity_type="research_projection", entity_id=build["projection_build_id"], action=action, previous_state_hash="", new_state_hash=projection["content_hash"], reason="Completed operator queue projection rebuild.", metadata={"projection_type": PROJECTION_TYPE, "status": build_status, "warnings": warnings, "errors": errors, "paths": paths}, store_root=store)
        if warnings:
            write_audit_event(actor=actor, entity_type="research_projection", entity_id=build["projection_build_id"], action="research_projection_integrity_warning", previous_state_hash="", new_state_hash=projection["content_hash"], reason="Operator projection built with integrity warnings.", metadata={"warnings": warnings}, store_root=store)
    return {"ok": build_status != "failed", "projection": projection, "build": build, "paths": paths}


def operator_home_projection_summary(*, store_root: Path | None = None) -> dict[str, Any]:
    queue = projection_api_payload(store_root=store_root)
    lanes = queue.get("lanes") or {}
    operator = operator_projection_payload(store_root=store_root)
    return {
        "hypothesis_queue_count": queue.get("projected_item_count", queue.get("count", 0)),
        "needs_data_count": len(lanes.get("needs_data") or []),
        "watchlist_count": len(lanes.get("watchlist") or []),
        "accepted_count": len(lanes.get("accepted_for_research") or []),
        "needs_review_count": len(lanes.get("needs_review") or []),
        "projection_integrity_status": _combined_integrity(queue.get("integrity_status"), operator.get("integrity_status")),
        "projection_operator_message": queue.get("operator_message") or operator.get("operator_message"),
        "operator_projection_built_at": operator.get("projection_built_at"),
        "research_plan_count": len(operator.get("research_plans") or []),
        "evidence_count": len(operator.get("evidence") or []),
        "paper_trial_count": len(operator.get("paper_trials") or []),
        "sleeve_review_count": len(operator.get("sleeves") or []),
        "blocked_work_count": len(operator.get("blocked_items") or []),
        "backlog_count": len(operator.get("backlog") or []),
    }


def operator_projection_payload(*, store_root: Path | None = None) -> dict[str, Any]:
    projection = read_operator_queue_projection(store_root=store_root)
    if projection is None:
        return _missing_projection_payload()
    queues = projection.get("queues") or {}
    evidence = queues.get("evidence") or []
    tabs = {name: [row for row in evidence if row.get("tab") == name] for name in ["Event Studies", "Backtests", "Regime Analysis", "Cost Analysis", "Longitudinal Results"]}
    return {
        "ok": True,
        "read_only": True,
        "projection_source": "latest.json",
        "projection_id": projection.get("projection_id"),
        "projection_type": projection.get("projection_type"),
        "projection_built_at": projection.get("built_at"),
        "integrity_status": projection.get("integrity_status"),
        "integrity_warnings": projection.get("integrity_warnings") or [],
        "integrity_errors": projection.get("integrity_errors") or [],
        "source_summary": projection.get("source_summary") or {},
        "operator_message": projection.get("operator_message"),
        "empty_state": projection.get("empty_state") or {},
        "research_plans": queues.get("research_plans") or [],
        "evidence": evidence,
        "tabs": tabs,
        "paper_trials": queues.get("paper_trials") or [],
        "sleeves": queues.get("sleeve_reviews") or [],
        "blocked_items": queues.get("blocked_work") or [],
        "backlog": queues.get("research_backlog") or [],
        "counts": {name: len(queues.get(name) or []) for name in QUEUE_NAMES},
    }


def research_plans_payload(*, store_root: Path | None = None) -> dict[str, Any]:
    payload = operator_projection_payload(store_root=store_root)
    return {**_projection_header(payload), "research_plans": payload.get("research_plans") or [], "count": len(payload.get("research_plans") or [])}


def evidence_payload(*, store_root: Path | None = None) -> dict[str, Any]:
    payload = operator_projection_payload(store_root=store_root)
    return {**_projection_header(payload), "evidence": payload.get("evidence") or [], "tabs": payload.get("tabs") or {}, "count": len(payload.get("evidence") or [])}


def paper_trials_payload(*, store_root: Path | None = None) -> dict[str, Any]:
    payload = operator_projection_payload(store_root=store_root)
    return {**_projection_header(payload), "paper_trials": payload.get("paper_trials") or [], "count": len(payload.get("paper_trials") or [])}


def sleeve_review_center_payload(*, store_root: Path | None = None) -> dict[str, Any]:
    payload = operator_projection_payload(store_root=store_root)
    return {**_projection_header(payload), "sleeves": payload.get("sleeves") or [], "count": len(payload.get("sleeves") or [])}


def blocked_work_payload(*, store_root: Path | None = None) -> dict[str, Any]:
    payload = operator_projection_payload(store_root=store_root)
    return {**_projection_header(payload), "blocked_items": payload.get("blocked_items") or [], "count": len(payload.get("blocked_items") or [])}


def research_backlog_payload(*, store_root: Path | None = None) -> dict[str, Any]:
    payload = operator_projection_payload(store_root=store_root)
    return {**_projection_header(payload), "backlog": payload.get("backlog") or [], "count": len(payload.get("backlog") or [])}


def _projection_header(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": payload.get("ok", False),
        "read_only": True,
        "projection_source": payload.get("projection_source", "missing"),
        "projection_built_at": payload.get("projection_built_at", ""),
        "integrity_status": payload.get("integrity_status", "source_missing"),
        "integrity_warnings": payload.get("integrity_warnings") or [],
        "integrity_errors": payload.get("integrity_errors") or [],
        "operator_message": payload.get("operator_message"),
        "empty_state": payload.get("empty_state") or {},
    }


def _missing_projection_payload() -> dict[str, Any]:
    return {
        "ok": False,
        "read_only": True,
        "projection_source": "missing",
        "integrity_status": "source_missing",
        "operator_message": "Operator queue projection has not been built.",
        "next_allowed_action": "Rebuild research projections",
        "empty_state": {name: {"is_empty": True, "trusted_empty": False, "reason": "projection_not_built"} for name in QUEUE_NAMES},
        "research_plans": [],
        "evidence": [],
        "tabs": {},
        "paper_trials": [],
        "sleeves": [],
        "blocked_items": [],
        "backlog": [],
    }


def _research_plans_queue(store: Path, warnings: list[str]) -> list[dict[str, Any]]:
    registry = _registry_by_id(store, "research_plans.jsonl", "research_plan_id", warnings)
    artifacts = _artifact_dirs(store / "research_plans", "research_plan.json", "research_plan_id", warnings)
    rows = []
    for plan_id in sorted(set(registry) | set(artifacts)):
        row = registry.get(plan_id) or {}
        plan = artifacts.get(plan_id)
        flags: list[str] = []
        if plan is None:
            flags.extend(["needs_review", "missing_artifact"])
            warnings.append(f"research_plan_artifact_missing:{plan_id}")
            plan = row
        elif plan.get("_parse_error"):
            flags.extend(["needs_review", "parse_error"])
        if plan_id not in registry:
            flags.extend(["needs_review", "integrity_warning"])
            warnings.append(f"research_plan_artifact_without_registry:{plan_id}")
        rows.append({
            "research_plan_id": plan_id,
            "hypothesis": plan.get("hypothesis") or plan.get("title") or row.get("title") or plan_id,
            "status": plan.get("status") or row.get("status") or "accepted_for_research",
            "dataset_readiness": "dataset_snapshot_id" if plan.get("dataset_snapshot_id") else "missing_dataset",
            "last_run": plan.get("last_run") or "not_run",
            "evidence_package": plan.get("evidence_package_id") or "none",
            "next_action": "Run Study" if plan.get("dataset_snapshot_id") else "Enable dataset",
            "display_flags": sorted(set(flags)),
            "source_refs": _source_refs("research_plan", plan_id, plan),
        })
    return rows


def _evidence_queue(store: Path, warnings: list[str]) -> list[dict[str, Any]]:
    registry = _registry_by_id(store, "evidence_packages.jsonl", "evidence_package_id", warnings)
    artifacts = _evidence_artifacts(store, warnings)
    rows = []
    for evidence_id in sorted(set(registry) | set(artifacts)):
        row = registry.get(evidence_id) or {}
        manifest = artifacts.get(evidence_id)
        flags: list[str] = []
        if manifest is None:
            flags.extend(["needs_review", "missing_artifact"])
            warnings.append(f"evidence_manifest_missing:{evidence_id}")
            manifest = row
        elif manifest.get("_parse_error"):
            flags.extend(["needs_review", "parse_error"])
        if evidence_id not in registry:
            flags.extend(["needs_review", "integrity_warning"])
            warnings.append(f"evidence_artifact_without_registry:{evidence_id}")
        runner = str(manifest.get("runner_name") or row.get("runner_name") or evidence_id).lower()
        tab = _evidence_tab(runner)
        rows.append({
            "evidence_package_id": evidence_id,
            "tab": tab,
            "hypothesis": manifest.get("hypothesis_id") or row.get("hypothesis_id") or "not reported",
            "evidence_quality": manifest.get("evidence_quality") or row.get("evidence_quality") or "not reported",
            "event_count": manifest.get("event_count") or row.get("event_count") or "not reported",
            "post_cost_results": manifest.get("post_cost_results") or manifest.get("post_cost_total_return") or "not reported",
            "regime_notes": manifest.get("regime_notes") or "not reported",
            "backtest_comparison": manifest.get("backtest_comparison") or "not reported",
            "conclusion_status": manifest.get("status") or manifest.get("evidence_quality") or "research evidence",
            "display_flags": sorted(set(flags)),
            "advanced": {"artifact_id": evidence_id, "storage_uri": row.get("storage_uri") or manifest.get("storage_uri") or "", "content_hash": row.get("content_hash") or manifest.get("content_hash") or manifest.get("manifest_hash") or ""},
        })
    return rows


def _paper_trials_queue(store: Path, warnings: list[str]) -> list[dict[str, Any]]:
    registry = _registry_by_id(store, "paper_trials.jsonl", "paper_trial_id", warnings)
    trial_artifacts = _artifact_dirs(store / "paper_trials", "paper_trial.json", "paper_trial_id", warnings)
    ops_reports = _artifact_dirs(store / "paper_trials", "paper_trial_operations_report.json", "paper_trial_id", warnings)
    rows = []
    for trial_id in sorted(set(registry) | set(trial_artifacts) | set(ops_reports)):
        row = registry.get(trial_id) or {}
        trial = trial_artifacts.get(trial_id) or row
        report = ops_reports.get(trial_id) or {}
        flags: list[str] = []
        if trial_id not in trial_artifacts:
            flags.append("missing_artifact")
        if not report:
            flags.extend(["needs_review", "integrity_warning"])
            warnings.append(f"paper_trial_operations_report_missing:{trial_id}")
        if trial.get("_parse_error") or report.get("_parse_error"):
            flags.extend(["needs_review", "parse_error"])
        due = report.get("due_outcomes", report.get("pending_due_outcomes", 0)) or 0
        rows.append({
            "paper_trial_id": trial_id,
            "sleeve_name": report.get("sleeve_id") or report.get("sleeve_version_id") or trial.get("sleeve_id") or "Paper trial",
            "status": report.get("status") or trial.get("status") or row.get("status") or "unknown",
            "observation_count": report.get("observation_count", 0),
            "candidate_count_total": report.get("candidate_count_total", 0),
            "due_outcomes": due,
            "pending_due_outcomes": report.get("pending_due_outcomes", 0),
            "measured_candidate_count": report.get("measured_candidate_count", 0),
            "recommended_next_action": report.get("recommended_next_action") or "Open Summary",
            "due_outcomes_need_measurement": bool(due or report.get("pending_due_outcomes")),
            "display_flags": sorted(set(flags)),
        })
    return rows


def _sleeve_reviews_queue(store: Path, warnings: list[str]) -> list[dict[str, Any]]:
    definitions = _registry_by_id(store, "sleeve_definitions.jsonl", "sleeve_id", warnings)
    artifacts = _artifact_dirs(store / "sleeves", "sleeve_definition.json", "sleeve_id", warnings)
    reviews = _latest_registry_by_id(store, "sleeve_reviews.jsonl", "sleeve_id", warnings)
    health = _latest_registry_by_id(store, "sleeve_health_snapshots.jsonl", "sleeve_id", warnings)
    rows = []
    for sleeve_id in sorted(set(definitions) | set(artifacts) | set(reviews) | {"slv_etf_drop_reversion_v1"}):
        definition = artifacts.get(sleeve_id) or definitions.get(sleeve_id) or {}
        review = reviews.get(sleeve_id) or {}
        health_row = health.get(sleeve_id) or {}
        flags: list[str] = []
        if sleeve_id not in artifacts and sleeve_id in definitions:
            flags.extend(["needs_review", "missing_artifact"])
            warnings.append(f"sleeve_definition_artifact_missing:{sleeve_id}")
        rows.append({
            "sleeve_id": sleeve_id,
            "sleeve_name": definition.get("name") or definition.get("sleeve_name") or sleeve_id,
            "overall_health": health_row.get("overall_health") or health_row.get("health") or review.get("overall_health") or "watch",
            "drift_status": health_row.get("drift_status") or "not_reported",
            "fragility_status": health_row.get("fragility_status") or "not_reported",
            "paper_trial_status": review.get("paper_trial_status") or "active" if sleeve_id == "slv_etf_drop_reversion_v1" else review.get("paper_trial_status") or "not_reported",
            "challenge_type": review.get("challenge_type") or "underperformance" if sleeve_id == "slv_etf_drop_reversion_v1" else review.get("challenge_type") or "not_reported",
            "recommended_action": review.get("recommended_action") or "continue_research",
            "latest_review": review.get("review_decision") or review.get("decision") or "continue",
            "automatic_sleeve_mutation_allowed": False,
            "display_flags": sorted(set(flags)),
        })
    return rows


def _blocked_work_queue(store: Path, warnings: list[str]) -> list[dict[str, Any]]:
    hypothesis_projection = read_hypothesis_queue_projection(store_root=store)
    if hypothesis_projection is None:
        warnings.append("hypothesis_queue_projection_missing")
    return blocked_items_from_projection(store_root=store)


def _research_backlog_queue(queues: dict[str, list[dict[str, Any]]], store: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    bucket_rank = {"blocked": 0, "high": 1, "medium": 2, "watchlist": 3, "low": 4, "": 5}
    hq = projection_api_payload(store_root=store)
    for row in hq.get("queue") or []:
        rows.append({"rank": 0, "label": row.get("title"), "type": "Hypothesis Proposal", "priority_bucket": row.get("priority_bucket"), "status": row.get("proposal_status"), "recommended_next_action": row.get("recommended_next_action"), "blocking_items": row.get("blocking_items") or []})
    for row in queues.get("research_plans") or []:
        rows.append({"rank": 0, "label": row.get("hypothesis"), "type": "Research Plan", "priority_bucket": "medium" if row.get("dataset_readiness") != "missing_dataset" else "blocked", "status": row.get("status"), "recommended_next_action": row.get("next_action"), "blocking_items": ["missing_dataset"] if row.get("dataset_readiness") == "missing_dataset" else []})
    for row in queues.get("paper_trials") or []:
        rows.append({"rank": 0, "label": row.get("paper_trial_id"), "type": "Paper Trial", "priority_bucket": "medium" if row.get("due_outcomes_need_measurement") else "watchlist", "status": row.get("status"), "recommended_next_action": row.get("recommended_next_action"), "blocking_items": ["due_outcomes"] if row.get("due_outcomes_need_measurement") else []})
    for row in queues.get("sleeve_reviews") or []:
        rows.append({"rank": 0, "label": row.get("sleeve_name"), "type": "Sleeve", "priority_bucket": "medium" if row.get("overall_health") in {"watch", "challenged"} else "watchlist", "status": row.get("overall_health"), "recommended_next_action": row.get("recommended_action"), "blocking_items": []})
    rows.sort(key=lambda item: (bucket_rank.get(str(item.get("priority_bucket") or ""), 5), str(item.get("label") or "")))
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    return rows


def _registry_by_id(store: Path, filename: str, id_field: str, warnings: list[str]) -> dict[str, dict[str, Any]]:
    path = store / "registries" / filename
    if not path.exists():
        warnings.append(f"source_registry_missing:{filename}")
        return {}
    rows = {}
    try:
        for row in read_jsonl(path):
            item_id = str(row.get(id_field) or "").strip()
            if item_id:
                rows[item_id] = row
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"registry_unreadable:{filename}:{exc}")
    return rows


def _latest_registry_by_id(store: Path, filename: str, id_field: str, warnings: list[str]) -> dict[str, dict[str, Any]]:
    path = store / "registries" / filename
    rows: dict[str, dict[str, Any]] = {}
    if not path.exists():
        warnings.append(f"source_registry_missing:{filename}")
        return rows
    try:
        for row in read_jsonl(path):
            item_id = str(row.get(id_field) or "").strip()
            if item_id:
                rows[item_id] = row
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"registry_unreadable:{filename}:{exc}")
    return rows


def _artifact_dirs(root: Path, filename: str, id_field: str, warnings: list[str]) -> dict[str, dict[str, Any]]:
    out = {}
    if not root.exists():
        return out
    for path in sorted(root.glob(f"*/{filename}")):
        payload, error = safe_json_load(path)
        item_id = path.parent.name
        if error or not isinstance(payload, dict):
            out[item_id] = {id_field: item_id, "_parse_error": error or "not_object", "_path": str(path)}
            warnings.append(f"artifact_parse_error:{path}:{error or 'not_object'}")
            continue
        payload["_path"] = str(path)
        out[str(payload.get(id_field) or item_id)] = payload
    return out


def _evidence_artifacts(store: Path, warnings: list[str]) -> dict[str, dict[str, Any]]:
    out = {}
    root = store / "evidence_packages"
    if not root.exists():
        return out
    for directory in sorted(path for path in root.iterdir() if path.is_dir()):
        manifest_path = directory / "evidence_manifest.json"
        if not manifest_path.exists():
            manifest_path = directory / "manifest.json"
        if not manifest_path.exists():
            continue
        payload, error = safe_json_load(manifest_path)
        if error or not isinstance(payload, dict):
            out[directory.name] = {"evidence_package_id": directory.name, "_parse_error": error or "not_object", "_path": str(manifest_path)}
            warnings.append(f"evidence_manifest_parse_error:{manifest_path}:{error or 'not_object'}")
            continue
        payload["_path"] = str(manifest_path)
        out[str(payload.get("evidence_package_id") or directory.name)] = payload
    return out


def _evidence_tab(runner: str) -> str:
    if "backtest" in runner or "bt_" in runner:
        return "Backtests"
    if "regime" in runner:
        return "Regime Analysis"
    if "cost" in runner:
        return "Cost Analysis"
    if "longitudinal" in runner:
        return "Longitudinal Results"
    return "Event Studies"


def _source_refs(kind: str, item_id: str, payload: dict[str, Any]) -> list[dict[str, str]]:
    refs = [{"kind": "registry_or_artifact", "name": kind, "id": item_id}]
    if payload.get("_path"):
        refs.append({"kind": "artifact", "name": kind, "id": item_id, "path": str(payload["_path"])})
    return refs


def _source_summary(store: Path) -> dict[str, Any]:
    registry_counts = {}
    source_hashes = {}
    for name, filename in SOURCE_REGISTRIES.items():
        path = store / "registries" / filename
        registry_counts[name] = len(read_jsonl(path)) if path.exists() else 0
        source_hashes[name] = _file_hash(path)
    artifact_counts = {
        "research_plan_artifacts": len(list((store / "research_plans").glob("*/research_plan.json"))),
        "evidence_artifacts": len(list((store / "evidence_packages").glob("*/evidence_manifest.json"))) + len(list((store / "evidence_packages").glob("*/manifest.json"))),
        "paper_trial_artifacts": len(list((store / "paper_trials").glob("*/paper_trial.json"))),
        "paper_trial_operation_reports": len(list((store / "paper_trials").glob("*/paper_trial_operations_report.json"))),
        "sleeve_definition_artifacts": len(list((store / "sleeves").glob("*/sleeve_definition.json"))),
    }
    hypothesis_projection = read_hypothesis_queue_projection(store_root=store)
    hypothesis_items = (hypothesis_projection or {}).get("items") or []
    hypothesis_blocked = [item for item in hypothesis_items if item.get("blocking_items") or "blocked" in (item.get("display_flags") or [])]
    return {
        "source_registries_checked": True,
        "registry_counts": registry_counts,
        "artifact_counts": artifact_counts,
        "source_hashes": source_hashes,
        "hypothesis_queue_projection": {
            "exists": hypothesis_projection is not None,
            "item_count": len(hypothesis_items),
            "blocked_count": len(hypothesis_blocked),
            "integrity_status": (hypothesis_projection or {}).get("integrity_status", "source_missing"),
        },
    }


def _file_hash(path: Path) -> str:
    if not path.exists():
        return ""
    return content_hash({"path": str(path), "content": path.read_text(encoding="utf-8")})


def _empty_state_for_queue(name: str, items: list[dict[str, Any]], source_summary: dict[str, Any]) -> dict[str, Any]:
    if items:
        return {"is_empty": False, "trusted_empty": False, "reason": "projection_has_items"}
    relevant_source_count = _queue_source_count(name, source_summary)
    relevant_warnings = _queue_source_warnings(name, source_summary.get("source_warnings") or [])
    if source_summary.get("source_registries_checked") is not True:
        return {"is_empty": True, "trusted_empty": False, "reason": "source_registries_not_checked"}
    if relevant_warnings:
        return {"is_empty": True, "trusted_empty": False, "reason": "source_integrity_warning", "warnings": relevant_warnings}
    if relevant_source_count > 0:
        return {"is_empty": True, "trusted_empty": False, "reason": "source_exists_but_projection_empty", "source_count": relevant_source_count}
    return {"is_empty": True, "trusted_empty": True, "reason": "projection_build_confirmed_no_items", "source_registries_checked": True}


def _queue_source_count(name: str, source_summary: dict[str, Any]) -> int:
    registries = source_summary.get("registry_counts") or {}
    artifacts = source_summary.get("artifact_counts") or {}
    hypothesis = source_summary.get("hypothesis_queue_projection") or {}
    if name == "research_plans":
        return int(registries.get("research_plans") or 0) + int(artifacts.get("research_plan_artifacts") or 0)
    if name == "evidence":
        return int(registries.get("evidence_packages") or 0) + int(artifacts.get("evidence_artifacts") or 0)
    if name == "paper_trials":
        return int(registries.get("paper_trials") or 0) + int(artifacts.get("paper_trial_artifacts") or 0) + int(artifacts.get("paper_trial_operation_reports") or 0)
    if name == "sleeve_reviews":
        return int(registries.get("sleeve_definitions") or 0) + int(registries.get("sleeve_reviews") or 0) + int(registries.get("sleeve_health_snapshots") or 0) + int(artifacts.get("sleeve_definition_artifacts") or 0)
    if name == "blocked_work":
        return int(hypothesis.get("blocked_count") or 0)
    if name == "research_backlog":
        return int(hypothesis.get("item_count") or 0) + _queue_source_count("research_plans", source_summary) + _queue_source_count("paper_trials", source_summary) + _queue_source_count("sleeve_reviews", source_summary)
    return 0


def _queue_source_warnings(name: str, warnings: list[str]) -> list[str]:
    warning_text = [str(item) for item in warnings]
    terms = {
        "research_plans": ["research_plan"],
        "evidence": ["evidence"],
        "paper_trials": ["paper_trial"],
        "sleeve_reviews": ["sleeve"],
        "blocked_work": ["hypothesis_queue"],
        "research_backlog": ["research_plan", "paper_trial", "sleeve", "hypothesis_queue"],
    }.get(name, [])
    return [item for item in warning_text if any(term in item for term in terms)]


def _needs_review(items: list[dict[str, Any]]) -> bool:
    return any("needs_review" in (item.get("display_flags") or []) for item in items)


def _operator_message(integrity_status: str, queues: dict[str, list[dict[str, Any]]]) -> str:
    if integrity_status == "ok":
        return "Operator queue projection built successfully."
    if integrity_status == "warning":
        return "Operator queue integrity warning: some source artifacts need review and remain visible."
    return "Operator queue integrity error: rebuild or inspect projection before trusting this queue."


def _combined_integrity(*statuses: Any) -> str:
    values = [str(item or "source_missing") for item in statuses]
    if "error" in values:
        return "error"
    if "source_missing" in values:
        return "source_missing"
    if "stale" in values:
        return "stale"
    if "warning" in values:
        return "warning"
    return "ok"


def _write_health(store: Path, projection: dict[str, Any], build: dict[str, Any]) -> None:
    health_path = store / "projections" / "projection_health" / "latest.json"
    existing = read_json(health_path) if health_path.exists() else {"projections": {}}
    projections = existing.get("projections") or {}
    projections[PROJECTION_TYPE] = {
        "projection_type": PROJECTION_TYPE,
        "latest_build_id": build["projection_build_id"],
        "latest_built_at": build["built_at"],
        "status": build["status"],
        "source_counts": projection["source_summary"].get("registry_counts", {}),
        "projected_item_count": sum(len(projection["queues"].get(name) or []) for name in QUEUE_NAMES),
        "integrity_status": projection.get("integrity_status"),
        "warnings": projection.get("integrity_warnings") or [],
        "errors": projection.get("integrity_errors") or [],
        "stale": False,
    }
    existing["ok"] = True
    existing["projections"] = projections
    # Preserve the original single-projection shape for clients that read hypothesis health.
    hypothesis = projections.get("hypothesis_queue") or next(iter(projections.values()), {})
    existing.update({k: v for k, v in hypothesis.items() if k != "projection_type"})
    write_json(health_path, existing, overwrite=True)


def _projection_markdown(projection: dict[str, Any], build: dict[str, Any]) -> str:
    counts = {name: len(projection["queues"].get(name) or []) for name in QUEUE_NAMES}
    return "\n".join([
        "# Operator Queue Projection",
        "",
        f"Build: {build['projection_build_id']}",
        f"Built at: {projection['built_at']}",
        f"Integrity: {projection['integrity_status']}",
        f"Queue counts: {json.dumps(counts, sort_keys=True)}",
        "",
        projection["operator_message"],
        "",
    ])
