from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import append_jsonl, read_json, read_jsonl, write_json
from research_lab.storage.paths import ensure_store_layout


RESEARCH_LABEL = (
    "Research governance view only. No broker execution. No live trading. "
    "No autonomous trading. No sleeve mutation. Backtests/model outputs are "
    "hypothetical research evidence, not achieved portfolio performance."
)


def _registry(store: Path, name: str) -> Path:
    return store / "registries" / name


def _read_registry(store: Path, name: str) -> list[dict[str, Any]]:
    return read_jsonl(_registry(store, name))


def _latest_by(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_key = str(row.get(key) or "")
        if not row_key:
            continue
        current = latest.get(row_key)
        if current is None or str(row.get("created_at") or row.get("reviewed_at") or "") >= str(current.get("created_at") or current.get("reviewed_at") or ""):
            latest[row_key] = row
    return latest


def _latest_for_sleeve(rows: list[dict[str, Any]], sleeve_id: str, sleeve_version_id: str | None = None) -> dict[str, Any]:
    filtered = [
        row
        for row in rows
        if row.get("sleeve_id") == sleeve_id and (sleeve_version_id is None or row.get("sleeve_version_id") == sleeve_version_id)
    ]
    return sorted(filtered, key=lambda row: str(row.get("created_at") or row.get("reviewed_at") or ""))[-1] if filtered else {}


def _read_optional(path: Path, missing: list[dict[str, str]]) -> dict[str, Any]:
    if not path.exists():
        missing.append({"path": str(path), "reason": "missing_optional_artifact"})
        return {}
    try:
        return read_json(path)
    except Exception as exc:
        missing.append({"path": str(path), "reason": f"json_parse_failed: {exc}"})
        return {}


def _paper_trial_summaries(store: Path) -> list[dict[str, Any]]:
    summaries = []
    for path in sorted((store / "paper_trials").glob("*/paper_trial_summary.json")):
        try:
            summaries.append(read_json(path))
        except Exception:
            continue
    return summaries


def _latest_operations_report(store: Path, paper_trial_id: str, missing: list[dict[str, str]]) -> dict[str, Any]:
    return _read_optional(store / "paper_trials" / paper_trial_id / "paper_trial_operations_report.json", missing)


def _latest_longitudinal_report(store: Path, sleeve_id: str, sleeve_version_id: str, missing: list[dict[str, str]]) -> dict[str, Any]:
    rows = [
        row
        for row in _read_registry(store, "sleeve_learning_reports.jsonl")
        if row.get("sleeve_id") == sleeve_id and row.get("sleeve_version_id") == sleeve_version_id
    ]
    if not rows:
        return {}
    row = sorted(rows, key=lambda item: str(item.get("created_at") or ""))[-1]
    report_id = str(row.get("sleeve_learning_report_id") or "")
    for path in sorted((store / "longitudinal_runs").glob("*/sleeve_learning_report.json")):
        payload = _read_optional(path, missing)
        if payload.get("sleeve_learning_report_id") == report_id:
            return payload
    return {}


def _backtest_summary(store: Path, evidence_package_id: str, missing: list[dict[str, str]]) -> dict[str, Any]:
    if not evidence_package_id:
        return {}
    return _read_optional(store / "evidence_packages" / evidence_package_id / "performance_summary.json", missing)


def _event_summary(store: Path, evidence_package_id: str, missing: list[dict[str, str]]) -> dict[str, Any]:
    if not evidence_package_id:
        return {}
    return _read_optional(store / "evidence_packages" / evidence_package_id / "summary.json", missing)


def _candidate_summary(store: Path, candidate_batch_id: str, missing: list[dict[str, str]]) -> dict[str, Any]:
    if not candidate_batch_id:
        return {}
    return _read_optional(store / "candidate_batches" / candidate_batch_id / "generation_summary.json", missing)


def _report_root(store: Path, family: str) -> Path:
    return store / "portfolio_reports" / family


def _write_report(
    *,
    store: Path,
    family: str,
    report_id: str,
    payload: dict[str, Any],
    registry_name: str,
    registry_row: dict[str, Any],
    markdown: str,
    audit_action: str,
    actor: str,
) -> dict[str, Any]:
    root = _report_root(store, family)
    json_path = root / f"{report_id}.json"
    md_path = root / f"{report_id}.md"
    write_json(json_path, payload, overwrite=False)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(markdown, encoding="utf-8")
    append_jsonl(_registry(store, registry_name), registry_row)
    write_audit_event(
        actor=actor,
        entity_type="portfolio_report",
        entity_id=report_id,
        action=audit_action,
        new_state_hash=payload["content_hash"],
        reason="Generated immutable read-only cross-sleeve research governance report.",
        metadata={"registry_row": registry_row, "json_path": str(json_path), "markdown_path": str(md_path)},
        store_root=store,
    )
    return payload


def build_evidence_inventory(*, store_root: Path | None = None, created_at: str | None = None, created_by: str = "Aegis") -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    missing_global: list[dict[str, str]] = []
    sleeve_rows = sorted(_read_registry(store, "sleeve_definitions.jsonl"), key=lambda row: str(row.get("sleeve_id") or ""))
    version_by_sleeve = _latest_by(_read_registry(store, "sleeve_versions.jsonl"), "sleeve_id")
    health_rows = _read_registry(store, "sleeve_health_snapshots.jsonl")
    challenge_rows = _read_registry(store, "sleeve_challenges.jsonl")
    review_rows = _read_registry(store, "sleeve_reviews.jsonl")
    paper_summaries = _paper_trial_summaries(store)
    sleeves: list[dict[str, Any]] = []
    for sleeve_row in sleeve_rows:
        sleeve_id = str(sleeve_row.get("sleeve_id") or "")
        version_row = version_by_sleeve.get(sleeve_id, {})
        sleeve_version_id = str(version_row.get("sleeve_version_id") or "")
        missing: list[dict[str, str]] = []
        version = _read_optional(store / "sleeves" / sleeve_id / "versions" / f"{sleeve_version_id}.json", missing) if sleeve_version_id else {}
        health = _latest_for_sleeve(health_rows, sleeve_id, sleeve_version_id)
        challenge = _latest_for_sleeve(challenge_rows, sleeve_id, sleeve_version_id)
        review = _latest_for_sleeve(review_rows, sleeve_id, sleeve_version_id)
        candidate_ids = [str(item) for item in version.get("linked_candidate_batch_ids") or version_row.get("linked_candidate_batch_ids") or []]
        event_ids = [str(version.get("linked_event_study_evidence_package_id") or "")] if version.get("linked_event_study_evidence_package_id") else []
        backtest_ids = [str(version.get("linked_backtest_evidence_package_id") or "")] if version.get("linked_backtest_evidence_package_id") else []
        longitudinal_ids = [
            str(row.get("longitudinal_run_id") or "")
            for row in _read_registry(store, "longitudinal_candidate_runs.jsonl")
            if row.get("sleeve_id") == sleeve_id and row.get("sleeve_version_id") == sleeve_version_id
        ]
        trial_summaries = [row for row in paper_summaries if row.get("sleeve_id") == sleeve_id and row.get("sleeve_version_id") == sleeve_version_id]
        paper_trial_ids = [str(row.get("paper_trial_id") or "") for row in trial_summaries]
        active_trial = next((row for row in trial_summaries if row.get("status") == "active"), {})
        latest_trial = trial_summaries[-1] if trial_summaries else {}
        learning = _latest_longitudinal_report(store, sleeve_id, sleeve_version_id, missing)
        event_summary = _event_summary(store, event_ids[0], missing) if event_ids else {}
        backtest_summary = _backtest_summary(store, backtest_ids[0], missing) if backtest_ids else {}
        candidate_summary = _candidate_summary(store, candidate_ids[0], missing) if candidate_ids else {}
        operations = _latest_operations_report(store, str((active_trial or latest_trial).get("paper_trial_id") or ""), missing) if (active_trial or latest_trial) else {}
        latest_summary = {
            "event_count": event_summary.get("event_count"),
            "event_evidence_quality": event_summary.get("evidence_quality"),
            "post_cost_total_return": (backtest_summary.get("post_cost") or {}).get("total_return"),
            "excess_return_vs_benchmark": backtest_summary.get("excess_return_vs_benchmark"),
            "candidate_count": candidate_summary.get("candidate_count"),
            "longitudinal_total_candidates": learning.get("total_candidates"),
            "longitudinal_measured_candidates": learning.get("measured_candidates"),
            "longitudinal_zero_candidate_batches": learning.get("zero_candidate_batches"),
            "ranking_quality_status": learning.get("ranking_quality_status"),
            "paper_trial_recommended_next_action": operations.get("recommended_next_action"),
            "paper_trial_observation_count": operations.get("observation_count"),
            "paper_trial_measured_candidate_count": operations.get("measured_candidate_count"),
        }
        missing_global.extend(missing)
        sleeves.append(
            {
                "sleeve_id": sleeve_id,
                "sleeve_version_id": sleeve_version_id,
                "hypothesis_id": str(sleeve_row.get("hypothesis_id") or version.get("hypothesis_id") or ""),
                "overall_health": str(health.get("overall_health") or "missing"),
                "challenge_type": str(challenge.get("challenge_type") or ""),
                "recommended_action": str(challenge.get("recommended_action") or learning.get("recommended_next_action") or ""),
                "latest_review_decision": str(review.get("review_decision") or ""),
                "event_study_evidence_ids": sorted([item for item in event_ids if item]),
                "backtest_evidence_ids": sorted([item for item in backtest_ids if item]),
                "candidate_batch_ids": sorted(candidate_ids),
                "longitudinal_run_ids": sorted([item for item in longitudinal_ids if item]),
                "paper_trial_ids": sorted([item for item in paper_trial_ids if item]),
                "paper_trial_status": str((active_trial or latest_trial).get("status") or "missing"),
                "latest_summary": latest_summary,
                "missing_artifacts": missing,
            }
        )
    payload = {
        "created_at": created_at or utc_now_iso(),
        "created_by": created_by,
        "sleeve_count": len(sleeves),
        "evidence_package_count": len(_read_registry(store, "evidence_packages.jsonl")),
        "dataset_snapshot_count": len(_read_registry(store, "dataset_snapshots.jsonl")),
        "candidate_batch_count": len(_read_registry(store, "candidate_batches.jsonl")),
        "paper_trial_count": len({row.get("paper_trial_id") for row in paper_summaries}),
        "sleeves": sleeves,
        "research_label": RESEARCH_LABEL,
        "missing_artifacts": missing_global,
        "schema_version": "evidence_inventory.v1",
    }
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    payload["evidence_inventory_id"] = f"einv_{short_hash(payload['content_hash'], 12)}"
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_contract("evidence_inventory", payload)
    return payload


def evidence_inventory_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Evidence Inventory",
        "",
        payload["research_label"],
        "",
        f"- sleeve_count: {payload['sleeve_count']}",
        f"- evidence_package_count: {payload['evidence_package_count']}",
        f"- dataset_snapshot_count: {payload['dataset_snapshot_count']}",
        f"- candidate_batch_count: {payload['candidate_batch_count']}",
        f"- paper_trial_count: {payload['paper_trial_count']}",
        "",
        "## Sleeves",
    ]
    for row in payload["sleeves"]:
        lines.append(f"- {row['sleeve_id']}: health={row['overall_health']} paper_trial={row['paper_trial_status']} next={row['latest_summary'].get('paper_trial_recommended_next_action') or row['recommended_action']}")
    return "\n".join(lines) + "\n"


def write_evidence_inventory(*, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    payload = build_evidence_inventory(store_root=store, created_by=actor)
    row = {
        "evidence_inventory_id": payload["evidence_inventory_id"],
        "sleeve_count": payload["sleeve_count"],
        "content_hash": payload["content_hash"],
        "created_at": payload["created_at"],
        "schema_version": payload["schema_version"],
    }
    return _write_report(
        store=store,
        family="evidence_inventory",
        report_id=payload["evidence_inventory_id"],
        payload=payload,
        registry_name="evidence_inventories.jsonl",
        registry_row=row,
        markdown=evidence_inventory_markdown(payload),
        audit_action="evidence_inventory_written",
        actor=actor,
    )


def latest_evidence_inventory(*, store_root: Path | None = None) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    rows = read_jsonl(_registry(store, "evidence_inventories.jsonl"))
    if not rows:
        return {}
    latest = sorted(rows, key=lambda row: str(row.get("created_at") or ""))[-1]
    return read_json(store / "portfolio_reports" / "evidence_inventory" / f"{latest['evidence_inventory_id']}.json")
