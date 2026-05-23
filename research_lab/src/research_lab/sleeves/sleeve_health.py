from __future__ import annotations

from pathlib import Path
from typing import Any

from research_lab.audit.audit_log import write_audit_event
from research_lab.contracts.schemas import validate_contract
from research_lab.evidence.evidence_registry import load_evidence_package_manifest
from research_lab.sleeves.sleeve_registry import (
    append_registry_row,
    load_sleeve_version,
    sleeve_health_path,
)
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.manifest_io import read_json, write_json
from research_lab.storage.paths import ensure_store_layout, resolve_research_uri


def validate_sleeve_health_snapshot(snapshot: dict[str, Any]) -> None:
    validate_contract("sleeve_health_snapshot", snapshot)


def _backtest_health(summary: dict[str, Any]) -> tuple[str, str]:
    post_total = summary.get("post_cost", {}).get("total_return")
    excess = summary.get("excess_return_vs_benchmark")
    if post_total is None or excess is None:
        return "insufficient_data", "Backtest summary did not contain post-cost total return and benchmark-relative excess return."
    if float(post_total) <= 0:
        return "challenged", "Post-cost backtest total return is non-positive."
    if float(excess) < 0:
        return "watch", "Positive post-cost total return but underperformed benchmark."
    return "healthy", "Positive post-cost total return and positive benchmark-relative excess return."


def _candidate_health(candidate_batch_id: str, store: Path) -> tuple[str, str]:
    summary = read_json(store / "candidate_batches" / candidate_batch_id / "generation_summary.json")
    if int(summary.get("candidate_count", 0)) == 0:
        return "watch", "Candidate batch exists but generated zero candidates."
    return "healthy", "Candidate batch generated candidates."


def _attribution_health(candidate_batch_id: str, store: Path) -> tuple[str, str, list[str]]:
    path = store / "candidate_batches" / candidate_batch_id / "attribution_report.json"
    if not path.exists():
        return "insufficient_data", "No attribution report exists for linked candidate batch.", []
    report = read_json(path)
    if int(report.get("measured_candidate_count", 0)) == 0:
        return "watch", "Attribution report exists but measured zero candidates.", [report["attribution_report_id"]]
    return "healthy", "Attribution report has measured candidate outcomes.", [report["attribution_report_id"]]


def _overall(backtest: str, candidate: str, attribution: str, regime: str) -> str:
    values = [backtest, candidate, attribution, regime]
    if "challenged" in values:
        return "challenged"
    if "watch" in values:
        return "watch"
    if values.count("insufficient_data") >= 2:
        return "insufficient_data"
    if all(value == "healthy" for value in values):
        return "healthy"
    return "watch"


def build_sleeve_health_snapshot(
    *,
    sleeve_id: str,
    sleeve_version_id: str,
    as_of_date: str,
    store_root: Path | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    version = load_sleeve_version(sleeve_id, sleeve_version_id, store_root=store)
    backtest_manifest = load_evidence_package_manifest(version["linked_backtest_evidence_package_id"], store_root=store)
    summary = read_json(resolve_research_uri(backtest_manifest["summary_uri"], store_root=store))
    backtest_health, backtest_reason = _backtest_health(summary)
    candidate_batch_id = version["linked_candidate_batch_ids"][0] if version["linked_candidate_batch_ids"] else ""
    candidate_health, candidate_reason = _candidate_health(candidate_batch_id, store) if candidate_batch_id else ("insufficient_data", "No candidate batch linked.")
    attribution_health, attribution_reason, attribution_ids = _attribution_health(candidate_batch_id, store) if candidate_batch_id else ("insufficient_data", "No candidate batch linked.", [])
    regime_health = "healthy" if version.get("regime_snapshot_id") else "insufficient_data"
    regime_reason = "Regime snapshot is linked." if regime_health == "healthy" else "No regime snapshot linked."
    overall_health = _overall(backtest_health, candidate_health, attribution_health, regime_health)
    payload = {
        "sleeve_id": sleeve_id,
        "sleeve_version_id": sleeve_version_id,
        "as_of_date": as_of_date,
        "linked_evidence_package_ids": version["linked_evidence_package_ids"],
        "linked_candidate_batch_ids": version["linked_candidate_batch_ids"],
        "linked_attribution_report_ids": attribution_ids,
        "evidence_quality": backtest_manifest.get("evidence_quality", "unknown"),
        "backtest_health": backtest_health,
        "candidate_health": candidate_health,
        "attribution_health": attribution_health,
        "regime_health": regime_health,
        "overall_health": overall_health,
        "health_reasons": {
            "backtest_health": backtest_reason,
            "candidate_health": candidate_reason,
            "attribution_health": attribution_reason,
            "regime_health": regime_reason,
            "compliance_label": "Linked backtest/model evidence is hypothetical research evidence, not achieved portfolio performance.",
        },
        "created_at": created_at or utc_now_iso(),
        "schema_version": "sleeve_health_snapshot.v1",
    }
    seed_hash = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    payload["sleeve_health_snapshot_id"] = f"slvh_{sleeve_id}_{as_of_date.replace('-', '')}_{short_hash(seed_hash, 10)}"
    payload["content_hash"] = content_hash(payload, exclude={"created_at"}, sort_lists=True)
    validate_sleeve_health_snapshot(payload)
    return payload


def store_sleeve_health_snapshot(snapshot: dict[str, Any], *, store_root: Path | None = None, actor: str = "Aegis") -> dict[str, Any]:
    validate_sleeve_health_snapshot(snapshot)
    store = ensure_store_layout(store_root)
    write_json(sleeve_health_path(snapshot["sleeve_id"], snapshot["sleeve_health_snapshot_id"], store_root=store), snapshot, overwrite=False)
    row = {
        "sleeve_health_snapshot_id": snapshot["sleeve_health_snapshot_id"],
        "sleeve_id": snapshot["sleeve_id"],
        "sleeve_version_id": snapshot["sleeve_version_id"],
        "as_of_date": snapshot["as_of_date"],
        "overall_health": snapshot["overall_health"],
        "content_hash": snapshot["content_hash"],
        "created_at": snapshot["created_at"],
        "schema_version": snapshot["schema_version"],
    }
    append_registry_row("sleeve_health_snapshots.jsonl", row, store_root=store)
    write_audit_event(actor=actor, entity_type="sleeve_health_snapshot", entity_id=snapshot["sleeve_health_snapshot_id"], action="sleeve_health_evaluated", new_state_hash=snapshot["content_hash"], reason="Evaluated deterministic sleeve health.", metadata={"registry_row": row}, store_root=store)
    return row


def load_sleeve_health_snapshot(sleeve_id: str, sleeve_health_snapshot_id: str, *, store_root: Path | None = None) -> dict[str, Any]:
    return read_json(sleeve_health_path(sleeve_id, sleeve_health_snapshot_id, store_root=store_root))

