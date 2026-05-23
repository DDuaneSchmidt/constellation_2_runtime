from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from research_lab.observations.observation_batch import latest_observation_candidate_batch, list_observation_candidate_batches, load_observation_candidate_batch
from research_lab.observations.observation_candidate import list_observation_candidates, load_observation_candidate
from research_lab.observations.observation_cluster import build_observation_cluster
from research_lab.storage.paths import ensure_store_layout


CLUSTERING_VERSION = "observation_clustering.v1"
SEVERITY_VALUES = {"high": 1.0, "medium": 0.6, "low": 0.3, "info": 0.1}


def _candidate_ids_from_batches(store: Path, *, latest_only: bool) -> tuple[list[str], list[dict[str, Any]]]:
    if latest_only:
        batch = latest_observation_candidate_batch(store_root=store)
        return (list(batch.get("observation_candidate_ids") or []), [batch]) if batch else ([], [])
    batches = [load_observation_candidate_batch(str(row["observation_candidate_batch_id"]), store_root=store) for row in list_observation_candidate_batches(store_root=store)]
    ids: list[str] = []
    for batch in batches:
        ids.extend(str(item) for item in batch.get("observation_candidate_ids") or [])
    return ids, batches


def _load_candidates(store: Path, ids: list[str]) -> list[dict[str, Any]]:
    rows = []
    seen: set[str] = set()
    for candidate_id in ids:
        if candidate_id in seen:
            continue
        seen.add(candidate_id)
        rows.append(load_observation_candidate(candidate_id, store_root=store))
    return rows


def _has_blocker(row: dict[str, Any], *tokens: str) -> bool:
    text = " ".join(str(value) for blocker in row.get("blockers") or [] for value in blocker.values())
    return any(token in text for token in tokens)


def _counts(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key) or "UNKNOWN") for row in rows).items()))


def _source_artifacts(rows: list[dict[str, Any]], batches: list[dict[str, Any]]) -> dict[str, Any]:
    source: dict[str, Any] = {}
    for batch in batches:
        for key, value in (batch.get("source_artifact_ids") or {}).items():
            if value:
                source.setdefault(key, value)
    for row in rows:
        for key, value in (row.get("source_artifact_ids") or {}).items():
            if value:
                source.setdefault(key, value)
    return source


def _scores(rows: list[dict[str, Any]], *, blocked: bool = False, legacy: bool = False) -> dict[str, float]:
    recurrence = len(rows)
    severity_score = max([SEVERITY_VALUES.get(str(row.get("severity")), 0.0) for row in rows] or ([0.6] if blocked else [0.0]))
    persistence_score = min(1.0, recurrence / 5.0)
    novelty_score = 0.2 if legacy else 1.0
    cluster_score = round((severity_score * 0.4) + (persistence_score * 0.4) + (novelty_score * 0.2), 6)
    return {
        "severity_score": severity_score,
        "persistence_score": persistence_score,
        "novelty_score": novelty_score,
        "cluster_score": cluster_score,
        "research_priority_score": 0.0 if legacy else cluster_score,
    }


def _status(rows: list[dict[str, Any]], *, blocked: bool = False) -> str:
    if blocked:
        return "blocked"
    if len(rows) >= 3:
        return "recurring"
    return "active"


def _research_status(cluster_status: str, score: float) -> str:
    if cluster_status == "legacy":
        return "ignored_legacy"
    if score >= 0.6:
        return "candidate_for_later_hypothesis_review"
    if cluster_status == "recurring":
        return "monitor"
    return "observe"


def _cluster(
    *,
    rows: list[dict[str, Any]],
    batches: list[dict[str, Any]],
    generated_at: str,
    family: str,
    label: str,
    summary: str,
    blocked: bool = False,
    blockers: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    score = _scores(rows, blocked=blocked)
    cluster_status = _status(rows, blocked=blocked)
    latest_integrity = next((str(row.get("latest_integrity_report_id") or "") for row in rows if row.get("latest_integrity_report_id")), "")
    latest_status = next((str(row.get("latest_research_os_status_report_id") or "") for row in rows if row.get("latest_research_os_status_report_id")), "")
    if not latest_integrity:
        latest_integrity = next((str(batch.get("latest_integrity_report_id") or "") for batch in batches if batch.get("latest_integrity_report_id")), "")
    if not latest_status:
        latest_status = next((str(batch.get("latest_research_os_status_report_id") or "") for batch in batches if batch.get("latest_research_os_status_report_id")), "")
    fallback_seen_at = [str(batch.get("generated_at") or "") for batch in batches if batch.get("generated_at")] or [generated_at]
    return build_observation_cluster(
        generated_at=generated_at,
        cluster_status=cluster_status,
        cluster_family=family,
        cluster_label=label,
        cluster_summary=summary,
        observation_candidate_ids=[str(row["observation_candidate_id"]) for row in rows],
        observation_candidate_batch_ids=[str(batch["observation_candidate_batch_id"]) for batch in batches],
        observation_type_counts=_counts(rows, "observation_type"),
        severity_counts=_counts(rows, "severity"),
        first_seen_at=min([str(row.get("generated_at") or generated_at) for row in rows] or fallback_seen_at),
        last_seen_at=max([str(row.get("generated_at") or generated_at) for row in rows] or fallback_seen_at),
        recurrence_count=len(rows),
        unique_symbol_or_asset_count=len({str(row.get("symbol_or_asset_id") or "") for row in rows if row.get("symbol_or_asset_id")}),
        universe_ids=[str(row.get("universe_id") or "") for row in rows if row.get("universe_id")],
        source_scanner_ids=[str(row.get("scanner_id") or "") for row in rows if row.get("scanner_id")],
        source_artifact_ids=_source_artifacts(rows, batches),
        latest_integrity_report_id=latest_integrity,
        latest_research_os_status_report_id=latest_status,
        cluster_score=score["cluster_score"],
        novelty_score=score["novelty_score"],
        persistence_score=score["persistence_score"],
        severity_score=score["severity_score"],
        research_priority_score=score["research_priority_score"],
        research_status=_research_status(cluster_status, score["research_priority_score"]),
        blockers=blockers or [],
    )


def cluster_observations(
    *,
    store_root: Path | None = None,
    latest_only: bool = True,
    generated_at: str,
    window_days: int = 30,
    max_clusters: int = 50,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    store = ensure_store_layout(store_root)
    ids, batches = _candidate_ids_from_batches(store, latest_only=latest_only)
    rows = _load_candidates(store, ids)
    clusters: list[dict[str, Any]] = []

    integrity_rows = [
        row
        for row in rows
        if row.get("observation_type") == "research_store_anomaly"
        or _has_blocker(row, "integrity_status_fail", "duplicate_paper_trial_registry_entry", "research_os_status_red", "lineage")
    ]
    if integrity_rows:
        clusters.append(
            _cluster(
                rows=integrity_rows,
                batches=batches,
                generated_at=generated_at,
                family="research_store_integrity",
                label="Research OS integrity blockers",
                summary="Research Store and Research OS observations indicate recurring integrity/governance blockers.",
                blockers=[blocker for row in integrity_rows for blocker in row.get("blockers") or []][:12],
            )
        )

    evidence_rows = [row for row in rows if row.get("observation_type") == "evidence_quality_anomaly"]
    if evidence_rows:
        clusters.append(
            _cluster(
                rows=evidence_rows,
                batches=batches,
                generated_at=generated_at,
                family="evidence_quality",
                label="Challenger evidence quality observations",
                summary="Challenger evidence, comparison, dossier, or decision observations appear related.",
            )
        )

    regime_rows = [row for row in rows if row.get("observation_type") == "regime_shift_candidate"]
    if regime_rows:
        clusters.append(
            _cluster(
                rows=regime_rows,
                batches=batches,
                generated_at=generated_at,
                family="regime_behavior",
                label="Regime behavior observations",
                summary="Regime or stability observations indicate possible fragility themes for later research review.",
            )
        )

    for batch in batches:
        for status in batch.get("scanner_statuses") or []:
            if status.get("scanner") == "market" and status.get("status") == "market_data_unavailable":
                clusters.append(
                    _cluster(
                        rows=[],
                        batches=[batch],
                        generated_at=generated_at,
                        family="market_price_volume",
                        label="Market anomaly scanner unavailable",
                        summary="Market price/volume anomaly scanner did not run because local OHLCV input was unavailable.",
                        blocked=True,
                        blockers=[{"blocker_code": "market_data_unavailable", "blocker_message": status.get("message") or "Local market data unavailable.", "severity": "WARNING", "recoverable": True}],
                    )
                )

    by_symbol_family: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        symbol = str(row.get("symbol_or_asset_id") or "")
        if symbol and symbol != "research_store":
            by_symbol_family[(symbol, str(row.get("observation_family") or "unknown"))].append(row)
    for (symbol, family_name), symbol_rows in sorted(by_symbol_family.items()):
        if len(symbol_rows) >= 2:
            clusters.append(
                _cluster(
                    rows=symbol_rows,
                    batches=batches,
                    generated_at=generated_at,
                    family="market_price_volume" if family_name == "price_volume" else "cross_sectional_behavior",
                    label=f"{symbol} recurring {family_name} observations",
                    summary=f"{symbol} has repeated observations in the {family_name} family.",
                )
            )

    by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_type[str(row.get("observation_type") or "unknown")].append(row)
    for observation_type, type_rows in sorted(by_type.items()):
        if len(type_rows) >= 3 and observation_type not in {"research_store_anomaly", "evidence_quality_anomaly"}:
            clusters.append(
                _cluster(
                    rows=type_rows,
                    batches=batches,
                    generated_at=generated_at,
                    family="unknown",
                    label=f"Recurring {observation_type}",
                    summary=f"{observation_type} appeared {len(type_rows)} times in the scanned observation window.",
                )
            )

    clusters = sorted(clusters, key=lambda row: (-float(row.get("research_priority_score") or 0.0), str(row.get("cluster_family") or ""), str(row.get("cluster_label") or "")))
    return clusters[:max_clusters], batches


def counts_by(clusters: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key) or "UNKNOWN") for row in clusters).items()))
