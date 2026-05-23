from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from research_lab.candidates.candidate_batch import (
    CANDIDATE_SCHEMA_VERSION,
    build_candidate_batch,
    validate_candidate,
)
from research_lab.candidates.candidate_scoring import RANKING_POLICY_VERSION, assign_ranking_buckets, score_candidate
from research_lab.costs.cost_model_registry import load_cost_model_snapshot
from research_lab.datasets.dataset_registry import load_dataset_snapshot
from research_lab.evidence.evidence_registry import load_evidence_package_manifest
from research_lab.regimes.regime_registry import load_regime_snapshot
from research_lab.storage.duckdb_query import canonical_parquet_path, load_dataset_snapshot_rows
from research_lab.storage.hashing import content_hash, short_hash, utc_now_iso
from research_lab.storage.parquet_io import read_parquet_records
from research_lab.storage.paths import ensure_store_layout, resolve_research_uri


def _date_text(value: Any) -> str:
    return str(value)[:10]


def _load_regime_by_date(regime_snapshot: dict[str, Any], store: Path) -> dict[str, dict[str, Any]]:
    labels_path = resolve_research_uri(regime_snapshot["labels"]["labels_uri"], store_root=store)
    return {_date_text(row["date"]): row for row in read_parquet_records(labels_path)}


def _rows_by_symbol_until(rows: list[dict[str, Any]], symbols: list[str], as_of_date: str) -> dict[str, list[dict[str, Any]]]:
    wanted = {symbol.upper() for symbol in symbols}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        symbol = str(row["symbol"]).upper()
        date = _date_text(row["date"])
        if symbol in wanted and date <= as_of_date:
            item = dict(row)
            item["symbol"] = symbol
            item["date"] = date
            grouped[symbol].append(item)
    for symbol in grouped:
        grouped[symbol].sort(key=lambda item: item["date"])
    return grouped


def _post_cost_expectancy_reference(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "evidence_type": manifest.get("evidence_type", "unknown"),
        "evidence_quality": manifest.get("evidence_quality", "unknown"),
        "event_count": manifest.get("event_count"),
        "trade_count": manifest.get("trade_count"),
        "summary_uri": manifest.get("summary_uri"),
        "hypothetical_performance_label": "research_simulation_not_live_results",
    }


def generate_drop_reversion_candidates(
    *,
    source_evidence_package_id: str,
    dataset_snapshot_id: str,
    regime_snapshot_id: str,
    cost_model_snapshot_id: str,
    as_of_date: str,
    threshold: float = -0.02,
    created_by: str = "Aegis",
    store_root: Path | None = None,
) -> dict[str, Any]:
    store = ensure_store_layout(store_root)
    evidence = load_evidence_package_manifest(source_evidence_package_id, store_root=store)
    dataset = load_dataset_snapshot(dataset_snapshot_id, store_root=store)
    regime = load_regime_snapshot(regime_snapshot_id, store_root=store)
    load_cost_model_snapshot(cost_model_snapshot_id, store_root=store)
    if not canonical_parquet_path(dataset_snapshot_id, store_root=store).exists():
        raise RuntimeError("Canonical parquet missing")
    symbols = sorted(str(symbol).upper() for symbol in dataset.get("symbols", []))
    if not symbols:
        raise RuntimeError("Dataset snapshot has no symbols")
    created_at = utc_now_iso()
    batch = build_candidate_batch(
        hypothesis_id=evidence["hypothesis_id"],
        source_evidence_package_id=source_evidence_package_id,
        dataset_snapshot_id=dataset_snapshot_id,
        regime_snapshot_id=regime_snapshot_id,
        cost_model_snapshot_id=cost_model_snapshot_id,
        symbols=symbols,
        as_of_date=as_of_date,
        threshold=threshold,
        created_at=created_at,
        created_by=created_by,
        ranking_policy_version=RANKING_POLICY_VERSION,
    )
    rows = load_dataset_snapshot_rows(dataset_snapshot_id, store_root=store)
    grouped = _rows_by_symbol_until(rows, symbols, as_of_date)
    if not any(grouped.values()):
        raise RuntimeError(f"No data available before as_of_date: {as_of_date}")
    regime_by_date = _load_regime_by_date(regime, store)
    candidates: list[dict[str, Any]] = []
    evaluated = 0
    for symbol in symbols:
        symbol_rows = grouped.get(symbol, [])
        if len(symbol_rows) < 2:
            continue
        evaluated += 1
        latest = symbol_rows[-1]
        prior = symbol_rows[-2]
        latest_adj = float(latest["adj_close"])
        prior_adj = float(prior["adj_close"])
        signal_return = latest_adj / prior_adj - 1.0
        if signal_return > float(threshold):
            continue
        signal_date = latest["date"]
        label = regime_by_date.get(signal_date, {})
        risk_regime = label.get("risk_regime") or "unknown"
        score = score_candidate(
            signal_return=signal_return,
            evidence_quality=evidence.get("evidence_quality", "unknown"),
            risk_regime=risk_regime,
        )
        stable = {
            "candidate_batch_id": batch["candidate_batch_id"],
            "symbol": symbol,
            "signal_date": signal_date,
            "signal_type": "daily_return_below_threshold",
            "signal_value": signal_return,
            "threshold": float(threshold),
        }
        candidate_id = f"cand_{symbol.lower()}_{signal_date.replace('-', '')}_{short_hash(content_hash(stable), 10)}"
        candidate = {
            "candidate_id": candidate_id,
            "candidate_batch_id": batch["candidate_batch_id"],
            "symbol": symbol,
            "sleeve_id": "research_drop_reversion_v1",
            "hypothesis_id": evidence["hypothesis_id"],
            "source_evidence_package_id": source_evidence_package_id,
            "signal_date": signal_date,
            "as_of_date": as_of_date,
            "signal_type": "daily_return_below_threshold",
            "signal_value": signal_return,
            "candidate_direction": "long",
            "candidate_status": "generated",
            "ranking_score": score,
            "ranking_bucket": "low",
            "why_now": f"{symbol} adjusted daily return {signal_return:.4f} breached threshold {float(threshold):.4f} using data through {signal_date}.",
            "risk_regime": risk_regime,
            "trend_regime": label.get("trend_regime") or "unknown",
            "vol_regime": label.get("vol_regime") or "unknown",
            "drawdown_regime": label.get("drawdown_regime") or "unknown",
            "expected_holding_period": "5 sessions",
            "evidence_quality": evidence.get("evidence_quality", "unknown"),
            "post_cost_expectancy_reference": _post_cost_expectancy_reference(evidence),
            "dataset_snapshot_id": dataset_snapshot_id,
            "regime_snapshot_id": regime_snapshot_id,
            "cost_model_snapshot_id": cost_model_snapshot_id,
            "created_at": created_at,
            "schema_version": CANDIDATE_SCHEMA_VERSION,
        }
        validate_candidate(candidate)
        candidates.append(candidate)
    ranked = assign_ranking_buckets(candidates)
    summary = {
        "candidate_count": len(ranked),
        "symbols_evaluated": evaluated,
        "symbols_triggered": [row["symbol"] for row in ranked],
        "as_of_date": as_of_date,
        "threshold": float(threshold),
        "source_evidence_package_id": source_evidence_package_id,
        "ranking_policy_version": RANKING_POLICY_VERSION,
        "top_candidates": ranked[:5],
        "zero_candidate_reason": None if ranked else "No latest symbol return breached the configured threshold.",
        "hypothetical_performance_notice": "Source evidence is research simulation/hypothetical performance, not live results or investment advice.",
        "schema_version": "candidate_generation_summary.v1",
    }
    return {"candidate_batch": batch, "candidates": ranked, "generation_summary": summary}

