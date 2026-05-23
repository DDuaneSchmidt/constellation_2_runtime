from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

from research_lab.challengers.challenger_comparison import list_challenger_comparison_reports, load_challenger_comparison_report
from research_lab.challengers.challenger_evidence import list_challenger_evidence_batches, load_challenger_evidence_batch
from research_lab.challengers.human_review_decision import list_human_review_decisions, load_human_review_decision
from research_lab.challengers.human_review_dossier import list_human_review_dossiers, load_human_review_dossier
from research_lab.integrity.research_store_integrity import latest_integrity_report
from research_lab.observations.observation_candidate import build_observation_candidate
from research_lab.paper_trials.paper_trial_proposal import latest_paper_trial_proposal
from research_lab.status.research_os_status import latest_research_os_status_report
from research_lab.stability.sleeve_stability import latest_expectancy_drift_report, latest_regime_fragility_report, latest_sleeve_stability_report
from research_lab.storage.paths import ensure_store_layout


SCANNER_VERSION = "observation_scanner.v1"
DEFAULT_SCANNERS = ["research_store", "challenger", "candidate", "market"]


def _latest(rows: list[dict[str, Any]], id_field: str) -> dict[str, Any] | None:
    return rows[-1] if rows else None


def _base_context(store: Path) -> dict[str, Any]:
    integrity = latest_integrity_report(store_root=store) or {}
    status = latest_research_os_status_report(store_root=store) or {}
    batch_row = _latest(list_challenger_evidence_batches(store_root=store), "challenger_evidence_batch_id")
    comparison_row = _latest(list_challenger_comparison_reports(store_root=store), "challenger_comparison_report_id")
    dossier_row = _latest(list_human_review_dossiers(store_root=store), "human_review_dossier_id")
    decision_row = _latest(list_human_review_decisions(store_root=store), "human_review_decision_id")
    return {
        "integrity": integrity,
        "status": status,
        "batch": load_challenger_evidence_batch(str(batch_row["challenger_evidence_batch_id"]), store_root=store) if batch_row else {},
        "comparison": load_challenger_comparison_report(str(comparison_row["challenger_comparison_report_id"]), store_root=store) if comparison_row else {},
        "dossier": load_human_review_dossier(str(dossier_row["human_review_dossier_id"]), store_root=store) if dossier_row else {},
        "decision": load_human_review_decision(str(decision_row["human_review_decision_id"]), store_root=store) if decision_row else {},
        "proposal": latest_paper_trial_proposal(store_root=store) or {},
        "drift": latest_expectancy_drift_report(store_root=store) or {},
        "fragility": latest_regime_fragility_report(store_root=store) or {},
        "stability": latest_sleeve_stability_report(store_root=store) or {},
    }


def _source_ids(ctx: dict[str, Any]) -> dict[str, str]:
    return {
        "integrity_report_id": str(ctx["integrity"].get("integrity_report_id") or ""),
        "research_os_status_report_id": str(ctx["status"].get("research_os_status_report_id") or ""),
        "challenger_evidence_batch_id": str(ctx["batch"].get("challenger_evidence_batch_id") or ""),
        "challenger_comparison_report_id": str(ctx["comparison"].get("challenger_comparison_report_id") or ""),
        "human_review_dossier_id": str(ctx["dossier"].get("human_review_dossier_id") or ""),
        "human_review_decision_id": str(ctx["decision"].get("human_review_decision_id") or ""),
        "paper_trial_proposal_id": str(ctx["proposal"].get("paper_trial_proposal_id") or ""),
        "expectancy_drift_report_id": str(ctx["drift"].get("expectancy_drift_report_id") or ""),
        "regime_fragility_report_id": str(ctx["fragility"].get("regime_fragility_report_id") or ""),
        "sleeve_stability_report_id": str(ctx["stability"].get("sleeve_stability_report_id") or ""),
    }


def _candidate(
    ctx: dict[str, Any],
    *,
    generated_at: str,
    scanner_id: str,
    observation_type: str,
    observation_family: str,
    summary: str,
    severity: str = "medium",
    measured: dict[str, Any] | None = None,
    baseline: dict[str, Any] | None = None,
    blockers: list[dict[str, Any]] | None = None,
    trigger: list[dict[str, Any]] | None = None,
    anomaly_score: float = 1.0,
    novelty_score: float = 0.5,
    repeat_count: int = 1,
    symbol_or_asset_id: str = "research_store",
    event_date: str = "",
    source_refs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return build_observation_candidate(
        generated_at=generated_at,
        scanner_id=scanner_id,
        scanner_version=SCANNER_VERSION,
        observation_type=observation_type,
        observation_family=observation_family,
        symbol_or_asset_id=symbol_or_asset_id,
        universe_id="research_lab",
        event_date=event_date or generated_at[:10],
        observation_summary=summary,
        trigger_conditions=trigger or [],
        measured_values=measured or {},
        baseline_values=baseline or {},
        anomaly_score=anomaly_score,
        novelty_score=novelty_score,
        repeat_count=repeat_count,
        severity=severity,
        research_status="new",
        non_actionable_reason="Research-only anomaly candidate. Not a trading signal, recommendation, hypothesis, sleeve, paper trial, order, or allocation.",
        source_artifact_ids=_source_ids(ctx),
        source_data_refs=source_refs or [],
        latest_integrity_report_id=str(ctx["integrity"].get("integrity_report_id") or ""),
        latest_research_os_status_report_id=str(ctx["status"].get("research_os_status_report_id") or ""),
        blockers=blockers or [],
    )


def scan_research_store(ctx: dict[str, Any], *, generated_at: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    integrity = ctx["integrity"]
    status = ctx["status"]
    if status.get("overall_status") == "RED" or integrity.get("overall_status") == "FAIL":
        observations.append(
            _candidate(
                ctx,
                generated_at=generated_at,
                scanner_id="research_store_anomaly_scanner",
                observation_type="research_store_anomaly",
                observation_family="research_quality",
                summary=f"Research OS is {status.get('overall_status')} and integrity is {integrity.get('overall_status')}.",
                severity="high",
                measured={"research_os_status": status.get("overall_status"), "integrity_status": integrity.get("overall_status"), "blocker_count": len(status.get("current_blockers") or [])},
                baseline={"expected_research_os_status": "GREEN_OR_YELLOW", "expected_integrity_status": "PASS_OR_PASS_WITH_WARNINGS"},
                blockers=status.get("current_blockers") or [],
                trigger=[{"field": "research_os_status", "operator": "==", "value": "RED"}, {"field": "integrity_status", "operator": "==", "value": "FAIL"}],
                anomaly_score=1.0,
            )
        )
    duplicates = integrity.get("duplicate_registry_entries") or []
    if duplicates:
        observations.append(
            _candidate(
                ctx,
                generated_at=generated_at,
                scanner_id="research_store_anomaly_scanner",
                observation_type="research_store_anomaly",
                observation_family="research_quality",
                summary=f"Duplicate registry entries detected: {len(duplicates)} affected id(s).",
                severity="high",
                measured={"duplicate_registry_entry_count": len(duplicates), "duplicates": duplicates},
                baseline={"duplicate_registry_entry_count": 0},
                blockers=duplicates,
                trigger=[{"field": "duplicate_registry_entry_count", "operator": ">", "value": 0}],
            )
        )
    warnings = integrity.get("warnings") or []
    lineage_warnings = [row for row in warnings if "lineage" in str(row.get("message") or "") or "referenced artifact missing" in str(row.get("message") or "")]
    if lineage_warnings:
        observations.append(
            _candidate(
                ctx,
                generated_at=generated_at,
                scanner_id="research_store_anomaly_scanner",
                observation_type="research_store_anomaly",
                observation_family="research_quality",
                summary=f"Legacy lineage warning cluster detected: {len(lineage_warnings)} warning(s).",
                severity="medium",
                measured={"lineage_warning_count": len(lineage_warnings)},
                baseline={"lineage_warning_count": 0},
                blockers=lineage_warnings[:8],
                trigger=[{"field": "lineage_warning_count", "operator": ">", "value": 0}],
                anomaly_score=0.7,
            )
        )
    known = integrity.get("unresolved_blockers") or []
    if known:
        observations.append(
            _candidate(
                ctx,
                generated_at=generated_at,
                scanner_id="research_store_anomaly_scanner",
                observation_type="research_store_anomaly",
                observation_family="research_quality",
                summary=f"Known blocker cluster remains present: {len(known)} blocker(s).",
                severity="medium",
                measured={"known_blocker_count": len(known)},
                baseline={"known_blocker_count": 0},
                blockers=known,
                trigger=[{"field": "known_blocker_count", "operator": ">", "value": 0}],
                anomaly_score=0.6,
            )
        )
    proposal = ctx["proposal"]
    if proposal.get("proposal_status") == "blocked":
        observations.append(
            _candidate(
                ctx,
                generated_at=generated_at,
                scanner_id="research_store_anomaly_scanner",
                observation_type="research_store_anomaly",
                observation_family="research_quality",
                summary=f"Latest paper-trial proposal is blocked with {len(proposal.get('blockers') or [])} blocker(s).",
                severity="medium",
                measured={"proposal_status": proposal.get("proposal_status"), "blocker_count": len(proposal.get("blockers") or [])},
                baseline={"expected_proposal_status": "eligible_or_needs_review"},
                blockers=proposal.get("blockers") or [],
                trigger=[{"field": "paper_trial_proposal_status", "operator": "==", "value": "blocked"}],
                anomaly_score=0.6,
            )
        )
    return observations, {"scanner": "research_store", "status": "completed", "observation_count": len(observations)}


def scan_challenger(ctx: dict[str, Any], *, generated_at: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    batch = ctx["batch"]
    comparison = ctx["comparison"]
    decision = ctx["decision"]
    items = batch.get("challenger_evidence_items") or []
    materialized = [item for item in items if item.get("status") == "generated" and (item.get("evidence_completeness") or {}).get("complete") is True]
    if materialized:
        observations.append(
            _candidate(
                ctx,
                generated_at=generated_at,
                scanner_id="challenger_evidence_anomaly_scanner",
                observation_type="evidence_quality_anomaly",
                observation_family="research_quality",
                summary=f"{len(materialized)} challenger evidence chains are fully materialized for review.",
                severity="info",
                measured={"materialized_challenger_count": len(materialized)},
                baseline={"materialized_challenger_count": 0},
                trigger=[{"field": "materialized_challenger_count", "operator": ">", "value": 0}],
                anomaly_score=0.4,
            )
        )
    if decision.get("decision") == "reject_challenger":
        observations.append(
            _candidate(
                ctx,
                generated_at=generated_at,
                scanner_id="challenger_evidence_anomaly_scanner",
                observation_type="evidence_quality_anomaly",
                observation_family="research_quality",
                summary=f"Human review rejected challenger {decision.get('challenger_id')} after complete evidence review.",
                severity="medium",
                measured={"decision": decision.get("decision"), "challenger_id": decision.get("challenger_id")},
                baseline={"expected_next_state": "human_decision_recorded"},
                trigger=[{"field": "human_review_decision", "operator": "==", "value": "reject_challenger"}],
                anomaly_score=0.5,
            )
        )
    blocked = [item for item in items if item.get("failure_reason")]
    for item in blocked[:3]:
        observations.append(
            _candidate(
                ctx,
                generated_at=generated_at,
                scanner_id="challenger_evidence_anomaly_scanner",
                observation_type="evidence_quality_anomaly",
                observation_family="research_quality",
                summary=f"Challenger {item.get('challenger_hypothesis_id')} is blocked: {item.get('failure_reason')}.",
                severity="low",
                measured={"failure_reason": item.get("failure_reason"), "recoverable": (item.get("blocker") or {}).get("recoverable")},
                baseline={"blocked_hypotheses": 0},
                blockers=[item.get("blocker") or {"reason": item.get("failure_reason")}],
                trigger=[{"field": "challenger_status", "operator": "==", "value": "blocked"}],
                anomaly_score=0.4,
            )
        )
    rows = [row for row in comparison.get("comparison_table") or [] if row.get("entity_type") == "challenger" and row.get("expectancy_delta_vs_incumbent") is not None]
    if rows:
        top = sorted(rows, key=lambda row: float(row.get("expectancy_delta_vs_incumbent") or 0.0), reverse=True)[0]
        observations.append(
            _candidate(
                ctx,
                generated_at=generated_at,
                scanner_id="challenger_evidence_anomaly_scanner",
                observation_type="evidence_quality_anomaly",
                observation_family="research_quality",
                summary=f"Challenger {top.get('entity_id')} has the largest expectancy delta vs incumbent in the comparison table.",
                severity="info",
                measured={"expectancy_delta_vs_incumbent": top.get("expectancy_delta_vs_incumbent"), "challenger_id": top.get("entity_id")},
                baseline={"incumbent_expectancy_delta": 0.0},
                trigger=[{"field": "expectancy_delta_vs_incumbent", "operator": "rank", "value": "max"}],
                anomaly_score=0.4,
            )
        )
    return observations, {"scanner": "challenger", "status": "completed", "observation_count": len(observations)}


def scan_candidate(ctx: dict[str, Any], *, generated_at: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    drift = ctx["drift"]
    fragility = ctx["fragility"]
    stability = ctx["stability"]
    if drift.get("drift_status") in {"degrading", "watch"}:
        observations.append(
            _candidate(
                ctx,
                generated_at=generated_at,
                scanner_id="candidate_backtest_event_study_scanner",
                observation_type="evidence_quality_anomaly",
                observation_family="research_quality",
                summary=f"Latest expectancy drift status is {drift.get('drift_status')}.",
                severity="high" if drift.get("drift_status") == "degrading" else "medium",
                measured={"drift_status": drift.get("drift_status"), "candidate_count": drift.get("candidate_count")},
                baseline={"expected_drift_status": "stable_or_improving"},
                trigger=[{"field": "drift_status", "operator": "in", "value": ["degrading", "watch"]}],
                anomaly_score=0.8 if drift.get("drift_status") == "degrading" else 0.5,
            )
        )
    if fragility.get("fragility_status") in {"fragile", "watch"}:
        observations.append(
            _candidate(
                ctx,
                generated_at=generated_at,
                scanner_id="candidate_backtest_event_study_scanner",
                observation_type="regime_shift_candidate",
                observation_family="regime",
                summary=f"Latest regime fragility status is {fragility.get('fragility_status')}.",
                severity="high" if fragility.get("fragility_status") == "fragile" else "medium",
                measured={"fragility_status": fragility.get("fragility_status")},
                baseline={"expected_fragility_status": "robust"},
                trigger=[{"field": "fragility_status", "operator": "in", "value": ["fragile", "watch"]}],
                anomaly_score=0.8 if fragility.get("fragility_status") == "fragile" else 0.5,
            )
        )
    if stability.get("overall_stability_status") in {"degrading", "fragile", "watch"}:
        observations.append(
            _candidate(
                ctx,
                generated_at=generated_at,
                scanner_id="candidate_backtest_event_study_scanner",
                observation_type="evidence_quality_anomaly",
                observation_family="research_quality",
                summary=f"Latest sleeve stability is {stability.get('overall_stability_status')}.",
                severity="high" if stability.get("overall_stability_status") == "degrading" else "medium",
                measured={"overall_stability_status": stability.get("overall_stability_status")},
                baseline={"expected_stability_status": "stable"},
                trigger=[{"field": "overall_stability_status", "operator": "in", "value": ["degrading", "fragile", "watch"]}],
                anomaly_score=0.8,
            )
        )
        if not any(row.get("observation_type") == "regime_shift_candidate" for row in observations):
            observations.append(
                _candidate(
                    ctx,
                    generated_at=generated_at,
                    scanner_id="candidate_backtest_event_study_scanner",
                    observation_type="regime_shift_candidate",
                    observation_family="regime",
                    summary=f"Sleeve stability status {stability.get('overall_stability_status')} requires regime review even though a current fragility observation was not available.",
                    severity="medium",
                    measured={"overall_stability_status": stability.get("overall_stability_status"), "regime_fragility_report_id": stability.get("regime_fragility_report_id")},
                    baseline={"expected_stability_status": "stable", "expected_regime_review": "available"},
                    trigger=[{"field": "overall_stability_status", "operator": "in", "value": ["degrading", "fragile", "watch"]}],
                    anomaly_score=0.6,
                )
            )
    return observations, {"scanner": "candidate", "status": "completed", "observation_count": len(observations)}


def _market_csv(store: Path) -> Path | None:
    for rel in ["market_data/ohlcv.csv", "market_data/local_ohlcv.csv"]:
        path = store / rel
        if path.exists():
            return path
    return None


def _market_rows(path: Path) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = str(row.get("symbol") or "").strip()
            if not symbol:
                continue
            try:
                grouped[symbol].append(
                    {
                        "date": str(row.get("date") or row.get("session_date") or ""),
                        "open": float(row.get("open") or 0.0),
                        "close": float(row.get("close") or 0.0),
                        "volume": float(row.get("volume") or 0.0),
                    }
                )
            except ValueError:
                continue
    return {symbol: sorted(rows, key=lambda item: item["date"]) for symbol, rows in grouped.items()}


def scan_market(ctx: dict[str, Any], *, generated_at: str, store: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    path = _market_csv(store)
    if not path:
        return [], {"scanner": "market", "status": "market_data_unavailable", "observation_count": 0, "message": "No local OHLCV CSV found in research_store/market_data/."}
    observations: list[dict[str, Any]] = []
    grouped = _market_rows(path)
    for symbol, rows in grouped.items():
        if len(rows) < 22:
            continue
        latest = rows[-1]
        prior = rows[-2]
        history = rows[-22:-2]
        returns = [(history[i]["close"] / history[i - 1]["close"] - 1.0) for i in range(1, len(history)) if history[i - 1]["close"]]
        if len(returns) < 5:
            continue
        latest_return = latest["close"] / prior["close"] - 1.0 if prior["close"] else 0.0
        sigma = pstdev(returns) or 0.0
        avg_volume = mean([row["volume"] for row in history if row["volume"]]) if history else 0.0
        if sigma and abs(latest_return) >= 3.0 * sigma:
            observations.append(
                _candidate(
                    ctx,
                    generated_at=generated_at,
                    scanner_id="market_price_volume_scanner",
                    observation_type="abnormal_price_move",
                    observation_family="price_volume",
                    symbol_or_asset_id=symbol,
                    event_date=latest["date"],
                    summary=f"{symbol} daily return exceeded 3 rolling standard deviations.",
                    severity="medium",
                    measured={"latest_return": latest_return, "rolling_sigma": sigma},
                    baseline={"threshold": 3.0 * sigma},
                    trigger=[{"field": "absolute_daily_return", "operator": ">=", "value": "3_sigma"}],
                    anomaly_score=abs(latest_return) / sigma,
                    source_refs=[{"path": str(path), "type": "local_ohlcv_csv"}],
                )
            )
        if avg_volume and latest["volume"] >= 3.0 * avg_volume:
            observations.append(
                _candidate(
                    ctx,
                    generated_at=generated_at,
                    scanner_id="market_price_volume_scanner",
                    observation_type="abnormal_volume",
                    observation_family="price_volume",
                    symbol_or_asset_id=symbol,
                    event_date=latest["date"],
                    summary=f"{symbol} volume exceeded 3x the rolling 20-day average.",
                    severity="medium",
                    measured={"latest_volume": latest["volume"], "rolling_avg_volume": avg_volume},
                    baseline={"threshold": 3.0 * avg_volume},
                    trigger=[{"field": "volume", "operator": ">=", "value": "3x_20d_avg"}],
                    anomaly_score=latest["volume"] / avg_volume,
                    source_refs=[{"path": str(path), "type": "local_ohlcv_csv"}],
                )
            )
    return observations, {"scanner": "market", "status": "completed", "observation_count": len(observations), "source_path": str(path)}


def run_observation_scanners(
    *,
    store_root: Path | None = None,
    scanners: list[str] | None = None,
    generated_at: str,
    max_observations: int = 50,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    store = ensure_store_layout(store_root)
    requested = scanners or DEFAULT_SCANNERS
    if "all" in requested:
        requested = DEFAULT_SCANNERS
    ctx = _base_context(store)
    observations: list[dict[str, Any]] = []
    statuses: list[dict[str, Any]] = []
    if "research_store" in requested:
        rows, status = scan_research_store(ctx, generated_at=generated_at)
        observations.extend(rows)
        statuses.append(status)
    if "challenger" in requested:
        rows, status = scan_challenger(ctx, generated_at=generated_at)
        observations.extend(rows)
        statuses.append(status)
    if "candidate" in requested:
        rows, status = scan_candidate(ctx, generated_at=generated_at)
        observations.extend(rows)
        statuses.append(status)
    if "market" in requested:
        rows, status = scan_market(ctx, generated_at=generated_at, store=store)
        observations.extend(rows)
        statuses.append(status)
    observations = sorted(observations, key=lambda row: (str(row.get("severity") or ""), str(row.get("observation_type") or ""), str(row.get("observation_candidate_id") or "")))[:max_observations]
    return observations, statuses


def counts_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key) or "UNKNOWN") for row in rows).items()))
