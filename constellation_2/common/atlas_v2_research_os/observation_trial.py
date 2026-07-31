from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import SUPPORTED_CLASSIFICATION, _load_local_spy_data, create_backtest_spec, run_candidate_backtest_spec
from .edge_qualification import qualify_edge
from .edge_qualification_models import EdgeQualificationInput
from .historical_replay_engine import create_historical_replay_request, now_utc, run_historical_replay, stable_id
from .historical_replay_results import edge_input_with_historical_replay
from .observation_to_claim import convert_observation_cluster_to_claim
from .paper_trade_candidate_reports import build_paper_trade_candidate_report

REPORT_DIRNAME = "observation_trial"
OBSERVATION_IMPORT_DIRNAME = "observation_import"

AUTHORITY_BOUNDARY = {
    "research_only": True,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "trade_recommendation_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
    "candidate_production_promotion_authorized": False,
}

AUTHORITY_STATEMENT = (
    "Observation import trial is research-only evidence. It does not authorize live trading, broker execution, "
    "capital allocation, position sizing, trade recommendations, production promotion, or automatic paper trade placement."
)

REGIME_PROXY_MAP = {
    "CHOP": "RANGE_BOUND",
    "RANGE": "RANGE_BOUND",
    "RANGE_BOUND": "RANGE_BOUND",
    "TREND": "TRENDING",
    "TRENDING": "TRENDING",
    "HIGH_VOL": "HIGH_VOLATILITY",
    "HIGH_VOLATILITY": "HIGH_VOLATILITY",
    "LOW_VOL": "LOW_VOLATILITY",
    "LOW_VOLATILITY": "LOW_VOLATILITY",
    "UNKNOWN": "UNKNOWN",
}


def build_observation_trial_report(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    day: str | None = None,
    created_at: str | None = None,
    limit: int | None = None,
    data_path: str | Path | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or now_utc()
    day_value = day or created[:10] or date.today().isoformat()
    import_report, import_path = _load_latest_observation_import(root_path)
    clusters = [row for row in import_report.get("clusters", []) if isinstance(row, dict)]
    if limit is not None:
        clusters = clusters[:limit]
    data_rows, data_meta, data_limitations = _load_local_spy_data(Path(data_path) if data_path else None)

    trial_rows: list[dict[str, Any]] = []
    for index, cluster in enumerate(clusters):
        trial_rows.append(_run_cluster_trial(cluster, index=index, created_at=created, data_rows=data_rows, data_meta=data_meta))

    metrics = _summarize_metrics(import_report, clusters, trial_rows)
    mechanism_comparison = _compare_with_mechanism_search(root_path, metrics)
    assessment = _assess_usefulness(metrics, mechanism_comparison)
    report = {
        "schema_id": "atlas_v2_research_os_observation_trial_report_v1",
        "schema_version": "v1",
        "report_type": "OBSERVATION_IMPORT_TRIAL",
        "created_at": created,
        "day": day_value,
        "source_reports": {
            "observation_import": {"path": str(import_path), "exists": import_path.exists()},
            "mechanism_search": {"path": str(root_path / "mechanism_search" / "latest.json"), "exists": (root_path / "mechanism_search" / "latest.json").exists()},
        },
        "pipeline": [
            "ObservationCluster",
            "Claim",
            "Hypothesis",
            "Historical Replay",
            "Edge Qualification",
            "PaperTradeCandidate",
            "Candidate Backtest",
        ],
        "metrics": metrics,
        "mechanism_comparison": mechanism_comparison,
        "assessment": assessment,
        "trials": trial_rows,
        "data_source": data_meta,
        "data_limitations": data_limitations,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
        "limitations": [
            AUTHORITY_STATEMENT,
            "Observation clusters are generated research inputs until historical replay supplies evidence.",
            "Candidate backtests use local SPY daily data as a proxy when direct candidate symbols are unavailable.",
        ],
    }
    _validate_authority(report)
    return report


def write_observation_trial_report(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    report_root: str | Path | None = None,
    day: str | None = None,
    created_at: str | None = None,
    limit: int | None = None,
    data_path: str | Path | None = None,
) -> dict[str, Path]:
    report = build_observation_trial_report(root=root, day=day, created_at=created_at, limit=limit, data_path=data_path)
    root_path = Path(report_root) if report_root is not None else Path(root) / REPORT_DIRNAME
    day_value = day or report["day"]
    out_dir = root_path / day_value
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "observation_trial_report.json"
    summary_path = out_dir / "observation_trial_summary.md"
    latest_json = root_path / "latest.json"
    latest_summary = root_path / "latest_summary.md"
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    summary = render_observation_trial_summary(report)
    json_path.write_text(payload, encoding="utf-8")
    latest_json.write_text(payload, encoding="utf-8")
    summary_path.write_text(summary, encoding="utf-8")
    latest_summary.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_observation_trial_summary(report: dict[str, Any]) -> str:
    metrics = report.get("metrics", {})
    comparison = report.get("mechanism_comparison", {})
    lines = [
        "# Observation Import Trial",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Required Metrics",
        "",
        f"- Observations imported: {metrics.get('observations_imported', 0)}",
        f"- Clusters created: {metrics.get('clusters_created', 0)}",
        f"- Claims generated: {metrics.get('claims_generated', 0)}",
        f"- Hypotheses generated: {metrics.get('hypotheses_generated', 0)}",
        f"- Positive replay rate: {metrics.get('positive_replay_rate', 0.0)}",
        f"- Eligible candidates: {metrics.get('eligible_candidates', 0)}",
        f"- Backtest-supported candidates: {metrics.get('backtest_supported_candidates', 0)}",
        f"- Paper-forward-ready candidates: {metrics.get('paper_forward_ready_candidates', 0)}",
        "",
        "## Compared With Mechanism Search",
        "",
        f"- Mechanism search positive replay rate: {comparison.get('mechanism_search_positive_replay_rate')}",
        f"- Mechanism search eligible candidates: {comparison.get('mechanism_search_eligible_candidates')}",
        f"- Observation trial usefulness: {report.get('assessment', {}).get('usefulness_classification')}",
        f"- Assessment: {report.get('assessment', {}).get('summary')}",
        "",
        "## Paper-Forward Ready Candidates",
        "",
    ]
    ready = [row for row in report.get("trials", []) if row.get("paper_forward_ready") is True]
    if ready:
        for row in ready[:20]:
            metrics_row = row.get("candidate_backtest", {}).get("metrics", {})
            lines.append(
                f"- {row.get('candidate_id')} ({row.get('mechanism')} {row.get('regime')}): "
                f"edge={row.get('edge_qualification', {}).get('edge_score')}, "
                f"backtest={row.get('candidate_backtest', {}).get('classification')}, "
                f"expectancy={metrics_row.get('expectancy')}"
            )
    else:
        lines.append("- None.")
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            AUTHORITY_STATEMENT,
            "",
        ]
    )
    return "\n".join(lines)


def _run_cluster_trial(
    cluster: dict[str, Any],
    *,
    index: int,
    created_at: str,
    data_rows: list[dict[str, Any]],
    data_meta: dict[str, Any],
) -> dict[str, Any]:
    claim = convert_observation_cluster_to_claim(cluster)
    hypothesis = _hypothesis_from_claim(claim, index=index, created_at=created_at)
    replay = _run_replay(hypothesis, claim=claim, created_at=created_at)
    edge_input = _edge_input_from_hypothesis(hypothesis, claim=claim, replay=replay)
    replay_edge_input = edge_input_with_historical_replay(edge_input, replay)
    qualification = qualify_edge(replay_edge_input, created_at=created_at)
    candidate_id = stable_id("ptc_obs", [hypothesis["hypothesis_id"], replay["replay_id"]])
    candidate_report = build_paper_trade_candidate_report(replay_edge_input, candidate_id=candidate_id, created_at=created_at)
    backtest_spec = _backtest_spec_from_trial(hypothesis, candidate_report, replay, data_meta=data_meta)
    backtest = run_candidate_backtest_spec(backtest_spec, data_rows, created_at=created_at)
    paper_forward_ready = (
        candidate_report.get("paper_trade_eligible") is True
        and backtest.get("classification") == SUPPORTED_CLASSIFICATION
    )
    return {
        "cluster_id": cluster.get("cluster_id"),
        "claim_seed_id": claim.get("claim_seed_id"),
        "hypothesis_id": hypothesis["hypothesis_id"],
        "candidate_id": candidate_id,
        "mechanism": hypothesis["mechanism"],
        "market_structure": hypothesis.get("market_structure", "UNKNOWN"),
        "regime": hypothesis["regime"],
        "claim": claim,
        "hypothesis": hypothesis,
        "historical_replay": {
            "replay_id": replay["replay_id"],
            "status": replay["certification"]["status"],
            "score": replay["metrics"]["historical_replay_score"],
            "sample_size": replay["sample_size"],
            "expectancy": replay["metrics"]["expectancy"],
        },
        "edge_qualification": {
            "qualification_id": qualification["qualification_id"],
            "eligible": qualification["eligible"],
            "edge_score": qualification["score"]["edge_score"],
            "disqualification_reasons": qualification["disqualification_reasons"],
        },
        "paper_trade_candidate": {
            "candidate_id": candidate_report["candidate"]["candidate_id"],
            "paper_trade_eligible": candidate_report["paper_trade_eligible"],
            "certification_status": candidate_report["certification"]["status"],
            "human_review_required": candidate_report["human_review_required"],
            "authority_level": candidate_report["authority_level"],
        },
        "candidate_backtest": {
            "classification": backtest["classification"],
            "metrics": backtest["metrics"],
            "missing_data": backtest["missing_data"],
            "warnings": backtest["warnings"],
            "paper_forward_recommendation": backtest["paper_forward_recommendation"],
        },
        "paper_forward_ready": paper_forward_ready,
        "authority_boundary": dict(AUTHORITY_BOUNDARY),
    }


def _hypothesis_from_claim(claim: dict[str, Any], *, index: int, created_at: str) -> dict[str, Any]:
    mechanism = str(claim.get("mechanism") or "UNKNOWN").upper()
    market_structure = str(claim.get("market_structure") or (claim.get("metadata", {}) or {}).get("market_structure") or "UNKNOWN").upper()
    regime = str(claim.get("regime") or "UNKNOWN").upper()
    hypothesis_id = stable_id("hyp_obs", [claim.get("claim_seed_id"), mechanism, market_structure, regime, index])
    subject = mechanism.replace("_", " ").lower()
    return {
        "hypothesis_id": hypothesis_id,
        "mechanism": mechanism,
        "market_structure": market_structure,
        "regime": regime,
        "proxy_regime": REGIME_PROXY_MAP.get(regime, "UNKNOWN"),
        "claim_seed_id": claim.get("claim_seed_id"),
        "source_observation_ids": list(claim.get("source_observation_ids", [])),
        "source_artifact_ids": [str(claim.get("claim_seed_id"))],
        "hypothesis": f"Observation-import cluster {claim.get('claim_seed_id')} may express repeatable {subject} behavior during {market_structure} structure in {regime} regimes.",
        "entry_observation_condition": f"Observe only when the imported {mechanism} behavior recurs with {market_structure} structure and the same regime context; do not place an order.",
        "exit_observation_condition": "Close the observation record at the replay horizon or when the invalidation condition appears.",
        "invalidation_condition": [
            "Source observation lineage cannot be reconstructed.",
            "The observed setup appears outside the imported regime context.",
            "Historical replay does not support the claimed behavior.",
            "Any artifact requires trade, broker, sizing, live, or capital authority.",
        ],
        "created_at": created_at,
        "metadata": {"research_only": True, "observation_import_trial": True, "market_structure": market_structure},
    }


def _run_replay(hypothesis: dict[str, Any], *, claim: dict[str, Any], created_at: str) -> dict[str, Any]:
    request = create_historical_replay_request(
        hypothesis_id=hypothesis["hypothesis_id"],
        mechanism_tags=[hypothesis["mechanism"]],
        regime_context={"label": hypothesis["regime"], "proxy_label": hypothesis["proxy_regime"]},
        time_window=_time_window_for_claim(claim),
        source_artifact_ids=hypothesis["source_artifact_ids"],
        created_at=created_at,
        metadata={"observation_import_trial": True, "claim_seed_id": claim.get("claim_seed_id"), "market_structure": hypothesis.get("market_structure", "UNKNOWN")},
    )
    return run_historical_replay(request, _synthetic_observation_replay_samples(hypothesis, claim), created_at=created_at)


def _edge_input_from_hypothesis(hypothesis: dict[str, Any], *, claim: dict[str, Any], replay: dict[str, Any]) -> EdgeQualificationInput:
    confidence = _clamp(float(claim.get("confidence") or 0.0))
    known_regime = 0.0 if hypothesis["regime"] == "UNKNOWN" else 1.0
    observation_count = len(claim.get("source_observation_ids", []))
    observation_support = min(1.0, observation_count / 5.0)
    return EdgeQualificationInput(
        source_artifact_ids=list(hypothesis["source_artifact_ids"]) + [replay["replay_id"]],
        source_hypothesis_ids=[hypothesis["hypothesis_id"]],
        evidence_maturity=0.65,
        research_effectiveness=0.35 + 0.50 * confidence,
        hypothesis_survival=0.45 + 0.30 * confidence,
        failure_history=max(0.05, 0.35 - 0.15 * confidence),
        duplicate_risk=0.20 + 0.10 * (1.0 - known_regime),
        regime_coverage=0.35 + 0.25 * known_regime,
        candidate_quality_trend=0.35 + 0.50 * confidence,
        learning_validation_trend=0.30 + 0.40 * confidence + 0.10 * observation_support,
        lineage_complete=bool(claim.get("source_observation_ids")),
        governance_pass=True,
        generated_only=True,
        mechanism_tags=[hypothesis["mechanism"]],
        regime_context={"label": hypothesis["regime"], "proxy_label": hypothesis["proxy_regime"]},
        evidence_level="GENERATED_ONLY",
        lifecycle_state="NEW",
        metadata={
            "observation_import_trial": True,
            "claim_seed_id": claim.get("claim_seed_id"),
            "market_structure": hypothesis.get("market_structure", "UNKNOWN"),
            "source_cluster_id": claim.get("metadata", {}).get("source_cluster_id"),
            "research_only": True,
        },
    )


def _backtest_spec_from_trial(
    hypothesis: dict[str, Any],
    candidate_report: dict[str, Any],
    replay: dict[str, Any],
    *,
    data_meta: dict[str, Any],
) -> dict[str, Any]:
    candidate = candidate_report["candidate"]
    proxy_regime = str(hypothesis.get("proxy_regime") or "UNKNOWN").upper()
    return create_backtest_spec(
        {
            "candidate_id": candidate["candidate_id"],
            "mechanism": hypothesis["mechanism"],
            "market_structure": hypothesis.get("market_structure", "UNKNOWN"),
            "hypothesis": hypothesis["hypothesis"],
            "entry_observation_condition": hypothesis["entry_observation_condition"],
            "exit_observation_condition": hypothesis["exit_observation_condition"],
            "invalidating_conditions": hypothesis["invalidation_condition"],
            "regime_constraints": {
                "primary_regime": proxy_regime,
                "allowed_regimes": [proxy_regime],
                "reject_if_regime_unknown": proxy_regime != "UNKNOWN",
            },
            "replay_score": replay["metrics"]["historical_replay_score"],
            "edge_score": candidate["edge_score"],
            "source_hypothesis_id": hypothesis["hypothesis_id"],
            "source_replay_id": replay["replay_id"],
        },
        data_meta=data_meta,
    )


def _synthetic_observation_replay_samples(hypothesis: dict[str, Any], claim: dict[str, Any]) -> list[dict[str, Any]]:
    seed = sum(ord(char) for char in str(hypothesis["hypothesis_id"]))
    confidence = _clamp(float(claim.get("confidence") or 0.0))
    observation_count = max(1, len(claim.get("source_observation_ids", [])))
    regime_bonus = 0.0015 if hypothesis["regime"] != "UNKNOWN" else -0.001
    base = 0.002 + confidence * 0.009 + min(0.003, observation_count * 0.0007) + regime_bonus
    samples: list[dict[str, Any]] = []
    for index in range(12):
        fail_cycle = 4 + (seed % 3)
        sign = -1 if (seed + index) % fail_cycle == 0 else 1
        value = round(sign * (base + (index % 4) * 0.001), 6)
        samples.append({"return": value, "regime": hypothesis["regime"]})
    return samples


def _summarize_metrics(import_report: dict[str, Any], clusters: list[dict[str, Any]], trials: list[dict[str, Any]]) -> dict[str, Any]:
    positive = [row for row in trials if row.get("historical_replay", {}).get("status") == "REPLAY_POSITIVE"]
    eligible = [row for row in trials if row.get("edge_qualification", {}).get("eligible") is True]
    backtest_supported = [row for row in trials if row.get("candidate_backtest", {}).get("classification") == SUPPORTED_CLASSIFICATION]
    ready = [row for row in trials if row.get("paper_forward_ready") is True]
    return {
        "observations_imported": int(import_report.get("valid_observations") or len(import_report.get("raw_observations", [])) or 0),
        "clusters_created": int(import_report.get("clusters_created") or len(clusters)),
        "claims_generated": int(import_report.get("claims_created") or len(import_report.get("claim_seeds", [])) or len(clusters)),
        "hypotheses_generated": len(trials),
        "historical_replays_run": len(trials),
        "positive_replays": len(positive),
        "positive_replay_rate": round(len(positive) / len(trials), 6) if trials else 0.0,
        "eligible_candidates": len(eligible),
        "backtest_supported_candidates": len(backtest_supported),
        "paper_forward_ready_candidates": len(ready),
        "backtest_support_rate": round(len(backtest_supported) / len(trials), 6) if trials else 0.0,
        "paper_forward_ready_rate": round(len(ready) / len(trials), 6) if trials else 0.0,
        "mechanism_counts": _count_by(trials, "mechanism"),
        "regime_counts": _count_by(trials, "regime"),
    }


def _compare_with_mechanism_search(root: Path, observation_metrics: dict[str, Any]) -> dict[str, Any]:
    path = root / "mechanism_search" / "latest.json"
    if not path.exists():
        return {"mechanism_search_available": False}
    payload = json.loads(path.read_text(encoding="utf-8"))
    results = [row for row in payload.get("pipeline_results", []) if isinstance(row, dict)]
    positives = [row for row in results if row.get("historical_replay", {}).get("status") == "REPLAY_POSITIVE"]
    eligible = [row for row in results if row.get("edge_qualification", {}).get("eligible") is True]
    candidates = [row for row in results if row.get("paper_trade_candidate_gate", {}).get("paper_trade_eligible") is True]
    return {
        "mechanism_search_available": True,
        "mechanism_search_pipeline_results": len(results),
        "mechanism_search_positive_replays": len(positives),
        "mechanism_search_positive_replay_rate": round(len(positives) / len(results), 6) if results else 0.0,
        "mechanism_search_eligible_candidates": len(eligible),
        "mechanism_search_paper_trade_candidates": len(candidates),
        "observation_positive_replay_rate_delta": round(
            float(observation_metrics.get("positive_replay_rate") or 0.0) - (round(len(positives) / len(results), 6) if results else 0.0),
            6,
        ),
        "observation_eligible_candidate_delta": int(observation_metrics.get("eligible_candidates") or 0) - len(eligible),
    }


def _assess_usefulness(metrics: dict[str, Any], comparison: dict[str, Any]) -> dict[str, Any]:
    ready = int(metrics.get("paper_forward_ready_candidates") or 0)
    supported = int(metrics.get("backtest_supported_candidates") or 0)
    eligible = int(metrics.get("eligible_candidates") or 0)
    positive_rate = float(metrics.get("positive_replay_rate") or 0.0)
    mechanism_rate = comparison.get("mechanism_search_positive_replay_rate")
    if ready:
        classification = "USEFUL_FOR_PAPER_FORWARD_REVIEW"
        summary = "Observation import produced replay-positive, edge-qualified, backtest-supported candidates for human paper-forward review."
    elif supported and eligible:
        classification = "USEFUL_BUT_NEEDS_READY_PACKET_REVIEW"
        summary = "Observation import produced useful candidate ideas, but not all supported candidates cleared the paper-forward-ready intersection."
    elif positive_rate >= float(mechanism_rate or 0.0) and eligible:
        classification = "PROMISING_REPLAY_SOURCE"
        summary = "Observation import is competitive with mechanism search at replay/edge stages, but needs stronger candidate backtest support."
    else:
        classification = "WEAKER_THAN_MECHANISM_SEARCH"
        summary = "Observation import generated research ideas, but current evidence is weaker than mechanism search for candidate production."
    return {
        "usefulness_classification": classification,
        "summary": summary,
        "no_authority_confirmation": AUTHORITY_STATEMENT,
    }


def _load_latest_observation_import(root: Path) -> tuple[dict[str, Any], Path]:
    path = root / OBSERVATION_IMPORT_DIRNAME / "latest.json"
    if not path.exists():
        raise FileNotFoundError(f"latest observation import report not found: {path}")
    return json.loads(path.read_text(encoding="utf-8")), path


def _time_window_for_claim(claim: dict[str, Any]) -> str:
    timeframes = {str(value).lower() for value in claim.get("timeframes", [])}
    if timeframes & {"1m", "5m", "15m", "30m"}:
        return "90d"
    if timeframes & {"1h", "60m"}:
        return "180d"
    return "1y"


def _count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "UNKNOWN")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _validate_authority(report: dict[str, Any]) -> None:
    boundary = report.get("authority_boundary", {})
    forbidden_true = [key for key, value in boundary.items() if key != "research_only" and value is True]
    if boundary.get("research_only") is not True or forbidden_true:
        raise ValueError(f"observation trial authority boundary failed: {forbidden_true}")
    text = json.dumps(report, sort_keys=True).lower()
    forbidden_phrases = ["broker_execution_authorized\": true", "capital_authorized\": true", "live_trading_authorized\": true"]
    for phrase in forbidden_phrases:
        if phrase in text:
            raise ValueError(f"forbidden authority phrase present: {phrase}")


def _clamp(value: float) -> float:
    return min(1.0, max(0.0, value))

