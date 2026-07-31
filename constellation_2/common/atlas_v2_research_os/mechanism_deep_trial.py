from __future__ import annotations

import json
import statistics
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import SUPPORTED_CLASSIFICATION, _load_local_spy_data, create_backtest_spec, run_candidate_backtest_spec
from .historical_replay_engine import now_utc, stable_id
from .mechanism_hypothesis_generator import generate_mechanism_search_1000
from .mechanism_search_models import MECHANISM_FAMILIES

REPORT_ROOT = Path("reports/atlas_v2_research_os/mechanism_deep_trial")

AUTHORITY_BOUNDARY = {
    "live_trading_allowed": False,
    "broker_execution_allowed": False,
    "capital_allocation_allowed": False,
    "position_sizing_allowed": False,
    "portfolio_construction_allowed": False,
    "trade_recommendation_allowed": False,
    "automatic_paper_trade_placement_allowed": False,
    "candidate_production_promotion_allowed": False,
}


def run_mechanism_deep_trial(
    *,
    root: str | Path = DEFAULT_STORE_ROOT,
    created_at: str | None = None,
    run: dict[str, Any] | None = None,
    data_path: str | Path | None = None,
) -> dict[str, Any]:
    created = created_at or now_utc()
    root_path = Path(root)
    mechanism_run = run or generate_mechanism_search_1000(root=root_path, created_at=created)
    hypotheses = list(mechanism_run.get("hypotheses") or [])
    pipeline_results = list(mechanism_run.get("pipeline_results") or [])
    hypotheses_by_id = {str(row.get("hypothesis_id")): row for row in hypotheses}

    data_rows, data_meta, data_limitations = _load_local_spy_data(Path(data_path) if data_path else None)
    backtests = []
    for pipeline in pipeline_results:
        hypothesis = hypotheses_by_id.get(str(pipeline.get("hypothesis_id")), {})
        if not hypothesis:
            continue
        plan = _pipeline_backtest_plan(hypothesis, pipeline)
        spec = create_backtest_spec(plan, data_meta=data_meta)
        backtests.append(run_candidate_backtest_spec(spec, data_rows, created_at=created))

    mechanism_metrics = _mechanism_metrics(hypotheses, pipeline_results, backtests)
    ranked_mechanisms = sorted(
        mechanism_metrics,
        key=lambda row: (
            row.get("backtest_supported_candidates", 0),
            row.get("positive_replay_rate") or 0.0,
            row.get("average_edge_score") or 0.0,
            row.get("average_expectancy") or -999.0,
        ),
        reverse=True,
    )
    top_candidates = _top_candidates(pipeline_results, backtests)
    weak_mechanisms = [
        row for row in sorted(
            mechanism_metrics,
            key=lambda item: (
                item.get("positive_replay_rate") or 0.0,
                -(item.get("failure_rate") or 0.0),
                item.get("average_edge_score") or 0.0,
            ),
        )
        if (row.get("positive_replay_rate") or 0.0) <= 0.0 or (row.get("failure_rate") or 0.0) >= 0.45
    ]

    report = {
        "schema_id": "atlas_v2_research_os_mechanism_deep_trial_report_v1",
        "schema_version": "v1",
        "report_id": stable_id("mechanism_deep_trial", [created, mechanism_run.get("run_id")]),
        "created_at": created,
        "research_only": True,
        "authority_boundary": AUTHORITY_BOUNDARY,
        "input_run": {
            "run_id": mechanism_run.get("run_id"),
            "requested_limit": mechanism_run.get("requested_limit"),
            "emitted_count": mechanism_run.get("emitted_count"),
            "pipeline_result_count": len(pipeline_results),
            "governance_result": mechanism_run.get("governance_result"),
        },
        "pipeline": ["Historical Replay", "Edge Qualification", "PaperTradeCandidate Gate", "Candidate Backtest Replay"],
        "data_source": data_meta,
        "data_limitations": data_limitations,
        "mechanism_metrics": mechanism_metrics,
        "ranked_mechanisms": ranked_mechanisms,
        "top_candidates": top_candidates,
        "weak_mechanisms": weak_mechanisms,
        "candidate_backtests": backtests,
        "summary": {
            "hypotheses_generated": len(hypotheses),
            "historical_replays_executed": len(pipeline_results),
            "paper_trade_candidate_gates_checked": len(pipeline_results),
            "candidate_backtests_executed": len(backtests),
            "backtest_supported_candidates": sum(1 for row in backtests if row.get("classification") == SUPPORTED_CLASSIFICATION),
            "top_mechanism": ranked_mechanisms[0]["mechanism"] if ranked_mechanisms else None,
            "weak_mechanism_count": len(weak_mechanisms),
        },
        "limitations": [
            "Mechanism search emits 1000 hypotheses, while bounded replay/edge/candidate-gate evaluation is limited by the current pipeline replay cap.",
            "Candidate backtests are local proxy replays over SPY daily data when available, not direct instrument-specific or intraday validation.",
            "The report ranks research evidence only and does not authorize trading, capital, broker execution, position sizing, portfolio construction, paper placement, or production promotion.",
        ],
    }
    return report


def write_mechanism_deep_trial_report(report: dict[str, Any], *, report_root: str | Path = REPORT_ROOT, day: str | None = None) -> dict[str, Path]:
    report_day = day or str(report.get("created_at") or now_utc())[:10]
    root = Path(report_root)
    out_dir = root / report_day
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "mechanism_deep_trial_report.json"
    summary_path = out_dir / "mechanism_deep_trial_summary.md"
    latest_json = root / "latest.json"
    latest_summary = root / "latest_summary.md"
    summary = render_mechanism_deep_trial_summary(report)
    for target in [json_path, latest_json]:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for target in [summary_path, latest_summary]:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(summary, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_mechanism_deep_trial_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Atlas Mechanism Deep Trial",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Summary",
        "",
        f"- Hypotheses generated: {summary.get('hypotheses_generated', 0)}",
        f"- Historical replays executed: {summary.get('historical_replays_executed', 0)}",
        f"- Candidate gates checked: {summary.get('paper_trade_candidate_gates_checked', 0)}",
        f"- Candidate backtests executed: {summary.get('candidate_backtests_executed', 0)}",
        f"- Backtest-supported candidates: {summary.get('backtest_supported_candidates', 0)}",
        f"- Top mechanism: {summary.get('top_mechanism')}",
        "",
        "## Ranked Mechanisms",
        "",
    ]
    for index, row in enumerate(report.get("ranked_mechanisms", []), start=1):
        lines.append(
            f"{index}. {row.get('mechanism')}: hypotheses={row.get('hypotheses_generated')}, "
            f"positive_replay_rate={row.get('positive_replay_rate')}, "
            f"avg_edge={row.get('average_edge_score')}, eligible={row.get('eligible_candidates')}, "
            f"backtest_supported={row.get('backtest_supported_candidates')}, "
            f"expectancy={row.get('average_expectancy')}, profit_factor={row.get('profit_factor')}, "
            f"failure_rate={row.get('failure_rate')}"
        )
    lines.extend(["", "## Top Candidates", ""])
    for row in report.get("top_candidates", [])[:10]:
        lines.append(
            f"- {row.get('candidate_id')} ({row.get('mechanism')}): edge={row.get('edge_score')}, "
            f"replay={row.get('replay_status')}/{row.get('replay_score')}, "
            f"backtest={row.get('backtest_classification')}, expectancy={row.get('expectancy')}, "
            f"profit_factor={row.get('profit_factor')}"
        )
    lines.extend(["", "## Weak Mechanisms", ""])
    for row in report.get("weak_mechanisms", [])[:10]:
        lines.append(f"- {row.get('mechanism')}: positive_replay_rate={row.get('positive_replay_rate')}, failure_rate={row.get('failure_rate')}, avg_edge={row.get('average_edge_score')}")
    lines.extend(["", "## Data Limitations", ""])
    for limitation in report.get("data_limitations", []):
        lines.append(f"- {limitation}")
    lines.extend(["", "## Guardrails", "", "Research evidence only. No live trading, broker execution, capital allocation, position sizing, portfolio construction, trade recommendations, automatic paper trade placement, or production promotion authority.", ""])
    return "\n".join(lines)


def _pipeline_backtest_plan(hypothesis: dict[str, Any], pipeline: dict[str, Any]) -> dict[str, Any]:
    candidate_id = pipeline.get("paper_trade_candidate_gate", {}).get("candidate_id") or f"ptc-{hypothesis.get('hypothesis_id')}"
    replay = pipeline.get("historical_replay", {})
    edge = pipeline.get("edge_qualification", {})
    return {
        "candidate_id": candidate_id,
        "mechanism": hypothesis.get("mechanism"),
        "hypothesis": "; ".join(hypothesis.get("conditions", [])),
        "entry_observation_condition": hypothesis.get("entry_observation_rule"),
        "exit_observation_condition": hypothesis.get("exit_observation_rule"),
        "invalidating_condition": hypothesis.get("invalidation_rule"),
        "regime_constraints": {
            "primary_regime": hypothesis.get("regime") or "UNKNOWN",
            "allowed_regimes": ["UNKNOWN"],
            "proxy_backtest_unfiltered_by_generated_regime": True,
        },
        "replay_score": replay.get("score"),
        "edge_score": edge.get("edge_score"),
        "source_hypothesis_id": hypothesis.get("hypothesis_id"),
        "source_replay_id": replay.get("replay_id"),
    }


def _mechanism_metrics(hypotheses: list[dict[str, Any]], pipeline_results: list[dict[str, Any]], backtests: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_hypothesis = {str(row.get("hypothesis_id")): row for row in hypotheses}
    pipeline_by_mechanism: dict[str, list[dict[str, Any]]] = {mechanism: [] for mechanism in MECHANISM_FAMILIES}
    for row in pipeline_results:
        hypothesis = by_hypothesis.get(str(row.get("hypothesis_id")), {})
        mechanism = str(hypothesis.get("mechanism") or "UNKNOWN")
        pipeline_by_mechanism.setdefault(mechanism, []).append(row)
    backtests_by_mechanism: dict[str, list[dict[str, Any]]] = {mechanism: [] for mechanism in MECHANISM_FAMILIES}
    for row in backtests:
        backtests_by_mechanism.setdefault(str(row.get("mechanism") or "UNKNOWN"), []).append(row)

    rows: list[dict[str, Any]] = []
    for mechanism in MECHANISM_FAMILIES:
        h_rows = [row for row in hypotheses if row.get("mechanism") == mechanism]
        p_rows = pipeline_by_mechanism.get(mechanism, [])
        b_rows = backtests_by_mechanism.get(mechanism, [])
        replay_statuses = [row.get("historical_replay", {}).get("status") for row in p_rows]
        edge_scores = [_float(row.get("edge_qualification", {}).get("edge_score")) for row in p_rows]
        replay_failure_rates = [_float(row.get("historical_replay", {}).get("metrics", {}).get("failure_rate")) for row in p_rows]
        expectancies = [_float(row.get("metrics", {}).get("expectancy")) for row in b_rows]
        profit_factors = [_float(row.get("metrics", {}).get("profit_factor")) for row in b_rows]
        backtest_failure_rates = [_float(row.get("metrics", {}).get("failure_rate")) for row in b_rows]
        failure_values = [value for value in replay_failure_rates if value is not None] or [value for value in backtest_failure_rates if value is not None]
        rows.append(
            {
                "mechanism": mechanism,
                "hypotheses_generated": len(h_rows),
                "historical_replays_executed": len(p_rows),
                "positive_replay_rate": _rate(sum(1 for status in replay_statuses if status == "REPLAY_POSITIVE"), len(replay_statuses)),
                "average_edge_score": _average(edge_scores),
                "eligible_candidates": sum(1 for row in p_rows if row.get("paper_trade_candidate_gate", {}).get("paper_trade_eligible") is True),
                "backtest_supported_candidates": sum(1 for row in b_rows if row.get("classification") == SUPPORTED_CLASSIFICATION),
                "average_expectancy": _average(expectancies),
                "profit_factor": _average(profit_factors),
                "failure_rate": _average(failure_values),
                "candidate_backtests_executed": len(b_rows),
            }
        )
    return rows


def _top_candidates(pipeline_results: list[dict[str, Any]], backtests: list[dict[str, Any]]) -> list[dict[str, Any]]:
    backtest_by_candidate = {str(row.get("candidate_id")): row for row in backtests}
    rows = []
    for pipeline in pipeline_results:
        gate = pipeline.get("paper_trade_candidate_gate", {})
        candidate_id = str(gate.get("candidate_id"))
        backtest = backtest_by_candidate.get(candidate_id, {})
        metrics = backtest.get("metrics", {})
        rows.append(
            {
                "candidate_id": candidate_id,
                "hypothesis_id": pipeline.get("hypothesis_id"),
                "mechanism": backtest.get("mechanism") or _mechanism_from_candidate(candidate_id),
                "replay_status": pipeline.get("historical_replay", {}).get("status"),
                "replay_score": pipeline.get("historical_replay", {}).get("score"),
                "edge_score": pipeline.get("edge_qualification", {}).get("edge_score"),
                "paper_trade_eligible": gate.get("paper_trade_eligible"),
                "candidate_certification_status": gate.get("certification_status"),
                "backtest_classification": backtest.get("classification"),
                "expectancy": metrics.get("expectancy"),
                "profit_factor": metrics.get("profit_factor"),
                "failure_rate": metrics.get("failure_rate"),
            }
        )
    return sorted(rows, key=lambda row: (_float(row.get("edge_score")) or 0.0, _float(row.get("expectancy")) or -999.0), reverse=True)


def _mechanism_from_candidate(candidate_id: str) -> str:
    return "UNKNOWN"


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _average(values: list[float | None]) -> float | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return round(statistics.mean(clean), 6)


def _rate(count: int, total: int) -> float | None:
    if total <= 0:
        return None
    return round(count / total, 6)
