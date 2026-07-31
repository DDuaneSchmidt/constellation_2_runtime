from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT
from .candidate_backtests import (
    INSUFFICIENT_DATA_CLASSIFICATION,
    SUPPORTED_CLASSIFICATION,
    WEAK_CLASSIFICATION,
    _load_local_spy_data,
    create_backtest_spec,
    run_candidate_backtest_spec,
)

REPORT_ROOT_NAME = "top_candidate_deep_backtest"
SOURCE_REPORTS = {
    "paper_forward_campaign": "paper_forward_campaign/latest.json",
    "observation_trial": "observation_trial/latest.json",
    "candidate_backtests": "candidate_backtests/latest.json",
}
AUTHORITY_BOUNDARY = {
    "paper_forward_observation_only": True,
    "research_only": True,
    "live_trading_authorized": False,
    "broker_execution_authorized": False,
    "capital_authorized": False,
    "position_sizing_authorized": False,
    "trade_recommendation_authorized": False,
    "candidate_promotion_authorized": False,
    "production_promotion_authorized": False,
    "automatic_paper_trade_placement_authorized": False,
}
AUTHORITY_STATEMENT = (
    "Top candidate deep backtest ranks candidates for paper-forward observation only. "
    "It does not authorize live trading, broker execution, capital allocation, position sizing, "
    "trade recommendations, candidate promotion, production promotion, or automatic paper trade placement."
)


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run_top_candidate_deep_backtest_report(
    *,
    root: Path | str = DEFAULT_STORE_ROOT,
    top_n: int = 20,
    data_path: Path | str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    report = build_top_candidate_deep_backtest_report(
        root=root,
        top_n=top_n,
        data_path=data_path,
        created_at=created_at,
    )
    write_top_candidate_deep_backtest_report(report, root=root)
    return report


def build_top_candidate_deep_backtest_report(
    *,
    root: Path | str = DEFAULT_STORE_ROOT,
    top_n: int = 20,
    data_path: Path | str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or now_utc()
    source_payloads = _load_source_reports(root_path)
    candidates = _collect_candidate_pool(source_payloads)
    selected = _select_top_candidates(candidates, top_n=top_n)
    rows, data_meta, data_limitations = _load_local_spy_data(Path(data_path) if data_path else None)

    results: list[dict[str, Any]] = []
    for rank, candidate in enumerate(selected, start=1):
        spec = _candidate_to_backtest_spec(candidate, data_meta)
        try:
            backtest = run_candidate_backtest_spec(spec, rows, created_at=created)
        except Exception as exc:  # pragma: no cover - defensive report path
            backtest = {
                "candidate_id": spec.get("candidate_id"),
                "mechanism": spec.get("mechanism"),
                "classification": "BACKTEST_FAILED",
                "metrics": {},
                "missing_data": [],
                "warnings": [],
                "errors": [str(exc)],
                "paper_forward_recommendation": "human review required before observation logging",
                "research_only": True,
            }
        results.append(_summarize_deep_result(rank, candidate, backtest))

    ranked = sorted(results, key=_deep_rank_key)
    for rank, row in enumerate(ranked, start=1):
        row["rank"] = rank

    summary = _summary(ranked, candidates, selected, data_meta, data_limitations)
    return {
        "schema_id": "atlas_v2_research_os_top_candidate_deep_backtest_v1",
        "schema_version": "1.0",
        "report_type": "TOP_CANDIDATE_DEEP_BACKTEST",
        "created_at": created,
        "source_reports": _source_report_metadata(root_path),
        "authority_boundary": AUTHORITY_BOUNDARY,
        "authority_statement": AUTHORITY_STATEMENT,
        "selection_policy": {
            "input_sources": list(SOURCE_REPORTS),
            "candidate_pool_count": len(candidates),
            "selected_for_deep_backtest": len(selected),
            "top_n": top_n,
            "dedupe_key": "candidate_id",
            "ranking_use": "paper-forward observation only",
        },
        "data_source": data_meta,
        "data_limitations": data_limitations,
        "candidates": ranked,
        "final_ranked_list_for_paper_forward_observation_only": [
            {
                "rank": row["rank"],
                "candidate_id": row["candidate_id"],
                "mechanism": row["mechanism"],
                "status": row["status"],
                "expectancy": row["expectancy"],
                "profit_factor": row["profit_factor"],
                "sample_size": row["sample_size"],
                "max_drawdown": row["max_drawdown"],
                "paper_forward_observation_only": True,
            }
            for row in ranked
        ],
        "summary": summary,
    }


def write_top_candidate_deep_backtest_report(report: dict[str, Any], *, root: Path | str = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    root_path = Path(root)
    created = str(report.get("created_at") or now_utc())
    day = created[:10]
    report_dir = root_path / REPORT_ROOT_NAME / day
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / "top_candidate_deep_backtest.json"
    summary_path = report_dir / "top_candidate_deep_backtest_summary.md"
    latest_json = root_path / REPORT_ROOT_NAME / "latest.json"
    latest_summary = root_path / REPORT_ROOT_NAME / "latest_summary.md"
    summary_md = render_top_candidate_deep_backtest_summary(report)
    for path, payload in [(json_path, report), (latest_json, report)]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for path in [summary_path, latest_summary]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(summary_md, encoding="utf-8")
    return {"json": json_path, "summary": summary_path, "latest_json": latest_json, "latest_summary": latest_summary}


def render_top_candidate_deep_backtest_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    lines = [
        "# Top Candidate Deep Backtest",
        "",
        f"Created: {report.get('created_at')}",
        "",
        "## Summary",
        "",
        f"- Candidate pool: {summary.get('candidate_pool_count')}",
        f"- Candidates deep-tested: {summary.get('candidates_deep_tested')}",
        f"- Supported: {summary.get('supported_count')}",
        f"- Weakened: {summary.get('weakened_count')}",
        f"- Insufficient data: {summary.get('insufficient_data_count')}",
        "",
        "## Final Ranked List For Paper-Forward Observation Only",
        "",
    ]
    for row in report.get("final_ranked_list_for_paper_forward_observation_only", []):
        lines.append(
            "- rank {rank}: {candidate_id} ({mechanism}) status={status} expectancy={expectancy} "
            "profit_factor={profit_factor} sample_size={sample_size} max_drawdown={max_drawdown}".format(**row)
        )
    lines.extend(["", "## Failure Modes", ""])
    for row in report.get("candidates", []):
        modes = row.get("failure_modes") or []
        lines.append(f"- {row.get('candidate_id')}: {', '.join(modes) if modes else 'none flagged'}")
    lines.extend(["", "## Authority", "", AUTHORITY_STATEMENT, ""])
    return "\n".join(lines)


def _load_source_reports(root: Path) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for name, relative in SOURCE_REPORTS.items():
        path = root / relative
        if not path.exists():
            payloads[name] = {}
            continue
        try:
            payloads[name] = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payloads[name] = {}
    return payloads


def _source_report_metadata(root: Path) -> dict[str, dict[str, Any]]:
    return {name: {"path": str(root / relative), "exists": (root / relative).exists()} for name, relative in SOURCE_REPORTS.items()}


def _collect_candidate_pool(source_payloads: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    campaign = source_payloads.get("paper_forward_campaign") or {}
    for row in campaign.get("campaign_candidates") or []:
        if isinstance(row, dict):
            candidates.append(_normalize_candidate(row, "paper_forward_campaign", row.get("rank")))

    trial = source_payloads.get("observation_trial") or {}
    for index, row in enumerate(trial.get("trials") or [], start=1):
        if isinstance(row, dict):
            candidates.append(_normalize_observation_trial_candidate(row, index))

    backtests = source_payloads.get("candidate_backtests") or {}
    for index, row in enumerate(backtests.get("candidates") or [], start=1):
        if isinstance(row, dict):
            candidates.append(_normalize_backtest_candidate(row, index))

    return _dedupe_candidates(candidates)


def _normalize_candidate(row: dict[str, Any], source: str, source_rank: Any = None) -> dict[str, Any]:
    return {
        "candidate_id": str(row.get("candidate_id") or row.get("paper_trade_candidate_id") or ""),
        "mechanism": str(row.get("mechanism") or "UNKNOWN").upper(),
        "source": source,
        "source_rank": _int_or_none(source_rank),
        "source_buckets": list(row.get("source_buckets") or [source]),
        "edge_score": _float_or_none(row.get("edge_score")),
        "replay_score": _float_or_none(row.get("replay_score")),
        "expectancy": _float_or_none(row.get("expectancy")),
        "profit_factor": _float_or_none(row.get("profit_factor")),
        "status": row.get("status") or row.get("backtest_classification"),
        "regime": str(row.get("regime") or "UNKNOWN").upper(),
        "proxy_regime": _proxy_regime(row.get("regime")),
        "hypothesis": row.get("hypothesis") or row.get("why_selected") or "",
        "entry_observation_condition": row.get("observation_rule") or "",
        "exit_observation_condition": row.get("exit_observation_condition") or "",
        "invalidation_condition": row.get("invalidation_rule") or "",
        "backtest_spec": row.get("backtest_spec") if isinstance(row.get("backtest_spec"), dict) else None,
        "raw": row,
    }


def _normalize_observation_trial_candidate(row: dict[str, Any], source_rank: int) -> dict[str, Any]:
    hypothesis = row.get("hypothesis") if isinstance(row.get("hypothesis"), dict) else {}
    backtest = row.get("candidate_backtest") if isinstance(row.get("candidate_backtest"), dict) else {}
    metrics = backtest.get("metrics") if isinstance(backtest.get("metrics"), dict) else {}
    replay = row.get("historical_replay") if isinstance(row.get("historical_replay"), dict) else {}
    edge = row.get("edge_qualification") if isinstance(row.get("edge_qualification"), dict) else {}
    normalized = _normalize_candidate(
        {
            "candidate_id": row.get("candidate_id"),
            "mechanism": row.get("mechanism") or hypothesis.get("mechanism"),
            "rank": source_rank,
            "edge_score": edge.get("edge_score"),
            "replay_score": replay.get("score"),
            "expectancy": metrics.get("expectancy"),
            "profit_factor": metrics.get("profit_factor"),
            "status": "PAPER_FORWARD_READY" if row.get("paper_forward_ready") else backtest.get("classification"),
            "regime": row.get("regime") or hypothesis.get("proxy_regime") or hypothesis.get("regime"),
            "hypothesis": hypothesis.get("hypothesis"),
            "observation_rule": hypothesis.get("entry_observation_condition"),
            "exit_observation_condition": hypothesis.get("exit_observation_condition"),
            "invalidation_rule": hypothesis.get("invalidation_condition"),
        },
        "observation_trial",
        source_rank,
    )
    normalized["proxy_regime"] = str(hypothesis.get("proxy_regime") or normalized["proxy_regime"]).upper()
    return normalized


def _normalize_backtest_candidate(row: dict[str, Any], source_rank: int) -> dict[str, Any]:
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    spec = row.get("backtest_spec") if isinstance(row.get("backtest_spec"), dict) else {}
    normalized = _normalize_candidate(
        {
            "candidate_id": row.get("candidate_id"),
            "mechanism": row.get("mechanism") or spec.get("mechanism"),
            "rank": source_rank,
            "edge_score": spec.get("edge_score"),
            "replay_score": spec.get("replay_score"),
            "expectancy": metrics.get("expectancy"),
            "profit_factor": metrics.get("profit_factor"),
            "status": row.get("classification"),
            "regime": spec.get("primary_regime"),
            "hypothesis": spec.get("hypothesis"),
            "observation_rule": spec.get("entry_observation_condition"),
            "exit_observation_condition": spec.get("exit_observation_condition"),
            "invalidation_rule": spec.get("invalidation_condition"),
            "backtest_spec": spec,
        },
        "candidate_backtests",
        source_rank,
    )
    normalized["sample_size"] = _int_or_none(metrics.get("sample_size"))
    return normalized


def _dedupe_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for row in candidates:
        candidate_id = row.get("candidate_id")
        if not candidate_id:
            continue
        existing = by_id.get(candidate_id)
        if existing is None:
            row["sources"] = [row["source"]]
            by_id[candidate_id] = row
            continue
        existing["sources"] = sorted(set(existing.get("sources", []) + [row["source"]]))
        existing["source_buckets"] = sorted(set(existing.get("source_buckets", []) + row.get("source_buckets", [])))
        if _candidate_priority(row) > _candidate_priority(existing):
            merged_sources = existing["sources"]
            merged_buckets = existing["source_buckets"]
            row["sources"] = merged_sources
            row["source_buckets"] = merged_buckets
            by_id[candidate_id] = row
    return list(by_id.values())


def _select_top_candidates(candidates: list[dict[str, Any]], *, top_n: int) -> list[dict[str, Any]]:
    return sorted(candidates, key=_candidate_sort_key)[:top_n]


def _candidate_to_backtest_spec(candidate: dict[str, Any], data_meta: dict[str, Any]) -> dict[str, Any]:
    if isinstance(candidate.get("backtest_spec"), dict) and candidate["backtest_spec"]:
        spec = dict(candidate["backtest_spec"])
        spec["data_source"] = data_meta
        spec["authority"] = "paper-forward observation evidence only; no trading, capital, broker, or sizing authority"
        spec["research_only"] = True
        return spec
    regime = str(candidate.get("proxy_regime") or candidate.get("regime") or "UNKNOWN").upper()
    plan = {
        "candidate_id": candidate.get("candidate_id"),
        "mechanism": candidate.get("mechanism"),
        "hypothesis": candidate.get("hypothesis"),
        "entry_observation_condition": candidate.get("entry_observation_condition"),
        "exit_observation_condition": candidate.get("exit_observation_condition"),
        "invalidating_condition": candidate.get("invalidation_condition"),
        "edge_score": candidate.get("edge_score"),
        "replay_score": candidate.get("replay_score"),
        "regime_constraints": {
            "primary_regime": regime,
            "allowed_regimes": [regime] if regime and regime != "UNKNOWN" else ["UNKNOWN"],
            "reject_if_regime_unknown": regime != "UNKNOWN",
        },
    }
    return create_backtest_spec(plan, data_meta=data_meta)


def _summarize_deep_result(rank: int, candidate: dict[str, Any], backtest: dict[str, Any]) -> dict[str, Any]:
    metrics = backtest.get("metrics") if isinstance(backtest.get("metrics"), dict) else {}
    status = _status_from_classification(str(backtest.get("classification") or "INSUFFICIENT_DATA"))
    return {
        "rank": rank,
        "candidate_id": backtest.get("candidate_id") or candidate.get("candidate_id"),
        "mechanism": backtest.get("mechanism") or candidate.get("mechanism"),
        "expectancy": metrics.get("expectancy"),
        "profit_factor": metrics.get("profit_factor"),
        "sample_size": metrics.get("sample_size") or backtest.get("sample_size") or 0,
        "max_drawdown": metrics.get("max_drawdown"),
        "regime_performance": metrics.get("regime_specific_performance") or {},
        "failure_modes": _failure_modes(backtest, metrics),
        "status": status,
        "backtest_classification": backtest.get("classification"),
        "source_score_inputs": {
            "sources": candidate.get("sources") or [candidate.get("source")],
            "source_rank": candidate.get("source_rank"),
            "edge_score": candidate.get("edge_score"),
            "replay_score": candidate.get("replay_score"),
            "source_expectancy": candidate.get("expectancy"),
            "source_profit_factor": candidate.get("profit_factor"),
        },
        "warnings": backtest.get("warnings") or [],
        "missing_data": backtest.get("missing_data") or [],
        "paper_forward_observation_only": True,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def _failure_modes(backtest: dict[str, Any], metrics: dict[str, Any]) -> list[str]:
    modes = list(backtest.get("missing_data") or [])
    sample_size = int(metrics.get("sample_size") or 0)
    expectancy = metrics.get("expectancy")
    profit_factor = metrics.get("profit_factor")
    max_drawdown = metrics.get("max_drawdown")
    if sample_size < 30:
        modes.append(f"sample size below 30 ({sample_size})")
    if expectancy is not None and float(expectancy) <= 0:
        modes.append("non-positive expectancy in proxy replay")
    if profit_factor is not None and float(profit_factor) < 1.1:
        modes.append("profit factor below support threshold")
    if max_drawdown is not None and float(max_drawdown) <= -0.25:
        modes.append("large proxy drawdown")
    for warning in backtest.get("warnings") or []:
        if "daily bars cannot fully test intraday" in warning:
            modes.append("daily proxy cannot fully test intraday behavior")
        if "filtered by candidate regime constraints" in warning:
            modes.append("regime filter materially reduced sample")
    return sorted(set(modes))


def _summary(
    ranked: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    selected: list[dict[str, Any]],
    data_meta: dict[str, Any],
    data_limitations: list[str],
) -> dict[str, Any]:
    return {
        "candidate_pool_count": len(candidates),
        "candidates_deep_tested": len(selected),
        "supported_count": sum(1 for row in ranked if row.get("status") == "supported"),
        "weakened_count": sum(1 for row in ranked if row.get("status") == "weakened"),
        "insufficient_data_count": sum(1 for row in ranked if row.get("status") == "insufficient data"),
        "data_source": data_meta,
        "data_limitations": data_limitations,
        "top_5": [
            {
                "rank": row["rank"],
                "candidate_id": row["candidate_id"],
                "mechanism": row["mechanism"],
                "status": row["status"],
                "expectancy": row["expectancy"],
                "profit_factor": row["profit_factor"],
                "sample_size": row["sample_size"],
            }
            for row in ranked[:5]
        ],
        "authority_confirmation": AUTHORITY_STATEMENT,
    }


def _candidate_sort_key(candidate: dict[str, Any]) -> tuple[float, float, float, int]:
    return (
        -_candidate_priority(candidate),
        -float(candidate.get("expectancy") or 0.0),
        -float(candidate.get("profit_factor") or 0.0),
        int(candidate.get("source_rank") or 9999),
    )


def _candidate_priority(candidate: dict[str, Any]) -> float:
    score = 0.0
    sources = set(candidate.get("sources") or [candidate.get("source")])
    score += 0.2 * len(sources)
    if candidate.get("status") in {SUPPORTED_CLASSIFICATION, "BACKTEST_SUPPORTED", "READY_FOR_OBSERVATION_REVIEW", "PAPER_FORWARD_READY"}:
        score += 1.0
    if candidate.get("expectancy") is not None:
        score += min(1.0, max(-1.0, float(candidate["expectancy"]) * 100.0))
    if candidate.get("profit_factor") is not None:
        score += min(1.0, float(candidate["profit_factor"]) / 2.0)
    if candidate.get("edge_score") is not None:
        score += float(candidate["edge_score"])
    if candidate.get("replay_score") is not None:
        score += float(candidate["replay_score"])
    rank = int(candidate.get("source_rank") or 9999)
    score += max(0.0, 1.0 - (rank / 100.0))
    return round(score, 6)


def _deep_rank_key(row: dict[str, Any]) -> tuple[int, float, float, int, float]:
    status_order = {"supported": 0, "weakened": 1, "insufficient data": 2}
    return (
        status_order.get(str(row.get("status")), 3),
        -float(row.get("expectancy") or -999.0),
        -float(row.get("profit_factor") or 0.0),
        -int(row.get("sample_size") or 0),
        abs(float(row.get("max_drawdown") or 0.0)),
    )


def _status_from_classification(classification: str) -> str:
    if classification == SUPPORTED_CLASSIFICATION:
        return "supported"
    if classification == WEAK_CLASSIFICATION:
        return "weakened"
    if classification == INSUFFICIENT_DATA_CLASSIFICATION:
        return "insufficient data"
    return "insufficient data"


def _proxy_regime(regime: Any) -> str:
    value = str(regime or "UNKNOWN").upper()
    return {"CHOP": "RANGE_BOUND"}.get(value, value)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def main() -> None:
    report = run_top_candidate_deep_backtest_report()
    day = str(report.get("created_at"))[:10]
    root = Path(DEFAULT_STORE_ROOT) / REPORT_ROOT_NAME
    print(json.dumps({"report": str(root / day / "top_candidate_deep_backtest.json"), "summary": str(root / day / "top_candidate_deep_backtest_summary.md")}, indent=2))


if __name__ == "__main__":
    main()
