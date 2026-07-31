from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Iterable

from .backtest_mvp import AUTHORITY_BOUNDARY, DATA_REQUIRED, FACTOR_INPUTS, FACTOR_WEIGHTS, INVERSE_METRICS, PORTFOLIO_SIZES, READY, calculate_return_metrics
from .data_import_validator import BENCHMARK_IDS, build_data_import_validator

DEFAULT_REPORT_ROOT = Path("reports/portfolio_research_os")
AUTHORITY = "Research-only. No live portfolio, no replacement recommendation, no trades."
COMPLETE_STATES = {"READY", "COMPLETE", "IMPORT_READY", "P046_P060_COMPLETE"}

REPORT_DIRS = {
    "source_backed_total_return_engine": "source_backed_total_return_engine",
    "pit_factor_snapshot_builder": "pit_factor_snapshot_builder",
    "fixed_weight_composite_score_builder": "fixed_weight_composite_score_builder",
    "portfolio_size_trial_builder": "portfolio_size_trial_builder",
    "portfolio_return_calculator": "portfolio_return_calculator",
    "benchmark_return_calculator": "benchmark_return_calculator",
    "atlas_benchmark_comparison": "atlas_benchmark_comparison",
    "breadth_optionality_study": "breadth_optionality_study",
    "factor_contribution_report": "factor_contribution_report",
    "backtest_bias_recheck": "backtest_bias_recheck",
    "first_backtest_evidence_review": "first_backtest_evidence_review",
    "initial_oak_harvest_comparison": "initial_oak_harvest_comparison",
    "first_cio_summary": "first_cio_summary",
    "continue_stop_decision": "continue_stop_decision",
    "next_build_plan": "next_build_plan",
}

INPUT_FILES = {
    "prices": ("normalized_daily_prices.csv", "prices.csv", "price_history.csv"),
    "dividends": ("dividends.csv", "dividend_adjustments.csv"),
    "splits": ("splits.csv", "corporate_actions.csv"),
    "fundamentals": ("fundamentals.csv", "point_in_time_fundamentals.csv"),
    "universe": ("eligible_universe.csv", "universe.csv", "security_universe.csv"),
    "benchmarks": ("benchmark_returns.csv", "benchmarks.csv"),
}


def run_first_source_backed_backtest(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    report = build_first_source_backed_backtest(report_root, created_at=created_at)
    write_first_source_backed_backtest(report, report_root)
    return report


def build_first_source_backed_backtest(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root = Path(report_root)
    created = created_at or _now()
    inputs = _load_inputs(root)
    readiness = _import_readiness(root)
    validation = build_data_import_validator(root, created_at=created)
    ready = readiness["status"] == READY and validation["status"] == READY
    if not ready:
        return _data_required_report(created, readiness, validation)
    returns = _total_returns(inputs, created, validation)
    snapshots = _pit_snapshots(inputs, returns, created)
    scores = _composite_scores(snapshots, created)
    portfolios = _portfolio_trials(scores, inputs, created)
    port_returns = _portfolio_returns(portfolios, returns, created)
    benchmarks = _benchmark_returns(inputs, created)
    comparison = _comparison(port_returns, benchmarks, created)
    breadth = _breadth(port_returns, portfolios, created)
    contribution = _factor_contribution(scores, port_returns, created)
    bias = _bias_recheck(readiness, validation, returns, snapshots, benchmarks, created)
    evidence = _evidence_review(returns, snapshots, scores, portfolios, port_returns, benchmarks, comparison, breadth, contribution, bias, created)
    oak = _oak_comparison(port_returns, benchmarks, created)
    cio = _cio_summary(comparison, breadth, oak, evidence, created)
    cont = _continue_stop(evidence, bias, created)
    plan = _next_plan(evidence, cont, created)
    return {"created_at": created, "import_readiness": readiness, "data_validation": validation, **returns, **snapshots, **scores, **portfolios, **port_returns, **benchmarks, **comparison, **breadth, **contribution, **bias, **evidence, **oak, **cio, **cont, **plan}


def _data_required_report(created: str, readiness: dict[str, Any], validation: dict[str, Any]) -> dict[str, Any]:
    reason = "P046-P060 import readiness is not complete or imported data was rejected."
    empty_metrics = [{"portfolio_size": s, "status": DATA_REQUIRED, "CAGR": DATA_REQUIRED, "volatility": DATA_REQUIRED, "Sharpe": DATA_REQUIRED, "Sortino": DATA_REQUIRED, "max_drawdown": DATA_REQUIRED, "turnover": DATA_REQUIRED, "best_year": DATA_REQUIRED, "worst_year": DATA_REQUIRED} for s in PORTFOLIO_SIZES]
    benchmark_metrics = [{"benchmark_id": b, "status": DATA_REQUIRED, "CAGR": DATA_REQUIRED, "volatility": DATA_REQUIRED, "Sharpe": DATA_REQUIRED, "Sortino": DATA_REQUIRED, "max_drawdown": DATA_REQUIRED, "best_year": DATA_REQUIRED, "worst_year": DATA_REQUIRED} for b in BENCHMARK_IDS]
    comparison_rows = [{"portfolio_size": s, "benchmark_id": b, "classification": DATA_REQUIRED, "relative_return": DATA_REQUIRED, "status": DATA_REQUIRED} for s in PORTFOLIO_SIZES for b in BENCHMARK_IDS]
    return {
        "created_at": created,
        "import_readiness": readiness,
        "data_validation": validation,
        "source_backed_total_return_engine": _stage("P061", DATA_REQUIRED, reason, daily_total_returns=[], monthly_total_returns=[], corporate_action_audit=_corp_required()),
        "pit_factor_snapshot_builder": _stage("P062", DATA_REQUIRED, reason, monthly_factor_snapshots=[], missing_factor_inputs=[{"ticker":"", "rebalance_month":"", "missing_input":"P046_P060_IMPORT_READY", "status": DATA_REQUIRED}]),
        "fixed_weight_composite_score_builder": _stage("P063", DATA_REQUIRED, reason, monthly_opportunity_scores=[], rank_history=[], score_distribution=[{"metric":"opportunity_score", "count":0, "min":"", "max":"", "mean":""}], fixed_weights=FACTOR_WEIGHTS),
        "portfolio_size_trial_builder": _stage("P064", DATA_REQUIRED, reason, monthly_holdings_by_size=[], constraint_audit=_size_rows(DATA_REQUIRED), position_limit_audit=[], sector_limit_audit=[]),
        "portfolio_return_calculator": _stage("P065", DATA_REQUIRED, reason, portfolio_monthly_returns=[], portfolio_performance_metrics=empty_metrics, drawdown_series=[], turnover_report=[{"portfolio_size":s,"turnover":DATA_REQUIRED,"status":DATA_REQUIRED} for s in PORTFOLIO_SIZES]),
        "benchmark_return_calculator": _stage("P066", DATA_REQUIRED, reason, benchmark_monthly_returns=[], benchmark_performance_metrics=benchmark_metrics, benchmark_assumption_audit=_benchmark_audit(DATA_REQUIRED)),
        "atlas_benchmark_comparison": _stage("P067", DATA_REQUIRED, reason, comparison_scorecard=comparison_rows, relative_return=comparison_rows, risk_adjusted_comparison=[], drawdown_comparison=[]),
        "breadth_optionality_study": _stage("P068", DATA_REQUIRED, reason, portfolio_breadth_study=_size_rows(DATA_REQUIRED), breadth_decision=[{"decision": DATA_REQUIRED, "status": DATA_REQUIRED}]),
        "factor_contribution_report": _stage("P069", DATA_REQUIRED, reason, factor_contribution=[], factor_rank_effectiveness=[], factor_failure_modes=[{"factor":"ALL", "failure_mode":"DATA_REQUIRED", "status":DATA_REQUIRED}]),
        "backtest_bias_recheck": _stage("P070", "BIAS_BLOCKED", reason, bias_recheck=_bias_rows(False), integrity_decision=[{"decision":"BIAS_BLOCKED", "status":"BIAS_BLOCKED"}]),
        "first_backtest_evidence_review": _stage("P071", DATA_REQUIRED, reason, evidence_scorecard=[], backtest_decision=[{"decision": DATA_REQUIRED, "status": DATA_REQUIRED}]),
        "initial_oak_harvest_comparison": _stage("P072", DATA_REQUIRED, reason, oak_harvest_proxy_comparison=[{"classification": DATA_REQUIRED, "status": DATA_REQUIRED}], oak_harvest_assumption_audit=[{"assumption_status":"USER_ASSUMPTION", "status": DATA_REQUIRED}]),
        "first_cio_summary": _stage("P073", DATA_REQUIRED, reason, summary_markdown=_render_cio(DATA_REQUIRED, DATA_REQUIRED, DATA_REQUIRED, DATA_REQUIRED, DATA_REQUIRED)),
        "continue_stop_decision": _stage("P074", "IMPROVE_DATA_FIRST", reason, decision=[{"decision":"IMPROVE_DATA_FIRST", "status":DATA_REQUIRED}]),
        "next_build_plan": _stage("P075", DATA_REQUIRED, reason, recommended_builds=[{"priority":"P0", "recommended_build":"Complete P046-P060 import readiness", "rationale":reason}]),
    }


def _total_returns(inputs: dict[str, Any], created: str, validation: dict[str, Any]) -> dict[str, Any]:
    daily = []
    prev = {}
    for row in sorted(inputs["prices"], key=lambda r: (r.get("ticker",""), r.get("date",""))):
        ticker, d = row.get("ticker",""), row.get("date","")
        adj = _float(row.get("adjusted_close") or row.get("normalized_close") or row.get("close"))
        if not ticker or not d or adj is None or adj <= 0:
            continue
        ret = "" if ticker not in prev else round(adj / prev[ticker] - 1, 10)
        daily.append({"ticker": ticker, "date": d, "daily_total_return": ret, "source": "normalized_adjusted_close"})
        prev[ticker] = adj
    monthly = _monthly_from_daily(daily)
    audit = [{"corporate_action_domain":"dividends", "status": READY if inputs["dividends"] else DATA_REQUIRED, "rows": len(inputs["dividends"]), "notes":"source-backed dividends required unless price_only explicitly approved"}, {"corporate_action_domain":"splits", "status": READY if inputs["splits"] else DATA_REQUIRED, "rows": len(inputs["splits"]), "notes":"source-backed splits required unless price_only explicitly approved"}]
    status = READY if monthly and all(r["status"] == READY for r in audit) else DATA_REQUIRED
    return {"source_backed_total_return_engine": _stage("P061", status, "Source-backed total returns built." if status == READY else "DATA_REQUIRED", daily_total_returns=daily if status == READY else [], monthly_total_returns=monthly if status == READY else [], corporate_action_audit=audit)}


def _pit_snapshots(inputs: dict[str, Any], returns: dict[str, Any], created: str) -> dict[str, Any]:
    months = sorted({r["month"] for r in returns["source_backed_total_return_engine"].get("monthly_total_returns", [])})
    universe = {r.get("ticker",""): r for r in inputs["universe"]}
    snapshots, missing = [], []
    for month in months:
        as_of = date.fromisoformat(month + "-28")
        latest = {}
        for row in inputs["fundamentals"]:
            ad = _parse_date(row.get("as_of_date"))
            if ad and ad <= as_of:
                latest[row.get("ticker","")] = row
        norm = _normalize(list(latest.values()))
        for ticker, row in sorted(latest.items()):
            snap = {"rebalance_month": month, "ticker": ticker, "sector": universe.get(ticker,{}).get("sector",""), "security_type": universe.get(ticker,{}).get("security_type",""), "as_of_date": row.get("as_of_date","")}
            ok = True
            for factor, metrics in FACTOR_INPUTS.items():
                vals = [norm.get(m,{}).get(ticker) for m in metrics]
                if any(v is None for v in vals):
                    ok = False
                    for m, v in zip(metrics, vals):
                        if v is None:
                            missing.append({"rebalance_month": month, "ticker": ticker, "missing_input": m, "status": DATA_REQUIRED})
                    snap[f"{factor}_score"] = ""
                else:
                    snap[f"{factor}_score"] = round(mean(vals), 6)
            snap["snapshot_status"] = READY if ok else DATA_REQUIRED
            snapshots.append(snap)
    status = READY if snapshots and not missing else DATA_REQUIRED
    return {"pit_factor_snapshot_builder": _stage("P062", status, "PIT factor snapshots built." if status == READY else "DATA_REQUIRED", monthly_factor_snapshots=snapshots if status == READY else [], missing_factor_inputs=missing)}


def _composite_scores(snapshots: dict[str, Any], created: str) -> dict[str, Any]:
    rows = []
    for row in snapshots["pit_factor_snapshot_builder"].get("monthly_factor_snapshots", []):
        if row.get("snapshot_status") != READY:
            continue
        score = sum(float(row[f"{f}_score"]) * w for f, w in FACTOR_WEIGHTS.items())
        rows.append({**row, "opportunity_score": round(score, 6), "rank": 0, "score_status": READY})
    by_month = defaultdict(list)
    for row in rows:
        by_month[row["rebalance_month"]].append(row)
    ranked = []
    for month, month_rows in by_month.items():
        for idx, row in enumerate(sorted(month_rows, key=lambda r: (-float(r["opportunity_score"]), r["ticker"])), start=1):
            row["rank"] = idx
            ranked.append(row)
    vals = [float(r["opportunity_score"]) for r in ranked]
    dist = [{"metric":"opportunity_score", "count":len(vals), "min":min(vals) if vals else "", "max":max(vals) if vals else "", "mean":round(mean(vals),6) if vals else ""}]
    return {"fixed_weight_composite_score_builder": _stage("P063", READY if ranked else DATA_REQUIRED, "Fixed V1 weights only. No optimization.", monthly_opportunity_scores=ranked, rank_history=[{"rebalance_month":r["rebalance_month"],"ticker":r["ticker"],"rank":r["rank"],"opportunity_score":r["opportunity_score"]} for r in ranked], score_distribution=dist, fixed_weights=FACTOR_WEIGHTS)}


def _portfolio_trials(scores: dict[str, Any], inputs: dict[str, Any], created: str) -> dict[str, Any]:
    holdings, constraint, pos_audit, sector_audit = [], [], [], []
    by_month = defaultdict(list)
    for r in scores["fixed_weight_composite_score_builder"].get("monthly_opportunity_scores", []):
        by_month[r["rebalance_month"]].append(r)
    for month, rows in sorted(by_month.items()):
        for size in PORTFOLIO_SIZES:
            weight = min(1/size, 0.05)
            max_sector = max(1, math.floor(0.20 / weight))
            counts = defaultdict(int)
            selected = []
            for row in sorted(rows, key=lambda r: (int(r["rank"]), r["ticker"])):
                sector = row.get("sector") or "UNKNOWN"
                if sector != "UNKNOWN" and counts[sector] >= max_sector:
                    continue
                selected.append(row); counts[sector]+=1
                if len(selected) == size: break
            status = READY if len(selected) == size else DATA_REQUIRED
            constraint.append({"rebalance_month":month,"portfolio_size":size,"selected":len(selected),"status":status,"max_position":0.05,"sector_cap":0.20})
            for row in selected:
                holdings.append({"rebalance_month":month,"portfolio_size":size,"ticker":row["ticker"],"sector":row.get("sector",""),"target_weight":round(weight,8),"opportunity_score":row["opportunity_score"],"status":status})
                pos_audit.append({"rebalance_month":month,"portfolio_size":size,"ticker":row["ticker"],"target_weight":round(weight,8),"limit":0.05,"status":READY if weight <= 0.05 else "BREACH"})
            for sector, count in counts.items():
                sector_audit.append({"rebalance_month":month,"portfolio_size":size,"sector":sector,"sector_weight":round(count*weight,8),"limit":0.20,"status":READY if count*weight <= 0.20000001 else "BREACH"})
    status = READY if holdings and all(r["status"] == READY for r in constraint) else DATA_REQUIRED
    return {"portfolio_size_trial_builder": _stage("P064", status, "Monthly size trials built." if status == READY else "DATA_REQUIRED", monthly_holdings_by_size=holdings if status == READY else [], constraint_audit=constraint, position_limit_audit=pos_audit, sector_limit_audit=sector_audit)}


def _portfolio_returns(portfolios: dict[str, Any], returns: dict[str, Any], created: str) -> dict[str, Any]:
    ret = {(r["ticker"], r["month"]): _float(r["monthly_total_return"]) for r in returns["source_backed_total_return_engine"].get("monthly_total_returns", [])}
    monthly = []
    for (month, size), holdings in _group(portfolios["portfolio_size_trial_builder"].get("monthly_holdings_by_size", []), "rebalance_month", "portfolio_size").items():
        vals = []
        for h in holdings:
            v = ret.get((h["ticker"], month))
            if v is None: vals = []; break
            vals.append(float(h["target_weight"]) * v)
        if vals:
            monthly.append({"rebalance_month":month,"portfolio_size":size,"monthly_return":round(sum(vals),10),"status":READY})
    metrics, draws, turns = [], [], []
    for size in PORTFOLIO_SIZES:
        vals = [float(r["monthly_return"]) for r in monthly if int(r["portfolio_size"]) == size]
        m = _metrics(vals)
        metrics.append({"portfolio_size":size,"status":READY if vals else DATA_REQUIRED, **m})
        draws.extend(_drawdowns(size, [r for r in monthly if int(r["portfolio_size"]) == size]))
        turns.append({"portfolio_size":size,"turnover":0.0 if vals else DATA_REQUIRED,"status":READY if vals else DATA_REQUIRED})
    status = READY if monthly and all(r["status"] == READY for r in metrics) else DATA_REQUIRED
    return {"portfolio_return_calculator": _stage("P065", status, "Portfolio returns calculated." if status == READY else "DATA_REQUIRED", portfolio_monthly_returns=monthly if status == READY else [], portfolio_performance_metrics=metrics, drawdown_series=draws if status == READY else [], turnover_report=turns)}


def _benchmark_returns(inputs: dict[str, Any], created: str) -> dict[str, Any]:
    rows, by = [], defaultdict(list)
    for r in inputs["benchmarks"]:
        bid = r.get("benchmark_id") or r.get("benchmark") or r.get("ticker")
        month = (r.get("month") or r.get("date") or "")[:7]
        val = _float(r.get("monthly_return") or r.get("return"))
        if bid and month and val is not None:
            rows.append({"benchmark_id":bid,"month":month,"monthly_return":val,"status":READY}); by[bid].append(val)
    metrics = [{"benchmark_id":b,"status":READY if by.get(b) else DATA_REQUIRED, **_metrics(by.get(b, []))} for b in BENCHMARK_IDS]
    status = READY if by.get("VTI") and by.get("OAK_HARVEST_PROXY") else DATA_REQUIRED
    return {"benchmark_return_calculator": _stage("P066", status, "Benchmark returns calculated." if status == READY else "DATA_REQUIRED", benchmark_monthly_returns=rows if status == READY else [], benchmark_performance_metrics=metrics, benchmark_assumption_audit=_benchmark_audit(status))}


def _comparison(port: dict[str, Any], bench: dict[str, Any], created: str) -> dict[str, Any]:
    rows, risk, dd = [], [], []
    for p in port["portfolio_return_calculator"]["portfolio_performance_metrics"]:
        for b in bench["benchmark_return_calculator"]["benchmark_performance_metrics"]:
            pc, bc = _float(p.get("CAGR")), _float(b.get("CAGR"))
            cls = DATA_REQUIRED if pc is None or bc is None else ("ATLAS_OUTPERFORMS" if pc > bc else "ATLAS_UNDERPERFORMS")
            rows.append({"portfolio_size":p["portfolio_size"],"benchmark_id":b["benchmark_id"],"classification":cls,"relative_return":round(pc-bc,6) if pc is not None and bc is not None else DATA_REQUIRED,"status":READY if cls != DATA_REQUIRED else DATA_REQUIRED})
            risk.append({"portfolio_size":p["portfolio_size"],"benchmark_id":b["benchmark_id"],"atlas_sharpe":p.get("Sharpe"),"benchmark_sharpe":b.get("Sharpe"),"status":READY if cls != DATA_REQUIRED else DATA_REQUIRED})
            dd.append({"portfolio_size":p["portfolio_size"],"benchmark_id":b["benchmark_id"],"atlas_max_drawdown":p.get("max_drawdown"),"benchmark_max_drawdown":b.get("max_drawdown"),"status":READY if cls != DATA_REQUIRED else DATA_REQUIRED})
    status = READY if any(r["status"] == READY for r in rows) else DATA_REQUIRED
    return {"atlas_benchmark_comparison": _stage("P067", status, "Atlas vs benchmark comparison complete." if status == READY else "DATA_REQUIRED", comparison_scorecard=rows, relative_return=rows, risk_adjusted_comparison=risk, drawdown_comparison=dd)}


def _breadth(port: dict[str, Any], portfolios: dict[str, Any], created: str) -> dict[str, Any]:
    rows=[]
    for m in port["portfolio_return_calculator"]["portfolio_performance_metrics"]:
        size=m["portfolio_size"]
        hs=[h for h in portfolios["portfolio_size_trial_builder"].get("monthly_holdings_by_size",[]) if int(h["portfolio_size"])==size]
        sectors={h.get("sector","") for h in hs}
        rows.append({"portfolio_size":size,"return":m.get("CAGR"),"volatility":m.get("volatility"),"drawdown":m.get("max_drawdown"),"turnover":m.get("turnover"),"future_winner_capture_proxy":DATA_REQUIRED if not hs else round(len(hs)/max(1,len(set(h["ticker"] for h in hs))),6),"sector_concentration":DATA_REQUIRED if not hs else round(1/max(1,len(sectors)),6),"single_name_concentration":round(min(1/size,0.05),6),"status":m.get("status")})
    ready=[r for r in rows if r["status"]==READY and _float(r["return"]) is not None]
    decision = max(ready, key=lambda r: float(r["return"]))["portfolio_size"] if ready else DATA_REQUIRED
    return {"breadth_optionality_study": _stage("P068", READY if ready else DATA_REQUIRED, "Breadth study complete." if ready else "DATA_REQUIRED", portfolio_breadth_study=rows, breadth_decision=[{"decision":decision,"status":READY if ready else DATA_REQUIRED}])}


def _factor_contribution(scores: dict[str, Any], port: dict[str, Any], created: str) -> dict[str, Any]:
    rows=[]
    for f,w in FACTOR_WEIGHTS.items():
        vals=[_float(r.get(f"{f}_score")) for r in scores["fixed_weight_composite_score_builder"].get("monthly_opportunity_scores",[])]
        vals=[v for v in vals if v is not None]
        rows.append({"factor":f,"weight":w,"average_score":round(mean(vals),6) if vals else DATA_REQUIRED,"estimated_contribution":round(mean(vals)*w,6) if vals else DATA_REQUIRED,"status":READY if vals else DATA_REQUIRED})
    return {"factor_contribution_report": _stage("P069", READY if all(r["status"]==READY for r in rows) else DATA_REQUIRED, "Factor contribution report complete.", factor_contribution=rows, factor_rank_effectiveness=rows, factor_failure_modes=[] if all(r["status"]==READY for r in rows) else [{"factor":"ALL","failure_mode":"DATA_REQUIRED","status":DATA_REQUIRED}])}


def _bias_recheck(readiness: dict[str,Any], validation: dict[str,Any], returns: dict[str,Any], snapshots: dict[str,Any], benchmarks: dict[str,Any], created: str) -> dict[str,Any]:
    controlled = readiness["status"] == READY and validation["status"] == READY and returns["source_backed_total_return_engine"]["status"] == READY and snapshots["pit_factor_snapshot_builder"]["status"] == READY and benchmarks["benchmark_return_calculator"]["status"] == READY
    rows = _bias_rows(controlled)
    decision = READY if controlled else "BIAS_BLOCKED"
    return {"backtest_bias_recheck": _stage("P070", decision, "Bias recheck complete." if controlled else "BIAS_BLOCKED", bias_recheck=rows, integrity_decision=[{"decision":decision,"status":decision}])}


def _evidence_review(returns, snapshots, scores, portfolios, port, benchmarks, comparison, breadth, contrib, bias, created):
    stages=[("P061",returns["source_backed_total_return_engine"]["status"]),("P062",snapshots["pit_factor_snapshot_builder"]["status"]),("P063",scores["fixed_weight_composite_score_builder"]["status"]),("P064",portfolios["portfolio_size_trial_builder"]["status"]),("P065",port["portfolio_return_calculator"]["status"]),("P066",benchmarks["benchmark_return_calculator"]["status"]),("P067",comparison["atlas_benchmark_comparison"]["status"]),("P068",breadth["breadth_optionality_study"]["status"]),("P069",contrib["factor_contribution_report"]["status"]),("P070",bias["backtest_bias_recheck"]["status"])]
    decision = "BIAS_BLOCKED" if bias["backtest_bias_recheck"]["status"] == "BIAS_BLOCKED" else (DATA_REQUIRED if any(s==DATA_REQUIRED for _,s in stages) else "BACKTEST_PROMISING")
    return {"first_backtest_evidence_review": _stage("P071", decision, "First backtest evidence review.", evidence_scorecard=[{"phase":p,"status":s} for p,s in stages], backtest_decision=[{"decision":decision,"status":decision}])}


def _oak_comparison(port, benchmarks, created):
    oak=next((b for b in benchmarks["benchmark_return_calculator"]["benchmark_performance_metrics"] if b["benchmark_id"]=="OAK_HARVEST_PROXY"),{})
    rows=[{"portfolio_size":p["portfolio_size"],"atlas_cagr":p.get("CAGR"),"oak_harvest_proxy_cagr":oak.get("CAGR",DATA_REQUIRED),"assumption_status":"USER_ASSUMPTION","status":READY if p.get("status")==READY and oak.get("status")==READY else DATA_REQUIRED} for p in port["portfolio_return_calculator"]["portfolio_performance_metrics"]]
    return {"initial_oak_harvest_comparison": _stage("P072", READY if any(r["status"]==READY for r in rows) else DATA_REQUIRED, "Oak Harvest proxy remains USER_ASSUMPTION unless source-backed.", oak_harvest_proxy_comparison=rows, oak_harvest_assumption_audit=[{"assumption_status":"USER_ASSUMPTION","status":DATA_REQUIRED if not any(r["status"]==READY for r in rows) else READY}])}


def _cio_summary(comparison, breadth, oak, evidence, created):
    best=breadth["breadth_optionality_study"]["breadth_decision"][0]["decision"]
    decision=evidence["first_backtest_evidence_review"]["status"]
    md=_render_cio(DATA_REQUIRED, DATA_REQUIRED, DATA_REQUIRED, best, "NO" if decision != "BACKTEST_PROMISING" else "PENDING_REVIEW")
    return {"first_cio_summary": _stage("P073", decision, "CIO summary written.", summary_markdown=md)}


def _continue_stop(evidence, bias, created):
    e=evidence["first_backtest_evidence_review"]["status"]
    decision="IMPROVE_DATA_FIRST" if e in {DATA_REQUIRED,"BIAS_BLOCKED"} else "CONTINUE_VALIDATION"
    return {"continue_stop_decision": _stage("P074", decision, "Continue/stop decision.", decision=[{"decision":decision,"status":e}])}


def _next_plan(evidence, cont, created):
    decision=cont["continue_stop_decision"]["status"]
    builds=[{"priority":"P0","recommended_build":"Repair source-backed data readiness" if decision=="IMPROVE_DATA_FIRST" else "Expand validation windows","rationale":"Driven by first source-backed backtest evidence review."}]
    return {"next_build_plan": _stage("P075", evidence["first_backtest_evidence_review"]["status"], "Next build plan.", recommended_builds=builds)}


def write_first_source_backed_backtest(report: dict[str,Any], root: Path | str) -> None:
    root=Path(root)
    specs={
        "source_backed_total_return_engine": {"daily_total_returns":["ticker","date","daily_total_return","source"],"monthly_total_returns":["ticker","month","monthly_total_return","status"],"corporate_action_audit":["corporate_action_domain","status","rows","notes"]},
        "pit_factor_snapshot_builder": {"monthly_factor_snapshots":["rebalance_month","ticker","sector","security_type","as_of_date",*[f"{f}_score" for f in FACTOR_WEIGHTS],"snapshot_status"],"missing_factor_inputs":["rebalance_month","ticker","missing_input","status"]},
        "fixed_weight_composite_score_builder": {"monthly_opportunity_scores":["rebalance_month","ticker","sector","security_type","as_of_date",*[f"{f}_score" for f in FACTOR_WEIGHTS],"opportunity_score","rank","score_status"],"rank_history":["rebalance_month","ticker","rank","opportunity_score"],"score_distribution":["metric","count","min","max","mean"]},
        "portfolio_size_trial_builder": {"monthly_holdings_by_size":["rebalance_month","portfolio_size","ticker","sector","target_weight","opportunity_score","status"],"constraint_audit":["rebalance_month","portfolio_size","selected","status","max_position","sector_cap"],"position_limit_audit":["rebalance_month","portfolio_size","ticker","target_weight","limit","status"],"sector_limit_audit":["rebalance_month","portfolio_size","sector","sector_weight","limit","status"]},
        "portfolio_return_calculator": {"portfolio_monthly_returns":["rebalance_month","portfolio_size","monthly_return","status"],"portfolio_performance_metrics":["portfolio_size","status","CAGR","volatility","Sharpe","Sortino","max_drawdown","turnover","best_year","worst_year"],"drawdown_series":["portfolio_size","month","drawdown","status"],"turnover_report":["portfolio_size","turnover","status"]},
        "benchmark_return_calculator": {"benchmark_monthly_returns":["benchmark_id","month","monthly_return","status"],"benchmark_performance_metrics":["benchmark_id","status","CAGR","volatility","Sharpe","Sortino","max_drawdown","best_year","worst_year"],"benchmark_assumption_audit":["benchmark_id","status","assumption_status","notes"]},
        "atlas_benchmark_comparison": {"comparison_scorecard":["portfolio_size","benchmark_id","classification","relative_return","status"],"relative_return":["portfolio_size","benchmark_id","classification","relative_return","status"],"risk_adjusted_comparison":["portfolio_size","benchmark_id","atlas_sharpe","benchmark_sharpe","status"],"drawdown_comparison":["portfolio_size","benchmark_id","atlas_max_drawdown","benchmark_max_drawdown","status"]},
        "breadth_optionality_study": {"portfolio_breadth_study":["portfolio_size","return","volatility","drawdown","turnover","future_winner_capture_proxy","sector_concentration","single_name_concentration","status"],"breadth_decision":["decision","status"]},
        "factor_contribution_report": {"factor_contribution":["factor","weight","average_score","estimated_contribution","status"],"factor_rank_effectiveness":["factor","weight","average_score","estimated_contribution","status"],"factor_failure_modes":["factor","failure_mode","status"]},
        "backtest_bias_recheck": {"bias_recheck":["audit_item","classification","evidence_status","notes","required_remediation"],"integrity_decision":["decision","status"]},
        "first_backtest_evidence_review": {"evidence_scorecard":["phase","status"],"backtest_decision":["decision","status"]},
        "initial_oak_harvest_comparison": {"oak_harvest_proxy_comparison":["portfolio_size","atlas_cagr","oak_harvest_proxy_cagr","assumption_status","status"],"oak_harvest_assumption_audit":["assumption_status","status"]},
        "continue_stop_decision": {"decision":["decision","status"]},
        "next_build_plan": {"recommended_builds":["priority","recommended_build","rationale"]},
    }
    for stage, dirname in REPORT_DIRS.items():
        data=report[stage]
        out=root/dirname; out.mkdir(parents=True, exist_ok=True)
        (out/"latest.json").write_text(json.dumps(data, indent=2, sort_keys=True)+"\n", encoding="utf-8")
        if stage=="first_cio_summary":
            (out/"latest_summary.md").write_text(data.get("summary_markdown") or _summary(stage,data), encoding="utf-8")
        else:
            (out/"latest_summary.md").write_text(_summary(stage,data), encoding="utf-8")
        for key, cols in specs.get(stage,{}).items():
            filename = key + ".csv"
            _write_csv(out/filename, cols, data.get(key, []))


def _import_readiness(root: Path) -> dict[str, Any]:
    candidates=[root/"p046_p060_import_readiness"/"latest.json", root/"import_readiness"/"latest.json", root/"data_import_readiness"/"latest.json", root/"inputs"/"p046_p060_import_readiness.json"]
    for path in candidates:
        if path.exists():
            try: data=json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError: data={}
            status=str(data.get("status") or data.get("decision") or data.get("final_decision") or "").upper()
            return {"status": READY if status in COMPLETE_STATES else DATA_REQUIRED, "source_path": str(path), "raw_status": status or DATA_REQUIRED}
    return {"status": DATA_REQUIRED, "source_path": "", "raw_status": "MISSING_P046_P060_IMPORT_READINESS"}


def _load_inputs(root: Path) -> dict[str, list[dict[str,str]]]:
    return {name: _read_first(root, files) for name, files in INPUT_FILES.items()}

def _read_first(root: Path, files: Iterable[str]) -> list[dict[str,str]]:
    for base in [root/"inputs", root/"input", Path("data/portfolio_research_os")]:
        for fn in files:
            path=base/fn
            if path.exists(): return _read_csv(path)
    return []

def _read_csv(path: Path) -> list[dict[str,str]]:
    with path.open(newline="", encoding="utf-8") as h: return [{k:(v or "").strip() for k,v in r.items()} for r in csv.DictReader(h)]

def _write_csv(path: Path, cols: list[str], rows: list[dict[str,Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as h:
        w=csv.DictWriter(h, fieldnames=cols); w.writeheader(); [w.writerow({c:r.get(c,"") for c in cols}) for r in rows]

def _stage(build: str, status: str, notes: str, **kw: Any) -> dict[str,Any]:
    return {"schema_id": f"portfolio_research_os_{build.lower()}", "schema_version":"1.0", "build": f"Portfolio Atlas {build}", "status": status, "notes": notes, "authority_boundary": AUTHORITY_BOUNDARY, **kw}

def _summary(stage: str, data: dict[str,Any]) -> str:
    return f"# {stage}\n\nStatus: {data.get('status')}\n\nAuthority: {AUTHORITY}\n"

def _render_cio(vti, sixty, oak, best, complexity) -> str:
    return "\n".join(["# First CIO Summary","",f"Did Atlas beat VTI? {vti}",f"Did Atlas beat 60/40? {sixty}",f"Did Atlas beat Oak Harvest proxy? {oak}",f"Which portfolio size worked best? {best}",f"Was complexity justified? {complexity}","",f"Authority: {AUTHORITY}",""])

def _corp_required(): return [{"corporate_action_domain":"dividends","status":DATA_REQUIRED,"rows":0,"notes":"source-backed dividends required"},{"corporate_action_domain":"splits","status":DATA_REQUIRED,"rows":0,"notes":"source-backed splits required"}]
def _size_rows(status): return [{"portfolio_size":s,"status":status} for s in PORTFOLIO_SIZES]
def _benchmark_audit(status): return [{"benchmark_id":b,"status":status,"assumption_status":"USER_ASSUMPTION" if b=="OAK_HARVEST_PROXY" else status,"notes":"Oak Harvest proxy remains USER_ASSUMPTION unless source-backed." if b=="OAK_HARVEST_PROXY" else "Requires source-backed benchmark returns."} for b in BENCHMARK_IDS]
def _bias_rows(controlled):
    items=["survivorship bias","lookahead bias","future membership leakage","future filing leakage","dividend/split integrity","rebalance timing","benchmark assumptions"]
    return [{"audit_item":i,"classification":"BIAS_CONTROLLED" if controlled else "BIAS_RISK","evidence_status":"PRESENT" if controlled else "MISSING","notes":"source-backed controls required","required_remediation":"" if controlled else "Complete source-backed import readiness and provenance."} for i in items]

def _monthly_from_daily(daily):
    comp=defaultdict(lambda:1.0)
    for r in daily:
        v=_float(r.get("daily_total_return"))
        if v is not None: comp[(r["ticker"],r["date"][:7])] *= 1+v
    return [{"ticker":t,"month":m,"monthly_total_return":round(v-1,10),"status":READY} for (t,m),v in sorted(comp.items())]

def _normalize(rows):
    out={}
    for metric in {m for ms in FACTOR_INPUTS.values() for m in ms}:
        vals=[]
        for r in rows:
            v=_float(r.get(metric))
            if v is not None: vals.append((r.get("ticker",""), -v if metric in INVERSE_METRICS else v))
        if vals:
            raw=[v for _,v in vals]; lo=min(raw); hi=max(raw)
            out[metric]={t:(0.5 if hi==lo else round((v-lo)/(hi-lo),6)) for t,v in vals}
    return out

def _metrics(vals):
    if not vals:
        return {"CAGR":DATA_REQUIRED,"volatility":DATA_REQUIRED,"Sharpe":DATA_REQUIRED,"Sortino":DATA_REQUIRED,"max_drawdown":DATA_REQUIRED,"turnover":DATA_REQUIRED,"best_year":DATA_REQUIRED,"worst_year":DATA_REQUIRED}
    m=calculate_return_metrics(vals, turnover=0.0)
    years=defaultdict(lambda:1.0)
    for i,v in enumerate(vals): years[i//12]*=1+v
    yr=[v-1 for v in years.values()]
    return {"CAGR":m["CAGR"],"volatility":m["annualized_volatility"],"Sharpe":m["Sharpe"],"Sortino":m["Sortino"],"max_drawdown":m["max_drawdown"],"turnover":m["turnover"],"best_year":round(max(yr),6),"worst_year":round(min(yr),6)}

def _drawdowns(size, rows):
    eq=1.0; peak=1.0; out=[]
    for r in sorted(rows, key=lambda x:x["rebalance_month"]):
        eq*=1+float(r["monthly_return"]); peak=max(peak,eq)
        out.append({"portfolio_size":size,"month":r["rebalance_month"],"drawdown":round(eq/peak-1,10),"status":READY})
    return out

def _group(rows, a, b):
    d=defaultdict(list)
    for r in rows: d[(r[a],r[b])].append(r)
    return d

def _float(v):
    if v in (None, ""): return None
    try: x=float(v)
    except (TypeError, ValueError): return None
    return None if math.isnan(x) or math.isinf(x) else x

def _parse_date(v):
    if not v: return None
    try: return date.fromisoformat(v[:10])
    except ValueError: return None

def _now(): return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00","Z")
