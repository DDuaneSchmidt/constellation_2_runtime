from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Any

from .data_import_package import DEFAULT_REPORT_ROOT


OUTPUT_NAME = "size_decomposition.md"
AUTHORITY = "Research-only. No trades. No recommendations."
DATA_REQUIRED = "DATA_REQUIRED"


def run_size_decomposition(
    report_root: Path | str = DEFAULT_REPORT_ROOT,
    *,
    output_path: Path | str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    report = build_size_decomposition(report_root=report_root, created_at=created_at)
    path = write_size_decomposition(report, report_root=report_root, output_path=output_path)
    report["output_path"] = str(path)
    return report


def build_size_decomposition(report_root: Path | str = DEFAULT_REPORT_ROOT, *, created_at: str | None = None) -> dict[str, Any]:
    root = Path(report_root)
    pb002_dir = root / "pb002_pb020_price_only_universe_expansion"
    full_model_dir = root / "full_model_data_transition"
    p123_dir = root / "portfolio123_evidence_review"

    performance_rows = _read_csv(pb002_dir / "portfolio_performance_by_size.csv")
    turnover_rows = _read_csv(pb002_dir / "turnover_report.csv")
    holdings_rows = _read_csv(pb002_dir / "latest.json", key="portfolio_holdings")
    factor_rows = _read_csv(pb002_dir / "latest.json", key="factor_scores")
    eligible_rows = _read_csv(pb002_dir / "eligible_price_only_universe.csv")
    full_model = _read_json(full_model_dir / "latest.json")
    p123 = _read_json(p123_dir / "latest.json")

    capacity_rows = _capacity_rows(performance_rows, turnover_rows)
    selected_factor_averages = _selected_factor_averages(holdings_rows, factor_rows)
    external_size_bridge = _external_size_bridge(full_model, p123)
    blockers = _blockers(full_model, eligible_rows)

    return {
        "schema_id": "portfolio_research_os_size_decomposition",
        "schema_version": "1.0",
        "created_at": created_at or _now(),
        "status": DATA_REQUIRED,
        "authority": AUTHORITY,
        "cap_buckets": _cap_bucket_rows(),
        "capacity_rows": capacity_rows,
        "selected_factor_averages": selected_factor_averages,
        "external_size_bridge": external_size_bridge,
        "blockers": blockers,
        "answers": _answers(external_size_bridge, blockers),
        "source_paths": {
            "pb002_latest": str(pb002_dir / "latest.json"),
            "pb002_performance_by_size": str(pb002_dir / "portfolio_performance_by_size.csv"),
            "pb002_turnover": str(pb002_dir / "turnover_report.csv"),
            "full_model_transition": str(full_model_dir / "latest.json"),
            "portfolio123_evidence": str(p123_dir / "latest.json"),
        },
    }


def write_size_decomposition(
    report: dict[str, Any],
    report_root: Path | str = DEFAULT_REPORT_ROOT,
    *,
    output_path: Path | str | None = None,
) -> Path:
    if output_path is None:
        output = Path(report_root).parent / OUTPUT_NAME
    else:
        output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(_markdown(report), encoding="utf-8")
    return output


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: Path, *, key: str | None = None) -> list[dict[str, Any]]:
    if path.suffix == ".json":
        payload = _read_json(path)
        value = payload.get(key or "")
        return value if isinstance(value, list) else []
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _capacity_rows(performance_rows: list[dict[str, Any]], turnover_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    turnover_by_size = {str(row.get("portfolio_size", "")): row.get("turnover", "") for row in turnover_rows}
    rows = []
    for row in performance_rows:
        size = str(row.get("portfolio_size", ""))
        rows.append(
            {
                "portfolio_size": size,
                "actual_max_holdings": row.get("actual_max_holdings", ""),
                "CAGR": _pct(row.get("CAGR")),
                "Sharpe": _num(row.get("Sharpe")),
                "turnover": _pct(turnover_by_size.get(size, row.get("turnover"))),
                "status": row.get("status", ""),
            }
        )
    return rows


def _selected_factor_averages(holdings_rows: list[dict[str, Any]], factor_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not holdings_rows:
        return {"status": DATA_REQUIRED, "reason": "No portfolio holdings rows available."}

    first_size = min(int(row["portfolio_size"]) for row in holdings_rows if str(row.get("portfolio_size", "")).isdigit())
    selected = [
        (str(row.get("rebalance_month")), str(row.get("ticker")))
        for row in holdings_rows
        if str(row.get("portfolio_size")) == str(first_size)
    ]
    selected_set = set(selected)
    selected_scores = [
        row
        for row in factor_rows
        if (str(row.get("rebalance_month")), str(row.get("ticker"))) in selected_set and str(row.get("status")) == "READY"
    ]
    if not selected_scores:
        return {"status": DATA_REQUIRED, "reason": "No matched factor score rows available for selected holdings."}

    return {
        "status": "PRICE_ONLY_PROXY",
        "portfolio_size_used": first_size,
        "holding_month_rows": len(selected),
        "matched_score_rows": len(selected_scores),
        "momentum_score": _avg(selected_scores, "momentum_score"),
        "volatility_score": _avg(selected_scores, "volatility_score", fallback_key="risk_score"),
        "drawdown_risk_score": _avg(selected_scores, "drawdown_risk_score"),
        "relative_strength_score": _avg(selected_scores, "relative_strength_score"),
        "composite_score": _avg(selected_scores, "composite_score"),
        "blocked_factor_scores": "quality,value,growth,income,PEG,PEGY",
    }


def _external_size_bridge(full_model: dict[str, Any], p123: dict[str, Any]) -> dict[str, Any]:
    evidence_rows = full_model.get("portfolio123_evidence") or p123.get("model_scorecard") or []
    small_cap = next((row for row in evidence_rows if str(row.get("model", "")).lower() == "small cap quality"), {})
    model_cagr = 0.217
    benchmark_cagr = 0.105
    excess = model_cagr - benchmark_cagr
    return {
        "status": "EXTERNAL_MANUAL_DIRECTIONAL",
        "model": small_cap.get("model", "Small Cap Quality"),
        "model_CAGR": model_cagr,
        "benchmark": "Russell 2000",
        "benchmark_CAGR": benchmark_cagr,
        "excess_CAGR": excess,
        "benchmark_share_of_model_CAGR": benchmark_cagr / model_cagr,
        "excess_share_of_model_CAGR": excess / model_cagr,
        "limitation": "External/manual observation; no holdings, market-cap buckets, turnover, Sharpe, drawdown, region, or factor-level export.",
    }


def _blockers(full_model: dict[str, Any], eligible_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    gap_rows = full_model.get("full_model_gap_report") or []
    blockers = [
        {"blocker": "market_cap_by_security", "status": DATA_REQUIRED, "impact": "Cannot assign Micro/Small/Mid/Large cap buckets."},
        {"blocker": "region_or_country_by_security", "status": DATA_REQUIRED, "impact": "Cannot isolate Europe from US/global evidence."},
        {"blocker": "PIT_fundamentals", "status": DATA_REQUIRED, "impact": "Cannot score quality or value without lookahead controls."},
        {"blocker": "broad_universe", "status": DATA_REQUIRED, "impact": "Current price-only run has only " + str(len(eligible_rows)) + " eligible instruments."},
    ]
    for row in gap_rows:
        if row.get("status") == DATA_REQUIRED and row.get("requirement") not in {"broad universe"}:
            blockers.append({"blocker": str(row.get("requirement")), "status": DATA_REQUIRED, "impact": str(row.get("why_required"))})
    return blockers


def _cap_bucket_rows() -> list[dict[str, str]]:
    return [
        {"bucket": "Micro", "CAGR": DATA_REQUIRED, "Sharpe": DATA_REQUIRED, "turnover": DATA_REQUIRED, "average_factor_scores": DATA_REQUIRED, "reason": "No market-cap field in current source-backed universe."},
        {"bucket": "Small", "CAGR": DATA_REQUIRED, "Sharpe": DATA_REQUIRED, "turnover": DATA_REQUIRED, "average_factor_scores": DATA_REQUIRED, "reason": "Small-cap external evidence exists, but no holdings/export for native bucket computation."},
        {"bucket": "Mid", "CAGR": DATA_REQUIRED, "Sharpe": DATA_REQUIRED, "turnover": DATA_REQUIRED, "average_factor_scores": DATA_REQUIRED, "reason": "No market-cap field in current source-backed universe."},
        {"bucket": "Large", "CAGR": DATA_REQUIRED, "Sharpe": DATA_REQUIRED, "turnover": DATA_REQUIRED, "average_factor_scores": DATA_REQUIRED, "reason": "No market-cap field in current source-backed universe."},
    ]


def _answers(external_size_bridge: dict[str, Any], blockers: list[dict[str, str]]) -> list[dict[str, str]]:
    excess = _pct(external_size_bridge["excess_CAGR"])
    benchmark_share = _pct(external_size_bridge["benchmark_share_of_model_CAGR"])
    excess_share = _pct(external_size_bridge["excess_share_of_model_CAGR"])
    return [
        {
            "question": "Is Europe's alpha concentrated in microcaps?",
            "answer": "DATA_REQUIRED. No Europe region field and no market-cap buckets exist in the current evidence, so microcap concentration cannot be measured.",
        },
        {
            "question": "Does quality matter more in microcaps?",
            "answer": "DATA_REQUIRED. Quality is blocked until PIT fundamentals and filing-date controls exist.",
        },
        {
            "question": "Does value matter more in microcaps?",
            "answer": "DATA_REQUIRED. Value is blocked until PIT fundamentals and valuation fields exist.",
        },
        {
            "question": "Does momentum matter less in microcaps?",
            "answer": "DATA_REQUIRED. Momentum is available only as a price-only score without cap buckets.",
        },
        {
            "question": "How much of the observed Small Cap Quality model is size exposure?",
            "answer": f"External bridge only: Russell 2000 CAGR is 10.5% versus Small Cap Quality 21.7%, so small-cap benchmark return is {benchmark_share} of observed CAGR and the observed excess is {excess} ({excess_share} of observed CAGR). This is not a true attribution.",
        },
    ]


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Size Decomposition",
        "",
        f"Generated: {report['created_at']}",
        "",
        f"Authority: {report['authority']}",
        "",
        "## Verdict",
        "",
        "True Micro/Small/Mid/Large decomposition is DATA_REQUIRED. The current source-backed Portfolio Atlas artifacts do not contain security-level market cap, country/region, or PIT fundamental fields. No consumer should infer size truth from ticker names, liquidity, or code.",
        "",
        "The only numeric size boundary available is external/manual: Small Cap Quality was recorded at 21.7% 10Y annualized versus Russell 2000 at 10.5%, leaving 11.2% annualized observed excess. That means the small-cap benchmark return accounts for 48.4% of the observed model CAGR and the unexplained excess accounts for 51.6%. This is directional evidence, not attribution.",
        "",
        "## Cap Buckets",
        "",
        "| Bucket | CAGR | Sharpe | Turnover | Average factor scores | Reason |",
        "|---|---:|---:|---:|---|---|",
    ]
    for row in report["cap_buckets"]:
        lines.append(f"| {row['bucket']} | {row['CAGR']} | {row['Sharpe']} | {row['turnover']} | {row['average_factor_scores']} | {row['reason']} |")

    lines.extend(
        [
            "",
            "## Available Size Proxy",
            "",
            "This is portfolio-size capacity sensitivity, not market-cap decomposition. All requested sizes collapse to the same 11 eligible holdings.",
            "",
            "| Requested portfolio size | Actual max holdings | CAGR | Sharpe | Turnover | Status |",
            "|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in report["capacity_rows"]:
        lines.append(f"| {row['portfolio_size']} | {row['actual_max_holdings']} | {row['CAGR']} | {row['Sharpe']} | {row['turnover']} | {row['status']} |")

    factor = report["selected_factor_averages"]
    lines.extend(
        [
            "",
            "## Average Factor Scores",
            "",
            "| Scope | Momentum | Volatility | Drawdown risk | Relative strength | Composite | Blocked scores |",
            "|---|---:|---:|---:|---:|---:|---|",
            f"| Price-only selected holdings, portfolio size {factor.get('portfolio_size_used', 'NA')} | {_num(factor.get('momentum_score'))} | {_num(factor.get('volatility_score'))} | {_num(factor.get('drawdown_risk_score'))} | {_num(factor.get('relative_strength_score'))} | {_num(factor.get('composite_score'))} | {factor.get('blocked_factor_scores', DATA_REQUIRED)} |",
            "",
            "Quality, value, growth, income, PEG, and PEGY are not computed here because the full-model transition artifacts mark PIT fundamentals and related fields as DATA_REQUIRED.",
            "",
            "## Research Questions",
            "",
        ]
    )
    for row in report["answers"]:
        lines.append(f"- {row['question']} {row['answer']}")

    lines.extend(["", "## Data Blockers", "", "| Blocker | Status | Impact |", "|---|---|---|"])
    for row in report["blockers"]:
        lines.append(f"| {row['blocker']} | {row['status']} | {row['impact']} |")

    lines.extend(["", "## Source Artifacts", ""])
    for label, path in report["source_paths"].items():
        lines.append(f"- {label}: `{path}`")
    lines.append("")
    return "\n".join(lines)


def _avg(rows: list[dict[str, Any]], key: str, *, fallback_key: str | None = None) -> float:
    vals = []
    for row in rows:
        value = row.get(key)
        if not _is_float(value) and fallback_key is not None:
            value = row.get(fallback_key)
        if _is_float(value):
            vals.append(float(value))
    return mean(vals) if vals else 0.0


def _is_float(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _pct(value: Any) -> str:
    if not _is_float(value):
        return DATA_REQUIRED if value in {None, ""} else str(value)
    return f"{float(value) * 100:.2f}%"


def _num(value: Any) -> str:
    if not _is_float(value):
        return DATA_REQUIRED if value in {None, ""} else str(value)
    return f"{float(value):.3f}"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
