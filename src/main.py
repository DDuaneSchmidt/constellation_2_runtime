from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "EFFICIENT_FRONTIER.md"
FINAL_RANKING_GLOB = "reports/atlas_v2_research_os/final_candidate_ranking/*/final_candidate_ranking_report.json"
VERIFIED_GRAPH_GLOB = (
    "/home/node/constellation_runtime_data/truth/reports/"
    "aegis_verified_runtime_graph_v1/*/verified_runtime_graph.v1.json"
)
ULTRASAFE_ID = "benchmark_override_ultrasafe"


@dataclass(frozen=True)
class FrontierCandidate:
    candidate_id: str
    label: str
    mechanism: str
    classification: str
    source_bucket: str
    source: str
    final_score: float
    max_drawdown: float | None
    sample_size: int
    expectancy: float | None
    cagr: float | None = None
    sharpe: float | None = None

    @property
    def risk(self) -> float:
        if self.max_drawdown is None:
            return float("inf")
        return abs(self.max_drawdown)

    @property
    def is_ultrasafe(self) -> bool:
        return self.candidate_id == ULTRASAFE_ID


def ultrasafe_benchmark() -> FrontierCandidate:
    return FrontierCandidate(
        candidate_id=ULTRASAFE_ID,
        label="UltraSafe",
        mechanism="benchmark_override",
        classification="benchmark_override",
        source_bucket="benchmark_override",
        source="benchmark_override",
        final_score=0.0,
        max_drawdown=-0.36,
        sample_size=0,
        expectancy=None,
        cagr=0.19,
        sharpe=1.0,
    )


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _latest_path(pattern: str) -> Path:
    matches = sorted(Path("/").glob(pattern.removeprefix("/")) if pattern.startswith("/") else REPO_ROOT.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No files matched {pattern}")
    return matches[-1]


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_rate(value: Any) -> float | None:
    parsed = _as_float(value)
    if parsed is None:
        return None
    if abs(parsed) > 1.0:
        return parsed / 100.0
    return parsed


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _candidate_rows(report: dict[str, Any]) -> list[FrontierCandidate]:
    rows: list[FrontierCandidate] = []
    for bucket in ("top_20_robust_candidates", "excluded_candidates"):
        for raw in report.get(bucket, []):
            final_score = _as_float(raw.get("final_score"))
            max_drawdown = _as_rate(raw.get("max_drawdown") or raw.get("max_dd"))
            candidate_id = str(raw.get("candidate_id") or "").strip()
            if not candidate_id or final_score is None:
                continue
            rows.append(
                FrontierCandidate(
                    candidate_id=candidate_id,
                    label=candidate_id,
                    mechanism=str(raw.get("mechanism") or ""),
                    classification=str(raw.get("classification") or raw.get("backtest_classification") or ""),
                    source_bucket=bucket,
                    source=str(raw.get("source") or "local_candidate_result"),
                    final_score=final_score,
                    max_drawdown=max_drawdown,
                    sample_size=_as_int(raw.get("sample_size")),
                    expectancy=_as_float(raw.get("expectancy")),
                    cagr=_as_rate(raw.get("cagr") or raw.get("annualized_return") or raw.get("annual_return")),
                    sharpe=_as_float(raw.get("sharpe") or raw.get("sharpe_ratio")),
                )
            )
    rows.sort(key=lambda row: (-row.final_score, row.risk, row.candidate_id))
    return rows


def _dominators(
    target: FrontierCandidate,
    candidates: list[FrontierCandidate],
    *,
    return_attr: str,
) -> list[FrontierCandidate]:
    target_return = float(getattr(target, return_attr))
    dominators: list[FrontierCandidate] = []
    for other in candidates:
        if other.candidate_id == target.candidate_id:
            continue
        other_return = float(getattr(other, return_attr))
        if other.risk <= target.risk and other_return >= target_return:
            if other.risk < target.risk or other_return > target_return:
                dominators.append(other)
    dominators.sort(key=lambda row: (row.risk, -float(getattr(row, return_attr)), row.candidate_id))
    return dominators


def _benchmark_metrics_available(row: FrontierCandidate) -> bool:
    return row.cagr is not None and row.sharpe is not None and row.max_drawdown is not None


def _benchmark_dominates(left: FrontierCandidate, right: FrontierCandidate) -> bool:
    if not _benchmark_metrics_available(left) or not _benchmark_metrics_available(right):
        return False
    assert (
        left.cagr is not None
        and left.sharpe is not None
        and left.max_drawdown is not None
        and right.cagr is not None
        and right.sharpe is not None
        and right.max_drawdown is not None
    )
    no_worse = left.cagr >= right.cagr and left.sharpe >= right.sharpe and left.max_drawdown >= right.max_drawdown
    strictly_better = left.cagr > right.cagr or left.sharpe > right.sharpe or left.max_drawdown > right.max_drawdown
    return no_worse and strictly_better


def benchmark_dominators(target: FrontierCandidate, candidates: list[FrontierCandidate]) -> list[FrontierCandidate]:
    complete = complete_frontier_candidates(candidates)
    rows = [row for row in complete if row.candidate_id != target.candidate_id and _benchmark_dominates(row, target)]
    rows.sort(key=lambda row: (-(row.cagr or 0.0), -(row.sharpe or 0.0), -row.max_drawdown, row.candidate_id))
    return rows


def benchmark_dominated_by(target: FrontierCandidate, candidates: list[FrontierCandidate]) -> list[FrontierCandidate]:
    complete = complete_frontier_candidates(candidates)
    rows = [row for row in complete if row.candidate_id != target.candidate_id and _benchmark_dominates(target, row)]
    rows.sort(key=lambda row: (row.cagr or 0.0, row.sharpe or 0.0, row.max_drawdown, row.candidate_id))
    return rows


def benchmark_relation(left: FrontierCandidate, right: FrontierCandidate) -> str:
    if _benchmark_dominates(left, right):
        return "dominates"
    if _benchmark_dominates(right, left):
        return "dominated"
    if not _benchmark_metrics_available(left) or not _benchmark_metrics_available(right):
        return "insufficient_metrics"
    return "different_tradeoff"


def _frontier(candidates: list[FrontierCandidate], *, return_attr: str) -> list[FrontierCandidate]:
    return [row for row in candidates if not _dominators(row, candidates, return_attr=return_attr)]


def benchmark_frontier(candidates: list[FrontierCandidate]) -> list[FrontierCandidate]:
    comparable = complete_frontier_candidates(candidates)
    return [row for row in comparable if not benchmark_dominators(row, comparable)]


def complete_frontier_candidates(candidates: list[FrontierCandidate]) -> list[FrontierCandidate]:
    return [row for row in candidates if _benchmark_metrics_available(row)]


def incomplete_frontier_candidates(candidates: list[FrontierCandidate]) -> list[FrontierCandidate]:
    return [row for row in candidates if not _benchmark_metrics_available(row)]


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.3f}%"


def _fmt_metric_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"


def _fmt_num(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.6f}"


def _fmt_signed_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:+.2f} pp"


def _fmt_signed_num(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.3f}"


def _table(
    rows: list[FrontierCandidate],
    *,
    return_attr: str = "final_score",
    include_expectancy: bool = True,
    limit: int | None = None,
) -> str:
    selected = rows[:limit] if limit else rows
    header = "| Rank | Candidate | Source | Mechanism | Class | Risk max DD | Return expectancy | Return score | N | Frontier |\n"
    sep = "|---:|---|---|---|---|---:|---:|---:|---:|---|\n"
    frontier_ids = {row.candidate_id for row in _frontier(rows, return_attr=return_attr)}
    lines = [header, sep]
    for idx, row in enumerate(selected, 1):
        expectancy = _fmt_pct(row.expectancy) if include_expectancy else "not exposed"
        frontier_flag = "yes" if row.candidate_id in frontier_ids else "no"
        lines.append(
            "| "
            f"{idx} | `{row.label}` | {row.source} | {row.mechanism or 'n/a'} | {row.classification or 'n/a'} | "
            f"{_fmt_pct(row.risk)} | {expectancy} | {_fmt_num(row.final_score)} | {row.sample_size} | {frontier_flag} |\n"
        )
    return "".join(lines)


def _benchmark_table(rows: list[FrontierCandidate]) -> str:
    header = "| Point | Source | CAGR | Sharpe | Max DD | Benchmark frontier |\n"
    sep = "|---|---|---:|---:|---:|---|\n"
    frontier_ids = {row.candidate_id for row in benchmark_frontier(rows)}
    lines = [header, sep]
    for row in rows:
        frontier_flag = "yes" if row.candidate_id in frontier_ids else "no"
        lines.append(
            f"| `{row.label}` | {row.source} | {_fmt_metric_pct(row.cagr)} | {_fmt_num(row.sharpe)} | "
            f"{_fmt_metric_pct(row.max_drawdown)} | {frontier_flag} |\n"
        )
    return "".join(lines)


def _candidate_list(rows: list[FrontierCandidate]) -> str:
    if not rows:
        return "- None.\n"
    return "".join(f"- `{row.label}` source={row.source} cagr={_fmt_metric_pct(row.cagr)} sharpe={_fmt_num(row.sharpe)} max_dd={_fmt_metric_pct(row.max_drawdown)}\n" for row in rows)


def _incomplete_candidate_list(rows: list[FrontierCandidate], *, limit: int = 50) -> str:
    if not rows:
        return "- None.\n"
    selected = rows[:limit]
    lines = []
    for row in selected:
        missing = []
        if row.cagr is None:
            missing.append("cagr")
        if row.sharpe is None:
            missing.append("sharpe")
        if row.max_drawdown is None:
            missing.append("max_drawdown")
        lines.append(
            f"- `{row.label}` source={row.source} missing={','.join(missing)} "
            f"cagr={_fmt_metric_pct(row.cagr)} sharpe={_fmt_num(row.sharpe)} "
            f"max_dd={_fmt_metric_pct(row.max_drawdown)}\n"
        )
    if len(rows) > limit:
        lines.append(f"- ... {len(rows) - limit} more incomplete candidate(s) omitted from this list.\n")
    return "".join(lines)


def _runtime_status() -> tuple[str, str, Path | None]:
    try:
        path = _latest_path(VERIFIED_GRAPH_GLOB)
    except FileNotFoundError:
        return "UNKNOWN", "UNKNOWN", None
    graph = _load_json(path)
    return (
        str(graph.get("active_mode_readiness_status") or graph.get("graph_status") or "UNKNOWN"),
        str(graph.get("active_mode") or "UNKNOWN"),
        path,
    )


def _best_challenger(candidates: list[FrontierCandidate]) -> FrontierCandidate | None:
    complete_rows = complete_frontier_candidates(candidates)
    if not complete_rows:
        return None
    return sorted(
        complete_rows,
        key=lambda row: (-(row.cagr or 0.0), -(row.sharpe or 0.0), -float(row.max_drawdown or -1.0), row.candidate_id),
    )[0]


def _gap_to_ultrasafe(challenger: FrontierCandidate | None, ultrasafe: FrontierCandidate) -> dict[str, float | None]:
    if challenger is None:
        return {"cagr": None, "sharpe": None, "max_drawdown": None}
    return {
        "cagr": None if challenger.cagr is None or ultrasafe.cagr is None else challenger.cagr - ultrasafe.cagr,
        "sharpe": None if challenger.sharpe is None or ultrasafe.sharpe is None else challenger.sharpe - ultrasafe.sharpe,
        "max_drawdown": challenger.max_drawdown - ultrasafe.max_drawdown,
    }


def build_efficient_frontier_markdown(top_n: int = 50) -> tuple[str, dict[str, Any]]:
    source_path = _latest_path(FINAL_RANKING_GLOB)
    report = _load_json(source_path)
    all_rows = _candidate_rows(report)
    top_rows = all_rows[:top_n]
    ultrasafe = ultrasafe_benchmark()
    complete_rows = complete_frontier_candidates(top_rows)
    incomplete_rows = incomplete_frontier_candidates(top_rows)
    benchmark_points = [ultrasafe, *complete_rows]
    score_frontier = _frontier(top_rows, return_attr="final_score")
    expectancy_rows = [row for row in top_rows if row.expectancy is not None]
    expectancy_frontier = _frontier(expectancy_rows, return_attr="expectancy")
    ultra_dominators = benchmark_dominators(ultrasafe, complete_rows)
    ultra_dominated = benchmark_dominated_by(ultrasafe, complete_rows)
    benchmark_frontier_ids = {row.candidate_id for row in benchmark_frontier(benchmark_points)}
    best = _best_challenger(complete_rows)
    gaps = _gap_to_ultrasafe(best, ultrasafe)
    runtime_status, active_mode, graph_path = _runtime_status()

    if ultra_dominators:
        dominated_answer = "Yes."
    else:
        dominated_answer = "No."

    if ultra_dominated:
        ultra_dominates_answer = f"UltraSafe dominates {len(ultra_dominated)} local candidate(s)."
    else:
        ultra_dominates_answer = "UltraSafe does not strictly dominate any local candidate."

    relative = (
        "UltraSafe is non-dominated on the strict CAGR/Sharpe/drawdown benchmark frontier. "
        "This does not mean Aegis discovered UltraSafe; it is injected only as `benchmark_override`."
        if ultrasafe.candidate_id in benchmark_frontier_ids
        else "UltraSafe is off the strict benchmark frontier."
    )

    best_label = f"`{best.label}`" if best else "n/a"
    best_gap = (
        f"Best complete local challenger is {best_label}. Gap vs UltraSafe: "
        f"CAGR {_fmt_signed_pct(gaps['cagr'])}, Sharpe {_fmt_signed_num(gaps['sharpe'])}, "
        f"drawdown {_fmt_signed_pct(gaps['max_drawdown'])}."
    )

    caveat = (
        f"The top-50 local comparison uses `{source_path.relative_to(REPO_ROOT)}` without changing candidate results. "
        f"Only {len(expectancy_rows)} of the top {len(top_rows)} rows expose true expectancy, and "
        f"{len(complete_rows)} expose complete benchmark metrics. UltraSafe is an explicit benchmark_override."
    )

    lines = [
        "# Efficient Frontier Analysis\n\n",
        "## Answer\n\n",
        f"- UltraSafe dominated by any local candidate? {dominated_answer}\n",
        "- Which candidates dominate UltraSafe?\n",
        _candidate_list(ultra_dominators),
        "- Which candidates are dominated by UltraSafe?\n",
        _candidate_list(ultra_dominated),
        f"- Where does UltraSafe sit relative to the local frontier? {relative}\n",
        f"- Complete best challenger: {best_label}\n",
        f"- Best challenger gap: {best_gap}\n",
        f"- Incomplete candidates excluded from comparison: {len(incomplete_rows)}\n",
        f"- Runtime truth context: `{runtime_status}` in `{active_mode}`. This report is read-only research analysis, not trade advice.\n\n",
        "## Inputs\n\n",
        f"- Requested local candidate count: {top_n}\n",
        f"- Local candidate rows available for score/drawdown comparison: {len(top_rows)}\n",
        f"- Local candidate rows with true expectancy: {len(expectancy_rows)}\n",
        f"- Complete local benchmark candidates with CAGR, Sharpe, and max_drawdown: {len(complete_rows)}\n",
        f"- Incomplete local benchmark candidates excluded from benchmark comparison: {len(incomplete_rows)}\n",
        "- UltraSafe benchmark: CAGR 19.00%, Sharpe 1.000000, Max DD -36.00%, source `benchmark_override`\n",
        f"- Source report: `{source_path.relative_to(REPO_ROOT)}`\n",
    ]
    if graph_path:
        lines.append(f"- Verified runtime graph: `{graph_path}`\n")
    lines.extend(
        [
            f"- Data caveat: {caveat}\n\n",
            "## Benchmark Frontier\n\n",
            _benchmark_table([ultrasafe, *complete_rows]),
            "\n## Incomplete Frontier Candidates\n\n",
            _incomplete_candidate_list(incomplete_rows),
            "\n## Risk vs Return Summary\n\n",
            f"- Score frontier points: {len(score_frontier)} of {len(top_rows)} local top candidates.\n",
            f"- Expectancy frontier points: {len(expectancy_frontier)} of {len(expectancy_rows)} local candidates with true expectancy.\n",
            "- Score/expectancy frontier excludes UltraSafe because UltraSafe is not a discovered local candidate result.\n",
            "- Strict benchmark dominance uses only complete candidates: CAGR higher is better, Sharpe higher is better, and max drawdown less negative is better.\n\n",
            "## Score Frontier Points\n\n",
            _table(score_frontier, return_attr="final_score", limit=None),
            "\n## Expectancy Frontier Points\n\n",
            _table(expectancy_frontier, return_attr="expectancy", limit=None),
            "\n## Top 50 Local Risk vs Return\n\n",
            _table(top_rows, return_attr="final_score", limit=None),
        ]
    )
    summary = {
        "output": str(DEFAULT_OUTPUT),
        "source": str(source_path),
        "top_rows": len(top_rows),
        "complete_frontier_candidate_count": len(complete_rows),
        "incomplete_frontier_candidate_count": len(incomplete_rows),
        "incomplete_frontier_candidates": [row.candidate_id for row in incomplete_rows],
        "score_frontier_count": len(score_frontier),
        "expectancy_rows": len(expectancy_rows),
        "expectancy_frontier_count": len(expectancy_frontier),
        "ultrasafe_present": True,
        "ultrasafe_source": ultrasafe.source,
        "ultrasafe_dominated": bool(ultra_dominators),
        "ultrasafe_dominator_count": len(ultra_dominators),
        "ultrasafe_dominates_count": len(ultra_dominated),
        "best_challenger": None if best is None else best.candidate_id,
        "best_challenger_gap": gaps,
        "local_comparable_cagr_sharpe_count": len(complete_rows),
        "runtime_status": runtime_status,
    }
    return "".join(lines), summary


def efficient_frontier_analysis(args: argparse.Namespace) -> int:
    markdown, summary = build_efficient_frontier_markdown(top_n=args.top_n)
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(markdown, encoding="utf-8")
    summary["output"] = str(output)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def portfolio_discovery_report(args: argparse.Namespace) -> int:
    from src.efficient_frontier_report import portfolio_discovery_report as run_report

    return run_report(args)


def ultrasafe_vulnerability_analysis(args: argparse.Namespace) -> int:
    from src.ultrasafe_vulnerability_analysis import run as run_report

    return run_report(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Constellation local research commands.")
    sub = parser.add_subparsers(dest="command", required=True)
    frontier = sub.add_parser("efficient-frontier-analysis")
    frontier.add_argument("--top-n", type=int, default=50)
    frontier.add_argument("--output", default=str(DEFAULT_OUTPUT))
    frontier.set_defaults(func=efficient_frontier_analysis)
    discovery = sub.add_parser("portfolio-discovery-report")
    discovery.add_argument("--top-n", type=int, default=50)
    discovery.add_argument("--output", default=str(REPO_ROOT / "docs" / "PORTFOLIO_DISCOVERY_REPORT.md"))
    discovery.set_defaults(func=portfolio_discovery_report)
    vulnerability = sub.add_parser("ultrasafe-vulnerability-analysis")
    vulnerability.add_argument("--portfolio-returns", default=str(REPO_ROOT / "reports/portfolio_research_os/price_only_baseline_001/portfolio_monthly_returns.csv"))
    vulnerability.add_argument("--benchmark-returns", default=str(REPO_ROOT / "reports/portfolio_research_os/price_only_baseline_001/benchmark_monthly_returns.csv"))
    vulnerability.add_argument("--portfolio-size", type=int, default=25)
    vulnerability.add_argument("--benchmark", default="SPY")
    vulnerability.add_argument("--output", default=str(REPO_ROOT / "docs" / "ULTRASAFE_VULNERABILITY_ANALYSIS.md"))
    vulnerability.set_defaults(func=ultrasafe_vulnerability_analysis)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
