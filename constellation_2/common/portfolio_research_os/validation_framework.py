from __future__ import annotations

import csv
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

DEFAULT_STORE_ROOT = Path("reports/portfolio_research_os")
REPORT_DIRNAME = "validation_framework"

AUTHORITY_BOUNDARY = (
    "Research-only Portfolio Atlas validation framework. No live portfolio, no replacement recommendation, "
    "no trades, no position sizing, no allocation, no broker connection, and no trading authority."
)

BENCHMARK_COLUMNS = [
    "benchmark_id",
    "benchmark_name",
    "definition",
    "gross_return_assumption",
    "fee_assumption",
    "net_return_assumption",
    "volatility_assumption",
    "drawdown_assumption",
    "assumption_status",
    "notes",
]
WALK_FORWARD_COLUMNS = [
    "window_id",
    "train_start",
    "train_end",
    "validation_start",
    "validation_end",
    "test_start",
    "test_end",
    "rebalance_frequency",
    "embargo_gap_days",
    "portfolio_result_status",
    "benchmark_comparison_status",
    "classification",
    "notes",
]
BIAS_COLUMNS = ["audit_item", "classification", "evidence_status", "notes", "required_remediation"]
DECISION_COLUMNS = [
    "decision",
    "benchmark_comparison",
    "walk_forward_stability",
    "drawdown_status",
    "turnover_status",
    "tax_risk_proxy",
    "data_integrity",
    "rationale",
    "authority_boundary",
]
NEXT_PHASE_COLUMNS = ["recommendation", "priority", "rationale", "required_before_next_phase", "authority_boundary"]


def run_validation_framework(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    oak_harvest_proxy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    report = build_validation_framework(root=root, created_at=created_at, oak_harvest_proxy=oak_harvest_proxy)
    write_validation_framework(report, root=root)
    return report


def build_validation_framework(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    created_at: str | None = None,
    oak_harvest_proxy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    created = created_at or _now()
    inputs = _input_status(root_path)
    benchmarks = _benchmark_definitions(oak_harvest_proxy or {})
    walk_forward = _walk_forward_results(inputs)
    bias = _bias_integrity_audit(inputs)
    decision = _validation_decision(inputs, walk_forward, bias)
    next_phase = _next_phase_recommendation(decision)
    summary = {
        "benchmark_count": len(benchmarks),
        "oak_harvest_assumption_status": _find(benchmarks, "benchmark_id", "OAK_HARVEST_PROXY").get("assumption_status"),
        "walk_forward_status": _overall_walk_status(walk_forward),
        "bias_status": _overall_bias_status(bias),
        "validation_decision": decision[0]["decision"],
        "next_phase_recommendation": next_phase[0]["recommendation"],
        "portfolio_data_available": inputs["portfolio_returns"]["loaded"],
        "holdings_data_available": inputs["holdings_history"]["loaded"],
        "no_trading_authority": True,
        "no_replacement_recommendation_authority": True,
    }
    return {
        "schema_id": "portfolio_research_os_validation_framework",
        "schema_version": "1.0",
        "report_type": "PORTFOLIO_ATLAS_VALIDATION_FRAMEWORK",
        "builds": "P008-P012",
        "created_at": created,
        "day": created[:10],
        "summary": summary,
        "input_status": inputs,
        "benchmark_definitions": benchmarks,
        "walk_forward_results": walk_forward,
        "bias_integrity_audit": bias,
        "validation_decision": decision,
        "next_phase_recommendation": next_phase,
        "authority_boundary": AUTHORITY_BOUNDARY,
    }


def write_validation_framework(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "benchmark_definitions": out_dir / "benchmark_definitions.csv",
        "walk_forward_results": out_dir / "walk_forward_results.csv",
        "bias_integrity_audit": out_dir / "bias_integrity_audit.csv",
        "validation_decision": out_dir / "validation_decision.csv",
        "next_phase_recommendation": out_dir / "next_phase_recommendation.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_validation_framework_summary(report), encoding="utf-8")
    _write_csv(paths["benchmark_definitions"], BENCHMARK_COLUMNS, report.get("benchmark_definitions") or [])
    _write_csv(paths["walk_forward_results"], WALK_FORWARD_COLUMNS, report.get("walk_forward_results") or [])
    _write_csv(paths["bias_integrity_audit"], BIAS_COLUMNS, report.get("bias_integrity_audit") or [])
    _write_csv(paths["validation_decision"], DECISION_COLUMNS, report.get("validation_decision") or [])
    _write_csv(paths["next_phase_recommendation"], NEXT_PHASE_COLUMNS, report.get("next_phase_recommendation") or [])
    return paths


def render_validation_framework_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary") or {}
    lines = [
        "# Portfolio Atlas P008-P012 - Validation Framework",
        "",
        "## Benchmarks",
        "",
    ]
    for row in report.get("benchmark_definitions") or []:
        lines.append(f"- {row.get('benchmark_name')}: {row.get('definition')} ({row.get('assumption_status')})")
    lines.extend(
        [
            "",
            "## Walk-Forward Status",
            "",
            f"- {summary.get('walk_forward_status')}",
            "",
            "## Bias Audit",
            "",
            f"- {summary.get('bias_status')}",
            "",
            "## Validation Decision",
            "",
            f"- {summary.get('validation_decision')}",
            "",
            "## Next Phase",
            "",
            f"- {summary.get('next_phase_recommendation')}",
            "",
            "## Authority",
            "",
            AUTHORITY_BOUNDARY,
            "",
        ]
    )
    return "\n".join(lines)


def _benchmark_definitions(oak: dict[str, Any]) -> list[dict[str, Any]]:
    gross = float(oak.get("gross_return_assumption", 0.06))
    fee = float(oak.get("fee_assumption", 0.01))
    net = float(oak.get("net_return_assumption", gross - fee))
    volatility = oak.get("volatility_assumption", "")
    drawdown = oak.get("drawdown_assumption", "")
    return [
        {
            "benchmark_id": "VTI_PROXY",
            "benchmark_name": "VTI proxy",
            "definition": "US total-market equity ETF proxy; requires source-backed return series before comparison.",
            "gross_return_assumption": "",
            "fee_assumption": "",
            "net_return_assumption": "",
            "volatility_assumption": "",
            "drawdown_assumption": "",
            "assumption_status": "DATA_REQUIRED",
            "notes": "Benchmark definition only until market data is supplied.",
        },
        {
            "benchmark_id": "SIXTY_FORTY_PROXY",
            "benchmark_name": "60/40 proxy",
            "definition": "60% broad equity proxy plus 40% aggregate bond proxy, periodically rebalanced.",
            "gross_return_assumption": "",
            "fee_assumption": "",
            "net_return_assumption": "",
            "volatility_assumption": "",
            "drawdown_assumption": "",
            "assumption_status": "DATA_REQUIRED",
            "notes": "Requires equity and bond total-return series.",
        },
        {
            "benchmark_id": "SIMPLE_FACTOR_PORTFOLIO",
            "benchmark_name": "simple factor portfolio",
            "definition": "Equal-weight value, quality, momentum, and low-volatility factor proxies.",
            "gross_return_assumption": "",
            "fee_assumption": "",
            "net_return_assumption": "",
            "volatility_assumption": "",
            "drawdown_assumption": "",
            "assumption_status": "DATA_REQUIRED",
            "notes": "Requires point-in-time factor proxy series.",
        },
        {
            "benchmark_id": "OAK_HARVEST_PROXY",
            "benchmark_name": "Oak Harvest proxy",
            "definition": "Configurable advisor/proxy assumption until real statement or performance data is supplied.",
            "gross_return_assumption": _pct(gross),
            "fee_assumption": _pct(fee),
            "net_return_assumption": _pct(net),
            "volatility_assumption": volatility,
            "drawdown_assumption": drawdown,
            "assumption_status": "USER_ASSUMPTION",
            "notes": "Default placeholder: gross_return=6%, fee=1%, net_return=5%; not evidence of real Oak Harvest performance.",
        },
    ]


def _walk_forward_results(inputs: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    windows = _deterministic_windows(date(2018, 1, 1), count=6, train_months=36, validation_months=6, test_months=6, embargo_gap_days=30)
    rows = []
    for index, window in enumerate(windows, start=1):
        data_loaded = inputs["portfolio_returns"]["loaded"] and inputs["benchmark_returns"]["loaded"]
        rows.append(
            {
                "window_id": f"WF{index:03d}",
                **window,
                "rebalance_frequency": "quarterly",
                "portfolio_result_status": "AVAILABLE" if data_loaded else "DATA_MISSING",
                "benchmark_comparison_status": "AVAILABLE" if data_loaded else "DATA_MISSING",
                "classification": "DATA_INSUFFICIENT" if not data_loaded else "PENDING_EVALUATION",
                "notes": "Deterministic rolling/expanding validation window; no performance claim without portfolio and benchmark returns.",
            }
        )
    return rows


def _deterministic_windows(start: date, *, count: int, train_months: int, validation_months: int, test_months: int, embargo_gap_days: int) -> list[dict[str, str]]:
    rows = []
    for idx in range(count):
        train_start = _add_months(start, idx * test_months)
        train_end = _add_months(train_start, train_months)
        validation_start = train_end
        validation_end = _add_months(validation_start, validation_months)
        test_start = _add_days(validation_end, embargo_gap_days)
        test_end = _add_months(test_start, test_months)
        rows.append(
            {
                "train_start": train_start.isoformat(),
                "train_end": train_end.isoformat(),
                "validation_start": validation_start.isoformat(),
                "validation_end": validation_end.isoformat(),
                "test_start": test_start.isoformat(),
                "test_end": test_end.isoformat(),
                "embargo_gap_days": embargo_gap_days,
            }
        )
    return rows


def _bias_integrity_audit(inputs: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    pit = inputs["point_in_time_fundamentals"]["loaded"]
    delisted = inputs["delisted_security_master"]["loaded"]
    corp = inputs["corporate_actions"]["loaded"]
    dividends = inputs["dividend_adjustments"]["loaded"]
    holdings = inputs["holdings_history"]["loaded"]
    return [
        _bias("survivorship bias", "BIAS_RISK" if not delisted else "BIAS_CONTROLLED", delisted, "Missing delisted security master can overstate historical results.", "Provide delisted/security master and delisting return treatment."),
        _bias("lookahead bias", "DATA_INSUFFICIENT" if not pit else "BIAS_CONTROLLED", pit, "Point-in-time input availability is not proven.", "Provide timestamped feature availability and signal construction times."),
        _bias("point-in-time fundamental availability", "DATA_INSUFFICIENT" if not pit else "BIAS_CONTROLLED", pit, "Missing point-in-time fundamentals.", "Provide point-in-time fundamentals with filing/effective dates."),
        _bias("future index membership leakage", "BIAS_RISK" if not holdings else "BIAS_CONTROLLED", holdings, "Historical membership snapshots are not present.", "Provide historical universe membership by date."),
        _bias("missing delisted securities", "BIAS_RISK" if not delisted else "BIAS_CONTROLLED", delisted, "Delisted securities are not represented.", "Add delisted securities and terminal returns."),
        _bias("corporate action handling", "DATA_INSUFFICIENT" if not corp else "BIAS_CONTROLLED", corp, "Splits/mergers/spinoffs are not source-backed.", "Provide corporate action adjusted price pipeline evidence."),
        _bias("dividend adjustment", "DATA_INSUFFICIENT" if not dividends else "BIAS_CONTROLLED", dividends, "Total-return/dividend adjustment is not source-backed.", "Provide dividend-adjusted total-return series."),
    ]


def _bias(item: str, classification: str, present: bool, notes: str, remediation: str) -> dict[str, str]:
    return {
        "audit_item": item,
        "classification": classification,
        "evidence_status": "PRESENT" if present else "MISSING",
        "notes": notes,
        "required_remediation": remediation,
    }


def _validation_decision(inputs: dict[str, dict[str, Any]], walk: list[dict[str, Any]], bias: list[dict[str, str]]) -> list[dict[str, str]]:
    missing_data = not inputs["portfolio_returns"]["loaded"] or any(row["classification"] in {"DATA_INSUFFICIENT", "BIAS_RISK", "BIAS_CONFIRMED"} for row in bias)
    decision = "DATA_INSUFFICIENT" if missing_data else "PORTFOLIO_ATLAS_PROMISING"
    rationale = (
        "No source-backed Portfolio Atlas returns, benchmark returns, point-in-time fundamentals, delisted-security handling, "
        "corporate-action handling, or dividend adjustment evidence is available; success cannot be claimed."
        if missing_data
        else "Required evidence is present and ready for comparative scoring."
    )
    return [
        {
            "decision": decision,
            "benchmark_comparison": "DATA_INSUFFICIENT" if missing_data else "PENDING_SCORE",
            "walk_forward_stability": _overall_walk_status(walk),
            "drawdown_status": "DATA_INSUFFICIENT",
            "turnover_status": "DATA_INSUFFICIENT",
            "tax_risk_proxy": "DATA_INSUFFICIENT",
            "data_integrity": _overall_bias_status(bias),
            "rationale": rationale,
            "authority_boundary": AUTHORITY_BOUNDARY,
        }
    ]


def _next_phase_recommendation(decision: list[dict[str, str]]) -> list[dict[str, str]]:
    value = decision[0]["decision"]
    if value == "DATA_INSUFFICIENT":
        rec = "IMPROVE_DATA_FIRST"
        rationale = "Validation framework exists, but Portfolio Atlas cannot be judged fairly until source-backed data integrity and performance inputs are present."
        required = "Portfolio returns, benchmark returns, point-in-time fundamentals, historical universe membership, delisted securities, corporate actions, dividend-adjusted total returns."
    elif value == "PORTFOLIO_ATLAS_PROMISING":
        rec = "CONTINUE_TO_RETIREMENT_ENGINE"
        rationale = "Evidence is sufficient for the next research-only retirement-engine phase."
        required = "Maintain evidence provenance and rerun validation."
    else:
        rec = "SIMPLIFY_MODEL"
        rationale = "Model needs simplification before further validation."
        required = "Reduce moving parts and rerun validation."
    return [{"recommendation": rec, "priority": "P0", "rationale": rationale, "required_before_next_phase": required, "authority_boundary": AUTHORITY_BOUNDARY}]


def _input_status(root: Path) -> dict[str, dict[str, Any]]:
    paths = {
        "portfolio_returns": root / "inputs" / "portfolio_returns.csv",
        "benchmark_returns": root / "inputs" / "benchmark_returns.csv",
        "holdings_history": root / "inputs" / "holdings_history.csv",
        "point_in_time_fundamentals": root / "inputs" / "point_in_time_fundamentals.csv",
        "delisted_security_master": root / "inputs" / "delisted_security_master.csv",
        "corporate_actions": root / "inputs" / "corporate_actions.csv",
        "dividend_adjustments": root / "inputs" / "dividend_adjustments.csv",
    }
    return {name: {"path": str(path), "exists": path.exists(), "loaded": path.exists()} for name, path in paths.items()}


def _overall_walk_status(rows: list[dict[str, Any]]) -> str:
    if not rows or any(row.get("classification") == "DATA_INSUFFICIENT" for row in rows):
        return "DATA_INSUFFICIENT"
    return "WALK_FORWARD_READY"


def _overall_bias_status(rows: list[dict[str, str]]) -> str:
    if any(row["classification"] == "BIAS_CONFIRMED" for row in rows):
        return "BIAS_CONFIRMED"
    if any(row["classification"] == "BIAS_RISK" for row in rows):
        return "BIAS_RISK"
    if any(row["classification"] == "DATA_INSUFFICIENT" for row in rows):
        return "DATA_INSUFFICIENT"
    return "BIAS_CONTROLLED"


def _find(rows: list[dict[str, Any]], key: str, value: Any) -> dict[str, Any]:
    return next((row for row in rows if row.get(key) == value), {})


def _add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, min(value.day, 28))


def _add_days(value: date, days: int) -> date:
    return date.fromordinal(value.toordinal() + days)


def _pct(value: float) -> str:
    return f"{value:.2%}"


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

