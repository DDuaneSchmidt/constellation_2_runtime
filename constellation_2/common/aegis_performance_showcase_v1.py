from __future__ import annotations

import html
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable, Mapping

from constellation_2.phaseD.lib.canon_json_v1 import canonical_sha256_hex_v1
from constellation_2.phaseD.lib.validate_against_schema_v1 import validate_against_repo_schema_v1


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_RELPATH_V1 = "governance/04_DATA/SCHEMAS/C2/REPORTS/aegis_performance_showcase.v1.schema.json"
SCHEMA_ID_V1 = "C2_AEGIS_PERFORMANCE_SHOWCASE_V1"
DATA_MODES_V1 = frozenset({"BACKTEST", "PAPER_TRADING", "LIVE"})
PCT_QUANT = Decimal("0.000001")
MONEY_QUANT = Decimal("0.000001")


@dataclass(frozen=True)
class ArtifactInputV1:
    logical_name: str
    path: str
    sha256: str
    payload: Any
    status: str = "PRESENT"


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _read_jsonl(path: Path) -> list[Any]:
    rows: list[Any] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if text:
                rows.append(json.loads(text))
    return rows


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _display_path(path: Path, repo_root: Path | None = None) -> str:
    resolved = path.resolve()
    root = repo_root.resolve() if repo_root is not None else REPO_ROOT.resolve()
    try:
        return resolved.relative_to(root).as_posix()
    except ValueError:
        return str(resolved)


def load_artifact_file_v1(
    path: str | Path,
    *,
    logical_name: str,
    repo_root: str | Path | None = None,
    jsonl: bool = False,
) -> ArtifactInputV1:
    artifact_path = Path(path)
    payload = _read_jsonl(artifact_path) if jsonl else _read_json(artifact_path)
    return ArtifactInputV1(
        logical_name=str(logical_name),
        path=_display_path(artifact_path, Path(repo_root) if repo_root is not None else REPO_ROOT),
        sha256=_sha256_file(artifact_path),
        payload=payload,
    )


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_text(value: Decimal, quant: Decimal = MONEY_QUANT) -> str:
    return format(value.quantize(quant, rounding=ROUND_HALF_UP), "f")


def _pct_text(value: Decimal) -> str:
    return _decimal_text(value, PCT_QUANT)


def _return_pct(current: Decimal, start: Decimal) -> Decimal:
    if start <= 0:
        return Decimal("0")
    return ((current / start) - Decimal("1")) * Decimal("100")


def _truthy_status(payload: Mapping[str, Any]) -> str:
    return str(payload.get("status") or "").strip().upper()


def _source_artifacts(artifacts: Iterable[ArtifactInputV1]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    rows: list[dict[str, Any]] = []
    for artifact in artifacts:
        key = (artifact.logical_name, artifact.path)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "logical_name": artifact.logical_name,
                "path": artifact.path,
                "sha256": artifact.sha256,
                "status": artifact.status,
            }
        )
    return rows


def _nav_points(nav_artifacts: Iterable[ArtifactInputV1]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for artifact in nav_artifacts:
        payload = artifact.payload
        if not isinstance(payload, Mapping):
            excluded.append({"path": artifact.path, "reason": "NAV artifact root is not an object."})
            continue
        day = str(payload.get("day_utc") or "").strip()
        nav_payload = payload.get("nav") if isinstance(payload.get("nav"), Mapping) else {}
        nav_total = _decimal(nav_payload.get("nav_total") if isinstance(nav_payload, Mapping) else None)
        status = _truthy_status(payload)
        reason_codes = [str(item) for item in payload.get("reason_codes") or []]
        if status == "BOOTSTRAP" or nav_total is None or nav_total <= 0:
            excluded.append(
                {
                    "path": artifact.path,
                    "day_utc": day,
                    "status": status or "UNKNOWN",
                    "nav_total": _decimal_text(nav_total) if nav_total is not None else None,
                    "reason": "Excluded from performance calculations because NAV is bootstrap, missing, or non-positive.",
                    "reason_codes": reason_codes,
                }
            )
            continue
        if status not in {"ACTIVE", "OK"}:
            excluded.append(
                {
                    "path": artifact.path,
                    "day_utc": day,
                    "status": status or "UNKNOWN",
                    "nav_total": _decimal_text(nav_total),
                    "reason": "Excluded from performance calculations because status is not ACTIVE or OK.",
                    "reason_codes": reason_codes,
                }
            )
            continue
        valid.append(
            {
                "date": day,
                "nav_total_decimal": nav_total,
                "nav_total": _decimal_text(nav_total),
                "status": status,
                "source_path": artifact.path,
                "source_sha256": artifact.sha256,
            }
        )
    valid.sort(key=lambda item: str(item["date"]))
    if not valid:
        return [], excluded
    start = valid[0]["nav_total_decimal"]
    for point in valid:
        point["cumulative_return_pct"] = _pct_text(_return_pct(point["nav_total_decimal"], start))
        del point["nav_total_decimal"]
    return valid, excluded


def _drawdown_curve(equity_curve: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    peak: Decimal | None = None
    rows: list[dict[str, Any]] = []
    for point in equity_curve:
        nav = _decimal(point.get("nav_total"))
        if nav is None:
            continue
        peak = nav if peak is None or nav > peak else peak
        drawdown = Decimal("0") if peak <= 0 else ((nav / peak) - Decimal("1")) * Decimal("100")
        rows.append(
            {
                "date": str(point.get("date") or ""),
                "nav_total": _decimal_text(nav),
                "peak_nav": _decimal_text(peak),
                "drawdown_pct": _pct_text(drawdown),
            }
        )
    return rows


def _max_drawdown_pct(drawdown_curve: Iterable[Mapping[str, Any]]) -> str | None:
    values = [_decimal(row.get("drawdown_pct")) for row in drawdown_curve]
    known = [value for value in values if value is not None]
    if not known:
        return None
    return _pct_text(min(known))


def _benchmark_curve(
    benchmark_artifact: ArtifactInputV1 | None,
    equity_curve: Iterable[Mapping[str, Any]],
    data_gaps: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if benchmark_artifact is None:
        data_gaps.append(
            {
                "field": "benchmark_curve",
                "severity": "missing",
                "reason": "No local benchmark artifact was supplied.",
                "required_artifacts": ["market_data_snapshot_v1/SPY/*.jsonl"],
            }
        )
        return []
    if not isinstance(benchmark_artifact.payload, list):
        data_gaps.append(
            {
                "field": "benchmark_curve",
                "severity": "invalid",
                "reason": "Benchmark artifact is not a JSONL row list.",
                "required_artifacts": [benchmark_artifact.path],
            }
        )
        return []
    needed_dates = {str(point.get("date") or "") for point in equity_curve if str(point.get("date") or "")}
    by_date: dict[str, Mapping[str, Any]] = {}
    for row in benchmark_artifact.payload:
        if not isinstance(row, Mapping):
            continue
        day = str(row.get("timestamp_utc") or "")[:10]
        if day in needed_dates:
            by_date[day] = row
    ordered_dates = sorted(needed_dates)
    missing = [day for day in ordered_dates if day not in by_date]
    if missing:
        data_gaps.append(
            {
                "field": "benchmark_curve",
                "severity": "partial",
                "reason": "SPY benchmark rows are not present for every valid NAV date.",
                "missing_dates": missing,
                "required_artifacts": [benchmark_artifact.path],
            }
        )
    rows: list[dict[str, Any]] = []
    start_close: Decimal | None = None
    for day in ordered_dates:
        row = by_date.get(day)
        if row is None:
            continue
        close = _decimal(row.get("adjusted_close") if row.get("adjusted_close") is not None else row.get("close"))
        if close is None or close <= 0:
            continue
        if start_close is None:
            start_close = close
        rows.append(
            {
                "date": day,
                "symbol": str(row.get("symbol") or "SPY"),
                "close": _decimal_text(close),
                "cumulative_return_pct": _pct_text(_return_pct(close, start_close)),
                "source_name": str(row.get("source_name") or ""),
                "source_hash": str(row.get("source_hash") or ""),
                "source_path": benchmark_artifact.path,
                "source_sha256": benchmark_artifact.sha256,
            }
        )
    return rows


def _capital_allocations(capital_allocation_artifact: ArtifactInputV1 | None) -> list[dict[str, Any]]:
    if capital_allocation_artifact is None or not isinstance(capital_allocation_artifact.payload, Mapping):
        return []
    rows: list[dict[str, Any]] = []
    for row in capital_allocation_artifact.payload.get("per_sleeve") or []:
        if not isinstance(row, Mapping):
            continue
        allowed = int(row.get("allowed_capital_at_risk_cents") or 0)
        used = int(row.get("used_capital_at_risk_cents") or 0)
        utilization = Decimal("0") if allowed <= 0 else (Decimal(used) / Decimal(allowed)) * Decimal("100")
        rows.append(
            {
                "sleeve_id": str(row.get("sleeve_id") or ""),
                "engine_ids": [str(item) for item in row.get("engine_ids") or []],
                "allowed_capital_at_risk_cents": allowed,
                "used_capital_at_risk_cents": used,
                "headroom_cents": int(row.get("headroom_cents") or 0),
                "utilization_pct": _pct_text(utilization),
                "source_path": capital_allocation_artifact.path,
                "source_sha256": capital_allocation_artifact.sha256,
            }
        )
    rows.sort(key=lambda item: str(item["sleeve_id"]))
    return rows


def _paper_orders(
    fill_ledger_artifacts: Iterable[ArtifactInputV1],
    order_plan_artifacts: Iterable[ArtifactInputV1],
    broker_submission_artifacts: Iterable[ArtifactInputV1],
) -> list[dict[str, Any]]:
    plans: dict[str, Mapping[str, Any]] = {}
    submissions: dict[str, Mapping[str, Any]] = {}
    for artifact in order_plan_artifacts:
        if isinstance(artifact.payload, Mapping):
            key = str(artifact.payload.get("intent_sha256") or artifact.payload.get("intent_hash") or "")
            if key:
                plans[key] = artifact.payload
    for artifact in broker_submission_artifacts:
        if isinstance(artifact.payload, Mapping):
            key = str(artifact.payload.get("submission_id") or artifact.payload.get("binding_hash") or "")
            if key:
                submissions[key] = artifact.payload

    rows: list[dict[str, Any]] = []
    for artifact in fill_ledger_artifacts:
        payload = artifact.payload
        if not isinstance(payload, Mapping):
            continue
        intent_sha = str(payload.get("intent_sha256") or "")
        submission_id = str(payload.get("submission_id") or payload.get("binding_hash") or "")
        plan = plans.get(intent_sha, {})
        submission = submissions.get(submission_id, {})
        rows.append(
            {
                "day_utc": str(payload.get("day_utc") or ""),
                "engine_id": str(payload.get("engine_id") or plan.get("engine_id") or ""),
                "source_intent_id": str(payload.get("source_intent_id") or plan.get("source_intent_id") or ""),
                "symbol": str(plan.get("symbol") or ""),
                "action": str(plan.get("action") or ""),
                "order_qty": int(payload.get("order_qty") or plan.get("qty_shares") or 0),
                "filled_qty": int(payload.get("filled_qty") or 0),
                "remaining_qty": int(payload.get("remaining_qty") or 0),
                "avg_fill_price_weighted": _decimal_text(_decimal(payload.get("avg_fill_price_weighted")) or Decimal("0")),
                "lifecycle_status": str(payload.get("lifecycle_status") or ""),
                "submission_status": str(submission.get("status") or ""),
                "broker_environment": str((submission.get("broker") or {}).get("environment") or ""),
                "source_path": artifact.path,
                "source_sha256": artifact.sha256,
            }
        )
    rows.sort(key=lambda item: (str(item["day_utc"]), str(item["engine_id"]), str(item["source_intent_id"])))
    return rows


def _data_label(data_mode: str) -> str:
    return "PAPER TRADING" if data_mode == "PAPER_TRADING" else data_mode


def build_aegis_performance_showcase_v1(
    *,
    report_date: str,
    data_mode: str,
    nav_artifacts: Iterable[ArtifactInputV1],
    benchmark_artifact: ArtifactInputV1 | None = None,
    paper_session_artifact: ArtifactInputV1 | None = None,
    capital_allocation_artifact: ArtifactInputV1 | None = None,
    fill_ledger_artifacts: Iterable[ArtifactInputV1] = (),
    order_plan_artifacts: Iterable[ArtifactInputV1] = (),
    broker_submission_artifacts: Iterable[ArtifactInputV1] = (),
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    mode = str(data_mode or "").strip()
    if mode not in DATA_MODES_V1:
        raise ValueError(f"data_mode must be one of {sorted(DATA_MODES_V1)}")
    if not str(report_date or "").strip():
        raise ValueError("report_date is required")

    nav_artifact_list = list(nav_artifacts)
    fill_ledger_list = list(fill_ledger_artifacts)
    order_plan_list = list(order_plan_artifacts)
    broker_submission_list = list(broker_submission_artifacts)
    data_gaps: list[dict[str, Any]] = []
    equity_curve, excluded_nav_points = _nav_points(nav_artifact_list)
    if not equity_curve:
        data_gaps.append(
            {
                "field": "equity_curve",
                "severity": "missing",
                "reason": "No ACTIVE/OK positive NAV points are available. Performance calculations are withheld.",
                "required_artifacts": ["accounting_v2/nav/<DAY>/nav.v2.json"],
            }
        )
    if len(equity_curve) < 20:
        data_gaps.append(
            {
                "field": "annualized_return_pct",
                "severity": "insufficient_history",
                "reason": "Annualized return is withheld until a longer validated NAV history exists.",
                "required_artifacts": ["20 or more ACTIVE/OK daily NAV points"],
            }
        )
        data_gaps.append(
            {
                "field": "sharpe_ratio",
                "severity": "insufficient_history",
                "reason": "Sharpe ratio is withheld until enough daily return observations exist.",
                "required_artifacts": ["daily return series from validated NAV history"],
            }
        )
    data_gaps.extend(
        [
            {
                "field": "win_rate_pct",
                "severity": "missing",
                "reason": "Win rate is not computed because no completed trade outcome ledger is available.",
                "required_artifacts": ["completed trade outcome records"],
            },
            {
                "field": "sleeve_curves",
                "severity": "missing",
                "reason": "Sleeve-level return/PnL series was not found. Sleeve returns are not fabricated.",
                "required_artifacts": ["sleeve_performance_truth or sleeve-level NAV/PnL series"],
            },
            {
                "field": "monthly_returns",
                "severity": "insufficient_history",
                "reason": "Monthly returns are withheld because available NAV history is too sparse.",
                "required_artifacts": ["validated month-end NAV or daily return history"],
            },
        ]
    )

    drawdown_curve = _drawdown_curve(equity_curve)
    benchmark_curve = _benchmark_curve(benchmark_artifact, equity_curve, data_gaps)
    capital_allocations = _capital_allocations(capital_allocation_artifact)
    paper_orders = _paper_orders(fill_ledger_list, order_plan_list, broker_submission_list)
    total_return = equity_curve[-1]["cumulative_return_pct"] if equity_curve else None
    max_drawdown = _max_drawdown_pct(drawdown_curve)
    completed_trades = sum(1 for row in paper_orders if int(row["filled_qty"]) >= int(row["order_qty"]) and int(row["order_qty"]) > 0)
    filled_quantity = sum(int(row["filled_qty"]) for row in paper_orders)
    active_sleeves = sum(1 for row in capital_allocations if int(row["allowed_capital_at_risk_cents"]) > 0)
    as_of_date = equity_curve[-1]["date"] if equity_curve else str(report_date)

    artifacts: list[ArtifactInputV1] = []
    artifacts.extend(nav_artifact_list)
    if benchmark_artifact is not None:
        artifacts.append(benchmark_artifact)
    if paper_session_artifact is not None:
        artifacts.append(paper_session_artifact)
    if capital_allocation_artifact is not None:
        artifacts.append(capital_allocation_artifact)
    artifacts.extend(fill_ledger_list)
    artifacts.extend(order_plan_list)
    artifacts.extend(broker_submission_list)

    annotations = [
        {
            "annotation_type": "DATA_MODE",
            "date": as_of_date,
            "title": "Paper trading data mode",
            "summary": "This cockpit uses available Aegis truth artifacts and is labeled PAPER TRADING.",
        },
        {
            "annotation_type": "PERFORMANCE_CLAIMS_LIMITED",
            "date": as_of_date,
            "title": "More history required before performance claims",
            "summary": "Sharpe, win rate, annualized return, monthly returns, and sleeve return curves are withheld.",
        },
    ]
    for excluded in excluded_nav_points:
        annotations.append(
            {
                "annotation_type": "EXCLUDED_NAV_POINT",
                "date": str(excluded.get("day_utc") or ""),
                "title": "NAV point excluded",
                "summary": str(excluded.get("reason") or ""),
            }
        )

    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID_V1,
        "schema_version": "v1",
        "report_id": "",
        "report_date": str(report_date),
        "data_mode": mode,
        "data_label": _data_label(mode),
        "as_of_date": as_of_date,
        "advisory_only": True,
        "controls_runtime_behavior": False,
        "controls_broker_execution": False,
        "controls_phasec_materialization": False,
        "source_artifacts": _source_artifacts(artifacts),
        "metrics": {
            "total_return_pct": total_return,
            "annualized_return_pct": None,
            "max_drawdown_pct": max_drawdown,
            "sharpe_ratio": None,
            "win_rate_pct": None,
            "total_orders": len(paper_orders),
            "completed_trades": completed_trades,
            "filled_quantity": str(filled_quantity),
            "active_sleeves": active_sleeves,
            "valid_nav_points": len(equity_curve),
            "excluded_nav_points": len(excluded_nav_points),
        },
        "equity_curve": equity_curve,
        "benchmark_curve": benchmark_curve,
        "sleeve_curves": [],
        "drawdown_curve": drawdown_curve,
        "monthly_returns": [],
        "capital_risk_allocation": capital_allocations,
        "paper_orders": paper_orders,
        "annotations": annotations,
        "excluded_nav_points": excluded_nav_points,
        "data_gaps": data_gaps,
        "disclaimer": (
            "Uses available Aegis truth artifacts. Not investment advice. "
            "Backtest/paper/live status shown explicitly. More history required before performance claims."
        ),
        "generated_at": generated_at_utc or _now_utc(),
    }
    payload["report_id"] = "aegis_performance_showcase:" + canonical_sha256_hex_v1(
        {
            "report_date": payload["report_date"],
            "data_mode": payload["data_mode"],
            "source_artifacts": payload["source_artifacts"],
            "metrics": payload["metrics"],
            "equity_curve": payload["equity_curve"],
            "benchmark_curve": payload["benchmark_curve"],
            "data_gaps": payload["data_gaps"],
        }
    )[:16]
    validate_against_repo_schema_v1(payload, REPO_ROOT, SCHEMA_RELPATH_V1)
    return payload


def build_aegis_performance_showcase_from_repo_v1(
    *,
    repo_root: str | Path = REPO_ROOT,
    report_date: str = "2026-04-08",
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root)
    nav_root = root / "constellation_2/runtime/truth/accounting_v2/nav"
    nav_artifacts = [
        load_artifact_file_v1(path / "nav.v2.json", logical_name="accounting_nav_v2", repo_root=root)
        for path in sorted(nav_root.iterdir())
        if (path / "nav.v2.json").is_file()
    ] if nav_root.is_dir() else []
    benchmark_path = root / "constellation_2/runtime/truth/market_data_snapshot_v1/SPY/2026.jsonl"
    benchmark_artifact = (
        load_artifact_file_v1(benchmark_path, logical_name="spy_market_data_snapshot_v1", repo_root=root, jsonl=True)
        if benchmark_path.is_file()
        else None
    )
    paper_session_path = (
        root
        / "constellation_2/runtime/truth/reports/paper_session_envelope_v1"
        / report_date
        / "paper_session_envelope.v1.json"
    )
    paper_session_artifact = (
        load_artifact_file_v1(paper_session_path, logical_name="paper_session_envelope_v1", repo_root=root)
        if paper_session_path.is_file()
        else None
    )
    capital_path = (
        root
        / "constellation_2/runtime/truth_sleeves/PRIMARY/PAPER/allocation_v1/capital_authority_allocation_v1"
        / report_date
        / "capital_authority_allocation.v1.json"
    )
    capital_artifact = (
        load_artifact_file_v1(capital_path, logical_name="capital_authority_allocation_v1", repo_root=root)
        if capital_path.is_file()
        else None
    )
    fill_root = root / "constellation_2/runtime/truth_sleeves/PRIMARY/PAPER/fill_ledger_v1" / report_date
    fill_artifacts = [
        load_artifact_file_v1(path, logical_name="fill_ledger_v1", repo_root=root)
        for path in sorted(fill_root.glob("*.json"))
    ] if fill_root.is_dir() else []
    submission_root = (
        root / "constellation_2/runtime/truth_sleeves/PRIMARY/PAPER/execution_evidence_v1/submissions" / report_date
    )
    order_artifacts = [
        load_artifact_file_v1(path, logical_name="equity_order_plan_v1", repo_root=root)
        for path in sorted(submission_root.glob("*/equity_order_plan.v1.json"))
    ] if submission_root.is_dir() else []
    broker_artifacts = [
        load_artifact_file_v1(path, logical_name="broker_submission_record_v2", repo_root=root)
        for path in sorted(submission_root.glob("*/broker_submission_record.v2.json"))
    ] if submission_root.is_dir() else []
    return build_aegis_performance_showcase_v1(
        report_date=report_date,
        data_mode="PAPER_TRADING",
        nav_artifacts=nav_artifacts,
        benchmark_artifact=benchmark_artifact,
        paper_session_artifact=paper_session_artifact,
        capital_allocation_artifact=capital_artifact,
        fill_ledger_artifacts=fill_artifacts,
        order_plan_artifacts=order_artifacts,
        broker_submission_artifacts=broker_artifacts,
        generated_at_utc=generated_at_utc,
    )


def _metric_value(value: Any, suffix: str = "") -> str:
    return "DATA GAP" if value is None else f"{html.escape(str(value))}{suffix}"


def _points_for_series(rows: list[Mapping[str, Any]], value_key: str, *, width: int, height: int) -> str:
    if not rows:
        return ""
    values = [_decimal(row.get(value_key)) or Decimal("0") for row in rows]
    min_value = min(values)
    max_value = max(values)
    span = max_value - min_value
    if span == 0:
        span = Decimal("1")
    count = max(len(values) - 1, 1)
    points: list[str] = []
    for index, value in enumerate(values):
        x = Decimal(index) * Decimal(width) / Decimal(count)
        y = Decimal(height) - ((value - min_value) * Decimal(height) / span)
        points.append(f"{_decimal_text(x, Decimal('0.01'))},{_decimal_text(y, Decimal('0.01'))}")
    return " ".join(points)


def _bar_width_pct(value: Any) -> str:
    decimal = _decimal(value) or Decimal("0")
    if decimal < 0:
        decimal = Decimal("0")
    if decimal > 100:
        decimal = Decimal("100")
    return _decimal_text(decimal, Decimal("0.01"))


def render_aegis_performance_showcase_html_v1(report: Mapping[str, Any]) -> str:
    metrics = report.get("metrics") if isinstance(report.get("metrics"), Mapping) else {}
    equity_rows = list(report.get("equity_curve") or [])
    benchmark_rows = list(report.get("benchmark_curve") or [])
    drawdown_rows = list(report.get("drawdown_curve") or [])
    allocation_rows = list(report.get("capital_risk_allocation") or [])
    order_rows = list(report.get("paper_orders") or [])
    gap_rows = list(report.get("data_gaps") or [])
    source_rows = list(report.get("source_artifacts") or [])
    width = 900
    height = 260
    equity_points = _points_for_series(equity_rows, "cumulative_return_pct", width=width, height=height)
    benchmark_points = _points_for_series(benchmark_rows, "cumulative_return_pct", width=width, height=height)
    drawdown_points = _points_for_series(drawdown_rows, "drawdown_pct", width=width, height=height)
    kpis = [
        ("Total Return", _metric_value(metrics.get("total_return_pct"), "%")),
        ("Annualized Return", _metric_value(metrics.get("annualized_return_pct"), "%")),
        ("Max Drawdown", _metric_value(metrics.get("max_drawdown_pct"), "%")),
        ("Sharpe Ratio", _metric_value(metrics.get("sharpe_ratio"))),
        ("Win Rate", _metric_value(metrics.get("win_rate_pct"), "%")),
        ("Paper Orders", html.escape(str(metrics.get("total_orders", 0)))),
        ("Completed Trades", html.escape(str(metrics.get("completed_trades", 0)))),
        ("Active Sleeves", html.escape(str(metrics.get("active_sleeves", 0)))),
    ]
    kpi_html = "\n".join(
        f"<section class=\"kpi\"><span>{label}</span><strong>{value}</strong></section>" for label, value in kpis
    )
    allocation_html = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(row.get('sleeve_id') or ''))}</td>"
        f"<td>{html.escape(', '.join(row.get('engine_ids') or []))}</td>"
        f"<td>{html.escape(str(row.get('used_capital_at_risk_cents')))}</td>"
        f"<td>{html.escape(str(row.get('allowed_capital_at_risk_cents')))}</td>"
        f"<td><div class=\"bar\"><i style=\"width:{_bar_width_pct(row.get('utilization_pct'))}%\"></i></div>"
        f"{html.escape(str(row.get('utilization_pct')))}%</td>"
        "</tr>"
        for row in allocation_rows
    )
    order_html = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(row.get('day_utc') or ''))}</td>"
        f"<td>{html.escape(str(row.get('engine_id') or ''))}</td>"
        f"<td>{html.escape(str(row.get('symbol') or ''))}</td>"
        f"<td>{html.escape(str(row.get('action') or ''))}</td>"
        f"<td>{html.escape(str(row.get('order_qty') or 0))}</td>"
        f"<td>{html.escape(str(row.get('filled_qty') or 0))}</td>"
        f"<td>{html.escape(str(row.get('lifecycle_status') or ''))}</td>"
        f"<td>{html.escape(str(row.get('submission_status') or ''))}</td>"
        "</tr>"
        for row in order_rows
    )
    gaps_html = "\n".join(
        "<li>"
        f"<strong>{html.escape(str(row.get('field') or ''))}</strong>"
        f"<span>{html.escape(str(row.get('severity') or ''))}</span>"
        f"<p>{html.escape(str(row.get('reason') or ''))}</p>"
        "</li>"
        for row in gap_rows
    )
    sources_html = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(row.get('logical_name') or ''))}</td>"
        f"<td>{html.escape(str(row.get('path') or ''))}</td>"
        f"<td><code>{html.escape(str(row.get('sha256') or '')[:16])}</code></td>"
        "</tr>"
        for row in source_rows
    )
    benchmark_svg = f"<polyline class=\"benchmark\" points=\"{benchmark_points}\" />" if benchmark_points else ""
    equity_svg = f"<polyline class=\"equity\" points=\"{equity_points}\" />" if equity_points else ""
    drawdown_svg = f"<polyline class=\"drawdown\" points=\"{drawdown_points}\" />" if drawdown_points else ""
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AEGIS Performance Cockpit</title>
<style>
:root {{ color-scheme: dark; --bg:#090b10; --panel:#111824; --panel2:#151d2a; --text:#edf3fb; --muted:#93a3b8; --line:#2b384a; --cyan:#4fd1c5; --gold:#f5c542; --red:#fb7185; --green:#8bd17c; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--text); font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }}
main {{ max-width:1240px; margin:0 auto; padding:34px 24px 48px; }}
header {{ display:flex; justify-content:space-between; gap:24px; align-items:flex-start; margin-bottom:28px; }}
h1 {{ margin:0; font-size:42px; line-height:1.05; letter-spacing:0; }}
h2 {{ margin:0 0 14px; font-size:18px; letter-spacing:0; }}
p {{ color:var(--muted); line-height:1.5; }}
.badges {{ display:flex; flex-wrap:wrap; gap:8px; justify-content:flex-end; }}
.badge {{ border:1px solid var(--line); background:var(--panel); padding:8px 10px; font-size:12px; color:var(--muted); }}
.badge strong {{ color:var(--text); }}
.notice {{ border-left:4px solid var(--gold); background:#1b1820; padding:12px 14px; margin:18px 0 26px; color:#f7e7ad; }}
.kpis {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; margin-bottom:18px; }}
.kpi {{ background:var(--panel); border:1px solid var(--line); padding:16px; min-height:94px; }}
.kpi span {{ display:block; color:var(--muted); font-size:12px; text-transform:uppercase; }}
.kpi strong {{ display:block; margin-top:14px; font-size:28px; line-height:1; }}
.grid {{ display:grid; grid-template-columns:minmax(0,1.35fr) minmax(320px,.65fr); gap:16px; }}
.panel {{ background:var(--panel); border:1px solid var(--line); padding:18px; margin-bottom:16px; }}
svg {{ width:100%; height:auto; display:block; background:#0c111a; border:1px solid #1f2937; }}
polyline {{ fill:none; stroke-width:4; stroke-linecap:round; stroke-linejoin:round; }}
.equity {{ stroke:var(--cyan); }} .benchmark {{ stroke:var(--gold); }} .drawdown {{ stroke:var(--red); }}
.legend {{ display:flex; gap:16px; color:var(--muted); font-size:13px; margin-top:10px; }}
.legend i {{ display:inline-block; width:24px; height:3px; vertical-align:middle; margin-right:6px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ padding:9px 8px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }}
th {{ color:var(--muted); font-weight:600; }}
.bar {{ display:inline-block; width:90px; height:8px; background:#263244; margin-right:8px; vertical-align:middle; }}
.bar i {{ display:block; height:8px; background:var(--green); }}
.gaps {{ list-style:none; padding:0; margin:0; display:grid; gap:10px; }}
.gaps li {{ border:1px solid var(--line); background:var(--panel2); padding:12px; }}
.gaps strong {{ display:block; }} .gaps span {{ color:var(--gold); font-size:12px; text-transform:uppercase; }}
.gaps p {{ margin:7px 0 0; }} code {{ color:var(--cyan); }}
@media (max-width:900px) {{ header,.grid {{ display:block; }} .badges {{ justify-content:flex-start; margin-top:16px; }} .kpis {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} }}
</style>
</head>
<body>
<main>
  <header>
    <div><h1>AEGIS Performance Cockpit</h1><p>Truthful performance view from available Aegis artifacts. Missing performance history is shown as DATA GAP.</p></div>
    <div class="badges">
      <span class="badge">Data mode <strong>{html.escape(str(report.get('data_label') or ''))}</strong></span>
      <span class="badge">As of <strong>{html.escape(str(report.get('as_of_date') or ''))}</strong></span>
      <span class="badge">Advisory only <strong>{html.escape(str(report.get('advisory_only')))}</strong></span>
    </div>
  </header>
  <div class="notice">{html.escape(str(report.get('disclaimer') or ''))}</div>
  <section class="kpis">{kpi_html}</section>
  <section class="grid">
    <div>
      <section class="panel"><h2>Cumulative Return</h2><svg viewBox="0 0 {width} {height}" role="img" aria-label="Cumulative return chart"><line x1="0" y1="{height / 2}" x2="{width}" y2="{height / 2}" stroke="#263244" stroke-width="1" />{equity_svg}{benchmark_svg}</svg><div class="legend"><span><i style="background:var(--cyan)"></i>Aegis NAV</span><span><i style="background:var(--gold)"></i>SPY benchmark</span></div></section>
      <section class="panel"><h2>Drawdown From Peak</h2><svg viewBox="0 0 {width} {height}" role="img" aria-label="Drawdown chart"><line x1="0" y1="0" x2="{width}" y2="0" stroke="#263244" stroke-width="1" />{drawdown_svg}</svg></section>
      <section class="panel"><h2>Capital/Risk Allocation By Sleeve</h2><table><thead><tr><th>Sleeve</th><th>Engine</th><th>Used cents</th><th>Allowed cents</th><th>Utilization</th></tr></thead><tbody>{allocation_html}</tbody></table></section>
    </div>
    <aside>
      <section class="panel"><h2>Paper Orders</h2><table><thead><tr><th>Day</th><th>Engine</th><th>Symbol</th><th>Side</th><th>Order</th><th>Filled</th><th>Lifecycle</th><th>Submission</th></tr></thead><tbody>{order_html}</tbody></table></section>
      <section class="panel"><h2>Data Gaps</h2><ul class="gaps">{gaps_html}</ul></section>
    </aside>
  </section>
  <section class="panel"><h2>Source Artifacts</h2><table><thead><tr><th>Artifact</th><th>Path</th><th>SHA-256</th></tr></thead><tbody>{sources_html}</tbody></table></section>
</main>
</body>
</html>
"""


def write_aegis_performance_showcase_artifacts_v1(
    *,
    report: Mapping[str, Any],
    output_dir: str | Path,
) -> dict[str, str]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "aegis_performance_showcase.v1.json"
    html_path = out / "aegis_performance_showcase.v1.html"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    html_path.write_text(render_aegis_performance_showcase_html_v1(report), encoding="utf-8")
    return {"json": str(json_path), "html": str(html_path)}
