#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from constellation_2.common.paper_session_fact_plane_v1 import (
    parse_day_utc_v1,
    read_json_object_v1,
    resolve_fact_plane_truth_root_v1,
    resolve_paper_intent_truth_root_v1,
)

PAPER_MODE = "PAPER"
REGIMES = {"TREND", "CHOPPY", "CRISIS", "DISPERSION", "VOL_SHOCK", "UNKNOWN"}
BOOTSTRAP_ACCEPTED_FOR_PAPER = "BOOTSTRAP_ACCEPTED_FOR_PAPER"
DEGRADED_INSUFFICIENT_HISTORY = "DEGRADED_INSUFFICIENT_HISTORY"
BLOCKED_FOR_LIVE = "BLOCKED_FOR_LIVE"


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")


def portfolio_state_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "portfolio_state_v1" / day_utc / "portfolio_state.v1.json"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return read_json_object_v1(path)
    except Exception:
        return {}


def _dec(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return Decimal(default)


def _load_market_rows(market_root: Path, symbol: str, day_utc: str) -> tuple[Path, list[dict[str, Any]]]:
    year_path = market_root / "market_data_snapshot_v1" / symbol / f"{day_utc[:4]}.jsonl"
    rows: list[dict[str, Any]] = []
    if not year_path.is_file():
        return year_path, rows
    for line in year_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if isinstance(row, dict) and str(row.get("timestamp_utc") or "")[:10] <= day_utc:
            rows.append(row)
    rows.sort(key=lambda item: str(item.get("timestamp_utc") or ""))
    return year_path, rows


def _sma(values: list[Decimal], n: int) -> Decimal | None:
    if len(values) < n:
        return None
    return sum(values[-n:]) / Decimal(n)


def _market_metrics(*, market_root: Path, day_utc: str, symbols: list[str]) -> tuple[dict[str, Any], list[dict[str, str]], list[str]]:
    inputs: list[dict[str, str]] = []
    missing: list[str] = []
    symbol_rows: dict[str, list[dict[str, Any]]] = {}
    for symbol in symbols:
        path, rows = _load_market_rows(market_root, symbol, day_utc)
        inputs.append({"type": "market_data_symbol_year", "symbol": symbol, "path": str(path)})
        if len(rows) < 2:
            missing.append(symbol)
        symbol_rows[symbol] = rows

    trend_ok = 0
    trend_eval = 0
    returns: list[Decimal] = []
    for symbol, rows in symbol_rows.items():
        if len(rows) >= 2:
            prev = _dec(rows[-2].get("close"))
            close = _dec(rows[-1].get("close"))
            if prev > 0:
                returns.append((close - prev) / prev)
        closes = [_dec(row.get("close")) for row in rows]
        fast = _sma(closes, 20)
        slow = _sma(closes, 100)
        if fast is not None and slow is not None:
            trend_eval += 1
            if fast > slow and closes[-1] > fast:
                trend_ok += 1

    trend_ratio = Decimal(trend_ok) / Decimal(trend_eval) if trend_eval else Decimal("-1")
    if trend_ratio < 0:
        trend_strength = "UNKNOWN"
    elif trend_ratio >= Decimal("0.50"):
        trend_strength = "HIGH"
    elif trend_ratio >= Decimal("0.25"):
        trend_strength = "MEDIUM"
    else:
        trend_strength = "LOW"

    abs_returns = [abs(item) for item in returns]
    max_abs_return = max(abs_returns) if abs_returns else Decimal("-1")
    dispersion = (max(returns) - min(returns)) if len(returns) >= 2 else Decimal("-1")
    if max_abs_return < 0:
        volatility_regime = "UNKNOWN"
    elif max_abs_return >= Decimal("0.04"):
        volatility_regime = "SHOCK"
    elif max_abs_return >= Decimal("0.02"):
        volatility_regime = "HIGH"
    elif max_abs_return >= Decimal("0.005"):
        volatility_regime = "NORMAL"
    else:
        volatility_regime = "LOW"

    if dispersion < 0:
        dispersion_regime = "UNKNOWN"
    elif dispersion >= Decimal("0.03"):
        dispersion_regime = "HIGH"
    elif dispersion >= Decimal("0.01"):
        dispersion_regime = "NORMAL"
    else:
        dispersion_regime = "LOW"

    equity_symbols = ["SPY", "QQQ", "IWM"]
    equity_trends = 0
    equity_eval = 0
    for symbol in equity_symbols:
        rows = symbol_rows.get(symbol, [])
        closes = [_dec(row.get("close")) for row in rows]
        fast = _sma(closes, 20)
        slow = _sma(closes, 100)
        if fast is None or slow is None:
            continue
        equity_eval += 1
        if fast > slow and closes[-1] > fast:
            equity_trends += 1
    if not equity_eval:
        equity_beta_state = "UNKNOWN"
    elif equity_trends >= 2:
        equity_beta_state = "HIGH"
    elif equity_trends == 1:
        equity_beta_state = "NORMAL"
    else:
        equity_beta_state = "LOW"

    return (
        {
            "trend_strength": trend_strength,
            "volatility_regime": volatility_regime,
            "dispersion_regime": dispersion_regime,
            "equity_beta_state": equity_beta_state,
            "trend_symbol_count": trend_ok,
            "trend_symbol_evaluated_count": trend_eval,
            "max_abs_daily_return": str(max_abs_return if max_abs_return >= 0 else Decimal("0")),
            "cross_symbol_return_dispersion": str(dispersion if dispersion >= 0 else Decimal("0")),
        },
        inputs,
        [f"market_data:{symbol}" for symbol in missing],
    )


def _correlation_state(path: Path, *, environment: str) -> tuple[str, str, dict[str, Any], list[str], list[str], str]:
    payload = _read_json(path)
    if not payload:
        return "UNKNOWN", "0", {}, ["engine_correlation_matrix"], [], ""
    bootstrap = payload.get("bootstrap_policy") if isinstance(payload.get("bootstrap_policy"), dict) else {}
    bootstrap_status = str(bootstrap.get("status") or "").strip().upper()
    corr_status = str(payload.get("status") or "").strip().upper()
    matrix = payload.get("matrix") if isinstance(payload.get("matrix"), dict) else {}
    max_corr: Decimal | None = None
    if isinstance(matrix.get("corr"), list):
        rows = matrix.get("corr")
        for i, row in enumerate(rows):
            if not isinstance(row, list):
                continue
            for j, value in enumerate(row):
                if i == j:
                    continue
                corr = _dec(value)
                if max_corr is None or corr > max_corr:
                    max_corr = corr
    else:
        legacy = payload.get("correlation_matrix") if isinstance(payload.get("correlation_matrix"), dict) else {}
        for row_key, row in legacy.items():
            if not isinstance(row, dict):
                continue
            for col_key, value in row.items():
                if str(row_key) == str(col_key):
                    continue
                corr = _dec(value)
                if max_corr is None or corr > max_corr:
                    max_corr = corr
    if max_corr is None:
        if bootstrap_status == BOOTSTRAP_ACCEPTED_FOR_PAPER and environment == PAPER_MODE:
            return "UNKNOWN", "0", payload, [], ["engine_correlation_matrix:BOOTSTRAP_ACCEPTED_FOR_PAPER"], BOOTSTRAP_ACCEPTED_FOR_PAPER
        if corr_status == DEGRADED_INSUFFICIENT_HISTORY:
            return "UNKNOWN", "0", payload, ["engine_correlation_matrix:DEGRADED_INSUFFICIENT_HISTORY"], [], DEGRADED_INSUFFICIENT_HISTORY
        return "UNKNOWN", "0", payload, ["engine_correlation_matrix:NO_PAIRWISE_CORRELATIONS"], [], corr_status
    if max_corr >= Decimal("0.70"):
        state = "HIGH"
    elif max_corr >= Decimal("0.30"):
        state = "NORMAL"
    else:
        state = "LOW"
    return state, str(max_corr), payload, [], [], str(bootstrap_status or corr_status)


def _derive_regime(*, regime_payload: dict[str, Any], market: dict[str, Any], correlation_regime: str) -> str:
    label = str(regime_payload.get("regime") or regime_payload.get("regime_label") or "").strip().upper()
    drawdown = _dec((regime_payload.get("evidence") if isinstance(regime_payload.get("evidence"), dict) else {}).get("drawdown_pct"))
    if label in {"CRISIS", "HIGH_VOL_UNSTABLE"} or drawdown < Decimal("-0.05"):
        return "CRISIS"
    if market["volatility_regime"] == "SHOCK":
        return "VOL_SHOCK"
    if market["dispersion_regime"] == "HIGH":
        return "DISPERSION"
    if market["trend_strength"] in {"MEDIUM", "HIGH"}:
        return "TREND"
    if label == "NORMAL":
        return "CHOPPY" if correlation_regime != "UNKNOWN" else "UNKNOWN"
    return "UNKNOWN"


def build_portfolio_state_v1(*, day_utc: str, truth_root: Path, environment: str = PAPER_MODE) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    environment = str(environment or PAPER_MODE).strip().upper()
    sleeve_truth_root = resolve_paper_intent_truth_root_v1(truth_root=truth_root, repo_root=REPO_ROOT) if environment == PAPER_MODE else truth_root
    symbols = ["DBC", "GLD", "HYG", "IEF", "IWM", "LQD", "QQQ", "SPY", "TLT", "UUP"]
    market, market_inputs, market_missing = _market_metrics(market_root=truth_root, day_utc=day_utc, symbols=symbols)

    regime_path = sleeve_truth_root / "monitoring_v1" / "regime_snapshot_v2" / day_utc / "regime_snapshot.v2.json"
    corr_path = sleeve_truth_root / "monitoring_v1" / "engine_correlation_matrix" / day_utc / "engine_correlation_matrix.v1.json"
    regime_payload = _read_json(regime_path)
    correlation_regime, max_corr, corr_payload, corr_missing, corr_bootstrap_inputs, corr_input_status = _correlation_state(
        corr_path,
        environment=environment,
    )
    regime = _derive_regime(regime_payload=regime_payload, market=market, correlation_regime=correlation_regime)

    missing_inputs: list[str] = []
    degraded_inputs: list[str] = []
    bootstrap_inputs: list[str] = []
    if not regime_payload:
        missing_inputs.append("regime_snapshot_v2")
    if not corr_payload:
        missing_inputs.append("engine_correlation_matrix")
    degraded_inputs.extend(corr_missing)
    bootstrap_inputs.extend(corr_bootstrap_inputs)
    missing_inputs.extend(market_missing)
    if regime not in REGIMES:
        regime = "UNKNOWN"
    if not market_inputs:
        status = "BLOCKED"
    elif missing_inputs or degraded_inputs:
        status = "DEGRADED"
    elif bootstrap_inputs and environment == PAPER_MODE:
        status = BOOTSTRAP_ACCEPTED_FOR_PAPER
    elif bootstrap_inputs:
        status = BLOCKED_FOR_LIVE
    elif regime == "UNKNOWN":
        status = "DEGRADED"
    else:
        status = "PASS"
    out_path = portfolio_state_path(truth_root=truth_root, day_utc=day_utc)
    payload = {
        "schema_id": "portfolio_state",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": status,
        "regime": regime,
        "trend_strength": market["trend_strength"],
        "volatility_regime": market["volatility_regime"],
        "correlation_regime": correlation_regime,
        "correlation_input_status": corr_input_status,
        "correlation_bootstrap_policy_status": (corr_payload.get("bootstrap_policy") if isinstance(corr_payload.get("bootstrap_policy"), dict) else {}).get("status", ""),
        "dispersion_regime": market["dispersion_regime"],
        "equity_beta_state": market["equity_beta_state"],
        "max_pairwise_corr": max_corr,
        "inputs_used": [
            {"type": "regime_snapshot_v2", "path": str(regime_path)},
            {"type": "engine_correlation_matrix", "path": str(corr_path)},
            *market_inputs,
        ],
        "missing_inputs": sorted(set(missing_inputs)),
        "degraded_inputs": sorted(set(degraded_inputs)),
        "bootstrap_inputs": sorted(set(bootstrap_inputs)),
        "derived_metrics": {k: v for k, v in market.items() if k not in {"trend_strength", "volatility_regime", "dispersion_regime", "equity_beta_state"}},
        "produced_at_utc": _now_iso(),
        "producer": "ops/tools/run_portfolio_state_v1.py",
        "operator_next_action": "Accumulate paper engine attribution history; bootstrap correlation is accepted only for PAPER." if status == BOOTSTRAP_ACCEPTED_FOR_PAPER else ("Review missing/degraded portfolio-state inputs before relying on portfolio activation." if status != "PASS" else ""),
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_portfolio_state_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_portfolio_state_v1(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment).strip().upper())
    print(json.dumps({"status": payload["status"], "regime": payload["regime"], "path": payload["artifact_path"]}, sort_keys=True))
    return 0 if payload["status"] in {"PASS", "DEGRADED", BOOTSTRAP_ACCEPTED_FOR_PAPER} else 2


if __name__ == "__main__":
    raise SystemExit(main())
