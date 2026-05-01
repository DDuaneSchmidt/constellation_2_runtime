#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
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
from ops.tools.run_position_lifecycle_state_v1 import position_lifecycle_state_path

PAPER_MODE = "PAPER"
MIN_TRADES_FOR_EDGE_HEALTH = 20


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(payload) + b"\n")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return read_json_object_v1(path)
    except Exception:
        return {}


def _float(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(str(value).strip())
    except Exception:
        return None


def edge_attribution_path(*, truth_root: Path, day_utc: str) -> Path:
    return Path(truth_root).resolve() / "reports" / "edge_attribution_v1" / day_utc / "edge_attribution.v1.json"


def _walk_dicts(obj: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        rows.append(obj)
        for value in obj.values():
            rows.extend(_walk_dicts(value))
    elif isinstance(obj, list):
        for value in obj:
            rows.extend(_walk_dicts(value))
    return rows


def _candidate_files(root: Path, day_utc: str) -> list[Path]:
    candidates: list[Path] = []
    for base in (
        root / "fill_ledger_v1",
        root / "accounting_v2" / "attribution",
        root / "reports" / "sleeve_intent_trade_attribution_v1",
        root / "reports" / "execution_reconciliation_v1",
    ):
        if base.exists():
            candidates.extend(path for path in base.rglob("*.json") if path.is_file() and str(day_utc) >= path.as_posix())
    return sorted(set(candidates))


def _trade_return(row: dict[str, Any]) -> float | None:
    for key in ("return_pct", "trade_return_pct", "realized_return_pct", "pnl_pct"):
        value = _float(row.get(key))
        if value is not None:
            return value
    pnl = _float(row.get("realized_pnl") or row.get("realized_pnl_usd") or row.get("pnl") or row.get("net_pnl"))
    notional = _float(row.get("notional") or row.get("entry_notional") or row.get("capital_at_risk") or row.get("risk_cents"))
    if pnl is not None and notional not in (None, 0.0):
        return pnl / abs(float(notional))
    return pnl


def _is_real_trade(row: dict[str, Any]) -> bool:
    if bool(row.get("proxy") is True) or str(row.get("pnl_source") or "").strip().upper() == "SIGNAL_PROXY":
        return False
    if any(str(row.get(key) or "").strip() for key in ("fill_id", "execution_id", "submission_id", "broker_order_id", "position_id", "trade_id")):
        return True
    return _trade_return(row) is not None and str(row.get("sleeve_id") or row.get("engine_id") or "").strip()


def _collect_trades(*, truth_root: Path, day_utc: str, environment: str) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    roots = [Path(truth_root).resolve()]
    if environment == PAPER_MODE:
        try:
            sleeve_root = resolve_paper_intent_truth_root_v1(truth_root=truth_root, repo_root=REPO_ROOT).resolve()
            if sleeve_root not in roots:
                roots.append(sleeve_root)
        except Exception:
            pass
    evidence_paths: list[str] = []
    by_sleeve: dict[str, list[dict[str, Any]]] = {}
    for root in roots:
        for path in _candidate_files(root, day_utc):
            payload = _read_json(path)
            rows = _walk_dicts(payload)
            used = False
            for row in rows:
                if not _is_real_trade(row):
                    continue
                sleeve_id = str(row.get("sleeve_id") or row.get("engine_id") or row.get("strategy_id") or "UNKNOWN").strip().upper()
                trade_return = _trade_return(row)
                if trade_return is None:
                    continue
                trade = dict(row)
                trade["_return"] = float(trade_return)
                by_sleeve.setdefault(sleeve_id, []).append(trade)
                used = True
            if used:
                evidence_paths.append(str(path))
    return by_sleeve, sorted(set(evidence_paths))


def _max_drawdown(returns: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for ret in returns:
        equity += ret
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return round(max_dd, 6)


def _rolling(values: list[float], window: int) -> dict[str, Any]:
    subset = values[-window:]
    return {"trades": len(subset), "expectancy": round(mean(subset), 6) if subset else None}


def _edge_health(trades: int, expectancy: float | None, rolling_20: dict[str, Any]) -> str:
    if trades < MIN_TRADES_FOR_EDGE_HEALTH or expectancy is None:
        return "UNPROVEN"
    if expectancy < 0:
        return "NEGATIVE"
    roll = rolling_20.get("expectancy")
    if isinstance(roll, (int, float)) and roll < 0:
        return "WEAKENING"
    if expectancy >= 0.02:
        return "STRONG"
    return "STABLE"


def _summary_for_sleeve(sleeve_id: str, rows: list[dict[str, Any]], evidence_paths: list[str]) -> dict[str, Any]:
    returns = [float(row["_return"]) for row in rows]
    wins = [ret for ret in returns if ret > 0]
    losses = [ret for ret in returns if ret < 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    expectancy = mean(returns) if returns else None
    rolling_20 = _rolling(returns, 20)
    return {
        "sleeve_id": sleeve_id,
        "trades": len(returns),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(returns), 6) if returns else None,
        "avg_return": round(mean(returns), 6) if returns else None,
        "avg_win": round(mean(wins), 6) if wins else None,
        "avg_loss": round(mean(losses), 6) if losses else None,
        "expectancy": round(expectancy, 6) if expectancy is not None else None,
        "max_drawdown": _max_drawdown(returns),
        "profit_factor": round(gross_win / gross_loss, 6) if gross_loss else (None if not gross_win else 999999.0),
        "avg_holding_period": None,
        "slippage_estimate": None,
        "rolling_20": rolling_20,
        "rolling_60": _rolling(returns, 60),
        "rolling_120": _rolling(returns, 120),
        "edge_health": _edge_health(len(returns), expectancy, rolling_20),
        "evidence_paths": evidence_paths,
    }


def build_edge_attribution_v1(*, day_utc: str, truth_root: Path, environment: str = PAPER_MODE) -> dict[str, Any]:
    truth_root = Path(truth_root).resolve()
    by_sleeve, evidence_paths = _collect_trades(truth_root=truth_root, day_utc=day_utc, environment=environment)
    position_path = position_lifecycle_state_path(truth_root=truth_root, day_utc=day_utc)
    if position_path.exists():
        evidence_paths.append(str(position_path))
    sleeves = sorted(by_sleeve) or ["ALL"]
    sleeve_rows = [_summary_for_sleeve(sleeve, by_sleeve.get(sleeve, []), sorted(set(evidence_paths))) for sleeve in sleeves]
    out_path = edge_attribution_path(truth_root=truth_root, day_utc=day_utc)
    payload = {
        "schema_id": "edge_attribution",
        "schema_version": "v1",
        "day_utc": day_utc,
        "environment": environment,
        "status": "PASS" if by_sleeve else "UNPROVEN",
        "history_source": "REAL_FILLS_AND_ATTRIBUTION_ONLY",
        "proxy_pnl_included": False,
        "minimum_trades_for_edge_health": MIN_TRADES_FOR_EDGE_HEALTH,
        "sleeves": sleeve_rows,
        "evidence_paths": sorted(set(evidence_paths)),
        "produced_at_utc": _now_iso(),
        "producer": "ops/tools/run_edge_attribution_v1.py",
        "artifact_path": str(out_path),
    }
    _write_json(out_path, payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="run_edge_attribution_v1")
    parser.add_argument("--day_utc", required=True)
    parser.add_argument("--environment", default=PAPER_MODE, choices=["PAPER"])
    parser.add_argument("--truth_root", default="")
    args = parser.parse_args(argv)
    day_utc = parse_day_utc_v1(args.day_utc)
    truth_root = resolve_fact_plane_truth_root_v1(args.truth_root)
    payload = build_edge_attribution_v1(day_utc=day_utc, truth_root=truth_root, environment=str(args.environment).strip().upper())
    print(json.dumps({"status": payload["status"], "path": payload["artifact_path"], "sleeve_count": len(payload["sleeves"])}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
