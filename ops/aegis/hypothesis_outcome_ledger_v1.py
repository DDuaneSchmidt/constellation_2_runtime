from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import write_json_v1
from ops.aegis.outcome_registry_v1 import build_outcome_registry_v1
from ops.aegis.research_mapping_rules_v1 import report_path_v1, stable_hash_v1, text_v1
from ops.aegis.research_portfolio_manager_v1 import build_research_portfolio_v1
from ops.aegis.validation_sample_generator_v1 import build_validation_samples_v1

REPORT_FAMILY = "aegis_hypothesis_outcome_ledger_v1"
REPORT_FILENAME = "hypothesis_outcome_ledger.v1.json"
SAFETY = {"read_only": True, "trade_advice_allowed": False, "broker_execution_allowed": False, "autonomous_execution_allowed": False, "live_trading_allowed": False}


def hypothesis_outcome_ledger_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return report_path_v1(truth_root, REPORT_FAMILY, day_utc, REPORT_FILENAME)


def build_hypothesis_outcome_ledger_v1(*, truth_root: Path | str, day_utc: str, outcome_registry: Mapping[str, Any] | None = None, validation_samples: Mapping[str, Any] | None = None, research_portfolio: Mapping[str, Any] | None = None) -> dict[str, Any]:
    portfolio = dict(research_portfolio or build_research_portfolio_v1(truth_root=truth_root, day_utc=day_utc))
    outcomes = [dict(row) for row in (outcome_registry or build_outcome_registry_v1(truth_root=truth_root, day_utc=day_utc)).get("outcomes", []) if isinstance(row, Mapping)]
    samples = [dict(row) for row in (validation_samples or build_validation_samples_v1(truth_root=truth_root, day_utc=day_utc, outcome_registry={"outcomes": outcomes})).get("samples", []) if isinstance(row, Mapping)]
    outcomes_by_h: dict[str, list[dict[str, Any]]] = defaultdict(list)
    samples_by_h: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in outcomes:
        outcomes_by_h[text_v1(row.get("hypothesis_id"))].append(row)
    for row in samples:
        samples_by_h[text_v1(row.get("hypothesis_id"))].append(row)
    rows=[]
    for hyp in portfolio.get("hypotheses") or []:
        if not isinstance(hyp, Mapping):
            continue
        hid = text_v1(hyp.get("hypothesis_id")); hs = outcomes_by_h.get(hid, []); ss = samples_by_h.get(hid, [])
        closed = [r for r in hs if text_v1(r.get("outcome_state")).startswith("CLOSED_")]
        included = [r for r in ss if r.get("sample_state") == "INCLUDED"]
        returns = [_num(r.get("return_value")) for r in included if _num(r.get("return_value")) is not None]
        wins = [r for r in closed if r.get("outcome_state") == "CLOSED_WIN"]
        losses = [r for r in closed if r.get("outcome_state") == "CLOSED_LOSS"]
        row = {"hypothesis_id": hid, "thesis_id": hyp.get("thesis_id"), "candidates_count": len(hyp.get("linked_candidates") or []), "paper_positions_count": len(hs), "open_positions_count": sum(1 for r in hs if r.get("outcome_state") == "OPEN"), "closed_positions_count": len(closed), "wins": len(wins), "losses": len(losses), "flats": sum(1 for r in closed if r.get("outcome_state") == "CLOSED_FLAT"), "expired": sum(1 for r in hs if r.get("outcome_state") == "EXPIRED"), "invalidated": sum(1 for r in hs if r.get("outcome_state") == "INVALIDATED"), "win_rate": _round(len(wins) / len(closed)) if closed else None, "average_win": _avg([_num(r.get("realized_return")) for r in wins]), "average_loss": _avg([_num(r.get("realized_return")) for r in losses]), "expectancy": _avg(returns), "max_drawdown": _max_drawdown(returns), "average_holding_period": _avg([_num(r.get("holding_period_days")) for r in hs]), "sample_count": len(ss), "usable_validation_sample_count": len(included), "validation_readiness_state": _readiness(len(included), returns), "source_outcome_ids": [r.get("outcome_id") for r in hs]}
        rows.append(row)
    payload = {"schema_id": "aegis_hypothesis_outcome_ledger", "schema_version": "v1", "artifact_id": REPORT_FAMILY, "day_utc": str(day_utc), "generated_at": _now(), "hypotheses": rows, "summary": {"hypothesis_count": len(rows), "usable_validation_sample_count": sum(r["usable_validation_sample_count"] for r in rows), "closed_positions_count": sum(r["closed_positions_count"] for r in rows), "open_positions_count": sum(r["open_positions_count"] for r in rows)}, "safety": dict(SAFETY), **SAFETY}
    payload["content_hash"] = stable_hash_v1({**payload, "generated_at": "", "content_hash": ""})
    return payload


def write_hypothesis_outcome_ledger_v1(*, truth_root: Path | str, day_utc: str, payload: dict[str, Any] | None = None) -> Path:
    return write_json_v1(hypothesis_outcome_ledger_path_v1(truth_root=truth_root, day_utc=day_utc), payload or build_hypothesis_outcome_ledger_v1(truth_root=truth_root, day_utc=day_utc))


def _readiness(n: int, returns: list[float]) -> str:
    if n == 0:
        return "UNDERPOWERED"
    if n < 10:
        return "ACCUMULATING"
    return "VALIDATION_READY"


def _num(value: Any) -> float | None:
    try:
        if value in (None, "") or isinstance(value, bool): return None
        return float(value)
    except Exception:
        return None


def _avg(values: list[float | None]) -> float | None:
    vals = [v for v in values if v is not None]
    return _round(mean(vals)) if vals else None


def _max_drawdown(returns: list[float]) -> float | None:
    if not returns: return None
    equity = 1.0; peak = 1.0; worst = 0.0
    for r in returns:
        equity *= (1.0 + r); peak = max(peak, equity); worst = min(worst, (equity - peak) / peak)
    return _round(worst)


def _round(value: float | None) -> float | None:
    return round(value, 8) if value is not None else None


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
