from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json
import os
from typing import Any


@dataclass(frozen=True)
class SleeveRiskRealizationResultV1:
    report_path: Path
    report: dict[str, Any]


def materialize_sleeve_risk_realization_v1(*, day_utc: str, truth_root: Path, evaluation_utc: str | None = None, scheduled_run: bool = False) -> SleeveRiskRealizationResultV1:
    day = _require_day(day_utc)
    truth_root = Path(truth_root).resolve()
    produced_at = _now(evaluation_utc)
    trade_path = truth_root / "reports" / "sleeve_trade_fact_v1" / day / "sleeve_trade_fact.v1.json"
    pnl_path = truth_root / "reports" / "sleeve_realized_pnl_v1" / day / "sleeve_realized_pnl.v1.json"
    risk_budget_path = truth_root / "reports" / "risk_budget_supply_v1" / day / "risk_budget_supply.v1.json"
    authorization_path = truth_root / "reports" / "authorization_supply_v1" / day / "authorization_supply.v1.json"
    structure_path = truth_root / "reports" / "structure_decision_supply_v1" / day / "structure_decision_supply.v1.json"
    allocation_path = truth_root / "allocation_v1" / "capital_authority_allocation_v1" / day / "capital_authority_allocation.v1.json"
    trade_report = _read_json_optional(trade_path) or {}
    pnl_report = _read_json_optional(pnl_path) or {}
    risk_budget = _read_json_optional(risk_budget_path) or {}
    authorization = _read_json_optional(authorization_path) or {}
    structure = _read_json_optional(structure_path) or {}
    allocation = _read_json_optional(allocation_path) or {}
    blockers: list[dict[str, Any]] = []

    if str(trade_report.get("status") or "").upper() == "NO_COMPLETED_TRADES":
        status = "NO_COMPLETED_TRADES"
        sleeves: list[dict[str, Any]] = []
    elif str(trade_report.get("status") or "").upper() != "PASS":
        blockers.append(_blocker("SLEEVE_TRADE_FACT_NOT_PASS", "sleeve_trade_fact_v1.status", "PASS", trade_report.get("status") or "MISSING", str(trade_path), "Run and clear sleeve trade facts before risk realization."))
        sleeves = []
    elif str(pnl_report.get("status") or "").upper() != "PASS":
        blockers.append(_blocker("SLEEVE_PNL_NOT_PASS", "sleeve_realized_pnl_v1.status", "PASS", pnl_report.get("status") or "MISSING", str(pnl_path), "Run and clear sleeve realized P&L before risk realization."))
        sleeves = []
    else:
        expected = _expected_risk(authorization, structure, risk_budget)
        if expected <= 0:
            blockers.append(_blocker("EXPECTED_RISK_MISSING", "authorization/structure/risk.expected_max_loss", "> 0", expected, str(authorization_path), "Materialize expected risk from authorization and structure decision before realized-vs-expected risk."))
        allocated = _allocated_risk(allocation, risk_budget, expected)
        sleeves = []
        for row in pnl_report.get("sleeves", []):
            if not isinstance(row, dict):
                continue
            realized_loss = max(0.0, -_number(row.get("net_pnl_after_fees")))
            capital_used = _number(row.get("capital_used"))
            ratio = round(realized_loss / expected, 8) if expected > 0 else None
            used = round(realized_loss / allocated, 8) if allocated > 0 else None
            sleeves.append(
                {
                    "sleeve_id": _text(row.get("sleeve_id")),
                    "expected_max_loss": expected if expected > 0 else None,
                    "allocated_risk": allocated if allocated > 0 else None,
                    "realized_drawdown": realized_loss,
                    "realized_loss": realized_loss,
                    "capital_used": capital_used,
                    "risk_budget_used": used,
                    "risk_budget_remaining": round(max(0.0, allocated - realized_loss), 6) if allocated > 0 else None,
                    "realized_vs_expected_risk_ratio": ratio,
                    "risk_verdict": "UNKNOWN_EXPECTED_RISK_MISSING" if expected <= 0 else ("BREACHED" if ratio and ratio > 1.0 else "WITHIN_EXPECTED_RISK"),
                }
            )
        status = "BLOCKED" if blockers else "PASS"

    if blockers:
        status = "BLOCKED"
    report = {
        "schema_version": "sleeve_risk_realization.v1",
        "day_utc": day,
        "status": status,
        "first_blocker": blockers[0]["blocker_code"] if blockers else "",
        "produced_at_utc": produced_at,
        "producer": "sleeve_risk_realization_v1",
        "scheduled_run": bool(scheduled_run),
        "sleeves": sleeves,
        "blockers": blockers,
        "evidence_paths": [str(path) for path in [trade_path, pnl_path, risk_budget_path, authorization_path, structure_path, allocation_path]],
    }
    out = truth_root / "reports" / "sleeve_risk_realization_v1" / day / "sleeve_risk_realization.v1.json"
    _write_json(out, report)
    return SleeveRiskRealizationResultV1(report_path=out, report=report)


def _expected_risk(*payloads: dict[str, Any]) -> float:
    keys = {"expected_max_loss", "expected_max_loss_cents", "max_loss", "max_loss_cents", "defined_risk_max_loss", "risk_limit_cents"}
    for payload in payloads:
        for row in _walk(payload):
            for key, value in row.items():
                if str(key) in keys:
                    number = _number(value)
                    if number > 0:
                        return number
    return 0.0


def _allocated_risk(allocation: dict[str, Any], risk_budget: dict[str, Any], default: float) -> float:
    for payload in (allocation, risk_budget):
        for row in _walk(payload):
            for key in ("allocated_risk", "allocated_risk_cents", "risk_budget_allocated", "risk_budget_cents"):
                number = _number(row.get(key))
                if number > 0:
                    return number
    return default


def _walk(obj: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        rows.append(obj)
        for value in obj.values():
            rows.extend(_walk(value))
    elif isinstance(obj, list):
        for value in obj:
            rows.extend(_walk(value))
    return rows


def _blocker(code: str, field: str, expected: Any, actual: Any, path: str, action: str) -> dict[str, Any]:
    return {"blocker_code": code, "failed_field": field, "expected_value": expected, "actual_value": actual, "artifact_path": path, "operator_next_action": action}


def _read_json_optional(path: Path) -> dict[str, Any] | None:
    try:
        if path.exists() and path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else None
    except Exception:
        return None
    return None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _number(value: Any) -> float:
    try:
        if value in (None, "") or isinstance(value, bool):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _text(value: Any) -> str:
    return str(value or "").strip()


def _require_day(value: str) -> str:
    text = _text(value)
    datetime.fromisoformat(text)
    if len(text) != 10:
        raise ValueError("day_utc must be YYYY-MM-DD")
    return text


def _now(value: str | None) -> str:
    return _text(value) if value else datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
