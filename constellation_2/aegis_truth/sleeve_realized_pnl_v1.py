from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json
import os
from typing import Any


@dataclass(frozen=True)
class SleeveRealizedPnlResultV1:
    report_path: Path
    report: dict[str, Any]


def materialize_sleeve_realized_pnl_v1(
    *,
    day_utc: str,
    truth_root: Path,
    evaluation_utc: str | None = None,
    scheduled_run: bool = False,
) -> SleeveRealizedPnlResultV1:
    day = _require_day(day_utc)
    truth_root = Path(truth_root).resolve()
    produced_at = _now(evaluation_utc)
    trade_path = truth_root / "reports" / "sleeve_trade_fact_v1" / day / "sleeve_trade_fact.v1.json"
    trade_report = _read_json_optional(trade_path) or {}
    trade_facts = [row for row in trade_report.get("trade_facts", []) if isinstance(row, dict)]
    blockers: list[dict[str, Any]] = []

    if not trade_report:
        blockers.append(_blocker("SLEEVE_TRADE_FACT_MISSING", "sleeve_trade_fact_v1", "present", "MISSING", str(trade_path), "Run sleeve trade fact materialization before P&L."))
    if trade_report and str(trade_report.get("status") or "").upper() == "BLOCKED":
        blockers.append(_blocker("SLEEVE_TRADE_FACT_BLOCKED", "sleeve_trade_fact_v1.status", "PASS or NO_COMPLETED_TRADES", "BLOCKED", str(trade_path), "Resolve trade fact blockers before P&L."))
    if not trade_facts and not blockers:
        return _write_report(
            truth_root=truth_root,
            day=day,
            produced_at=produced_at,
            status="NO_COMPLETED_TRADES",
            first_blocker="",
            blockers=[],
            sleeves=[],
            totals=_empty_totals(),
            evidence_paths=[str(trade_path)],
            scheduled_run=scheduled_run,
        )

    reconciliation = _post_trade_reconciliation(truth_root, day)
    if reconciliation.get("status") == "BLOCKED":
        blockers.append(_blocker("BROKER_POSITION_OR_CASH_RECONCILIATION_BLOCKED", "post_trade_reconciliation_v1.status", "not BLOCKED", "BLOCKED", str(reconciliation.get("_source_path") or ""), "Resolve broker-backed post-trade reconciliation before P&L."))
    nav = _read_json_optional(truth_root / "accounting_v2" / "nav" / day / "nav.v2.json") or {}
    cash_ledger = _read_json_optional(truth_root / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json") or {}
    nav_body = nav.get("nav") if isinstance(nav.get("nav"), dict) else nav
    nav_total = _number(nav_body.get("nav_total_cents") or nav_body.get("nav_total"))
    nav_cash = _number(nav_body.get("cash_total_cents") or nav_body.get("cash_total"))
    ledger_nlv = _number(cash_ledger.get("nlv_total_cents"))
    ledger_cash = _number(cash_ledger.get("cash_total_cents"))
    if nav_total <= 0:
        blockers.append(_blocker("NAV_MISSING_OR_NONPOSITIVE", "nav.nav_total_cents", "> 0", nav_total, str(truth_root / "accounting_v2" / "nav" / day / "nav.v2.json"), "Materialize broker-backed NAV before sleeve P&L."))
    if str(cash_ledger.get("status") or "").upper() != "PASS" or str(cash_ledger.get("source_type") or "").upper() != "BROKER_ACCOUNT":
        blockers.append(_blocker("BROKER_BACKED_CASH_LEDGER_MISSING", "cash_ledger.source_type/status", "BROKER_ACCOUNT/PASS", {"status": cash_ledger.get("status"), "source_type": cash_ledger.get("source_type")}, str(truth_root / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json"), "Materialize broker-backed cash ledger before sleeve P&L."))
    if ledger_nlv > 0 and nav_total > 0 and abs(ledger_nlv - nav_total) > 100:
        blockers.append(_blocker("CASH_NAV_MISMATCH", "cash_ledger.nlv_total_cents", nav_total, ledger_nlv, str(truth_root / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json"), "Reconcile broker account summary, cash ledger, and NAV before sleeve P&L."))
    if ledger_cash > 0 and nav_cash > 0 and abs(ledger_cash - nav_cash) > 100:
        blockers.append(_blocker("CASH_NAV_MISMATCH", "cash_ledger.cash_total_cents", nav_cash, ledger_cash, str(truth_root / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json"), "Reconcile broker cash and accounting NAV cash before sleeve P&L."))

    sleeves = _build_sleeve_pnl(trade_facts)
    status = "BLOCKED" if blockers else "PASS"
    return _write_report(
        truth_root=truth_root,
        day=day,
        produced_at=produced_at,
        status=status,
        first_blocker=blockers[0]["blocker_code"] if blockers else "",
        blockers=blockers,
        sleeves=sleeves,
        totals=_totals(sleeves),
        evidence_paths=[str(trade_path), str(truth_root / "accounting_v2" / "nav" / day / "nav.v2.json"), str(truth_root / "cash_ledger_v1" / "snapshots" / day / "cash_ledger_snapshot.v1.json")],
        scheduled_run=scheduled_run,
    )


def _build_sleeve_pnl(trade_facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for fact in trade_facts:
        grouped.setdefault(_text(fact.get("sleeve_id")) or "UNKNOWN", []).append(fact)
    sleeves: list[dict[str, Any]] = []
    for sleeve_id, rows in sorted(grouped.items()):
        buys = sum(_number(row.get("gross_notional")) for row in rows if _side(row) in {"BUY", "BOT"})
        sells = sum(_number(row.get("gross_notional")) for row in rows if _side(row) in {"SELL", "SLD"})
        fees = sum(_number(row.get("fees")) for row in rows)
        signed_qty = sum(_signed_qty(row) for row in rows)
        realized_pnl = round(sells - min(buys, sells) - fees, 6) if sells > 0 else round(-fees, 6)
        gross_pnl = round(sells - min(buys, sells), 6) if sells > 0 else 0.0
        capital_used = max(buys, sells)
        sleeves.append(
            {
                "sleeve_id": sleeve_id,
                "trade_fact_count": len(rows),
                "realized_pnl": realized_pnl,
                "unrealized_pnl": None,
                "unrealized_pnl_status": "UNKNOWN_NO_BROKER_MARK" if abs(signed_qty) > 0 else "FLAT",
                "gross_pnl": gross_pnl,
                "net_pnl_after_fees": realized_pnl,
                "fees": round(fees, 6),
                "capital_used": round(capital_used, 6),
                "return_on_capital": round(realized_pnl / capital_used, 8) if capital_used else 0.0,
                "position_state": "OPEN" if abs(signed_qty) > 0.000001 else "CLOSED",
            }
        )
    return sleeves


def _write_report(*, truth_root: Path, day: str, produced_at: str, status: str, first_blocker: str, blockers: list[dict[str, Any]], sleeves: list[dict[str, Any]], totals: dict[str, Any], evidence_paths: list[str], scheduled_run: bool = False) -> SleeveRealizedPnlResultV1:
    report = {
        "schema_version": "sleeve_realized_pnl.v1",
        "day_utc": day,
        "status": status,
        "first_blocker": first_blocker,
        "produced_at_utc": produced_at,
        "producer": "sleeve_realized_pnl_v1",
        "scheduled_run": bool(scheduled_run),
        "sleeves": sleeves,
        "totals": totals,
        "blockers": blockers,
        "evidence_paths": evidence_paths,
    }
    path = truth_root / "reports" / "sleeve_realized_pnl_v1" / day / "sleeve_realized_pnl.v1.json"
    _write_json(path, report)
    return SleeveRealizedPnlResultV1(report_path=path, report=report)


def _post_trade_reconciliation(root: Path, day: str) -> dict[str, Any]:
    path = root / "reports" / "post_trade_reconciliation_v1" / day / "post_trade_reconciliation.v1.json"
    payload = _read_json_optional(path) or {}
    if payload:
        payload["_source_path"] = str(path)
    return payload


def _totals(sleeves: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "realized_pnl": round(sum(_number(row.get("realized_pnl")) for row in sleeves), 6),
        "net_pnl_after_fees": round(sum(_number(row.get("net_pnl_after_fees")) for row in sleeves), 6),
        "fees": round(sum(_number(row.get("fees")) for row in sleeves), 6),
        "capital_used": round(sum(_number(row.get("capital_used")) for row in sleeves), 6),
    }


def _empty_totals() -> dict[str, Any]:
    return {"realized_pnl": 0.0, "net_pnl_after_fees": 0.0, "fees": 0.0, "capital_used": 0.0}


def _signed_qty(row: dict[str, Any]) -> float:
    qty = _number(row.get("filled_quantity"))
    return -qty if _side(row) in {"SELL", "SLD"} else qty


def _side(row: dict[str, Any]) -> str:
    return _text(row.get("side")).upper()


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
