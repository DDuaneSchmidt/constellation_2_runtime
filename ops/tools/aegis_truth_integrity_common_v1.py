from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ops.tools import run_aegis_bod_prepare_v1 as bod


READY_FINAL_STATUSES = {"PRE_MARKET_READY", "PAPER_READY", "PAPER_READY_WITH_DELAYED_DATA", "TRADING_ACTIVE", "EOD_COMPLETE"}


AUTHORITATIVE_ARTIFACTS: tuple[dict[str, Any], ...] = (
    {"artifact_type": "aegis_day_run_v1", "family": "aegis_day_run_v1", "filename": "day_run.v1.json", "blocking_class": "HARD_BLOCKER", "authoritative": True},
    {"artifact_type": "aegis_requirement_graph_v1", "family": "aegis_requirement_graph_v1", "filename": "requirement_graph.v1.json", "blocking_class": "HARD_BLOCKER", "authoritative": True},
    {"artifact_type": "market_open_data_gate_v1", "family": "market_open_data_gate_v1", "filename": "market_open_data_gate.v1.json", "blocking_class": "HARD_BLOCKER", "authoritative": True},
    {"artifact_type": "market_data_supply_v1", "family": "market_data_supply_v1", "filename": "market_data_supply.v1.json", "blocking_class": "HARD_BLOCKER", "authoritative": True},
    {"artifact_type": "broker_supply_v1", "family": "broker_supply_v1", "filename": "broker_supply.v1.json", "blocking_class": "HARD_BLOCKER", "authoritative": True},
    {"artifact_type": "capital_supply_v1", "family": "capital_supply_v1", "filename": "capital_supply.v1.json", "blocking_class": "HARD_BLOCKER", "authoritative": True},
    {"artifact_type": "risk_budget_supply_v1", "family": "risk_budget_supply_v1", "filename": "risk_budget_supply.v1.json", "blocking_class": "HARD_BLOCKER", "authoritative": True},
    {"artifact_type": "authorization_supply_v1", "family": "authorization_supply_v1", "filename": "authorization_supply.v1.json", "blocking_class": "HARD_BLOCKER", "authoritative": True},
    {"artifact_type": "submit_boundary_status_v1", "family": "submit_boundary_status_v1", "filename": "submit_boundary_status.v1.json", "blocking_class": "HARD_BLOCKER", "authoritative": True},
    {"artifact_type": "trading_day_control_plane_v1", "family": "trading_day_control_plane_v1", "filename": "trading_day_control_plane.v1.json", "blocking_class": "DIAGNOSTIC_ONLY", "authoritative": False},
    {"artifact_type": "aegis_operator_projection_v1", "family": "aegis_operator_projection_v1", "filename": "operator_projection.v1.json", "blocking_class": "DIAGNOSTIC_ONLY", "authoritative": False},
    {"artifact_type": "aegis_live_intelligence_v1", "family": "aegis_live_intelligence_v1", "filename": "live_intelligence.v1.json", "blocking_class": "DIAGNOSTIC_ONLY", "authoritative": False},
)


def now_iso_v1() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso_v1(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return None


def read_json_v1(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json_v1(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n", encoding="utf-8")


def report_path_v1(ctx: bod.BodContext, family: str, filename: str) -> Path:
    return (ctx.truth_root / "reports" / family / ctx.day_utc / filename).resolve()


def artifact_specs_v1() -> list[dict[str, Any]]:
    return [dict(row) for row in AUTHORITATIVE_ARTIFACTS]


def generated_at_v1(payload: dict[str, Any]) -> str:
    for key in ("generated_at_utc", "updated_at_utc", "produced_at_utc", "evaluated_at_utc", "created_at_utc"):
        text = str(payload.get(key) or "").strip()
        if text:
            return text
    pc = payload.get("producer_contract_v1") if isinstance(payload.get("producer_contract_v1"), dict) else {}
    return str(pc.get("generated_at_utc") or "").strip()


def status_of_v1(payload: dict[str, Any]) -> str:
    return str(payload.get("status") or payload.get("final_status") or payload.get("boundary_status") or "UNKNOWN").strip().upper()


def blocker_of_v1(payload: dict[str, Any]) -> str:
    return str(payload.get("canonical_blocker") or payload.get("first_blocker") or payload.get("first_blocker_code") or "").strip()


def canonical_report_paths_v1(ctx: bod.BodContext) -> dict[str, Path]:
    return {
        row["artifact_type"]: report_path_v1(ctx, str(row["family"]), str(row["filename"]))
        for row in artifact_specs_v1()
    }
