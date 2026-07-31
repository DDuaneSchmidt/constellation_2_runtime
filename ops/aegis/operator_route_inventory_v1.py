from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ops.aegis.intelligence_common_v1 import now_utc_v1, write_json_v1, read_json_v1
from ops.aegis.operator_surface_contract_v1 import operator_surface_contract_path_v1

REPORT_FAMILY = "aegis_operator_route_inventory_v1"
REPORT_FILENAME = "operator_route_inventory.v1.json"

ROUTES: tuple[dict[str, str | bool], ...] = (
    {"route_id": "aegis_command_center", "path": "/aegis-command-center", "surface_id": "command_center", "template_type": "OperatorInboxTemplate", "contract_required": True, "status_source": "aegis_operator_surface_contract_v1"},
    {"route_id": "aegis_positions", "path": "/aegis-positions", "surface_id": "positions", "template_type": "EntityListTemplate", "contract_required": True, "status_source": "aegis_operator_surface_contract_v1"},
    {"route_id": "aegis_history", "path": "/aegis-history", "surface_id": "history", "template_type": "EntityListTemplate", "contract_required": True, "status_source": "aegis_operator_surface_contract_v1"},
    {"route_id": "aegis_paper_performance", "path": "/aegis-paper-performance", "surface_id": "performance", "template_type": "AnalyticsTemplate", "contract_required": True, "status_source": "aegis_operator_surface_contract_v1"},
    {"route_id": "aegis_sleeve_analytics", "path": "/aegis-sleeve-analytics", "surface_id": "sleeve_analytics", "template_type": "AnalyticsTemplate", "contract_required": True, "status_source": "aegis_operator_surface_contract_v1"},
    {"route_id": "aegis_position_review", "path": "/aegis-position-review", "surface_id": "position_review", "template_type": "ReviewTemplate", "contract_required": True, "status_source": "aegis_operator_surface_contract_v1"},
    {"route_id": "research_lab", "path": "/research-lab", "surface_id": "research", "template_type": "EntityListTemplate", "contract_required": True, "status_source": "aegis_operator_surface_contract_v1"},
    {"route_id": "research_review", "path": "/research-lab/review", "surface_id": "research", "template_type": "ReviewTemplate", "contract_required": True, "status_source": "aegis_operator_surface_contract_v1"},
    {"route_id": "research_blocked_work", "path": "/research-lab/blocked-work", "surface_id": "research", "template_type": "EntityListTemplate", "contract_required": True, "status_source": "aegis_operator_surface_contract_v1"},
    {"route_id": "aegis_opportunities", "path": "/aegis-opportunities", "surface_id": "engineering", "template_type": "TroubleshootingTemplate", "contract_required": True, "status_source": "aegis_operator_surface_contract_v1"},
)

SAFETY = {
    "trade_advice_allowed": False,
    "broker_execution_allowed": False,
    "broker_submit_transmit_allowed": False,
    "live_trading_allowed": False,
    "autonomous_live_trading_allowed": False,
}


def operator_route_inventory_path_v1(*, truth_root: Path | str, day_utc: str) -> Path:
    return Path(truth_root).expanduser().resolve() / "reports" / REPORT_FAMILY / str(day_utc) / REPORT_FILENAME


def build_operator_route_inventory_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    contract_path = operator_surface_contract_path_v1(truth_root=root, day_utc=day)
    contract = read_json_v1(contract_path)
    contract_by_id = contract.get("contract_by_id") if isinstance(contract.get("contract_by_id"), Mapping) else {}
    rows: list[dict[str, Any]] = []
    orphan_routes: list[str] = []
    for route in ROUTES:
        row = dict(route)
        surface = str(row["surface_id"])
        has_contract = surface in contract_by_id
        row["contract_row_present"] = has_contract
        row["contract_status"] = str(contract_by_id.get(surface, {}).get("status") or "MISSING") if isinstance(contract_by_id.get(surface), Mapping) else "MISSING"
        if row.get("contract_required") and not has_contract:
            orphan_routes.append(str(row["route_id"]))
        rows.append(row)
    return {
        "schema_id": REPORT_FAMILY,
        "schema_version": "v1",
        "artifact_id": REPORT_FAMILY,
        "day_utc": day,
        "generated_at": now_utc_v1(),
        "summary": {
            "route_count": len(rows),
            "contract_required_count": sum(1 for row in rows if row.get("contract_required") is True),
            "orphan_route_count": len(orphan_routes),
            "template_types": sorted({str(row["template_type"]) for row in rows}),
        },
        "routes": rows,
        "orphan_routes": orphan_routes,
        "source_artifacts": {"operator_surface_contract": str(contract_path)},
        "safety": dict(SAFETY),
        **SAFETY,
    }


def write_operator_route_inventory_v1(*, truth_root: Path | str, day_utc: str, payload: Mapping[str, Any] | None = None) -> Path:
    body = dict(payload or build_operator_route_inventory_v1(truth_root=truth_root, day_utc=day_utc))
    return write_json_v1(operator_route_inventory_path_v1(truth_root=truth_root, day_utc=day_utc), body)


def run_operator_route_inventory_self_check_v1(*, truth_root: Path | str, day_utc: str) -> dict[str, Any]:
    root = Path(truth_root).expanduser().resolve()
    day = str(day_utc)
    payload = read_json_v1(operator_route_inventory_path_v1(truth_root=root, day_utc=day))
    if not payload:
        payload = build_operator_route_inventory_v1(truth_root=root, day_utc=day)
        write_operator_route_inventory_v1(truth_root=root, day_utc=day, payload=payload)
    failures: list[dict[str, str]] = []
    allowed_templates = {"OperatorInboxTemplate", "EntityListTemplate", "AnalyticsTemplate", "ReviewTemplate", "TroubleshootingTemplate"}
    rows = payload.get("routes") if isinstance(payload.get("routes"), list) else []
    seen_route_ids: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            failures.append({"check": "route_row_mapping", "reason": "route row is not an object"})
            continue
        rid = str(row.get("route_id") or "")
        if not rid:
            failures.append({"check": "route_id_present", "reason": "route_id missing"})
        if rid in seen_route_ids:
            failures.append({"check": "route_id_unique", "route_id": rid, "reason": "duplicate route_id"})
        seen_route_ids.add(rid)
        if not row.get("surface_id"):
            failures.append({"check": "surface_id_present", "route_id": rid, "reason": "surface_id missing"})
        if row.get("template_type") not in allowed_templates:
            failures.append({"check": "template_type_valid", "route_id": rid, "reason": f"invalid template {row.get('template_type')}"})
        if row.get("contract_required") is True and row.get("contract_row_present") is not True:
            failures.append({"check": "contract_row_present", "route_id": rid, "reason": "required contract row missing"})
    for route in payload.get("orphan_routes") or []:
        failures.append({"check": "no_orphan_routes", "route_id": str(route), "reason": "route has no contract row"})
    return {
        "ok": not failures,
        "status": "PASS" if not failures else "FAIL",
        "day_utc": day,
        "path": str(operator_route_inventory_path_v1(truth_root=root, day_utc=day)),
        "summary": payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {},
        "failures": failures,
        "safety": dict(SAFETY),
    }
