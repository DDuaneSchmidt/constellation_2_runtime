from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .artifact_store import DEFAULT_STORE_ROOT

REPORT_DIRNAME = "verified_runtime_graph_evidence_coverage_repair"
DEFAULT_TRUTH_ROOT = Path("/home/node/constellation_runtime_data/truth")

INVENTORY_COLUMNS = [
    "blocker_id",
    "module",
    "capability",
    "blocker_type",
    "severity",
    "current_status",
    "evidence_required",
    "evidence_found",
    "repair_action",
]
REPAIR_COLUMNS = [
    "action_id",
    "module",
    "file_changed",
    "repair_type",
    "description",
    "authority_changed",
    "evidence_fabricated",
    "status",
]
REMAINING_COLUMNS = [
    "blocker_id",
    "module",
    "capability",
    "reason_remaining",
    "required_future_evidence",
    "priority",
]
MATRIX_COLUMNS = [
    "module",
    "capability",
    "declared_status",
    "evidence_artifact",
    "artifact_exists",
    "artifact_registered",
    "artifact_fresh",
    "graph_visible",
    "authority_status",
]


def run_verified_runtime_graph_evidence_coverage_repair(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    truth_root: str | Path = DEFAULT_TRUTH_ROOT,
    day_utc: str | None = None,
    created_at: str | None = None,
    audit_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    report = build_verified_runtime_graph_evidence_coverage_repair(
        root=root,
        truth_root=truth_root,
        day_utc=day_utc,
        created_at=created_at,
        audit_result=audit_result,
    )
    write_verified_runtime_graph_evidence_coverage_repair(report, root=root)
    return report


def build_verified_runtime_graph_evidence_coverage_repair(
    root: str | Path = DEFAULT_STORE_ROOT,
    *,
    truth_root: str | Path = DEFAULT_TRUTH_ROOT,
    day_utc: str | None = None,
    created_at: str | None = None,
    audit_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    truth_path = Path(truth_root)
    created = created_at or _now()
    day = day_utc or created[:10] or date.today().isoformat()
    graph_dir = truth_path / "reports" / "aegis_verified_runtime_graph_v1" / day
    graph = _read_json(graph_dir / "verified_runtime_graph.v1.json", {})
    ledger = _read_json(graph_dir / "evidence_ledger.v1.json", {})
    portal = _read_json(graph_dir / "portal_runtime_model.v1.json", {})

    ledger_by_id = {entry.get("evidence_id", ""): entry for entry in ledger.get("entries", []) if entry.get("evidence_id")}
    capability_status = _capability_status_map(graph, portal)
    capability_links = _capability_link_map(portal)

    inventory: list[dict[str, str]] = []
    repair_actions: list[dict[str, str]] = []
    remaining: list[dict[str, str]] = []
    matrix: list[dict[str, str]] = []

    for index, raw_blocker in enumerate(graph.get("audit_blockers", []) or [], start=1):
        parsed = _parse_blocker(str(raw_blocker))
        blocker_id = f"blocker_{index:03d}"
        evidence_ids = parsed["evidence_ids"]
        evidence_rows = [_evidence_state(eid, ledger_by_id, capability_links.get((parsed["module"], parsed["capability"]), set())) for eid in evidence_ids]
        found_ids = [row["evidence_artifact"] for row in evidence_rows if row["artifact_exists"] == "true" and row["artifact_fresh"] == "true"]
        current_status = capability_status.get((parsed["module"], parsed["capability"]), "UNKNOWN")
        repair_type = _repair_type(current_status, evidence_rows)
        repair_description = _repair_description(repair_type, evidence_ids)
        inventory.append(
            {
                "blocker_id": blocker_id,
                "module": parsed["module"],
                "capability": parsed["capability"],
                "blocker_type": parsed["blocker_type"],
                "severity": "STRICT_GRAPH_BLOCKER",
                "current_status": current_status,
                "evidence_required": ",".join(evidence_ids),
                "evidence_found": ",".join(found_ids),
                "repair_action": repair_type,
            }
        )
        repair_actions.append(
            {
                "action_id": f"action_{index:03d}",
                "module": parsed["module"],
                "file_changed": "NONE",
                "repair_type": repair_type,
                "description": repair_description,
                "authority_changed": "false",
                "evidence_fabricated": "false",
                "status": "REMAINING_BLOCKED" if repair_type in {"DOCUMENT_UNAVAILABLE_EVIDENCE", "NO_SAFE_REPAIR", "MARK_CAPABILITY_BLOCKED"} else "DOCUMENTED",
            }
        )
        if repair_type != "REGISTER_EXISTING_EVIDENCE":
            remaining.append(
                {
                    "blocker_id": blocker_id,
                    "module": parsed["module"],
                    "capability": parsed["capability"],
                    "reason_remaining": "Required graph-visible evidence is missing, stale, unverified, or unavailable; no safe repair can mark it ready.",
                    "required_future_evidence": ",".join(evidence_ids),
                    "priority": "P0",
                }
            )
        for row in evidence_rows:
            matrix.append(
                {
                    "module": parsed["module"],
                    "capability": parsed["capability"],
                    "declared_status": current_status,
                    "authority_status": _authority_status(graph, portal),
                    **row,
                }
            )

    repair_counts = Counter(row["repair_type"] for row in repair_actions)
    runtime_readiness = graph.get("runtime_readiness_status") or {}
    readiness_linkage = graph.get("readiness_linkage_to_runtime_truth_kernel") or {}
    summary = {
        "blockers_inventoried": len(inventory),
        "blockers_repaired": repair_counts.get("REGISTER_EXISTING_EVIDENCE", 0),
        "remaining_blockers": len(remaining),
        "graph_status": graph.get("graph_status", "UNKNOWN"),
        "runtime_truth_classification": _get_mapping_value(readiness_linkage, "runtime_truth_classification", "UNKNOWN"),
        "highest_readiness_layer": _get_mapping_value(runtime_readiness, "highest_readiness_layer", str(runtime_readiness or "UNKNOWN")),
        "authority_changed": False,
        "evidence_fabricated": False,
        "confidence_impact": "NONE",
        "audit_result_recorded": audit_result is not None,
        "repair_type_counts": dict(sorted(repair_counts.items())),
    }
    return {
        "schema_id": "atlas_v2_research_os_verified_runtime_graph_evidence_coverage_repair",
        "schema_version": "1.0",
        "report_type": "VERIFIED_RUNTIME_GRAPH_EVIDENCE_COVERAGE_REPAIR",
        "build": "113",
        "created_at": created,
        "day": day,
        "source_inputs": {
            "verified_runtime_graph": str(graph_dir / "verified_runtime_graph.v1.json"),
            "evidence_ledger": str(graph_dir / "evidence_ledger.v1.json"),
            "portal_runtime_model": str(graph_dir / "portal_runtime_model.v1.json"),
        },
        "summary": summary,
        "audit_result": audit_result or {"command": "npm run aegis:audit", "status": "not_run_by_build_cli"},
        "graph_blocker_inventory": inventory,
        "repair_actions": repair_actions,
        "remaining_blockers": remaining,
        "graph_visibility_matrix": matrix,
        "authority_boundary": "No trading authority, candidate promotion, production promotion, paper placement, position sizing, or trade recommendations.",
    }


def write_verified_runtime_graph_evidence_coverage_repair(report: dict[str, Any], root: str | Path = DEFAULT_STORE_ROOT) -> dict[str, Path]:
    out_dir = Path(root) / REPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "latest_json": out_dir / "latest.json",
        "latest_summary": out_dir / "latest_summary.md",
        "graph_blocker_inventory": out_dir / "graph_blocker_inventory.csv",
        "repair_actions": out_dir / "repair_actions.csv",
        "remaining_blockers": out_dir / "remaining_blockers.csv",
        "graph_visibility_matrix": out_dir / "graph_visibility_matrix.csv",
    }
    paths["latest_json"].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["latest_summary"].write_text(render_verified_runtime_graph_evidence_coverage_repair_summary(report), encoding="utf-8")
    _write_csv(paths["graph_blocker_inventory"], INVENTORY_COLUMNS, report.get("graph_blocker_inventory") or [])
    _write_csv(paths["repair_actions"], REPAIR_COLUMNS, report.get("repair_actions") or [])
    _write_csv(paths["remaining_blockers"], REMAINING_COLUMNS, report.get("remaining_blockers") or [])
    _write_csv(paths["graph_visibility_matrix"], MATRIX_COLUMNS, report.get("graph_visibility_matrix") or [])
    return paths


def render_verified_runtime_graph_evidence_coverage_repair_summary(report: dict[str, Any]) -> str:
    summary = report.get("summary", {})
    return "\n".join(
        [
            "# Build 113 - Verified Runtime Graph Evidence Coverage Repair",
            "",
            f"Graph status: {summary.get('graph_status')}",
            f"Blockers inventoried: {summary.get('blockers_inventoried')}",
            f"Blockers repaired: {summary.get('blockers_repaired')}",
            f"Remaining blockers: {summary.get('remaining_blockers')}",
            f"Authority changed: {str(summary.get('authority_changed')).lower()}",
            f"Evidence fabricated: {str(summary.get('evidence_fabricated')).lower()}",
            f"Confidence impact: {summary.get('confidence_impact')}",
            "",
            "Repairs are limited to graph-visible evidence coverage classification. Missing or stale evidence remains blocked.",
            report.get("authority_boundary", ""),
            "",
        ]
    )


def _parse_blocker(raw: str) -> dict[str, Any]:
    parts = raw.split(":", 3)
    module = parts[0] if len(parts) > 0 else "UNKNOWN"
    capability = parts[1] if len(parts) > 1 else "UNKNOWN"
    blocker_type = parts[2] if len(parts) > 2 else "UNKNOWN"
    evidence = parts[3] if len(parts) > 3 else ""
    return {
        "module": module,
        "capability": capability,
        "blocker_type": blocker_type,
        "evidence_ids": sorted(item.strip() for item in evidence.split(",") if item.strip()),
    }


def _evidence_state(evidence_id: str, ledger_by_id: dict[str, dict[str, Any]], capability_links: set[str]) -> dict[str, str]:
    entry = ledger_by_id.get(evidence_id, {})
    artifact_path = str(entry.get("artifact_path") or "")
    artifact_exists = bool(artifact_path and Path(artifact_path).exists())
    registered = evidence_id in ledger_by_id
    fresh = entry.get("freshness_status") == "CURRENT" and entry.get("hash_verification_status") == "VERIFIED" and entry.get("validation_status") in {"VALID", ""}
    graph_visible = registered and fresh and (artifact_path in capability_links or evidence_id in capability_links)
    return {
        "evidence_artifact": evidence_id,
        "artifact_exists": str(artifact_exists).lower(),
        "artifact_registered": str(registered).lower(),
        "artifact_fresh": str(bool(fresh)).lower(),
        "graph_visible": str(bool(graph_visible)).lower(),
    }


def _repair_type(current_status: str, evidence_rows: list[dict[str, str]]) -> str:
    if evidence_rows and all(row["artifact_exists"] == "true" and row["artifact_fresh"] == "true" and row["graph_visible"] == "false" for row in evidence_rows):
        return "REGISTER_EXISTING_EVIDENCE"
    if evidence_rows and all(row["artifact_exists"] == "false" for row in evidence_rows):
        return "DOCUMENT_UNAVAILABLE_EVIDENCE"
    if current_status == "BLOCKED":
        return "MARK_CAPABILITY_BLOCKED"
    return "NO_SAFE_REPAIR"


def _repair_description(repair_type: str, evidence_ids: list[str]) -> str:
    evidence = ",".join(evidence_ids)
    if repair_type == "REGISTER_EXISTING_EVIDENCE":
        return f"Existing verified artifacts require manifest/graph registration before the blocker can clear: {evidence}"
    if repair_type == "DOCUMENT_UNAVAILABLE_EVIDENCE":
        return f"Required evidence artifacts are unavailable in the verified ledger and remain blocked: {evidence}"
    if repair_type == "MARK_CAPABILITY_BLOCKED":
        return f"Capability remains blocked until required graph-visible evidence exists: {evidence}"
    return f"No safe repair without producer evidence: {evidence}"


def _capability_status_map(graph: dict[str, Any], portal: dict[str, Any]) -> dict[tuple[str, str], str]:
    statuses: dict[tuple[str, str], str] = {}
    for capability in portal.get("capabilities", []) or []:
        statuses[(capability.get("module_id", ""), capability.get("capability_id", ""))] = capability.get("state", "UNKNOWN")
    for transition in graph.get("state_transitions", []) or []:
        statuses[(transition.get("module_id", ""), transition.get("capability_id", ""))] = transition.get("to_state", "UNKNOWN")
    return statuses


def _capability_link_map(portal: dict[str, Any]) -> dict[tuple[str, str], set[str]]:
    links: dict[tuple[str, str], set[str]] = {}
    for capability in portal.get("capabilities", []) or []:
        key = (capability.get("module_id", ""), capability.get("capability_id", ""))
        links[key] = set(str(item) for item in capability.get("evidence_links", []) or [])
    return links


def _authority_status(graph: dict[str, Any], portal: dict[str, Any]) -> str:
    safety = graph.get("safety_invariants") or portal.get("safety_invariants") or {}
    trade_allowed = safety.get("trade_advice_allowed", False)
    broker_allowed = safety.get("broker_execution_allowed", False)
    if trade_allowed or broker_allowed:
        return "AUTHORITY_REVIEW_REQUIRED"
    return "NO_AUTHORITY_CHANGE_RESEARCH_ONLY"


def _get_mapping_value(value: Any, key: str, default: str) -> str:
    if isinstance(value, dict):
        return str(value.get(key, default))
    return default


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
